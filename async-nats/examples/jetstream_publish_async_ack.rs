use async_nats::jetstream::{self, stream};
use clap::{ArgAction, Parser};
use futures::future::join_all;
use std::future::IntoFuture;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of messages to publish
    #[arg(short, long, alias = "msgs", default_value_t = 10000)]
    count: usize,

    /// Size of each message in bytes
    #[arg(long, default_value_t = 32)]
    size: usize,

    /// Subject to publish to
    #[arg(long, default_value = "bench.test", value_delimiter = ',')]
    subjects: Vec<String>,

    /// Stream name
    #[arg(long, default_value = "BENCH_STREAM")]
    stream: String,

    /// NATS server URL
    #[arg(short = 's', long = "server", default_value = "nats://localhost:4222")]
    url: String,

    /// Max outstanding acks
    #[arg(long, alias = "batch", default_value_t = 10000)]
    outstanding_acks: usize,

    /// Whether to create the stream or assert its existence
    #[arg(long, default_value_t = false, action = ArgAction::Set, value_parser = clap::value_parser!(bool))]
    create_stream: bool,

    /// Username for NATS authentication
    #[arg(long)]
    user: Option<String>,

    /// Password for NATS authentication
    #[arg(long)]
    pass: Option<String>,

    /// Number of parallel clients to create
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Number of Tokio worker threads (defaults to number of CPU cores)
    #[arg(long)]
    threads: Option<usize>,
}

#[derive(Debug)]
struct ClientResult {
    client_id: usize,
    success_count: usize,
    error_count: usize,
    publish_duration: Duration,
    ack_duration: Duration,
    total_duration: Duration,
}

async fn run_client(
    client_id: usize,
    messages_per_client: usize,
    outstanding_acks_per_client: usize,
    args: Arc<Args>,
) -> Result<ClientResult, async_nats::Error> {
    // Create a semaphore for this client
    let semaphore = Arc::new(Semaphore::new(outstanding_acks_per_client));
    println!("[Client {}] Connecting to {}...", client_id, args.url);
    let client = if let (Some(user), Some(pass)) = (&args.user, &args.pass) {
        async_nats::ConnectOptions::new()
            .user_and_password(user.clone(), pass.clone())
            .connect(&args.url)
            .await?
    } else {
        async_nats::connect(&args.url).await?
    };
    let jetstream = jetstream::new(client);

    // Only the first client creates the stream
    if client_id == 0 && args.create_stream {
        println!("[Client {}] Creating stream '{}'", client_id, args.stream);
        jetstream
            .get_or_create_stream(stream::Config {
                name: args.stream.clone(),
                subjects: args.subjects.clone(),
                ..Default::default()
            })
            .await?;
    }

    println!(
        "[Client {}] Publishing {} messages of {} bytes each",
        client_id, messages_per_client, args.size
    );

    // Prepare the message payload
    let payload = vec![b'X'; args.size];
    let subjects = &args.subjects;
    let subjects_len = subjects.len();

    // Start timing
    let start = Instant::now();
    let publish_start = start;

    // Publish all messages without awaiting acks
    let mut ack_futures = Vec::with_capacity(messages_per_client);
    for i in 0..messages_per_client {
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let ack_future = jetstream
            .publish(subjects[i % subjects_len].clone(), payload.clone().into())
            .await?;

        // Spawn a task to release the permit when ack completes
        let ack_with_permit = tokio::spawn(async move {
            let result = ack_future.into_future().await;
            drop(permit); // Release the semaphore permit
            result
        });

        ack_futures.push(ack_with_permit);
    }

    let publish_duration = publish_start.elapsed();
    println!(
        "[Client {}] All {} messages published in {:?}",
        client_id, messages_per_client, publish_duration
    );

    let ack_start = Instant::now();
    let results = join_all(ack_futures).await;
    let ack_duration = ack_start.elapsed();
    let total_duration = start.elapsed();

    // Check for any errors
    let mut success_count = 0;
    let mut error_count = 0;
    for result in results {
        match result {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(e)) => {
                error_count += 1;
                if error_count <= 5 {
                    eprintln!("[Client {}] Ack error: {}", client_id, e);
                }
            }
            Err(e) => {
                error_count += 1;
                if error_count <= 5 {
                    eprintln!("[Client {}] Task join error: {}", client_id, e);
                }
            }
        }
    }

    Ok(ClientResult {
        client_id,
        success_count,
        error_count,
        publish_duration,
        ack_duration,
        total_duration,
    })
}

fn main() -> Result<(), async_nats::Error> {
    let args = Args::parse();

    // Build the runtime with the specified number of threads
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all();
    
    if let Some(threads) = args.threads {
        runtime_builder.worker_threads(threads);
        println!("Using {} worker threads", threads);
    } else {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        println!("Using {} worker threads (default, based on CPU cores)", num_cpus);
    }

    let runtime = runtime_builder.build().expect("Failed to build Tokio runtime");
    
    runtime.block_on(async_main(Arc::new(args)))
}

async fn async_main(args: Arc<Args>) -> Result<(), async_nats::Error> {

    // Calculate messages per client
    let base_messages_per_client = args.count / args.clients;
    let remainder = args.count % args.clients;

    // Calculate outstanding acks per client
    let outstanding_acks_per_client = args.outstanding_acks / args.clients;

    println!("Starting {} client(s)...", args.clients);
    println!("Total messages to publish: {}", args.count);
    println!("Messages per client: ~{}", base_messages_per_client);
    println!("Outstanding acks per client: {}", outstanding_acks_per_client);
    println!("Total outstanding acks: {}", outstanding_acks_per_client * args.clients);
    println!();

    let start = Instant::now();

    // Spawn all client tasks
    let mut client_tasks = Vec::with_capacity(args.clients);
    for client_id in 0..args.clients {
        // Distribute remainder messages among first clients
        let messages_for_this_client = if client_id < remainder {
            base_messages_per_client + 1
        } else {
            base_messages_per_client
        };

        let args_clone = args.clone();
        let task = tokio::spawn(async move {
            run_client(client_id, messages_for_this_client, outstanding_acks_per_client, args_clone).await
        });
        client_tasks.push(task);
    }

    // Wait for all clients to complete
    let results = join_all(client_tasks).await;

    let total_elapsed = start.elapsed();

    // Aggregate results
    let mut total_success = 0;
    let mut total_errors = 0;
    let mut all_results = Vec::new();

    for (idx, result) in results.into_iter().enumerate() {
        match result {
            Ok(Ok(client_result)) => {
                total_success += client_result.success_count;
                total_errors += client_result.error_count;
                all_results.push(client_result);
            }
            Ok(Err(e)) => {
                eprintln!("Client {} failed: {}", idx, e);
                // We don't know exactly how many messages this client was supposed to send
                // but we can estimate based on the total count and number of clients
                let estimated_messages = args.count / args.clients + if idx < (args.count % args.clients) { 1 } else { 0 };
                total_errors += estimated_messages;
            }
            Err(e) => {
                eprintln!("Client {} task failed: {}", idx, e);
                let estimated_messages = args.count / args.clients + if idx < (args.count % args.clients) { 1 } else { 0 };
                total_errors += estimated_messages;
            }
        }
    }

    // Print individual client results
    println!("\n=== Per-Client Results ===");
    for result in &all_results {
        println!("Client {}:", result.client_id);
        println!("  Messages acknowledged: {}", result.success_count);
        if result.error_count > 0 {
            println!("  Errors: {}", result.error_count);
        }
        println!("  Publish time: {:?}", result.publish_duration);
        println!("  Ack wait time: {:?}", result.ack_duration);
        println!("  Total time: {:?}", result.total_duration);

        let client_rate = result.success_count as f64 / result.total_duration.as_secs_f64();
        let client_throughput = (result.success_count * args.size) as f64
            / result.total_duration.as_secs_f64()
            / 1024.0
            / 1024.0;
        println!("  Message rate: {:.0} msgs/sec", client_rate);
        println!("  Throughput: {:.2} MB/sec", client_throughput);
        println!();
    }

    // Print aggregate results
    println!("=== Aggregate Results ===");
    println!("Total messages published: {}", args.count);
    println!("Total messages acknowledged: {}", total_success);
    if total_errors > 0 {
        println!("Total errors: {}", total_errors);
    }
    println!("Total time: {:?}", total_elapsed);

    let aggregate_rate = total_success as f64 / total_elapsed.as_secs_f64();
    let aggregate_throughput =
        (total_success * args.size) as f64 / total_elapsed.as_secs_f64() / 1024.0 / 1024.0;

    println!("\nAggregate Performance:");
    println!("  Message rate: {:.0} msgs/sec", aggregate_rate);
    println!("  Throughput: {:.2} MB/sec", aggregate_throughput);
    if args.clients > 0 {
        println!(
            "  Avg latency: {:.2} ms/msg",
            total_elapsed.as_millis() as f64 / total_success as f64
        );
    }

    Ok(())
}