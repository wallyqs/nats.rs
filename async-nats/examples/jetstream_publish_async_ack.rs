use async_nats::jetstream::{self, stream};
use clap::{ArgAction, Parser};
use futures::future::join_all;
use rand::Rng;
use std::future::IntoFuture;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser, Debug, Clone)]
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

    /// Number of Tokio worker threads per runtime (defaults to number of CPU cores divided by runtimes)
    #[arg(long)]
    threads: Option<usize>,

    /// Number of independent Tokio runtimes to create (defaults to 1)
    #[arg(long, default_value_t = 1)]
    runtimes: usize,

    /// Suppress progress output, only show configuration and final results
    #[arg(long, default_value_t = false)]
    no_progress: bool,

    /// Maximum connection jitter in milliseconds (0 to disable)
    #[arg(long, default_value_t = 100)]
    connection_jitter_ms: u64,

    /// Synchronized start delay in milliseconds before publishing begins
    #[arg(long, default_value_t = 250)]
    start_delay_ms: u64,

    /// Maximum number of blocking threads in the runtime thread pool
    #[arg(long, default_value_t = 512)]
    max_blocking_threads: usize,
}

#[derive(Debug, Clone)]
struct ClientResult {
    client_id: usize,
    runtime_id: usize,
    success_count: usize,
    error_count: usize,
    publish_duration: Duration,
    ack_duration: Duration,
    total_duration: Duration,
}

async fn run_client(
    client_id: usize,
    runtime_id: usize,
    messages_per_client: usize,
    outstanding_acks_per_client: usize,
    args: Arc<Args>,
) -> Result<ClientResult, async_nats::Error> {
    // Create a semaphore for this client
    let semaphore = Arc::new(Semaphore::new(outstanding_acks_per_client));
    
    // Add random jitter to connection establishment to avoid thundering herd
    if args.connection_jitter_ms > 0 {
        let jitter_ms = rand::thread_rng().gen_range(0..=args.connection_jitter_ms);
        tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
    }
    
    if !args.no_progress {
        println!("[Runtime {} Client {}] Connecting to {}...", runtime_id, client_id, args.url);
    }
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
        if !args.no_progress {
            println!("[Runtime {} Client {}] Creating stream '{}'", runtime_id, client_id, args.stream);
        }
        jetstream
            .get_or_create_stream(stream::Config {
                name: args.stream.clone(),
                subjects: args.subjects.clone(),
                ..Default::default()
            })
            .await?;
    }

    if !args.no_progress {
        println!(
            "[Runtime {} Client {}] Publishing {} messages of {} bytes each",
            runtime_id, client_id, messages_per_client, args.size
        );
    }

    // Prepare the message payload
    let payload = vec![b'X'; args.size];
    let subjects = &args.subjects;
    let subjects_len = subjects.len();

    // Synchronized start delay - all clients wait before beginning to publish
    if args.start_delay_ms > 0 {
        if !args.no_progress && client_id < 5 {
            println!("[Runtime {} Client {}] Waiting {}ms before starting to publish...", 
                runtime_id, client_id, args.start_delay_ms);
        }
        tokio::time::sleep(Duration::from_millis(args.start_delay_ms)).await;
    }

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
    if !args.no_progress {
        println!(
            "[Runtime {} Client {}] All {} messages published in {:?}",
            runtime_id, client_id, messages_per_client, publish_duration
        );
    }

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
                if error_count <= 5 && !args.no_progress {
                    eprintln!("[Runtime {} Client {}] Ack error: {}", runtime_id, client_id, e);
                }
            }
            Err(e) => {
                error_count += 1;
                if error_count <= 5 && !args.no_progress {
                    eprintln!("[Runtime {} Client {}] Task join error: {}", runtime_id, client_id, e);
                }
            }
        }
    }

    Ok(ClientResult {
        client_id,
        runtime_id,
        success_count,
        error_count,
        publish_duration,
        ack_duration,
        total_duration,
    })
}

async fn run_runtime_clients(
    runtime_id: usize,
    client_ids: Vec<usize>,
    messages_per_client: Vec<usize>,
    outstanding_acks_per_client: usize,
    args: Arc<Args>,
) -> Vec<Result<ClientResult, async_nats::Error>> {
    let mut client_tasks = Vec::new();
    
    for (idx, &client_id) in client_ids.iter().enumerate() {
        let messages = messages_per_client[idx];
        let args_clone = args.clone();
        
        let task = tokio::spawn(async move {
            run_client(client_id, runtime_id, messages, outstanding_acks_per_client, args_clone).await
        });
        client_tasks.push(task);
    }
    
    let results = join_all(client_tasks).await;
    results.into_iter().map(|r| r.unwrap_or_else(|e| Err(async_nats::Error::from(std::io::Error::new(std::io::ErrorKind::Other, e))))).collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let args_arc = Arc::new(args.clone());
    
    // Calculate distribution
    let base_messages_per_client = args.count / args.clients;
    let remainder = args.count % args.clients;
    let outstanding_acks_per_client = args.outstanding_acks; // Each client gets the full batch size
    
    // Calculate clients per runtime
    let base_clients_per_runtime = args.clients / args.runtimes;
    let runtime_remainder = args.clients % args.runtimes;
    
    // Determine threads per runtime
    let threads_per_runtime = if let Some(threads) = args.threads {
        threads
    } else {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        std::cmp::max(1, num_cpus / args.runtimes)
    };
    
    println!("Configuration:");
    println!("  Total runtimes: {}", args.runtimes);
    println!("  Worker threads per runtime: {}", threads_per_runtime);
    println!("  Max blocking threads per runtime: {}", args.max_blocking_threads);
    println!("  Total clients: {}", args.clients);
    println!("  Clients per runtime: ~{}", base_clients_per_runtime);
    println!("  Total messages: {}", args.count);
    println!("  Messages per client: ~{}", base_messages_per_client);
    println!("  Outstanding acks per client: {}", outstanding_acks_per_client);
    println!("  Total outstanding acks: {}", outstanding_acks_per_client * args.clients);
    println!("  Connection jitter: {}ms max", args.connection_jitter_ms);
    println!("  Start delay: {}ms", args.start_delay_ms);
    println!();
    
    let start = Instant::now();
    
    // Create and run multiple runtimes in separate threads
    let mut handles = Vec::new();
    let mut client_offset = 0;
    
    for runtime_id in 0..args.runtimes {
        // Calculate how many clients this runtime gets
        let clients_for_runtime = if runtime_id < runtime_remainder {
            base_clients_per_runtime + 1
        } else {
            base_clients_per_runtime
        };
        
        // Assign client IDs for this runtime
        let client_ids: Vec<usize> = (client_offset..client_offset + clients_for_runtime).collect();
        client_offset += clients_for_runtime;
        
        // Calculate messages for each client in this runtime
        let messages_per_client: Vec<usize> = client_ids.iter().map(|&id| {
            if id < remainder {
                base_messages_per_client + 1
            } else {
                base_messages_per_client
            }
        }).collect();
        
        let args_clone = args_arc.clone();
        
        // Spawn a thread for each runtime
        let handle = std::thread::spawn(move || {
            // Build a new runtime for this thread
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads_per_runtime)
                .max_blocking_threads(args_clone.max_blocking_threads)
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime");
            
            // Run the clients in this runtime
            runtime.block_on(run_runtime_clients(
                runtime_id,
                client_ids,
                messages_per_client,
                outstanding_acks_per_client,
                args_clone,
            ))
        });
        
        handles.push(handle);
    }
    
    // Wait for all runtimes to complete
    let mut all_results = Vec::new();
    for (runtime_id, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(runtime_results) => {
                for result in runtime_results {
                    match result {
                        Ok(client_result) => all_results.push(client_result),
                        Err(e) => {
                            if !args.no_progress {
                                eprintln!("Runtime {} client error: {}", runtime_id, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if !args.no_progress {
                    eprintln!("Runtime {} thread panic: {:?}", runtime_id, e);
                }
            }
        }
    }
    
    let total_elapsed = start.elapsed();
    
    // Aggregate results
    let mut total_success = 0;
    let mut total_errors = 0;
    
    for result in &all_results {
        total_success += result.success_count;
        total_errors += result.error_count;
    }
    
    // Print per-client results (optional, can be verbose)
    if args.clients <= 10 && !args.no_progress {
        println!("\n=== Per-Client Results ===");
        for result in &all_results {
            println!("Runtime {} Client {}:", result.runtime_id, result.client_id);
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
    }
    
    // Print per-runtime summary
    if args.runtimes > 1 && !args.no_progress {
        println!("=== Per-Runtime Summary ===");
        for runtime_id in 0..args.runtimes {
            let runtime_results: Vec<&ClientResult> = all_results.iter()
                .filter(|r| r.runtime_id == runtime_id)
                .collect();
            
            if !runtime_results.is_empty() {
                let runtime_success: usize = runtime_results.iter().map(|r| r.success_count).sum();
                let runtime_errors: usize = runtime_results.iter().map(|r| r.error_count).sum();
                
                println!("Runtime {}:", runtime_id);
                println!("  Clients: {}", runtime_results.len());
                println!("  Messages acknowledged: {}", runtime_success);
                if runtime_errors > 0 {
                    println!("  Errors: {}", runtime_errors);
                }
            }
        }
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
    if total_success > 0 {
        println!(
            "  Avg latency: {:.2} ms/msg",
            total_elapsed.as_millis() as f64 / total_success as f64
        );
    }
    
    Ok(())
}
