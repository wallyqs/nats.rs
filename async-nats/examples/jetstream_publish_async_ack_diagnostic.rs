use async_nats::jetstream::{self, stream};
use clap::{ArgAction, Parser};
use futures::future::join_all;
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

    /// Enable detailed diagnostic output
    #[arg(long, default_value_t = false)]
    diagnostic: bool,
}

#[derive(Debug, Clone)]
struct ClientResult {
    client_id: usize,
    runtime_id: usize,
    success_count: usize,
    error_count: usize,
    connection_time: Duration,
    publish_duration: Duration,
    ack_duration: Duration,
    total_duration: Duration,
    first_publish_time: Duration,
    last_publish_time: Duration,
}

async fn run_client(
    client_id: usize,
    runtime_id: usize,
    messages_per_client: usize,
    outstanding_acks_per_client: usize,
    args: Arc<Args>,
) -> Result<ClientResult, async_nats::Error> {
    let start = Instant::now();
    
    // Create a semaphore for this client
    let semaphore = Arc::new(Semaphore::new(outstanding_acks_per_client));
    
    if !args.no_progress && args.diagnostic {
        println!("[Runtime {} Client {}] Starting with {} outstanding acks", runtime_id, client_id, outstanding_acks_per_client);
    }
    
    // Time connection establishment
    let connection_start = Instant::now();
    let client = if let (Some(user), Some(pass)) = (&args.user, &args.pass) {
        async_nats::ConnectOptions::new()
            .user_and_password(user.clone(), pass.clone())
            .connect(&args.url)
            .await?
    } else {
        async_nats::connect(&args.url).await?
    };
    let connection_time = connection_start.elapsed();
    
    if args.diagnostic {
        println!("[Runtime {} Client {}] Connected in {:?}", runtime_id, client_id, connection_time);
    }
    
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

    // Start timing
    let publish_start = Instant::now();
    let mut first_publish_time = None;
    let mut last_publish_time = None;

    // Publish all messages without awaiting acks
    let mut ack_futures = Vec::with_capacity(messages_per_client);
    
    for i in 0..messages_per_client {
        let permit_start = Instant::now();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        
        if args.diagnostic && i < 5 {
            println!("[Runtime {} Client {}] Acquired permit {} in {:?}", runtime_id, client_id, i, permit_start.elapsed());
        }

        let publish_msg_start = Instant::now();
        let ack_future = jetstream
            .publish(subjects[i % subjects_len].clone(), payload.clone().into())
            .await?;
        
        if first_publish_time.is_none() {
            first_publish_time = Some(publish_msg_start.elapsed());
        }
        last_publish_time = Some(publish_msg_start.elapsed());

        if args.diagnostic && i < 5 {
            println!("[Runtime {} Client {}] Published message {} in {:?}", runtime_id, client_id, i, publish_msg_start.elapsed());
        }

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
        connection_time,
        publish_duration,
        ack_duration,
        total_duration,
        first_publish_time: first_publish_time.unwrap_or_default(),
        last_publish_time: last_publish_time.unwrap_or_default(),
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
    
    println!("=== DIAGNOSTIC CONFIGURATION ===");
    println!("  Total runtimes: {}", args.runtimes);
    println!("  Worker threads per runtime: {}", threads_per_runtime);
    println!("  Total clients: {}", args.clients);
    println!("  Clients per runtime: ~{}", base_clients_per_runtime);
    println!("  Total messages: {}", args.count);
    println!("  Messages per client: ~{}", base_messages_per_client);
    println!("  Outstanding acks per client: {}", outstanding_acks_per_client);
    println!("  Total outstanding acks: {}", outstanding_acks_per_client * args.clients);
    println!("  Message size: {} bytes", args.size);
    println!("  Total data: {:.2} MB", (args.count * args.size) as f64 / 1024.0 / 1024.0);
    
    if outstanding_acks_per_client < 10 {
        println!("  ⚠️  WARNING: Outstanding acks per client ({}) is very low!", outstanding_acks_per_client);
        println!("     This severely limits concurrency. Try increasing --batch or reducing --clients");
    }
    
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
    
    // Aggregate results and analyze
    let mut total_success = 0;
    let mut total_errors = 0;
    let mut total_connection_time = Duration::default();
    let mut max_connection_time = Duration::default();
    let mut min_connection_time = Duration::from_secs(3600); // Start with a large value
    
    for result in &all_results {
        total_success += result.success_count;
        total_errors += result.error_count;
        total_connection_time += result.connection_time;
        max_connection_time = max_connection_time.max(result.connection_time);
        min_connection_time = min_connection_time.min(result.connection_time);
    }
    
    // Print diagnostic analysis
    println!("=== DIAGNOSTIC ANALYSIS ===");
    if !all_results.is_empty() {
        println!("Connection Times:");
        println!("  Average: {:?}", total_connection_time / all_results.len() as u32);
        println!("  Min: {:?}", min_connection_time);
        println!("  Max: {:?}", max_connection_time);
        
        if max_connection_time > Duration::from_millis(100) {
            println!("  ⚠️  Connection establishment is slow, this might be a bottleneck");
        }
        
        // Analyze per-client performance variance
        let mut publish_rates: Vec<f64> = all_results.iter()
            .map(|r| r.success_count as f64 / r.publish_duration.as_secs_f64())
            .collect();
        publish_rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let median_rate = publish_rates[publish_rates.len() / 2];
        let min_rate = publish_rates[0];
        let max_rate = publish_rates[publish_rates.len() - 1];
        
        println!("Per-Client Publish Rates:");
        println!("  Min: {:.0} msgs/sec", min_rate);
        println!("  Median: {:.0} msgs/sec", median_rate);
        println!("  Max: {:.0} msgs/sec", max_rate);
        
        if max_rate / min_rate > 2.0 {
            println!("  ⚠️  High variance in client performance - possible resource contention");
        }
    }
    
    // Print aggregate results
    println!("\n=== AGGREGATE RESULTS ===");
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