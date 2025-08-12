use async_nats;
use clap::Parser;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(author, version, about = "NATS request method benchmark", long_about = None)]
struct Args {
    /// Number of parallel clients
    #[arg(long, default_value_t = 10)]
    clients: usize,

    /// Size of the payload in bytes
    #[arg(long, default_value_t = 1024)]
    size: usize,

    /// Number of inflight requests per client before blocking
    #[arg(long, alias = "batch", default_value_t = 100)]
    batch: usize,

    /// NATS server URL
    #[arg(short = 's', long, alias = "server", default_value = "nats://localhost:4222")]
    url: String,

    /// Username for authentication
    #[arg(long)]
    user: Option<String>,

    /// Password for authentication
    #[arg(long)]
    pass: Option<String>,

    /// Total number of messages to send (distributed among clients)
    #[arg(long, alias = "messages", default_value_t = 10000)]
    msgs: usize,

    /// Subject to use for requests
    #[arg(long, default_value = "bench.request")]
    subject: String,

    /// Request timeout in seconds
    #[arg(long, default_value_t = 5)]
    timeout: u64,

    /// Enable multi-subject mode (distributes requests across numbered subjects)
    #[arg(long)]
    multisubject: bool,

    /// Maximum subject number for multi-subject mode (subjects will be numbered 00 to max)
    #[arg(long, default_value_t = 63)]
    multisubjectmax: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    // Generate subject list
    let subjects = generate_subjects(&args);
    
    println!("NATS Request Benchmark");
    println!("======================");
    println!("Server: {}", args.url);
    println!("Clients: {}", args.clients);
    println!("Total messages: {}", args.msgs);
    println!("Message size: {} bytes", args.size);
    println!("Inflight requests per client: {}", args.batch);
    if args.multisubject {
        println!("Subjects: {}.00 to {}.{:02}", args.subject, args.subject, args.multisubjectmax);
        println!("Total subjects: {}", subjects.len());
    } else {
        println!("Subject: {}", args.subject);
    }
    println!();

    // First, set up a responder service
    println!("Setting up responder service...");
    let _responder_client = create_client(&args).await?;
    
    // Subscribe to all subjects
    // for subject in &subjects {
    //     let client = responder_client.clone();
    //     let mut subscriber = client.subscribe(subject.clone()).await?;
    //     let response_payload = vec![b'R'; args.size]; // Response payload
    // 
    //     // Spawn responder task for this subject
    //     tokio::spawn(async move {
    //         while let Some(msg) = subscriber.next().await {
    //             if let Some(reply) = msg.reply {
    //                 let _ = client
    //                     .publish(reply, response_payload.clone().into())
    //                     .await;
    //             }
    //         }
    //     });
    // }

    // Give responder time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stats tracking
    let total_requests = Arc::new(AtomicU64::new(0));
    let successful_requests = Arc::new(AtomicU64::new(0));
    let failed_requests = Arc::new(AtomicU64::new(0));
    let total_latency_us = Arc::new(AtomicU64::new(0));
    let active_requests = Arc::new(AtomicUsize::new(0));
    let latencies: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    // Calculate messages per client
    let base_count = args.msgs / args.clients;
    let remainder = args.msgs % args.clients;

    println!("Starting benchmark with {} clients...\n", args.clients);
    let start = Instant::now();

    // Spawn stats reporter
    let stats_total = total_requests.clone();
    let stats_success = successful_requests.clone();
    let stats_failed = failed_requests.clone();
    let stats_active = active_requests.clone();
    let stats_latency = total_latency_us.clone();
    let total_msgs = args.msgs;
    
    let stats_handle = tokio::spawn(async move {
        let mut last_total = 0u64;
        let mut last_time = Instant::now();
        
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            
            let current_total = stats_total.load(Ordering::Relaxed);
            let current_success = stats_success.load(Ordering::Relaxed);
            let current_failed = stats_failed.load(Ordering::Relaxed);
            let current_active = stats_active.load(Ordering::Relaxed);
            let current_time = Instant::now();
            
            if current_success + current_failed >= total_msgs as u64 {
                break;
            }
            
            let elapsed = current_time.duration_since(last_time).as_secs_f64();
            let rate = (current_total - last_total) as f64 / elapsed;
            
            let avg_latency = if current_success > 0 {
                stats_latency.load(Ordering::Relaxed) as f64 / current_success as f64 / 1000.0
            } else {
                0.0
            };
            
            println!("Progress: {}/{} | Rate: {:.0} req/s | Active: {} | Success: {} | Failed: {} | Avg Latency: {:.2} ms",
                     current_total, total_msgs, rate, current_active, current_success, current_failed, avg_latency);
            
            last_total = current_total;
            last_time = current_time;
        }
    });

    // Create and run clients
    let mut handles = Vec::new();
    
    for client_id in 0..args.clients {
        // Distribute messages evenly
        let messages_for_client = if client_id < remainder {
            base_count + 1
        } else {
            base_count
        };

        let client_args = args.clone();
        let client_subjects = subjects.clone();
        let total_req = total_requests.clone();
        let success_req = successful_requests.clone();
        let failed_req = failed_requests.clone();
        let latency_tracker = total_latency_us.clone();
        let active_req = active_requests.clone();
        let client_latencies = latencies.clone();

        let handle = tokio::spawn(async move {
            match run_client(
                client_id,
                messages_for_client,
                client_args,
                client_subjects,
                total_req,
                success_req,
                failed_req,
                latency_tracker,
                active_req,
                client_latencies,
            ).await {
                Ok(_) => {},
                Err(e) => eprintln!("Client {} error: {}", client_id, e),
            }
        });

        handles.push(handle);
    }

    // Wait for all clients to complete
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    stats_handle.abort();

    // Print final results
    let total = total_requests.load(Ordering::Relaxed);
    let success = successful_requests.load(Ordering::Relaxed);
    let failed = failed_requests.load(Ordering::Relaxed);
    let total_latency = total_latency_us.load(Ordering::Relaxed);

    println!("\n=== Final Results ===");
    println!("Duration: {:?}", elapsed);
    println!("Total requests: {}", total);
    println!("Successful: {}", success);
    println!("Failed: {}", failed);
    
    if success > 0 {
        let avg_latency_ms = total_latency as f64 / success as f64 / 1000.0;
        println!("Average latency: {:.2} ms", avg_latency_ms);
        
        // Calculate actual percentiles
        if let Ok(mut latency_data) = latencies.lock() {
            if !latency_data.is_empty() {
                latency_data.sort_unstable();
                
                let p50 = calculate_percentile(&latency_data, 50.0) / 1000.0;
                let p75 = calculate_percentile(&latency_data, 75.0) / 1000.0;
                let p90 = calculate_percentile(&latency_data, 90.0) / 1000.0;
                let p99 = calculate_percentile(&latency_data, 99.0) / 1000.0;
                let p99_9 = calculate_percentile(&latency_data, 99.9) / 1000.0;
                
                println!("Latency percentiles:");
                println!("  p50:  {:.2} ms", p50);
                println!("  p75:  {:.2} ms", p75);
                println!("  p90:  {:.2} ms", p90);
                println!("  p99:  {:.2} ms", p99);
                println!("  p99.9: {:.2} ms", p99_9);
            }
        }
    }
    
    let req_per_sec = total as f64 / elapsed.as_secs_f64();
    let total_bytes = (total as usize * args.size * 2) as f64; // Request + response
    let throughput_mb = total_bytes / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    let throughput_gbps = (total_bytes * 8.0) / 1_000_000_000.0 / elapsed.as_secs_f64();
    
    println!("\nPerformance:");
    println!("  Request rate: {:.0} req/s", req_per_sec);
    println!("  Throughput: {:.2} MB/s, {:.3} Gbps", throughput_mb, throughput_gbps);
    println!("  Messages per client: ~{}", args.msgs / args.clients);

    Ok(())
}

fn generate_subjects(args: &Args) -> Vec<String> {
    if args.multisubject {
        (0..=args.multisubjectmax)
            .map(|i| format!("{}.{:02}", args.subject, i))
            .collect()
    } else {
        vec![args.subject.clone()]
    }
}

fn calculate_percentile(sorted_latencies: &[u64], percentile: f64) -> f64 {
    if sorted_latencies.is_empty() {
        return 0.0;
    }
    
    let index = (percentile / 100.0) * (sorted_latencies.len() - 1) as f64;
    let lower_index = index.floor() as usize;
    let upper_index = index.ceil() as usize;
    
    if lower_index == upper_index {
        sorted_latencies[lower_index] as f64
    } else {
        let weight = index - lower_index as f64;
        let lower_value = sorted_latencies[lower_index] as f64;
        let upper_value = sorted_latencies[upper_index] as f64;
        lower_value + weight * (upper_value - lower_value)
    }
}

async fn create_client(args: &Args) -> Result<async_nats::Client, Box<dyn std::error::Error + Send + Sync>> {
    let client = if let (Some(user), Some(pass)) = (&args.user, &args.pass) {
        async_nats::ConnectOptions::new()
            .user_and_password(user.clone(), pass.clone())
            .connect(&args.url)
            .await?
    } else {
        async_nats::connect(&args.url).await?
    };
    Ok(client)
}

async fn run_client(
    client_id: usize,
    message_count: usize,
    args: Args,
    subjects: Vec<String>,
    total_requests: Arc<AtomicU64>,
    successful_requests: Arc<AtomicU64>,
    failed_requests: Arc<AtomicU64>,
    total_latency_us: Arc<AtomicU64>,
    active_requests: Arc<AtomicUsize>,
    latencies: Arc<Mutex<Vec<u64>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = create_client(&args).await?;
    let payload = vec![b'X'; args.size];
    let timeout = Duration::from_secs(args.timeout);
    
    // Semaphore to limit inflight requests
    let semaphore = Arc::new(Semaphore::new(args.batch));
    let mut tasks = Vec::new();

    for i in 0..message_count {
        let client = client.clone();
        // Round-robin across subjects
        let subject = subjects[i % subjects.len()].clone();
        let payload = payload.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        
        total_requests.fetch_add(1, Ordering::Relaxed);
        active_requests.fetch_add(1, Ordering::Relaxed);
        
        let success = successful_requests.clone();
        let failed = failed_requests.clone();
        let latency = total_latency_us.clone();
        let active = active_requests.clone();
        let latency_vec = latencies.clone();
        
        let task = tokio::spawn(async move {
            let start = Instant::now();
            
            match tokio::time::timeout(
                timeout,
                client.request(subject, payload.into())
            ).await {
                Ok(Ok(_response)) => {
                    let elapsed_us = start.elapsed().as_micros() as u64;
                    latency.fetch_add(elapsed_us, Ordering::Relaxed);
                    
                    // Store individual latency for percentile calculation
                    if let Ok(mut latencies) = latency_vec.lock() {
                        latencies.push(elapsed_us);
                    }
                    
                    success.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    eprintln!("Client {} request {} error: {}", client_id, i, e);
                    failed.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    eprintln!("Client {} request {} timeout", client_id, i);
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            active.fetch_sub(1, Ordering::Relaxed);
            drop(permit); // Release the semaphore permit
        });
        
        tasks.push(task);
    }

    // Wait for all requests to complete
    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}

// Helper to clone Args (derive Clone for Args)
impl Clone for Args {
    fn clone(&self) -> Self {
        Args {
            clients: self.clients,
            size: self.size,
            batch: self.batch,
            url: self.url.clone(),
            user: self.user.clone(),
            pass: self.pass.clone(),
            msgs: self.msgs,
            subject: self.subject.clone(),
            timeout: self.timeout,
            multisubject: self.multisubject,
            multisubjectmax: self.multisubjectmax,
        }
    }
}
