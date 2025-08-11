use clap::Parser;
use std::process::Command;
use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Multi-process wrapper for jetstream_publish_async_ack", long_about = None)]
struct Args {
    /// Number of messages to publish total
    #[arg(short, long, alias = "msgs", default_value_t = 10000)]
    count: usize,

    /// Size of each message in bytes
    #[arg(long, default_value_t = 32)]
    size: usize,

    /// Subjects to publish to
    #[arg(long, default_value = "bench.test", value_delimiter = ',')]
    subjects: Vec<String>,

    /// Stream name
    #[arg(long, default_value = "BENCH_STREAM")]
    stream: String,

    /// NATS server URL
    #[arg(short = 's', long = "server", default_value = "nats://localhost:4222")]
    url: String,

    /// Max outstanding acks per client
    #[arg(long, alias = "batch", default_value_t = 10000)]
    outstanding_acks: usize,

    /// Whether to create the stream
    #[arg(long, default_value_t = false)]
    create_stream: bool,

    /// Username for NATS authentication
    #[arg(long)]
    user: Option<String>,

    /// Password for NATS authentication
    #[arg(long)]
    pass: Option<String>,

    /// Number of parallel clients per process
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Number of Tokio worker threads per process
    #[arg(long, alias = "workers")]
    threads: Option<usize>,

    /// Number of independent Tokio runtimes per process
    #[arg(long, default_value_t = 1)]
    runtimes: usize,

    /// Number of independent OS processes to spawn
    #[arg(long, default_value_t = 1)]
    processes: usize,

    /// Suppress progress output
    #[arg(long, default_value_t = false)]
    no_progress: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    if args.processes == 1 {
        // Just run the single process version directly
        let mut cmd = Command::new("cargo");
        cmd.arg("run")
            .arg("--release")
            .arg("--example")
            .arg("jetstream_publish_async_ack")
            .arg("--");
        
        build_command_args(&mut cmd, &args, args.count, 0);
        
        let status = cmd.status()?;
        std::process::exit(status.code().unwrap_or(1));
    }
    
    println!("Starting {} independent processes...", args.processes);
    println!("Total messages to publish: {}", args.count);
    println!("Messages per process: ~{}", args.count / args.processes);
    println!("Clients per process: {}", args.clients);
    println!("Total clients: {}", args.clients * args.processes);
    println!();
    
    let start = Instant::now();
    
    // Calculate messages per process
    let base_messages_per_process = args.count / args.processes;
    let remainder = args.count % args.processes;
    
    // Spawn all processes
    let mut handles = Vec::new();
    let total_success = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    
    for process_id in 0..args.processes {
        let messages_for_process = if process_id < remainder {
            base_messages_per_process + 1
        } else {
            base_messages_per_process
        };
        
        let args_clone = args.clone();
        let success_clone = total_success.clone();
        let errors_clone = total_errors.clone();
        
        let handle = thread::spawn(move || {
            let mut cmd = Command::new("cargo");
            cmd.arg("run")
                .arg("--release")
                .arg("--example")
                .arg("jetstream_publish_async_ack")
                .arg("--");
            
            build_command_args(&mut cmd, &args_clone, messages_for_process, process_id);
            
            if !args_clone.no_progress {
                println!("[Process {}] Starting with {} messages...", process_id, messages_for_process);
            }
            
            match cmd.output() {
                Ok(output) => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    
                    // Parse the output to extract success/error counts
                    if let Some(success) = parse_metric(&stdout, "Total messages acknowledged:") {
                        success_clone.fetch_add(success, Ordering::Relaxed);
                    }
                    if let Some(errors) = parse_metric(&stdout, "Total errors:") {
                        errors_clone.fetch_add(errors, Ordering::Relaxed);
                    }
                    
                    if !args_clone.no_progress {
                        println!("[Process {}] Output:\n{}", process_id, stdout);
                    }
                    
                    if !output.status.success() {
                        eprintln!("[Process {}] Failed with status: {}", process_id, output.status);
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        if !stderr.is_empty() {
                            eprintln!("[Process {}] Stderr:\n{}", process_id, stderr);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[Process {}] Failed to execute: {}", process_id, e);
                    errors_clone.fetch_add(messages_for_process, Ordering::Relaxed);
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all processes
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    
    let total_elapsed = start.elapsed();
    let success_count = total_success.load(Ordering::Relaxed);
    let error_count = total_errors.load(Ordering::Relaxed);
    
    // Print aggregate results
    println!("\n=== Multi-Process Aggregate Results ===");
    println!("Total processes: {}", args.processes);
    println!("Total messages published: {}", args.count);
    println!("Total messages acknowledged: {}", success_count);
    if error_count > 0 {
        println!("Total errors: {}", error_count);
    }
    println!("Total time: {:?}", total_elapsed);
    
    let aggregate_rate = success_count as f64 / total_elapsed.as_secs_f64();
    let aggregate_throughput =
        (success_count * args.size) as f64 / total_elapsed.as_secs_f64() / 1024.0 / 1024.0;
    
    println!("\nAggregate Performance:");
    println!("  Message rate: {:.0} msgs/sec", aggregate_rate);
    println!("  Throughput: {:.2} MB/sec", aggregate_throughput);
    if success_count > 0 {
        println!(
            "  Avg latency: {:.2} ms/msg",
            total_elapsed.as_millis() as f64 / success_count as f64
        );
    }
    
    Ok(())
}

fn build_command_args(cmd: &mut Command, args: &Args, messages: usize, process_id: usize) {
    cmd.arg("--count").arg(messages.to_string());
    cmd.arg("--size").arg(args.size.to_string());
    cmd.arg("--subjects").arg(args.subjects.join(","));
    cmd.arg("--stream").arg(&args.stream);
    cmd.arg("--server").arg(&args.url);
    cmd.arg("--outstanding-acks").arg(args.outstanding_acks.to_string());
    cmd.arg("--clients").arg(args.clients.to_string());
    cmd.arg("--runtimes").arg(args.runtimes.to_string());
    
    if let Some(threads) = args.threads {
        cmd.arg("--threads").arg(threads.to_string());
    }
    
    if let Some(user) = &args.user {
        cmd.arg("--user").arg(user);
    }
    
    if let Some(pass) = &args.pass {
        cmd.arg("--pass").arg(pass);
    }
    
    // Only first process creates the stream
    if args.create_stream && process_id == 0 {
        cmd.arg("--create-stream").arg("true");
    }
    
    if args.no_progress {
        cmd.arg("--no-progress");
    }
}

fn parse_metric(output: &str, prefix: &str) -> Option<usize> {
    output.lines()
        .find(|line| line.contains(prefix))
        .and_then(|line| {
            line.split(':')
                .nth(1)
                .and_then(|s| s.trim().parse().ok())
        })
}