use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of connections to create
    #[arg(short, long, default_value_t = 1000)]
    connections: usize,
    
    /// Size of each message in bytes
    #[arg(long, default_value_t = 1024)]
    size: usize,
    
    /// Number of messages to send per connection
    #[arg(short, long, default_value_t = 100)]
    messages: usize,
    
    /// Server address
    #[arg(short, long, default_value = "127.0.0.1:4222")]
    server: String,
    
    /// Delay between creating connections (milliseconds)
    #[arg(long, default_value_t = 10)]
    connection_delay: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    
    println!("Connecting {} clients to {}", args.connections, args.server);
    println!("Each client will send {} messages of {} bytes", args.messages, args.size);
    
    let start = Instant::now();
    let active_connections = Arc::new(AtomicUsize::new(0));
    let total_bytes_sent = Arc::new(AtomicU64::new(0));
    let total_bytes_received = Arc::new(AtomicU64::new(0));
    let completed_connections = Arc::new(AtomicUsize::new(0));
    let failed_connections = Arc::new(AtomicUsize::new(0));
    
    // Spawn stats reporter
    let active = active_connections.clone();
    let sent = total_bytes_sent.clone();
    let received = total_bytes_received.clone();
    let completed = completed_connections.clone();
    let failed = failed_connections.clone();
    let total_conns = args.connections;
    
    tokio::spawn(async move {
        let mut last_sent = 0u64;
        let mut last_received = 0u64;
        let mut last_time = Instant::now();
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            
            let current_active = active.load(Ordering::Relaxed);
            let current_sent = sent.load(Ordering::Relaxed);
            let current_received = received.load(Ordering::Relaxed);
            let current_completed = completed.load(Ordering::Relaxed);
            let current_failed = failed.load(Ordering::Relaxed);
            let current_time = Instant::now();
            
            let elapsed = current_time.duration_since(last_time).as_secs_f64();
            let send_rate = (current_sent - last_sent) as f64 / elapsed / (1024.0 * 1024.0);
            let recv_rate = (current_received - last_received) as f64 / elapsed / (1024.0 * 1024.0);
            
            println!("\n=== Client Stats ===");
            println!("Active: {}/{}", current_active, total_conns);
            println!("Completed: {}, Failed: {}", current_completed, current_failed);
            println!("Total sent: {:.2} MB, received: {:.2} MB", 
                     current_sent as f64 / (1024.0 * 1024.0),
                     current_received as f64 / (1024.0 * 1024.0));
            println!("Current rate: TX {:.2} MB/s, RX {:.2} MB/s", send_rate, recv_rate);
            
            last_sent = current_sent;
            last_received = current_received;
            last_time = current_time;
            
            // Exit reporter when all connections are done
            if current_completed + current_failed >= total_conns {
                break;
            }
        }
    });
    
    // Create connections with optional delay
    let mut handles = Vec::new();
    for i in 0..args.connections {
        let server = args.server.clone();
        let message_size = args.size;
        let message_count = args.messages;
        let active = active_connections.clone();
        let sent = total_bytes_sent.clone();
        let received = total_bytes_received.clone();
        let completed = completed_connections.clone();
        let failed = failed_connections.clone();
        
        let handle = tokio::spawn(async move {
            active.fetch_add(1, Ordering::Relaxed);
            
            match run_client(&server, message_size, message_count, sent.clone(), received.clone()).await {
                Ok(_) => {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("Client {} failed: {}", i, e);
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            active.fetch_sub(1, Ordering::Relaxed);
        });
        
        handles.push(handle);
        
        // Small delay between connection attempts to avoid overwhelming the server
        if args.connection_delay > 0 && i < args.connections - 1 {
            tokio::time::sleep(tokio::time::Duration::from_millis(args.connection_delay)).await;
        }
    }
    
    // Wait for all clients to complete
    for handle in handles {
        let _ = handle.await;
    }
    
    let elapsed = start.elapsed();
    let total_sent = total_bytes_sent.load(Ordering::Relaxed);
    let total_received = total_bytes_received.load(Ordering::Relaxed);
    let completed = completed_connections.load(Ordering::Relaxed);
    let failed = failed_connections.load(Ordering::Relaxed);
    
    println!("\n=== Final Results ===");
    println!("Time elapsed: {:?}", elapsed);
    println!("Connections: {} completed, {} failed", completed, failed);
    println!("Total data sent: {:.2} MB", total_sent as f64 / (1024.0 * 1024.0));
    println!("Total data received: {:.2} MB", total_received as f64 / (1024.0 * 1024.0));
    println!("Average throughput: TX {:.2} MB/s, RX {:.2} MB/s",
             total_sent as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
             total_received as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64());
    
    let total_gbps = ((total_sent + total_received) * 8) as f64 / elapsed.as_secs_f64() / 1_000_000_000.0;
    println!("Combined throughput: {:.3} Gbps", total_gbps);
    
    Ok(())
}

async fn run_client(
    server: &str,
    message_size: usize,
    message_count: usize,
    bytes_sent: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = TcpStream::connect(server).await?;
    
    let data = vec![0x42u8; message_size]; // Fill with 'B' bytes
    let mut buf = vec![0u8; message_size];
    
    for _ in 0..message_count {
        // Send data
        stream.write_all(&data).await?;
        bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);
        
        // Read echo response
        stream.read_exact(&mut buf).await?;
        bytes_received.fetch_add(buf.len() as u64, Ordering::Relaxed);
    }
    
    Ok(())
}