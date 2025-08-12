use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Simple echo server for testing
    tokio::spawn(echo_server());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Client test
    let start = Instant::now();
    let bytes_transferred = Arc::new(AtomicU64::new(0));
    
    let handles: Vec<_> = (0..1000)
        .map(|_| {
            let counter = Arc::clone(&bytes_transferred);
            tokio::spawn(async move {
                benchmark_client(counter).await
            })
        })
        .collect();
    
    futures::future::try_join_all(handles).await?;
    
    let elapsed = start.elapsed();
    let total_bytes = bytes_transferred.load(Ordering::Relaxed);
    let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
    let mb_per_sec = total_mb / elapsed.as_secs_f64();
    let gbps = (total_bytes as f64 * 8.0) / (elapsed.as_secs_f64() * 1_000_000_000.0);
    
    println!("Total: {:.2} MB in {:?}", total_mb, elapsed);
    println!("Throughput: {:.2} MB/s, {:.2} Gbps", mb_per_sec, gbps);
    
    Ok(())
}

async fn echo_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if socket.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

async fn benchmark_client(counter: Arc<AtomicU64>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
    
    let data = vec![1u8; 1024 * 1024]; // 1MB chunks
    let mut buf = vec![0u8; 1024 * 1024];
    
    for _ in 0..100 { // 100MB per connection
        stream.write_all(&data).await?;
        stream.read_exact(&mut buf).await?;
        counter.fetch_add(data.len() as u64 * 2, Ordering::Relaxed); // Send + receive
    }
    
    Ok(())
}