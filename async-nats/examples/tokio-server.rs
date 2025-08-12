use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("0.0.0.0:4222").await?;
    
    let connections = Arc::new(AtomicUsize::new(0));
    let bytes_received = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();
    
    println!("Server listening on 0.0.0.0:4222");
    println!("Waiting for connections...");
    
    // Spawn stats reporter
    let conn_clone = connections.clone();
    let bytes_rx_clone = bytes_received.clone();
    let bytes_tx_clone = bytes_sent.clone();
    tokio::spawn(async move {
        let mut last_bytes_rx = 0u64;
        let mut last_bytes_tx = 0u64;
        let mut last_time = Instant::now();
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            
            let current_conns = conn_clone.load(Ordering::Relaxed);
            let current_bytes_rx = bytes_rx_clone.load(Ordering::Relaxed);
            let current_bytes_tx = bytes_tx_clone.load(Ordering::Relaxed);
            let current_time = Instant::now();
            
            let elapsed = current_time.duration_since(last_time).as_secs_f64();
            let rx_rate = (current_bytes_rx - last_bytes_rx) as f64 / elapsed / (1024.0 * 1024.0);
            let tx_rate = (current_bytes_tx - last_bytes_tx) as f64 / elapsed / (1024.0 * 1024.0);
            
            let total_elapsed = current_time.duration_since(start_time);
            let total_mb_rx = current_bytes_rx as f64 / (1024.0 * 1024.0);
            let total_mb_tx = current_bytes_tx as f64 / (1024.0 * 1024.0);
            
            println!("\n=== Server Stats ===");
            println!("Active connections: {}", current_conns);
            println!("Total received: {:.2} MB", total_mb_rx);
            println!("Total sent: {:.2} MB", total_mb_tx);
            println!("Current rate: RX {:.2} MB/s, TX {:.2} MB/s", rx_rate, tx_rate);
            println!("Uptime: {:?}", total_elapsed);
            
            last_bytes_rx = current_bytes_rx;
            last_bytes_tx = current_bytes_tx;
            last_time = current_time;
        }
    });
    
    loop {
        let (socket, addr) = listener.accept().await?;
        connections.fetch_add(1, Ordering::Relaxed);
        
        let conn_id = connections.load(Ordering::Relaxed);
        println!("Connection #{} from {}", conn_id, addr);
        
        let connections = connections.clone();
        let bytes_received = bytes_received.clone();
        let bytes_sent = bytes_sent.clone();
        
        tokio::spawn(async move {
            handle_connection(socket, bytes_received, bytes_sent).await;
            connections.fetch_sub(1, Ordering::Relaxed);
            println!("Connection from {} closed", addr);
        });
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    bytes_received: Arc<AtomicU64>,
    bytes_sent: Arc<AtomicU64>,
) {
    let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer
    
    loop {
        match socket.read(&mut buf).await {
            Ok(0) => break, // Connection closed
            Ok(n) => {
                bytes_received.fetch_add(n as u64, Ordering::Relaxed);
                
                // Echo back the data
                if let Err(_) = socket.write_all(&buf[..n]).await {
                    break;
                }
                bytes_sent.fetch_add(n as u64, Ordering::Relaxed);
            }
            Err(_) => break,
        }
    }
}