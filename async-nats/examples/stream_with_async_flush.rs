use async_nats::jetstream::{self, stream};
use std::env;

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    // Get connection parameters from environment variables or use defaults
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_user = env::var("NATS_USER").unwrap_or_else(|_| "js".to_string());
    let nats_pass = env::var("NATS_PASS").unwrap_or_else(|_| "js".to_string());
    
    // Build connection options with user/pass authentication
    let client = async_nats::ConnectOptions::new()
        .user_and_password(nats_user, nats_pass)
        .connect(&nats_url)
        .await?;

    // Access the JetStream Context
    let jetstream = jetstream::new(client);

    // Create a stream named "bar" with allow_async_flush, allow_msg_counter, 
    // and allow_atomic_publish enabled, with 2 replicas
    let stream_config = stream::Config {
        name: "bar".to_string(),
        subjects: vec!["bar.>".to_string()],
        num_replicas: 2,
        allow_async_flush: true,
        allow_msg_counter: true,
        allow_atomic_publish: true,
        ..Default::default()
    };

    println!("Creating stream 'bar' with async flush, message counter, and atomic publish enabled, with 2 replicas...");

    // Create the stream
    match jetstream.create_stream(stream_config).await {
        Ok(stream) => {
            println!("Stream '{}' created successfully!", stream.cached_info().config.name);
            println!("Number of replicas: {}", stream.cached_info().config.num_replicas);
            println!("Allow async flush: {}", stream.cached_info().config.allow_async_flush);
            println!("Allow message counter: {}", stream.cached_info().config.allow_msg_counter);
            println!("Allow atomic publish: {}", stream.cached_info().config.allow_atomic_publish);
            
            // Publish a test message to verify the stream works
            jetstream
                .publish("bar.test", "Hello from async stream!".into())
                .await?
                .await?;

            println!("Test message published successfully!");
        }
        Err(e) => {
            println!("Failed to create stream: {}", e);
            // Try to get existing stream instead
            match jetstream.get_stream("bar").await {
                Ok(stream) => {
                    println!("Stream 'bar' already exists!");
                    println!("Number of replicas: {}", stream.cached_info().config.num_replicas);
                    println!("Allow async flush: {}", stream.cached_info().config.allow_async_flush);
                    println!("Allow message counter: {}", stream.cached_info().config.allow_msg_counter);
                    println!("Allow atomic publish: {}", stream.cached_info().config.allow_atomic_publish);
                }
                Err(e) => println!("Failed to get existing stream: {}", e),
            }
        }
    }

    Ok(())
}
