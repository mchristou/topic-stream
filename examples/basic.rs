use std::sync::Arc;
use tokio::task;

use topic_stream::TopicStream;

#[tokio::main]
async fn main() {
    let topic_stream = Arc::new(TopicStream::<String, String>::new(10));

    let topic = "news".to_string();
    let mut receiver = topic_stream.subscribe(&[topic.clone()]);

    // Spawn a publisher
    let publisher = topic_stream.clone();
    task::spawn(async move {
        publisher
            .publish(&topic, "Publish message".to_string())
            .await
            .unwrap();
    });

    // Receive the message
    if let Some(message) = receiver.recv().await {
        println!("Received: {}", message);
    }
}
