use std::sync::Arc;

use topic_stream::TopicStream;

#[tokio::main]
async fn main() {
    let topic_stream = Arc::new(TopicStream::<String, String>::new(10));

    let topics = vec!["sports".to_string(), "weather".to_string()];
    let mut receiver = topic_stream.subscribe(&topics);

    topic_stream
        .publish(&"sports".to_string(), "Football match tonight".to_string())
        .await
        .unwrap();
    topic_stream
        .publish(&"weather".to_string(), "Rain expected tomorrow".to_string())
        .await
        .unwrap();

    if let Some(message) = receiver.recv().await {
        println!("Received: {}", message);
    }

    if let Some(message) = receiver.recv().await {
        println!("Received: {}", message);
    }
}
