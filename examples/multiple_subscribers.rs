use std::sync::Arc;

use topic_stream::TopicStream;

#[tokio::main]
async fn main() {
    let topic_stream = Arc::new(TopicStream::<String, String>::new(10));

    let topic = "updates".to_string();
    let mut receiver1 = topic_stream.subscribe(&[topic.clone()]);
    let mut receiver2 = topic_stream.subscribe(&[topic.clone()]);

    topic_stream.publish(&topic, "Version 1.1 released!".to_string());

    if let Some(message) = receiver1.recv().await {
        println!("Receiver 1 got: {}", message);
    }

    if let Some(message) = receiver2.recv().await {
        println!("Receiver 2 got: {}", message);
    }
}
