# topic-stream

topic_stream is an asynchronous, topic-based publish-subscribe library for Rust,
designed to provide an efficient way to broadcast messages to multiple subscribers.
It leverages async-broadcast for message passing and dashmap for concurrent topic management.

## Features

- **Topic-Based Messaging**: Subscribers receive messages based on topics they subscribe to.

- **Asynchronous & Non-Blocking**: Uses async-broadcast for efficient message delivery.

- **Multiple Subscribers per Topic**: Supports multiple receivers listening to the same topic.

- **Multi-Topic Subscription**: Subscribe to multiple topics simultaneously and receive messages from all of them.


## Usage

Here's a basic example of how to use `topic_stream`:

```rust
use topic_stream::TopicStream;

#[tokio::main]
async fn main() {
    let topic_stream = TopicStream::<String, String>::new(10);

    let topic = "news".to_string();
    let mut receiver = topic_stream.subscribe(&[topic.clone()]);

    topic_stream
        .publish(&topic, "Publish message".to_string())
        .await
        .unwrap();

    // Receive the message
    if let Some(message) = receiver.recv().await {
        println!("Received: {}", message);
    }
}
```

## Running Tests
```
cargo test
```

### License

This project is licensed under the MIT License.

### Contributions

Contributions, issues, and feature requests are welcome! Feel free to submit a PR or open an issue.
