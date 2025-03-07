use dashmap::DashMap;
use futures::future::select_all;
use std::{hash::Hash, sync::Arc};
use tokio::sync::broadcast;

#[derive(Debug)]
pub struct FastPub<T: Eq + Hash + Clone, M: Clone> {
    subscribers: Arc<DashMap<T, broadcast::Sender<M>>>,
    capacity: usize,
}

impl<T: Eq + Hash + Clone, M: Clone> FastPub<T, M> {
    pub fn new(capacity: usize) -> Self {
        Self {
            subscribers: Arc::new(DashMap::new()),
            capacity,
        }
    }

    pub fn subscribe(&self, topics: &[T]) -> MultiTopicReceiver<M> {
        let receivers: Vec<_> = topics
            .iter()
            .map(|topic| {
                // Check if the topic exists in the map, and if not, insert a new sender
                let sender = self
                    .subscribers
                    .entry(topic.clone())
                    .or_insert_with(|| broadcast::channel(self.capacity).0); // Create a new channel if topic doesn't exist

                // Subscribe to the sender and get the receiver for this topic
                sender.subscribe()
            })
            .collect();

        MultiTopicReceiver::new(receivers)
    }

    pub fn publish(&self, topic: &T, message: M) {
        if let Some(sender) = self.subscribers.get(topic) {
            let _ = sender.send(message);
        }
    }
}

#[derive(Debug)]
pub struct MultiTopicReceiver<M: Clone> {
    receivers: Vec<broadcast::Receiver<M>>,
}

impl<M: Clone> MultiTopicReceiver<M> {
    pub fn new(receivers: Vec<broadcast::Receiver<M>>) -> Self {
        Self { receivers }
    }

    pub async fn recv(&mut self) -> Option<M> {
        // Collect all receiver futures into a vector
        let futures = self
            .receivers
            .iter_mut()
            .map(|receiver| Box::pin(receiver.recv()))
            .collect::<Vec<_>>();

        // Use select_all to select the first resolved future
        let (result, _index, _remaining) = select_all(futures).await;

        match result {
            Ok(m) => Some(m), // Return the first message received
            Err(_) => None,   // If all receivers error out, return None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hash;

    #[derive(Debug, Clone, Hash, Eq, PartialEq)]
    struct Topic(String);

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct Message(String);

    #[tokio::test]
    async fn test_subscribe_and_publish_single_subscriber() {
        let fast_sub = FastPub::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to the topic
        let mut receiver = fast_sub.subscribe(&[topic.clone()]);

        // Publisher sends a message to the topic
        let message = Message("Hello, Subscriber!".to_string());
        fast_sub.publish(&topic, message.clone());

        // Subscriber receives the message
        let received_message = receiver.recv().await.unwrap();
        assert_eq!(received_message, message);
    }

    #[tokio::test]
    async fn test_subscribe_multiple_subscribers() {
        let fast_sub = FastPub::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber 1 subscribes to the topic
        let mut receiver1 = fast_sub.subscribe(&[topic.clone()]);
        // Subscriber 2 subscribes to the topic
        let mut receiver2 = fast_sub.subscribe(&[topic.clone()]);

        // Publisher sends a message to the topic
        let message = Message("Hello, Subscribers!".to_string());
        fast_sub.publish(&topic, message.clone());

        // Subscriber 1 receives the message
        let received_message1 = receiver1.recv().await.unwrap();
        assert_eq!(received_message1, message);

        // Subscriber 2 receives the message
        let received_message2 = receiver2.recv().await.unwrap();
        assert_eq!(received_message2, message);
    }

    #[tokio::test]
    async fn test_publish_to_unsubscribed_topic() {
        let fast_sub = FastPub::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to a non-existent topic
        let mut receiver = fast_sub.subscribe(&[Topic("invalid_topic".to_string())]);

        // Publisher sends a message to the topic with no subscribers
        let message = Message("Hello, World!".to_string());
        fast_sub.publish(&topic, message.clone());

        // No subscribers, so nothing to receive
        // Here we assume that nothing crashes or any side effects occur.
        // Test should pass as no message should be received

        // Use a timeout to ensure the test completes
        let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(1));
        tokio::select! {
            _ = timeout => {
                // Timeout reached, test completes
            }
            _ = receiver.recv() => {
                panic!("Unexpected message received after timeout");
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_messages_for_single_subscriber() {
        let fast_sub = FastPub::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to the topic
        let mut receiver = fast_sub.subscribe(&[topic.clone()]);

        // Publisher sends multiple messages
        let message1 = Message("Message 1".to_string());
        let message2 = Message("Message 2".to_string());
        fast_sub.publish(&topic, message1.clone());
        fast_sub.publish(&topic, message2.clone());

        // Subscriber receives the first message
        let received_message1 = receiver.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        // Subscriber receives the second message
        let received_message2 = receiver.recv().await.unwrap();
        assert_eq!(received_message2, message2);
    }

    #[tokio::test]
    async fn test_multiple_publishers() {
        let fast_sub = FastPub::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to the topic
        let mut receiver = fast_sub.subscribe(&[topic.clone()]);

        // Publisher 1 sends a message
        let message1 = Message("Message from Publisher 1".to_string());
        fast_sub.publish(&topic, message1.clone());

        // Publisher 2 sends a message
        let message2 = Message("Message from Publisher 2".to_string());
        fast_sub.publish(&topic, message2.clone());

        // Subscriber receives the first message
        let received_message1 = receiver.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        // Subscriber receives the second message
        let received_message2 = receiver.recv().await.unwrap();
        assert_eq!(received_message2, message2);
    }

    #[tokio::test]
    async fn test_subscribe_to_different_topics() {
        let fast_sub = FastPub::<Topic, Message>::new(2);
        let topic1 = Topic("test_topic_1".to_string());
        let topic2 = Topic("test_topic_2".to_string());

        // Subscriber subscribes to topic 1
        let mut receiver1 = fast_sub.subscribe(&[topic1.clone()]);

        // Publisher sends a message to topic 1
        let message1 = Message("Hello, Topic 1".to_string());
        fast_sub.publish(&topic1, message1.clone());

        // Subscriber 1 receives the message for topic 1
        let received_message1 = receiver1.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        // Subscriber subscribes to topic 2
        let mut receiver2 = fast_sub.subscribe(&[topic2.clone()]);

        // Publisher sends a message to topic 2
        let message2 = Message("Hello, Topic 2".to_string());
        fast_sub.publish(&topic2, message2.clone());

        // Subscriber 2 receives the message for topic 2
        let received_message2 = receiver2.recv().await.unwrap();
        assert_eq!(received_message2, message2);
    }

    #[tokio::test]
    async fn test_single_receiver_multiple_topics() {
        let fast_sub = FastPub::<Topic, Message>::new(2);

        // Define multiple topics
        let topic1 = Topic("test_topic_1".to_string());
        let topic2 = Topic("test_topic_2".to_string());
        let topic3 = Topic("test_topic_3".to_string());

        // Subscriber subscribes to multiple topics
        let mut receiver = fast_sub.subscribe(&[topic1.clone(), topic2.clone(), topic3.clone()]);

        // Publisher sends messages to each topic
        let message1 = Message("Message for Topic 1".to_string());
        let message2 = Message("Message for Topic 2".to_string());
        let message3 = Message("Message for Topic 3".to_string());

        fast_sub.publish(&topic1, message1.clone());
        fast_sub.publish(&topic2, message2.clone());
        fast_sub.publish(&topic3, message3.clone());

        // Subscriber should receive the messages in the order they were published
        let received_message1 = receiver.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        let received_message2 = receiver.recv().await.unwrap();
        assert_eq!(received_message2, message2);

        let received_message3 = receiver.recv().await.unwrap();
        assert_eq!(received_message3, message3);
    }
}
