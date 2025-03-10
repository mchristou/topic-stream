use dashmap::DashMap;
use futures::future::select_all;
use std::{collections::HashSet, hash::Hash, sync::Arc};
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct TopicStream<T: Eq + Hash + Clone, M: Clone> {
    subscribers: Arc<DashMap<T, broadcast::Sender<M>>>,
    capacity: usize,
}

impl<T: Eq + Hash + Clone, M: Clone> TopicStream<T, M> {
    pub fn new(capacity: usize) -> Self {
        Self {
            subscribers: Arc::new(DashMap::with_capacity(capacity)),
            capacity,
        }
    }

    pub fn subscribe(&self, topics: &[T]) -> MultiTopicReceiver<T, M> {
        let mut receiver = MultiTopicReceiver::new(Arc::new(self.clone()));
        receiver.subscribe(topics);
        receiver
    }

    pub fn publish(&self, topic: &T, message: M) {
        if let Some(sender) = self.subscribers.get(topic) {
            let _ = sender.send(message);
        }
    }

    fn get_or_create_sender(&self, topic: &T) -> broadcast::Sender<M> {
        let topic = topic.clone();
        self.subscribers
            .entry(topic)
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .clone()
    }
}

#[derive(Debug)]
pub struct MultiTopicReceiver<T: Eq + Hash + Clone, M: Clone> {
    topic_stream: Arc<TopicStream<T, M>>,
    receivers: Vec<broadcast::Receiver<M>>, // Stores the active receivers
    subscribed_topics: HashSet<T>,          // Tracks subscribed topics
}

impl<T: Eq + Hash + Clone, M: Clone> MultiTopicReceiver<T, M> {
    pub fn new(topic_stream: Arc<TopicStream<T, M>>) -> Self {
        Self {
            topic_stream,
            receivers: Vec::new(),
            subscribed_topics: HashSet::new(),
        }
    }

    pub fn subscribe(&mut self, topics: &[T]) {
        for topic in topics {
            if self.subscribed_topics.insert(topic.clone()) {
                let sender = self.topic_stream.get_or_create_sender(topic);
                self.receivers.push(sender.subscribe());
            }
        }
    }

    pub async fn recv(&mut self) -> Option<M> {
        if self.receivers.is_empty() {
            return None;
        }

        let futures = self
            .receivers
            .iter_mut()
            .map(|receiver| Box::pin(receiver.recv()))
            .collect::<Vec<_>>();

        let (result, _index, _remaining) = select_all(futures).await;

        result.ok() // If message received, return it. If all error out, return None.
    }
}

impl<T: Eq + Hash + Clone, M: Clone> Drop for MultiTopicReceiver<T, M> {
    fn drop(&mut self) {
        let mut to_remove = Vec::new();

        for topic in &self.subscribed_topics {
            if let Some(sender) = self.topic_stream.subscribers.get(topic) {
                if sender.receiver_count() == 1 {
                    to_remove.push(topic.clone());
                }
            }
        }

        for topic in to_remove {
            self.topic_stream.subscribers.remove(&topic);
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
        let publisher = TopicStream::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to the topic
        let mut receiver = publisher.subscribe(&[topic.clone()]);

        // Publisher sends a message to the topic
        let message = Message("Hello, Subscriber!".to_string());
        publisher.publish(&topic, message.clone());

        // Subscriber receives the message
        let received_message = receiver.recv().await.unwrap();
        assert_eq!(received_message, message);
    }

    #[tokio::test]
    async fn test_subscribe_multiple_subscribers() {
        let publisher = TopicStream::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber 1 subscribes to the topic
        let mut receiver1 = publisher.subscribe(&[topic.clone()]);
        // Subscriber 2 subscribes to the topic
        let mut receiver2 = publisher.subscribe(&[topic.clone()]);

        // Publisher sends a message to the topic
        let message = Message("Hello, Subscribers!".to_string());
        publisher.publish(&topic, message.clone());

        // Subscriber 1 receives the message
        let received_message1 = receiver1.recv().await.unwrap();
        assert_eq!(received_message1, message);

        // Subscriber 2 receives the message
        let received_message2 = receiver2.recv().await.unwrap();
        assert_eq!(received_message2, message);
    }

    #[tokio::test]
    async fn test_publish_to_unsubscribed_topic() {
        let publisher = TopicStream::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to a non-existent topic
        let mut receiver = publisher.subscribe(&[Topic("invalid_topic".to_string())]);

        // Publisher sends a message to the topic with no subscribers
        let message = Message("Hello, World!".to_string());
        publisher.publish(&topic, message.clone());

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
        let publisher = TopicStream::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to the topic
        let mut receiver = publisher.subscribe(&[topic.clone()]);

        // Publisher sends multiple messages
        let message1 = Message("Message 1".to_string());
        let message2 = Message("Message 2".to_string());
        publisher.publish(&topic, message1.clone());
        publisher.publish(&topic, message2.clone());

        // Subscriber receives the first message
        let received_message1 = receiver.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        // Subscriber receives the second message
        let received_message2 = receiver.recv().await.unwrap();
        assert_eq!(received_message2, message2);
    }

    #[tokio::test]
    async fn test_multiple_publishers() {
        let publisher = TopicStream::<Topic, Message>::new(2);
        let topic = Topic("test_topic".to_string());

        // Subscriber subscribes to the topic
        let mut receiver = publisher.subscribe(&[topic.clone()]);

        // Publisher 1 sends a message
        let message1 = Message("Message from Publisher 1".to_string());
        publisher.publish(&topic, message1.clone());

        // Publisher 2 sends a message
        let message2 = Message("Message from Publisher 2".to_string());
        publisher.publish(&topic, message2.clone());

        // Subscriber receives the first message
        let received_message1 = receiver.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        // Subscriber receives the second message
        let received_message2 = receiver.recv().await.unwrap();
        assert_eq!(received_message2, message2);
    }

    #[tokio::test]
    async fn test_subscribe_to_different_topics() {
        let publisher = TopicStream::<Topic, Message>::new(2);
        let topic1 = Topic("test_topic_1".to_string());
        let topic2 = Topic("test_topic_2".to_string());

        // Subscriber subscribes to topic 1
        let mut receiver1 = publisher.subscribe(&[topic1.clone()]);

        // Publisher sends a message to topic 1
        let message1 = Message("Hello, Topic 1".to_string());
        publisher.publish(&topic1, message1.clone());

        // Subscriber 1 receives the message for topic 1
        let received_message1 = receiver1.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        // Subscriber subscribes to topic 2
        let mut receiver2 = publisher.subscribe(&[topic2.clone()]);

        // Publisher sends a message to topic 2
        let message2 = Message("Hello, Topic 2".to_string());
        publisher.publish(&topic2, message2.clone());

        // Subscriber 2 receives the message for topic 2
        let received_message2 = receiver2.recv().await.unwrap();
        assert_eq!(received_message2, message2);
    }

    #[tokio::test]
    async fn test_single_receiver_multiple_topics() {
        let publisher = TopicStream::<Topic, Message>::new(2);

        // Define multiple topics
        let topic1 = Topic("test_topic_1".to_string());
        let topic2 = Topic("test_topic_2".to_string());
        let topic3 = Topic("test_topic_3".to_string());

        // Subscriber subscribes to multiple topics
        let mut receiver = publisher.subscribe(&[topic1.clone(), topic2.clone(), topic3.clone()]);

        // Publisher sends messages to each topic
        let message1 = Message("Message for Topic 1".to_string());
        let message2 = Message("Message for Topic 2".to_string());
        let message3 = Message("Message for Topic 3".to_string());

        publisher.publish(&topic1, message1.clone());
        publisher.publish(&topic2, message2.clone());
        publisher.publish(&topic3, message3.clone());

        // Subscriber should receive the messages in the order they were published
        let received_message1 = receiver.recv().await.unwrap();
        assert_eq!(received_message1, message1);

        let received_message2 = receiver.recv().await.unwrap();
        assert_eq!(received_message2, message2);

        let received_message3 = receiver.recv().await.unwrap();
        assert_eq!(received_message3, message3);
    }
}
