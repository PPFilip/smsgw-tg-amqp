use log::*;
use lapin::{Connection, ConnectionProperties, types::FieldTable, Channel};
use lapin::options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions};
use futures_lite::stream::StreamExt;
use tokio::sync::broadcast::Sender;
use crate::tg::{SmsSend};

/// Async/await enabled AMQP (RabbitMX) consumer, used to relay SMS received by TG
pub struct AmqpConsumer {
    #[allow(dead_code)]
    connection: Connection,
    channel: Channel,
    consumer_queue: String,
    consumer_tag: String,
    broadcast: Sender<SmsSend>,
}

impl AmqpConsumer {

    /// Create new AmqpConsumer from provided configuration
    pub async fn new(url: String, consumer_tag: String, consumer_queue: String, broadcast: Sender<SmsSend>) -> Result<Self, ()> {
                //connect to amqp
        let connection = match Connection::connect(&*url, ConnectionProperties::default()).await {
            Ok(c) => c,
            Err(e) => {
                error!("[CONSUMER]: {}", e);
                return Err(())
            }
        };
        info!("[CONSUMER] Connection established");
        let channel = match connection.create_channel().await {
            Ok(c) => c,
            Err(e) => {
                error!("[AMQP]: {}", e);
                return Err(())
            }
        };
        info!("[CONSUMER] Channel created");

        // declare consumer queue
        let _queue = channel.queue_declare(
            &*consumer_queue,
            QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false
            },
            FieldTable::default()
        ).await.unwrap();

        Ok(AmqpConsumer{
            connection,
            channel,
            consumer_queue,
            consumer_tag,
            broadcast,
        })

    }


    /// Spawn the consumer and let it process all messages in a loop
    pub async fn spawn(&self) {

        // Declare consumer
        let mut consumer = self.channel.basic_consume(
            &*self.consumer_queue,
            &*self.consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default()
        ).await.unwrap();

        // Receive messages from consumer indefinitely
        loop {
            if let Some(Ok(delivery)) = consumer.next().await {

                // Acknowledge delivery
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    error!("[CONSUMER] Cannot ack received message: {}", e);
                    continue
                }

                // Parse string from delivery
                let msg = match String::from_utf8(delivery.data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!("[CONSUMER] Error parsing received string: {}", e);
                        continue
                    }
                };

                info!("[CONSUMER] sms send received: {}", msg);

                // Serialize delivery as SmsSend
                let sms = match serde_json::from_str(&*msg) {
                    Ok(sms) => sms,
                    Err(e) => {
                        error!("[CONSUMER] Error deserializing received message: {}", e);
                        continue;
                    }
                };

                // Broadcast
                if let Err(e) = self.broadcast.send(sms) {
                    error!("[CONSUMER] Unexpected error: {}", e)
                } else {
                    trace!("[CONSUMER] Message published to queue")
                };
            } else {
                error!("[CONSUMER] Unexpected delivery error")
            }
        }
    }
}
