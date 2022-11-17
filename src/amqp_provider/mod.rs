use log::*;
use lapin::{BasicProperties, Connection, ConnectionProperties, types::FieldTable, ExchangeKind, Channel};
use lapin::options::{BasicPublishOptions, ExchangeDeclareOptions};
use tokio::sync::broadcast::Sender;

/// Async/await enabled AMQP (RabbitMX) provider, used to relay SMS received by TG
pub struct AmqpProvider {
    #[allow(dead_code)]
    connection: Connection,
    channel: Channel,
    exchange: String,
    sms_routing_key: String,
    command_routing_key: String,
    broadcast: Sender<(String, String)>
}

impl AmqpProvider {

    /// Create new AmqpProvider from provided configuration
    pub async fn new(url: String, exchange: String, sms_routing_key: String, command_routing_key: String, broadcast: Sender<(String, String)>) -> Result<Self, ()> {
        info!("[PROVIDER] Initializing");

        //connect to amqp
        let connection = match Connection::connect(&*url, ConnectionProperties::default()).await {
            Ok(c) => c,
            Err(e) => {
                error!("[PROVIDER]: {}", e);
                return Err(())
            }
        };
        info!("[PROVIDER] Connection established");
        let channel = match connection.create_channel().await {
            Ok(c) => c,
            Err(e) => {
                error!("[AMQP]: {}", e);
                return Err(())
            }
        };
        info!("[PROVIDER] Channel created");

        // declare exchange
        let _rabbit_exchange = channel.exchange_declare(
            &*exchange,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default()
        ).await.unwrap();


        Ok(AmqpProvider {
            connection,
            channel,
            exchange,
            sms_routing_key,
            command_routing_key,
            broadcast,
        })
    }


    /// Spawn the provider and let it process all messages in a loop
    pub async fn spawn(&self) {

        // Subscribe to internal broadcast channel
        let mut inner_receiver = self.broadcast.subscribe();

        loop {
            if let Ok((msg_type, msg)) = inner_receiver.recv().await {
                match msg_type.as_str() {
                    "sms" => match self.channel.basic_publish (
                        &*self.exchange,
                        &*self.sms_routing_key,
                        BasicPublishOptions::default(),
                        msg.as_bytes(),
                        BasicProperties::default()
                        .with_delivery_mode(1)
                    ).await {
                        Ok(_) => debug!("[PROVIDER] Message published to {}", &*self.sms_routing_key),
                        Err(e) => error!("[PROVIDER]: {}", e)
                    },

                    "smsstatus" => match self.channel.basic_publish (
                        &*self.exchange,
                        &*self.command_routing_key,
                        BasicPublishOptions::default(),
                        msg.as_bytes(),
                        BasicProperties::default()
                        .with_delivery_mode(1)
                    ).await {
                        Ok(_) => debug!("[PROVIDER] Message published to {}", &*self.command_routing_key),
                        Err(e) => error!("[PROVIDER]: {}", e)
                    },

                    "SMSCommand" => {
                        trace!("[PROVIDER] Command ack")
                    },

                    "response" => {
                        trace!("[PROVIDER] Response ack")
                    },

                    _ => error!("[PROVIDER] Unknown message type '{}' => '{}'", msg_type, msg)
                }
            }
        }

    }
}