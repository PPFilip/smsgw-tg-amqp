mod tg;

use std::time::Duration;
use tokio::{
    time::sleep,
    sync::broadcast
};
use log::*;
use lapin::{options::{BasicPublishOptions}, BasicProperties, Connection, ConnectionProperties, types::FieldTable, ExchangeKind};
use lapin::options::{BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueDeclareOptions};
use futures_lite::stream::StreamExt;
use crate::tg::{SmsSend, builder::TgClientBuilder};


#[tokio::main]
async fn main() {

    //
    // Read settings
    //
    use config::Config;
    use std::str::FromStr;

    let settings = Config::builder()
        .add_source(config::File::with_name("Settings"))
        .build()
        .unwrap();

    // Set log level
    let log_level_str:String = match settings.get_string("log_level") {
        Err(_) => String::from("Info"),
        Ok(i) => i
    };
    let log_level = Level::from_str(&log_level_str.as_str()).unwrap();
    stderrlog::new().module(module_path!()).verbosity(log_level).init().unwrap();

    // Get api url/name/pass
    let api_url = settings.get_string("api_url").unwrap();
    let api_user = settings.get_string("api_user").unwrap();
    let api_pass = settings.get_string("api_pass").unwrap();
    let api_sms_send_span = settings.get_string("api_sms_send_span").unwrap();

    // Get rabbit url/queue/tag
    let rabbit_url = settings.get_string("rabbit_url").unwrap();
    let rabbit_exchange = settings.get_string("rabbit_exchange").unwrap();
    let rabbit_sms_routing_key = settings.get_string("rabbit_sms_routing_key").unwrap();
    let rabbit_command_routing_key = settings.get_string("rabbit_command_routing_key").unwrap();
    let rabbit_consumer_tag = settings.get_string("rabbit_consumer_tag").unwrap();
    let rabbit_consumer_queue = settings.get_string("rabbit_consumer_queue").unwrap();

    // retry time in seconds
    let retry_wait_s = Duration::from_secs(match settings.get_int("retry_wait_s") {
        Err(_) => 10_u64,
        Ok(i) => i as u64
    });

    // Initialize broadcast channels for messages and rabbit
    // This will miss messages if consumer is not live yet, for now we use barrier
    let (rabbit_tx, mut rabbit_rx) = broadcast::channel::<(String, String)>(10);
    let (tg_tx, _tg_rx) = broadcast::channel::<SmsSend>(10);

    // for use in tg_client,  tg_tx will be used in rabbit thread
    let tg_tx_2 = tg_tx.clone();

    //
    // Spawn TG thread
    //
    let tg_client = TgClientBuilder::new()
        .api_url(api_url)
        .send_span(api_sms_send_span)
        .api_user(api_user)
        .api_pass(api_pass)
        .out_channel(rabbit_tx)
        .in_channel(tg_tx_2)
        .build().await.unwrap();

    tg_client.login().await;

    let tg_task = tg_client.spawn();


    //
    // Spawn rabbit thread
    //
    let tg_rabbit_task = tokio::spawn(async move { loop {
        info!("[AMQP] Initializing");

        //connect to amqp
        let rabbit_connection = match Connection::connect(&*rabbit_url, ConnectionProperties::default()).await {
            Ok(c) => c,
            Err(e) => {
                error!("[AMQP]: {}", e);
                sleep(retry_wait_s).await;
                continue
            }
        };
        info!("[AMQP] Connection established");
        let rabbit_channel = match rabbit_connection.create_channel().await {
            Ok(c) => c,
            Err(e) => {
                error!("[AMQP]: {}", e);
                sleep(retry_wait_s).await;
                continue
            }
        };
        info!("[AMQP] Channel created");

        // declare exchange
        let _rabbit_exchange = rabbit_channel.exchange_declare(
            &*rabbit_exchange,
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default()
        ).await.unwrap();

        // declare consumer queue
        let _rabbit_consumer_queue = rabbit_channel.queue_declare(
            &*rabbit_consumer_queue,
            QueueDeclareOptions {
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false
            },
            FieldTable::default()
        ).await.unwrap();

        let mut consumer = rabbit_channel.basic_consume(
            &*rabbit_consumer_queue,
            &*rabbit_consumer_tag,
            BasicConsumeOptions::default(),
            FieldTable::default()
        ).await.unwrap();

        loop { tokio::select! {
            //
            // Received sms to send on AMQP => send to TG
            //
            delivery = consumer.next() => {
                match delivery {
                    Some(d) => {
                        let d = d.expect("");
                        d.ack(BasicAckOptions::default()).await.expect("ack");
                        let msg = String::from_utf8(d.data).expect("UTF-8");

                        info!("[CONSUMER] sms send received: {}", msg);

                        let sms = match serde_json::from_str(&*msg) {
                            Ok(sms) => sms,
                            Err(e) => {
                                error!("[CONSUMER] Error deserializing received message: {}", e);
                                continue;
                            }
                        };

                        match tg_tx.send(sms) {
                            Ok(_t) => {
                                trace!("[CONSUMER] Message published to queue")
                            },
                            Err(e) => {
                                error!("[CONSUMER] Unexpected error: {}", e);
                                continue;
                            }
                        };
                    },
                    None => {
                        error!("[CONSUMER] Unexpected error");
                        continue;
                    }
                }
            },

            //
            // Received message from TG => send to AMQP
            //
            result = rabbit_rx.recv() => {
                match result {
                    Ok((msg_type, msg)) => {
                        match msg_type.as_str() {
                            "sms" => match rabbit_channel.basic_publish (
                                &*rabbit_exchange,
                                &*rabbit_sms_routing_key,
                                BasicPublishOptions::default(),
                                msg.as_bytes(),
                                BasicProperties::default()
                                .with_delivery_mode(1)
                            ).await {
                                Ok(_) => debug!("[AMQP] Message published to {}", rabbit_sms_routing_key),
                                Err(e) => error!("[AMQP]: {}", e)
                            },
                            _ => error!("[AMQP] Unknown message type '{}' => '{}'", msg_type, msg)
                        }

                    },
                    Err(e) => error!("[AMQP]: {}", e)
                }
            }

        } }



    }});

    tg_task.await;
    tg_rabbit_task.await.unwrap();
}

