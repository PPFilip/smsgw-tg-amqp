use log::*;
use tokio::sync::broadcast;
use config::Config;
use std::str::FromStr;

mod tg;
mod amqp_provider;
mod amqp_consumer;
use crate::amqp_consumer::AmqpConsumer;
use crate::amqp_provider::AmqpProvider;
use crate::tg::{SmsSend, builder::TgClientBuilder};


#[tokio::main]
async fn main() {

    //
    // Read settings
    //
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
    let rabbit_url2 = rabbit_url.clone();
    let rabbit_exchange = settings.get_string("rabbit_exchange").unwrap();
    let rabbit_sms_routing_key = settings.get_string("rabbit_sms_routing_key").unwrap();
    let rabbit_command_routing_key = settings.get_string("rabbit_command_routing_key").unwrap();
    let rabbit_consumer_tag = settings.get_string("rabbit_consumer_tag").unwrap();
    let rabbit_consumer_queue = settings.get_string("rabbit_consumer_queue").unwrap();

    // Initialize broadcast channels for messages and rabbit
    // This will miss messages if consumer is not live yet, for now we use barrier
    let (rabbit_tx, _rabbit_rx) = broadcast::channel::<(String, String)>(10);
    let (tg_tx, _tg_rx) = broadcast::channel::<SmsSend>(10);

    // for use by AMQP provider
    let tg_tx_2 = tg_tx.clone();
    // for use by AMQP consumer
    let rabbit_tx_2 = rabbit_tx.clone();

    // Spawn TG thread
    let tg_task = tokio::spawn(async move {
        let tg_client = TgClientBuilder::new()
        .api_url(api_url)
        .send_span(api_sms_send_span)
        .api_user(api_user)
        .api_pass(api_pass)
        .out_channel(rabbit_tx)
        .in_channel(tg_tx)
        .build().await.unwrap();

        tg_client.login().await;
        tg_client.spawn().await;
    });

    // Spawn AMQP Provider to process SMS received by TG
    let pp_task = tokio::spawn(async move {
        let p = AmqpProvider::new(rabbit_url, rabbit_exchange, rabbit_sms_routing_key, rabbit_command_routing_key, rabbit_tx_2).await.unwrap();
        let _p_task = p.spawn().await;
    });

    // Spawn AMQP Consumer to process SMS to be sent by TG
    let pc_task = tokio::spawn(async move {
        let c = AmqpConsumer::new(rabbit_url2, rabbit_consumer_tag, rabbit_consumer_queue, tg_tx_2).await.unwrap();
        let _c_task = c.spawn().await;
    });


    // Run all threads
    tg_task.await.unwrap();
    pp_task.await.unwrap();
    pc_task.await.unwrap();
}

