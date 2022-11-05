use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::sleep,
    sync::{Barrier, broadcast}
};

use log::*;
use lapin::{options::{BasicPublishOptions}, BasicProperties, Connection, ConnectionProperties, types::FieldTable, ExchangeKind};
use lapin::options::{BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueDeclareOptions};
use serde::{Deserialize, Serialize};
use regex::{Regex, RegexSet};
use std::sync::Arc;
use futures_lite::stream::StreamExt;

#[derive(Clone, Deserialize)]
struct SmsSend {
    destination: String,
    message: String,
    id: String
}

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


    //
    // Initialize broadcast channels for messages and rabbit
    // This will miss messages if consumer is not live yet, for now we use barrier
    let (rabbit_tx, mut rabbit_rx) = broadcast::channel::<(String, String)>(10);
    let (tg_tx, mut tg_rx) = broadcast::channel::<SmsSend>(10);


    //
    // Set up barriers to sync tasks. Not final solution
    //
    let barrier = Arc::new(Barrier::new(2));
    let tg_task_barrier = barrier.clone();

    //
    // Spawn rabbit task
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

        // sync point with TG thread
        debug!("[AMQP] Initialized, waiting on barrier");
        barrier.wait().await;

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
                    Ok((routing_key, msg)) => {
                        match rabbit_channel.basic_publish (
                            &*rabbit_exchange,
                            &*routing_key,
                            BasicPublishOptions::default(),
                            msg.as_bytes(),
                            BasicProperties::default()
                            .with_delivery_mode(1)
                        ).await {
                            Ok(_) => debug!("[AMQP] Message published to {}", routing_key),
                            Err(e) => error!("[AMQP]: {}", e)
                        }
                    },
                    Err(e) => error!("[AMQP]: {}", e)
                }
            }

        } }



    }});

    //
    // Spawn TG task
    //
    let tg_handle_task = tokio::spawn( async move { loop {

        info!("[TG] Initializing");
        let mut socket = match TcpStream::connect(api_url.clone()).await {
            Ok(s) => s,
            Err(e) => {
                error!("[TG]: {}", e);
                sleep(retry_wait_s).await;
                continue
            }
        };
        info!("[TG] Connection established");

        let (reader, mut writer) = socket.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();


        let _r = reader.read_line(&mut line).await.unwrap();
        if line != "Asterisk Call Manager/1.1\r\n" {
            error!("[TG] Bad headers received: {}", line.trim());
            std::process::exit(1);
        } else {
            trace!("[TG] Header received: {}", line.trim());
        }
        line.clear();

        let login_cmd = format!("Action: login\r\nUsername: {}\r\nSecret: {}\r\n\r\n", api_user, api_pass);
        writer.write_all(login_cmd.as_bytes()).await.unwrap();

        let list_cmd = "Action: smscommand\r\ncommand: gsm show spans\r\n\r\n";
        info!("[TG] listing spans");
        writer.write_all(list_cmd.as_bytes()).await.unwrap();

        // sync point with rabbit thread
        debug!("[TG] Initialized, waiting on barrier");
        tg_task_barrier.wait().await;

        loop { tokio::select! {
            //
            // Received text from TG API
            //
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(c) => {
                        if c == 2 {
                            trace!("[TG] Full received message:\n{}-----", line);
                            let _r = match parse_reply(line.clone()) {
                                Ok((msg_type, msg)) => {
                                    match msg_type.as_str() {
                                        "sms" => rabbit_tx.send((rabbit_sms_routing_key.clone(), msg.clone())).unwrap(),
                                        "response" | "smsstatus" => rabbit_tx.send((rabbit_command_routing_key.clone(), msg.clone())).unwrap(),
                                        _ => 0
                                    }
                                },
                                Err(_e) => {
                                    0
                                }
                            };

                            line.clear();
                        }
                    },
                    Err(e) => {
                        error!("[TG] Error reading: {}", e);
                    }
                };

            },

            //
            // Received sms via rabbit - send text to TG API
            //
            result = tg_rx.recv() => {
                match result {
                    Ok(sms) => {
                        let send_cmd = format!("Action: smscommand\r\ncommand: gsm send sms {} {} \"{}\" \"{}\"\r\n\r\n",
                            api_sms_send_span,
                            sms.destination,
                            sms.message,
                            sms.id
                        );
                        match writer.write_all(send_cmd.as_bytes()).await {
                            Ok(_t) => {
                                trace!("[TG] send sms command forwarded");
                            },
                            Err(e) => {
                                error!("[TG] Unexpected error: {}", e);
                            }
                        };
                    },
                    Err(_e) => {
                        error!("Internal tg_sms queue error");
                    }
                }
            }
        } }

    }});

    tg_rabbit_task.await.unwrap();
    tg_handle_task.await.unwrap();

}

fn parse_reply(str: String) -> Result<(String, String), (String, String)> {
    trait OptionMatchExt {
        fn sane_default(self) -> String;
    }

    impl OptionMatchExt for Option<regex::Match<'_>> {
        fn sane_default(self) -> String {
            self.map_or(String::from(""), |m| String::from(m.as_str()))
        }
    }

    let set = RegexSet::new(&[
        r"^Response: Success\r\n",
        r"^Response: Error\r\n",
        r"^Event: ReceivedSMS\r\n",
        r"^Event: UpdateSMSSend\r\n",
        r"^Response: Follows\r\n",
    ]).unwrap();

    let matches = set.matches(str.as_str());

    if matches.matched(0) {
        // Matched Response: Success
        let success_re = Regex::new(r"^Response: Success\r\nMessage: (.*)\r\n").unwrap();
        match success_re.captures(str.as_str()) {
            Some(captures) => {
                let msg = captures.get(1).sane_default();
                info!("[TG] {}", msg);
                Ok((String::from("success"), msg))
            },
            None => {
                error!("[TG] Malformed success message: {}", str);
                Err((String::from("success"), str))
            }
        }

    } else if matches.matched(1) {
        // Matched Response: Error
        let error_re = Regex::new(r"^Response: Error\r\nMessage: (.*)\r\n").unwrap();
        match error_re.captures(str.as_str()) {
            Some(captures) => {
                let msg = captures.get(1).sane_default();
                error!("[TG] {}", msg);
                Ok((String::from("error"), msg))
            },
            None => {
                error!("[TG] Malformed error message: {}", str);
                Err((String::from("error"), str))
            }
        }

    } else if matches.matched(2) {
        // Matched Event: ReceivedSMS

        let msg_re = Regex::new(r"(?x)^
            Event:\s(?P<event>ReceivedSMS)\r\n
            Privilege:\s(?P<privilege>.*)\r\n
            ID:\s(?P<id>.*)\r\n
            GsmSpan:\s(?P<gsmspan>.*)\r\n
            Sender:\s(?P<sender>.*)\r\n
            Recvtime:\s(?P<recvtime>.*)\r\n
            Index:\s(?P<index>.*)\r\n
            Total:\s(?P<total>.*)\r\n
            Smsc:\s(?P<smsc>.*)\r\n
            Content:\s(?P<content>.*)\r\n
            --END\sSMS\sEVENT--\r\n"
        ).unwrap();

        /// Structure to represent received SMS
        #[derive(Serialize)]
        struct ReceiveSms {
            event: String,
            privilege: String,
            id: String,
            gsmspan: String,
            sender: String,
            recvtime: String,
            index: String,
            total: String,
            smsc: String,
            content: String
        }

        match msg_re.captures(str.as_str()) {
            Some(captures) => {
                let sms = ReceiveSms {
                    event: captures.name("event").sane_default(),
                    privilege: captures.name("privilege").sane_default(),
                    id: captures.name("id").sane_default(),
                    gsmspan: captures.name("gsmspan").sane_default(),
                    sender: captures.name("sender").sane_default(),
                    recvtime: captures.name("recvtime").sane_default(),
                    index: captures.name("index").sane_default(),
                    total: captures.name("total").sane_default(),
                    smsc: captures.name("smsc").sane_default(),
                    content: match urlencoding::decode(captures.name("content").sane_default().as_str()) {
                        Ok(cs) => cs.into_owned().replace("+", " "),
                        Err(_e) => captures.name("content").sane_default()
                    }
                };

                info!("[TG] Received SMS on port '{}' from '{}' with content '{}'", sms.gsmspan, sms.sender, sms.content);

                let sms_string = serde_json::to_string(&sms).unwrap();
                trace!("[TG] Message json: {}", sms_string);
                Ok((String::from("sms"), sms_string))
            }
            _ => {
                error!("[TG] Malformed ReceivedSMS event: {}", str);
                Err((String::from("sms"), str))
            }
        }

    } else if matches.matched(3) {
        // Matched Event: UpdateSMSSend
        let msg_re = Regex::new(r"(?x)^
            Event:\s(?P<event>UpdateSMSSend)\r\n
            Privilege:\s(?P<privilege>.*)\r\n
            ID:\s(?P<id>.*)\r\n
            Smsc:\s(?P<smsc>.*)\r\n
            Status:\s(?P<status>.*)\r\n
            --END\sSMS\sEVENT--\r\n"
        ).unwrap();

        match msg_re.captures(str.as_str()) {
            Some(captures) => {
                #[derive(Serialize)]
                struct Event {
                    event: String,
                    id: String,
                    smsc: String,
                    status: String,
                }
                let event = Event {
                    event: captures.name("event").sane_default(),
                    id: captures.name("id").sane_default(),
                    smsc: captures.name("smsc").sane_default(),
                    status: captures.name("status").sane_default(),
                };

                info!("[TG] {} for id {} via smsc {}: {}", event.event, event.id, event.smsc, event.status);
                let event_json = serde_json::to_string(&event).unwrap();
                Ok((String::from("smsstatus"), event_json))
            },
            None => {
                error!("[TG] Malformed UpdateSMSSend event");
                Err((String::from("smsstatus"), str))
            }
        }

    } else if matches.matched(4) {
        // Matched Response: Follows
        let msg_re = Regex::new(r"(?x)^
            Response:\sFollows\r\n
            Privilege:\s(?P<privilege>.*)\r\n
            (?s)(?P<command>.*)(?-s)
            --END\sCOMMAND--\r\n"
        ).unwrap();

        match msg_re.captures(str.as_str()) {
            Some(c) => {
                let resp = c.name("command").sane_default();
                let privilege = c.name("privilege").sane_default();
                debug!("[TG] Response for '{}':\n{}", privilege, resp);
                Ok((String::from("response"), resp))
            },
            None => {
                error!("[TG] Malformed response message: {}", str);
                Err((String::from("response"),str))
            }
        }

    } else {
        // No match for received commands
        error!("[TG] Unknown message: {}", str);
        Err((String::from("unknown"), str))
    }
}