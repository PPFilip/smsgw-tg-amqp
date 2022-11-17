pub(crate) mod builder;

use std::sync::Arc;
use log::*;
use regex::{Regex, RegexSet};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;

/// An async/await enabled Yeastar TG client (Asterisk Call Manager implementation)
///
/// Internal state is wrapped inside Arc, so the [`TgClient`] can be cloned if necessary.
#[derive(Clone)]
pub struct TgClient {
    pub(crate) inner: Arc<ClientInner>,
}

/// Structure to hold client state
pub(crate) struct ClientInner {
    // IP:PORT to connect to
    url: String,
    // API user
    user: String,
    // API password
    pass: String,
    // Span / port to send outgoing SMS
    send_span: u64,
    // TcpStream representing established connection
    socket: Mutex<TcpStream>,
    // Outgoing channel - all received messages by [`TgClient`] will be published to this channel
    // No outgoing message will be relayed if channel is not set up.
    out_channel: Option<Sender<(String, String)>>,
    // Incoming channel - [`TgClient`] will listen and relay incoming SMS via specified span
    // We will be subscribing to the sender
    // TODO: No incoming message will be relayed if channel is not set up.
    in_channel: Sender<SmsSend>,
}

/// Structure to relay incoming SMS
#[derive(Clone, Deserialize, Debug)]
pub struct SmsSend {
    pub(crate) destination: String,
    pub(crate) message: String,
    pub(crate) id: String
}

impl TgClient {

    /// Log in via set up credentials
    pub async fn login(&self) {
        let mut lock_socket = self.inner.socket.lock().await;
        let (_reader, mut writer) = lock_socket.split();

        debug!("[TG] Logging in to {}", self.inner.url);

        let login_cmd = format!("Action: login\r\nUsername: {}\r\nSecret: {}\r\n\r\n", self.inner.user, self.inner.pass);
        writer.write_all(login_cmd.as_bytes()).await.unwrap();
    }


    /// Spawn the client and let it process all messages
    ///
    /// If text is received from TG, out_channel is used to broadcast the message
    /// If in_channel receives SmsSend, it is written back to TG
    pub async fn spawn(&self) {
        let mut lock_socket = self.inner.socket.lock().await;
        let (reader, mut writer) = lock_socket.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        let mut inner_rx = self.inner.in_channel.subscribe();
        let inner_tx = self.inner.out_channel.clone();

        loop { tokio::select! {
            //
            // Received text from TG API
            //
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(line_length) => {
                        if line_length == 2 {
                            trace!("[TG] Full received message:\n{}-----", line);
                            if let Ok((msg_type, msg)) = parse_reply(line.clone()) {
                                if let Some(ref tx) = inner_tx {
                                    if let Ok(_) = tx.send((msg_type.clone(), msg.clone())) {
                                        trace!("[TG] Relayed message");
                                    } else {
                                        error!("[TG] Error relaying message");
                                    }
                                } else {
                                    trace!("[TG] No broadcast channel to relay message")
                                }
                            } else {
                                error!("[TG] Error parsing reply");
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
            // SmsSend received - send SMS to TG API
            //
            result = inner_rx.recv() => {
                match result {
                    Ok(sms) => {
                        let send_cmd = format!("Action: smscommand\r\ncommand: gsm send sms {} {} \"{}\" \"{}\"\r\n\r\n",
                            self.inner.send_span,
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
                        }
                    },
                    Err(_e) => {
                        error!("Internal tg_sms queue error");
                    }
                }
            }
        }}
    }

}

/// Function parses multiline string received from TG and extracts commands
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
                trace!("[TG] Response for '{}':\n{}", privilege, resp);
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

#[cfg(test)]
mod tests {
    use super::parse_reply;

    #[test]
    fn it_works() {
        let msg = String::from("Response: Success\r\nMessage: xyz afddfa\r\n\r\n");

        let result = parse_reply(msg);
        assert_eq!(result, Ok((String::from("success"),String::from("xyz afddfa"))));
    }
}