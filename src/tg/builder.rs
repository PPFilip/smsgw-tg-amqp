use std::sync::Arc;
use log::*;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use crate::SmsSend;
use super::{TgClient, ClientInner};

/// Builder that allows creating and configuring various parts of a [`TgClient`]
/// # Example
/// ```
/// let builder = TgClientBuilder::new()
///         .api_url(api_url)
///         .send_span(api_sms_send_span)
///         .api_user(api_user)
///         .api_pass(api_pass)
///         .out_channel(rabbit_tx)
///         .in_channel(tg_rx)
///         .build().await.unwrap();
/// ```
pub struct TgClientBuilder {
    url: Option<String>,
    user: Option<String>,
    pass: Option<String>,
    send_span: Option<String>,
    out_channel: Option<Sender<(String,String)>>,
    in_channel: Option<Sender<SmsSend>>, //TODO: Make handling incoming messages optional
}

#[derive(Debug, Error)]
pub enum ClientBuildError {
    #[error("No API URL was configured")]
    MissingURL,
    #[error("Wrong send span configured")]
    BadSendSpan,
    #[error("No API user was configured")]
    MissingUser,
    #[error("No API password was configured")]
    MissingPassword,
    #[error("Cannot connect to specified server")]
    SocketUnavailable,
    #[error("Specified server is not a TG gateway")]
    SocketNotTG,
    #[error("Missing input channel")]
    MissingInChannel,
}

impl TgClientBuilder {

    /// Default constructor
    pub(crate) fn new() -> Self {
        Self {
            url: None,
            user: None,
            pass: None,
            send_span: Some("1".to_string()),
            out_channel: None,
            in_channel: None,
        }
    }

    /// Set TG server url to connect to
    pub fn api_url(mut self, url: impl AsRef<str>) -> Self {
        self.url = Some(url.as_ref().to_owned());
        self
    }

    /// Set API user
    pub fn api_user(mut self, user: impl AsRef<str>) -> Self {
        self.user = Some(user.as_ref().to_owned());
        self
    }

    /// Set API password
    pub fn api_pass(mut self, pass: impl AsRef<str>) -> Self {
        self.pass = Some(pass.as_ref().to_owned());
        self
    }

    /// Set send span - this port will be used by [`TgClient`] to send outgoing messages
    pub fn send_span(mut self, span: impl AsRef<str>) -> Self {
        self.send_span = Some(span.as_ref().to_owned());
        self
    }

    /// Set outgoing channel - all received messages by [`TgClient`] will be published to this channel
    pub fn out_channel(mut self, sender: Sender<(String,String)>) -> Self {
        self.out_channel = Some(sender);
        self
    }

    /// Set incoming channel - [`TgClient`] will listen and relay incoming SMS via specified span
    /// We will be calling .subscribe() so we need the sender half here
    pub(crate) fn in_channel(mut self, sender: Sender<SmsSend>) -> Self {
        self.in_channel = Some(sender);
        self
    }

    /// Create [`TgClient`] with options passed to the builder, and connect to its TCP stream
    ///
    /// # Errors
    /// Builder will fail:
    /// - if mandatory options are missing
    /// - if it can't connect to the socket
    /// - if connected server does not present correct header
    pub async fn build(self) -> Result<TgClient, ClientBuildError> {
        // Check if all values are present
        let url = self.url.ok_or(ClientBuildError::MissingURL);
        let user = self.user.ok_or(ClientBuildError::MissingUser);
        let pass = self.pass.ok_or(ClientBuildError::MissingPassword);
        let span = self.send_span.ok_or(ClientBuildError::BadSendSpan);
        let u64_span = span.unwrap().parse::<u64>().or(Err(ClientBuildError::BadSendSpan));
        let in_channel = self.in_channel.ok_or(ClientBuildError::MissingInChannel);

        // Connect to socket, acquire lock
        let socket = TcpStream::connect(url.as_ref().unwrap()).await.or(Err(ClientBuildError::SocketUnavailable));
        let mutex_socket = Mutex::new(socket.unwrap());

        let mut mutex_socket_lock = mutex_socket.lock().await;
        let (reader, _writer) = mutex_socket_lock.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        // Read headers from socket
        let _r = reader.read_line(&mut line).await.unwrap();
        if line != "Asterisk Call Manager/1.1\r\n" {
            error!("[TG] Bad headers received: {}", line.trim());
            return Err(ClientBuildError::SocketNotTG);
        }
        line.clear();
        // Drop lock
        drop(mutex_socket_lock);

        // Construct the Client
        let inner = Arc::new(
            ClientInner {
                url: url.unwrap(),
                user: user.unwrap(),
                pass: pass.unwrap(),
                send_span: u64_span.unwrap(),
                socket: mutex_socket,
                out_channel: self.out_channel,
                in_channel: in_channel.unwrap(),
            }
        );

        Ok(TgClient{inner}.to_owned())
    }
}
