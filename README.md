# Yeastar TG GSM Gateway to RabbitMQ

## Intro
This program relays messages between Yeastar TG series and a RabbitMQ server. RabbitMQ is much saner API for when you need to read received messages, since the only native API TG provides to read received messages is a raw telnet socket. Since we need to connect to the socket anyway, SMS send functionality is provided as well.

## Detail
### Receiving SMS
Program waits for incoming messages on TG gateway. 
If a SMS message is detected, it is urldecoded and forwarded to the RabbitMQ with selected routing key. 
**This routing key should be bound to a durable queue**, otherwise you risk to lose messages if no consumer is connected. 
SMS messages are published to the queue as json in the following format:
```json
{
  "event":"ReceivedSMS",
  "privilege":"all,smscommand",
  "id":"",
  "gsmspan":"2",
  "sender":"+123456789",
  "recvtime":"2022-11-05 20:33:05",
  "index":"0",
  "total":"1",
  "smsc":"+123400000",
  "content":"Url decoded SMS text"
}
```
This corresponds to how the raw socket would represent the message
- id - id of received SMS (needed to reconstruct multipart SMS)
- gsmspan - port on which the SMS was received
- index - orded of the message in case of SMS
- total - total number of messages in case of SMS
- smsc - relaying SMS Center

In case any other message is received (send confirmations, span status, etc...), this message is redirected to a secondary queue as a raw text message. This does not need to be durable.

### Sending SMS
Program listens as a RabbitMQ consumer on a specified queue and waits for json message in the following format:

```json
{
  "destination": "+123456789",
  "message": "Text to send, no need to urlencode",
  "id": "123"
}
```
- destination - where to send
- message - Raw text of SMS to send
- id - can be set to any number or 1-word text, it will be used in the reply on the confirmation queue.

## Build
Just use ```cargo build```. No binary distribution is provided for now

## Setup
```cp Settings.toml.example Settings.toml``` and edit the file

General options:
- log_level - "Info", "Debug", "Trace" based on verbosity you need
- retry_wait_s - time in seconds between reconnects

TG options:
- api_url - TG server:port
- api_user - your tg user
- api_pass - your tg password
- api_sms_send_span - which span to use for sending OUT messages

RabbitMQ options:
- rabbit_url - connection string in for of "amqp://user:pass@ip:port"
- rabbit_exchange - name of fanout exchange where incoming SMS will be published
- rabbit_sms_routing_key - routing key for incoming SMS
- rabbit_command_routing_key - routing key for incoming commands
- rabbit_consumer_tag - identification tag for consumer
- rabbit_consumer_queue - queue where outgoins SMS will arrive

You also need to have RabbitMQ server running. Default options are fine, but make sure your queues are properly bound to routing keys.

## Todo
In no particular order:
- Allow sending and receiving USSD
- Better reconnection handling, reconnect to TG or RabbitMQ after disconnect
- Keep received messages in memory while TG thread is running but RabbitMQ is not live
- provide Dockerfile or systemd service files

## Reference
Vendor API description:
- https://support.yeastar.com/hc/en-us/articles/217392758-How-to-Use-TG-SMS-API
- https://support.yeastar.com/hc/en-us/articles/900005386406-How-To-Use-TG-SMS-HTTP-API-Video-

The project was tested on TG400, but should run with all current lineup - TG100, TG200, TG400, TG800 and TG1600

If you find this useful, I am open to feedback or pull requests
