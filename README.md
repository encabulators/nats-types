# nats-types

 The `nats-types` crate contains an enum `ProtocolMessage`. This enum can be used to
 parse the string output from a NATS server as well as produce strings to be sent to
 a NATS server.

 The primary use for this crate is to be used in support of building a NATS client, though
 other potential uses might be possible.

 To produce a protocol message, simply create the enum:
 ```rust
 extern crate nats_types;

 use nats_types::{PublishMessage, ProtocolMessage};

 let publish = ProtocolMessage::Publish( PublishMessage {
     reply_to: Some("INBOX.42".to_string()),
     subject: "workdispatch".to_string(),
     payload_size: 11,
     payload: b"Hello World".to_vec(),
 });

 let out = format!("{}", publish);
 assert_eq!(out, "PUB workdispatch INBOX.42 11\r\nHello World\r\n");
 ```

 The same message can be constructed from the 2-line message received from a NATS server:
 ```rust
 extern crate nats_types;

 use std::str::FromStr;
 use nats_types::{ProtocolMessage};

 let msg = "PUB FOO 11\r\nHello NATS!\r\n";
 let protomsg = ProtocolMessage::from_str(&msg).unwrap();
 if let ProtocolMessage::Publish(pubm) = protomsg {
     assert_eq!(pubm.payload_size, 11);
     assert_eq!(pubm.subject, "FOO");
     assert_eq!(pubm.reply_to, None);
     assert_eq!(pubm.payload, b"Hello NATS!");
 }
 ```