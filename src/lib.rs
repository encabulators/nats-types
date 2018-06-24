//! # nats-types
//!
//! The `nats-types` crate contains an enum `ProtocolMessage`. This enum can be used to
//! parse the string output from a NATS server as well as produce strings to be sent to
//! a NATS server.
//!
//! The primary use for this crate is to be used in support of building a NATS client, though
//! other potential uses might be possible.
//!
//! To produce a protocol message, simply create the enum:
//! ```rust
//! extern crate nats_types;
//!
//! use nats_types::{PublishMessage, ProtocolMessage};
//!
//! let publish = ProtocolMessage::Publish( PublishMessage {
//!     reply_to: Some("INBOX.42".to_string()),
//!     subject: "workdispatch".to_string(),
//!     payload_size: 11,
//!     payload: b"Hello World".to_vec(),
//! });
//!
//! let out = format!("{}", publish);
//! assert_eq!(out, "PUB workdispatch INBOX.42 11\r\nHello World\r\n");
//! ```
//!
//! The same message can be constructed from the 2-line message received from a NATS server:
//! ```rust
//! extern crate nats_types;
//!
//! use std::str::FromStr;
//! use nats_types::{ProtocolMessage};
//!
//! let msg = "PUB FOO 11\r\nHello NATS!\r\n";
//! let protomsg = ProtocolMessage::from_str(&msg).unwrap();
//! if let ProtocolMessage::Publish(pubm) = protomsg {
//!     assert_eq!(pubm.payload_size, 11);
//!     assert_eq!(pubm.subject, "FOO");
//!     assert_eq!(pubm.reply_to, None);
//!     assert_eq!(pubm.payload, b"Hello NATS!");
//! }
//! ```
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate nom;

use nom::types::CompleteStr;
use nom::AsBytes;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

/// An enum whose variants are all of the available protocol messages as defined by the
/// NATS protocol documentation.
#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMessage {
    Unsubscribe(UnsubscribeMessage),
    Publish(PublishMessage),
    Message(DeliveredMessage),
    Ping,
    Pong,
    Ok,
    Error(String),
    Info(ServerInformation),
    Connect(ConnectionInformation),
}

impl Display for ProtocolMessage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), ::std::fmt::Error> {
        match self {
            ProtocolMessage::Unsubscribe(m) => write!(f, "{}", m),
            ProtocolMessage::Publish(m) => write!(f, "{}", m),
            ProtocolMessage::Message(m) => write!(f, "{}", m),
            ProtocolMessage::Ping => write!(f, "PING"),
            ProtocolMessage::Pong => write!(f, "PONG"),
            ProtocolMessage::Ok => write!(f, "+OK"),
            ProtocolMessage::Error(s) => write!(f, "-ERR '{}'", s),
            ProtocolMessage::Info(si) => write!(f, "{}", si),
            ProtocolMessage::Connect(ci) => write!(f, "{}", ci),
        }
    }
}

impl FromStr for ProtocolMessage {
    type Err = NatsParseError;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {

        if s.starts_with("UNSUB") {
            match UnsubscribeMessage::from_str(s) {
                Ok(m) => Ok(ProtocolMessage::Unsubscribe(m)),
                Err(e) => Err(e),
            }
        } else if s.starts_with("PUB") {
           match PublishMessage::from_str(s) {
               Ok(m) => Ok(ProtocolMessage::Publish(m)),
               Err(e) => Err(e),
           }
        } else if s.starts_with("MSG") {
            match DeliveredMessage::from_str(s) {
                Ok(m) => Ok(ProtocolMessage::Message(m)),
                Err(e) => Err(e),
            }
        } else if s.starts_with("PING") {
            Ok(ProtocolMessage::Ping)
        } else if s.starts_with("PONG") {
            Ok(ProtocolMessage::Pong)
        } else if s.starts_with("+OK") {
            Ok(ProtocolMessage::Ok)
        } else if s.starts_with("-ERR") {
            match parser::parse_err_header(s) {
                Some(h) => Ok(ProtocolMessage::Error(h.message)),
                None => Err(NatsParseError{msg: "Failed to parse protocol message of type ERR".to_string()}),
            }
        } else if s.starts_with("INFO") {
            match ServerInformation::from_str(s) {
                Ok(m) => Ok(ProtocolMessage::Info(m)),
                Err(e) => Err(e),
            }
        } else if s.starts_with("CONNECT") {
            match ConnectionInformation::from_str(s) {
                Ok(m) => Ok(ProtocolMessage::Connect(m)),
                Err(e) => Err(e),
            }
        }
        else {
            Err(NatsParseError{msg: "Failed to parse protocol message - unknown message type?".to_string()})
        }
    }
}

/// Represents server connection information sent by the client to configure the connection
/// immediately after connecting. The NATS protocol definition for this is as follows:
/// ```text
/// CONNECT [json]
/// ```
#[derive(Serialize, Debug, Clone, PartialEq, Deserialize)]
pub struct ConnectionInformation {
    pub verbose: bool,
    pub pedantic: bool,
    pub tls_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pass: Option<String>,
    pub lang: String,
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<u64>,
}

impl Display for ConnectionInformation {
    fn fmt(&self, f: &mut Formatter) -> Result<(), ::std::fmt::Error> {
        let out = serde_json::to_string(self);
        match out {
            Ok(json) => write!(f, "CONNECT {}", json),
            Err(e) => write!(f, "<<BAD CONNECT INFO - CAN'T SERIALIZE>>: {}", e),
        }
    }
}

impl FromStr for ConnectionInformation {
    type Err = NatsParseError;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.replace("CONNECT ", "");
        println!("{}", s);
        match serde_json::from_str(s.trim()) {
            Ok(ci) => Ok(ci),
            Err(e) => Err(NatsParseError {
                msg: format!("Failed to parse connection info JSON: {}", e),
            }),
        }
    }
}

/// Represents a NATS server information message, defined according to the NATS
/// protocol documentation:
/// ```text
/// INFO {["option_name":option_value],...}
/// ```
#[derive(Serialize, Debug, Clone, PartialEq, Deserialize)]
pub struct ServerInformation {
    pub server_id: String,
    pub version: String,
    pub go: String,
    pub host: String,
    pub port: u64,
    pub tls_required: bool,
    pub max_payload: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_urls: Option<Vec<String>>,
}

impl Display for ServerInformation {
    fn fmt(&self, f: &mut Formatter) -> Result<(), ::std::fmt::Error> {
        let out = serde_json::to_string(self);
        match out {
            Ok(json) => write!(f, "INFO {}", json),
            Err(e) => write!(f, "<<BAD SERVERINFO - CAN'T SERIALIZE>>: {}", e),
        }
    }
}

impl FromStr for ServerInformation {
    type Err = NatsParseError;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let s = s.replace("INFO ", "");
        match serde_json::from_str(s.trim()) {
            Ok(si) => Ok(si),
            Err(_) => Err(NatsParseError {
                msg: "Failed to parse server info JSON".to_string(),
            }),
        }
    }
}

/// Represents a message as per the NATS protocol documentation:
/// ```text
/// MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveredMessage {
    pub subject: String,
    pub subscription_id: u64,
    pub reply_to: Option<String>,
    pub payload_size: u64,
    pub payload: Vec<u8>,
}

impl Display for DeliveredMessage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), ::std::fmt::Error> {
        match self.reply_to {
            None => write!(
                f,
                "MSG {} {} {}\r\n{}\r\n",
                self.subject,
                self.subscription_id,
                self.payload_size,
                vec_to_str(&self.payload)
            ),
            Some(ref rt) => write!(
                f,
                "MSG {} {} {} {}\r\n{}\r\n",
                self.subject,
                self.subscription_id,
                rt,
                self.payload_size,
                vec_to_str(&self.payload)
            ),
        }
    }
}

impl FromStr for DeliveredMessage {
    type Err = NatsParseError;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let split = parser::split_header_and_payload(s);
        match split {
            None => Err(NatsParseError {
                msg: "Failed to parse message - possibly not a 2-line message".to_string(),
            }),
            Some(split) => {
                let res = parser::parse_msg_header(&split.0);
                match res {
                    Some(r) => Ok(DeliveredMessage {
                        subject: r.subject,
                        subscription_id: r.sid,
                        reply_to: r.reply_to,
                        payload_size: r.message_len,
                        payload: split.1,
                    }),
                    None => Err(NatsParseError {
                        msg: "Failed to parse delivered message".to_string(),
                    }),
                }
            }
        }
    }
}

/// A struct that represents an unsubscribe message. This message conforms
/// to the following format from the NATS protocol definition:
/// ```text
/// UNSUB <sid> [max_msgs]
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct UnsubscribeMessage {
    pub subscription_id: u64,
    pub max_messages: Option<u64>,
}

impl Display for UnsubscribeMessage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), ::std::fmt::Error> {
        match self.max_messages {
            None => write!(f, "UNSUB {}", self.subscription_id),
            Some(n) => write!(f, "UNSUB {} {}", self.subscription_id, n),
        }
    }
}

impl FromStr for UnsubscribeMessage {
    type Err = NatsParseError;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let res = parser::parse_unsub_header(s);
        match res {
            Some(r) => Ok(UnsubscribeMessage {
                subscription_id: r.sid,
                max_messages: r.max_messages,
            }),
            None => Err(NatsParseError {
                msg: "Failed to parse Unsubscribe message".to_string(),
            }),
        }
    }
}

/// Represents a publish message. This message conforms to the following format from the
/// NATS protocol documentation:
/// ```text
/// PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct PublishMessage {
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload_size: u64,
    pub payload: Vec<u8>,
}

impl Display for PublishMessage {
    fn fmt(&self, f: &mut Formatter) -> Result<(), ::std::fmt::Error> {
        match self.reply_to {
            None => write!(
                f,
                "PUB {} {}\r\n{}\r\n",
                self.subject,
                self.payload_size,
                vec_to_str(&self.payload)
            ),
            Some(ref rt) => write!(
                f,
                "PUB {} {} {}\r\n{}\r\n",
                self.subject,
                rt,
                self.payload_size,
                vec_to_str(&self.payload)
            ),
        }
    }
}

impl FromStr for PublishMessage {
    type Err = NatsParseError;

    fn from_str(s: &str) -> Result<Self, <Self as FromStr>::Err> {
        let split = parser::split_header_and_payload(s);
        match split {
            None => Err(NatsParseError {
                msg: "Failed to parse Publish message - possibly not a 2-line message".to_string(),
            }),
            Some(split) => {
                let res = parser::parse_pub_header(&split.0);
                match res {
                    Some(r) => Ok(PublishMessage {
                        subject: r.subject,
                        reply_to: r.reply_to,
                        payload_size: r.message_len,
                        payload: split.1,
                    }),
                    None => Err(NatsParseError {
                        msg: "Failed to parse Publish message".to_string(),
                    }),
                }
            }
        }
    }
}

fn vec_to_str(bytes: &Vec<u8>) -> String {
    let s = String::from_utf8(bytes.as_bytes().to_owned());
    match s {
        Ok(s) => s,
        Err(_) => "<<BAD PAYLOAD>>".to_string(),
    }
}

/// Indicates an error occurred during parsing of a NATS protocol message. Do not use this
/// type directly, instead use the error trait.
#[derive(Debug)]
pub struct NatsParseError {
    msg: String,
}

impl Error for NatsParseError {
    fn description(&self) -> &str {
        &self.msg
    }
}

impl Display for NatsParseError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

mod parser;

#[cfg(test)]
mod tests {
    use super::{
        ConnectionInformation, DeliveredMessage, PublishMessage, ServerInformation,
        UnsubscribeMessage, ProtocolMessage,
    };
    use std::str::FromStr;

    #[test]
    fn unsub_roundtrip() {
        let msg = "UNSUB 21 40";
        let unsub = UnsubscribeMessage::from_str(msg).unwrap();
        assert_eq!(unsub.subscription_id, 21);
        assert_eq!(unsub.max_messages, Some(40));
        let out = format!("{}", unsub);
        assert_eq!(out, msg);
    }

    #[test]
    fn pub_roundtrip() {
        let msg = "PUB FOO 11\r\nHello NATS!\r\n";
        let pubm = PublishMessage::from_str(msg).unwrap();
        assert_eq!(pubm.payload_size, 11);
        assert_eq!(pubm.subject, "FOO");
        assert_eq!(pubm.reply_to, None);
        assert_eq!(pubm.payload, b"Hello NATS!");
        let out = format!("{}", pubm);
        assert_eq!(out, msg);
    }

    #[test]
    fn msg_roundtrip() {
        let msg = "MSG FOO.BAR 9 INBOX.34 11\r\nHello World\r\n";
        let mmsg = DeliveredMessage::from_str(msg).unwrap();
        assert_eq!(mmsg.reply_to, Some("INBOX.34".to_string()));
        assert_eq!(mmsg.payload_size, 11);
        assert_eq!(mmsg.subscription_id, 9);
        assert_eq!(mmsg.subject, "FOO.BAR");
        assert_eq!(mmsg.payload, b"Hello World");
        let out = format!("{}", mmsg);
        assert_eq!(out, msg);
    }

    #[test]
    fn serverinfo_roundtrip() {
        let msg = r#"INFO {"server_id":"1ec445b504f4edfb4cf7927c707dd717",
        "version":"0.6.6","go":"go1.4.2","host":"0.0.0.0",
        "port":4222,"auth_required":false,"tls_required":false,
        "max_payload":1048576}"#;

        let si = ServerInformation::from_str(msg);
        assert!(si.is_ok());
        if let Ok(info) = si {
            assert_eq!(info.connect_urls, None);
            assert_eq!(info.server_id, "1ec445b504f4edfb4cf7927c707dd717");
            assert_eq!(info.go, "go1.4.2");
            assert_eq!(info.version, "0.6.6");
            assert_eq!(info.max_payload, 1048576);
            assert_eq!(info.tls_required, false);
            assert_eq!(info.port, 4222);
            assert_eq!(info.host, "0.0.0.0");
        }
    }

    #[test]
    fn connect_roundtrip() {
        let msg = r#"CONNECT {"verbose":false,"pedantic":false,"tls_required":false,"lang":"go","name":"testing","version":"1.2.2","protocol":1}
        "#;
        let ci = ConnectionInformation::from_str(msg);
        println!("{:?}", ci);
        assert!(ci.is_ok());
        if let Ok(info) = ci {
            assert_eq!(info.name, "testing");
            assert_eq!(info.pedantic, false);
            assert_eq!(info.tls_required, false);

            let out = format!("{}", info);
            assert_eq!(out, msg.trim());
        }
    }

    #[test]
    fn enum_round() {
        let publish = ProtocolMessage::Publish(PublishMessage {
            subject: "workdispatch".to_string(),
            reply_to: None,
            payload_size: 11,
            payload: b"Hello World".to_vec(),
        });
        let out = format!("{}", publish);
        let pub2 = ProtocolMessage::from_str(&out).unwrap();
        assert_eq!(publish, pub2);
        assert_eq!(out, "PUB workdispatch 11\r\nHello World\r\n");
    }
}
