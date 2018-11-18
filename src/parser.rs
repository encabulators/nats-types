// NOTE: many thanks to @Lehona from the Nom Gitter (https://gitter.im/Geal/nom) for putting
// up with my newbie questions and helping me get through some of the peculiarities of nom
// and parser combinators. I would not have been able to write any of the code in this file
// without their assistance.

use nom::types::CompleteStr;


// MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n
#[derive(Debug)]
pub struct MessageHeader {
    pub subject: String,
    pub sid: u64,
    pub reply_to: Option<String>,
    pub message_len: u64,
}

// PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n
#[derive(Debug)]
pub struct PubHeader {
    pub subject: String,
    pub reply_to: Option<String>,
    pub message_len: u64,
}

// SUB <subject> [queue group] <sid>\r\n
#[derive(Debug)]
pub struct SubHeader {
    pub subject: String,
    pub queue_group: Option<String>,
    pub sid: u64,
}

// UNSUB <sid> [max_msgs]
#[derive(Debug)]
pub struct UnsubHeader {
    pub sid: u64,
    pub max_messages: Option<u64>,
}

// -ERR <error message>
#[derive(Debug)]
pub struct ErrorHeader {
    pub message: String,
}

fn is_digit(chr: char) -> bool {
    chr == '1'
        || chr == '0'
        || chr == '2'
        || chr == '3'
        || chr == '4'
        || chr == '5'
        || chr == '6'
        || chr == '7'
        || chr == '8'
        || chr == '9'
}

fn is_not_space(chr: char) -> bool {
    chr != ' ' && chr != '\r' && chr != '\n'
}

fn is_not_tick(chr: char) -> bool {
    chr != '\''
}

pub fn split_header_and_payload(source: &str) -> Option<(String, Vec<u8>)> {
    let s: Vec<&str> = source.split("\r\n").collect();
    if s.len() < 2 {
        None
    } else {
        Some((s[0].to_string(), s[1].as_bytes().to_vec()))
    }
}

named!(parse_u64<::nom::types::CompleteStr, u64>,
    flat_map!(take_while1_s!(is_digit), parse_to!(u64))
);

named!(parse_completestr<::nom::types::CompleteStr, String >, map!(
    take_while1_s!(is_not_space),
    |r|r.to_string()
));

named!(parse_alpha<CompleteStr, String>, map!(
    take_while1_s!(is_not_tick),
    |r|r.to_string()
));

named!(spec_whitespace, eat_separator!(&b" \t"[..]));

named!(msg_header<::nom::types::CompleteStr, MessageHeader>,
    do_parse!(
        tag_s!("MSG")                           >>
        is_a!(" \t")                            >>
        subject: parse_completestr              >>                
        is_a!(" \t")                            >>
        sid:  parse_u64                         >>                
        is_a!(" \t")                            >>
        reply_to: opt!(terminated!(parse_completestr, is_a!(" \t"))) >>
        message_len: parse_u64                  >>

        ( MessageHeader { sid, subject, reply_to, message_len } )
    )
);
pub fn parse_msg_header(header: &str) -> Option<MessageHeader> {
    msg_header(CompleteStr(header)).ok().map(|h| h.1)
}

named!(pub_header<CompleteStr, PubHeader>,
    do_parse!(
        tag_s!("PUB")                               >>
        is_a!(" \t")                                >>
        subject: parse_completestr                  >>
        is_a!(" \t")                                >>
        reply_to: opt!(terminated!(parse_completestr, is_a!(" \t"))) >>
        message_len: parse_u64                      >>

        ( PubHeader { subject, reply_to, message_len } )
    )
);
pub fn parse_pub_header(header: &str) -> Option<PubHeader> {
    pub_header(CompleteStr(header)).ok().map(|h| h.1)
}

named!(sub_header<CompleteStr, SubHeader>,
    do_parse!(
        tag_s!("SUB")                                   >>
        is_a!(" \t")                                    >>
        subject: parse_completestr                      >>
        is_a!(" \t")                                    >>
        queue_group: opt!(terminated!(parse_completestr, is_a!(" \t"))) >>
        sid: parse_u64                                  >>

        ( SubHeader { subject, queue_group, sid } )
    )
);

pub fn parse_sub_header(header: &str) -> Option<SubHeader> {
    sub_header(CompleteStr(header)).ok().map(|h| h.1)
}

named!(unsub_header<CompleteStr, UnsubHeader>,
    do_parse!(
        tag_s!("UNSUB")                 >>
        is_a!(" \t")                    >>
        sid: parse_u64                  >>
        opt!(is_a!(" \t"))              >>
        max_messages: opt!(parse_u64)   >>

        ( UnsubHeader { sid, max_messages })
    )
);
pub fn parse_unsub_header(header: &str) -> Option<UnsubHeader> {
    unsub_header(CompleteStr(header)).ok().map(|h| h.1)
}

named!(err_header<CompleteStr, ErrorHeader>,
    do_parse!(
        tag_s!("-ERR '") >>
        message: parse_alpha >>
        char!('\'') >>

        ( ErrorHeader { message } )
    )
);
pub fn parse_err_header(header: &str) -> Option<ErrorHeader> {
    err_header(CompleteStr(header)).ok().map(|h| h.1)
}

#[cfg(test)]
mod test {
    use super::{
        err_header, msg_header, pub_header, split_header_and_payload, sub_header, unsub_header,
    };
    use nom::types::CompleteStr;

    #[test]
    fn msg_reply_to() {
        let raw = "MSG workdispatch 1 reply.topic 11\r\nHello World\r\n";
        let split = split_header_and_payload(raw);
        assert!(split.is_some());
        if let Some(split) = split {
            let hdr = split.0;
            let payload = split.1;

            assert_eq!(String::from_utf8(payload).unwrap(), "Hello World");
            let res = msg_header(CompleteStr(&hdr));
            println!("{:?}", res);
            assert!(res.is_ok());
        }
    }

    #[test]
    fn msg_irreg_whitespace() {
        let raw = "MSG  \t  workdispatch 1 reply.topic 11\r\nHello World\r\n";
        let split = split_header_and_payload(raw);
        assert!(split.is_some());
        if let Some(split) = split {
            let hdr = split.0;
            let payload = split.1;

            assert_eq!(String::from_utf8(payload).unwrap(), "Hello World");
            let res = msg_header(CompleteStr(&hdr));
            assert!(res.is_ok());
        }
    }

    #[test]
    fn unsub_no_max() {
        let msg = "UNSUB 1";
        let res = unsub_header(CompleteStr(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.sid, 1);
            assert_eq!(header.1.max_messages, None);
        }
    }

    #[test]
    fn unsub_max() {
        let msg = "UNSUB 1 5";
        let res = unsub_header(CompleteStr(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.sid, 1);
            assert_eq!(header.1.max_messages, Some(5));
        }
    }

    #[test]
    fn pub_no_reply() {
        let msg = "PUB FOO 11";
        let res = pub_header(CompleteStr(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "FOO");
            assert!(header.1.reply_to.is_none());
        }
    }

    #[test]
    fn pub_reply() {
        let msg = "PUB FRONT.DOOR INBOX.22 11";
        let res = pub_header(CompleteStr(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "FRONT.DOOR");
            assert_eq!(header.1.reply_to, Some("INBOX.22".to_string()));
        }
    }

    #[test]
    fn sub_no_qg() {
        let msg = "SUB FOO 1";
        let res = sub_header(CompleteStr(&msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "FOO");
            assert_eq!(header.1.sid, 1);
            assert_eq!(header.1.queue_group, None);
        }
    }

    #[test]
    fn sub_qg() {
        let msg = "SUB BAR G1 44";
        let res = sub_header(CompleteStr(&msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "BAR");
            assert_eq!(header.1.sid, 44);
            assert_eq!(header.1.queue_group, Some("G1".to_string()));
        }
    }

    #[test]
    fn msg_no_reply() {
        let msg = "MSG workdispatch 1 11";
        let res = msg_header(CompleteStr(msg));
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn error_header() {
        let msg = "-ERR 'Attempted To Connect To Route Port'";
        let res = err_header(CompleteStr(msg));
        println!("{:?}", res);
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.message, "Attempted To Connect To Route Port");
        }
    }
}
