#[macro_use]
extern crate criterion;
extern crate nats_types;

use criterion::Criterion;
use nats_types::*;
use std::str::FromStr;

fn benchmark_parser(c: &mut Criterion) {
    c.bench_function("connect_parse", |b| {
        let cmd = "CONNECT\t{\"verbose\":false,\"pedantic\":false,\"tls_required\":false,\"name\":\"encabulators\",\"lang\":\"rust\",\"version\":\"1.0.0\"}\r\n";
        b.iter(|| ProtocolMessage::from_str(cmd))
    });

    c.bench_function("connect_write", |b| {
        b.iter(|| format!("{}", ConnectionInformation::default()))
    });

    c.bench_function("pub_parse", |b| {
        let cmd = "PUB\tFOO\t11\r\nHello NATS!\r\n";
        b.iter(|| ProtocolMessage::from_str(cmd))
    });

    c.bench_function("pub_write", |b| {
        b.iter(|| format!("{}", PublishMessage::default()))
    });

    c.bench_function("sub_parse", |b| {
        let cmd = "SUB\tFOO\tpouet\r\n";
        b.iter(|| ProtocolMessage::from_str(cmd))
    });

    c.bench_function("sub_write", |b| {
        b.iter(|| format!("{}", SubscribeMessage::default()))
    });

    c.bench_function("unsub_parse", |b| {
        let cmd = "UNSUB\tpouet\r\n";
        b.iter(|| ProtocolMessage::from_str(cmd))
    });

    c.bench_function("unsub_write", |b| {
        b.iter(|| format!("{}", UnsubscribeMessage::default()))
    });

    c.bench_function("info_parse", |b| {
        let cmd = "INFO\t{\"server_id\":\"test\",\"version\":\"1.3.0\",\"go\":\"go1.10.3\",\"host\":\"0.0.0.0\",\"port\":4222,\"max_payload\":4000,\"proto\":1,\"client_id\":1337}\r\n";
        b.iter(|| ProtocolMessage::from_str(cmd))
    });

    c.bench_function("info_write", |b| {
        b.iter(|| format!("{}", ServerInformation::default()))
    });

    c.bench_function("message_parse", |b| {
        let cmd = "MSG\tFOO\tpouet\t4\r\ntoto\r\n";
        b.iter(|| ProtocolMessage::from_str(cmd))
    });

    c.bench_function("message_write", |b| {
        b.iter(|| format!("{}", DeliveredMessage::default()))
    });
}

criterion_group!(benches, benchmark_parser);
criterion_main!(benches);
