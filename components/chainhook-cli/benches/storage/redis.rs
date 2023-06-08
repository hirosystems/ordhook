use criterion::{black_box, criterion_group, criterion_main, Criterion};
use chainhook_event_indexer::ingestion::start_ingesting;
use chainhook_sdk::indexer::IndexerConfig;


fn criterion_benchmark(c: &mut Criterion) {
    let config = IndexerConfig {
        stacks_node_rpc_url: "http://0.0.0.0:20443".into(),
        bitcoind_rpc_url: "http://0.0.0.0:18443".into(),
        bitcoind_rpc_username: "devnet".into(),
        bitcoind_rpc_password: "devnet".into(),    
    };
    c.bench_function("redis", |b| b.iter(|| start_ingesting("/Users/ludovic/Downloads/stacks-blockchain-api.tsv".into(), config.clone()).unwrap()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
