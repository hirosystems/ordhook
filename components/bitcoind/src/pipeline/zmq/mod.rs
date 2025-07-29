use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use config::Config;
use zmq::Socket;

use crate::{
    pipeline::{
        rpc::{build_http_client, download_and_parse_block_with_retry, standardize_bitcoin_block},
        BlockProcessor, BlockProcessorCommand,
    },
    try_info, try_warn,
    types::{BitcoinNetwork, BlockBytesCursor},
    utils::Context,
};

fn new_zmq_socket() -> Socket {
    let context = zmq::Context::new();
    let socket = context.socket(zmq::SUB).unwrap();
    assert!(socket.set_subscribe(b"hashblock").is_ok());
    assert!(socket.set_rcvhwm(0).is_ok());
    // We override the OS default behavior:
    assert!(socket.set_tcp_keepalive(1).is_ok());
    // The keepalive routine will wait for 5 minutes
    assert!(socket.set_tcp_keepalive_idle(300).is_ok());
    // And then resend it every 60 seconds
    assert!(socket.set_tcp_keepalive_intvl(60).is_ok());
    // 120 times
    assert!(socket.set_tcp_keepalive_cnt(120).is_ok());
    socket
}

pub(crate) async fn start_zeromq_pipeline(
    block_processor: &mut BlockProcessor,
    start_sequencing_blocks_at_height: u64,
    compress_blocks: bool,
    abort_signal: &Arc<AtomicBool>,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let http_client = build_http_client();
    let bitcoind_zmq_url = config.bitcoind.zmq_url.clone();
    let network = BitcoinNetwork::from_network(config.bitcoind.network);
    try_info!(
        ctx,
        "zmq: Waiting for ZMQ connection acknowledgment from bitcoind"
    );

    let mut socket = new_zmq_socket();
    assert!(socket.connect(&bitcoind_zmq_url).is_ok());
    try_info!(
        ctx,
        "zmq: Connected, waiting for ZMQ messages from bitcoind"
    );

    loop {
        // Check if the indexer has been interrupted. If so, send a terminate command to the block processor.
        if abort_signal.load(Ordering::SeqCst) {
            block_processor
                .commands_tx
                .send(BlockProcessorCommand::Terminate)
                .map_err(|e| e.to_string())?;
            return Ok(());
        }

        // Receive a new ZMQ message from bitcoind.
        let msg = match socket.recv_multipart(0) {
            Ok(msg) => msg,
            Err(e) => {
                try_warn!(ctx, "zmq: Unable to receive ZMQ message: {e}");
                socket = new_zmq_socket();
                assert!(socket.connect(&bitcoind_zmq_url).is_ok());
                continue;
            }
        };
        let (topic, data, _sequence) = (&msg[0], &msg[1], &msg[2]);

        if !topic.eq(b"hashblock") {
            try_warn!(
                ctx,
                "zmq: {} Topic not supported",
                String::from_utf8(topic.clone()).unwrap()
            );
            continue;
        }

        let block_hash = hex::encode(data);

        try_info!(ctx, "zmq: Bitcoin block hash announced {block_hash}");
        let raw_block_data = match download_and_parse_block_with_retry(
            &http_client,
            &block_hash,
            &config.bitcoind,
            ctx,
        )
        .await
        {
            Ok(block) => block,
            Err(e) => {
                try_warn!(ctx, "zmq: Unable to download block: {e}");
                continue;
            }
        };
        let block_height = raw_block_data.height as u64;
        let compacted_blocks = if compress_blocks {
            vec![(
                block_height,
                BlockBytesCursor::from_full_block(&raw_block_data)
                    .expect("unable to compress block"),
            )]
        } else {
            vec![]
        };
        let blocks = if block_height >= start_sequencing_blocks_at_height {
            let block = standardize_bitcoin_block(raw_block_data, &network, ctx)
                .expect("unable to deserialize block");
            vec![block]
        } else {
            vec![]
        };
        block_processor
            .commands_tx
            .send(BlockProcessorCommand::ProcessBlocks {
                compacted_blocks,
                blocks,
            })
            .map_err(|e| e.to_string())?;
    }
}
