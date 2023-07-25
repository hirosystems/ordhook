use std::{
    sync::{
        mpsc::{channel, Sender},
        Arc,
    },
    thread::JoinHandle,
};

use chainhook_sdk::{indexer::bitcoin::BitcoinBlockFullBreakdown, utils::Context};

use crate::{
    config::Config,
    db::{open_readwrite_hord_db_conn, process_blocks},
};

use super::new_traversals_lazy_cache;

pub fn start_ordinals_number_processor(
    config: &Config,
    ctx: &Context,
) -> (Sender<Vec<BitcoinBlockFullBreakdown>>, JoinHandle<()>) {
    // let mut batches = HashMap::new();

    let (tx, rx) = channel::<Vec<BitcoinBlockFullBreakdown>>();
    // let (inner_tx, inner_rx) = channel();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Batch receiver")
        .spawn(move || {
            let cache_l2 = Arc::new(new_traversals_lazy_cache(1024));
            let mut inscriptions_db_conn_rw =
                open_readwrite_hord_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let hord_config = config.get_hord_config();
            while let Ok(raw_blocks) = rx.recv() {
                info!(
                    ctx.expect_logger(),
                    "Processing {} blocks",
                    raw_blocks.len()
                );
                process_blocks(
                    raw_blocks,
                    config.network.bitcoin_network.clone(),
                    &cache_l2,
                    &mut inscriptions_db_conn_rw,
                    &hord_config,
                    &ctx,
                )
            }
        })
        .expect("unable to spawn thread");

    (tx, handle)
}
