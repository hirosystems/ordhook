use std::sync::mpsc::Sender;

use chainhook_sdk::{
    chainhooks::types::{BitcoinChainhookSpecification, ChainhookSpecification},
    observer::ObserverCommand,
    utils::Context,
};
use threadpool::ThreadPool;

use crate::{
    config::Config,
    scan::bitcoin::scan_bitcoin_chainstate_via_rpc_using_predicate,
    service::observers::{
        open_readwrite_observers_db_conn_or_panic, update_observer_streaming_enabled,
    },
    try_error,
};

pub fn start_bitcoin_scan_runloop(
    config: &Config,
    bitcoin_scan_op_rx: crossbeam_channel::Receiver<BitcoinChainhookSpecification>,
    observer_command_tx: Sender<ObserverCommand>,
    ctx: &Context,
) {
    let bitcoin_scan_pool = ThreadPool::new(config.resources.expected_observers_count);

    while let Ok(predicate_spec) = bitcoin_scan_op_rx.recv() {
        let moved_ctx = ctx.clone();
        let moved_config = config.clone();
        let observer_command_tx = observer_command_tx.clone();
        bitcoin_scan_pool.execute(move || {
            let op = scan_bitcoin_chainstate_via_rpc_using_predicate(
                &predicate_spec,
                &moved_config,
                None,
                &moved_ctx,
            );

            match hiro_system_kit::nestable_block_on(op) {
                Ok(_) => {}
                Err(e) => {
                    try_error!(
                        moved_ctx,
                        "Unable to evaluate predicate on Bitcoin chainstate: {e}",
                    );

                    // Update predicate
                    let mut observers_db_conn =
                        open_readwrite_observers_db_conn_or_panic(&moved_config, &moved_ctx);
                    update_observer_streaming_enabled(
                        &predicate_spec.uuid,
                        false,
                        &mut observers_db_conn,
                        &moved_ctx,
                    );
                    return;
                }
            };
            let _ = observer_command_tx.send(ObserverCommand::EnablePredicate(
                ChainhookSpecification::Bitcoin(predicate_spec),
            ));
        });
    }
    let _ = bitcoin_scan_pool.join();
}
