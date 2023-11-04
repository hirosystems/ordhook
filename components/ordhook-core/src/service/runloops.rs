use std::sync::mpsc::Sender;

use chainhook_sdk::{
    chainhooks::types::{BitcoinChainhookSpecification, ChainhookSpecification},
    observer::ObserverCommand,
    utils::Context,
};
use threadpool::ThreadPool;

use crate::{
    config::{Config, PredicatesApi},
    scan::bitcoin::scan_bitcoin_chainstate_via_rpc_using_predicate,
    service::{
        predicates::open_readwrite_predicates_db_conn_or_panic, update_predicate_status,
        PredicateStatus,
    },
};

pub fn start_bitcoin_scan_runloop(
    config: &Config,
    bitcoin_scan_op_rx: crossbeam_channel::Receiver<BitcoinChainhookSpecification>,
    observer_command_tx: Sender<ObserverCommand>,
    ctx: &Context,
) {
    let bitcoin_scan_pool = ThreadPool::new(config.limits.max_number_of_concurrent_bitcoin_scans);

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
                    ctx.try_log(|logger| {
                        error!(
                            logger,
                            "Unable to evaluate predicate on Bitcoin chainstate: {e}",
                        )
                    });

                    // Update predicate status in redis
                    if let PredicatesApi::On(ref api_config) = moved_config.http_api {
                        let status = PredicateStatus::Interrupted(format!(
                            "Unable to evaluate predicate on Bitcoin chainstate: {e}"
                        ));
                        let mut predicates_db_conn =
                            open_readwrite_predicates_db_conn_or_panic(api_config, &moved_ctx);
                        update_predicate_status(
                            &predicate_spec.key(),
                            status,
                            &mut predicates_db_conn,
                            &moved_ctx,
                        );
                    }
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
