use std::sync::mpsc::Sender;

use chainhook_event_observer::{
    chainhooks::types::{
        BitcoinChainhookSpecification, ChainhookSpecification, StacksChainhookSpecification,
    },
    observer::ObserverCommand,
    utils::Context,
};
use threadpool::ThreadPool;

use crate::{
    config::{Config, PredicatesApi},
    scan::{
        bitcoin::scan_bitcoin_chainstate_via_rpc_using_predicate,
        stacks::scan_stacks_chainstate_via_rocksdb_using_predicate,
    },
    storage::open_readonly_stacks_db_conn, service::{PredicateStatus, update_predicate_status, open_readwrite_predicates_db_conn_or_panic},
};

pub fn start_stacks_scan_runloop(
    config: &Config,
    stacks_scan_op_rx: crossbeam_channel::Receiver<StacksChainhookSpecification>,
    observer_command_tx: Sender<ObserverCommand>,
    ctx: &Context,
) {
    let stacks_scan_pool = ThreadPool::new(config.limits.max_number_of_concurrent_stacks_scans);
    while let Ok(mut predicate_spec) = stacks_scan_op_rx.recv() {
        let moved_ctx = ctx.clone();
        let moved_config = config.clone();
        let observer_command_tx = observer_command_tx.clone();
        stacks_scan_pool.execute(move || {
            let stacks_db_conn =
                match open_readonly_stacks_db_conn(&moved_config.expected_cache_path(), &moved_ctx)
                {
                    Ok(db_conn) => db_conn,
                    Err(e) => {
                        error!(
                            moved_ctx.expect_logger(),
                            "unable to store stacks block: {}",
                            e.to_string()
                        );
                        unimplemented!()
                    }
                };

            let op = scan_stacks_chainstate_via_rocksdb_using_predicate(
                &predicate_spec,
                &stacks_db_conn,
                &moved_config,
                &moved_ctx,
            );
            let res = hiro_system_kit::nestable_block_on(op);
            let last_block_scanned = match res {
                Ok(last_block_scanned) => last_block_scanned,
                Err(e) => {
                    error!(
                        moved_ctx.expect_logger(),
                        "Unable to evaluate predicate on Stacks chainstate: {e}",
                    );

                    // Update predicate status in redis
                    if let PredicatesApi::On(ref api_config) = moved_config.http_api {
                        let status = PredicateStatus::Interrupted("Unable to evaluate predicate on Stacks chainstate: {e}".to_string());
                        let mut predicates_db_conn = open_readwrite_predicates_db_conn_or_panic(api_config, &moved_ctx);
                        update_predicate_status(&predicate_spec.key(), status, &mut predicates_db_conn, &moved_ctx);
                    }

                    return;
                }
            };
            info!(
                moved_ctx.expect_logger(),
                "Stacks chainstate scan completed up to block: {}", last_block_scanned.index
            );
            predicate_spec.end_block = Some(last_block_scanned.index);
            let _ = observer_command_tx.send(ObserverCommand::EnablePredicate(
                ChainhookSpecification::Stacks(predicate_spec),
            ));
        });
    }
    let res = stacks_scan_pool.join();
    res
}

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
                &moved_ctx,
            );

            match hiro_system_kit::nestable_block_on(op) {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        moved_ctx.expect_logger(),
                        "Unable to evaluate predicate on Bitcoin chainstate: {e}",
                    );
                    return;
                }
            };
            let _ = observer_command_tx.send(ObserverCommand::EnablePredicate(
                ChainhookSpecification::Bitcoin(predicate_spec),
            ));
        });
    }
    let res = bitcoin_scan_pool.join();
}
