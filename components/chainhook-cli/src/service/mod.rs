mod http_api;

use crate::config::{Config, PredicatesApi};
use crate::scan::bitcoin::scan_bitcoin_chainstate_via_http_using_predicate;
use crate::scan::stacks::{
    consolidate_local_stacks_chainstate_using_csv,
    scan_stacks_chainstate_via_rocksdb_using_predicate,
};
use crate::service::http_api::{load_predicates_from_redis, start_predicate_api_server};
use crate::storage::{
    insert_entries_in_stacks_blocks, open_readonly_stacks_db_conn, open_readwrite_stacks_db_conn,
};

use chainhook_event_observer::chainhooks::types::{ChainhookConfig, ChainhookFullSpecification};

use chainhook_event_observer::chainhooks::types::ChainhookSpecification;
use chainhook_event_observer::observer::{start_event_observer, ObserverCommand, ObserverEvent};
use chainhook_event_observer::utils::Context;
use chainhook_types::{BitcoinBlockSignaling, StacksChainEvent};
use redis::{Commands, Connection};
use threadpool::ThreadPool;

use std::sync::mpsc::channel;

pub struct Service {
    config: Config,
    ctx: Context,
}

impl Service {
    pub fn new(config: Config, ctx: Context) -> Self {
        Self { config, ctx }
    }

    pub async fn run(
        &mut self,
        predicates: Vec<ChainhookFullSpecification>,
        hord_disabled: bool,
    ) -> Result<(), String> {
        let mut chainhook_config = ChainhookConfig::new();

        // If no predicates passed at launch, retrieve predicates from Redis
        if predicates.is_empty() && self.config.is_http_api_enabled() {
            let registered_predicates = match load_predicates_from_redis(&self.config, &self.ctx) {
                Ok(predicates) => predicates,
                Err(e) => {
                    error!(
                        self.ctx.expect_logger(),
                        "Failed loading predicate from storage: {}",
                        e.to_string()
                    );
                    vec![]
                }
            };
            for predicate in registered_predicates.into_iter() {
                let predicate_uuid = predicate.uuid().to_string();
                match chainhook_config.register_specification(predicate, true) {
                    Ok(_) => {
                        info!(
                            self.ctx.expect_logger(),
                            "Predicate {} retrieved from storage and loaded", predicate_uuid,
                        );
                    }
                    Err(e) => {
                        error!(
                            self.ctx.expect_logger(),
                            "Failed loading predicate from storage: {}",
                            e.to_string()
                        );
                    }
                }
            }
        }

        // For each predicate found, register in memory.
        for predicate in predicates.into_iter() {
            match chainhook_config.register_full_specification(
                (
                    &self.config.network.bitcoin_network,
                    &self.config.network.stacks_network,
                ),
                predicate,
            ) {
                Ok(spec) => {
                    info!(
                        self.ctx.expect_logger(),
                        "Predicate {} retrieved from config and loaded",
                        spec.uuid(),
                    );
                }
                Err(e) => {
                    error!(
                        self.ctx.expect_logger(),
                        "Failed loading predicate from config: {}",
                        e.to_string()
                    );
                }
            }
        }

        let (observer_command_tx, observer_command_rx) = channel();
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        // let (ordinal_indexer_command_tx, ordinal_indexer_command_rx) = channel();

        let mut event_observer_config = self.config.get_event_observer_config();
        event_observer_config.chainhook_config = Some(chainhook_config);
        event_observer_config.hord_config = match hord_disabled {
            true => None,
            false => Some(self.config.get_hord_config()),
        };

        // Download and ingest a Stacks dump
        let _ = consolidate_local_stacks_chainstate_using_csv(&mut self.config, &self.ctx).await;

        // Download and ingest a Ordinal dump, if hord is enabled
        if !hord_disabled {
            // TODO: add flag
            // let _ = download_ordinals_dataset_if_required(&mut self.config, &self.ctx).await;
        }

        // Start chainhook event observer
        let context_cloned = self.ctx.clone();
        let event_observer_config_moved = event_observer_config.clone();
        let observer_command_tx_moved = observer_command_tx.clone();
        let _ = std::thread::spawn(move || {
            let future = start_event_observer(
                event_observer_config_moved,
                observer_command_tx_moved,
                observer_command_rx,
                Some(observer_event_tx),
                context_cloned,
            );
            let _ = hiro_system_kit::nestable_block_on(future);
        });

        // Stacks scan operation threadpool
        let (stacks_scan_op_tx, stacks_scan_op_rx) = crossbeam_channel::unbounded();
        let stacks_scan_pool =
            ThreadPool::new(self.config.limits.max_number_of_concurrent_stacks_scans);
        let ctx = self.ctx.clone();
        let config = self.config.clone();
        let observer_command_tx_moved = observer_command_tx.clone();
        let _ = hiro_system_kit::thread_named("Stacks scan runloop")
            .spawn(move || {
                while let Ok(mut predicate_spec) = stacks_scan_op_rx.recv() {
                    let moved_ctx = ctx.clone();
                    let moved_config = config.clone();
                    let observer_command_tx = observer_command_tx_moved.clone();
                    stacks_scan_pool.execute(move || {
                        let stacks_db_conn = match open_readonly_stacks_db_conn(
                            &moved_config.expected_cache_path(),
                            &moved_ctx,
                        ) {
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
                        let last_block_scanned = match hiro_system_kit::nestable_block_on(op) {
                            Ok(last_block_scanned) => last_block_scanned,
                            Err(e) => {
                                error!(
                                    moved_ctx.expect_logger(),
                                    "Unable to evaluate predicate on Stacks chainstate: {e}",
                                );
                                return;
                            }
                        };
                        info!(
                            moved_ctx.expect_logger(),
                            "Stacks chainstate scan completed up to block: {}",
                            last_block_scanned.index
                        );
                        predicate_spec.end_block = Some(last_block_scanned.index);
                        let _ = observer_command_tx.send(ObserverCommand::EnablePredicate(
                            ChainhookSpecification::Stacks(predicate_spec),
                        ));
                    });
                }
                let res = stacks_scan_pool.join();
                res
            })
            .expect("unable to spawn thread");

        // Bitcoin scan operation threadpool
        let (bitcoin_scan_op_tx, bitcoin_scan_op_rx) = crossbeam_channel::unbounded();
        let bitcoin_scan_pool =
            ThreadPool::new(self.config.limits.max_number_of_concurrent_bitcoin_scans);
        let ctx = self.ctx.clone();
        let config = self.config.clone();
        let moved_observer_command_tx = observer_command_tx.clone();
        let _ = hiro_system_kit::thread_named("Bitcoin scan runloop")
            .spawn(move || {
                while let Ok(predicate_spec) = bitcoin_scan_op_rx.recv() {
                    let moved_ctx = ctx.clone();
                    let moved_config = config.clone();
                    let observer_command_tx = moved_observer_command_tx.clone();
                    bitcoin_scan_pool.execute(move || {
                        let op = scan_bitcoin_chainstate_via_http_using_predicate(
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
                res
            })
            .expect("unable to spawn thread");

        info!(
            self.ctx.expect_logger(),
            "Listening on port {} for Stacks chain events", event_observer_config.ingestion_port
        );
        match event_observer_config.bitcoin_block_signaling {
            BitcoinBlockSignaling::ZeroMQ(ref url) => {
                info!(
                    self.ctx.expect_logger(),
                    "Observing Bitcoin chain events via ZeroMQ: {}", url
                );
            }
            BitcoinBlockSignaling::Stacks(ref _url) => {
                info!(
                    self.ctx.expect_logger(),
                    "Observing Bitcoin chain events via Stacks node"
                );
            }
        }
        // Enable HTTP Chainhook API, if required
        let mut redis_con = match self.config.http_api {
            PredicatesApi::On(ref api_config) => {
                info!(
                    self.ctx.expect_logger(),
                    "Listening for chainhook predicate registrations on port {}",
                    api_config.http_port
                );
                let ctx = self.ctx.clone();
                let api_config = api_config.clone();
                let moved_observer_command_tx = observer_command_tx.clone();

                let _ = hiro_system_kit::thread_named("HTTP Predicate API").spawn(move || {
                    let future =
                        start_predicate_api_server(&api_config, moved_observer_command_tx, ctx);
                    let _ = hiro_system_kit::nestable_block_on(future);
                });

                // Test and initialize a database connection
                let redis_uri = self.config.expected_api_database_uri();
                let client = redis::Client::open(redis_uri.clone()).unwrap();
                let redis_con = match client.get_connection() {
                    Ok(con) => con,
                    Err(message) => {
                        error!(self.ctx.expect_logger(), "Redis: {}", message.to_string());
                        panic!();
                    }
                };
                Some(redis_con)
            }
            PredicatesApi::Off => None,
        };

        let mut stacks_event = 0;
        loop {
            let event = match observer_event_rx.recv() {
                Ok(cmd) => cmd,
                Err(e) => {
                    error!(
                        self.ctx.expect_logger(),
                        "Error: broken channel {}",
                        e.to_string()
                    );
                    break;
                }
            };

            match event {
                ObserverEvent::HookRegistered(chainhook) => {
                    // If start block specified, use it.
                    // I no start block specified, depending on the nature the hook, we'd like to retrieve:
                    // - contract-id
                    if let Some(ref mut redis_con) = redis_con {
                        let chainhook_key = chainhook.key();
                        let res: Result<(), redis::RedisError> = redis_con.hset_multiple(
                            &chainhook_key,
                            &[
                                ("specification", json!(chainhook).to_string()),
                                ("last_evaluation", json!(0).to_string()),
                                ("last_trigger", json!(0).to_string()),
                            ],
                        );
                        if let Err(e) = res {
                            error!(
                                self.ctx.expect_logger(),
                                "unable to store chainhook {chainhook_key}: {}",
                                e.to_string()
                            );
                        }
                        match chainhook {
                            ChainhookSpecification::Stacks(predicate_spec) => {
                                let _ = stacks_scan_op_tx.send(predicate_spec);
                            }
                            ChainhookSpecification::Bitcoin(predicate_spec) => {
                                let _ = bitcoin_scan_op_tx.send(predicate_spec);
                            }
                        }
                    }
                }
                ObserverEvent::HookDeregistered(chainhook) => {
                    if let Some(ref mut redis_con) = redis_con {
                        let chainhook_key = chainhook.key();
                        let _: Result<(), redis::RedisError> = redis_con.del(chainhook_key);
                    }
                }
                ObserverEvent::BitcoinChainEvent((chain_update, report)) => {
                    debug!(self.ctx.expect_logger(), "Bitcoin update not stored");
                }
                ObserverEvent::StacksChainEvent((chain_event, report)) => {
                    let stacks_db_conn_rw = match open_readwrite_stacks_db_conn(
                        &self.config.expected_cache_path(),
                        &self.ctx,
                    ) {
                        Ok(db_conn) => db_conn,
                        Err(e) => {
                            error!(
                                self.ctx.expect_logger(),
                                "unable to store stacks block: {}",
                                e.to_string()
                            );
                            continue;
                        }
                    };
                    match &chain_event {
                        StacksChainEvent::ChainUpdatedWithBlocks(data) => {
                            stacks_event += 1;
                            insert_entries_in_stacks_blocks(
                                &data.confirmed_blocks,
                                &stacks_db_conn_rw,
                                &self.ctx,
                            );
                        }
                        StacksChainEvent::ChainUpdatedWithReorg(data) => {
                            insert_entries_in_stacks_blocks(
                                &data.confirmed_blocks,
                                &stacks_db_conn_rw,
                                &self.ctx,
                            );
                        }
                        StacksChainEvent::ChainUpdatedWithMicroblocks(_)
                        | StacksChainEvent::ChainUpdatedWithMicroblocksReorg(_) => {}
                    };
                    for (predicate_uuid, blocks_ids) in report.predicates_evaluated.iter() {}
                    for (predicate_uuid, blocks_ids) in report.predicates_triggered.iter() {}
                    // Every 32 blocks, we will check if there's a new Stacks file archive to ingest
                    if stacks_event > 32 {
                        stacks_event = 0;
                        let _ = consolidate_local_stacks_chainstate_using_csv(
                            &mut self.config,
                            &self.ctx,
                        )
                        .await;
                    }
                }
                ObserverEvent::Terminate => {
                    info!(self.ctx.expect_logger(), "Terminating runloop");
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

pub enum PredicateStatus {
    InitialScan(u64, u64, u64),
    Active(u64),
    Disabled,
}

pub fn update_predicate(uuid: String, status: PredicateStatus, redis_con: &Connection) {
    // let res: Result<(), redis::RedisError> = redis_con.hset_multiple(
    //     &chainhook_key,
    //     &[
    //         ("specification", json!(chainhook).to_string()),
    //         ("last_evaluation", json!(0).to_string()),
    //         ("last_trigger", json!(0).to_string()),
    //     ],
    // );
}
