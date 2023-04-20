use crate::config::Config;
use crate::scan::bitcoin::scan_bitcoin_chainstate_via_http_using_predicate;
use crate::scan::stacks::scan_stacks_chainstate_via_csv_using_predicate;

use chainhook_event_observer::chainhooks::types::{ChainhookConfig, ChainhookFullSpecification};

use chainhook_event_observer::chainhooks::types::ChainhookSpecification;
use chainhook_event_observer::observer::{
    start_event_observer, ApiKey, ObserverCommand, ObserverEvent,
};
use chainhook_event_observer::utils::Context;
use chainhook_types::{BitcoinBlockSignaling, StacksBlockData, StacksChainEvent};
use redis::{Commands, Connection};
use threadpool::ThreadPool;

use std::sync::mpsc::channel;

pub const DEFAULT_INGESTION_PORT: u16 = 20455;
pub const DEFAULT_CONTROL_PORT: u16 = 20456;
pub const STACKS_SCAN_THREAD_POOL_SIZE: usize = 12;
pub const BITCOIN_SCAN_THREAD_POOL_SIZE: usize = 12;

pub struct Service {
    config: Config,
    ctx: Context,
}

impl Service {
    pub fn new(config: Config, ctx: Context) -> Self {
        Self { config, ctx }
    }

    pub async fn run(&mut self, predicates: Vec<ChainhookFullSpecification>) -> Result<(), String> {
        let mut chainhook_config = ChainhookConfig::new();

        if predicates.is_empty() {
            let registered_predicates = load_predicates_from_redis(&self.config, &self.ctx)?;
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

        for predicate in predicates.into_iter() {
            match chainhook_config.register_full_specification(
                (
                    &self.config.network.bitcoin_network,
                    &self.config.network.stacks_network,
                ),
                predicate,
                &ApiKey(None),
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

        if self.config.chainhooks.enable_http_api {
            info!(
                self.ctx.expect_logger(),
                "Listening for chainhook predicate registrations on port {}",
                event_observer_config.control_port
            );
        }

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
        let stacks_scan_pool = ThreadPool::new(STACKS_SCAN_THREAD_POOL_SIZE);
        let ctx = self.ctx.clone();
        let config = self.config.clone();
        let observer_command_tx_moved = observer_command_tx.clone();
        let _ = hiro_system_kit::thread_named("Stacks scan runloop")
            .spawn(move || {
                while let Ok((predicate_spec, api_key)) = stacks_scan_op_rx.recv() {
                    let moved_ctx = ctx.clone();
                    let mut moved_config = config.clone();
                    let observer_command_tx = observer_command_tx_moved.clone();
                    stacks_scan_pool.execute(move || {
                        let op = scan_stacks_chainstate_via_csv_using_predicate(
                            &predicate_spec,
                            &mut moved_config,
                            &moved_ctx,
                        );
                        let end_block = match hiro_system_kit::nestable_block_on(op) {
                            Ok(end_block) => end_block,
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
                            "Stacks chainstate scan completed up to block: {}", end_block.index
                        );
                        let _ = observer_command_tx.send(ObserverCommand::EnablePredicate(
                            ChainhookSpecification::Stacks(predicate_spec),
                            api_key,
                        ));
                    });
                }
                let res = stacks_scan_pool.join();
                res
            })
            .expect("unable to spawn thread");

        // Bitcoin scan operation threadpool
        let (bitcoin_scan_op_tx, bitcoin_scan_op_rx) = crossbeam_channel::unbounded();
        let bitcoin_scan_pool = ThreadPool::new(BITCOIN_SCAN_THREAD_POOL_SIZE);
        let ctx = self.ctx.clone();
        let config = self.config.clone();
        let moved_observer_command_tx = observer_command_tx.clone();
        let _ = hiro_system_kit::thread_named("Bitcoin scan runloop")
            .spawn(move || {
                while let Ok((predicate_spec, api_key)) = bitcoin_scan_op_rx.recv() {
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
                            api_key,
                        ));
                    });
                }
                let res = bitcoin_scan_pool.join();
                res
            })
            .expect("unable to spawn thread");

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
            let redis_config = self.config.expected_redis_config();

            let client = redis::Client::open(redis_config.uri.clone()).unwrap();
            let mut redis_con = match client.get_connection() {
                Ok(con) => con,
                Err(message) => {
                    error!(self.ctx.expect_logger(), "Redis: {}", message.to_string());
                    panic!();
                }
            };
            match event {
                ObserverEvent::HookRegistered(chainhook, api_key) => {
                    // If start block specified, use it.
                    // I no start block specified, depending on the nature the hook, we'd like to retrieve:
                    // - contract-id

                    let chainhook_key = chainhook.key();
                    let history: Vec<u64> = vec![];
                    let res: Result<(), redis::RedisError> = redis_con.hset_multiple(
                        &chainhook_key,
                        &[
                            ("specification", json!(chainhook).to_string()),
                            ("history", json!(history).to_string()),
                            ("scan_progress", json!(0).to_string()),
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
                            // let _ = stacks_scan_op_tx.send((predicate_spec, api_key));
                            info!(
                                self.ctx.expect_logger(),
                                "Enabling stacks predicate {}", predicate_spec.uuid
                            );
                            let _ = observer_command_tx.send(ObserverCommand::EnablePredicate(
                                ChainhookSpecification::Stacks(predicate_spec),
                                api_key,
                            ));
                        }
                        ChainhookSpecification::Bitcoin(predicate_spec) => {
                            let _ = bitcoin_scan_op_tx.send((predicate_spec, api_key));
                        }
                    }
                }
                ObserverEvent::HookDeregistered(chainhook) => {
                    let chainhook_key = chainhook.key();
                    let _: Result<(), redis::RedisError> = redis_con.del(chainhook_key);
                }
                ObserverEvent::BitcoinChainEvent(_chain_update) => {
                    debug!(self.ctx.expect_logger(), "Bitcoin update not stored");
                }
                ObserverEvent::StacksChainEvent(chain_event) => {
                    match &chain_event {
                        StacksChainEvent::ChainUpdatedWithBlocks(data) => {
                            update_storage_with_confirmed_stacks_blocks(
                                &mut redis_con,
                                &data.confirmed_blocks,
                                &self.ctx,
                            );
                        }
                        StacksChainEvent::ChainUpdatedWithReorg(data) => {
                            update_storage_with_confirmed_stacks_blocks(
                                &mut redis_con,
                                &data.confirmed_blocks,
                                &self.ctx,
                            );
                        }
                        StacksChainEvent::ChainUpdatedWithMicroblocks(_)
                        | StacksChainEvent::ChainUpdatedWithMicroblocksReorg(_) => {}
                    };
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

fn update_storage_with_confirmed_stacks_blocks(
    redis_con: &mut Connection,
    blocks: &Vec<StacksBlockData>,
    ctx: &Context,
) {
    let current_tip_height: u64 = redis_con.get(&format!("stx:tip")).unwrap_or(0);

    let mut new_tip = None;

    for block in blocks.iter() {
        let res: Result<(), redis::RedisError> = redis_con.hset_multiple(
            &format!("stx:{}", block.block_identifier.index),
            &[
                (
                    "block_identifier",
                    json!(block.block_identifier).to_string(),
                ),
                (
                    "parent_block_identifier",
                    json!(block.parent_block_identifier).to_string(),
                ),
                ("transactions", json!(block.transactions).to_string()),
                ("metadata", json!(block.metadata).to_string()),
            ],
        );
        if let Err(error) = res {
            crit!(
                ctx.expect_logger(),
                "unable to archive block {}: {}",
                block.block_identifier,
                error.to_string()
            );
        }
        if block.block_identifier.index >= current_tip_height {
            new_tip = Some(block);
        }
    }

    if let Some(block) = new_tip {
        info!(
            ctx.expect_logger(),
            "Archiving confirmed Stacks chain block {}", block.block_identifier
        );
        let _: Result<(), redis::RedisError> =
            redis_con.set(&format!("stx:tip"), block.block_identifier.index);
    }
}

fn load_predicates_from_redis(
    config: &Config,
    ctx: &Context,
) -> Result<Vec<ChainhookSpecification>, String> {
    let redis_config = config.expected_redis_config();
    let client = redis::Client::open(redis_config.uri.clone()).unwrap();
    let mut redis_con = match client.get_connection() {
        Ok(con) => con,
        Err(message) => {
            error!(
                ctx.expect_logger(),
                "Unable to connect to redis server: {}",
                message.to_string()
            );
            std::thread::sleep(std::time::Duration::from_secs(1));
            std::process::exit(1);
        }
    };

    let chainhooks_to_load: Vec<String> = redis_con
        .scan_match("chainhook:*:*:*")
        .expect("unable to retrieve prunable entries")
        .into_iter()
        .collect();

    let mut predicates = vec![];
    for key in chainhooks_to_load.iter() {
        let chainhook = match redis_con.hget::<_, _, String>(key, "specification") {
            Ok(spec) => match ChainhookSpecification::deserialize_specification(&spec, key) {
                Ok(spec) => spec,
                Err(e) => {
                    error!(
                        ctx.expect_logger(),
                        "unable to load chainhook associated with key {}: {}",
                        key,
                        e.to_string()
                    );
                    continue;
                }
            },
            Err(e) => {
                error!(
                    ctx.expect_logger(),
                    "unable to load chainhook associated with key {}: {}",
                    key,
                    e.to_string()
                );
                continue;
            }
        };
        predicates.push(chainhook);
    }
    Ok(predicates)
}
