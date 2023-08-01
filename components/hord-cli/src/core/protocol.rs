use chainhook_sdk::types::{BitcoinBlockData, OrdinalOperation, TransactionIdentifier};
use chainhook_sdk::utils::Context;
use dashmap::DashMap;
use fxhash::FxHasher;
use rand::seq::SliceRandom;
use rand::thread_rng;
use rusqlite::Connection;
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::mpsc::channel;
use std::sync::Arc;

use crate::db::{find_all_inscriptions_in_block, retrieve_satoshi_point_using_lazy_storage_v3};

use crate::db::{LazyBlockTransaction, TraversalResult};

use super::HordConfig;

pub fn retrieve_inscribed_satoshi_points_from_block_v3(
    block: &BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    cache_l1: &mut HashMap<(TransactionIdentifier, usize), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_conn: &mut Connection,
    hord_config: &HordConfig,
    ctx: &Context,
) -> Result<bool, String> {
    let (mut transactions_ids, l1_cache_hits) =
        get_transactions_to_process(block, cache_l1, inscriptions_db_conn, ctx);

    let inner_ctx = if hord_config.logs.ordinals_computation {
        ctx.clone()
    } else {
        Context::empty()
    };

    let has_transactions_to_process = !transactions_ids.is_empty() || !l1_cache_hits.is_empty();

    let thread_max = hord_config.ingestion_thread_max * 2;

    if has_transactions_to_process {
        let expected_traversals = transactions_ids.len() + l1_cache_hits.len();
        let (traversal_tx, traversal_rx) = channel();

        let mut tx_thread_pool = vec![];
        let mut thread_pool_handles = vec![];

        for thread_index in 0..thread_max {
            let (tx, rx) = channel();
            tx_thread_pool.push(tx);

            let moved_traversal_tx = traversal_tx.clone();
            let moved_ctx = inner_ctx.clone();
            let moved_hord_db_path = hord_config.db_path.clone();
            let local_cache = cache_l2.clone();

            let handle = hiro_system_kit::thread_named("Worker")
                .spawn(move || {
                    while let Ok(Some((
                        transaction_id,
                        block_identifier,
                        input_index,
                        prioritary,
                    ))) = rx.recv()
                    {
                        let traversal: Result<TraversalResult, String> =
                            retrieve_satoshi_point_using_lazy_storage_v3(
                                &moved_hord_db_path,
                                &block_identifier,
                                &transaction_id,
                                input_index,
                                0,
                                &local_cache,
                                &moved_ctx,
                            );
                        let _ = moved_traversal_tx.send((traversal, prioritary, thread_index));
                    }
                })
                .expect("unable to spawn thread");
            thread_pool_handles.push(handle);
        }

        // Empty cache
        let mut thread_index = 0;
        for key in l1_cache_hits.iter() {
            if let Some(entry) = cache_l1.remove(key) {
                let _ = traversal_tx.send((Ok(entry), true, thread_index));
                thread_index = (thread_index + 1) % thread_max;
            }
        }

        ctx.try_log(|logger| {
            info!(
                logger,
                "Number of inscriptions in block #{} to process: {} (L1 cache hits: {}, queue len: {}, L1 cache len: {}, L2 cache len: {})",
                block.block_identifier.index,
                transactions_ids.len(),
                l1_cache_hits.len(),
                next_blocks.len(),
                cache_l1.len(),
                cache_l2.len(),
            )
        });

        let mut rng = thread_rng();
        transactions_ids.shuffle(&mut rng);
        let mut priority_queue = VecDeque::new();
        let mut warmup_queue = VecDeque::new();

        for (transaction_id, input_index) in transactions_ids.into_iter() {
            priority_queue.push_back((
                transaction_id,
                block.block_identifier.clone(),
                input_index,
                true,
            ));
        }

        // Feed each workers with 2 workitems each
        for thread_index in 0..thread_max {
            let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
        }
        for thread_index in 0..thread_max {
            let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
        }

        let mut next_block_iter = next_blocks.iter();
        let mut traversals_received = 0;
        while let Ok((traversal_result, prioritary, thread_index)) = traversal_rx.recv() {
            if prioritary {
                traversals_received += 1;
            }
            match traversal_result {
                Ok(traversal) => {
                    inner_ctx.try_log(|logger| {
                        info!(
                            logger,
                            "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times (progress: {traversals_received}/{expected_traversals}) (priority queue: {prioritary}, thread: {thread_index}).",
                            traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
                            )
                    });
                    cache_l1.insert(
                        (
                            traversal.transaction_identifier_inscription.clone(),
                            traversal.inscription_input_index,
                        ),
                        traversal,
                    );
                }
                Err(e) => {
                    ctx.try_log(|logger| {
                        error!(logger, "Unable to compute inscription's Satoshi: {e}",)
                    });
                }
            }
            if traversals_received == expected_traversals {
                break;
            }

            if let Some(w) = priority_queue.pop_front() {
                let _ = tx_thread_pool[thread_index].send(Some(w));
            } else {
                if let Some(w) = warmup_queue.pop_front() {
                    let _ = tx_thread_pool[thread_index].send(Some(w));
                } else {
                    if let Some(next_block) = next_block_iter.next() {
                        let (mut transactions_ids, _) = get_transactions_to_process(
                            next_block,
                            cache_l1,
                            inscriptions_db_conn,
                            ctx,
                        );

                        ctx.try_log(|logger| {
                            info!(
                                logger,
                                "Number of inscriptions in block #{} to pre-process: {}",
                                block.block_identifier.index,
                                transactions_ids.len()
                            )
                        });

                        transactions_ids.shuffle(&mut rng);
                        for (transaction_id, input_index) in transactions_ids.into_iter() {
                            warmup_queue.push_back((
                                transaction_id,
                                next_block.block_identifier.clone(),
                                input_index,
                                false,
                            ));
                        }
                        let _ = tx_thread_pool[thread_index].send(warmup_queue.pop_front());
                    }
                }
            }
        }

        for tx in tx_thread_pool.iter() {
            // Empty the queue
            if let Ok((traversal_result, prioritary, thread_index)) = traversal_rx.try_recv() {
                if let Ok(traversal) = traversal_result {
                    inner_ctx.try_log(|logger| {
                            info!(
                                logger,
                                "Satoshi #{} was minted in block #{} at offset {} and was transferred {} times (progress: {traversals_received}/{expected_traversals}) (priority queue: {prioritary}, thread: {thread_index}).",
                                traversal.ordinal_number, traversal.get_ordinal_coinbase_height(), traversal.get_ordinal_coinbase_offset(), traversal.transfers
                                )
                        });
                    cache_l1.insert(
                        (
                            traversal.transaction_identifier_inscription.clone(),
                            traversal.inscription_input_index,
                        ),
                        traversal,
                    );
                }
            }
            let _ = tx.send(None);
        }

        let _ = hiro_system_kit::thread_named("Garbage collection").spawn(move || {
            for handle in thread_pool_handles.into_iter() {
                let _ = handle.join();
            }
        });
    } else {
        ctx.try_log(|logger| {
            info!(
                logger,
                "No inscriptions to index in block #{}", block.block_identifier.index
            )
        });
    }
    Ok(has_transactions_to_process)
}

fn get_transactions_to_process(
    block: &BitcoinBlockData,
    cache_l1: &mut HashMap<(TransactionIdentifier, usize), TraversalResult>,
    inscriptions_db_conn: &mut Connection,
    ctx: &Context,
) -> (
    Vec<(TransactionIdentifier, usize)>,
    Vec<(TransactionIdentifier, usize)>,
) {
    let mut transactions_ids: Vec<(TransactionIdentifier, usize)> = vec![];
    let mut l1_cache_hits = vec![];

    let mut known_transactions =
        find_all_inscriptions_in_block(&block.block_identifier.index, inscriptions_db_conn, ctx);

    for tx in block.transactions.iter().skip(1) {
        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            let inscription_data = match ordinal_event {
                OrdinalOperation::InscriptionRevealed(inscription_data) => inscription_data,
                OrdinalOperation::CursedInscriptionRevealed(inscription_data) => inscription_data,
                OrdinalOperation::InscriptionTransferred(_) => {
                    continue;
                }
            };
            let key = (
                tx.transaction_identifier.clone(),
                inscription_data.inscription_input_index,
            );
            if cache_l1.contains_key(&key) {
                l1_cache_hits.push(key);
                continue;
            }

            if let Some(entry) = known_transactions.remove(&key) {
                l1_cache_hits.push(key.clone());
                cache_l1.insert(key, entry);
                continue;
            }

            // Enqueue for traversals
            transactions_ids.push((
                tx.transaction_identifier.clone(),
                inscription_data.inscription_input_index,
            ));
        }
    }
    (transactions_ids, l1_cache_hits)
}
