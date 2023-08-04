use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    hash::BuildHasherDefault,
    sync::Arc,
};

use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{hashes::hex::FromHex, Address, Network, Script},
    types::{
        BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, BlockIdentifier,
        OrdinalInscriptionCurseType, OrdinalOperation, TransactionIdentifier,
    },
    utils::Context,
};
use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::{Connection, Transaction};

use crate::{
    core::HordConfig,
    db::{
        find_blessed_inscription_with_ordinal_number,
        find_latest_cursed_inscription_number_at_block_height,
        find_latest_inscription_number_at_block_height, format_satpoint_to_watch,
        insert_new_inscriptions_from_block_in_inscriptions_and_locations, LazyBlockTransaction,
        TraversalResult,
    },
    ord::height::Height,
};

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::mpsc::channel;

use crate::db::find_all_inscriptions_in_block;

use super::{
    inscription_tracking::augment_transaction_with_ordinals_transfers_data,
    satoshi_numbering::compute_satoshi_number,
};

pub fn parallelize_inscription_data_computations(
    block: &BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_tx: &Transaction,
    hord_config: &HordConfig,
    ctx: &Context,
) -> Result<bool, String> {
    let (mut transactions_ids, l1_cache_hits) =
        get_transactions_to_process(block, cache_l1, inscriptions_db_tx, ctx);

    let inner_ctx = if hord_config.logs.ordinals_internals {
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
                        let traversal: Result<TraversalResult, String> = compute_satoshi_number(
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
                            inscriptions_db_tx,
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
    }
    Ok(has_transactions_to_process)
}

fn get_transactions_to_process(
    block: &BitcoinBlockData,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    inscriptions_db_tx: &Transaction,
    ctx: &Context,
) -> (
    Vec<(TransactionIdentifier, usize)>,
    Vec<(TransactionIdentifier, usize)>,
) {
    let mut transactions_ids: Vec<(TransactionIdentifier, usize)> = vec![];
    let mut l1_cache_hits = vec![];

    let mut known_transactions =
        find_all_inscriptions_in_block(&block.block_identifier.index, inscriptions_db_tx, ctx);

    for tx in block.transactions.iter().skip(1) {
        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            let inscription_data = match ordinal_event {
                OrdinalOperation::InscriptionRevealed(inscription_data) => inscription_data,
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

            if let Some(_) = known_transactions.remove(&key) {
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

/// For each input of each transaction in the block, we retrieve the UTXO spent (outpoint_pre_transfer)
/// and we check using a `storage` (in-memory or sqlite) absctraction if we have some existing inscriptions
/// for this entry.
/// When this is the case, it means that an inscription_transfer event needs to be produced. We need to
/// compute the output index (if any) `post_transfer_output` that will now include the inscription.
/// When identifying the output index, we will also need to provide an updated offset for pin pointing
/// the satoshi location.

pub struct SequenceCursor {
    blessed: Option<i64>,
    cursed: Option<i64>,
    inscriptions_db_conn: Connection,
    current_block_height: u64,
}

impl SequenceCursor {
    pub fn new(inscriptions_db_conn: Connection) -> SequenceCursor {
        SequenceCursor {
            blessed: None,
            cursed: None,
            inscriptions_db_conn,
            current_block_height: 0,
        }
    }

    pub fn reset(&mut self) {
        self.blessed = None;
        self.cursed = None;
        self.current_block_height = 0;
    }

    pub fn pick_next(&mut self, cursed: bool, block_height: u64) -> i64 {
        if block_height < self.current_block_height {
            self.reset();
        }
        self.current_block_height = block_height;

        match cursed {
            true => self.pick_next_cursed(),
            false => self.pick_next_blessed(),
        }
    }

    fn pick_next_blessed(&mut self) -> i64 {
        match self.blessed {
            None => {
                match find_latest_inscription_number_at_block_height(
                    &self.current_block_height,
                    &None,
                    &self.inscriptions_db_conn,
                    &Context::empty(),
                ) {
                    Ok(Some(inscription_number)) => {
                        self.blessed = Some(inscription_number);
                        inscription_number + 1
                    }
                    _ => {
                        0
                    }
                }
            }
            Some(value) => value + 1,
        }
    }

    fn pick_next_cursed(&mut self) -> i64 {
        match self.cursed {
            None => {
                match find_latest_cursed_inscription_number_at_block_height(
                    &self.current_block_height,
                    &None,
                    &self.inscriptions_db_conn,
                    &Context::empty(),
                ) {
                    Ok(Some(inscription_number)) => {
                        self.cursed = Some(inscription_number);
                        inscription_number - 1
                    }
                    _ => {
                        -1
                    }
                }
            }
            Some(value) => value - 1,
        }
    }

    pub fn increment_cursed(&mut self) {
        self.cursed = Some(self.pick_next_cursed());
    }

    pub fn increment_blessed(&mut self) {
        self.blessed = Some(self.pick_next_blessed())
    }
}

pub fn augment_block_with_ordinals_inscriptions_data_and_write_to_db_tx(
    block: &mut BitcoinBlockData,
    sequence_cursor: &mut SequenceCursor,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    inscriptions_db_tx: &Transaction,
    ctx: &Context,
) -> bool {
    // Handle re-inscriptions
    let mut reinscriptions_data = HashMap::new();
    for (_, inscription_data) in inscriptions_data.iter() {
        if let Some(inscription_id) = find_blessed_inscription_with_ordinal_number(
            &inscription_data.ordinal_number,
            inscriptions_db_tx,
            ctx,
        ) {
            reinscriptions_data.insert(inscription_data.ordinal_number, inscription_id);
        }
    }

    let any_events = augment_block_with_ordinals_inscriptions_data(
        block,
        sequence_cursor,
        inscriptions_data,
        &reinscriptions_data,
        &ctx,
    );

    // Store inscriptions
    insert_new_inscriptions_from_block_in_inscriptions_and_locations(
        block,
        inscriptions_db_tx,
        ctx,
    );

    any_events
}

pub fn augment_block_with_ordinals_inscriptions_data(
    block: &mut BitcoinBlockData,
    sequence_cursor: &mut SequenceCursor,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    reinscriptions_data: &HashMap<u64, String>,
    ctx: &Context,
) -> bool {
    // Handle sat oveflows
    let mut sats_overflows = VecDeque::new();
    let mut any_event = false;

    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
    };

    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        any_event |= augment_transaction_with_ordinals_inscriptions_data(
            tx,
            tx_index,
            &block.block_identifier,
            sequence_cursor,
            &network,
            inscriptions_data,
            &mut sats_overflows,
            &reinscriptions_data,
            ctx,
        );
    }

    // Handle sats overflow
    while let Some((tx_index, op_index)) = sats_overflows.pop_front() {
        let OrdinalOperation::InscriptionRevealed(ref mut inscription_data) = block.transactions[tx_index].metadata.ordinal_operations[op_index] else {
            continue;
        };
        let is_curse = inscription_data.curse_type.is_some();
        let inscription_number = sequence_cursor.pick_next(is_curse, block.block_identifier.index);
        inscription_data.inscription_number = inscription_number;

        if is_curse {
            sequence_cursor.increment_cursed();
        } else {
            sequence_cursor.increment_blessed();
        };

        ctx.try_log(|logger| {
            info!(
                logger,
                "Unbound inscription {} (#{}) detected on Satoshi {} (block {}, {} transfers)",
                inscription_data.inscription_id,
                inscription_data.inscription_number,
                inscription_data.ordinal_number,
                block.block_identifier.index,
                inscription_data.transfers_pre_inscription,
            );
        });
    }
    any_event
}

pub fn augment_transaction_with_ordinals_inscriptions_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    block_identifier: &BlockIdentifier,
    sequence_cursor: &mut SequenceCursor,
    network: &Network,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    sats_overflows: &mut VecDeque<(usize, usize)>,
    reinscriptions_data: &HashMap<u64, String>,
    ctx: &Context,
) -> bool {
    let any_event = tx.metadata.ordinal_operations.is_empty() == false;
    let mut ordinals_ops_indexes_to_discard = VecDeque::new();

    for (op_index, op) in tx.metadata.ordinal_operations.iter_mut().enumerate() {
        let (mut is_cursed, inscription) = match op {
            OrdinalOperation::InscriptionRevealed(inscription) => {
                (inscription.curse_type.as_ref().is_some(), inscription)
            }
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let transaction_identifier = tx.transaction_identifier.clone();
        let traversal = match inscriptions_data
            .remove(&(transaction_identifier, inscription.inscription_input_index))
        {
            Some(traversal) => traversal,
            None => {
                ctx.try_log(|logger| {
                    error!(
                        logger,
                        "Unable to retrieve cached inscription data for inscription {}",
                        tx.transaction_identifier.hash
                    );
                });
                ordinals_ops_indexes_to_discard.push_front(op_index);
                continue;
            }
        };

        // Do we need to curse the inscription because of re-inscription?
        let mut curse_type_override = None;
        if !is_cursed {
            // Is this inscription re-inscribing an existing blessed inscription?
            if let Some(exisiting_inscription_id) =
                reinscriptions_data.get(&traversal.ordinal_number)
            {
                ctx.try_log(|logger| {
                    info!(
                        logger,
                        "Satoshi #{} was inscribed with blessed inscription {}, cursing inscription {}",
                        traversal.ordinal_number,
                        exisiting_inscription_id,
                        traversal.get_inscription_id(),
                    );
                });

                is_cursed = true;
                inscription_number = sequence_cursor.pick_next(is_cursed, block_identifier.index);
                curse_type_override = Some(OrdinalInscriptionCurseType::Reinscription)
            }
        };

        let mut inscription_number = sequence_cursor.pick_next(is_cursed, block_identifier.index);

        let outputs = &tx.metadata.outputs;
        inscription.inscription_number = inscription_number;
        inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
        inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
        inscription.ordinal_number = traversal.ordinal_number;
        inscription.transfers_pre_inscription = traversal.transfers;
        inscription.inscription_fee = tx.metadata.fee;
        inscription.tx_index = tx_index;
        inscription.curse_type = match curse_type_override {
            Some(curse_type) => Some(curse_type),
            None => inscription.curse_type.take(),
        };
        inscription.satpoint_post_inscription = format_satpoint_to_watch(
            &traversal.transfer_data.transaction_identifier_location,
            traversal.transfer_data.output_index,
            traversal.transfer_data.inscription_offset_intra_output,
        );
        if let Some(output) = outputs.get(traversal.transfer_data.output_index) {
            inscription.inscription_output_value = output.value;
            inscription.inscriber_address = {
                let script_pub_key = output.get_script_pubkey_hex();
                match Script::from_hex(&script_pub_key) {
                    Ok(script) => match Address::from_script(&script, network.clone()) {
                        Ok(a) => Some(a.to_string()),
                        _ => None,
                    },
                    _ => None,
                }
            };
        } else {
            ctx.try_log(|logger| {
                warn!(
                    logger,
                    "Database corrupted, skipping cursed inscription => {:?} / {:?}",
                    traversal,
                    outputs
                );
            });
        }

        if traversal.ordinal_number == 0 {
            // If the satoshi inscribed correspond to a sat overflow, we will store the inscription
            // and assign an inscription number after the other inscriptions, to mimick the
            // bug in ord.
            sats_overflows.push_back((tx_index, op_index));
            continue;
        }

        ctx.try_log(|logger| {
            info!(
                logger,
                "Inscription {} (#{}) detected on Satoshi {} (block {}, {} transfers)",
                inscription.inscription_id,
                inscription.inscription_number,
                inscription.ordinal_number,
                block_identifier.index,
                inscription.transfers_pre_inscription,
            );
        });

        if is_cursed {
            sequence_cursor.increment_cursed();
        } else {
            sequence_cursor.increment_blessed();
        }
    }
    any_event
}

pub fn consolidate_transaction_with_pre_computed_inscription_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    coinbase_txid: &TransactionIdentifier,
    network: &Network,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    _ctx: &Context,
) {
    for operation in tx.metadata.ordinal_operations.iter_mut() {
        let inscription = match operation {
            OrdinalOperation::InscriptionRevealed(ref mut inscription) => inscription,
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let Some(traversal) = inscriptions_data.remove(&(tx.transaction_identifier.clone(), inscription.inscription_input_index)) else {
            continue;
        };

        inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
        inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
        inscription.ordinal_number = traversal.ordinal_number;
        inscription.inscription_number = traversal.inscription_number;
        inscription.transfers_pre_inscription = traversal.transfers;
        inscription.inscription_fee = tx.metadata.fee;
        inscription.tx_index = tx_index;
        inscription.satpoint_post_inscription = format_satpoint_to_watch(
            &traversal.transfer_data.transaction_identifier_location,
            traversal.transfer_data.output_index,
            traversal.transfer_data.inscription_offset_intra_output,
        );

        if inscription.inscription_number < 0 {
            inscription.curse_type = Some(OrdinalInscriptionCurseType::Unknown);
        }

        if traversal.transfer_data.transaction_identifier_location.eq(coinbase_txid) {
            continue;
        }

        if let Some(output) = tx.metadata.outputs.get(traversal.transfer_data.output_index) {
            inscription.inscription_output_value = output.value;
            inscription.inscriber_address = {
                let script_pub_key = output.get_script_pubkey_hex();
                match Script::from_hex(&script_pub_key) {
                    Ok(script) => match Address::from_script(&script, network.clone()) {
                        Ok(a) => Some(a.to_string()),
                        _ => None,
                    },
                    _ => None,
                }
            };
        }

    }
}

pub fn consolidate_block_with_pre_computed_ordinals_data(
    block: &mut BitcoinBlockData,
    inscriptions_db_tx: &Transaction,
    include_transfers: bool,
    ctx: &Context,
) {
    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
    };

    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_txid = &block.transactions[0].transaction_identifier.clone();
    let mut cumulated_fees = 0;
    let mut inscriptions_data =
        find_all_inscriptions_in_block(&block.block_identifier.index, inscriptions_db_tx, ctx);
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        // Add inscriptions data
        consolidate_transaction_with_pre_computed_inscription_data(
            tx,
            tx_index,
            coinbase_txid,
            &network,
            &mut inscriptions_data,
            ctx,
        );

        // Add transfers data
        if include_transfers {
            let _ = augment_transaction_with_ordinals_transfers_data(
                tx,
                tx_index,
                &block.block_identifier,
                &network,
                &coinbase_txid,
                coinbase_subsidy,
                &mut cumulated_fees,
                inscriptions_db_tx,
                ctx,
            );
        }
    }
}
