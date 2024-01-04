use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    hash::BuildHasherDefault,
    sync::Arc,
};

use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{Address, Network, ScriptBuf},
    types::{
        BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, BlockIdentifier,
        OrdinalInscriptionCurseType, OrdinalInscriptionNumber, OrdinalOperation,
        TransactionIdentifier,
    },
    utils::Context,
};
use crossbeam_channel::unbounded;
use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::{Connection, Transaction};

use crate::{
    core::OrdhookConfig,
    db::{
        find_blessed_inscription_with_ordinal_number, find_nth_classic_neg_number_at_block_height,
        find_nth_classic_pos_number_at_block_height, find_nth_jubilee_number_at_block_height,
        format_inscription_id, format_satpoint_to_watch, update_inscriptions_with_block,
        update_sequence_metadata_with_block, TransactionBytesCursor, TraversalResult,
    },
    ord::height::Height,
};

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::mpsc::channel;

use crate::db::find_all_inscriptions_in_block;

use super::{
    inscription_parsing::get_inscriptions_revealed_in_block,
    inscription_tracking::augment_transaction_with_ordinals_transfers_data,
    satoshi_numbering::compute_satoshi_number,
};

/// Parallelize the computation of ordinals numbers for inscriptions present in a block.
///
/// This function will:
/// 1) Limit the number of ordinals numbers to compute by filtering out all the ordinals numbers  pre-computed
/// and present in the L1 cache.
/// 2) Create a threadpool, by spawning as many threads as specified by the config to process the batch ordinals to
/// retrieve
/// 3) Consume eventual entries in cache L1
/// 4) Inject the ordinals to compute (random order) in a priority queue
/// via the command line).
/// 5) Keep injecting ordinals from next blocks (if any) as long as the ordinals from the current block are not all
/// computed and augment the cache L1 for future blocks.
///
/// If the block has already been computed in the past (so presence of ordinals number present in the `inscriptions` db)
/// the transaction is removed from the set to compute, and not injected in L1 either.
/// This behaviour should be refined.
///
/// # Panics
/// - unability to spawn threads
///
/// # Todos / Optimizations
/// - Pre-computed entries are being consumed from L1, and then re-injected in L1, which is wasting a bunch of cycles.
///
pub fn parallelize_inscription_data_computations(
    block: &BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_tx: &Transaction,
    ordhook_config: &OrdhookConfig,
    ctx: &Context,
) -> Result<bool, String> {
    let inner_ctx = if ordhook_config.logs.ordinals_internals {
        ctx.clone()
    } else {
        Context::empty()
    };

    inner_ctx.try_log(|logger| {
        info!(
            logger,
            "Inscriptions data computation for block #{} started", block.block_identifier.index
        )
    });

    cache_l1.clear();
    let (mut transactions_ids, l1_cache_hits) =
        get_transactions_to_process(block, cache_l1, inscriptions_db_tx, ctx);

    let has_transactions_to_process = !transactions_ids.is_empty() || !l1_cache_hits.is_empty();

    let thread_max = ordhook_config.ingestion_thread_max;

    // Nothing to do? early return
    if !has_transactions_to_process {
        return Ok(false);
    }

    let expected_traversals = transactions_ids.len() + l1_cache_hits.len();
    let (traversal_tx, traversal_rx) = unbounded();

    let mut tx_thread_pool = vec![];
    let mut thread_pool_handles = vec![];

    for thread_index in 0..thread_max {
        let (tx, rx) = channel();
        tx_thread_pool.push(tx);

        let moved_traversal_tx = traversal_tx.clone();
        let moved_ctx = inner_ctx.clone();
        let moved_ordhook_db_path = ordhook_config.db_path.clone();
        let local_cache = cache_l2.clone();

        let handle = hiro_system_kit::thread_named("Worker")
            .spawn(move || {
                while let Ok(Some((transaction_id, block_identifier, input_index, prioritary))) =
                    rx.recv()
                {
                    let traversal: Result<(TraversalResult, _), String> = compute_satoshi_number(
                        &moved_ordhook_db_path,
                        &block_identifier,
                        &transaction_id,
                        input_index,
                        &local_cache,
                        false,
                        &moved_ctx,
                    );
                    let _ = moved_traversal_tx.send((traversal, prioritary, thread_index));
                }
            })
            .expect("unable to spawn thread");
        thread_pool_handles.push(handle);
    }

    // Consume L1 cache
    let mut thread_index = 0;
    for key in l1_cache_hits.iter() {
        if let Some(entry) = cache_l1.remove(key) {
            let _ = traversal_tx.send((Ok((entry, vec![])), true, thread_index));
            thread_index = (thread_index + 1) % thread_max;
        }
    }

    let next_block_heights = next_blocks
        .iter()
        .map(|b| format!("{}", b.block_identifier.index))
        .collect::<Vec<_>>();

    inner_ctx.try_log(|logger| {
        info!(
            logger,
            "Number of inscriptions in block #{} to process: {} (L1 cache hits: {}, queue: [{}], L1 cache len: {}, L2 cache len: {})",
            block.block_identifier.index,
            transactions_ids.len(),
            l1_cache_hits.len(),
            next_block_heights.join(", "),
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
            Ok((traversal, _)) => {
                inner_ctx.try_log(|logger| {
                    info!(
                        logger,
                        "Completed ordinal number retrieval for Satpoint {}:{}:0 (block: #{}:{}, transfers: {}, progress: {traversals_received}/{expected_traversals}, priority queue: {prioritary}, thread: {thread_index})",
                        traversal.transaction_identifier_inscription.hash,
                        traversal.inscription_input_index,
                        traversal.get_ordinal_coinbase_height(),
                        traversal.get_ordinal_coinbase_offset(),
                        traversal.transfers
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
                    let (mut transactions_ids, _) =
                        get_transactions_to_process(next_block, cache_l1, inscriptions_db_tx, ctx);

                    inner_ctx.try_log(|logger| {
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
    inner_ctx.try_log(|logger| {
        info!(
            logger,
            "Inscriptions data computation for block #{} collected", block.block_identifier.index
        )
    });

    // Collect eventual results for incoming blocks
    for tx in tx_thread_pool.iter() {
        // Empty the queue
        if let Ok((traversal_result, _prioritary, thread_index)) = traversal_rx.try_recv() {
            if let Ok((traversal, _)) = traversal_result {
                inner_ctx.try_log(|logger| {
                    info!(
                        logger,
                        "Completed ordinal number retrieval for Satpoint {}:{}:0 (block: #{}:{}, transfers: {}, pre-retrieval, thread: {thread_index})",
                        traversal.transaction_identifier_inscription.hash,
                        traversal.inscription_input_index,
                        traversal.get_ordinal_coinbase_height(),
                        traversal.get_ordinal_coinbase_offset(),
                        traversal.transfers
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

    let ctx_moved = inner_ctx.clone();
    let _ = hiro_system_kit::thread_named("Garbage collection").spawn(move || {
        ctx_moved.try_log(|logger| info!(logger, "Cleanup: threadpool deallocation started",));

        for handle in thread_pool_handles.into_iter() {
            let _ = handle.join();
        }
        ctx_moved.try_log(|logger| info!(logger, "Cleanup: threadpool deallocation ended",));
    });

    inner_ctx.try_log(|logger| {
        info!(
            logger,
            "Inscriptions data computation for block #{} ended", block.block_identifier.index
        )
    });

    Ok(has_transactions_to_process)
}

/// Given a block, a cache L1, and a readonly DB connection, returns a tuple with the transactions that must be included
/// for ordinals computation and the list of transactions where we have a cache hit.
///
/// This function will:
/// 1) Retrieve all the eventual inscriptions previously stored in DB for the block  
/// 2) Traverse the list of transaction present in the block (except coinbase).
/// 3) Check if the transaction is present in the cache L1 and augment the cache hit list accordingly and move on to the
/// next transaction.
/// 4) Check if the transaction was processed in the pastand move on to the next transaction.
/// 5) Augment the list of transaction to process.
///
/// # Todos / Optimizations
/// - DB query (inscriptions + locations) could be expensive.
///
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

/// Helper caching inscription sequence cursor
///
/// When attributing an inscription number to a new inscription, retrieving the next inscription number to use (both for
/// blessed and cursed sequence) is an expensive operation, challenging to optimize from a SQL point of view.
/// This structure is wrapping the expensive SQL query and helping us keeping track of the next inscription number to
/// use.
///
pub struct SequenceCursor<'a> {
    pos_cursor: Option<i64>,
    neg_cursor: Option<i64>,
    jubilee_cursor: Option<i64>,
    inscriptions_db_conn: &'a Connection,
    current_block_height: u64,
}

impl<'a> SequenceCursor<'a> {
    pub fn new(inscriptions_db_conn: &'a Connection) -> SequenceCursor<'a> {
        SequenceCursor {
            jubilee_cursor: None,
            pos_cursor: None,
            neg_cursor: None,
            inscriptions_db_conn,
            current_block_height: 0,
        }
    }

    pub fn reset(&mut self) {
        self.pos_cursor = None;
        self.neg_cursor = None;
        self.jubilee_cursor = None;
        self.current_block_height = 0;
    }

    pub fn pick_next(
        &mut self,
        cursed: bool,
        block_height: u64,
        network: &Network,
        ctx: &Context,
    ) -> OrdinalInscriptionNumber {
        if block_height < self.current_block_height {
            self.reset();
        }
        self.current_block_height = block_height;

        let classic = match cursed {
            true => self.pick_next_neg_number(ctx),
            false => self.pick_next_pos_number(ctx),
        };
        let jubilee_height = match network {
            Network::Bitcoin => 824544,
            Network::Regtest => 110,
            Network::Signet => 175392,
            Network::Testnet => 2544192,
            _ => unreachable!(),
        };
        let jubilee = if block_height >= jubilee_height {
            self.pick_next_jubilee_number(ctx)
        } else {
            classic
        };
        OrdinalInscriptionNumber { classic, jubilee }
    }

    fn pick_next_pos_number(&mut self, ctx: &Context) -> i64 {
        match self.pos_cursor {
            None => {
                match find_nth_classic_pos_number_at_block_height(
                    &self.current_block_height,
                    &self.inscriptions_db_conn,
                    &ctx,
                ) {
                    Some(inscription_number) => {
                        self.pos_cursor = Some(inscription_number);
                        inscription_number + 1
                    }
                    _ => 0,
                }
            }
            Some(value) => value + 1,
        }
    }

    fn pick_next_jubilee_number(&mut self, ctx: &Context) -> i64 {
        match self.pos_cursor {
            None => {
                match find_nth_jubilee_number_at_block_height(
                    &self.current_block_height,
                    &self.inscriptions_db_conn,
                    &ctx,
                ) {
                    Some(inscription_number) => {
                        self.jubilee_cursor = Some(inscription_number);
                        inscription_number + 1
                    }
                    _ => 0,
                }
            }
            Some(value) => value + 1,
        }
    }

    fn pick_next_neg_number(&mut self, ctx: &Context) -> i64 {
        match self.neg_cursor {
            None => {
                match find_nth_classic_neg_number_at_block_height(
                    &self.current_block_height,
                    &self.inscriptions_db_conn,
                    &ctx,
                ) {
                    Some(inscription_number) => {
                        self.neg_cursor = Some(inscription_number);
                        inscription_number - 1
                    }
                    _ => -1,
                }
            }
            Some(value) => value - 1,
        }
    }

    pub fn increment_neg_cursor(&mut self, ctx: &Context) {
        self.neg_cursor = Some(self.pick_next_neg_number(ctx));
    }

    pub fn increment_pos_number(&mut self, ctx: &Context) {
        self.pos_cursor = Some(self.pick_next_pos_number(ctx))
    }

    pub fn increment_jubilee_number(&mut self, ctx: &Context) {
        self.jubilee_cursor = Some(self.pick_next_jubilee_number(ctx))
    }
}

/// Given a `BitcoinBlockData` that have been augmented with the functions `parse_inscriptions_in_raw_tx`, `parse_inscriptions_in_standardized_tx`
/// or `parse_inscriptions_and_standardize_block`, mutate the ordinals drafted informations with actual, consensus data.
///
/// This function will write the updated informations to the Sqlite transaction (`inscriptions` and `locations` tables),
/// but is leaving the responsibility to the caller to commit the transaction.
///
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
        if inscription_data.ordinal_number != 0 {
            if let Some(inscription_id) = find_blessed_inscription_with_ordinal_number(
                &inscription_data.ordinal_number,
                inscriptions_db_tx,
                ctx,
            ) {
                reinscriptions_data.insert(inscription_data.ordinal_number, inscription_id);
            }
        }
    }

    let any_events = augment_block_with_ordinals_inscriptions_data(
        block,
        sequence_cursor,
        inscriptions_data,
        &mut reinscriptions_data,
        &ctx,
    );

    // Store inscriptions
    update_inscriptions_with_block(block, inscriptions_db_tx, ctx);
    update_sequence_metadata_with_block(block, inscriptions_db_tx, ctx);
    any_events
}

/// Given a `BitcoinBlockData` that have been augmented with the functions `parse_inscriptions_in_raw_tx`, `parse_inscriptions_in_standardized_tx`
/// or `parse_inscriptions_and_standardize_block`, mutate the ordinals drafted informations with actual, consensus data,
/// by using informations from `inscription_data` and `reinscription_data`.
///
/// This function is responsible for handling the sats overflow / unbound inscription case.
/// https://github.com/ordinals/ord/issues/2062
///
/// The block is in a correct state from a consensus point of view after the execution of this function.
pub fn augment_block_with_ordinals_inscriptions_data(
    block: &mut BitcoinBlockData,
    sequence_cursor: &mut SequenceCursor,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    reinscriptions_data: &mut HashMap<u64, String>,
    ctx: &Context,
) -> bool {
    // Handle sat oveflows
    let mut sats_overflows = VecDeque::new();
    let mut any_event = false;

    let network = match block.metadata.network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
        BitcoinNetwork::Signet => Network::Signet,
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
            reinscriptions_data,
            ctx,
        );
    }

    // Handle sats overflow
    while let Some((tx_index, op_index)) = sats_overflows.pop_front() {
        let OrdinalOperation::InscriptionRevealed(ref mut inscription_data) =
            block.transactions[tx_index].metadata.ordinal_operations[op_index]
        else {
            continue;
        };
        let is_curse = inscription_data.curse_type.is_some();
        let inscription_number =
            sequence_cursor.pick_next(is_curse, block.block_identifier.index, &network, &ctx);
        inscription_data.inscription_number = inscription_number;

        if is_curse {
            sequence_cursor.increment_neg_cursor(ctx);
        } else {
            sequence_cursor.increment_pos_number(ctx);
        };

        ctx.try_log(|logger| {
            info!(
                logger,
                "Unbound inscription {} (#{}) detected on Satoshi {} (block #{}, {} transfers)",
                inscription_data.inscription_id,
                inscription_data.get_inscription_number(),
                inscription_data.ordinal_number,
                block.block_identifier.index,
                inscription_data.transfers_pre_inscription,
            );
        });
    }
    any_event
}

/// Given a `BitcoinTransactionData` that have been augmented with the functions `parse_inscriptions_in_raw_tx` or
/// `parse_inscriptions_in_standardized_tx`,  mutate the ordinals drafted informations with actual, consensus data, by
/// using informations from `inscription_data` and `reinscription_data`.
///
/// Transactions are not fully correct from a consensus point of view state transient state after the execution of this
/// function.
fn augment_transaction_with_ordinals_inscriptions_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    block_identifier: &BlockIdentifier,
    sequence_cursor: &mut SequenceCursor,
    network: &Network,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize), TraversalResult>,
    sats_overflows: &mut VecDeque<(usize, usize)>,
    reinscriptions_data: &mut HashMap<u64, String>,
    ctx: &Context,
) -> bool {
    let any_event = tx.metadata.ordinal_operations.is_empty() == false;
    let mut inscription_subindex = 0;
    for (op_index, op) in tx.metadata.ordinal_operations.iter_mut().enumerate() {
        let (mut is_cursed, inscription) = match op {
            OrdinalOperation::InscriptionRevealed(inscription) => {
                (inscription.curse_type.as_ref().is_some(), inscription)
            }
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let transaction_identifier = tx.transaction_identifier.clone();
        let inscription_id = format_inscription_id(&transaction_identifier, inscription_subindex);
        let traversal = match inscriptions_data
            .get(&(transaction_identifier, inscription.inscription_input_index))
        {
            Some(traversal) => traversal,
            None => {
                let err_msg = format!(
                    "Unable to retrieve backward traversal result for inscription {}",
                    tx.transaction_identifier.hash
                );
                ctx.try_log(|logger| {
                    error!(logger, "{}", err_msg);
                });
                std::process::exit(1);
            }
        };

        // Do we need to curse the inscription?
        let mut inscription_number =
            sequence_cursor.pick_next(is_cursed, block_identifier.index, network, ctx);
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
                inscription_number =
                    sequence_cursor.pick_next(is_cursed, block_identifier.index, network, ctx);
                curse_type_override = Some(OrdinalInscriptionCurseType::Reinscription)
            }
        };

        inscription.inscription_id = inscription_id;
        inscription.inscription_number = inscription_number;
        let outputs = &tx.metadata.outputs;
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
                match ScriptBuf::from_hex(&script_pub_key) {
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

        // The reinscriptions_data needs to be augmented as we go, to handle transaction chaining.
        if !is_cursed {
            reinscriptions_data.insert(traversal.ordinal_number, traversal.get_inscription_id());
        }

        ctx.try_log(|logger| {
            info!(
                logger,
                "Inscription {} (#{}) detected on Satoshi {} (block #{}, {} transfers)",
                inscription.inscription_id,
                inscription.get_inscription_number(),
                inscription.ordinal_number,
                block_identifier.index,
                inscription.transfers_pre_inscription,
            );
        });

        if is_cursed {
            sequence_cursor.increment_neg_cursor(ctx);
        } else {
            sequence_cursor.increment_pos_number(ctx);
        }
        inscription_subindex += 1;
    }
    any_event
}

/// Best effort to re-augment a `BitcoinTransactionData` with data coming from `inscriptions` and `locations` tables.
/// Some informations are being lost (curse_type).
fn consolidate_transaction_with_pre_computed_inscription_data(
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

        let Some(traversal) = inscriptions_data.remove(&(
            tx.transaction_identifier.clone(),
            inscription.inscription_input_index,
        )) else {
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

        if inscription.inscription_number.classic < 0 {
            inscription.curse_type = Some(OrdinalInscriptionCurseType::Generic);
        }

        if traversal
            .transfer_data
            .transaction_identifier_location
            .eq(coinbase_txid)
        {
            continue;
        }

        if let Some(output) = tx
            .metadata
            .outputs
            .get(traversal.transfer_data.output_index)
        {
            inscription.inscription_output_value = output.value;
            inscription.inscriber_address = {
                let script_pub_key = output.get_script_pubkey_hex();
                match ScriptBuf::from_hex(&script_pub_key) {
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

/// Best effort to re-augment a `BitcoinBlockData` with data coming from `inscriptions` and `locations` tables.
/// Some informations are being lost (curse_type).
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
        BitcoinNetwork::Signet => Network::Signet,
    };

    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_txid = &block.transactions[0].transaction_identifier.clone();
    let mut cumulated_fees = 0;
    let expected_inscriptions_count = get_inscriptions_revealed_in_block(&block).len();
    let mut inscriptions_data = loop {
        let results =
            find_all_inscriptions_in_block(&block.block_identifier.index, inscriptions_db_tx, ctx);
        if results.len() == expected_inscriptions_count {
            break results;
        }
        ctx.try_log(|logger| {
            warn!(
                logger,
                "Database retuning {} results instead of the expected {expected_inscriptions_count}",
                results.len()
            );
        });
    };
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
