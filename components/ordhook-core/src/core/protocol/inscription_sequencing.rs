use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    hash::BuildHasherDefault,
    sync::Arc,
};

use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::Network,
    types::{
        BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, BlockIdentifier,
        OrdinalInscriptionCurseType, OrdinalInscriptionNumber,
        OrdinalInscriptionTransferDestination, OrdinalOperation, TransactionIdentifier,
    },
    utils::Context,
};
use crossbeam_channel::unbounded;
use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::{Connection, Transaction};

use crate::{
    core::{
        meta_protocols::brc20::db::{
            augment_transaction_with_brc20_operation_data, get_brc20_operations_on_block,
        },
        resolve_absolute_pointer, OrdhookConfig,
    },
    db::{
        find_blessed_inscription_with_ordinal_number, find_nth_classic_neg_number_at_block_height,
        find_nth_classic_pos_number_at_block_height, find_nth_jubilee_number_at_block_height,
        format_inscription_id, update_ordinals_db_with_block, update_sequence_metadata_with_block,
        TransactionBytesCursor, TraversalResult,
    },
    ord::height::Height,
};

use std::sync::mpsc::channel;

use crate::db::find_all_inscriptions_in_block;

use super::{
    inscription_parsing::get_inscriptions_revealed_in_block,
    satoshi_numbering::compute_satoshi_number,
    satoshi_tracking::{
        augment_transaction_with_ordinals_transfers_data, compute_satpoint_post_transfer,
    },
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
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
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

    let (transactions_ids, l1_cache_hits) =
        get_transactions_to_process(block, cache_l1, inscriptions_db_tx, ctx);

    let has_transactions_to_process = !transactions_ids.is_empty() || !l1_cache_hits.is_empty();

    let thread_pool_capacity = ordhook_config.resources.get_optimal_thread_pool_capacity();

    // Nothing to do? early return
    if !has_transactions_to_process {
        return Ok(false);
    }

    let expected_traversals = transactions_ids.len() + l1_cache_hits.len();
    let (traversal_tx, traversal_rx) = unbounded();

    let mut tx_thread_pool = vec![];
    let mut thread_pool_handles = vec![];

    for thread_index in 0..thread_pool_capacity {
        let (tx, rx) = channel();
        tx_thread_pool.push(tx);

        let moved_traversal_tx = traversal_tx.clone();
        let moved_ctx = inner_ctx.clone();
        let moved_ordhook_db_path = ordhook_config.db_path.clone();
        let ulimit = ordhook_config.resources.ulimit;
        let memory_available = ordhook_config.resources.memory_available;

        let local_cache = cache_l2.clone();

        let handle = hiro_system_kit::thread_named("Worker")
            .spawn(move || {
                while let Ok(Some((
                    transaction_id,
                    block_identifier,
                    input_index,
                    inscription_pointer,
                    prioritary,
                ))) = rx.recv()
                {
                    let traversal: Result<(TraversalResult, u64, _), String> =
                        compute_satoshi_number(
                            &moved_ordhook_db_path,
                            &block_identifier,
                            &transaction_id,
                            input_index,
                            inscription_pointer,
                            &local_cache,
                            ulimit,
                            memory_available,
                            false,
                            &moved_ctx,
                        );
                    let _ = moved_traversal_tx.send((traversal, prioritary, thread_index));
                }
            })
            .expect("unable to spawn thread");
        thread_pool_handles.push(handle);
    }

    // Consume L1 cache: if the traversal was performed in a previous round
    // retrieve it and inject it to the "reduce" worker (by-passing the "map" thread pool)
    let mut round_robin_thread_index = 0;
    for key in l1_cache_hits.iter() {
        if let Some(entry) = cache_l1.get(key) {
            let _ = traversal_tx.send((
                Ok((entry.clone(), key.2, vec![])),
                true,
                round_robin_thread_index,
            ));
            round_robin_thread_index = (round_robin_thread_index + 1) % thread_pool_capacity;
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

    let mut priority_queue = VecDeque::new();
    let mut warmup_queue = VecDeque::new();

    for (transaction_id, input_index, inscription_pointer) in transactions_ids.into_iter() {
        priority_queue.push_back((
            transaction_id,
            block.block_identifier.clone(),
            input_index,
            inscription_pointer,
            true,
        ));
    }

    // Feed each worker from the thread pool with 2 workitems each
    for thread_index in 0..thread_pool_capacity {
        let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
    }
    for thread_index in 0..thread_pool_capacity {
        let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
    }

    let mut next_block_iter = next_blocks.iter();
    let mut traversals_received = 0;
    while let Ok((traversal_result, prioritary, thread_index)) = traversal_rx.recv() {
        if prioritary {
            traversals_received += 1;
        }
        match traversal_result {
            Ok((traversal, inscription_pointer, _)) => {
                inner_ctx.try_log(|logger| {
                    info!(
                        logger,
                        "Completed ordinal number retrieval for Satpoint {}:{}:{} (block: #{}:{}, transfers: {}, progress: {traversals_received}/{expected_traversals}, priority queue: {prioritary}, thread: {thread_index})",
                        traversal.transaction_identifier_inscription.hash,
                        traversal.inscription_input_index,
                        inscription_pointer,
                        traversal.get_ordinal_coinbase_height(),
                        traversal.get_ordinal_coinbase_offset(),
                        traversal.transfers
                        )
                });
                cache_l1.insert(
                    (
                        traversal.transaction_identifier_inscription.clone(),
                        traversal.inscription_input_index,
                        inscription_pointer,
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
                    let (transactions_ids, _) =
                        get_transactions_to_process(next_block, cache_l1, inscriptions_db_tx, ctx);

                    inner_ctx.try_log(|logger| {
                        info!(
                            logger,
                            "Number of inscriptions in block #{} to pre-process: {}",
                            block.block_identifier.index,
                            transactions_ids.len()
                        )
                    });

                    for (transaction_id, input_index, inscription_pointer) in
                        transactions_ids.into_iter()
                    {
                        warmup_queue.push_back((
                            transaction_id,
                            next_block.block_identifier.clone(),
                            input_index,
                            inscription_pointer,
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
            if let Ok((traversal, inscription_pointer, _)) = traversal_result {
                inner_ctx.try_log(|logger| {
                    info!(
                        logger,
                        "Completed ordinal number retrieval for Satpoint {}:{}:{} (block: #{}:{}, transfers: {}, pre-retrieval, thread: {thread_index})",
                        traversal.transaction_identifier_inscription.hash,
                        traversal.inscription_input_index,
                        inscription_pointer,
                        traversal.get_ordinal_coinbase_height(),
                        traversal.get_ordinal_coinbase_offset(),
                        traversal.transfers
                        )
                    });
                cache_l1.insert(
                    (
                        traversal.transaction_identifier_inscription.clone(),
                        traversal.inscription_input_index,
                        inscription_pointer,
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
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    inscriptions_db_tx: &Transaction,
    ctx: &Context,
) -> (
    HashSet<(TransactionIdentifier, usize, u64)>,
    Vec<(TransactionIdentifier, usize, u64)>,
) {
    let mut transactions_ids = HashSet::new();
    let mut l1_cache_hits = vec![];

    let known_transactions =
        find_all_inscriptions_in_block(&block.block_identifier.index, inscriptions_db_tx, ctx);

    for tx in block.transactions.iter().skip(1) {
        let inputs = tx
            .metadata
            .inputs
            .iter()
            .map(|i| i.previous_output.value)
            .collect::<Vec<u64>>();

        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            let inscription_data = match ordinal_event {
                OrdinalOperation::InscriptionRevealed(inscription_data) => inscription_data,
                OrdinalOperation::InscriptionTransferred(_) => {
                    continue;
                }
            };

            let (input_index, relative_offset) = match inscription_data.inscription_pointer {
                Some(pointer) => resolve_absolute_pointer(&inputs, pointer),
                None => (inscription_data.inscription_input_index, 0),
            };

            let key = (
                tx.transaction_identifier.clone(),
                input_index,
                relative_offset,
            );
            if cache_l1.contains_key(&key) {
                l1_cache_hits.push(key);
                continue;
            }

            if let Some(_) = known_transactions.get(&inscription_data.inscription_id) {
                continue;
            }

            if transactions_ids.contains(&key) {
                continue;
            }

            // Enqueue for traversals
            transactions_ids.insert(key);
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
            true => self.pick_next_neg_classic(ctx),
            false => self.pick_next_pos_classic(ctx),
        };

        let jubilee = if block_height >= get_jubilee_block_height(&network) {
            self.pick_next_jubilee_number(ctx)
        } else {
            classic
        };
        OrdinalInscriptionNumber { classic, jubilee }
    }

    fn pick_next_pos_classic(&mut self, ctx: &Context) -> i64 {
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
        match self.jubilee_cursor {
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

    fn pick_next_neg_classic(&mut self, ctx: &Context) -> i64 {
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

    pub fn increment_neg_classic(&mut self, ctx: &Context) {
        self.neg_cursor = Some(self.pick_next_neg_classic(ctx));
    }

    pub fn increment_pos_classic(&mut self, ctx: &Context) {
        self.pos_cursor = Some(self.pick_next_pos_classic(ctx));
    }

    pub fn increment_jubilee_number(&mut self, ctx: &Context) {
        self.jubilee_cursor = Some(self.pick_next_jubilee_number(ctx))
    }
}

pub fn get_jubilee_block_height(network: &Network) -> u64 {
    match network {
        Network::Bitcoin => 824544,
        Network::Regtest => 110,
        Network::Signet => 175392,
        Network::Testnet => 2544192,
        _ => unreachable!(),
    }
}

pub fn get_bitcoin_network(network: &BitcoinNetwork) -> Network {
    match network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
        BitcoinNetwork::Signet => Network::Signet,
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
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
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
    update_ordinals_db_with_block(block, inscriptions_db_tx, ctx);
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
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    reinscriptions_data: &mut HashMap<u64, String>,
    ctx: &Context,
) -> bool {
    // Handle sat oveflows
    let mut sats_overflows = VecDeque::new();
    let mut any_event = false;

    let network = get_bitcoin_network(&block.metadata.network);
    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_txid = &block.transactions[0].transaction_identifier.clone();
    let mut cumulated_fees = 0u64;

    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        any_event |= augment_transaction_with_ordinals_inscriptions_data(
            tx,
            tx_index,
            &block.block_identifier,
            sequence_cursor,
            &network,
            inscriptions_data,
            coinbase_txid,
            coinbase_subsidy,
            &mut cumulated_fees,
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

        sequence_cursor.increment_jubilee_number(ctx);
        if is_curse {
            sequence_cursor.increment_neg_classic(ctx);
        } else {
            sequence_cursor.increment_pos_classic(ctx);
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
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    coinbase_txid: &TransactionIdentifier,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    sats_overflows: &mut VecDeque<(usize, usize)>,
    reinscriptions_data: &mut HashMap<u64, String>,
    ctx: &Context,
) -> bool {
    let inputs = tx
        .metadata
        .inputs
        .iter()
        .map(|i| i.previous_output.value)
        .collect::<Vec<u64>>();

    let any_event = tx.metadata.ordinal_operations.is_empty() == false;
    let mut mutated_operations = vec![];
    mutated_operations.append(&mut tx.metadata.ordinal_operations);
    let mut inscription_subindex = 0;
    for (op_index, op) in mutated_operations.iter_mut().enumerate() {
        let (mut is_cursed, inscription) = match op {
            OrdinalOperation::InscriptionRevealed(inscription) => {
                (inscription.curse_type.as_ref().is_some(), inscription)
            }
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let (input_index, relative_offset) = match inscription.inscription_pointer {
            Some(pointer) => resolve_absolute_pointer(&inputs, pointer),
            None => (inscription.inscription_input_index, 0),
        };

        let transaction_identifier = tx.transaction_identifier.clone();
        let inscription_id = format_inscription_id(&transaction_identifier, inscription_subindex);
        let traversal =
            match inscriptions_data.get(&(transaction_identifier, input_index, relative_offset)) {
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

        let (destination, satpoint_post_transfer, output_value) = compute_satpoint_post_transfer(
            &&*tx,
            tx_index,
            input_index,
            relative_offset,
            network,
            coinbase_txid,
            coinbase_subsidy,
            cumulated_fees,
            ctx,
        );

        // Compute satpoint_post_inscription
        inscription.satpoint_post_inscription = satpoint_post_transfer;
        inscription_subindex += 1;

        match destination {
            OrdinalInscriptionTransferDestination::SpentInFees => {
                // Inscriptions are assigned inscription numbers starting at zero, first by the
                // order reveal transactions appear in blocks, and the order that reveal envelopes
                // appear in those transactions.
                // Due to a historical bug in `ord` which cannot be fixed without changing a great
                // many inscription numbers, inscriptions which are revealed and then immediately
                // spent to fees are numbered as if they appear last in the block in which they
                // are revealed.
                sats_overflows.push_back((tx_index, op_index));
                continue;
            }
            OrdinalInscriptionTransferDestination::Burnt(_) => {}
            OrdinalInscriptionTransferDestination::Transferred(address) => {
                inscription.inscription_output_value = output_value.unwrap_or(0);
                inscription.inscriber_address = Some(address);
            }
        };

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

        sequence_cursor.increment_jubilee_number(ctx);
        if is_cursed {
            sequence_cursor.increment_neg_classic(ctx);
        } else {
            sequence_cursor.increment_pos_classic(ctx);
        }
    }
    tx.metadata
        .ordinal_operations
        .append(&mut mutated_operations);

    any_event
}

/// Best effort to re-augment a `BitcoinTransactionData` with data coming from `inscriptions` and `locations` tables.
/// Some informations are being lost (curse_type).
fn consolidate_transaction_with_pre_computed_inscription_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    coinbase_txid: &TransactionIdentifier,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    network: &Network,
    inscriptions_data: &mut BTreeMap<String, TraversalResult>,
    ctx: &Context,
) {
    let mut subindex = 0;
    let mut mutated_operations = vec![];
    mutated_operations.append(&mut tx.metadata.ordinal_operations);

    let inputs = tx
        .metadata
        .inputs
        .iter()
        .map(|i| i.previous_output.value)
        .collect::<Vec<u64>>();

    for operation in mutated_operations.iter_mut() {
        let inscription = match operation {
            OrdinalOperation::InscriptionRevealed(ref mut inscription) => inscription,
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let inscription_id = format_inscription_id(&tx.transaction_identifier, subindex);
        let Some(traversal) = inscriptions_data.get(&inscription_id) else {
            // Should we remove the operation instead
            continue;
        };
        subindex += 1;

        inscription.inscription_id = inscription_id.clone();
        inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
        inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
        inscription.ordinal_number = traversal.ordinal_number;
        inscription.inscription_number = traversal.inscription_number.clone();
        inscription.transfers_pre_inscription = traversal.transfers;
        inscription.inscription_fee = tx.metadata.fee;
        inscription.tx_index = tx_index;

        let (input_index, relative_offset) = match inscription.inscription_pointer {
            Some(pointer) => resolve_absolute_pointer(&inputs, pointer),
            None => (traversal.inscription_input_index, 0),
        };
        // Compute satpoint_post_inscription
        let (destination, satpoint_post_transfer, output_value) = compute_satpoint_post_transfer(
            tx,
            tx_index,
            input_index,
            relative_offset,
            network,
            coinbase_txid,
            coinbase_subsidy,
            cumulated_fees,
            ctx,
        );

        inscription.satpoint_post_inscription = satpoint_post_transfer;

        if inscription.inscription_number.classic < 0 {
            inscription.curse_type = Some(OrdinalInscriptionCurseType::Generic);
        }

        match destination {
            OrdinalInscriptionTransferDestination::SpentInFees => continue,
            OrdinalInscriptionTransferDestination::Burnt(_) => continue,
            OrdinalInscriptionTransferDestination::Transferred(address) => {
                inscription.inscription_output_value = output_value.unwrap_or(0);
                inscription.inscriber_address = Some(address);
            }
        }
    }
    tx.metadata
        .ordinal_operations
        .append(&mut mutated_operations);
}

/// Best effort to re-augment a `BitcoinBlockData` with data coming from `inscriptions` and `locations` tables.
/// Some informations are being lost (curse_type).
pub fn consolidate_block_with_pre_computed_ordinals_data(
    block: &mut BitcoinBlockData,
    inscriptions_db_tx: &Transaction,
    include_transfers: bool,
    brc20_db_conn: Option<&Connection>,
    ctx: &Context,
) {
    let network = get_bitcoin_network(&block.metadata.network);
    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_txid = &block.transactions[0].transaction_identifier.clone();
    let mut cumulated_fees = 0;
    let expected_inscriptions_count = get_inscriptions_revealed_in_block(&block).len();
    let mut inscriptions_data = loop {
        let results =
            find_all_inscriptions_in_block(&block.block_identifier.index, inscriptions_db_tx, ctx);
        // TODO: investigate, sporadically the set returned is empty, and requires a retry.
        if results.len() != expected_inscriptions_count {
            ctx.try_log(|logger| {
                warn!(
                    logger,
                    "Database retuning {} results instead of the expected {expected_inscriptions_count}",
                    results.len()
                );
            });
            continue;
        }
        break results;
    };
    let mut brc20_token_map = HashMap::new();
    let mut brc20_block_ledger_map = match brc20_db_conn {
        Some(conn) => get_brc20_operations_on_block(&block.block_identifier, &conn, &ctx),
        None => HashMap::new(),
    };
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        // Add inscriptions data
        consolidate_transaction_with_pre_computed_inscription_data(
            tx,
            tx_index,
            coinbase_txid,
            coinbase_subsidy,
            &mut cumulated_fees,
            &network,
            &mut inscriptions_data,
            ctx,
        );

        // Add transfers data
        if include_transfers {
            let _ = augment_transaction_with_ordinals_transfers_data(
                tx,
                tx_index,
                &network,
                &coinbase_txid,
                coinbase_subsidy,
                &mut cumulated_fees,
                inscriptions_db_tx,
                ctx,
            );
        }
        if let Some(brc20_db_conn) = brc20_db_conn {
            augment_transaction_with_brc20_operation_data(
                tx,
                &mut brc20_token_map,
                &mut brc20_block_ledger_map,
                &brc20_db_conn,
                &ctx,
            );
        }
    }
}
