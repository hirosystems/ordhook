use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    hash::BuildHasherDefault,
    sync::{mpsc::channel, Arc},
};

use bitcoin::Network;
use bitcoind::{
    try_debug, try_error, try_info,
    types::{
        BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, BlockIdentifier,
        OrdinalInscriptionCurseType, OrdinalInscriptionTransferDestination, OrdinalOperation,
        TransactionBytesCursor, TransactionIdentifier,
    },
    utils::Context,
};
use config::Config;
use crossbeam_channel::unbounded;
use dashmap::DashMap;
use deadpool_postgres::Transaction;
use fxhash::FxHasher;
use ord::{charm::Charm, sat::Sat};

use super::{
    satoshi_numbering::{compute_satoshi_number, TraversalResult},
    satoshi_tracking::compute_satpoint_post_transfer,
    sequence_cursor::SequenceCursor,
};
use crate::{
    core::{protocol::satoshi_tracking::UNBOUND_INSCRIPTION_SATPOINT, resolve_absolute_pointer},
    db::{self, ordinals_pg},
    utils::format_inscription_id,
};

/// Parallelize the computation of ordinals numbers for inscriptions present in a block.
///
/// This function will:
/// 1) Limit the number of ordinals numbers to compute by filtering out all the ordinals numbers  pre-computed
///     and present in the L1 cache.
/// 2) Create a threadpool, by spawning as many threads as specified by the config to process the batch ordinals to
///     retrieve the ordinal number.
/// 3) Consume eventual entries in cache L1
/// 4) Inject the ordinals to compute (random order) in a priority queue
///     via the command line).
/// 5) Keep injecting ordinals from next blocks (if any) as long as the ordinals from the current block are not all
///     computed and augment the cache L1 for future blocks.
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
    next_blocks: &[BitcoinBlockData],
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    config: &Config,
    ctx: &Context,
) -> Result<bool, String> {
    let inner_ctx = ctx.clone();
    let block_height = block.block_identifier.index;

    try_debug!(
        inner_ctx,
        "Inscriptions data computation for block #{block_height} started"
    );

    let (transactions_ids, l1_cache_hits) = get_transactions_to_process(block, cache_l1);
    let has_transactions_to_process = !transactions_ids.is_empty() || !l1_cache_hits.is_empty();
    if !has_transactions_to_process {
        try_debug!(
            inner_ctx,
            "No reveal transactions found at block #{block_height}"
        );
        return Ok(false);
    }

    let expected_traversals = transactions_ids.len() + l1_cache_hits.len();
    let (traversal_tx, traversal_rx) = unbounded();

    let mut tx_thread_pool = vec![];
    let mut thread_pool_handles = vec![];
    let blocks_db = Arc::new(db::blocks::open_blocks_db_with_retry(false, config, ctx));

    let thread_pool_capacity = config.resources.get_optimal_thread_pool_capacity();
    for thread_index in 0..thread_pool_capacity {
        let (tx, rx) = channel();
        tx_thread_pool.push(tx);

        let moved_traversal_tx = traversal_tx.clone();
        let moved_ctx = inner_ctx.clone();
        let moved_config = config.clone();

        let local_cache = cache_l2.clone();
        let local_db = blocks_db.clone();

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
                            &block_identifier,
                            &transaction_id,
                            input_index,
                            inscription_pointer,
                            &local_cache,
                            &local_db,
                            &moved_config,
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

    try_debug!(
        inner_ctx,
        "Number of inscriptions in block #{block_height} to process: {num_inscriptions} \
        (L1 cache hits: {l1_hits}, queue: [{next_heights}], L1 cache len: {l1_len}, L2 cache len: {l2_len})",
        num_inscriptions = transactions_ids.len(),
        l1_hits = l1_cache_hits.len(),
        next_heights = next_block_heights.join(", "),
        l1_len = cache_l1.len(),
        l2_len = cache_l2.len(),
    );

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
    for thread in &tx_thread_pool {
        let _ = thread.send(priority_queue.pop_front());
    }
    for thread in &tx_thread_pool {
        let _ = thread.send(priority_queue.pop_front());
    }

    let mut next_block_iter = next_blocks.iter();
    let mut traversals_received = 0;
    while let Ok((traversal_result, prioritary, thread_index)) = traversal_rx.recv() {
        if prioritary {
            traversals_received += 1;
        }
        match traversal_result {
            Ok((traversal, inscription_pointer, _)) => {
                try_debug!(
                    inner_ctx,
                    "Completed ordinal number retrieval for Satpoint {tx_hash}:{input_index}:{inscription_pointer} \
                    (block: #{coinbase_height}:{coinbase_offset}, transfers: {transfers}, \
                    progress: {traversals_received}/{expected_traversals}, priority queue: {prioritary}, thread: {thread_index})",
                    tx_hash = &traversal.transaction_identifier_inscription.hash,
                    input_index = traversal.inscription_input_index,
                    coinbase_height = traversal.get_ordinal_coinbase_height(),
                    coinbase_offset = traversal.get_ordinal_coinbase_offset(),
                    transfers = traversal.transfers
                );
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
                try_error!(inner_ctx, "Unable to compute inscription's Satoshi: {e}");
            }
        }

        if traversals_received == expected_traversals {
            break;
        }

        if let Some(w) = priority_queue.pop_front() {
            let _ = tx_thread_pool[thread_index].send(Some(w));
        } else if let Some(w) = warmup_queue.pop_front() {
            let _ = tx_thread_pool[thread_index].send(Some(w));
        } else if let Some(next_block) = next_block_iter.next() {
            let (transactions_ids, _) = get_transactions_to_process(next_block, cache_l1);

            try_info!(
                inner_ctx,
                "Number of inscriptions in block #{block_height} to pre-process: {num_inscriptions}",
                num_inscriptions = transactions_ids.len()
            );

            for (transaction_id, input_index, inscription_pointer) in transactions_ids.into_iter() {
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
    try_debug!(
        inner_ctx,
        "Inscriptions data computation for block #{block_height} collected"
    );

    // Collect eventual results for incoming blocks
    for tx in tx_thread_pool.iter() {
        // Empty the queue
        if let Ok((Ok((traversal, inscription_pointer, _)), _prioritary, thread_index)) =
            traversal_rx.try_recv()
        {
            try_debug!(
                inner_ctx,
                "Completed ordinal number retrieval for Satpoint {tx_hash}:{input_index}:{inscription_pointer} \
                (block: #{coinbase_height}:{coinbase_offset}, transfers: {transfers}, pre-retrieval, thread: {thread_index})",
                tx_hash = &traversal.transaction_identifier_inscription.hash,
                input_index = traversal.inscription_input_index,
                coinbase_height = traversal.get_ordinal_coinbase_height(),
                coinbase_offset = traversal.get_ordinal_coinbase_offset(),
                transfers = traversal.transfers
            );
            cache_l1.insert(
                (
                    traversal.transaction_identifier_inscription.clone(),
                    traversal.inscription_input_index,
                    inscription_pointer,
                ),
                traversal,
            );
        }
        let _ = tx.send(None);
    }

    let _ = hiro_system_kit::thread_named("Garbage collection").spawn(move || {
        for handle in thread_pool_handles.into_iter() {
            let _ = handle.join();
        }
    });

    try_debug!(
        inner_ctx,
        "Inscriptions data computation for block #{block_height} ended"
    );

    Ok(has_transactions_to_process)
}

/// Given a block, a cache L1, and a readonly DB connection, returns a tuple with the transactions that must be included
/// for ordinals computation and the list of transactions where we have a cache hit.
///
/// This function will:
/// 1) Retrieve all the eventual inscriptions previously stored in DB for the block  
/// 2) Traverse the list of transaction present in the block (except coinbase).
/// 3) Check if the transaction is present in the cache L1 and augment the cache hit list accordingly and move on to the
///     next transaction.
/// 4) Check if the transaction was processed in the pastand move on to the next transaction.
/// 5) Augment the list of transaction to process.
///
/// # Todos / Optimizations
/// - DB query (inscriptions + locations) could be expensive.
///
fn get_transactions_to_process(
    block: &BitcoinBlockData,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
) -> (
    HashSet<(TransactionIdentifier, usize, u64)>,
    Vec<(TransactionIdentifier, usize, u64)>,
) {
    let mut transactions_ids = HashSet::new();
    let mut l1_cache_hits = vec![];

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

            if transactions_ids.contains(&key) {
                continue;
            }

            // Enqueue for traversals
            transactions_ids.insert(key);
        }
    }
    (transactions_ids, l1_cache_hits)
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

/// Given a `BitcoinBlockData` that have been augmented with the functions `parse_inscriptions_in_raw_tx`,
/// `parse_inscriptions_in_standardized_tx` or `parse_inscriptions_and_standardize_block`, mutate the ordinals drafted
/// informations with actual, consensus data.
pub async fn update_block_inscriptions_with_consensus_sequence_data(
    block: &mut BitcoinBlockData,
    sequence_cursor: &mut SequenceCursor,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    db_tx: &Transaction<'_>,
    ctx: &Context,
) -> Result<(), String> {
    // Check if we've previously inscribed over any satoshi being inscribed to in this new block. This would be a reinscription.
    let mut reinscriptions_data =
        ordinals_pg::get_reinscriptions_for_block(inscriptions_data, db_tx).await?;
    // Keep a reference of inscribed satoshis that will go towards miner fees. These would be unbound inscriptions.
    let mut sat_overflows = VecDeque::new();
    let network = get_bitcoin_network(&block.metadata.network);
    let block_height = block.block_identifier.index;

    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        update_tx_inscriptions_with_consensus_sequence_data(
            tx,
            tx_index,
            &block.block_identifier,
            sequence_cursor,
            &network,
            inscriptions_data,
            &mut sat_overflows,
            &mut reinscriptions_data,
            db_tx,
            ctx,
        )
        .await?;
    }

    // Assign inscription numbers to remaining unbound inscriptions.
    while let Some((tx_index, op_index)) = sat_overflows.pop_front() {
        let OrdinalOperation::InscriptionRevealed(ref mut inscription_data) =
            block.transactions[tx_index].metadata.ordinal_operations[op_index]
        else {
            continue;
        };

        let is_cursed = inscription_data.curse_type.is_some();
        let inscription_number = sequence_cursor
            .pick_next(is_cursed, block_height, &network, db_tx)
            .await?;
        inscription_data.inscription_number = inscription_number;
        sequence_cursor.increment(is_cursed, db_tx).await?;

        // Also assign an unbound sequence number and set outpoint to all zeros, just like `ord`.
        let unbound_sequence = sequence_cursor.increment_unbound(db_tx).await?;
        inscription_data.satpoint_post_inscription =
            format!("{UNBOUND_INSCRIPTION_SATPOINT}:{unbound_sequence}");
        inscription_data.ordinal_offset = unbound_sequence as u64;
        inscription_data.unbound_sequence = Some(unbound_sequence);
        try_debug!(
            ctx,
            "Unbound inscription {inscription_id} (#{inscription_number}) detected on Satoshi {ordinal_number} \
            (block #{block_height}, {transfers} transfers)",
            inscription_id = &inscription_data.inscription_id,
            inscription_number = inscription_data.get_inscription_number(),
            ordinal_number = inscription_data.ordinal_number,
            transfers = inscription_data.transfers_pre_inscription
        );
    }
    Ok(())
}

/// Given a `BitcoinTransactionData` that have been augmented with `parse_inscriptions_in_standardized_tx`, mutate the ordinals
/// drafted informations with actual, consensus data, by using informations from `inscription_data` and `reinscription_data`.
///
/// Transactions are not fully correct from a consensus point of view state transient state after the execution of this function.
async fn update_tx_inscriptions_with_consensus_sequence_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    block_identifier: &BlockIdentifier,
    sequence_cursor: &mut SequenceCursor,
    network: &Network,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    sats_overflows: &mut VecDeque<(usize, usize)>,
    reinscriptions_data: &mut HashMap<u64, String>,
    db_tx: &Transaction<'_>,
    ctx: &Context,
) -> Result<bool, String> {
    if tx.metadata.ordinal_operations.is_empty() {
        return Ok(false);
    }

    let tx_input_values = tx
        .metadata
        .inputs
        .iter()
        .map(|i| i.previous_output.value)
        .collect::<Vec<u64>>();
    let mut mut_operations = vec![];
    mut_operations.append(&mut tx.metadata.ordinal_operations);

    let mut inscription_subindex = 0;
    for (op_index, op) in mut_operations.iter_mut().enumerate() {
        let (mut is_cursed, inscription) = match op {
            OrdinalOperation::InscriptionRevealed(inscription) => {
                (inscription.curse_type.as_ref().is_some(), inscription)
            }
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let (input_index, relative_offset) = match inscription.inscription_pointer {
            Some(pointer) => resolve_absolute_pointer(&tx_input_values, pointer),
            None => (inscription.inscription_input_index, 0),
        };

        let transaction_identifier = tx.transaction_identifier.clone();
        let inscription_id = format_inscription_id(&transaction_identifier, inscription_subindex);
        let traversal =
            match inscriptions_data.get(&(transaction_identifier, input_index, relative_offset)) {
                Some(traversal) => traversal,
                None => {
                    return Err(format!(
                        "Unable to retrieve backward traversal result for inscription in tx {}",
                        tx.transaction_identifier.hash
                    ));
                }
            };

        // Do we need to curse the inscription? Is this inscription re-inscribing an existing blessed inscription?
        let mut inscription_number = sequence_cursor
            .pick_next(is_cursed, block_identifier.index, network, db_tx)
            .await?;
        let mut curse_type_override = None;
        if !is_cursed {
            if let Some(existing_inscription_id) =
                reinscriptions_data.get(&traversal.ordinal_number)
            {
                try_debug!(
                    ctx,
                    "Satoshi {ordinal_number} was previously inscribed with blessed inscription {existing_inscription_id}, \
                    cursing inscription {cursed_id}",
                    ordinal_number = traversal.ordinal_number,
                    cursed_id = &traversal.get_inscription_id()
                );
                is_cursed = true;
                inscription_number = sequence_cursor
                    .pick_next(is_cursed, block_identifier.index, network, db_tx)
                    .await?;
                curse_type_override = Some(OrdinalInscriptionCurseType::Reinscription);
                Charm::Reinscription.set(&mut inscription.charms);
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

        inscription.charms |= Sat(traversal.ordinal_number).charms();
        if is_cursed {
            if block_identifier.index >= get_jubilee_block_height(network) {
                Charm::Vindicated.set(&mut inscription.charms);
            } else {
                Charm::Cursed.set(&mut inscription.charms);
            }
        }

        let (destination, satpoint_post_transfer, output_value) =
            compute_satpoint_post_transfer(&*tx, input_index, relative_offset, network, ctx);
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
                Charm::Unbound.set(&mut inscription.charms);
                continue;
            }
            OrdinalInscriptionTransferDestination::Burnt(_) => {
                Charm::Burned.set(&mut inscription.charms);
            }
            OrdinalInscriptionTransferDestination::Transferred(address) => {
                inscription.inscription_output_value = output_value.unwrap_or(0);
                inscription.inscriber_address = Some(address);
                if output_value.is_none() {
                    Charm::Lost.set(&mut inscription.charms);
                }
            }
        };

        if !is_cursed {
            // The reinscriptions_data needs to be augmented as we go, to handle transaction chaining.
            reinscriptions_data.insert(traversal.ordinal_number, traversal.get_inscription_id());
        }

        try_debug!(
            ctx,
            "Inscription reveal {inscription_id} (#{inscription_number}) detected on Satoshi {ordinal_number} \
            at block #{block_index}",
            inscription_id = &inscription.inscription_id,
            inscription_number = inscription.get_inscription_number(),
            ordinal_number = inscription.ordinal_number,
            block_index = block_identifier.index
        );
        sequence_cursor.increment(is_cursed, db_tx).await?;
    }
    tx.metadata.ordinal_operations.append(&mut mut_operations);

    Ok(true)
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use bitcoind::{
        types::{
            bitcoin::{OutPoint, TxIn, TxOut},
            OrdinalInscriptionCurseType, OrdinalInscriptionNumber, OrdinalInscriptionRevealData,
            OrdinalOperation, TransactionIdentifier,
        },
        utils::Context,
    };
    use ord::charm::Charm;
    use postgres::{pg_begin, pg_pool_client};
    use test_case::test_case;

    use super::update_block_inscriptions_with_consensus_sequence_data;
    use crate::{
        core::{
            protocol::{satoshi_numbering::TraversalResult, sequence_cursor::SequenceCursor},
            test_builders::{TestBlockBuilder, TestTransactionBuilder},
        },
        db::{
            ordinals_pg::{self, insert_block},
            pg_reset_db, pg_test_connection, pg_test_connection_pool,
        },
    };

    #[test_case(None => Ok(("0000000000000000000000000000000000000000000000000000000000000000:0:0".into(), Some(0))); "first unbound sequence")]
    #[test_case(Some(230) => Ok(("0000000000000000000000000000000000000000000000000000000000000000:0:231".into(), Some(231))); "next unbound sequence")]
    #[tokio::test]
    async fn unbound_inscription_sequence(
        curr_sequence: Option<i64>,
    ) -> Result<(String, Option<i64>), String> {
        let ctx = Context::empty();
        let mut sequence_cursor = SequenceCursor::new();
        let mut cache_l1 = BTreeMap::new();
        let tx_id = TransactionIdentifier {
            hash: "0xb4722ad74e7092a194e367f2ec0609994ef7a006db4f9b9d055b46cfb6514e06".into(),
        };
        let input_index = 1;

        cache_l1.insert(
            (tx_id.clone(), input_index, 0),
            TraversalResult {
                inscription_number: OrdinalInscriptionNumber {
                    classic: 0,
                    jubilee: 0,
                },
                inscription_input_index: input_index,
                transaction_identifier_inscription: tx_id.clone(),
                ordinal_number: 817263817263,
                transfers: 0,
            },
        );
        let mut pg_client = pg_test_connection().await;
        ordinals_pg::migrate(&mut pg_client).await?;
        let result = {
            let mut ord_client = pg_pool_client(&pg_test_connection_pool()).await?;
            let client = pg_begin(&mut ord_client).await?;

            if let Some(curr_sequence) = curr_sequence {
                // Simulate previous unbound sequence
                let mut tx = TestTransactionBuilder::new_with_operation().build();
                if let OrdinalOperation::InscriptionRevealed(data) =
                    &mut tx.metadata.ordinal_operations[0]
                {
                    data.unbound_sequence = Some(curr_sequence);
                };
                let block = TestBlockBuilder::new().transactions(vec![tx]).build();
                insert_block(&block, &client).await?;
            }

            // Insert new block
            let mut block = TestBlockBuilder::new()
                .height(878878)
                // Coinbase
                .add_transaction(TestTransactionBuilder::new().build())
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(tx_id.hash.clone())
                        // Normal input
                        .add_input(TxIn {
                            previous_output: OutPoint {
                                txid: TransactionIdentifier { hash: "0xf181aa98f2572879bd02278c72c83c7eaac2db82af713d1d239fc41859b2a26e".into() },
                                vout: 0,
                                value: 8000,
                                block_height: 884200,
                            },
                            script_sig: "0x00".into(),
                            sequence: 0,
                            witness: vec!["0x00".into()],
                        })
                        // Goes to fees
                        .add_input(TxIn {
                            previous_output: OutPoint {
                                txid: TransactionIdentifier { hash: "0xf181aa98f2572879bd02278c72c83c7eaac2db82af713d1d239fc41859b2a26e".into() },
                                vout: 1,
                                value: 250,
                                block_height: 884200,
                            },
                            script_sig: "0x00".into(),
                            sequence: 0,
                            witness: vec!["0x00".into()],
                        })
                        .add_output(TxOut { value: 8000, script_pubkey: "0x5120694b38ea24908e86a857279105c376a82cd1556f51655abb2ebef398b57daa8b".into() })
                        .add_ordinal_operation(OrdinalOperation::InscriptionRevealed(
                            OrdinalInscriptionRevealData {
                                content_bytes: "0x101010".into(),
                                content_type: "text/plain".into(),
                                content_length: 3,
                                inscription_number: OrdinalInscriptionNumber {
                                    classic: 0,
                                    jubilee: 0,
                                },
                                inscription_fee: 0,
                                inscription_output_value: 0,
                                inscription_id: "".into(),
                                inscription_input_index: input_index,
                                inscription_pointer: Some(8000),
                                inscriber_address: Some("bc1pd99n363yjz8gd2zhy7gstsmk4qkdz4t029j44wewhmee3dta429sm5xqrd".into()),
                                delegate: None,
                                metaprotocol: None,
                                metadata: None,
                                parents: vec![],
                                ordinal_number: 0,
                                ordinal_block_height: 0,
                                ordinal_offset: 0,
                                tx_index: 1,
                                transfers_pre_inscription: 0,
                                satpoint_post_inscription: "".into(),
                                curse_type: Some(OrdinalInscriptionCurseType::DuplicateField),
                                charms: 0,
                                unbound_sequence: None,
                            },
                        ))
                        .build(),
                )
                .build();

            update_block_inscriptions_with_consensus_sequence_data(
                &mut block,
                &mut sequence_cursor,
                &mut cache_l1,
                &client,
                &ctx,
            )
            .await?;

            let result = &block.transactions[1].metadata.ordinal_operations[0];
            let data = match result {
                OrdinalOperation::InscriptionRevealed(data) => data,
                _ => unreachable!(),
            };
            Ok((
                data.satpoint_post_inscription.clone(),
                data.unbound_sequence,
            ))
        };
        pg_reset_db(&mut pg_client).await?;

        result
    }

    #[test_case((884207, false, 1262349832364434, "0x5120694b38ea24908e86a857279105c376a82cd1556f51655abb2ebef398b57daa8b".into()) => Ok(vec![]); "common sat")]
    #[test_case((884207, false, 0, "0x5120694b38ea24908e86a857279105c376a82cd1556f51655abb2ebef398b57daa8b".into()) => Ok(vec![Charm::Coin, Charm::Mythic, Charm::Palindrome]); "mythic sat")]
    #[test_case((884207, false, 1050000000000000, "0x5120694b38ea24908e86a857279105c376a82cd1556f51655abb2ebef398b57daa8b".into()) => Ok(vec![Charm::Coin, Charm::Epic]); "epic sat")]
    #[test_case((884207, false, 123454321, "0x5120694b38ea24908e86a857279105c376a82cd1556f51655abb2ebef398b57daa8b".into()) => Ok(vec![Charm::Palindrome]); "palindrome sat")]
    #[test_case((884207, false, 1262349832364434, "0x00".into()) => Ok(vec![Charm::Burned]); "burned inscription")]
    #[test_case((780000, true, 1262349832364434, "0x5120694b38ea24908e86a857279105c376a82cd1556f51655abb2ebef398b57daa8b".into()) => Ok(vec![Charm::Cursed]); "cursed inscription")]
    #[test_case((884207, true, 1262349832364434, "0x5120694b38ea24908e86a857279105c376a82cd1556f51655abb2ebef398b57daa8b".into()) => Ok(vec![Charm::Vindicated]); "vindicated inscription")]
    #[tokio::test]
    async fn inscription_charms(
        (block_height, cursed, ordinal_number, script_pubkey): (u64, bool, u64, String),
    ) -> Result<Vec<Charm>, String> {
        let ctx = Context::empty();
        let mut sequence_cursor = SequenceCursor::new();
        let mut cache_l1 = BTreeMap::new();
        let tx_id = TransactionIdentifier {
            hash: "b4722ad74e7092a194e367f2ec0609994ef7a006db4f9b9d055b46cfb6514e06".into(),
        };
        cache_l1.insert(
            (tx_id.clone(), 0, 0),
            TraversalResult {
                inscription_number: OrdinalInscriptionNumber {
                    classic: if cursed { -1 } else { 0 },
                    jubilee: 0,
                },
                inscription_input_index: 0,
                transaction_identifier_inscription: tx_id,
                ordinal_number,
                transfers: 0,
            },
        );
        let mut pg_client = pg_test_connection().await;
        ordinals_pg::migrate(&mut pg_client).await?;
        let result = {
            let mut ord_client = pg_pool_client(&pg_test_connection_pool()).await?;
            let client = pg_begin(&mut ord_client).await?;

            let mut block = TestBlockBuilder::new()
                .height(block_height)
                // Coinbase
                .add_transaction(TestTransactionBuilder::new().build())
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "b4722ad74e7092a194e367f2ec0609994ef7a006db4f9b9d055b46cfb6514e06"
                                .into(),
                        )
                        .add_input(TxIn {
                            previous_output: OutPoint {
                                txid: TransactionIdentifier { hash: "f181aa98f2572879bd02278c72c83c7eaac2db82af713d1d239fc41859b2a26e".into() },
                                vout: 0,
                                value: 10000,
                                block_height: 884200,
                            },
                            script_sig: "0x00".into(),
                            sequence: 0,
                            witness: vec!["0x00".into()],
                        })
                        .add_output(TxOut { value: 8000, script_pubkey })
                        .add_ordinal_operation(OrdinalOperation::InscriptionRevealed(
                            OrdinalInscriptionRevealData {
                                content_bytes: "0x101010".into(),
                                content_type: "text/plain".into(),
                                content_length: 3,
                                inscription_number: OrdinalInscriptionNumber {
                                    classic: if cursed { -1 } else { 0 },
                                    jubilee: 0,
                                },
                                inscription_fee: 0,
                                inscription_output_value: 0,
                                inscription_id: "".into(),
                                inscription_input_index: 0,
                                inscription_pointer: Some(0),
                                inscriber_address: Some("bc1pd99n363yjz8gd2zhy7gstsmk4qkdz4t029j44wewhmee3dta429sm5xqrd".into()),
                                delegate: None,
                                metaprotocol: None,
                                metadata: None,
                                parents: vec![],
                                ordinal_number: 0,
                                ordinal_block_height: 0,
                                ordinal_offset: 0,
                                tx_index: 0,
                                transfers_pre_inscription: 0,
                                satpoint_post_inscription: "".into(),
                                curse_type: if cursed { Some(OrdinalInscriptionCurseType::Generic) } else { None },
                                charms: 0,
                                unbound_sequence: None,
                            },
                        ))
                        .build(),
                )
                .build();

            update_block_inscriptions_with_consensus_sequence_data(
                &mut block,
                &mut sequence_cursor,
                &mut cache_l1,
                &client,
                &ctx,
            )
            .await?;

            let result = &block.transactions[1].metadata.ordinal_operations[0];
            let charms = match result {
                OrdinalOperation::InscriptionRevealed(data) => data.charms,
                _ => unreachable!(),
            };
            Ok(Charm::charms(charms))
        };
        pg_reset_db(&mut pg_client).await?;

        result
    }
}
