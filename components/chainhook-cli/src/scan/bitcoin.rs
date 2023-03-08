use crate::config::Config;
use chainhook_event_observer::bitcoincore_rpc;
use chainhook_event_observer::bitcoincore_rpc::bitcoin::{BlockHash, OutPoint};
use chainhook_event_observer::bitcoincore_rpc::bitcoincore_rpc_json::{
    GetBlockchainInfoResult, GetRawTransactionResult,
};
use chainhook_event_observer::bitcoincore_rpc::{jsonrpc, RpcApi};
use chainhook_event_observer::bitcoincore_rpc::{Auth, Client};
use chainhook_event_observer::chainhooks::bitcoin::{
    handle_bitcoin_hook_action, BitcoinChainhookOccurrence, BitcoinTriggerChainhook,
};
use chainhook_event_observer::chainhooks::types::{
    BitcoinChainhookFullSpecification, BitcoinPredicateType, HookAction, OrdinalOperations,
    Protocols,
};
use chainhook_event_observer::indexer::bitcoin::{
    BitcoinBlockFullBreakdown, BitcoinTransactionOutputFullBreakdown,
};
use chainhook_event_observer::indexer::ordinals::indexing::entry::Entry;
use chainhook_event_observer::indexer::ordinals::indexing::{
    HEIGHT_TO_BLOCK_HASH, INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, OUTPOINT_TO_SAT_RANGES,
    SAT_TO_SATPOINT,
};
use chainhook_event_observer::indexer::ordinals::initialize_ordinal_index;
use chainhook_event_observer::indexer::ordinals::inscription_id::InscriptionId;
use chainhook_event_observer::indexer::ordinals::sat_point::SatPoint;
use chainhook_event_observer::indexer::{self, BitcoinChainContext};
use chainhook_event_observer::observer::{
    EventObserverConfig, DEFAULT_CONTROL_PORT, DEFAULT_INGESTION_PORT,
};
use chainhook_event_observer::redb::ReadableTable;
use chainhook_event_observer::utils::{file_append, send_request, Context};
use chainhook_types::{
    BitcoinTransactionData, OrdinalInscriptionRevealData, OrdinalOperation, TransactionIdentifier,
};
use reqwest::Client as HttpClient;
use rusqlite::{Connection, OpenFlags, Result, ToSql};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::Duration;

pub fn initialize_bitcoin_block_traversal_cache(path: &PathBuf) -> Connection {
    let conn = create_or_open_readwrite_db(path);
    conn.execute(
        "CREATE TABLE blocks (
            id INTEGER NOT NULL PRIMARY KEY,
            compacted_bytes TEXT NOT NULL
        )",
        [],
    )
    .unwrap();
    conn
}

fn create_or_open_readwrite_db(path: &PathBuf) -> Connection {
    let open_flags = match std::fs::metadata(path) {
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                // need to create
                if let Some(dirp) = PathBuf::from(path).parent() {
                    std::fs::create_dir_all(dirp).unwrap_or_else(|e| {
                        eprintln!("Failed to create {:?}: {:?}", dirp, &e);
                    });
                }
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
            } else {
                panic!("FATAL: could not stat {}", path.display());
            }
        }
        Ok(_md) => {
            // can just open
            OpenFlags::SQLITE_OPEN_READ_WRITE
        }
    };

    let conn = Connection::open_with_flags(path, open_flags).unwrap();
    // db.profile(Some(trace_profile));
    // db.busy_handler(Some(tx_busy_handler))?;
    conn.pragma_update(None, "journal_mode", &"WAL").unwrap();
    conn.pragma_update(None, "synchronous", &"NORMAL").unwrap();
    conn
}

fn open_existing_readonly_db(path: &PathBuf) -> Connection {
    let open_flags = match std::fs::metadata(path) {
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                panic!("FATAL: could not find {}", path.display());
            } else {
                panic!("FATAL: could not stat {}", path.display());
            }
        }
        Ok(_md) => {
            // can just open
            OpenFlags::SQLITE_OPEN_READ_ONLY
        }
    };

    let conn = Connection::open_with_flags(path, open_flags).unwrap();
    // db.profile(Some(trace_profile));
    // db.busy_handler(Some(tx_busy_handler))?;
    conn
}

#[derive(Debug, Serialize, Deserialize)]
// pub struct CompactedBlock(Vec<(Vec<(u32, u16, u64)>, Vec<u64>)>);
pub struct CompactedBlock(
    (
        ([u8; 4], u64),
        Vec<([u8; 4], Vec<([u8; 4], u32, u16, u64)>, Vec<u64>)>,
    ),
);

impl CompactedBlock {
    pub fn from_full_block(block: &BitcoinBlockFullBreakdown) -> CompactedBlock {
        let mut txs = vec![];
        let mut coinbase_value = 0;
        let coinbase_txid = {
            let txid = hex::decode(block.tx[0].txid.to_string()).unwrap();
            [txid[0], txid[1], txid[2], txid[3]]
        };
        for coinbase_output in block.tx[0].vout.iter() {
            coinbase_value += coinbase_output.value.to_sat();
        }
        for tx in block.tx.iter().skip(1) {
            let mut inputs = vec![];
            for input in tx.vin.iter().skip(0) {
                let txin = hex::decode(tx.txid.to_string()).unwrap();

                inputs.push((
                    [txin[0], txin[1], txin[2], txin[3]],
                    input.prevout.as_ref().unwrap().height as u32,
                    input.vout.unwrap() as u16,
                    input.prevout.as_ref().unwrap().value.to_sat(),
                ));
            }
            let mut outputs = vec![];
            for output in tx.vout.iter().skip(1) {
                outputs.push(output.value.to_sat());
            }
            let txid = hex::decode(tx.txid.to_string()).unwrap();
            txs.push(([txid[0], txid[1], txid[2], txid[3]], inputs, outputs));
        }
        CompactedBlock(((coinbase_txid, coinbase_value), txs))
    }

    pub fn from_hex_bytes(bytes: &str) -> CompactedBlock {
        let bytes = hex::decode(&bytes).unwrap();
        let value = ciborium::de::from_reader(&bytes[..]).unwrap();
        value
    }

    pub fn to_hex_bytes(&self) -> String {
        use ciborium::cbor;
        let value = cbor!(self).unwrap();
        let mut bytes = vec![];
        let _ = ciborium::ser::into_writer(&value, &mut bytes);
        let hex_bytes = hex::encode(bytes);
        hex_bytes
    }
}

pub fn retrieve_compacted_block_from_index(
    block_id: u32,
    storage_conn: &Connection,
) -> Option<CompactedBlock> {
    let args: &[&dyn ToSql] = &[&block_id.to_sql().unwrap()];
    let mut stmt = storage_conn
        .prepare("SELECT compacted_bytes FROM blocks WHERE id = ?1")
        .unwrap();
    let block_iter = stmt
        .query_map(args, |row| {
            let hex_bytes: String = row.get(0).unwrap();
            Ok(CompactedBlock::from_hex_bytes(&hex_bytes))
        })
        .unwrap();

    for block in block_iter {
        return Some(block.unwrap());
    }
    return None;
}

pub fn write_compacted_block_to_index(
    block_id: u32,
    compacted_block: &CompactedBlock,
    storage_conn: &Connection,
) {
    let serialized_compacted_block = compacted_block.to_hex_bytes();
    storage_conn
        .execute(
            "INSERT INTO blocks (id, compacted_bytes) VALUES (?1, ?2)",
            rusqlite::params![&block_id, &serialized_compacted_block],
        )
        .unwrap();
}

pub async fn scan_bitcoin_chain_with_predicate(
    predicate: BitcoinChainhookFullSpecification,
    apply: bool,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    // build_tx_coord_cache();
    // return Ok(());

    // let block_id = 1;

    // let conn = initialize_block_cache();

    // let block = CompactedBlock(vec![(vec![(148903239, 2423, 323940940)], vec![243242394023])]);

    // write_compacted_block_to_index(block_id, &block, &conn);

    // println!("{:?}", retrieve_compacted_block_from_index(block_id, &conn));

    let auth = Auth::UserPass(
        config.network.bitcoin_node_rpc_username.clone(),
        config.network.bitcoin_node_rpc_password.clone(),
    );

    let bitcoin_rpc = match Client::new(&config.network.bitcoin_node_rpc_url, auth) {
        Ok(con) => con,
        Err(message) => {
            return Err(format!("Bitcoin RPC error: {}", message.to_string()));
        }
    };

    let predicate_uuid = predicate.uuid.clone();
    let predicate_spec =
        match predicate.into_selected_network_specification(&config.network.bitcoin_network) {
            Ok(predicate) => predicate,
            Err(e) => {
                return Err(format!(
                    "Specification missing for network {:?}: {e}",
                    config.network.bitcoin_network
                ));
            }
        };

    let start_block = match predicate_spec.start_block {
        Some(start_block) => start_block,
        None => {
            return Err(
                "Bitcoin chainhook specification must include a field start_block in replay mode"
                    .into(),
            );
        }
    };
    let tip_height = match bitcoin_rpc.get_blockchain_info() {
        Ok(result) => result.blocks,
        Err(e) => {
            return Err(format!(
                "unable to retrieve Bitcoin chain tip ({})",
                e.to_string()
            ));
        }
    };
    let end_block = predicate_spec.end_block.unwrap_or(tip_height);

    info!(
        ctx.expect_logger(),
        "Processing Bitcoin chainhook {}, will scan blocks [{}; {}] (apply = {})",
        predicate_uuid,
        start_block,
        end_block,
        apply
    );

    // Optimization: we will use the ordinal storage to provide a set of hints.
    let mut inscriptions_hints = BTreeMap::new();
    let mut is_scanning_inscriptions = false;
    let ordinal_index = if let BitcoinPredicateType::Protocol(Protocols::Ordinal(
        OrdinalOperations::InscriptionRevealed,
    )) = &predicate_spec.predicate
    {
        let event_observer_config = EventObserverConfig {
            normalization_enabled: true,
            grpc_server_enabled: false,
            hooks_enabled: true,
            bitcoin_rpc_proxy_enabled: true,
            event_handlers: vec![],
            chainhook_config: None,
            ingestion_port: DEFAULT_INGESTION_PORT,
            control_port: DEFAULT_CONTROL_PORT,
            bitcoin_node_username: config.network.bitcoin_node_rpc_username.clone(),
            bitcoin_node_password: config.network.bitcoin_node_rpc_password.clone(),
            bitcoin_node_rpc_url: config.network.bitcoin_node_rpc_url.clone(),
            stacks_node_rpc_url: config.network.stacks_node_rpc_url.clone(),
            operators: HashSet::new(),
            display_logs: false,
            cache_path: "cache/tmp".to_string(),
            bitcoin_network: config.network.bitcoin_network.clone(),
        };

        let ordinal_index = match initialize_ordinal_index(&event_observer_config, None, &ctx) {
            Ok(index) => index,
            Err(e) => {
                panic!()
            }
        };

        for (_inscription_number, inscription_id) in ordinal_index
            .database
            .begin_read()
            .unwrap()
            .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)
            .unwrap()
            .iter()
            .unwrap()
        {
            let inscription = InscriptionId::load(*inscription_id.value());

            let entry = ordinal_index
                .get_inscription_entry(inscription)
                .unwrap()
                .unwrap();

            let blockhash = ordinal_index
                .database
                .begin_read()
                .unwrap()
                .open_table(HEIGHT_TO_BLOCK_HASH)
                .unwrap()
                .get(&entry.height)
                .unwrap()
                .map(|k| BlockHash::load(*k.value()))
                .unwrap();

            inscriptions_hints.insert(entry.height, blockhash);
        }
        is_scanning_inscriptions = true;
        Some(ordinal_index)
    } else {
        None
    };
    let mut bitcoin_context = BitcoinChainContext::new(ordinal_index);

    let mut total_hits = vec![];

    let (retrieve_ordinal_tx, retrieve_ordinal_rx) =
        channel::<std::option::Option<((BitcoinTransactionData, Vec<HookAction>))>>();
    let (process_ordinal_tx, process_ordinal_rx) = channel();
    let (cache_block_tx, cache_block_rx) = channel();

    let _config = config.clone();
    let handle_1 = hiro_system_kit::thread_named("Ordinal retrieval")
        .spawn(move || {
            while let Ok(Some((mut transaction, actions))) = retrieve_ordinal_rx.recv() {
                let txid = &transaction.transaction_identifier.hash[2..];
                println!("Retrieving satoshi point for {txid}");
                let f = retrieve_satoshi_point_using_bitcoin_rpc(&_config, &txid, 0);
                let (block_number, block_offset) = match hiro_system_kit::nestable_block_on(f) {
                    Ok(res) => res,
                    Err(err) => {
                        println!("{err}");
                        let _ = process_ordinal_tx.send(None);
                        return;
                    }
                };
                if let Some(OrdinalOperation::InscriptionRevealed(inscription)) =
                    transaction.metadata.ordinal_operations.get_mut(0)
                {
                    inscription.ordinal_offset = block_offset;
                    inscription.ordinal_block_height = block_number;
                }
                let _ = process_ordinal_tx.send(Some((transaction, actions)));
            }
            let _ = process_ordinal_tx.send(None);
        })
        .expect("unable to detach thread");

    let handle_2 = hiro_system_kit::thread_named("Ordinal ingestion")
        .spawn(move || {
            while let Ok(Some((transaction, actions))) = process_ordinal_rx.recv() {
                let txid = &transaction.transaction_identifier.hash[2..];
                if let Some(OrdinalOperation::InscriptionRevealed(inscription)) =
                    transaction.metadata.ordinal_operations.get(0)
                {
                    println!(
                        "Executing action for {txid} - {}:{}",
                        inscription.ordinal_block_height, inscription.ordinal_offset
                    );
                }
            }
        })
        .expect("unable to detach thread");

    let ctx_ = ctx.clone();
    let db_file = config.get_bitcoin_block_traversal_db_path();
    let handle_3 = hiro_system_kit::thread_named("Ordinal ingestion")
        .spawn(move || {
            let conn = initialize_bitcoin_block_traversal_cache(&db_file);
            while let Ok(Some((height, compacted_block))) = cache_block_rx.recv() {
                info!(ctx_.expect_logger(), "Caching block #{height}");
                write_compacted_block_to_index(height, &compacted_block, &conn);
            }
        })
        .expect("unable to detach thread");

    let mut pipeline_started = false;

    for cursor in start_block..=end_block {
        debug!(
            ctx.expect_logger(),
            "Evaluating predicate #{} on block #{}", predicate_uuid, cursor
        );

        let block_hash = if false {
            match inscriptions_hints.remove(&cursor) {
                Some(block_hash) => block_hash.to_string(),
                None => continue,
            }
        } else {
            loop {
                match retrieve_block_hash(config, &cursor).await {
                    Ok(res) => break res,
                    Err(e) => {
                        error!(ctx.expect_logger(), "Error retrieving block {}", cursor,);
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(3000));
            }
        };

        let block_breakdown = loop {
            match retrieve_block_full_breakdown(config, &block_hash).await {
                Ok(res) => break res,
                Err(e) => {
                    error!(ctx.expect_logger(), "Error retrieving block {}", cursor,);
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(3000));
        };

        let _ = cache_block_tx.send(Some((
            block_breakdown.height as u32,
            CompactedBlock::from_full_block(&block_breakdown),
        )));

        let block = indexer::bitcoin::standardize_bitcoin_block(
            &config.network,
            cursor,
            block_breakdown,
            &mut bitcoin_context,
            ctx,
        )?;

        let mut hits = vec![];
        for tx in block.transactions.iter() {
            if predicate_spec.predicate.evaluate_transaction_predicate(&tx) {
                info!(
                    ctx.expect_logger(),
                    "Action #{} triggered by transaction {} (block #{})",
                    predicate_uuid,
                    tx.transaction_identifier.hash,
                    cursor
                );
                if is_scanning_inscriptions {
                    pipeline_started = true;
                    let _ = retrieve_ordinal_tx
                        .send(Some((tx.clone(), vec![predicate_spec.action.clone()])));
                } else {
                    hits.push(tx);
                }
                total_hits.push(tx.transaction_identifier.hash.to_string());
            }
        }

        if hits.len() > 0 {
            if apply {
                if is_scanning_inscriptions {

                    // Start thread pool hitting bitdoind
                    // Emitting ordinal updates
                } else {
                    let trigger = BitcoinTriggerChainhook {
                        chainhook: &predicate_spec,
                        apply: vec![(hits, &block)],
                        rollback: vec![],
                    };

                    let proofs = HashMap::new();
                    match handle_bitcoin_hook_action(trigger, &proofs) {
                        Err(e) => {
                            error!(ctx.expect_logger(), "unable to handle action {}", e);
                        }
                        Ok(BitcoinChainhookOccurrence::Http(request)) => {
                            send_request(request, &ctx).await;
                        }
                        Ok(BitcoinChainhookOccurrence::File(path, bytes)) => {
                            file_append(path, bytes, &ctx)
                        }
                        Ok(BitcoinChainhookOccurrence::Data(_payload)) => unreachable!(),
                    }
                }
            }
        }
    }

    if pipeline_started {
        let _ = retrieve_ordinal_tx.send(None);
        handle_3.join();
        handle_1.join();
        handle_2.join();
    }

    Ok(())
}

pub async fn retrieve_block_full_breakdown(
    config: &Config,
    block_hash: &str,
) -> Result<BitcoinBlockFullBreakdown, String> {
    let body = json!({
        "jsonrpc": "1.0",
        "id": "chainhook-cli",
        "method": "getblock",
        "params": [block_hash, 3]
    });
    let http_client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("Unable to build http client");
    let raw_block = http_client
        .post(&config.network.bitcoin_node_rpc_url)
        .basic_auth(
            &config.network.bitcoin_node_rpc_username,
            Some(&config.network.bitcoin_node_rpc_password),
        )
        .header("Content-Type", "application/json")
        .header("Host", &config.network.bitcoin_node_rpc_url[7..])
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("unable to send request ({})", e))?
        .json::<jsonrpc::Response>()
        .await
        .map_err(|e| format!("unable to parse response ({})", e))?
        .result::<indexer::bitcoin::BitcoinBlockFullBreakdown>()
        .map_err(|e| format!("unable to parse response ({})", e))?;

    Ok(raw_block)
}

pub async fn retrieve_block_hash(config: &Config, block_height: &u64) -> Result<String, String> {
    let body = json!({
        "jsonrpc": "1.0",
        "id": "chainhook-cli",
        "method": "getblockhash",
        "params": [block_height]
    });
    let http_client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("Unable to build http client");
    let block_hash = http_client
        .post(&config.network.bitcoin_node_rpc_url)
        .basic_auth(
            &config.network.bitcoin_node_rpc_username,
            Some(&config.network.bitcoin_node_rpc_password),
        )
        .header("Content-Type", "application/json")
        .header("Host", &config.network.bitcoin_node_rpc_url[7..])
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("unable to send request ({})", e))?
        .json::<jsonrpc::Response>()
        .await
        .map_err(|e| format!("unable to parse response ({})", e))?
        .result::<String>()
        .map_err(|e| format!("unable to parse response ({})", e))?;

    Ok(block_hash)
}

pub async fn retrieve_satoshi_point_using_local_storage(
    config: &Config,
    origin_block_height: &u64,
    transaction_identifier: &TransactionIdentifier,
) -> Result<(u64, u64), String> {
    let path = config.get_bitcoin_block_traversal_db_path();
    let storage_conn = open_existing_readonly_db(&path);

    let mut ordinal_offset = 0;
    let mut ordinal_block_number = *origin_block_height as u32;
    let txid = {
        let bytes = hex::decode(&transaction_identifier.hash).unwrap();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    };
    let mut tx_cursor = (txid, 0);

    loop {
        let res = retrieve_compacted_block_from_index(ordinal_block_number, &storage_conn).unwrap();

        // evaluate exit condition: did we reach a coinbase transaction?
        let coinbase_txid = &res.0 .0 .0;
        if coinbase_txid.eq(&tx_cursor.0) {
            let coinbase_value = &res.0 .0 .1;
            if ordinal_offset.lt(coinbase_value) {
                break;
            }

            // loop over the transaction fees to detect the right range
            let cut_off = ordinal_offset - coinbase_value;
            let mut accumulated_fees = 0;
            for (txid, inputs, outputs) in res.0 .1 {
                let mut total_in = 0;
                for (_, _, _, input_value) in inputs.iter() {
                    total_in += input_value;
                }

                let mut total_out = 0;
                for output_value in outputs.iter() {
                    total_out += output_value;
                }

                let fee = total_in - total_out;
                accumulated_fees += fee;
                if accumulated_fees > cut_off {
                    // We are looking at the right transaction
                    // Retraverse the inputs to select the index to be picked
                    let mut sats_in = 0;
                    for (txin, block_height, vout, txin_value) in inputs.into_iter() {
                        sats_in += txin_value;
                        if sats_in >= total_out {
                            ordinal_offset = total_out - (sats_in - txin_value);
                            ordinal_block_number = block_height;
                            // println!("{h}: {blockhash} -> {} [in:{} , out: {}] {}/{vout} (input #{in_index}) {compounded_offset}", transaction.txid, transaction.vin.len(), transaction.vout.len(), txid);
                            tx_cursor = (txin, vout as usize);
                            break;
                        }
                    }
                    break;
                }
            }
        } else {
            // isolate the target transaction
            for (txid, inputs, outputs) in res.0 .1 {
                // we iterate over the transactions, looking for the transaction target
                if !txid.eq(&tx_cursor.0) {
                    continue;
                }

                let mut sats_out = 0;
                for (index, output_value) in outputs.iter().enumerate() {
                    if index == tx_cursor.1 {
                        break;
                    }
                    sats_out += output_value;
                }
                sats_out += ordinal_offset;

                let mut sats_in = 0;
                for (txin, block_height, vout, txin_value) in inputs.into_iter() {
                    sats_in += txin_value;
                    if sats_in >= sats_out {
                        ordinal_offset = sats_out - (sats_in - txin_value);
                        ordinal_block_number = block_height;
                        // println!("{h}: {blockhash} -> {} [in:{} , out: {}] {}/{vout} (input #{in_index}) {compounded_offset}", transaction.txid, transaction.vin.len(), transaction.vout.len(), txid);
                        tx_cursor = (txin, vout as usize);
                        break;
                    }
                }
            }
        }
    }
    Ok((ordinal_block_number.into(), ordinal_offset))
}

pub async fn scan_bitcoin_chain_for_ordinal_inscriptions(
    subscribers: Vec<HookAction>,
    first_inscription_height: u64,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    Ok(())
}
