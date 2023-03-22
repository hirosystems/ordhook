use std::{path::PathBuf, time::Duration};

use chainhook_types::{
    BitcoinBlockData, BlockIdentifier, OrdinalInscriptionRevealData, TransactionIdentifier,
};
use hiro_system_kit::slog;
use rand::RngCore;
use rusqlite::{Connection, OpenFlags, ToSql};
use threadpool::ThreadPool;

use crate::{
    indexer::bitcoin::{
        retrieve_block_hash, retrieve_full_block_breakdown_with_retry, BitcoinBlockFullBreakdown,
    },
    observer::BitcoinConfig,
    utils::Context,
};

fn get_default_ordinals_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("bitcoin_block_traversal.sqlite");
    destination_path
}

pub fn open_readonly_ordinals_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let path = get_default_ordinals_db_file_path(&base_dir);
    let conn = open_existing_readonly_db(&path, ctx);
    Ok(conn)
}

pub fn open_readwrite_ordinals_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let conn = create_or_open_readwrite_db(&base_dir, ctx);
    Ok(conn)
}

pub fn initialize_ordinal_state_storage(path: &PathBuf, ctx: &Context) -> Connection {
    let conn = create_or_open_readwrite_db(path, ctx);
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS blocks (
            id INTEGER NOT NULL PRIMARY KEY,
            compacted_bytes TEXT NOT NULL
        )",
        [],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS inscriptions (
            inscription_id TEXT NOT NULL PRIMARY KEY,
            outpoint_to_watch TEXT NOT NULL,
            satoshi_id TEXT NOT NULL,
            inscription_number INTEGER NOT NULL,
            offset NOT NULL
        )",
        [],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
    if let Err(e) = conn.execute(
        "CREATE INDEX IF NOT EXISTS index_inscriptions_on_outpoint_to_watch ON inscriptions(outpoint_to_watch);",
        [],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
    if let Err(e) = conn.execute(
        "CREATE INDEX IF NOT EXISTS index_inscriptions_on_satoshi_id ON inscriptions(satoshi_id);",
        [],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }

    conn
}

fn create_or_open_readwrite_db(cache_path: &PathBuf, ctx: &Context) -> Connection {
    let path = get_default_ordinals_db_file_path(&cache_path);
    let open_flags = match std::fs::metadata(&path) {
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                // need to create
                if let Some(dirp) = PathBuf::from(&path).parent() {
                    std::fs::create_dir_all(dirp).unwrap_or_else(|e| {
                        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
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

    let conn = loop {
        match Connection::open_with_flags(&path, open_flags) {
            Ok(conn) => break conn,
            Err(e) => {
                ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
            }
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    // db.profile(Some(trace_profile));
    // db.busy_handler(Some(tx_busy_handler))?;
    conn.pragma_update(None, "journal_mode", &"WAL").unwrap();
    conn.pragma_update(None, "synchronous", &"NORMAL").unwrap();
    conn
}

fn open_existing_readonly_db(path: &PathBuf, ctx: &Context) -> Connection {
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

    let conn = loop {
        match Connection::open_with_flags(path, open_flags) {
            Ok(conn) => break conn,
            Err(e) => {
                ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
            }
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    return conn;
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
            for input in tx.vin.iter() {
                let txin = hex::decode(input.txid.unwrap().to_string()).unwrap();

                inputs.push((
                    [txin[0], txin[1], txin[2], txin[3]],
                    input.prevout.as_ref().unwrap().height as u32,
                    input.vout.unwrap() as u16,
                    input.prevout.as_ref().unwrap().value.to_sat(),
                ));
            }
            let mut outputs = vec![];
            for output in tx.vout.iter() {
                outputs.push(output.value.to_sat());
            }
            let txid = hex::decode(tx.txid.to_string()).unwrap();
            txs.push(([txid[0], txid[1], txid[2], txid[3]], inputs, outputs));
        }
        CompactedBlock(((coinbase_txid, coinbase_value), txs))
    }

    pub fn from_standardized_block(block: &BitcoinBlockData) -> CompactedBlock {
        let mut txs = vec![];
        let mut coinbase_value = 0;
        let coinbase_txid = {
            let txid =
                hex::decode(&block.transactions[0].transaction_identifier.hash[2..]).unwrap();
            [txid[0], txid[1], txid[2], txid[3]]
        };
        for coinbase_output in block.transactions[0].metadata.outputs.iter() {
            coinbase_value += coinbase_output.value;
        }
        for tx in block.transactions.iter().skip(1) {
            let mut inputs = vec![];
            for input in tx.metadata.inputs.iter() {
                let txin = hex::decode(&input.previous_output.txid[2..]).unwrap();

                inputs.push((
                    [txin[0], txin[1], txin[2], txin[3]],
                    input.previous_output.block_height as u32,
                    input.previous_output.vout as u16,
                    input.previous_output.value,
                ));
            }
            let mut outputs = vec![];
            for output in tx.metadata.outputs.iter() {
                outputs.push(output.value);
            }
            let txid = hex::decode(&tx.transaction_identifier.hash[2..]).unwrap();
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
    let result_iter = stmt
        .query_map(args, |row| {
            let hex_bytes: String = row.get(0).unwrap();
            Ok(CompactedBlock::from_hex_bytes(&hex_bytes))
        })
        .unwrap();

    for result in result_iter {
        return Some(result.unwrap());
    }
    return None;
}

pub fn scan_existing_inscriptions_id(
    inscription_id: &str,
    storage_conn: &Connection,
) -> Option<String> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let mut stmt = storage_conn
        .prepare("SELECT inscription_id FROM inscriptions WHERE inscription_id = ?1")
        .unwrap();
    let result_iter = stmt
        .query_map(args, |row| {
            let inscription_id: String = row.get(0).unwrap();
            Ok(inscription_id)
        })
        .unwrap();

    for result in result_iter {
        return Some(result.unwrap());
    }
    return None;
}

pub fn store_new_inscription(
    inscription_data: &OrdinalInscriptionRevealData,
    storage_conn: &Connection,
    ctx: &Context,
) {
    if let Err(e) = storage_conn.execute(
        "INSERT INTO inscriptions (inscription_id, outpoint_to_watch, satoshi_id, inscription_number) VALUES (?1, ?2)",
        rusqlite::params![&inscription_data.inscription_id, &inscription_data.outpoint_post_inscription, &inscription_data.inscription_id, &inscription_data.inscription_id],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn update_transfered_inscription(
    inscription_id: &str,
    outpoint_post_transfer: &str,
    offset: u64,
    storage_conn: &Connection,
    ctx: &Context,
) {
    if let Err(e) = storage_conn.execute(
        "UPDATE inscriptions SET outpoint_to_watch = ?1, offset = ?2 WHERE inscription_id = ?3",
        rusqlite::params![&outpoint_post_transfer, &offset, &inscription_id],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn find_last_inscription_number(
    storage_conn: &Connection,
    ctx: &Context,
) -> Result<u64, String> {
    let args: &[&dyn ToSql] = &[];
    let mut stmt = storage_conn
        .prepare(
            "SELECT inscription_number FROM inscriptions ORDER BY inscription_number DESC LIMIT 1",
        )
        .unwrap();
    let result_iter = stmt
        .query_map(args, |row| {
            let inscription_number: u64 = row.get(0).unwrap();
            Ok(inscription_number)
        })
        .unwrap();

    for result in result_iter {
        return Ok(result.unwrap());
    }
    return Ok(0);
}

pub fn find_inscription_with_satoshi_id(
    satoshi_id: &str,
    storage_conn: &Connection,
    ctx: &Context,
) -> Option<String> {
    let args: &[&dyn ToSql] = &[&satoshi_id.to_sql().unwrap()];
    let mut stmt = storage_conn
        .prepare("SELECT inscription_id FROM inscriptions WHERE satoshi_id = ?1")
        .unwrap();
    let result_iter = stmt
        .query_map(args, |row| {
            let inscription_id: String = row.get(0).unwrap();
            Ok(inscription_id)
        })
        .unwrap();

    for result in result_iter {
        return Some(result.unwrap());
    }
    return None;
}

pub fn find_inscriptions_at_wached_outpoint(
    txin: &str,
    storage_conn: &Connection,
) -> Vec<(String, u64, String, u64)> {
    let args: &[&dyn ToSql] = &[&txin.to_sql().unwrap()];
    let mut stmt = storage_conn
        .prepare("SELECT inscription_id, inscription_number, satoshi_id, offset FROM inscriptions WHERE outpoint_to_watch = ?1 ORDER BY offset ASC")
        .unwrap();
    let mut results = vec![];
    let result_iter = stmt
        .query_map(args, |row| {
            let inscription_id: String = row.get(0).unwrap();
            let inscription_number: u64 = row.get(1).unwrap();
            let satoshi_id: String = row.get(2).unwrap();
            let offset: u64 = row.get(1).unwrap();
            results.push((inscription_id, inscription_number, satoshi_id, offset));
            Ok(())
        })
        .unwrap();

    return results;
}

pub fn write_compacted_block_to_index(
    block_id: u32,
    compacted_block: &CompactedBlock,
    storage_conn: &Connection,
    ctx: &Context,
) {
    let serialized_compacted_block = compacted_block.to_hex_bytes();

    if let Err(e) = storage_conn.execute(
        "INSERT INTO blocks (id, compacted_bytes) VALUES (?1, ?2)",
        rusqlite::params![&block_id, &serialized_compacted_block],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub async fn build_bitcoin_traversal_local_storage(
    bitcoin_config: &BitcoinConfig,
    storage_conn: &Connection,
    start_block: u64,
    end_block: u64,
    ctx: &Context,
    network_thread: usize,
) -> Result<(), String> {
    let retrieve_block_hash_pool = ThreadPool::new(network_thread);
    let (block_hash_tx, block_hash_rx) = crossbeam_channel::unbounded();
    let retrieve_block_data_pool = ThreadPool::new(network_thread);
    let (block_data_tx, block_data_rx) = crossbeam_channel::unbounded();
    let compress_block_data_pool = ThreadPool::new(8);
    let (block_compressed_tx, block_compressed_rx) = crossbeam_channel::unbounded();

    for block_cursor in start_block..end_block {
        let block_height = block_cursor.clone();
        let block_hash_tx = block_hash_tx.clone();
        let config = bitcoin_config.clone();
        retrieve_block_hash_pool.execute(move || {
            let mut err_count = 0;
            let mut rng = rand::thread_rng();
            loop {
                let future = retrieve_block_hash(&config, &block_height);
                match hiro_system_kit::nestable_block_on(future) {
                    Ok(block_hash) => {
                        let _ = block_hash_tx.send(Some((block_cursor, block_hash)));
                        break;
                    }
                    Err(e) => {
                        err_count += 1;
                        let delay = (err_count + (rng.next_u64() % 3)) * 1000;
                        std::thread::sleep(std::time::Duration::from_millis(delay));
                    }
                }
            }
        });
    }

    let bitcoin_config = bitcoin_config.clone();
    let moved_ctx = ctx.clone();
    let block_data_tx_moved = block_data_tx.clone();
    let handle_1 = hiro_system_kit::thread_named("Block data retrieval").spawn(move || {
        while let Ok(Some((block_height, block_hash))) = block_hash_rx.recv() {
            let moved_bitcoin_config = bitcoin_config.clone();
            let block_data_tx = block_data_tx_moved.clone();
            let moved_ctx = moved_ctx.clone();
            retrieve_block_data_pool.execute(move || {
                moved_ctx.try_log(|logger| slog::info!(logger, "Fetching block #{block_height}"));
                let future = retrieve_full_block_breakdown_with_retry(
                    &moved_bitcoin_config,
                    &block_hash,
                    &moved_ctx,
                );
                let block_data = hiro_system_kit::nestable_block_on(future).unwrap();
                let _ = block_data_tx.send(Some(block_data));
            });
            let res = retrieve_block_data_pool.join();
            res
        }
    }).expect("unable to spawn thread");

    let handle_2 = hiro_system_kit::thread_named("Block data compression").spawn(move || {
        while let Ok(Some(block_data)) = block_data_rx.recv() {
            let block_compressed_tx_moved = block_compressed_tx.clone();
            compress_block_data_pool.execute(move || {
                let compressed_block = CompactedBlock::from_full_block(&block_data);
                let _ = block_compressed_tx_moved
                    .send(Some((block_data.height as u32, compressed_block)));
            });

            let res = compress_block_data_pool.join();
            // let _ = block_compressed_tx.send(None);
            res
        }
    }).expect("unable to spawn thread");

    let mut blocks_stored = 0;
    while let Ok(Some((block_height, compacted_block))) = block_compressed_rx.recv() {
        ctx.try_log(|logger| slog::info!(logger, "Storing block #{block_height}"));
        write_compacted_block_to_index(block_height, &compacted_block, &storage_conn, &ctx);
        blocks_stored+= 1;
        if blocks_stored == end_block - start_block {
            let _ = block_data_tx.send(None);
            let _ = block_hash_tx.send(None);
            ctx.try_log(|logger| slog::info!(logger, "Local ordinals storage successfully seeded with #{blocks_stored} blocks"));
            return Ok(())
        }
    }

    retrieve_block_hash_pool.join();

    Ok(())
}

pub fn retrieve_satoshi_point_using_local_storage(
    storage_conn: &Connection,
    block_identifier: &BlockIdentifier,
    transaction_identifier: &TransactionIdentifier,
    ctx: &Context,
) -> Result<(u64, u64), String> {
    let mut ordinal_offset = 0;
    let mut ordinal_block_number = block_identifier.index as u32;
    let txid = {
        let bytes = hex::decode(&transaction_identifier.hash[2..]).unwrap();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    };
    let mut tx_cursor = (txid, 0);

    loop {
        let res = match retrieve_compacted_block_from_index(ordinal_block_number, &storage_conn) {
            Some(res) => res,
            None => {
                return Err(format!("unable to retrieve block ##{ordinal_block_number}"));
            }
        };

        ctx.try_log(|logger| {
            slog::debug!(
                logger,
                "{ordinal_block_number}:{:?}:{:?}",
                hex::encode(&res.0 .0 .0),
                hex::encode(txid)
            )
        });

        std::thread::sleep(std::time::Duration::from_millis(300));

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

                ctx.try_log(|logger| {
                    slog::debug!(logger, "Evaluating {}: {:?}", hex::encode(&txid), outputs)
                });

                let mut sats_out = 0;
                for (index, output_value) in outputs.iter().enumerate() {
                    if index == tx_cursor.1 {
                        break;
                    }
                    ctx.try_log(|logger| {
                        slog::debug!(logger, "Adding {} from output #{}", output_value, index)
                    });
                    sats_out += output_value;
                }
                sats_out += ordinal_offset;

                let mut sats_in = 0;
                for (txin, block_height, vout, txin_value) in inputs.into_iter() {
                    sats_in += txin_value;
                    if sats_in >= sats_out {
                        ordinal_offset = sats_out - (sats_in - txin_value);
                        ordinal_block_number = block_height;

                        ctx.try_log(|logger| slog::debug!(logger, "Block {ordinal_block_number} / Tx {} / [in:{sats_in}, out:{sats_out}]: {block_height} -> {ordinal_block_number}:{ordinal_offset} -> {}:{vout}",
                        hex::encode(&txid),
                        hex::encode(&txin)));
                        tx_cursor = (txin, vout as usize);
                        break;
                    }
                }
            }
        }
    }
    Ok((ordinal_block_number.into(), ordinal_offset))
}

