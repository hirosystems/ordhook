use std::{
    collections::{BTreeMap, HashMap},
    hash::BuildHasherDefault,
    path::PathBuf,
    sync::{mpsc::Sender, Arc},
};

use chainhook_types::{
    BitcoinBlockData, BlockIdentifier, OrdinalInscriptionRevealData,
    OrdinalInscriptionTransferData, TransactionIdentifier,
};
use dashmap::DashMap;
use fxhash::FxHasher;
use hiro_system_kit::slog;
use rand::{thread_rng, Rng};

use rocksdb::DB;
use rusqlite::{Connection, OpenFlags, ToSql};
use std::io::Cursor;
use std::io::{Read, Write};
use threadpool::ThreadPool;

use crate::{
    indexer::bitcoin::{
        download_block_with_retry, retrieve_block_hash_with_retry, standardize_bitcoin_block,
        BitcoinBlockFullBreakdown,
    },
    observer::BitcoinConfig,
    utils::Context,
};

use super::{
    new_traversals_lazy_cache,
    ord::{height::Height, sat::Sat},
    update_hord_db_and_augment_bitcoin_block, HordConfig,
};

fn get_default_hord_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("hord.sqlite");
    destination_path
}

pub fn open_readonly_hord_db_conn(base_dir: &PathBuf, ctx: &Context) -> Result<Connection, String> {
    let path = get_default_hord_db_file_path(&base_dir);
    let conn = open_existing_readonly_db(&path, ctx);
    Ok(conn)
}

pub fn open_readwrite_hord_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let conn = create_or_open_readwrite_db(&base_dir, ctx);
    Ok(conn)
}

pub fn initialize_hord_db(path: &PathBuf, ctx: &Context) -> Connection {
    let conn = create_or_open_readwrite_db(path, ctx);
    // TODO: introduce initial output
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS inscriptions (
            inscription_id TEXT NOT NULL PRIMARY KEY,
            block_height INTEGER NOT NULL,
            ordinal_number INTEGER NOT NULL,
            inscription_number INTEGER NOT NULL
        )",
        [],
    ) {
        ctx.try_log(|logger| {
            slog::warn!(
                logger,
                "Unable to create table inscriptions: {}",
                e.to_string()
            )
        });
    } else {
        if let Err(e) = conn.execute(
            "CREATE TABLE IF NOT EXISTS locations (
                inscription_id TEXT NOT NULL,
                block_height INTEGER NOT NULL,
                tx_index INTEGER NOT NULL,
                outpoint_to_watch TEXT NOT NULL,
                offset INTEGER NOT NULL
            )",
            [],
        ) {
            ctx.try_log(|logger| {
                slog::warn!(logger, "Unable to create table locations:{}", e.to_string())
            });
        }

        // Legacy table - to be removed
        if let Err(e) = conn.execute(
            "CREATE TABLE IF NOT EXISTS activities (
                block_height INTEGER NOT NULL PRIMARY KEY
            )",
            [],
        ) {
            ctx.try_log(|logger| {
                slog::warn!(logger, "Unable to create table locations:{}", e.to_string())
            });
        }

        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_ordinal_number ON inscriptions(ordinal_number);",
            [],
        ) {
            ctx.try_log(|logger| slog::warn!(logger, "{}", e.to_string()));
        }

        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_block_height ON inscriptions(block_height);",
            [],
        ) {
            ctx.try_log(|logger| slog::warn!(logger, "{}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_locations_on_block_height ON locations(block_height);",
            [],
        ) {
            ctx.try_log(|logger| slog::warn!(logger, "{}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_locations_on_outpoint_to_watch ON locations(outpoint_to_watch);",
            [],
        ) {
            ctx.try_log(|logger| slog::warn!(logger, "{}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_locations_on_inscription_id ON locations(inscription_id);",
            [],
        ) {
            ctx.try_log(|logger| slog::warn!(logger, "{}", e.to_string()));
        }
    }
    conn
}

fn create_or_open_readwrite_db(cache_path: &PathBuf, ctx: &Context) -> Connection {
    let path = get_default_hord_db_file_path(&cache_path);
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
    // let mmap_size: i64 = 256 * 1024 * 1024;
    // let page_size: i64 = 16384;
    // conn.pragma_update(None, "mmap_size", mmap_size).unwrap();
    // conn.pragma_update(None, "page_size", page_size).unwrap();
    // conn.pragma_update(None, "synchronous", &"NORMAL").unwrap();
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

fn get_default_hord_db_file_path_rocks_db(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("hord.rocksdb");
    destination_path
}

fn rocks_db_default_options() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    // opts.prepare_for_bulk_load();
    // opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    // opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
    // opts.increase_parallelism(parallelism)
    // Per rocksdb's documentation:
    // If cache_index_and_filter_blocks is false (which is default),
    // the number of index/filter blocks is controlled by option max_open_files.
    // If you are certain that your ulimit will always be bigger than number of files in the database,
    // we recommend setting max_open_files to -1, which means infinity.
    // This option will preload all filter and index blocks and will not need to maintain LRU of files.
    // Setting max_open_files to -1 will get you the best possible performance.
    opts.set_max_open_files(2048);
    opts
}

pub fn open_readonly_hord_db_conn_rocks_db(
    base_dir: &PathBuf,
    _ctx: &Context,
) -> Result<DB, String> {
    let path = get_default_hord_db_file_path_rocks_db(&base_dir);
    let mut opts = rocks_db_default_options();
    opts.set_disable_auto_compactions(true);
    opts.set_max_background_jobs(0);
    let db = DB::open_for_read_only(&opts, path, false)
        .map_err(|e| format!("unable to open blocks_db: {}", e.to_string()))?;
    Ok(db)
}

pub fn open_readonly_hord_db_conn_rocks_db_loop(base_dir: &PathBuf, ctx: &Context) -> DB {
    let blocks_db = loop {
        match open_readonly_hord_db_conn_rocks_db(&base_dir, &ctx) {
            Ok(db) => break db,
            Err(e) => {
                ctx.try_log(|logger| {
                    slog::warn!(logger, "Unable to open db: {e}",);
                });
                continue;
            }
        }
    };
    blocks_db
}

pub fn open_readwrite_hord_dbs(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<(DB, Connection), String> {
    let blocks_db = open_readwrite_hord_db_conn_rocks_db(&base_dir, &ctx)?;
    let inscriptions_db = open_readwrite_hord_db_conn(&base_dir, &ctx)?;
    Ok((blocks_db, inscriptions_db))
}

pub fn open_readwrite_hord_db_conn_rocks_db(
    base_dir: &PathBuf,
    _ctx: &Context,
) -> Result<DB, String> {
    let path = get_default_hord_db_file_path_rocks_db(&base_dir);
    let opts = rocks_db_default_options();
    let db = DB::open(&opts, path)
        .map_err(|e| format!("unable to open blocks_db: {}", e.to_string()))?;
    Ok(db)
}

pub fn insert_entry_in_blocks(
    block_height: u32,
    lazy_block: &LazyBlock,
    blocks_db_rw: &DB,
    _ctx: &Context,
) {
    let block_height_bytes = block_height.to_be_bytes();
    blocks_db_rw
        .put(&block_height_bytes, &lazy_block.bytes)
        .expect("unable to insert blocks");
    blocks_db_rw
        .put(b"metadata::last_insert", block_height_bytes)
        .expect("unable to insert metadata");
}

pub fn find_last_block_inserted(blocks_db: &DB) -> u32 {
    match blocks_db.get(b"metadata::last_insert") {
        Ok(Some(bytes)) => u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        _ => 0,
    }
}

pub fn find_lazy_block_at_block_height(
    block_height: u32,
    retry: u8,
    try_iterator: bool,
    blocks_db: &DB,
    ctx: &Context,
) -> Option<LazyBlock> {
    let mut attempt = 1;
    // let mut read_options = rocksdb::ReadOptions::default();
    // read_options.fill_cache(true);
    // read_options.set_verify_checksums(false);
    let mut backoff: f64 = 1.0;
    let mut rng = thread_rng();

    loop {
        match blocks_db.get(block_height.to_be_bytes()) {
            Ok(Some(res)) => return Some(LazyBlock::new(res)),
            _ => {
                if attempt == 1 && try_iterator {
                    ctx.try_log(|logger| {
                        slog::warn!(
                            logger,
                            "Attempt to retrieve block {} through iterator",
                            block_height,
                        )
                    });
                    let mut iter = blocks_db.iterator(rocksdb::IteratorMode::End);
                    let block_height_bytes = block_height.to_be_bytes();
                    while let Some(Ok((k, res))) = iter.next() {
                        if (*k).eq(&block_height_bytes) {
                            return Some(LazyBlock::new(res.to_vec()));
                        }
                    }
                }
                attempt += 1;
                backoff = 2.0 * backoff + (backoff * rng.gen_range(0.0..1.0));
                let duration = std::time::Duration::from_millis((backoff * 1_000.0) as u64);
                ctx.try_log(|logger| {
                    slog::warn!(
                        logger,
                        "Unable to find block {}, will retry in {:?}",
                        block_height,
                        duration
                    )
                });
                std::thread::sleep(duration);
                if attempt > retry {
                    return None;
                }
            }
        }
    }
}

pub fn remove_entry_from_blocks(block_height: u32, blocks_db_rw: &DB, ctx: &Context) {
    if let Err(e) = blocks_db_rw.delete(block_height.to_be_bytes()) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn delete_blocks_in_block_range(
    start_block: u32,
    end_block: u32,
    blocks_db_rw: &DB,
    ctx: &Context,
) {
    for block_height in start_block..=end_block {
        remove_entry_from_blocks(block_height, blocks_db_rw, ctx);
    }
    let start_block_bytes = (start_block - 1).to_be_bytes();
    blocks_db_rw
        .put(b"metadata::last_insert", start_block_bytes)
        .expect("unable to insert metadata");
}

pub fn insert_entry_in_inscriptions(
    inscription_data: &OrdinalInscriptionRevealData,
    block_identifier: &BlockIdentifier,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    if let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO inscriptions (inscription_id, ordinal_number, inscription_number, block_height) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![&inscription_data.inscription_id, &inscription_data.ordinal_number, &inscription_data.inscription_number, &block_identifier.index],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
    insert_inscription_in_locations(
        &inscription_data,
        &block_identifier,
        &inscriptions_db_conn_rw,
        ctx,
    );
}

pub fn insert_inscription_in_locations(
    inscription_data: &OrdinalInscriptionRevealData,
    block_identifier: &BlockIdentifier,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    let (tx, output_index, offset) =
        parse_satpoint_to_watch(&inscription_data.satpoint_post_inscription);
    let outpoint_to_watch = format_outpoint_to_watch(&tx, output_index);
    if let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO locations (inscription_id, outpoint_to_watch, offset, block_height, tx_index) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![&inscription_data.inscription_id, &outpoint_to_watch, offset, &block_identifier.index, &inscription_data.tx_index],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn insert_transfer_in_locations(
    transfer_data: &OrdinalInscriptionTransferData,
    block_identifier: &BlockIdentifier,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    let (tx, output_index, offset) = parse_satpoint_to_watch(&transfer_data.satpoint_post_transfer);
    let outpoint_to_watch = format_outpoint_to_watch(&tx, output_index);
    if let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO locations (inscription_id, outpoint_to_watch, offset, block_height, tx_index) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![&transfer_data.inscription_id, &outpoint_to_watch, offset, &block_identifier.index, &transfer_data.tx_index],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn insert_entry_in_ordinal_activities(
    block_height: u32,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    if let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO activities (block_height) VALUES (?1)",
        rusqlite::params![&block_height],
    ) {
        ctx.try_log(|logger| slog::warn!(logger, "{}", e.to_string()));
    }
}

pub fn get_any_entry_in_ordinal_activities(
    block_height: &u64,
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> bool {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare("SELECT block_height FROM activities WHERE block_height = ?")
        .unwrap();
    let mut rows = stmt.query(args).unwrap();
    while let Ok(Some(_)) = rows.next() {
        return true;
    }
    false
}

pub fn patch_inscription_number(
    inscription_id: &str,
    inscription_number: u64,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    if let Err(e) = inscriptions_db_conn_rw.execute(
        "UPDATE inscriptions SET inscription_number = ? WHERE inscription_id = ?",
        rusqlite::params![&inscription_number, &inscription_id],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn find_latest_inscription_block_height(
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> Result<Option<u64>, String> {
    let args: &[&dyn ToSql] = &[];
    let mut stmt = inscriptions_db_conn
        .prepare("SELECT block_height FROM inscriptions ORDER BY block_height DESC LIMIT 1")
        .unwrap();
    let mut rows = stmt.query(args).unwrap();
    while let Ok(Some(row)) = rows.next() {
        let block_height: u64 = row.get(0).unwrap();
        return Ok(Some(block_height));
    }
    Ok(None)
}

pub fn find_initial_inscription_transfer_data(
    inscription_id: &str,
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> Result<Option<(TransactionIdentifier, usize, u64)>, String> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare("SELECT outpoint_to_watch, offset FROM locations WHERE inscription_id = ? ORDER BY block_height ASC, tx_index ASC LIMIT 1")
        .unwrap();
    let mut rows = stmt.query(args).unwrap();
    while let Ok(Some(row)) = rows.next() {
        let outpoint_to_watch: String = row.get(0).unwrap();
        let (transaction_identifier, output_index) = parse_outpoint_to_watch(&outpoint_to_watch);
        let inscription_offset_intra_output: u64 = row.get(1).unwrap();
        return Ok(Some((
            transaction_identifier,
            output_index,
            inscription_offset_intra_output,
        )));
    }
    Ok(None)
}

pub fn find_latest_inscription_transfer_data(
    inscription_id: &str,
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> Result<Option<(TransactionIdentifier, usize, u64)>, String> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare("SELECT outpoint_to_watch, offset FROM locations WHERE inscription_id = ? ORDER BY block_height DESC, tx_index DESC LIMIT 1")
        .unwrap();
    let mut rows = stmt.query(args).unwrap();
    while let Ok(Some(row)) = rows.next() {
        let outpoint_to_watch: String = row.get(0).unwrap();
        let (transaction_identifier, output_index) = parse_outpoint_to_watch(&outpoint_to_watch);
        let inscription_offset_intra_output: u64 = row.get(1).unwrap();
        return Ok(Some((
            transaction_identifier,
            output_index,
            inscription_offset_intra_output,
        )));
    }
    Ok(None)
}

#[derive(Debug, Clone)]
pub struct TransferData {
    pub inscription_offset_intra_output: u64,
    pub transaction_identifier_location: TransactionIdentifier,
    pub output_index: usize,
}

pub fn find_all_transfers_in_block(
    block_height: &u64,
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> BTreeMap<String, Vec<TransferData>> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
    .prepare("SELECT inscription_id, offset, outpoint_to_watch, tx_index FROM locations WHERE block_height = ? ORDER BY tx_index ASC")
    .unwrap();
    let mut results: BTreeMap<String, Vec<TransferData>> = BTreeMap::new();
    let mut rows = stmt.query(args).unwrap();
    while let Ok(Some(row)) = rows.next() {
        let inscription_id: String = row.get(0).unwrap();
        let inscription_offset_intra_output: u64 = row.get(1).unwrap();
        let outpoint_to_watch: String = row.get(2).unwrap();
        let (transaction_identifier_location, output_index) =
            parse_outpoint_to_watch(&outpoint_to_watch);
        let transfer = TransferData {
            inscription_offset_intra_output,
            transaction_identifier_location,
            output_index,
        };
        results
            .entry(inscription_id)
            .and_modify(|v| v.push(transfer.clone()))
            .or_insert(vec![transfer]);
    }
    return results;
}

pub fn find_latest_inscription_number_at_block_height(
    block_height: &u64,
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> Result<Option<i64>, String> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare(
            "SELECT inscription_number FROM inscriptions WHERE block_height < ? ORDER BY inscription_number DESC LIMIT 1",
        )
        .map_err(|e| format!("unable to query inscriptions: {}", e.to_string()))?;
    let mut rows = stmt
        .query(args)
        .map_err(|e| format!("unable to query inscriptions: {}", e.to_string()))?;
    while let Ok(Some(row)) = rows.next() {
        let inscription_number: i64 = row.get(0).unwrap();
        return Ok(Some(inscription_number));
    }
    Ok(None)
}

pub fn find_latest_cursed_inscription_number_at_block_height(
    block_height: &u64,
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> Result<Option<i64>, String> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare(
            "SELECT inscription_number FROM inscriptions WHERE block_height < ? ORDER BY inscription_number ASC LIMIT 1",
        )
        .map_err(|e| format!("unable to query inscriptions: {}", e.to_string()))?;
    let mut rows = stmt
        .query(args)
        .map_err(|e| format!("unable to query inscriptions: {}", e.to_string()))?;
    while let Ok(Some(row)) = rows.next() {
        let inscription_number: i64 = row.get(0).unwrap();
        return Ok(Some(inscription_number));
    }
    Ok(None)
}

pub fn find_inscription_with_ordinal_number(
    ordinal_number: &u64,
    inscriptions_db_conn: &Connection,
    _ctx: &Context,
) -> Option<String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare("SELECT inscription_id FROM inscriptions WHERE ordinal_number = ? AND inscription_number > 0")
        .unwrap();
    let mut rows = stmt.query(args).unwrap();
    while let Ok(Some(row)) = rows.next() {
        let inscription_id: String = row.get(0).unwrap();
        return Some(inscription_id);
    }
    return None;
}

pub fn find_inscription_with_id(
    inscription_id: &str,
    inscriptions_db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<TraversalResult>, String> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let mut stmt = loop {
        match inscriptions_db_conn.prepare(
            "SELECT inscription_number, ordinal_number FROM inscriptions WHERE inscription_id = ?",
        ) {
            Ok(stmt) => break stmt,
            Err(e) => {
                ctx.try_log(|logger| {
                    slog::warn!(
                        logger,
                        "unable to retrieve inscription with id: {}",
                        e.to_string(),
                    )
                });
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    };
    let mut rows = stmt.query(args).unwrap();

    if let Some((transaction_identifier_location, output_index, inscription_offset_intra_output)) =
        find_initial_inscription_transfer_data(inscription_id, inscriptions_db_conn, ctx)?
    {
        while let Ok(Some(row)) = rows.next() {
            let inscription_number: i64 = row.get(0).unwrap();
            let ordinal_number: u64 = row.get(1).unwrap();
            let (transaction_identifier_inscription, inscription_input_index) =
                parse_inscription_id(inscription_id);

            let traversal = TraversalResult {
                inscription_number,
                ordinal_number,
                inscription_input_index,
                transaction_identifier_inscription,
                transfers: 0,
                transfer_data: TransferData {
                    inscription_offset_intra_output,
                    transaction_identifier_location,
                    output_index,
                },
            };
            return Ok(Some(traversal));
        }
    }
    return Ok(None);
}

pub fn find_all_inscriptions_in_block(
    block_height: &u64,
    inscriptions_db_conn: &Connection,
    ctx: &Context,
) -> BTreeMap<u64, Vec<(TransactionIdentifier, TraversalResult)>> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare("SELECT inscription_number, ordinal_number, inscription_id FROM inscriptions where block_height = ? ORDER BY inscription_number ASC")
        .unwrap();
    let mut results: BTreeMap<u64, Vec<(TransactionIdentifier, TraversalResult)>> = BTreeMap::new();
    let mut rows = stmt.query(args).unwrap();

    let transfers_data = find_all_transfers_in_block(block_height, inscriptions_db_conn, ctx);
    while let Ok(Some(row)) = rows.next() {
        let inscription_number: i64 = row.get(0).unwrap();
        let ordinal_number: u64 = row.get(1).unwrap();
        let inscription_id: String = row.get(2).unwrap();
        let (transaction_identifier_inscription, inscription_input_index) =
            { parse_inscription_id(&inscription_id) };
        let transfer_data = transfers_data
            .get(&inscription_id)
            .unwrap()
            .first()
            .unwrap()
            .clone();
        let traversal = TraversalResult {
            inscription_number,
            ordinal_number,
            inscription_input_index,
            transfers: 0,
            transaction_identifier_inscription: transaction_identifier_inscription.clone(),
            transfer_data: transfer_data,
        };
        results
            .entry(*block_height)
            .and_modify(|v| {
                v.push((
                    transaction_identifier_inscription.clone(),
                    traversal.clone(),
                ))
            })
            .or_insert(vec![(transaction_identifier_inscription, traversal)]);
    }
    return results;
}

#[derive(Clone, Debug)]
pub struct WatchedSatpoint {
    pub inscription_id: String,
    pub offset: u64,
}

impl WatchedSatpoint {
    pub fn get_genesis_satpoint(&self) -> String {
        let (transaction_id, input) = parse_inscription_id(&self.inscription_id);
        format!("{}:{}", transaction_id.hash, input)
    }
}

pub fn find_watched_satpoint_for_inscription(
    inscription_id: &str,
    inscriptions_db_conn: &Connection,
) -> Result<(u64, WatchedSatpoint), String> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let mut stmt = inscriptions_db_conn
        .prepare("SELECT inscription_id, offset, block_height FROM locations WHERE inscription_id = ? ORDER BY offset ASC")
        .map_err(|e| format!("unable to query locations table: {}", e.to_string()))?;
    let mut rows = stmt
        .query(args)
        .map_err(|e| format!("unable to query locations table: {}", e.to_string()))?;
    while let Ok(Some(row)) = rows.next() {
        let inscription_id: String = row.get(0).unwrap();
        let offset: u64 = row.get(1).unwrap();
        let block_height: u64 = row.get(2).unwrap();
        return Ok((
            block_height,
            WatchedSatpoint {
                inscription_id,
                offset,
            },
        ));
    }
    return Err(format!(
        "unable to find inscription with id {}",
        inscription_id
    ));
}

pub fn find_inscriptions_at_wached_outpoint(
    outpoint: &str,
    hord_db_conn: &Connection,
) -> Result<Vec<WatchedSatpoint>, String> {
    let args: &[&dyn ToSql] = &[&outpoint.to_sql().unwrap()];
    let mut stmt = hord_db_conn
        .prepare("SELECT inscription_id, offset FROM locations WHERE outpoint_to_watch = ? ORDER BY offset ASC")
        .map_err(|e| format!("unable to query locations table: {}", e.to_string()))?;
    let mut results = vec![];
    let mut rows = stmt
        .query(args)
        .map_err(|e| format!("unable to query locations table: {}", e.to_string()))?;
    while let Ok(Some(row)) = rows.next() {
        let inscription_id: String = row.get(0).unwrap();
        let offset: u64 = row.get(1).unwrap();
        results.push(WatchedSatpoint {
            inscription_id,
            offset,
        });
    }
    return Ok(results);
}

pub fn delete_inscriptions_in_block_range(
    start_block: u32,
    end_block: u32,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    if let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM inscriptions WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
    if let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM locations WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn remove_entry_from_inscriptions(
    inscription_id: &str,
    inscriptions_db_rw_conn: &Connection,
    ctx: &Context,
) {
    if let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM inscriptions WHERE inscription_id = ?1",
        rusqlite::params![&inscription_id],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
    if let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM locations WHERE inscription_id = ?1",
        rusqlite::params![&inscription_id],
    ) {
        ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
    }
}

pub fn delete_data_in_hord_db(
    start_block: u64,
    end_block: u64,
    blocks_db_rw: &DB,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) -> Result<(), String> {
    delete_blocks_in_block_range(start_block as u32, end_block as u32, blocks_db_rw, &ctx);
    delete_inscriptions_in_block_range(
        start_block as u32,
        end_block as u32,
        inscriptions_db_conn_rw,
        &ctx,
    );
    Ok(())
}

pub async fn fetch_and_cache_blocks_in_hord_db(
    bitcoin_config: &BitcoinConfig,
    blocks_db_rw: &DB,
    inscriptions_db_conn_rw: &Connection,
    start_block: u64,
    end_block: u64,
    hord_config: &HordConfig,
    block_post_processor: Option<Sender<BitcoinBlockData>>,
    ctx: &Context,
) -> Result<(), String> {
    let ordinal_computing_height = hord_config.first_inscription_height;
    let number_of_blocks_to_process = end_block - start_block + 1;
    let (block_hash_req_lim, block_req_lim, block_process_lim) =
        if start_block >= ordinal_computing_height {
            (32, 24, 24)
        } else {
            (256, 128, 128)
        };
    let retrieve_block_hash_pool = ThreadPool::new(hord_config.network_thread_max);
    let (block_hash_tx, block_hash_rx) = crossbeam_channel::bounded(block_hash_req_lim);
    let retrieve_block_data_pool = ThreadPool::new(hord_config.network_thread_max);
    let (block_data_tx, block_data_rx) = crossbeam_channel::bounded(block_req_lim);
    let compress_block_data_pool = ThreadPool::new(hord_config.ingestion_thread_max);
    let (block_compressed_tx, block_compressed_rx) = crossbeam_channel::bounded(block_process_lim);

    // Thread pool #1: given a block height, retrieve the block hash
    for block_cursor in start_block..=end_block {
        let block_height = block_cursor.clone();
        let block_hash_tx = block_hash_tx.clone();
        let config = bitcoin_config.clone();
        let moved_ctx = ctx.clone();
        retrieve_block_hash_pool.execute(move || {
            let future = retrieve_block_hash_with_retry(&block_height, &config, &moved_ctx);
            let block_hash = hiro_system_kit::nestable_block_on(future).unwrap();
            block_hash_tx
                .send(Some((block_height, block_hash)))
                .expect("unable to channel block_hash");
        })
    }

    // Thread pool #2: given a block hash, retrieve the full block (verbosity max, including prevout)
    let bitcoin_network = bitcoin_config.network.clone();
    let bitcoin_config = bitcoin_config.clone();
    let moved_ctx = ctx.clone();
    let block_data_tx_moved = block_data_tx.clone();
    let _ = hiro_system_kit::thread_named("Block data retrieval")
        .spawn(move || {
            while let Ok(Some((block_height, block_hash))) = block_hash_rx.recv() {
                let moved_bitcoin_config = bitcoin_config.clone();
                let block_data_tx = block_data_tx_moved.clone();
                let moved_ctx = moved_ctx.clone();
                retrieve_block_data_pool.execute(move || {
                    moved_ctx
                        .try_log(|logger| slog::debug!(logger, "Fetching block #{block_height}"));
                    let future =
                        download_block_with_retry(&block_hash, &moved_bitcoin_config, &moved_ctx);
                    let res = match hiro_system_kit::nestable_block_on(future) {
                        Ok(block_data) => Some(block_data),
                        Err(e) => {
                            moved_ctx.try_log(|logger| {
                                slog::error!(logger, "unable to fetch block #{block_height}: {e}")
                            });
                            None
                        }
                    };
                    let _ = block_data_tx.send(res);
                });
                // TODO: remove this join?
                if block_height >= ordinal_computing_height {
                    let _ = retrieve_block_data_pool.join();
                }
            }
            let res = retrieve_block_data_pool.join();
            res
        })
        .expect("unable to spawn thread");

    let _ = hiro_system_kit::thread_named("Block data compression")
        .spawn(move || {
            while let Ok(Some(block_data)) = block_data_rx.recv() {
                let block_compressed_tx_moved = block_compressed_tx.clone();
                let block_height = block_data.height as u64;
                compress_block_data_pool.execute(move || {
                    let compressed_block =
                        LazyBlock::from_full_block(&block_data).expect("unable to serialize block");
                    let block_index = block_data.height as u32;
                    let _ = block_compressed_tx_moved.send(Some((
                        block_index,
                        compressed_block,
                        block_data,
                    )));
                });
                if block_height >= ordinal_computing_height {
                    let _ = compress_block_data_pool.join();
                }
            }
            let res = compress_block_data_pool.join();
            res
        })
        .expect("unable to spawn thread");

    let mut blocks_stored = 0;
    let mut cursor = start_block as usize;
    let mut inbox = HashMap::new();
    let mut num_writes = 0;
    let traversals_cache = Arc::new(new_traversals_lazy_cache(hord_config.cache_size));

    while let Ok(Some((block_height, compacted_block, raw_block))) = block_compressed_rx.recv() {
        insert_entry_in_blocks(block_height, &compacted_block, &blocks_db_rw, &ctx);
        blocks_stored += 1;
        num_writes += 1;

        // In the context of ordinals, we're constrained to process blocks sequentially
        // Blocks are processed by a threadpool and could be coming out of order.
        // Inbox block for later if the current block is not the one we should be
        // processing.

        // Should we start look for inscriptions data in blocks?
        if raw_block.height as u64 >= ordinal_computing_height {
            if cursor == 0 {
                cursor = raw_block.height;
            }
            ctx.try_log(|logger| slog::info!(logger, "Queueing compacted block #{block_height}",));
            // Is the action of processing a block allows us
            // to process more blocks present in the inbox?
            inbox.insert(raw_block.height, raw_block);
            while let Some(next_block) = inbox.remove(&cursor) {
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Dequeuing block #{cursor} for processing (# blocks inboxed: {})",
                        inbox.len()
                    )
                });
                let mut new_block =
                    match standardize_bitcoin_block(next_block, &bitcoin_network, &ctx) {
                        Ok(block) => block,
                        Err((e, _)) => {
                            ctx.try_log(|logger| {
                                slog::error!(logger, "Unable to standardize bitcoin block: {e}",)
                            });
                            return Err(e);
                        }
                    };

                let _ = blocks_db_rw.flush();

                if let Err(e) = update_hord_db_and_augment_bitcoin_block(
                    &mut new_block,
                    blocks_db_rw,
                    &inscriptions_db_conn_rw,
                    false,
                    &hord_config,
                    &traversals_cache,
                    &ctx,
                ) {
                    ctx.try_log(|logger| {
                        slog::error!(
                            logger,
                            "Unable to augment bitcoin block {} with hord_db: {e}",
                            new_block.block_identifier.index
                        )
                    });
                    return Err(e);
                }

                if let Some(ref tx) = block_post_processor {
                    let _ = tx.send(new_block);
                }
                cursor += 1;
            }
        } else {
            ctx.try_log(|logger| slog::info!(logger, "Storing compacted block #{block_height}",));
        }

        if blocks_stored == number_of_blocks_to_process {
            let _ = block_data_tx.send(None);
            let _ = block_hash_tx.send(None);
            ctx.try_log(|logger| {
                slog::info!(
                    logger,
                    "Local ordinals storage successfully seeded with #{blocks_stored} blocks"
                )
            });
            return Ok(());
        }

        if !traversals_cache.is_empty() {
            if num_writes % 24 == 0 {
                ctx.try_log(|logger| {
                    slog::info!(
                        logger,
                        "Flushing traversals cache (#{} entries)",
                        traversals_cache.len()
                    );
                });
                traversals_cache.clear();
            }
        }

        if num_writes % 128 == 0 {
            ctx.try_log(|logger| {
                slog::info!(logger, "Flushing DB to disk ({num_writes} inserts)");
            });
            if let Err(e) = blocks_db_rw.flush() {
                ctx.try_log(|logger| {
                    slog::error!(logger, "{}", e.to_string());
                });
            }
            num_writes = 0;
        }
    }

    if let Err(e) = blocks_db_rw.flush() {
        ctx.try_log(|logger| {
            slog::error!(logger, "{}", e.to_string());
        });
    }

    retrieve_block_hash_pool.join();

    Ok(())
}

#[derive(Clone, Debug)]
pub struct TraversalResult {
    pub inscription_number: i64,
    pub inscription_input_index: usize,
    pub transaction_identifier_inscription: TransactionIdentifier,
    pub ordinal_number: u64,
    pub transfers: u32,
    pub transfer_data: TransferData,
}

impl TraversalResult {
    pub fn get_ordinal_coinbase_height(&self) -> u64 {
        let sat = Sat(self.ordinal_number);
        sat.height().n()
    }

    pub fn get_ordinal_coinbase_offset(&self) -> u64 {
        let sat = Sat(self.ordinal_number);
        self.ordinal_number - sat.height().starting_sat().n()
    }
}

pub fn format_satpoint_to_watch(
    transaction_identifier: &TransactionIdentifier,
    output_index: usize,
    offset: u64,
) -> String {
    format!(
        "{}:{}:{}",
        transaction_identifier.get_hash_bytes_str(),
        output_index,
        offset
    )
}

pub fn parse_satpoint_to_watch(outpoint_to_watch: &str) -> (TransactionIdentifier, usize, u64) {
    let comps: Vec<&str> = outpoint_to_watch.split(":").collect();
    let tx = TransactionIdentifier::new(comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().unwrap();
    let offset = comps[2].to_string().parse::<u64>().unwrap();
    (tx, output_index, offset)
}

pub fn format_outpoint_to_watch(
    transaction_identifier: &TransactionIdentifier,
    output_index: usize,
) -> String {
    format!(
        "{}:{}",
        transaction_identifier.get_hash_bytes_str(),
        output_index
    )
}

pub fn parse_inscription_id(inscription_id: &str) -> (TransactionIdentifier, usize) {
    let comps: Vec<&str> = inscription_id.split("i").collect();
    let tx = TransactionIdentifier::new(&comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().unwrap();
    (tx, output_index)
}

pub fn parse_outpoint_to_watch(outpoint_to_watch: &str) -> (TransactionIdentifier, usize) {
    let comps: Vec<&str> = outpoint_to_watch.split(":").collect();
    let tx = TransactionIdentifier::new(&comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().unwrap();
    (tx, output_index)
}

pub fn retrieve_satoshi_point_using_lazy_storage(
    blocks_db_dir: &PathBuf,
    block_identifier: &BlockIdentifier,
    transaction_identifier: &TransactionIdentifier,
    inscription_input_index: usize,
    inscription_number: i64,
    traversals_cache: Arc<
        DashMap<(u32, [u8; 8]), LazyBlockTransaction, BuildHasherDefault<FxHasher>>,
    >,
    ctx: &Context,
) -> Result<TraversalResult, String> {
    let mut inscription_offset_intra_output = 0;
    let mut inscription_output_index: usize = 0;
    let mut ordinal_offset = 0;
    let mut ordinal_block_number = block_identifier.index as u32;
    let txid = transaction_identifier.get_8_hash_bytes();

    let mut blocks_db = open_readonly_hord_db_conn_rocks_db_loop(&blocks_db_dir, &ctx);

    let (sats_ranges, inscription_offset_cross_outputs) = match traversals_cache
        .get(&(block_identifier.index as u32, txid.clone()))
    {
        Some(entry) => {
            let tx = entry.value();
            (
                tx.get_sat_ranges(),
                tx.get_cumulated_sats_in_until_input_index(inscription_input_index),
            )
        }
        None => {
            let mut attempt = 0;
            loop {
                match find_lazy_block_at_block_height(
                    ordinal_block_number,
                    3,
                    false,
                    &blocks_db,
                    &ctx,
                ) {
                    None => {
                        if attempt < 3 {
                            attempt += 1;
                            blocks_db =
                                open_readonly_hord_db_conn_rocks_db_loop(&blocks_db_dir, &ctx);
                        } else {
                            return Err(format!("block #{ordinal_block_number} not in database"));
                        }
                    }
                    Some(block) => match block.find_and_serialize_transaction_with_txid(&txid) {
                        Some(tx) => {
                            let sats_ranges = tx.get_sat_ranges();
                            let inscription_offset_cross_outputs =
                                tx.get_cumulated_sats_in_until_input_index(inscription_input_index);
                            traversals_cache.insert((ordinal_block_number, txid.clone()), tx);
                            break (sats_ranges, inscription_offset_cross_outputs);
                        }
                        None => return Err(format!("txid not in block #{ordinal_block_number}")),
                    },
                }
            }
        }
    };

    for (i, (min, max)) in sats_ranges.into_iter().enumerate() {
        if inscription_offset_cross_outputs >= min && inscription_offset_cross_outputs < max {
            inscription_output_index = i;
            inscription_offset_intra_output = inscription_offset_cross_outputs - min;
        }
    }
    ctx.try_log(|logger| {
        slog::info!(
            logger,
            "Computing ordinal number for Satoshi point {} ({}:0 -> {}:{}/{})  (block #{})",
            transaction_identifier.hash,
            inscription_input_index,
            inscription_output_index,
            inscription_offset_intra_output,
            inscription_offset_cross_outputs,
            block_identifier.index
        )
    });

    let mut tx_cursor: ([u8; 8], usize) = (txid, inscription_input_index);
    let mut hops: u32 = 0;

    loop {
        hops += 1;
        if hops as u64 > block_identifier.index {
            return Err(format!(
                "Unable to process transaction {} detected after {hops} iterations. Manual investigation required",
                transaction_identifier.hash
            ));
        }

        if let Some(cached_tx) = traversals_cache.get(&(ordinal_block_number, tx_cursor.0)) {
            let tx = cached_tx.value();

            let mut next_found_in_cache = false;
            let mut sats_out = 0;
            for (index, output_value) in tx.outputs.iter().enumerate() {
                if index == tx_cursor.1 {
                    break;
                }
                sats_out += output_value;
            }
            sats_out += ordinal_offset;

            let mut sats_in = 0;
            for input in tx.inputs.iter() {
                sats_in += input.txin_value;

                if sats_out < sats_in {
                    ordinal_offset = sats_out - (sats_in - input.txin_value);
                    ordinal_block_number = input.block_height;
                    tx_cursor = (input.txin.clone(), input.vout as usize);
                    next_found_in_cache = true;
                    break;
                }
            }

            if next_found_in_cache {
                continue;
            }

            if sats_in == 0 {
                ctx.try_log(|logger| {
                    slog::error!(
                        logger,
                        "Transaction {} is originating from a non spending transaction",
                        transaction_identifier.hash
                    )
                });
                return Ok(TraversalResult {
                    inscription_number: 0,
                    ordinal_number: 0,
                    transfers: 0,
                    inscription_input_index,
                    transaction_identifier_inscription: transaction_identifier.clone(),
                    transfer_data: TransferData {
                        inscription_offset_intra_output,
                        transaction_identifier_location: transaction_identifier.clone(),
                        output_index: inscription_output_index,
                    },
                });
            }
        }

        let lazy_block = {
            let mut attempt = 0;
            loop {
                match find_lazy_block_at_block_height(
                    ordinal_block_number,
                    3,
                    false,
                    &blocks_db,
                    &ctx,
                ) {
                    Some(block) => break block,
                    None => {
                        if attempt < 3 {
                            attempt += 1;
                            blocks_db =
                                open_readonly_hord_db_conn_rocks_db_loop(&blocks_db_dir, &ctx);
                        } else {
                            return Err(format!("block #{ordinal_block_number} not in database"));
                        }
                    }
                }
            }
        };

        let coinbase_txid = lazy_block.get_coinbase_txid();
        let txid = tx_cursor.0;

        // evaluate exit condition: did we reach the **final** coinbase transaction
        if coinbase_txid.eq(&txid) {
            let subsidy = Height(ordinal_block_number.into()).subsidy();
            if ordinal_offset.lt(&subsidy) {
                // Great!
                break;
            }

            // loop over the transaction fees to detect the right range
            let mut accumulated_fees = subsidy;

            for tx in lazy_block.iter_tx() {
                let mut total_in = 0;
                for input in tx.inputs.iter() {
                    total_in += input.txin_value;
                }

                let mut total_out = 0;
                for output_value in tx.outputs.iter() {
                    total_out += output_value;
                }

                let fee = total_in - total_out;
                if accumulated_fees + fee > ordinal_offset {
                    // We are looking at the right transaction
                    // Retraverse the inputs to select the index to be picked
                    let offset_within_fee = ordinal_offset - accumulated_fees;
                    total_out += offset_within_fee;
                    let mut sats_in = 0;

                    for input in tx.inputs.into_iter() {
                        sats_in += input.txin_value;

                        if sats_in >= total_out {
                            ordinal_offset = total_out - (sats_in - input.txin_value);
                            ordinal_block_number = input.block_height;
                            tx_cursor = (input.txin.clone(), input.vout as usize);
                            break;
                        }
                    }
                    break;
                } else {
                    accumulated_fees += fee;
                }
            }
        } else {
            // isolate the target transaction
            let lazy_tx = match lazy_block.find_and_serialize_transaction_with_txid(&txid) {
                Some(entry) => entry,
                None => unreachable!(),
            };

            let mut sats_out = 0;
            for (index, output_value) in lazy_tx.outputs.iter().enumerate() {
                if index == tx_cursor.1 {
                    break;
                }
                sats_out += output_value;
            }
            sats_out += ordinal_offset;

            let mut sats_in = 0;
            for input in lazy_tx.inputs.iter() {
                sats_in += input.txin_value;

                if sats_out < sats_in {
                    traversals_cache.insert((ordinal_block_number, tx_cursor.0), lazy_tx.clone());
                    ordinal_offset = sats_out - (sats_in - input.txin_value);
                    ordinal_block_number = input.block_height;
                    tx_cursor = (input.txin.clone(), input.vout as usize);
                    break;
                }
            }

            if sats_in == 0 {
                ctx.try_log(|logger| {
                    slog::error!(
                        logger,
                        "Transaction {} is originating from a non spending transaction",
                        transaction_identifier.hash
                    )
                });
                return Ok(TraversalResult {
                    inscription_number: 0,
                    ordinal_number: 0,
                    transfers: 0,
                    inscription_input_index,
                    transaction_identifier_inscription: transaction_identifier.clone(),
                    transfer_data: TransferData {
                        inscription_offset_intra_output,
                        transaction_identifier_location: transaction_identifier.clone(),
                        output_index: inscription_output_index,
                    },
                });
            }
        }
    }

    let height = Height(ordinal_block_number.into());
    let ordinal_number = height.starting_sat().0 + ordinal_offset + inscription_offset_intra_output;

    Ok(TraversalResult {
        inscription_number,
        ordinal_number,
        transfers: hops,
        inscription_input_index,
        transaction_identifier_inscription: transaction_identifier.clone(),
        transfer_data: TransferData {
            inscription_offset_intra_output,
            transaction_identifier_location: transaction_identifier.clone(),
            output_index: inscription_output_index,
        },
    })
}

#[derive(Debug)]
pub struct LazyBlock {
    pub bytes: Vec<u8>,
    pub tx_len: u16,
}

#[derive(Debug, Clone)]
pub struct LazyBlockTransaction {
    pub txid: [u8; 8],
    pub inputs: Vec<LazyBlockTransactionInput>,
    pub outputs: Vec<u64>,
}

impl LazyBlockTransaction {
    pub fn get_average_bytes_size() -> usize {
        TXID_LEN + 3 * LazyBlockTransactionInput::get_average_bytes_size() + 3 * SATS_LEN
    }

    pub fn get_sat_ranges(&self) -> Vec<(u64, u64)> {
        let mut sats_ranges = vec![];
        let mut bound = 0u64;
        for output_value in self.outputs.iter() {
            sats_ranges.push((bound, bound + output_value));
            bound += output_value;
        }
        sats_ranges
    }

    pub fn get_cumulated_sats_in_until_input_index(&self, input_index: usize) -> u64 {
        let mut cumulated_sats_in = 0;
        for (i, input) in self.inputs.iter().enumerate() {
            if i == input_index {
                break;
            }
            cumulated_sats_in += input.txin_value;
        }
        cumulated_sats_in
    }
}

#[derive(Debug, Clone)]
pub struct LazyBlockTransactionInput {
    pub txin: [u8; 8],
    pub block_height: u32,
    pub vout: u16,
    pub txin_value: u64,
}

impl LazyBlockTransactionInput {
    pub fn get_average_bytes_size() -> usize {
        TXID_LEN + SATS_LEN + 4 + 2
    }
}

const TXID_LEN: usize = 8;
const SATS_LEN: usize = 8;
const INPUT_SIZE: usize = TXID_LEN + 4 + 2 + SATS_LEN;
const OUTPUT_SIZE: usize = 8;

impl LazyBlock {
    pub fn new(bytes: Vec<u8>) -> LazyBlock {
        let tx_len = u16::from_be_bytes([bytes[0], bytes[1]]);
        LazyBlock { bytes, tx_len }
    }

    pub fn get_coinbase_data_pos(&self) -> usize {
        (2 + self.tx_len * 2 * 2) as usize
    }

    pub fn get_u64_at_pos(&self, pos: usize) -> u64 {
        u64::from_be_bytes([
            self.bytes[pos],
            self.bytes[pos + 1],
            self.bytes[pos + 2],
            self.bytes[pos + 3],
            self.bytes[pos + 4],
            self.bytes[pos + 5],
            self.bytes[pos + 6],
            self.bytes[pos + 7],
        ])
    }

    pub fn get_coinbase_txid(&self) -> &[u8] {
        let pos = self.get_coinbase_data_pos();
        &self.bytes[pos..pos + TXID_LEN]
    }

    pub fn get_coinbase_sats(&self) -> u64 {
        let pos = self.get_coinbase_data_pos() + TXID_LEN;
        self.get_u64_at_pos(pos)
    }

    pub fn get_transactions_data_pos(&self) -> usize {
        self.get_coinbase_data_pos() + TXID_LEN + SATS_LEN
    }

    pub fn get_transaction_format(&self, index: u16) -> (u16, u16, usize) {
        let inputs_len_pos = (2 + index * 2 * 2) as usize;
        let inputs =
            u16::from_be_bytes([self.bytes[inputs_len_pos], self.bytes[inputs_len_pos + 1]]);
        let outputs = u16::from_be_bytes([
            self.bytes[inputs_len_pos + 2],
            self.bytes[inputs_len_pos + 3],
        ]);
        let size = TXID_LEN + (inputs as usize * INPUT_SIZE) + (outputs as usize * OUTPUT_SIZE);
        (inputs, outputs, size)
    }

    pub fn get_lazy_transaction_at_pos(
        &self,
        cursor: &mut Cursor<&Vec<u8>>,
        txid: [u8; 8],
        inputs_len: u16,
        outputs_len: u16,
    ) -> LazyBlockTransaction {
        let mut inputs = Vec::with_capacity(inputs_len as usize);
        for _ in 0..inputs_len {
            let mut txin = [0u8; 8];
            cursor.read_exact(&mut txin).expect("data corrupted");
            let mut block_height = [0u8; 4];
            cursor
                .read_exact(&mut block_height)
                .expect("data corrupted");
            let mut vout = [0u8; 2];
            cursor.read_exact(&mut vout).expect("data corrupted");
            let mut txin_value = [0u8; 8];
            cursor.read_exact(&mut txin_value).expect("data corrupted");
            inputs.push(LazyBlockTransactionInput {
                txin: txin,
                block_height: u32::from_be_bytes(block_height),
                vout: u16::from_be_bytes(vout),
                txin_value: u64::from_be_bytes(txin_value),
            });
        }
        let mut outputs = Vec::with_capacity(outputs_len as usize);
        for _ in 0..outputs_len {
            let mut value = [0u8; 8];
            cursor.read_exact(&mut value).expect("data corrupted");
            outputs.push(u64::from_be_bytes(value))
        }
        LazyBlockTransaction {
            txid,
            inputs,
            outputs,
        }
    }

    pub fn find_and_serialize_transaction_with_txid(
        &self,
        searched_txid: &[u8],
    ) -> Option<LazyBlockTransaction> {
        // println!("{:?}", hex::encode(searched_txid));
        let mut entry = None;
        let mut cursor = Cursor::new(&self.bytes);
        let mut cumulated_offset = 0;
        let mut i = 0;
        while entry.is_none() {
            let pos = self.get_transactions_data_pos() + cumulated_offset;
            let (inputs_len, outputs_len, size) = self.get_transaction_format(i);
            // println!("{inputs_len} / {outputs_len} / {size}");
            cursor.set_position(pos as u64);
            let mut txid = [0u8; 8]; // todo 20 bytes
            let _ = cursor.read_exact(&mut txid);
            // println!("-> {}", hex::encode(txid));
            if searched_txid.eq(&txid) {
                entry = Some(self.get_lazy_transaction_at_pos(
                    &mut cursor,
                    txid,
                    inputs_len,
                    outputs_len,
                ));
            } else {
                cumulated_offset += size;
                i += 1;
                if i >= self.tx_len {
                    break;
                }
            }
        }
        entry
    }

    pub fn iter_tx(&self) -> LazyBlockTransactionIterator {
        LazyBlockTransactionIterator::new(&self)
    }

    pub fn from_full_block(block: &BitcoinBlockFullBreakdown) -> std::io::Result<LazyBlock> {
        let mut buffer = vec![];
        // Number of transactions in the block (not including coinbase)
        let tx_len = block.tx.len() as u16 - 1;
        buffer.write(&tx_len.to_be_bytes())?;
        // For each transaction:
        for tx in block.tx.iter().skip(1) {
            let inputs_len = tx.vin.len() as u16;
            let outputs_len = tx.vout.len() as u16;
            // Number of inputs
            buffer.write(&inputs_len.to_be_bytes())?;
            // Number of outputs
            buffer.write(&outputs_len.to_be_bytes())?;
        }
        // Coinbase transaction txid -  8 first bytes
        let coinbase_txid = {
            let txid = hex::decode(block.tx[0].txid.to_string()).unwrap();
            [
                txid[0], txid[1], txid[2], txid[3], txid[4], txid[5], txid[6], txid[7],
            ]
        };
        buffer.write_all(&coinbase_txid)?;
        // Coinbase transaction value
        let mut coinbase_value = 0;
        for coinbase_output in block.tx[0].vout.iter() {
            coinbase_value += coinbase_output.value.to_sat();
        }
        buffer.write(&coinbase_value.to_be_bytes())?;
        // For each transaction:
        for tx in block.tx.iter().skip(1) {
            // txid - 8 first bytes
            let txid = {
                let txid = hex::decode(tx.txid.to_string()).unwrap();
                [
                    txid[0], txid[1], txid[2], txid[3], txid[4], txid[5], txid[6], txid[7],
                ]
            };
            buffer.write_all(&txid)?;
            // For each transaction input:
            for input in tx.vin.iter() {
                // txin - 8 first bytes
                let txin = {
                    let txid = hex::decode(input.txid.unwrap().to_string()).unwrap();
                    [
                        txid[0], txid[1], txid[2], txid[3], txid[4], txid[5], txid[6], txid[7],
                    ]
                };
                buffer.write_all(&txin)?;
                // txin's block height
                let block_height = input.prevout.as_ref().unwrap().height as u32;
                buffer.write(&block_height.to_be_bytes())?;
                // txin's vout index
                let vout = input.vout.unwrap() as u16;
                buffer.write(&vout.to_be_bytes())?;
                // txin's sats value
                let sats = input.prevout.as_ref().unwrap().value.to_sat();
                buffer.write(&sats.to_be_bytes())?;
            }
            // For each transaction output:
            for output in tx.vout.iter() {
                let sats = output.value.to_sat();
                buffer.write(&sats.to_be_bytes())?;
            }
        }
        Ok(Self::new(buffer))
    }

    pub fn from_standardized_block(block: &BitcoinBlockData) -> std::io::Result<LazyBlock> {
        let mut buffer = vec![];
        // Number of transactions in the block (not including coinbase)
        let tx_len = block.transactions.len() as u16 - 1;
        buffer.write(&tx_len.to_be_bytes())?;
        // For each transaction:
        for tx in block.transactions.iter().skip(1) {
            let inputs_len = tx.metadata.inputs.len() as u16;
            let outputs_len = tx.metadata.outputs.len() as u16;
            // Number of inputs
            buffer.write(&inputs_len.to_be_bytes())?;
            // Number of outputs
            buffer.write(&outputs_len.to_be_bytes())?;
        }
        // Coinbase transaction txid -  8 first bytes
        let coinbase_txid = block.transactions[0]
            .transaction_identifier
            .get_8_hash_bytes();
        buffer.write_all(&coinbase_txid)?;
        // Coinbase transaction value
        let mut coinbase_value = 0;
        for coinbase_output in block.transactions[0].metadata.outputs.iter() {
            coinbase_value += coinbase_output.value;
        }
        buffer.write(&coinbase_value.to_be_bytes())?;
        // For each transaction:
        for tx in block.transactions.iter().skip(1) {
            // txid - 8 first bytes
            let txid = tx.transaction_identifier.get_8_hash_bytes();
            buffer.write_all(&txid)?;
            // For each transaction input:
            for input in tx.metadata.inputs.iter() {
                // txin - 8 first bytes
                let txin = input.previous_output.txid.get_8_hash_bytes();
                buffer.write_all(&txin)?;
                // txin's block height
                let block_height = input.previous_output.block_height as u32;
                buffer.write(&block_height.to_be_bytes())?;
                // txin's vout index
                let vout = input.previous_output.vout as u16;
                buffer.write(&vout.to_be_bytes())?;
                // txin's sats value
                let sats = input.previous_output.value;
                buffer.write(&sats.to_be_bytes())?;
            }
            // For each transaction output:
            for output in tx.metadata.outputs.iter() {
                let sats = output.value;
                buffer.write(&sats.to_be_bytes())?;
            }
        }
        Ok(Self::new(buffer))
    }
}

pub struct LazyBlockTransactionIterator<'a> {
    lazy_block: &'a LazyBlock,
    tx_index: u16,
    cumulated_offset: usize,
}

impl<'a> LazyBlockTransactionIterator<'a> {
    pub fn new(lazy_block: &'a LazyBlock) -> LazyBlockTransactionIterator<'a> {
        LazyBlockTransactionIterator {
            lazy_block,
            tx_index: 0,
            cumulated_offset: 0,
        }
    }
}

impl<'a> Iterator for LazyBlockTransactionIterator<'a> {
    type Item = LazyBlockTransaction;

    fn next(&mut self) -> Option<LazyBlockTransaction> {
        if self.tx_index >= self.lazy_block.tx_len {
            return None;
        }
        let pos = self.lazy_block.get_transactions_data_pos() + self.cumulated_offset;
        let (inputs_len, outputs_len, size) = self.lazy_block.get_transaction_format(self.tx_index);
        // println!("{inputs_len} / {outputs_len} / {size}");
        let mut cursor = Cursor::new(&self.lazy_block.bytes);
        cursor.set_position(pos as u64);
        let mut txid = [0u8; 8];
        let _ = cursor.read_exact(&mut txid);
        self.cumulated_offset += size;
        self.tx_index += 1;
        Some(self.lazy_block.get_lazy_transaction_at_pos(
            &mut cursor,
            txid,
            inputs_len,
            outputs_len,
        ))
    }
}
