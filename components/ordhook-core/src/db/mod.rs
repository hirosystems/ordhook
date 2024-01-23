use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::PathBuf,
    thread::sleep,
    time::Duration,
};

use rand::{thread_rng, Rng};

use rocksdb::{DBPinnableSlice, DB};
use rusqlite::{Connection, OpenFlags, ToSql, Transaction};
use std::io::Cursor;

use chainhook_sdk::{
    indexer::bitcoin::BitcoinBlockFullBreakdown,
    types::{
        BitcoinBlockData, BlockIdentifier, OrdinalInscriptionNumber, OrdinalInscriptionRevealData,
        OrdinalInscriptionTransferData, TransactionIdentifier,
    },
    utils::Context,
};

use crate::{
    core::protocol::inscription_parsing::{
        get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
    },
    ord::sat::Sat,
};

pub fn get_default_ordhook_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("hord.sqlite");
    destination_path
}

pub fn open_readonly_ordhook_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let path = get_default_ordhook_db_file_path(&base_dir);
    let conn = open_existing_readonly_db(&path, ctx);
    Ok(conn)
}

pub fn open_readwrite_ordhook_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let db_path = get_default_ordhook_db_file_path(&base_dir);
    let conn = create_or_open_readwrite_db(&db_path, ctx);
    Ok(conn)
}

pub fn initialize_ordhook_db(base_dir: &PathBuf, ctx: &Context) -> Connection {
    let db_path = get_default_ordhook_db_file_path(&base_dir);
    let conn = create_or_open_readwrite_db(&db_path, ctx);
    // TODO: introduce initial output
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS inscriptions (
            inscription_id TEXT NOT NULL PRIMARY KEY,
            input_index INTEGER NOT NULL,
            block_height INTEGER NOT NULL,
            ordinal_number INTEGER NOT NULL,
            jubilee_inscription_number INTEGER NOT NULL,
            classic_inscription_number INTEGER NOT NULL
        )",
        [],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "Unable to create table inscriptions: {}",
                e.to_string()
            )
        });
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_ordinal_number ON inscriptions(ordinal_number);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_jubilee_inscription_number ON inscriptions(jubilee_inscription_number);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }

        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_classic_inscription_number ON inscriptions(classic_inscription_number);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }

        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_block_height ON inscriptions(block_height);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }
    }
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS locations (
            ordinal_number INTEGER NOT NULL,
            block_height INTEGER NOT NULL,
            tx_index INTEGER NOT NULL,
            outpoint_to_watch TEXT NOT NULL,
            offset INTEGER NOT NULL
        )",
        [],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "Unable to create table locations: {}",
                e.to_string()
            )
        });
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_indexed_on_block_height ON locations(block_height);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_indexed_on_outpoint_to_watch ON locations(outpoint_to_watch);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_indexed_on_ordinal_number ON locations(ordinal_number);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }
    }

    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS sequence_metadata (
            block_height INTEGER NOT NULL,
            nth_classic_pos_number INTEGER NOT NULL,
            nth_classic_neg_number INTEGER NOT NULL,
            nth_jubilee_number INTEGER NOT NULL
        )",
        [],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "Unable to create table sequence_metadata: {}",
                e.to_string()
            )
        });
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS sequence_metadata_indexed_on_block_height ON sequence_metadata(block_height);",
            [],
        ) {
            ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        }
    }

    conn
}

pub fn create_or_open_readwrite_db(db_path: &PathBuf, ctx: &Context) -> Connection {
    let open_flags = match std::fs::metadata(&db_path) {
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                // need to create
                if let Some(dirp) = PathBuf::from(&db_path).parent() {
                    std::fs::create_dir_all(dirp).unwrap_or_else(|e| {
                        ctx.try_log(|logger| error!(logger, "{}", e.to_string()));
                    });
                }
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
            } else {
                panic!("FATAL: could not stat {}", db_path.display());
            }
        }
        Ok(_md) => {
            // can just open
            OpenFlags::SQLITE_OPEN_READ_WRITE
        }
    };

    let conn = loop {
        match Connection::open_with_flags(&db_path, open_flags) {
            Ok(conn) => break conn,
            Err(e) => {
                ctx.try_log(|logger| error!(logger, "{}", e.to_string()));
            }
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    connection_with_defaults_pragma(conn)
}

pub fn open_existing_readonly_db(db_path: &PathBuf, ctx: &Context) -> Connection {
    let open_flags = match std::fs::metadata(db_path) {
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                panic!("FATAL: could not find {}", db_path.display());
            } else {
                panic!("FATAL: could not stat {}", db_path.display());
            }
        }
        Ok(_md) => {
            // can just open
            OpenFlags::SQLITE_OPEN_READ_ONLY
        }
    };

    let conn = loop {
        match Connection::open_with_flags(db_path, open_flags) {
            Ok(conn) => break conn,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to open hord.rocksdb: {}", e.to_string())
                });
            }
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    connection_with_defaults_pragma(conn)
}

fn connection_with_defaults_pragma(conn: Connection) -> Connection {
    conn.busy_timeout(std::time::Duration::from_secs(300))
        .expect("unable to set db timeout");
    conn.pragma_update(None, "mmap_size", 512 * 1024 * 1024)
        .expect("unable to enable mmap_size");
    conn.pragma_update(None, "cache_size", 512 * 1024 * 1024)
        .expect("unable to enable cache_size");
    conn.pragma_update(None, "journal_mode", &"WAL")
        .expect("unable to enable wal");
    conn
}

fn get_default_ordhook_db_file_path_rocks_db(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("hord.rocksdb");
    destination_path
}

fn rocks_db_default_options(ulimit: usize, memory_available: usize) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    // Per rocksdb's documentation:
    // If cache_index_and_filter_blocks is false (which is default),
    // the number of index/filter blocks is controlled by option max_open_files.
    // If you are certain that your ulimit will always be bigger than number of files in the database,
    // we recommend setting max_open_files to -1, which means infinity.
    // This option will preload all filter and index blocks and will not need to maintain LRU of files.
    // Setting max_open_files to -1 will get you the best possible performance.
    // Additional documentation:
    // https://betterprogramming.pub/navigating-the-minefield-of-rocksdb-configuration-options-246af1e1d3f9
    // opts.set_write_buffer_size(64 * 1024 * 1024);
    // opts.set_blob_file_size(1 * 1024 * 1024 * 1024);
    // opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.set_max_open_files(ulimit as i32);
    opts.create_if_missing(true);
    // opts.set_allow_mmap_reads(true);

    // set_arena_block_size

    // opts.optimize_for_point_lookup(1 * 1024 * 1024 * 1024);
    // opts.set_level_zero_stop_writes_trigger(64);
    // opts.set_level_zero_slowdown_writes_trigger(20);
    // opts.set_enable_blob_files(true);
    // opts.set_enable_blob_gc(true);
    // opts.set_use_fsync(false);
    // opts.set_bytes_per_sync(8388608);
    // opts.set_compaction_style(DBCompactionStyle::Universal);
    // opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    // opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
    opts
}

pub fn open_readonly_ordhook_db_conn_rocks_db(
    base_dir: &PathBuf,
    ulimit: usize,
    memory_available: usize,
    _ctx: &Context,
) -> Result<DB, String> {
    let path = get_default_ordhook_db_file_path_rocks_db(&base_dir);
    let mut opts = rocks_db_default_options(ulimit, memory_available);
    opts.set_disable_auto_compactions(true);
    opts.set_max_background_jobs(0);
    let db = DB::open_for_read_only(&opts, path, false)
        .map_err(|e| format!("unable to read hord.rocksdb: {}", e.to_string()))?;
    Ok(db)
}

pub fn open_ordhook_db_conn_rocks_db_loop(
    readwrite: bool,
    base_dir: &PathBuf,
    ulimit: usize,
    memory_available: usize,
    ctx: &Context,
) -> DB {
    let mut retries = 0;
    let blocks_db = loop {
        let res = if readwrite {
            open_readwrite_ordhook_db_conn_rocks_db(&base_dir, ulimit, memory_available, &ctx)
        } else {
            open_readonly_ordhook_db_conn_rocks_db(&base_dir, ulimit, memory_available, &ctx)
        };
        match res {
            Ok(db) => break db,
            Err(e) => {
                retries += 1;
                if retries > 10 {
                    ctx.try_log(|logger| {
                        warn!(logger, "Unable to open db: {e}. Retrying in 10s",);
                    });
                    sleep(Duration::from_secs(10));
                } else {
                    sleep(Duration::from_secs(2));
                }
                continue;
            }
        }
    };
    blocks_db
}

pub fn open_readwrite_ordhook_dbs(
    base_dir: &PathBuf,
    ulimit: usize,
    memory_available: usize,
    ctx: &Context,
) -> Result<(DB, Connection), String> {
    let blocks_db =
        open_ordhook_db_conn_rocks_db_loop(true, &base_dir, ulimit, memory_available, &ctx);
    let inscriptions_db = open_readwrite_ordhook_db_conn(&base_dir, &ctx)?;
    Ok((blocks_db, inscriptions_db))
}

fn open_readwrite_ordhook_db_conn_rocks_db(
    base_dir: &PathBuf,
    ulimit: usize,
    memory_available: usize,
    _ctx: &Context,
) -> Result<DB, String> {
    let path = get_default_ordhook_db_file_path_rocks_db(&base_dir);
    let opts = rocks_db_default_options(ulimit, memory_available);
    let db = DB::open(&opts, path)
        .map_err(|e| format!("unable to read-write hord.rocksdb: {}", e.to_string()))?;
    Ok(db)
}

pub fn insert_entry_in_blocks(
    block_height: u32,
    block_bytes: &[u8],
    update_tip: bool,
    blocks_db_rw: &DB,
    ctx: &Context,
) {
    let block_height_bytes = block_height.to_be_bytes();
    let mut retries = 0;
    loop {
        let res = blocks_db_rw.put(&block_height_bytes, block_bytes);
        match res {
            Ok(_) => break,
            Err(e) => {
                retries += 1;
                if retries > 10 {
                    ctx.try_log(|logger| {
                        error!(
                            logger,
                            "unable to insert block {block_height} ({}). will retry in 5 secs",
                            e.to_string()
                        );
                    });
                    sleep(Duration::from_secs(5));
                }
            }
        }
    }

    if update_tip {
        blocks_db_rw
            .put(b"metadata::last_insert", block_height_bytes)
            .expect("unable to insert metadata");
    }
}

pub fn find_last_block_inserted(blocks_db: &DB) -> u32 {
    match blocks_db.get(b"metadata::last_insert") {
        Ok(Some(bytes)) => u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        _ => 0,
    }
}

pub fn find_pinned_block_bytes_at_block_height<'a>(
    block_height: u32,
    retry: u8,
    blocks_db: &'a DB,
    ctx: &Context,
) -> Option<DBPinnableSlice<'a>> {
    let mut attempt = 1;
    // let mut read_options = rocksdb::ReadOptions::default();
    // read_options.fill_cache(true);
    // read_options.set_verify_checksums(false);
    let mut backoff: f64 = 1.0;
    let mut rng = thread_rng();
    loop {
        match blocks_db.get_pinned(block_height.to_be_bytes()) {
            Ok(Some(res)) => return Some(res),
            _ => {
                attempt += 1;
                backoff = 2.0 * backoff + (backoff * rng.gen_range(0.0..1.0));
                let duration = std::time::Duration::from_millis((backoff * 1_000.0) as u64);
                ctx.try_log(|logger| {
                    warn!(
                        logger,
                        "Unable to find block #{}, will retry in {:?}", block_height, duration
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

pub fn find_block_bytes_at_block_height<'a>(
    block_height: u32,
    retry: u8,
    blocks_db: &DB,
    ctx: &Context,
) -> Option<Vec<u8>> {
    let mut attempt = 1;
    // let mut read_options = rocksdb::ReadOptions::default();
    // read_options.fill_cache(true);
    // read_options.set_verify_checksums(false);
    let mut backoff: f64 = 1.0;
    let mut rng = thread_rng();

    loop {
        match blocks_db.get(block_height.to_be_bytes()) {
            Ok(Some(res)) => return Some(res),
            _ => {
                attempt += 1;
                backoff = 2.0 * backoff + (backoff * rng.gen_range(0.0..1.0));
                let duration = std::time::Duration::from_millis((backoff * 1_000.0) as u64);
                ctx.try_log(|logger| {
                    warn!(
                        logger,
                        "Unable to find block #{}, will retry in {:?}", block_height, duration
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

pub fn run_compaction(blocks_db_rw: &DB, lim: u32) {
    let gen = 0u32.to_be_bytes();
    let _ = blocks_db_rw.compact_range(Some(&gen), Some(&lim.to_be_bytes()));
}

pub fn find_missing_blocks(blocks_db: &DB, start: u32, end: u32, ctx: &Context) -> Vec<u32> {
    let mut missing_blocks = vec![];
    for i in start..=end {
        if find_pinned_block_bytes_at_block_height(i as u32, 0, &blocks_db, ctx).is_none() {
            missing_blocks.push(i);
        }
    }
    missing_blocks
}

pub fn remove_entry_from_blocks(block_height: u32, blocks_db_rw: &DB, ctx: &Context) {
    if let Err(e) = blocks_db_rw.delete(block_height.to_be_bytes()) {
        ctx.try_log(|logger| error!(logger, "{}", e.to_string()));
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
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO inscriptions (inscription_id, ordinal_number, jubilee_inscription_number, classic_inscription_number, block_height, input_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![&inscription_data.inscription_id, &inscription_data.ordinal_number, &inscription_data.inscription_number.jubilee, &inscription_data.inscription_number.classic, &block_identifier.index, &inscription_data.inscription_input_index],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
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
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO locations (ordinal_number, outpoint_to_watch, offset, block_height, tx_index) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![&inscription_data.ordinal_number, &outpoint_to_watch, offset, &block_identifier.index, &inscription_data.tx_index],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn update_inscriptions_with_block(
    block: &BitcoinBlockData,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    for inscription_data in get_inscriptions_revealed_in_block(&block).iter() {
        insert_entry_in_inscriptions(
            inscription_data,
            &block.block_identifier,
            inscriptions_db_conn_rw,
            &ctx,
        );
        insert_inscription_in_locations(
            &inscription_data,
            &block.block_identifier,
            &inscriptions_db_conn_rw,
            ctx,
        );
    }
}

pub fn update_locations_with_block(
    block: &BitcoinBlockData,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    for transfer_data in get_inscriptions_transferred_in_block(&block).iter() {
        insert_transfer_in_locations(
            &transfer_data,
            &block.block_identifier,
            &inscriptions_db_conn_rw,
            ctx,
        );
    }
}

pub fn update_sequence_metadata_with_block(
    block: &BitcoinBlockData,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    let mut nth_classic_pos_number = find_nth_classic_pos_number_at_block_height(
        &block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    )
    .unwrap_or(0);
    let mut nth_classic_neg_number = find_nth_classic_neg_number_at_block_height(
        &block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    )
    .unwrap_or(0);
    let mut nth_jubilee_number = find_nth_jubilee_number_at_block_height(
        &block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    )
    .unwrap_or(0);
    for inscription_data in get_inscriptions_revealed_in_block(&block).iter() {
        nth_classic_pos_number =
            nth_classic_pos_number.max(inscription_data.inscription_number.classic);
        nth_classic_neg_number =
            nth_classic_neg_number.min(inscription_data.inscription_number.classic);
        nth_jubilee_number = nth_jubilee_number.max(inscription_data.inscription_number.jubilee);
    }
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO sequence_metadata (block_height, nth_classic_pos_number, nth_classic_neg_number, nth_jubilee_number) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![&block.block_identifier.index, nth_classic_pos_number, nth_classic_neg_number, nth_jubilee_number],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to update sequence_metadata: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn insert_new_inscriptions_from_block_in_locations(
    block: &BitcoinBlockData,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    for inscription_data in get_inscriptions_revealed_in_block(&block).iter() {
        insert_inscription_in_locations(
            inscription_data,
            &block.block_identifier,
            inscriptions_db_conn_rw,
            &ctx,
        );
    }
}

pub fn insert_transfer_in_locations_tx(
    transfer_data: &OrdinalInscriptionTransferData,
    block_identifier: &BlockIdentifier,
    inscriptions_db_conn_rw: &Transaction,
    ctx: &Context,
) {
    let (tx, output_index, offset) = parse_satpoint_to_watch(&transfer_data.satpoint_post_transfer);
    let outpoint_to_watch = format_outpoint_to_watch(&tx, output_index);
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO locations (ordinal_number, outpoint_to_watch, offset, block_height, tx_index) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![&transfer_data.ordinal_number, &outpoint_to_watch, offset, &block_identifier.index, &transfer_data.tx_index],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
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
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO locations (ordinal_number, outpoint_to_watch, offset, block_height, tx_index) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![&transfer_data.ordinal_number, &outpoint_to_watch, offset, &block_identifier.index, &transfer_data.tx_index],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn perform_query_exists(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
) -> bool {
    let res = perform_query(query, args, db_conn, ctx, |_| true, true);
    !res.is_empty()
}

pub fn perform_query_one<F, T>(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
    mapping_func: F,
) -> Option<T>
where
    F: Fn(&rusqlite::Row<'_>) -> T,
{
    let mut res = perform_query(query, args, db_conn, ctx, mapping_func, true);
    match res.is_empty() {
        true => None,
        false => Some(res.remove(0)),
    }
}

pub fn perform_query_set<F, T>(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
    mapping_func: F,
) -> Vec<T>
where
    F: Fn(&rusqlite::Row<'_>) -> T,
{
    perform_query(query, args, db_conn, ctx, mapping_func, false)
}

fn perform_query<F, T>(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
    mapping_func: F,
    stop_at_first: bool,
) -> Vec<T>
where
    F: Fn(&rusqlite::Row<'_>) -> T,
{
    let mut results = vec![];
    loop {
        let mut stmt = match db_conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to prepare query {query}: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };

        match stmt.query(args) {
            Ok(mut rows) => loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        let r = mapping_func(row);
                        results.push(r);
                        if stop_at_first {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        ctx.try_log(|logger| {
                            warn!(
                                logger,
                                "unable to iterate over results from {query}: {}",
                                e.to_string()
                            )
                        });
                        std::thread::sleep(std::time::Duration::from_secs(5));
                        continue;
                    }
                }
            },
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to execute query {query}: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };
        break;
    }
    results
}

pub fn get_any_entry_in_ordinal_activities(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> bool {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT DISTINCT block_height FROM inscriptions WHERE block_height = ?";
    if perform_query_exists(query, args, db_conn, ctx) {
        return true;
    }

    let query = "SELECT DISTINCT block_height FROM locations WHERE block_height = ?";
    perform_query_exists(query, args, db_conn, ctx)
}

pub fn find_latest_inscription_block_height(
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<u64>, String> {
    let args: &[&dyn ToSql] = &[];
    let query = "SELECT block_height FROM inscriptions ORDER BY block_height DESC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let block_height: u64 = row.get(0).unwrap();
        block_height
    });
    Ok(entry)
}

pub fn find_initial_inscription_transfer_data(
    ordinal_number: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<TransferData>, String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let query = "SELECT outpoint_to_watch, offset, tx_index FROM locations WHERE ordinal_number = ? ORDER BY block_height ASC, tx_index ASC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let outpoint_to_watch: String = row.get(0).unwrap();
        let (transaction_identifier_location, output_index) =
            parse_outpoint_to_watch(&outpoint_to_watch);
        let inscription_offset_intra_output: u64 = row.get(1).unwrap();
        let tx_index: u64 = row.get(2).unwrap();
        TransferData {
            transaction_identifier_location,
            output_index,
            inscription_offset_intra_output,
            tx_index,
        }
    });
    Ok(entry)
}

pub fn find_latest_inscription_transfer_data(
    ordinal_number: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<TransferData>, String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let query = "SELECT outpoint_to_watch, offset, tx_index FROM locations WHERE ordinal_number = ? ORDER BY block_height DESC, tx_index DESC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let outpoint_to_watch: String = row.get(0).unwrap();
        let (transaction_identifier_location, output_index) =
            parse_outpoint_to_watch(&outpoint_to_watch);
        let inscription_offset_intra_output: u64 = row.get(1).unwrap();
        let tx_index: u64 = row.get(2).unwrap();
        TransferData {
            transaction_identifier_location,
            output_index,
            inscription_offset_intra_output,
            tx_index,
        }
    });
    Ok(entry)
}

pub fn find_latest_transfers_block_height(db_conn: &Connection, ctx: &Context) -> Option<u64> {
    let args: &[&dyn ToSql] = &[];
    let query = "SELECT block_height FROM locations ORDER BY block_height DESC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let block_height: u64 = row.get(0).unwrap();
        block_height
    });
    entry
}

#[derive(Debug, Clone)]
pub struct TransferData {
    pub inscription_offset_intra_output: u64,
    pub transaction_identifier_location: TransactionIdentifier,
    pub output_index: usize,
    pub tx_index: u64,
}

pub fn find_all_transfers_in_block(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> BTreeMap<u64, Vec<TransferData>> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];

    let mut stmt = loop {
        match db_conn.prepare("SELECT ordinal_number, offset, outpoint_to_watch, tx_index FROM locations WHERE block_height = ? ORDER BY tx_index ASC")
        {
            Ok(stmt) => break stmt,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to prepare query hord.sqlite: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };

    let mut results: BTreeMap<u64, Vec<TransferData>> = BTreeMap::new();
    let mut rows = loop {
        match stmt.query(args) {
            Ok(rows) => break rows,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to query hord.sqlite: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };
    loop {
        match rows.next() {
            Ok(Some(row)) => {
                let ordinal_number: u64 = row.get(0).unwrap();
                let inscription_offset_intra_output: u64 = row.get(1).unwrap();
                let outpoint_to_watch: String = row.get(2).unwrap();
                let tx_index: u64 = row.get(3).unwrap();
                let (transaction_identifier_location, output_index) =
                    parse_outpoint_to_watch(&outpoint_to_watch);
                let transfer = TransferData {
                    inscription_offset_intra_output,
                    transaction_identifier_location,
                    output_index,
                    tx_index,
                };
                results
                    .entry(ordinal_number)
                    .and_modify(|v| v.push(transfer.clone()))
                    .or_insert(vec![transfer]);
            }
            Ok(None) => break,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to query hord.sqlite: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
    return results;
}

pub fn find_all_inscription_transfers(
    inscription_id: &str,
    db_conn: &Connection,
    ctx: &Context,
) -> Vec<(TransferData, u64)> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let query = "SELECT offset, outpoint_to_watch, tx_index, block_height FROM locations WHERE inscription_id = ? ORDER BY block_height ASC, tx_index ASC";
    perform_query_set(query, args, db_conn, ctx, |row| {
        let inscription_offset_intra_output: u64 = row.get(0).unwrap();
        let outpoint_to_watch: String = row.get(1).unwrap();
        let tx_index: u64 = row.get(2).unwrap();
        let block_height: u64 = row.get(3).unwrap();

        let (transaction_identifier_location, output_index) =
            parse_outpoint_to_watch(&outpoint_to_watch);
        let transfer = TransferData {
            inscription_offset_intra_output,
            transaction_identifier_location,
            output_index,
            tx_index,
        };
        (transfer, block_height)
    })
}

pub fn find_nth_classic_pos_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT nth_classic_pos_number FROM sequence_metadata WHERE block_height < ? ORDER BY block_height DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
    .or_else(|| compute_nth_classic_pos_number_at_block_height(block_height, db_conn, ctx))
}

pub fn find_nth_classic_neg_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT nth_classic_neg_number FROM sequence_metadata WHERE block_height < ? ORDER BY block_height DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
    .or_else(|| compute_nth_classic_neg_number_at_block_height(block_height, db_conn, ctx))
}

pub fn find_nth_jubilee_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT nth_jubilee_number FROM sequence_metadata WHERE block_height < ? ORDER BY block_height DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
    .or_else(|| compute_nth_jubilee_number_at_block_height(block_height, db_conn, ctx))
}

pub fn compute_nth_jubilee_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    ctx.try_log(|logger| {
        warn!(
            logger,
            "Start computing latest_inscription_number at block height: {block_height}"
        )
    });
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT jubilee_inscription_number FROM inscriptions WHERE block_height < ? ORDER BY jubilee_inscription_number DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
}

pub fn compute_nth_classic_pos_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    ctx.try_log(|logger| {
        warn!(
            logger,
            "Start computing latest_inscription_number at block height: {block_height}"
        )
    });
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT classic_inscription_number FROM inscriptions WHERE block_height < ? ORDER BY classic_inscription_number DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
}

pub fn compute_nth_classic_neg_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    ctx.try_log(|logger| {
        warn!(
            logger,
            "Start computing nth_classic_neg_number at block height: {block_height}"
        )
    });
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT classic_inscription_number FROM inscriptions WHERE block_height < ? ORDER BY classic_inscription_number ASC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
}

pub fn find_blessed_inscription_with_ordinal_number(
    ordinal_number: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let query = "SELECT inscription_id FROM inscriptions WHERE ordinal_number = ? AND classic_inscription_number >= 0";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_id: String = row.get(0).unwrap();
        inscription_id
    })
}

pub fn find_inscription_with_id(
    inscription_id: &str,
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<(TraversalResult, u64)>, String> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let query = "SELECT classic_inscription_number, jubilee_inscription_number, ordinal_number, block_height, input_index FROM inscriptions WHERE inscription_id = ?";
    let entry = perform_query_one(query, args, db_conn, ctx, move |row| {
        let inscription_number = OrdinalInscriptionNumber {
            classic: row.get(0).unwrap(),
            jubilee: row.get(1).unwrap(),
        };
        let ordinal_number: u64 = row.get(2).unwrap();
        let block_height: u64 = row.get(3).unwrap();
        let inscription_input_index: usize = row.get(4).unwrap();
        let (transaction_identifier_inscription, _) = parse_inscription_id(inscription_id);
        (
            inscription_number,
            ordinal_number,
            inscription_input_index,
            transaction_identifier_inscription,
            block_height,
        )
    });

    let Some((
        inscription_number,
        ordinal_number,
        inscription_input_index,
        transaction_identifier_inscription,
        block_height,
    )) = entry
    else {
        return Err(format!(
            "unable to retrieve inscription for {inscription_id}"
        ));
    };

    Ok(Some((
        TraversalResult {
            inscription_number,
            ordinal_number,
            inscription_input_index,
            transaction_identifier_inscription,
            transfers: 0,
        },
        block_height,
    )))
}

pub fn find_all_inscriptions_in_block(
    block_height: &u64,
    inscriptions_db_tx: &Connection,
    ctx: &Context,
) -> BTreeMap<String, TraversalResult> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];

    let mut stmt = loop {
        match inscriptions_db_tx.prepare("SELECT classic_inscription_number, jubilee_inscription_number, ordinal_number, inscription_id, input_index FROM inscriptions where block_height = ?")
        {
            Ok(stmt) => break stmt,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to prepare query hord.sqlite: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };

    let mut rows = loop {
        match stmt.query(args) {
            Ok(rows) => break rows,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to query hord.sqlite: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };
    let mut results = BTreeMap::new();
    loop {
        match rows.next() {
            Ok(Some(row)) => {
                let inscription_number = OrdinalInscriptionNumber {
                    classic: row.get(0).unwrap(),
                    jubilee: row.get(1).unwrap(),
                };
                let ordinal_number: u64 = row.get(2).unwrap();
                let inscription_id: String = row.get(3).unwrap();
                let inscription_input_index: usize = row.get(4).unwrap();
                let (transaction_identifier_inscription, _) =
                    { parse_inscription_id(&inscription_id) };
                let traversal = TraversalResult {
                    inscription_number,
                    ordinal_number,
                    inscription_input_index,
                    transfers: 0,
                    transaction_identifier_inscription: transaction_identifier_inscription.clone(),
                };
                results.insert(inscription_id, traversal);
            }
            Ok(None) => break,
            Err(e) => {
                ctx.try_log(|logger| {
                    warn!(logger, "unable to query hord.sqlite: {}", e.to_string())
                });
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
    return results;
}

#[derive(Clone, Debug)]
pub struct WatchedSatpoint {
    pub ordinal_number: u64,
    pub offset: u64,
}

pub fn find_inscriptions_at_wached_outpoint(
    outpoint: &str,
    db_conn: &Connection,
    ctx: &Context,
) -> Vec<WatchedSatpoint> {
    let args: &[&dyn ToSql] = &[&outpoint.to_sql().unwrap()];
    let query = "SELECT ordinal_number, offset FROM locations WHERE outpoint_to_watch = ? ORDER BY offset ASC";
    perform_query_set(query, args, db_conn, ctx, |row| {
        let ordinal_number: u64 = row.get(0).unwrap();
        let offset: u64 = row.get(1).unwrap();
        WatchedSatpoint {
            ordinal_number,
            offset,
        }
    })
}

pub fn delete_inscriptions_in_block_range(
    start_block: u32,
    end_block: u32,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM inscriptions WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM locations WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM sequence_metadata WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn remove_entry_from_inscriptions(
    inscription_id: &str,
    inscriptions_db_rw_conn: &Connection,
    ctx: &Context,
) {
    while let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM inscriptions WHERE inscription_id = ?1",
        rusqlite::params![&inscription_id],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM locations WHERE inscription_id = ?1",
        rusqlite::params![&inscription_id],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn remove_entries_from_locations_at_block_height(
    block_height: &u64,
    inscriptions_db_rw_conn: &Transaction,
    ctx: &Context,
) {
    while let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM locations WHERE block_height = ?1",
        rusqlite::params![&block_height],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn insert_entry_in_locations(
    inscription_id: &str,
    block_height: u64,
    transfer_data: &TransferData,
    inscriptions_db_rw_conn: &Transaction,
    ctx: &Context,
) {
    let outpoint_to_watch = format_outpoint_to_watch(
        &transfer_data.transaction_identifier_location,
        transfer_data.output_index,
    );
    while let Err(e) = inscriptions_db_rw_conn.execute(
        "INSERT INTO locations (inscription_id, outpoint_to_watch, offset, block_height, tx_index) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![&inscription_id, &outpoint_to_watch, &transfer_data.inscription_offset_intra_output, &block_height, &transfer_data.tx_index],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query hord.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn delete_data_in_ordhook_db(
    start_block: u64,
    end_block: u64,
    blocks_db_rw: &DB,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) -> Result<(), String> {
    ctx.try_log(|logger| {
        info!(
            logger,
            "Deleting entries from block #{start_block} to block #{end_block}"
        )
    });
    delete_blocks_in_block_range(start_block as u32, end_block as u32, blocks_db_rw, &ctx);
    ctx.try_log(|logger| {
        info!(
            logger,
            "Deleting inscriptions and locations from block #{start_block} to block #{end_block}"
        )
    });
    delete_inscriptions_in_block_range(
        start_block as u32,
        end_block as u32,
        inscriptions_db_conn_rw,
        &ctx,
    );
    Ok(())
}

#[derive(Clone, Debug)]
pub struct TraversalResult {
    pub inscription_number: OrdinalInscriptionNumber,
    pub inscription_input_index: usize,
    pub transaction_identifier_inscription: TransactionIdentifier,
    pub ordinal_number: u64,
    pub transfers: u32,
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

    pub fn get_inscription_id(&self) -> String {
        format!(
            "{}i{}",
            self.transaction_identifier_inscription.get_hash_bytes_str(),
            self.inscription_input_index
        )
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

pub fn format_inscription_id(
    transaction_identifier: &TransactionIdentifier,
    inscription_subindex: usize,
) -> String {
    format!(
        "{}i{}",
        transaction_identifier.get_hash_bytes_str(),
        inscription_subindex,
    )
}

pub fn parse_satpoint_to_watch(outpoint_to_watch: &str) -> (TransactionIdentifier, usize, u64) {
    let comps: Vec<&str> = outpoint_to_watch.split(":").collect();
    let tx = TransactionIdentifier::new(comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().expect(&format!(
        "fatal: unable to extract output_index from outpoint {}",
        outpoint_to_watch
    ));
    let offset = comps[2].to_string().parse::<u64>().expect(&format!(
        "fatal: unable to extract offset from outpoint {}",
        outpoint_to_watch
    ));
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
    let output_index = comps[1].to_string().parse::<usize>().expect(&format!(
        "fatal: unable to extract output_index from inscription_id {}",
        inscription_id
    ));
    (tx, output_index)
}

pub fn parse_outpoint_to_watch(outpoint_to_watch: &str) -> (TransactionIdentifier, usize) {
    let comps: Vec<&str> = outpoint_to_watch.split(":").collect();
    let tx = TransactionIdentifier::new(&comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().expect(&format!(
        "fatal: unable to extract output_index from outpoint {}",
        outpoint_to_watch
    ));
    (tx, output_index)
}

#[derive(Debug)]
pub struct BlockBytesCursor<'a> {
    pub bytes: &'a [u8],
    pub tx_len: u16,
}

#[derive(Debug, Clone)]
pub struct TransactionBytesCursor {
    pub txid: [u8; 8],
    pub inputs: Vec<TransactionInputBytesCursor>,
    pub outputs: Vec<u64>,
}

impl TransactionBytesCursor {
    pub fn get_average_bytes_size() -> usize {
        TXID_LEN + 3 * TransactionInputBytesCursor::get_average_bytes_size() + 3 * SATS_LEN
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
pub struct TransactionInputBytesCursor {
    pub txin: [u8; 8],
    pub block_height: u32,
    pub vout: u16,
    pub txin_value: u64,
}

impl TransactionInputBytesCursor {
    pub fn get_average_bytes_size() -> usize {
        TXID_LEN + SATS_LEN + 4 + 2
    }
}

const TXID_LEN: usize = 8;
const SATS_LEN: usize = 8;
const INPUT_SIZE: usize = TXID_LEN + 4 + 2 + SATS_LEN;
const OUTPUT_SIZE: usize = 8;

impl<'a> BlockBytesCursor<'a> {
    pub fn new(bytes: &[u8]) -> BlockBytesCursor {
        let tx_len = u16::from_be_bytes([bytes[0], bytes[1]]);
        BlockBytesCursor { bytes, tx_len }
    }

    pub fn get_coinbase_data_pos(&self) -> usize {
        (2 + self.tx_len * 2 * 2) as usize
    }

    pub fn get_coinbase_outputs_len(&self) -> usize {
        u16::from_be_bytes([self.bytes[4], self.bytes[5]]) as usize
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

    pub fn get_transactions_data_pos(&self) -> usize {
        self.get_coinbase_data_pos()
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

    pub fn get_transaction_bytes_cursor_at_pos(
        &self,
        cursor: &mut Cursor<&[u8]>,
        txid: [u8; 8],
        inputs_len: u16,
        outputs_len: u16,
    ) -> TransactionBytesCursor {
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
            inputs.push(TransactionInputBytesCursor {
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
        TransactionBytesCursor {
            txid,
            inputs,
            outputs,
        }
    }

    pub fn find_and_serialize_transaction_with_txid(
        &self,
        searched_txid: &[u8],
    ) -> Option<TransactionBytesCursor> {
        // println!("{:?}", hex::encode(searched_txid));
        let mut entry = None;
        let mut cursor = Cursor::new(self.bytes);
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
                entry = Some(self.get_transaction_bytes_cursor_at_pos(
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

    pub fn iter_tx(&self) -> TransactionBytesCursorIterator {
        TransactionBytesCursorIterator::new(&self)
    }

    pub fn from_full_block<'b>(block: &BitcoinBlockFullBreakdown) -> std::io::Result<Vec<u8>> {
        let mut buffer = vec![];
        // Number of transactions in the block (not including coinbase)
        let tx_len = block.tx.len() as u16;
        buffer.write(&tx_len.to_be_bytes())?;
        // For each transaction:
        let u16_max = u16::MAX as usize;
        for (i, tx) in block.tx.iter().enumerate() {
            let mut inputs_len = if tx.vin.len() > u16_max {
                0
            } else {
                tx.vin.len() as u16
            };
            let outputs_len = if tx.vout.len() > u16_max {
                0
            } else {
                tx.vout.len() as u16
            };
            if i == 0 {
                inputs_len = 0;
            }
            // Number of inputs
            buffer.write(&inputs_len.to_be_bytes())?;
            // Number of outputs
            buffer.write(&outputs_len.to_be_bytes())?;
        }
        // For each transaction:
        for tx in block.tx.iter() {
            // txid - 8 first bytes
            let txid = {
                let txid = hex::decode(tx.txid.to_string()).unwrap();
                [
                    txid[0], txid[1], txid[2], txid[3], txid[4], txid[5], txid[6], txid[7],
                ]
            };
            buffer.write_all(&txid)?;

            let inputs_len = if tx.vin.len() > u16_max {
                0
            } else {
                tx.vin.len() as usize
            };
            let outputs_len = if tx.vout.len() > u16_max {
                0
            } else {
                tx.vout.len() as usize
            };

            // For each transaction input:
            for i in 0..inputs_len {
                let input = &tx.vin[i];
                // txin - 8 first bytes
                let Some(input_txid) = input.txid.as_ref() else {
                    continue;
                };
                let txin = {
                    let txid = hex::decode(input_txid).unwrap();
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
            for i in 0..outputs_len {
                let output = &tx.vout[i];
                let sats = output.value.to_sat();
                buffer.write(&sats.to_be_bytes())?;
            }
        }
        Ok(buffer)
    }

    pub fn from_standardized_block<'b>(block: &BitcoinBlockData) -> std::io::Result<Vec<u8>> {
        let mut buffer = vec![];
        // Number of transactions in the block (not including coinbase)
        let tx_len = block.transactions.len() as u16;
        buffer.write(&tx_len.to_be_bytes())?;
        // For each transaction:
        for (i, tx) in block.transactions.iter().enumerate() {
            let inputs_len = if i > 0 {
                tx.metadata.inputs.len() as u16
            } else {
                0
            };
            let outputs_len = tx.metadata.outputs.len() as u16;
            // Number of inputs
            buffer.write(&inputs_len.to_be_bytes())?;
            // Number of outputs
            buffer.write(&outputs_len.to_be_bytes())?;
        }
        // For each transaction:
        for (i, tx) in block.transactions.iter().enumerate() {
            // txid - 8 first bytes
            let txid = tx.transaction_identifier.get_8_hash_bytes();
            buffer.write_all(&txid)?;
            // For each non coinbase transaction input:
            if i > 0 {
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
            }
            // For each transaction output:
            for output in tx.metadata.outputs.iter() {
                let sats = output.value;
                buffer.write(&sats.to_be_bytes())?;
            }
        }
        Ok(buffer)
    }
}

pub struct TransactionBytesCursorIterator<'a> {
    block_bytes_cursor: &'a BlockBytesCursor<'a>,
    tx_index: u16,
    cumulated_offset: usize,
}

impl<'a> TransactionBytesCursorIterator<'a> {
    pub fn new(block_bytes_cursor: &'a BlockBytesCursor) -> TransactionBytesCursorIterator<'a> {
        TransactionBytesCursorIterator {
            block_bytes_cursor,
            tx_index: 0,
            cumulated_offset: 0,
        }
    }
}

impl<'a> Iterator for TransactionBytesCursorIterator<'a> {
    type Item = TransactionBytesCursor;

    fn next(&mut self) -> Option<TransactionBytesCursor> {
        if self.tx_index >= self.block_bytes_cursor.tx_len {
            return None;
        }
        let pos = self.block_bytes_cursor.get_transactions_data_pos() + self.cumulated_offset;
        let (inputs_len, outputs_len, size) = self
            .block_bytes_cursor
            .get_transaction_format(self.tx_index);
        // println!("{inputs_len} / {outputs_len} / {size}");
        let mut cursor = Cursor::new(self.block_bytes_cursor.bytes);
        cursor.set_position(pos as u64);
        let mut txid = [0u8; 8];
        let _ = cursor.read_exact(&mut txid);
        self.cumulated_offset += size;
        self.tx_index += 1;
        Some(self.block_bytes_cursor.get_transaction_bytes_cursor_at_pos(
            &mut cursor,
            txid,
            inputs_len,
            outputs_len,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainhook_sdk::{
        indexer::bitcoin::{parse_downloaded_block, standardize_bitcoin_block},
        types::BitcoinNetwork,
    };

    #[test]
    fn test_block_cursor_roundtrip() {
        let ctx = Context::empty();
        let block = include_str!("./fixtures/blocks_json/279671.json");
        let decoded_block =
            parse_downloaded_block(block.as_bytes().to_vec()).expect("unable to decode block");
        let standardized_block =
            standardize_bitcoin_block(decoded_block.clone(), &BitcoinNetwork::Mainnet, &ctx)
                .expect("unable to standardize block");

        for (index, (tx_in, tx_out)) in decoded_block
            .tx
            .iter()
            .zip(standardized_block.transactions.iter())
            .enumerate()
        {
            // Test outputs
            assert_eq!(tx_in.vout.len(), tx_out.metadata.outputs.len());
            for (output, src) in tx_out.metadata.outputs.iter().zip(tx_in.vout.iter()) {
                assert_eq!(output.value, src.value.to_sat());
            }
            // Test inputs (non-coinbase transactions only)
            if index == 0 {
                continue;
            }
            assert_eq!(tx_in.vin.len(), tx_out.metadata.inputs.len());
            for (input, src) in tx_out.metadata.inputs.iter().zip(tx_in.vin.iter()) {
                assert_eq!(
                    input.previous_output.block_height,
                    src.prevout.as_ref().unwrap().height
                );
                assert_eq!(
                    input.previous_output.value,
                    src.prevout.as_ref().unwrap().value.to_sat()
                );
                let txin = hex::decode(src.txid.as_ref().unwrap()).unwrap();
                assert_eq!(input.previous_output.txid.get_hash_bytes(), txin);
                assert_eq!(input.previous_output.vout, src.vout.unwrap());
            }
        }

        let bytes = BlockBytesCursor::from_full_block(&decoded_block).expect("unable to serialize");
        let bytes_via_standardized = BlockBytesCursor::from_standardized_block(&standardized_block)
            .expect("unable to serialize");
        assert_eq!(bytes, bytes_via_standardized);

        let block_bytes_cursor = BlockBytesCursor::new(&bytes);
        assert_eq!(decoded_block.tx.len(), block_bytes_cursor.tx_len as usize);

        // Test helpers
        let coinbase_txid = block_bytes_cursor.get_coinbase_txid();
        assert_eq!(
            coinbase_txid,
            standardized_block.transactions[0]
                .transaction_identifier
                .get_8_hash_bytes()
        );

        // Test transactions
        for (index, (tx_in, tx_out)) in decoded_block
            .tx
            .iter()
            .zip(block_bytes_cursor.iter_tx())
            .enumerate()
        {
            // Test outputs
            assert_eq!(tx_in.vout.len(), tx_out.outputs.len());
            for (sats, src) in tx_out.outputs.iter().zip(tx_in.vout.iter()) {
                assert_eq!(*sats, src.value.to_sat());
            }
            // Test inputs (non-coinbase transactions only)
            if index == 0 {
                continue;
            }
            assert_eq!(tx_in.vin.len(), tx_out.inputs.len());
            for (tx_bytes_cursor, src) in tx_out.inputs.iter().zip(tx_in.vin.iter()) {
                assert_eq!(
                    tx_bytes_cursor.block_height as u64,
                    src.prevout.as_ref().unwrap().height
                );
                assert_eq!(
                    tx_bytes_cursor.txin_value,
                    src.prevout.as_ref().unwrap().value.to_sat()
                );
                let txin = hex::decode(src.txid.as_ref().unwrap()).unwrap();
                assert_eq!(tx_bytes_cursor.txin, txin[0..tx_bytes_cursor.txin.len()]);
                assert_eq!(tx_bytes_cursor.vout as u32, src.vout.unwrap());
            }
        }
    }
}
