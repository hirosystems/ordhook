use std::path::PathBuf;

use chainhook_sdk::utils::Context;
use chainhook_types::{BlockIdentifier, StacksBlockData, StacksBlockUpdate};
use rocksdb::{Options, DB};

fn get_db_default_options() -> Options {
    let mut opts = Options::default();
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

fn get_default_stacks_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("stacks.rocksdb");
    destination_path
}

pub fn open_readonly_stacks_db_conn(base_dir: &PathBuf, _ctx: &Context) -> Result<DB, String> {
    let path = get_default_stacks_db_file_path(&base_dir);
    let opts = get_db_default_options();
    let db = DB::open_for_read_only(&opts, path, false)
        .map_err(|e| format!("unable to open stacks.rocksdb: {}", e.to_string()))?;
    Ok(db)
}

pub fn open_readwrite_stacks_db_conn(base_dir: &PathBuf, _ctx: &Context) -> Result<DB, String> {
    let path = get_default_stacks_db_file_path(&base_dir);
    let opts = get_db_default_options();
    let db = DB::open(&opts, path)
        .map_err(|e| format!("unable to open stacks.rocksdb: {}", e.to_string()))?;
    Ok(db)
}

fn get_default_bitcoin_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("bitcoin.rocksdb");
    destination_path
}

pub fn open_readonly_bitcoin_db_conn(base_dir: &PathBuf, _ctx: &Context) -> Result<DB, String> {
    let path = get_default_bitcoin_db_file_path(&base_dir);
    let opts = get_db_default_options();
    let db = DB::open_for_read_only(&opts, path, false)
        .map_err(|e| format!("unable to open bitcoin.rocksdb: {}", e.to_string()))?;
    Ok(db)
}

pub fn open_readwrite_bitcoin_db_conn(base_dir: &PathBuf, _ctx: &Context) -> Result<DB, String> {
    let path = get_default_bitcoin_db_file_path(&base_dir);
    let opts = get_db_default_options();
    let db = DB::open(&opts, path)
        .map_err(|e| format!("unable to open bitcoin.rocksdb: {}", e.to_string()))?;
    Ok(db)
}

fn get_block_key(block_identifier: &BlockIdentifier) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..2].copy_from_slice(b"b:");
    key[2..10].copy_from_slice(&block_identifier.index.to_be_bytes());
    key[10..].copy_from_slice(b":d");
    key
}

fn get_unconfirmed_block_key(block_identifier: &BlockIdentifier) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..2].copy_from_slice(b"~:");
    key[2..10].copy_from_slice(&block_identifier.index.to_be_bytes());
    key[10..].copy_from_slice(b":d");
    key
}

fn get_last_confirmed_insert_key() -> [u8; 3] {
    *b"m:t"
}

fn get_last_unconfirmed_insert_key() -> [u8; 3] {
    *b"m:~"
}

pub fn insert_entry_in_stacks_blocks(block: &StacksBlockData, stacks_db_rw: &DB, _ctx: &Context) {
    let key = get_block_key(&block.block_identifier);
    let block_bytes = json!(block);
    stacks_db_rw
        .put(&key, &block_bytes.to_string().as_bytes())
        .expect("unable to insert blocks");
    stacks_db_rw
        .put(
            get_last_confirmed_insert_key(),
            block.block_identifier.index.to_be_bytes(),
        )
        .expect("unable to insert metadata");
}

pub fn insert_unconfirmed_entry_in_stacks_blocks(
    block: &StacksBlockData,
    stacks_db_rw: &DB,
    _ctx: &Context,
) {
    let key = get_unconfirmed_block_key(&block.block_identifier);
    let block_bytes = json!(block);
    stacks_db_rw
        .put(&key, &block_bytes.to_string().as_bytes())
        .expect("unable to insert blocks");
    stacks_db_rw
        .put(
            get_last_unconfirmed_insert_key(),
            block.block_identifier.index.to_be_bytes(),
        )
        .expect("unable to insert metadata");
}

pub fn delete_unconfirmed_entry_from_stacks_blocks(
    block_identifier: &BlockIdentifier,
    stacks_db_rw: &DB,
    _ctx: &Context,
) {
    let key = get_unconfirmed_block_key(&block_identifier);
    stacks_db_rw.delete(&key).expect("unable to delete blocks");
}

pub fn get_last_unconfirmed_block_height_inserted(stacks_db: &DB, _ctx: &Context) -> Option<u64> {
    stacks_db
        .get(get_last_unconfirmed_insert_key())
        .unwrap_or(None)
        .and_then(|bytes| {
            Some(u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))
        })
}

pub fn get_last_block_height_inserted(stacks_db: &DB, _ctx: &Context) -> Option<u64> {
    stacks_db
        .get(get_last_confirmed_insert_key())
        .unwrap_or(None)
        .and_then(|bytes| {
            Some(u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))
        })
}

pub fn confirm_entries_in_stacks_blocks(
    blocks: &Vec<StacksBlockData>,
    stacks_db_rw: &DB,
    ctx: &Context,
) {
    for block in blocks.iter() {
        insert_entry_in_stacks_blocks(block, stacks_db_rw, ctx);
        delete_unconfirmed_entry_from_stacks_blocks(&block.block_identifier, stacks_db_rw, ctx);
    }
}

pub fn draft_entries_in_stacks_blocks(
    block_updates: &Vec<StacksBlockUpdate>,
    stacks_db_rw: &DB,
    ctx: &Context,
) {
    for update in block_updates.iter() {
        // TODO: Could be imperfect, from a microblock point of view
        insert_unconfirmed_entry_in_stacks_blocks(&update.block, stacks_db_rw, ctx);
    }
}

pub fn get_stacks_block_at_block_height(
    block_height: u64,
    confirmed: bool,
    retry: u8,
    stacks_db: &DB,
) -> Result<Option<StacksBlockData>, String> {
    let mut attempt = 0;
    loop {
        let block_identifier = &BlockIdentifier {
            hash: "".to_string(),
            index: block_height,
        };
        match stacks_db.get(match confirmed {
            true => get_block_key(block_identifier),
            false => get_unconfirmed_block_key(block_identifier),
        }) {
            Ok(Some(entry)) => {
                return Ok(Some({
                    let spec: StacksBlockData =
                        serde_json::from_slice(&entry[..]).map_err(|e| {
                            format!("unable to deserialize Stacks chainhook {}", e.to_string())
                        })?;
                    spec
                }))
            }
            Ok(None) => return Ok(None),
            _ => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_secs(2));
                if attempt > retry {
                    return Ok(None); // TODO
                }
            }
        }
    }
}

pub fn is_stacks_block_present(
    block_identifier: &BlockIdentifier,
    retry: u8,
    stacks_db: &DB,
) -> bool {
    let mut attempt = 0;
    loop {
        match stacks_db.get(get_block_key(block_identifier)) {
            Ok(Some(_)) => return true,
            Ok(None) => return false,
            _ => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_secs(2));
                if attempt > retry {
                    return false;
                }
            }
        }
    }
}
