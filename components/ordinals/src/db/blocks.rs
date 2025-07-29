use std::{path::PathBuf, thread::sleep, time::Duration};

use bitcoind::{try_error, try_warn, utils::Context};
use config::Config;
use rand::{rng, Rng};
use rocksdb::{DBPinnableSlice, Options, DB};

fn get_default_blocks_db_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("hord.rocksdb");
    destination_path
}

fn rocks_db_default_options(ulimit: usize, _memory_available: usize) -> Options {
    let mut opts = Options::default();
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

pub fn open_blocks_db_with_retry(readwrite: bool, config: &Config, ctx: &Context) -> DB {
    let mut retries = 0;
    loop {
        let res = if readwrite {
            open_readwrite_blocks_db(config, ctx)
        } else {
            open_readonly_blocks_db(config, ctx)
        };
        match res {
            Ok(db) => break db,
            Err(e) => {
                retries += 1;
                if retries > 10 {
                    try_warn!(ctx, "Unable to open db: {e}. Retrying in 10s",);
                    sleep(Duration::from_secs(10));
                } else {
                    sleep(Duration::from_secs(2));
                }
                continue;
            }
        }
    }
}

pub fn open_readonly_blocks_db(config: &Config, _ctx: &Context) -> Result<DB, String> {
    let path = get_default_blocks_db_path(&config.expected_cache_path());
    let mut opts =
        rocks_db_default_options(config.resources.ulimit, config.resources.memory_available);
    opts.set_disable_auto_compactions(true);
    opts.set_max_background_jobs(0);
    let db = DB::open_for_read_only(&opts, path, false)
        .map_err(|e| format!("unable to read hord.rocksdb: {}", e))?;
    Ok(db)
}

fn open_readwrite_blocks_db(config: &Config, _ctx: &Context) -> Result<DB, String> {
    let path = get_default_blocks_db_path(&config.expected_cache_path());
    let opts = rocks_db_default_options(config.resources.ulimit, config.resources.memory_available);
    let db =
        DB::open(&opts, path).map_err(|e| format!("unable to read-write hord.rocksdb: {}", e))?;
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
        let res = blocks_db_rw.put(block_height_bytes, block_bytes);
        match res {
            Ok(_) => break,
            Err(e) => {
                retries += 1;
                if retries > 10 {
                    try_error!(
                        ctx,
                        "unable to insert block {block_height} ({}). will retry in 5 secs",
                        e.to_string()
                    );
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
    let mut rng = rng();
    loop {
        match blocks_db.get_pinned(block_height.to_be_bytes()) {
            Ok(Some(res)) => return Some(res),
            _ => {
                attempt += 1;
                backoff = 2.0 * backoff + (backoff * rng.random_range(0.0..1.0));
                let duration = std::time::Duration::from_millis((backoff * 1_000.0) as u64);
                try_warn!(
                    ctx,
                    "Unable to find block #{}, will retry in {:?}",
                    block_height,
                    duration
                );
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
        try_error!(ctx, "{}", e.to_string());
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

#[cfg(test)]
pub fn insert_standardized_block(
    block: &bitcoind::types::BitcoinBlockData,
    blocks_db_rw: &DB,
    ctx: &Context,
) {
    let block_bytes = match bitcoind::types::BlockBytesCursor::from_standardized_block(&block) {
        Ok(block_bytes) => block_bytes,
        Err(e) => {
            try_error!(
                ctx,
                "Unable to compress block #{}: #{}",
                block.block_identifier.index,
                e.to_string()
            );
            return;
        }
    };
    insert_entry_in_blocks(
        block.block_identifier.index as u32,
        &block_bytes,
        true,
        &blocks_db_rw,
        &ctx,
    );
    if let Err(e) = blocks_db_rw.flush() {
        try_error!(ctx, "{}", e.to_string());
    }
}
