use std::collections::{HashMap, VecDeque};

use crate::{
    archive::download_stacks_dataset_if_required,
    block::{Record, RecordKind},
    config::{Config, PredicatesApi},
    service::{
        open_readwrite_predicates_db_conn_or_panic, update_predicate_status, PredicateStatus,
        ScanningData,
    },
    storage::{
        get_last_block_height_inserted, get_last_unconfirmed_block_height_inserted,
        get_stacks_block_at_block_height, insert_entry_in_stacks_blocks, is_stacks_block_present,
        open_readwrite_stacks_db_conn,
    },
};
use chainhook_sdk::{
    chainhooks::stacks::evaluate_stacks_chainhook_on_blocks,
    indexer::{self, stacks::standardize_stacks_serialized_block_header, Indexer},
    rocksdb::DB,
    utils::Context,
};
use chainhook_sdk::{
    chainhooks::{
        stacks::{handle_stacks_hook_action, StacksChainhookOccurrence, StacksTriggerChainhook},
        types::StacksChainhookSpecification,
    },
    utils::{file_append, send_request, AbstractStacksBlock},
};
use chainhook_types::BlockIdentifier;

pub async fn get_canonical_fork_from_tsv(
    config: &mut Config,
    ctx: &Context,
) -> Result<VecDeque<(BlockIdentifier, BlockIdentifier, String)>, String> {
    let seed_tsv_path = config.expected_local_stacks_tsv_file().clone();

    let (record_tx, record_rx) = std::sync::mpsc::channel();

    let start_block = 0;

    let parsing_handle = hiro_system_kit::thread_named("Stacks chainstate CSV parsing")
        .spawn(move || {
            let mut reader_builder = csv::ReaderBuilder::default()
                .has_headers(false)
                .delimiter(b'\t')
                .buffer_capacity(8 * (1 << 10))
                .from_path(&seed_tsv_path)
                .expect("unable to create csv reader");

            for result in reader_builder.deserialize() {
                let record: Record = result.unwrap();
                match &record.kind {
                    RecordKind::StacksBlockReceived => match record_tx.send(Some(record)) {
                        Err(_e) => {
                            break;
                        }
                        _ => {}
                    },
                    _ => {}
                };
            }
            let _ = record_tx.send(None);
        })
        .expect("unable to spawn thread");

    let canonical_fork = {
        let mut cursor = BlockIdentifier::default();
        let mut dump = HashMap::new();

        while let Ok(Some(mut record)) = record_rx.recv() {
            let (block_identifier, parent_block_identifier) = match (&record.kind, &record.blob) {
                (RecordKind::StacksBlockReceived, Some(blob)) => {
                    match standardize_stacks_serialized_block_header(&blob) {
                        Ok(data) => data,
                        Err(e) => {
                            error!(ctx.expect_logger(), "{e}");
                            continue;
                        }
                    }
                }
                _ => unreachable!(),
            };

            if start_block > block_identifier.index {
                continue;
            }

            if block_identifier.index > cursor.index {
                cursor = block_identifier.clone(); // todo(lgalabru)
            }
            dump.insert(
                block_identifier,
                (parent_block_identifier, record.blob.take().unwrap()),
            );
        }

        let mut canonical_fork = VecDeque::new();
        while cursor.index > 0 {
            let (block_identifer, (parent_block_identifier, blob)) =
                match dump.remove_entry(&cursor) {
                    Some(entry) => entry,
                    None => break,
                };
            cursor = parent_block_identifier.clone(); // todo(lgalabru)
            canonical_fork.push_front((block_identifer, parent_block_identifier, blob));
        }
        canonical_fork
    };
    let _ = parsing_handle.join();

    Ok(canonical_fork)
}

pub async fn scan_stacks_chainstate_via_rocksdb_using_predicate(
    predicate_spec: &StacksChainhookSpecification,
    stacks_db_conn: &DB,
    config: &Config,
    ctx: &Context,
) -> Result<BlockIdentifier, String> {
    let start_block = match predicate_spec.start_block {
        Some(start_block) => start_block,
        None => {
            return Err(
                "Chainhook specification must include fields 'start_block' when using the scan command"
                    .into(),
            );
        }
    };

    let (mut end_block, follow_tip) = match predicate_spec.end_block {
        Some(end_block) => (end_block, false),
        None => match get_last_unconfirmed_block_height_inserted(stacks_db_conn, ctx) {
            Some(end_block) => (end_block, true),
            None => match get_last_block_height_inserted(stacks_db_conn, ctx) {
                Some(end_block) => (end_block, true),
                None => {
                    return Err(
                        "Chainhook specification must include fields 'end_block' when using the scan command"
                            .into(),
                    );
                }
            },
        },
    };

    let mut predicates_db_conn = match config.http_api {
        PredicatesApi::On(ref api_config) => {
            Some(open_readwrite_predicates_db_conn_or_panic(api_config, ctx))
        }
        PredicatesApi::Off => None,
    };

    let proofs = HashMap::new();
    let mut occurrences_found = 0;
    let mut blocks_scanned = 0;
    info!(
        ctx.expect_logger(),
        "Starting predicate evaluation on Stacks blocks"
    );
    let mut last_block_scanned = BlockIdentifier::default();
    let mut err_count = 0;
    let mut cursor = start_block;
    while cursor <= end_block {
        let block_data = match get_stacks_block_at_block_height(cursor, true, 3, stacks_db_conn) {
            Ok(Some(block)) => block,
            Ok(None) => match get_stacks_block_at_block_height(cursor, false, 3, stacks_db_conn) {
                Ok(Some(block)) => block,
                Ok(None) => return Err(format!("Unable to retrieve block {cursor}")),
                Err(e) => return Err(format!("Unable to retrieve block {cursor}: {e}")),
            },
            Err(e) => return Err(format!("Unable to retrieve block {cursor}: {e}")),
        };
        last_block_scanned = block_data.block_identifier.clone();
        blocks_scanned += 1;

        let blocks: Vec<&dyn AbstractStacksBlock> = vec![&block_data];

        let hits_per_blocks = evaluate_stacks_chainhook_on_blocks(blocks, &predicate_spec, ctx);
        if hits_per_blocks.is_empty() {
            cursor += 1;
            continue;
        }

        let trigger = StacksTriggerChainhook {
            chainhook: &predicate_spec,
            apply: hits_per_blocks,
            rollback: vec![],
        };
        match handle_stacks_hook_action(trigger, &proofs, &ctx) {
            Err(e) => {
                error!(ctx.expect_logger(), "unable to handle action {}", e);
            }
            Ok(action) => {
                occurrences_found += 1;
                let res = match action {
                    StacksChainhookOccurrence::Http(request) => {
                        send_request(request, 3, 1, &ctx).await
                    }
                    StacksChainhookOccurrence::File(path, bytes) => file_append(path, bytes, &ctx),
                    StacksChainhookOccurrence::Data(_payload) => unreachable!(),
                };
                if res.is_err() {
                    err_count += 1;
                } else {
                    err_count = 0;
                }
            }
        }
        // We abort after 3 consecutive errors
        if err_count >= 3 {
            return Err(format!("Scan aborted (consecutive action errors >= 3)"));
        }

        if let Some(ref mut predicates_db_conn) = predicates_db_conn {
            if blocks_scanned % 5000 == 0 {
                let status = PredicateStatus::Scanning(ScanningData {
                    start_block,
                    end_block,
                    cursor,
                    occurrences_found,
                });
                update_predicate_status(&predicate_spec.key(), status, predicates_db_conn, &ctx)
            }
        }

        cursor += 1;
        // Update end_block, in case a new block was discovered during the scan
        if cursor == end_block && follow_tip {
            end_block = match predicate_spec.end_block {
                Some(end_block) => end_block,
                None => match get_last_unconfirmed_block_height_inserted(stacks_db_conn, ctx) {
                    Some(end_block) => end_block,
                    None => match get_last_block_height_inserted(stacks_db_conn, ctx) {
                        Some(end_block) => end_block,
                        None => {
                            return Err(
                                "Chainhook specification must include fields 'end_block' when using the scan command"
                                    .into(),
                            );
                        }
                    },
                },
            };
        }
    }
    info!(
        ctx.expect_logger(),
        "{blocks_scanned} blocks scanned, {occurrences_found} occurrences found"
    );

    if let Some(ref mut predicates_db_conn) = predicates_db_conn {
        let status = PredicateStatus::Scanning(ScanningData {
            start_block,
            end_block,
            cursor,
            occurrences_found,
        });
        update_predicate_status(&predicate_spec.key(), status, predicates_db_conn, &ctx)
    }
    Ok(last_block_scanned)
}

pub async fn scan_stacks_chainstate_via_csv_using_predicate(
    predicate_spec: &StacksChainhookSpecification,
    config: &mut Config,
    ctx: &Context,
) -> Result<BlockIdentifier, String> {
    let start_block = match predicate_spec.start_block {
        Some(start_block) => start_block,
        None => {
            return Err(
                "Chainhook specification must include fields 'start_block' when using the scan command"
                    .into(),
            );
        }
    };

    let _ = download_stacks_dataset_if_required(config, ctx).await;

    let mut canonical_fork = get_canonical_fork_from_tsv(config, ctx).await?;

    let mut indexer = Indexer::new(config.network.clone());

    let proofs = HashMap::new();

    let mut occurrences_found = 0;
    let mut blocks_scanned = 0;
    info!(
        ctx.expect_logger(),
        "Starting predicate evaluation on Stacks blocks"
    );
    let mut last_block_scanned = BlockIdentifier::default();
    let mut err_count = 0;
    for (block_identifier, _parent_block_identifier, blob) in canonical_fork.drain(..) {
        if block_identifier.index <= start_block {
            continue
        }

        last_block_scanned = block_identifier;
        blocks_scanned += 1;
        let block_data = match indexer::stacks::standardize_stacks_serialized_block(
            &indexer.config,
            &blob,
            &mut indexer.stacks_context,
            ctx,
        ) {
            Ok(block) => block,
            Err(e) => {
                error!(&ctx.expect_logger(), "{e}");
                continue;
            }
        };

        let blocks: Vec<&dyn AbstractStacksBlock> = vec![&block_data];

        let hits_per_blocks = evaluate_stacks_chainhook_on_blocks(blocks, &predicate_spec, ctx);
        if hits_per_blocks.is_empty() {
            continue;
        }

        let trigger = StacksTriggerChainhook {
            chainhook: &predicate_spec,
            apply: hits_per_blocks,
            rollback: vec![],
        };
        match handle_stacks_hook_action(trigger, &proofs, &ctx) {
            Err(e) => {
                error!(ctx.expect_logger(), "unable to handle action {}", e);
            }
            Ok(action) => {
                occurrences_found += 1;
                let res = match action {
                    StacksChainhookOccurrence::Http(request) => {
                        send_request(request, 3, 1, &ctx).await
                    }
                    StacksChainhookOccurrence::File(path, bytes) => file_append(path, bytes, &ctx),
                    StacksChainhookOccurrence::Data(_payload) => unreachable!(),
                };
                if res.is_err() {
                    err_count += 1;
                } else {
                    err_count = 0;
                }
            }
        }
        // We abort after 3 consecutive errors
        if err_count >= 3 {
            return Err(format!("Scan aborted (consecutive action errors >= 3)"));
        }
    }
    info!(
        ctx.expect_logger(),
        "{blocks_scanned} blocks scanned, {occurrences_found} occurrences found"
    );

    Ok(last_block_scanned)
}

pub async fn consolidate_local_stacks_chainstate_using_csv(
    config: &mut Config,
    ctx: &Context,
) -> Result<(), String> {
    info!(
        ctx.expect_logger(),
        "Building local chainstate from Stacks archive file"
    );

    let new_archive_to_process = download_stacks_dataset_if_required(config, ctx).await;
    // Nothing to do - early return
    if !new_archive_to_process {
        return Ok(());
    }

    let mut canonical_fork = get_canonical_fork_from_tsv(config, ctx).await?;

    let mut indexer = Indexer::new(config.network.clone());
    let mut blocks_inserted = 0;
    let mut blocks_read = 0;
    let blocks_to_insert = canonical_fork.len();
    let stacks_db_rw = open_readwrite_stacks_db_conn(&config.expected_cache_path(), ctx)?;
    for (block_identifier, _parent_block_identifier, blob) in canonical_fork.drain(..) {
        blocks_read += 1;

        // If blocks already stored, move on
        if is_stacks_block_present(&block_identifier, 3, &stacks_db_rw) {
            continue;
        }
        blocks_inserted += 1;

        let block_data = match indexer::stacks::standardize_stacks_serialized_block(
            &indexer.config,
            &blob,
            &mut indexer.stacks_context,
            ctx,
        ) {
            Ok(block) => block,
            Err(e) => {
                error!(&ctx.expect_logger(), "{e}");
                continue;
            }
        };

        // TODO: return a result
        insert_entry_in_stacks_blocks(&block_data, &stacks_db_rw, ctx);

        if blocks_inserted % 2500 == 0 {
            info!(
                ctx.expect_logger(),
                "Importing Stacks blocks: {}/{}", blocks_read, blocks_to_insert
            );
            let _ = stacks_db_rw.flush();
        }
    }
    let _ = stacks_db_rw.flush();
    info!(
        ctx.expect_logger(),
        "{blocks_read} Stacks blocks read, {blocks_inserted} inserted"
    );

    Ok(())
}
