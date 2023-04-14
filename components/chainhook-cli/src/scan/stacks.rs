use std::{
    collections::{HashMap, VecDeque},
    process,
};

use crate::{
    archive,
    block::{Record, RecordKind},
    config::Config,
};
use chainhook_event_observer::{
    chainhooks::stacks::evaluate_stacks_chainhook_on_blocks,
    indexer::{self, stacks::standardize_stacks_serialized_block_header, Indexer},
    utils::Context,
};
use chainhook_event_observer::{
    chainhooks::{
        stacks::{handle_stacks_hook_action, StacksChainhookOccurrence, StacksTriggerChainhook},
        types::StacksChainhookSpecification,
    },
    utils::{file_append, send_request, AbstractStacksBlock},
};
use chainhook_types::BlockIdentifier;

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

    let _ = download_dataset_if_required(config, ctx).await;

    let seed_tsv_path = config.expected_local_tsv_file().clone();

    let (record_tx, record_rx) = std::sync::mpsc::channel();

    let _parsing_handle = std::thread::spawn(move || {
        let mut reader_builder = csv::ReaderBuilder::default()
            .has_headers(false)
            .delimiter(b'\t')
            .buffer_capacity(8 * (1 << 10))
            .from_path(&seed_tsv_path)
            .expect("unable to create csv reader");

        for result in reader_builder.deserialize() {
            // Notice that we need to provide a type hint for automatic
            // deserialization.
            let record: Record = result.unwrap();
            match &record.kind {
                RecordKind::StacksBlockReceived => {
                    match record_tx.send(Some(record)) {
                        Err(_e) => {
                            // Abord the traversal once the receiver closed
                            break;
                        }
                        _ => {}
                    }
                }
                // RecordKind::BitcoinBlockReceived => {
                //     let _ = bitcoin_record_tx.send(Some(record));
                // }
                // RecordKind::StacksMicroblockReceived => {
                //     let _ = stacks_record_tx.send(Some(record));
                // },
                _ => {}
            };
        }
        let _ = record_tx.send(None);
    });

    let mut indexer = Indexer::new(config.network.clone());

    let mut canonical_fork = {
        let mut cursor = BlockIdentifier::default();
        let mut dump = HashMap::new();

        while let Ok(Some(record)) = record_rx.recv() {
            let (block_identifier, parent_block_identifier) = match &record.kind {
                RecordKind::StacksBlockReceived => {
                    match standardize_stacks_serialized_block_header(&record.raw_log) {
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

            if let Some(end_block) = predicate_spec.end_block {
                if block_identifier.index > end_block {
                    break;
                }
            }

            if block_identifier.index > cursor.index {
                cursor = block_identifier.clone(); // todo(lgalabru)
            }
            dump.insert(block_identifier, (parent_block_identifier, record.raw_log));
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
    let proofs = HashMap::new();

    let mut actions_triggered = 0;
    let mut blocks_scanned = 0;
    info!(
        ctx.expect_logger(),
        "Starting predicate evaluation on Stacks blocks"
    );
    let mut last_block_scanned = BlockIdentifier::default();
    let mut err_count = 0;
    for (block_identifier, _parent_block_identifier, blob) in canonical_fork.drain(..) {
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
                actions_triggered += 1;
                let res = match action {
                    StacksChainhookOccurrence::Http(request) => send_request(request, 3, 1, &ctx).await,
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
        "{blocks_scanned} blocks scanned, {actions_triggered} actions triggered"
    );

    Ok(last_block_scanned)
}

async fn download_dataset_if_required(config: &mut Config, ctx: &Context) -> bool {
    if config.is_initial_ingestion_required() {
        // Download default tsv.
        if config.rely_on_remote_tsv() && config.should_download_remote_tsv() {
            let url = config.expected_remote_tsv_url();
            let mut destination_path = config.expected_cache_path();
            destination_path.push(archive::default_tsv_file_path(
                &config.network.stacks_network,
            ));
            // Download archive if not already present in cache
            if !destination_path.exists() {
                info!(ctx.expect_logger(), "Downloading {}", url);
                match archive::download_tsv_file(&config).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(ctx.expect_logger(), "{}", e);
                        process::exit(1);
                    }
                }
            } else {
                info!(
                    ctx.expect_logger(),
                    "Building in-memory chainstate from file {}",
                    destination_path.display()
                );
            }
            config.add_local_tsv_source(&destination_path);
        }
        true
    } else {
        info!(
            ctx.expect_logger(),
            "Streaming blocks from stacks-node {}", config.network.stacks_node_rpc_url
        );
        false
    }
}
