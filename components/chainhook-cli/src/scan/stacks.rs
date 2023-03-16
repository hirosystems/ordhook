use std::{
    collections::{HashMap, HashSet, VecDeque},
    process,
};

use crate::{
    archive,
    block::{Record, RecordKind},
    config::Config,
};
use chainhook_event_observer::{
    chainhooks::stacks::{
        handle_stacks_hook_action, StacksChainhookOccurrence, StacksTriggerChainhook,
    },
    observer::{EventObserverConfig, DEFAULT_CONTROL_PORT, DEFAULT_INGESTION_PORT},
    utils::{file_append, send_request, AbstractStacksBlock},
};
use chainhook_event_observer::{
    chainhooks::{
        stacks::evaluate_stacks_chainhook_on_blocks, types::StacksChainhookFullSpecification,
    },
    indexer::{self, stacks::standardize_stacks_serialized_block_header, Indexer},
    utils::Context,
};
use chainhook_types::BlockIdentifier;

pub async fn scan_stacks_chain_with_predicate(
    predicate: StacksChainhookFullSpecification,
    _apply: bool,
    config: &mut Config,
    ctx: &Context,
) -> Result<(), String> {
    let selected_predicate =
        predicate.into_selected_network_specification(&config.network.stacks_network)?;

    let start_block = match selected_predicate.start_block {
        Some(start_block) => start_block,
        None => {
            return Err(
                "Chainhook specification must include fields 'start_block' and 'end_block' when using the scan command"
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
        cache_path: config.storage.cache_path.clone(),
        bitcoin_network: config.network.bitcoin_network.clone(),
        stacks_network: config.network.stacks_network.clone(),
    };

    let mut indexer = Indexer::new(config.network.clone(), None);

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

            if let Some(end_block) = selected_predicate.end_block {
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

    for (_block_identifier, _parent_block_identifier, blob) in canonical_fork.drain(..) {
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

        let hits_per_blocks = evaluate_stacks_chainhook_on_blocks(blocks, &selected_predicate, ctx);
        if hits_per_blocks.is_empty() {
            continue;
        }

        let trigger = StacksTriggerChainhook {
            chainhook: &selected_predicate,
            apply: hits_per_blocks,
            rollback: vec![],
        };
        match handle_stacks_hook_action(trigger, &proofs, &ctx) {
            Err(e) => {
                error!(ctx.expect_logger(), "unable to handle action {}", e);
            }
            Ok(StacksChainhookOccurrence::Http(request)) => {
                send_request(request, &ctx).await;
            }
            Ok(StacksChainhookOccurrence::File(path, bytes)) => {
                file_append(path, bytes, &ctx);
            }
            Ok(StacksChainhookOccurrence::Data(_payload)) => unreachable!(),
        }
    }

    Ok(())
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
            }
            config.add_local_tsv_source(&destination_path);
        }
        true
    } else {
        info!(
            ctx.expect_logger(),
            "Streaming blocks from stacks-node {}",
            config.expected_stacks_node_event_source()
        );
        false
    }
}
