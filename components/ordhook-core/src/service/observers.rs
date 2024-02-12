use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::mpsc::{channel, Sender},
};

use chainhook_sdk::{
    chainhooks::types::{
        BitcoinChainhookFullSpecification, BitcoinChainhookNetworkSpecification,
        BitcoinChainhookSpecification, ChainhookConfig, ChainhookSpecification,
    },
    observer::EventObserverConfig,
    types::BitcoinBlockData,
    utils::Context,
};
use rusqlite::{Connection, ToSql};
use serde_json::json;

use crate::{
    config::Config,
    db::{
        create_or_open_readwrite_db, open_existing_readonly_db, perform_query_one,
        perform_query_set,
    },
    scan::bitcoin::process_block_with_predicates,
};

pub fn update_observer_progress(
    uuid: &str,
    last_block_height_update: u64,
    observers_db_conn: &Connection,
    ctx: &Context,
) {
    while let Err(e) = observers_db_conn.execute(
        "UPDATE observers SET last_block_height_update = ? WHERE uuid = ?",
        rusqlite::params![last_block_height_update, uuid],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to query observers.sqlite: {}",
                e.to_string()
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn update_observer_streaming_enabled(
    uuid: &str,
    streaming_enabled: bool,
    observers_db_conn: &Connection,
    ctx: &Context,
) {
    while let Err(e) = observers_db_conn.execute(
        "UPDATE observers SET streaming_enabled = ? WHERE uuid = ?",
        rusqlite::params![streaming_enabled, uuid],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to query observers.sqlite: {}",
                e.to_string()
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn insert_entry_in_observers(
    spec: &ChainhookSpecification,
    report: &ObserverReport,
    observers_db_conn: &Connection,
    ctx: &Context,
) {
    remove_entry_from_observers(&spec.uuid(), observers_db_conn, ctx);
    while let Err(e) = observers_db_conn.execute(
        "INSERT INTO observers (uuid, spec, streaming_enabled, last_block_height_update) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![&spec.uuid(), json!(spec).to_string(), report.streaming_enabled, report.last_block_height_update],
    ) {
        ctx.try_log(|logger| warn!(logger, "unable to query observers.sqlite: {}", e.to_string()));
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn get_default_observers_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("observers.sqlite");
    destination_path
}

pub fn open_readonly_observers_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let db_path = get_default_observers_db_file_path(&base_dir);
    let conn = open_existing_readonly_db(&db_path, ctx);
    Ok(conn)
}

pub fn open_readwrite_observers_db_conn(
    base_dir: &PathBuf,
    ctx: &Context,
) -> Result<Connection, String> {
    let db_path = get_default_observers_db_file_path(&base_dir);
    let conn = create_or_open_readwrite_db(&db_path, ctx);
    Ok(conn)
}

pub fn open_readwrite_observers_db_conn_or_panic(base_dir: &PathBuf, ctx: &Context) -> Connection {
    let conn = match open_readwrite_observers_db_conn(base_dir, ctx) {
        Ok(con) => con,
        Err(message) => {
            error!(ctx.expect_logger(), "Storage: {}", message.to_string());
            panic!();
        }
    };
    conn
}

pub fn initialize_observers_db(base_dir: &PathBuf, ctx: &Context) -> Connection {
    let db_path = get_default_observers_db_file_path(&base_dir);
    let conn = create_or_open_readwrite_db(&db_path, ctx);
    // TODO: introduce initial output
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS observers (
            uuid TEXT NOT NULL PRIMARY KEY,
            spec TEXT NOT NULL,
            streaming_enabled INTEGER NOT NULL,
            last_block_height_update INTEGER NOT NULL
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
    }
    conn
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ObserverReport {
    pub streaming_enabled: bool,
    pub last_block_height_update: u64,
}

pub fn find_observer_with_uuid(
    uuid: &str,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<(ChainhookSpecification, ObserverReport)> {
    let args: &[&dyn ToSql] = &[&uuid.to_sql().unwrap()];
    let query =
        "SELECT spec, streaming_enabled, last_block_height_update FROM observers WHERE uuid = ?";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let encoded_spec: String = row.get(0).unwrap();
        let spec = ChainhookSpecification::deserialize_specification(&encoded_spec).unwrap();
        let report = ObserverReport {
            streaming_enabled: row.get(1).unwrap(),
            last_block_height_update: row.get(2).unwrap(),
        };
        (spec, report)
    })
}

pub fn find_all_observers(
    db_conn: &Connection,
    ctx: &Context,
) -> Vec<(ChainhookSpecification, ObserverReport)> {
    let args: &[&dyn ToSql] = &[];
    let query = "SELECT spec, streaming_enabled, last_block_height_update FROM observers";
    perform_query_set(query, args, db_conn, ctx, |row| {
        let encoded_spec: String = row.get(0).unwrap();
        let spec = ChainhookSpecification::deserialize_specification(&encoded_spec).unwrap();
        let report = ObserverReport {
            streaming_enabled: row.get(1).unwrap(),
            last_block_height_update: row.get(2).unwrap(),
        };
        (spec, report)
    })
}

pub fn remove_entry_from_observers(uuid: &str, db_conn: &Connection, ctx: &Context) {
    while let Err(e) = db_conn.execute(
        "DELETE FROM observers WHERE uuid = ?1",
        rusqlite::params![&uuid],
    ) {
        ctx.try_log(|logger| {
            warn!(
                logger,
                "unable to query observers.sqlite: {}",
                e.to_string()
            )
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

// Cases to cover:
// - Empty state
// - State present, but not up to date
//      - Blocks presents, no inscriptions
//      - Blocks presents, inscription presents
// - State up to date

pub fn start_predicate_processor(
    event_observer_config: &EventObserverConfig,
    ctx: &Context,
) -> Sender<BitcoinBlockData> {
    let (tx, rx) = channel();

    let mut moved_event_observer_config = event_observer_config.clone();
    let moved_ctx = ctx.clone();

    let _ = hiro_system_kit::thread_named("Initial predicate processing")
        .spawn(move || {
            if let Some(mut chainhook_config) = moved_event_observer_config.chainhook_config.take()
            {
                let mut bitcoin_predicates_ref: Vec<&BitcoinChainhookSpecification> = vec![];
                for bitcoin_predicate in chainhook_config.bitcoin_chainhooks.iter_mut() {
                    bitcoin_predicate.enabled = false;
                    bitcoin_predicates_ref.push(bitcoin_predicate);
                }
                while let Ok(block) = rx.recv() {
                    let future = process_block_with_predicates(
                        block,
                        &bitcoin_predicates_ref,
                        &moved_event_observer_config,
                        &moved_ctx,
                    );
                    let res = hiro_system_kit::nestable_block_on(future);
                    if let Err(_) = res {
                        error!(moved_ctx.expect_logger(), "Initial ingestion failing");
                    }
                }
            }
        })
        .expect("unable to spawn thread");
    tx
}

pub fn create_and_consolidate_chainhook_config_with_predicates(
    provided_observers: Vec<BitcoinChainhookSpecification>,
    chain_tip_height: u64,
    enable_internal_trigger: bool,
    config: &Config,
    ctx: &Context,
) -> Result<(ChainhookConfig, Vec<BitcoinChainhookFullSpecification>), String> {
    let mut chainhook_config: ChainhookConfig = ChainhookConfig::new();

    if enable_internal_trigger {
        let _ = chainhook_config.register_specification(ChainhookSpecification::Bitcoin(
            BitcoinChainhookSpecification {
                uuid: format!("ordhook-internal-trigger"),
                owner_uuid: None,
                name: format!("ordhook-internal-trigger"),
                network: config.network.bitcoin_network.clone(),
                version: 1,
                blocks: None,
                start_block: None,
                end_block: None,
                expired_at: None,
                expire_after_occurrence: None,
                predicate: chainhook_sdk::chainhooks::types::BitcoinPredicateType::OrdinalsProtocol(
                    chainhook_sdk::chainhooks::types::OrdinalOperations::InscriptionFeed,
                ),
                action: chainhook_sdk::chainhooks::types::HookAction::Noop,
                include_proof: false,
                include_inputs: true,
                include_outputs: false,
                include_witness: false,
                enabled: true,
            },
        ));
    }

    let observers_db_conn = initialize_observers_db(&config.expected_cache_path(), ctx);

    let mut observers_to_catchup = vec![];
    let mut observers_to_clean_up = vec![];
    let mut observers_ready = vec![];

    let previously_registered_observers = find_all_observers(&observers_db_conn, ctx);
    for (spec, report) in previously_registered_observers.into_iter() {
        let ChainhookSpecification::Bitcoin(spec) = spec else {
            continue;
        };
        // De-register outdated observers: was end_block (if specified) scanned?
        if let Some(expiration) = spec.end_block {
            if report.last_block_height_update >= expiration {
                observers_to_clean_up.push(spec.uuid);
                continue;
            }
        }
        // De-register outdated observers: were all blocks (if specified) scanned?
        if let Some(ref blocks) = spec.blocks {
            let mut scanning_completed = true;
            for block in blocks.iter() {
                if block.gt(&report.last_block_height_update) {
                    scanning_completed = false;
                    break;
                }
            }
            if scanning_completed {
                observers_to_clean_up.push(spec.uuid);
                continue;
            }
        }

        if report.last_block_height_update == chain_tip_height {
            observers_ready.push(spec);
        } else {
            observers_to_catchup.push((spec, report));
        }
    }

    // Clean-up
    for outdated_observer in observers_to_clean_up.iter() {
        remove_entry_from_observers(outdated_observer, &observers_db_conn, ctx);
    }

    // Registrations
    for mut bitcoin_spec in observers_ready.into_iter() {
        bitcoin_spec.enabled = true;
        let spec = ChainhookSpecification::Bitcoin(bitcoin_spec);
        chainhook_config.register_specification(spec)?;
    }

    // Among observers provided, only consider the ones that are not known
    for observer in provided_observers.into_iter() {
        let existing_observer = find_observer_with_uuid(&observer.uuid, &observers_db_conn, ctx);
        if existing_observer.is_some() {
            continue;
        }
        let report = ObserverReport::default();
        observers_to_catchup.push((observer, report));
    }

    let mut full_specs = vec![];

    for (observer, report) in observers_to_catchup.into_iter() {
        let mut networks = BTreeMap::new();
        networks.insert(
            config.network.bitcoin_network.clone(),
            BitcoinChainhookNetworkSpecification {
                start_block: Some(report.last_block_height_update + 1),
                end_block: observer.end_block,
                blocks: observer.blocks,
                expire_after_occurrence: observer.expire_after_occurrence,
                include_proof: Some(observer.include_proof),
                include_inputs: Some(observer.include_inputs),
                include_outputs: Some(observer.include_outputs),
                include_witness: Some(observer.include_witness),
                predicate: observer.predicate,
                action: observer.action,
            },
        );
        let full_spec = BitcoinChainhookFullSpecification {
            uuid: observer.uuid,
            owner_uuid: observer.owner_uuid,
            name: observer.name,
            version: observer.version,
            networks,
        };
        info!(
            ctx.expect_logger(),
            "Observer '{}' to be caught-up (last block sent: {}, tip: {})",
            full_spec.name,
            report.last_block_height_update,
            chain_tip_height
        );
        full_specs.push(full_spec);
    }

    Ok((chainhook_config, full_specs))
}
