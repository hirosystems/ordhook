use std::sync::mpsc::{channel, Sender};

use chainhook_sdk::{
    chainhooks::types::{
        BitcoinChainhookSpecification, ChainhookConfig, ChainhookFullSpecification,
        ChainhookSpecification,
    },
    observer::EventObserverConfig,
    types::BitcoinBlockData,
    utils::Context,
};
use redis::{Commands, Connection};
use serde_json::json;

use crate::{
    config::{Config, PredicatesApiConfig},
    scan::bitcoin::process_block_with_predicates,
};

use super::http_api::load_predicates_from_redis;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateStatus {
    Scanning(ScanningData),
    Streaming(StreamingData),
    InitialScanCompleted,
    Interrupted(String),
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanningData {
    pub number_of_blocks_to_scan: u64,
    pub number_of_blocks_scanned: u64,
    pub number_of_blocks_sent: u64,
    pub current_block_height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingData {
    pub last_occurence: u64,
    pub last_evaluation: u64,
}

pub fn update_predicate_status(
    predicate_key: &str,
    status: PredicateStatus,
    predicates_db_conn: &mut Connection,
    ctx: &Context,
) {
    let serialized_status = json!(status).to_string();
    if let Err(e) =
        predicates_db_conn.hset::<_, _, _, ()>(&predicate_key, "status", &serialized_status)
    {
        error!(
            ctx.expect_logger(),
            "Error updating status: {}",
            e.to_string()
        );
    } else {
        info!(
            ctx.expect_logger(),
            "Updating predicate {predicate_key} status: {serialized_status}"
        );
    }
}

pub fn update_predicate_spec(
    predicate_key: &str,
    spec: &ChainhookSpecification,
    predicates_db_conn: &mut Connection,
    ctx: &Context,
) {
    let serialized_spec = json!(spec).to_string();
    if let Err(e) =
        predicates_db_conn.hset::<_, _, _, ()>(&predicate_key, "specification", &serialized_spec)
    {
        error!(
            ctx.expect_logger(),
            "Error updating status: {}",
            e.to_string()
        );
    } else {
        info!(
            ctx.expect_logger(),
            "Updating predicate {predicate_key} with spec: {serialized_spec}"
        );
    }
}

pub fn retrieve_predicate_status(
    predicate_key: &str,
    predicates_db_conn: &mut Connection,
) -> Option<PredicateStatus> {
    match predicates_db_conn.hget::<_, _, String>(predicate_key.to_string(), "status") {
        Ok(ref payload) => match serde_json::from_str(payload) {
            Ok(data) => Some(data),
            Err(_) => None,
        },
        Err(_) => None,
    }
}

pub fn open_readwrite_predicates_db_conn(
    config: &PredicatesApiConfig,
) -> Result<Connection, String> {
    let redis_uri = &config.database_uri;
    let client = redis::Client::open(redis_uri.clone()).unwrap();
    client
        .get_connection()
        .map_err(|e| format!("unable to connect to db: {}", e.to_string()))
}

pub fn open_readwrite_predicates_db_conn_or_panic(
    config: &PredicatesApiConfig,
    ctx: &Context,
) -> Connection {
    let redis_con = match open_readwrite_predicates_db_conn(config) {
        Ok(con) => con,
        Err(message) => {
            error!(ctx.expect_logger(), "Redis: {}", message.to_string());
            panic!();
        }
    };
    redis_con
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
    predicates: Vec<ChainhookFullSpecification>,
    enable_internal_trigger: bool,
    config: &Config,
    ctx: &Context,
) -> ChainhookConfig {
    let mut chainhook_config: ChainhookConfig = ChainhookConfig::new();

    // If no predicates passed at launch, retrieve predicates from Redis
    if predicates.is_empty() && config.is_http_api_enabled() {
        let registered_predicates = match load_predicates_from_redis(&config, &ctx) {
            Ok(predicates) => predicates,
            Err(e) => {
                error!(
                    ctx.expect_logger(),
                    "Failed loading predicate from storage: {}",
                    e.to_string()
                );
                vec![]
            }
        };
        for (predicate, _status) in registered_predicates.into_iter() {
            let predicate_uuid = predicate.uuid().to_string();
            match chainhook_config.register_specification(predicate) {
                Ok(_) => {
                    info!(
                        ctx.expect_logger(),
                        "Predicate {} retrieved from storage and loaded", predicate_uuid,
                    );
                }
                Err(e) => {
                    error!(
                        ctx.expect_logger(),
                        "Failed loading predicate from storage: {}",
                        e.to_string()
                    );
                }
            }
        }
    }

    // For each predicate found, register in memory.
    for predicate in predicates.into_iter() {
        match chainhook_config.register_full_specification(
            (
                &config.network.bitcoin_network,
                &config.network.stacks_network,
            ),
            predicate,
        ) {
            Ok(ref mut spec) => {
                chainhook_config.enable_specification(spec);
                info!(
                    ctx.expect_logger(),
                    "Predicate {} retrieved from config and loaded",
                    spec.uuid(),
                );
            }
            Err(e) => {
                error!(
                    ctx.expect_logger(),
                    "Failed loading predicate from config: {}",
                    e.to_string()
                );
            }
        }
    }

    if enable_internal_trigger {
        let _ = chainhook_config.register_specification(ChainhookSpecification::Bitcoin(
            BitcoinChainhookSpecification {
                uuid: format!("ordhook"),
                owner_uuid: None,
                name: format!("ordhook"),
                network: chainhook_sdk::types::BitcoinNetwork::Mainnet,
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
    chainhook_config
}
