use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    sync::{mpsc::Sender, Arc, Mutex},
};

use chainhook_sdk::{
    chainhooks::types::{ChainhookFullSpecification, ChainhookSpecification},
    observer::ObserverCommand,
    utils::Context,
};
use hiro_system_kit::slog;
use redis::{Commands, Connection};
use rocket::config::{self, Config, LogLevel};
use rocket::serde::json::{json, Json, Value as JsonValue};
use rocket::State;
use std::error::Error;

use crate::config::PredicatesApiConfig;

use super::{open_readwrite_predicates_db_conn, PredicateStatus};

pub async fn start_predicate_api_server(
    api_config: PredicatesApiConfig,
    observer_commands_tx: Sender<ObserverCommand>,
    ctx: Context,
) -> Result<(), Box<dyn Error>> {
    let log_level = LogLevel::Off;

    let mut shutdown_config = config::Shutdown::default();
    shutdown_config.ctrlc = false;
    shutdown_config.grace = 1;
    shutdown_config.mercy = 1;

    let control_config = Config {
        port: api_config.http_port,
        workers: 1,
        address: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        keep_alive: 5,
        temp_dir: std::env::temp_dir().into(),
        log_level,
        cli_colors: false,
        shutdown: shutdown_config,
        ..Config::default()
    };

    let routes = routes![
        handle_ping,
        handle_get_predicates,
        handle_get_predicate,
        handle_create_predicate,
        handle_delete_bitcoin_predicate,
    ];

    let background_job_tx_mutex = Arc::new(Mutex::new(observer_commands_tx.clone()));

    let ctx_cloned = ctx.clone();

    let ignite = rocket::custom(control_config)
        .manage(background_job_tx_mutex)
        .manage(api_config)
        .manage(ctx_cloned)
        .mount("/", routes)
        .ignite()
        .await?;

    let _ = std::thread::spawn(move || {
        let _ = hiro_system_kit::nestable_block_on(ignite.launch());
    });
    Ok(())
}

#[get("/ping")]
fn handle_ping(ctx: &State<Context>) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "Handling HTTP GET /ping"));
    Json(json!({
        "status": 200,
        "result": "chainhook service up and running",
    }))
}

#[get("/v1/observers", format = "application/json")]
fn handle_get_predicates(
    api_config: &State<PredicatesApiConfig>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "Handling HTTP GET /v1/observers"));
    match open_readwrite_predicates_db_conn(api_config) {
        Ok(mut predicates_db_conn) => {
            let predicates = match get_entries_from_predicates_db(&mut predicates_db_conn, &ctx) {
                Ok(predicates) => predicates,
                Err(e) => {
                    ctx.try_log(|logger| slog::warn!(logger, "unable to retrieve predicates: {e}"));
                    return Json(json!({
                        "status": 500,
                        "message": "unable to retrieve predicates",
                    }));
                }
            };

            let serialized_predicates = predicates
                .iter()
                .map(|(p, _)| p.into_serialized_json())
                .collect::<Vec<_>>();

            Json(json!({
                "status": 200,
                "result": serialized_predicates
            }))
        }
        Err(e) => Json(json!({
            "status": 500,
            "message": e,
        })),
    }
}

#[post("/v1/observers", format = "application/json", data = "<predicate>")]
fn handle_create_predicate(
    predicate: Json<ChainhookFullSpecification>,
    api_config: &State<PredicatesApiConfig>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "Handling HTTP POST /v1/observers"));
    let predicate = predicate.into_inner();
    if let Err(e) = predicate.validate() {
        return Json(json!({
            "status": 422,
            "error": e,
        }));
    }

    let predicate_uuid = predicate.get_uuid().to_string();

    if let Ok(mut predicates_db_conn) = open_readwrite_predicates_db_conn(api_config) {
        let key: String = format!(
            "hord::{}",
            ChainhookSpecification::bitcoin_key(&predicate_uuid)
        );
        match get_entry_from_predicates_db(&key, &mut predicates_db_conn, &ctx) {
            Ok(Some(_)) => {
                return Json(json!({
                    "status": 409,
                    "error": "Predicate uuid already in use",
                }))
            }
            _ => {}
        }
    }

    let background_job_tx = background_job_tx.inner();
    match background_job_tx.lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::RegisterPredicate(predicate));
        }
        _ => {}
    };

    Json(json!({
        "status": 200,
        "result": predicate_uuid,
    }))
}

#[get("/v1/observers/<predicate_uuid>", format = "application/json")]
fn handle_get_predicate(
    predicate_uuid: String,
    api_config: &State<PredicatesApiConfig>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| slog::info!(logger, "Handling HTTP GET /v1/observers/{}", predicate_uuid));

    match open_readwrite_predicates_db_conn(api_config) {
        Ok(mut predicates_db_conn) => {
            let key: String = format!(
                "hord::{}",
                ChainhookSpecification::bitcoin_key(&predicate_uuid)
            );
            let entry = match get_entry_from_predicates_db(&key, &mut predicates_db_conn, &ctx) {
                Ok(Some((ChainhookSpecification::Stacks(spec), status))) => json!({
                    "chain": "stacks",
                    "uuid": spec.uuid,
                    "network": spec.network,
                    "predicate": spec.predicate,
                    "status": status,
                    "enabled": spec.enabled,
                }),
                Ok(Some((ChainhookSpecification::Bitcoin(spec), status))) => json!({
                    "chain": "bitcoin",
                    "uuid": spec.uuid,
                    "network": spec.network,
                    "predicate": spec.predicate,
                    "status": status,
                    "enabled": spec.enabled,
                }),
                _ => {
                    return Json(json!({
                        "status": 404,
                    }))
                }
            };
            Json(json!({
                "status": 200,
                "result": entry
            }))
        }
        Err(e) => Json(json!({
            "status": 500,
            "message": e,
        })),
    }
}

#[delete("/v1/observers/<predicate_uuid>", format = "application/json")]
fn handle_delete_bitcoin_predicate(
    predicate_uuid: String,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    ctx.try_log(|logger| {
        slog::info!(
            logger,
            "Handling HTTP DELETE /v1/observers/{}",
            predicate_uuid
        )
    });

    let background_job_tx = background_job_tx.inner();
    match background_job_tx.lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::DeregisterBitcoinPredicate(predicate_uuid));
        }
        _ => {}
    };

    Json(json!({
        "status": 200,
        "result": "Ok",
    }))
}

pub fn get_entry_from_predicates_db(
    predicate_key: &str,
    predicate_db_conn: &mut Connection,
    _ctx: &Context,
) -> Result<Option<(ChainhookSpecification, PredicateStatus)>, String> {
    let entry: HashMap<String, String> = predicate_db_conn.hgetall(predicate_key).map_err(|e| {
        format!(
            "unable to load predicate associated with key {}: {}",
            predicate_key,
            e.to_string()
        )
    })?;

    let encoded_spec = match entry.get("specification") {
        None => return Ok(None),
        Some(payload) => payload,
    };

    let spec = ChainhookSpecification::deserialize_specification(&encoded_spec)?;

    let encoded_status = match entry.get("status") {
        None => unimplemented!(),
        Some(payload) => payload,
    };

    let status = serde_json::from_str(&encoded_status).map_err(|e| format!("{}", e.to_string()))?;

    Ok(Some((spec, status)))
}

pub fn get_entries_from_predicates_db(
    predicate_db_conn: &mut Connection,
    ctx: &Context,
) -> Result<Vec<(ChainhookSpecification, PredicateStatus)>, String> {
    let key: String = format!("hord::{}", ChainhookSpecification::bitcoin_key("*"));
    let chainhooks_to_load: Vec<String> = predicate_db_conn
        .scan_match(key)
        .map_err(|e| format!("unable to connect to redis: {}", e.to_string()))?
        .into_iter()
        .collect();

    let mut predicates = vec![];
    for predicate_key in chainhooks_to_load.iter() {
        let chainhook = match get_entry_from_predicates_db(predicate_key, predicate_db_conn, ctx) {
            Ok(Some((spec, status))) => (spec, status),
            Ok(None) => {
                warn!(
                    ctx.expect_logger(),
                    "unable to load predicate associated with key {}", predicate_key,
                );
                continue;
            }
            Err(e) => {
                error!(
                    ctx.expect_logger(),
                    "unable to load predicate associated with key {}: {}",
                    predicate_key,
                    e.to_string()
                );
                continue;
            }
        };
        predicates.push(chainhook);
    }
    Ok(predicates)
}

pub fn load_predicates_from_redis(
    config: &crate::config::Config,
    ctx: &Context,
) -> Result<Vec<(ChainhookSpecification, PredicateStatus)>, String> {
    let redis_uri: &str = config.expected_api_database_uri();
    let client = redis::Client::open(redis_uri.clone())
        .map_err(|e| format!("unable to connect to redis: {}", e.to_string()))?;
    let mut predicate_db_conn = client
        .get_connection()
        .map_err(|e| format!("unable to connect to redis: {}", e.to_string()))?;
    get_entries_from_predicates_db(&mut predicate_db_conn, ctx)
}
