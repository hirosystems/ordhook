use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{mpsc::Sender, Arc, Mutex},
};

use chainhook_sdk::{
    chainhooks::types::{
        BitcoinChainhookSpecification, ChainhookFullSpecification, ChainhookSpecification,
    },
    observer::{ObserverCommand, ObserverEvent},
    utils::Context,
};
use rocket::serde::json::{json, Json, Value as JsonValue};
use rocket::State;
use rocket::{
    config::{self, Config, LogLevel},
    Ignite, Rocket, Shutdown,
};

use crate::{
    config::PredicatesApi,
    service::observers::{
        insert_entry_in_observers, open_readwrite_observers_db_conn, remove_entry_from_observers,
        update_observer_progress, update_observer_streaming_enabled,
    },
    try_error, try_info,
};

use super::observers::{
    find_all_observers, find_observer_with_uuid, open_readonly_observers_db_conn, ObserverReport,
};

pub async fn start_observers_http_server(
    config: &crate::Config,
    observer_commands_tx: &std::sync::mpsc::Sender<ObserverCommand>,
    observer_event_rx: crossbeam_channel::Receiver<ObserverEvent>,
    bitcoin_scan_op_tx: crossbeam_channel::Sender<BitcoinChainhookSpecification>,
    ctx: &Context,
) -> Result<Shutdown, String> {
    let ignite = build_server(config, observer_commands_tx, ctx).await;
    let shutdown = ignite.shutdown();
    let _ = hiro_system_kit::thread_named("HTTP Observers API").spawn(move || {
        let _ = hiro_system_kit::nestable_block_on(ignite.launch());
    });

    loop {
        let event = match observer_event_rx.recv() {
            Ok(cmd) => cmd,
            Err(e) => {
                try_error!(ctx, "Error: broken channel {}", e.to_string());
                break;
            }
        };
        match event {
            ObserverEvent::PredicateRegistered(spec) => {
                let observers_db_conn = match open_readwrite_observers_db_conn(config, ctx) {
                    Ok(con) => con,
                    Err(e) => {
                        try_error!(ctx, "unable to register predicate: {}", e.to_string());
                        continue;
                    }
                };
                let report = ObserverReport::default();
                insert_entry_in_observers(&spec, &report, &observers_db_conn, ctx);
                match spec {
                    ChainhookSpecification::Bitcoin(predicate_spec) => {
                        let _ = bitcoin_scan_op_tx.send(predicate_spec);
                    }
                    _ => {}
                }
            }
            ObserverEvent::PredicateEnabled(spec) => {
                let observers_db_conn = match open_readwrite_observers_db_conn(&config, &ctx) {
                    Ok(con) => con,
                    Err(e) => {
                        try_error!(ctx, "unable to enable observer: {}", e.to_string());
                        continue;
                    }
                };
                update_observer_streaming_enabled(&spec.uuid(), true, &observers_db_conn, ctx);
            }
            ObserverEvent::PredicateDeregistered(uuid) => {
                let observers_db_conn = match open_readwrite_observers_db_conn(config, ctx) {
                    Ok(con) => con,
                    Err(e) => {
                        try_error!(ctx, "unable to deregister observer: {}", e.to_string());
                        continue;
                    }
                };
                remove_entry_from_observers(&uuid, &observers_db_conn, ctx);
            }
            ObserverEvent::BitcoinPredicateTriggered(data) => {
                if let Some(ref tip) = data.apply.last() {
                    let observers_db_conn = match open_readwrite_observers_db_conn(config, ctx) {
                        Ok(con) => con,
                        Err(e) => {
                            try_error!(ctx, "unable to update observer: {}", e.to_string());
                            continue;
                        }
                    };
                    let last_block_height_update = tip.block.block_identifier.index;
                    update_observer_progress(
                        &data.chainhook.uuid,
                        last_block_height_update,
                        &observers_db_conn,
                        ctx,
                    )
                }
            }
            ObserverEvent::Terminate => {
                try_info!(ctx, "Terminating runloop");
                break;
            }
            _ => {}
        }
    }
    Ok(shutdown)
}

async fn build_server(
    config: &crate::Config,
    observer_command_tx: &std::sync::mpsc::Sender<ObserverCommand>,
    ctx: &Context,
) -> Rocket<Ignite> {
    let PredicatesApi::On(ref api_config) = config.http_api else {
        unreachable!();
    };
    let moved_config = config.clone();
    let moved_ctx = ctx.clone();
    let moved_observer_commands_tx = observer_command_tx.clone();
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
        log_level: LogLevel::Off,
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
    let background_job_tx_mutex = Arc::new(Mutex::new(moved_observer_commands_tx));

    let ignite = rocket::custom(control_config)
        .manage(background_job_tx_mutex)
        .manage(moved_config)
        .manage(moved_ctx.clone())
        .mount("/", routes)
        .ignite()
        .await
        .expect("Unable to build observers API");
    ignite
}

#[get("/ping")]
fn handle_ping(ctx: &State<Context>) -> Json<JsonValue> {
    try_info!(ctx, "Handling HTTP GET /ping");
    Json(json!({
        "status": 200,
        "result": "chainhook service up and running",
    }))
}

#[get("/v1/observers", format = "application/json")]
fn handle_get_predicates(config: &State<crate::Config>, ctx: &State<Context>) -> Json<JsonValue> {
    try_info!(ctx, "Handling HTTP GET /v1/observers");
    match open_readonly_observers_db_conn(config, ctx) {
        Ok(mut db_conn) => {
            let observers = find_all_observers(&mut db_conn, &ctx);
            let serialized_predicates = observers
                .iter()
                .map(|(p, s)| serialized_predicate_with_status(p, s))
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
    config: &State<crate::Config>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    try_info!(ctx, "Handling HTTP POST /v1/observers");
    let predicate = predicate.into_inner();
    if let Err(e) = predicate.validate() {
        return Json(json!({
            "status": 422,
            "error": e,
        }));
    }

    let predicate_uuid = predicate.get_uuid().to_string();

    if let Ok(mut predicates_db_conn) = open_readonly_observers_db_conn(config, ctx) {
        let key: String = format!("{}", ChainhookSpecification::bitcoin_key(&predicate_uuid));
        match find_observer_with_uuid(&key, &mut predicates_db_conn, &ctx) {
            Some(_) => {
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
    config: &State<crate::Config>,
    ctx: &State<Context>,
) -> Json<JsonValue> {
    try_info!(ctx, "Handling HTTP GET /v1/observers/{}", predicate_uuid);
    match open_readonly_observers_db_conn(config, ctx) {
        Ok(mut predicates_db_conn) => {
            let key: String = format!("{}", ChainhookSpecification::bitcoin_key(&predicate_uuid));
            let entry = match find_observer_with_uuid(&key, &mut predicates_db_conn, &ctx) {
                Some((ChainhookSpecification::Bitcoin(spec), report)) => json!({
                    "chain": "bitcoin",
                    "uuid": spec.uuid,
                    "network": spec.network,
                    "predicate": spec.predicate,
                    "status": report,
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
    try_info!(ctx, "Handling HTTP DELETE /v1/observers/{}", predicate_uuid);
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

fn serialized_predicate_with_status(
    predicate: &ChainhookSpecification,
    report: &ObserverReport,
) -> JsonValue {
    match (predicate, report) {
        (ChainhookSpecification::Stacks(_), _) => json!({}),
        (ChainhookSpecification::Bitcoin(spec), report) => json!({
            "chain": "bitcoin",
            "uuid": spec.uuid,
            "network": spec.network,
            "predicate": spec.predicate,
            "status": report,
            "enabled": spec.enabled,
        }),
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::channel;

    use chainhook_sdk::utils::Context;
    use reqwest::Client;
    use rocket::{form::validate::Len, Shutdown};
    use serde_json::{json, Value};

    use crate::{
        config::{Config, PredicatesApi, PredicatesApiConfig},
        service::observers::{delete_observers_db, initialize_observers_db},
    };

    use super::start_observers_http_server;

    fn get_config() -> Config {
        let mut config = Config::devnet_default();
        config.http_api = PredicatesApi::On(PredicatesApiConfig {
            http_port: 20456,
            display_logs: true,
        });
        config.storage.observers_working_dir = "tmp".to_string();
        config
    }

    async fn launch_server() -> Shutdown {
        let config = get_config();
        let ctx = Context::empty();
        let _ = initialize_observers_db(&config, &ctx);
        let (bitcoin_scan_op_tx, _) = crossbeam_channel::unbounded();
        let (observer_command_tx, _) = channel();
        let (_, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = start_observers_http_server(
            &config,
            &observer_command_tx,
            observer_event_rx,
            bitcoin_scan_op_tx,
            &ctx,
        )
        .await
        .expect("start failed");
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        shutdown
    }

    fn shutdown_server(shutdown: Shutdown) {
        let config = get_config();
        shutdown.notify();
        delete_observers_db(&config);
    }

    #[tokio::test]
    async fn lists_empty_predicates() {
        let shutdown = launch_server().await;

        let client = Client::new();
        let response = client
            .get("http://localhost:20456/v1/observers")
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 200);
        assert_eq!(json["result"].as_array().len(), 0);

        shutdown_server(shutdown);
    }

    #[tokio::test]
    async fn rejects_invalid_predicate() {
        let shutdown = launch_server().await;

        let client = Client::new();
        let response = client
            .post("http://localhost:20456/v1/observers")
            .json(&json!({
                "id": 1,
            }))
            .send()
            .await
            .unwrap();

        // TODO: This should be a real error response body instead of a generic rocket error.
        assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

        shutdown_server(shutdown);
    }
}
