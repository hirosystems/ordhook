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
use rocket::{
    config::{self, Config, LogLevel},
    Ignite, Rocket, Shutdown,
};
use rocket::{
    http::Status,
    response::status,
    serde::json::{json, Json, Value},
};
use rocket::{response::status::Custom, State};

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
    // Build and start HTTP server.
    let ignite = build_server(config, observer_commands_tx, ctx).await;
    let shutdown = ignite.shutdown();
    let _ = hiro_system_kit::thread_named("observers_api-server").spawn(move || {
        let _ = hiro_system_kit::nestable_block_on(ignite.launch());
    });

    // Spawn predicate observer event tread.
    let moved_config = config.clone();
    let moved_ctx = ctx.clone();
    let _ = hiro_system_kit::thread_named("observers_api-events").spawn(move || loop {
        let event = match observer_event_rx.recv() {
            Ok(cmd) => cmd,
            Err(e) => {
                try_error!(&moved_ctx, "Error: broken channel {}", e.to_string());
                break;
            }
        };
        match event {
            ObserverEvent::PredicateRegistered(spec) => {
                let observers_db_conn =
                    match open_readwrite_observers_db_conn(&moved_config, &moved_ctx) {
                        Ok(con) => con,
                        Err(e) => {
                            try_error!(
                                &moved_ctx,
                                "unable to register predicate: {}",
                                e.to_string()
                            );
                            continue;
                        }
                    };
                let report = ObserverReport::default();
                insert_entry_in_observers(&spec, &report, &observers_db_conn, &moved_ctx);
                match spec {
                    ChainhookSpecification::Bitcoin(predicate_spec) => {
                        let _ = bitcoin_scan_op_tx.send(predicate_spec);
                    }
                    _ => {}
                }
            }
            ObserverEvent::PredicateEnabled(spec) => {
                let observers_db_conn =
                    match open_readwrite_observers_db_conn(&moved_config, &moved_ctx) {
                        Ok(con) => con,
                        Err(e) => {
                            try_error!(&moved_ctx, "unable to enable observer: {}", e.to_string());
                            continue;
                        }
                    };
                update_observer_streaming_enabled(
                    &spec.uuid(),
                    true,
                    &observers_db_conn,
                    &moved_ctx,
                );
            }
            ObserverEvent::PredicateDeregistered(uuid) => {
                let observers_db_conn =
                    match open_readwrite_observers_db_conn(&moved_config, &moved_ctx) {
                        Ok(con) => con,
                        Err(e) => {
                            try_error!(
                                &moved_ctx,
                                "unable to deregister observer: {}",
                                e.to_string()
                            );
                            continue;
                        }
                    };
                remove_entry_from_observers(&uuid, &observers_db_conn, &moved_ctx);
            }
            ObserverEvent::BitcoinPredicateTriggered(data) => {
                if let Some(ref tip) = data.apply.last() {
                    let observers_db_conn =
                        match open_readwrite_observers_db_conn(&moved_config, &moved_ctx) {
                            Ok(con) => con,
                            Err(e) => {
                                try_error!(
                                    &moved_ctx,
                                    "unable to update observer: {}",
                                    e.to_string()
                                );
                                continue;
                            }
                        };
                    let last_block_height_update = tip.block.block_identifier.index;
                    update_observer_progress(
                        &data.chainhook.uuid,
                        last_block_height_update,
                        &observers_db_conn,
                        &moved_ctx,
                    )
                }
            }
            ObserverEvent::Terminate => {
                try_info!(&moved_ctx, "Terminating runloop");
                break;
            }
            _ => {}
        }
    });

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
fn handle_ping(ctx: &State<Context>) -> Json<Value> {
    try_info!(ctx, "Handling HTTP GET /ping");
    Json(json!({
        "status": 200,
        "result": "chainhook service up and running",
    }))
}

#[get("/v1/observers", format = "application/json")]
fn handle_get_predicates(
    config: &State<crate::Config>,
    ctx: &State<Context>,
) -> Result<Json<Value>, Custom<Json<Value>>> {
    try_info!(ctx, "Handling HTTP GET /v1/observers");
    match open_readonly_observers_db_conn(config, ctx) {
        Ok(mut db_conn) => {
            let observers = find_all_observers(&mut db_conn, &ctx);
            let serialized_predicates = observers
                .iter()
                .map(|(p, s)| serialized_predicate_with_status(p, s))
                .collect::<Vec<_>>();
            Ok(Json(json!({
                "status": 200,
                "result": serialized_predicates
            })))
        }
        Err(e) => Err(Custom(
            Status::InternalServerError,
            Json(json!({
                "status": 500,
                "message": e,
            })),
        )),
    }
}

#[post("/v1/observers", format = "application/json", data = "<predicate>")]
fn handle_create_predicate(
    predicate: Json<Value>,
    config: &State<crate::Config>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Result<Json<Value>, Custom<Json<Value>>> {
    try_info!(ctx, "Handling HTTP POST /v1/observers");
    let predicate =
        match serde_json::from_value::<ChainhookFullSpecification>(predicate.into_inner()) {
            Ok(predicate) => predicate,
            Err(_) => {
                return Err(Custom(
                    Status::UnprocessableEntity,
                    Json(json!({
                        "status": 422,
                        "error": "Invalid predicate JSON",
                    })),
                ));
            }
        };
    if let Err(e) = predicate.validate() {
        return Err(Custom(
            Status::UnprocessableEntity,
            Json(json!({
                "status": 422,
                "error": e,
            })),
        ));
    }
    let mut predicates_db_conn = match open_readonly_observers_db_conn(config, ctx) {
        Ok(conn) => conn,
        Err(err) => {
            return Err(Custom(
                Status::InternalServerError,
                Json(json!({
                    "status": 500,
                    "error": err.to_string(),
                })),
            ));
        }
    };
    let predicate_uuid = predicate.get_uuid().to_string();
    if find_observer_with_uuid(&predicate_uuid, &mut predicates_db_conn, &ctx).is_some() {
        return Err(status::Custom(
            Status::Conflict,
            Json(json!({
                "status": 409,
                "error": "Predicate uuid already in use",
            })),
        ));
    }
    match background_job_tx.inner().lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::RegisterPredicate(predicate));
        }
        Err(err) => {
            return Err(Custom(
                Status::InternalServerError,
                Json(json!({
                    "status": 500,
                    "error": err.to_string(),
                })),
            ));
        }
    };
    Ok(Json(json!({
        "status": 200,
        "result": predicate_uuid,
    })))
}

#[get("/v1/observers/<predicate_uuid>", format = "application/json")]
fn handle_get_predicate(
    predicate_uuid: String,
    config: &State<crate::Config>,
    ctx: &State<Context>,
) -> Result<Json<Value>, Custom<Json<Value>>> {
    try_info!(ctx, "Handling HTTP GET /v1/observers/{}", predicate_uuid);
    match open_readonly_observers_db_conn(config, ctx) {
        Ok(mut predicates_db_conn) => {
            let entry =
                match find_observer_with_uuid(&predicate_uuid, &mut predicates_db_conn, &ctx) {
                    Some((ChainhookSpecification::Bitcoin(spec), report)) => json!({
                        "chain": "bitcoin",
                        "uuid": spec.uuid,
                        "network": spec.network,
                        "predicate": spec.predicate,
                        "status": report,
                        "enabled": spec.enabled,
                    }),
                    _ => {
                        return Err(Custom(
                            Status::NotFound,
                            Json(json!({
                                "status": 404,
                            })),
                        ))
                    }
                };
            Ok(Json(json!({
                "status": 200,
                "result": entry
            })))
        }
        Err(e) => Err(Custom(
            Status::InternalServerError,
            Json(json!({
                "status": 500,
                "message": e,
            })),
        )),
    }
}

#[delete("/v1/observers/<predicate_uuid>", format = "application/json")]
fn handle_delete_bitcoin_predicate(
    predicate_uuid: String,
    config: &State<crate::Config>,
    background_job_tx: &State<Arc<Mutex<Sender<ObserverCommand>>>>,
    ctx: &State<Context>,
) -> Result<Json<Value>, Custom<Json<Value>>> {
    try_info!(ctx, "Handling HTTP DELETE /v1/observers/{}", predicate_uuid);
    let mut predicates_db_conn = match open_readonly_observers_db_conn(config, ctx) {
        Ok(conn) => conn,
        Err(err) => {
            return Err(Custom(
                Status::InternalServerError,
                Json(json!({
                    "status": 500,
                    "error": err.to_string(),
                })),
            ));
        }
    };
    if find_observer_with_uuid(&predicate_uuid, &mut predicates_db_conn, &ctx).is_none() {
        return Err(status::Custom(
            Status::NotFound,
            Json(json!({
                "status": 404,
                "error": "Predicate not found",
            })),
        ));
    }
    match background_job_tx.inner().lock() {
        Ok(tx) => {
            let _ = tx.send(ObserverCommand::DeregisterBitcoinPredicate(predicate_uuid));
        }
        Err(err) => {
            return Err(Custom(
                Status::InternalServerError,
                Json(json!({
                    "status": 500,
                    "error": err.to_string(),
                })),
            ));
        }
    };

    Ok(Json(json!({
        "status": 200,
        "result": "Predicate deleted",
    })))
}

fn serialized_predicate_with_status(
    predicate: &ChainhookSpecification,
    report: &ObserverReport,
) -> Value {
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

    use chainhook_sdk::{
        chainhooks::types::{
            BitcoinChainhookSpecification, BitcoinPredicateType, ChainhookSpecification,
            HookAction, HttpHook, InscriptionFeedData, OrdinalOperations,
        },
        observer::ObserverEvent,
        types::BitcoinNetwork,
        utils::Context,
    };
    use crossbeam_channel::{Receiver, Sender};
    use reqwest::{Client, Response};
    use rocket::{form::validate::Len, Shutdown};
    use serde_json::{json, Value};

    use crate::{
        config::{Config, PredicatesApi, PredicatesApiConfig},
        service::observers::{delete_observers_db, initialize_observers_db},
    };

    use super::start_observers_http_server;

    async fn launch_server(observer_event_rx: Receiver<ObserverEvent>) -> Shutdown {
        let mut config = Config::devnet_default();
        config.http_api = PredicatesApi::On(PredicatesApiConfig {
            http_port: 20456,
            display_logs: true,
        });
        config.storage.observers_working_dir = "tmp".to_string();
        let ctx = Context::empty();
        delete_observers_db(&config);
        let _ = initialize_observers_db(&config, &ctx);
        let (bitcoin_scan_op_tx, _) = crossbeam_channel::unbounded();
        let (observer_command_tx, _) = channel();
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

    fn shutdown_server(observer_event_tx: Sender<ObserverEvent>, shutdown: Shutdown) {
        let _ = observer_event_tx.send(ObserverEvent::Terminate);
        shutdown.notify();
    }

    async fn register_predicate(
        client: &Client,
        observer_event_tx: &Sender<ObserverEvent>,
    ) -> Response {
        let response = client
            .post("http://localhost:20456/v1/observers")
            .json(&json!({
                "uuid": "00000001-0001-0001-0001-000000000001",
                "name": "inscription_feed",
                "version": 1,
                "chain": "bitcoin",
                "networks": {
                    "mainnet": {
                        "start_block": 767430,
                        "if_this": {
                            "scope": "ordinals_protocol",
                            "operation": "inscription_feed",
                        },
                        "then_that": {
                            "http_post": {
                                "url": "http://localhost:3700/payload",
                                "authorization_header": "Bearer test"
                            }
                        }
                    }
                }
            }))
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            // Simulate predicate accepted by chainhook-sdk
            let spec = ChainhookSpecification::Bitcoin(BitcoinChainhookSpecification {
                uuid: "00000001-0001-0001-0001-000000000001".to_string(),
                owner_uuid: None,
                name: "inscription_feed".to_string(),
                network: BitcoinNetwork::Mainnet,
                version: 1,
                blocks: None,
                start_block: Some(767430),
                end_block: None,
                expire_after_occurrence: None,
                predicate: BitcoinPredicateType::OrdinalsProtocol(
                    OrdinalOperations::InscriptionFeed(InscriptionFeedData {
                        meta_protocols: None,
                    }),
                ),
                action: HookAction::HttpPost(HttpHook {
                    url: "http://localhost:3700/payload".to_string(),
                    authorization_header: "Bearer test".to_string(),
                }),
                include_proof: false,
                include_inputs: false,
                include_outputs: false,
                include_witness: false,
                enabled: true,
                expired_at: None,
            });
            let _ = observer_event_tx.send(ObserverEvent::PredicateRegistered(spec.clone()));
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let _ = observer_event_tx.send(ObserverEvent::PredicateEnabled(spec));
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        response
    }

    async fn delete_predicate(
        client: &Client,
        observer_event_tx: &Sender<ObserverEvent>,
    ) -> Response {
        let response = client
            .delete("http://localhost:20456/v1/observers/00000001-0001-0001-0001-000000000001")
            .header("content-type", "application/json")
            .send()
            .await
            .unwrap();
        if response.status().is_success() {
            // Simulate predicate deregistered by chainhook-sdk
            let _ = observer_event_tx.send(ObserverEvent::PredicateDeregistered(
                "00000001-0001-0001-0001-000000000001".to_string(),
            ));
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        response
    }

    #[tokio::test]
    async fn lists_empty_predicates() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

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

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn rejects_arbitrary_json() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let response = client
            .post("http://localhost:20456/v1/observers")
            .json(&json!({
                "id": 1,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 422);

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn rejects_invalid_predicate() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let response = client
            .post("http://localhost:20456/v1/observers")
            .json(&json!({
                "uuid": "00000001-0001-0001-0001-000000000001",
                "name": "inscription_feed",
                "version": 1,
                "chain": "bitcoin",
                "networks": {
                    "mainnet": {
                        "start_block": 767430,
                        "end_block": 200, // Invalid
                        "if_this": {
                            "scope": "ordinals_protocol",
                            "operation": "inscription_feed",
                        },
                        "then_that": {
                            "http_post": {
                                "url": "http://localhost:3700/payload",
                                "authorization_header": "Bearer test"
                            }
                        }
                    }
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 422);
        assert_eq!(
            json["error"],
            "Chainhook specification field `end_block` should be greater than `start_block`."
        );

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn accepts_valid_predicate() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let response = register_predicate(&client, &observer_event_tx).await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 200);
        assert_eq!(json["result"], "00000001-0001-0001-0001-000000000001");

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn lists_predicate() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let _ = register_predicate(&client, &observer_event_tx).await;
        let response = client
            .get("http://localhost:20456/v1/observers")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let json2: Value = response.json().await.unwrap();
        assert_eq!(json2["status"], 200);
        let results = json2["result"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["uuid"], "00000001-0001-0001-0001-000000000001");

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn shows_predicate() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let _ = register_predicate(&client, &observer_event_tx).await;
        let response = client
            .get("http://localhost:20456/v1/observers/00000001-0001-0001-0001-000000000001")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 200);
        assert_eq!(
            json["result"]["uuid"],
            "00000001-0001-0001-0001-000000000001"
        );

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn rejects_duplicate_predicate_uuid() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let _ = register_predicate(&client, &observer_event_tx).await;
        let response = register_predicate(&client, &observer_event_tx).await;
        assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 409);

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn deletes_registered_predicate() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let _ = register_predicate(&client, &observer_event_tx).await;
        let response = delete_predicate(&client, &observer_event_tx).await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 200);

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn unlists_deleted_predicate() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let _ = register_predicate(&client, &observer_event_tx).await;
        let _ = delete_predicate(&client, &observer_event_tx).await;
        let response = client
            .get("http://localhost:20456/v1/observers")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["result"].as_array().len(), 0);

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn unshows_deleted_predicate() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let _ = register_predicate(&client, &observer_event_tx).await;
        let _ = delete_predicate(&client, &observer_event_tx).await;
        let response = client
            .get("http://localhost:20456/v1/observers/00000001-0001-0001-0001-000000000001")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn rejects_non_existing_predicate_delete() {
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let shutdown = launch_server(observer_event_rx).await;

        let client = Client::new();
        let response = delete_predicate(&client, &observer_event_tx).await;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        let json: Value = response.json().await.unwrap();
        assert_eq!(json["status"], 404);

        shutdown_server(observer_event_tx, shutdown);
    }

    #[tokio::test]
    async fn accepts_ping() {
        //
    }
}
