#[macro_use]
extern crate error_chain;

mod serde;

use core::panic;
use crossbeam_channel::{select, Sender};
use neon::prelude::*;
use ordhook::chainhook_sdk::observer::DataHandlerEvent;
use ordhook::chainhook_sdk::utils::Context as OrdhookContext;
use ordhook::config::Config;
use ordhook::service::Service;
use std::thread;

struct OrdinalsIndexerConfig {
    pub bitcoin_rpc_url: String,
    pub bitcoin_rpc_username: String,
    pub bitcoin_rpc_password: String,
    pub working_directory: String,
    pub logs_enabled: bool,
}

impl OrdinalsIndexerConfig {
    pub fn default() -> OrdinalsIndexerConfig {
        OrdinalsIndexerConfig {
            bitcoin_rpc_url: "http://0.0.0.0:8332".to_string(),
            bitcoin_rpc_username: "devnet".to_string(),
            bitcoin_rpc_password: "devnet".to_string(),
            working_directory: "/tmp/ordinals".to_string(),
            logs_enabled: true,
        }
    }
}

struct OrdinalsIndexer {
    command_tx: Sender<IndexerCommand>,
    custom_indexer_command_tx: Sender<CustomIndexerCommand>,
}

#[allow(dead_code)]
enum IndexerCommand {
    Start,
    Stop,
}

enum CustomIndexerCommand {
    UpdateApplyCallback(Root<JsFunction>),
    UpdateUndoCallback(Root<JsFunction>),
}

impl Finalize for OrdinalsIndexer {}

impl OrdinalsIndexer {
    fn new<'a, C>(cx: &mut C, ordhook_config: Config) -> Self
    where
        C: Context<'a>,
    {
        let (command_tx, command_rx) = crossbeam_channel::unbounded();
        let (custom_indexer_command_tx, custom_indexer_command_rx) = crossbeam_channel::unbounded();

        let logger = hiro_system_kit::log::setup_logger();
        let _guard = hiro_system_kit::log::setup_global_logger(logger.clone());
        let ctx = OrdhookContext {
            logger: Some(logger),
            tracer: false,
        };
    
        // Initialize service
        // {
        //     let _ = initialize_ordhook_db(&ordhook_config.expected_cache_path(), &ctx);
        //     let _ = open_readwrite_ordhook_db_conn_rocks_db(&ordhook_config.expected_cache_path(), &ctx);
        // }
        let mut service: Service = Service::new(ordhook_config, ctx);

        // Set-up the observer sidecar - used for augmenting the bitcoin blocks with
        // ordinals informations
        let observer_sidecar = service
            .set_up_observer_sidecar_runloop()
            .expect("unable to setup indexer");
        // Prepare internal predicate
        let (observer_config, payload_rx) = service
            .set_up_observer_config(vec![], true)
            .expect("unable to setup indexer");

        // Indexing thread
        let channel = cx.channel();
        thread::spawn(move || {
            let payload_rx = payload_rx.unwrap();

            channel.send(move |mut cx| {
                let mut apply_callback: Option<Root<JsFunction>> = None;
                let mut undo_callback: Option<Root<JsFunction>> = None;

                loop {
                    select! {
                        recv(payload_rx) -> msg => {
                            match msg {
                                Ok(DataHandlerEvent::Process(payload)) => {
                                    if let Some(ref callback) = undo_callback {
                                        for to_rollback in payload.rollback.into_iter() {
                                            let callback = callback.clone(&mut cx).into_inner(&mut cx);
                                            let this = cx.undefined();
                                            let payload = serde::to_value(&mut cx, &to_rollback).expect("Unable to serialize block");
                                            let args: Vec<Handle<JsValue>> = vec![payload];
                                            callback.call(&mut cx, this, args)?;
                                        }
                                    }

                                    if let Some(ref callback) = apply_callback {
                                        for to_apply in payload.apply.into_iter() {
                                            let callback = callback.clone(&mut cx).into_inner(&mut cx);
                                            let this = cx.undefined();
                                            let payload = serde::to_value(&mut cx, &to_apply).expect("Unable to serialize block");
                                            let args: Vec<Handle<JsValue>> = vec![payload];
                                            callback.call(&mut cx, this, args)?;
                                        }
                                    }
                                }
                                Ok(DataHandlerEvent::Terminate) => {
                                    return Ok(());
                                }
                                _ => {

                                }
                            }
                        }
                        recv(custom_indexer_command_rx) -> msg => {
                            match msg {
                                Ok(CustomIndexerCommand::UpdateApplyCallback(callback)) => {
                                    apply_callback = Some(callback);
                                }
                                Ok(CustomIndexerCommand::UpdateUndoCallback(callback)) => {
                                    undo_callback = Some(callback);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            });
        });

        // Processing thread
        thread::spawn(move || {
            loop {
                let cmd = match command_rx.recv() {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        panic!("Runloop error: {}", e.to_string());
                    }
                };

                match cmd {
                    IndexerCommand::Start => {
                        // We start the service as soon as the start() method is being called.
                        let future = service.catch_up_with_chain_tip(false, &observer_config);
                        let _ = hiro_system_kit::nestable_block_on(future)
                            .expect("unable to start indexer");
                        let future = service.start_event_observer(observer_sidecar);
                        let (command_tx, event_rx) = hiro_system_kit::nestable_block_on(future)
                            .expect("unable to start indexer");
                        // Blocking call
                        let _ = service.start_main_runloop(&command_tx, event_rx, None);
                        break;
                    }
                    IndexerCommand::Stop => {
                        break;
                    }
                }
            }
        });

        Self {
            command_tx,
            custom_indexer_command_tx,
            // termination_rx,
        }
    }

    fn start(&self) -> Result<bool, String> {
        let _ = self.command_tx.send(IndexerCommand::Start);
        Ok(true)
    }

    fn update_apply_callback(&self, callback: Root<JsFunction>) -> Result<bool, String> {
        let _ = self
            .custom_indexer_command_tx
            .send(CustomIndexerCommand::UpdateApplyCallback(callback));
        Ok(true)
    }

    fn update_undo_callback(&self, callback: Root<JsFunction>) -> Result<bool, String> {
        let _ = self
            .custom_indexer_command_tx
            .send(CustomIndexerCommand::UpdateUndoCallback(callback));
        Ok(true)
    }
}

impl OrdinalsIndexer {
    fn js_new(mut cx: FunctionContext) -> JsResult<JsBox<OrdinalsIndexer>> {
        let settings = cx.argument::<JsObject>(0)?;

        let mut config = OrdinalsIndexerConfig::default();

        if let Ok(res) = settings
            .get(&mut cx, "bitcoinRpcUrl")?
            .downcast::<JsString, _>(&mut cx)
        {
            config.bitcoin_rpc_url = res.value(&mut cx);
        }
        if let Ok(res) = settings
            .get(&mut cx, "bitcoinRpcUsername")?
            .downcast::<JsString, _>(&mut cx)
        {
            config.bitcoin_rpc_username = res.value(&mut cx);
        }

        if let Ok(res) = settings
            .get(&mut cx, "bitcoinRpcPassword")?
            .downcast::<JsString, _>(&mut cx)
        {
            config.bitcoin_rpc_password = res.value(&mut cx);
        }

        if let Ok(res) = settings
            .get(&mut cx, "workingDirectory")?
            .downcast::<JsString, _>(&mut cx)
        {
            config.working_directory = res.value(&mut cx);
        }

        if let Ok(res) = settings
            .get(&mut cx, "logs")?
            .downcast::<JsBoolean, _>(&mut cx)
        {
            config.logs_enabled = res.value(&mut cx);
        }

        let mut ordhook_config = Config::mainnet_default();
        ordhook_config.network.bitcoind_rpc_username = config.bitcoin_rpc_username.clone();
        ordhook_config.network.bitcoind_rpc_password = config.bitcoin_rpc_password.clone();
        ordhook_config.network.bitcoind_rpc_url = config.bitcoin_rpc_url.clone();
        ordhook_config.storage.working_dir = config.working_directory.clone();
        ordhook_config.logs.chainhook_internals = config.logs_enabled;
        ordhook_config.logs.ordinals_internals = config.logs_enabled;

        let devnet: OrdinalsIndexer = OrdinalsIndexer::new(&mut cx, ordhook_config);
        Ok(cx.boxed(devnet))
    }

    fn js_start(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        cx.this()
            .downcast_or_throw::<JsBox<OrdinalsIndexer>, _>(&mut cx)?
            .start()
            .or_else(|err| cx.throw_error(err.to_string()))?;

        Ok(cx.undefined())
    }

    fn js_terminate(mut _cx: FunctionContext) -> JsResult<JsBoolean> {
        unimplemented!();
    }

    fn js_on_block_apply(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let callback = cx.argument::<JsFunction>(0)?.root(&mut cx);

        cx.this()
            .downcast_or_throw::<JsBox<OrdinalsIndexer>, _>(&mut cx)?
            .update_apply_callback(callback)
            .or_else(|err| cx.throw_error(err.to_string()))?;

        Ok(cx.undefined())
    }

    fn js_on_block_undo(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let callback = cx.argument::<JsFunction>(0)?.root(&mut cx);

        cx.this()
            .downcast_or_throw::<JsBox<OrdinalsIndexer>, _>(&mut cx)?
            .update_undo_callback(callback)
            .or_else(|err| cx.throw_error(err.to_string()))?;

        Ok(cx.undefined())
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("ordinalsIndexerNew", OrdinalsIndexer::js_new)?;
    cx.export_function("ordinalsIndexerStart", OrdinalsIndexer::js_start)?;
    cx.export_function("ordinalsIndexerTerminate", OrdinalsIndexer::js_terminate)?;
    cx.export_function(
        "ordinalsIndexerOnBlockApply",
        OrdinalsIndexer::js_on_block_apply,
    )?;
    cx.export_function(
        "ordinalsIndexerOnBlockUndo",
        OrdinalsIndexer::js_on_block_undo,
    )?;
    Ok(())
}
