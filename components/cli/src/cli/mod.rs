use std::{
    path::PathBuf,
    process,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};

use bitcoind::{try_error, try_info, types::BlockIdentifier, utils::Context};
use clap::Parser;
use commands::{Command, ConfigCommand, DatabaseCommand, IndexCommand, Protocol, ServiceCommand};
use config::{generator::generate_toml_config, Config};
use hiro_system_kit;

mod commands;

pub fn main() {
    let logger = hiro_system_kit::log::setup_logger();
    let _guard = hiro_system_kit::log::setup_global_logger(logger.clone());
    let ctx = Context {
        logger: Some(logger),
        tracer: false,
    };

    let opts: Protocol = match Protocol::try_parse() {
        Ok(opts) => opts,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };

    if let Err(e) = hiro_system_kit::nestable_block_on(handle_command(opts, &ctx)) {
        try_error!(&ctx, "{e}");
        std::thread::sleep(std::time::Duration::from_millis(500));
        process::exit(1);
    }
}

fn check_maintenance_mode(ctx: &Context) {
    let maintenance_enabled = std::env::var("ORDHOOK_MAINTENANCE").unwrap_or("0".into());
    if maintenance_enabled.eq("1") {
        try_info!(
            ctx,
            "Entering maintenance mode. Unset ORDHOOK_MAINTENANCE and reboot to resume operations"
        );
        sleep(Duration::from_secs(u64::MAX))
    }
}

fn confirm_rollback(
    current_chain_tip: &BlockIdentifier,
    blocks_to_rollback: u32,
) -> Result<(), String> {
    println!("Index chain tip is at #{current_chain_tip}");
    println!(
        "{} blocks will be dropped. New index chain tip will be at #{}. Confirm? [Y/n]",
        blocks_to_rollback,
        current_chain_tip.index - blocks_to_rollback as u64
    );
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer).unwrap();
    if buffer.starts_with('n') {
        return Err("Deletion aborted".to_string());
    }
    Ok(())
}

async fn handle_command(opts: Protocol, ctx: &Context) -> Result<(), String> {
    // Set up the interrupt signal handler.
    let abort_signal = Arc::new(AtomicBool::new(false));
    let abort_signal_clone = abort_signal.clone();
    let ctx_moved = ctx.clone();
    ctrlc::set_handler(move || {
        try_info!(
            ctx_moved,
            "bitcoin-indexer received interrupt signal, shutting down..."
        );
        abort_signal_clone.store(true, Ordering::SeqCst);
    })
    .map_err(|e| format!("bitcoin-indexer failed to set interrupt signal handler: {e}"))?;

    match opts {
        Protocol::Ordinals(subcmd) => match subcmd {
            Command::Service(subcmd) => match subcmd {
                ServiceCommand::Start(cmd) => {
                    check_maintenance_mode(ctx);
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_ordinals_config()?;
                    ordinals::start_ordinals_indexer(true, &abort_signal, &config, ctx).await?
                }
            },
            Command::Index(index_command) => match index_command {
                IndexCommand::Sync(cmd) => {
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_ordinals_config()?;
                    ordinals::start_ordinals_indexer(false, &abort_signal, &config, ctx).await?
                }
                IndexCommand::Rollback(cmd) => {
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_ordinals_config()?;
                    let chain_tip = ordinals::get_chain_tip(&config).await?;
                    confirm_rollback(&chain_tip, cmd.blocks)?;
                    ordinals::rollback_block_range(
                        chain_tip.index - cmd.blocks as u64,
                        chain_tip.index,
                        &config,
                        ctx,
                    )
                    .await?;
                    println!("{} blocks dropped", cmd.blocks);
                }
            },
            Command::Database(database_command) => match database_command {
                DatabaseCommand::Migrate(cmd) => {
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_ordinals_config()?;
                    ordinals::db::migrate_dbs(&config, ctx).await?;
                }
            },
        },
        Protocol::Runes(subcmd) => match subcmd {
            Command::Service(service_command) => match service_command {
                ServiceCommand::Start(cmd) => {
                    check_maintenance_mode(ctx);
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_runes_config()?;
                    runes::start_runes_indexer(true, &abort_signal, &config, ctx).await?
                }
            },
            Command::Index(index_command) => match index_command {
                IndexCommand::Sync(cmd) => {
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_runes_config()?;
                    runes::start_runes_indexer(false, &abort_signal, &config, ctx).await?
                }
                IndexCommand::Rollback(cmd) => {
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_runes_config()?;
                    let chain_tip = runes::get_chain_tip(&config).await?;
                    confirm_rollback(&chain_tip, cmd.blocks)?;
                    runes::rollback_block_range(
                        chain_tip.index - cmd.blocks as u64,
                        chain_tip.index,
                        &config,
                        ctx,
                    )
                    .await?;
                    println!("{} blocks dropped", cmd.blocks);
                }
            },
            Command::Database(database_command) => match database_command {
                DatabaseCommand::Migrate(cmd) => {
                    let config = Config::from_file_path(&cmd.config_path)?;
                    config.assert_runes_config()?;
                    runes::db::run_migrations(&config, ctx).await;
                }
            },
        },
        Protocol::Config(subcmd) => match subcmd {
            ConfigCommand::New(cmd) => {
                use std::{fs::File, io::Write};
                let network = match (cmd.mainnet, cmd.testnet, cmd.regtest) {
                    (true, false, false) => "mainnet",
                    (false, true, false) => "testnet",
                    (false, false, true) => "regtest",
                    _ => return Err("Invalid network".into()),
                };
                let config_content = generate_toml_config(network);
                let mut file_path = PathBuf::new();
                file_path.push("Indexer.toml");
                let mut file = File::create(&file_path)
                    .map_err(|e| format!("unable to open file {}\n{}", file_path.display(), e))?;
                file.write_all(config_content.as_bytes())
                    .map_err(|e| format!("unable to write file {}\n{}", file_path.display(), e))?;
                println!("Created file Indexer.toml");
            }
        },
    }
    Ok(())
}
