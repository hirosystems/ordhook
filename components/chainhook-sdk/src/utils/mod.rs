use std::{fs::OpenOptions, io::Write};

use chainhook_types::{
    BitcoinBlockData, BlockHeader, BlockIdentifier, StacksBlockData, StacksMicroblockData,
    StacksTransactionData,
};
use hiro_system_kit::slog::{self, Logger};
use reqwest::RequestBuilder;
use serde_json::Value as JsonValue;

#[derive(Clone)]
pub struct Context {
    pub logger: Option<Logger>,
    pub tracer: bool,
}

impl Context {
    pub fn empty() -> Context {
        Context {
            logger: None,
            tracer: false,
        }
    }

    pub fn try_log<F>(&self, closure: F)
    where
        F: FnOnce(&Logger),
    {
        if let Some(ref logger) = self.logger {
            closure(logger)
        }
    }

    pub fn expect_logger(&self) -> &Logger {
        self.logger.as_ref().unwrap()
    }
}

pub trait AbstractStacksBlock {
    fn get_identifier(&self) -> &BlockIdentifier;
    fn get_parent_identifier(&self) -> &BlockIdentifier;
    fn get_transactions(&self) -> &Vec<StacksTransactionData>;
    fn get_timestamp(&self) -> i64;
    fn get_serialized_metadata(&self) -> JsonValue;
}

impl AbstractStacksBlock for StacksBlockData {
    fn get_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }

    fn get_parent_identifier(&self) -> &BlockIdentifier {
        &self.parent_block_identifier
    }

    fn get_transactions(&self) -> &Vec<StacksTransactionData> {
        &self.transactions
    }

    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    fn get_serialized_metadata(&self) -> JsonValue {
        json!(self.metadata)
    }
}

impl AbstractStacksBlock for StacksMicroblockData {
    fn get_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }

    fn get_parent_identifier(&self) -> &BlockIdentifier {
        &self.parent_block_identifier
    }

    fn get_transactions(&self) -> &Vec<StacksTransactionData> {
        &self.transactions
    }

    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    fn get_serialized_metadata(&self) -> JsonValue {
        json!(self.metadata)
    }
}

pub trait AbstractBlock {
    fn get_identifier(&self) -> &BlockIdentifier;
    fn get_parent_identifier(&self) -> &BlockIdentifier;
    fn get_header(&self) -> BlockHeader {
        BlockHeader {
            block_identifier: self.get_identifier().clone(),
            parent_block_identifier: self.get_parent_identifier().clone(),
        }
    }
}

impl AbstractBlock for BlockHeader {
    fn get_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }

    fn get_parent_identifier(&self) -> &BlockIdentifier {
        &self.parent_block_identifier
    }
}

impl AbstractBlock for StacksBlockData {
    fn get_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }

    fn get_parent_identifier(&self) -> &BlockIdentifier {
        &self.parent_block_identifier
    }
}

impl AbstractBlock for StacksMicroblockData {
    fn get_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }

    fn get_parent_identifier(&self) -> &BlockIdentifier {
        &self.parent_block_identifier
    }
}

impl AbstractBlock for BitcoinBlockData {
    fn get_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }

    fn get_parent_identifier(&self) -> &BlockIdentifier {
        &self.parent_block_identifier
    }
}

pub async fn send_request(
    request_builder: RequestBuilder,
    attempts_max: u16,
    attempts_interval_sec: u16,
    ctx: &Context,
) -> Result<(), ()> {
    let mut retry = 0;
    loop {
        let request_builder = match request_builder.try_clone() {
            Some(rb) => rb,
            None => {
                ctx.try_log(|logger| slog::warn!(logger, "unable to clone request builder"));
                return Err(());
            }
        };
        match request_builder.send().await {
            Ok(res) => {
                if res.status().is_success() {
                    ctx.try_log(|logger| slog::info!(logger, "Trigger {} successful", res.url()));
                    return Ok(());
                } else {
                    retry += 1;
                    ctx.try_log(|logger| {
                        slog::warn!(
                            logger,
                            "Trigger {} failed with status {}",
                            res.url(),
                            res.status()
                        )
                    });
                }
            }
            Err(e) => {
                retry += 1;
                ctx.try_log(|logger| {
                    slog::warn!(logger, "unable to send request {}", e.to_string())
                });
            }
        }
        if retry >= attempts_max {
            ctx.try_log(|logger| {
                slog::error!(logger, "unable to send request after several retries")
            });
            return Err(());
        }
        std::thread::sleep(std::time::Duration::from_secs(attempts_interval_sec.into()));
    }
}

pub fn file_append(path: String, bytes: Vec<u8>, ctx: &Context) -> Result<(), ()> {
    let mut file_path = match std::env::current_dir() {
        Err(e) => {
            ctx.try_log(|logger| {
                slog::warn!(logger, "unable to retrieve current_dir {}", e.to_string())
            });
            return Err(());
        }
        Ok(p) => p,
    };
    file_path.push(path);
    if !file_path.exists() {
        match std::fs::File::create(&file_path) {
            Ok(ref mut file) => {
                let _ = file.write_all(&bytes);
            }
            Err(e) => {
                ctx.try_log(|logger| {
                    slog::warn!(
                        logger,
                        "unable to create file {}: {}",
                        file_path.display(),
                        e.to_string()
                    )
                });
                return Err(());
            }
        }
    }

    let mut file = match OpenOptions::new()
        .create(false)
        .write(true)
        .append(true)
        .open(file_path)
    {
        Err(e) => {
            ctx.try_log(|logger| slog::warn!(logger, "unable to open file {}", e.to_string()));
            return Err(());
        }
        Ok(p) => p,
    };

    let utf8 = match String::from_utf8(bytes) {
        Ok(string) => string,
        Err(e) => {
            ctx.try_log(|logger| {
                slog::warn!(
                    logger,
                    "unable serialize bytes as utf8 string {}",
                    e.to_string()
                )
            });
            return Err(());
        }
    };

    if let Err(e) = writeln!(file, "{}", utf8) {
        ctx.try_log(|logger| slog::warn!(logger, "unable to open file {}", e.to_string()));
        eprintln!("Couldn't write to file: {}", e);
        return Err(());
    }

    Ok(())
}
