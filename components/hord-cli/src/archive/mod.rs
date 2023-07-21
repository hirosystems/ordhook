use crate::config::Config;
use crate::utils::read_file_content_at_path;
use chainhook_sdk::types::BitcoinNetwork;
use chainhook_sdk::utils::Context;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use std::fs;
use std::io::{self, Cursor};
use std::io::{Read, Write};
use progressing::Baring;
use progressing::mapping::Bar as MappingBar;

pub fn default_sqlite_file_path(_network: &BitcoinNetwork) -> String {
    format!("hord.sqlite").to_lowercase()
}

pub fn default_sqlite_sha_file_path(_network: &BitcoinNetwork) -> String {
    format!("hord.sqlite.sha256").to_lowercase()
}

pub async fn download_sqlite_file(config: &Config, _ctx: &Context) -> Result<(), String> {
    let mut destination_path = config.expected_cache_path();
    std::fs::create_dir_all(&destination_path).unwrap_or_else(|e| {
        println!("{}", e.to_string());
    });

    // let remote_sha_url = config.expected_remote_ordinals_sqlite_sha256();
    // let res = reqwest::get(&remote_sha_url)
    //     .await
    //     .or(Err(format!("Failed to GET from '{}'", &remote_sha_url)))?
    //     .bytes()
    //     .await
    //     .or(Err(format!("Failed to GET from '{}'", &remote_sha_url)))?;

    // let mut local_sha_file_path = destination_path.clone();
    // local_sha_file_path.push(default_sqlite_sha_file_path(
    //     &config.network.bitcoin_network,
    // ));
    // write_file_content_at_path(&local_sha_file_path, &res.to_vec())?;

    let file_url = config.expected_remote_ordinals_sqlite_url();
    println!("=> {file_url}");
    let res = reqwest::get(&file_url)
        .await
        .or(Err(format!("Failed to GET from '{}'", &file_url)))?;

    // Download chunks
    let (tx, rx) = flume::bounded(0);
    destination_path.push(default_sqlite_file_path(&config.network.bitcoin_network));

    let decoder_thread = std::thread::spawn(move || {
        let input = ChannelRead::new(rx);
        let mut decoder = GzDecoder::new(input);
        let mut content = Vec::new();
        let _ = decoder.read_to_end(&mut content);
        let mut file = fs::File::create(&destination_path).unwrap();
        if let Err(e) = file.write_all(&content[..]) {
            println!("unable to write file: {}", e.to_string());
            std::process::exit(1);
        }
    });

    if res.status() == reqwest::StatusCode::OK {
        let mut progress_bar = MappingBar::with_range(0i64, 5_400_000_000);
        progress_bar.set_len(60);
        let mut stdout = std::io::stdout();
        print!(
            "{}", progress_bar
        );
        let _ = stdout.flush();
        let mut stream = res.bytes_stream();
        let mut progress = 0;
        while let Some(item) = stream.next().await {
            let chunk = item.or(Err(format!("Error while downloading file")))?;
            progress += chunk.len() as i64;
            progress_bar.set(progress);
            if progress_bar.has_progressed_significantly() {
                progress_bar.remember_significant_progress();
                print!(
                    "\r{}", progress_bar
                );
                let _ = stdout.flush();
            }
            tx.send_async(chunk.to_vec())
                .await
                .map_err(|e| format!("unable to download stacks event: {}", e.to_string()))?;
        }
        print!(
            "\r"
        );
        let _ = stdout.flush();
        println!();
        drop(tx);
    }

    tokio::task::spawn_blocking(|| decoder_thread.join())
        .await
        .unwrap()
        .unwrap();

    Ok(())
}

// Wrap a channel into something that impls `io::Read`
struct ChannelRead {
    rx: flume::Receiver<Vec<u8>>,
    current: Cursor<Vec<u8>>,
}

impl ChannelRead {
    fn new(rx: flume::Receiver<Vec<u8>>) -> ChannelRead {
        ChannelRead {
            rx,
            current: Cursor::new(vec![]),
        }
    }
}

impl Read for ChannelRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.current.position() == self.current.get_ref().len() as u64 {
            // We've exhausted the previous chunk, get a new one.
            if let Ok(vec) = self.rx.recv() {
                self.current = io::Cursor::new(vec);
            }
            // If recv() "fails", it means the sender closed its part of
            // the channel, which means EOF. Propagate EOF by allowing
            // a read from the exhausted cursor.
        }
        self.current.read(buf)
    }
}

pub async fn download_ordinals_dataset_if_required(config: &Config, ctx: &Context) -> bool {
    if config.is_initial_ingestion_required() {
        // Download default tsv.
        if config.rely_on_remote_ordinals_sqlite()
            && config.should_download_remote_ordinals_sqlite()
        {
            let url = config.expected_remote_ordinals_sqlite_url();
            let mut sqlite_file_path = config.expected_cache_path();
            sqlite_file_path.push(default_sqlite_file_path(&config.network.bitcoin_network));
            let mut sqlite_sha_file_path = config.expected_cache_path();
            sqlite_sha_file_path.push(default_sqlite_sha_file_path(
                &config.network.bitcoin_network,
            ));

            // Download archive if not already present in cache
            // Load the local
            let local_sha_file = read_file_content_at_path(&sqlite_sha_file_path);
            let sha_url = config.expected_remote_ordinals_sqlite_sha256();

            let remote_sha_file = match reqwest::get(&sha_url).await {
                Ok(response) => response.bytes().await,
                Err(e) => Err(e),
            };
            let should_download = match (local_sha_file, remote_sha_file) {
                (Ok(local), Ok(remote_response)) => {
                    let cache_not_expired = remote_response.starts_with(&local[0..32]) == false;
                    if cache_not_expired {
                        info!(
                            ctx.expect_logger(),
                            "More recent hord.sqlite file detected"
                        );
                    }
                    cache_not_expired == false
                }
                (_, _) => {
                    match std::fs::metadata(&sqlite_file_path) {
                        Ok(_) => false,
                        _ => {
                            info!(
                                ctx.expect_logger(),
                                "Unable to retrieve hord.sqlite file locally"
                            );
                            true        
                        }
                    }
                }
            };
            if should_download {
                info!(ctx.expect_logger(), "Downloading {}", url);
                match download_sqlite_file(&config, &ctx).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(ctx.expect_logger(), "{}", e);
                        std::process::exit(1);
                    }
                }
            } else {
                info!(
                    ctx.expect_logger(),
                    "Basing ordinals evaluation on database {}",
                    sqlite_file_path.display()
                );
            }
            // config.add_local_ordinals_sqlite_source(&sqlite_file_path);
        }
        true
    } else {
        info!(
            ctx.expect_logger(),
            "Streaming blocks from bitcoind {}", config.network.bitcoind_rpc_url
        );
        false
    }
}
