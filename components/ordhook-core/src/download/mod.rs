use crate::config::Config;
use crate::utils::read_file_content_at_path;
use chainhook_sdk::types::BitcoinNetwork;
use chainhook_sdk::utils::Context;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use progressing::mapping::Bar as MappingBar;
use progressing::Baring;
use std::fs::{self, File};
use std::io::{self, Cursor};
use std::io::{Read, Write};
use std::path::PathBuf;
use tar::Archive;

pub fn default_sqlite_file_path(_network: &BitcoinNetwork) -> String {
    format!("hord.sqlite").to_lowercase()
}

pub fn default_sqlite_sha_file_path(_network: &BitcoinNetwork) -> String {
    format!("hord.sqlite.sha256").to_lowercase()
}

pub async fn download_sqlite_file(config: &Config, ctx: &Context) -> Result<(), String> {
    let destination_path = config.expected_cache_path();
    std::fs::create_dir_all(&destination_path).unwrap_or_else(|e| {
        if ctx.logger.is_some() {
            println!("{}", e.to_string());
        }
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
    if ctx.logger.is_some() {
        println!("=> {file_url}");
    }
    let res = reqwest::get(&file_url)
        .await
        .or(Err(format!("Failed to GET from '{}'", &file_url)))?;

    // Download chunks
    let (tx, rx) = flume::bounded(0);
    if res.status() == reqwest::StatusCode::OK {
        let limit = res.content_length().unwrap_or(10_000_000_000) as i64;
        let archive_tmp_file = PathBuf::from("db.tar");
        let decoder_thread = std::thread::spawn(move || {
            {
                let input = ChannelRead::new(rx);
                let mut decoder = GzDecoder::new(input);
                let mut tmp = File::create(&archive_tmp_file).unwrap();
                let mut buffer = [0; 512_000];
                loop {
                    match decoder.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => {
                            if let Err(e) = tmp.write_all(&buffer[..n]) {
                                let err = format!(
                                    "unable to update compressed archive: {}",
                                    e.to_string()
                                );
                                return Err(err);
                            }
                        }
                        Err(e) => {
                            let err =
                                format!("unable to write compressed archive: {}", e.to_string());
                            return Err(err);
                        }
                    }
                }
                let _ = tmp.flush();
            }
            let archive_file = File::open(&archive_tmp_file).unwrap();
            let mut archive = Archive::new(archive_file);
            if let Err(e) = archive.unpack(&destination_path) {
                let err = format!("unable to decompress file: {}", e.to_string());
                return Err(err);
            }
            let _ = fs::remove_file(archive_tmp_file);
            Ok(())
        });

        let mut progress_bar = MappingBar::with_range(0i64, limit);
        progress_bar.set_len(60);
        let mut stdout = std::io::stdout();
        if ctx.logger.is_some() {
            print!("{}", progress_bar);
            let _ = stdout.flush();
        }
        let mut stream = res.bytes_stream();
        let mut progress = 0;
        let mut steps = 0;
        let mut tx_err = None;
        while let Some(item) = stream.next().await {
            let chunk = item.or(Err(format!("Error while downloading file")))?;
            if chunk.is_empty() {
                continue;
            }
            progress += chunk.len() as i64;
            steps += chunk.len() as i64;
            if steps > 5_000_000 {
                steps = 0;
            }
            progress_bar.set(progress);
            if steps == 0 {
                if ctx.logger.is_some() {
                    print!("\r{}", progress_bar);
                    let _ = stdout.flush();
                }
            }
            if let Err(e) = tx.send_async(chunk.to_vec()).await {
                let err = format!("unable to download archive: {}", e.to_string());
                tx_err = Some(err);
                break;
            }
        }
        progress_bar.set(limit);
        if ctx.logger.is_some() {
            print!("\r{}", progress_bar);
            let _ = stdout.flush();
            println!();
        }
        drop(tx);

        decoder_thread.join().unwrap()?;
        if let Some(_e) = tx_err.take() {}
    }

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
    if config.should_bootstrap_through_download() {
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
                    info!(ctx.expect_logger(), "More recent hord.sqlite file detected");
                }
                cache_not_expired == false
            }
            (_, _) => match std::fs::metadata(&sqlite_file_path) {
                Ok(_) => false,
                _ => {
                    info!(
                        ctx.expect_logger(),
                        "Unable to retrieve hord.sqlite file locally"
                    );
                    true
                }
            },
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
        true
    } else {
        false
    }
}
