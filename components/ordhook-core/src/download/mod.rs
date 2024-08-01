use crate::config::{Config, SnapshotConfig};
use crate::utils::read_file_content_at_path;
use crate::{try_error, try_info, try_warn};
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

/// Downloads and decompresses a remote `tar.gz` file.
pub async fn download_and_decompress_archive_file(
    file_url: String,
    file_name: &str,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    let destination_dir_path = config.expected_cache_path();
    std::fs::create_dir_all(&destination_dir_path).unwrap_or_else(|e| {
        try_error!(ctx, "{e}");
    });

    try_info!(ctx, "=> {file_url}");
    let res = reqwest::get(&file_url)
        .await
        .or(Err(format!("Failed to GET from '{}'", &file_url)))?;

    // Download chunks
    let (tx, rx) = flume::bounded(0);
    if res.status() == reqwest::StatusCode::OK {
        let limit = res.content_length().unwrap_or(10_000_000_000) as i64;
        let archive_tmp_file = PathBuf::from(format!("{file_name}.tar.gz"));
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
            if let Err(e) = archive.unpack(&destination_dir_path) {
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

/// Compares the SHA256 of a previous local archive to the latest remote archive and downloads if required.
async fn validate_or_download_archive_file(
    snapshot_url: &String,
    file_name: &str,
    config: &Config,
    ctx: &Context,
) {
    let remote_archive_url = format!("{snapshot_url}.tar.gz");
    let remote_sha_url = format!("{snapshot_url}.sha256");

    let mut local_sqlite_file_path = config.expected_cache_path();
    local_sqlite_file_path.push(format!("{file_name}.sqlite"));
    let mut local_sha_file_path = config.expected_cache_path();
    local_sha_file_path.push(format!("{file_name}.sqlite.sha256"));

    // Compare local SHA256 to remote to see if there's a new one available.
    let local_sha_file = read_file_content_at_path(&local_sha_file_path);
    let remote_sha_file = match reqwest::get(&remote_sha_url).await {
        Ok(response) => response.bytes().await,
        Err(e) => Err(e),
    };
    let should_download = match (local_sha_file, remote_sha_file) {
        (Ok(local), Ok(remote_response)) => {
            let cache_not_expired = remote_response.starts_with(&local[0..32]) == false;
            if cache_not_expired {
                try_info!(ctx, "More recent {file_name}.sqlite file detected");
            }
            cache_not_expired == false
        }
        (_, _) => match std::fs::metadata(&local_sqlite_file_path) {
            Ok(_) => false,
            _ => {
                try_info!(ctx, "Unable to retrieve {file_name}.sqlite file locally");
                true
            }
        },
    };

    if should_download {
        try_info!(ctx, "Downloading {remote_archive_url}");
        match download_and_decompress_archive_file(remote_archive_url, file_name, &config, &ctx)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                try_error!(ctx, "{e}");
                std::process::exit(1);
            }
        }
    } else {
        try_info!(
            ctx,
            "Basing ordinals evaluation on database {}",
            local_sqlite_file_path.display()
        );
    }
}

/// Downloads remote SQLite archive datasets.
pub async fn download_archive_datasets_if_required(config: &Config, ctx: &Context) {
    if !config.should_bootstrap_through_download() {
        return;
    }
    let snapshot_urls = match &config.snapshot {
        SnapshotConfig::Build => unreachable!(),
        SnapshotConfig::Download(url) => url,
    };
    validate_or_download_archive_file(&snapshot_urls.ordinals, "hord", config, ctx).await;
    if config.meta_protocols.brc20 {
        match &snapshot_urls.brc20 {
            Some(url) => validate_or_download_archive_file(url, "brc20", config, ctx).await,
            None => {
                try_warn!(ctx, "No brc20 snapshot url configured");
            }
        }
    }
}
