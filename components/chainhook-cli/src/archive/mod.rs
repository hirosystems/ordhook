use crate::config::Config;
use chainhook_types::StacksNetwork;
use clarinet_files::FileLocation;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use std::fs;
use std::io::{self, Cursor};
use std::io::{Read, Write};

pub fn default_tsv_file_path(network: &StacksNetwork) -> String {
    format!("{:?}-stacks-events.tsv", network).to_lowercase()
}

pub fn default_tsv_sha_file_path(network: &StacksNetwork) -> String {
    format!("{:?}-stacks-events.sha256", network).to_lowercase()
}

pub async fn download_tsv_file(config: &Config) -> Result<(), String> {
    let mut destination_path = config.expected_cache_path();
    std::fs::create_dir_all(&destination_path).unwrap_or_else(|e| {
        println!("{}", e.to_string());
    });

    let remote_sha_url = config.expected_remote_tsv_sha256();
    let res = reqwest::get(&remote_sha_url)
        .await
        .or(Err(format!("Failed to GET from '{}'", &remote_sha_url)))?
        .bytes()
        .await
        .or(Err(format!("Failed to GET from '{}'", &remote_sha_url)))?;

    let mut local_sha_file_path = destination_path.clone();
    local_sha_file_path.push(default_tsv_sha_file_path(&config.network.stacks_network));

    println!("1");
    let local_sha_file = FileLocation::from_path(local_sha_file_path);
    let _ = local_sha_file.write_content(&res.to_vec());

    let file_url = config.expected_remote_tsv_url();
    let res = reqwest::get(&file_url)
        .await
        .or(Err(format!("Failed to GET from '{}'", &file_url)))?;

    // Download chunks
    let (tx, rx) = flume::bounded(0);
    destination_path.push(default_tsv_file_path(&config.network.stacks_network));

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
        let mut stream = res.bytes_stream();
        while let Some(item) = stream.next().await {
            let chunk = item.or(Err(format!("Error while downloading file")))?;
            tx.send_async(chunk.to_vec())
                .await
                .map_err(|e| format!("unable to download stacks event: {}", e.to_string()))?;
        }
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
