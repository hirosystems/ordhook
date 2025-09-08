use std::sync::{Mutex, OnceLock};

use bitcoin::Network;
use config::{BitcoindConfig, RedisConfig};
use redis::{aio::MultiplexedConnection, cluster::ClusterClient, Client, IntoConnectionInfo};
use serde::Serialize;

use crate::types::BlockIdentifier;

#[derive(Serialize)]
struct IndexProgressPayload<'a> {
    chain: &'a str,
    network: &'a str,
    indexer: &'a str,
    apply_blocks: Vec<BlockIndexRef>,
    rollback_blocks: Vec<BlockIndexRef>,
}

#[derive(Serialize)]
struct Message<'a> {
    id: String,
    payload: IndexProgressPayload<'a>,
}

#[derive(Serialize)]
struct BlockIndexRef {
    hash: String,
    index: u64,
}

pub struct RedisNotifier {
    pub queue: String,
    client: RedisClientKind,
    sentinel_cfg: Option<(Vec<String>, String)>,
    retry_attempts: u32,
    retry_backoff_ms: u64,
    database: Option<u8>,
}

enum RedisClientKind {
    Single(Client),
    Cluster(ClusterClient),
}

// No separate connection enum; we connect per attempt in `attempt_push`

impl RedisNotifier {
    pub fn new(redis: &RedisConfig) -> Result<Self, String> {
        let client = build_redis_client(redis)?;
        let sentinel_cfg = match (&redis.sentinel_nodes, &redis.sentinel_master_name) {
            (Some(nodes), Some(master)) if !nodes.is_empty() => {
                Some((nodes.clone(), master.clone()))
            }
            _ => None,
        };
        Ok(Self {
            queue: redis.queue.clone(),
            client,
            sentinel_cfg,
            retry_attempts: redis.retry_attempts.unwrap_or(3),
            retry_backoff_ms: redis.retry_backoff_ms.unwrap_or(200),
            database: redis.database,
        })
    }

    fn network_str(bitcoind: &BitcoindConfig) -> &'static str {
        match bitcoind.network {
            Network::Bitcoin => "mainnet",
            Network::Testnet => "testnet",
            Network::Regtest => todo!(),
            Network::Signet => todo!(),
            _ => todo!(),
        }
    }

    pub async fn notify(
        &self,
        indexer: &str,
        bitcoind: &BitcoindConfig,
        apply_blocks: &[BlockIdentifier],
        rollback_blocks: &[BlockIdentifier],
    ) -> Result<(), String> {
        let payload = build_message_json(indexer, bitcoind, apply_blocks, rollback_blocks)?;
        // In-test capture path (no real Redis required)
        if std::env::var("REDIS_TEST_CAPTURE").ok().as_deref() == Some("1") {
            capture_messages().lock().unwrap().push(payload);
            return Ok(());
        }

        let mut attempts = 0;
        loop {
            if let Err(e) = self.attempt_push(&payload).await {
                attempts += 1;
                if attempts >= self.retry_attempts {
                    return Err(format!(
                        "unable to rpush redis message after {attempts} attempts: {e}"
                    ));
                }
                // If configured for Sentinel, attempt a fresh master resolution and push immediately
                if let Some((nodes, master)) = &self.sentinel_cfg {
                    if let Some(master_url) =
                        resolve_sentinel_master(nodes, master, self.database).await
                    {
                        if let Ok(client) = Client::open(master_url) {
                            // Create a scoped connection that will be automatically dropped
                            let push_result = {
                                let mut single_conn = client
                                    .get_multiplexed_tokio_connection()
                                    .await
                                    .map_err(|e| format!("unable to get sentinel connection: {e}"));
                                match single_conn {
                                    Ok(ref mut conn) => redis::cmd("RPUSH")
                                        .arg(&self.queue)
                                        .arg(&payload)
                                        .query_async(conn)
                                        .await
                                        .map(|_: ()| ()),
                                    Err(e) => Err(redis::RedisError::from((
                                        redis::ErrorKind::IoError,
                                        "connection error",
                                        e,
                                    ))),
                                }
                            }; // Connection is dropped here
                            if push_result.is_ok() {
                                return Ok(());
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(
                    self.retry_backoff_ms * attempts as u64,
                ))
                .await;
                continue;
            }
            return Ok(());
        }
    }

    async fn attempt_push(&self, payload: &str) -> Result<(), String> {
        // Use scoped connections to ensure they're properly dropped after use
        match &self.client {
            RedisClientKind::Single(client) => {
                // Connection is scoped and dropped at the end of this block
                let mut conn: MultiplexedConnection = client
                    .get_multiplexed_tokio_connection()
                    .await
                    .map_err(|e| format!("unable to get redis connection: {e}"))?;
                redis::cmd("RPUSH")
                    .arg(&self.queue)
                    .arg(payload)
                    .query_async(&mut conn)
                    .await
                    .map(|_: ()| ())
                    .map_err(|e| format!("unable to rpush redis message: {e}"))
            }
            RedisClientKind::Cluster(client) => {
                // Connection is scoped and dropped at the end of this block
                let mut conn = client
                    .get_async_connection()
                    .await
                    .map_err(|e| format!("unable to get redis cluster connection: {e}"))?;
                redis::cmd("RPUSH")
                    .arg(&self.queue)
                    .arg(payload)
                    .query_async(&mut conn)
                    .await
                    .map(|_: ()| ())
                    .map_err(|e| format!("unable to rpush redis message: {e}"))
            }
        }
    }
}

fn build_redis_client(redis: &RedisConfig) -> Result<RedisClientKind, String> {
    // Prefer cluster when cluster_nodes set
    if let Some(nodes) = &redis.cluster_nodes {
        if !nodes.is_empty() {
            let client = ClusterClient::new(nodes.clone())
                .map_err(|e| format!("unable to create redis cluster client: {e}"))?;
            return Ok(RedisClientKind::Cluster(client));
        }
    }
    // If sentinel provided, resolve master and connect as Single
    if let (Some(sentinels), Some(master)) = (&redis.sentinel_nodes, &redis.sentinel_master_name) {
        if !sentinels.is_empty() {
            if let Some(master_url) = futures::executor::block_on(resolve_sentinel_master(
                sentinels,
                master,
                redis.database,
            )) {
                let client = Client::open(master_url)
                    .map_err(|e| format!("unable to connect to sentinel-resolved master: {e}"))?;
                return Ok(RedisClientKind::Single(client));
            } else {
                return Err(format!("unable to resolve sentinel master '{}'", master));
            }
        }
    }
    // Fallbacks via comma-separated URL(s)
    let endpoints: Vec<&str> = redis
        .url
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let endpoints = if endpoints.is_empty() {
        vec![redis.url.as_str()]
    } else {
        endpoints
    };
    for ep in endpoints {
        if let Ok(info) = ep.into_connection_info() {
            if let Ok(client) = Client::open(info) {
                return Ok(RedisClientKind::Single(client));
            }
        }
    }
    Err("unable to connect to any configured redis endpoint".to_string())
}

async fn resolve_sentinel_master(
    sentinels: &Vec<String>,
    master: &str,
    database: Option<u8>,
) -> Option<String> {
    for s in sentinels {
        if let Ok(client) = Client::open(s.as_str()) {
            if let Ok(mut conn) = client.get_multiplexed_tokio_connection().await {
                let res: redis::RedisResult<Vec<String>> = redis::cmd("SENTINEL")
                    .arg("get-master-addr-by-name")
                    .arg(master)
                    .query_async(&mut conn)
                    .await;
                if let Ok(v) = res {
                    if v.len() == 2 {
                        let host = &v[0];
                        let port = &v[1];
                        let db = database.unwrap_or(0);
                        return Some(format!("redis://{}:{}/{}", host, port, db));
                    }
                }
            }
        }
    }
    None
}

pub fn ensure_0x<S: AsRef<str>>(s: S) -> String {
    let v = s.as_ref();
    if v.starts_with("0x") || v.starts_with("0X") {
        v.to_string()
    } else {
        format!("0x{}", v)
    }
}

static CAPTURED_MESSAGES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn capture_messages() -> &'static Mutex<Vec<String>> {
    CAPTURED_MESSAGES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn take_captured_messages() -> Vec<String> {
    let mut guard = capture_messages().lock().unwrap();
    let messages = guard.clone();
    guard.clear();
    messages
}

fn js_sys_time_ms() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

pub fn build_message_json(
    indexer: &str,
    bitcoind: &BitcoindConfig,
    apply_blocks: &[BlockIdentifier],
    rollback_blocks: &[BlockIdentifier],
) -> Result<String, String> {
    let network = RedisNotifier::network_str(bitcoind);
    let id = format!(
        "bitcoin-{}-{}-{}",
        indexer,
        apply_blocks
            .last()
            .map(|b| b.index.to_string())
            .unwrap_or_else(|| "na".to_string()),
        js_sys_time_ms()
    );
    let message = Message {
        id,
        payload: IndexProgressPayload {
            chain: "bitcoin",
            network,
            indexer,
            apply_blocks: apply_blocks
                .iter()
                .map(|b| BlockIndexRef {
                    hash: ensure_0x(&b.hash),
                    index: b.index,
                })
                .collect(),
            rollback_blocks: rollback_blocks
                .iter()
                .map(|b| BlockIndexRef {
                    hash: ensure_0x(&b.hash),
                    index: b.index,
                })
                .collect(),
        },
    };
    serde_json::to_string(&message).map_err(|e| format!("unable to serialize redis message: {e}"))
}

#[cfg(test)]
mod tests {
    use bitcoin::Network;
    use config::{BitcoindConfig, RedisConfig};
    use tokio::runtime::Runtime;

    use super::{build_message_json, ensure_0x, take_captured_messages, RedisNotifier};
    use crate::types::BlockIdentifier;

    fn sample_bitcoind(network: Network) -> BitcoindConfig {
        BitcoindConfig {
            network,
            rpc_url: "http://localhost:18443".into(),
            rpc_username: "user".into(),
            rpc_password: "pass".into(),
            zmq_url: "tcp://127.0.0.1:28332".into(),
        }
    }

    #[test]
    fn test_build_message_json_for_runes() {
        let bitcoind = sample_bitcoind(Network::Testnet);
        let apply = vec![
            BlockIdentifier {
                index: 100,
                hash: ensure_0x("abc"),
            },
            BlockIdentifier {
                index: 101,
                hash: ensure_0x("def"),
            },
        ];
        let rollback = vec![BlockIdentifier {
            index: 99,
            hash: ensure_0x("999"),
        }];
        let json = build_message_json("runes", &bitcoind, &apply, &rollback).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v["id"].as_str().unwrap().starts_with("bitcoin-runes-"));
        assert_eq!(v["payload"]["chain"], "bitcoin");
        assert_eq!(v["payload"]["indexer"], "runes");
        assert_eq!(v["payload"]["network"], "testnet");
        let apply_blocks = v["payload"]["apply_blocks"].as_array().unwrap();
        assert_eq!(apply_blocks.len(), 2);
        assert_eq!(apply_blocks[0]["hash"], "0xabc");
        assert_eq!(apply_blocks[0]["index"], 100);
        assert_eq!(apply_blocks[1]["hash"], "0xdef");
        assert_eq!(apply_blocks[1]["index"], 101);
        let rollback_blocks = v["payload"]["rollback_blocks"].as_array().unwrap();
        assert_eq!(rollback_blocks.len(), 1);
        assert_eq!(rollback_blocks[0]["hash"], "0x999");
        assert_eq!(rollback_blocks[0]["index"], 99);
    }

    #[test]
    fn test_build_message_json_for_ordinals() {
        let bitcoind = sample_bitcoind(Network::Testnet);
        let apply = vec![BlockIdentifier {
            index: 800_000,
            hash: ensure_0x("feed"),
        }];
        let rollback: Vec<BlockIdentifier> = vec![];
        let json = build_message_json("ordinals", &bitcoind, &apply, &rollback).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v["id"].as_str().unwrap().starts_with("bitcoin-ordinals-"));
        assert_eq!(v["payload"]["chain"], "bitcoin");
        assert_eq!(v["payload"]["indexer"], "ordinals");
        assert_eq!(v["payload"]["network"], "testnet");
        let apply_blocks = v["payload"]["apply_blocks"].as_array().unwrap();
        assert_eq!(apply_blocks.len(), 1);
        assert_eq!(apply_blocks[0]["hash"], "0xfeed");
        assert_eq!(apply_blocks[0]["index"], 800_000);
        let rollback_blocks = v["payload"]["rollback_blocks"].as_array().unwrap();
        assert!(rollback_blocks.is_empty());
    }

    #[test]
    fn test_build_message_json_for_runes_reorg() {
        let bitcoind = sample_bitcoind(Network::Testnet);
        let apply = vec![
            BlockIdentifier {
                index: 2,
                hash: ensure_0x("1235aa"),
            },
            BlockIdentifier {
                index: 3,
                hash: ensure_0x("1236"),
            },
        ];
        let rollback = vec![BlockIdentifier {
            index: 2,
            hash: ensure_0x("1235"),
        }];
        let json = build_message_json("runes", &bitcoind, &apply, &rollback).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v["id"].as_str().unwrap().starts_with("bitcoin-runes-"));
        assert_eq!(v["payload"]["chain"], "bitcoin");
        assert_eq!(v["payload"]["indexer"], "runes");
        assert_eq!(v["payload"]["network"], "testnet");
        let apply_blocks = v["payload"]["apply_blocks"].as_array().unwrap();
        assert_eq!(apply_blocks.len(), 2);
        assert_eq!(apply_blocks[0]["hash"], "0x1235aa");
        assert_eq!(apply_blocks[0]["index"], 2);
        assert_eq!(apply_blocks[1]["hash"], "0x1236");
        assert_eq!(apply_blocks[1]["index"], 3);
        let rollback_blocks = v["payload"]["rollback_blocks"].as_array().unwrap();
        assert_eq!(rollback_blocks.len(), 1);
        assert_eq!(rollback_blocks[0]["hash"], "0x1235");
        assert_eq!(rollback_blocks[0]["index"], 2);
    }

    #[test]
    fn test_build_message_json_for_ordinals_reorg() {
        let bitcoind = sample_bitcoind(Network::Testnet);
        let apply = vec![
            BlockIdentifier {
                index: 820_001,
                hash: ensure_0x("abc1"),
            },
            BlockIdentifier {
                index: 820_002,
                hash: ensure_0x("abc2"),
            },
        ];
        let rollback = vec![BlockIdentifier {
            index: 820_001,
            hash: ensure_0x("old1"),
        }];
        let json = build_message_json("ordinals", &bitcoind, &apply, &rollback).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v["id"].as_str().unwrap().starts_with("bitcoin-ordinals-"));
        assert_eq!(v["payload"]["chain"], "bitcoin");
        assert_eq!(v["payload"]["indexer"], "ordinals");
        assert_eq!(v["payload"]["network"], "testnet");
        let apply_blocks = v["payload"]["apply_blocks"].as_array().unwrap();
        assert_eq!(apply_blocks.len(), 2);
        assert_eq!(apply_blocks[0]["hash"], "0xabc1");
        assert_eq!(apply_blocks[0]["index"], 820_001);
        assert_eq!(apply_blocks[1]["hash"], "0xabc2");
        assert_eq!(apply_blocks[1]["index"], 820_002);
        let rollback_blocks = v["payload"]["rollback_blocks"].as_array().unwrap();
        assert_eq!(rollback_blocks.len(), 1);
        assert_eq!(rollback_blocks[0]["hash"], "0xold1");
        assert_eq!(rollback_blocks[0]["index"], 820_001);
    }

    #[test]
    fn test_notifier_pushes_captured_message() {
        // Enable capture via env var and run notify; ensure a payload is captured
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            std::env::set_var("REDIS_TEST_CAPTURE", "1");
            let notifier = RedisNotifier::new(&RedisConfig {
                enabled: true,
                url: "redis://127.0.0.1:6379/0".into(),
                queue: "test-queue".into(),
                database: None,
                cluster_nodes: None,
                sentinel_nodes: None,
                sentinel_master_name: None,
                username: None,
                password: None,
                retry_attempts: None,
                retry_backoff_ms: None,
                connection_timeout_ms: None,
                command_timeout_ms: None,
            })
            .unwrap();
            let bitcoind = sample_bitcoind(Network::Testnet);
            let apply = vec![BlockIdentifier {
                index: 1,
                hash: ensure_0x("1234"),
            }];
            let rollback: Vec<BlockIdentifier> = vec![];
            notifier
                .notify("runes", &bitcoind, &apply, &rollback)
                .await
                .unwrap();
            std::env::remove_var("REDIS_TEST_CAPTURE");
        });
        let captured = take_captured_messages();
        assert_eq!(captured.len(), 1);
        let v: serde_json::Value = serde_json::from_str(&captured[0]).unwrap();
        assert_eq!(v["payload"]["chain"], "bitcoin");
        assert_eq!(v["payload"]["indexer"], "runes");
        assert_eq!(v["payload"]["network"], "testnet");
        let apply_blocks = v["payload"]["apply_blocks"].as_array().unwrap();
        assert_eq!(apply_blocks.len(), 1);
        assert_eq!(apply_blocks[0]["hash"], "0x1234");
        assert_eq!(apply_blocks[0]["index"], 1);
    }

    #[test]
    #[ignore = "requires local Redis at redis://127.0.0.1:6379/0"]
    fn notify_pushes_to_redis() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let queue = "it_test_q";
            let notifier = RedisNotifier::new(&RedisConfig {
                enabled: true,
                url: "redis://127.0.0.1:6379/0".into(),
                queue: queue.into(),
                database: None,
                cluster_nodes: None,
                sentinel_nodes: None,
                sentinel_master_name: None,
                username: None,
                password: None,
                retry_attempts: None,
                retry_backoff_ms: None,
                connection_timeout_ms: None,
                command_timeout_ms: None,
            })
            .unwrap();

            let bitcoind = sample_bitcoind(Network::Testnet);
            let apply = vec![BlockIdentifier {
                index: 2,
                hash: "0xabc".into(),
            }];
            notifier
                .notify("ordinals", &bitcoind, &apply, &[])
                .await
                .unwrap();

            let client = redis::Client::open("redis://127.0.0.1:6379/0").unwrap();
            let mut conn = client.get_multiplexed_tokio_connection().await.unwrap();
            let msg: String = redis::cmd("RPOP")
                .arg(queue)
                .query_async(&mut conn)
                .await
                .unwrap();
            let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
            assert_eq!(v["payload"]["indexer"], "ordinals");
            assert_eq!(v["payload"]["apply_blocks"][0]["hash"], "0xabc");
        });
    }
}
