mod blocktime;
mod chain;
mod deserialize_from_str;
mod epoch;
mod height;
pub mod indexing;
pub mod inscription;
mod inscription_id;
mod sat;
mod sat_point;

use std::time::Duration;

type Result<T = (), E = anyhow::Error> = std::result::Result<T, E>;

use crate::observer::EventObserverConfig;

const DIFFCHANGE_INTERVAL: u64 =
    bitcoincore_rpc::bitcoin::blockdata::constants::DIFFCHANGE_INTERVAL as u64;
const SUBSIDY_HALVING_INTERVAL: u64 =
    bitcoincore_rpc::bitcoin::blockdata::constants::SUBSIDY_HALVING_INTERVAL as u64;
const CYCLE_EPOCHS: u64 = 6;

pub fn initialize_ordinal_index(
    config: &EventObserverConfig,
) -> Result<self::indexing::OrdinalIndex, String> {
    let index_options = self::indexing::Options {
        rpc_username: config.bitcoin_node_username.clone(),
        rpc_password: config.bitcoin_node_password.clone(),
        data_dir: config.cache_path.clone().into(),
        chain: chain::Chain::Mainnet,
        first_inscription_height: None,
        height_limit: None,
        index: None,
        rpc_url: config.bitcoin_node_rpc_url.clone(),
    };
    let index = match self::indexing::OrdinalIndex::open(&index_options) {
        Ok(index) => index,
        Err(e) => {
            println!("unable to open ordinal index: {}", e.to_string());
            panic!()
        }
    };
    Ok(index)
}

// 1) Retrieve the block height of the oldest block (coinbase), which will indicates the range
// 2) Look at the following transaction N:
//      - Compute SUM of the inputs located before the Coinbase Spend, and remove , remove the outputs (N+1) in the list of
//

// -> 10-20
//
// 10-20 -> 10-15
//       -> 15-18
//
//          10-15 -> 10-12
//                -> 12-14
//
//                   10-12 -> X
// -> 10-20
//
// 10-20 -> 10-15
//       -> 15-18
//
//          15-18 -> 15-17
//                -> 17-18
//
//                   17-18 -> X
//

// Open a transaction:
// Locate output, based on amounts transfered

// pub async fn retrieve_satoshi_point(
//     config: &Config,
//     origin_txid: &str,
//     output_index: usize,
// ) -> Result<(), String> {
//     let http_client = HttpClient::builder()
//         .timeout(Duration::from_secs(20))
//         .build()
//         .expect("Unable to build http client");

//     let mut transactions_chain = VecDeque::new();
//     let mut tx_cursor = (origin_txid.to_string(), 0);
//     let mut offset = 0;

//     loop {
//         println!("{:?}", tx_cursor);

//         // Craft RPC request
//         let body = json!({
//             "jsonrpc": "1.0",
//             "id": "chainhook-cli",
//             "method": "getrawtransaction",
//             "params": vec![json!(tx_cursor.0), json!(true)]
//         });

//         // Send RPC request
//         let transaction = http_client
//             .post(&config.network.bitcoin_node_rpc_url)
//             .basic_auth(
//                 &config.network.bitcoin_node_rpc_username,
//                 Some(&config.network.bitcoin_node_rpc_password),
//             )
//             .header("Content-Type", "application/json")
//             .header("Host", &config.network.bitcoin_node_rpc_url[7..])
//             .json(&body)
//             .send()
//             .await
//             .map_err(|e| format!("unable to send request ({})", e))?
//             .json::<bitcoincore_rpc::jsonrpc::Response>()
//             .await
//             .map_err(|e| format!("unable to parse response ({})", e))?
//             .result::<GetRawTransactionResult>()
//             .map_err(|e| format!("unable to parse response ({})", e))?;

//         if transaction.is_coinbase() {
//             transactions_chain.push_front(transaction);
//             break;
//         }

//         // Identify the TXIN that we should select, just take the 1st one for now
//         let mut sats_out = 0;
//         for (index, output) in transaction.vout.iter().enumerate() {
//             // should reorder output.n?
//             assert_eq!(index as u32, output.n);
//             if index == tx_cursor.1 {
//                 break;
//             }
//             sats_out += output.value.to_sat();
//         }

//         let mut sats_in = 0;
//         for input in transaction.vin.iter() {
//             let txid = input.txid.unwrap().to_string();
//             let vout = input.vout.unwrap() as usize;
//             let body = json!({
//                 "jsonrpc": "1.0",
//                 "id": "chainhook-cli",
//                 "method": "getrawtransaction",
//                 "params": vec![json!(txid), json!(true)]
//                 // "method": "gettxout",
//                 // "params": vec![json!(&txid), json!(&(vout + 1)), json!(false)]
//             });

//             let raw_txin = http_client
//                 .post(&config.network.bitcoin_node_rpc_url)
//                 .basic_auth(
//                     &config.network.bitcoin_node_rpc_username,
//                     Some(&config.network.bitcoin_node_rpc_password),
//                 )
//                 .header("Content-Type", "application/json")
//                 .header("Host", &config.network.bitcoin_node_rpc_url[7..])
//                 .json(&body)
//                 .send()
//                 .await
//                 .map_err(|e| format!("unable to send request ({})", e))?
//                 .json::<bitcoincore_rpc::jsonrpc::Response>()
//                 .await
//                 .map_err(|e| format!("unable to parse response ({})", e))?
//                 // .result::<GetTxOutResult>()
//                 // .map_err(|e| format!("unable to parse response ({})", e))?;
//                 .result::<GetRawTransactionResult>()
//                 .map_err(|e| format!("unable to parse response ({})", e))?;

//             // println!("{:?}", txout);

//             sats_in += raw_txin.vout[vout].value.to_sat();
//             if sats_in >= sats_out {
//                 tx_cursor = (txid, vout as usize);
//                 break;
//             }
//         }
//         transactions_chain.push_front(transaction);
//     }

//     Ok(())
// }
