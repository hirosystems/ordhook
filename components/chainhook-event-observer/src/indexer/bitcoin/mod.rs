mod blocks_pool;

use std::time::Duration;

use crate::chainhooks::types::{
    get_canonical_pox_config, get_stacks_canonical_magic_bytes, PoxConfig, StacksOpcodes,
};
use crate::indexer::IndexerConfig;
use crate::observer::BitcoinConfig;
use crate::utils::Context;
use bitcoincore_rpc::bitcoin::{self, Script};
use bitcoincore_rpc_json::{GetRawTransactionResult, GetRawTransactionResultVout};
pub use blocks_pool::BitcoinBlockPool;
use chainhook_types::bitcoin::{OutPoint, TxIn, TxOut};
use chainhook_types::{
    BitcoinBlockData, BitcoinBlockMetadata, BitcoinTransactionData, BitcoinTransactionMetadata,
    BlockCommitmentData, BlockIdentifier, KeyRegistrationData, LockSTXData,
    OrdinalInscriptionRevealData, OrdinalInscriptionRevealInscriptionData,
    OrdinalInscriptionRevealOrdinalData, OrdinalOperation, PobBlockCommitmentData,
    PoxBlockCommitmentData, PoxReward, StacksBaseChainOperation, TransactionIdentifier,
    TransferSTXData,
};
use clarity_repl::clarity::util::hash::to_hex;
use hiro_system_kit::slog;
use rocket::serde::json::Value as JsonValue;

use super::ordinals::indexing::updater::OrdinalIndexUpdater;
use super::ordinals::indexing::OrdinalIndex;
use super::ordinals::inscription::InscriptionParser;
use super::ordinals::inscription_id::InscriptionId;
use super::BitcoinChainContext;

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub hash: bitcoin::BlockHash,
    pub confirmations: i32,
    pub size: usize,
    pub strippedsize: Option<usize>,
    pub weight: usize,
    pub height: usize,
    pub version: i32,
    pub merkleroot: bitcoin::TxMerkleNode,
    pub tx: Vec<GetRawTransactionResult>,
    pub time: usize,
    pub mediantime: Option<usize>,
    pub nonce: u32,
    pub bits: String,
    pub difficulty: f64,
    pub n_tx: usize,
    pub previousblockhash: bitcoin::BlockHash,
}

#[derive(Deserialize)]
pub struct NewBitcoinBlock {
    pub burn_block_hash: String,
    pub burn_block_height: u64,
    pub reward_slot_holders: Vec<String>,
    pub reward_recipients: Vec<RewardParticipant>,
    pub burn_amount: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
pub struct RewardParticipant {
    recipient: String,
    amt: u64,
}

pub async fn retrieve_full_block(
    bitcoin_config: &BitcoinConfig,
    marshalled_block: JsonValue,
    _ctx: &Context,
) -> Result<(u64, Block), String> {
    let partial_block: NewBitcoinBlock = serde_json::from_value(marshalled_block)
        .map_err(|e| format!("unable for parse bitcoin block: {}", e.to_string()))?;
    let block_hash = partial_block.burn_block_hash.strip_prefix("0x").unwrap();

    use reqwest::Client as HttpClient;
    let body = json!({
        "jsonrpc": "1.0",
        "id": "chainhook-cli",
        "method": "getblock",
        "params": [block_hash, 2]
    });
    let http_client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("Unable to build http client");
    let block = http_client
        .post(&bitcoin_config.rpc_url)
        .basic_auth(&bitcoin_config.username, Some(&bitcoin_config.password))
        .header("Content-Type", "application/json")
        .header("Host", &bitcoin_config.rpc_url[7..])
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("unable to send request ({})", e))?
        .json::<bitcoincore_rpc::jsonrpc::Response>()
        .await
        .map_err(|e| format!("unable to parse response ({})", e))?
        .result::<Block>()
        .map_err(|e| format!("unable to parse response ({})", e))?;

    let block_height = partial_block.burn_block_height;
    Ok((block_height, block))
}

pub fn standardize_bitcoin_block(
    indexer_config: &IndexerConfig,
    block_height: u64,
    block: Block,
    bitcoin_context: &mut BitcoinChainContext,
    ctx: &Context,
) -> Result<BitcoinBlockData, String> {
    let mut transactions = vec![];

    match OrdinalIndexUpdater::update(&mut bitcoin_context.ordinal_index) {
        Ok(_) => {
            ctx.try_log(|logger| {
                slog::info!(
                    logger,
                    "Ordinal index updated (block count: {:?})",
                    bitcoin_context.ordinal_index.block_count()
                )
            });
        }
        Err(e) => {
            ctx.try_log(|logger| slog::error!(logger, "{}", e.to_string()));
        }
    }

    let expected_magic_bytes = get_stacks_canonical_magic_bytes(&indexer_config.bitcoin_network);
    let pox_config = get_canonical_pox_config(&indexer_config.bitcoin_network);

    ctx.try_log(|logger| slog::debug!(logger, "Standardizing Bitcoin block {}", block.hash,));

    for mut tx in block.tx.into_iter() {
        let txid = tx.txid.to_string();

        ctx.try_log(|logger| slog::debug!(logger, "Standardizing Bitcoin transaction {txid}"));

        let mut stacks_operations = vec![];
        if let Some(op) = try_parse_stacks_operation(
            &tx.vout,
            &pox_config,
            &expected_magic_bytes,
            block_height,
            ctx,
        ) {
            stacks_operations.push(op);
        }

        let mut ordinal_operations = vec![];
        if let Some(op) =
            try_parse_ordinal_operation(&tx, block_height, &bitcoin_context.ordinal_index, ctx)
        {
            ordinal_operations.push(op);
        }

        let mut inputs = vec![];
        for input in tx.vin.drain(..) {
            if input.is_coinbase() {
                continue;
            }
            inputs.push(TxIn {
                previous_output: OutPoint {
                    txid: input
                        .txid
                        .expect("not provided for coinbase txs")
                        .to_string(),
                    vout: input.vout.expect("not provided for coinbase txs"),
                },
                script_sig: format!(
                    "0x{}",
                    to_hex(&input.script_sig.expect("not provided for coinbase txs").hex)
                ),
                sequence: input.sequence,
                witness: input
                    .txinwitness
                    .unwrap_or(vec![])
                    .to_vec()
                    .iter()
                    .map(|w| format!("0x{}", to_hex(w)))
                    .collect::<Vec<_>>(),
            })
        }

        let mut outputs = vec![];
        for output in tx.vout.drain(..) {
            outputs.push(TxOut {
                value: output.value.to_sat(),
                script_pubkey: format!("0x{}", to_hex(&output.script_pub_key.hex)),
            });
        }

        let tx = BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: format!("0x{}", txid),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs,
                outputs,
                stacks_operations,
                ordinal_operations,
                proof: None,
            },
        };
        transactions.push(tx);
    }

    Ok(BitcoinBlockData {
        block_identifier: BlockIdentifier {
            hash: format!("0x{}", block.hash),
            index: block_height,
        },
        parent_block_identifier: BlockIdentifier {
            hash: format!("0x{}", block.previousblockhash),
            index: block_height - 1,
        },
        timestamp: block.time as u32,
        metadata: BitcoinBlockMetadata {},
        transactions,
    })
}

fn try_parse_ordinal_operation(
    tx: &GetRawTransactionResult,
    _block_height: u64,
    ordinal_index: &OrdinalIndex,
    ctx: &Context,
) -> Option<OrdinalOperation> {
    for input in tx.vin.iter() {
        if let Some(ref witnesses) = input.txinwitness {
            for bytes in witnesses.iter() {
                let script = Script::from(bytes.to_vec());
                let parser = InscriptionParser {
                    instructions: script.instructions().peekable(),
                };

                let inscription = match parser.parse_script() {
                    Ok(inscription) => inscription,
                    Err(_) => continue,
                };

                let inscription_id = InscriptionId {
                    txid: tx.txid.clone(),
                    index: 0,
                };
                let entries = ordinal_index.get_feed_inscriptions(3).unwrap();
                ctx.try_log(|logger| slog::info!(logger, "Feed: {:?}", entries));

                let inscription_entry = match ordinal_index
                    .get_inscription_entry(inscription_id.clone())
                {
                    Ok(Some(entry)) => entry,
                    _ => {
                        ctx.try_log(|logger| slog::info!(logger, "No inscriptions entry found in index, despite inscription detected in transaction"));
                        return None;
                    }
                };

                let no_content_bytes = vec![];
                let inscription_content_bytes = inscription.body().unwrap_or(&no_content_bytes);
                return Some(OrdinalOperation::InscriptionRevealed(
                    OrdinalInscriptionRevealData {
                        inscription: OrdinalInscriptionRevealInscriptionData {
                            content_type: inscription
                                .content_type()
                                .unwrap_or("unknown")
                                .to_string(),
                            content_bytes: format!("0x{}", to_hex(&inscription_content_bytes)),
                            content_length: inscription_content_bytes.len(),
                            inscription_id: inscription_id.to_string(),
                            inscription_number: inscription_entry.number,
                            inscription_author: "".into(),
                            inscription_fee: inscription_entry.fee,
                        },
                        ordinal: Some(OrdinalInscriptionRevealOrdinalData {
                            ordinal_number: inscription_entry.sat.unwrap().n(),
                            ordinal_block_height: 0,
                            ordinal_offset: 0,
                        }),
                    },
                ));
            }
        }
    }
    None
}

fn try_parse_stacks_operation(
    outputs: &Vec<GetRawTransactionResultVout>,
    pox_config: &PoxConfig,
    expected_magic_bytes: &[u8; 2],
    block_height: u64,
    ctx: &Context,
) -> Option<StacksBaseChainOperation> {
    if outputs.is_empty() {
        return None;
    }

    // Safely parsing the first 2 bytes (following OP_RETURN + PUSH_DATA)
    let op_return_output = &outputs[0].script_pub_key.hex;
    if op_return_output.len() < 7 {
        return None;
    }
    if op_return_output[3] != expected_magic_bytes[0]
        || op_return_output[4] != expected_magic_bytes[1]
    {
        return None;
    }
    // Safely classifying the Stacks operation;
    let op_type: StacksOpcodes = match op_return_output[5].try_into() {
        Ok(op) => op,
        Err(_) => {
            ctx.try_log(|logger| {
                slog::debug!(
                    logger,
                    "Stacks operation parsing - opcode unknown {}",
                    op_return_output[5]
                )
            });
            return None;
        }
    };
    let op = match op_type {
        StacksOpcodes::KeyRegister => {
            let res = try_parse_key_register_op(&op_return_output[6..])?;
            StacksBaseChainOperation::KeyRegistration(res)
        }
        StacksOpcodes::PreStx => {
            let _ = try_parse_pre_stx_op(&op_return_output[6..])?;
            return None;
        }
        StacksOpcodes::TransferStx => {
            let res = try_parse_transfer_stx_op(&op_return_output[6..])?;
            StacksBaseChainOperation::TransferSTX(res)
        }
        StacksOpcodes::StackStx => {
            let res = try_parse_stacks_stx_op(&op_return_output[6..])?;
            StacksBaseChainOperation::LockSTX(res)
        }
        StacksOpcodes::BlockCommit => {
            let res = try_parse_block_commit_op(&op_return_output[5..])?;
            // We need to determine wether the transaction was a PoB or a Pox commitment
            if pox_config.is_consensus_rewarding_participants_at_block_height(block_height) {
                if outputs.len() < 1 + pox_config.rewarded_addresses_per_block {
                    return None;
                }
                let mut rewards = vec![];
                for output in outputs[1..pox_config.rewarded_addresses_per_block].into_iter() {
                    rewards.push(PoxReward {
                        recipient: format!("0x{}", to_hex(&output.script_pub_key.hex)),
                        amount: output.value.to_sat(),
                    });
                }
                StacksBaseChainOperation::PoxBlockCommitment(PoxBlockCommitmentData {
                    signers: vec![], // todo(lgalabru)
                    stacks_block_hash: res.stacks_block_hash.clone(),
                    rewards,
                })
            } else {
                if outputs.len() < 2 {
                    return None;
                }
                let amount = outputs[1].value;
                StacksBaseChainOperation::PobBlockCommitment(PobBlockCommitmentData {
                    signers: vec![], // todo(lgalabru)
                    stacks_block_hash: res.stacks_block_hash.clone(),
                    amount: amount.to_sat(),
                })
            }
        }
    };

    Some(op)
}

fn try_parse_block_commit_op(bytes: &[u8]) -> Option<BlockCommitmentData> {
    if bytes.len() < 32 {
        return None;
    }

    Some(BlockCommitmentData {
        stacks_block_hash: format!("0x{}", to_hex(&bytes[0..32])),
    })
}

fn try_parse_key_register_op(_bytes: &[u8]) -> Option<KeyRegistrationData> {
    Some(KeyRegistrationData {})
}

fn try_parse_pre_stx_op(_bytes: &[u8]) -> Option<()> {
    None
}

fn try_parse_transfer_stx_op(bytes: &[u8]) -> Option<TransferSTXData> {
    if bytes.len() < 16 {
        return None;
    }

    // todo(lgalabru)
    Some(TransferSTXData {
        sender: "".into(),
        recipient: "".into(),
        amount: "".into(),
    })
}

fn try_parse_stacks_stx_op(bytes: &[u8]) -> Option<LockSTXData> {
    if bytes.len() < 16 {
        return None;
    }

    // todo(lgalabru)
    Some(LockSTXData {
        sender: "".into(),
        amount: "".into(),
        duration: 1,
    })
}

#[cfg(test)]
pub mod tests;

// Test vectors
// 1) Devnet PoB
// 2022-10-26T03:06:17.376341Z  INFO chainhook_event_observer::indexer: BitcoinBlockData { block_identifier: BlockIdentifier { index: 104, hash: "0x210d0d095a75d88fc059cb97f453eee33b1833153fb1f81b9c3c031c26bb106b" }, parent_block_identifier: BlockIdentifier { index: 103, hash: "0x5d5a4b8113c35f20fb0b69b1fb1ae1b88461ea57e2a2e4c036f97fae70ca1abb" }, timestamp: 1666753576, transactions: [BitcoinTransactionData { transaction_identifier: TransactionIdentifier { hash: "0xfaaac1833dc4883e7ec28f61e35b41f896c395f8d288b1a177155de2abd6052f" }, operations: [], metadata: BitcoinTransactionMetadata { inputs: [TxIn { previous_output: OutPoint { txid: "0000000000000000000000000000000000000000000000000000000000000000", vout: 4294967295 }, script_sig: "01680101", sequence: 4294967295, witness: [] }], outputs: [TxOut { value: 5000017550, script_pubkey: "76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac" }, TxOut { value: 0, script_pubkey: "6a24aa21a9ed4a190dfdc77e260409c2a693e6d3b8eca43afbc4bffb79ddcdcc9516df804d9b" }], stacks_operations: [] } }, BitcoinTransactionData { transaction_identifier: TransactionIdentifier { hash: "0x59193c24cb2325cd2271b89f790f958dcd4065088680ffbc201a0ebb2f3cbf25" }, operations: [], metadata: BitcoinTransactionMetadata { inputs: [TxIn { previous_output: OutPoint { txid: "9eebe848baaf8dd4810e4e4a91168e2e471c949439faf5d768750ca21d067689", vout: 3 }, script_sig: "483045022100a20f90e9e3c3bb7e558ad4fa65902d8cf6ce4bff1f5af0ac0a323b547385069c022021b9877abbc9d1eef175c7f712ac1b2d8f5ce566be542714effe42711e75b83801210239810ebf35e6f6c26062c99f3e183708d377720617c90a986859ec9c95d00be9", sequence: 4294967293, witness: [] }], outputs: [TxOut { value: 0, script_pubkey: "6a4c5069645b1681995f8e568287e0e4f5cbc1d6727dafb5e3a7822a77c69bd04208265aca9424d0337dac7d9e84371a2c91ece1891d67d3554bd9fdbe60afc6924d4b0773d90000006700010000006600012b" }, TxOut { value: 10000, script_pubkey: "76a914000000000000000000000000000000000000000088ac" }, TxOut { value: 10000, script_pubkey: "76a914000000000000000000000000000000000000000088ac" }, TxOut { value: 4999904850, script_pubkey: "76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac" }], stacks_operations: [PobBlockCommitment(PobBlockCommitmentData { signers: [], stacks_block_hash: "0x5b1681995f8e568287e0e4f5cbc1d6727dafb5e3a7822a77c69bd04208265aca", amount: 10000 })] } }], metadata: BitcoinBlockMetadata }
// 2022-10-26T03:06:21.929157Z  INFO chainhook_event_observer::indexer: BitcoinBlockData { block_identifier: BlockIdentifier { index: 105, hash: "0x0302c4c6063eb7199d3a565351bceeea9df4cb4aa09293194dbab277e46c2979" }, parent_block_identifier: BlockIdentifier { index: 104, hash: "0x210d0d095a75d88fc059cb97f453eee33b1833153fb1f81b9c3c031c26bb106b" }, timestamp: 1666753581, transactions: [BitcoinTransactionData { transaction_identifier: TransactionIdentifier { hash: "0xe7de433aa89c1f946f89133b0463b6cfebb26ad73b0771a79fd66c6acbfe3fb9" }, operations: [], metadata: BitcoinTransactionMetadata { inputs: [TxIn { previous_output: OutPoint { txid: "0000000000000000000000000000000000000000000000000000000000000000", vout: 4294967295 }, script_sig: "01690101", sequence: 4294967295, witness: [] }], outputs: [TxOut { value: 5000017600, script_pubkey: "76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac" }, TxOut { value: 0, script_pubkey: "6a24aa21a9ed98ac3bc4e0c9ed53e3418a3bf3aa511dcd76088cf0e1c4fc71fb9755840d7a08" }], stacks_operations: [] } }, BitcoinTransactionData { transaction_identifier: TransactionIdentifier { hash: "0xe654501805d80d59ef0d95b57ad7a924f3be4a4dc0db5a785dfebe1f70c4e23e" }, operations: [], metadata: BitcoinTransactionMetadata { inputs: [TxIn { previous_output: OutPoint { txid: "59193c24cb2325cd2271b89f790f958dcd4065088680ffbc201a0ebb2f3cbf25", vout: 3 }, script_sig: "483045022100b59d2d07f68ea3a4f27a49979080a07b2432cfad9fc90e1edd0241496f0fd83f02205ac233f4cb68ada487f16339abedb7093948b683ba7d76b3b4058b2c0181a68901210239810ebf35e6f6c26062c99f3e183708d377720617c90a986859ec9c95d00be9", sequence: 4294967293, witness: [] }], outputs: [TxOut { value: 0, script_pubkey: "6a4c5069645b351bb015ef4f7dcdce4c9d95cbf157f85a3714626252cfc9078f3f1591ccdb13c3c7e22b34c4ffc2f6064a41df6fcd7f1b759d4f28b2f7cb6b27f283c868406e0000006800010000006600012c" }, TxOut { value: 10000, script_pubkey: "76a914000000000000000000000000000000000000000088ac" }, TxOut { value: 10000, script_pubkey: "76a914000000000000000000000000000000000000000088ac" }, TxOut { value: 4999867250, script_pubkey: "76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac" }], stacks_operations: [PobBlockCommitment(PobBlockCommitmentData { signers: [], stacks_block_hash: "0x5b351bb015ef4f7dcdce4c9d95cbf157f85a3714626252cfc9078f3f1591ccdb", amount: 10000 })] } }], metadata: BitcoinBlockMetadata }
// 2022-10-26T03:07:53.298531Z  INFO chainhook_event_observer::indexer: BitcoinBlockData { block_identifier: BlockIdentifier { index: 106, hash: "0x52eb2aa15aa99afc4b918a552cef13e8b6eed84b257be097ad954b4f37a7e98d" }, parent_block_identifier: BlockIdentifier { index: 105, hash: "0x0302c4c6063eb7199d3a565351bceeea9df4cb4aa09293194dbab277e46c2979" }, timestamp: 1666753672, transactions: [BitcoinTransactionData { transaction_identifier: TransactionIdentifier { hash: "0xd28d7f5411416f94b95e9f999d5ee8ded5543ba9daae9f612b80f01c5107862d" }, operations: [], metadata: BitcoinTransactionMetadata { inputs: [TxIn { previous_output: OutPoint { txid: "0000000000000000000000000000000000000000000000000000000000000000", vout: 4294967295 }, script_sig: "016a0101", sequence: 4294967295, witness: [] }], outputs: [TxOut { value: 5000017500, script_pubkey: "76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac" }, TxOut { value: 0, script_pubkey: "6a24aa21a9ed71aaf7e5384879a1b112bf623ac8b46dd88b39c3d2c6f8a1d264fc4463e6356a" }], stacks_operations: [] } }, BitcoinTransactionData { transaction_identifier: TransactionIdentifier { hash: "0x72e8e43afc4362cf921ccc57fde3e07b4cb6fac5f306525c86d38234c18e21d1" }, operations: [], metadata: BitcoinTransactionMetadata { inputs: [TxIn { previous_output: OutPoint { txid: "e654501805d80d59ef0d95b57ad7a924f3be4a4dc0db5a785dfebe1f70c4e23e", vout: 3 }, script_sig: "4730440220798bb7d7fb14df35610db2ef04e5d5b6588440b7c429bf650a96f8570904052b02204a817e13e7296a24a8f6cc8737bddb55d1835e513ec2b9dcb03424e4536ae34c01210239810ebf35e6f6c26062c99f3e183708d377720617c90a986859ec9c95d00be9", sequence: 4294967293, witness: [] }], outputs: [TxOut { value: 0, script_pubkey: "6a4c5069645b504d310fc27c86a6b65d0b0e0297db1e185d3432fdab9fa96a1053407ed07b537b8b7d23c6309dfd24340e85b75cff11ad685f8b310c1d2098748a0fffb146ec00000069000100000066000128" }, TxOut { value: 20000, script_pubkey: "76a914000000000000000000000000000000000000000088ac" }, TxOut { value: 4999829750, script_pubkey: "76a914ee9369fb719c0ba43ddf4d94638a970b84775f4788ac" }], stacks_operations: [PobBlockCommitment(PobBlockCommitmentData { signers: [], stacks_block_hash: "0x5b504d310fc27c86a6b65d0b0e0297db1e185d3432fdab9fa96a1053407ed07b", amount: 20000 })] } }], metadata: BitcoinBlockMetadata }
