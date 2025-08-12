use std::str::FromStr;

use bitcoin::{Script, Transaction, Witness};
use bitcoind::{
    bitcoincore_rpc::{self, Client as BitcoinRPCClient},
    try_info, try_warn,
    utils::{
        bitcoind::{bitcoin_get_raw_transaction, bitcoind_get_block_height, bitcoind_get_client},
        Context,
    },
};
use config::BitcoindConfig;
use ordinals_parser::{Rune, Runestone};

fn unversioned_leaf_script_from_witness(witness: &Witness) -> Option<&Script> {
    #[allow(deprecated)]
    witness.tapscript()
}

/// Reserved runes cannot be etched because they serve as a fallback mechanism for failed etchings
/// and prevent namespace collision in the Bitcoin Runes protocol. When an etching transaction fails
/// to specify a valid rune name or the etching is malformed, the protocol automatically assigns a
/// "reserved" rune name using a deterministic formula to maintain protocol integrity.
pub fn is_reserved(rune: &Rune) -> bool {
    rune.0 >= Rune::RESERVED
}

/// Validates that a rune etching transaction has a proper commitment transaction.
///
/// The Bitcoin Runes protocol requires a two-step "commit-reveal" process for etching new runes:
/// 1. **Commit**: A transaction includes the rune's commitment bytes in a tapscript
/// 2. **Reveal**: A subsequent transaction (this one) actually performs the etching
///
/// This function validates the reveal transaction by checking that:
/// - At least one input spends from a taproot output that contains the rune's commitment
/// - The commitment transaction was confirmed at least 6 blocks before the reveal
/// - The commitment appears in a valid tapscript witness
///
/// # Arguments
/// * `config` - Bitcoin node configuration for RPC calls
/// * `ctx` - Logging and error context
/// * `tx` - The reveal transaction attempting to etch the rune
/// * `rune` - The rune being etched (used to generate expected commitment bytes)
/// * `reveal_block_height` - Block height where this reveal transaction appears
/// * `inputs_counter` - Mutable counter tracking total inputs processed (for metrics)
///
/// # Returns
/// * `Ok(true)` - Valid commitment found with sufficient confirmations
/// * `Ok(false)` - No valid commitment found or insufficient confirmations
/// * `Err(_)` - Error accessing blockchain data or parsing scripts
///
/// # Validation Rules
/// - Commitment must appear in tapscript witness data
/// - The spent output must be a P2TR (taproot) output
/// - At least 6 block confirmations between commit and reveal
/// - Commitment bytes must exactly match `rune.commitment()`
async fn rune_etching_has_valid_commit(
    bitcoin_client: &BitcoinRPCClient,
    ctx: &Context,
    tx: &Transaction,
    rune: &Rune,
    reveal_block_height: u32,
    inputs_counter: &mut u64,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let commitment = rune.commitment();

    // Check each input for valid commitment (the commitment could be in any input)
    for input in &tx.input {
        *inputs_counter += 1;
        // Extract tapscript from witness data (note: this doesn't guarantee the spent output is taproot)
        let Some(tapscript) = unversioned_leaf_script_from_witness(&input.witness) else {
            continue;
        };

        for instruction in tapscript.instructions() {
            // Skip invalid script instructions
            let Ok(instruction) = instruction else {
                break;
            };

            let Some(pushbytes) = instruction.push_bytes() else {
                continue;
            };

            // Check if this instruction pushes the expected commitment bytes
            if pushbytes.as_bytes() != commitment {
                continue;
            }

            let txid =
                bitcoincore_rpc::bitcoin::Txid::from_str(&input.previous_output.txid.to_string())
                    .unwrap();

            // Fetch the commit transaction to validate taproot output and get block height
            let Some(commit_tx_info) = bitcoin_get_raw_transaction(bitcoin_client, ctx, &txid)
            else {
                panic!(
                    "can't get input transaction: {}",
                    input.previous_output.txid
                );
            };

            // Verify the spent output is a taproot (P2TR) output
            let taproot = commit_tx_info.vout[input.previous_output.vout as usize]
                .script_pub_key
                .script()?
                .is_p2tr();

            if !taproot {
                continue;
            }

            // Get commit transaction's block height to check confirmation count
            let commit_tx_height =
                bitcoind_get_block_height(bitcoin_client, ctx, &commit_tx_info.blockhash.unwrap());

            // Calculate confirmations and check minimum requirement (6 blocks)
            let confirmations = reveal_block_height.checked_sub(commit_tx_height).unwrap() + 1;
            if confirmations >= u32::from(Runestone::COMMIT_CONFIRMATIONS) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Validates that a rune etching transaction has a proper commitment transaction with automatic reconnection.
///
/// This function wraps the core validation logic with connection error handling. If a connection
/// error is detected, it will attempt to reconnect the Bitcoin RPC client and retry the validation.
pub async fn rune_etching_has_valid_commit_with_reconnect(
    bitcoin_client: &mut BitcoinRPCClient,
    config: &BitcoindConfig,
    ctx: &Context,
    tx: &Transaction,
    rune: &Rune,
    reveal_block_height: u32,
    inputs_counter: &mut u64,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match rune_etching_has_valid_commit(
        bitcoin_client,
        ctx,
        tx,
        rune,
        reveal_block_height,
        inputs_counter,
    )
    .await
    {
        Ok(result) => Ok(result),
        Err(error) => {
            // Check if this is a connection error
            if is_connection_error(&error) {
                try_warn!(ctx, "Bitcoin RPC connection error detected: {}", error);
                try_info!(ctx, "Attempting to reconnect Bitcoin RPC client...");

                // Create a new client connection
                *bitcoin_client = bitcoind_get_client(config, ctx);
                try_info!(
                    ctx,
                    "Bitcoin RPC client reconnected, retrying validation..."
                );

                // Retry the validation with the new connection
                rune_etching_has_valid_commit(
                    bitcoin_client,
                    ctx,
                    tx,
                    rune,
                    reveal_block_height,
                    inputs_counter,
                )
                .await
            } else {
                // Not a connection error, propagate the original error
                Err(error)
            }
        }
    }
}

/// Checks if an error indicates a connection issue that requires reconnection
#[allow(clippy::borrowed_box)]
fn is_connection_error(error: &Box<dyn std::error::Error + Send + Sync>) -> bool {
    let error_msg = error.to_string().to_lowercase();

    // Check for common connection-related error patterns
    error_msg.contains("connection refused") ||
    error_msg.contains("connection reset") ||
    error_msg.contains("connection closed") ||
    error_msg.contains("connection timeout") ||
    error_msg.contains("connection aborted") ||
    error_msg.contains("broken pipe") ||
    error_msg.contains("network unreachable") ||
    error_msg.contains("host unreachable") ||
    error_msg.contains("timed out") ||
    error_msg.contains("could not connect") ||
    error_msg.contains("transport error") ||
    error_msg.contains("rpc_client_not_connected") ||
    error_msg.contains("no connection available") ||
    error_msg.contains("io error") ||
    error_msg.contains("transport is disconnected") ||
    // Bitcoin Core specific RPC errors
    error_msg.contains("rpc_client_not_connected") ||
    error_msg.contains("work queue depth exceeded")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bitcoin::{
        opcodes, script::Builder, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
    };
    use bitcoind::utils::{bitcoind::bitcoind_get_client, Context};
    use config::BitcoindConfig;
    use ordinals_parser::Rune;

    use super::*;

    // Mock implementations for testing
    struct MockBitcoindConfig;

    impl MockBitcoindConfig {
        fn new() -> BitcoindConfig {
            BitcoindConfig {
                rpc_username: "test".to_string(),
                rpc_password: "test".to_string(),
                rpc_url: "http://localhost:8332".to_string(),
                network: bitcoin::Network::Regtest,
                zmq_url: "tcp://localhost:28332".to_string(),
            }
        }
    }

    fn create_mock_transaction_with_witness(witness_data: Vec<Vec<u8>>) -> Transaction {
        let mut witness = Witness::new();
        for data in witness_data {
            witness.push(data);
        }

        Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_str(
                        "0000000000000000000000000000000000000000000000000000000000000001",
                    )
                    .unwrap(),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness,
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn create_mock_transaction_no_witness() -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_str(
                        "0000000000000000000000000000000000000000000000000000000000000001",
                    )
                    .unwrap(),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: bitcoin::Amount::from_sat(1000),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    fn create_tapscript_with_commitment(commitment: &[u8]) -> ScriptBuf {
        use bitcoin::script::PushBytesBuf;

        let push_bytes = PushBytesBuf::try_from(commitment.to_vec()).unwrap();
        Builder::new()
            .push_slice(&push_bytes)
            .push_opcode(opcodes::all::OP_DROP)
            .push_opcode(opcodes::OP_TRUE)
            .into_script()
    }

    /// Tests that reserved runes are correctly identified
    /// This validates the core reserved rune detection logic
    #[test]
    fn test_is_reserved_returns_true_for_reserved_rune() {
        let reserved_rune = Rune::reserved(840000, 1);
        // Additional reserved runes for testing
        let reserved_rune2 = Rune(6402364363415443603228541259936211926);
        let reserved_rune3 = Rune(6402364363415443603228541259936211927);
        assert!(is_reserved(&reserved_rune));
        assert!(is_reserved(&reserved_rune2));
        assert!(is_reserved(&reserved_rune3));
    }

    /// Tests that non-reserved runes are not incorrectly flagged as reserved
    /// This ensures the RESERVED threshold is working correctly
    #[test]
    fn test_is_reserved_returns_false_for_non_reserved_rune() {
        let non_reserved_rune = Rune(1000); // Well below RESERVED threshold
        assert!(!is_reserved(&non_reserved_rune));
    }

    /// Tests that transactions without witness data fail commitment validation
    /// This validates: "tx input is not a commit" case
    #[tokio::test]
    async fn test_tx_commits_to_rune_fails_with_no_witness() {
        let config = MockBitcoindConfig::new();
        let ctx = Context::empty();
        let mut bitcoin_client = bitcoind_get_client(&config, &ctx);
        let tx = create_mock_transaction_no_witness();
        let rune = Rune(1000);
        let mut inputs_counter = 0;

        let result = rune_etching_has_valid_commit(
            &mut bitcoin_client,
            &ctx,
            &tx,
            &rune,
            840005,
            &mut inputs_counter,
        )
        .await
        .unwrap();

        assert!(!result);
        assert_eq!(inputs_counter, 1);
    }

    /// Tests that transactions with incorrect commitment data fail validation
    /// This validates the commitment matching logic
    #[tokio::test]
    async fn test_tx_commits_to_rune_fails_with_wrong_commitment() {
        let config = MockBitcoindConfig::new();
        let ctx = Context::empty();
        let mut bitcoin_client = bitcoind_get_client(&config, &ctx);
        let rune = Rune(1000);
        let wrong_commitment = b"wrong_commitment_data";

        // Create tapscript with wrong commitment
        let tapscript = create_tapscript_with_commitment(wrong_commitment);

        // Create witness with the tapscript
        let witness_data = vec![
            vec![], // Signature placeholder
            tapscript.to_bytes(),
        ];

        let tx = create_mock_transaction_with_witness(witness_data);
        let mut inputs_counter = 0;

        let result = rune_etching_has_valid_commit(
            &mut bitcoin_client,
            &ctx,
            &tx,
            &rune,
            840005,
            &mut inputs_counter,
        )
        .await
        .unwrap();

        assert!(!result);
        assert_eq!(inputs_counter, 1);
    }

    /// Tests that rune commitment generation is deterministic and correct
    /// This validates the core commitment encoding (rune value as bytes, not a hash!)
    #[test]
    fn test_rune_commitment_generation() {
        let rune = Rune(1000);
        let commitment = rune.commitment();

        // The commitment is the rune value encoded as bytes (little-endian)
        // For Rune(1000), this is [232, 3] (little-endian encoding of 1000)
        // 1000 = 0x03E8 -> little-endian [0xE8, 0x03] = [232, 3]
        assert_eq!(commitment.len(), 2);
        assert_eq!(commitment, &[232, 3]);

        // Same rune should generate same commitment
        let rune2 = Rune(1000);
        let commitment2 = rune2.commitment();
        assert_eq!(commitment, commitment2);

        // Different rune should generate different commitment
        let rune3 = Rune(2000);
        let commitment3 = rune3.commitment();
        assert_ne!(commitment, commitment3);
        // Rune(2000) = 0x07D0 -> little-endian [0xD0, 0x07] = [208, 7]
        assert_eq!(commitment3, &[208, 7]);

        // Test larger rune value that requires more bytes
        let large_rune = Rune(0x123456789ABCDEF0);
        let large_commitment = large_rune.commitment();
        assert!(large_commitment.len() > 2); // Larger values need more bytes

        // Test very small rune (single byte)
        let small_rune = Rune(42);
        let small_commitment = small_rune.commitment();
        assert_eq!(small_commitment, &[42]); // Should be just [42]
    }

    // Helper function for testing - mimics tx_commits_to_rune but with mockable RPC calls
    async fn tx_commits_to_rune_with_mocks(
        tx: &Transaction,
        rune: &Rune,
        reveal_block_height: u32,
        mock_commit_tx_block_height: u32,
        mock_is_taproot: bool,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let commitment = rune.commitment();

        for input in &tx.input {
            let Some(tapscript) = unversioned_leaf_script_from_witness(&input.witness) else {
                continue;
            };

            for instruction in tapscript.instructions() {
                let Ok(instruction) = instruction else {
                    break;
                };

                let Some(pushbytes) = instruction.push_bytes() else {
                    continue;
                };

                if pushbytes.as_bytes() != commitment {
                    continue;
                }

                // Mock: Assume we found the commit transaction
                // In real code: bitcoin_get_raw_transaction(config, ctx, &txid)
                if !mock_is_taproot {
                    continue;
                }

                // Mock: Return our mock block height
                // In real code: bitcoind_get_block_height(config, ctx, &commit_tx_info.blockhash.unwrap())
                let commit_tx_height = mock_commit_tx_block_height;

                let confirmations = reveal_block_height.checked_sub(commit_tx_height).unwrap() + 1;
                if confirmations >= u32::from(Runestone::COMMIT_CONFIRMATIONS) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn create_transaction_with_valid_commitment(rune: &Rune) -> Transaction {
        let commitment = rune.commitment();
        let tapscript = create_tapscript_with_commitment(&commitment);

        // For taproot script path spending, we need:
        // - signature (can be empty for script-only validation)
        // - script
        // - control block (minimal for testing)
        let witness_data = vec![
            vec![], // Empty signature for script-only validation
            tapscript.to_bytes(),
            vec![0xc0], // Minimal control block (leaf version + empty merkle path)
        ];

        create_mock_transaction_with_witness(witness_data)
    }

    /// Tests that commit_tx in the same block as reveal_tx fails validation
    /// This validates: "commit_tx (tx_input) happened in same block"
    #[tokio::test]
    async fn test_tx_commits_to_rune_fails_same_block() {
        let rune = Rune(1000);
        let tx = create_transaction_with_valid_commitment(&rune);

        let reveal_block_height = 840010;
        let commit_block_height = 840010; // Same block!
        let is_taproot = true;

        let result = tx_commits_to_rune_with_mocks(
            &tx,
            &rune,
            reveal_block_height,
            commit_block_height,
            is_taproot,
        )
        .await
        .unwrap();

        // Should fail because commit and reveal are in the same block
        // confirmations = 840010 - 840010 + 1 = 1, which is < 6
        assert!(!result);
    }

    /// Tests that commit_tx with insufficient confirmations fails validation
    /// This validates: "commit_tx has not enough block heights confirmed (6)"
    #[tokio::test]
    async fn test_tx_commits_to_rune_fails_insufficient_confirmations() {
        let rune = Rune(1000);
        let tx = create_transaction_with_valid_commitment(&rune);

        let reveal_block_height = 840010;
        let commit_block_height = 840006; // 5 confirmations (840010 - 840006 + 1 = 5)
        let is_taproot = true;

        let result = tx_commits_to_rune_with_mocks(
            &tx,
            &rune,
            reveal_block_height,
            commit_block_height,
            is_taproot,
        )
        .await
        .unwrap();

        // Should fail because only 5 confirmations, need 6
        assert!(!result);
    }

    /// Tests edge case: exactly 5 confirmations should fail
    #[tokio::test]
    async fn test_tx_commits_to_rune_fails_exactly_five_confirmations() {
        let rune = Rune(1000);
        let tx = create_transaction_with_valid_commitment(&rune);

        let reveal_block_height = 840010;
        let commit_block_height = 840006; // Exactly 5 confirmations
        let is_taproot = true;

        let result = tx_commits_to_rune_with_mocks(
            &tx,
            &rune,
            reveal_block_height,
            commit_block_height,
            is_taproot,
        )
        .await
        .unwrap();

        assert!(!result);
    }

    /// Tests that exactly 6 confirmations should succeed
    #[tokio::test]
    async fn test_tx_commits_to_rune_succeeds_exactly_six_confirmations() {
        let rune = Rune(1000);
        let tx = create_transaction_with_valid_commitment(&rune);

        let reveal_block_height = 840010;
        let commit_block_height = 840005; // Exactly 6 confirmations (840010 - 840005 + 1 = 6)
        let is_taproot = true;

        let result = tx_commits_to_rune_with_mocks(
            &tx,
            &rune,
            reveal_block_height,
            commit_block_height,
            is_taproot,
        )
        .await
        .unwrap();

        // Should succeed with exactly 6 confirmations
        assert!(result);
    }

    /// Tests that proper commitment with ≥6 confirmations succeeds
    /// This validates the happy path of commitment validation
    #[tokio::test]
    async fn test_tx_commits_to_rune_succeeds_with_valid_commitment() {
        let rune = Rune(1000);
        let tx = create_transaction_with_valid_commitment(&rune);

        let reveal_block_height = 840010;
        let commit_block_height = 840000; // 11 confirmations (840010 - 840000 + 1 = 11)
        let is_taproot = true;

        let result = tx_commits_to_rune_with_mocks(
            &tx,
            &rune,
            reveal_block_height,
            commit_block_height,
            is_taproot,
        )
        .await
        .unwrap();

        // Should succeed with sufficient confirmations
        assert!(result);
    }

    /// Tests that non-taproot commitment transactions fail
    /// This validates the taproot requirement
    #[tokio::test]
    async fn test_tx_commits_to_rune_fails_non_taproot() {
        let rune = Rune(1000);
        let tx = create_transaction_with_valid_commitment(&rune);

        let reveal_block_height = 840010;
        let commit_block_height = 840000; // Sufficient confirmations
        let is_taproot = false; // Not a taproot transaction

        let result = tx_commits_to_rune_with_mocks(
            &tx,
            &rune,
            reveal_block_height,
            commit_block_height,
            is_taproot,
        )
        .await
        .unwrap();

        // Should fail because commit tx is not taproot
        assert!(!result);
    }

    /// Tests multiple inputs where only one has valid commitment
    #[tokio::test]
    async fn test_tx_commits_to_rune_multiple_inputs_one_valid() {
        let rune = Rune(1000);
        let commitment = rune.commitment();

        // Create transaction with multiple inputs
        let mut tx = create_mock_transaction_no_witness();

        // First input: no valid commitment
        let wrong_tapscript = create_tapscript_with_commitment(b"wrong_commitment");
        let mut wrong_witness = Witness::new();
        wrong_witness.push(vec![]); // Empty signature
        wrong_witness.push(wrong_tapscript.to_bytes());
        wrong_witness.push(vec![0xc0]); // Control block
        tx.input[0].witness = wrong_witness;

        // Add second input with valid commitment
        let valid_tapscript = create_tapscript_with_commitment(&commitment);
        let mut valid_witness = Witness::new();
        valid_witness.push(vec![]); // Empty signature
        valid_witness.push(valid_tapscript.to_bytes());
        valid_witness.push(vec![0xc0]); // Control block

        tx.input.push(TxIn {
            previous_output: OutPoint {
                txid: bitcoin::Txid::from_str(
                    "0000000000000000000000000000000000000000000000000000000000000002",
                )
                .unwrap(),
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: valid_witness,
        });

        let reveal_block_height = 840010;
        let commit_block_height = 840000;
        let is_taproot = true;

        let result = tx_commits_to_rune_with_mocks(
            &tx,
            &rune,
            reveal_block_height,
            commit_block_height,
            is_taproot,
        )
        .await
        .unwrap();

        // Should succeed because second input has valid commitment
        assert!(result);
    }
}
