use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Context as Ctx;
use bitcoincore_rpc::bitcoin::{Amount, Block, BlockHash, OutPoint, Transaction, Txid};
use bitcoincore_rpc::RpcApi;
use chrono::{DateTime, TimeZone, Utc};

use crate::utils::Context;

use super::blocktime::Blocktime;
use super::chain::Chain;
use super::height::Height;
use super::sat_point::SatPoint;
use super::{inscription_id::InscriptionId, sat::Sat};
use std::cmp;

pub mod entry;
mod fetcher;

use {
    self::{
        entry::{
            BlockHashValue, Entry, InscriptionEntry, InscriptionEntryValue, InscriptionIdValue,
            OutPointValue, SatPointValue, SatRange,
        },
        updater::OrdinalIndexUpdater,
    },
    super::*,
    bitcoincore_rpc::bitcoin::BlockHeader,
    bitcoincore_rpc::{json::GetBlockHeaderResult, Auth, Client},
    chrono::SubsecRound,
    redb::{Database, ReadableTable, Table, TableDefinition, WriteStrategy, WriteTransaction},
    std::collections::HashMap,
    std::sync::atomic::{self, AtomicBool},
};

mod rtx;
pub mod updater;

const SCHEMA_VERSION: u64 = 2;

macro_rules! define_table {
    ($name:ident, $key:ty, $value:ty) => {
        const $name: TableDefinition<$key, $value> = TableDefinition::new(stringify!($name));
    };
}

define_table! { HEIGHT_TO_BLOCK_HASH, u64, &BlockHashValue }
define_table! { INSCRIPTION_ID_TO_INSCRIPTION_ENTRY, &InscriptionIdValue, InscriptionEntryValue }
define_table! { INSCRIPTION_ID_TO_SATPOINT, &InscriptionIdValue, &SatPointValue }
define_table! { INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, u64, &InscriptionIdValue }
define_table! { OUTPOINT_TO_SAT_RANGES, &OutPointValue, &[u8] }
define_table! { OUTPOINT_TO_VALUE, &OutPointValue, u64}
define_table! { SATPOINT_TO_INSCRIPTION_ID, &SatPointValue, &InscriptionIdValue }
define_table! { SAT_TO_INSCRIPTION_ID, u64, &InscriptionIdValue }
define_table! { SAT_TO_SATPOINT, u64, &SatPointValue }
define_table! { STATISTIC_TO_COUNT, u64, u64 }
define_table! { WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP, u64, u128 }

pub fn timestamp(seconds: u32) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds.into(), 0).unwrap()
}

pub(crate) struct Options {
    pub rpc_username: String,
    pub rpc_password: String,
    pub data_dir: PathBuf,
    pub chain: Chain,
    pub first_inscription_height: Option<u64>,
    pub height_limit: Option<u64>,
    pub index: Option<PathBuf>,
    pub rpc_url: String,
}

pub struct OrdinalIndex {
    auth: Auth,
    client: Client,
    database: Database,
    path: PathBuf,
    first_inscription_height: u64,
    genesis_block_coinbase_transaction: Transaction,
    genesis_block_coinbase_txid: Txid,
    height_limit: Option<u64>,
    reorged: AtomicBool,
    rpc_url: String,
}

#[derive(Debug, PartialEq)]
pub(crate) enum List {
    Spent,
    Unspent(Vec<(u64, u64)>),
}

#[derive(Copy, Clone)]
#[repr(u64)]
pub(crate) enum Statistic {
    Schema = 0,
    Commits = 1,
    LostSats = 2,
    OutputsTraversed = 3,
    SatRanges = 4,
}

impl Statistic {
    fn key(self) -> u64 {
        self.into()
    }
}

impl From<Statistic> for u64 {
    fn from(statistic: Statistic) -> Self {
        statistic as u64
    }
}

#[derive(Serialize, Debug)]
pub(crate) struct Info {
    pub(crate) blocks_indexed: u64,
    pub(crate) branch_pages: usize,
    pub(crate) fragmented_bytes: usize,
    pub(crate) index_file_size: u64,
    pub(crate) index_path: PathBuf,
    pub(crate) leaf_pages: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) outputs_traversed: u64,
    pub(crate) page_size: usize,
    pub(crate) sat_ranges: u64,
    pub(crate) stored_bytes: usize,
    pub(crate) transactions: Vec<TransactionInfo>,
    pub(crate) tree_height: usize,
    pub(crate) utxos_indexed: usize,
}

#[derive(Serialize, Debug)]
pub(crate) struct TransactionInfo {
    pub(crate) starting_block_count: u64,
    pub(crate) starting_timestamp: u128,
}

trait BitcoinCoreRpcResultExt<T> {
    fn into_option(self) -> Result<Option<T>>;
}

impl<T> BitcoinCoreRpcResultExt<T> for Result<T, bitcoincore_rpc::Error> {
    fn into_option(self) -> Result<Option<T>> {
        match self {
            Ok(ok) => Ok(Some(ok)),
            Err(bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::error::Error::Rpc(
                bitcoincore_rpc::jsonrpc::error::RpcError { code: -8, .. },
            ))) => Ok(None),
            Err(bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::error::Error::Rpc(
                bitcoincore_rpc::jsonrpc::error::RpcError { message, .. },
            ))) if message.ends_with("not found") => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

impl OrdinalIndex {
    pub(crate) fn open(options: &Options) -> Result<Self> {
        let rpc_url = options.rpc_url.clone();

        let auth = Auth::UserPass(options.rpc_username.clone(), options.rpc_password.clone());

        let client = Client::new(&rpc_url, auth.clone()).context("failed to connect to RPC URL")?;

        let data_dir = options.data_dir.clone();

        if let Err(err) = std::fs::create_dir_all(&data_dir) {
            anyhow::bail!("failed to create data dir `{}`: {err}", data_dir.display());
        }

        let path = if let Some(path) = &options.index {
            path.clone()
        } else {
            data_dir.join("ordinal_index.redb")
        };

        let database = match unsafe { Database::builder().open_mmapped(&path) } {
            Ok(database) => {
                let schema_version = database
                    .begin_read()?
                    .open_table(STATISTIC_TO_COUNT)?
                    .get(&Statistic::Schema.key())?
                    .map(|x| x.value())
                    .unwrap_or(0);

                match schema_version.cmp(&SCHEMA_VERSION) {
          cmp::Ordering::Less =>
            anyhow::bail!(
              "index at `{}` appears to have been built with an older, incompatible version of ord, consider deleting and rebuilding the index: index schema {schema_version}, ord schema {SCHEMA_VERSION}",
              path.display()
            ),
          cmp::Ordering::Greater =>
            anyhow::bail!(
              "index at `{}` appears to have been built with a newer, incompatible version of ord, consider updating ord: index schema {schema_version}, ord schema {SCHEMA_VERSION}",
              path.display()
            ),
          cmp::Ordering::Equal => {
          }
        }

                database
            }
            Err(redb::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                let database = unsafe {
                    Database::builder()
                        .set_write_strategy(if cfg!(test) {
                            WriteStrategy::Checksum
                        } else {
                            WriteStrategy::TwoPhase
                        })
                        .create_mmapped(&path)?
                };
                let tx = database.begin_write()?;

                #[cfg(test)]
                let tx = {
                    let mut tx = tx;
                    tx.set_durability(redb::Durability::None);
                    tx
                };

                tx.open_table(HEIGHT_TO_BLOCK_HASH)?;
                tx.open_table(INSCRIPTION_ID_TO_INSCRIPTION_ENTRY)?;
                tx.open_table(INSCRIPTION_ID_TO_SATPOINT)?;
                tx.open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?;
                tx.open_table(OUTPOINT_TO_VALUE)?;
                tx.open_table(SATPOINT_TO_INSCRIPTION_ID)?;
                tx.open_table(SAT_TO_INSCRIPTION_ID)?;
                tx.open_table(SAT_TO_SATPOINT)?;
                tx.open_table(WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP)?;

                tx.open_table(STATISTIC_TO_COUNT)?
                    .insert(&Statistic::Schema.key(), &SCHEMA_VERSION)?;

                tx.open_table(OUTPOINT_TO_SAT_RANGES)?
                    .insert(&OutPoint::null().store(), [].as_slice())?;

                tx.commit()?;

                database
            }
            Err(error) => return Err(error.into()),
        };

        let genesis_block_coinbase_transaction =
            options.chain.genesis_block().coinbase().unwrap().clone();

        Ok(Self {
            genesis_block_coinbase_txid: genesis_block_coinbase_transaction.txid(),
            auth,
            client,
            database,
            path,
            first_inscription_height: options.chain.first_inscription_height(),
            genesis_block_coinbase_transaction,
            height_limit: options.height_limit,
            reorged: AtomicBool::new(false),
            rpc_url,
        })
    }

    pub fn get_unspent_outputs(&self) -> Result<BTreeMap<OutPoint, Amount>> {
        let mut utxos = BTreeMap::new();
        utxos.extend(
            self.client
                .list_unspent(None, None, None, None, None)?
                .into_iter()
                .map(|utxo| {
                    let outpoint = OutPoint::new(utxo.txid, utxo.vout);
                    let amount = utxo.amount;

                    (outpoint, amount)
                }),
        );

        #[derive(Deserialize)]
        pub(crate) struct JsonOutPoint {
            txid: Txid,
            vout: u32,
        }

        for JsonOutPoint { txid, vout } in self
            .client
            .call::<Vec<JsonOutPoint>>("listlockunspent", &[])?
        {
            utxos.insert(
                OutPoint { txid, vout },
                Amount::from_sat(
                    self.client.get_raw_transaction(&txid, None)?.output[vout as usize].value,
                ),
            );
        }
        let rtx = self.database.begin_read()?;
        let outpoint_to_value = rtx.open_table(OUTPOINT_TO_VALUE)?;
        for outpoint in utxos.keys() {
            if outpoint_to_value.get(&outpoint.store())?.is_none() {
                return Err(anyhow::anyhow!(
                    "output in Bitcoin Core wallet but not in ord index: {outpoint}"
                ));
            }
        }

        Ok(utxos)
    }

    pub fn has_sat_index(&self) -> Result<bool> {
        match self.begin_read()?.0.open_table(OUTPOINT_TO_SAT_RANGES) {
            Ok(_) => Ok(true),
            Err(redb::Error::TableDoesNotExist(_)) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    fn require_sat_index(&self, feature: &str) -> Result {
        if !self.has_sat_index()? {
            anyhow::bail!("{feature} requires index created with `--index-sats` flag")
        }

        Ok(())
    }

    pub(crate) fn info(&self) -> Result<Info> {
        let wtx = self.begin_write()?;

        let stats = wtx.stats()?;

        let info = {
            let statistic_to_count = wtx.open_table(STATISTIC_TO_COUNT)?;
            let sat_ranges = statistic_to_count
                .get(&Statistic::SatRanges.key())?
                .map(|x| x.value())
                .unwrap_or(0);
            let outputs_traversed = statistic_to_count
                .get(&Statistic::OutputsTraversed.key())?
                .map(|x| x.value())
                .unwrap_or(0);
            Info {
                index_path: self.path.clone(),
                blocks_indexed: wtx
                    .open_table(HEIGHT_TO_BLOCK_HASH)?
                    .range(0..)?
                    .rev()
                    .next()
                    .map(|(height, _hash)| height.value() + 1)
                    .unwrap_or(0),
                branch_pages: stats.branch_pages(),
                fragmented_bytes: stats.fragmented_bytes(),
                index_file_size: std::fs::metadata(&self.path)?.len(),
                leaf_pages: stats.leaf_pages(),
                metadata_bytes: stats.metadata_bytes(),
                sat_ranges,
                outputs_traversed,
                page_size: stats.page_size(),
                stored_bytes: stats.stored_bytes(),
                transactions: wtx
                    .open_table(WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP)?
                    .range(0..)?
                    .map(
                        |(starting_block_count, starting_timestamp)| TransactionInfo {
                            starting_block_count: starting_block_count.value(),
                            starting_timestamp: starting_timestamp.value(),
                        },
                    )
                    .collect(),
                tree_height: stats.tree_height(),
                utxos_indexed: wtx.open_table(OUTPOINT_TO_SAT_RANGES)?.len()?,
            }
        };

        Ok(info)
    }

    pub fn is_reorged(&self) -> bool {
        self.reorged.load(atomic::Ordering::Relaxed)
    }

    fn begin_read(&self) -> Result<rtx::Rtx> {
        Ok(rtx::Rtx(self.database.begin_read()?))
    }

    fn begin_write(&self) -> Result<WriteTransaction> {
        if cfg!(test) {
            let mut tx = self.database.begin_write()?;
            tx.set_durability(redb::Durability::None);
            Ok(tx)
        } else {
            Ok(self.database.begin_write()?)
        }
    }

    fn increment_statistic(wtx: &WriteTransaction, statistic: Statistic, n: u64) -> Result {
        let mut statistic_to_count = wtx.open_table(STATISTIC_TO_COUNT)?;
        let value = statistic_to_count
            .get(&(statistic.key()))?
            .map(|x| x.value())
            .unwrap_or(0)
            + n;
        statistic_to_count.insert(&statistic.key(), &value)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn statistic(&self, statistic: Statistic) -> u64 {
        self.database
            .begin_read()
            .unwrap()
            .open_table(STATISTIC_TO_COUNT)
            .unwrap()
            .get(&statistic.key())
            .unwrap()
            .map(|x| x.value())
            .unwrap_or(0)
    }

    pub(crate) fn height(&self) -> Result<Option<Height>> {
        self.begin_read()?.height()
    }

    pub fn block_count(&self) -> Result<u64> {
        self.begin_read()?.block_count()
    }

    pub fn blocks(&self, take: usize) -> Result<Vec<(u64, BlockHash)>> {
        let mut blocks = Vec::new();

        let rtx = self.begin_read()?;

        let block_count = rtx.block_count()?;

        let height_to_block_hash = rtx.0.open_table(HEIGHT_TO_BLOCK_HASH)?;

        for next in height_to_block_hash.range(0..block_count)?.rev().take(take) {
            blocks.push((next.0.value(), Entry::load(*next.1.value())));
        }

        Ok(blocks)
    }

    pub fn rare_sat_satpoints(&self) -> Result<Option<Vec<(Sat, SatPoint)>>> {
        if self.has_sat_index()? {
            let mut result = Vec::new();

            let rtx = self.database.begin_read()?;

            let sat_to_satpoint = rtx.open_table(SAT_TO_SATPOINT)?;

            for (sat, satpoint) in sat_to_satpoint.range(0..)? {
                result.push((Sat(sat.value()), Entry::load(*satpoint.value())));
            }

            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub fn rare_sat_satpoint(&self, sat: Sat) -> Result<Option<SatPoint>> {
        if self.has_sat_index()? {
            Ok(self
                .database
                .begin_read()?
                .open_table(SAT_TO_SATPOINT)?
                .get(&sat.n())?
                .map(|satpoint| Entry::load(*satpoint.value())))
        } else {
            Ok(None)
        }
    }

    pub fn block_header(&self, hash: BlockHash) -> Result<Option<BlockHeader>> {
        self.client.get_block_header(&hash).into_option()
    }

    pub fn block_header_info(&self, hash: BlockHash) -> Result<Option<GetBlockHeaderResult>> {
        self.client.get_block_header_info(&hash).into_option()
    }

    pub fn get_block_by_height(&self, height: u64) -> Result<Option<Block>> {
        Ok(self
            .client
            .get_block_hash(height)
            .into_option()?
            .map(|hash| self.client.get_block(&hash))
            .transpose()?)
    }

    pub fn get_block_by_hash(&self, hash: BlockHash) -> Result<Option<Block>> {
        self.client.get_block(&hash).into_option()
    }

    pub fn get_inscription_id_by_sat(&self, sat: Sat) -> Result<Option<InscriptionId>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(SAT_TO_INSCRIPTION_ID)?
            .get(&sat.n())?
            .map(|inscription_id| Entry::load(*inscription_id.value())))
    }

    pub fn get_inscription_id_by_inscription_number(
        &self,
        n: u64,
    ) -> Result<Option<InscriptionId>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?
            .get(&n)?
            .map(|id| Entry::load(*id.value())))
    }

    pub fn get_inscription_satpoint_by_id(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<SatPoint>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_ID_TO_SATPOINT)?
            .get(&inscription_id.store())?
            .map(|satpoint| Entry::load(*satpoint.value())))
    }

    pub fn get_inscriptions_on_output(&self, outpoint: OutPoint) -> Result<Vec<InscriptionId>> {
        Ok(Self::inscriptions_on_output(
            &self
                .database
                .begin_read()?
                .open_table(SATPOINT_TO_INSCRIPTION_ID)?,
            outpoint,
        )?
        .into_iter()
        .map(|(_satpoint, inscription_id)| inscription_id)
        .collect())
    }

    pub fn get_transaction(&self, txid: Txid) -> Result<Option<Transaction>> {
        if txid == self.genesis_block_coinbase_txid {
            Ok(Some(self.genesis_block_coinbase_transaction.clone()))
        } else {
            self.client.get_raw_transaction(&txid, None).into_option()
        }
    }

    pub fn get_transaction_blockhash(&self, txid: Txid) -> Result<Option<BlockHash>> {
        Ok(self
            .client
            .get_raw_transaction_info(&txid, None)
            .into_option()?
            .and_then(|info| {
                if info.in_active_chain.unwrap_or_default() {
                    info.blockhash
                } else {
                    None
                }
            }))
    }

    pub fn is_transaction_in_active_chain(&self, txid: Txid) -> Result<bool> {
        Ok(self
            .client
            .get_raw_transaction_info(&txid, None)
            .into_option()?
            .and_then(|info| info.in_active_chain)
            .unwrap_or(false))
    }

    pub fn find(&self, sat: u64) -> Result<Option<SatPoint>> {
        self.require_sat_index("find")?;

        let rtx = self.begin_read()?;

        if rtx.block_count()? <= Sat(sat).height().n() {
            return Ok(None);
        }

        let outpoint_to_sat_ranges = rtx.0.open_table(OUTPOINT_TO_SAT_RANGES)?;

        for (key, value) in outpoint_to_sat_ranges.range::<&[u8; 36]>(&[0; 36]..)? {
            let mut offset = 0;
            for chunk in value.value().chunks_exact(11) {
                let (start, end) = SatRange::load(chunk.try_into().unwrap());
                if start <= sat && sat < end {
                    return Ok(Some(SatPoint {
                        outpoint: Entry::load(*key.value()),
                        offset: offset + sat - start,
                    }));
                }
                offset += end - start;
            }
        }

        Ok(None)
    }

    fn list_inner(&self, outpoint: OutPointValue) -> Result<Option<Vec<u8>>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(OUTPOINT_TO_SAT_RANGES)?
            .get(&outpoint)?
            .map(|outpoint| outpoint.value().to_vec()))
    }

    pub(crate) fn list(&self, outpoint: OutPoint) -> Result<Option<List>> {
        self.require_sat_index("list")?;

        let array = outpoint.store();

        let sat_ranges = self.list_inner(array)?;

        match sat_ranges {
            Some(sat_ranges) => Ok(Some(List::Unspent(
                sat_ranges
                    .chunks_exact(11)
                    .map(|chunk| SatRange::load(chunk.try_into().unwrap()))
                    .collect(),
            ))),
            None => {
                if self.is_transaction_in_active_chain(outpoint.txid)? {
                    Ok(Some(List::Spent))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub(crate) fn blocktime(&self, height: Height) -> Result<Blocktime> {
        let height = height.n();

        match self.get_block_by_height(height)? {
            Some(block) => Ok(Blocktime::confirmed(block.header.time)),
            None => {
                let tx = self.database.begin_read()?;

                let current = tx
                    .open_table(HEIGHT_TO_BLOCK_HASH)?
                    .range(0..)?
                    .rev()
                    .next()
                    .map(|(height, _hash)| height)
                    .map(|x| x.value())
                    .unwrap_or(0);

                let expected_blocks = height.checked_sub(current).with_context(|| {
                    format!("current {current} height is greater than sat height {height}")
                })?;

                Ok(Blocktime::Expected(
                    Utc::now()
                        .round_subsecs(0)
                        .checked_add_signed(chrono::Duration::seconds(
                            10 * 60 * i64::try_from(expected_blocks)?,
                        ))
                        .ok_or_else(|| anyhow::anyhow!("block timestamp out of range"))?,
                ))
            }
        }
    }

    pub fn get_inscriptions(&self, n: Option<usize>) -> Result<BTreeMap<SatPoint, InscriptionId>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(SATPOINT_TO_INSCRIPTION_ID)?
            .range::<&[u8; 44]>(&[0; 44]..)?
            .map(|(satpoint, id)| (Entry::load(*satpoint.value()), Entry::load(*id.value())))
            .take(n.unwrap_or(usize::MAX))
            .collect())
    }

    pub fn get_homepage_inscriptions(&self) -> Result<Vec<InscriptionId>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?
            .iter()?
            .rev()
            .take(8)
            .map(|(_number, id)| Entry::load(*id.value()))
            .collect())
    }

    pub fn get_latest_inscriptions_with_prev_and_next(
        &self,
        n: usize,
        from: Option<u64>,
    ) -> Result<(Vec<InscriptionId>, Option<u64>, Option<u64>)> {
        let rtx = self.database.begin_read()?;

        let inscription_number_to_inscription_id =
            rtx.open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?;

        let latest = match inscription_number_to_inscription_id.iter()?.rev().next() {
            Some((number, _id)) => number.value(),
            None => return Ok(Default::default()),
        };

        let from = from.unwrap_or(latest);

        let prev = if let Some(prev) = from.checked_sub(n.try_into()?) {
            inscription_number_to_inscription_id
                .get(&prev)?
                .map(|_| prev)
        } else {
            None
        };

        let next = if from < latest {
            Some(
                from.checked_add(n.try_into()?)
                    .unwrap_or(latest)
                    .min(latest),
            )
        } else {
            None
        };

        let inscriptions = inscription_number_to_inscription_id
            .range(..=from)?
            .rev()
            .take(n)
            .map(|(_number, id)| Entry::load(*id.value()))
            .collect();

        Ok((inscriptions, prev, next))
    }

    pub fn get_feed_inscriptions(&self, n: usize) -> Result<Vec<(u64, InscriptionId)>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?
            .iter()?
            .rev()
            .take(n)
            .map(|(number, id)| (number.value(), Entry::load(*id.value())))
            .collect())
    }

    pub fn get_inscription_entry(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<InscriptionEntry>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_ID_TO_INSCRIPTION_ENTRY)?
            .get(&inscription_id.store())?
            .map(|value| InscriptionEntry::load(value.value())))
    }

    #[cfg(test)]
    fn assert_inscription_location(
        &self,
        inscription_id: InscriptionId,
        satpoint: SatPoint,
        sat: u64,
    ) {
        let rtx = self.database.begin_read().unwrap();

        let satpoint_to_inscription_id = rtx.open_table(SATPOINT_TO_INSCRIPTION_ID).unwrap();

        let inscription_id_to_satpoint = rtx.open_table(INSCRIPTION_ID_TO_SATPOINT).unwrap();

        assert_eq!(
            satpoint_to_inscription_id.len().unwrap(),
            inscription_id_to_satpoint.len().unwrap(),
        );

        assert_eq!(
            SatPoint::load(
                *inscription_id_to_satpoint
                    .get(&inscription_id.store())
                    .unwrap()
                    .unwrap()
                    .value()
            ),
            satpoint,
        );

        assert_eq!(
            InscriptionId::load(
                *satpoint_to_inscription_id
                    .get(&satpoint.store())
                    .unwrap()
                    .unwrap()
                    .value()
            ),
            inscription_id,
        );

        if self.has_sat_index().unwrap() {
            assert_eq!(
                InscriptionId::load(
                    *rtx.open_table(SAT_TO_INSCRIPTION_ID)
                        .unwrap()
                        .get(&sat)
                        .unwrap()
                        .unwrap()
                        .value()
                ),
                inscription_id,
            );

            assert_eq!(
                SatPoint::load(
                    *rtx.open_table(SAT_TO_SATPOINT)
                        .unwrap()
                        .get(&sat)
                        .unwrap()
                        .unwrap()
                        .value()
                ),
                satpoint,
            );
        }
    }

    fn inscriptions_on_output<'a: 'tx, 'tx>(
        satpoint_to_id: &'a impl ReadableTable<&'static SatPointValue, &'static InscriptionIdValue>,
        outpoint: OutPoint,
    ) -> Result<impl Iterator<Item = (SatPoint, InscriptionId)> + 'tx> {
        let start = SatPoint {
            outpoint,
            offset: 0,
        }
        .store();

        let end = SatPoint {
            outpoint,
            offset: u64::MAX,
        }
        .store();

        Ok(satpoint_to_id
            .range::<&[u8; 44]>(&start..=&end)?
            .map(|(satpoint, id)| (Entry::load(*satpoint.value()), Entry::load(*id.value()))))
    }
}
