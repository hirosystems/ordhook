use crate::{
    hord::ord::{height::Height, sat::Sat, sat_point::SatPoint},
    utils::Context,
};
use anyhow::Context as Ctx;
use bitcoincore_rpc::bitcoin::{OutPoint, Transaction, Txid};
use hiro_system_kit::slog;

use std::{
    collections::VecDeque,
    time::{Instant, SystemTime},
};

use super::Result;
use {self::inscription_updater::InscriptionUpdater, super::*, std::sync::mpsc};

use {
    super::fetcher::Fetcher,
    futures::future::try_join_all,
    tokio::sync::mpsc::{error::TryRecvError, Receiver, Sender},
};

mod inscription_updater;

#[derive(Debug)]
pub struct BlockData {
    pub header: BlockHeader,
    pub txdata: Vec<(Transaction, Txid)>,
}

pub struct OrdinalIndexUpdater {
    pub range_cache: HashMap<OutPointValue, Vec<u8>>,
    pub height: u64,
    pub sat_ranges_since_flush: u64,
    pub outputs_cached: u64,
    pub outputs_inserted_since_flush: u64,
    pub outputs_traversed: u64,
}

impl OrdinalIndexUpdater {
    pub async fn update(
        index: &OrdinalIndex,
        block_opt: Option<BlockData>,
        ctx: &Context,
    ) -> Result {
        let wtx = index.begin_write()?;

        let height = wtx
            .open_table(HEIGHT_TO_BLOCK_HASH)?
            .range(0..)?
            .rev()
            .next()
            .map(|(height, _hash)| height.value() + 1)
            .unwrap_or(0);

        wtx.open_table(WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP)?
            .insert(
                &height,
                &SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .unwrap_or(0),
            )?;

        let mut updater = Self {
            range_cache: HashMap::new(),
            height,
            sat_ranges_since_flush: 0,
            outputs_cached: 0,
            outputs_inserted_since_flush: 0,
            outputs_traversed: 0,
        };

        if let Some(block) = block_opt {
            updater
                .update_index_with_block(index, wtx, block, ctx)
                .await
        } else {
            updater.update_index(index, wtx, ctx).await
        }
    }

    async fn update_index_with_block<'index>(
        &mut self,
        index: &'index OrdinalIndex,
        mut wtx: WriteTransaction<'index>,
        block: BlockData,
        ctx: &Context,
    ) -> Result {
        let (mut outpoint_sender, mut value_receiver) = Self::spawn_fetcher(index)?;

        let mut value_cache = HashMap::new();
        self.index_block(
            index,
            &mut outpoint_sender,
            &mut value_receiver,
            &mut wtx,
            block,
            &mut value_cache,
            ctx,
        )
        .await?;

        self.commit(wtx, value_cache, ctx)?;
        Ok(())
    }

    async fn update_index<'index>(
        &mut self,
        index: &'index OrdinalIndex,
        mut wtx: WriteTransaction<'index>,
        ctx: &Context,
    ) -> Result {
        let starting_height = index.client.get_block_count()? + 1;

        let rx = Self::fetch_blocks_from(index, self.height, ctx)?;

        let (mut outpoint_sender, mut value_receiver) = Self::spawn_fetcher(index)?;

        let mut uncommitted = 0;
        let mut value_cache = HashMap::new();
        loop {
            let block = match rx.recv() {
                Ok(block) => block,
                Err(mpsc::RecvError) => break,
            };

            self.index_block(
                index,
                &mut outpoint_sender,
                &mut value_receiver,
                &mut wtx,
                block,
                &mut value_cache,
                ctx,
            )
            .await?;

            uncommitted += 1;

            if uncommitted == 5000 {
                self.commit(wtx, value_cache, ctx)?;
                value_cache = HashMap::new();
                uncommitted = 0;
                wtx = index.begin_write()?;
                let height = wtx
                    .open_table(HEIGHT_TO_BLOCK_HASH)?
                    .range(0..)?
                    .rev()
                    .next()
                    .map(|(height, _hash)| height.value() + 1)
                    .unwrap_or(0);
                if height != self.height {
                    // another update has run between committing and beginning the new
                    // write transaction
                    break;
                }
                wtx.open_table(WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP)?
                    .insert(
                        &self.height,
                        &SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|duration| duration.as_millis())
                            .unwrap_or(0),
                    )?;
            }
        }

        if uncommitted > 0 {
            self.commit(wtx, value_cache, ctx)?;
        }

        Ok(())
    }

    fn fetch_blocks_from(
        index: &OrdinalIndex,
        mut height: u64,
        ctx: &Context,
    ) -> Result<mpsc::Receiver<BlockData>> {
        let (tx, rx) = mpsc::sync_channel(32);

        let height_limit = index.height_limit;

        let client = Client::new(&index.rpc_url, index.auth.clone())
            .context("failed to connect to RPC URL")?;

        let first_inscription_height = index.first_inscription_height;

        let ctx_ = ctx.clone();
        std::thread::spawn(move || loop {
            if let Some(height_limit) = height_limit {
                if height >= height_limit {
                    break;
                }
            }

            match Self::get_block_with_retries(&client, height, &ctx_) {
                Ok(Some(block)) => {
                    if let Err(err) = tx.send(block.into()) {
                        ctx_.try_log(|logger| {
                            slog::error!(logger, "Block receiver disconnected: {err}",)
                        });
                        break;
                    }
                    height += 1;
                }
                Ok(None) => {
                    ctx_.try_log(|logger| slog::error!(logger, "failed to fetch block {height}",));
                    break;
                }
                Err(err) => {
                    ctx_.try_log(|logger| {
                        slog::error!(logger, "failed to fetch block {height}: {err}",)
                    });
                    break;
                }
            }
        });

        Ok(rx)
    }

    fn get_block_with_retries(
        _client: &Client,
        _height: u64,
        _ctx: &Context,
    ) -> Result<Option<BlockData>> {
        return Ok(None);
    }

    fn spawn_fetcher(index: &OrdinalIndex) -> Result<(Sender<OutPoint>, Receiver<u64>)> {
        let fetcher = Fetcher::new(&index.rpc_url, index.auth.clone())?;

        // Not sure if any block has more than 20k inputs, but none so far after first inscription block
        const CHANNEL_BUFFER_SIZE: usize = 20_000;
        let (outpoint_sender, mut outpoint_receiver) =
            tokio::sync::mpsc::channel::<OutPoint>(CHANNEL_BUFFER_SIZE);
        let (value_sender, value_receiver) = tokio::sync::mpsc::channel::<u64>(CHANNEL_BUFFER_SIZE);

        // Batch 2048 missing inputs at a time. Arbitrarily chosen for now, maybe higher or lower can be faster?
        // Did rudimentary benchmarks with 1024 and 4096 and time was roughly the same.
        const BATCH_SIZE: usize = 2048;
        // Default rpcworkqueue in bitcoind is 16, meaning more than 16 concurrent requests will be rejected.
        // Since we are already requesting blocks on a separate thread, and we don't want to break if anything
        // else runs a request, we keep this to 12.
        const PARALLEL_REQUESTS: usize = 12;

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
          loop {
            let Some(outpoint) = outpoint_receiver.recv().await else {
            //   log::debug!("Outpoint channel closed");
              return;
            };
            // There's no try_iter on tokio::sync::mpsc::Receiver like std::sync::mpsc::Receiver.
            // So we just loop until BATCH_SIZE doing try_recv until it returns None.
            let mut outpoints = vec![outpoint];
            for _ in 0..BATCH_SIZE-1 {
              let Ok(outpoint) = outpoint_receiver.try_recv() else {
                break;
              };
              outpoints.push(outpoint);
            }
            // Break outpoints into chunks for parallel requests
            let chunk_size = (outpoints.len() / PARALLEL_REQUESTS) + 1;
            let mut futs = Vec::with_capacity(PARALLEL_REQUESTS);
            for chunk in outpoints.chunks(chunk_size) {
              let txids = chunk.iter().map(|outpoint| outpoint.txid).collect();
              let fut = fetcher.get_transactions(txids);
              futs.push(fut);
            }
            let txs = match try_join_all(futs).await {
              Ok(txs) => txs,
              Err(_e) => {
                // log::error!("Couldn't receive txs {e}");
                return;
              }
            };
            // Send all tx output values back in order
            for (i, tx) in txs.iter().flatten().enumerate() {
              let Ok(_) = value_sender.send(tx.output[usize::try_from(outpoints[i].vout).unwrap()].value).await else {
                // log::error!("Value channel closed unexpectedly");
                return;
              };
            }
          }
        })
        });

        Ok((outpoint_sender, value_receiver))
    }

    async fn index_block(
        &mut self,
        index: &OrdinalIndex,
        _outpoint_sender: &mut Sender<OutPoint>,
        value_receiver: &mut Receiver<u64>,
        wtx: &mut WriteTransaction<'_>,
        block: BlockData,
        value_cache: &mut HashMap<OutPoint, u64>,
        ctx: &Context,
    ) -> Result<()> {
        // If value_receiver still has values something went wrong with the last block
        // Could be an assert, shouldn't recover from this and commit the last block
        let Err(TryRecvError::Empty) = value_receiver.try_recv() else {
        return Err(anyhow::anyhow!("Previous block did not consume all input values")); 
      };

        let mut outpoint_to_value = wtx.open_table(OUTPOINT_TO_VALUE)?;

        let mut height_to_block_hash = wtx.open_table(HEIGHT_TO_BLOCK_HASH)?;

        let _start = Instant::now();
        let mut sat_ranges_written = 0;
        let mut outputs_in_block = 0;

        let time = timestamp(block.header.time);

        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Block {} at {} with {} transactions…",
                self.height,
                time,
                block.txdata.len()
            )
        });

        if let Some(prev_height) = self.height.checked_sub(1) {
            let prev_hash = height_to_block_hash.get(&prev_height)?.unwrap();

            if prev_hash.value() != block.header.prev_blockhash.as_ref() {
                index.reorged.store(true, atomic::Ordering::Relaxed);
                return Err(anyhow::anyhow!("reorg detected at or before {prev_height}"));
            }
        }

        let mut inscription_id_to_inscription_entry =
            wtx.open_table(INSCRIPTION_ID_TO_INSCRIPTION_ENTRY)?;
        let mut inscription_id_to_satpoint = wtx.open_table(INSCRIPTION_ID_TO_SATPOINT)?;
        let mut inscription_number_to_inscription_id =
            wtx.open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?;
        let mut sat_to_inscription_id = wtx.open_table(SAT_TO_INSCRIPTION_ID)?;
        let mut satpoint_to_inscription_id = wtx.open_table(SATPOINT_TO_INSCRIPTION_ID)?;
        let mut statistic_to_count = wtx.open_table(STATISTIC_TO_COUNT)?;

        let mut lost_sats = statistic_to_count
            .get(&Statistic::LostSats.key())?
            .map(|lost_sats| lost_sats.value())
            .unwrap_or(0);

        let mut inscription_updater = InscriptionUpdater::new(
            self.height,
            &mut inscription_id_to_satpoint,
            value_receiver,
            &mut inscription_id_to_inscription_entry,
            lost_sats,
            &mut inscription_number_to_inscription_id,
            &mut outpoint_to_value,
            &mut sat_to_inscription_id,
            &mut satpoint_to_inscription_id,
            block.header.time,
            value_cache,
        )?;

        let mut sat_to_satpoint = wtx.open_table(SAT_TO_SATPOINT)?;
        let mut outpoint_to_sat_ranges = wtx.open_table(OUTPOINT_TO_SAT_RANGES)?;

        let mut coinbase_inputs = VecDeque::new();

        let h = Height(self.height);
        if h.subsidy() > 0 {
            let start = h.starting_sat();
            coinbase_inputs.push_front((start.n(), (start + h.subsidy()).n()));
            self.sat_ranges_since_flush += 1;
        }

        for (_tx_offset, (tx, txid)) in block.txdata.iter().enumerate().skip(1) {
            let mut input_sat_ranges = VecDeque::new();

            for input in &tx.input {
                let key = input.previous_output.store();

                let sat_ranges = match self.range_cache.remove(&key) {
                    Some(sat_ranges) => {
                        self.outputs_cached += 1;
                        sat_ranges
                    }
                    None => outpoint_to_sat_ranges
                        .remove(&key)?
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Could not find outpoint {} in index",
                                input.previous_output
                            )
                        })?
                        .value()
                        .to_vec(),
                };

                for chunk in sat_ranges.chunks_exact(11) {
                    input_sat_ranges.push_back(SatRange::load(chunk.try_into().unwrap()));
                }
            }

            self.index_transaction_sats(
                tx,
                *txid,
                &mut sat_to_satpoint,
                &mut input_sat_ranges,
                &mut sat_ranges_written,
                &mut outputs_in_block,
                &mut inscription_updater,
                &ctx,
            )
            .await?;

            coinbase_inputs.extend(input_sat_ranges);
        }

        if let Some((tx, txid)) = block.txdata.get(0) {
            self.index_transaction_sats(
                tx,
                *txid,
                &mut sat_to_satpoint,
                &mut coinbase_inputs,
                &mut sat_ranges_written,
                &mut outputs_in_block,
                &mut inscription_updater,
                &ctx,
            )
            .await?;
        }

        if !coinbase_inputs.is_empty() {
            let mut lost_sat_ranges = outpoint_to_sat_ranges
                .remove(&OutPoint::null().store())?
                .map(|ranges| ranges.value().to_vec())
                .unwrap_or_default();

            for (start, end) in coinbase_inputs {
                if !Sat(start).is_common() {
                    sat_to_satpoint.insert(
                        &start,
                        &SatPoint {
                            outpoint: OutPoint::null(),
                            offset: lost_sats,
                        }
                        .store(),
                    )?;
                }

                lost_sat_ranges.extend_from_slice(&(start, end).store());

                lost_sats += end - start;
            }

            outpoint_to_sat_ranges.insert(&OutPoint::null().store(), lost_sat_ranges.as_slice())?;
        }

        statistic_to_count.insert(&Statistic::LostSats.key(), &lost_sats)?;

        height_to_block_hash.insert(&self.height, &block.header.block_hash().store())?;

        self.height += 1;
        self.outputs_traversed += outputs_in_block;

        Ok(())
    }

    async fn index_transaction_sats(
        &mut self,
        tx: &Transaction,
        txid: Txid,
        sat_to_satpoint: &mut Table<'_, '_, u64, &SatPointValue>,
        input_sat_ranges: &mut VecDeque<(u64, u64)>,
        sat_ranges_written: &mut u64,
        outputs_traversed: &mut u64,
        inscription_updater: &mut InscriptionUpdater<'_, '_, '_>,
        ctx: &Context,
    ) -> Result {
        inscription_updater
            .index_transaction_inscriptions(tx, txid, Some(input_sat_ranges), ctx)
            .await?;

        for (vout, output) in tx.output.iter().enumerate() {
            let outpoint = OutPoint {
                vout: vout.try_into().unwrap(),
                txid,
            };
            let mut sats = Vec::new();

            let mut remaining = output.value;
            while remaining > 0 {
                let range = input_sat_ranges.pop_front().ok_or_else(|| {
                    anyhow::anyhow!("insufficient inputs for transaction outputs")
                })?;

                if !Sat(range.0).is_common() {
                    sat_to_satpoint.insert(
                        &range.0,
                        &SatPoint {
                            outpoint,
                            offset: output.value - remaining,
                        }
                        .store(),
                    )?;
                }

                let count = range.1 - range.0;

                let assigned = if count > remaining {
                    self.sat_ranges_since_flush += 1;
                    let middle = range.0 + remaining;
                    input_sat_ranges.push_front((middle, range.1));
                    (range.0, middle)
                } else {
                    range
                };

                sats.extend_from_slice(&assigned.store());

                remaining -= assigned.1 - assigned.0;

                *sat_ranges_written += 1;
            }

            *outputs_traversed += 1;

            self.range_cache.insert(outpoint.store(), sats);
            self.outputs_inserted_since_flush += 1;
        }

        Ok(())
    }

    fn commit(
        &mut self,
        wtx: WriteTransaction,
        value_cache: HashMap<OutPoint, u64>,
        ctx: &Context,
    ) -> Result {
        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Committing at block height {}, {} outputs traversed, {} in map, {} cached",
                self.height,
                self.outputs_traversed,
                self.range_cache.len(),
                self.outputs_cached
            )
        });

        ctx.try_log(|logger| {
            slog::info!(
                logger,
                "Flushing {} entries ({:.1}% resulting from {} insertions) from memory to database",
                self.range_cache.len(),
                self.range_cache.len() as f64 / self.outputs_inserted_since_flush as f64 * 100.,
                self.outputs_inserted_since_flush,
            )
        });

        {
            let mut outpoint_to_sat_ranges = wtx.open_table(OUTPOINT_TO_SAT_RANGES)?;

            for (outpoint, sat_range) in self.range_cache.drain() {
                outpoint_to_sat_ranges.insert(&outpoint, sat_range.as_slice())?;
            }

            self.outputs_inserted_since_flush = 0;
        }
        {
            let mut outpoint_to_value = wtx.open_table(OUTPOINT_TO_VALUE)?;

            for (outpoint, value) in value_cache {
                outpoint_to_value.insert(&outpoint.store(), &value)?;
            }
        }

        OrdinalIndex::increment_statistic(
            &wtx,
            Statistic::OutputsTraversed,
            self.outputs_traversed,
        )?;
        self.outputs_traversed = 0;
        OrdinalIndex::increment_statistic(&wtx, Statistic::SatRanges, self.sat_ranges_since_flush)?;
        self.sat_ranges_since_flush = 0;
        OrdinalIndex::increment_statistic(&wtx, Statistic::Commits, 1)?;

        wtx.commit()?;
        Ok(())
    }
}
