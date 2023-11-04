use std::{
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{types::BitcoinBlockData, utils::Context};
use crossbeam_channel::{Sender, TryRecvError};

use crate::{
    config::Config,
    core::{
        pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
        protocol::{
            inscription_sequencing::consolidate_block_with_pre_computed_ordinals_data,
            inscription_tracking::augment_block_with_ordinals_transfer_data,
        },
    },
    db::{
        insert_new_inscriptions_from_block_in_locations, open_readwrite_ordhook_db_conn,
        remove_entries_from_locations_at_block_height,
    },
};

pub fn start_transfers_recomputing_processor(
    config: &Config,
    ctx: &Context,
    post_processor: Option<Sender<BitcoinBlockData>>,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Inscription indexing runloop")
        .spawn(move || {
            let mut inscriptions_db_conn_rw =
                open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let mut empty_cycles = 0;

            loop {
                let mut blocks = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(_, blocks)) => {
                        empty_cycles = 0;
                        blocks
                    }
                    Ok(PostProcessorCommand::Terminate) => {
                        let _ = events_tx.send(PostProcessorEvent::Terminated);
                        break;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            empty_cycles += 1;
                            if empty_cycles == 10 {
                                ctx.try_log(|logger| {
                                    warn!(logger, "Block processor reached expiration")
                                });
                                let _ = events_tx.send(PostProcessorEvent::Expired);
                                break;
                            }
                            sleep(Duration::from_secs(1));
                            continue;
                        }
                        _ => {
                            break;
                        }
                    },
                };

                ctx.try_log(|logger| info!(logger, "Processing {} blocks", blocks.len()));
                let inscriptions_db_tx = inscriptions_db_conn_rw.transaction().unwrap();

                for block in blocks.iter_mut() {
                    consolidate_block_with_pre_computed_ordinals_data(
                        block,
                        &inscriptions_db_tx,
                        false,
                        &ctx,
                    );

                    remove_entries_from_locations_at_block_height(
                        &block.block_identifier.index,
                        &inscriptions_db_tx,
                        &ctx,
                    );

                    insert_new_inscriptions_from_block_in_locations(
                        block,
                        &inscriptions_db_tx,
                        &ctx,
                    );

                    augment_block_with_ordinals_transfer_data(
                        block,
                        &inscriptions_db_tx,
                        true,
                        &ctx,
                    );

                    if let Some(ref post_processor) = post_processor {
                        let _ = post_processor.send(block.clone());
                    }
                }
                let _ = inscriptions_db_tx.commit();
            }
        })
        .expect("unable to spawn thread");

    PostProcessorController {
        commands_tx,
        events_rx,
        thread_handle: handle,
    }
}
