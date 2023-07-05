use std::sync::mpsc::{channel, Sender};

use super::HordConfig;

pub struct Batch {
    pub jobs: Vec<Job>,
    pub jobs_count: u32,
}

pub struct Job {
    pub inputs: JobInputs,
    pub result: Result<JobResult, String>,
}

pub struct JobInputs {}

pub struct JobResult {}

pub fn start_ordinals_number_processor(config: &HordConfig) -> Sender<Batch> {
    // let mut batches = HashMap::new();

    let (tx, rx) = channel();
    // let (inner_tx, inner_rx) = channel();

    hiro_system_kit::thread_named("Batch receiver")
        .spawn(move || {})
        .expect("unable to spawn thread");

    tx
}
