use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use bitcoind::{try_debug, try_info, try_warn, utils::Context};
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use prometheus::{
    core::{AtomicF64, AtomicU64, GenericCounter, GenericGauge},
    Encoder, Histogram, HistogramOpts, Registry, TextEncoder,
};
use tokio::time::{sleep, Duration};

type UInt64Gauge = GenericGauge<AtomicU64>;
type F64Gauge = GenericGauge<AtomicF64>;
type U64Counter = GenericCounter<AtomicU64>;

#[derive(Debug, Clone)]
pub struct PrometheusMonitoring {
    pub last_indexed_block_height: UInt64Gauge,
    pub last_indexed_rune_number: UInt64Gauge,

    // Performance metrics
    pub block_processing_time: Histogram,
    pub rune_parsing_time: Histogram,
    pub rune_computation_time: Histogram,
    pub rune_db_write_time: Histogram,

    // Volumetric metrics
    pub runes_per_block: Histogram,

    // Runes specific metrics
    pub runes_etching_operations_per_block: UInt64Gauge,
    pub runes_edict_operations_per_block: UInt64Gauge,
    pub runes_mint_operations_per_block: UInt64Gauge,
    pub runes_cenotaph_operations_per_block: UInt64Gauge,
    pub runes_cenotaph_etching_operations_per_block: UInt64Gauge,
    pub runes_cenotaph_mint_operations_per_block: UInt64Gauge,
    pub runes_etching_inputs_checked_per_block: UInt64Gauge,

    // Registry
    pub registry: Registry,
}

impl Default for PrometheusMonitoring {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusMonitoring {
    pub fn new() -> PrometheusMonitoring {
        let registry = Registry::new();

        let last_indexed_block_height = Self::create_and_register_uint64_gauge(
            &registry,
            "last_indexed_block_height",
            "Height of the last indexed block",
        );
        let last_indexed_rune_number = Self::create_and_register_uint64_gauge(
            &registry,
            "last_indexed_rune_number",
            "Number of the last indexed Rune",
        );

        // Performance metrics
        let block_processing_time = Self::create_and_register_histogram(
            &registry,
            "runes_block_processing_time",
            "Time taken to process a block in milliseconds",
            vec![10_000.0, 20_000.0, 30_000.0, 60_000.0, 120_000.0, 300_000.0],
        );
        let rune_parsing_time = Self::create_and_register_histogram(
            &registry,
            "rune_parsing_time",
            "Time taken to parse Runes operations in milliseconds",
            vec![
                1_000.0, 5_000.0, 10_000.0, 20_000.0, 60_000.0, 120_000.0, 300_000.0,
            ],
        );
        let rune_computation_time = Self::create_and_register_histogram(
            &registry,
            "rune_computation_time",
            "Time taken to compute Runes data in milliseconds",
            vec![
                1_000.0, 5_000.0, 10_000.0, 20_000.0, 60_000.0, 120_000.0, 300_000.0,
            ],
        );
        let rune_db_write_time = Self::create_and_register_histogram(
            &registry,
            "rune_db_write_time",
            "Time taken to write Runes data to database in milliseconds",
            vec![
                1_000.0, 5_000.0, 10_000.0, 20_000.0, 60_000.0, 120_000.0, 300_000.0,
            ],
        );

        // Volumetric metrics
        let runes_per_block = Self::create_and_register_histogram(
            &registry,
            "runes_per_block",
            "Number of Runes per block",
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
        );

        // Runes specific metrics per block
        let runes_etching_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "runes_etching_operations_per_block",
            "Number of Runes etchings processed per block",
        );
        let runes_edict_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "runes_edict_operations_per_block",
            "Number of Runes edicts processed per block",
        );
        let runes_mint_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "runes_mint_operations_per_block",
            "Number of Runes mints processed per block",
        );
        let runes_cenotaph_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "runes_cenotaph_operations_per_block",
            "Number of cenotaph Runes processed per block",
        );
        let runes_cenotaph_etching_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "runes_cenotaph_etching_operations_per_block",
            "Number of cenotaph Runes etchings processed per block",
        );
        let runes_cenotaph_mint_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "runes_cenotaph_mint_operations_per_block",
            "Number of cenotaph Runes mints processed per block",
        );
        let runes_etching_inputs_checked_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "runes_etching_inputs_checked_per_block",
            "Number of inputs checked for rune commitment per block",
        );

        PrometheusMonitoring {
            last_indexed_block_height,
            last_indexed_rune_number,
            block_processing_time,
            rune_parsing_time,
            rune_computation_time,
            rune_db_write_time,
            runes_per_block,
            runes_etching_operations_per_block,
            runes_edict_operations_per_block,
            runes_mint_operations_per_block,
            runes_cenotaph_operations_per_block,
            runes_cenotaph_etching_operations_per_block,
            runes_cenotaph_mint_operations_per_block,
            runes_etching_inputs_checked_per_block,
            registry,
        }
    }

    pub fn create_and_register_uint64_gauge(
        registry: &Registry,
        name: &str,
        help: &str,
    ) -> UInt64Gauge {
        let g = UInt64Gauge::new(name, help).unwrap();
        registry.register(Box::new(g.clone())).unwrap();
        g
    }

    pub fn create_and_register_f64_gauge(registry: &Registry, name: &str, help: &str) -> F64Gauge {
        let g = F64Gauge::new(name, help).unwrap();
        registry.register(Box::new(g.clone())).unwrap();
        g
    }

    pub fn create_and_register_counter(registry: &Registry, name: &str, help: &str) -> U64Counter {
        let c = U64Counter::new(name, help).unwrap();
        registry.register(Box::new(c.clone())).unwrap();
        c
    }

    pub fn create_and_register_histogram(
        registry: &Registry,
        name: &str,
        help: &str,
        buckets: Vec<f64>,
    ) -> Histogram {
        let h = Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets)).unwrap();
        registry.register(Box::new(h.clone())).unwrap();
        h
    }

    pub async fn initialize(&self, max_rune_number: u64, block_height: u64) -> Result<(), String> {
        self.metrics_block_indexed(block_height);
        self.metrics_rune_indexed(max_rune_number);

        // Reset per-block metrics to 0
        self.metrics_record_runes_etching_per_block(0);
        self.metrics_record_runes_mint_per_block(0);
        self.metrics_record_runes_edict_per_block(0);
        self.metrics_record_runes_cenotaph_per_block(0);
        self.metrics_record_runes_cenotaph_etching_per_block(0);
        self.metrics_record_runes_cenotaph_mint_per_block(0);

        Ok(())
    }

    pub fn metrics_block_indexed(&self, block_height: u64) {
        let highest_appended = self.last_indexed_block_height.get();
        if block_height > highest_appended {
            self.last_indexed_block_height.set(block_height);
        }
    }

    pub fn metrics_rune_indexed(&self, rune_number: u64) {
        let highest_appended = self.last_indexed_rune_number.get();
        if rune_number > highest_appended {
            self.last_indexed_rune_number.set(rune_number);
        }
    }

    // Performance metrics methods
    pub fn metrics_record_block_processing_time(&self, process_time: f64) {
        self.block_processing_time.observe(process_time);
    }

    pub fn metrics_record_rune_parsing_time(&self, ms: f64) {
        self.rune_parsing_time.observe(ms);
    }

    pub fn metrics_record_rune_computation_time(&self, ms: f64) {
        self.rune_computation_time.observe(ms);
    }

    pub fn metrics_record_rune_db_write_time(&self, ms: f64) {
        self.rune_db_write_time.observe(ms);
    }

    // Volumetric metrics methods
    pub fn metrics_record_runes_per_block(&self, count: u64) {
        self.runes_per_block.observe(count as f64);
    }

    // Runes specific metrics methods per block
    pub fn metrics_record_runes_etching_per_block(&self, etching_count: u64) {
        self.runes_etching_operations_per_block.set(etching_count);
    }

    pub fn metrics_record_runes_edict_per_block(&self, edict_count: u64) {
        self.runes_edict_operations_per_block.set(edict_count);
    }

    pub fn metrics_record_runes_mint_per_block(&self, mint_count: u64) {
        self.runes_mint_operations_per_block.set(mint_count);
    }

    pub fn metrics_record_runes_cenotaph_per_block(&self, cenotaph_count: u64) {
        self.runes_cenotaph_operations_per_block.set(cenotaph_count);
    }

    pub fn metrics_record_runes_cenotaph_etching_per_block(&self, cenotaph_etching_count: u64) {
        self.runes_cenotaph_etching_operations_per_block
            .set(cenotaph_etching_count);
    }

    pub fn metrics_record_runes_cenotaph_mint_per_block(&self, cenotaph_mint_count: u64) {
        self.runes_cenotaph_mint_operations_per_block
            .set(cenotaph_mint_count);
    }
    pub fn metrics_record_runes_etching_inputs_checked_per_block(&self, inputs_count: u64) {
        self.runes_etching_inputs_checked_per_block
            .set(inputs_count);
    }
}

async fn serve_req(
    req: Request<Body>,
    registry: Registry,
    ctx: Context,
) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            try_debug!(ctx, "Prometheus monitoring: responding to metrics request");

            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            let mut buffer = vec![];
            let response = match encoder.encode(&metric_families, &mut buffer) {
                Ok(_) => Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, encoder.format_type())
                    .body(Body::from(buffer))
                    .unwrap(),
                Err(e) => {
                    try_debug!(
                        ctx,
                        "Prometheus monitoring: failed to encode metrics: {}",
                        e.to_string()
                    );
                    Response::builder().status(500).body(Body::empty()).unwrap()
                }
            };
            Ok(response)
        }
        (_, _) => {
            try_debug!(
                ctx,
                "Prometheus monitoring: received request with invalid method/route: {}/{}",
                req.method(),
                req.uri().path()
            );
            let response = Response::builder().status(404).body(Body::empty()).unwrap();
            Ok(response)
        }
    }
}

pub async fn start_serving_prometheus_metrics(
    port: u16,
    registry: Registry,
    ctx: Context,
    abort_signal: Arc<AtomicBool>,
) {
    let addr = ([0, 0, 0, 0], port).into();
    let ctx_clone = ctx.clone();
    let make_svc = make_service_fn(|_| {
        let registry = registry.clone();
        let ctx_clone = ctx_clone.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |r| {
                serve_req(r, registry.clone(), ctx_clone.clone())
            }))
        }
    });
    let shutdown_future = async move {
        loop {
            if abort_signal.load(Ordering::SeqCst) {
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }
    };
    let serve_future = Server::bind(&addr)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown_future);
    try_info!(ctx, "Prometheus monitoring: listening on port {}", port);
    if let Err(err) = serve_future.await {
        try_warn!(ctx, "Prometheus monitoring: server error: {}", err);
    } else {
        try_info!(ctx, "Prometheus monitoring: shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use prometheus::core::Collector;

    use super::*;

    fn verify_metric_exists(metrics: &[prometheus::proto::MetricFamily], name: &str) -> bool {
        metrics.iter().any(|m| m.get_name() == name)
    }

    #[test]
    fn test_block_processing_time() {
        let monitoring = PrometheusMonitoring::new();
        let start_time = Instant::now();

        // Simulate some processing time
        std::thread::sleep(std::time::Duration::from_millis(10_000));

        monitoring.metrics_record_block_processing_time(start_time.elapsed().as_millis() as f64);

        // Get the histogram values directly
        let mut mfs = monitoring.block_processing_time.collect();
        assert_eq!(mfs.len(), 1);

        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let proto_histogram = m.get_histogram();

        // Verify we recorded exactly 1 observation
        assert_eq!(
            proto_histogram.get_sample_count(),
            1,
            "Should have recorded 1 observation"
        );

        // Verify the observation value is within reasonable bounds
        let actual_time = proto_histogram.get_sample_sum();
        assert!(
            actual_time >= 9_500.0 && actual_time <= 10_500.0,
            "Observation should be within reasonable bounds (9.5-10.5 seconds)"
        );
    }

    #[test]
    fn test_rune_parsing_time() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different parsing times
        monitoring.metrics_record_rune_parsing_time(50.0);
        monitoring.metrics_record_rune_parsing_time(150.0);

        // Get the histogram values directly
        let mut mfs = monitoring.rune_parsing_time.collect();
        assert_eq!(mfs.len(), 1);

        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let proto_histogram = m.get_histogram();

        // Verify we recorded exactly 2 observations
        assert_eq!(
            proto_histogram.get_sample_count(),
            2,
            "Should have recorded 2 observations"
        );

        // Verify the sum of our observations (50 + 150 = 200)
        assert_eq!(
            proto_histogram.get_sample_sum(),
            200.0,
            "Sum of observations should be 200.0"
        );
    }

    #[test]
    fn test_runes_computation_time() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different computation times
        monitoring.metrics_record_rune_computation_time(75.0);
        monitoring.metrics_record_rune_computation_time(200.0);

        // Get the histogram values directly
        let mut mfs = monitoring.rune_computation_time.collect();
        assert_eq!(mfs.len(), 1);

        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let proto_histogram = m.get_histogram();

        // Verify we recorded exactly 2 observations
        assert_eq!(
            proto_histogram.get_sample_count(),
            2,
            "Should have recorded 2 observations"
        );

        // Verify the sum of our observations (75 + 200 = 275)
        assert_eq!(
            proto_histogram.get_sample_sum(),
            275.0,
            "Sum of observations should be 275.0"
        );
    }

    #[test]
    fn test_rune_db_write_time() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different write times
        monitoring.metrics_record_rune_db_write_time(25.0);
        monitoring.metrics_record_rune_db_write_time(100.0);

        // Get the histogram values directly
        let mut mfs = monitoring.rune_db_write_time.collect();
        assert_eq!(mfs.len(), 1);

        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let proto_histogram = m.get_histogram();

        // Verify we recorded exactly 2 observations
        assert_eq!(
            proto_histogram.get_sample_count(),
            2,
            "Should have recorded 2 observations"
        );

        // Verify the sum of our observations (25 + 100 = 125)
        assert_eq!(
            proto_histogram.get_sample_sum(),
            125.0,
            "Sum of observations should be 125.0"
        );
    }

    #[test]
    fn test_runes_in_block() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different operation counts
        monitoring.metrics_record_runes_per_block(5);
        monitoring.metrics_record_runes_per_block(10);

        // Get the histogram values directly
        let mut mfs = monitoring.runes_per_block.collect();
        assert_eq!(mfs.len(), 1);

        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let proto_histogram = m.get_histogram();

        // Verify we recorded exactly 2 observations
        assert_eq!(
            proto_histogram.get_sample_count(),
            2,
            "Should have recorded 2 observations"
        );

        // Verify the sum of our observations (5 + 10 = 15)
        assert_eq!(
            proto_histogram.get_sample_sum(),
            15.0,
            "Sum of observations should be 15.0"
        );

        // Verify the values were properly bucketed
        let buckets = proto_histogram.get_bucket();
        assert!(!buckets.is_empty(), "Should have bucket data");

        // The value 5 should be in the 5-10 bucket
        let bucket_5 = buckets
            .iter()
            .find(|b| b.get_upper_bound() == 5.0)
            .expect("Should have 5 bucket");
        assert_eq!(
            bucket_5.get_cumulative_count(),
            1,
            "First value (5) should be in 5-10 bucket"
        );

        // The value 10 should be in the 10-25 bucket
        let bucket_10 = buckets
            .iter()
            .find(|b| b.get_upper_bound() == 10.0)
            .expect("Should have 10 bucket");
        assert_eq!(
            bucket_10.get_cumulative_count(),
            2,
            "Second value (10) should be in 10-25 bucket"
        );
    }

    #[test]
    fn test_metric_registry() {
        let monitoring = PrometheusMonitoring::new();

        // Record some test metrics
        monitoring.metrics_record_rune_parsing_time(50.0);
        monitoring.metrics_record_rune_computation_time(75.0);
        monitoring.metrics_record_rune_db_write_time(25.0);

        // Verify registry contains the metrics
        let metrics = monitoring.registry.gather();

        // Verify all expected metrics exist
        assert!(verify_metric_exists(&metrics, "rune_parsing_time"));
        assert!(verify_metric_exists(&metrics, "rune_computation_time"));
        assert!(verify_metric_exists(&metrics, "rune_db_write_time"));
    }

    #[test]
    fn test_runes_operations_per_block() {
        let monitoring = PrometheusMonitoring::new();

        // First block operations
        monitoring.metrics_record_runes_etching_per_block(2);
        monitoring.metrics_record_runes_mint_per_block(3);
        monitoring.metrics_record_runes_edict_per_block(1);
        monitoring.metrics_record_runes_cenotaph_etching_per_block(1);
        monitoring.metrics_record_runes_cenotaph_mint_per_block(1);

        // Verify first block values
        let mut mfs = monitoring.runes_etching_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            2.0,
            "Should have recorded 2 etching operations in first block"
        );

        // Verify first block mint operations
        mfs = monitoring.runes_mint_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Should have recorded 3 mint operations in first block"
        );

        // Verify first block edict operations
        mfs = monitoring.runes_edict_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            1.0,
            "Should have recorded 1 edict operation in first block"
        );

        // Verify first block cenotaph etching operations
        mfs = monitoring
            .runes_cenotaph_etching_operations_per_block
            .collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            1.0,
            "Should have recorded 1 cenotaph etching operation in first block"
        );

        // Verify first block cenotaph mint operations
        mfs = monitoring
            .runes_cenotaph_mint_operations_per_block
            .collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            1.0,
            "Should have recorded 1 cenotaph mint operation in first block"
        );

        // Second block operations (different values)
        monitoring.metrics_record_runes_etching_per_block(4);
        monitoring.metrics_record_runes_mint_per_block(1);
        monitoring.metrics_record_runes_edict_per_block(3);
        monitoring.metrics_record_runes_cenotaph_etching_per_block(2);
        monitoring.metrics_record_runes_cenotaph_mint_per_block(0);

        // Verify second block values (should overwrite first block values)
        mfs = monitoring.runes_etching_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            4.0,
            "Should have recorded 4 etching operations in second block"
        );

        mfs = monitoring.runes_mint_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            1.0,
            "Should have recorded 1 mint operation in second block"
        );

        mfs = monitoring.runes_edict_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Should have recorded 3 edict operations in second block"
        );

        mfs = monitoring
            .runes_cenotaph_etching_operations_per_block
            .collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            2.0,
            "Should have recorded 2 cenotaph etching operations in second block"
        );

        mfs = monitoring
            .runes_cenotaph_mint_operations_per_block
            .collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            0.0,
            "Should have recorded 0 cenotaph mint operations in second block"
        );
    }

    #[test]
    fn test_block_indexed() {
        let monitoring = PrometheusMonitoring::new();

        // Record block indexing
        monitoring.metrics_block_indexed(100);
        monitoring.metrics_block_indexed(200);

        // Get the counter value
        let mut mfs = monitoring.last_indexed_block_height.collect();
        assert_eq!(mfs.len(), 1);

        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();

        // Verify the total count (100 + 200 = 300)
        assert_eq!(
            gauge.get_value(),
            200.0,
            "Highest block height indexed should be 200"
        );
    }

    #[test]
    fn test_rune_indexed() {
        let monitoring = PrometheusMonitoring::new();

        // Record rune indexing
        monitoring.metrics_rune_indexed(50);
        monitoring.metrics_rune_indexed(100);

        // Get the counter value
        let mut mfs = monitoring.last_indexed_rune_number.collect();
        assert_eq!(mfs.len(), 1);

        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();

        // Verify the total count (50 + 100 = 150)
        assert_eq!(
            gauge.get_value(),
            100.0,
            "Highest rune number indexed should be 100"
        );
    }

    #[test]
    fn test_runes_etching_inputs_checked_per_block_metric() {
        let monitoring = PrometheusMonitoring::new();

        // Record inputs checked for different blocks
        monitoring.metrics_record_runes_etching_inputs_checked_per_block(5);
        monitoring.metrics_record_runes_etching_inputs_checked_per_block(10);
        monitoring.metrics_record_runes_etching_inputs_checked_per_block(3);

        // Get the gauge values using the registry
        let metrics = monitoring.registry.gather();

        // Find the runes_etching_inputs_checked_per_block metric
        let metric_family = metrics
            .iter()
            .find(|mf| mf.get_name() == "runes_etching_inputs_checked_per_block")
            .expect("Should find runes_etching_inputs_checked_per_block metric");

        let metric = metric_family.get_metric().first().unwrap();
        let gauge = metric.get_gauge();

        // Verify the gauge value (should be the last value set)
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Should have recorded 3 as the last value"
        );
    }
}
