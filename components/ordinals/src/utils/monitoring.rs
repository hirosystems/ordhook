use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use bitcoind::{try_debug, try_info, try_warn, utils::Context};
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use postgres::{pg_begin, pg_pool_client};
use prometheus::{
    core::{AtomicU64, GenericGauge},
    Encoder, Histogram, HistogramOpts, Registry, TextEncoder,
};
use tokio::time::sleep;

use crate::{db::ordinals_pg, PgConnectionPools};

type UInt64Gauge = GenericGauge<AtomicU64>;

#[derive(Debug, Clone)]
pub struct PrometheusMonitoring {
    // Inscriptions metrics
    pub last_indexed_block_height: UInt64Gauge,
    pub last_indexed_inscription_number: UInt64Gauge,
    pub last_classic_indexed_blessed_inscription_number: UInt64Gauge,
    pub last_classic_indexed_cursed_inscription_number: UInt64Gauge,

    // Performance metrics
    pub block_processing_time: Histogram,
    pub inscription_parsing_time: Histogram,
    pub inscription_computation_time: Histogram,
    pub inscription_db_write_time: Histogram,

    // Volumetric metrics
    pub inscriptions_per_block: Histogram,
    pub brc20_operations_per_block: Histogram,

    // BRC-20 specific metrics per block
    pub brc20_deploy_operations_per_block: UInt64Gauge,
    pub brc20_mint_operations_per_block: UInt64Gauge,
    pub brc20_transfer_operations_per_block: UInt64Gauge,
    pub brc20_transfer_send_operations_per_block: UInt64Gauge,

    // BRC-20 specific metrics total
    pub brc20_deploy_operations_total: UInt64Gauge,
    pub brc20_mint_operations_total: UInt64Gauge,
    pub brc20_transfer_operations_total: UInt64Gauge,
    pub brc20_transfer_send_operations_total: UInt64Gauge,

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

        // Inscriptions metrics
        let last_indexed_block_height = Self::create_and_register_uint64_gauge(
            &registry,
            "last_indexed_block_height",
            "The latest Bitcoin block indexed for ordinals.",
        );
        let last_indexed_inscription_number = Self::create_and_register_uint64_gauge(
            &registry,
            "last_indexed_inscription_number",
            "The latest indexed inscription number.",
        );

        let last_classic_indexed_blessed_inscription_number =
            Self::create_and_register_uint64_gauge(
                &registry,
                "last_classic_indexed_blessed_inscription_number",
                "The latest indexed blessed inscription number.",
            );

        let last_classic_indexed_cursed_inscription_number = Self::create_and_register_uint64_gauge(
            &registry,
            "last_classic_indexed_cursed_inscription_number",
            "The latest indexed cursed inscription number.",
        );

        // Performance metrics
        let block_processing_time = Self::create_and_register_histogram(
            &registry,
            "block_processing_time",
            "Time taken to process a block in milliseconds",
            vec![10_000.0, 20_000.0, 30_000.0, 60_000.0, 120_000.0, 300_000.0],
        );
        let inscription_parsing_time = Self::create_and_register_histogram(
            &registry,
            "inscription_parsing_time",
            "Time taken to parse inscriptions in a block in milliseconds",
            vec![
                1_000.0, 5_000.0, 10_000.0, 20_000.0, 60_000.0, 120_000.0, 300_000.0,
            ],
        );
        let inscription_computation_time = Self::create_and_register_histogram(
            &registry,
            "inscription_computation_time",
            "Time taken to compute inscription in milliseconds",
            vec![
                1_000.0, 5_000.0, 10_000.0, 20_000.0, 60_000.0, 120_000.0, 300_000.0,
            ],
        );
        let inscription_db_write_time = Self::create_and_register_histogram(
            &registry,
            "inscription_db_write_time",
            "Time taken to write to the database in milliseconds",
            vec![
                1_000.0, 5_000.0, 10_000.0, 20_000.0, 60_000.0, 120_000.0, 300_000.0,
            ],
        );

        // Volumetric metrics
        let inscriptions_per_block = Self::create_and_register_histogram(
            &registry,
            "inscriptions_per_block",
            "Number of inscriptions per block",
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
        );
        let brc20_operations_per_block = Self::create_and_register_histogram(
            &registry,
            "brc20_operations_per_block",
            "Number of BRC-20 operations per block",
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
        );

        // BRC-20 specific metrics per block
        let brc20_deploy_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_deploy_operations_per_block",
            "Count of BRC-20 deploy operations processed per block",
        );
        let brc20_mint_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_mint_operations_per_block",
            "Count of BRC-20 mint operations processed per block",
        );
        let brc20_transfer_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_transfer_operations_per_block",
            "Count of BRC-20 transfer operations processed per block",
        );
        let brc20_transfer_send_operations_per_block = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_transfer_send_operations_per_block",
            "Count of BRC-20 transfer send operations processed per block",
        );

        // BRC-20 specific metrics in total
        let brc20_deploy_operations_total = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_deploy_operations_total",
            "Count of BRC-20 deploy operations processed in total",
        );
        let brc20_mint_operations_total = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_mint_operations_total",
            "Count of BRC-20 mint operations processed in total",
        );
        let brc20_transfer_operations_total = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_transfer_operations_total",
            "Count of BRC-20 transfer operations processed in total",
        );
        let brc20_transfer_send_operations_total = Self::create_and_register_uint64_gauge(
            &registry,
            "brc20_transfer_send_operations_total",
            "Count of BRC-20 transfer send operations processed in total",
        );

        PrometheusMonitoring {
            last_indexed_block_height,
            last_indexed_inscription_number,
            last_classic_indexed_blessed_inscription_number,
            last_classic_indexed_cursed_inscription_number,
            block_processing_time,
            inscription_parsing_time,
            inscription_computation_time,
            inscription_db_write_time,
            inscriptions_per_block,
            brc20_operations_per_block,
            brc20_deploy_operations_per_block,
            brc20_mint_operations_per_block,
            brc20_transfer_operations_per_block,
            brc20_transfer_send_operations_per_block,
            brc20_deploy_operations_total,
            brc20_mint_operations_total,
            brc20_transfer_operations_total,
            brc20_transfer_send_operations_total,
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

    pub async fn initialize(
        &self,
        max_inscription_number: u64,
        block_height: u64,
        pg_pools: &PgConnectionPools,
    ) -> Result<(), String> {
        self.metrics_block_indexed(block_height);
        self.metrics_inscription_indexed(max_inscription_number);
        self.metrics_record_brc20_deploy_per_block(0);
        self.metrics_record_brc20_mint_per_block(0);
        self.metrics_record_brc20_transfer_per_block(0);
        self.metrics_record_brc20_transfer_send_per_block(0);

        // Read initial values from the database for inscriptions
        let mut ord_client = pg_pool_client(&pg_pools.ordinals).await?;
        let ord_tx = pg_begin(&mut ord_client).await?;

        let blessed_count = ordinals_pg::get_blessed_count_from_counts_by_type(&ord_tx)
            .await?
            .unwrap_or(0) as u64;
        let cursed_count = ordinals_pg::get_cursed_count_from_counts_by_type(&ord_tx)
            .await?
            .unwrap_or(0) as u64;

        // Set the total counts
        self.metrics_classic_blessed_inscription_indexed(blessed_count);
        self.metrics_classic_cursed_inscription_indexed(cursed_count);

        ord_tx
            .commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        // Read initial values from the database for BRC-20
        if let Some(brc20_pool) = &pg_pools.brc20 {
            let mut brc20_client = pg_pool_client(brc20_pool).await?;
            let brc20_tx = pg_begin(&mut brc20_client).await?;

            // Get counts from counts_by_operation table
            let row = brc20_tx
                .query_opt(
                    "SELECT 
                        COALESCE(MAX(CASE WHEN operation = 'deploy' THEN count END), 0) AS deploy,
                        COALESCE(MAX(CASE WHEN operation = 'mint' THEN count END), 0) AS mint,
                        COALESCE(MAX(CASE WHEN operation = 'transfer' THEN count END), 0) AS transfer,
                        COALESCE(MAX(CASE WHEN operation = 'transfer_send' THEN count END), 0) AS transfer_send
                    FROM counts_by_operation",
                    &[],
                )
                .await
                .map_err(|e| format!("Failed to query counts_by_operation: {}", e))?;

            if let Some(row) = row {
                let deploys: i32 = row.get("deploy");
                let mints: i32 = row.get("mint");
                let transfers: i32 = row.get("transfer");
                let transfer_sends: i32 = row.get("transfer_send");

                // Set the total counts
                self.metrics_record_brc20_deploy_total(deploys as u64);
                self.metrics_record_brc20_mint_total(mints as u64);
                self.metrics_record_brc20_transfer_total(transfers as u64);
                self.metrics_record_brc20_transfer_send_total(transfer_sends as u64);
            }

            brc20_tx
                .commit()
                .await
                .map_err(|e| format!("Failed to commit transaction: {}", e))?;
        }

        Ok(())
    }

    pub fn metrics_inscription_indexed(&self, inscription_number: u64) {
        let highest_appended = self.last_indexed_inscription_number.get();
        if inscription_number > highest_appended {
            self.last_indexed_inscription_number.set(inscription_number);
        }
    }

    pub fn metrics_classic_blessed_inscription_indexed(&self, blessed_inscription_number: u64) {
        let highest_appended = self.last_classic_indexed_blessed_inscription_number.get();
        if blessed_inscription_number > highest_appended {
            self.last_classic_indexed_blessed_inscription_number
                .set(blessed_inscription_number);
        }
    }

    pub fn metrics_classic_cursed_inscription_indexed(&self, cursed_inscription_number: u64) {
        let highest_appended = self.last_classic_indexed_cursed_inscription_number.get();
        if cursed_inscription_number > highest_appended {
            self.last_classic_indexed_cursed_inscription_number
                .set(cursed_inscription_number);
        }
    }

    pub fn metrics_block_indexed(&self, block_height: u64) {
        let highest_appended = self.last_indexed_block_height.get();
        if block_height > highest_appended {
            self.last_indexed_block_height.set(block_height);
        }
    }

    // Performance metrics methods
    pub fn metrics_record_block_processing_time(&self, process_time: f64) {
        self.block_processing_time.observe(process_time);
    }

    pub fn metrics_record_inscription_parsing_time(&self, elapsed_ms: f64) {
        self.inscription_parsing_time.observe(elapsed_ms);
    }

    pub fn metrics_record_inscription_computation_time(&self, elapsed_ms: f64) {
        self.inscription_computation_time.observe(elapsed_ms);
    }

    pub fn metrics_record_inscription_db_write_time(&self, elapsed_ms: f64) {
        self.inscription_db_write_time.observe(elapsed_ms);
    }

    pub fn metrics_record_inscriptions_per_block(&self, count: u64) {
        self.inscriptions_per_block.observe(count as f64);
    }

    pub fn metrics_record_brc20_operations_per_block(&self, count: u64) {
        self.brc20_operations_per_block.observe(count as f64);
    }

    // BRC-20 specific metrics methods per block
    pub fn metrics_record_brc20_deploy_per_block(&self, deploy_count: u64) {
        self.brc20_deploy_operations_per_block.set(deploy_count);
    }

    pub fn metrics_record_brc20_mint_per_block(&self, mint_count: u64) {
        self.brc20_mint_operations_per_block.set(mint_count);
    }

    pub fn metrics_record_brc20_transfer_per_block(&self, transfer_count: u64) {
        self.brc20_transfer_operations_per_block.set(transfer_count);
    }

    pub fn metrics_record_brc20_transfer_send_per_block(&self, transfer_send_count: u64) {
        self.brc20_transfer_send_operations_per_block
            .set(transfer_send_count);
    }

    // BRC-20 specific metrics methods in total
    pub fn metrics_record_brc20_deploy_total(&self, deploy_count: u64) {
        self.brc20_deploy_operations_total.add(deploy_count);
    }

    pub fn metrics_record_brc20_mint_total(&self, mint_count: u64) {
        self.brc20_mint_operations_total.add(mint_count);
    }

    pub fn metrics_record_brc20_transfer_total(&self, transfer_count: u64) {
        self.brc20_transfer_operations_total.add(transfer_count);
    }

    pub fn metrics_record_brc20_transfer_send_total(&self, transfer_send_count: u64) {
        self.brc20_transfer_send_operations_total
            .add(transfer_send_count);
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

        // Simulate some processing time with a more reliable method
        std::thread::sleep(std::time::Duration::from_millis(10_000));

        let elapsed = start_time.elapsed();
        monitoring.metrics_record_block_processing_time(elapsed.as_millis() as f64);

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
    fn test_inscription_parsing_time() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different parsing times
        monitoring.metrics_record_inscription_parsing_time(50.0);
        monitoring.metrics_record_inscription_parsing_time(150.0);

        // Get the histogram values directly
        let mut mfs = monitoring.inscription_parsing_time.collect();
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
    fn test_inscription_computation_time() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different computation times
        monitoring.metrics_record_inscription_computation_time(75.0);
        monitoring.metrics_record_inscription_computation_time(200.0);

        // Get the histogram values directly
        let mut mfs = monitoring.inscription_computation_time.collect();
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
    fn test_inscription_db_write_time() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different write times
        monitoring.metrics_record_inscription_db_write_time(25.0);
        monitoring.metrics_record_inscription_db_write_time(100.0);

        // Get the histogram values directly
        let mut mfs = monitoring.inscription_db_write_time.collect();
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
    fn test_inscriptions_per_block() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different inscription counts
        monitoring.metrics_record_inscriptions_per_block(5);
        monitoring.metrics_record_inscriptions_per_block(10);

        // Get the histogram values directly
        let mut mfs = monitoring.inscriptions_per_block.collect();
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
    fn test_brc20_operations_per_block() {
        let monitoring = PrometheusMonitoring::new();

        // Test with different BRC-20 operation counts
        monitoring.metrics_record_brc20_operations_per_block(3);
        monitoring.metrics_record_brc20_operations_per_block(7);

        // Get the histogram values directly
        let mut mfs = monitoring.brc20_operations_per_block.collect();
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

        // Verify the sum of our observations (3 + 7 = 10)
        assert_eq!(
            proto_histogram.get_sample_sum(),
            10.0,
            "Sum of observations should be 10.0"
        );
    }

    #[test]
    fn test_metric_buckets() {
        let monitoring = PrometheusMonitoring::new();

        // Test that metrics are properly bucketed
        let test_values = vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

        for value in test_values {
            monitoring.metrics_record_inscription_parsing_time(value);
            monitoring.metrics_record_inscription_computation_time(value);
            monitoring.metrics_record_inscription_db_write_time(value);
        }

        // Verify metrics were recorded
        let metrics = monitoring.registry.gather();
        assert!(verify_metric_exists(&metrics, "inscription_parsing_time"));
        assert!(verify_metric_exists(
            &metrics,
            "inscription_computation_time"
        ));
        assert!(verify_metric_exists(&metrics, "inscription_db_write_time"));
    }

    #[test]
    fn test_metric_registry() {
        let monitoring = PrometheusMonitoring::new();

        // Record some test metrics
        monitoring.metrics_record_inscription_parsing_time(50.0);
        monitoring.metrics_record_inscription_computation_time(75.0);
        monitoring.metrics_record_inscription_db_write_time(25.0);

        // Verify registry contains the metrics
        let metrics = monitoring.registry.gather();

        // Verify all expected metrics exist
        assert!(verify_metric_exists(&metrics, "inscription_parsing_time"));
        assert!(verify_metric_exists(
            &metrics,
            "inscription_computation_time"
        ));
        assert!(verify_metric_exists(&metrics, "inscription_db_write_time"));
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
    fn test_inscription_indexed() {
        let monitoring = PrometheusMonitoring::new();

        // Record regular inscription indexing
        monitoring.metrics_inscription_indexed(50);
        monitoring.metrics_inscription_indexed(100);
        monitoring.metrics_inscription_indexed(75);

        // Record blessed inscription indexing
        monitoring.metrics_classic_blessed_inscription_indexed(1);
        monitoring.metrics_classic_blessed_inscription_indexed(3);
        monitoring.metrics_classic_blessed_inscription_indexed(2);

        // Record cursed inscription indexing
        monitoring.metrics_classic_cursed_inscription_indexed(1);
        monitoring.metrics_classic_cursed_inscription_indexed(3);
        monitoring.metrics_classic_cursed_inscription_indexed(2);

        // Get the regular inscription number gauge
        let mut mfs = monitoring.last_indexed_inscription_number.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();

        // Verify the highest regular inscription number
        assert_eq!(
            gauge.get_value(),
            100.0,
            "Highest regular inscription number indexed should be 100"
        );

        // Get the blessed inscription number gauge
        let mut mfs = monitoring
            .last_classic_indexed_blessed_inscription_number
            .collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();

        // Verify the highest blessed inscription number
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Highest blessed inscription number indexed should be 3"
        );

        // Get the cursed inscription number gauge
        let mut mfs = monitoring
            .last_classic_indexed_cursed_inscription_number
            .collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();

        // Verify the highest cursed inscription number
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Highest cursed inscription number indexed should be 3"
        );
    }

    #[test]
    fn test_brc20_operations() {
        let monitoring = PrometheusMonitoring::new();

        // First block operations
        monitoring.metrics_record_brc20_deploy_per_block(2);
        monitoring.metrics_record_brc20_mint_per_block(3);
        monitoring.metrics_record_brc20_transfer_per_block(1);
        monitoring.metrics_record_brc20_transfer_send_per_block(1);

        // Record these operations in total
        monitoring.metrics_record_brc20_deploy_total(2);
        monitoring.metrics_record_brc20_mint_total(3);
        monitoring.metrics_record_brc20_transfer_total(1);
        monitoring.metrics_record_brc20_transfer_send_total(1);

        // Second block operations
        monitoring.metrics_record_brc20_deploy_per_block(1);
        monitoring.metrics_record_brc20_mint_per_block(2);
        monitoring.metrics_record_brc20_transfer_per_block(3);
        monitoring.metrics_record_brc20_transfer_send_per_block(2);

        // Record these operations in total
        monitoring.metrics_record_brc20_deploy_total(1);
        monitoring.metrics_record_brc20_mint_total(2);
        monitoring.metrics_record_brc20_transfer_total(3);
        monitoring.metrics_record_brc20_transfer_send_total(2);

        // Test per-block metrics (should show only latest block values)
        let mut mfs = monitoring.brc20_deploy_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            1.0,
            "Should have recorded 1 deploy operation in latest block"
        );

        mfs = monitoring.brc20_mint_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            2.0,
            "Should have recorded 2 mint operations in latest block"
        );

        mfs = monitoring.brc20_transfer_operations_per_block.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Should have recorded 3 transfer operations in latest block"
        );

        mfs = monitoring
            .brc20_transfer_send_operations_per_block
            .collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            2.0,
            "Should have recorded 2 transfer send operations in latest block"
        );

        // Test total metrics (should show cumulative values)
        mfs = monitoring.brc20_deploy_operations_total.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Should have recorded 3 deploy operations in total"
        );

        mfs = monitoring.brc20_mint_operations_total.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            5.0,
            "Should have recorded 5 mint operations in total"
        );

        mfs = monitoring.brc20_transfer_operations_total.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            4.0,
            "Should have recorded 4 transfer operations in total"
        );

        mfs = monitoring.brc20_transfer_send_operations_total.collect();
        assert_eq!(mfs.len(), 1);
        let mf = mfs.pop().unwrap();
        let m = mf.get_metric().first().unwrap();
        let gauge = m.get_gauge();
        assert_eq!(
            gauge.get_value(),
            3.0,
            "Should have recorded 3 transfer send operations in total"
        );
    }
}
