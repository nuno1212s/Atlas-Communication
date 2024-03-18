use atlas_metrics::metrics::MetricKind;
use atlas_metrics::MetricRegistry;

pub const CLIENT_POOL_COLLECT_TIME: &str = "CLIENT_POOL_COLLECT_TIME";
pub const CLIENT_POOL_COLLECT_TIME_ID: usize = 404;

pub const CLIENT_POOL_BATCH_PASSING_TIME: &str = "CLIENT_POOL_BATCH_PASSING_TIME";
pub const CLIENT_POOL_BATCH_PASSING_TIME_ID: usize = 405;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (
            CLIENT_POOL_COLLECT_TIME_ID,
            CLIENT_POOL_COLLECT_TIME.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            CLIENT_POOL_BATCH_PASSING_TIME_ID,
            CLIENT_POOL_BATCH_PASSING_TIME.to_string(),
            MetricKind::Duration,
        )
            .into(),
    ]
}
