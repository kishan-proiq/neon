use std::sync::Arc;

use pageserver_client::mgmt_api;
use tracing::info;
use utils::id::TenantTimelineId;

pub(crate) struct Spec {
    /// Optional guardrail to cap the number of targets.
    ///
    /// Operationally, this prevents the CLI from generating excessive load and log volume
    /// when pointed at a pageserver with a very large number of timelines.
    pub(crate) limit_to_first_n_targets: Option<usize>,
    /// Explicit targets provided by the caller; if absent, targets are discovered via mgmt API.
    pub(crate) targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) async fn discover(
    api_client: &Arc<mgmt_api::Client>,
    spec: Spec,
) -> anyhow::Result<Vec<TenantTimelineId>> {
    let mut timelines = if let Some(targets) = spec.targets {
        targets
    } else {
        mgmt_api::util::get_pageserver_tenant_timelines_unsharded(api_client)
            .await
            .with_context(|| "[tomo-id-003] Failed to discover tenant timelines from pageserver mgmt API")?
    };

    if let Some(limit) = spec.limit_to_first_n_targets {
        timelines.sort(); // for determinism
        timelines.truncate(limit);
        if timelines.len() < limit {
            anyhow::bail!(
                "[tomo-id-004] Insufficient timelines for requested limit: requested={limit} available={available}",
                requested = limit,
                available = timelines.len()
            );
        }
    }

    info!(
        "[tomo-id-001] Discovered pageserver timelines",
        total_timelines = timelines.len(),
        limited = spec.limit_to_first_n_targets.is_some(),
        limit = spec.limit_to_first_n_targets
    );
    tracing::debug!("[tomo-id-002] Timeline targets (debug)", timelines = ?timelines);

    Ok(timelines)
}
