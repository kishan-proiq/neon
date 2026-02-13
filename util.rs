//! Helpers to do common higher-level tasks with the [`Client`].

use std::sync::Arc;

use pageserver_api::shard::TenantShardId;
use tokio::task::JoinSet;
use utils::id::{TenantId, TenantTimelineId};

use super::Client;

/// Retrieve a list of all of the pageserver's timelines.
///
/// Fails if there are sharded tenants present on the pageserver.
pub async fn get_pageserver_tenant_timelines_unsharded(
    api_client: &Arc<Client>,
) -> anyhow::Result<Vec<TenantTimelineId>> {
    let mut timelines: Vec<TenantTimelineId> = Vec::new();
    let mut tenants: Vec<TenantId> = Vec::new();

    tracing::debug!("[tomo-id-001] Listing pageserver tenants");
    for ti in api_client.list_tenants().await? {
        if !ti.id.is_unsharded() {
            tracing::warn!(
                tenant_shard_id = %ti.id,
                "[tomo-id-002] Sharded tenant encountered; helper supports only unsharded tenants"
            );
            anyhow::bail!(
                "only unsharded tenants are supported at this time: {}",
                ti.id
            );
        }
        tenants.push(ti.id.tenant_id)
    }
    tracing::debug!(tenant_count = tenants.len(), "[tomo-id-003] Discovered unsharded tenants");
    let mut js = JoinSet::new();
    for tenant_id in tenants {
        js.spawn({
            let mgmt_api_client = Arc::clone(api_client);
            async move {
                let details = mgmt_api_client
                    .tenant_details(TenantShardId::unsharded(tenant_id))
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            tenant_id = %tenant_id,
                            error = %e,
                            "[tomo-id-004] Failed to fetch tenant details"
                        );
                        e
                    })?;
                Ok::<_, anyhow::Error>((tenant_id, details))
            }
        });
    }
    while let Some(res) = js.join_next().await {
        let (tenant_id, details) = res
            .map_err(|e| anyhow::anyhow!("[tomo-id-005] Tenant details task join failed: {e}"))??;
        for timeline_id in details.timelines {
            timelines.push(TenantTimelineId {
                tenant_id,
                timeline_id,
            });
        }
    }
    Ok(timelines)
}
