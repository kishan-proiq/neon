use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use pageserver_api::models::{TenantConfig, TenantConfigRequest};
use pageserver_api::shard::TenantShardId;
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;

/// Ingest aux files into the pageserver.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,

    targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build tokio runtime: {e}"))?;

    let main_task = rt.spawn(main_impl(args));
    rt.block_on(main_task)
        .map_err(|e| anyhow::anyhow!("main task join error: {e}"))?
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let http_client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(3))
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        http_client, // TODO: support ssl_ca_file for https APIs in pagebench.
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    // discover targets
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: None,
            targets: {
                if let Some(targets) = &args.targets {
                    if targets.len() != 1 {
                        anyhow::bail!("must specify exactly one target");
                    }
                    Some(targets.clone())
                } else {
                    None
                }
            },
        },
    )
    .await?;

    let timeline = timelines[0];
    let tenant_shard_id = TenantShardId::unsharded(timeline.tenant_id);
    let timeline_id = timeline.timeline_id;

    tracing::info!(
        "[tomo-id-001] operating on timeline",
        tenant_id = %timeline.tenant_id,
        timeline_id = %timeline.timeline_id
    );

    {
        let mut delay_ms: u64 = 200;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let res = mgmt_api_client
                .set_tenant_config(&TenantConfigRequest {
                    tenant_id: timeline.tenant_id,
                    config: TenantConfig::default(),
                })
                .await;
            match res {
                Ok(()) => break,
                Err(e) if attempt < 5 => {
                    tracing::warn!(
                        "[tomo-id-004] set_tenant_config failed; retrying",
                        attempt = attempt,
                        delay_ms = delay_ms,
                        tenant_id = %timeline.tenant_id,
                        error = %e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms.saturating_mul(2)).min(5_000);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    const INGEST_BATCHES: u32 = 100;
    const INGEST_ITEMS_PER_BATCH: u32 = 100;

    for batch in 0..INGEST_BATCHES {
        let items = (0..100)
            .map(|id| {
                (
                    format!("pg_logical/mappings/{batch:03}.{id:03}"),
                    format!("{id:08}"),
                )
            })
            .collect::<HashMap<_, _>>();
        let file_cnt = items.len();
        mgmt_api_client
            .ingest_aux_files(tenant_shard_id, timeline_id, items)
            .await?;
        tracing::info!(
            "[tomo-id-002] ingested aux files batch",
            batch = batch,
            file_cnt = file_cnt,
            tenant_shard_id = %tenant_shard_id,
            timeline_id = %timeline_id
        );
    }

    const LIST_ITERATIONS: u32 = 100;

    for _ in 0..LIST_ITERATIONS {
        let start = Instant::now();
        let files = mgmt_api_client
            .list_aux_files(tenant_shard_id, timeline_id, Lsn(Lsn::MAX.0 - 1))
            .await?;
        tracing::info!(
            "[tomo-id-003] listed aux files",
            files_len = files.len(),
            elapsed_s = start.elapsed().as_secs_f64(),
            tenant_shard_id = %tenant_shard_id,
            timeline_id = %timeline_id
        );
    }

    anyhow::Ok(())
}
