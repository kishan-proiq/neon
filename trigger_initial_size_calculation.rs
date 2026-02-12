use std::sync::Arc;

use humantime::Duration;
use pageserver_api::shard::TenantShardId;
use pageserver_client::mgmt_api::ForceAwaitLogicalSize;
use tokio::task::JoinSet;
use utils::id::TenantTimelineId;

#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "localhost:64000")]
    page_service_host_port: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(
        long,
        help = "if specified, poll mgmt api to check whether init logical size calculation has completed"
    )]
    poll_for_completion: Option<Duration>,
    #[clap(long)]
    limit_to_first_n_targets: Option<usize>,
    #[clap(
        long,
        default_value_t = 32,
        help = "maximum number of concurrent mgmt API requests (overload protection)"
    )]
    max_concurrency: usize,
    targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    tracing::info!("[tomo-id-001] pagebench trigger-initial-size starting");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("[tomo-id-002] failed to build tokio runtime: {e}"))?;

    let main_task = rt.spawn(main_impl(args));
    let res = rt
        .block_on(main_task)
        .map_err(|e| anyhow::anyhow!("[tomo-id-003] main task join failed: {e}"))?;

    tracing::info!("[tomo-id-004] pagebench trigger-initial-size finished");
    res
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let http_timeout = std::env::var("PAGEBENCH_MGMT_API_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(10));

    let http_client = reqwest::Client::builder()
        .timeout(http_timeout)
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
            limit_to_first_n_targets: args.limit_to_first_n_targets,
            targets: args.targets.clone(),
        },
    )
    .await?;

    // kick it off

    let max_concurrency = std::env::var("PAGEBENCH_MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32);
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrency));

    let mut js = JoinSet::new();
    for tl in timelines {
        let mgmt_api_client = Arc::clone(&mgmt_api_client);
        let sem = Arc::clone(&sem);
        js.spawn(async move {
            let _permit = sem.acquire_owned().await?;

            let info = mgmt_api_client
                .timeline_info(
                    TenantShardId::unsharded(tl.tenant_id),
                    tl.timeline_id,
                    ForceAwaitLogicalSize::Yes,
                )
                .await;

            let mut info = match info {
                Ok(info) => info,
                Err(e) => {
                    tracing::warn!(
                        tenant_id = %tl.tenant_id,
                        timeline_id = %tl.timeline_id,
                        "[tomo-id-005] timeline_info initial call failed: {e}"
                    );
                    return Ok::<(), anyhow::Error>(());
                }
            };

            // Polling should not be strictly required here since we await
            // for the initial logical size, however it's possible for the request
            // to land before the timeline is initialised. This results in an approximate
            // logical size.
            if let Some(period) = args.poll_for_completion {
                let mut ticker = tokio::time::interval(period.into());
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                while !info.current_logical_size_is_accurate {
                    ticker.tick().await;
                    info = match mgmt_api_client
                        .timeline_info(
                            TenantShardId::unsharded(tl.tenant_id),
                            tl.timeline_id,
                            ForceAwaitLogicalSize::Yes,
                        )
                        .await
                    {
                        Ok(info) => info,
                        Err(e) => {
                            tracing::warn!(
                                tenant_id = %tl.tenant_id,
                                timeline_id = %tl.timeline_id,
                                "[tomo-id-006] timeline_info poll call failed: {e}"
                            );
                            return Ok::<(), anyhow::Error>(());
                        }
                    };
                }
            }

            Ok::<(), anyhow::Error>(())
        });
    }
    while let Some(res) = js.join_next().await {
        if let Err(e) = res? {
            tracing::warn!("[tomo-id-007] target task failed: {e}");
        }
    }
    Ok(())
}
