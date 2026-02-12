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
    targets: Option<Vec<TenantTimelineId>>,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    // Basic JSON-ish structured logging for CLI runs.
    // If the wider repo already initializes tracing globally, this is still safe for a standalone command.
    let _ = tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .json()
        .try_init();

    tracing::info!("[tomo-id-001] pagebench trigger_initial_size_calculation starting");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build tokio runtime: {e}"))?;

    let main_task = rt.spawn(main_impl(args));
    rt.block_on(main_task)
        .map_err(|e| anyhow::anyhow!("main task join error: {e}"))?
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    let args = Arc::new(args);

    let http_client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(3))
        .timeout(std::time::Duration::from_secs(10))
        .pool_max_idle_per_host(8)
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

    let max_in_flight: usize = std::env::var("PAGEBENCH_MAX_IN_FLIGHT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

    let mut js = JoinSet::new();
    for tl in timelines {
        let mgmt_api_client = Arc::clone(&mgmt_api_client);
        let semaphore = Arc::clone(&semaphore);
        js.spawn(async move {
            let _permit = semaphore.acquire_owned().await.expect("semaphore closed");

            let info = mgmt_api_client
                .timeline_info(
                    TenantShardId::unsharded(tl.tenant_id),
                    tl.timeline_id,
                    ForceAwaitLogicalSize::Yes,
                )
                .await
                .unwrap();

            // Polling should not be strictly required here since we await
            // for the initial logical size, however it's possible for the request
            // to land before the timeline is initialised. This results in an approximate
            // logical size.
            if let Some(period) = args.poll_for_completion {
                let mut ticker = tokio::time::interval(period.into());
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut info = info;
                while !info.current_logical_size_is_accurate {
                    ticker.tick().await;
                    info = mgmt_api_client
                        .timeline_info(
                            TenantShardId::unsharded(tl.tenant_id),
                            tl.timeline_id,
                            ForceAwaitLogicalSize::Yes,
                        )
                        .await
                        .unwrap();
                }
            }
        });
    }
    while let Some(res) = js.join_next().await {
        match res {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("[tomo-id-006] join_next task failed", error = %e);
            }
        }
    }
    Ok(())
}
