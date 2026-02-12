use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use tonic::transport::Endpoint;
use tracing::info;

use pageserver_page_api::{GetPageClass, GetPageRequest, GetPageStatusCode, ReadLsn, RelTag};
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;
use utils::shard::ShardIndex;

/// Starts a large number of idle gRPC GetPage streams.
#[derive(clap::Parser)]
pub(crate) struct Args {
    /// The Pageserver to connect to. Must use grpc://.
    #[clap(long, default_value = "grpc://localhost:51051")]
    server: String,
    /// The Pageserver HTTP API.
    #[clap(long, default_value = "http://localhost:9898")]
    http_server: String,
    /// The number of streams to open.
    #[clap(long, default_value = "100000")]
    count: usize,
    /// Number of streams per connection.
    #[clap(long, default_value = "100")]
    per_connection: usize,
    /// Send a single GetPage request on each stream.
    #[clap(long, default_value_t = false)]
    send_request: bool,
}

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(main_impl(args))
}

async fn main_impl(args: Args) -> anyhow::Result<()> {
    // Discover a tenant and timeline to use.
    info!("[tomo-id-002] discovering tenant/timeline via mgmt API (http_server={})", args.http_server);
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        http_client,
        args.http_server.clone(),
        None,
    ));
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: Some(1),
            targets: None,
        },
    )
    .await?;
    let ttid = timelines
        .first()
        .ok_or_else(|| anyhow!("no timelines found"))?;

    // Set up the initial client.
    let endpoint = Endpoint::from_shared(args.server.clone())?
        .connect_timeout(std::time::Duration::from_secs(5));

    let connect = async || {
        pageserver_page_api::Client::new(
            endpoint.connect().await?,
            ttid.tenant_id,
            ttid.timeline_id,
            ShardIndex::unsharded(),
            None,
            None,
        )
    };

    let max_streams = 1_000_000usize;
    if args.count > max_streams {
        return Err(anyhow!("refusing to open {} streams (max_streams={})", args.count, max_streams));
    }

    let mut client = connect().await?;
    let mut streams = Vec::with_capacity(args.count);

    // Create streams.
    let progress_every = 1_000usize;
    for i in 0..args.count {
        if i % progress_every == 0 {
            info!("[tomo-id-001] opened {}/{} streams (server={}, per_connection={}, send_request={})", i, args.count, args.server, args.per_connection, args.send_request);
        }
        if i % args.per_connection == 0 && i > 0 {
            info!("[tomo-id-004] rotating gRPC connection at stream_index={} (per_connection={})", i, args.per_connection);
            client = connect().await?;
        }

        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel();
        let req_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(req_rx);
        let mut resp_stream = client.get_pages(req_stream).await?;

        // Send request if specified.
        if args.send_request {
            info!("[tomo-id-005] sending single GetPage request on stream_index={}", i);
            req_tx.send(GetPageRequest {
                request_id: 1.into(),
                request_class: GetPageClass::Normal,
                read_lsn: ReadLsn {
                    request_lsn: Lsn::MAX,
                    not_modified_since_lsn: Some(Lsn(1)),
                },
                rel: RelTag {
                    spcnode: 1664, // pg_global
                    dbnode: 0,     // shared database
                    relnode: 1262, // pg_authid
                    forknum: 0,    // init
                },
                block_numbers: vec![0],
            })?;

            let resp = tokio::time::timeout(std::time::Duration::from_secs(10), resp_stream.next())
                .await
                .map_err(|_| anyhow!("timed out waiting for GetPage response"))?
                .transpose()?
                .ok_or_else(|| anyhow!("no response"))?;
            if resp.status_code != GetPageStatusCode::Ok {
                return Err(anyhow!("{} response", resp.status_code));
            }
        }

        // Hold onto streams to avoid closing them.
        streams.push((req_tx, resp_stream));
    }

    info!("[tomo-id-006] opened {} streams, entering idle hold (grpc_server={}, http_server={})", args.count, args.server, args.http_server);

    // Block forever, to hold the idle streams open for inspection.
    futures::future::pending::<()>().await;

    Ok(())
}
