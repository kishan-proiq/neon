//! Upload queue benchmarks.

use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use criterion::{Bencher, Criterion, criterion_group, criterion_main};
use pageserver::tenant::IndexPart;
use pageserver::tenant::metadata::TimelineMetadata;
use pageserver::tenant::remote_timeline_client::index::LayerFileMetadata;
use pageserver::tenant::storage_layer::LayerName;
use pageserver::tenant::upload_queue::{Delete, UploadOp, UploadQueue, UploadTask};
use pprof::criterion::{Output, PProfProfiler};
use utils::generation::Generation;
use utils::shard::{ShardCount, ShardIndex, ShardNumber};

// Register benchmarks with Criterion.
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_upload_queue_next_ready,
);
criterion_main!(benches);

/// Benchmarks the cost of UploadQueue::next_ready() with the given number of in-progress tasks
/// (which is equivalent to tasks ahead of it in the queue). This has linear cost, and the upload
/// queue as a whole is thus quadratic.
///
/// NOTE: This bench intentionally includes large `inprogress` values to surface algorithmic
/// regressions. It can be memory-heavy on small CI runners; consider reducing the largest case
/// locally if you hit OOM.
///
/// UploadOp::UploadLayer requires an entire tenant and timeline to construct, so we just test
/// Delete and UploadMetadata instead. This is incidentally the most expensive case.
fn bench_upload_queue_next_ready(c: &mut Criterion) {
    tracing::info!("[tomo-id-001] starting benchmark group", group = "upload_queue_next_ready");
    let mut g = c.benchmark_group("upload_queue_next_ready");
    for inprogress in [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000] {
        tracing::info!("[tomo-id-002] registering benchmark case", inprogress);
        g.bench_function(format!("inprogress={inprogress}"), |b| {
            if let Err(e) = run_bench(b, inprogress) {
                tracing::error!("[tomo-id-003] benchmark case failed", inprogress, error = %e);
                panic!("benchmark case failed: inprogress={inprogress}: {e}");
            }
        });
    }

    fn run_bench(b: &mut Bencher, inprogress: usize) -> anyhow::Result<()> {
        // Construct two layers. layer0 is in the indexes, layer1 will be deleted.
        const LAYER0_NAME: &str = "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51";
        const LAYER1_NAME: &str = "100000000000000000000000000000000001-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51";
        let layer0 = LayerName::from_str(LAYER0_NAME).expect("invalid name");
        let layer1 = LayerName::from_str(LAYER1_NAME).expect("invalid name");

        let _span = tracing::info_span!(
            "upload_queue_bench_setup",
            message = "[tomo-id-004] building benchmark inputs",
            inprogress,
            shard_number = 1u32,
            shard_count = 2u32,
            generation = 1u32,
        )
        .entered();

        let metadata = LayerFileMetadata {
            shard: ShardIndex::new(ShardNumber(1), ShardCount(2)),
            generation: Generation::Valid(1),
            file_size: 0,
        };

        // Construct the (initial and uploaded) index with layer0.
        let mut index = IndexPart::empty(TimelineMetadata::example());
        index.layer_metadata.insert(layer0, metadata.clone());

        // Construct the queue.
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&index, 0)?;

        // Populate inprogress_tasks with a bunch of layer1 deletions.
        let delete = UploadOp::Delete(Delete {
            layers: vec![(layer1, metadata)],
        });

        if inprogress > 1_000_000 {
            tracing::warn!("[tomo-id-005] unusually large inprogress; benchmark may allocate heavily", inprogress);
        }

        for task_id in 0..(inprogress as u64) {
            queue.inprogress_tasks.insert(
                task_id,
                Arc::new(UploadTask {
                    task_id,
                    retries: AtomicU32::new(0),
                    op: delete.clone(),
                    coalesced_ops: Vec::new(),
                }),
            );
        }

        // Benchmark index upload scheduling.
        let index_upload = UploadOp::UploadMetadata {
            uploaded: Box::new(index),
        };

        b.iter(|| {
            // Clone is intentionally outside the measured `next_ready()` cost focus.
            let op = index_upload.clone();
            queue.queued_operations.push_front(op);
            assert!(queue.next_ready().is_some());
        });

        Ok(())
    }
}
