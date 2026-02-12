pub(crate) type ThreadLocalStats<T> = Arc<Mutex<T>>;
pub(crate) type AllThreadLocalStats<T> = Arc<Mutex<Vec<ThreadLocalStats<T>>>>;

macro_rules! declare {
    ($THREAD_LOCAL_NAME:ident: $T:ty) => {
        thread_local! {
            pub static $THREAD_LOCAL_NAME: std::cell::RefCell<crate::util::tokio_thread_local_stats::ThreadLocalStats<$T>> = std::cell::RefCell::new(
                std::sync::Arc::new(std::sync::Mutex::new(Default::default()))
            );
        }
    };
}

use std::sync::{Arc, Mutex};

pub(crate) use declare;

macro_rules! main {
    ($THREAD_LOCAL_NAME:ident, $main_impl:expr) => {{
        // Operational note: this macro pre-initializes per-thread stats in `on_thread_start`
        // to avoid paying init cost during the measurement window.
        // If benchmarks regress, inspect thread-start initialization time and mutex contention here.
        let main_impl = $main_impl;
        let all = Arc::new(Mutex::new(Vec::new()));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .on_thread_start({
                let all = Arc::clone(&all);
                move || {
                    tracing::debug!("[tomo-id-001] pageserver.pagebench.thread_start: initializing thread-local stats");
                    // pre-initialize the thread local stats by accessesing them
                    // (some stats like requests_stats::Stats are quite costly to initialize,
                    //  we don't want to pay that cost during the measurement period)
                    $THREAD_LOCAL_NAME.with(|stats| {
                        let stats: Arc<_> = Arc::clone(&*stats.borrow());
                        let mut guard = all.lock().unwrap();
                        guard.push(stats);
                        tracing::debug!(
                            count = guard.len(),
                            "[tomo-id-002] pageserver.pagebench.thread_start: registered thread-local stats"
                        );
                    });
                }
            })
            .enable_all()
            .build()
            .expect("[tomo-id-003] pageserver.pagebench: failed to build tokio runtime");

        let main_task = rt.spawn(main_impl(all));
        rt.block_on(main_task).expect("[tomo-id-004] pageserver.pagebench: main task join failed")
    }};
}

pub(crate) use main;
