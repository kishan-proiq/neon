use std::time::Duration;

use anyhow::Context;

pub(crate) struct Stats {
    latency_histo: hdrhistogram::Histogram<u64>,
}

impl Stats {
    pub(crate) fn try_new() -> anyhow::Result<Self> {
        Ok(Self {
            // Initialize with fixed bounds so that we panic at runtime instead of resizing the histogram,
            // which would skew the benchmark results.
            latency_histo: hdrhistogram::Histogram::new_with_bounds(1, 1_000_000_000, 3)
                .context("initialize latency histogram")?,
        })
    }

    pub(crate) fn new() -> Self {
        // Preserve previous behavior (fail fast) while enabling callers to opt into non-panicking init.
        Self::try_new().expect("initialize latency histogram")
    }
    pub(crate) fn observe(&mut self, latency: Duration) -> anyhow::Result<()> {
        let micros: u64 = latency
            .as_micros()
            .try_into()
            .with_context(|| format!("latency too large for u64 micros: {:?}", latency))?;
        self.latency_histo
            .record(micros)
            .with_context(|| format!("add to histogram (micros={micros})"))?;
        Ok(())
    }
    pub(crate) fn output(&self) -> Output {
        let request_count = self.latency_histo.len();

        let latency_percentiles = if request_count == 0 {
            [Duration::from_micros(0); 4]
        } else {
            std::array::from_fn(|idx| {
                let micros = self
                    .latency_histo
                    .value_at_percentile(LATENCY_PERCENTILES[idx]);
                Duration::from_micros(micros)
            })
        };

        Output {
            request_count,
            latency_mean: if request_count == 0 {
                Duration::from_micros(0)
            } else {
                Duration::from_micros(self.latency_histo.mean() as u64)
            },
            latency_percentiles: LatencyPercentiles {
                latency_percentiles,
            },
        }
    }
    pub(crate) fn add(&mut self, other: &Self) -> anyhow::Result<()> {
        let Self { latency_histo } = self;
        latency_histo
            .add(&other.latency_histo)
            .context("merge latency histogram")?;
        Ok(())
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

const LATENCY_PERCENTILES: [f64; 4] = [95.0, 99.00, 99.90, 99.99];

struct LatencyPercentiles {
    latency_percentiles: [Duration; LATENCY_PERCENTILES.len()],
}

impl serde::Serialize for LatencyPercentiles {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Support note:
        // - Histogram values are stored in microseconds.
        // - Percentiles are serialized as human-readable durations for benchmark reports.
        // - If you see frequent saturation at the upper bound, adjust the histogram bounds in `Stats`.
        use serde::ser::SerializeMap;
        let mut ser = serializer.serialize_map(Some(LATENCY_PERCENTILES.len()))?;
        for (p, v) in LATENCY_PERCENTILES.iter().zip(&self.latency_percentiles) {
            ser.serialize_entry(
                &format!("p{p}"),
                &format!("{}", humantime::format_duration(*v)),
            )?;
        }
        ser.end()
    }
}

#[derive(serde::Serialize)]
pub(crate) struct Output {
    request_count: u64,
    #[serde(with = "humantime_serde")]
    latency_mean: Duration,
    latency_percentiles: LatencyPercentiles,
}
