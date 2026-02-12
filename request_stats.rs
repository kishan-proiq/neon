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
                .context("init latency histogram")?,
        })
    }

    pub(crate) fn new() -> Self {
        Self::try_new().expect("Stats::new: init latency histogram")
    }
    pub(crate) fn observe(&mut self, latency: Duration) -> anyhow::Result<()> {
        let micros: u64 = latency
            .as_micros()
            .try_into()
            .context("latency greater than u64")?;
        anyhow::ensure!(micros >= 1, "latency must be >= 1us to record");
        self.latency_histo
            .record(micros)
            .context("add to histogram")?;
        Ok(())
    }
    pub(crate) fn output(&self) -> Output {
        // Note: histogram values are recorded in microseconds (µs).
        let latency_percentiles = std::array::from_fn(|idx| {
            let micros = self
                .latency_histo
                .value_at_percentile(LATENCY_PERCENTILES[idx]);
            Duration::from_micros(micros)
        });
        Output {
            request_count: self.latency_histo.len(),
            latency_mean: Duration::from_micros(self.latency_histo.mean() as u64),
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

const LATENCY_PERCENTILES_COUNT: usize = 4;
const LATENCY_PERCENTILES: [f64; LATENCY_PERCENTILES_COUNT] = [95.0, 99.00, 99.90, 99.99];

struct LatencyPercentiles {
    latency_percentiles: [Duration; LATENCY_PERCENTILES_COUNT],
}

impl serde::Serialize for LatencyPercentiles {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
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
