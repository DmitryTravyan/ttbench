use std::{
    fmt::Display,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::config::{BenchMod, Config};

pub mod iterations;

pub trait Target {
    type Output;

    fn new(config: &Config) -> Self;
    fn make_progress(&self) -> Result<Option<Self::Output>, anyhow::Error>;
    fn is_reached(&self) -> Result<bool, anyhow::Error>;
}

pub struct Status<T: Target + Send + Sync> {
    pub target: T,
    pub progress: Progress,
    pub logger: Logger,
}

impl<T: Target + Send + Sync> Status<T> {
    pub async fn wait_the_end(&self) -> Result<(), anyhow::Error> {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.progress.time_elapsed.fetch_add(100, Ordering::SeqCst);
            if self.target.is_reached()? {
                return Ok(());
            }
        }
    }

    pub fn make_progress(&self) -> Result<Option<T::Output>, anyhow::Error> {
        let progress = self.target.make_progress();
        if let Ok(Some(_)) = &progress {
            self.progress
                .iterations_count
                .fetch_add(1, Ordering::SeqCst);
        }
        progress
    }

    pub fn is_reached(&self) -> Result<bool, anyhow::Error> {
        self.target.is_reached()
    }
}

impl<'a, T: Target + Send + Sync> TryFrom<&'a Config> for Status<T> {
    type Error = anyhow::Error;

    fn try_from(config: &'a Config) -> Result<Self, Self::Error> {
        match config.mode {
            BenchMod::Iterations => Ok(Status {
                target: T::new(config),
                progress: Progress::default(),
                logger: Logger,
            }),
            BenchMod::Time => todo!(),
        }
    }
}

pub struct Logger;

impl Logger {
    pub fn error<D: Display>(&self, text: D) {
        println!("{text}")
    }
}

#[derive(Default)]
pub struct Progress {
    iterations_count: AtomicU64,
    /// iterations per second
    iterations: f64,
    iteration_duration_avg: Duration,
    iteration_duration_p95: Duration,
    iteration_duration_max: Duration,
    time_elapsed: AtomicU64,
    time_estimate: Duration,
    jobs_in_progress: u64,
}

impl Progress {
    pub fn update(&mut self, exec_time: Duration) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

impl Display for Progress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "duration: {} s",
            self.time_elapsed.load(Ordering::Relaxed)
        )?;
        write!(
            f,
            "number or transactions actualy processed: {}",
            self.iterations_count.load(Ordering::Relaxed)
        )?;
        write!(
            f,
            "latency average: {} ms",
            self.iteration_duration_avg.as_millis()
        )?;
        write!(
            f,
            "latency p95: {} ms",
            self.iteration_duration_p95.as_millis()
        )?;
        write!(
            f,
            "latency max: {} ms",
            self.iteration_duration_max.as_millis()
        )?;
        write!(f, "tps = {}", self.iterations)
    }
}
