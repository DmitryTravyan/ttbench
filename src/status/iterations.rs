use std::{
    ops::SubAssign,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::config::Config;

use super::{Logger, Progress, Status, Target};

pub struct Iterations<T> {
    pub generator: T,
    pub counter: Arc<Mutex<u64>>,
}

impl<T> Iterations<T> {
    pub fn new(generator: T, counter: Arc<Mutex<u64>>) -> Self {
        Self { generator, counter }
    }
}

pub type IncreasingUsize = AtomicU64;

impl Target for Iterations<IncreasingUsize> {
    type Output = u64;

    fn new(config: &Config) -> Self {
        Iterations {
            generator: AtomicU64::new(config.transactions_total),
            counter: Arc::new(Mutex::new(0)),
        }
    }

    fn make_progress(&self) -> Result<Option<Self::Output>, anyhow::Error> {
        let mut locked = self.counter.lock().unwrap();
        if locked.eq(&0) {
            return Ok(None);
        }
        locked.sub_assign(1);
        Ok(Some(self.generator.fetch_add(1, Ordering::SeqCst)))
    }

    fn is_reached(&self) -> Result<bool, anyhow::Error> {
        let locked = self.counter.lock().unwrap();
        if locked.eq(&0) {
            return Ok(true);
        }
        Ok(false)
    }
}

impl Status<Iterations<IncreasingUsize>> {
    pub fn single_run() -> Self {
        Status {
            target: Iterations::new(AtomicU64::new(0), Arc::new(Mutex::new(1))),
            progress: Progress::default(),
            logger: Logger,
        }
    }
}
