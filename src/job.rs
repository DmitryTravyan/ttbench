use anyhow::anyhow;
use std::{
    future::Future,
    ops::AddAssign,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use crate::{
    config::Config,
    status::{Status, Target},
};

use self::connections::Pool;

// TODO remove pub
pub mod accounts;
pub mod branches;
pub mod connections;
pub mod create;
pub mod drop;
pub mod tellers;
pub mod tpcb;

pub struct JobConfig {
    pub config: Config,
    pub connections: Pool,
}

pub struct Job<T, S, F, O>
where
    T: Target + Send + Sync,
    S: Spawn<T::Output, Output = F> + Unpin + Clone,
    F: Future<Output = Result<O, anyhow::Error>> + Send,
{
    id: u64,
    task: Pin<Box<F>>,
    spawner: S,
    job_config: Pin<Arc<JobConfig>>,
    job_retries: u64,
    status: Pin<Arc<Status<T>>>,
}

impl<T, S, F, O> Future for Job<T, S, F, O>
where
    T: Target + Send + Sync,
    S: Spawn<T::Output, Output = F> + Unpin + Clone,
    F: Future<Output = Result<O, anyhow::Error>> + Send,
{
    type Output = Result<O, anyhow::Error>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match (
            Pin::new(&mut self.task).poll(cx),
            self.status.target.is_reached()?,
        ) {
            (Poll::Ready(out), true) if out.is_ok() => {
                if let Err(err) = &out {
                    self.status.logger.error(err);
                    if self.job_retries > self.job_config.config.max_retries {
                        return Poll::Ready(out);
                    }
                    self.job_retries.add_assign(1);
                }
                Poll::Ready(out)
            }
            (Poll::Ready(out), false) => {
                if let Err(err) = &out {
                    self.status.logger.error(err);
                }
                if let Some(value) = self.status.make_progress()? {
                    self.task =
                        Box::pin(self.spawner.clone().spawn(self.job_config.clone(), value));

                    Waker::wake_by_ref(cx.waker());
                }
                Poll::Pending
            }
            _ => {
                Waker::wake_by_ref(cx.waker());
                Poll::Pending
            }
        }
    }
}

impl<T, S, F, O> Job<T, S, F, O>
where
    T: Target + Send + Sync,
    S: Spawn<T::Output, Output = F> + Unpin + Clone,
    F: Future<Output = Result<O, anyhow::Error>> + Send,
{
    pub async fn new(
        id: u64,
        spawner: S,
        job_config: Pin<Arc<JobConfig>>,
        status: Pin<Arc<Status<T>>>,
    ) -> Result<Self, anyhow::Error> {
        if let Some(value) = status.make_progress()? {
            return Ok(Job {
                id,
                task: Box::pin(spawner.clone().spawn(job_config.clone(), value)),
                spawner,
                job_config,
                job_retries: 0,
                status,
            });
        }
        Err(anyhow!(
            "failed to build job, because test target already reached"
        ))
    }
}

pub trait Spawn<V> {
    type Output;

    fn spawn(self, job_state: Pin<Arc<JobConfig>>, value: V) -> Self::Output;
}

impl<F, Fut, V> Spawn<V> for F
where
    F: FnOnce(Pin<Arc<JobConfig>>, V) -> Fut,
    Fut: Future<Output = Result<(), anyhow::Error>>,
{
    type Output = Fut;

    fn spawn(self, job_state: Pin<Arc<JobConfig>>, value: V) -> Self::Output {
        (self)(job_state, value)
    }
}
