use std::sync::{atomic::AtomicU64, Arc, Mutex};

use job::tpcb::TransactionGenerator;
use status::{iterations::Iterations, Progress};

mod args;
mod config;
mod job;
mod status;
mod utils;

#[cfg(not(tarpaulin_include))]
pub fn main() -> Result<(), anyhow::Error> {
    let cli = args::init(std::env::args());
    let config = config::Config::new(&cli)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        // TODO Продумать количество тредов
        .worker_threads(config.jobs as usize + 1)
        .enable_all()
        .build()?;

    runtime.block_on(run_bench(config))?;

    Ok(())
}

async fn run_bench(config: config::Config) -> Result<(), anyhow::Error> {
    let connections = job::connections::Pool::new(&config.instances).await?;
    let job_config = Arc::pin(job::JobConfig {
        config: config.clone(),
        connections,
    });

    if job_config.config.contains_step(&config::InitStep::Create) {
        let status = Arc::pin(status::Status::single_run());
        tokio::spawn(
            job::Job::new(0, job::create::vshard, job_config.clone(), status.clone()).await?,
        );
        status.wait_the_end().await?;
    }

    if job_config
        .config
        .contains_step(&config::InitStep::GenerateData)
    {
        let status = Arc::pin(status::Status {
            target: status::iterations::Iterations::new(
                AtomicU64::new(0),
                Arc::new(Mutex::new(config.test_config.ttbench_accounts)),
            ),
            progress: Progress::default(),
            logger: status::Logger {},
        });
        for id in 0..config.jobs {
            tokio::spawn(
                job::Job::new(
                    id,
                    job::accounts::vshard,
                    job_config.clone(),
                    status.clone(),
                )
                .await?,
            );
        }
        status.wait_the_end().await?;

        let jobs = if config.jobs > config.test_config.ttbench_tellers {
            config.test_config.ttbench_tellers
        } else {
            config.jobs
        };
        let status = Arc::pin(status::Status {
            target: status::iterations::Iterations::new(
                AtomicU64::new(0),
                Arc::new(Mutex::new(config.test_config.ttbench_tellers)),
            ),
            progress: Progress::default(),
            logger: status::Logger {},
        });
        for id in 0..jobs {
            tokio::spawn(
                job::Job::new(id, job::tellers::vshard, job_config.clone(), status.clone()).await?,
            );
        }
        status.wait_the_end().await?;

        let jobs = if config.jobs > config.test_config.ttbench_branches {
            config.test_config.ttbench_branches
        } else {
            config.jobs
        };
        let status = Arc::pin(status::Status {
            target: status::iterations::Iterations::new(
                AtomicU64::new(0),
                Arc::new(Mutex::new(config.test_config.ttbench_branches)),
            ),
            progress: Progress::default(),
            logger: status::Logger {},
        });
        for id in 0..jobs {
            tokio::spawn(
                job::Job::new(
                    id,
                    job::branches::vshard,
                    job_config.clone(),
                    status.clone(),
                )
                .await?,
            );
        }
        status.wait_the_end().await?;
    }

    let status = Arc::pin(status::Status::<Iterations<TransactionGenerator>>::try_from(&config)?);
    for id in 0..config.jobs {
        tokio::spawn(
            job::Job::new(id, job::tpcb::vshard, job_config.clone(), status.clone()).await?,
        );
    }
    status.wait_the_end().await?;
    println!("{}", status.progress);

    if job_config.config.contains_step(&config::InitStep::Drop) {
        let status = Arc::pin(status::Status {
            target: status::iterations::Iterations::new(AtomicU64::new(0), Arc::new(Mutex::new(1))),
            progress: Progress::default(),
            logger: status::Logger {},
        });
        tokio::spawn(
            job::Job::new(0, job::drop::vshard, job_config.clone(), status.clone()).await?,
        );
        status.wait_the_end().await?;
    }

    Ok(())
}
