use std::{
    ops::SubAssign,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tarantool_rs::{Executor, ExecutorExt};
use uuid::Uuid;

use crate::{
    config::Config,
    job::JobConfig,
    status::{iterations::Iterations, Target},
    utils::calculate_bucket_id,
};

use super::{accounts::Account, branches::Branch, tellers::Teller};

pub struct TransactionGenerator {
    max_aid: u64,
    max_tid: u64,
    max_bid: u64,
    max_delta: u64,
}

impl TransactionGenerator {
    pub fn generate(&self) -> Transaction {
        let mut rng = rand::thread_rng();
        Transaction {
            uuid: Uuid::new_v4().to_string(),
            aid: rng.gen_range(0..self.max_aid),
            tid: rng.gen_range(0..self.max_tid),
            bid: rng.gen_range(0..self.max_bid),
            delta: rng.gen_range(0..self.max_delta),
            time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            bucket_id: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    uuid: String,
    tid: u64,
    bid: u64,
    aid: u64,
    delta: u64,
    time: u64,
    bucket_id: u32,
}

impl Target for Iterations<TransactionGenerator> {
    type Output = Transaction;

    fn new(config: &Config) -> Self {
        Iterations {
            generator: TransactionGenerator {
                max_aid: config.test_config.ttbench_accounts,
                max_tid: config.test_config.ttbench_tellers,
                max_bid: config.test_config.ttbench_branches,
                // TODO Решить какой именно будет диапазон у дельты
                max_delta: 1000,
            },
            counter: Arc::new(Mutex::new(0)),
        }
    }

    fn make_progress(&self) -> Result<Option<Self::Output>, anyhow::Error> {
        let mut locked = self.counter.lock().unwrap();
        if locked.eq(&0) {
            return Ok(None);
        }
        locked.sub_assign(1);
        Ok(Some(self.generator.generate()))
    }

    fn is_reached(&self) -> Result<bool, anyhow::Error> {
        let locked = self.counter.lock().unwrap();
        if locked.eq(&0) {
            return Ok(true);
        }
        Ok(false)
    }
}

pub async fn vshard(
    job_config: Pin<Arc<JobConfig>>,
    value: Transaction,
) -> Result<(), anyhow::Error> {
    let transaction = job_config
        .connections
        .get_connection()
        .transaction()
        .await?;

    let aid_bucket_id = calculate_bucket_id(value.aid.to_string(), job_config.config.bucket_count);
    transaction
        .call(
            "vshard.router.callrw",
            (
                aid_bucket_id,
                "box.space.ttbench_accounts:update",
                (value.aid, (("+", "abalance", value.delta),)),
            ),
        )
        .await
        .and_then(|response| response.decode_result::<Account>())?;

    transaction
        .call(
            "vshard.router.callbro",
            (
                aid_bucket_id,
                "box.space.ttbench_accounts:get",
                (value.aid,),
            ),
        )
        .await
        .and_then(|response| response.decode_result::<Account>())?;

    let tid_bucket_id = calculate_bucket_id(value.tid.to_string(), job_config.config.bucket_count);
    transaction
        .call(
            "vshard.router.callrw",
            (
                tid_bucket_id,
                "box.space.ttbench_tellers:update",
                (value.tid, (("+", "tbalance", value.delta),)),
            ),
        )
        .await
        .and_then(|response| response.decode_result::<Teller>())?;

    let bid_bucket_id = calculate_bucket_id(value.bid.to_string(), job_config.config.bucket_count);
    transaction
        .call(
            "vshard.router.callrw",
            (
                bid_bucket_id,
                "box.space.ttbench_branches:update",
                (value.bid, (("+", "bbalance", value.delta),)),
            ),
        )
        .await
        .and_then(|response| response.decode_result::<Branch>())?;

    let bucket_id = calculate_bucket_id(&value.uuid, job_config.config.bucket_count);
    transaction
        .call(
            "vshard.router.callrw",
            (
                bucket_id,
                "box.space.ttbench_history:insert",
                (Transaction { bucket_id, ..value },),
            ),
        )
        .await
        .and_then(|response| response.decode_result::<Transaction>())?;

    transaction.commit().await?;
    Ok(())
}
