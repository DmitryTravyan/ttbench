use std::{pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};
use tarantool_rs::ExecutorExt;

use crate::{job::JobConfig, utils::calculate_bucket_id};

#[derive(Serialize, Deserialize, Debug)]
pub struct Branch {
    id: u64,
    abalance: u64,
    bucket_id: u32,
}

pub async fn vshard(job_config: Pin<Arc<JobConfig>>, id: u64) -> Result<(), anyhow::Error> {
    let conn = job_config.connections.get_connection();
    let bucket_id = calculate_bucket_id(id.to_string(), job_config.config.bucket_count);

    conn.call(
        "vshard.router.callrw",
        (
            bucket_id,
            "box.space.ttbench_branches:replace",
            (Branch {
                id,
                abalance: 0,
                bucket_id,
            },),
        ),
    )
    .await
    .unwrap()
    .decode_result::<Branch>()?;

    Ok(())
}
