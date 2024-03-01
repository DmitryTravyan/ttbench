use std::{pin::Pin, sync::Arc};

use tarantool_rs::ExecutorExt;

use crate::job::JobConfig;

pub async fn vshard(job_config: Pin<Arc<JobConfig>>, _id: u64) -> Result<(), anyhow::Error> {
    let conn = job_config.connections.get_connection();

    conn.eval(
        r#"
local nb = require("net.box")
local shards, err = vshard.router.routeall()
if err then
    return nil, err
end
for _, shard in pairs(shards) do
    local uri = shard.master.uri
    local conn, err = nb.connect(uri)
    if err then
        return nil, err
    end    

    local _, err = conn:call("box.space.ttbench_accounts:drop")
    if err then
        return nil, err
    end
end
return true
        "#,
        (),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())?;

    conn.eval(
        r#"
local nb = require("net.box")
local shards, err = vshard.router.routeall()
if err then
    return nil, err
end
for _, shard in pairs(shards) do
    local uri = shard.master.uri
    local conn, err = nb.connect(uri)
    if err then
        return nil, err
    end    

    local _, err = conn:call("box.space.ttbench_tellers:drop")
    if err then
        return nil, err
    end
end
return true
        "#,
        (),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())?;

    conn.eval(
        r#"
local nb = require("net.box")
local shards, err = vshard.router.routeall()
if err then
    return nil, err
end
for _, shard in pairs(shards) do
    local uri = shard.master.uri
    local conn, err = nb.connect(uri)
    if err then
        return nil, err
    end    

    local _, err = conn:call("box.space.ttbench_branches:drop")
    if err then
        return nil, err
    end
end
return true
        "#,
        (),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())?;

    if !job_config.config.keep_history {
        conn.eval(
            r#"
local nb = require("net.box")
local shards, err = vshard.router.routeall()
if err then
    return nil, err
end
for _, shard in pairs(shards) do
    local uri = shard.master.uri
    local conn, err = nb.connect(uri)
    if err then
        return nil, err
    end    

    local _, err = conn:call("box.space.ttbench_history:drop")
    if err then
        return nil, err
    end
end
return true
        "#,
            (),
        )
        .await
        .and_then(|response| response.decode_result::<bool>())?;
    }

    Ok(())
}
