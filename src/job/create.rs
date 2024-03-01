use std::{pin::Pin, sync::Arc};

use serde::Serialize;
use tarantool_rs::{ExecutorExt, Tuple};

use crate::config::InitStep;

use super::JobConfig;

#[derive(Serialize)]
pub struct CreateSpaceOptions {
    if_not_exists: bool,
    format: Vec<FieldFormat>,
}

impl Tuple for CreateSpaceOptions {
    fn encode_into_writer<W: std::io::Write>(
        &self,
        mut buf: W,
    ) -> Result<(), tarantool_rs::errors::EncodingError> {
        rmp_serde::encode::write_named(&mut buf, self)?;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct FieldFormat {
    name: &'static str,
    r#type: &'static str,
}

impl Tuple for FieldFormat {
    fn encode_into_writer<W: std::io::Write>(
        &self,
        mut buf: W,
    ) -> Result<(), tarantool_rs::errors::EncodingError> {
        rmp_serde::encode::write_named(&mut buf, self)?;
        Ok(())
    }
}

pub async fn vshard(job_config: Pin<Arc<JobConfig>>, _id: u64) -> Result<(), anyhow::Error> {
    let conn = job_config.connections.get_connection();

    conn.eval(
        r#"
local service_registry = require("cartridge.service-registry")
if service_registry.get("vshard-router") then
    _G.vshard = require("vshard")
end               
return true
            "#,
        (),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())
    .map(|created| {
        if created {
            println!("_G.vshard created")
        } else {
            println!("Error")
        }
    })?;

    conn.eval(
        r#"
local create_primary, create_secondary = ...
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

    local _, err = conn:eval([[
        local create_primary, create_secondary = ...
        box.schema.create_space("ttbench_accounts", {
            if_not_exists = true,
            format = {
                { name = "aid", type = "unsigned" },
                { name = "abalance", type = "unsigned" },
                { name = "bucket_id", type = "unsigned" },
            }
        })

        if create_primary then
            box.space.ttbench_accounts:create_index("primary", {
                type = "hash",
                unique = true,
                if_not_exists = true,
                parts = { "aid" }
            })
        end

        if create_secondary then
            box.space.ttbench_accounts:create_index("abalance", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "abalance" }
            })
        end
    ]], {create_primary, create_secondary})
    if err then
        return nil, err
    end
end
return true
        "#,
        (
            job_config.config.contains_step(&InitStep::Primary),
            job_config.config.contains_step(&InitStep::Foreign),
        ),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())?;

    conn.eval(
        r#"
local create_primary, create_secondary = ...
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

    local _, err = conn:eval([[
        local create_primary, create_secondary = ...
        box.schema.create_space("ttbench_tellers", {
            if_not_exists = true,
            format = {
                { name = "tid", type = "unsigned" },
                { name = "tbalance", type = "unsigned" },
                { name = "bucket_id", type = "unsigned" },
            }
        })

        if create_primary then
            box.space.ttbench_tellers:create_index("primary", {
                type = "hash",
                unique = true,
                if_not_exists = true,
                parts = { "tid" }
            })
        end

        if create_secondary then
            box.space.ttbench_tellers:create_index("tbalance", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "tbalance" }
            })
        end
    ]], {create_primary, create_secondary})
    if err then
        return nil, err
    end
end
return true
        "#,
        (
            job_config.config.contains_step(&InitStep::Primary),
            job_config.config.contains_step(&InitStep::Foreign),
        ),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())?;

    conn.eval(
        r#"
local create_primary, create_secondary = ...
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

    local _, err = conn:eval([[
        local create_primary, create_secondary = ...
        box.schema.create_space("ttbench_branches", {
            if_not_exists = true,
            format = {
                { name = "bid", type = "unsigned" },
                { name = "bbalance", type = "unsigned" },
                { name = "bucket_id", type = "unsigned" },
            }
        })

        if create_primary then
            box.space.ttbench_branches:create_index("primary", {
                type = "hash",
                unique = true,
                if_not_exists = true,
                parts = { "bid" }
            })
        end

        if create_secondary then
            box.space.ttbench_branches:create_index("bbalance", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "bbalance" }
            })
        end
    ]], {create_primary, create_secondary})
    if err then
        return nil, err
    end
end
return true
        "#,
        (
            job_config.config.contains_step(&InitStep::Primary),
            job_config.config.contains_step(&InitStep::Foreign),
        ),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())?;

    conn.eval(
        r#"
local create_primary, create_secondary = ...
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

    local _, err = conn:eval([[
        local create_primary, create_secondary = ...
        box.schema.create_space("ttbench_history", {
            if_not_exists = true,
            format = {
                { name = "uuid", type = "string" },
                { name = "tid", type = "unsigned" },
                { name = "bid", type = "unsigned" },
                { name = "aid", type = "unsigned" },
                { name = "delta", type = "unsigned" },
                { name = "time", type = "unsigned" },
                { name = "bucket_id", type = "unsigned" },
            }
        })

        if create_primary then
            box.space.ttbench_history:create_index("primary", {
                type = "hash",
                unique = true,
                if_not_exists = true,
                parts = { "uuid" }
            })
        end

        if create_secondary then
            box.space.ttbench_history:create_index("tid", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "tid" }
            })

            box.space.ttbench_history:create_index("bid", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "bid" }
            })

            box.space.ttbench_history:create_index("aid", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "aid" }
            })

            box.space.ttbench_history:create_index("delta", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "delta" }
            })

            box.space.ttbench_history:create_index("time", {
                type = "tree",
                unique = false,
                if_not_exists = true,
                parts = { "time" }
            })
        end
    ]], {create_primary, create_secondary})
    if err then
        return nil, err
    end
end
return true
        "#,
        (
            job_config.config.contains_step(&InitStep::Primary),
            job_config.config.contains_step(&InitStep::Foreign),
        ),
    )
    .await
    .and_then(|response| response.decode_result::<bool>())?;

    Ok(())
}
