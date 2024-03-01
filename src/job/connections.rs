use std::{
    ops::{Add, AddAssign, SubAssign},
    sync::Mutex,
};

use tarantool_rs::Connection;

use crate::config::ConnectionConfig;

pub struct Pool {
    index: Mutex<usize>,
    pool: Vec<Connection>,
}

impl Pool {
    pub async fn new(instances: &Vec<ConnectionConfig>) -> Result<Self, anyhow::Error> {
        if instances.is_empty() {
            return Err(anyhow::anyhow!("Instance list can't be empty!"));
        }

        let mut pool = Vec::new();

        for ConnectionConfig {
            addr,
            timeout,
            user,
            password,
            connections,
        } in instances
        {
            for _ in 0..*connections {
                pool.push(
                    Connection::builder()
                        .timeout(*timeout)
                        .auth(user, *password)
                        .build(*addr)
                        .await?,
                );
            }
        }

        Ok(Self {
            index: Mutex::new(0),
            pool,
        })
    }

    pub fn get_connection(&self) -> &Connection {
        let mut locked = self.index.lock().unwrap();
        let mut index = locked.add(1);
        if index == self.pool.len() {
            locked.sub_assign(self.pool.len() - 1);
            index = 0;
        } else {
            locked.add_assign(1);
        }
        &self.pool[index]
    }
}
