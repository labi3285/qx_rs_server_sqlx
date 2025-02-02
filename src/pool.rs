#![allow(unused)]

use once_cell::sync::Lazy;
use sqlx::Acquire;
use sqlx::{
    self,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    pool::PoolConnection,
    MySql, Pool, Transaction,
};
use tokio::sync::Mutex;
use std::{collections::HashMap, str::FromStr};
use tracing::{self, instrument};

use qx_rs_server::err::{Error, Result};
use qx_rs_server::env::{self, DEFAULT};


static POOLS: Lazy<Mutex<HashMap<&'static str, Pool<MySql>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

#[instrument]
pub async fn get_conn() -> Result<PoolConnection<MySql>> {
    _get_conn(DEFAULT).await
}

#[instrument]
pub async fn get_conn_from_database(
    which_database: &'static str,
) -> Result<PoolConnection<MySql>> {
    _get_conn(which_database).await
}

#[instrument]
pub async fn setup() -> Result<()> {
    _setup(DEFAULT).await
}

#[instrument]
pub async fn setup_database(which_database: &'static str) -> Result<()> {
    _setup(which_database).await
}

#[instrument]
pub async fn get_trans<'q>(
    conn: &'q mut PoolConnection<MySql>,
) -> Result<Transaction<'q, sqlx::MySql>> {
    _get_trans(&mut *conn).await
}

#[instrument]
pub async fn commit<'q>(trans: Transaction<'q, MySql>) -> Result<()> {
    let res = trans.commit().await;
    match res {
        Ok(_) => Ok(()),
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("commit failed:{:?}", err)));
        }
    }
}

#[instrument]
pub async fn rollback<'q>(trans: Transaction<'q, MySql>) -> Result<()> {
    let res = trans.rollback().await;
    match res {
        Ok(_) => Ok(()),
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("rollback failed:{:?}", err)));
        }
    }
}

async fn _get_trans<'q>(
    conn: &'q mut PoolConnection<MySql>,
) -> Result<Transaction<'q, sqlx::MySql>> {
    let res = conn.begin().await;
    match res {
        Ok(tx) => Ok(tx),
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("_get_trans failed:{:?}", err)));
        }
    }
}

async fn _get_conn(which_database: &'static str) -> Result<PoolConnection<MySql>> {
    let map = POOLS.lock().await;
    let res = map.get(which_database);
    if let Some(pool) = res {
        let connect = pool.acquire().await;
        match connect {
            Ok(con) => Ok(con),
            Err(err) => {
                tracing::error!("{}", err);
                return Err(Error::Database(format!("_get_conn acquire failed:{:?}", err)));
            }
        }
    } else {
        return Err(Error::Database("_get_conn failed".to_string()));
    }
}

async fn _setup(which_database: &'static str) -> Result<()> {
    let mut which = "MYSQL".to_string();
    if which_database != DEFAULT {
        which = format!("MYSQL.{}", which_database);
    }
    let url = env::str(&format!("{}.URL", which))?;
    let database: String = env::str(&format!("{}.DATABASE", which))?;
    let user_name = env::str(&format!("{}.USER_NAME", which))?;
    let password = env::str(&format!("{}.PASSWORD", which))?;
    let max_connects = env::val::<u32>(&format!("{}.MAX_CONNECTS", which))?;
    let full_url = format!("mysql://{}:{}@{}/{}", user_name, password, url, database);

    tracing::info!("full_url: {}", full_url);

    tracing::info!("connecting database: {}", database);
    let res = MySqlConnectOptions::from_str(&full_url);
    match res {
        Ok(connection_options) => {
            let res = MySqlPoolOptions::new()
                .max_connections(max_connects)
                .connect_with(connection_options)
                .await;
            match res {
                Ok(pool) => {
                    let mut map = POOLS.lock().await;
                    map.insert(which_database, pool);
                    tracing::info!("database connected");
                    Ok(())
                }
                Err(err) => {
                    tracing::error!("{}", err);
                    return Err(Error::Database(format!("_setup connect_with failed:{:?}", err)));
                }
            }
        }
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("_setup from_str failed:{:?}", err)));
        }
    }
}
