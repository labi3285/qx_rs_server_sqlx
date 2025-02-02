
#[allow(unused)]


use sqlx::database::HasArguments;
use sqlx::query::Query;
use sqlx::{IntoArguments, MySql, MySqlConnection};
use sqlx::{mysql::{MySqlRow, MySqlArguments}, query::QueryAs, FromRow};

use qx_rs_server::err::{Error, Result};


pub async fn exec_arr<'q, T>(conn: &mut MySqlConnection, sql_as: QueryAs<'q, MySql, T, MySqlArguments>) -> Result<Vec<T>> 
where
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
{
    let res = sql_as.fetch_all(&mut *conn).await;
    match res {
        Ok(users) => {
            Ok(users)
        },
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("exec_arr failed:{:?}", err)));
        }
    }
}

pub async fn exec_one<'q, T>(conn: &mut MySqlConnection, sql_as: QueryAs<'q, MySql, T, MySqlArguments>) -> Result<T> 
where
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
{
    let res = sql_as.fetch_one(&mut *conn).await;
    match res {
        Ok(arr) => Ok(arr),
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("exec_one failed:{:?}", err)));
        }
    }
}

pub async fn exec_opt_one<'q, T>(conn: &mut MySqlConnection, sql_as: QueryAs<'q, MySql, T, MySqlArguments>) -> Result<Option<T>> 
where
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
{
    let res = sql_as.fetch_optional(&mut *conn).await;
    match res {
        Ok(a) => Ok(a),
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("exec_opt_one failed:{:?}", err)));
        }
    }
}


pub async fn exec<'q>(conn: &mut MySqlConnection, sql: Query<'q, MySql, <MySql as HasArguments<'_>>::Arguments>) -> Result<(u64, u64)> 
{
    let res = sql.execute(&mut *conn).await;
    match res {
        Ok(a) => Ok((a.rows_affected(), a.last_insert_id())),
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("exec failed:{:?}", err)));
        }
    }
}


pub fn query<'q>(sql: &'q str) -> Query<'q, MySql, <MySql as HasArguments<'_>>::Arguments>
{
    sqlx::query(sql)
}

pub fn query_as<'q, T>(sql: &'q str) -> QueryAs<'q, MySql, T, MySqlArguments>
where
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
{
    sqlx::query_as::<_, T>(sql)
}

pub async fn query_as_with<'q, T, A: 'q>(connect: &mut MySqlConnection, sql: &'q str, args: A) -> Result<Vec<T>> 
where
    T: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
    A: IntoArguments<'q, MySql>,
{
    let res = sqlx::query_as_with::<_, T, A>(sql, args)
        .fetch_all(&mut *connect).await;
    match res {
        Ok(users) => {
            Ok(users)
        },
        Err(err) => {
            tracing::error!("{}", err);
            return Err(Error::Database(format!("query_as_with failed:{:?}", err)));
        }
    }
}
