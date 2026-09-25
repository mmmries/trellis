use anyhow::Result;
use tokio_postgres::{Client, NoTls};

pub fn conn_str(db: &str) -> String {
    let sock = std::env::var("EXP_SOCK").unwrap_or_else(|_| "/home/mike/exp558/a/sock".into());
    let port = std::env::var("EXP_PORT").unwrap_or_else(|_| "54321".into());
    format!("host={sock} port={port} user=postgres dbname={db}")
}

pub async fn connect(db: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(&conn_str(db), NoTls).await?;
    tokio::spawn(async move {
        // Scenarios end by terminating their backends; the dropped connection is expected noise.
        let _ = connection.await;
    });
    Ok(client)
}

/// `xid8` comes back from tokio-postgres only as text; parse it.
pub async fn xid8(client: &Client, sql: &str) -> Result<u64> {
    let row = client
        .query_one(&format!("select ({sql})::text"), &[])
        .await?;
    Ok(row.get::<_, String>(0).parse()?)
}

pub fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
