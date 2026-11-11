//! Plain `tokio_postgres` connections for tests: a client outside any
//! pool, for reference queries, catalog reads and sessions a test holds
//! open on purpose.

use tokio_postgres::{Client, NoTls};

/// Connects to `dsn` and runs `setup` (session `SET`s, say) on the new
/// session; an empty `setup` runs nothing. Panics if either fails, naming
/// which.
pub async fn connect_with(dsn: &str, setup: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    if !setup.is_empty() {
        client
            .batch_execute(setup)
            .await
            .unwrap_or_else(|e| panic!("session setup `{setup}`: {e}"));
    }
    client
}

/// The session [`connect_raw`] opens: the engine's schema first on the
/// path, and date and `bytea` output pinned to what the engine's own pool
/// reads and the tests compare against.
#[cfg(feature = "engine-access")]
pub fn engine_session() -> String {
    format!(
        "set search_path to {}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'",
        trellis::dev::config::DEFAULT_SCHEMA
    )
}

/// [`connect_with`] the [`engine_session`].
#[cfg(feature = "engine-access")]
pub async fn connect_raw(dsn: &str) -> Client {
    connect_with(dsn, &engine_session()).await
}

#[cfg(feature = "engine-access")]
impl crate::TestDatabase {
    /// [`connect_raw`] to this database.
    pub async fn connect_raw(&self) -> Client {
        connect_raw(self.dsn()).await
    }
}
