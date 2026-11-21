//! The roles a case runs as (`docs/generative-test-suite.md` §1, "Roles").
//!
//! A case provisioned by [`CaseDatabase::provision`] runs the way
//! `docs/recommendations.md` ("Roles and permissions") sets a deployment
//! up:
//!
//! - [`APP_ROLE`], `LOGIN`, the application: it creates the source tables in
//!   `public`, so it owns them, and makes every application write.
//! - [`TRELLIS_ROLE`], `LOGIN`, not superuser, no `BYPASSRLS`: a member of
//!   [`APP_ROLE`] with `INHERIT`, holding `CREATE` on the database and
//!   nothing else. It runs the migrations, so it owns the instance schema,
//!   and every Trellis connection: definitions, the staging and drain
//!   workers, the oracle's pool and the harness's catalog reads.
//! - The cluster's bootstrap superuser, only for [`Roles::admin`]: actions an
//!   operator, not Trellis, takes (`CHECKPOINT`).
//!
//! Targets go in `public` (the default target schema) or in
//! [`TARGETS_SCHEMA`], which Trellis owns, as the recommendations set it up;
//! `ManualBackend::install` draws which from the program. Trellis's `CREATE`
//! on `public` comes through its membership in [`APP_ROLE`], which holds it
//! as the schema's application owner would.
//!
//! The privilege tier (`crate::run::source_ddl`) uses three more roles,
//! none of which Trellis inherits unless named: [`WRITER_ROLE`], a login
//! the application's writes switch to while a change takes the owner's
//! privileges away; [`OTHER_OWNER_ROLE`], an owner Trellis doesn't inherit;
//! and [`HEIR_ROLE`], an owner Trellis and the application both inherit.

use testkit::{TestCluster, TestDatabase};
use trellis::Config;

/// The role every Trellis connection of a provisioned case logs in as.
pub const TRELLIS_ROLE: &str = "trellis_runner";

/// The application role: owns the source tables and makes every write.
pub const APP_ROLE: &str = "app_owner";

/// The recommended target schema (`docs/recommendations.md`), owned by
/// [`TRELLIS_ROLE`], with `USAGE` for [`APP_ROLE`] as a reader's.
pub const TARGETS_SCHEMA: &str = "trellis_targets";

/// A login holding `SELECT`, `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` on
/// the sources by grant, and no role Trellis inherits: the application's
/// writes run as it while a change takes the owner's privileges away.
pub const WRITER_ROLE: &str = "app_writer";

/// A role neither Trellis nor the application is a member of.
pub const OTHER_OWNER_ROLE: &str = "other_owner";

/// A role both [`TRELLIS_ROLE`] and [`APP_ROLE`] are members of, with
/// `INHERIT`.
pub const HEIR_ROLE: &str = "app_heir";

/// The connection strings of a case's three roles. [`Roles::single`] is the
/// legacy shape: one role (the bootstrap superuser) for everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roles {
    /// Every Trellis connection.
    pub trellis: String,
    /// Source DDL and every application write.
    pub app: String,
    /// Operator actions that need more than either role holds.
    pub admin: String,
}

impl Roles {
    /// One connection string for all three roles.
    pub fn single(dsn: impl Into<String>) -> Self {
        let dsn = dsn.into();
        Self {
            trellis: dsn.clone(),
            app: dsn.clone(),
            admin: dsn,
        }
    }

    /// Whether application statements run as a role of their own.
    pub fn separate_app(&self) -> bool {
        self.app != self.trellis
    }

    /// [`WRITER_ROLE`]'s connection string, for a provisioned case.
    pub fn writer(&self) -> String {
        as_role(&self.admin, WRITER_ROLE)
    }
}

/// Opens the application's session when `roles` gives it a role of its own
/// (`super::roles`), with `search_path` at `source_schema` (`public` when
/// `None`), where its source tables go. `None` means application statements
/// share the raw session.
pub(super) async fn connect_app(
    roles: &Roles,
    source_schema: Option<&str>,
) -> Result<Option<tokio_postgres::Client>, tokio_postgres::Error> {
    if !roles.separate_app() {
        return Ok(None);
    }
    connect_app_as(&roles.app).await.map(Some)
}

/// An application session logged in with `dsn`, set up as [`connect_app`]
/// sets one up.
pub(super) async fn connect_app_as(
    dsn: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (app, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    app.batch_execute(&format!(
        "set search_path to {}; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'; \
         set timezone to 'UTC'; set intervalstyle to 'postgres'",
        source_schema.map_or_else(|| "public".to_string(), |schema| format!("\"{schema}\""))
    ))
    .await?;
    Ok(app)
}

/// A fresh database set up as the module doc describes, migrated by
/// [`TRELLIS_ROLE`]. Dropped with the inner [`TestDatabase`].
pub struct CaseDatabase {
    db: TestDatabase,
    roles: Roles,
}

impl CaseDatabase {
    /// Creates the cluster-wide roles if this cluster lacks them, then an
    /// empty database granted and migrated as the module doc describes.
    pub async fn provision(cluster: &TestCluster) -> Self {
        let db = cluster.create_empty_database().await;
        let admin = db.dsn().to_string();
        let roles = Roles {
            trellis: as_role(&admin, TRELLIS_ROLE),
            app: as_role(&admin, APP_ROLE),
            admin,
        };
        let (client, connection) = tokio_postgres::connect(&roles.admin, tokio_postgres::NoTls)
            .await
            .expect("connect as the bootstrap superuser");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&create_roles_sql())
            .await
            .expect("create the case roles");
        client
            .batch_execute(&database_grants_sql(db.name()))
            .await
            .expect("grant the case roles");
        let config = Config::from_dsn(roles.trellis.clone()).expect("trellis config");
        trellis::migrate(&config)
            .await
            .unwrap_or_else(|err| panic!("migrate as {TRELLIS_ROLE}: {err}"));
        Self { db, roles }
    }

    /// The Trellis role's connection string.
    pub fn dsn(&self) -> &str {
        &self.roles.trellis
    }

    /// All three roles' connection strings.
    pub fn roles(&self) -> &Roles {
        &self.roles
    }

    /// The database's name.
    pub fn name(&self) -> &str {
        self.db.name()
    }
}

/// `dsn`, logging in as `role`.
pub fn as_role(dsn: &str, role: &str) -> String {
    assert!(dsn.contains("user=postgres"), "{dsn}");
    dsn.replace("user=postgres", &format!("user={role}"))
}

/// Creates the roles and memberships, tolerating a concurrent creator:
/// roles are cluster-wide and cases on one cluster can provision at once.
fn create_roles_sql() -> String {
    let create = |role: &str, login: &str| {
        format!(
            "do $roles$ begin \
               if not exists (select from pg_roles where rolname = '{role}') then \
                 create role {role} {login} nosuperuser nobypassrls; \
               end if; \
             exception when duplicate_object or unique_violation then null; \
             end $roles$;"
        )
    };
    format!(
        "{} {} {} {} {} grant {APP_ROLE} to {TRELLIS_ROLE}; \
         grant {HEIR_ROLE} to {TRELLIS_ROLE}, {APP_ROLE};",
        create(APP_ROLE, "login"),
        create(TRELLIS_ROLE, "login"),
        create(WRITER_ROLE, "login"),
        create(OTHER_OWNER_ROLE, "nologin"),
        create(HEIR_ROLE, "nologin"),
    )
}

/// The grants on one database: what the recommendations list for
/// [`TRELLIS_ROLE`] beyond `PUBLIC`'s defaults, `CREATE` on `public` for
/// [`APP_ROLE`] (`PUBLIC` holds it only before PostgreSQL 15), and
/// [`TARGETS_SCHEMA`] as the recommendations' setup creates it.
fn database_grants_sql(database: &str) -> String {
    format!(
        "grant create on database \"{database}\" to {TRELLIS_ROLE}; \
         grant create, usage on schema public to {APP_ROLE}; \
         create schema {TARGETS_SCHEMA} authorization {TRELLIS_ROLE}; \
         grant usage on schema {TARGETS_SCHEMA} to {APP_ROLE};"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_role_swaps_only_the_user() {
        assert_eq!(
            as_role("host=/tmp port=5 user=postgres dbname=x", APP_ROLE),
            "host=/tmp port=5 user=app_owner dbname=x"
        );
    }

    #[test]
    fn a_single_role_has_no_separate_app() {
        assert!(!Roles::single("dsn").separate_app());
    }
}
