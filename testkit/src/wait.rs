//! Waiting for a condition another task or session brings about, with a
//! hard deadline and a failure that says what was awaited and what was last
//! seen.
//!
//! A wait is only for a condition the test cannot step to itself (a
//! background worker's progress, a session reaching a lock). Each probe runs
//! to completion before the next, [`POLL`] apart, and the deadline is checked
//! only after a probe that came back unsatisfied, so a slow probe on a
//! loaded box costs time, never a spurious failure before the condition
//! was checked once.

use std::fmt::{Debug, Display};
use std::time::{Duration, Instant};

/// How long a wait sleeps between probes.
pub const POLL: Duration = Duration::from_millis(10);

/// Probes until `probe` returns `Ok`, and returns its value. `Err` carries
/// what the probe saw instead, and the last one is in the panic when
/// `timeout` runs out: "timed out after {timeout:?} waiting until {what};
/// last seen: {observation:?}".
pub async fn wait_for<T, E: Debug>(
    what: impl Display,
    timeout: Duration,
    mut probe: impl AsyncFnMut() -> Result<T, E>,
) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        match probe().await {
            Ok(value) => return value,
            Err(seen) => {
                if Instant::now() >= deadline {
                    panic!("timed out after {timeout:?} waiting until {what}; last seen: {seen:?}");
                }
            }
        }
        tokio::time::sleep(POLL).await;
    }
}

/// [`wait_for`] a probe that answers only whether the condition holds:
/// panics "timed out after {timeout:?} waiting until {what}" when `timeout`
/// runs out.
pub async fn wait_until(
    what: impl Display,
    timeout: Duration,
    mut probe: impl AsyncFnMut() -> bool,
) {
    let deadline = Instant::now() + timeout;
    while !probe().await {
        if Instant::now() >= deadline {
            panic!("timed out after {timeout:?} waiting until {what}");
        }
        tokio::time::sleep(POLL).await;
    }
}
