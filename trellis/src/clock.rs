//! The engine's monotonic clock. Every engine read of the time goes through
//! [`now`] (and [`elapsed`]), never `Instant::now()` directly, so a test can
//! step the clock under a running engine.
//!
//! In a production build [`now`] is `Instant::now()`, inlined: there is no
//! clock object and nothing to configure. Under `cfg(any(test, feature =
//! "test-util"))` a thread may carry an [`EngineClock`], an offset from real
//! time that a test steps forward or back ([`EngineClock::step`]). [`now`]
//! adds the current thread's offset; a thread with none reads real time.
//!
//! # Which threads carry it
//!
//! A clock is per engine, not per process, so a test stepping one engine's
//! clock never moves another test's engine in the same binary. A client
//! inherits the clock of the thread that starts it
//! ([`crate::Client::start_with_config`]): its driving thread and every
//! worker and blocking-pool thread of its runtime enter it ([`Inherit`]), as
//! they enter its instance name (`crate::instance_log`). A harness enters a
//! clock only around the start of the engine it steps, so its own waits
//! keep real time.
//!
//! # What a step means
//!
//! A forward step is time the engine did not see pass: a process frozen
//! for hours (a stopped container, a paused VM). A step back is a clock that
//! regressed, which `Instant` does not promise can't happen. Every engine
//! comparison of instants saturates (`saturating_duration_since`), so a step
//! back only makes what is due later.
//!
//! Timers keep real time: a `tokio::time::sleep` is pacing, and the engine
//! decides what is due by reading [`now`] when it wakes. A deadline handed to
//! tokio converts through [`tokio_deadline`], so it lands the same distance
//! from real time as it stands from [`now`].

use std::time::{Duration, Instant};

/// The engine's current instant.
#[cfg(not(any(test, feature = "test-util")))]
#[inline(always)]
pub(crate) fn now() -> Instant {
    Instant::now()
}

/// The engine's current instant: real time plus the current thread's
/// [`EngineClock`] offset, if it carries one.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn now() -> Instant {
    stepped::now()
}

/// How long ago `since` was by [`now`]; zero if `since` is later (a clock
/// stepped back).
#[inline]
pub(crate) fn elapsed(since: Instant) -> Duration {
    now().saturating_duration_since(since)
}

/// The tokio deadline as far from tokio's clock as `at` is from [`now`].
#[cfg(not(any(test, feature = "test-util")))]
#[inline(always)]
pub(crate) fn tokio_deadline(at: Instant) -> tokio::time::Instant {
    tokio::time::Instant::from_std(at)
}

/// The tokio deadline as far from tokio's clock as `at` is from [`now`].
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn tokio_deadline(at: Instant) -> tokio::time::Instant {
    let now = now();
    let real = tokio::time::Instant::now();
    match at.checked_duration_since(now) {
        Some(ahead) => real + ahead,
        None => real
            .checked_sub(now.saturating_duration_since(at))
            .unwrap_or(real),
    }
}

/// The current thread's clock, carried onto the threads a client starts.
/// A zero-sized no-op in a production build.
#[derive(Clone, Default)]
pub(crate) struct Inherit {
    #[cfg(any(test, feature = "test-util"))]
    clock: Option<EngineClock>,
}

impl Inherit {
    /// The clock the current thread carries, if any.
    pub(crate) fn capture() -> Self {
        Self {
            #[cfg(any(test, feature = "test-util"))]
            clock: stepped::current(),
        }
    }

    /// Puts the captured clock on the current thread for the rest of its
    /// life.
    pub(crate) fn enter(&self) {
        #[cfg(any(test, feature = "test-util"))]
        stepped::set(self.clock.clone());
    }
}

#[cfg(any(test, feature = "test-util"))]
pub use stepped::{EngineClock, Entered};

#[cfg(any(test, feature = "test-util"))]
mod stepped {
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::{Duration, Instant};

    thread_local! {
        static CURRENT: RefCell<Option<EngineClock>> = const { RefCell::new(None) };
    }

    /// A steppable engine clock: an offset from real time, shared by every
    /// thread that carries it. **Test-only.** See the module doc.
    #[derive(Clone, Debug, Default)]
    pub struct EngineClock {
        offset_nanos: Arc<AtomicI64>,
    }

    impl EngineClock {
        /// A clock at real time.
        pub fn new() -> Self {
            Self::default()
        }

        /// Moves the clock `seconds` forward (negative: back). Steps add
        /// up. Every thread carrying the clock reads the new time at once.
        pub fn step(&self, seconds: i64) {
            self.offset_nanos
                .fetch_add(seconds.saturating_mul(1_000_000_000), Ordering::SeqCst);
        }

        /// How far the clock stands from real time, in seconds.
        pub fn offset_seconds(&self) -> i64 {
            self.offset_nanos.load(Ordering::SeqCst) / 1_000_000_000
        }

        /// Puts this clock on the current thread until the guard drops, so a
        /// client started meanwhile inherits it.
        pub fn enter(&self) -> Entered {
            Entered {
                previous: CURRENT.with(|c| c.replace(Some(self.clone()))),
            }
        }

        fn read(&self, real: Instant) -> Instant {
            let offset = self.offset_nanos.load(Ordering::SeqCst);
            let by = Duration::from_nanos(offset.unsigned_abs());
            let stepped = if offset >= 0 {
                real.checked_add(by)
            } else {
                real.checked_sub(by)
            };
            // A step back past the platform's earliest instant reads as
            // that earliest instant would: as far back as the clock goes.
            stepped.unwrap_or(real)
        }
    }

    /// Restores the thread's previous clock when dropped.
    #[must_use = "the clock is entered only while the guard lives"]
    pub struct Entered {
        previous: Option<EngineClock>,
    }

    impl Drop for Entered {
        fn drop(&mut self) {
            let previous = self.previous.take();
            let _ = CURRENT.try_with(|c| *c.borrow_mut() = previous);
        }
    }

    pub(super) fn now() -> Instant {
        let real = Instant::now();
        CURRENT
            .try_with(|c| c.borrow().as_ref().map(|clock| clock.read(real)))
            .ok()
            .flatten()
            .unwrap_or(real)
    }

    pub(super) fn current() -> Option<EngineClock> {
        CURRENT.try_with(|c| c.borrow().clone()).ok().flatten()
    }

    pub(super) fn set(clock: Option<EngineClock>) {
        let _ = CURRENT.try_with(|c| *c.borrow_mut() = clock);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A step moves `now` on the thread that carries the clock, and only
    /// there; dropping the guard restores real time.
    #[test]
    fn a_step_moves_the_entered_thread_only() {
        let clock = EngineClock::new();
        let entered = clock.enter();
        clock.step(3 * 3600);
        let lead = now().saturating_duration_since(Instant::now());
        assert!(lead >= Duration::from_secs(3 * 3600 - 1), "{lead:?}");
        let other = std::thread::spawn(|| now().saturating_duration_since(Instant::now()))
            .join()
            .unwrap();
        assert!(other < Duration::from_secs(1), "{other:?}");
        drop(entered);
        assert!(now().saturating_duration_since(Instant::now()) < Duration::from_secs(1));
    }

    /// A step back makes `elapsed` saturate at zero rather than panic, and
    /// steps add up.
    #[test]
    fn a_step_back_saturates() {
        let clock = EngineClock::new();
        let _entered = clock.enter();
        let before = now();
        clock.step(-30);
        assert_eq!(elapsed(before), Duration::ZERO);
        clock.step(60);
        assert_eq!(clock.offset_seconds(), 30);
        assert!(elapsed(before) >= Duration::from_secs(29));
    }

    /// `Inherit` carries the clock onto another thread.
    #[test]
    fn inherit_carries_the_clock_onto_a_new_thread() {
        let clock = EngineClock::new();
        let _entered = clock.enter();
        clock.step(86_400);
        let inherit = Inherit::capture();
        let lead = std::thread::spawn(move || {
            inherit.enter();
            now().saturating_duration_since(Instant::now())
        })
        .join()
        .unwrap();
        assert!(lead >= Duration::from_secs(86_399), "{lead:?}");
    }

    /// A deadline `d` ahead of the stepped clock is `d` ahead of tokio's.
    #[tokio::test]
    async fn a_tokio_deadline_keeps_its_distance_from_now() {
        let clock = EngineClock::new();
        let _entered = clock.enter();
        clock.step(86_400);
        let at = now() + Duration::from_secs(5);
        let ahead = tokio_deadline(at) - tokio::time::Instant::now();
        assert!(
            ahead <= Duration::from_secs(5) && ahead >= Duration::from_secs(4),
            "{ahead:?}"
        );
        clock.step(-2 * 86_400);
        let past = now() - Duration::from_secs(5);
        assert!(tokio_deadline(past) <= tokio::time::Instant::now());
    }
}
