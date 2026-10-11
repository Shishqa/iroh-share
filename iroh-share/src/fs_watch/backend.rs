//! Narrow containment for notify's panic-prone registration and shutdown.
use std::panic::{catch_unwind, AssertUnwindSafe};

pub(super) fn attempt<T>(operation: impl FnOnce() -> notify::Result<T>) -> notify::Result<T> {
    catch_unwind(AssertUnwindSafe(operation))
        .unwrap_or_else(|_| Err(notify::Error::generic("watcher backend panicked")))
}

/// Never let registration panic unwind through a panic-prone watcher destructor.
/// The caller discards the backend after any operation error. Requires panic=unwind.
pub(super) struct Guarded<T>(Option<T>);
impl<T> Guarded<T> {
    pub(super) fn new(inner: T) -> Self {
        Self(Some(inner))
    }
    pub(super) fn call(
        &mut self,
        operation: impl FnOnce(&mut T) -> notify::Result<()>,
    ) -> notify::Result<()> {
        attempt(|| operation(self.0.as_mut().expect("initialized backend")))
    }
}
impl<T> Drop for Guarded<T> {
    fn drop(&mut self) {
        let result = attempt(|| {
            // Empty the owner before Drop can panic, preventing a second Drop.
            drop(self.0.take());
            Ok(())
        });
        if let Err(error) = result {
            tracing::warn!(%error, "watcher shutdown failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backend_panics_are_contained_child() {
        if std::env::var_os("IROH_FS_PANIC_CHILD").is_none() {
            return;
        }
        struct Panicky;
        impl Drop for Panicky {
            fn drop(&mut self) {
                panic!("synthetic destructor panic");
            }
        }
        let mut watcher = Guarded::new(Panicky);
        assert!(watcher
            .call(|_| panic!("synthetic registration panic"))
            .is_err());
        drop(watcher);
        assert!(attempt::<()>(|| panic!("synthetic constructor/thread creation panic")).is_err());
        // A distinctive exit code proves the exact child test actually ran.
        std::process::exit(42);
    }
    #[test]
    fn double_panic_regression_runs_in_disposable_subprocess() {
        use std::{
            process::{Command, Stdio},
            time::{Duration, Instant},
        };
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "fs_watch::backend::tests::backend_panics_are_contained_child",
            ])
            .env("IROH_FS_PANIC_CHILD", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(42));
                break;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("panic probe timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
