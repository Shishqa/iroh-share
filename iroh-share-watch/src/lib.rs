//! Event-driven wakeups; content validation and the two-scan debounce stay in the caller.
use notify::{
    event::{AccessKind, AccessMode},
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};

const FALLBACK: Duration = Duration::from_secs(600);
const RETRY: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
pub enum Wake {
    Changed,
    Manual,
    Fallback,
}

/// Reading a published blob must not cause a reimport/notification feedback loop.
/// Folder events are significant: creation of an immediate child can race watch setup.
pub fn needs_refresh(event: &notify::Result<Event>) -> bool {
    match event {
        Err(_) => true,
        Ok(event) if event.need_rescan() => true,
        Ok(event) => match event.kind {
            EventKind::Access(AccessKind::Close(AccessMode::Write)) => true,
            EventKind::Access(_) => false,
            _ => true,
        },
    }
}

pub struct ChangeMonitor {
    rx: watch::Receiver<u64>,
    // Keeps the event channel open even in polling fallback mode.
    _keepalive: watch::Sender<u64>,
    watcher: Option<RecommendedWatcher>,
    root: Option<PathBuf>,
    rearm: Arc<AtomicBool>,
    fallback: Duration,
    manual_closed: bool,
}

impl ChangeMonitor {
    fn from_events(
        rx: watch::Receiver<u64>,
        keepalive: watch::Sender<u64>,
        fallback: Duration,
    ) -> Self {
        Self {
            rx,
            _keepalive: keepalive,
            watcher: None,
            root: None,
            rearm: Arc::new(AtomicBool::new(false)),
            fallback,
            manual_closed: false,
        }
    }

    /// Register before the initial fingerprint/import so writes during import are retained.
    pub fn watch(root: &Path) -> notify::Result<Self> {
        let root = root.to_path_buf();
        let (tx, rx) = watch::channel(0u64);
        let mut monitor = Self::from_events(rx, tx.clone(), FALLBACK);
        let rearm = monitor.rearm.clone();
        let filter_root = root.clone();
        let mut watcher = notify::recommended_watcher(move |event: notify::Result<Event>| {
            if !needs_refresh(&event) {
                return;
            }
            let relevant = match &event {
                Err(_) => true,
                Ok(e) => {
                    e.need_rescan()
                        || e.paths.is_empty()
                        || e.paths.iter().any(|p| p.starts_with(&filter_root))
                }
            };
            if !relevant {
                return;
            }
            if match &event {
                Err(_) => true,
                Ok(e) => e.need_rescan() || e.paths.iter().any(|p| p == &filter_root),
            } {
                rearm.store(true, Ordering::Relaxed);
            }
            // One generation counter, not an unbounded queue of path/event objects.
            tx.send_modify(|generation| *generation = generation.wrapping_add(1));
        })?;
        // The parent detects root replacement/deletion/recreation. Sibling events
        // are filtered out above and never trigger scans of the shared tree.
        if let Some(parent) = root.parent() {
            watcher.watch(parent, RecursiveMode::NonRecursive)?;
        }
        watcher.watch(&root, RecursiveMode::Recursive)?;
        monitor.root = Some(root);
        monitor.watcher = Some(watcher);
        Ok(monitor)
    }

    /// Unsupported notifications must retain eventual detection, not disable refresh.
    pub fn polling_only() -> Self {
        let (tx, rx) = watch::channel(0u64);
        Self::from_events(rx, tx, Duration::from_secs(60))
    }

    fn rearm_if_needed(&mut self, force: bool) {
        if !force && !self.rearm.swap(false, Ordering::Relaxed) {
            return;
        }
        if let (Some(watcher), Some(root)) = (&mut self.watcher, &self.root) {
            // Re-register recursively after root replacement. If the root is absent,
            // parent notifications and the safety scan will retry; no busy loop.
            if watcher.watch(root, RecursiveMode::Recursive).is_err() {
                self.rearm.store(true, Ordering::Relaxed);
            }
        }
    }

    pub async fn wait(&mut self, manual: &mut mpsc::Receiver<()>, failed: bool) -> Wake {
        self.rearm_if_needed(false);
        let delay = if failed { RETRY } else { self.fallback };
        let timer = tokio::time::sleep(delay);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                // Manual requests take precedence when both sources are ready.
                biased;
                request = manual.recv(), if !self.manual_closed => {
                    if request.is_some() { return Wake::Manual; }
                    self.manual_closed = true;
                }
                result = self.rx.changed() => {
                    if result.is_ok() {
                        self.rx.borrow_and_update();
                        return Wake::Changed;
                    }
                    // The owned keepalive makes this unreachable in normal use.
                    timer.as_mut().await;
                    self.rearm_if_needed(true);
                    return Wake::Fallback;
                }
                _ = &mut timer => {
                    self.rearm_if_needed(true);
                    return Wake::Fallback;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
