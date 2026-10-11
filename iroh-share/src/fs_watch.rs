//! Optional event hints plus a persistent refresh scheduler. File contents are
//! always validated by transfer.rs, never inferred from notification paths.
use notify::{
    event::{AccessKind, AccessMode, ModifyKind},
    Event, EventKind, RecursiveMode, Watcher,
};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
mod backend;
use backend::Guarded;

#[cfg(panic = "abort")]
compile_error!("fs-watch requires panic=unwind for notify backend recovery");

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc, Arc, LazyLock,
    },
    time::{Duration, Instant as StdInstant},
};
use tokio::{
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore, SemaphorePermit},
    time::Instant,
};

const SAFETY: Duration = Duration::from_secs(600);
const DEGRADED: Duration = Duration::from_secs(60);
const RETRY: Duration = Duration::from_secs(30);
const MIN_SCAN: Duration = Duration::from_secs(1);
const MIN_REBUILD: Duration = Duration::from_secs(5);
const MAX_WORKERS: usize = 16;
static WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(MAX_WORKERS)));
// A lease lives in the callback, not the Watcher handle: notify Drop only
// requests shutdown. Retiring, still-running backends must keep their slot.
static BACKENDS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(MAX_WORKERS)));
static SCANS: Semaphore = Semaphore::const_new(1);

pub(crate) async fn scan_permit() -> SemaphorePermit<'static> {
    SCANS
        .acquire()
        .await
        .expect("static scan semaphore is never closed")
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Wake {
    Changed,
    Manual,
    Safety,
}

#[derive(Clone, Copy, Debug, Default)]
struct Signal {
    invalidated: bool,
    registration: u64,
    healthy: bool,
}

impl Signal {
    fn invalidate(&mut self) -> bool {
        let was_healthy = self.healthy;
        self.invalidated = true;
        self.healthy = false;
        was_healthy
    }
    fn installed(&mut self) -> bool {
        self.healthy = !self.invalidated;
        self.healthy
    }
}

fn needs_refresh(event: &notify::Result<Event>) -> bool {
    match event {
        Err(_) => true,
        Ok(e) if e.need_rescan() => true,
        Ok(e) => match e.kind {
            EventKind::Access(AccessKind::Close(AccessMode::Write)) => true,
            EventKind::Access(_) => false,
            _ => true,
        },
    }
}

fn classify(root: &Path, event: &notify::Result<Event>) -> (bool, bool) {
    if !needs_refresh(event) {
        return (false, false);
    }
    match event {
        Err(_) => (true, true),
        Ok(e) if e.need_rescan() || e.paths.is_empty() => (true, true),
        Ok(e) => {
            let lifecycle = matches!(
                e.kind,
                EventKind::Create(_)
                    | EventKind::Remove(_)
                    | EventKind::Modify(ModifyKind::Name(_))
            );
            let root_event = e.paths.iter().any(|p| p == root);
            let parent_event =
                lifecycle && e.paths.iter().any(|p| Some(p.as_path()) == root.parent());
            let relevant = parent_event || e.paths.iter().any(|p| p.starts_with(root));
            (relevant, parent_event || (root_event && lifecycle))
        }
    }
}

fn deliver(
    root: &Path,
    tx: &watch::Sender<Signal>,
    requests: &std_mpsc::SyncSender<()>,
    registration: u64,
    event: notify::Result<Event>,
) {
    let (dirty, rebuild) = classify(root, &event);
    if !dirty {
        return;
    }
    let delivered = tx.send_if_modified(|s| {
        if s.registration != registration {
            return false;
        }
        if rebuild {
            s.invalidate();
        }
        true
    });
    if delivered && rebuild {
        let _ = requests.try_send(());
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
enum Observed {
    Mutation { path: PathBuf, registration: u64 },
    Setup { success: bool },
}

struct Callback {
    root: PathBuf,
    tx: watch::Sender<Signal>,
    requests: std_mpsc::SyncSender<()>,
    registration: u64,
    _lease: OwnedSemaphorePermit,
    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
    observed: mpsc::Sender<Observed>,
}
impl notify::EventHandler for Callback {
    fn handle_event(&mut self, event: notify::Result<Event>) {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        if needs_refresh(&event) && self.tx.borrow().registration == self.registration {
            if let Ok(e) = &event {
                for path in &e.paths {
                    let _ = self.observed.try_send(Observed::Mutation {
                        path: path.clone(),
                        registration: self.registration,
                    });
                }
            }
        }
        deliver(
            &self.root,
            &self.tx,
            &self.requests,
            self.registration,
            event,
        );
    }
}

impl Drop for Callback {
    fn drop(&mut self) {
        // A dead backend still has a live handle/worker sender. Notify through
        // the epoch lock; destruction of an already retired handler is ignored.
        let mut repair = false;
        self.tx.send_if_modified(|s| {
            if s.registration != self.registration || s.invalidated {
                return false;
            }
            repair = s.invalidate();
            true
        });
        // Failed installation already schedules a retry. Do not turn handler
        // destruction during setup into an early-rebuild feedback loop.
        if repair {
            let _ = self.requests.try_send(());
        }
    }
}

struct WorkerStop {
    stop: Arc<AtomicBool>,
    requests: std_mpsc::SyncSender<()>,
}
impl Drop for WorkerStop {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // If full, the pending command already wakes the worker. Never block an
        // async executor on backend registration/destruction or thread joining.
        let _ = self.requests.try_send(());
    }
}

/// Independent registration deadlines: events may advance a repair, never postpone it.
struct RebuildSchedule {
    due: Duration,
    last: Duration,
    healthy_period: Duration,
    degraded_period: Duration,
    min_interval: Duration,
}
impl RebuildSchedule {
    fn new() -> Self {
        Self {
            due: Duration::ZERO,
            last: Duration::ZERO,
            healthy_period: SAFETY,
            degraded_period: DEGRADED,
            min_interval: MIN_REBUILD,
        }
    }
    fn request(&mut self) {
        self.due = self.due.min(self.last + self.min_interval);
    }
    fn completed(&mut self, now: Duration, healthy: bool) {
        self.last = now;
        self.due = now
            + if healthy {
                self.healthy_period
            } else {
                self.degraded_period
            };
    }
}

fn run_worker<F, B>(
    mut install: F,
    tx: watch::Sender<Signal>,
    requests: std_mpsc::Receiver<()>,
    stop: Arc<AtomicBool>,
    _permit: OwnedSemaphorePermit,
    mut schedule: RebuildSchedule,
) where
    F: FnMut() -> notify::Result<B>,
    B: Send,
{
    let start = StdInstant::now();
    let mut registered: Option<B> = None;
    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let now = start.elapsed();
        if now >= schedule.due {
            // Invalidate old callbacks before requesting backend shutdown. Notify
            // does not expose a join handle; do not assume Drop joins its thread.
            tx.send_modify(|s| {
                s.registration = s.registration.wrapping_add(1);
                s.invalidated = false;
                s.healthy = false;
            });
            drop(registered.take());
            let result = install();
            let mut healthy = false;
            match result {
                Ok(watcher) => {
                    registered = Some(watcher);
                    tx.send_modify(|s| {
                        healthy = s.installed();
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "filesystem watcher degraded; using minute safety scans");
                }
            }
            schedule.completed(start.elapsed(), healthy);
            if !healthy {
                tracing::debug!("watcher repair will be retried");
            }
        }
        match requests.recv_timeout(schedule.due.saturating_sub(start.elapsed())) {
            Ok(()) => schedule.request(),
            Err(std_mpsc::RecvTimeoutError::Timeout) => (),
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    drop(registered);
}

pub(crate) struct Monitor {
    events: watch::Receiver<Signal>,
    _worker: Option<WorkerStop>,
    closed: bool,
    manual_closed: bool,
    pending: bool,
    healthy: bool,
    safety_due: Instant,
    retry_due: Option<Instant>,
    not_before: Instant,
    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
    observed: Option<mpsc::Receiver<Observed>>,
}
impl Monitor {
    fn from_events(events: watch::Receiver<Signal>, healthy: bool) -> Self {
        let now = Instant::now();
        Self {
            events,
            _worker: None,
            closed: false,
            manual_closed: false,
            pending: false,
            healthy,
            safety_due: now + if healthy { SAFETY } else { DEGRADED },
            retry_due: None,
            not_before: now,
            #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
            observed: None,
        }
    }

    pub(crate) fn new(root: &Path) -> Self {
        if !cfg!(any(target_os = "linux", target_os = "android")) {
            tracing::warn!(
                "native fs-watch currently supports Linux/Android only; using minute polling"
            );
            return Self::polling();
        }
        Self::with_schedule(root, RebuildSchedule::new())
    }

    fn polling() -> Self {
        let (_tx, rx) = watch::channel(Signal::default());
        Self::from_events(rx, false)
    }

    fn with_schedule(root: &Path, schedule: RebuildSchedule) -> Self {
        let (tx, rx) = watch::channel(Signal::default());
        let mut monitor = Self::from_events(rx, false);
        let Ok(permit) = WORKERS.clone().try_acquire_owned() else {
            tracing::warn!("watcher worker limit reached; using minute safety scans");
            return monitor;
        };
        let root: PathBuf = root.to_owned();
        let (commands, requests) = std_mpsc::sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = WorkerStop {
            stop: stop.clone(),
            requests: commands.clone(),
        };
        let emit = tx.clone();
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        let (observed, probe) = mpsc::channel(32);
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        {
            monitor.observed = Some(probe);
        }
        let install = move || {
            let result = (|| {
                let lease = BACKENDS
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| notify::Error::generic("live/retiring watcher limit reached"))?;
                let callback = Callback {
                    root: root.clone(),
                    tx: emit.clone(),
                    requests: commands.clone(),
                    registration: emit.borrow().registration,
                    _lease: lease,
                    #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
                    observed: observed.clone(),
                };
                let mut watcher = Guarded::new(backend::attempt(|| {
                    notify::RecommendedWatcher::new(
                        callback,
                        notify::Config::default().with_follow_symlinks(false),
                    )
                })?);
                if !root.is_absolute() {
                    return Err(notify::Error::generic("watch root must be absolute"));
                }
                if let Some(parent) = root.parent() {
                    watcher.call(|w| w.watch(parent, RecursiveMode::NonRecursive))?;
                }
                watcher.call(|w| w.watch(&root, RecursiveMode::Recursive))?;
                Ok(watcher)
            })();
            #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
            let _ = observed.try_send(Observed::Setup {
                success: result.is_ok(),
            });
            result
        };
        match std::thread::Builder::new()
            .name("share-file-watch".into())
            .stack_size(256 * 1024)
            .spawn(move || run_worker(install, tx, requests, stop, permit, schedule))
        {
            Ok(_thread) => monitor._worker = Some(worker_stop),
            Err(error) => {
                tracing::warn!(%error, "cannot start watcher worker; using minute safety scans")
            }
        }
        monitor
    }

    fn observe(&mut self) {
        match self.events.has_changed() {
            Ok(true) => {
                let signal = *self.events.borrow_and_update();
                self.received(signal);
            }
            Err(_) if !self.closed => {
                self.closed = true;
                self.healthy = false;
                self.safety_due = self.safety_due.min(Instant::now() + DEGRADED);
                tracing::warn!("watcher worker unavailable; using minute safety scans");
            }
            _ => (),
        }
    }

    fn received(&mut self, signal: Signal) {
        self.pending = true;
        self.healthy = signal.healthy;
        if !self.healthy {
            self.safety_due = self.safety_due.min(Instant::now() + DEGRADED);
        }
    }

    #[cfg(test)]
    pub(crate) fn quiet(&self) -> bool {
        self.healthy
            && !self.pending
            && self.retry_due.is_none()
            && matches!(self.events.has_changed(), Ok(false))
    }

    pub(crate) fn unstable(&mut self) {
        self.retry_due = Some(Instant::now() + MIN_SCAN);
    }
    pub(crate) fn scan_failed(&mut self) {
        let due = Instant::now() + RETRY;
        self.retry_due = Some(due);
        self.not_before = self.not_before.max(due);
    }
    pub(crate) fn confirmed(&mut self) {
        self.retry_due = None;
    }

    pub(crate) async fn wait(&mut self, manual: &mut mpsc::Receiver<()>) -> Wake {
        loop {
            self.observe();
            let now = Instant::now();
            // Persistent deadlines have priority over manual/events. Cancellation
            // retains all state, and no traffic resets the safety deadline.
            if now >= self.not_before && now >= self.safety_due {
                self.safety_due = now + if self.healthy { SAFETY } else { DEGRADED };
                self.pending = false;
                self.retry_due = None;
                self.not_before = now + MIN_SCAN;
                return Wake::Safety;
            }
            // Read buffered manual requests before ordinary automatic work:
            // persistent mutation traffic must not starve force-refresh.
            if !self.manual_closed {
                match manual.try_recv() {
                    Ok(()) => return Wake::Manual,
                    Err(mpsc::error::TryRecvError::Disconnected) => self.manual_closed = true,
                    Err(mpsc::error::TryRecvError::Empty) => (),
                }
            }
            if now >= self.not_before
                && (self.pending || self.retry_due.is_some_and(|due| now >= due))
            {
                self.pending = false;
                self.retry_due = None;
                self.not_before = now + MIN_SCAN;
                return Wake::Changed;
            }
            let mut deadline = self.safety_due.max(self.not_before);
            if let Some(retry) = self.retry_due {
                deadline = deadline.min(retry.max(self.not_before));
            }
            if self.pending {
                deadline = deadline.min(self.not_before);
            }
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => (),
                request = manual.recv(), if !self.manual_closed => {
                    if request.is_some() { return Wake::Manual; }
                    self.manual_closed = true;
                }
                result = self.events.changed(), if !self.closed => {
                    if result.is_ok() {
                        let signal = *self.events.borrow_and_update();
                        self.received(signal);
                    } else { self.observe(); }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
