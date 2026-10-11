use super::*;
use notify::event::{AccessKind, AccessMode, CreateKind, Flag, RemoveKind, RenameMode};

fn fixture() -> (
    Monitor,
    watch::Sender<Signal>,
    mpsc::Sender<()>,
    mpsc::Receiver<()>,
) {
    let (events, rx) = watch::channel(Signal {
        healthy: true,
        ..Signal::default()
    });
    let (manual, manual_rx) = mpsc::channel(1);
    (Monitor::from_events(rx, true), events, manual, manual_rx)
}
fn emit(tx: &watch::Sender<Signal>, healthy: bool) {
    tx.send_modify(|s| s.healthy = healthy);
}

#[test]
fn read_access_is_ignored_but_mutations_and_rescan_are_dirty() {
    for kind in [
        AccessKind::Open(AccessMode::Read),
        AccessKind::Close(AccessMode::Read),
        AccessKind::Any,
    ] {
        assert!(!needs_refresh(&Ok(Event::new(EventKind::Access(kind)))));
    }
    for kind in [
        EventKind::Create(CreateKind::Folder),
        EventKind::Create(CreateKind::File),
        EventKind::Modify(ModifyKind::Any),
        EventKind::Remove(RemoveKind::Any),
        EventKind::Access(AccessKind::Close(AccessMode::Write)),
    ] {
        assert!(needs_refresh(&Ok(Event::new(kind))));
    }
    assert!(needs_refresh(&Ok(Event::new(EventKind::Access(
        AccessKind::Any
    ))
    .set_flag(Flag::Rescan))));
    assert!(needs_refresh(&Err(notify::Error::generic("synthetic"))));
}
#[test]
fn root_and_parent_lifecycle_request_repair_but_siblings_do_not() {
    let root = Path::new("/fixture/root");
    let create = |p: &str| Ok(Event::new(EventKind::Create(CreateKind::Folder)).add_path(p.into()));
    assert_eq!(classify(root, &create("/fixture/root")), (true, true));
    assert_eq!(
        classify(root, &create("/fixture/root/nested")),
        (true, false)
    );
    assert_eq!(classify(root, &create("/fixture/sibling")), (false, false));
    let parent = Ok(
        Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
            .add_path("/fixture".into()),
    );
    assert_eq!(classify(root, &parent), (true, true));
    assert_eq!(
        classify(root, &Ok(Event::new(EventKind::Other))),
        (true, true)
    );
}
#[test]
fn callback_error_invalidates_health_and_coalesces_repair_commands() {
    let (tx, rx) = watch::channel(Signal {
        healthy: true,
        ..Signal::default()
    });
    let (commands, requests) = std_mpsc::sync_channel(1);
    for _ in 0..1000 {
        deliver(
            Path::new("/fixture"),
            &tx,
            &commands,
            0,
            Err(notify::Error::generic("synthetic")),
        );
    }
    assert!(!rx.borrow().healthy);
    assert!(rx.borrow().invalidated);
    assert_eq!(requests.try_iter().count(), 1);
}
#[test]
fn callback_error_wins_in_both_installation_completion_orderings() {
    for error_first in [true, false] {
        let (tx, rx) = watch::channel(Signal::default());
        let (commands, _requests) = std_mpsc::sync_channel(1);
        if !error_first {
            tx.send_modify(|s| {
                s.installed();
            });
        }
        deliver(
            Path::new("/fixture"),
            &tx,
            &commands,
            0,
            Err(notify::Error::generic("racing installation")),
        );
        if error_first {
            tx.send_modify(|s| {
                s.installed();
            });
        }
        assert!(!rx.borrow().healthy);
        assert!(rx.borrow().invalidated);
    }
}

#[test]
fn callbacks_from_retired_registrations_are_ignored() {
    let (tx, rx) = watch::channel(Signal {
        registration: 2,
        healthy: true,
        ..Signal::default()
    });
    let (commands, requests) = std_mpsc::sync_channel(1);
    deliver(
        Path::new("/fixture"),
        &tx,
        &commands,
        1,
        Err(notify::Error::generic("stale")),
    );
    assert!(rx.borrow().healthy);
    assert!(!rx.borrow().invalidated);
    assert!(!rx.has_changed().unwrap());
    assert!(requests.try_recv().is_err());
}
fn callback_for_test(
    tx: watch::Sender<Signal>,
    requests: std_mpsc::SyncSender<()>,
    registration: u64,
) -> Callback {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let (observed, _probe) = mpsc::channel(1);
    Callback {
        root: PathBuf::from("/fixture"),
        tx,
        requests,
        registration,
        _lease: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        observed,
    }
}

#[tokio::test(start_paused = true)]
async fn handler_death_degrades_while_handle_and_worker_senders_remain_alive() {
    let (mut monitor, events, _tx, mut rx) = fixture();
    let _handle_sender = events.clone();
    let (commands, requests) = std_mpsc::sync_channel(1);
    drop(callback_for_test(events.clone(), commands, 0));
    assert!(!events.borrow().healthy);
    assert!(events.borrow().invalidated);
    requests
        .try_recv()
        .expect("handler death requests repair without another event");
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    let start = Instant::now();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Safety);
    assert_eq!(start.elapsed(), DEGRADED);
    assert!(
        !monitor.closed,
        "health change did not depend on channel closing"
    );
}

#[test]
fn uninstalled_handler_drop_does_not_create_a_repair_feedback_loop() {
    let (tx, rx) = watch::channel(Signal::default());
    let (commands, requests) = std_mpsc::sync_channel(1);
    drop(callback_for_test(tx, commands, 0));
    assert!(rx.borrow().invalidated);
    assert!(
        requests.try_recv().is_err(),
        "failed setup must keep its scheduled backoff"
    );
}

#[test]
fn handler_death_wins_before_or_after_installation_completion() {
    for death_first in [true, false] {
        let (tx, rx) = watch::channel(Signal::default());
        let (commands, _requests) = std_mpsc::sync_channel(1);
        if !death_first {
            tx.send_modify(|s| {
                s.installed();
            });
        }
        drop(callback_for_test(tx.clone(), commands, 0));
        if death_first {
            tx.send_modify(|s| {
                s.installed();
            });
        }
        assert!(rx.borrow().invalidated);
        assert!(!rx.borrow().healthy);
    }
}

#[test]
fn retired_handler_destruction_does_not_degrade_current_registration() {
    let (tx, rx) = watch::channel(Signal {
        registration: 2,
        healthy: true,
        ..Signal::default()
    });
    let (commands, requests) = std_mpsc::sync_channel(1);
    let callback = Arc::new(callback_for_test(tx.clone(), commands, 1));
    let budget = callback._lease.semaphore().clone();
    let retired = callback.clone();
    drop(callback);
    assert!(
        budget.clone().try_acquire_owned().is_err(),
        "retiring handler keeps its slot"
    );
    drop(retired);
    assert_eq!(
        budget.available_permits(),
        1,
        "only handler destruction frees its slot"
    );
    assert!(rx.borrow().healthy);
    assert!(!rx.has_changed().unwrap());
    assert!(requests.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn non_native_policy_uses_minute_polling_without_notifications() {
    // The exact constructor used by Monitor::new on unsupported platforms.
    let mut monitor = Monitor::polling();
    let (_tx, mut rx) = mpsc::channel(1);
    let start = Instant::now();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Safety);
    assert_eq!(start.elapsed(), DEGRADED);
}

#[tokio::test(start_paused = true)]
async fn idle_and_manual_and_closed_manual_channel() {
    let (mut monitor, _events, tx, mut rx) = fixture();
    assert!(
        tokio::time::timeout(Duration::from_secs(59), monitor.wait(&mut rx))
            .await
            .is_err()
    );
    tx.send(()).await.unwrap();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Manual);
    drop(tx);
    assert!(
        tokio::time::timeout(Duration::from_secs(59), monitor.wait(&mut rx))
            .await
            .is_err()
    );
}
#[tokio::test(start_paused = true)]
async fn continuous_events_cannot_postpone_safety() {
    let (mut monitor, events, _tx, mut rx) = fixture();
    let start = Instant::now();
    for n in 1..=600 {
        tokio::time::advance(Duration::from_secs(1)).await;
        emit(&events, true);
        assert_eq!(
            monitor.wait(&mut rx).await,
            if n == 600 {
                Wake::Safety
            } else {
                Wake::Changed
            }
        );
    }
    assert_eq!(start.elapsed(), SAFETY);
}
#[tokio::test(start_paused = true)]
async fn manual_requests_cannot_starve_safety() {
    let (mut monitor, _events, tx, mut rx) = fixture();
    tokio::time::advance(SAFETY).await;
    tx.send(()).await.unwrap();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Safety);
    assert_eq!(monitor.wait(&mut rx).await, Wake::Manual);
}
#[tokio::test(start_paused = true)]
async fn mutation_traffic_does_not_starve_buffered_manual_refresh() {
    let (mut monitor, events, tx, mut rx) = fixture();
    emit(&events, true);
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    tokio::time::advance(MIN_SCAN).await;
    emit(&events, true);
    tx.send(()).await.unwrap();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Manual);
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
}
#[tokio::test(start_paused = true)]
async fn failed_manual_import_replaces_an_old_scan_retry_deadline() {
    let (mut monitor, events, tx, mut rx) = fixture();
    monitor.scan_failed();
    tokio::time::advance(Duration::from_secs(25)).await;
    tx.send(()).await.unwrap();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Manual);
    // The same explicit transition transfer.rs calls on every new import Err.
    monitor.scan_failed();
    emit(&events, true);
    let start = Instant::now();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    assert_eq!(start.elapsed(), RETRY);
}
#[tokio::test(start_paused = true)]
async fn event_storm_is_rate_limited_and_cancellation_retains_pending_work() {
    let (mut monitor, events, _tx, mut rx) = fixture();
    emit(&events, true);
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    for _ in 0..1000 {
        emit(&events, true);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), monitor.wait(&mut rx))
            .await
            .is_err()
    );
    let start = Instant::now();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    assert_eq!(start.elapsed(), Duration::from_millis(900));
}
#[tokio::test(start_paused = true)]
async fn event_arriving_during_wait_is_not_lost() {
    let (mut monitor, events, _tx, mut rx) = fixture();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        emit(&events, true);
    });
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    producer.await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn stability_retry_does_not_require_another_event() {
    let (mut monitor, _events, _tx, mut rx) = fixture();
    monitor.unstable();
    let start = Instant::now();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    assert_eq!(start.elapsed(), MIN_SCAN);
}
#[tokio::test(start_paused = true)]
async fn failed_scans_back_off_even_during_event_storm() {
    let (mut monitor, events, _tx, mut rx) = fixture();
    monitor.scan_failed();
    emit(&events, true);
    let start = Instant::now();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    assert_eq!(start.elapsed(), RETRY);
}
#[tokio::test(start_paused = true)]
async fn callback_error_and_worker_exit_enable_minute_safety_scans() {
    let (mut monitor, events, _tx, mut rx) = fixture();
    emit(&events, false);
    assert_eq!(monitor.wait(&mut rx).await, Wake::Changed);
    drop(events);
    let start = Instant::now();
    assert_eq!(monitor.wait(&mut rx).await, Wake::Safety);
    assert_eq!(start.elapsed(), DEGRADED);
    assert_eq!(monitor.wait(&mut rx).await, Wake::Safety);
    assert_eq!(start.elapsed(), DEGRADED * 2);
}
#[test]
fn rebuild_deadlines_are_absolute_and_recovery_retries_are_bounded() {
    let mut schedule = RebuildSchedule::new();
    schedule.request();
    assert_eq!(
        schedule.due,
        Duration::ZERO,
        "first install must remain immediate"
    );
    schedule.completed(Duration::ZERO, true);
    assert_eq!(schedule.due, SAFETY);
    schedule.request();
    for _ in 0..1000 {
        schedule.request();
    }
    assert_eq!(schedule.due, MIN_REBUILD);
    schedule.completed(MIN_REBUILD, false);
    assert_eq!(schedule.due, MIN_REBUILD + DEGRADED);
    schedule.completed(MIN_REBUILD + DEGRADED, true);
    assert_eq!(schedule.due, MIN_REBUILD + DEGRADED + SAFETY);
}
fn fast_schedule() -> RebuildSchedule {
    RebuildSchedule {
        due: Duration::ZERO,
        last: Duration::ZERO,
        healthy_period: SAFETY,
        degraded_period: Duration::from_millis(100),
        min_interval: Duration::from_millis(30),
    }
}
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn healthy(monitor: &mut Monitor, after: u64) {
    tokio::time::timeout(
        Duration::from_secs(3),
        monitor
            .events
            .wait_for(|s| s.healthy && s.registration > after),
    )
    .await
    .unwrap()
    .unwrap();
    monitor.healthy = true;
    monitor.pending = false;
}
#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn repair_ack_accepts_completion_before_waiter_starts() {
    let (mut monitor, events, _tx, _rx) = fixture();
    // Simulate a complete false -> true repair before the waiter is polled.
    events.send_modify(|s| {
        s.registration = 1;
        s.healthy = false;
    });
    events.send_modify(|s| {
        s.registration = 2;
        s.healthy = true;
    });
    healthy(&mut monitor, 1).await;
    assert!(monitor.healthy);
    assert_eq!(monitor.events.borrow().registration, 2);
}

#[tokio::test]
async fn worker_retries_setup_and_runtime_errors_and_releases_registrations() {
    struct Registration(Arc<AtomicUsize>);
    impl Drop for Registration {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let factory_drops = drops.clone();
    let (tx, mut rx) = watch::channel(Signal::default());
    let (commands, requests) = std_mpsc::sync_channel(1);
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = WorkerStop {
        stop: stop.clone(),
        requests: commands.clone(),
    };
    let emit = tx.clone();
    let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
    let thread = std::thread::spawn(move || {
        run_worker(
            move || {
                if factory_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    Err(notify::Error::generic("synthetic setup failure"))
                } else {
                    Ok(Registration(factory_drops.clone()))
                }
            },
            emit,
            requests,
            stop,
            permit,
            fast_schedule(),
        )
    });
    tokio::time::timeout(Duration::from_secs(3), rx.wait_for(|s| s.healthy))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    let registration = rx.borrow().registration;
    deliver(
        Path::new("/fixture"),
        &tx,
        &commands,
        registration,
        Err(notify::Error::generic("synthetic runtime failure")),
    );
    tokio::time::timeout(
        Duration::from_secs(3),
        rx.wait_for(|s| s.healthy && s.registration > registration),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 3);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(worker_stop);
    tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || thread.join()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}
#[cfg(any(target_os = "linux", target_os = "android"))]
async fn observe_write(monitor: &mut Monitor, path: &Path) {
    let mut observed = monitor.observed.take().unwrap();
    for n in 0..6 {
        let registration = monitor.events.borrow().registration;
        std::fs::write(path, [n]).unwrap();
        let matched = tokio::time::timeout(Duration::from_millis(200), async {
            while let Some(event) = observed.recv().await {
                if let Observed::Mutation {
                    path: p,
                    registration: r,
                } = event
                {
                    if p == path && r == registration {
                        return true;
                    }
                }
            }
            false
        })
        .await;
        if matches!(matched, Ok(true)) {
            monitor.observed = Some(observed);
            return;
        }
        healthy(monitor, 0).await;
    }
    panic!("no mutation callback for the exact path/current registration");
}
#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn setup_failure_recovers_and_watches_a_deep_file_after_root_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    let mut monitor = Monitor::with_schedule(&root, fast_schedule());
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(event) = monitor.observed.as_mut().unwrap().recv().await {
            if matches!(event, Observed::Setup { success: false }) {
                return;
            }
        }
        panic!("probe closed before setup failure");
    })
    .await
    .unwrap();
    std::fs::create_dir_all(root.join("a/b")).unwrap();
    healthy(&mut monitor, 0).await;
    let previous = monitor.events.borrow().registration;
    std::fs::rename(&root, dir.path().join("old")).unwrap();
    std::fs::create_dir_all(root.join("a/b")).unwrap();
    // Watch is coalescing: repair may finish before this task observes degradation.
    healthy(&mut monitor, previous).await;
    observe_write(&mut monitor, &root.join("a/b/after.txt")).await;
    let (_tx, mut rx) = mpsc::channel(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), monitor.wait(&mut rx))
            .await
            .unwrap(),
        Wake::Changed
    );
}
#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn file_root_atomic_replacement_then_subsequent_write_is_observed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("file");
    std::fs::write(&root, b"before").unwrap();
    let mut monitor = Monitor::with_schedule(&root, fast_schedule());
    healthy(&mut monitor, 0).await;
    let previous = monitor.events.borrow().registration;
    std::fs::write(dir.path().join("replacement"), b"replacement").unwrap();
    std::fs::rename(dir.path().join("replacement"), &root).unwrap();
    healthy(&mut monitor, previous).await;
    observe_write(&mut monitor, &root).await;
    let (_tx, mut rx) = mpsc::channel(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), monitor.wait(&mut rx))
            .await
            .unwrap(),
        Wake::Changed
    );
}
#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn sibling_writes_and_reads_are_filtered() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("file"), b"synthetic").unwrap();
    let mut monitor = Monitor::with_schedule(&root, fast_schedule());
    healthy(&mut monitor, 0).await;
    std::fs::read(root.join("file")).unwrap();
    std::fs::write(dir.path().join("sibling"), b"synthetic").unwrap();
    let (_tx, mut rx) = mpsc::channel(1);
    assert!(
        tokio::time::timeout(Duration::from_millis(250), monitor.wait(&mut rx))
            .await
            .is_err()
    );
    assert!(!monitor.events.has_changed().unwrap());
}
