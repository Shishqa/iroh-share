use super::*;
use notify::{
    event::{AccessKind, AccessMode, CreateKind, Flag, ModifyKind, RemoveKind, RenameMode},
    EventKind,
};

fn fixture() -> (
    ChangeMonitor,
    watch::Sender<u64>,
    mpsc::Sender<()>,
    mpsc::Receiver<()>,
) {
    let (events, rx) = watch::channel(0);
    let monitor = ChangeMonitor::from_events(rx, events.clone(), Duration::from_secs(600));
    let (manual, manual_rx) = mpsc::channel(1);
    (monitor, events, manual, manual_rx)
}

#[test]
fn read_access_does_not_trigger_refresh() {
    for kind in [
        AccessKind::Open(AccessMode::Read),
        AccessKind::Close(AccessMode::Read),
        AccessKind::Any,
    ] {
        assert!(!needs_refresh(&Ok(Event::new(EventKind::Access(kind)))));
    }
}

#[test]
fn mutations_folder_creation_rescan_and_errors_trigger_refresh() {
    for kind in [
        EventKind::Create(CreateKind::Folder),
        EventKind::Create(CreateKind::File),
        EventKind::Modify(ModifyKind::Any),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
        EventKind::Remove(RemoveKind::Any),
        EventKind::Access(AccessKind::Close(AccessMode::Write)),
    ] {
        assert!(needs_refresh(&Ok(Event::new(kind))));
    }
    assert!(needs_refresh(&Ok(Event::new(EventKind::Access(
        AccessKind::Any
    ))
    .set_flag(Flag::Rescan))));
    assert!(needs_refresh(&Err(notify::Error::generic(
        "synthetic watcher error"
    ))));
}

#[tokio::test(start_paused = true)]
async fn idle_does_not_wake_every_two_seconds() {
    let (mut monitor, _, _tx, mut rx) = fixture();
    assert!(
        tokio::time::timeout(Duration::from_secs(59), monitor.wait(&mut rx, false))
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn manual_refresh_is_immediate() {
    let (mut monitor, _, tx, mut rx) = fixture();
    tx.send(()).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(1), monitor.wait(&mut rx, false))
            .await
            .unwrap(),
        Wake::Manual
    );
}

#[tokio::test(start_paused = true)]
async fn event_during_import_is_not_lost_and_bursts_coalesce() {
    let (mut monitor, events, _tx, mut rx) = fixture();
    for n in 1..=1000 {
        events.send_replace(n);
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(1), monitor.wait(&mut rx, false))
            .await
            .unwrap(),
        Wake::Changed
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(59), monitor.wait(&mut rx, false))
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn manual_channel_closed_does_not_spin() {
    let (mut monitor, _events, tx, mut rx) = fixture();
    drop(tx);
    assert!(
        tokio::time::timeout(Duration::from_secs(59), monitor.wait(&mut rx, false))
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn fallback_eventually_wakes_without_notifications() {
    let (mut monitor, _, _tx, mut rx) = fixture();
    let start = tokio::time::Instant::now();
    assert_eq!(monitor.wait(&mut rx, false).await, Wake::Fallback);
    assert_eq!(start.elapsed(), Duration::from_secs(600));
}

#[tokio::test(start_paused = true)]
async fn failed_import_retries_without_waiting_ten_minutes() {
    let (mut monitor, _, _tx, mut rx) = fixture();
    let start = tokio::time::Instant::now();
    assert_eq!(monitor.wait(&mut rx, true).await, Wake::Fallback);
    assert_eq!(start.elapsed(), Duration::from_secs(30));
}

#[tokio::test]
async fn native_directory_events_trigger_full_tree_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let mut monitor = ChangeMonitor::watch(dir.path()).unwrap();
    let (_tx, mut rx) = mpsc::channel(1);
    // An immediate child may have no event; parent creation must wake the caller.
    let child = dir.path().join("nested");
    std::fs::create_dir(&child).unwrap();
    std::fs::write(child.join("immediate.txt"), b"synthetic").unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), monitor.wait(&mut rx, false))
            .await
            .unwrap(),
        Wake::Changed
    );
    assert!(child.join("immediate.txt").is_file());
}

#[tokio::test]
async fn unrelated_sibling_writes_do_not_wake() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("shared");
    std::fs::create_dir(&root).unwrap();
    let mut monitor = ChangeMonitor::watch(&root).unwrap();
    let (_tx, mut rx) = mpsc::channel(1);
    std::fs::write(dir.path().join("not-shared.txt"), b"synthetic").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), monitor.wait(&mut rx, false))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn replaced_root_is_rearmed_for_subsequent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("shared");
    std::fs::create_dir(&root).unwrap();
    let mut monitor = ChangeMonitor::watch(&root).unwrap();
    let (_tx, mut rx) = mpsc::channel(1);
    std::fs::rename(&root, dir.path().join("old")).unwrap();
    std::fs::create_dir(&root).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), monitor.wait(&mut rx, false))
            .await
            .unwrap(),
        Wake::Changed
    );
    // Enter wait to re-register the replaced root, then let rename/create events drain.
    for _ in 0..10 {
        if tokio::time::timeout(Duration::from_millis(100), monitor.wait(&mut rx, false))
            .await
            .is_err()
        {
            break;
        }
    }
    std::fs::write(root.join("after.txt"), b"synthetic").unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), monitor.wait(&mut rx, false))
            .await
            .unwrap(),
        Wake::Changed
    );
}

#[test]
fn nonexistent_root_falls_back_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    assert!(ChangeMonitor::watch(&dir.path().join("missing")).is_err());
}

#[tokio::test(start_paused = true)]
async fn polling_fallback_still_detects_changes_eventually() {
    let mut monitor = ChangeMonitor::polling_only();
    let (_tx, mut rx) = mpsc::channel(1);
    let start = tokio::time::Instant::now();
    assert_eq!(monitor.wait(&mut rx, false).await, Wake::Fallback);
    assert_eq!(start.elapsed(), Duration::from_secs(60));
}
