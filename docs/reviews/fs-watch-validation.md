# fs-watch validation

Base: upstream `19dbaa4`; review branch: `feat/notify-refresh`.

Eight independent static architecture passes and a separate complexity-review
track were used to challenge successive snapshots. The final cycle-8 reports
both return PASS with zero actionable findings:

- [Architecture](fs-watch-architecture-latest.md)
- [Complexity](fs-watch-complexity.md)

The reviewers had no tools and did not run tests. Their static conclusions are
separate from the checks executed by the implementation agent below.

## Executed checks on Termux/Android

- 28 watcher/scheduler/backend unit tests passed, including a disposable
  subprocess regression for registration plus destructor panic.
- Integration: file changes retarget a managed name, with fs-watch enabled
  and with the feature disabled.
- Integration: notification refresh publishes the expected collection hash,
  child paths and final bytes after acknowledgment of the initial check.
- Virtual-time test: second fingerprint read starts only after the stability
  interval; changed snapshots and scan errors are handled separately.
- Clippy with warnings denied, formatting and `git diff --check` passed.
- Feature-off daemon dependency graph excludes `notify`.
- Cargo install with fs-watch into a disposable staging prefix succeeded for
  both binaries; CLI help smoke checks do not start a daemon.

Reproduce focused checks from the workspace root:

```sh
cargo test --locked -p iroh-share --features fs-watch fs_watch:: -- --test-threads=1
cargo test --locked -p iroh-share --features fs-watch transfer::tests::shared_directory_changes_retarget_name -- --exact
cargo test --locked -p iroh-share --no-default-features transfer::tests::shared_directory_changes_retarget_name -- --exact
cargo test --locked -p iroh-share --features fs-watch transfer::tests::notification_refresh_publishes_final_bytes -- --exact
cargo test --locked -p iroh-share --features fs-watch transfer::tests::stability_delay_precedes_second_read_and_handles_changes_and_errors -- --exact
cargo clippy --locked -p iroh-share --features fs-watch --all-targets -- -D warnings
cargo fmt --all -- --check
cargo tree --locked -p iroh-share --no-default-features
```

Tests use synthetic temporary files and Minimal endpoints bound to loopback.
The entire workspace test suite was not run.

## Negative controls

Three temporary source mutations produced actual test failures, not compilation
errors. All mutations were reverted before the final tests and review snapshots:

| Mutation | Expected regression observed |
| --- | --- |
| Suppress mutation callback delivery | Final-bytes notification integration times out |
| Remove stability delay | Virtual-time second-read assertion fails: 0 ns vs 1 s |
| Replace recursive root watch with nonrecursive | Deep-path callback recovery test fails |

These controls were executed manually; there is no new mutation-test framework.

## Limits and deployment

PASS is not production certification or a whole-project security audit. Native
notifications are scoped to Linux/Android. Other platforms use minute fallback;
no Windows/macOS execution was performed. Android cross-app delivery, Doze,
suspend/resume, load/storm performance and battery savings remain unmeasured.
Metadata fingerprints are not atomic snapshots; referenced mutable files retain
the existing consistency limitations. Immutable build directories remain useful.

The installed production binaries, running service and its state were not
replaced or restarted. Local Android DNS/key-creation compatibility patches are
not included in this upstream-facing branch; keep them separate when preparing
an installation on this phone.
