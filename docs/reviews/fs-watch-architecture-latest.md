VERDICT: PASS
Remaining actionable findings: 0

## Результат повторной проверки

В предоставленном cycle-8 snapshot оставшихся подтверждённых ошибок opt-in patch не обнаружено. Это результат статического архитектурного обзора, а не подтверждение успешного исполнения тестов или эксплуатационной готовности.

**Finding cycle-7 исправлен:**

- `iroh-share/src/fs_watch/tests.rs:395–405` теперь ожидает `healthy && registration > after`, а не обязательного наблюдения промежуточного `healthy = false`.
- Оба native lifecycle-теста сохраняют предыдущую registration до замены root и ожидают новую: `tests.rs:544–548,566–569`.
- `tests.rs:411–425` детерминированно проверяет завершение восстановления до начала ожидания. Тест доказывает корректность нового acknowledgment-предиката при coalescing; он не подменяет собой проверку реального backend.
- Проверка callback для точного пути и соответствующей registration сохранена: `tests.rs:498–524`.

При healthy rebuild interval в десять минут новая registration за трёхсекундное ожидание не может объясняться обычным периодическим rebuild.

## Проверенные архитектурные свойства

- **Health epochs.** Проверка registration и invalidation выполняются внутри изменения watch-state: `iroh-share/src/fs_watch.rs:120–128,177–183`. Ошибка или destruction handler до завершения установки сохраняются через `invalidated`; `installed()` их не стирает. Retired handler не деградирует новую registration.
- **Repair и blind windows.** Worker независим от import. Epoch меняется до retirement старого backend; завершение установки публикует дополнительный сигнал для проверки содержимого: `fs_watch.rs:258–277`. Таким образом, проверка, начавшаяся до окончания регистрации, не является единственной проверкой после её завершения.
- **Deadlines и cancellation.** Обычные events и manual wakeups не переносят safety deadline. Просроченный safety имеет приоритет с учётом backoff; buffered manual request предшествует обычной автоматической работе. Состояние pending/retry сохраняется при отмене ожидания: `fs_watch.rs:451–503`.
- **Retry.** Нестабильность вызывает повтор без дополнительного event. Ошибки fingerprint и import устанавливают backoff; новая ошибка manual import заменяет прежний retry deadline: `transfer.rs:115–120,155–190`.
- **Закрытые каналы и idle CPU.** После обнаружения закрытия соответствующие ветви ожидания отключаются. Постоянно готовой ветви, создающей idle busy loop, не найдено. При закрытии event-channel последний непрочитанный сигнал может быть пропущен, но minute fallback сохраняется.
- **Ownership и shutdown.** WorkerStop не выполняет join или блокирующую отправку на async executor. Scan permit удерживается самой blocking closure. Backend permit принадлежит callback, поэтому учитывает продолжающуюся жизнь retired backend.
- **Feature boundary.** Приватный модуль и новая интеграция feature-gated; optional `notify` подключается через `dep:notify`. Feature-off ветка сохраняет двухсекундное polling-ожидание: `iroh-share/Cargo.toml:13–15,37`, `main.rs:3–4`, `transfer.rs:150–154`. Запись notify в lockfile сама по себе не означает его включения в feature-off dependency graph.

## Фактические ограничения supplied notify/inotify

`notify-8.2.0/src/inotify.rs:606–610` лишь отправляет shutdown и будит backend; **Drop не делает join**. Архитектура patch этого не предполагает.

Healthy означает успешную регистрацию без известной invalidation, но не полноту покрытия:

- динамические watches добавляются после доставки исходных events;
- часть ошибок обхода и динамического добавления watches не передаётся handler;
- непрерывный поток inotify events может задерживать обработку команд, включая shutdown.

Safety scans и ownership budgets поэтому остаются необходимыми. Guarded раздельно перехватывает panic операций и destructor; он не является универсальным перехватчиком panic внутри backend thread.

## Необязательные рекомендации

1. Добавить проверку backend budget через production factory. `iroh-share/src/fs_watch/tests.rs:204–229` использует локальный semaphore и продолжит проходить даже при удалении acquisition из `fs_watch.rs:360–363`. Acquisition сейчас присутствует; это ограничение покрытия, не ошибка реализации.
2. Добавить тест закрытия event-channel с последним непрочитанным сигналом, проверяющий продолжение fallback без busy loop.

## Ограничения validation и baseline

Не выполнялись builds, тесты, install/package commands и проверка полного Cargo dependency graph. Содержимое новых README-ссылок на архитектурные документы отсутствует в snapshot и не оценивалось; их отсутствие в приложении не доказывает сломанные ссылки.

Не доказаны экономия батареи, реальные задержки и стоимость event storm. Cross-app writes в Termux, Android Doze/suspend, другие notify backends, патологические filesystem stalls и внутренности зависимостей за пределами supplied files остаются deployment limits. Таймеры задают scheduling policy, а не жёсткие wall-clock гарантии.

**Baseline, не findings patch:** metadata-only fingerprint consistency, mutable-file поведение `TryReference`, прежняя manual-refresh policy, control-plane/backpressure и глобальные квоты вне feature. Удержание tag не гарантирует неизменность referenced bytes или непрерывность announcement.

## Чеклист предыдущих восьми findings

1. Safety timer / starvation — **resolved**.
2. Stability retry без новых events — **resolved**.
3. Callback error / degraded health — **resolved**.
4. Registration lifecycle / root replacement — **resolved**; гонка lifecycle-тестов cycle-7 также исправлена.
5. Coalescing / стоимость обработки — **resolved** для bounded signals и rate limiting; батарея не подтверждена.
6. Helper crate / packaging — **resolved** структурно; сборочная проверка не выполнена.
7. Manual refresh policy — **baseline/documented**.
8. Недоказательные тесты — предыдущие actionable-проблемы **resolved**; необязательные улучшения покрытия перечислены отдельно.

**Область заключения:** независимый статический обзор приложенного cycle-8 snapshot и отчёта cycle-7. Команды не выполнялись; credentials, данные и сервисы не использовались; файлы и работающий daemon не изменялись. PASS относится только к рассмотренному opt-in patch и не является production certification или полным security audit.
