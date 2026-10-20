# Спека: фазы BackupOrphanSweeper — выделенный ключ /pgworker/backups/orphan-journal

Фаза 1 dev-flow. Все решения закрыты (режим делегирования: пользователь одобрил
«делай что раскопал самостоятельно»). Исполнитель плана вопросов не имеет —
ни одна развилка не остаётся открытой.

## 1. Цель

Фикс пункта код-ревью: orphan-фазы глобального свипера (`BackupOrphanSweeper`)
перестают писаться в per-cluster work-ключ `/pgworker/work/<C>` и пишутся в
выделенный глобальный ключ `/pgworker/backups/orphan-journal`. Устраняются:

1. RMW-гонка двух писателей одного ключа (свипер-лидер против держателя
   клэйма кластера — на multi-host это разные инстансы) с потерей
   HA-фактов / `rest_pending` / unreachable-трека / серии ретраев.
2. Фантомные per-cluster work-ключи удалённых кластеров: Deprovisioning D2
   удаляет `/pgworker/work/<C>`, а пут свипера (сирота — чаще всего префикс
   снесённого кластера) пересоздаёт ключ; панель видит журнал
   несуществующего кластера в `PgWorkerWork`.
3. Вечные фазовые метрики `worker_process_phase_duration_seconds` с растущей
   кардинальностью (orphan-фазы не терминальны, каждая сирота — новая
   фаза-строка).

## 2. Принципы

- **arch-first**: контракт меняется — сначала `arch/19-backups.md` §4 и
  `arch/18-metrics.md` §2.2, затем код. `arch/14` §3.3 уже каноничен (его
  op-перечень work-ключа `provision|deprovision|evacuate|rebuild` свипера не
  включает) — правка arch/14 не нужна.
- **Паттерн глобальных ключей домена сирот** — как у реестра: ключ вне
  per-cluster префиксов (D2-чистки не касается), пишет ТОЛЬКО глобальный
  лидер `/pgworker/leader` (единственный писатель — гонок нет), plain put
  без read-modify-write, «последняя фаза побеждает» (arch/19 §4, паттерн
  `storage`/`supervisor`).
- **Надёжность (base-rules №9)**: один писатель + plain put — переживает
  отказ любого инстанса без миграций и чисток; ключ перезаписывается каждым
  проходом свипера, отдельная чистка не нужна.
- Журнал — наблюдаемость, не барьер: неудача записи фазы НЕ роняет проход
  свипера (семантика сегодняшнего кода — результат `WritePhaseAsync`
  игнорируется).

## 3. Механика дефекта (проверено по фактическому коду worktree)

### 3.1. Кто и куда пишет сегодня

- `src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs` — глобальный
  процесс лидера `/pgworker/leader`; конструктор (строки 26-35) принимает
  `WorkJournal journal`. Шесть вызовов `journal.WritePhaseAsync` с
  cluster-аргументом `prefix.Split('/')[0]` (первый сегмент префикса сироты):
  - строка 126 — `orphan-key-cleaned/<C>/<X>` (санитар hold-ключей);
  - строка 140 — `orphan-key-cleaned/<C>/<X>` (санитар заявок);
  - строка 197 — `orphan-cancelled/<C>/<X>` (воскресший владелец);
  - строка 215 — `orphan-delete-requested/<C>/<X>` (по заявке, только
    `byRequest`);
  - строка 226 — `orphan-deleting/<C>/<X>`;
  - строка 262 — `orphan-deleted/<C>/<X>`.
  Все пишут в `/pgworker/work/<C>` кластера, чей S3-префикс удаляется.
- `src/Shared.Etcd/Coordination/WorkJournal.cs:93-131`
  (`WritePhaseAsync`) — RMW без транзакции: `ReadAsync` (carry-forward
  `unreachable`/HA-фактов/`rest_pending`, строки 109-121) → `PutAsync`
  (строки 126-127). Интерливинг read-read-put-put двух писателей: пут
  свипера с устаревшим carry затирает свежую запись владельца клэйма.
- `arch/14-pgworker.md` §3.3 (строка 678): `/pgworker/work/<C>` — «журнал
  ТЕКУЩЕГО процесса кластера»; op-перечень
  `provision|deprovision|evacuate|rebuild` свипера не содержит; владелец
  HA-полей — надзор, фазовые записи процессов их переносят. Смысловое
  допущение ключа — писатель один: держатель клэйма `<C>` (инвариант
  «мутации — только держателем клэйма», arch/14 §3.3, строки 692-700).
  Свипер-лидер — не держатель клэйма: его запись в work-ключ — отклонение
  кода от канона.

### 3.2. Что теряется (потребители читают ИЗ ключа)

- HA-факты `last_failover`/`last_rebuild`: надзор читает на старте тика —
  `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs:76-80`
  (`journal.ReadSupervisionStateAsync` → `HaFactState.FromStored(...)`).
  Пут свипера с устаревшим carry затирает факт насовсем (takeover открытых
  событий ломается).
- Окно REST-ротации `rest_pending`: ротатор читает из ключа —
  `src/PgWorker.Provisioning/Processes/ClusterSecretRotator.cs:105`
  (`work.Value?.RestPending`). Потеря — сломанная ROTATION-пара в полёте.
- Трек `unreachable`: самоисправляется следующим тиком надзора, но разово
  сбрасываются пороги детекции NodeDead/BrokerDead.
- Серия ретраев (`fail_count`/`fail_first_unix`/`retry_not_before_unix`):
  `WritePhaseAsync` её carry-forward НЕ делает (payload пишет
  `series?.*` — при null поля опускаются, arch/14 §3.3 «переносятся
  записями фаз внутри серии» — только явной передачей процессом). Любой пут
  свипера сбрасывает активный бэкофф процесса кластера.
- Фантомные ключи: `src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs:189`
  — D2 удаляет `/pgworker/work/<C>` при сносе кластера; пут свипера после
  D2 пересоздаёт ключ, панель видит его в `PgWorkerWork`
  (arch/adminpanel/02-etcd-contract.md §2.3.1) как журнал несуществующего
  кластера.
- Метрики: подписка `Program.cs:107` (`WorkJournal.PhaseWritten →
  OnJournalPhase`) пропускает orphan-фазы в
  `WorkerMetricsInstrumentation.ProcessPhase` — серии
  `worker_process_phase_duration_seconds{cluster=<C>,process="backup-orphan-sweeper",phase="orphan-…"}`
  никогда не закрываются (orphan-фазы не входят в `FinalPhases`), кардинальность
  фаз растёт с числом сирот.

### 3.3. Расхождения раскопанного анализа с фактическим кодом

Анализ подтверждён целиком; три уточнения:

1. **Тестовые места конструктора свипера — не одно, а четыре** (анализ
   называл только `BuildSweeper`): `BuildSweeper`
   (IntegrationTests, строки 50-53), инлайн-конструктор в
   `SweepKeyInTwoPoolsCountedTwice` (строки 419-422) — оба в
   `src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs`;
   плюс ДВА инлайн-конструктора в
   `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs`:
   `Create` (строки 29-33) и
   `ExecuteAsync_LeaderHangingSweep_PulsesActivityBeyondTick` (строки
   129-134). Без правки LoopTests код НЕ соберётся.
2. **`using Shared.Etcd.Coordination` в файле свипера отсутствует** —
   резолвится глобальным `<Using Include="Shared.Etcd.Coordination"/>` из
   `src/PgWorker.Backups/PgWorker.Backups.csproj`. Удалять из csproj нечего:
   тот же namespace нужен `ClaimStore`. Из файла свипера удаляется только
   параметр конструктора `WorkJournal journal`.
3. **Тест подавленных метрик-опов уже существует**:
   `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs:160`
   `OnJournalPhase_SuppressedOps_EmitNoPhaseSeries` — пополняется третьей
   строкой Act (см. §6, этап 2).

Дополнительно к анализу: `SuppressedOps` зеркалится в КОДЕ
(`WorkerMetricsInstrumentation.cs:20-21`) — правка arch/18 без правки кода
даст расхождение кода с arch (блокирующее замечание ревью); пополняем оба.

## 4. Целевое состояние (структура/компоненты)

### 4.1. Новый ключ etcd

- Ключ: `/pgworker/backups/orphan-journal` — ГЛОБАЛЬНЫЙ, вне per-cluster
  префиксов, D2-чистки не касается (как `/pgworker/backups/storage` и
  `/pgworker/backups/orphans`).
- Писатель: ТОЛЬКО глобальный лидер `/pgworker/leader` (гварды `claims.IsLeader`
  в `SweepAsync` уже стоят) — единственный писатель, гонок нет; plain put
  без read-modify-write (без `WorkJournal`), «последняя фаза побеждает».
- Чистка ключа не нужна и не вводится: перезаписывается каждым проходом.
- Панель ключ НЕ читает: состояние сирот наблюдаемо через
  `/pgworker/backups/orphans` + `/pgworker/backups/orphan-holds/` +
  `/pgworker/backups/orphan-deletes/`; строка в
  `arch/adminpanel/02-etcd-contract.md` не добавляется (§2 перечисляет только
  читаемые панелью ключи).

### 4.2. Формат значения

Минимальный WorkState-подобный JSON (snake_case, null-поля опускаются —
`JsonIgnoreCondition.WhenWritingNull`, как у `WorkState`):

```json
{"op":"backup-orphan-sweeper","phase":"orphan-deleting/<C>/<X>","instance":"<id>","updated_unix":T,"last_error"?}
```

- `phase` — БЕЗ ИЗМЕНЕНИЙ к нынешним строкам (они уже несут префикс сироты):
  `orphan-key-cleaned/<C>/<X>`, `orphan-cancelled/<C>/<X>`,
  `orphan-delete-requested/<C>/<X>`, `orphan-deleting/<C>/<X>`,
  `orphan-deleted/<C>/<X>`.
- `instance` — `claims.InstanceId`; `updated_unix` — из инжектированного
  `time.GetUtcNow().ToUnixTimeSeconds()` (тестируемость, как `nowUnix` в
  `SweepAsync`).
- `last_error` — сегодня у свипера всегда null (поле в payload присутствует
  для симметрии с `WorkState`; при null сериализацией опускается).
- Поля `unreachable`/`facts`/`rest_pending`/серия ретраев в новый ключ НЕ
  переносятся — это домен надзора/процессов кластера, к сиротам отношения
  не имеет.

### 4.3. Реализация (НЕ новый класс-store)

- Константа ключа: `public const string JournalKey = "/pgworker/backups/orphan-journal";`
  — в `OrphanRegistry` (`src/PgWorker.Backups/Supervisor/OrphanRegistry.cs`),
  рядом с `Key`/`HoldsPrefix`/`DeletesPrefix` (все ключи домена сирот — в
  одном месте; doc-comment в стиле соседних констант).
- Сериализация значения: `public static string JournalPhaseToJson(string phase, string instance, long updatedUnix, string? lastError)`
  в `OrphanRegistry` + приватный record `JournalPayload` с
  `[JsonPropertyName]`-полями `op`/`phase`/`instance`/`updated_unix`/`last_error`
  (`op` внутри метода константен `"backup-orphan-sweeper"`); образец —
  существующие `HoldToJson`/`DeleteToJson` с приватными
  `HoldPayload`/`DeletePayload`. Парсер в прод-коде не нужен (читателей
  нет; тест читает `JsonDocument`'ом).
- Свипер: приватный метод
  `private async Task WriteJournalAsync(string phase, CancellationToken ct)`
  в `BackupOrphanSweeper` — failover-put по `endpoints` (образец —
  существующий `PutRegistryAsync` свипера): по каждому endpoint
  `etcd.PutAsync(endpoint, OrphanRegistry.JournalKey, OrphanRegistry.JournalPhaseToJson(phase, claims.InstanceId, time.GetUtcNow().ToUnixTimeSeconds(), null), null, ct)`;
  первый успешный — `return`; все endpoints упали —
  `logger?.LogWarning` (журнал — наблюдаемость: проход НЕ ронять, результат
  put на исход `SweepAsync` не влияет — семантика сегодняшнего кода).
- Все 6 точек вызова (§3.1) заменяются на
  `await WriteJournalAsync($"orphan-…/{…}", ct);` — те же места, те же
  фазы-строки; cluster-аргумент больше не нужен (ключ глобальный).
- Зависимость `WorkJournal journal` УДАЛЯЕТСЯ: параметр primary-конструктора
  `BackupOrphanSweeper` (строка 32), аргумент DI в
  `src/PgWorker.App/Program.cs:774`
  (`sp.GetRequiredService<WorkJournal>()`). Явного using в файле свипера нет
  (§3.3 п.2); регистрация `WorkJournal`-сервиса в Program.cs:164 остаётся —
  её потребляют остальные процессы.
- Метрики: `WorkerMetricsInstrumentation.SuppressedOps`
  (`src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs:20-21`)
  пополняется `"backup-orphan-sweeper"` — зеркало arch/18 §2.2 и защита от
  будущих писателей orphan-фаз через `WorkJournal` (событие `PhaseWritten`
  после фикса свипером не эмитится; suppression закрывает канон и случай
  возврата). Комментарий у множества обновляется: «...evacuate (только
  waiting-*/blocked-moving) и backup-orphan-sweeper (журнал домена сирот —
  свой ключ /pgworker/backups/orphan-journal, arch/19 §4)».

## 5. Этапы работ

### Этап 0 — arch-first (ДО кода; только arch/, отдельный коммит ветки)

1. `arch/19-backups.md` §4 (таблица, строки 591-601):
   - добавить строку ПОСЛЕ `/pgworker/backups/orphan-deletes/<C>/<X>`
     (строка 599), перед строкой restore (строка 600):

     > `| /pgworker/backups/orphan-journal | журнал фаз лидер-прохода свипера сирот: {"op":"backup-orphan-sweeper","phase":"orphan-…/<C>/<X>","instance":"<id>","updated_unix":T,"last_error"?} (фазы: orphan-key-cleaned/orphan-cancelled/orphan-delete-requested/orphan-deleting/orphan-deleted — каждая с «/<C>/<X>»); ключ ГЛОБАЛЬНЫЙ (вне per-cluster префиксов, D2-чистки не касается); пишет ТОЛЬКО глобальный лидер /pgworker/leader (единственный писатель — гонок нет; plain put без read-modify-write); «последняя фаза побеждает» — ключ перезаписывается каждым проходом, чистка не нужна; панель ключ НЕ читает (состояние сирот наблюдаемо через /pgworker/backups/orphans + orphan-holds + orphan-deletes) |`

   - в строке 597 (`orphans`) заменить фрагмент
     «кандидат → DELETING (journal) → batch-delete» на
     «кандидат → DELETING (фаза orphan-journal) → batch-delete»;
   - в строке 599 (`orphan-deletes`) заменить фрагмент
     «исполняет sweeper ближайшим проходом: DELETING (journal) → batch-delete»
     на «исполняет sweeper ближайшим проходом: DELETING (фаза orphan-journal)
     → batch-delete».
   - Прочие «journal»-упоминания arch/19 (строки 327, 342, 552-562, 605,
     644, 867) — записи владельцев клэйма в `/pgworker/work/<C>`,
     корректные: НЕ трогать.
2. `arch/18-metrics.md` §2.2:
   - строка 92 (таблица `worker_operation_total`): «подавленные ops —
     supervise/evacuate — не считаются, см. ниже» → «подавленные ops —
     supervise/evacuate/backup-orphan-sweeper — не считаются, см. ниже»;
   - абзац строк 108-112 привести к виду (добавка третьего op):

     > «Стационарные ops без терминальной фазы — `supervise` (пишется и через
     > `WriteSupervisionAsync`, мимо фазового события) и `evacuate` (только
     > `waiting-*`/`blocked-moving`), а также `backup-orphan-sweeper`
     > (журнал домена сирот — свой ключ `/pgworker/backups/orphan-journal`
     > (arch/19 §4), в work-фазовые метрики не входит) — фазовых серий НЕ
     > эмитят: их живость закрывает `worker_loop_last_success_timestamp_seconds`
     > (алерт WorkerLoopStalled), вечные серии не копятся.»

   - Список `process` у PgWorker (строки 96-99) НЕ пополняется: свипер в
     журнал работы больше не пишет.
3. `arch/adminpanel/02-etcd-contract.md` — БЕЗ правок: §2 перечисляет только
   читаемые панелью ключи, новый ключ панель не читает; orphan-опы исчезают
   из `PgWorkerWork`-листинга автоматически (их там быть не должно — цель
   фикса). Зафиксировано здесь как решение, в файл правки нет.

### Этап 1 — код (после правок arch)

1. `OrphanRegistry`: константа `JournalKey` + `JournalPhaseToJson` +
   приватный record `JournalPayload` (§4.3).
2. `BackupOrphanSweeper`: удалить параметр `WorkJournal journal`; заменить
   6 вызовов `journal.WritePhaseAsync` на `await WriteJournalAsync(...)`;
   добавить приватный `WriteJournalAsync` (§4.3). Поведение прохода вне
   журнальных точек не меняется.
3. `src/PgWorker.App/Program.cs:768-779`: из фабрики `BackupOrphanSweeper`
   удалить аргумент `sp.GetRequiredService<WorkJournal>()` (строка 774).
4. `WorkerMetricsInstrumentation.SuppressedOps`: добавить
   `"backup-orphan-sweeper"` + обновить комментарий (§4.3).
5. Тестовые конструкторы (компиляция):
   - `src/tests/PgWorker.IntegrationTests/Backups/BackupOrphanSweeperTests.cs`:
     `BuildSweeper` (убрать аргумент `new WorkJournal(...)` — строка 52) и
     инлайн-конструктор `SweepKeyInTwoPoolsCountedTwice` (строки 419-422).
   - `src/tests/PgWorker.UnitTests/App/BackupOrphanSweeperLoopTests.cs`:
     два инлайн-конструктора (строки 29-33 и 129-134) — убрать аргумент
     `new WorkJournal(...)`.
   - Using-и тестовых файлов НЕ трогаются: `Shared.Etcd.Coordination` в обоих
     тестовых проектах — глобальный `<Using>` из csproj (нужен другим файлам:
     `ClaimStore`, `WorkJournalParserTests` и др.); в самих файлах явного
     using нет.

### Этап 2 — тесты

1. `BackupOrphanSweeperTests` (IntegrationTests, OwnedEtcdFixture):
   - `SeedAsync`: добавить чистку нового ключа (изоляция сценариев) —
     `await fixture.Gateway.DeleteAsync(fixture.Endpoint, OrphanRegistry.JournalKey, prefix: false, ct);`
     рядом с существующими чистками (строки 36-41);
   - НОВЫЙ сценарий (AAA-комментарии обязательны), рядом с
     `SweepTtlExpiredRemovesPrefixAndRecord`:
     «после прохода с TTL-удалением сироты ключ
     /pgworker/backups/orphan-journal содержит последнюю фазу»: Arrange —
     как в `SweepTtlExpiredRemovesPrefixAndRecord` (сид, лидерство, ttl 60,
     первый проход, время за TTL); Act — проход с удалением; Assert —
     `GetAsync(JournalKey)` отдаёт JSON с `op == "backup-orphan-sweeper"`,
     `phase == "orphan-deleted/ghost/s1"`, `instance ==
     claims.InstanceId`, `updated_unix == clock.Now.ToUnixTimeSeconds()`
     (JsonDocument/десериализация — без прод-парсера), И work-ключ
     `/pgworker/work/ghost` ОТСУТСТВУЕТ (прямой ассерт цели фикса —
     фантомных записей нет);
   - расширить `SweepWithoutLeadershipWritesNothing` ассертом: ключ
     `OrphanRegistry.JournalKey` отсутствует (не-лидер не пишет и журнал).
2. `WorkerMetricsInstrumentationTests.OnJournalPhase_SuppressedOps_EmitNoPhaseSeries`:
   дополнить Act третьей строкой
   `sut.OnJournalPhase("demo", "backup-orphan-sweeper", "orphan-deleting/ghost/s1");`
   (Assert без изменений — ни серий, ни операций), комментарий теста
   дополнить упоминанием свипера.
3. Существующие сценарии `BackupOrphanSweeperTests` не переписываются:
   их ассерты (реестр/объекты/ключи hold-заявок) от журнала не зависят.

### Этап 3 — верификация (канон запуска: сборка и тесты ДВУМЯ отдельными командами)

1. Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   — ноль warnings (`TreatWarningsAsErrors=true`).
2. Юниты:
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~BackupOrphanSweeperLoopTests|FullyQualifiedName~WorkerMetricsInstrumentationTests"`.
3. Интеграция (docker etcd, OwnedEtcdFixture):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~BackupOrphanSweeperTests`.
   После серии — зачистка docker-окружения (контейнеры/сети фикстуры; правила
   AGENTS.md о зачистке после КАЖДОЙ серии). Таймауты наблюдения — канон
   base-rules №13 (старт виден в логе, зависаний нет).
4. Мерж-гейт (код воркера меняется — правило AGENTS.md): свежий Release +
   статическая подготовка ОДНИМ процессом ДО тестов
   `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`,
   затем docker-E2E минимум-маркер
   `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`
   (свежесть бинаря — отдельной командой сборки п.1; provisioning/portalloc/
   moves не меняются — полного E2eFixture не требуется). После серии —
   зачистка.

## 6. Ограничения (осознанные, вне scope)

1. Остальные потребители `WorkJournal` (provisioning-процессы, moves,
   `WalStreamProcess`, `RetentionProcess`, `BackupSupervisorProcess`,
   backup/restore/drill-процессы и аналоги Kafka/S3/ValkeyWorker) НЕ
   трогаются: они пишут фазы в work-ключ СВОЕГО кластера как держатели
   клэйма — их RMW-интерливинг с надзором того же инстанса — отдельная
   тема, не этого фикса.
2. Существующие фантомные work-ключи на стендах (если накопились) — вне
   scope: система домашняя, прод-миграций нет; чистка руками по надобности.
3. `last_error` свипером не пишется (всегда null) — расширение не вводится.
4. Панель/алерты: impact-а нет — алерт `provision-stuck` кормится
   `last_error`+`fail_first_unix` из work-ключей (свипер писал `last_error=null`,
   fail-полей не имел); из листинга `PgWorkerWork` orphan-опы исчезают — это
   цель, а не регрессия.
5. Никаких записей `docs/**` кроме настоящей спеки (история задач — сам
   `docs/superpowers/`-каталог) и roadmap-пунктов не создаётся.

## 7. Принятые решения (закрытые развилки)

| # | Развилка | Решение |
|---|---|---|
| Р1 | Куда пишет свипер | Новый ОДИН ГЛОБАЛЬНЫЙ ключ `/pgworker/backups/orphan-journal`, вне per-cluster префиксов (D2 не касается), писатель — только лидер, plain put, «последняя фаза побеждает», без чистки (перезаписывается проходом). |
| Р2 | Формат значения | Минимальный WorkState-подобный JSON `op/phase/instance/updated_unix/last_error?`; фазы-строки без изменений; поля надзора/ретраев НЕ переносятся; `last_error` — поле есть, значение null. |
| Р3 | Реализация | Без нового класса-store: const `OrphanRegistry.JournalKey` + `OrphanRegistry.JournalPhaseToJson` (образец `HoldToJson`) + приватный `WriteJournalAsync` свипера (образец `PutRegistryAsync`, failover-put); зависимость `WorkJournal` удаляется из свипера, Program.cs и всех четырёх тестовых конструкторов; глобальный using csproj остаётся (нужен `ClaimStore`). |
| Р4 | Порядок и объём arch-правок | arch-first: arch/19 §4 — новая строка таблицы + замена «(journal)» на «(фаза orphan-journal)» в строках orphans/orphan-deletes; arch/18 §2.2 — suppression backup-orphan-sweeper рядом с supervise/evacuate (строка 92 + абзац 108-112), список process не пополняется; arch/14 и adminpanel/02 — без правок (канон уже верен / панель ключ не читает). |
| Р5 | Панель/алерты | Без изменений: алертам корм не портится, orphan-опы уходят из `PgWorkerWork` (цель фикса). |
| Р6 | Легаси-фантомные ключи | Вне scope, миграций нет. |
| Р7 | Тесты | §5 этап 2: чистка ключа в `SeedAsync`; новый AAA-сценарий (последняя фаза `orphan-deleted/<C>/<X>`, `op`, `instance`, `updated_unix` + ОТСУТСТВИЕ `/pgworker/work/<C>`); ассерт в `SweepWithoutLeadershipWritesNothing`; третья строка Act в `OnJournalPhase_SuppressedOps_EmitNoPhaseSeries`. Прогон — интегросерия `BackupOrphanSweeperTests` (две отдельные команды) + юниты; зачистка после серий. |
| Р8 | Остальные потребители WorkJournal | Не трогаются (§6 п.1). |
| Д1 | Ошибки `WriteJournalAsync` | Не влияют на исход прохода (семантика текущего кода: результат журнала игнорировался); все endpoints упали — `logger?.LogWarning`, не исключение. |
| Д2 | Источник времени | Инжектированный `time` свипера (тестируемость), не `DateTimeOffset.UtcNow`. |
| Д3 | SuppressedOps в коде | Пополняется `"backup-orphan-sweeper"` — зеркало arch/18 и защита от будущих писателей (§4.3). |
| Д4 | Читатель ключа | Только оператор (etcd-инспектор). Панель/прод-парсер не вводятся; тест читает `JsonDocument`'ом. |
| Д5 | E2E-гейт | Задача трогает код воркера → минимум-маркер `Scale_AddEmptyShard` на свежем Release + `E2ePrepare` (AGENTS.md); полный E2eFixture не нужен (provisioning/portalloc/moves не меняются). |

## 8. Критерии приёмки

1. `grep -rn "WritePhaseAsync" src/PgWorker.Backups/Supervisor/BackupOrphanSweeper.cs`
   — пусто; конструктор свипера не принимает `WorkJournal`.
2. После прохода свипера с удалением сироты: ключ
   `/pgworker/backups/orphan-journal` содержит последнюю фазу
   `orphan-deleted/<C>/<X>` c `op=backup-orphan-sweeper`; ключ
   `/pgworker/work/<C>` сироты НЕ создаётся (интеграционный ассерт зелёный).
3. Не-лидер не пишет ни реестр, ни orphan-journal (ассерт
   `SweepWithoutLeadershipWritesNothing`).
4. `WorkerMetricsInstrumentation.SuppressedOps` содержит
   `backup-orphan-sweeper`; тест подавленных opов зелёный (ни серий, ни
   операций от orphan-фаз).
5. arch/19 §4 описывает ключ `/pgworker/backups/orphan-journal` (строка
   таблицы; «(journal)»-упоминания orphans/orphan-deletes указывают на него);
   arch/18 §2.2 содержит backup-orphan-sweeper в списке подавленных; правки
   arch закоммичены ДО кодовых (или одним коммитом ветки с arch-частью
   первой в diff-порядке — этап 0 фактически исполнен первым).
6. Сборка Release — ноль warnings; юниты (`BackupOrphanSweeperLoopTests`,
   `WorkerMetricsInstrumentationTests`) и интегросерия
   `BackupOrphanSweeperTests` зелёные; docker-окружение после серий зачищено;
   E2E-минимум `Scale_AddEmptyShard` на свежем Release зелёный.
7. Потребители `WorkJournal` вне свипера не изменились
   (`git diff --stat` не содержит их файлов).
