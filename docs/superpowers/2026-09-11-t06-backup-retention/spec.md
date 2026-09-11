# Spec: t06-backup-retention — управление бэкапами и местом

Дата: 2026-09-11
Канон: [arch/19-backups.md](../../../arch/19-backups.md) §4 (ретенция, ключ
`/pgworker/backups/storage`, guard, API policy), §5 (ретенционная чистка
layout), §3 (пересчёт `chain_start` после чистки), §8/§9/§10 (карта, конфиг
`Retention`/`Quota`, риски); панельный контракт —
[arch/adminpanel/02-etcd-contract.md](../../../arch/adminpanel/02-etcd-contract.md)
§2.3.1; контекст сервиса — [arch/14-pgworker.md](../../../arch/14-pgworker.md).
Правки канона этой задачей — §3/§4/§5/§8/§9/§10 + 02 §2.3.1 (внесены до
кода, этой же веткой). Roadmap:
[`t06-backup-retention`](../../../arch/roadmap/backup.md).

## 1. Цель

Управление жизненным циклом бэкапов шардов и местом в хранилище:

1. **GFS-ретенция полных бэкапов** (календарная схема): дневные — все полные
   последних `retention.days` календарных суток UTC; недельные — последний
   полный каждой из `retention.weeks` предыдущих календарных недель
   (ISO-неделя); месячные — последний полный каждого из `retention.months`
   предыдущих календарных месяцев. Устаревшие полные удаляются (объекты S3 +
   etcd-ключ) через транзитную фазу `DELETING`; guard держит последний
   COMPLETED полного шарда всегда.
2. **Чистка WAL**: сегменты префикса `wal/` строго ниже стартовой точки
   (`wal_start_segment`) старейшего ОСТАВЛЯЕМОГО полного бэкапа удаляются
   (включая `.history` таймлайнов ниже стартового); `chain_start` t03
   поднимается автоматически, list-префикс укорачивается.
3. **Защита от переполнения хранилища**: занятость bucket (`used_bytes`)
   считается ретенционным проходом и публикуется в глобальный etcd-ключ
   `/pgworker/backups/storage` с вердиктом OK/WARN/CRIT относительно квоты
   `Quota { Bytes, WarnPercent, CritPercent }`; панель зажигает алерт
   `backup-storage-quota`. Реакции автоматикой (emergency-чистка, запрет
   новых полных) — нет, действует оператор.
4. **API приёма per-cluster политики** `POST /api/clusters/<C>/backups/policy`
   (валидация + put policy-ключа; формат — канон §4, замыкает решение t02
   «ручная запись до t06»).
5. **Гигиена истории**: FAILED-ключи шарда — держать последние
   `Retention:KeepFailed`, старше — удалять (etcd не растёт от циклов
   переснятия; бэкофф t02 не ломается).

Границы t06:

- ВХОДИТ: `RetentionProcess` + чистые функции отбора/планов чистки,
  расширение `BackupS3` (list с размерами, batch-delete), DELETING-фаза,
  ключ хранилища + воркер-монитор занятости, API policy, панельные правила
  `backup-storage-quota`/`backup-deleting-stuck`, конфиг
  `Retention`/`Quota`, E2E-сценарий ретенции.
- НЕ входит: verify (t04 — идёт параллельно, статусы OK/FAILED может не
  быть; ретенция от них НЕ зависит, см. §2 п.6), восстановление (t05),
  reconcile S3↔etcd/сироты (t07), UI бэкапов и грань MinIO (t08),
  emergency-чистка и запреты новых полных при нехватке места (осознанно
  исключено решением пользователя; при необходимости — t07+), per-cluster
  квоты (квота per-install на bucket).

### Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| GFS-семантика отбора | Календарный GFS (UTC): последние `days` суток — все; далее последний COMPLETED каждой из `weeks` календарных недель и каждого из `months` календарных месяцев; классика pgBackRest/WAL-G, детерминирован по `started_unix` |
| API приёма policy в t06 | Да: `POST /api/clusters/{C}/backups/policy` (mTLS-контур API воркера, валидация + put); по канону arch/19 §4 и решению t02 «API — t06+». UI-кнопка панели — t08 |
| «Валидный» без t04 (параллельная задача) | Валидный = COMPLETED; verify на guard НЕ влияет. Guard: последний COMPLETED не удаляется никогда, даже просроченный. После мержа t04: `verify.state=FAILED` — первые кандидаты на удаление, guard «≥1 COMPLETED» держится и над ними |
| Реакция на нехватку места | Мониторинг + панельный алерт (`backup-storage-quota` по state ключа storage); ретенция — детерминированный GFS; без emergency-чисток и запретов новых полных |

## 2. Принципы

1. **arch-first**: канон arch/19 (§3/§4/§5/§8/§9/§10) и панельный контракт
   02 §2.3.1 обновлены этой задачей ДО кода; расхождения вскрываются —
   канон правится тем же dev-flow.
2. **PgWorker — единственный писатель** `/pgworker/backups/*` (клэйм `<C>`);
   ретенция — единственный осознанный удаляющий S3-объектов воркера
   (стоп-семантика/deprovisioning объекты не трогают — R4; сироты — t07).
3. **Удаление — через транзитную фазу**: journal-before-manipulations —
   `DELETING` до delete-объектов; идемпотентные повторные проходы доводят
   прерванное удаление (takeover/рестарт воркера ничего не ломают).
4. **Данные дороже места, но место конечно**: guard держит последний
   COMPLETED всегда; чистка WAL — строго ниже стартовой точки старейшего
   оставляемого; всё, что правее стартовой точки любого оставляемого
   полного, — неприкосновенно.
5. **Отбор и планы чистки — чистые функции** (без I/O): GFS-календарь,
   WAL-план, verdict хранилища — полный юнит-покрытием (риск «удалили
   нужное» закрывается тестами детерминированности, а не ревью наизусть).
6. **Деградация без t04**: ретенция не читает verify как обязательное
   (модель уже несёт `verify: PENDING` от t02); при появлении
   `verify.state=FAILED` (мерж t04) приоритет удаления включается сам по
   данным модели — синхронизация мерж-гейтов не требуется.
7. **Паттерны arch/17/14**: идемпотентность каждого шага, transient vs
   permanent (S3-отказ → DELETING остаётся, повтор; etcd-отказ → повтор
   тика), клэйм-гварды, тик не блокируется на длинные операции (один
   полный за проход, batch-delete чанками).
8. **Поведение по умолчанию не меняется**: `Backups:Enabled=false` (дефолт)
   — ретенция no-op; существующие тесты зелёные без правок ожиданий.
9. Тесты: docker-порты только динамические, таймауты фикстур ≤100 с,
   зачистка контейнеров/сетей после серий (`docker network prune -f`),
   полный teardown сценариев — ассерт чистоты; E2E на свежем Release —
   обязателен (тронуты PgWorker.App/Backups). Язык — русский для
   документации/комментариев, идентификаторы — английский; AAA-комментарии
   в тестах; `TreatWarningsAsErrors=true`, centralized packages.

## 3. Структура/компоненты

### 3.1. Чистые функции отбора — `src/PgWorker.Backups/Retention/RetentionPlanner.cs`

Без I/O, TimeProvider-инъекция не нужна (момент передаётся аргументом).
Полное юнит-покрытие (фаза Ф1):

- **`SelectKeep(fulls, policy, nowUtc) → RetentionSelection`**: вход —
  COMPLETED-полные шарда (Id, StartedUnix, WalStartSegment, Verify) +
  `BackupPolicy` (RetentionDays/Weeks/Months) + момент; выход — `Keep`
  (множество id) и `Delete` (отсортированные кандидаты). Правила:
  - в отборе участвуют только COMPLETED (PLANNED/RUNNING/UPLOADING —
    чужая машина состояний t02; DELETING — доводка, не отбор);
  - дневные: все COMPLETED с `started_unix` в последних
    `retention.days` календарных сутках UTC (уникальные календарные даты,
    включая текущую);
  - недельные: полные вне дневного окна группируются по ISO-неделе UTC
    (`ISOWeek.GetWeekOfYear`-эквивалент над `started_unix`); из каждой из
    `retention.weeks` свежейших групп — последний (`max started_unix`);
  - месячные: аналогично по календарному месяцу UTC, `retention.months`
    свежейших групп, из каждой — последний;
  - guard: самый свежий COMPLETED — всегда в Keep; если после отбора
    Delete непусто И в Keep нет ни одного COMPLETED — перевести старейший
    кандидат обратно в Keep (последний валидный не удаляется);
  - порядок Delete: `verify.state=FAILED` — первыми (после мержа t04;
    сегодня таких нет — порядок вырождается в старшинство), далее по
    возрастанию `started_unix` (старейшие вперёд — место освобождается
    от самого старого).
- **`SelectWalForDeletion(objectNames, cutoffSegment) → IReadOnlyList<string>`**:
  сегменты с `(Tli < cutoff.Tli) || (Tli == cutoff.Tli && позиция
  (Log·256+Seg) < позиции cutoff)`; `.history` с `TLI < cutoff.TLI`;
  сам cutoff, `.history` cutoff-TLI и новее, сегменты выше/новее — НЕ
  входят (консервативно: объект с `TLI > cutoff.TLI` позицией ниже cutoff
  не трогаем — контроль t03 его игнорирует, удалять незачем).
- **`EvaluateStorage(usedBytes, quotaBytes, warnPercent, critPercent) →
  StorageVerdict`**: квота 0/не задана → `OK` без процентов; иначе
  `usedPercent >= critPercent → CRIT`, `>= warnPercent → WARN`, иначе
  `OK`.

### 3.2. S3-операции — расширение `src/PgWorker.Backups/BackupS3.cs`

Интерфейс `IBackupS3` дополняется (реализация — тот же AWSSDK-клиент,
path-style):

- **`ListPrefixAsync(string prefix, int? maxKeysPerTest = null)`** →
  `IReadOnlyList<S3ObjectInfo>` (`Key` — полный ключ объекта, `SizeBytes`,
  `LastModified`); list-objects-v2 с пагинацией (образец — существующий
  `ListWalAsync`, но с размерами и произвольным префиксом: `full/<id>/`,
  `wal/`, весь bucket — пустой префикс).
- **`DeleteKeysAsync(IReadOnlyList<string> keys)`**: batch-delete
  (DeleteObjects, чанки по 1000 ключей); идемпотентно — отсутствие ключа
  в batch-ответе не ошибка (повтор прохода после сбоя безопасен).

Загрузку по-прежнему делает `mc` в джобах/агентах — воркер получает
delete-операции ВПЕРВЫЕ и только в ретенционных путях (R4-инвариант
остальных путей сохранён).

### 3.3. Процесс — `src/PgWorker.Backups/Retention/RetentionProcess.cs`

Машина одного тика per-cluster (по образцу `BackupProcess`: клэйм-гвард,
`IEtcdGateway` с failover-перебором endpoints, `WorkJournal`,
`TimeProvider`). Вызов — `ClusterProcesses.RetentionAsync(snap, backups,
ct)` из Active-ветки `ReconcileLoop` ПОСЛЕ `backup-wal`, до `repair`, под
guard `Backups:Enabled` (как `BackupsAsync`). Расписание per-cluster:
`ConcurrentDictionary<cluster, lastUnix>`, период
`Retention:IntervalSec` (не каждый тик — list/delete редкая тяжёлая
работа).

Тик (кратко; guard'ы — как у BackupProcess):

```
G0  клэйм не наш → отказ; Enabled=false → Done (no-op); кластер не Active → Done
S   расписание: now - lastPass < IntervalSec → только storage-монитор (§3.4)

на каждый шард X из снапшота (dsn не обязателен — ретенция работает
по etcd-ключам; шард без ключей бэкапов → skip):
  1. DELETING-доводка: каждый полный в DELETING → list full/<id>/ →
     есть объекты → DeleteKeysAsync → list снова пуст → del etcd-ключа;
     transient-отказ S3 → статус не трогаем, следующий проход повторит
  2. GFS-отбор RetentionPlanner.SelectKeep(COMPLETED-полные, policy, now)
     (policy — policy-ключ кластера из снапшота, null → дефолт конфига
     PgWorker:Backups:Policy)
  3. удаление ОДНОГО кандидата (первый из Delete): put {state: DELETING}
     (journal-before-manipulations) → list+delete full/<id>/ → del ключа
     → journal "deleted-full/<X>/<id>"
  4. чистка WAL: cutoff = min(wal_start_segment оставляемых COMPLETED,
     т.е. Keep ∪ ещё не удалённые после шага 3); cutoff есть →
     list wal/ → SelectWalForDeletion → DeleteKeysAsync → journal
     "wal-trimmed/<X>/<кол-во>" ; cutoff нет (полных нет / без
     wal_start) → пропустить
  5. гигиена FAILED: FAILED-ключи по started_unix, держать последние
     Retention:KeepFailed, старше → del etcd-ключей (journal
     "failed-pruned/<X>/<кол-во>")
```

- Ошибка шарда не роняет остальные (обёртка try/catch + журнал — образец
  `WalStreamProcess.TickShardAsync`).
- После прохождения всех шардов — storage-монитор (§3.4) и фиксация
  `lastPass`.
- Гонки с t02/t03 исключены клэймом и фазами: ретенция не трогает
  активные (PLANNED/RUNNING/UPLOADING) записи; удаляемые — только
  COMPLETED→DELETING (t02 их уже не супервизит); WAL-чистка режет только
  ниже cutoff — агент грузит только свежие сегменты (слот дальше).

### 3.4. Монитор занятости — ключ `/pgworker/backups/storage`

В том же `RetentionProcess` (per-instance расписание — словарь
`"storage" → lastUnix`, тот же `Retention:IntervalSec`):

1. `ListPrefixAsync("")` — весь bucket (включая чужие/осиротевшие
   префиксы: занятость bucket, не только свои кластеры; осиротевшее
   место тоже съедает квоту — увидеть его в used — фича) → `used_bytes`
   = сумма `SizeBytes`.
2. `EvaluateStorage(used, Quota:Bytes, WarnPercent, CritPercent)` →
   verdict.
3. Put ключа `/pgworker/backups/storage` при изменении (сравнение с
   текущим значением — образец `WalStatusWriter.WriteIfChangedAsync`):
   `{"used_bytes":…,"quota_bytes"?,"used_percent"?,"state":"OK|WARN|CRIT","updated_unix":…}`;
   `quota_bytes=0` → поля квоты/процентов опускаются, `state="OK"`.
   Сериализация — `Retention/StorageStatusJson.cs` (по образцу
   `BackupStatusJson`).

Ключ глобальный (вне per-cluster префиксов): D2-чистка deprovisioning его
не трогает; пишет проход любого живого клэйма — значения одинаково свежие,
put при изменении идемпотентен. Воркерный `BackupsParser` ключ
пропускает автоматически (глубина сегментов пути < 5 — «не наша забота»);
воркер сам ключ не читает.

### 3.5. API приёма policy — `src/PgWorker.App/Api/Operations/BackupsPolicyHandler.cs`

`POST /api/clusters/{cluster}/backups/policy` (регистрация в
`ApiModule.MapPost`, mTLS-контур существующий):

- Тело — JSON канона §4: `{"retention":{"days":…,"weeks":…,"months":…},
  "full_max_age_sec":…,"verify":{"on_create":…}}` (поля retention могут
  опускаться — как парсер: отсутствующее → дефолты; но тело целиком
  замещает политику — put полного значения).
- Валидация (400 с перечнем нарушений): `days ∈ [1..365]`,
  `weeks ∈ [0..52]`, `months ∈ [0..120]`,
  `full_max_age_sec ≥ 600`, `on_create ∈ {true,false}`; мусорный JSON — 400.
- Гвард кластера (404 если `/clusters/<C>/config` отсутствует — образец
  существующих хендлеров, `ClusterGuardData`).
- Успех → put `/pgworker/backups/<C>/policy` (сериализация канона) → 200
  с применённым значением. Политика действует со следующего тика
  планировщика t02/ретенции (отдельной нотификации нет — etcd-снапшот).

### 3.6. Панель AdminPanel

- **Парсер** (`AdminPanel.Etcd/Parsing/BackupsParser.cs`): ключ
  `/pgworker/backups/storage` → `BackupStorageInfo { UsedBytes,
  QuotaBytes?, UsedPercent?, State, UpdatedUnix }`; попадает в
  `BackupsInfo.Storage` (`AdminPanel.Core`). Битый JSON — parseError,
  толерантно (паттерн панели).
- **Правила алертов** (`AdminPanel.Core/Alerting/Rules/`, паттерн
  `IAlertRule`, регистрация в AlertEngine):
  - `backup-storage-quota`: ключа нет → молчим; `state=WARN` → warning,
    `state=CRIT` → critical; текст — used/quota/проценты + remedy
    «расширить квоту или ужать политику ретенции»;
  - `backup-deleting-stuck` (warning): полный в `DELETING` с возрастом
    (`finished_unix`, иначе `started_unix`) старше
    `Alerts:Backups:DeletingStaleSec` (дефолт 21600 = 6 ч) — ретенция
    не может довести удаление (S3-отказ и т.п.).
- UI-грань (список бэкапов, MinIO) — t08; алерты видны в общем списке.

### 3.7. Конфигурация

`BackupsOptions` (App) + `BackupsRuntimeOptions` (Backups) дополняются
(канон §9):

```
PgWorker:Backups {
  ...existing...
  Retention { IntervalSec=600, KeepFailed=20 }   # период прохода, FAILED-глубина
  Quota { Bytes=0, WarnPercent=80, CritPercent=90 }  # 0 = квота не задана
}
```

Валидация старта (fail-fast, образец существующих `BackupsOptionsTests`):
`WarnPercent < CritPercent ≤ 100`, `IntervalSec ≥ 60`, `KeepFailed ≥ 5`.
Панель: `Alerts:Backups { DeletingStaleSec=21600 }`.

### 3.8. Отражение в коде (сводка)

| Место | Что |
|---|---|
| `arch/19-backups.md`, `arch/adminpanel/02-etcd-contract.md` | контракт ретенции (уже внесён этой веткой) |
| `src/PgWorker.Backups/Retention/` | `RetentionPlanner.cs` (чистые функции), `RetentionProcess.cs` (тик), `StorageStatusJson.cs` |
| `src/PgWorker.Backups/BackupS3.cs` | `ListPrefixAsync` (с размерами), `DeleteKeysAsync` (batch) |
| `src/PgWorker.App/Options.cs` | `BackupsRetentionOptions`, `BackupsQuotaOptions` (+валидация, маппер `ToRuntime`) |
| `src/PgWorker.App/Loops/` | `IClusterProcesses.RetentionAsync` + wiring `ReconcileLoop` (после `backup-wal`, guard Enabled) |
| `src/PgWorker.App/Api/` | `Operations/BackupsPolicyHandler.cs` + маршрут в `ApiModule` |
| `src/AdminPanel.Etcd/Parsing/`, `src/AdminPanel.Core/` | parse `storage`-ключа, `BackupStorageInfo`, `BackupsInfo.Storage`, правила `backup-storage-quota`/`backup-deleting-stuck` |
| `src/tests/*` | юниты планнера/правил/опций/API-валидации; интеграция S3/процесса; E2E-сценарий |

## 4. Фазы (порядок исполнения)

- **Ф0. Канон** (выполнено этой веткой до spec): arch/19 §3/§4/§5/§8/§9/§10,
  adminpanel/02 §2.3.1. Первым коммитом кода.
- **Ф1. Чистые функции**: `RetentionPlanner` (GFS-календарь, guard,
  verify-приоритет), `SelectWalForDeletion`, `EvaluateStorage`,
  `StorageStatusJson`; юниты (включая календарные точки: недели/месяцы
  через границы, високосность/ISO-недели, пустые наборы, политика 0/0/0).
- **Ф2. S3**: `ListPrefixAsync`/`DeleteKeysAsync` + интеграция против
  testcontainers-MinIO (динамический порт): пагинация, размеры,
  batch-delete чанками, идемпотентность повтора, несуществующие ключи.
- **Ф3. Процесс**: `RetentionProcess` (расписание, DELETING-доводка,
  один-кандидат-за-проход, WAL-чистка, гигиена FAILED, storage-монитор),
  опции + валидация, wiring `ClusterProcesses`/`ReconcileLoop`;
  интеграционные тесты на fake-deps (docker/S3 фейки по образцу
  `FakeBackupDeps`): полный цикл удаления, guard, cutoff, FAILED-глубина,
  клэйм/Enabled-гварды.
- **Ф4. API policy**: хендлер + валидация + маршрут; тесты 200/400/404
  (интеграция API-хоста по образцу существующих хендлеров).
- **Ф5. Панель**: парсер `storage`, модель, 2 правила; юниты правил
  (нет ключа/WARN/CRIT/DELETING-свежий/застарелый).
- **Ф6. E2E** (свежий Release, `PGW_TEST_DOCKER=1`): сценарий
  `Retention_TrimsToGfs` — изолированное окружение сценария (E2eEnvironment:
  своя сеть/etcd/MinIO, динамические порты, полный teardown с ассертом
  чистоты): provisioned-кластер + включённые бэкапы → дождаться одного
  реального COMPLETED полного (маленький `full_max_age_sec`) → посеять
  синтетику: etcd-ключи «старых» COMPLETED-полных (started_unix ~40 дней
  назад, wal_start ниже реального) + их объекты `full/<old-id>/` в MinIO
  (mc-контейнер) + WAL-объекты ниже cutoff → policy `days=1, weeks=0,
  months=0` + маленький `Retention:IntervalSec` → ассерты: старые
  префиксы/ключи удалены, реальный полный жив, WAL ниже cutoff удалён,
  ≥cutoff живы, ключ `storage` пишется с used > 0. Плюс обязательный
  мерж-гейт-маркер `Scale_AddEmptyShard`. Зачистка после серий.
- **Ф7. Мерж-гейт**: полный прогон юниты+интеграция+E2E зелёный на свежем
  Release; roadmap-гейт — удалить `t06-backup-retention` из
  `arch/roadmap/backup.md` (пункт и `←`-зависимость t07) мерж-коммитом.

## 5. Ограничения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, LangVersion
  latest; версии пакетов — `Directory.Packages.props` (новых пакетов нет —
  AWSSDK.S3 уже подключён t03).
- Порты docker в тестах — только динамические; никаких литералов
  вида `:16000`; таймауты фикстур ≤100 с; зачистка контейнеров/сетей
  после КАЖДОЙ серии (`docker rm -f` + `docker network prune -f`);
  каждый сценарий полностью чистит за собой (teardown при любом исходе,
  проверка чистоты — ассерт).
- Не трогать: HA-контур нод, Patroni, префиксы чужих писателей,
  механику t02/t03 (планировщик/агент/контроль цепочки) — ретенция
  встраивается рядом, не внутрь; S3-удаления не появляются ни в каком
  пути, кроме ретенции (R4).
- Воркер НЕ удаляет: активные полные, последний COMPLETED, WAL
  ≥ стартовой точки оставляемых, `.history` стартового/новейших TLI,
  объекты при `Enabled=false`, объекты кластеров без etcd-ключей
  (сироты — t07).
- Мерж-порядок: t06 не зависит от t04 (параллельный worktree) — verify
  используется опционально по данным модели; конфликт с t04 при мерже —
  только семантический слой `verify.state=FAILED` (приоритет удаления),
  механических пересечений нет (t04 пишет verify-поля статусов, t06
  читает).

## 6. Критерии приёмки

1. **AC1 (GFS-отбор)**: юниты `RetentionPlanner` покрывают: все свежие
   дневные; недельная/месячная точка = ПОСЛЕДНИЙ COMPLETED календарного
   периода UTC (не первый); границы недель/месяцев; политика
   `weeks=0`/`months=0` исключает гранулу; активные/DELETING вне отбора.
2. **AC2 (guard)**: сценарий «единственный COMPLETED просрочен» —
   остаётся (Keep), S3-объекты не тронуты (интеграционный тест). После
   мержа t04: `verify.state=FAILED`-полные удаляются первыми, но при
   единственном COMPLETED-FAILED — не удаляются.
3. **AC3 (удаление полного)**: DELETING → объекты `full/<id>/` удалены
   batch-ами → etcd-ключ удалён; прерывание посередине (сбой S3 в тесте)
   → повтор прохода доводит до конца, дублей/остатков нет; один
   кандидат за проход.
4. **AC4 (чистка WAL)**: сегменты строго ниже cutoff удалены; cutoff,
   выше, `.history` cutoff-TLI — живы; `.history` старых TLI удалены;
   после прохода контроль t03 видит непрерывную цепочку от поднявшегося
   `chain_start` (интеграционный сценарий с посеянными объектами).
5. **AC5 (гигиена)**: при >KeepFailed FAILED-записях старейшие удалены,
   бэкофф t02 (`BackupPlanner.BackoffPassed`) на оставшейся истории
   даёт то же `retry_not_before`, что и до чистки (юнит: чистка не
   сбрасывает бэкофф — cap достигается раньше границы).
6. **AC6 (хранилище)**: ключ `/pgworker/backups/storage` пишется с
   `used_bytes` = сумме размеров объектов bucket, verdict по порогам;
   `Quota:Bytes=0` → без квоты-полей, `state=OK`; панельные правила:
   `backup-storage-quota` warning/critical по WARN/CRIT, молчит без
   ключа; `backup-deleting-stuck` на застарелом DELETING (юниты).
7. **AC7 (API policy)**: валидное тело → 200 + policy-ключ в etcd
   (формат канона, парсер t01 читает без parseErrors); невалидное → 400 с
   причиной; несуществующий кластер → 404; применённая политика действует
   (маленький days → лишние полные удаляются).
8. **AC8 (изоляция по умолчанию)**: `Enabled=false` — ретенция no-op,
   воркер/панель ведут себя как раньше; существующие тесты PgWorker/
   AdminPanel зелёны без правки ожиданий.
9. **AC9 (мерж-гейт)**: полный прогон на свежем Release, включая
   `Retention_TrimsToGfs` и маркер `Scale_AddEmptyShard`; зачистка
   контейнеров/сетей после серий; мерж-коммит снимает
   `t06-backup-retention` с roadmap.
10. **AC10 (канон)**: arch/19 + adminpanel/02 из Ф0 — в ветке первым
    коммитом; код не расходится с каноном (ревью plan↔spec по
    чек-листам dev-flow).

## 7. Риски

| Риск | Митигация |
|---|---|
| Баг отбора удаляет нужный бэкап | guard последнего COMPLETED; чистые функции отбора под полным юнит-покрытием; удаление только через DELETING-фазу с идемпотентной доводкой (§2 п.3/5) |
| Чистка WAL отрезает нужное для восстановления | cutoff = min(wal_start_segment) оставляемых — всё нужное для PITR от оставляемых полных выше cutoff; консервативные правила сравнения TLI; сам cutoff и `.history` стартового TLI не трогаются |
| Долгий list/delete большого полного в тике воркера | один кандидат-полный за проход; batch-delete чанками ≤1000; расписание IntervalSec (не каждый тик); тик не ждёт завершения чужих операций |
| list всего bucket для used_bytes дорог | per-instance расписание (раз в IntervalSec, не per-cluster·тик); пагинация; `Quota:Bytes=0` — вердикт не считается |
| Гонка ретенции с агентом WAL/планировщиком | клэйм-сериалзация; фазовые ограничения (только COMPLETED→DELETING); агент грузит сегменты выше слот-позиции >> cutoff; контроль t03 пересчитает chain_start от оставшихся |
| Ключ storage пишут несколько клэймов | put при изменении одинакового значения — идемпотентно; D2-чистка ключ не трогает (вне per-cluster префикса) |
| API policy принимает ломающие значения | валидация диапазонов (400); политика целиком замещается put-ом формата канона; применение — следующим тиком (etcd-снапшот) |
| Стык с параллельным t04 | verify-поля читаются опционально; приоритет FAILED-verify включается сам по данным модели; механических конфликтов нет |
| Синтетика E2E (старые ключи/объекты) размазывает реальность | сценарий проверяет инварианты (guard/cutoff/фазы) на контролируемых данных + реальный свежий полный от живого кластера; полный teardown — ассерт чистоты (AGENTS.md) |

## Open questions

Нет — все развилки закрыты решениями пользователя (§1.1) и каноном Ф0.
