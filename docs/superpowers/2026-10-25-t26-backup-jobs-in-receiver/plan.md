# t26-backup-jobs-in-receiver — план реализации (Фаза 3)

> **Для исполнителей:** план исполняет субагент coder (СИЛЬНАЯ модель, контекст
> проекта есть) по шагам; пункты «Подпункт для слабой модели» передаются
> субагенту coder-flash БЕЗ остального плана — каждый подпункт самодостаточен
> (только его текст + путь к файлу). Чекбоксы `- [ ]` — трекинг шагов.

**Goal:** Backup-операции (полный, verify, drill, restore) исполняет WalReceiver
(.NET, внутри контейнера PG-ноды / tool-контейнера); PgWorker управляет через
HTTP API приёмника `alias:8080`; ephemeral bash-джобы `pgworker-backup`,
stdout-протокол маркеров и чтение docker-логов упраздняются.

**Architecture:** arch-first (шаги 1–3 — правки канонов arch/19/25/14/18 ДО
кода). WalReceiver: тот же Kestrel `:8080` + Minimal API операций; нодовый
режим — операция полного фоном в процессе со стримом (статус — IMMUTABLE-снимок
в памяти, атомарная замена ссылки, паттерн `LiveFacts`); tool-режим
(`PGW_NODE_TOOL=1`) — WalReceiver как главный процесс контейнера без identity,
единственный канал команд/статуса — API. PgWorker: `WalReceiverApiClient` +
tool-контейнеры образа `pgworker-node` (`Node:Image`) с динамическим
host-портом API. Канал — `alias:8080` в общей сети воркера и нод, порт нод
`:8080` наружу хоста НЕ публикуется, `NodePorts`/portalloc НЕ меняются (Р1).

**Tech Stack:** .NET 10 (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`, `Directory.Packages.props`), Minimal APIs,
AWSSDK (существующий `IBackupS3`/`PoolBackupS3`), etcd через
`IEtcdGateway`, docker Engine API через `IClusterDriver`/`IDockerEngine`.

**Spec:** `docs/superpowers/2026-10-25-t26-backup-jobs-in-receiver/spec.md`
(одобрен user-review 2026-10-25; решения Р1–Р16 обязательны; новые решения в
плане фиксируются только из spec/реального кода).

**Базовые правила:** `AGENTS.base.md` (корень репо) + `AGENTS.md` worktree —
прочитать до исполнения, исполнять безукоризненно (§15 вывод команд в файл,
§13 наблюдение за тестами, §12 таймауты ≤ 30 с, правила E2E/изоляции).

## Global Constraints (spec §7 + AGENTS.base.md)

- Arch-first: шаги 1–3 (правки `arch/`) ДО любого кода; расхождение кода с arch/ — брак.
- Никаких новых отклонений arch/25 §1/§2: docker-логи/exec/копирование файлов в
  продуктовых путях бэкапов НЕ появляются; `list/inspect` tool-контейнеров —
  разрешённая проверка факта запуска; exec-гварды pg_hba (G2) — не трогаются (t03).
- Канал API: `alias:8080` по имени ноды в общей сети воркера и нод (docker И
  swarm); `:8080` нод наружу хоста НЕ публикуется; `NodePorts`/portalloc НЕ
  меняются (Р1). Tool-контейнеры — динамический host-порт (зонд свободных портов).
- Секреты: не в etcd-статусах, не в GET-статусах API, не в логах, не в argv
  child-процессов (`PGPASSWORD` — env, Р13); tool-операции — секреты только в POST.
- Аутентификация API не вводится (Р12); новых серий метрик приёмника нет (Р16).
- Резюмов нет: FAILED → переснятие новым id; рестарт приёмника посреди операции
  ⇒ 404 ⇒ `operation-vanished` (Р9); бюджеты — воркерские факты etcd (Р8).
- Идентификаторы — английский, комментарии/док — русский; в документах — только
  текущее/планируемое состояние (без «(tNN)»-атрибуций слитых задач).
- Тесты: динамические порты, `BrokerBootSec ≤ 100 с`, полный teardown при любом
  исходе + ассерт чистоты, `[PHASE]`-телеметрия, зачистка после КАЖДОЙ серии,
  E2E-образы — publish на хосте / артефакты в контейнер, гейт — свежий Release
  (сборка и тесты двумя отдельными командами, `--no-build`).

## Сквозные решения (фиксация из spec — используются в шагах дословно)

- **Адрес API ноды**: `http://<docker-имя ноды>:8080`; docker-имя канонической
  ноды — `pgw-<C>-<X>-<n>` (прецедент `DeprovisioningProcess.PlainNodeName`),
  object-нода (усыновлённая) — имя её контейнера `NodeAddress.Object`.
  Недостижимость (DNS/отказ/5xx/таймаут) — transient тика (journal-факт с адресом).
- **Адрес API tool-контейнера**: контур (`ContourNetwork` задан) —
  `http://<имя tool-контейнера>:8080` в сети контура, БЕЗ публикации на хост
  (host-порт НЕ используется, спека создаётся с `hostPort: null` — шаг 11);
  прод — `http://<docker-хост>:<динамический порт>` (публикация `:8080` при
  create, порт из зонда `FreePortProbe`; адрес — из инспекта публикации,
  `ForTool(..., contourMode: false)`).
- **Staging полного** (Р5): `<Staging:Dir>/<id>/` в data-volume ноды; дефолт
  `Staging:Dir=/home/postgres/pgdata/backup-staging`; ENOSPC — реактивный FAILED.
- **Маркер транзиента скачивания** (spec §3.5): статус
  `{"state":"failed","phase":"downloading","error":"<однострочная причина>"}`
  = сбой скачивания (transient); `"phase":"verifying"|"recovering"` при
  `failed` = permanent. Маркер — пара state/phase; `error` — только причина.
- **Состояния операций** (spec §3.2): `running | succeeded | failed`; у полного
  дополнительно `uploading`. Фазы: full `basebackup|uploading`; verify
  `downloading|verifying`; restore `downloading|recovering`.
- **Секреты full** (Р4): приёмник ноды резолвит сам из etcd (как стрим);
  команда `POST /api/backup/full` несёт только `id`. Tool-операции — полный
  S3-контекст в POST-теле.

## Review Focus (входы/отказы, не покрытые позитивными тестами шагов)

1. Alias ноды не резолвится / connection refused / 5xx / таймаут — ожидание:
   transient тика воркера (journal-факт с адресом), статус не портится,
   краш тика недопустим. Тест: шаг 15 (таблица transient-исходов клиента).
2. Рестарт приёмника/ноды посреди полного — ожидание: GET 404 ⇒ FAILED
   `operation-vanished`, переснятие новым id с бэкоффом. Тест: шаг 15.
3. Вторая активная операция на контейнер (409 гвард приёмника) — ожидание:
   воркер трактует как «уже жива» (PLANNED ⇒ RUNNING), статус не портится.
   Тест: шаг 15.
4. ENOSPC staging data-volume ноды — ожидание: приёмник отдаёт `failed` с
   однострочной причиной (stderr pg_basebackup/IOException put), реактивный
   FAILED воркера, бэкофф. Тест: шаг 8 (фейк runner, ненулевой exit).
5. Смена роли ноды посреди полного — ожидание: операция не рвётся (write-список
   S3 зафиксирован на старте операции). Тест: шаг 8 (список не перечитывается).

---

# Фаза 1. Arch-first (ДО кода)

## Шаг 1. Правки arch/19 (§2, §3.5, §3.6, §5, §6, §7, §8, §9, §10)

- [ ] **Вход:** одобренный spec §5 (состав правок выписан там построчно);
  файл `arch/19-backups.md` в актуальном состоянии worktree.
- [ ] **Действие** (редактирование `arch/19-backups.md`, БЕЗ кода):
  - **§2** (Механика полных): блок «Исполнитель» — вместо ephemeral-джоба
    `pgw-backup-full-<C>-<X>-<id>`/образа `pgworker-backup` — приёмник WalReceiver
    выбранной ноды: команда `POST /api/backup/full {"id"}` от воркера, статус
    `GET /api/backup/full` (404 = vanished), `POST /api/backup/full/cancel`
    best-effort; pg_basebackup локально `127.0.0.1:5432` под `backup_exec`
    (пароль — env `PGPASSWORD` child-процесса, не argv); staging — каталог
    `<Staging:Dir>/<id>/` data-volume ноды (реактивный ENOSPC, чистит приёмник
    после исхода и при старте новой операции); upload — .NET `IBackupS3`/
    `PoolBackupS3` по write-списку СВОЕЙ фактической роли (REST-проба, симметрия
    стриму; write-список фиксируется на старте операции — смена роли посреди
    полного её не рвёт); выбор ноды-источника и node/role-факты — воркер
    (`ResolveBackupSourceAsync` sync-standby fallback мастер); stdout-протокол
    джоба и блок «Пайплайн джоба»/env-конфигурация (`PGW_BK_*`) удаляются;
    `wal_start_segment` — из `backup_label` (парсер `BackupLabel`), `size_bytes`
    — сумма файлов; файлы 1:1 в `full/<id>/` (вкл. `backup_manifest`, `pg_wal/`)
    + дублирование закрытых сегментов набора в `wal/` — сохраняется; 503
    стоп-гейтов приёмника — операция не начинается; vanished ⇒ FAILED
    `operation-vanished` (Р9); контейнера полного больше нет — cleanup только
    cancel; «Планировщик» — без изменений по логике (PLANNED → API-запуск → RUNNING).
  - **§3.5** (restore): блок «Restore-джоб» → «Tool-контейнер restore»:
    контейнер образа `pgworker-node` в tool-режиме (`PGW_NODE_TOOL=1`; без
    Patroni/supervisord-программ, WalReceiver — главный процесс, без identity),
    имя `pgw-backup-restore-<C>-<X>-<id>` сохраняется; монтирует data-volume
    первой ноды `pgw-<C>-<X>-<n1>-data` в `/home/postgres/pgdata` (PGDATA =
    `/home/postgres/pgdata/pgroot/data`, Spilo-layout); старт БЕЗ команд — все
    операции через API (`POST /api/ops/restore` — backup_id/target_time/
    recovery_timeout_sec/S3-контекст, `GET` — фазы downloading|recovering +
    restored_to_lsn/system_id); механика восстановления — WalReceiver (.NET,
    child-процессы, `restore_command = walreceiver fetch-wal <seg> <dest>` —
    sub-режим того же бинаря, S3-контекст из env процесса, секреты не в argv);
    инцидентная механика (chown 101:101 после модификаций, chmod 700,
    standby.signal → recovery.signal, pristine auto.conf `.orig` с возвратом и
    вычисткой `archive_mode/archive_command`, data_directory/hba_file/ident_file
    -переопределения, `wal_level=replica`, `summarize_wal=off`,
    `shared_preload_libraries=''`, `ssl=off`, `logging_collector=off`,
    trust-строка pg_hba на время поллинга, `HOME=/tmp`, setpriv-эквивалент
    (запуск под uid/gid 101), канонический набор каталогов initdb, регистр
    сегментов (`%f` ВЕРХНИЙ → ключи строчные), поллинг `pg_is_in_recovery()`
    с бюджетом, promote-окно ретраится до бюджета) — переносится в .NET 1:1;
    динамическая host-публикация API tool-контейнера (прод) / alias сети
    контура (контур); REJOINING/демонтаж (только до первого старта
    tool-контейнера)/initialize-щит/SQL-гейт/бюджеты/стоп-семантика — как было.
  - **§3.6** (drill): «Drill-джоб» → tool-контейнер `pgw-backup-drill-<C>-<X>-<id>`
    (контейнер+volume одно имя — D1 жив), свой ephemeral volume, restore latest
    (без target) через ту же `POST /api/ops/restore`; изоляция (Ports/сеть,
    лимиты `Agent`) — как было; `drill-vanished`/`drill-timeout`/cleaning — как было.
  - **§5** (verify): checksums-фаза — tool-контейнер `pgw-backup-verify-<C>-<X>-<id>`
    + API (`POST /api/ops/verify` — S3-контекст read-списка, `GET` — фазы
    downloading|verifying); сбой скачивания — transient (статус failed с фазой
    `downloading` ⇒ PENDING-ретраи тиками), `pg_verifybackup` ≠ 0 — permanent;
    цепочечная проверка (list + LSN-разбор `.history`) — БЕЗ изменений.
  - **§6** (лимиты): `Agent {Cpu,Mem}` — лимиты tool-контейнеров; staging-тома
    джобов упразднены (staging полного — data-volume ноды); бюджеты зависших —
    воркерские (`Job:*TimeoutSec`), отмена — cancel API / kill+rm (tool).
  - **§7** (секреты): env-джобов больше нет; tool-операции — S3-контекст в
    POST-команде (секреты только в POST, не в статусах/логах); полный в ноде —
    резолв приёмником из etcd (как стрим); CA https — PEM в команде (tool) /
    резолв приёмником (нода, `PGW_BACKUP_S3_CA_PEM` — уже есть); `mc`-материализация
    `SSL_CERT_FILE` удаляется (.NET-фабрика доверия).
  - **§8** (карта разделов): обновить строки §2/§3–§5/§3.5 — «джоб» → «приёмник/
    tool-контейнер», где формулировки называют docker-джобы.
  - **§9** (конфигурация): `Job {Image}` упраздняется; `Staging {Dir}` — новый
    смысл/дефолт `/home/postgres/pgdata/backup-staging`, `QuotaBytes`
    упраздняется; `Agent {Cpu,Mem}` — tool-контейнеры; бюджеты
    `FullTimeoutSec/VerifyTimeoutSec/RestoreTimeoutSec` — остаются; остальное —
    без изменений.
  - **§10** (риски): строка «Протокол джоба (stdout-маркеры)» — удаляется;
    строка staging-квоты — переформулировать (staging в data-volume ноды,
    реактивный ENOSPC); сетевые риски tool-контейнеров/S3 — по extra_hosts/
    контуру (как у джобов сегодня).
- [ ] **Выход:** arch/19 описывает исполнителя-приёмника/tool-контейнеры и API
  во всех тронутых секциях; stdout-протокол/env-джобов в каноне не осталось.
- [ ] **Проверка:** `grep -n "pgworker-backup\|stdout-маркер\|PGW_BK_" arch/19-backups.md`
  — упоминаний упраздняемого нет (кроме, где явно «упразднён», — таких быть не
  должно: документ описывает текущее/планируемое, не историю);
  `grep -n "alias:8080\|/api/backup/full\|/api/ops/" arch/19-backups.md` —
  механика API присутствует в §2/§3.5/§3.6/§5.
- **Связь со spec:** §5 п.1 (arch/19), §2 принцип 1; Р3/Р4/Р5/Р8/Р9/Р13/Р15.

## Шаг 2. Правки arch/25 (реестр каналов, №22/№13, §5)

- [ ] **Вход:** Шаг 1 выполнен (arch/19 уже называет API-канал).
- [ ] **Действие** (`arch/25-component-communications.md`):
  - Реестр §4, строка **№13** «Backup-джобы (full/verify/drill/restore) → S3-пул
    | S3 API (mc, pg_basebackup) | канон» — УДАЛЯЕТСЯ (S3-доступ у приёмника —
    уже №12; отдельного компонента «backup-джобы» больше нет).
  - Реестр §4, строка **№22** «PgWorker → backup-джобы | docker logs | отклонение
    → t24» — ЗАМЕНЯЕТСЯ канонической записью: «PgWorker → WalReceiver (операции
    бэкапов) | HTTP API приёмника `alias:8080` в общей сети воркера и нод, без
    публикации на хост (docker и swarm); нодовый режим — API полного бэкапа,
    tool-режим — API verify/restore/cancel; статусы — GET того же API | канон».
  - §5: пункт «№22 — backup.md t24-job-status-channel…» — УДАЛЯЕТСЯ (перехода
    больше нет); упоминание №22 в списке отклонений §5 («Отклонения №22–24…»)
    — скорректировать нумерацию оставшихся (№23 exec-переход остаётся, задача
    t03; №24 в §4 канон — список §5 формулируется по факту файла).
- [ ] **Выход:** реестр каналов отражает новый канал; отклонение №22 и канал
  №13 не существуют; §5 не содержит переходов по бэкап-джобам.
- [ ] **Проверка:** `grep -n "t24\|docker logs" arch/25-component-communications.md`
  — пусто; `grep -n "alias:8080" arch/25-component-communications.md` —
  канал PgWorker → WalReceiver присутствует со статусом «канон».
- **Связь со spec:** §5 п.7 (arch/25); Р6/Р7 (t24-канон закрыт без нового
  etcd-канала — docker-логи уходят целиком).

## Шаг 3. Правки arch/14 (§2.1/§2.4) и arch/18 (§2.8)

- [ ] **Вход:** Шаги 1–2 выполнены (терминология API операций согласована).
- [ ] **Действие**:
  - `arch/14-pgworker.md` §2.1, таблица ролей: строка «walreceiver | :8080
    (metrics) | …на порту — только /metrics…» → «:8080 (/metrics + API операций
    бэкапов) | …`/metrics` (Prometheus через ScrapeNetwork, arch/18 §2.8) И
    HTTP API операций бэкапов (arch/19 §2/§3.5/§3.6/§5) по каналу
    `alias:8080` — общей сети воркера и нод; наружу хоста порт НЕ публикуется
    (docker и swarm)». Ниже строка «Наружу (хост) публикуются порты: pg, patroni,
    doorman…» — остаётся без четвёртого порта (тройка порт-аллокатора — факт,
    portalloc/NodePorts не меняются).
  - arch/14 §2.4: дополнить инвариант сети (после пункта про `ContourNetwork`):
    контейнеры воркера и нод ВСЕГДА в одной сети (контур — сеть контура; прод
    docker/swarm — per-cluster `pgw-net-<C>`/overlay, воркер в ней по
    построению поставки) — адресация API приёмника `alias:8080` по имени ноды
    работает в обоих режимах; недостижимость alias — transient процесса воркера
    с journal-диагностикой (архив-адрес: arch/19 §2/§3.5).
- [ ] **Действие** (`arch/18-metrics.md` §2.8): первый абзац — «экспозиция —
  Kestrel `:8080`… `/metrics`» дополнить: тот же порт несёт HTTP API операций
  бэкапов (arch/19); новых серий приёмника не вводится (наблюдаемость операций —
  etcd-статусы воркера + журнал воркера). Таблица серий/лейблы — без изменений.
- [ ] **Выход:** arch/14 фиксирует «/metrics + API» и не публикует :8080 на
  хост; arch/18 упоминает API на том же порту и отсутствие новых серий.
- [ ] **Проверка:** `grep -n "8080" arch/14-pgworker.md arch/18-metrics.md` —
  формулировки «только /metrics» не осталось; `grep -n "Receiver" src/PgWorker.Core/Model/Domain.cs`
  — порт НЕ добавлен в `NodePorts` (инвариант Р1 уже соблюдён кодом — правок
  кода портalloc нет ни на одном шаге).
- **Связь со spec:** §5 п.2/п.8 (arch/14 §2.1/§2.4, arch/18 §2.8); Р1.

---

# Фаза 2. WalReceiver: API, tool-режим, .NET-механика операций

## Шаг 4. Каркас: режимы WalReceiver, реестр операций, runner child-процессов, файловые S3-методы

- [ ] **Вход:** Фаза 1 смержена в ветку (arch/ обновлён). Проект
  `src/PgWorker.WalReceiver` компилируется (сегодняшний каркас: `Program.cs`
  — Kestrel `0.0.0.0:8080` + `/metrics` + hosted `WalReceiverLoop`).
- [ ] **Действие** (новые файлы + точечные правки):
  - `src/PgWorker.WalReceiver/Operations/OperationSnapshot.cs` — IMMUTABLE-снимок
    статуса операции (паттерн `LiveFacts`):
    `public sealed record OperationSnapshot(string Id, string State, string Phase,
    string? WalStartSegment = null, long? SizeBytes = null, string? RestoredToLsn = null,
    string? SystemId = null, string? Error = null);`
    Константы состояний/фаз (static class `OperationStates`): `Running="running"`,
    `Uploading="uploading"`, `Succeeded="succeeded"`, `Failed="failed"`;
    фазы `Basebackup="basebackup"`, `Uploading="uploading"`, `Downloading="downloading"`,
    `Verifying="verifying"`, `Recovering="recovering"`.
  - `src/PgWorker.WalReceiver/Operations/OperationRegistry.cs` — реестр ОДНОЙ
    активной операции процесса (нодовый: полный; tool: verify/restore — одна
    на контейнер): `Volatile`-хранение `OperationSnapshot?`, метод
    `bool TryBegin(OperationSnapshot initial)` (CAS: есть активная
    (running/uploading) → false = 409-гвард), `OperationSnapshot? Current`,
    `void Update(OperationSnapshot next)`, `void Clear()` (после терминала —
    следующий GET до новой команды = 404; спека §3.2).
  - `src/PgWorker.WalReceiver/Operations/IProcessRunner.cs` +
    `ProcessRunner.cs` — абстракция child-процессов (для фейков тестов):
    `Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
    ProcessStartInfo psi, CancellationToken ct);` Реализация: `Process.Start`
    (async), полный слив stdout/stderr в память, при `ct` —
    `Kill(entireProcessTree: true)` и `OperationCanceledException`. Все
    `ProcessStartInfo` операций создаются только через этот интерфейс.
  - `src/PgWorker.Backups/BackupS3.cs` (`IBackupS3`): добавить два файловых
    метода (тела в `BackupS3`/`PoolBackupS3`; фейки — Фаза 4):
    `Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default);`
    (AWSSDK `PutObjectRequest` с `InputStream` файла; ключ = полный ключ в
    bucket) и
    `Task<Result> GetObjectToFileAsync(string cluster, string shard, string key,
    string destFilePath, CancellationToken ct = default);`
    (`key` — относительно `<C>/<X>/`, как `GetObjectAsync`; ответ — в FileStream).
    `PoolBackupS3`: `PutFileAsync` — перебор write-списка (как `PutObjectAsync`),
    `GetObjectToFileAsync` — перебор ReadAll до найденного (как `GetObjectAsync`).
  - `src/PgWorker.WalReceiver/Program.cs` — режимы: env `PGW_NODE_TOOL`
    (`"1"` = tool-режим). Оба режима: Kestrel `0.0.0.0:8080` + `/metrics`
    (как сегодня). Нодовый: `AddHostedService<WalReceiverLoop>()` + singleton
    `OperationRegistry` (под полный). Tool: `WalReceiverLoop` НЕ регистрируется
    (нет identity — нет цикла стрима), `OperationRegistry` — singleton;
    identity (`WalReceiverIdentity.Parse`) в tool-режиме не вызывается.
    Метрики — как сегодня (новых серий нет, Р16). DI-контракт операций:
    Program регистрирует ИНТЕРФЕЙСЫ фабрик операций —
    `IFullBackupOperationFactory` (нодовый) и `IToolOperationFactory` (tool:
    verify/restore по виду запроса); реализации — шаги 5–7, тесты подменяют
    фейками (шаг 9). Тестируемость Program (`WebApplicationFactory` в
    шаге 9): `Program` обязан остаться `internal` (публичный запрещён —
    CS0433 с `PgWorker.App.Program` в тестах, комментарий в Program.cs) —
    в `src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj` добавить
    `<InternalsVisibleTo Include="PgWorker.IntegrationTests"/>` (прецедент —
    KafkaWorker.App.csproj; `PgWorker.UnitTests` уже имеет доступ — не трогать).
  - env staging полного: `PGW_BACKUP_STAGING_DIR` (дефолт
    `/home/postgres/pgdata/backup-staging`) — читается в Program и передаётся
    фабрике полной операции (env ноды ставит EnsureNode — шаг 13).
- [ ] **Подпункт для слабой модели** (класс `OperationRegistry`):
  создать файл `/Users/demakaev/ZCodeProject/worktrees/feat-t26-backup-jobs-in-receiver/src/PgWorker.WalReceiver/Operations/OperationRegistry.cs`,
  namespace `PgWorker.WalReceiver.Operations`, usings только `System.Threading`.
  Класс `public sealed class OperationRegistry` — потокобезопасное хранение
  снимка одной активной операции (паттерн атомарной замены ссылки):
  ```csharp
  private OperationSnapshot? _current; // доступ только через Volatile

  /// Текущий снимок (null — активной операции нет).
  public OperationSnapshot? Current => Volatile.Read(ref _current);

  /// CAS-гвард старта: true — снимок принят (было null/терминальное);
  /// false — активная операция уже есть (running/uploading) — вызывающий
  /// отвечает 409. Принятый снимок публикуется до возврата.
  public bool TryBegin(OperationSnapshot initial)
  {
      var cur = Volatile.Read(ref _current);
      if (cur is { State: "running" or "uploading" }) return false;
      return ReferenceEquals(Interlocked.CompareExchange(ref _current, initial, cur), cur);
  }

  /// Публикация нового снимка (атомарная замена; вызывается только владельцем операции).
  public void Update(OperationSnapshot next) => Volatile.Write(ref _current, next);

  /// Снятие после терминального исхода (succeeded/failed) — следующий GET = 404.
  public void Clear() => Volatile.Write(ref _current, null);
  ```
  Комментарий класса (по-русски): «Реестр одной активной операции приёмника
  (spec §3.2): нодовый режим — полный бэкап, tool-режим — одна операция на
  контейнер. Снимки IMMUTABLE, публикация — атомарная замена ссылки (паттерн
  LiveFacts); TryBegin — 409-гвард второй активной операции.»
- [ ] **Подпункт для слабой модели** (интерфейс + реализация runner):
  создать файл `/Users/demakaev/ZCodeProject/worktrees/feat-t26-backup-jobs-in-receiver/src/PgWorker.WalReceiver/Operations/IProcessRunner.cs`,
  namespace `PgWorker.WalReceiver.Operations`:
  ```csharp
  /// <summary>Абстракция запуска child-процессов операций бэкапов (PG-утилиты):
  /// полный слив stdout/stderr, kill дерева процессов при отмене. Тесты — фейки.</summary>
  public interface IProcessRunner
  {
      Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
          ProcessStartInfo psi, CancellationToken ct);
  }
  ```
  и файл `.../Operations/ProcessRunner.cs` (тот же namespace, usings
  `System.Diagnostics`):
  ```csharp
  public sealed class ProcessRunner : IProcessRunner
  {
      public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
          ProcessStartInfo psi, CancellationToken ct)
      {
          psi.RedirectStandardOutput = true;
          psi.RedirectStandardError = true;
          psi.UseShellExecute = false;
          using var process = Process.Start(psi)!;
          using var reg = ct.Register(() =>
          {
              try { process.Kill(entireProcessTree: true); } catch { /* уже вышел */ }
          });
          var stdout = await process.StandardOutput.ReadToEndAsync(ct);
          var stderr = await process.StandardError.ReadToEndAsync(ct);
          await process.WaitForExitAsync(ct);
          return (process.ExitCode, stdout, stderr);
      }
  }
  ```
- [ ] **Выход:** каркас компилируется; реестр/runner готовы к использованию
  операциями; IBackupS3 расширен файловыми методами (сборка зелёная — реализации
  в BackupS3/PoolBackupS3 сделаны в этом же шаге сильной моделью).
- [ ] **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.WalReceiver -c Release --nologo > /tmp/t26-build4.log 2>&1; grep -c "error" /tmp/t26-build4.log` — 0.
- **Связь со spec:** §3.2 (состояния операций, 409-гвард), §3.3 (режимы,
  hosted-сервис стрима в tool-режиме не стартует, Kestrel всегда), §3.4
  (child-процессы — `Process.Start` с теми же флагами), Р16.

## Шаг 5. Нодовый режим: операция полного бэкапа + маршруты `/api/backup/full*`

- [ ] **Вход:** Шаг 4 выполнен (Registry/Runner/файловые S3-методы готовы).
- [ ] **Действие** (новые файлы + маршруты в Program):
  - `src/PgWorker.WalReceiver/Operations/FullBackupOperation.cs` — операция
    полного (фоновая Task, параллельная стриму). Конструктор принимает:
    `string id`, `string stagingDir` (из env `PGW_BACKUP_STAGING_DIR`),
    `WalReceiverIdentity identity`, `IEtcdGateway etcd`, `string[] etcdEndpoints`,
    `IS3PoolStore pools`, `Func<CancellationToken, Task<WalReceiverRole>> roleProbe`
    (та же `PatroniRoleProbe.ProbeAsync`, что у цикла), `IProcessRunner runner`,
    `OperationRegistry registry`. Публичные члены: `OperationSnapshot Snapshot`
    (чтение registry), `Task RunAsync(CancellationToken ct)` — весь пайплайн
    ниже, `Task CancelAsync()` — отмена через внутренний CTS (best-effort).
    Пайплайн (все шаги идемпотентны; FAILED → переснятие новым id, резюмов нет):
    1. Резолв кредов/пула — `new WalReceiverTargets(etcd, etcdEndpoints, pools)
       .ResolveAsync(identity.Cluster, identity.Shard, ct)`: WaitingPassword/
       WaitingPolicy → `failed` с `error` из результата (операция не начинается;
       гвард воркера G1/G3 обычно исключает — честный transient-отказ).
       База/CA — `identity.S3Bucket` (per-install, как у стрима); Р4.
    2. Роль — `roleProbe(ct)`; `WriteTargets = role==Master ? pool.WriteMaster :
       pool.WriteReplica`; write-список ФИКСИРУЕТСЯ на старте операции (смена
       роли посреди полного её не рвёт — spec §3.4); S3-клиент —
       `new PoolBackupS3(writeTargets, pool.ReadAll, pool.AccessKey,
       pool.SecretKey, bucket, caBundlePath, requestTimeout: WalReceiverCore.PutAttemptTimeout)`
       (CA-файл — материализация `identity.S3CaPem`, образец WalReceiverLoop).
    3. Чистка staging: рекурсивно удалить `<stagingDir>/*` (прошлые `<id>`-каталоги),
       создать `<stagingDir>/<id>/full`.
    4. Фаза basebackup: registry.Update(running/basebackup). `pg_basebackup` —
       `ProcessStartInfo { FileName="pg_basebackup", ArgumentList={
       "-d", "host=127.0.0.1 port=5432 user=backup_exec dbname=postgres sslmode=require",
       "-D", $"{staging}/{id}/full", "-X", "stream", "--checkpoint=fast",
       "--manifest-checksums=SHA256" }, EnvironmentVariables["PGPASSWORD"]=
       targets.Password }` (пароль ТОЛЬКО в env, не argv/DSN — Р13; DSN-параметры
       — как у стримного подключения приёмника, WalReceiverOptions «127.0.0.1»/5432).
       ExitCode != 0 → failed, `error` = однострочная причина
       (`stderr.Split('\n')[0].Trim('"')`).
    5. `wal_start_segment`: `BackupLabel.WalStartSegment(File.ReadAllText(
       $"{staging}/{id}/full/backup_label"))` (парсер `PgWorker.Backups.Restore.BackupLabel`
       уже есть); null → failed `backup_label: START WAL LOCATION not found`.
       `size_bytes` — рекурсивная сумма длин файлов staging.
    6. Фаза uploading: registry.Update(uploading/uploading + WalStartSegment).
       Upload 1:1 (spec §3.4/arch/19 §5): рекурсивный обход `<staging>/<id>/full`
       → `PutFileAsync($"{identity.Cluster}/{identity.Shard}/full/{id}/<относительный путь>", file)`
       (вкл. `backup_manifest`, `pg_wal/` — ключ ПОЛНЫЙ в bucket, симметрия
       `PutObjectAsync`); ЗАТЕМ файлы `<staging>/<id>/full/pg_wal/*`
       кроме `*.partial`/`*.history` →
       `PutFileAsync($"{identity.Cluster}/{identity.Shard}/wal/{имя}", file)`
       (дублирование закрытых сегментов набора в общий префикс `wal/`;
       идемпотентная перезапись). Исчерпание write-списка → Result.Failed с
       маркером `S3PoolMarkers.Exhausted` — failed с той же строкой в error.
    7. Итог succeeded (+SizeBytes); `registry.Update(...)`; чистка staging
       (рекурсивное удаление `<stagingDir>/<id>`); DisposeAsync S3-клиента.
       Любой отказ → failed + однострочный error + та же чистка + cancel-флаг.
    8. Cancel (`CancelAsync`): отмена CTS операции → runner убивает дерево
       текущего child-процесса; операция завершается failed `cancelled` + чистка.
    Отдельный `catch (Exception)` вокруг всего тела → failed
    `"<type>: <message.Split('\n')[0]"` (операция не роняет процесс).
  - Маршруты (Minimal APIs, в `Program.cs` нодовой ветки; JSON — System.Text.Json
    с дефолтами + `DefaultIgnoreCondition = WhenWritingNull`):
    - `POST /api/backup/full` — тело `{"id":"<YYYYMMDDHHMMSSZ>"}` (id не
      валидируется форматом — детерминизм обеспечен воркером): стоп-гейты
      `WalReceiverGates.ReadAsync(...)` → `Disabled||Paused||Unknown` ⇒ **503**
      `{"error":"backups disabled, paused or gates unknown"}` (Unknown —
      транспортный отказ etcd при чтении гейтов: операция не начинается,
      fail-closed — симметрия «503 — операция не начинается» spec §3.2/Р11;
      воркер повторит тиком — transient §3.1); registry.TryBegin ⇒ false ⇒
      **409** `{"error":"operation already active"}`; иначе создать операцию,
      запустить `Task.Run(RunAsync)` и ответить **202** `{"state":"running","phase":"basebackup"}`.
    - `GET /api/backup/full` — registry.Current null ⇒ **404**; иначе **200**
      `{"state","phase","wal_start_segment"?,"size_bytes"?,"error"?}` (имена
      полей — snake_case из spec §3.2; `JsonSerializerOptions` с
      `JsonNamingPolicy.SnakeCaseLower` или явные `JsonPropertyName`).
    - `POST /api/backup/full/cancel` — активной нет ⇒ **404**; иначе `CancelAsync()`
      и **202** `{"state":"cancelling"}` (best-effort; итог — failed/cancelled по GET).
  - Гейты при СТАРТЕ только: идущая операция стоп-гейтами не рвётся (спека:
    503 — «операция не начинается»; отменяет только cancel/бюджет воркера).
- [ ] **Подпункт для слабой модели** (DTO-контракт API):
  создать файл `/Users/demakaev/ZCodeProject/worktrees/feat-t26-backup-jobs-in-receiver/src/PgWorker.WalReceiver/Operations/ApiContracts.cs`,
  namespace `PgWorker.WalReceiver.Operations`, using `System.Text.Json.Serialization`.
  Записи JSON-контракта маршрутов приёмника (snake_case — явными атрибутами):
  ```csharp
  /// <summary>Тело POST /api/backup/full и POST /api/ops/* (запуск операции).</summary>
  public sealed record StartFullRequest([property: JsonPropertyName("id")] string Id);

  /// <summary>Статус операции полного бэкапа (GET /api/backup/full).</summary>
  public sealed record FullStatusResponse(
      [property: JsonPropertyName("state")] string State,
      [property: JsonPropertyName("phase")] string Phase,
      [property: JsonPropertyName("wal_start_segment")] string? WalStartSegment = null,
      [property: JsonPropertyName("size_bytes")] long? SizeBytes = null,
      [property: JsonPropertyName("error")] string? Error = null);

  /// <summary>S3-контекст tool-операций (секреты — ТОЛЬКО в POST-теле, spec §3.2).</summary>
  public sealed record S3Context(
      [property: JsonPropertyName("endpoints")] string[] Endpoints,
      [property: JsonPropertyName("access_key")] string AccessKey,
      [property: JsonPropertyName("secret_key")] string SecretKey,
      [property: JsonPropertyName("bucket")] string Bucket,
      [property: JsonPropertyName("prefix")] string Prefix,
      [property: JsonPropertyName("ca_pem")] string? CaPem = null);

  public sealed record StartVerifyRequest(
      [property: JsonPropertyName("backup_id")] string BackupId,
      [property: JsonPropertyName("s3")] S3Context S3);

  public sealed record StartRestoreRequest(
      [property: JsonPropertyName("backup_id")] string BackupId,
      [property: JsonPropertyName("target_time")] string? TargetTime, // RFC3339; null = latest
      [property: JsonPropertyName("recovery_timeout_sec")] int RecoveryTimeoutSec,
      [property: JsonPropertyName("s3")] S3Context S3);

  /// <summary>Статус tool-операции (GET /api/ops/verify|restore).</summary>
  public sealed record ToolStatusResponse(
      [property: JsonPropertyName("state")] string State,
      [property: JsonPropertyName("phase")] string Phase,
      [property: JsonPropertyName("restored_to_lsn")] string? RestoredToLsn = null,
      [property: JsonPropertyName("system_id")] string? SystemId = null,
      [property: JsonPropertyName("error")] string? Error = null);

  public sealed record ErrorResponse([property: JsonPropertyName("error")] string Error);
  ```
- [ ] **Выход:** полный бэкап исполняется приёмником ноды по API; маршруты
  202/409/503/404/200 соответствуют spec §3.2; staging чистится приёмником.
- [ ] **Проверка:** сборка зелёная (та же команда, шаг 4); юнит-тесты операций —
  шаг 9. Смоук ручной не требуется.
- **Связь со spec:** §3.2 (таблица нодового режима), §3.4 (Full backup),
  §2 п.4 (бюджеты/отмена), Р4 (резолв из etcd), Р5 (staging), Р10 (write-список
  по фактической роли, фиксация на старте), Р11 (гварды), Р13 (PGPASSWORD).

## Шаг 6. Tool-режим: операция verify + маршруты `/api/ops/verify` и `/api/ops/cancel`

- [ ] **Вход:** Шаги 4–5 выполнены; `ApiContracts` (StartVerifyRequest/
  ToolStatusResponse/S3Context) готовы.
- [ ] **Действие**:
  - `src/PgWorker.WalReceiver/Operations/VerifyOperation.cs` — операция verify
    tool-контейнера (одна активная на контейнер — общий `OperationRegistry`).
    Конструктор: `StartVerifyRequest`, `IProcessRunner`, `OperationRegistry`,
    staging-корень tool-контейнера `/staging` (том tool-контейнера, монтирует
    воркер — шаг 11; путь фиксируется константой `ToolPaths.Staging = "/staging"`
    в `src/PgWorker.WalReceiver/Operations/ToolPaths.cs`). Пайплайн:
    1. registry.TryBegin (гвард 409 — маршрут); CA: `s3.CaPem` непуст →
       материализация в temp-файл (образец WalReceiverLoop, 0600) → путь в
       `S3ClientOptions.CaBundlePath`.
    2. S3-клиент: `new PoolBackupS3(s3.Endpoints, s3.Endpoints, s3.AccessKey,
       s3.SecretKey, s3.Bucket, caPath, requestTimeout: WalReceiverCore.PutAttemptTimeout)`
       — ReadAll-семантика (перебор нод до найденного), prefix из команды
       (`"<C>/<X>"`), Р4.
    3. Фаза downloading: `ListPrefixAsync($"{prefix}/full/{backup_id}/")` →
       `GetObjectToFileAsync(cluster, shard, key, $"/staging/full/{backup_id}/<rel>")`
       для каждого объекта (mkdir по относительному пути; cluster/shard —
       разбор prefix по первому `/`). Отказ list/get → registry.Update(failed,
       phase=downloading, error однострочный) — МАРКЕР ТРАНЗИЕНТА (воркер
       держит PENDING и ретраит тиками), операция ЗАВЕРШАЕТСЯ (state=failed) —
       verify-контейнер один на кандидата, перезапуск операции = новый POST тем
       же воркером (transient ветка воркера сносит контейнер и создаёт заново —
       шаг 13, `SuperviseAsync`).
    4. Фаза verifying: `pg_verifybackup -D /staging/full/<backup_id>` через
       runner. ExitCode == 0 → succeeded (phase=verifying); != 0 → failed
       (phase=verifying, error=однострочная причина) — PERMANENT (данные плохие).
    5. Чистка staging (`/staging/full/<backup_id>` рекурсивно) при любом
       исходе; registry остаётся с терминальным снимком до Clear (следующий
       POST — новая операция; TryBegin перезапишет терминальный).
  - Маршруты tool-ветки Program.cs:
    - `POST /api/ops/verify` — тело `StartVerifyRequest`: TryBegin false ⇒ 409;
      иначе запуск фоном и **202** `{"state":"running","phase":"downloading"}`.
    - `GET /api/ops/verify` — **200** ToolStatusResponse или **404** (нет активной/
      терминал снят).
    - `POST /api/ops/cancel` — общий для tool-операций: активной нет ⇒ 404;
      иначе CancelAsync() активной (kill postgres/pg_verifybackup/скачивания +
      чистка) и **202**.
  - OperationRegistry: расширить понятие «активная» на verify/restore (state
    running — уже покрыто; uploading у tool-операций не бывает).
- [ ] **Выход:** verify исполняется в tool-контейнере по API; транзиент
  скачивания и permanent verify-отказа различимы парой state/phase.
- [ ] **Проверка:** сборка зелёная; юнит-тесты — шаг 9.
- **Связь со spec:** §3.2 (tool-таблица), §3.4 (Verify), §5 arch/19 (семантика
  transient/permanent), Р2 (tool-режим), Р4 (S3-контекст в команде).

## Шаг 7. Tool-режим: операция restore/drill, `fetch-wal` sub-режим, маршруты `/api/ops/restore`

- [ ] **Вход:** Шаг 6 выполнен (S3-контекст/гварды/cancel работают).
- [ ] **Действие**:
  - `src/PgWorker.WalReceiver/Operations/FetchWalCommand.cs` — sub-режим того
    же бинаря: `args[0]=="fetch-wal" <seg> <dest>` (Program.Main ДО создания
    builder: разбор args, запуск, `Environment.Exit` по итогу). S3-контекст —
    env процесса (WalReceiver выставляет при старте postgres — наследуется до
    restore_command; секреты не в argv): `PGW_S3_ENDPOINTS` (запятая, read-список,
    порядок из команды запуска), `PGW_S3_BUCKET`, `PGW_S3_PREFIX` (`<C>/<X>`),
    `PGW_S3_ACCESS_KEY`, `PGW_S3_SECRET_KEY`, опц. `PGW_S3_CA_PEM`. Логика:
    `seg.ToLowerInvariant()` (нормализация регистра: `%f` приходит ВЕРХНИМ,
    объекты S3 — строчные); перебор endpoints по порядку — на каждый
    `new BackupS3(new S3ClientOptions(ep, bucket, accessKey, secretKey,
    Region: null, CaBundlePath: caFile), WalReceiverCore.PutAttemptTimeout)`
    → `GetObjectToFileAsync(prefix-кластер, prefix-шард, $"wal/{seg}", dest)`
    (ca_pem → temp-файл один раз при непустом); успех → exit 0 (объект
    записан в dest); объекта нет/все отказали → exit 1 (конец WAL для postgres).
    Получение восстановления: файл уже лежит по dest — вернуть 0.
  - `src/PgWorker.WalReceiver/Operations/RestoreOperation.cs` — операция
    restore (drill = тот же класс: команда без `target_time` = latest).
    Константа `ToolPaths.Pgdata = "/home/postgres/pgdata/pgroot/data"`
    (tool-контейнер монтирует volume в `/home/postgres/pgdata` — шаг 11;
    Spilo-layout). Пайплайн (механика 1:1 из `RestoreJobCommand.Script`,
    все инцидентные правки обязательны — Р15):
    1. Гвард 409 (маршрут); S3-клиент — как verify (step 3 Шага 6); CA — так же.
    2. Фаза downloading: скачивание `full/<backup_id>/` 1:1 в PGDATA
       (ListPrefixAsync + GetObjectToFileAsync по относительным путям; mkdir);
       проверка `backup_label` (нет → failed/downloading).
    3. Канонические каталоги initdb (mkdir, отсутствуют в S3): pg_tblspc,
       pg_replslot, pg_commit_ts, pg_snapshots, pg_serial, pg_twophase,
       pg_subtrans, pg_dynshmem, pg_notify, pg_stat, pg_stat_tmp,
       pg_wal/archive_status, pg_logical/snapshots, pg_logical/mappings.
    4. `postgresql.auto.conf`: скопировать в `.orig` (File.Copy); дописать
       строки (File.AppendAllText): `data_directory = '<PGDATA>'`,
       `hba_file = '<PGDATA>/pg_hba.conf'`, `ident_file = '<PGDATA>/pg_ident.conf'`,
       `wal_level = 'replica'`, `summarize_wal = 'off'`,
       `restore_command = '/opt/pgw-wal/walreceiver fetch-wal %f %p'`,
       `recovery_target_action = 'promote'`, `recovery_target_time = '<ts>'`
       (только если target_time задан — НЕпустая строка; null/пустая строка не
       приходят: latest — всегда null, воркер маппит "" → null при построении
       команды, шаг 14; воркер нормализует RFC3339 → PG-формат
       «yyyy-MM-dd HH:mm:sszzz» — существующая нормализация RestoreProcess),
       `shared_preload_libraries = ''`, `ssl = off`, `logging_collector = off`.
    5. Сигналы: `File.Delete(PGDATA/standby.signal)` (набор снят с реплики —
       без удаления standby-режим); создать пустой `recovery.signal`.
    6. `pg_hba.conf`: прочитать строки, prepend `local all all trust`, записать.
    7. Владелец/права ПОСЛЕ модификаций: runner: `chown -R 101:101
       /home/postgres/pgdata`, затем `chmod 700 <PGDATA>` (обёртка setpriv —
       ниже; chown/chmod — системные утилиты child-процессами).
    8. Фаза recovering: pg_ctl под uid/gid 101 — обёртка setpriv (РЕШЕНИЕ
       плана по spec §3.4 «выбор за планом»): каждый PG-процесс —
       `ProcessStartInfo { FileName="setpriv", ArgumentList={ "--reuid=101",
       "--regid=101", "--clear-groups", "<утилита>", … } }`; env процесса:
       `HOME=/tmp`, `PGPASSWORD` не нужен, env S3-контекста из шага fetch-wal
       (`PGW_S3_*` — postgres наследует до restore_command). Старт:
       `pg_ctl -D <PGDATA> -l /tmp/restore-pg.log -w -t 60 -o "-c
       listen_addresses='' -c unix_socket_directories='/tmp'" start`.
    9. Поллинг (бюджет `RecoveryTimeoutSec` из команды, сек): `psql -h /tmp
       -U postgres -tAc "SELECT pg_is_in_recovery()"` (root-клиент по trust —
       без setpriv, как bash); err (promote-окно рестарта postmaster) — ретрай
       2 с до бюджета; `f` → выход. Исчерпание бюджета → failed
       `recovery budget exceeded (<N> s)` (+stop best-effort).
    10. `psql … "SELECT pg_current_wal_lsn()"` → `restored_to_lsn`;
        `pg_ctl -D <PGDATA> -m fast stop` (setpriv).
    11. Чистка recovery-остатков: удалить `recovery.signal`; вернуть pristine
        `auto.conf` из `.orig` и вычистить из него `archive_mode`/`archive_command`
        (строки `^archive_mode\s*=` / `^archive_command\s*=` — Regex-фильтр
        построчно, запись обратно); `chown 101:101 <auto.conf>`; удалить
        trust-строку из `pg_hba.conf`.
    12. `pg_controldata <PGDATA>` → `system_id` (строка `Database system identifier:`,
        Regex; нет → failed). Итог succeeded (+RestoredToLsn, SystemId);
        failed-ветки — однострочная причина; cancel — как verify (kill дерева +
        `pg_ctl stop -m fast` best-effort + чистка сигналов НЕ делает — контейнер
        сносится воркером).
  - Маршрут `POST /api/ops/restore` — тело `StartRestoreRequest` (гвард 409,
    запуск фоном, 202); `GET /api/ops/restore` — ToolStatusResponse/404
    (drill использует те же маршруты — механика одна, spec §3.4).
- [ ] **Выход:** restore/drill исполняются приёмником tool-контейнера;
  инцидентная bash-механика воспроизведена 1:1 в .NET; fetch-wal — рабочий
  sub-режим бинаря.
- [ ] **Проверка:** сборка зелёная; юнит-тесты механики (порядок runner-вызовов,
  содержимое auto.conf/pg_hba/сигналов на temp-PGDATA, нормализация регистра
  fetch-wal) — шаг 9; сквозной прогон — E2E (шаг 17).
- **Связь со spec:** §3.2 (tool-таблица restore), §3.4 (Restore/drill +
  fetch-wal + перечень инцидентных правок), Р3 (.NET), Р13 (секреты не в argv),
  Р15 (1:1 механика).

## Шаг 8. Entrypoint/supervisord tool-ветка; образ `pgworker-node` — без пересборки контракта

- [ ] **Вход:** Шаги 4–7 выполнены (бинарь `walreceiver` умеет tool-режим и
  `fetch-wal`).
- [ ] **Действие**:
  - `docker/node/docker-entrypoint.sh`: ветка tool-режима ПЕРЕД генерацией
    конфигов (при `PGW_NODE_TOOL=1` — никаких PG-команд при старте: только
    старт контейнера, канон spec §3.3): `exec /opt/pgw-wal/walreceiver` —
    WalReceiver главный процесс контейнера (PID-1 через entrypoint-ветку;
    supervisord и spilo/doorman/haproxy-программы не стартуют вовсе).
    Решение плана (spec §3.3 оставлял выбор: «supervisord-минимум + walreceiver,
    либо walreceiver напрямую entrypoint'ом») — entrypoint-ветка напрямую:
    меньше движущихся частей, сигналы docker идут прямо в бинарь, env
    `PGW_NODE_TOOL` — единственное условие.
  - `docker/node/supervisord.conf` — БЕЗ изменений (нодовый режим как сегодня;
    `[program:walreceiver]` и далее стартует приёмник с identity).
  - `docker/node/Dockerfile` — БЕЗ изменений (тот же publish WalReceiver;
    образ один на оба режима — Р2; `EXPOSE 5432 8008 6432` — 8080 не публикуется
    на хост, EXPOSE-строку можно не менять: EXPOSE — документация, публикацию
    делает воркер; для tool-контейнеров публикацию :8080 выполняет их спека).
- [ ] **Подпункт для слабой модели** (entrypoint-ветка):
  файл `/Users/demakaev/ZCodeProject/worktrees/feat-t26-backup-jobs-in-receiver/docker/node/docker-entrypoint.sh`.
  В самое начало исполняемой части скрипта (сразу после строки `set -e`, ПЕРЕД
  `mkdir -p /etc/haproxy /etc/pg_doorman`) вставить блок:
  ```sh
  # Tool-режим контейнера (spec t26 §3.3): никаких PG-команд при старте —
  # только старт контейнера; WalReceiver — главный процесс (PID-1), весь
  # жизненный цикл операций — через его HTTP API. Patroni/supervisord и
  # генерация нодовых конфигов не запускаются.
  if [ "$PGW_NODE_TOOL" = "1" ]; then
      exec /opt/pgw-wal/walreceiver
  fi
  ```
  Больше ничего в файле не менять.
- [ ] **Выход:** tool-контейнер стартует одним бинарем; нодовый запуск не
  изменился; образ — тот же.
- [ ] **Проверка:** `sh -n docker/node/docker-entrypoint.sh` (синтаксис —
  выход 0); сборка образа не нужна в этом шаге (E2ePrepare — шаг 17);
  `grep -n "PGW_NODE_TOOL" docker/node/docker-entrypoint.sh` — блок на месте.
- **Связь со spec:** §3.3 (режимы WalReceiver и образ; supervisord/entrypoint
  tool-ветка), Р2.

## Шаг 9. Юнит-тесты приёмника (API-контракт, операции, инцидентная механика)

- [ ] **Вход:** Шаги 4–8 выполнены; `Microsoft.AspNetCore.Mvc.Testing` уже
  подключён в `src/tests/PgWorker.IntegrationTests`.
- [ ] **Действие** (новые тест-файлы; стиль — существующие тесты проекта
  (`NodeWalReceiverTests`, `BackupProcessTests`): xUnit + фейки, AAA-комментарии):
  - `src/tests/PgWorker.UnitTests/WalReceiver/OperationsRegistryTests.cs`:
    TryBegin на пустом реестре = true; повторный TryBegin при running = false
    (409-гвард); при терминальном снимке = true (перезапись); Update/Clear —
    видимость Current (атомарность без гонок — параллельный цикл Update не
    роняет).
  - `src/tests/PgWorker.IntegrationTests/Backups/BackupApiOperationTests.cs`
    (WebApplicationFactory над WalReceiver-Program — доступ к internal-Program
    обеспечивает `InternalsVisibleTo` из шага 4; фейки операций через DI —
    подменяются интерфейсы фабрик `IFullBackupOperationFactory`/
    `IToolOperationFactory` (шаг 4), фейковые фабрики возвращают
    фейк-операции с заданным снимком/исходом):
    POST full без операций → 202; повторный POST → 409; GET → 200 со снимком
    фейка → после Clear → 404; гейты (фейк IEtcdGateway с wal-ключом STOPPED/
    `backups-disabled`; ОТДЕЛЬНЫЙ кейс: гейт-чтение с Unknown=true — тоже 503,
    операция не стартовала) → POST → 503 и операция не стартовала; cancel без
    активной → 404; tool-маршруты verify/restore — те же таблицы (202/409/404).
  - `src/tests/PgWorker.UnitTests/WalReceiver/FullBackupOperationTests.cs`
    (фейк IProcessRunner + фейк IBackupS3): счастливый путь — порядок фаз
    running/basebackup → uploading (+wal_start_segment из фейкового
    backup_label) → succeeded, put-ключи `<C>/<X>/full/<id>/…` 1:1 +
    `<C>/<X>/wal/<seg>` без
    .partial/.history, PGPASSWORD в env psi, НЕТ пароля в ArgumentList; отказ
    pg_basebackup (exit 1) → failed + однострочный error из stderr; отказ
    upload (Result.Failed с `s3-pool-exhausted`) → failed с маркером; cancel →
    failed `cancelled`; чистка staging-каталога после исхода (temp-dir).
  - `src/tests/PgWorker.UnitTests/WalReceiver/RestoreOperationTests.cs` —
    ТАБЛИЧНЫЕ тесты инцидентной механики (temp-PGDATA + фейк-runner, который
    пишет stdout по FileName): после подготовки — `standby.signal` удалён,
    `recovery.signal` создан; auto.conf содержит все override-строки (data_directory/
    hba_file/ident_file/wal_level=replica/summarize_wal=off/restore_command с
    `fetch-wal %f %p`/recovery_target_action=promote/shared_preload_libraries=''/
    ssl=off/logging_collector=off; target_time только при заданной цели);
    pg_hba.conf — trust-строка первой; порядок runner-вызовов: chown -R 101:101 →
    chmod 700 → pg_ctl start (setpriv --reuid=101 --regid=101 --clear-groups,
    env HOME=/tmp, listen_addresses='') → psql-поллинг → pg_current_wal_lsn →
    pg_ctl stop -m fast → chown auto.conf; после итога — auto.conf восстановлен
    из .orig без archive_mode/archive_command (pristine с archive_mode=on →
    строк нет), trust-строка удалена, recovery.signal удалён; target_time
    отсутствует → строки recovery_target_time нет (latest); нормализация
    fetch-wal: `FetchWalCommand` — тест парсинга env + строчный регистр сегмента
    (фейк BackupS3 не нужен: тест логики выбора endpoint/регистра — чистая
    функция, вынести выбор в тестируемый helper).
  - `src/tests/PgWorker.UnitTests/WalReceiver/VerifyOperationTests.cs`:
    счастливый путь — скачивание всех объектов листинга в staging → pg_verifybackup
    → succeeded; отказ скачивания → failed + phase=downloading (маркер
    транзиента); pg_verifybackup exit 1 → failed + phase=verifying (permanent).
- [ ] **Выход:** контракт API, гварды, механики full/verify/restore и
  инцидентные правки покрыты юнитами на фейках (без docker).
- [ ] **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --nologo --filter "FullyQualifiedName~WalReceiver" > /tmp/t26-ut9.log 2>&1; tail -5 /tmp/t26-ut9.log` — Passed! (без Failed);
  интеграционные — `dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~BackupApiOperationTests"` — зелёный.
- **Связь со spec:** §3.8 п.1 (юниты: API-контракт, механика, табличные
  инцидентные тесты).

---

# Фаза 3. PgWorker: клиент API, адресация, перестройка процессов, конфиг, упразднение джобов

## Шаг 10. `WalReceiverApiClient` + адресация `alias:8080` + диагностика недостижимости

- [ ] **Вход:** Фаза 2 готова (контракт маршрутов зафиксирован `ApiContracts`);
  процессы-потребители пока работают по-старому (клиент только добавляется).
- [ ] **Действие** (новые файлы в `src/PgWorker.Backups/`):
  - `WalReceiverApiClient.cs` — интерфейс + реализация:
    ```csharp
    /// Исход вызова API приёмника: StatusCode 0 = транспортный отказ/таймаут;
    /// 5xx = приёмник ответил ошибкой; оба — transient для тиков воркера.
    public sealed record ReceiverReply(int StatusCode, ReceiverStatus? Status, string? Error)
    {
        public bool IsTransient => StatusCode is 0 or >= 500;
        public bool IsNotFound => StatusCode == 404;
        public bool IsConflict => StatusCode == 409;
        public bool IsGated => StatusCode == 503;
        public bool IsAccepted => StatusCode == 202;
    }
    /// Снимок статуса из GET (поля spec §3.2).
    public sealed record ReceiverStatus(string State, string Phase,
        string? WalStartSegment = null, long? SizeBytes = null,
        string? RestoredToLsn = null, string? SystemId = null, string? Error = null);

    public interface IWalReceiverApiClient
    {
        Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct);
        Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct);
        Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct);
        Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct);
        Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct);
        Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct);
        Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct);
        Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct);
    }
    ```
    Реализация `WalReceiverApiClient(HttpClient http)`: JSON POST/GET через
    `System.Text.Json`. Направление ссылки проектов — `PgWorker.WalReceiver` →
    `PgWorker.Backups` (приёмник использует `BackupLabel`/`PoolBackupS3`),
    обратной ссылки нет, цикл запрещён — РЕШЕНИЕ: записи JSON-контракта клиента
    описываются В САМОМ `src/PgWorker.Backups/WalReceiverApiClient.cs` (см.
    подпункт ниже); отдельный файл `WalReceiverApiContracts.cs` НЕ создаётся;
    имена JSON-полей — те же snake_case, что у `ApiContracts.cs` приёмника
    (фактическая сверка контракта — API-тесты маршрутов шага 9, реальный
    HTTP-обмен). HttpClient timeout 10 с (per-вызов
    ~10 с — spec §3.5); 5xx/таймаут/отказ соединения → `ReceiverReply(0/5xx)`
    (транспортный класс — НЕ исключение наружу).
  - `WalReceiverAddress.cs` — резолв адреса API:
    ```csharp
    public static class WalReceiverAddress
    {
        /// Базовый URL API приёмника ноды: имя docker-контейнера :8080
        /// (канонические — pgw-<C>-<X>-<n>; object — NodeAddress.Object).
        public static string ForNode(string cluster, string shard, string node, string? objectContainer)
            => $"http://{objectContainer is { Length: > 0 } o ? o : $"pgw-{cluster}-{shard}-{node}"}:8080";

        /// Базовый URL API tool-контейнера: контур (ContourNetwork задан) —
        /// имя контейнера :8080 в сети контура; прод — host:<динамический порт>
        /// из инспекта публикации :8080.
        public static string ForTool(string containerName, string host, int? publishedPort, bool contourMode)
            => contourMode ? $"http://{containerName}:8080" : $"http://{host}:{publishedPort ?? 0}";
    }
    ```
  - DI: регистрация `IWalReceiverApiClient` singleton в `PgWorker.App/Program.cs`
    (HttpClient с Timeout 10 с).
- [ ] **Подпункт для слабой модели** (клиент):
  создать файл `/Users/demakaev/ZCodeProject/worktrees/feat-t26-backup-jobs-in-receiver/src/PgWorker.Backups/WalReceiverApiClient.cs`,
  namespace `PgWorker.Backups`, usings `System.Net.Http.Json`,
  `System.Text.Json`. Содержимое: записи-контракты (JSON-имена snake_case
  атрибутами `JsonPropertyName`) `StartFullRequest { string Id }`,
  `S3Context { string[] Endpoints; string AccessKey; string SecretKey; string Bucket; string Prefix; string? CaPem }`,
  `StartVerifyRequest { string BackupId; S3Context S3 }`,
  `StartRestoreRequest { string? TargetTime; int RecoveryTimeoutSec; string BackupId; S3Context S3 }`,
  `ReceiverStatus { string State; string Phase; string? WalStartSegment; long? SizeBytes; string? RestoredToLsn; string? SystemId; string? Error }`,
  `ReceiverReply { int StatusCode; ReceiverStatus? Status; string? Error }` с
  bool-свойствами `IsTransient` (0 или >=500), `IsNotFound` (404), `IsConflict`
  (409), `IsGated` (503), `IsAccepted` (202). Интерфейс — вставить ДОСЛОВНО:
  ```csharp
  /// <summary>Клиент HTTP API приёмника WalReceiver (spec t26 §3.2):
  /// baseUrl — базовый URL приёмника (нода — alias:8080, tool — host:порт);
  /// любой транспортный отказ/таймаут/5xx — transient-исход (IsTransient),
  /// исключения наружу не пробрасываются.</summary>
  public interface IWalReceiverApiClient
  {
      Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct);
      Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct);
      Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct);
      Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct);
      Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct);
      Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct);
      Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct);
      Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct);
  }
  ```
  Реализация `WalReceiverApiClient(HttpClient http)`:
  JsonSerializerOptions с `DefaultIgnoreCondition = WhenWritingNull`; каждый
  метод: `try { using var resp = await http.<Get/PostAsJsonAsync>(
  $"{baseUrl}<path>", …, options, ct); if ((int)resp.StatusCode == 200 или 202)
  читать JSON-тело в ReceiverStatus/принять; return new ReceiverReply((int)resp.StatusCode, status, null); }
  catch (Exception e) { return new ReceiverReply(0, null,
  e.Message.Split('\n')[0]); }` — ЛЮБОЙ транспортный отказ/таймаут (HttpRequestException,
  TaskCanceledException) — StatusCode 0, исключение наружу не пробрасывается.
  Пути: full → `/api/backup/full`, full cancel → `/api/backup/full/cancel`,
  verify → `/api/ops/verify`, restore → `/api/ops/restore`, tool-cancel →
  `/api/ops/cancel`. Комментарии — по-русски, без упоминания задач.
- [ ] **Подпункт для слабой модели** (зонд портов):
  создать файл `/Users/demakaev/ZCodeProject/worktrees/feat-t26-backup-jobs-in-receiver/src/PgWorker.Backups/Tool/FreePortProbe.cs`,
  namespace `PgWorker.Backups.Tool`, usings `System.Net`,
  `System.Net.Sockets`, `Shared.Core`:
  ```csharp
  /// <summary>Зонд свободного host-порта (правило AGENTS.md «динамические
  /// порты»): TcpListener на IPAddress.Any:0 — порт выделен ядром, слушатель
  /// немедленно освобождается. Гонка «порт заняли до create» — create падает,
  /// вызывающий трактует transient и повторяет с новым портом.</summary>
  public static class FreePortProbe
  {
      public static Result<int> Allocate()
      {
          var listener = new TcpListener(IPAddress.Any, 0);
          try
          {
              listener.Start();
              return Result<int>.Success(((IPEndPoint)listener.LocalEndpoint).Port);
          }
          catch (Exception e)
          {
              return Result<int>.Failed(new ApplicationException($"free port probe failed: {e.Message}", e));
          }
          finally
          {
              listener.Stop();
          }
      }
  }
  ```
- [ ] **Выход:** клиент и адресация готовы (потребители — шаги 12–14); DI
  зарегистрирован; порт-зонд готов.
- [ ] **Проверка:** сборка солюшена зелёная (`dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t26-build10.log 2>&1`).
- **Связь со spec:** §3.1 (канал alias:8080, диагностика недостижимости —
  transient), §3.5 (общий клиент per-вызов ~10 с, 5xx/таймаут = transient).

## Шаг 11. Env ноды `PGW_BACKUP_STAGING_DIR`; спека tool-контейнера `ToolJobSpec`

- [ ] **Вход:** Шаг 10 выполнен; `BackupsRuntimeOptions` пока со старыми
  полями (JobImage/StagingQuotaBytes — удалит шаг 15; здесь добавляется
  `NodeImage`).
- [ ] **Действие**:
  - env ноды: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` —
    `NodeBackupEnv(string? Bucket, string? CaPem)` → добавить
    `string? StagingDir` (третий аргумент);
    `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (блок `nodeBackupEnv`,
    ~строка 680): `env["PGW_BACKUP_STAGING_DIR"] = backupEnv.StagingDir` —
    ставить всегда (приёмнику нужен путь staging полного; null/пусто → env не
    ставить, приёмник возьмёт дефолт `/home/postgres/pgdata/backup-staging`).
    Точку передачи значения найти по grep `new NodeBackupEnv(` (одна точка —
    DI-путь из BackupsRuntimeOptions.StagingDir; поле существующее).
    Приёмник: `Program.cs` (шаг 4) читает `PGW_BACKUP_STAGING_DIR` — сверить
    имя.
  - `src/PgWorker.Backups/Tool/ToolJobSpec.cs` — спека tool-контейнера
    (единая для verify/drill/restore — spec §3.3 Р2):
    ```csharp
    public static class ToolJobSpec
    {
        // image = opts.NodeImage; env: PGW_NODE_TOOL=1 (+ больше НИКАКОЙ
        // identity-env: весь S3-контекст приходит POST-командой — Р4).
        public static ContainerSpec Build(
            BackupsRuntimeOptions opts, string containerName,
            string? volumeName, string volumeDest,
            int? hostPort) // null = контур-режим: сеть контура, без публикации
        => new ContainerSpec(
            Image: opts.NodeImage,
            Env: new Dictionary<string, string> { ["PGW_NODE_TOOL"] = "1" },
            VolumeName: volumeName, VolumeDest: volumeDest,
            Ports: hostPort is { } p ? [new PortMap(8080, p)] : [],
            Hostname: containerName,
            CpuCores: opts.AgentCpu, MemoryBytes: opts.AgentMem,
            LabelKey: "pgworker", Label: /* cluster */,
            Cmd: null, ResetEntrypoint: false, // entrypoint образа: tool-ветка — шаг 8
            Network: opts.ContourNetwork,
            NetworkAliases: null, Tmpfs: null,
            ExtraHosts: opts.ContourNetwork is null
                ? ["host.docker.internal:host-gateway", "local:host-gateway"] : null,
            RestartPolicy: "no");
    }
    ```
    Точки монтирования: verify — `volumeName=pgw-backup-verify-<C>-<X>-<id>`,
    `volumeDest="/staging"`; restore — `volumeName=pgw-<C>-<X>-<n1>-data`
    (data-volume первой ноды), `volumeDest="/home/postgres/pgdata"`; drill —
    `volumeName=pgw-backup-drill-<C>-<X>-<id>` (то же имя контейнера — D1),
    `volumeDest="/home/postgres/pgdata"`. Имена контейнеров — существующие
    `BackupNames.VerifyContainerName/RestoreContainerName/DrillContainerName`.
    (параметр `cluster` добавить в сигнатуру Build для Label.)
  - `BackupsRuntimeOptions`: добавить `string NodeImage = "pgworker-node:dev"`;
    маппинг в `PgWorker.App/Options.cs` `ToRuntime`: `NodeImage: docker.Images.Node`
    (в `ToRuntime` передать образ ноды — сигнатура `ToRuntime(string? contourNetwork, string nodeImage)`
    или через поле — точка сборки одна, `PgWorkerOptions.BackupsRuntime()`).
- [ ] **Выход:** env ноды несёт staging-путь; спека tool-контейнера готова
  (потребители — шаги 12–14); образ ноды доезжает в опции бэкапов.
- [ ] **Проверка:** сборка зелёная; `grep -n "PGW_BACKUP_STAGING_DIR" src/ -r --include="*.cs" | grep -v tests` — 2 точки (установка драйвером + чтение приёмником).
- **Связь со spec:** §3.3 (tool-режим env), §3.6 (Staging.Dir новый смысл,
  Agent-лимиты tool-контейнеров), Р2 (образ нод), Р5 (staging в data-volume).

## Шаг 12. `BackupProcess` на API: G0–G3 без изменений, запуск/супервизия через приёмник

- [ ] **Вход:** Шаги 10–11 выполнены; `BackupProcess` работает по-старому
  (джоб-контейнер + docker-логи) — заменяется содержимое `SuperviseActiveAsync`
  и ветки G3.
- [ ] **Действие** (`src/PgWorker.Backups/Process/BackupProcess.cs`):
  - Конструктор: добавить `IWalReceiverApiClient receiver` (замена docker-веток;
    `driver` остаётся — G2 hba-гвард и прочие контуры его используют);
    удалить `BackupS3Factory`-env-джобовые списки из спек (резолв пула остаётся
    — `poolResolved`/G3-гварды работают как сегодня).
  - G0/G1/pool-резолв/addresses/restore-гвард/G2 (роль+pg_hba)/G3-планировщик
    (`HasActive/IsDue/BackoffPassed/NextId`, BROKEN/gap-покрытие), journal-before-
    manipulations, клэйм-гварды — БЕЗ ИЗМЕНЕНИЙ.
  - G3 вместо create/start джоба: после PLANNED-put — `receiver.StartFullAsync(
    WalReceiverAddress.ForNode(cluster, shard.Name, node, nodeKv.Value.Object), id, ct)`:
    `IsAccepted || IsConflict` ⇒ put RUNNING (409 = пере-старт прошлого тика,
    операция уже жива — spec §3.5); `IsGated/IsTransient` ⇒ PLANNED остаётся,
    следующий тик повторит (journal-факт при недостижимости: phase
    `receiver-unavailable/<X>/<id>`, error = адрес + причина — spec §3.1
    диагностика); контейнера полного больше нет — docker-движок источника
    здесь не нужен.
  - `SuperviseActiveAsync` — перестройка ветвления:
    - возрастной бюджет — ПЕРВЫЙ гвард (как сегодня `Job:FullTimeoutSec`):
      FAILED `job-timeout: <age> s > FullTimeoutSec` + best-effort
      `receiver.CancelFullAsync(адрес ноды)` (docker kill+rm НЕ нужен —
      контейнера нет; staging чистит приёмник; недостижим приёмник →
      journal-пометка «cancel пропущен» в той же записи — spec Р8);
    - `PLANNED` — идемпотентный повтор `StartFullAsync` (тот же исход-таблица);
    - `RUNNING/UPLOADING` — `receiver.GetFullAsync`:
      `IsTransient` ⇒ статус не меняем (transient тик);
      `IsNotFound` ⇒ FAILED `operation-vanished` (Р9 — рестарт приёмника/ноды;
      переснятие по G3 с бэкоффом);
      `Status.State == "uploading"` ⇒ put UPLOADING + `WalStartSegment` (как
      сегодня из маркеров);
      `State == "succeeded"` ⇒ COMPLETED + `SizeBytes`, `verify = PENDING`
      при on_create;
      `State == "failed"` ⇒ FAILED + `Status.Error`;
    - cleanup-методы `CleanupJobAsync`/docker-имя `pgw-backup-full-*` —
      удалить из процесса ( vanished-ветка docker-контейнера не существует).
  - Мульти-хост takeover: адрес ноды из portalloc (`addresses` уже в тике);
    source/host-недоступность портalloc — transient (как сегодня).
- [ ] **Выход:** полный бэкап: PLANNED → API-запуск → RUNNING → UPLOADING
  (`wal_start_segment`) → COMPLETED/FAILED; docker-логи/контейнеры полного не
  существуют; etcd-контракт статусов не изменился (`BackupStatusJson`).
- [ ] **Проверка:** сборка зелёная; юнит-тесты — шаг 16 (таблицы
  202/409/503/404/transient, бюджет+cancel, vanished).
- **Связь со spec:** §3.5 п.1 (BackupProcess), Р8 (бюджет+cancel), Р9
  (vanished), Р10 (выбор ноды — воркер), Р11 (инвариант одного активного).

## Шаг 13. `BackupVerifyProcess`: цепочечная проверка без изменений; checksums — tool-контейнер + API

- [ ] **Вход:** Шаги 10–12 выполнены.
- [ ] **Действие** (`src/PgWorker.Backups/Process/BackupVerifyProcess.cs`):
  - due-резолв/цепочечная проверка (list+`WalChain.CheckRange`+LSN-разбор
    `.history`, union-листинги) — БЕЗ ИЗМЕНЕНИЙ.
  - Checksums-фаза вместо `VerifyJobSpec`: зонд `FreePortProbe.Allocate()`
    (прод; контур-режим — без публикации) → `ToolJobSpec.Build(opts, cluster,
    BackupNames.VerifyContainerName(cluster, shard, id),
    volumeName: BackupNames.VerifyVolumeName(cluster, shard, id),
    volumeDest: "/staging", hostPort)` → `engine.CreateContainerAsync` +
    `StartContainerAsync` (хост — как сегодня: `EngineForShardAsync`,
    fallback-хост с journal-фактом) → `receiver.StartVerifyAsync(адрес, new
    StartVerifyRequest(id, new S3Context(poolCtx.ReadAll, poolCtx.AccessKey,
    poolCtx.SecretKey, opts.S3Bucket, $"{cluster}/{shard}", caPem)), ct)` —
    адрес: контур `ForTool(name, host, null, contourMode: true)`, прод —
    инспект `InspectContainerAsync(name).Ports` (PortMap 8080 → HostPort) →
    `ForTool(name, addr.Host, port, false)`. CA — `BackupCaPem.ReadIfConfigured(opts)`.
  - `SuperviseAsync` (перестройка): orphan/stale-снос (ключ ушёл/verify
    FAILED) — kill+rm контейнера+volume как сегодня; running — возрастной
    бюджет `Job:VerifyTimeoutSec` по `StartedAtUnix` инспекта (как сегодня):
      kill+rm tool-контейнера, `checked_unix = now` (вердикт НЕ ставится);
    `created` → старт; exited-ветка УХОДИТ — итог по API: `GetVerifyAsync`:
    `IsTransient` ⇒ ждём; `succeeded` ⇒ verify OK + `checked_unix`;
    `failed` + `Phase=="downloading"` (МАРКЕР ТРАНЗИЕНТА) ⇒ статус не трогаем
    (PENDING остаётся), снос контейнера+volume, journal `download-retry`
    — следующий тик создаст заново (spec §3.5); `failed` + `phase=="verifying"`
    ⇒ verify FAILED + `error`; `IsNotFound` (операция vanished, контейнер жив)
    ⇒ transient: снос контейнера+volume, PENDING — ретрай (контейнер-сирота
    без операции = факт рестарта приёмника).
  - Итог-ветки (OK/FAILED) — снос контейнера+volume (как сегодня
    `CleanupJobAsync`).
- [ ] **Выход:** verify: on_create + периодика; checksums в tool-контейнере
  через API; таймаут ⇒ PENDING (вердикт не ставится); docker-логов нет.
- [ ] **Проверка:** сборка зелёная; юнит/интеграционные — шаг 16.
- **Связь со spec:** §3.5 п.2 (BackupVerifyProcess), §5 arch/19 (семантика
  transient/permanent сохранена), Р8 (бюджет verify).

## Шаг 14. `RestoreProcess` + `RestoreDrillProcess`: tool-контейнер + API

- [ ] **Вход:** Шаги 10–13 выполнены; оба процесса работают (bash-джобы) —
  заменяется только RUNNING-фаза restore и супервиз/создание дрилла.
- [ ] **Действие `RestoreProcess`** (`src/PgWorker.Backups/Process/RestoreProcess.cs`):
  - PLANNED-валидация (кластер Active, шард без усыновлённых, бэкап существует
    etcd/list-S3, WalChain непрерывен), демонтаж (ТОЛЬКО до первого старта
    tool-контейнера — 409 volume-in-use прецедент жив), REJOINING
    (EnsureNode, initialize-щит system_id, SQL-гейт), стоп-гвард исключения —
    БЕЗ ИЗМЕНЕНИЙ.
  - RUNNING вместо `RestoreJobSpec`: после демонтажа — зонд порта (прод) →
    `ToolJobSpec.Build(opts, cluster, RestoreContainerName(cluster, shard, op.Id),
    volumeName: $"pgw-{cluster}-{shard.Name}-{first}-data",
    volumeDest: "/home/postgres/pgdata", hostPort)` → create+start (тот же
    идемпотентный порядок: нет контейнера → демонтаж+create; старт в обоих
    случаях) → `receiver.StartRestoreAsync(адрес, new StartRestoreRequest(
    BackupId: op.BackupId, TargetTime: нормализованный target, "" → null
    (ЕДИНОЕ ПРАВИЛО: latest = null в контракте, шаг 10; пустая строка
    приёмнику не передаётся),
    RecoveryTimeoutSec: options.RestoreRecoveryTimeoutSec, S3: контекст
    source_pool ?? собственного пула (ReadAll/креды — уже резолвится в
    `ResolveReadPoolAsync`), ct)`. DR `source_pool` — тот же пул-контекст,
    prefix в S3Context = `SrcCluster(op)/SrcShard(op)`.
  - Супервиз RUNNING: статусный бюджет `Job:RestoreTimeoutSec` (как сегодня,
    от `started_unix`) → kill+rm tool-контейнера + FAILED
    `restore-job-timeout`; поллинг `GetRestoreAsync`: `IsTransient` ⇒ ждём;
    `Phase` (`downloading|recovering`) ⇒ put Phase статуса etcd (панель видит
    прогресс, как сегодня); `succeeded` ⇒ REJOINING (+`RestoredToLsn`/
    `SystemId` из статуса — дальше существующий код щита/EnsureNode);
    `failed` + `downloading` ⇒ transient (статус не трогаем, ретраи тиками —
    скачивание перезапустится пере-POST: идемпотентная досоздача по имени
    контейнера + повторный StartRestore; контейнер жив — операция завершилась:
    снос контейнера+пере-POST командой); `failed` + `recovering` ⇒
    permanent-FAILED + чистка (как сегодня); `IsNotFound` (контейнер жив,
    операции нет — рестарт приёмника) ⇒ transient: снос + пере-POST.
    Бюджет `RecoveryTimeoutSec` — внутри операции (приёмник), 60-сек запас
    воркера на download — сохранить существующий guard.
  - EXIT-инспект ветка удаляется (итог только по API).
- [ ] **Действие `RestoreDrillProcess`**
  (`src/PgWorker.Backups/Process/RestoreDrillProcess.cs`):
  - Отбор/валидация/cleaning-доводка/мульти-хост-супервиз (поиск по хостам
    `Docker:Hosts`) — БЕЗ ИЗМЕНЕНИЙ.
  - Запуск: `ToolJobSpec.Build(opts, cluster, DrillContainerName(...),
    volumeName: DrillVolumeName(...), volumeDest: "/home/postgres/pgdata",
    hostPort)` + `receiver.StartRestoreAsync(адрес, StartRestoreRequest(
    drill.BackupId, TargetTime: null (latest), RecoveryTimeoutSec:
    options.RestoreRecoveryTimeoutSec, S3: read-контекст кластера), ct)`;
    vanished-досоздача (молодой RUNNING без контейнера → create+start+POST по
    имени) и `drill-vanished` (старше бюджета) — как сегодня.
  - Супервиз: `Drill:TimeoutSec` — как сегодня; поллинг `GetRestoreAsync`:
    фаза → put Phase; `succeeded` ⇒ SUCCEEDED + `RestoredToLsn`; `failed` +
    `recovering` ⇒ FAILED + error; `failed` + `downloading` / `IsNotFound` /
    `IsTransient` ⇒ transient (снос при отсутствующей операции; терминальный
    вердикт по завершающей операции). Вердикт — ОДНИМ put с `phase:"cleaning"`
    → `FinishCleanupAsync` (kill+rm контейнера/volume, подтверждение, снятие
    phase) — БЕЗ ИЗМЕНЕНИЙ.
- [ ] **Выход:** restore/drill исполняются tool-контейнерами по API;
  etcd-контракт заявки/дрилла не изменился; budgets/чистки/D1/D2 живы.
- [ ] **Проверка:** сборка зелёная; юнит/интеграционные — шаг 16.
- **Связь со spec:** §3.5 п.3–4 (RestoreProcess/RestoreDrillProcess), §3.4
  (tool-контейнер монтирует data-volume первой ноды / свой volume), Р8
  (бюджеты), Р9 (vanished), Р15.

## Шаг 15. Конфиг (§3.6), удаление джоб-механики, упразднение `pgworker-backup`

- [ ] **Вход:** Шаги 12–14 выполнены — потребители джоб-спек/логов/команд
  переведены на API; код компилируется со старыми классами.
- [ ] **Действие**:
  - Конфиг (spec §3.6):
    `src/PgWorker.App/Options.cs` (`BackupsOptions`): удалить `BackupsJobOptions.Image`
    (секция `Job` остаётся с таймаутами), `StagingQuotaBytes`;
    `Staging.Dir` дефолт → `/home/postgres/pgdata/backup-staging`;
    `IsValid()`: убрать проверку `Job.Image` непустого; `appsettings.json`
    (App) — обновить секцию `Backups` (без Job.Image/QuotaBytes, новый Staging.Dir).
  - deploy/dev-stand (spec §3.7 «упоминания в images-инфраструктуре/ранбуке — убрать»):
    `deploy/docker-compose.yml` — удалить build-сервис `pgworker-backup`
    (блок с `build: docker/PgWorker.Backup.Dockerfile`, `image: pgworker-backup:dev`,
    `profiles: ["build"]`, entrypoint-заглушкой, ~строки 451–457, вместе с
    поясняющим комментарием), env-строку
    `PgWorker__Backups__Job__Image: ${PGW_BACKUP_JOB_IMAGE:-pgworker-backup:dev}`
    (~строка 54) и её комментарий; проверить, что `PGW_BACKUP_JOB_IMAGE` больше
    нигде в `deploy/`/`dev-stand/` не упоминается (в `deploy/.env.example`
    этого ключа нет — сверить);
    `dev-stand/adminpanel/checks/00-up.sh` — удалить блок сборки
    `pgworker-backup:dev` (~строки 192–197: комментарий + `docker build -q -f
    "$ROOT/docker/PgWorker.Backup.Dockerfile" …` + echo);
    `dev-stand/images/images.txt` — убрать `pgworker-backup:*` из перечисления
    локально собираемых образов (строка 4).
  - Удаление мёртвого кода (потребителей больше нет):
    `BackupJobSpec.cs`, `BackupJobLog.cs`, `VerifyJobSpec.cs`, `VerifyLog.cs`,
    `RestoreJobSpec.cs`, `RestoreJobLog.cs`, `RestoreJobCommand.cs`,
    `VerifyJobCommand.cs`, `DrillJobSpec.cs`, `DrillJobCommand.cs`,
    `Drill/DrillJobLog.cs`
    (юнит-тесты этих классов: `BackupJobLogTests`, `BackupJobSpecTests`,
    `DrillJobSpecTests` — удалить; правки остальных тестов — шаг 16).
  - Упразднение `pgworker-backup`: удалить `docker/PgWorker.Backup.Dockerfile`,
    `docker/PgWorker.Backup.E2E.Dockerfile`, `docker/backup/entrypoint.sh`;
    `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`: удалить
    сборку `pgworker-backup:e2e-*` (строка ~1947) и `JobImage` (~127) —
    полная E2E-адаптация — шаг 17 (здесь только упразднение образа, чтобы
    E2ePrepare не собирал мёртвый образ).
  - D1-чистка: `src/PgWorker.Docker/Drivers/ClusterDriver.cs`
    `RemoveBackupJobsAsync` — убрать снос префикса `pgw-backup-full-<C>-*`
    (контейнеры полного не существуют; префикс умирает — spec §3.7); снос
    verify/restore/drill префиксов и staging-volumes verify/drill — оставить;
    `BackupNames.ContainerName/VolumeName/JobContainerPrefix/JobVolumePrefix`
    (full-имена) — удалить вместе с потребителями.
  - `GetContainerLogsAsync` — продуктовых вызовов из бэкап-процессов больше
    нет (сегодняшние 5: BackupProcess/BackupVerifyProcess/RestoreProcess/
    RestoreDrillProcess — сняты шагами 12–14); метод интерфейса движка
    остаётся (API движка — приёмки других подсистем вне t26).
- [ ] **Подпункт для слабой модели** (компиляция после удаления):
  в каталоге `/Users/demakaev/ZCodeProject/worktrees/feat-t26-backup-jobs-in-receiver`
  выполнить, записав вывод в файл:
  `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t26-build15.log 2>&1`
  и сообщить: количество строк `error` в `/tmp/t26-build15.log` (ожидается 0).
  Если ошибки — НЕ править код, вертать текст первых 30 строк с `error` в ответ.
- [ ] **Выход:** конфиг §3.6 соответствует spec; джоб-механика/bash-скрипты/
  образ `pgworker-backup` отсутствуют (код, docker, deploy, dev-stand); D1 жив
  для tool-префиксов.
- [ ] **Проверка:** (шаблон отсекает S3-бакет `pgworker-backups`, который НЕ
  упраздняется) `grep -rEn "pgworker-backup([^s]|$)" src/ docker/ deploy/ dev-stand/ --include="*.cs" --include="Dockerfile*" --include="*.yml" --include="*.yaml" --include="*.sh" --include="*.txt" -l` — пусто;
  `grep -rn "BackupJobSpec\|RestoreJobCommand\|VerifyJobSpec\|DrillJobSpec\|BackupJobLog\|RestoreJobLog\|DrillJobLog\|VerifyLog" src/ docker/ --include="*.cs" --include="Dockerfile*" -l` — пусто;
  `grep -rn "Job__Image\|Job:Image\|PGW_BACKUP_JOB_IMAGE" deploy/ dev-stand/ -l` — пусто;
  `grep -n "GetContainerLogsAsync" src/PgWorker.Backups -r` — пусто;
  сборка зелёная (подпункт).
- **Связь со spec:** §3.6 (конфиг), §3.7 (образы и чистки), §8 п.1
  (критерий: GetContainerLogsAsync из продуктовых путей нет; образ отсутствует).

---

# Фаза 4. Тесты: юниты → интеграция → docker-E2E (§3.8)

## Шаг 16. Тесты процессов воркера на фейке `IWalReceiverApiClient`

- [ ] **Вход:** Шаги 12–15 выполнены; существующие тесты
  (`src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`,
  `src/tests/PgWorker.IntegrationTests/Backups/BackupVerifyProcessTests.cs`,
  `RestoreProcessTests.cs`, `RestoreDrillProcessTests.cs`) сломаны сменой
  зависимостей — правятся здесь.
- [ ] **Действие**:
  - Фейк `FakeReceiverClient : IWalReceiverApiClient` — в `FakeBackupDeps.cs`
    (IntegrationTests); для `PgWorker.UnitTests` — локальный фейк в файле
    теста (или общий файл `src/tests/PgWorker.UnitTests/Backups/FakeReceiverClient.cs`);
    программирование ответов по методу/порядку вызовов (очередь
    `ReceiverReply`), счётчики вызовов Start/Cancel/Get с baseUrl-аргументами.
  - `BackupProcessTests` — таблицы (AAA-комментарии): 202 ⇒ RUNNING; 409 при
    PLANNED ⇒ RUNNING (пере-старт); 503/5xx/0 ⇒ PLANNED остаётся + journal
    `receiver-unavailable`; GET uploading ⇒ UPLOADING+WalStartSegment; GET
    succeeded ⇒ COMPLETED+SizeBytes+verify PENDING; GET failed ⇒ FAILED+error;
    404 при RUNNING ⇒ FAILED `operation-vanished`; возраст > FullTimeoutSec ⇒
    FAILED job-timeout + CancelFullAsync вызван; G0/G1/G2/G3-гварды — прежние
    кейсы живут (фейк-драйвер etcd/db — как сейчас).
  - `BackupVerifyProcessTests`: создание tool-контейнера (`ToolJobSpec`-спека
    в фейк-драйвере: образ `NodeImage`, env `PGW_NODE_TOOL=1`, volume verify,
    порт-публикация в прод-режиме) + StartVerify с S3-контекстом; succeeded ⇒
    OK+checked_unix; failed+downloading ⇒ PENDING и снос; failed+verifying ⇒
    FAILED; таймаут (StartedAtUnix инспекта) ⇒ kill+rm+checked_unix.
  - `RestoreProcessTests`/`RestoreDrillProcessTests`: tool-контейнер с data-volume
    первой ноды (restore) / своим volume (drill); StartRestore-тело
    (backup_id/target/recovery_timeout_sec/S3-контекст source_pool); фазы в
    статус etcd; succeeded ⇒ REJOINING (+restored_to_lsn/system_id — щит);
    failed+recovering ⇒ permanent-FAILED; drill: SUCCEEDED+restored_to_lsn,
    drill-vanished/drill-timeout — прежние кейсы.
  - Адресация: юнит `WalReceiverAddressTests` — ForNode каноническая/object;
    ForTool контур/прод; transient-таблица клиента (шаг 10) — Review Focus 1.
- [ ] **Выход:** все процессы покрыты таблицами исходов API; старые
  docker-лог-фейки из бэкап-тестов удалены.
- [ ] **Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --nologo > /tmp/t26-ut16.log 2>&1; tail -3 /tmp/t26-ut16.log` и `dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~Backups" --nologo > /tmp/t26-it16.log 2>&1; tail -3 /tmp/t26-it16.log` — зелёные (skip'ы оркестратора — штатный исход).
- **Связь со spec:** §3.8 п.1 (процессы воркера на фейке WalReceiverApiClient:
  202/409/503/404, бюджеты, vanished, cancel; адресация alias:8080).

## Шаг 17. E2E-адаптация (`E2eEnvironment`/сценарии): образ ноды, tool-контейнеры, динамические порты

- [ ] **Вход:** Шаги 12–16 выполнены; E2E-сценарии бэкапов компилируются, но
  упоминают `pgworker-backup`/JobImage.
- [ ] **Действие** (`src/tests/PgWorker.IntegrationTests/E2e/`):
  - `E2eEnvironment.cs`: удалить `JobImage`/сборку `pgworker-backup:e2e-*`
    (остатки шага 15); env воркера в сценариях — без джобовых ключей; ContourNetwork/
    advertised — как было; НОДА: `docker inspect` нод сценария — публикаций
    :8080 нет (E2ePrepare/EnsureNode их не ставят — инвариант Р1 сохранён
    кодом); WAL_PUBLISH для tool — `PgWorker.WalReceiver` publish — существующий
    (образ ноды уже содержит tool-режим).
  - Сценарии бэкапов (`E2eBackupJobsScenarios`, `E2eBackupVerifyScenarios`,
    `E2eRestoreScenarios`, `E2eRestoreNewClusterScenarios`, `E2eDrillScenarios`,
    `E2eTliSwitchScenarios`, `E2eBackupFailsOnBadS3Scenarios`,
    `E2eBackupPoolScenarios` и др.): assertions на etcd-статусы — БЕЗ ИЗМЕНЕНИЙ;
    прямые docker-действия над джоб-контейнерами (если сценарий их делал) —
    перевести на tool-контейнеры (имена те же) или API-фейк (для fault-injection:
    недостижимость приёмника — блокировка alias, рестарт tool-контейнера —
    vanished-сценарии); динамические порты — `GetMappedPublicPort`-стиль
    (инспект публикации tool-контейнера), никаких литералов портов.
  - Изоляция/teardown/телеметрия — по `docs/e2e-isolation.md` и AGENTS.md:
    MarkFailed, артефакты, `[PHASE]` — существующая инфраструктура не меняется.
  - Документация (упоминания образа/джобов): `docs/runbook.md`,
    `docs/backup-restore.md`, `docs/e2e-isolation.md` — убрать упоминания
    `pgworker-backup`/`PgWorker:Backups:Job:Image`/`PGW_BACKUP_JOB_IMAGE`
    (формулировки привести к приёмнику/tool-контейнерам; тон окружающего
    текста, без исторических пассажей).
- [ ] **Выход:** E2E-сценарии бэкапов адаптированы к приёмнику/tool-контейнерам;
  артефакты `pgworker-backup` отсутствуют во всём репо (кроме S3-бакета
  `pgworker-backups` — он НЕ упраздняется).
- [ ] **Проверка:** (шаблон отсекает бакет `pgworker-backups`)
  `grep -rEn "pgworker-backup([^s]|$)" src/ docker/ deploy/ dev-stand/ docs/ -l | grep -v superpowers` — пусто;
  `grep -rn "Job__Image\|PGW_BACKUP_JOB_IMAGE" deploy/ dev-stand/ docs/ -l` — пусто;
  сборка зелёная.
- **Связь со spec:** §3.8 п.2 (изоляция/teardown/телеметрия, динамические
  порты, зачистка), §8 п.2 (docker inspect нод — публикаций :8080 нет).

## Шаг 18. Мерж-гейт: E2ePrepare, свежий Release, полный E2E + зачистка серий

- [ ] **Вход:** Шаги 12–17 выполнены; рабочее дерево чисто (все правки в
  ветке worktree); docker доступен.
- [ ] **Действие** (КАЖДАЯ команда — фон (`run_in_background`), вывод ПОЛНОСТЬЮ
  в файл, чтение из файла; таймауты ожидания ≤ 30 с на проверку):
  1. Статика E2E ОДНИМ процессом: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/t26-e2eprep.log 2>&1` — финальная строка `[PHASE] e2e-prepare: done`.
  2. Сборка (свежесть бинаря): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t26-gate-build.log 2>&1` — 0 errors.
  3. Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build --nologo > /tmp/t26-gate-ut.log 2>&1` — зелёный.
  4. Интеграция: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --no-build --nologo > /tmp/t26-gate-it.log 2>&1` — зелёный; после КАЖДОЙ серии — зачистка (`docker ps -a --filter name=pgw- | head`, `docker network ls | grep -c kfw-net`, `docker network prune -f` при сиротах) до финальной строки прогона.
  5. Docker-E2E: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.slnx -c Release --no-build --filter "FullyQualifiedName~Scale_AddEmptyShard" > /tmp/t26-gate-e2e-min.log 2>&1` — зелёный; затем ПОЛНЫЙ прогон E2eFixture (`--filter "FullyQualifiedName~E2e"`), так как меняются образ ноды/supervisord-ветка, адресация API и все бэкап-пути; зачистка после серии; фейлы вне области задачи — задача в `arch/roadmap/bugs.md`, работа продолжается; ПЕРЕЗАПУСК упавших — запрещён без согласия пользователя (разбор по логам `/tmp/pgw-e2e-artifacts-<guid>/`).
- [ ] **Выход:** гейт зелёный на свежем Release; серии зачищены.
- [ ] **Проверка:** финальные строки всех лог-файлов (`Passed!`, `[PHASE] … done`); `docker ps -a | grep -c pgw-` ≈ 0, `docker network ls | grep -c kfw-net` ≈ 0.
- **Связь со spec:** §3.8 п.3 (гейт: свежий Release, две отдельные команды,
  E2ePrepare без pgworker-backup, минимум Scale_AddEmptyShard + полный
  E2eFixture), §8 п.8.

---

# Фаза 5. Мерж-коммит: роадмап

## Шаг 19. Удалить `t26`, `t24`, `t23` из `arch/roadmap/backup.md`

- [ ] **Вход:** Шаг 18 зелёный; мерж в `main` одобрен пользователем
  (коммит/пуш — только по явному приказу, AGENTS.base §7).
- [ ] **Действие** (`arch/roadmap/backup.md` — ТЕМ ЖЕ коммитом, что и мерж):
  удалить пункты `t26-backup-jobs-in-receiver`, `t24-job-status-channel`
  (Р6 — удаляется ЦЕЛИКОМ), `t23-backup-job-endpoint` (Р7); вычистить
  `←`-ссылки на эти теги из всех `arch/roadmap/*.md`; никаких пометок
  «закрыта/реализована» — история в git и `docs/superpowers/`.
- [ ] **Выход:** роадмап без t23/t24/t26; зависимости чисты.
- [ ] **Проверка:** `grep -rn "t26-\|t24-\|t23-" arch/roadmap/` — пусто.
- **Связь со spec:** §4 фаза 5, §6 Р6/Р7; правило роадмапа «слито → тег
  удаляется тем же коммитом» (AGENTS.md).

---

## Самопроверка плана (заполнена при составлении)

- **Покрытие spec §4:** Фаза 1 → шаги 1–3; Фаза 2 → шаги 4–9; Фаза 3 → шаги
  10–15; Фаза 4 → шаги 16–18; Фаза 5 → шаг 19. §3.1 (канал/диагностика) → 3,
  10, 11; §3.2 (API) → 5, 6, 7; §3.3 (режимы/образ) → 4, 8; §3.4 (механики) →
  5, 6, 7; §3.5 (процессы) → 12–14; §3.6 (конфиг) → 11, 15; §3.7 (образы/
  чистки) → 8, 15; §3.8 (тесты) → 9, 16, 17, 18. §8 (критерии 1–9): 1 → 15;
  2 → 3, 11, 17; 3–4 → 12; 5 → 13; 6 → 14; 7 → 8, 14; 8 → 18; 9 → 1–3, 19.
- **Развилки:** выбор entrypoint-ветки (§3.3 «выбор за планом») — зафиксирован
  в шаге 8; setpriv-эквивалент (§3.4 «выбор за планом») — зафиксирован в шаге 7;
  маркер транзиента скачивания (§3.5 «план фиксирует маркер») — зафиксирован
  в «Сквозных решениях» и шагах 6/7/13/14; дублирование записей контракта
  между PgWorker.WalReceiver и PgWorker.Backups (запрет цикла ссылок) —
  зафиксировано в шаге 10. Иных точек выбора при реализации нет.
- **Сигнатуры** — из реального кода: `WalReceiverIdentity`, `WalReceiverGates`,
  `WalReceiverTargets`, `PoolBackupS3(write, read, access, secret, bucket,
  caPath, requestTimeout)`, `WalReceiverCore.PutAttemptTimeout`,
  `BackupLabel.WalStartSegment`, `IBackupS3`, `BackupNames.*`,
  `ContainerSpec`, `NodeBackupEnv(Bucket, CaPem)`, `DockerImagesOptions.Node`,
  `BackupsRuntimeOptions`, `ShardEndpoints.ResolveBackupSourceAsync`,
  `BackupPlanner.HasActive/IsDue/BackoffPassed/NextId`, `RestoreStatus.*`,
  `DrillStatus.*`, `BackupVerify(BackupVerifyStatus, CheckedUnix, Error)`.

