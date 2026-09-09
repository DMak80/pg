# Spec: t03-backup-wal-stream — непрерывная online-архивация WAL-сегментов

Дата: 2026-09-10
Канон: [arch/19-backups.md](../../../arch/19-backups.md) §3 (агент), §4 (etcd-контракт), §5 (layout S3), §6 (слот/лимиты), §7 (секреты); контекст сервиса — [arch/14-pgworker.md](../../../arch/14-pgworker.md). Правки канона этой задачей — §2 (контракт образа), §3 (переписан: истина S3, правила непрерывности, lag, стоп-семантика), §8/§9 (карта задач, конфиг); roadmap: `t03 ← t02 (мерж-порядок)`.

## 1. Цель

Непрерывная online-доставка WAL-сегментов каждого шарда каждого Active-кластера
в объектное хранилище бэкапов (S3/MinIO) — инкремент к последнему полному
бэкапу: агент `pg_receivewal` в docker-контейнере, супервизируемый PgWorker,
контроль непрерывности цепочки от стартовой точки (нет дыр — восстановление
возможно) и алерт при разрыве/отставании цепочки (статус воркера в etcd +
панельные правила алертов AdminPanel).

Границы t03:

- ВХОДИТ: процесс воркера `WalStreamProcess` (ensure слота, подъём/супервиз
  агента, смена мастера, стоп-семантика), S3-листер и контроль непрерывности
  (парсер имён сегментов, gap-детектор, TLI-переходы), etcd-статус
  `/pgworker/backups/<C>/<X>/wal`, панельные правила алертов, живой
  docker-E2E маркер.
- НЕ входит: образ `pgworker-backup` и ensure роли `backup_exec`/ключа
  `/clusters/<C>/backup_password` — приходят из t02 (мерж-порядок t03 после
  t02; контракт образа зафиксирован каноном arch/19 §2 — bash +
  postgres-клиентские утилиты + mc, без встроенного шиппера); полные бэкапы
  (t02), verify (t04), восстановление (t05), ретенция/чистка WAL (t06),
  супервизор-reconcile S3↔etcd (t07), UI бэкапов (t08).

## 2. Принципы

1. **arch-first**: канон arch/19 — источник правды; правки канона (контракт
   образа, истина S3, правила непрерывности, конфиг) внесены в этой задаче
   ДО кода; расхождения вскрываются — канон правится тем же dev-flow.
2. **PgWorker — единственный писатель** `/pgworker/backups/*` (клэйм `<C>`);
   агент etcd не касается вовсе.
3. **Истина прогресса — объекты S3**: воркер list-ит префикс `wal/` и
   вычисляет `last_uploaded`/непрерывность по факту; никаких
   статус-объектов агента (один источник истины).
4. **Агент тупой и заменяемый**: `pg_receivewal` + bash-цикл доставки
   (inline-команда от воркера — поведение агента версионируется кодом
   воркера, смена механики не требует пересборки образа).
5. **Паттерны arch/17/14**: идемпотентность каждого шага (перепроверка
   факта), journal-before-manipulations, transient vs permanent,
   «факт над записью» (S3 — истина; etcd-статус — следствие), клэйм-гварды.
6. **Параллельная разработка, мерж после t02**: кодирование ведётся на
   контракте канона; финальный мерж — после t02 в main (нужны Dockerfile
   образа и ensure роли/секрета).

## 3. Структура/компоненты

### 3.1. Новый проект `src/PgWorker.Backups` (по образцу `PgWorker.Moves`)

Зависимости: `PgWorker.Core` (Result, модели), `PgWorker.Etcd`
(IEtcdGateway, Coordination), `PgWorker.Docker` (IDockerEngine,
ContainerSpec), `PgWorker.Provisioning` (ShardEndpoints — резолв мастера,
DSN-билдеры). Пакет `AWSSDK.S3` (Directory.Packages.props; path-style через
`ForcePathStyle=true` — MinIO и облако одним клиентом).

- **`Model/WalFileName.cs`** — разбор/сравнение имён файлов WAL:
  сегмент = 24 hex (`TLI(8)+log(8)+seg(8)`), `.partial`-суффикс
  (незакрытый — в цепочку не входит), `NNNNNNNN.history` (timeline-история);
  `Next()`: `seg+1`, при `seg=0xFF` → `log+1, seg=0`; `Distance(other)` в
  сегментах (для lag). Чистые функции, полный юнит-покрытием.
- **`Model/WalChain.cs`** — gap-детектор непрерывности: вход — набор имён
  объектов S3 (сегменты + history) + `chainStart`; выход —
  `ChainResult { IsContinuous, FirstGap?, LastSegment }`. Правила (канон
  §3): последовательность `Next()` без пропусков; TLI-переход валиден при
  наличии `<newTLI>.history` и первом сегменте нового TLI ∈ {последний
  сегмент старого TLI, `Next(последний)`}; `.partial` игнорируется.
- **`WalAgentCommand.cs`** — билдер inline bash-команды контейнера агента
  (единственное место, где живёт механика шиппера): запуск
  `pg_receivewal --slot=<slot> -D <staging> -d <conninfo>` + цикл поллинга
  staging: файл без `.partial` (закрытый сегмент или `.history`) →
  `mc cp .../<C>/<X>/wal/<name>` → `rm` после успешного cp; `.partial`
  не грузится. Секреты — через env контейнера (не интерполяция в строку
  команды).
- **`BackupS3.cs`** — тонкая обёртка S3-клиента: `ListWalAsync(cluster,
  shard)` (list-objects-v2 с пагинацией по префиксу `<C>/<X>/wal/`),
  `BucketExistsAsync` (диагностика старта). Никаких других операций t03
  не требует (загрузка — mc внутри агента).
- **`WalStreamProcess.cs`** — процесс одного тика под клэймом `<C>`
  (см. §4).
- **`WalStatusWriter.cs`** — сборка JSON статуса (формат arch/19 §4
  1:1 с каркасной моделью `WalStreamState`) + put etcd-ключа; пишет при
  изменении (идемпотентность: безделье не пишет).

### 3.2. Процесс `WalStreamProcess` (машина одного тика)

Вызов — из `ClusterProcesses`/`ReconcileLoop` в Active-ветке, после
`rotate-app-password`, до `repair`/`moves` (короткая операция; list —
по расписанию `Wal:VerifyIntervalSec`, не каждый тик). Guard'ы: конфиг
`Backups:Enabled=true`; кластер Active; шард с `dsn` (поднят). Для каждого
шарда — независимо, ошибка шарда не роняет остальные.

1. **Креды/ensure-зависимости**: чтение `/clusters/<C>/backup_password`;
   отсутствует (t02 не смержена/ensure не прошёл) → transient-пропуск
   шарда с journal-заметкой (журнал `/pgworker/work/<C>`,
   op=`backup-wal`) — агент не поднимается, ретраи тиками.
2. **Резолв мастера**: `ShardEndpoints.ResolveMasterAsync`; недоступен →
   transient-пропуска шарда (статус не деградирует: failover-окно).
3. **Ensure слота** `pgw_bkp_<C>_<X>` (sha1-усечение > 63 симв, канон §3):
   SQL к мастеру (Npgsql, admin-DSN): слот есть → пропустить; нет И
   etcd-статуса цепочки тоже нет (первый старт) → `pg_create_physical_
   replication_slot(<slot>, true)`; нет ПРИ живой записи о цепочке —
   инвалидация слота → разрыв (см. шаг 6, DEGRADED + стоп агента).
4. **Контейнер агента** `pgw-backup-wal-<C>-<X>` (создание идемпотентно,
   по образцу `EnsureNode`, но без портов): образ `Backups:AgentImage`,
   env — S3-креды/endpoint/bucket, conninfo-параметры мастера (роль
   `backup_exec`, пароль из etcd-ключа), имя слота, staging-каталог;
   `Cmd` — inline-команда `WalAgentCommand`; volume staging
   `pgw-backup-wal-<C>-<X>-staging` (ephemeral, квота `Staging:QuotaBytes`
   — при исчерпании агент умрёт → рестарт-контур шага 5); лимиты
   `Agent { Cpu, Mem }` → HostConfig; сеть — per-cluster сеть кластера
   (как ноды; адрес мастера = alias ноды `:5432`; усыновлённые `object` —
   `host:pg-port` из portalloc); restart-политика `unless-stopped`.
5. **Супервиз**: контейнер есть и running → пропустить; есть, но exited
   (restart-луп: креды устарели после ротации, staging переполнен) →
   `stop+rm` и пересоздание со свежими env/кредами; смена мастера
   (резолв ≠ `master_node` статуса) → пересоздание с нового мастера.
   Смерть контейнера без docker-рестарта — подхватит `unless-stopped`,
   воркер вмешивается только при расхождении конфигурации.
6. **Контроль (по расписанию `Wal:VerifyIntervalSec`)**: `BackupS3.ListWal`
   → `WalChain.CheckChain` от `chain_start` → при дыре: `state=DEGRADED`,
   `error` с границами дыры, ОСТАНОВ агента (stop+rm; слот не
   пересоздаётся — продолжение с дырой бессмысленно, лечение — новый
   полный t02/t07; при появлении нового COMPLETED полного с
   `wal_start_segment` выше дыры — `chain_start` пересчитывается от
   полных, агент поднимается заново).
7. **Lag-зонд** (в том же контрольном проходе): SQL мастера
   `pg_current_wal_lsn()` → имя сегмента → `lag_segments = Distance(
   master_segment, last_uploaded)`; `> Wal:LagMaxSegments` или возраст
   `last_uploaded_unix > Wal:StaleSec` → `DEGRADED` + `error` (transient
   по природе: ретраи тиками, восстановление → ACTIVE).
8. **Статус в etcd** (`WalStatusWriter`): `chain_start_segment` =
   `min(wal_start_segment по COMPLETED полным)` ?? закреплённое значение
   из текущего ключа ?? min-объект S3 (первый сегмент потока — при
   отсутствии полных, канон §3); `last_received_segment` =
   `last_uploaded_segment` (наблюдаемое воркером принято == загружено;
   внутренний прогресс staging — дело агента, не наблюдаем без exec —
   осознанно); `master_node`, `slot`, `last_uploaded_unix`, `lag_segments`,
   `state` = ACTIVE | DEGRADED | STOPPED, `error`. Ключ не пишется до
   первого наблюдения (нет полных, нет объектов S3, нет прошлого ключа —
   агент ещё ничего не доставил; появление первого объекта закрепляет
   `chain_start` = min-объект).

**Стоп-семантика** (канон §3): `Enabled=false`, remove-shard (S-процессы),
deprovisioning (D1) → stop+rm контейнеров агентов (идемпотентно по имени;
в D1 — рядом с нодами, D2 уже чистит `del --prefix /pgworker/backups/<C>/`);
шард QUARANTINED (эвакуация) → stop агента + `state=STOPPED`. Остановка по
`Enabled=false`/QUARANTINED при живом ключе пишет финальный
`state=STOPPED` (последнее касание ключа — иначе застывший ACTIVE кормил
бы ложный панельный алерт wal-stream-lag; демонтаж ключи удаляет).

### 3.3. Панель AdminPanel (алерты, без UI-грани)

- **Чтение префикса**: etcd-refresher панели читает `/pgworker/backups/`
  в снапшот; `AdminPanel.Etcd/Parsing/BackupsParser` — панельный парсер
  (по образцу воркерного каркасного: битые значения → parseErrors,
  толерантно; модель — только поля статусов, политика не нужна).
- **Правила алертов** (паттерн `IAlertRule`, словарь severity по образцу
  существующих):
  - `wal-chain-broken` (critical): `state=DEGRADED` у шарда живого
    Active-кластера (текст — `error` статуса: границы дыры); remedy —
    «переснять полный бэкап (t02), разбор по runbook»;
  - `wal-stream-lag` (warning): `state=ACTIVE`, но `lag_segments` ≥
    панельного порога или `last_uploaded_unix` старше порога (значения —
    `AlertsOptions`, синхронизированы с воркерными дефолтами);
  - `wal-stream-stopped` (warning): `state=STOPPED` при живом шарде.
- UI-грань бэкапов (списки/детали/MinIO) — t08; алерты видны в общем
  списке алертов панели без новых экранов.

### 3.4. Конфигурация

`BackupsOptions` (каркас t01) дополняется (канон §9):

```
PgWorker:Backups {
  ...existing S3/Policy/Staging/Agent...
  AgentImage = "pgworker-backup:latest"   # образ §2, сборка t02
  Wal { VerifyIntervalSec=30, LagMaxSegments=1024, StaleSec=300 }
}
```

Валидация старта не меняется (fail-fast Enabled+пустой S3 — уже в каркасе).
Деплой/стенд: env уже в `deploy/.env.example` (`PGW_BACKUPS_*`);
dev-стенд включает подсистему при `PGW_BACKUPS_ENABLED=true` (стендовая
настройка — в scope t03 не входит: стенд-включение закроет t02 вместе с
полными бэкапами; E2E поднимает своё MinIO).

### 3.5. Наблюдаемость

Логи воркера: подъём/остановка агента, разрыв цепочки (границы дыры),
пересоздание при смене мастера, lag-деградация. Метрика
`pgworker_backup_wal_lag_segments{cluster,shard}` (gauge, существующий
`WorkerMetricsInstrumentation`) — оператору без панели. Всё значимое — в
etcd-ключе wal (панель/оператор).

## 4. Фазы

- **Ф0. Канон (эта фаза spec — уже выполнена)**: arch/19 §2 (контракт
  образа: только инструменты, inline-команда), §3 (переписан: истина S3,
  правила непрерывности, lag-зонд, стоп-семантика, слот-ensure воркером),
  §8 (карта: панельные правила), §9 (конфиг AgentImage/Wal); roadmap
  t03 ← t02 (мерж-порядок).
- **Ф1. Ядро без docker/S3**: `WalFileName`, `WalChain`, `WalAgentCommand`,
  сборка JSON статуса; юнит-тесты (инкремент/0xFF-переход, `.partial`,
  TLI-переходы ±history, gap-детектор, команда агента).
- **Ф2. S3-листер + статус-райтер**: `BackupS3` (пагинация list-v2),
  `WalStatusWriter` (put при изменении); интеграционные тесты против
  testcontainers-MinIO (динамический порт!) — list/пагинация/отсутствие
  префикса.
- **Ф3. Процесс**: `WalStreamProcess` (guard'ы, слот-SQL, контейнер агента
  через ContainerSpec/Cmd, супервиз, смена мастера, контроль по расписанию,
  lag-зонд, стоп-семантика), DI + `ClusterProcesses`/`ReconcileLoop`,
  правки Deprovisioning (D1: агенты) и RemoveShard (S: агент шарда);
  интеграционные тесты с fake-docker/S3 (существующие паттерны тестов
  процессов): подъём/идемпотентность, смена мастера, инвалидация слота →
  DEGRADED+стоп, лаги.
- **Ф4. Панель**: чтение префикса, парсер, 3 правила алертов; юниты
  правил (снапшоты с состояниями ACTIVE/DEGRADED/STOPPED, пороги).
- **Ф5. E2E-маркер** `WalStream_UploadsSegmentsContinuously` (Release,
  `PGW_TEST_DOCKER=1`): фикстура E2e + MinIO-контейнер (динамический порт)
  + сборка образа `pgworker-backup` из `docker/pgworker-backup.Dockerfile`
  (файл из t02; отсутствует → явный skip с внятным сообщением — до мержа
  t02); сценарий: кластер provisioned → INSERT-нагрузка (генерация WAL) →
  за бюджет появляются сегменты в MinIO → etcd-ключ `wal`=ACTIVE,
  `last_uploaded` растёт, цепочка непрерывна. Мерж-гейт: маркер зелёный
  на свежем Release (AGENTS.md — обязателен для задач воркеров).
- **Ф6. Мерж-гейт**: полный прогон юниты+интеграция+E2E, зачистка
  контейнеров/сетей после серий (`docker rm -f` + `network prune`),
  roadmap-гейт (снятие тега t03 — после t02).

## 5. Ограничения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, LangVersion
  latest; версии пакетов — только `Directory.Packages.props` (добавляется
  `AWSSDK.S3`).
- Порты docker в тестах — только динамические (`assignRandomHostPort:
  true`/`GetMappedPublicPort`, зонд свободных портов); никаких литералов.
  Таймауты фикстур: `BrokerBootSec`-аналоги ≤ 100 с; WAL-бюджеты E2E —
  короткие (нагрузка INSERT-ами форсирует переключение сегментов
  `pg_switch_wal()`, не ожиданием таймаутов).
- Зачистка контейнеров и сетей между сериями — обязательна (incl.
  `docker network prune -f`; сети per-cluster движка ryuk не подбирает).
- Язык: документация/комментарии — русский; идентификаторы — английский.
- Тесты — с AAA-комментариями.
- Не трогать: HA-контур нод, конфиги Patroni, префиксы чужих писателей;
  S3-объекты никогда не удаляются автоматически (R4-симметрия: воркер не
  уничтожает потенциально ценные данные).
- Каноническая модель t01 (`BackupsParser`/`WalStreamState`) — совместимость
  1:1: формат ключа wal не меняется; BackupsParser подключается к
  потреблению в воркере (каркас его не читал — потребители t02+; t03
  начинает читать префикс для собственных нужд процесса).

## 6. Критерии приёмки

1. **AC1 (доставка)**: на E2E-стенде (Release, PG-кластер + MinIO) при
   `Backups:Enabled=true` после INSERT-нагрузки в бакете появляются
   объекты `<C>/<X>/wal/<segment>` (закрытые сегменты, без `.partial`),
   слот `pgw_bkp_<C>_<X>` создан на мастере, контейнер
   `pgw-backup-wal-<C>-<X>` running.
2. **AC2 (статус)**: etcd-ключ `/pgworker/backups/<C>/<X>/wal` в формате
   каркасной модели: `state=ACTIVE`, `slot`, `master_node`,
   `chain_start_segment`, `last_received_segment`, `last_uploaded_segment`/
   `_unix`, `lag_segments`; парсер t01 читает его без parseErrors.
3. **AC3 (непрерывность)**: юниты gap-детектора покрывают: сплошная
   цепочка, дыра внутри, TLI-переход с `.history` (первый сегмент нового
   TLI == последнему/next), TLI-переход без history → разрыв; в S3
   `.history` загружается (юнит билдера команды: фильтр `.partial`,
   обязательность history).
4. **AC4 (разрыв)**: инвалидация слота (интеграционный сценарий) →
   `state=DEGRADED` + `error` с границами дыры + агент остановлен;
   появление нового полного со свежим `wal_start_segment` → цепочка и
   агент восстанавливаются (ACTIVE).
5. **AC5 (отставание/алерты)**: lag > порога / тишина загрузок → DEGRADED
   (воркер) и панельные алерты `wal-chain-broken`/`wal-stream-lag`/
   `wal-stream-stopped` зажигаются по снапшоту (юниты правил).
6. **AC6 (стоп-семантика)**: deprovisioning кластера / remove-shard /
   `Enabled=false` останавливают и удаляют контейнеры агентов (без
   сирот); D2 чистит `/pgworker/backups/<C>/`.
7. **AC7 (мерж-гейт)**: полный прогон зелёный на свежем Release, включая
   маркер `WalStream_UploadsSegmentsContinuously`; зачистка
   контейнеров/сетей после серий; мерж строго после t02.
8. **AC8 (канон)**: правки arch/19/roadmap из Ф0 — в ветке, канон и код
   не расходятся (ревью plan↔spec по чек-листам dev-flow).

## 7. Риски

| Риск | Митигация |
|---|---|
| list S3 растёт с длиной цепочки (тысячи объектов) | list-v2 пагинация; VerifyIntervalSec=30; при необходимости t06/t07 сдвигают chain_start (ретенция удаляет хвост) — list обратно короткий |
| Инвалидация слота тихо рвёт цепочку | шаг 6 процесса: слот исчез при живой цепочке → DEGRADED + стоп агента; `max_slot_wal_keep_size` уже в каноне нод (arch/14 §2.1) ограничивает диск |
| Агент-луп после ротации пароля backup_exec | супервиз: exited-контейнер → пересоздание со свежими env (шаг 5); ротация ключа — t02 |
| До мержа t02 нет Dockerfile образа | E2E собирает образ из файла t02; отсутствует → явный skip с сообщением; юниты/интеграция (fake docker/S3) от образа не зависят |
| TLI-эвристика по номерам без LSN пропускает экзотические разрывы | полный LSN-разбор `.history` — t04 (verify); t03 детектирует пропуски номеров и отсутствие history |
| Мёртвый мастер: агент рестартует в пустоту | `unless-stopped` + transient-статус; failover подхватит смена-мастера процессом; stale-порог переводит в DEGRADED |
| `.partial`-гонка: cp начат, файл переименован | грузим только файлы БЕЗ `.partial`; переименование атомарно (rename в той же ФС); cp идемпотентен (повтор — перезапись объекта) |
