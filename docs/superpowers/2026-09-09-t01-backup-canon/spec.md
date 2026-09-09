# Spec: канон подсистемы бэкапов шардов (t01-backup-canon)

Задача roadmap: [`t01-backup-canon`](../../../arch/roadmap/backup.md) —
арх-канон подсистемы бэкапов PG-шардов (новый `arch/19-backups.md`) +
spec-скелет подсистемы для последующих задач t02–t07 + код-каркас
(конфиг, модель etcd, MinIO в стенде). Реализация процессной логики —
t02–t07; t01 ничего не исполняет.

## 1. Цель

1. **Канон** `arch/19-backups.md`: единый документ подсистемы бэкапов —
   механика (полные `pg_basebackup`, online-WAL `pg_receivewal`-агент),
   layout объектного хранилища S3/MinIO, контракт etcd-состояния
   `/pgworker/backups/*`, источник бэкапа (реплика → fallback мастер),
   влияние на HA-контур и ресурсные лимиты, секреты. Номер 19: 17 и 18
   заняты (`17-synchronization-principles.md`, `18-metrics.md`); фраза
   roadmap «будущий 17-backups.md» устарела — мерж-гейт правит ссылку.
2. **Spec-скелет**: этот документ фиксирует архитектурные решения для
   t02–t07 (полный бэкап, WAL-поток, проверки, восстановление, ретенция,
   супервизор) — каждая следующая задача реализует раздел канона, не
   переоткрывая архитектуру.
3. **Код-каркас** (решение пользователя): MinIO-сервис dev-станда +
   S3-секция env в `deploy/.env.example`, конфиг-секция
   `PgWorker:Backups` (options, default `Enabled=false`),
   модель/парсер etcd-ключей `/pgworker/backups/*` в `PgWorker.Etcd` с
   юнит-тестами. Без процессной логики, без docker-объектов, без UI.

### Решения пользователя (зафиксированы вопросами)

| Вопрос roadmap | Решение |
|---|---|
| Механика | Базовые утилиты PostgreSQL + оркестрация PgWorker (WAL-G/pgBackRest Spilo НЕ используем) |
| Хранилище бэкапов | Объектное хранилище S3/MinIO (не файловый том docker-хоста) |
| С кого снимается полный бэкап | Реплика (sync-standby), fallback на мастер; источник — факт в etcd |
| Online-WAL | `pg_receivewal`-агент: long-running контейнер воркера, pull, replication slot |
| Границы t01 | Канон + spec + код-каркас (MinIO, конфиг, модель etcd) — без процессной логики |

## 2. Принципы

1. **arch-first**: контракт — сначала `arch/19-backups.md` (новый) +
   указатели (`arch/README.md`, `arch/14-pgworker.md` §границ,
   `arch/adminpanel/02-etcd-contract.md` §2.3.1, roadmap-шапка), затем
   отражение в коде.
2. **PgWorker — хозяин бэкапов** (roadmap backup.md): создаёт,
   ретраит/пересоздаёт, чистит, сигнализирует в панель. Ноды (Spilo)
   бэкапами не управляют: без `archive_command`, без WAL-G-env, без
   S3-кредов в нодах.
3. **Единый etcd-контур и существующие паттерны** (arch/14, arch/17):
   префикс `/pgworker/backups/*` пишет ТОЛЬКО PgWorker (держатель клэйма
   `<C>`); панель только читает; заявки/декларации — через API воркера
   (протоколы §9 контракта панели, по мере надобности в t02+);
   идемпотентность каждого шага; journal-before-manipulations; transient
   vs permanent-отказы; `TreatWarningsAsErrors`, .NET 10.
4. **Хранилище — S3-совместимое**: endpoint/креды per-install из env
   воркера (не в etcd, не в git — группа секретов §4 arch/14: S3 —
   транспортная граница, как TLS/SSH); dev-стенд — MinIO, прод — любое
   S3-совместимое (вкл. облако). Клиент воркера — `PathStyle=true`
   (MinIO).
5. **Никаких изменений поведения воркера в t01**: `Enabled=false`
   по умолчанию; новый код — только опции и модель данных; существующие
   процессы/циклы не затронуты.
6. Язык: документация — русский, идентификаторы — английский.

## 3. Канон подсистемы (содержание `arch/19-backups.md`)

Новый документ `arch/19-backups.md` в стиле arch/14–16 (роль, механика,
контракт etcd, процессы-заготовки для t02–t07, надёжность, наблюдаемость,
конфигурация, риски). Ниже — фиксируемые архитектурные решения.

### 3.1. Механика полных бэкапов (реализация — t02)

- **Инструмент**: `pg_basebackup` (plain-формат, `-X stream`,
  `--checkpoint=spread`, `--manifest-checksums=SHA256`) — ephemeral
  docker-контейнер джоба `pgw-backup-full-<C>-<X>-<id>`, запускает
  PgWorker; образ — лёгкий `pgworker-backup` (postgres-клиентские
  утилиты + S3-uploader; сборка — t02, от отдельного Dockerfile в
  `docker/`).
- **Источник**: реплика шарда (sync-standby из Patroni `GET /cluster`);
  реплика недоступна/отстала → fallback на мастер (резолв мастера —
  существующий `ShardEndpoints`, arch/14 §5 F). Источник фиксируется в
  etcd-статусе (`node`, `role`).
- **Пайплайн джоба**: staging-каталог (ephemeral volume джоба) →
  `pg_basebackup -D <staging>` → потоковая загрузка в S3: файлы бэкапа
  1:1 объектами (включая `backup_manifest`), WAL-сегменты
  инкрементального набора `-X stream` — в общий WAL-префикс шарда.
  Завершение — атомарная фиксация статуса в etcd (§3.4).
- **Идентификатор**: `id = YYYYMMDDHHMMSSZ` (UTC старта, сортируемый);
  коллизия в пределах шарда — суффикс `-2`, `-3`… Детерминизм имён — как
  у `pgw-<C>-<X>-<n>` (arch/14 §6).
- **Подключение**: адресация из portalloc (`pg`-порты нод) в том же
  namespace адресов, что панель (advertised-правило arch/14 §2.4 п.5);
  БД-роль — §3.6.

### 3.2. Online-WAL (реализация — t03)

- **Агент**: long-running контейнер `pgw-backup-wal-<C>-<X>` (тот же
  образ `pgworker-backup`), запускает и супервизирует PgWorker.
  Внутри — `pg_receivewal --slot=<slot> -D <staging>` + S3-шиппер:
  закрытый сегмент → upload `wal/<segment>` → удаление из staging;
  `.partial`-файлы и `.history` — по канону `pg_receivewal` (history
  файлы ЗАГРУЖАЮТСЯ обязательно — PITR через смену timeline).
- **Слот**: один на шард, детерминированное имя `pgw_bkp_<C>_<X>`;
  если длиннее NAMEDATALEN(63) — `pgw_bkp_` + sha1(`<C>/<X>`)[:16].
  Слот создаёт агент (в t03), держит WAL на мастере до подтверждения
  приёма — защита непрерывности цепочки.
- **Подключение**: к мастеру шарда (резолв `ShardEndpoints`); смена
  мастера → агент переподключается (рестарт с нового мастера,
  догнавание цепочки; склейка timeline — через `.history`).
- **Непрерывность цепочки**: инвариант — от стартовой точки каждого
  COMPLETED полного бэкапа до `last_uploaded` нет дыр (контроль имён
  сегментов + TLI-переходы через history; проверка — t03/t04, база —
  парсер имён в каркасной модели t01 не входит, фиксируется каноном).

### 3.3. Хранилище S3: layout

Один bucket на установку (`PgWorker:Backups:S3:Bucket`); префиксы —
per-cluster/per-shard:

```
s3://<bucket>/<C>/<X>/
  full/<id>/                    # файлы pg_basebackup 1:1 (вкл. backup_manifest, pg_wal/)
  wal/<segment>                 # 000000010000000000000001, …
  wal/00000002.history          # timeline-истории (обязательны)
```

- Ключ объекта = путь файла; `backup_manifest` в корне `full/<id>/` —
  база `pg_verifybackup` (t04: скачивание в staging → verify).
- Никаких tar-обёрток: файлы 1:1 (верифицируемость и простой restore);
  WAL-сегмент = один объект.
- Layout одинаков для MinIO и облака; регион/endpoint — конфиг.

### 3.4. Контракт etcd `/pgworker/backups/*`

Пишет ТОЛЬКО PgWorker (держатель клэйма `<C>`; операции бэкапов — под
тем же клэймом, что и остальные процессы кластера); панель читает
(алерты t02/t03: «нет валидного полного за окно суток», «разрыв/отставание
WAL-цепочки» — контекст adminpanel/02 §2.3.1).

| Ключ | Тип | Значение |
|---|---|---|
| `/pgworker/backups/<C>/policy` | обычный | per-cluster политика: `{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":86400,"verify":{"on_create":true}}`; пишет воркер (приём через API — с t02/t06); отсутствует → дефолт `PgWorker:Backups:Policy` |
| `/pgworker/backups/<C>/<X>/full/<id>` | обычный | статус полного бэкапа: `{"state":"PLANNED\|RUNNING\|UPLOADING\|COMPLETED\|FAILED\|DELETING","node":"<n>","role":"replica\|master","started_unix","finished_unix"?,"wal_start_segment","size_bytes"?,"error"?,"verify":{"state":"PENDING\|OK\|FAILED","checked_unix"?}}` |
| `/pgworker/backups/<C>/<X>/wal` | обычный | состояние WAL-потока шарда: `{"state":"ACTIVE\|DEGRADED\|STOPPED","slot":"<slot>","master_node","chain_start_segment","last_received_segment","last_uploaded_segment","last_uploaded_unix","lag_segments"?,"error"?}` |

Правила: transient-сбой операции — статус с `error`, ретраи тиками
(t02/t03); permanent-отказ — фиксация причины, алерт панели. `DELETING` —
транзитная фаза ретенции (t06; запрет удалять последний валидный полный
— guard t06). Deprovisioning кластера (D2, arch/14 §5 B) чистит
etcd-префикс `/pgworker/backups/<C>/` тем же `del --prefix`; объекты S3
НЕ удаляются автоматически — данные дороже места: префикс S3 без
etcd-владельца = orphan, его видит супервизор (t07: алерт + политика
возраста/ручной разбор — «судьба бэкапов» из roadmap t07), а
восстановление удалённого кластера из S3-объектов — runbook t05
(симметрия R4 arch/14: воркер не уничтожает потенциально ценные данные
автоматикой).

### 3.5. Источник, HA-контур и ресурсные лимиты

- **Реплика-источник**: штатно полный бэкап не трогает мастер (запись,
  переезды). Fallback на мастер — журнал-факт (`role=master`) + I/O
  одной операции (не потоковой). Смерть источника посреди бэкапа →
  джоб убивается, статус FAILED, переснятие (окно/алерты — t02).
- **Слот на мастере**: WAL копится, пока агент не принял; потолок —
  `max_slot_wal_keep_size` (уже в каноне нод, arch/14 §2.1 P3/P4):
  смерть агента → WAL в пределах лимита → по исчерпании слот
  инвалидируется PG → цепочка рвётся → алерт t03 + переснятие полного
  (t07). Диск мастера переполнить нельзя (лимит уже действует).
- **Лимиты джоба/агента**: `PgWorker:Backups:Agent { Cpu, Mem }` →
  `HostConfig.NanoCPUs/Memory` (образец: request_* нод, arch/14 §2.4
  п.4); staging volume — ephemeral с квотой `Staging:QuotaBytes`
  (guard «нет места» → FAILED + алерт, t02).
- **Patroni/HA не затрагивается**: бэкапы — сторонние клиенты нод;
  никаких изменений в конфиги нод/HA-контура подсистема не вносит.

### 3.6. Секреты

- **Per-install, env воркера** (не etcd, не git): `PGW_BACKUP_S3_ENDPOINT`,
  `PGW_BACKUP_S3_REGION` (опц., MinIO не требует), `PGW_BACKUP_S3_BUCKET`,
  `PGW_BACKUP_S3_ACCESS_KEY`, `PGW_BACKUP_S3_SECRET_KEY` —
  конфиг-биндинг `PgWorker:Backups:S3` (группа транспортных секретов
  arch/14 §4 п.3: S3 — та же «внешняя граница», etcd-креды для него не
  заводим). Передача агенту/джобу — env контейнера (t02/t03).
- **Per-cluster БД-роль** `backup_exec` (LOGIN + REPLICATION — для
  `pg_basebackup` и `pg_receivewal`; отдельная от bucket_mover — у
  mover иная зона доверия/ротации): пароль per-cluster
  `/clusters/<C>/backup_password` — канон по образцу t02-секретов
  (ensure put-if-absent, ротация в общем тикете §9.8, 32 симв
  `[A-Za-z0-9]`). Реализация ensure/ротации — t02 (в каркас t01 —
  только канон).

### 3.7. Карта задач (канон §«Дальше»)

| Задача | Что реализует из канона |
|---|---|
| t02-backup-full-daily | джоб полного бэкапа, планировщик, статусы/ретраи, суточный алерт, роль/ensure `backup_exec` |
| t03-backup-wal-stream | WAL-агент, слот, загрузка сегментов, контроль непрерывности, алерты |
| t04-backup-verify | `pg_verifybackup` + полнота WAL-цепочки, статусы verify |
| t05-backup-restore | восстановление полного+WAL (PITR), runbook-сценарии |
| t06-backup-retention | GFS-ретенция, чистка WAL, квоты/пороги, защита последнего валидного |
| t07-backup-supervisor | reconcile S3↔etcd, сироты, reconnect агента, рестарт-устойчивость |

## 4. Структура и компоненты (отражение в коде/стенде)

### 4.1. arch/ (фаза 1 — до кода)

- **Новый `arch/19-backups.md`**: канон по §3 этого spec (разделы:
  роль/границы, механика полных+WAL, layout S3, контракт etcd, HA+лимиты,
  секреты, процессы-заготовки t02–t07, конфигурация, риски).
- **`arch/README.md`**: указатель — строка `19-backups.md` в структуру и
  список «Дальше».
- **`arch/14-pgworker.md`**: §границ (что НЕ входит) — ссылка на
  arch/19 (бэкапы — отдельная подсистема с хозяином PgWorker).
- **`arch/adminpanel/02-etcd-contract.md`**: §2.3.1 — панель читает
  префикс `/pgworker/backups/` (минимальная правка; UI/алерты — t02+).
- **`arch/roadmap/backup.md`**: шапка — «канон появился:
  [../19-backups.md]» (было «появится с t01 (будущий 17)»).

### 4.2. PgWorker — каркас (фаза 2)

- **`BackupsOptions`** (PgWorker.Core, паттерн существующих Options):
  `Enabled=false`, `S3 { Endpoint, Region=null, Bucket, AccessKey,
  SecretKey, PathStyle=true }`, `Policy { Retention { Days=7, Weeks=4,
  Months=6 }, FullMaxAgeSec=86400, VerifyOnCreate=true }`, `Staging {
  Dir="/backup-staging", QuotaBytes }`, `Agent { Cpu, Mem }`.
  Валидация старта: `Enabled=true` при пустых S3-полях — fail-fast
  (образец TLS-конфигов §2.2.1 arch/14).
- **PgWorker.Etcd — модель/парсер**: `ClusterBackups { Policy,
  Shards: map<X, ShardBackups { Full: IReadOnlyList<FullBackupState>,
  Wal: WalStreamState? }> }` — парсинг префикса `/pgworker/backups/`
  по §3.4: неизвестные ключи/поля игнорируются (обратная
  совместимость, образец снапшот-парсеров); malformed-значения —
  пропуск с диагностику, не исключение (панельная толерантность).
  Подключение к снапшоту воркера — чтение без изменений поведения.
- **Юнит-тесты** (PgWorker.UnitTests): options-валидация (fail-fast),
  парсер (полный набор ключей, отсутствующие опциональные поля,
  malformed, неизвестный ключ — игнор). Комментарии тестов — AAA.

### 4.3. Стенд и deploy (фаза 3)

- **dev-stand**: сервис `as-minio` (MinIO) в `dev-stand/compose.yaml`
  (входит в полный стенд 00-up.sh): порты фиксированные стенда
  (`9000` API / `9001` консоль — не пересекаются с существующими),
  volume `as-minio-data`, healthcheck. Креды стендовые — дефолты
  compose (`minioadmin`/`minioadmin`; только локальный стенд).
- **deploy**: `deploy/.env.example` — блок `PGW_BACKUP_S3_*`
  (закомментирован; включается с t02), `deploy/docker-compose.yml` —
  проброс env в контейнер воркера (по образцу существующих секретов).
- Стендовый bucket `pgworker-backups`: инициализация — сид-шаг 00-up.sh
  (`mc mb` против as-minio; канонический MinIO-путь, идемпотентен).

### 4.4. Мерж-гейт (roadmap)

Тем же мерж-коммитом: удалить пункт `t01-backup-canon` из
`arch/roadmap/backup.md` И снять `← t01-backup-canon` из зависимостей
t02–t07 (правила `arch/roadmap/README.md`).

## 5. Фазы (порядок исполнения)

1. **arch/**: `arch/19-backups.md` + указатели (README, 14, adminpanel/02
   §2.3.1, roadmap-шапка) — весь контракт до кода.
2. **Каркас PgWorker**: BackupsOptions (+валидация), модель/парсер
   `/pgworker/backups/*` (+юнит-тесты); wiring опций в App — без
   потребителей.
3. **Стенд/deploy**: as-minio в dev-stand, env-блоки deploy, README-заметка.
4. **Проверки**: юниты PgWorker зелёные; E2E-маркер
   `Scale_AddEmptyShard` на свежем Release (трогается `PgWorker.Etcd`/
   `App` — гейт AGENTS.md обязательный); поведение воркера не изменилось
   (`Enabled=false`).
5. **Мерж-гейт**: roadmap-правки (§4.4).

## 6. Ограничения

- t01 НЕ реализует: джобы/агенты/образ `pgworker-backup`, слоты, S3-
  загрузки, ретенцию, restore, UI панели, API-эндпоинты бэкапов — всё
  это t02–t07 по канону.
- Ноды (Spilo) не меняются: без `archive_command`, без WAL-G-env, без
  S3-кредов в нодах (решение «базовые утилиты + воркер»).
- S3-клиент в воркере (загрузка/скачивание) — t02/t03; t01 не тянет
  SDK-пакеты (только опции-строки).
- WAL-G/pgBackRest — вне канона (не встраиваем); их использование
  оператором параллельно — не поддерживается и не смешивается.
- Минio в тестах t02+ — testcontainers с динамическими портами (правила
  AGENTS.md: без хардкодов, зачистка после серий); BrokerBootSec-аналог
  ≤100 с.
- .NET 10, `TreatWarningsAsErrors=true`, centralized packages
  (`Directory.Packages.props`).

## 7. Риски и их закрытие

| Риск | Закрытие |
|---|---|
| Канон зафиксирует детали, которые t02–t07 вскрыть иначе (S3-стриминг больших баз, partial-сегменты) | arch/19 — живой документ: корректировки тем же dev-flow через правку канона; spec-скелет задаёт рамку, не микрошаги |
| MinIO — ещё один сервис стенда (порты/ресурсы/зачистка) | фиксированные порты только стенда (не тестов); testcontainers — в t02+ с динамическими портами |
| S3-креды per-install в env — компрометация = доступ ко ВСЕМ бэкапам установки | env-секреты вне git/etcd (существующий контур §4 arch/14); отдельный bucket/креды per-install; ротация — перегенерация пакета (как TLS) |
| Слот `pg_receivewal` при смерти агента копит WAL на мастере | потолок `max_slot_wal_keep_size` уже в каноне нод (P3/P4) — диск не переполнится; инвалидция слота → алерт t03 → переснятие (t07) |
| Длинные имена `<C>-<X>` ломают имена слота/контейнера | слот — sha1-усечение (§3.2); имена контейнеров — те же ограничения, что pgw-ноды (существующая практика) |
| Превышение staging-квоты джоба («нет места» — частый сбой бэкапов) | `Staging:QuotaBytes` + guard до старта джоба (реализация t02; параметр — в каркасе t01) |
| Расхождение spec↔arch при будущих правках | arch-правки — всегда первой фазой; ревью plan↔spec по чек-листам dev-flow |

## 8. Критерии приёмки

1. `arch/19-backups.md` существует и покрывает все темы roadmap t01:
   механика (полные + WAL), layout хранилища S3, контракт
   `/pgworker/backups/*` (полные/WAL-цепочки/статусы-ошибки/ретенция
   per-cluster), источник (реплика/fallback), HA-контур и ресурсные
   лимиты, секреты; решения §1 зафиксированы (S3/MinIO, базовые
   утилиты, реплика-источник, pg_receivewal-агент).
2. Указатели согласованы: `arch/README.md` (структура + «Дальше»),
   arch/14 (границы → arch/19), adminpanel/02 §2.3.1 (панель читает
   префикс), roadmap/backup.md шапка ссылается на arch/19.
3. Каркас: `PgWorker:Backups` options с валидацией fail-fast;
   парсер `/pgworker/backups/*` в `PgWorker.Etcd` с юнит-тестами
   (AAA-комментарии) — зелёные.
4. Dev-стенд поднимается с `as-minio`; `deploy/.env.example` содержит
   блок `PGW_BACKUP_S3_*`.
5. Поведение воркера не изменилось: `Enabled=false` по умолчанию,
   существующие юниты/интеграция PgWorker зелёные; E2E-маркер
   `Scale_AddEmptyShard` на свежем Release — зелёный; после серий —
   docker-зачистка (контейнеры + сети).
6. Мерж-коммит удалил `t01-backup-canon` из `arch/roadmap/backup.md`
   (пункт и `←`-зависимости t02–t07).
