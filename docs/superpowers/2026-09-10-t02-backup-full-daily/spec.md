# Spec: полные суточные бэкапы шардов (t02-backup-full-daily)

Задача roadmap: [`t02-backup-full-daily`](../../../arch/roadmap/backup.md) —
полные бэкапы БД каждого шарда раз в сутки (`pg_basebackup`): планировщик в
PgWorker, фиксация бэкапа и его статуса в etcd, повтор/пересоздание при
неудаче (сетевой сбой, нет места, мастер сменился посреди бэкапа), алерт в
панель если к концу окна суток валидного полного бэкапа нет. Канон —
[arch/19-backups.md](../../../arch/19-backups.md) (обновлён этой задачей,
§2/§4/§5/§6/§9/§10); контекст сервиса —
[arch/14-pgworker.md](../../../arch/14-pgworker.md). Каркас (options,
etcd-парсер, MinIO-стенд, deploy env) — t01, уже в main.

## 1. Цель

1. **Планировщик полных бэкапов** в PgWorker: rolling-правило — тик воркера
   для каждого шарда Active-кластера с `dsn` проверяет возраст последнего
   COMPLETED полного бэкапа; старше `full_max_age_sec` (политика кластера,
   дефолт 86400) → новый полный бэкап.
2. **Джоб полного бэкапа**: ephemeral docker-контейнер
   `pgw-backup-full-<C>-<X>-<id>` (новый образ `pgworker-backup`:
   postgres-клиенты + `mc`), запускает и супервизирует PgWorker; источник —
   реплика (sync-standby), fallback на мастер; стриминг файлов 1:1 в S3
   (MinIO стенда / любое S3 прода) — из джоба, воркер без S3 SDK.
3. **Статусы в etcd** `/pgworker/backups/<C>/<X>/full/<id>`: жизненный цикл
   PLANNED → RUNNING → UPLOADING → COMPLETED/FAILED пишет ТОЛЬКО воркер
   (по stdout-маркерам и exit-коду джоба); FAILED → переснятие новым id с
   бэкоффом, без лимита попыток.
4. **Роль `backup_exec`** (LOGIN + REPLICATION): per-cluster пароль
   `/clusters/<C>/backup_password` (ensure put-if-absent по образцу
   P1.5), роль на мастере каждого шарда идемпотентно, включение в общий
   ротатор секретов (arch/14 §5 I).
5. **Суточный алерт панели** `backup-full-stale` per-shard: панель читает
   префикс `/pgworker/backups/` и вычисляет возраст последнего COMPLETED;
   воркер алерты не пишет.

### Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Расписание планировщика | Rolling по возрасту последнего COMPLETED (`full_max_age_sec`), без фиксированного окна — самовосстанавливается после сбоев |
| Кто стримит в S3 | Джоб-контейнер (`mc` в образе `pgworker-backup`); воркер без S3 SDK в t02 (S3-клиент воркера — t04/t07) |
| Суточный алерт | Панель вычисляет по снапшоту префикса (парсер + правило AlertEngine), по образцу остальных 19 правил |
| Политика при неудаче | Бэкофф без лимита попыток: `min(BaseSec·2^(n−1), MaxSec)`, n = FAILED с последнего COMPLETED; порог «долго без валидного» — суточный алерт |
| API приёма policy в t02 | Нет: policy-ключ читается (ручная запись etcdctl), дефолт — `PgWorker:Backups:Policy`; API-эндпоинт — t06 |

## 2. Принципы

1. **arch-first**: контракт детализирован в `arch/19-backups.md` ПЕРВОЙ
   фазой (уже сделано в этой ветке: §2 пайплайн/stdout-протокол/WAL-
   дублирование/планировщик, §4 `wal_start_segment` опционален + алерт
   `backup-full-stale`, §5 дублирование WAL, §6 квота staging, §9
   `Job.Image`/`Retry`, §10 риски; строка adminpanel/02 §2.3.1) — код
   следует канону, не наоборот.
2. **PgWorker — хозяин бэкапов** (arch/19 §1): создаёт, ретраит/пересоздаёт,
   сигнализирует; ноды (Spilo) бэкапами не управляют. Префикс
   `/pgworker/backups/*` и `/clusters/<C>/backup_password` пишет ТОЛЬКО
   PgWorker под клэймом `<C>` (как остальные процессы кластера, arch/19 §4).
3. **Джоб — исполнитель без etcd**: контейнер знает только env и stdout;
   весь etcd-контракт — за воркером. Takeover-устойчивость: статус в etcd +
   детерминированное имя контейнера — любой инстанс продолжает супервизить.
4. **Существующие паттерны** (arch/17): идемпотентность каждого шага
   (PLANNED без контейнера → запуск; RUNNING без контейнера → FAILED),
   journal-before-manipulations (PLANNED до создания контейнера), transient
   vs permanent (сетевые сбои — ретраи; конфигурационные — FAILED с ошибкой
   в статусе), тик не блокируется на длинные операции (бэкап живёт в
   контейнере, тик только запускает/поллит).
5. **Поведение по умолчанию не меняется**: `PgWorker:Backups:Enabled=false`
   — воркер без бэкапов; новый процесс — no-op. Панель без включённой
   подсистемы (пустой префикс) алертов не даёт.
6. Тесты: docker-контейнеры с динамическими портами (MinIO — testcontainers,
   без хардкодов), зачистка контейнеров/сетей после каждой серии, таймауты
   фикстур ≤100 с; E2E на свежем Release — обязателен (тронуты
   PgWorker.App/Etcd/Provisioning).
7. Язык: документация — русский, идентификаторы — английский; тесты с
   AAA-комментариями. .NET 10, `TreatWarningsAsErrors=true`,
   centralized packages.

## 3. Архитектура

### 3.1. Планировщик BackupProcess (воркер)

Новый процесс `BackupProcess` (проект `PgWorker.Backups` — по образцу
`PgWorker.Moves`; DI-модуль, см. §4). Вызов — Active-ветка `ReconcileLoop`
через `IClusterProcesses.BackupsAsync(snap, ct)` после
`rotate-app-password` и до `repair` (короткие плановые операции — раньше;
бэкап-планировщик non-blocking: тик запускает/поллит джобы, не ждёт их
завершения). Под клэймом `<C>` (клэйм уже держит ReconcileLoop).

Тик процесса (идемпотентен, для каждого шарда Active-кластера с `dsn`):

```
G0  Enabled=false → Done (no-op); кластер не Active → skip
G1  ensure backup_password (P1.5-образец: ClusterSecretEnsurer + 4-й ключ,
    txn put-if-absent, 32 симв [A-Za-z0-9])
G2  ensure роли backup_exec на мастере каждого шарда с dsn (резолв —
    ShardEndpoints.ResolveMasterAsync; CREATE ROLE IF NOT EXISTS-эквивалент
    идемпотентным ALTER ROLE ... LOGIN REPLICATION; паттерн P2.3);
    мастер недоступен → transient: шард в этом тике skip (джоб без
    роли не запускать — упал бы на auth), следующий тик дообеспечит
G3  на каждый шард X с dsn:
      активен (PLANNED|RUNNING|UPLOADING) → супервизия (S-ветка ниже)
      due: нет COMPLETED ИЛИ age(последний COMPLETED) > full_max_age_sec
        → бэкофф-гвард: now < last_attempt + min(BaseSec·2^(n−1), MaxSec)
           (n = FAILED-записи с последнего COMPLETED; last_attempt =
            последний PLANNED/RUNNING/UPLOADING/FAILED started_unix)
           → skip (следующий тик)
        → резолв источника: sync-standby (Patroni GET /cluster: member
           state=running, role=replica, sync-статус) → нет → мастер
           (ShardEndpoints.ResolveMasterAsync); резолв не удался
           (шард недоступен) → transient: journal нет, следующий тик
        → id = YYYYMMDDHHMMSSZ UTC (коллизия в пределах шарда → -2/-3)
        → put full/<id> {state: PLANNED, node, role, started_unix}
           (journal-before-manipulations)
        → создать staging volume + контейнер джоба (хост источника,
           спецификация §3.2) → put {state: RUNNING}
S   супервизия активного (по детерминированному имени контейнера):
      контейнер running → поллинг логов (GET /containers/<id>/logs, tail):
        маркер {"phase":"uploading","wal_start_segment":…} → put
        {state: UPLOADING, wal_start_segment}
      контейнер exited:
        exit 0 + result {"ok":true,"wal_start_segment","size_bytes"} →
          put {state: COMPLETED, finished_unix, wal_start_segment,
               size_bytes, verify: PENDING если VerifyOnCreate} →
          удалить контейнер + staging volume
        exit != 0 / result {"ok":false,"error"} → put {state: FAILED,
          finished_unix, error} → удалить контейнер + volume
      контейнера нет вовсе (список включая exited) → put {state: FAILED,
        error: "container-vanished"} + удалить осиротевший staging volume
        (404 = ок) — переснятие по общему правилу G3
      transport-отказ docker (inspect/logs недоступны) → transient:
        статус не меняем, следующий тик повторит супервизию
```

Инварианты: максимум один активный (PLANNED/RUNNING/UPLOADING) полный на
шард; новые PLANNED создаются только при due; каждая попытка — новый id
(`pg_basebackup` не резюмится, FAILED-записи — история); БД-подключение
джоба — от роли `backup_exec`, источник фиксируется в статусе
(`node`, `role`).

Смерть источника/смена мастера посреди бэкапа: walsender убивается
failover'ом → `pg_basebackup` падает → джоб exited ≠ 0 → FAILED →
переснятие (новый id; источник резолвится заново — уже с нового
мастера/реплики). Отдельной специализации не требуется — общий
retry-контур.

### 3.2. Джоб полного бэкапа (образ `pgworker-backup`)

Образ `docker/PgWorker.Backup.Dockerfile`: база `postgres:18` (клиентские
утилиты: `pg_basebackup`, далее t04 — `pg_verifybackup`) + `mc`
(MinIO client, версия запинена; работает с любым S3). ENTRYPOINT — скрипт
образа (без параметров: всё через env). Никаких .NET-компонентов.

Env джоба (передаёт воркер; S3-креды per-install — arch/19 §7, группой как
воркеру):

| Env | Значение |
|---|---|
| `PGW_BK_DSN` | параметры подключения к источнику: `host=<advertised-хост> port=<pg-порт> user=backup_exec password=<per-cluster> sslmode=require` (адресация portalloc/advertised — тот же namespace, что панель, arch/19 §2) |
| `PGW_BK_ID` | id бэкапа (имя контейнера/volume — детерминизм) |
| `PGW_BK_S3_ENDPOINT/REGION/BUCKET/ACCESS_KEY/SECRET_KEY` | per-install S3-комплект (из `PgWorker:Backups:S3`) |
| `PGW_BK_PREFIX` | `<C>/<X>` (layout arch/19 §5) |
| `PGW_BK_STAGING_DIR` | путь staging-каталога внутри джоба (из `PgWorker:Backups:Staging:Dir`, дефолт `/backup-staging`) — точка монтирования staging volume |

Пайплайн скрипта (все шаги после pg_basebackup — без влияния на ноды):

1. stdout `{"phase":"basebackup"}`;
   `pg_basebackup -D <staging>/full -X stream --checkpoint=spread
   --manifest-checksums=SHA256` (plain-формат).
2. Парс `backup_label` (START WAL LOCATION) → `wal_start_segment`;
   `du` staging → `size_bytes`.
3. stdout `{"phase":"uploading","wal_start_segment":…}`;
   `mc cp --recursive staging/full/… → s3://<bucket>/<C>/<X>/full/<id>/`
   (файлы 1:1, включая `backup_manifest` и `pg_wal/`); затем закрытые
   сегменты `staging/full/pg_wal/` → `wal/<segment>` (дублирование в общий
   WAL-префикс — arch/19 §2/§5; идемпотентная перезапись).
4. stdout `{"ok":true,"wal_start_segment":…,"size_bytes":…}`; exit 0.
   Любой шаг упал → stdout `{"ok":false,"error":<однострочная причина>}`;
   exit 1 (ENOSPC staging-квоты, обрыв сети к источнику/S3 — здесь).

Контейнер: без публикации портов (клиент); `extra_hosts
host.docker.internal:host-gateway` (advertised-адресация); staging — named
volume `pgw-backup-<C>-<X>-<id>` → `Staging.Dir`; `Staging.QuotaBytes`
задан → tmpfs `size=` (жёсткая квота), null → disk volume (реактивный
ENOSPC) — arch/19 §6; лимиты `Agent { Cpu, Mem }` → NanoCPUs/Memory;
запуск на docker-хосте источника (адрес ноды достижим по advertised, оттуда
же достижим S3-endpoint — per-install).

Протокол воркер↔джоб — stdout-маркеры (arch/19 §2): воркер парсит только
свои JSON-строки, незнакомые строки лога игнорирует (толерантность);
смена формата — правкой канона.

### 3.3. Контракт etcd (изменения против каркаса t01)

Канон обновлён (см. §2 п.1); модель/парсер `PgWorker.Etcd.Parsing`
(t01) расширяются точечно:

- `wal_start_segment` опционален (null до фазы UPLOADING; обязательные
  поля статуса: `state`, `node`, `role`, `started_unix`) — правка
  `BackupsParser.TryParseFull` + фикстуры/тесты t01.
- Записи статусов теперь ПРОИЗВОДИТ воркер (сериализация модели → JSON
  канона: put `full/<id>`; поля `finished_unix`, `size_bytes`, `error`,
  `verify` — по состоянию). Пишет только держатель клэйма `<C>`.
- Чтение префикса — `ReconcileLoop.TickAsync` добавляет range
  `/pgworker/backups/` → `BackupsParser` (переиспользование t01) → вход
  `BackupProcess`; политика кластера — из снапшота (policy-ключ), нет
  ключа → дефолт `PgWorker:Backups:Policy`.
- DeprovisioningProcess (D2): дополнить — `del --prefix
  /pgworker/backups/<C>/` + удаление бегущих джоб-контейнеров
  `pgw-backup-full-<C>-*` и их staging volumes (объекты S3 НЕ удаляются —
  arch/19 §4).

Новых ключей и новых полей контракта (кроме опциональности
`wal_start_segment`) t02 НЕ вводит: бэкофф вычислим из истории ключей,
суточный алерт вычисляет панель.

### 3.4. Роль `backup_exec` и секреты

- Ensure: `ClusterSecretEnsurer` (P1.5-паттерн) расширяется четвёртым
  ключом `/clusters/<C>/backup_password` (put-if-absent txn; 32 симв
  `[A-Za-z0-9]`; удаляется с префиксом кластера D2 — уже общий `del
  --prefix`).
- Роль на мастере каждого шарда с dsn: `CREATE ROLE backup_exec LOGIN
  REPLICATION PASSWORD …` идемпотентно (по образцу P2.3: существование →
  `ALTER ROLE … PASSWORD`); реплики получают роль физической репликацией.
- Ротация: `ClusterSecretRotator` (arch/14 §5 I) включает `backup_exec`:
  R2 — идемпотентное выравнивание пароля `backup_exec` на мастере каждого
  шарда (гвард CREATE-if-absent/ALTER по P2.3 — роли может не быть при
  `Enabled=false`); R3 — txn-коммит дополнен `put backup_password=NEW`.
  Контракт заявки ротации (панель) не меняется — состав ролей — дело
  воркера.
- Джоб получает пароль через env контейнера (не в etcd-статусах, не в
  логах; статусы бэкапов паролей не содержат).

### 3.5. Панель: чтение префикса + суточный алерт

- `AdminPanel.Etcd/Parsing/BackupsParser.cs` — панельный парсер префикса
  (модель `ClusterBackupsInfo`: политика + per-shard полные; по образцу
  воркерного парсера t01 — дубли осознанные, унификация — roadmap
  t08-unify-adminpanel-duplicates). Битые значения — parseErrors, обход
  толерантный (паттерн панели).
- `SnapshotRefresher`: чтение префикса `/pgworker/backups/` → новое поле
  `EtcdSnapshot.Backups`.
- Правило `BackupFullStaleRule` (AlertEngine, по образцу `MoveStaleRule`):
  kind `backup-full-stale`, per-shard; условие — пустой префикс кластера
  (ни одного ключа) → молчим (подсистема не включена); иначе возраст
  последнего COMPLETED (`finished_unix`; COMPLETED нет вовсе → алерт
  «полного никогда не было») > `full_max_age_sec` политики кластера (нет
  policy-ключа → панельный дефолт 86400). Severity — критичный (данные
  без свежего полного = риск потери; уровень каталога алертов arch/03).
- UI-грань бэкапов, WAL-алерты — НЕ t02 (t08/t03).

### 3.6. Стенд и deploy

- Сборка образа `pgworker-backup:dev`: `docker/PgWorker.Backup.Dockerfile`;
  deploy-стенд собирает его рядом с `pgworker:dev` (compose `build` +
  00-up.sh), тег — дефолт `Job.Image`.
- `deploy/.env.example`: блок `PGW_BACKUPS_ENABLED`/`PGW_BACKUP_S3_*`
  (заложен t01) — раскомментирован для стенда (MinIO стенда:
  `http://host.docker.internal:9000`, bucket `pgworker-backups`,
  стендовые креды); прод — per-install.
- Dev-стенд (полный профиль) получает работающие суточные бэкапы «из
  коробки»: as-minio уже в стенде (t01), воркер с `Enabled=true`.

## 4. Структура и компоненты (отражение в коде)

| Место | Что |
|---|---|
| `arch/19-backups.md`, `arch/adminpanel/02-etcd-contract.md` | детализация контракта t02 (уже внесена, §2 п.1) |
| `src/PgWorker.Backups/` (новый проект, образец `PgWorker.Moves`) | `Process/BackupProcess.cs` (тик G0–G3/S), `Process/BackupPlanner.cs` (чистые решения: due/бэкофф/инвариант одного активного — юнит-тестируемы), `Job/BackupJobSpec.cs` (имена контейнера/volume, env, ContainerSpec-маппинг), `Job/BackupJobLog.cs` (парсер stdout-маркеров/result), DI-модуль |
| `src/PgWorker.Docker/Engine/` | `IDockerEngine`: + `GetContainerLogsAsync(id, tail)` (GET /containers/\<id\>/logs?tail=); docker-драйвер хоста — выбор хоста источника при создании джоба |
| `src/PgWorker.Etcd/Parsing/` | `BackupsParser`: `wal_start_segment` опционален (+тесты/фикстуры) |
| `src/PgWorker.Provisioning/` | `ClusterSecretEnsurer` (+backup_password), `ClusterSecretRotator` (+backup_exec в R2/R3), `DeprovisioningProcess` (D2: +del prefix backups, +rm джоб-контейнеров/volumes), `ShardEndpoints`: +резолв sync-standby источника (`ResolveBackupSourceAsync` по Patroni GET /cluster → fallback ResolveMasterAsync) |
| `src/PgWorker.App/` | `Options.cs`: `BackupsOptions` + `Job { Image }`, `Retry { BaseSec=300, MaxSec=3600 }` (+валидация: Enabled=true → Job.Image непуст); `ReconcileLoop`: range `/pgworker/backups/` + `IClusterProcesses.BackupsAsync`; DI-wiring |
| `src/AdminPanel.Etcd/`, `src/AdminPanel.Core/` | парсер префикса, `SnapshotRefresher` (+префикс), `EtcdSnapshot.Backups`, `BackupFullStaleRule` |
| `docker/PgWorker.Backup.Dockerfile` | образ джоба (postgres:18 + mc + скрипт) |
| `deploy/`, `dev-stand/` | сборка образа, env стенда (Enabled=true) |

## 5. Фазы (порядок исполнения)

1. **arch/** — детализация канона (выполнено в этой ветке до spec; войдёт
   первым коммитом кода).
2. **Каркас воркера**: опции `Job`/`Retry` (+валидация, юниты); парсер
   `wal_start_segment`? (+фикстуры); `GetContainerLogsAsync` в DockerEngine.
3. **BackupProcess**: planner (юниты: due/бэкофф/инвариант/опциональные
   поля) → процесс G0–G3/S (интеграционные тесты с docker) → wiring в
   ReconcileLoop.
4. **Секреты/роль**: ClusterSecretEnsurer (+backup_password), Rotator
   (+backup_exec), Deprovisioning D2 — юниты + интеграция.
5. **Образ джоба** + `BackupJobSpec`/лог-парсер (юниты парсера; сборка
   образа в deploy/стенд).
6. **Панель**: парсер префикса + SnapshotRefresher + `BackupFullStaleRule`
   (юниты правила: свежий полный / просроченный / нет COMPLETED / пустой
   префикс).
7. **Стенд/deploy**: сборка `pgworker-backup:dev`, env Enabled=true для
   стенда; проверка 00-up.sh (воркер жив, при поднятом кластере — бэкапы
   появляются, объекты в MinIO, алерт в панели при заглушенном S3).
8. **Тестовые серии с зачисткой** (AGENTS.md): юниты → интеграция (MinIO
   testcontainer, динамические порты; кейсы §7) → E2E на свежем Release
   (маркер `Scale_AddEmptyShard` — обязательный гейт; + E2E-сценарий
   полного бэкапа живого кластера) — после каждой серии docker-зачистка
   (контейнеры + `docker network prune -f`).
9. **Мерж-гейт roadmap**: удалить пункт `t02-backup-full-daily` из
   `arch/roadmap/backup.md` и снять `← t02-backup-full-daily` из t04–t07
   тем же мерж-коммитом.

## 6. Ограничения (что НЕ входит в t02)

- WAL-агент `pg_receivewal`, слоты, контроль непрерывности цепочки — t03
  (t02 пишет только закрытые сегменты набора `-X stream` в `wal/`).
- `pg_verifybackup`/статусы verify кроме PENDING — t04 (t02 проставляет
  `verify: PENDING` при `VerifyOnCreate=true`).
- Восстановление/restore — t05. Ретенция/`DELETING`/квоты хранилища/
  защита последнего валидного — t06. Reconcile S3↔etcd, сироты,
  orphan-разбор, рестарт-устойчивость сверх takeover — t07. UI-грань
  бэкапов панели — t08.
- API-эндпоинты бэкапов (policy, on-demand backup) — t06+; в t02 policy —
  только чтение ключа (ручная запись).
- Воркер НЕ получает S3 SDK (загрузка — в джобе; S3-клиент воркера —
  t04/t07).
- Ноды/HA-контур не меняются: без `archive_command`, без WAL-G-env, без
  S3-кредов в нодах; Patroni-конфиги не трогаем.
- Планировщик не делает stagger между шардами (джобы независимы, лимиты
  Agent ограничивают ресурс; отправка запросов к S3 и БД — не волна
  перегрузки: `--checkpoint=spread`); выравнивание нагрузки — при
  необходимости в t06/t07.
- Partial-сегменты, `.history`-файлы — домен t03 (t03-агент); t02 грузит
  только закрытые сегменты своего набора.

## 7. Тестирование (сценарии приёмки)

- **Юниты** (PgWorker.UnitTests): planner — due по возрасту/отсутствию
  COMPLETED, бэкофф-расчёт из истории FAILED, инвариант одного активного,
  суффикс коллизии id; лог-парсер джоба (фазы/result/мусорные строки);
  парсер etcd `wal_start_segment` опционален; опции `Job`/`Retry`;
  ClusterSecretEnsurer (+backup_password txn); панельный парсер;
  `BackupFullStaleRule` (4 случая §3.5).
- **Интеграция** (docker, MinIO testcontainer с динамическим портом,
  фикстуры кластеров PgWorker; таймауты ≤100 с):
  1. `Backup_FullDaily_Completes`: кластер + policy с малым
     `full_max_age_sec` → PLANNED→RUNNING→UPLOADING→COMPLETED в etcd;
     объекты `full/<id>/` (вкл. `backup_manifest`, `pg_wal/`) и `wal/` в
     MinIO (проверка через mc-контейнер к testcontainer-порту);
     `verify=PENDING`; staging volume удалён.
  2. `Backup_FailsOnBadS3_RetriesWithNewId`: недоступный S3-endpoint →
     FAILED с error; повторный PLANNED с новым id (бэкофф ускорен
     policy/конфигом).
  3. `Backup_Deprovision_CleansPrefix`: TO_REMOVE → префикс
     `/pgworker/backups/<C>/` пуст, джоб-контейнеры/volumes удалены.
  4. `Backup_Rotator_IncludesBackupExec`: заявка ротации → пароль
     backup_password изменён в etcd, роль принимает новый пароль
     (подключение).
- **E2E** (свежий Release; гейт AGENTS.md): маркер
  `Scale_AddEmptyShard` (обязательный минимум) + E2E-сценарий
  `Backup_FullDaily` (живой кластер E2E-фикстуры: включённые бэкапы →
  COMPLETED + объект в MinIO E2E-контура; при недоступном S3 — FAILED и
  повтор). После серий — docker-зачистка.
- **Стенд**: полный подъём 00-up.sh — бэкапы идут в as-minio, панель
  показывает алерт при искусственно сломанном S3 (проверка руками/чеком).

## 8. Риски и их закрытие

| Риск | Закрытие |
|---|---|
| stdout-протокол джоба — неявный контракт (поллинг фаз) | формат зафиксирован каноном arch/19 §2; воркер толерантен к незнакомым строкам; exit-код — истина итога (result-JSON — метаданные) |
| RUNNING-статус без контейнера после рестарта docker-хоста | FAILED `container-vanished` → переснятие (данные в S3 либо недописаны — недописанный полный не COMPLETED, восстановление из него не обещается до t04) |
| tmpfs-квота = RAM хоста (большие базы) | QuotaBytes=null → disk volume (канон §6); для больших баз квоту не задавать; ENOSPC в обоих путях → FAILED + суточный алерт |
| S3-endpoint недостижим с хоста источника | endpoint per-install обязан быть сетево-доступен отовсюду (объектное хранилище); стенд — host.docker.internal + extra_hosts (канон §10) |
| Минio testcontainers в CI — ещё один сервис | динамические порты, зачистка после серий (AGENTS.md); BrokerBootSec-аналог ≤100 с |
| mc-версия в образе (без пина = supply-chain/дрифт версий) | пин версии mc в Dockerfile; обновление — отдельной правкой |
| Дублирование WAL (`full/<id>/pg_wal` + `wal/`) — двойное место | объём = один набор `-X stream` (сегменты малы относительно базы); осознанное решение канона (verify t04 требует файлы в full, цепочка t03 — в wal/) |
| Ложный алерт при подсистеме, включённой не для всех кластеров | правило молчит при пустом префиксе кластера (канон §4) |
| Параллельные джобы всех шардов разом (первый включённый тик) | лимиты Agent { Cpu, Mem } на джоб; MaxClusters ограничивает кластеры; stagger — t06/t07 при необходимости |

## 9. Критерии приёмки

1. Воркер с `PgWorker:Backups:Enabled=true`: для каждого шарда
   Active-кластера с dsn при просроченном/отсутствующем последнем
   COMPLETED (порог `full_max_age_sec` per-cluster policy → дефолт
   конфига) создаётся полный бэкап: ключи
   `/pgworker/backups/<C>/<X>/full/<id>` проходят
   PLANNED→RUNNING→UPLOADING→COMPLETED; в COMPLETED заполнены
   `node`/`role` (реплика либо fallback мастер — факт источника),
   `started_unix`/`finished_unix`, `wal_start_segment`, `size_bytes`,
   `verify=PENDING` (при VerifyOnCreate=true).
2. Объекты в S3 соответствуют канону arch/19 §5: `full/<id>/` — файлы
   `pg_basebackup` 1:1 (вкл. `backup_manifest`, `pg_wal/`); закрытые
   сегменты набора — дублированы в `wal/`. Staging volume и контейнер
   джоба удалены после фиксации итога.
3. Неудача (обрыв источника/S3, ENOSPC, смена мастера посреди бэкапа):
   статус FAILED с `error` + `finished_unix`; переснятие — новый id через
   бэкофф `min(Retry.BaseSec·2^(n−1), Retry.MaxSec)`; одновременно на
   шарде не более одного активного полного.
4. Takeover: RUNNING-статус переживает смерть инстанса воркера (джоб в
   docker жив) — новый инстанс доводит его до COMPLETED/FAILED; исчезнувший
   контейнер → FAILED `container-vanished`.
5. Роль `backup_exec`: `/clusters/<C>/backup_password` создаётся ensure
   (put-if-absent); роль существует на мастере каждого шарда с dsn;
   ротация кластерных секретов меняет и её (txn + идемпотентное
   выравнивание пароля роли на всех шардах — гвард P2.3);
   Deprovisioning чистит `/pgworker/backups/<C>/` и
   джоб-контейнеры (объекты S3 остаются — orphan t07).
6. Панель: снапшот содержит бэкапы кластеров; правило `backup-full-stale`
   зажигается для шарда с просроченным/отсутствующим последним COMPLETED
   (порог per-cluster policy, дефолт 86400), молчит при пустом префиксе
   кластера; воркер алерты не пишет.
7. `Enabled=false` (дефолт): поведение воркера и панели не изменилось —
   существующие юниты/интеграция PgWorker/AdminPanel зелёные; новые
   тесты (§7) зелёные; E2E-маркер `Scale_AddEmptyShard` на свежем Release
   зелёный; после серий — docker-зачистка (контейнеры + сети).
8. Полный dev-стенд (00-up.sh) с включёнными бэкапами: суточные полные
   идут в as-minio (объекты видны), панель отображает статусы бэкапов в
   алертах только при реальной проблеме.
9. Мерж-коммит удалил `t02-backup-full-daily` из `arch/roadmap/backup.md`
   (пункт и `←`-зависимости t04–t07).

## Open questions

Нет — все развилки закрыты решениями пользователя (§1.1) и каноном.
