# t33-s3-node-failover — spec

Отказ одной ноды S-кластера не ломает потребителей хранилища: S3-клиент при
недоступности endpoint'а переходит к следующей ноде (перебор endpoints до
первого успеха). Задача роадмапа
[`arch/roadmap/s3.md`](../../../arch/roadmap/s3.md), пункт `t33-s3-node-failover`.

## 1. Цель

Инвентаризировать ВСЕХ S3-клиентов системы (бэкап-джобы PgWorker.Backups —
full/WAL/restore/verify/drill, etcd-export sink, панель, S3Worker) и закрыть
перебором единственный подтверждённый пробел: **выгрузка слепков etcd
(`EtcdSnapshotSink`) ходит в один фиксированный endpoint** env-пары
`PgWorker:Snapshots:Export:S3` — отказ этой ноды останавливает выкладку слепков
до её возврата.

Результат задачи:

1. Инвентаризационная таблица (ниже, §3) — зафиксированный факт «у кого
   перебор есть, у кого нет, кому не нужен».
2. Добивка перебора в etcd-export sink: настройка целей становится списком
   `PGW_SNAPSHOTS_S3_ENDPOINTS` (запятая; одиночный адрес — частный случай),
   перебор — каноническим `PoolBackupS3` (arch/19 §5), без изменения бюджетов
   и статус-семантики sink.
3. Прочие клиенты — БЕЗ изменений: перебор уже реализован (или не нужен по
   смыслу адресных операций).

Non-goals (вне задачи):

- breaker-окно в панельном `S3StorageClient` (канон adminpanel/02 §2.5
  требует перебор «с первого доступного» — есть; breaker каноном панели не
  предписан);
- изменения в S3Worker (его вызовы адресные: health/join/bucket/probe
  КОНКРЕТНОЙ ноды — перебор менял бы смысл операции);
- внешние S3-цели `external` вне `/s3/clusters/` (roadmap: s3-external-targets);
- mesh-репликация между целями слепочной пары (если цели — ноды ownS3-кластера,
  их mesh — зона S3Worker по `links`; перебор слепков — только транспортная
  страховка);
- etcdworker и спец-ключ `S3W_ETCD_BACKUP_*` (arch/23 §6) — отдельная
  сущность, слепки PgWorker не касаются.

## 2. Принципы

- **arch-first**: контракт слепочной пары описан в arch/14 §6/§8 и arch/19 §7(в)
  — правится ПЕРВОЙ фазой, затем код зеркалит.
- **Канон перебора — arch/19 §5 / arch/23 §7**: запись — перебор «любой
  доступный» с per-попыточным таймаутом («быстрый отказ/успех, не молчание» —
  R3), транспортный отказ цели исключается из перебора на breaker-окно 45 с,
  NotFound — не отказ; инвентарь-листинги — первая доступная нода (лаг mesh
  допускается, повторные проходы досматривают).
- **Переиспользование, а не новый класс**: носитель перебора — существующий
  `PoolBackupS3` (PgWorker.Backups) — он уже реализует канон и интерфейс
  `IBackupS3`, который принимает sink.
- **Бюджеты не меняются**: общий бюджет попытки `TimeoutSec=30`, пер-попыточный
  таймаут клиента 10 с (дефолт `BackupS3`), breaker-окно 45 с
  (`PoolBackupS3.DefaultBreakerWindow`) — значения прежние (правило проекта:
  бюджеты без согласования не меняются).
- **Статус-семантика sink неизменна**: любая неудача выгрузки — статус-ключ
  `/pgworker/etcd-snapshots` `state=FAILED` + `error`, догоняет доводка
  `CatchUpAsync` (SnapshotLoop); новых маркеров/алертов/ключей НЕ вводится.
- **Источник правды о DR-слепках — НЕ etcd**: список целей слепков живёт в
  env-паре per-install (arch/19 §7(в)); резолв целей из `/s3/clusters/` для
  слепков НЕ вводится (chicken-and-egg: пул объявлен в восстанавливаемом etcd).

## 3. Инвентаризация S3-клиентов (факт кода, работа выполнена)

| # | Потребитель | Код | Перебор нод | Вердикт |
|---|---|---|---|---|
| 1 | Стрим WAL приёмника | `WalReceiverLoop` → `PoolBackupS3` (write-список роли, read — все ноды; `PutAttemptTimeout` 15 с) | да | не трогать |
| 2 | Полный бэкап (upload) | `WalReceiver/Operations/FullBackupOperation` → `PoolBackupS3` (`FileAttemptTimeout` 300 с) | да | не трогать |
| 3 | Verify (скачивание) | `WalReceiver/Operations/VerifyOperation` → `PoolBackupS3` (union-листинг) | да | не трогать |
| 4 | Restore/drill (скачивание) | `WalReceiver/Operations/RestoreOperation` → `PoolBackupS3` (union) | да | не трогать |
| 5 | fetch-wal (`restore_command`) | `WalReceiver/Operations/FetchWalCommand` — перебор read-списка до найденного, дедлайн вызова 45 с, ретраи 3×2 с | да | не трогать |
| 6 | Хвост S3 (возобновление стрима) | `WalReceiver/S3TailResolver` ← `IBackupS3` (PoolBackupS3) | да | не трогать |
| 7 | Воркер: контроль цепочки / verify-полнота / ретенция / sweeper / глобальные сверки | `BackupS3Factory` → `PoolBackupS3`, кэш per-контекст (arch/19 §5 «Кэш пул-клиентов») | да | не трогать |
| 8 | **etcd-export sink** | `PgWorker.App/Program.cs` → `new BackupS3(EtcdSnapshotSinkFactory.Create(opts))` — ОДИН endpoint | **НЕТ** | **ДОБИВКА (эта задача)** |
| 9 | Панель, грань «Хранилище бэкапов» | `AdminPanel.Probes/S3/S3StorageClient` — `AnyTargetAsync`/`ProbeHealthAsync`, цели из etcd-снапшота S3-домена | да (канон adminpanel/02 §2.5) | не трогать |
| 10 | S3Worker | `S3Worker.Core/OwnS3/OwnS3NodeClient` — health/bucket/join/denylist/probe КОНКРЕТНОЙ ноды по `host:port` из portalloc | адресные операции — перебор меняет смысл | не трогать |

Пробел ровно один — строка 8.

## 4. Структура/компоненты (что меняется)

### 4.1. arch (ПЕРВОЙ фазой)

- `arch/14-pgworker.md`
  - §6 «Экспорт слепков в S3»: цель выгрузки — СПИСОК endpoints env-пары
    (`env PGW_SNAPSHOTS_S3_ENDPOINTS`, запятая; одиночный адрес — частный
    случай); перебор целей до первого доступного по канону arch/19 §5
    (per-попыточный таймаут, breaker-окно, «быстрый отказ/успех»); bucket,
    креды, Region, CA — одни на все цели пары; отказ всех целей —
    transient (FAILED статуса + доводка CatchUp).
  - §8 конфиг-блок: `S3 { Endpoints, Bucket, AccessKey, SecretKey, Region?,
    CaBundlePath? }` (поле `Endpoint` переименовано во множественное число).
- `arch/19-backups.md` §7(в): перечисление env пары —
  `PGW_SNAPSHOTS_S3_ENDPOINTS/BUCKET/ACCESS_KEY/SECRET_KEY` + фраза о
  переборе целей до первого доступного (§5). Исторических пометок не
  оставлять (правило «текущее и планируемое»).

### 4.2. Код

- `src/PgWorker.App/Options.cs` (`SnapshotExportS3Options`):
  - поле `Endpoint: string` → `Endpoints: string` — запятая-список ПОЛНЫХ URI
    (`http://h1:9000,http://h2:9000`; схема обязательна — значение идёт в
    `ServiceURL`, как сегодня);
  - `SnapshotExportOptions.IsValid()`: `Enabled=true` требует после сплита
    (','; `RemoveEmptyEntries|TrimEntries` — прецедент `FetchWalCommand.ParseEnv`)
    ≥1 непустой URI; остальные проверки без изменений;
  - doc-комментарии — по новому формату.
- `src/PgWorker.App/EtcdSnapshotSinkFactory.cs`: `Create(opts)` возвращает
  `PoolBackupS3` (не `S3ClientOptions`): write=read=разобранный список
  `Endpoints`; per-цель — `BackupS3(S3ClientOptions(uri, bucket, access,
  secret, Region, CaBundlePath))` без `requestTimeout` (дефолт 10 с, как
  сегодня); breaker — дефолт `DefaultBreakerWindow` 45 с.
- `src/PgWorker.App/Program.cs` (сборка sink, ~строка 377): вместо
  `new BackupS3(EtcdSnapshotSinkFactory.Create(opts))` —
  `EtcdSnapshotSinkFactory.Create(opts)` (результат — `IBackupS3`;
  `PoolBackupS3` его реализует). Sink остаётся синглтоном на процесс —
  breaker-стейт переживает попытки выгрузки. Dispose не вводится (паттерн
  прежний: клиент живёт до конца процесса).
- `EtcdSnapshotSink` — БЕЗ изменений (принимает `IBackS3`/`IBackupS3`,
  перебор получает через реализацию).

### 4.3. deploy

- `deploy/docker-compose.yml`: маппинг
  `PgWorker__Snapshots__Export__S3__Endpoints: ${PGW_SNAPSHOTS_S3_ENDPOINTS:-}`
  (было `...S3__Endpoint: ${PGW_SNAPSHOTS_S3_ENDPOINT:-}`).
- `deploy/.env.example`: строка-пример
  `#PGW_SNAPSHOTS_S3_ENDPOINTS=http://owns3:9000,http://owns3-2:9000` +
  комментарий о переборе до первого живого.

### 4.4. Семантика перебора слепков (следует из PoolBackupS3, фиксируется)

- put (`.db`, `.meta.json`) — перебор целей С ПЕРВОЙ по списку, первый успех
  выигрывает; транспортный отказ → следующая цель; NotFound для put не
  возникает.
- list (ретенция N пар) — первая доступная цель (лаг mesh допускается —
  повторные проходы досматривают; guard «≥1 полной пары» зашит в
  `EtcdExportRetention.Select`: `ids.Take(count - retentionObjects)` при
  `retentionObjects ≥ 1` — новейшая пара неуязвима).
- delete (ретенция) — write-перебор (первая доступная; mesh/tombstone —
  зона целей, не sink).
- breaker: транспортный отказ цели исключает её из перебора на 45 с; окно
  живёт в синглтоне — переживает попытки.
- исчерпание списка → `Result.Failed` с текстом `s3-pool-exhausted: …` →
  sink ставит статус `FAILED` + `error` (текст причины видно оператору),
  `CatchUpAsync` догоняет следующим тиком — штатная семантика; НОВЫХ
  панельных маркеров/алертов НЕ вводится (алертная грань слепков —
  state=FAILED/stale статуса `/pgworker/etcd-snapshots`, без изменений).
- бюджет попытки `TimeoutSec=30` общий: при 10 с per-попытку покрывает до
  двух отказных целей + успех; исчерпание бюджета — тот же transient-FAILED
  → доводка (короткий сон `RetryIntervalSec`, RPO-окно слепков закрывается
  минутами — существующее поведение).

## 5. Фазы

1. **arch**: правки arch/14 §6+§8, arch/19 §7(в) (§4.1).
2. **Код**: Options/фабрика/Program + deploy-шаблоны (§4.2–4.3). Сборка
   `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` —
   без warnings (TreatWarningsAsErrors).
3. **Тесты юнит+интеграция** (§6.1–6.2) — зелёные.
4. **E2E + гейт** (§6.3): обновление env-имён в существующих E2E, новый
   сценарий отказа цели, гейт `Scale_AddEmptyShard`.

## 6. Тесты (критерии приёмки по уровням)

### 6.1. Юнит (PgWorker.UnitTests)

- `App/SnapshotSinkFactoryTests`: разбор `Endpoints` — список «a,b» →
  PoolBackupS3 с двумя целями (write=read), одиночный URI → одна цель,
  пробелы/пустые элементы отбрасываются; bucket/креды/Region/CaBundlePath
  пары применяются ко всем целям (AAA-комментарии по канону).
- `App/SnapshotExportOptionsTests`: `IsValid` — `Endpoints="http://a:9000,http://b:9000"`
  валиден; `Endpoints=" , "` и `""` при `Enabled=true` — невалиден; прежние
  кейсы диапазонов — без изменений.
- `App/BackupsOptionsValidationTests` (строка ~48): обновить на новое поле.

### 6.2. Интеграция (PgWorker.IntegrationTests, `EtcdSnapshotSinkTests` — OwnEtcd+OwnS3 per-Fact)

- **Failover**: цель 1 — закрытый порт (прецедент `BadS3Runtime` +
  `E2eFixture.FreePort()`), цель 2 — живая ownS3 → `ExportAsync` OK; пара
  `.db`+`.meta.json` реально лежит на живой ноде (чтение SeedClient'ом),
  статус-ключ OK.
- **Исчерпание → доводка**: обе цели закрытые порты → `ExportAsync` Failed,
  статус `FAILED` (поля последнего успеха сохранены) — прецедент расширить
  вторым узлом (существующий кейс «транзиент → FAILED → доводка»).

### 6.3. E2E (PgWorker.E2eTests)

- Обновить env-имя в существующих: `E2eEtcdSnapshotExportScenarios.cs` (~42),
  `E2eBackupPoolScenarios.cs` (~921, PoolSnapshotsOwnPairScenarios) →
  `PgWorker__Snapshots__Export__S3__Endpoints`.
- **Новый сценарный класс** (в `E2eEtcdSnapshotExportScenarios.cs`, фикстура
  по прецеденту `E2eEtcdSnapshotExportScenariosFx`: `WithS3Node=true`, свой
  slug): старт второй ownS3-ноды (`Fx.StartExtraS3NodeAsync`, прецедент
  E2eBackupPoolScenarios AC4/AC6); env воркера
  `...Endpoints = http://e2e-owns3:9000,http://<alias2>:9000`; дождаться
  OK-выгрузки → `docker stop` первой ноды → дождаться СЛЕДУЮЩЕГО слепка:
  статус-ключ `/pgworker/etcd-snapshots` снова OK, свежая пара видна на
  второй ноде (mc через контейнер-помощник — прецедент RunMc). Полный
  teardown при любом исходе + ассерт чистоты (канон docs/e2e-isolation.md);
  телеметрия — по docs/e2e-launch.md.
- **Мерж-гейт** (AGENTS.md, задача трогает PgWorker.App): статическая
  подготовка ОДНИМ процессом ДО тестов
  (`dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`), затем
  ОТДЕЛЬНО сборка `dotnet build src/PgWorker.slnx -c Release` и тесты с
  `--no-build`: минимум
  `--filter FullyQualifiedName~Scale_AddEmptyShard`; плюс прогон обновлённых
  классов (E2eEtcdSnapshotExportScenarios, PoolSnapshotsOwnPairScenarios) и
  нового сценария. Зачистка контейнеров/сетей между сериями — по канону.

## 7. Ограничения

- P/Invoke запрещён; только управляемый .NET API (AWSSDK — как есть).
- Таймауты/бюджеты — без изменений значений (§2).
- Язык: документация/комментарии — русский, идентификаторы — английские.
- Docker-E2E: динамические порты, самоочистка, `[PHASE]`-метки — каноны
  AGENTS.md/docs/e2e-*.md.
- Тренеровочные/битые прогоны не перезапускаются без разбора логов
  (AGENTS.md «Телеметрия E2E»).

## 8. Принятые решения (закрытые развилки)

| # | Развилка | Решение |
|---|---|---|
| 1 | Область добивки | Только etcd-export sink; все прочие клиенты (§3) — без изменений: перебор уже есть либо операции адресные по смыслу (S3Worker). Вытекает из инвентаризации + буквы задачи. |
| 2 | Имя настройки списка целей | `PGW_SNAPSHOTS_S3_ENDPOINTS` (множественное; конфиг-поле `Endpoints`), значение — запятая-список URI; СТАРОЕ имя/поле `ENDPOINT` УДАЛЯЕТСЯ (решение пользователя 2026-10-30). Забытый деплой ловится fail-fast старта (Enabled=true + пустой список пары) — молчаливого отказа нет; `.env.example`/compose обновляются задачей. |
| 3 | Носитель перебора | Существующий `PoolBackupS3` (write=read=список env), синглтон на процесс (breaker переживает попытки). Новый класс перебора НЕ пишется. |
| 4 | Формат значения | Полные URI с схемой через запятую; сплит ',' + TrimEntries/RemoveEmptyEntries (прецедент FetchWalCommand.ParseEnv); одиночный URI — частный случай. |
| 5 | Бюджеты | Не меняются: попытка 30 с, per-попытка 10 с (дефолт BackupS3), breaker 45 с. Исчерпание бюджета — штатный transient-FAILED → CatchUp. |
| 6 | Статус/алерты sink | Без изменений: FAILED + error → CatchUp; текст `s3-pool-exhausted` из PoolBackupS3 попадает в `error` как причина (информативен), новых панельных правил НЕ вводится. |
| 7 | Креды/CA/bucket/Region | Одни на ВСЕ цели пары (канон per-install пары; смесь целей с разными кредами не поддерживается по построению). |
| 8 | Резолв целей из etcd | НЕ вводится: источник правды о DR-слепках не живёт в восстанавливаемом etcd (arch/19 §7(в)). |
| 9 | Панель | Не трогается: перебор есть (adminpanel/02 §2.5), breaker каноном панели не предписан. |
| 10 | S3Worker | Не трогается: health/join/bucket/probe — адресные операции к конкретной ноде. |
| 11 | E2E-рамка | Новый сценарный класс рядом с E2eEtcdSnapshotExportScenarios (свой slug-фикстура, вторая нода по прецеденту StartExtraS3NodeAsync) + обновление env-имён в двух существующих файлах + мерж-гейт Scale_AddEmptyShard с E2ePrepare. |
| 12 | Риск лага mesh + FirstAlive-ретенция слепков | Принят: guard «≥1 полной пары» зашит в EtcdExportRetention; худший случай — удаление старой пары, которая была бы N-й по полному листингу; повторные проходы досматривают (канон инвентарь-листингов §5). |

## 9. Критерии приёмки

1. Инвентаризация §3 подтверждена кодом (таблица соответствует факту;
   исполнитель не добавляет клиентов в область задачи).
2. arch/14 §6+§8 и arch/19 §7(в) обновлены ПЕРВОЙ фазой; код зеркалит
   контракт: `PGW_SNAPSHOTS_S3_ENDPOINTS` (список), перебор, fail-fast
   валидация.
3. Юнит- и интеграционные тесты §6.1–6.2 зелёные (failover на реальном
   ownS3: выкладка уходит на живую цель; исчерпание → FAILED → доводка).
4. E2E §6.3: отказ первой цели — следующий слепок OK на второй ноде;
   существующие E2E со старым env-именем обновлены и зелёные; гейт
   (E2ePrepare → сборка → Scale_AddEmptyShard `--no-build`) пройден; зачистка
   серий по канону.
5. Бюджеты/значения таймаутов не изменены (§2, решение 5).
6. deploy/.env.example и docker-compose.yml несут новое имя и пример списка.
7. Скипов в штатном прогоне нет; документация — русская, идентификаторы —
   английские.
