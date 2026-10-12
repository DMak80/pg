# t35-owns3-replication — план реализации (межсерверная mesh-репликация ownS3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Межсерверная mesh-репликация ownS3 (2–5 серверов, full-mesh active-active) по модели «уведомления + pull»: источник рассылает соседям лёгкие уведомления из outbox и двигает per-target курсор ACK-ом ПРИЁМА; приёмник сам фетчит данные, применяет LWW с tombstones, дыры закрывает reconcile.

**Architecture:** Новый проект `src/OwnS3.Replication` (журнал/курсоры/notifier/фетчер/reconcile/tombstones/цикл обслуживания/метрики) поверх касаний `OwnS3.Storage` (метод применения реплики через штатный xl-коммит) и `OwnS3.Protocol` (исходящая SigV4); `OwnS3.App` стыкует всё: события в 7 хендлерах после коммит-поинта, replication-грань `/replication/*` и админ-грань `/admin/replication/*` в S3Middleware, hosted-сервисы, env-трансляция. Arch-first: каноническая глава `arch/owns3/07-replication.md` коммитится раньше кода.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), Kestrel, System.Text.Json, xUnit v3 + FluentAssertions + Testcontainers, AWS SDK .NET (E2E-клиенты), `mc` (E2E-оператор).

**Spec:** [`docs/superpowers/2026-10-11-t35-owns3-replication/spec.md`](spec.md) — план аргументируется от спеки; исполнители читают ОБА документа. Входные каноны спеки: `docs/owns3-replication-requirements.md` (R1–R6), `arch/owns3/` главы 01–06, `docs/e2e-isolation.md`, `docs/e2e-launch.md`.

---

## Global Constraints

- .NET 10, `Nullable=enable`, **`TreatWarningsAsErrors=true`** (наследуется `Directory.Build.props`), пакеты — только из `Directory.Packages.props` (новых внешних пакетов НЕ добавляем).
- **Arch-first**: любое изменение контракта — сначала `arch/`, потом код (Task 0 делает канон целиком; далее код обязан ему соответствовать).
- Комментарии/документация — по-русски; идентификаторы — на английском. Тесты — с комментариями по AAA-нотации (`// Arrange`, `// Act`, `// Assert`).
- Путь записи клиента НЕ содержит синхронных сетевых вызовов: события = append+fsync в локальный журнал (мс), всё остальное — фон (R1).
- Соседей нет (env пуст, targets.json нет) → репликация неактивна, поведение байт-в-байт как текущее: события не ставятся, грань `/replication/*` отвечает 403/404 по канону, метрики репликации не активны (Existing-регресс).
- Все локальные состояния репликации — на томе в `.owns3.sys/replication/` (multi-host-правило: локальный том = кэш без durability; гарантии — от самих реплицируемых данных и reconcile).
- Тесты docker: ТОЛЬКО динамические порты (`WithPortBinding(..., assignRandomHostPort: true)` / зонд свободного порта), никаких литералов портов. Таймауты ожидания ≤30 с с промежуточными проверками; бюджет срастания 5 мин — поллингом с шагом ≤30 с. `BrokerBootSec`-аналогов >100 с нет.
- Каждый E2E-сценарий: полностью своё окружение (guid в именах сети/контейнеров/томов), полный teardown при любом исходе, ассерт чистоты, телеметрия по `docs/e2e-launch.md` (`[PHASE]`, логи в `/tmp/pgw-e2e-artifacts-<guid>/`, MarkFailed — контейнеры остановить, не удалять). Между сериями — зачистка (`docker ps -a --filter name=os3`, `docker network ls | grep os3`, при осиротевших сетях `docker network prune -f`).
- E2E-образы: publish на хосте + runtime-COPY (`docker/OwnS3.Dockerfile` уже существует, менять не нужно); локально собираемые образы в registry `192.168.0.1:5000` НЕ класть.
- Коммит в ветке t35 после каждой задачи; мерж в `main` — только по явной команде пользователя. Roadmap-тег `t35-owns3-replication` удаляется из `arch/roadmap/owns3.md` мерж-коммитом (не в этом плане отдельно).
- Рабочие команды тестов (все — из корня worktree, фон/блокировка — по базовым правилам исполняющего агента):
  - юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release`
  - WAF-интеграция: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~Api"`
  - docker-E2E: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~E2e.Replication"`
  - полный регресс OwnS3: обе серии выше без `--filter` (docker для E2e-части).
  - мерж-гейт PgWorker-стека: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`.

---

## Карта файлов (создаётся/меняется)

| Файл/каталог | Задача |
|---|---|
| `arch/owns3/07-replication.md` (новый), `arch/owns3/{01,04,05}.md`, `arch/22-owns3.md` (правки) | Task 0 |
| `src/OwnS3.Storage/Domain/DomainTypes.cs` (PutResult/CompleteResult), `Drives/{SingleXlDrive,ErasureXlDrive}.cs`, `XlObjectStore.Multipart.cs`, юниты | Task 1 |
| `src/OwnS3.Storage/ReplicaStore.cs`, `IReplicaStore.cs`, `ReplicationListEntry.cs`; `IXlDrive.CreateBucket` (optional creationDate), `BucketMetaStore`, `ErasureXlDrive.CreateBucket` | Task 2 |
| `src/OwnS3.Replication/` (новый проект): `OwnS3.Replication.csproj`, `ReplicationEvent.cs`, `ReplicationOptions.cs`, `ReplicationTargets.cs`; `src/PgWorker.slnx` | Task 3 |
| `src/OwnS3.Replication/Outbox/{OutboxEntryCodec,OutboxJournal}.cs` | Task 4 |
| `src/OwnS3.Replication/Cursors/TargetCursors.cs` | Task 5 |
| `src/OwnS3.Replication/Lww/{LwwComparator,TombstoneStore,ReplicaApplier}.cs` | Task 6 |
| `src/OwnS3.Protocol/Auth/SigV4Signer.cs` (новый), `src/OwnS3.Replication/Transport/{ReplicationDto,IReplicationTransport,ReplicationHttpClient}.cs` | Task 7 |
| `src/OwnS3.App/Access/{AccessPolicy,AccessKeyRegistry,OperationAccessMatrix}.cs` | Task 8 |
| `src/OwnS3.Replication/Inbound/{NotificationInbox,FetchQueue}.cs`; `src/OwnS3.App/Pipeline/S3Middleware.cs` (`/replication/*`), `src/OwnS3.App/Handlers/ReplicationHandlers.cs` (новый); юниты `NotificationInboxTests` | Task 9 |
| `src/OwnS3.App/Handlers/{ObjectHandlers,BucketHandlers,MultipartHandlers}.cs` (7 хендлеров), `src/OwnS3.App/Program.cs`, `OwnS3Options.cs`, `src/OwnS3.Replication/ReplicationEventSink.cs` | Task 10 |
| `src/OwnS3.Replication/Notifier/NotificationDispatcher.cs`, `ReplicationMetrics.cs` (создание: исходящие серии) | Task 11 |
| `src/OwnS3.Replication/Fetching/ObjectFetcher.cs`, `ReplicationState.cs`, `ReplicationMaintenance.cs`, `src/OwnS3.Replication/ReplicationCoordinator.cs`; `ReplicationMetrics.cs` (+fetch-серии/счётчики исходов) | Task 12 |
| `src/OwnS3.Replication/Reconcile/ReconcileRunner.cs` | Task 13 |
| `src/OwnS3.App/Pipeline/S3Middleware.cs` (админ-грань), `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs` (новой), `ReplicationMetrics.cs` (+снапшот счётчиков для статуса) | Task 14 |
| `src/OwnS3.Replication/ReplicationMetrics.cs` (доведение полного словаря/экспорт), `Program.cs` | Task 15 |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/{OwnS3MeshE2eEnvironment,MeshClients}.cs` | Task 16 |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/WriteIsolationScenarios.cs` | Task 17 |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/ConvergenceScenarios.cs` | Task 18 |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/MembershipScenarios.cs` | Task 19 |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/BoundaryScenarios.cs` | Task 20 |
| `deploy/.env.example`, `deploy/docker-compose.yml` (mesh-инструкции) | Task 21 |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/ReplicationLatencyScenarios.cs` (новый), `ConvergenceScenarios.cs` + `OwnS3MeshE2eEnvironment.cs` (правки), `arch/owns3/07-replication.md` (§10 — абзац сквозных измерений) | Task 22 |

---

## Task 0: Канон — arch/owns3/07-replication.md + правки глав 01/04/05/22 (Ф1)

**Вход:** spec утверждён; `arch/owns3/01..06` прочитаны; код ещё не тронут.

**Действие:**
- Create: `arch/owns3/07-replication.md` — каноническая глава по разделу 3 spec ДОСЛОВНО-структурированно (не пересказ, а канон):
  - §1 Модель доставки: уведомления+pull, курсор per-target по ACK приёма (`acceptedToSeqNo`), зоны ответственности источника/приёмника; принципы 2–8 spec (анти-петля, доставляй актуальное, bounded-журнал+reconcile, самолечение).
  - §2 Состав mesh: `TargetRecord {name, endpoint, accessKey, secretKey}`; файл тома `.owns3.sys/replication/targets.json` — источник истины; env `OWNS3_REPLICATION_TARGETS__<i>__{NAME,ENDPOINT,ACCESSKEY,SECRETKEY}` — seed первого старта; изменение без даунтайма через `PUT /admin/replication/targets`; новый сосед → notifier с текущего конца журнала + reconcile-забор его данных; убранный → исполнитель останавливается, курсор сохраняется. Идентификация соседа-источника на приёмнике — по его accessKey из подписи (имя известно из targets приёмника); `OWNS3_HOST_ID` (`originHostId`) — диагностика конфликтов, НЕ ключ идентификации.
  - §3 События: таблица 3.2 spec (4 типа, коммит-поинт, состав записи); append+fsync до ответа клиенту; приём реплики события не ставит.
  - §4 Журнал: `.owns3.sys/replication/outbox/` сегментный append-only; бинарная запись v1 (поля: version, type, seqNo, modTimeUnixMs, size, строки bucket/key/etag/contentSha256/originHostId); сегмент закрывается по 64 МиБ или 5 мин возраста; имена `seg-<startSeqNo>.log`; тримминг по min(watermark приёма активных соседей); бюджет `OWNS3_REPLICATION_OUTBOX_BUDGET_BYTES` (2 ГиБ) — превышение: алерт + принудительный тримминг, запись клиента не отказывает. Владелец цикла обслуживания (штатный тримминг, принудительный тримминг при бюджете, retention-чистка tombstones) — ReplicationCoordinator (§8-а ниже).
  - §5 Курсоры приёма: `cursors/<name>.json {acceptedSeqNo, mode: tail|reconciling}`; продвижение по `acceptedToSeqNo`; fsync-дебаунс ~1 с + graceful-shutdown.
  - §6 Replication-грань: пути/контракты трёх операций таблицы 3.4 (notifications с дедупом по (источник, seqNo) — источник резолвится по accessKey подписи (§2), `originHostId` — диагностика; 429-backpressure; object-фетч с заголовками `x-owns3-repl-{modtime,sha256,etag,origin,tombstone}` и 404-семантикой; listing для reconcile с пагинацией по маркеру); политика `replication` — только эта грань.
  - §7 Приёмник: fetch-очередь in-memory bounded per-source (параметр `OWNS3_REPLICATION_FETCH_QUEUE_MAX`, дефолт 10000), фетчер per-source (parallelism/rate/backoff/пауза по месту `OWNS3_REPLICATION_DISK_FREE_MIN_BYTES` = 1 ГиБ), маркер чистого завершения `state.json {shutdownClean}`, LWW `(modTime, contentSha256)` с тай-брейком sha лексикографически и журналируемым конфликтом при равных modTime/разных sha, tombstone-хранилище `tombstones/<sha256(bucket)>/<sha256(key)>.json {bucket, key, deleteTimeUnixMs}` с retention `OWNS3_REPLICATION_TOMBSTONE_RETENTION_HOURS` (168) — хранилище едино: пишется и локальным клиентским DELETE (fetch-ответы «ключ удалён»), и применением реплики object-delete, и защитой от воскрешения; отложенные bucket-delete.
  - §8 Notifier: per-target исполнитель, окно `OWNS3_REPLICATION_INFLIGHT_WINDOW` (64), бэкофф эксп. 1 с → `OWNS3_REPLICATION_BACKOFF_MAX_SEC` (60), at-least-once + дедуп.
  - §8-а Цикл обслуживания (ReplicationCoordinator): периодический проход (60 с): штатный тримминг `Trim(min(watermark приёма активных соседей))`; при превышении бюджета журнала — `ForceTrimToBudget()` + лог-событие «потеряны уведомления для отстающих соседей» (алерт-метрика `ownS3_replication_outbox_budget_exceeded`); retention-чистка tombstones `Cleanup(now, retentionHours)`.
  - §9 Reconcile: ВСЕ ПЯТЬ триггеров 3.7 явно: (1) добавление соседа; (2) старт на пустом томе при существующем составе; (3) грязная остановка процесса (отсутствующий/`false` маркер `state.json` — стартовый reconcile-проход по всем соседям; «пустой том» триггера (2) реализуется этим же механизмом: на пустом томе нет `state.json` → маркер грязный → стартовый reconcile); (4) периодически `OWNS3_REPLICATION_RECONCILE_INTERVAL_HOURS`; (5) админ-команда. Механизм: постраничный листинг+LWW+маркер `reconcile/<source>.marker`, сверка состава бакетов в сторону backfill, параллельный не создаётся. Листинг-грань без `bucket` возвращает состав бакетов (backfill).
  - §10 Наблюдаемость: таблица метрик 3.8 (имена как в spec), sync-report формат `{bucket, prefix, marker, firstKey, firstModTime, lastKey, lastModTime, count, totalSize, aggregateHash}` (aggregateHash = XOR 64-бит FNV-1a свёртка пар (key, modTimeUnixMs)), лог-события; `GET /admin/replication/status` отражает по каждому соседу стейт (online/offline, watermark приёма, глубины очередей, лаги, ход reconcile) И счётчики исходов (notified accepted/backoff/retry; fetched; applied/skipped/conflict). Точки инкремента счётчиков исходов приёма: applied — применение самодостаточных событий приёмником уведомлений И применение фетча; skipped — LWW-предфильтр уведомлений и skip фетча.
  - §11 Раскладка/env-таблица (все `OWNS3_REPLICATION_*`), EC-режим: состояние репликации на служебном контуре первого диска, НЕ реплицируется на диски сета.
  - §12 Границы надёжности (раздел 6 spec).
- Modify: `arch/owns3/01-overview.md` — §3 таблица проектов: строка `src/OwnS3.Replication`; §2 граница этапа: снять «Межсайтовая и бакетная репликация — не делаются» → ссылка на главу 07 (межсерверная репликация — глава 07; бакетная политика/lifecycle — вне).
- Modify: `arch/owns3/04-storage.md` — §1 раскладка `.owns3.sys/`: строка-ссылка `replication/` → глава 07 (владелец — 07).
- Modify: `arch/owns3/05-access-config.md` — §3 матрица: строка политики `replication` (доступна ТОЛЬКО replication-грань, все 21 операция «—»); §4 env-таблица: все `OWNS3_REPLICATION_*`; §5: ссылка на таблицу метрик главы 07 §10; §7 админ-грань: эндпоинты `/admin/replication/{targets,status,reconcile,sync-report}` (права admin/root, тем же конвейером; status включает счётчики исходов — гл. 07 §10).
- Modify: `arch/22-owns3.md` — граница этапа: репликация реализована главой 07.

**Выход:** канон репликации в `arch/`; правки глав; roadmap НЕ трогаем (тег живёт до мержа).

**Проверка:** `git diff --stat` показывает только `arch/**`; согласованность имён env/метрик/путей между 07 и правками 05 (ручная сверка при ревью задачи).

**Spec:** Ф1 (раздел 5), вся структура раздела 3.

- [ ] **Шаг 1.** Написать `arch/owns3/07-replication.md` целиком (§1–§12 выше).
- [ ] **Шаг 2.** Внести 4 инкрементальные правки глав 01/04/05/22.
- [ ] **Шаг 3.** Самопроверка: каждому разделу spec 3.x соответствует § главы 07; всем `OWNS3_REPLICATION_*` из 3.3/3.5/3.6/3.7 есть строки в 05 §4; все 5 триггеров reconcile перечислены; дедуп-ключ — источник по accessKey.
- [ ] **Шаг 4.** Коммит: `git add arch/ && git commit -m "t35: arch-first — глава 07-replication (mesh: уведомления+pull, курсор по ACK приёма) + правки глав 01/04/05/22"`.

---

## Task 1: Storage — расширение результатов коммитов метаданными события (Ф2)

**Files:**
- Modify: `src/OwnS3.Storage/Domain/DomainTypes.cs` (PutResult, CompleteResult)
- Modify: `src/OwnS3.Storage/Drives/SingleXlDrive.cs:216,429`; `src/OwnS3.Storage/Drives/ErasureXlDrive.cs:259,1379`; `src/OwnS3.Storage/XlObjectStore.Multipart.cs:129`
- Test: `src/tests/OwnS3.UnitTests/Storage/` (существующие Storage-юниты + новый кейс)

**Interfaces:**
- Produces (для Task 10): `PutResult(string ETag, DateTimeOffset LastModified, long Size, string ContentSha256Hex)`; `CompleteResult(string ETag, DateTimeOffset LastModified, long Size, string ContentSha256Hex)`.

**Вход:** Task 0 смержён (канон описывает состав события object-put).

**Действие:** расширить оба record новыми полями (XlMetaRecord уже несёт Size/ContentSha256 — пронести из записи драйва). Пять точек создания — заполнить из `record`.

- [ ] **Шаг 1. Тест (fail).** В `src/tests/OwnS3.UnitTests/Storage/` добавить кейс (по образцу существующих Store-юнитов на temp-каталоге):

```csharp
[Fact]
public void PutObject_ResultCarriesEventMetadata()
{
    // Arrange — объектный слой на temp-томе; хелпер построения взять из
    // существующих Store-юнитов (XlObjectStore над SingleXlDrive на Path.GetTempPath())
    var (store, _) = OwnS3StorageTestHarness.BuildOnTempVolume();
    var payload = "replication-meta"u8.ToArray();

    // Act
    var result = store.PutObjectAsync("b", "k", new MemoryStream(payload), payload.Length,
        new ObjectUploadMetadata("text/plain", new Dictionary<string, string>()), ct).Result;

    // Assert — результат несёт всё для события object-put без повторного чтения меты
    result.ETag.Should().NotBeNullOrEmpty();
    result.LastModified.Should().BeOnOrAfter(before);
    result.Size.Should().Be(payload.Length);
    result.ContentSha256Hex.Should().Be(Sha256HexOf(payload));
}
```

Аналогичный кейс для `CompleteMultipartUploadAsync` (составной объект 2 части): CompleteResult несёт LastModified/Size/ContentSha256Hex.

- [ ] **Шаг 2.** Прогнать: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release --filter FullyQualifiedName~Storage` → новые кейсы FAIL (полей нет), существующие зелёные.
- [ ] **Шаг 3.** Реализация: расширить records, заполнить 5 точек (`record.Size`, `record.ContentSha256`, `record.ModTime`); поправить юниты, конструирующие PutResult/CompleteResult напрямую.
- [ ] **Шаг 4.** Прогон юнитов OwnS3 целиком → PASS.
- [ ] **Шаг 5.** Коммит: `t35(storage): PutResult/CompleteResult несут Size/ContentSha256 для событий репликации`.

**Выход:** коммит-поинт хендлеров отдаёт полные метаданные события без повторного чтения xl-меты.

**Проверка:** юниты OwnS3 зелёные (существующие + 2 новых кейса).

**Spec:** 3.2 (состав записи object-put), Ф2.

---

## Task 2: Storage — IReplicaStore: применение реплики и репликационные чтения (Ф2)

**Files:**
- Create: `src/OwnS3.Storage/IReplicaStore.cs`, `src/OwnS3.Storage/ReplicaStore.cs`
- Modify: `src/OwnS3.Storage/Drives/IXlDrive.cs` (CreateBucket optional creationDate), `src/OwnS3.Storage/BucketMetaStore.cs` (перегрузка с датой), `src/OwnS3.Storage/Drives/SingleXlDrive.cs`, `src/OwnS3.Storage/Drives/ErasureXlDrive.cs` (CreateBucket)
- Test: `src/tests/OwnS3.UnitTests/Storage/ReplicaStoreTests.cs`

**Interfaces:**
- Produces (для Task 6/9/12/13):

```csharp
namespace OwnS3.Storage;

public sealed record ReplicaObjectMetadata(DateTimeOffset ModTime, string ETagHex, string ContentSha256Hex,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata);

public sealed record ReplicaKeyState(DateTimeOffset ModTime, string ContentSha256Hex, long Size);

public sealed record ReplicationListEntry(string Key, DateTimeOffset ModTime, string ContentSha256Hex, long Size);

public sealed class ReplicationChecksumMismatchException : Exception; // sha тела ≠ ожидаемого, до Commit

public interface IReplicaStore
{
    // Штатный xl-коммит (staging→WritePart→WriteMeta→Commit) с ИСХОДНЫМИ
    // modTime/etag/sha/метаданными; expectedSha256Hex сверяется по дайджесту
    // тела ДО Commit (мисматч → ReplicationChecksumMismatchException, staging сносится).
    Task ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        ReplicaObjectMetadata metadata, string expectedSha256Hex, CancellationToken ct);
    void DeleteObject(string bucket, string key);            // идемпотентно (204-семантика Delete)
    ReplicaKeyState? ReadKeyState(string bucket, string key); // null — ключа нет
    IReadOnlyList<ReplicationListEntry> ListReplicationEntries(string bucket, string? marker,
        int maxKeys, out string? nextMarker);                 // Utf8ByteOrder от маркера (reconcile/листинг грани)
    (ObjectMetadata Metadata, Stream Body) OpenObjectForReplication(string bucket, string key); // fetch-грань: без VerifyFullChecksum
    void ApplyBucketCreate(string bucket, DateTimeOffset creationDate); // идемпотентно: есть → skip
    void ApplyBucketDelete(string bucket);                    // пустой → удалить; непустой → ObjectStoreException(BucketNotEmpty)
}
```

**Вход:** Task 1; IXlDrive-контракт (`CreateStaging`/`WritePartAsync`/`WriteMeta`/`Commit`/`TryReadMeta`/`OpenData`/`EnumerateObjects`) прочитан.

**Действие:** `ReplicaStore(IXlDrive drive, TimeProvider time)` — реализация:
- `ApplyObjectAsync`: `EnsureBucket` → `CreateStaging` → `WritePartAsync(1, body, len)` → `ReplicationChecksumMismatchException` если `digest.Sha256Hex != expectedSha256Hex` → `WriteMeta(new XlMetaRecord(Guid.Empty, len, metadata.ModTime, metadata.ETagHex, metadata.ContentType, metadata.UserMetadata, EmptyHeaders, metadata.ContentSha256Hex))` → `Commit`. `await Task.Yield()` по контракту P4.
- `ListReplicationEntries`: `drive.EnumerateObjects(bucket)` (ленивый курсор Utf8ByteOrder) от первого ключа > marker, до maxKeys; `nextMarker` — последний отданный ключ.
- `OpenObjectForReplication`: `TryReadMeta ?? NoSuchKey` → `(ToMetadata(key, meta), drive.OpenData(bucket, key, meta, 0, meta.Size))` — без `VerifyFullChecksum` (sha-сверка — обязанность приёмника, 3.4).
- `ApplyBucketCreate`: `BucketExists → return`; `drive.CreateBucket(bucket, creationDate)`.
- `IXlDrive.CreateBucket(string bucket, DateTimeOffset? creationDate = null)`; `BucketMetaStore.CreateBucketMeta(string bucket, DateTimeOffset? at = null)`; single: `File.WriteAllText` с датой; EC: по образцу single (bucket.json per-disk с той же датой).

- [ ] **Шаг 1. Тесты (fail):** `ReplicaStoreTests` на temp-томе, AAA-комментарии:

```csharp
[Fact]
public void ApplyObject_PreservesSourceMetadata()
{
    // Arrange
    var sourceModTime = new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);
    var meta = new ReplicaObjectMetadata(sourceModTime, "abcd0123", Sha256Hex(payload),
        "application/x-test", new Dictionary<string, string> { ["origin"] = "mesh-1" });

    // Act
    await replicaStore.ApplyObjectAsync("b", "k", new MemoryStream(payload), payload.Length, meta, meta.ContentSha256Hex, ct);

    // Assert — xl-запись несёт ИСХОДНЫЕ modTime/etag/sha/userMeta (LWW-сходимость)
    var state = replicaStore.ReadKeyState("b", "k");
    state!.ModTime.Should().Be(sourceModTime);
    state.ContentSha256Hex.Should().Be(meta.ContentSha256Hex);
    // повторное применение идемпотентно
    await replicaStore.ApplyObjectAsync(... то же ...);
    replicaStore.ReadKeyState("b", "k").Should().BeEquivalentTo(state);
}

[Fact]
public void ApplyObject_ChecksumMismatch_RejectedWithoutCommit()
{
    // Arrange — expectedSha не совпадает с телом
    // Act
    var act = () -> replicaStore.ApplyObjectAsync("b", "k", body, len, meta, "deadbeef", ct);
    // Assert — исключение ДО коммита, ключа нет, staging снесён
    await act.Should().ThrowAsync<ReplicationChecksumMismatchException>();
    replicaStore.ReadKeyState("b", "k").Should().BeNull();
}

[Fact]
public void ListReplicationEntries_PagesByMarker_Utf8Order() { /* 3 ключа, maxKeys=2 → страница + nextMarker → вторая страница */ }

[Fact]
public void ApplyBucketCreate_Idempotent_KeepsFirstCreationDate() { /* create(t1); create(t2) → CreationDate осталась t1 */ }
```

- [ ] **Шаг 2.** Прогон: новые кейсы FAIL.
- [ ] **Шаг 3.** Реализация `IReplicaStore`/`ReplicaStore` + расширения CreateBucket.
- [ ] **Шаг 4.** Прогон юнитов OwnS3 → PASS (включая EC-юниты CreateBucket — опциональный параметр не ломает вызовы).
- [ ] **Шаг 5.** Коммит: `t35(storage): IReplicaStore — применение реплики штатным xl-коммитом с исходными метаданными + репликационные чтения`.

**Выход:** Storage умеет применять реплику и читать состояние для LWW/fetch/листинга.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release` зелёный.

**Spec:** 3.9 (Storage-касаение), Ф2, R4 (LWW-применение).

---

## Task 3: OwnS3.Replication — проект, модели, конфиг, targets (Ф3)

**Files:**
- Create: `src/OwnS3.Replication/OwnS3.Replication.csproj` (net10.0; ProjectReference: OwnS3.Storage, OwnS3.Protocol; чистая библиотека без FrameworkReference; PackageReference — ТОЛЬКО уже Version-которые в `src/Directory.Packages.props`: `Microsoft.Extensions.Options.ConfigurationExtensions` (10.0.0 — транзитивно даёт Microsoft.Extensions.Options для IOptions) и `Microsoft.Extensions.Logging.Abstractions` (10.0.9); новых строк в Directory.Packages.props НЕ добавлять)
- Create: `src/OwnS3.Replication/ReplicationEvent.cs`, `ReplicationOptions.cs`, `ReplicationTargets.cs`
- Modify: `src/PgWorker.slnx` (папка `/owns3/`: + OwnS3.Replication)
- Test: `src/tests/OwnS3.UnitTests/Replication/ReplicationTargetsTests.cs` (+ ProjectReference OwnS3.Replication в `OwnS3.UnitTests.csproj`)

**Interfaces:**
- Produces:

```csharp
namespace OwnS3.Replication;

public enum ReplicationEventType : byte { ObjectPut = 1, ObjectDelete = 2, BucketCreate = 3, BucketDelete = 4 }

// Единая модель события журнала/уведомления; для delete/бакетов ETag/ContentSha256 = "".
public sealed record ReplicationEvent(long SeqNo, ReplicationEventType Type, string Bucket, string Key,
    long ModTimeUnixMs, string ETag, long Size, string ContentSha256, string OriginHostId);

public sealed class ReplicationOptions
{
    public const string SectionName = "OwnS3:Replication";
    public TargetOptions[] Targets { get; set; } = [];                       // seed env, не источник истины
    public long OutboxBudgetBytes { get; set; } = 2L * 1024 * 1024 * 1024;
    public int InflightWindow { get; set; } = 64;
    public int Parallelism { get; set; } = 1;
    public long MaxRateBytes { get; set; } = 0;                               // 0 — безлимит
    public int BackoffMaxSec { get; set; } = 60;
    public long DiskFreeMinBytes { get; set; } = 1024L * 1024 * 1024;
    public int TombstoneRetentionHours { get; set; } = 168;
    public int ReconcileIntervalHours { get; set; } = 24;
    public int FetchQueueMax { get; set; } = 10000;
}
public sealed record TargetOptions(string Name, string Endpoint, string AccessKey, string SecretKey);

// Состав mesh: персист targets.json (tmp+rename) + hot-apply; потокобезопасный снапшот.
public sealed class ReplicationTargets(string sysDir, ILogger<ReplicationTargets> logger)
{
    public IReadOnlyList<TargetRecord> Snapshot { get; }
    public bool Active { get; }                       // ≥1 сосед — репликация активна
    // Первый старт (файла нет): env-seed → targets.json; env пуст → файл не создаётся, Active=false.
    public static ReplicationTargets Load(string sysDir, IEnumerable<TargetOptions> seed, ILogger<...> logger);
    // Идемпотентная замена полного списка: персист tmp+rename; событие Changes для координатора (Task 12).
    public event Action<IReadOnlyList<TargetRecord>>? TargetsChanged;
    public void Replace(IReadOnlyList<TargetRecord> targets);
}
```

**Вход:** Task 0 (канон §2).

**Действие:** создать проект, модели, конфиг; `ReplicationTargets` — файл `.owns3.sys/replication/targets.json` (JSON camelCase, `[{name, endpoint, accessKey, secretKey}]`), seed только при отсутствии файла, `Replace` — персист + событие.

- [ ] **Шаг 1. Тесты (fail):** `ReplicationTargetsTests` (temp-каталог):
  - seed переносится в targets.json при первом `Load` (файла нет, env не пуст);
  - повторный `Load` при существующем файле env НЕ перезаписывает (источник истины — файл);
  - env пуст + файла нет → `Active == false`, файл не создан;
  - `Replace` персистит (перезагрузка видит новый список), поднимает `TargetsChanged` c новым снапшотом;
  - имена соседей уникальны — `Replace` с дублем имени → ArgumentException.
- [ ] **Шаг 2.** Прогон → FAIL.
- [ ] **Шаг 3.** Реализация; добавить проект в `src/PgWorker.slnx` и ProjectReference в OwnS3.UnitTests.
- [ ] **Шаг 4.** Прогон юнитов → PASS; `dotnet build src/PgWorker.slnx -c Release` без warn-as-error падений (PackageReference-версии резолвятся из центрального props — NU1008 не возникает).
- [ ] **Шаг 5.** Коммит: `t35(replication): проект OwnS3.Replication — модели событий, конфиг, состав mesh (targets.json + env-seed)`.

**Выход:** каркас проекта; состав mesh персистентен и горяче-заменяем.

**Проверка:** юниты зелёные; сборка slnx чистая.

**Spec:** 3.1, 3.9 (границы зависимостей: Replication → Storage+Protocol), Ф3.

---

## Task 4: Журнал доставки (outbox) — сегменты, seqNo, тримминг, бюджет (Ф3)

**Files:**
- Create: `src/OwnS3.Replication/Outbox/OutboxEntryCodec.cs`, `src/OwnS3.Replication/Outbox/OutboxJournal.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/OutboxJournalTests.cs`

**Interfaces:**
- Produces:

```csharp
namespace OwnS3.Replication.Outbox;

// Бинарная запись v1: [int32 totalLen][byte version=1][byte type][int64 seqNo]
// [int64 modTimeUnixMs][int64 size][str bucket][str key][str etag][str contentSha256][str originHostId]
// (str = int32 len + utf8). Кодек: Encode(ReplicationEvent) → byte[]; TryDecode(buffer, off) → (event, next).
public static class OutboxEntryCodec { ... }

// Сегментный append-only журнал .owns3.sys/replication/outbox/seg-<startSeqNo:020>.log
public sealed class OutboxJournal(string outboxDir, long budgetBytes, ILogger<OutboxJournal> logger) : IDisposable
{
    // Append+Flush+fsync; создаёт сегмент при отсутствии; seqNo сквозной (при открытии —
    // скан хвоста последнего сегмента; пусто → seqNo=1). Потокобезопасен (lock).
    public ReplicationEvent Append(ReplicationEventType type, string bucket, string key,
        long modTimeUnixMs, string etag, long size, string contentSha256, string originHostId);
    public IReadOnlyList<ReplicationEvent> ReadBatch(long fromSeqNoExclusive, int maxCount); // через границы сегментов
    public long LastSeqNo { get; }
    public long TotalBytes { get; }                     // сумма размеров сегментов (метрика ownS3_replication_outbox_bytes)
    // Штатный тримминг: удалить сегменты с endSeqNo < minAcceptedSeqNo. Принудительный (бюджет превышен):
    // удалять старейшие закрытые пока TotalBytes <= budget (активный сегмент не трогаем);
    // возвращает факт принудительного тримма (лог-событие «потеряны уведомления» — вызывающий).
    // Вызывающий обоих — цикл обслуживания ReplicationMaintenance (Task 12), НЕ путь записи.
    public bool Trim(long minAcceptedSeqNo);
    public bool ForceTrimToBudget();
    public bool BudgetExceeded { get; }                  // метрика ownS3_replication_outbox_budget_exceeded
}
```

Сегмент закрывается (новый `seg-<next>.log`) при 64 МиБ или 5 мин возраста — константы класса.

**Вход:** Task 3.

**Действие:** реализовать кодек и журнал; fsync — `FileStream.Flush(true)`.

- [ ] **Шаг 1. Тесты (fail)** (temp-каталог, AAA):
  - append возвращает монотонные seqNo 1,2,3; `ReadBatch(0, 10)` возвращает все три в порядке;
  - рестарт журнала (новый экземпляр того же каталога) → seqNo продолжается (4), `ReadBatch(2, 10)` — события >2;
  - тримминг: watermark=2 → сегмент с seqNo 1..2 удалён, `ReadBatch(0,...)` пуст, `ReadBatch(2,...)` жив;
  - бюджет (journal с budgetBytes=малый): append'ы превышают бюджет → `BudgetExceeded==true`, `ForceTrimToBudget()` удаляет старейшие закрытые сегменты, запись (Append) всё ещё succeeds;
  - пустой каталог без соседей — журнал создаётся лениво (каталог не создаётся до первого Append — см. Task 10: события не ставятся при неактивной репликации).
- [ ] **Шаг 2.** Прогон → FAIL.
- [ ] **Шаг 3.** Реализация.
- [ ] **Шаг 4.** Прогон → PASS.
- [ ] **Шаг 5.** Коммит: `t35(replication): сегментный outbox-журнал — seqNo/fsync/чтение батчами/тримминг/bounded-бюджет`.

**Выход:** журнал доставки с bounded-бюджетом (R2, R6); цикл обслуживания подключается в Task 12.

**Проверка:** юниты `FullyQualifiedName~OutboxJournal` зелёные.

**Spec:** 3.3, принципы 3/8, R2/R6, Ф3.

---

## Task 5: Watermark-курсоры приёма per-target (Ф3)

**Files:**
- Create: `src/OwnS3.Replication/Cursors/TargetCursors.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/TargetCursorsTests.cs`

**Interfaces:**
- Produces:

```csharp
namespace OwnS3.Replication.Cursors;

// cursors/<name>.json {acceptedSeqNo, mode: "tail"|"reconciling"}; персист дебаунсом ~1 с
// (по TimeProvider) и при Dispose (graceful). Потеря хвоста = повторная рассылка (идемпотентно).
public sealed class TargetCursors(string cursorsDir, TimeProvider timeProvider) : IDisposable
{
    public long GetAccepted(string targetName);          // файла нет → -1 (источник решает: новый сосед → с конца журнала)
    public void Advance(string targetName, long acceptedSeqNo); // только вперёд
    public void SetMode(string targetName, string mode);
    public void Flush();                                  // синхронный fsync-персист всех грязных
}
```

**Вход:** Task 3.

**Действие:** JSON-файл на соседа; монотонность (назад не двигается); дебаунс-таймер внутри.

- [ ] **Шаг 1. Тесты (fail):** advance двух соседей независимо; персист после `Flush()` виден новому экземпляру; advance назад игнорируется; дебаунс (FakeTimeProvider из Microsoft.Extensions.TimeProvider.Testing — проверить наличие пакета в `Directory.Packages.props`; если нет — inject `TimeProvider` и тест через ручной `Flush()`).
- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(replication): watermark-курсоры приёма per-target — персист/дебаунс-fsync/монотонность`.

**Выход:** курсоры приёма (ACK приёма двигает, 3.3/3.6).

**Проверка:** юниты `FullyQualifiedName~TargetCursors` зелёные.

**Spec:** 3.3 (курсоры), 3.6 (acceptedToSeqNo → watermark), Ф3.

---

## Task 6: LWW-компаратор, tombstone-хранилище, ReplicaApplier (Ф3)

**Files:**
- Create: `src/OwnS3.Replication/Lww/LwwComparator.cs`, `src/OwnS3.Replication/Lww/TombstoneStore.cs`, `src/OwnS3.Replication/Lww/ReplicaApplier.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/{LwwComparatorTests,TombstoneStoreTests,ReplicaApplierTests}.cs`

**Interfaces:**
- Consumes: `IReplicaStore` (Task 2).
- Produces:

```csharp
namespace OwnS3.Replication.Lww;

public enum LwwOutcome { IncomingWins, LocalWins, Identical }

public static class LwwComparator
{
    // Compare((modTimeMs, sha)) → (outcome, conflict). Полная таблица:
    // in.mod > local.mod → IncomingWins; < → LocalWins; == и sha == → Identical;
    // == и sha != → тай-брейк лексикографически больший sha; conflict=true.
    public static (LwwOutcome Outcome, bool Conflict) Compare(long inModMs, string inSha, long localModMs, string localSha);
}

// tombstones/<sha256hex(bucket)>/<sha256hex(key)>.json {bucket, key, deleteTimeUnixMs}
// Retention-чистку по расписанию зовёт ReplicationMaintenance (Task 12) — сам store чисток не планирует.
public sealed class TombstoneStore(string tombstonesDir, TimeProvider timeProvider)
{
    public void Put(string bucket, string key, long deleteTimeUnixMs); // максимум старого и нового
    public long? GetDeleteTime(string bucket, string key);             // null — нет знания
    public void Remove(string bucket, string key);                     // применённый новее-tombstone PUT
    public int Cleanup(DateTimeOffset now, int retentionHours);        // retention-чистка, возвращает удалённые
}

// LWW-слой применения поверх IReplicaStore + TombstoneStore. Результат применения —
// ReplicaApplyResult {Applied, Conflict}: вызывающий (inbox-предфильтр Task 9 /
// фетчер Task 12) инкрементирует счётчики исходов по результату СВОЕГО вызова.
// Событие ConflictDetected (bucket, key, обе версии) — ТОЛЬКО для лога (подписчик —
// координатор, Task 12); метрику ownS3_replication_conflicts_total по нему НЕ
// инкрементировать: аплайер один на приёмник и событие не несёт sourceName —
// инкремент по нему дал бы мульти-счёт при ≥2 живых источниках и ложную атрибуцию.
public sealed class ReplicaApplier(IReplicaStore store, TombstoneStore tombstones, TimeProvider timeProvider)
{
    // object-put: локальное (запись|tombstone) vs incoming → IncomingWins → применить
    // (перезапись после tombstone: in.mod > tomb.deleteTime → применить и Remove tombstone);
    // Identical/LocalWins → skip; конфликт (equal mod, разный sha) → победитель по sha.
    public readonly record struct ReplicaApplyResult(bool Applied, bool Conflict); // Applied = IncomingWins
    public Task<ReplicaApplyResult> ApplyObjectAsync(string bucket, string key, Stream body, long contentLength,
        OwnS3.Storage.ReplicaObjectMetadata metadata, string expectedSha256, CancellationToken ct);
    // object-delete (tombstone-время): локальная запись modTime <= deleteTime → DeleteObject + Put(tombstone);
    // записи нет → Put(tombstone); локальная новее → skip.
    public void ApplyDelete(string bucket, string key, long deleteTimeUnixMs);
    // bucket-create: идемпотентно; bucket-delete: пустой → удалить, непустой → отложить (RetryQueue
    // in-memory bounded 256, повтор по бэкоффу 1с→60с — недоставленные объектные события опустошат бакет).
    public void ApplyBucketCreate(string bucket, long creationDateUnixMs);
    public void ApplyBucketDelete(string bucket);
    public event Action<string, string, long, string, long, string>? ConflictDetected; // bucket, key, обе версии
}
```

**Вход:** Task 2.

**Действие:** реализация трёх классов по контрактам; RetryQueue — внутренний таймер через `TimeProvider` (тесты — FakeTimeProvider/ручной тик).

- [ ] **Шаг 1. Тесты (fail):**
  - `LwwComparatorTests` — ПОЛНАЯ таблица исходов: (in>local), (in<local), (== sha==), (== sha: "aa">"a9" → IncomingWins + conflict), (== sha: "a9"<"aa" → LocalWins + conflict);
  - `TombstoneStoreTests` — put/get/remove; Put с более старым временем не откатывает; Cleanup удаляет по retention, свежие остаются;
  - `ReplicaApplierTests` — модтайм-сохранение через применение; старый PUT после tombstone (in.mod < deleteTime) → skip, ключ не воскрешён; новый PUT (in.mod > deleteTime) → применён, tombstone снят; delete при локальной записи новее → skip; конфликт возвращает `ReplicaApplyResult { Applied=…, Conflict=true }` И поднимает `ConflictDetected` с обеими версиями (событие — для лога); bucket-delete непустого — отложен, после удаления объектов (ApplyDelete по ключам + ретрай-тик) бакет удалён.
- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(replication): LWW-компаратор (полная таблица), tombstones+retention, ReplicaApplier`.

**Выход:** идемпотентный LWW-слой применения с tombstones (R4).

**Проверка:** юниты `FullyQualifiedName~Replication` зелёные.

**Spec:** 3.5 (LWW/tombstones/бакеты), принципы 4/6, R4, Ф3.

---

## Task 7: Исходящая SigV4 + транспортные модели (Ф3)

**Files:**
- Create: `src/OwnS3.Protocol/Auth/SigV4Signer.cs`
- Create: `src/OwnS3.Replication/Transport/ReplicationDto.cs`, `src/OwnS3.Replication/Transport/IReplicationTransport.cs`, `src/OwnS3.Replication/Transport/ReplicationHttpClient.cs`
- Test: `src/tests/OwnS3.UnitTests/SigV4SignerTests.cs`

**Interfaces:**
- Produces:

```csharp
namespace OwnS3.Protocol.Auth;

// Исходящая заголовочная SigV4 (зеркало SigV4HeaderVerifier): собирает Authorization
// для (method, path, canonicalQuery, headers, payloadHash) по CredentialDate/region "us-east-1".
// Region не проверяется верификатором жёстко (scope из заголовка), фиксируем "us-east-1".
public static class SigV4Signer
{
    public static (string Authorization, string AmzDate, string ContentSha256) Sign(
        string secretKey, string accessKey, string method, string path, string canonicalQuery,
        IReadOnlyList<(string Name, string Value)> headers, string payloadString,
        DateTimeOffset at, string region = "us-east-1");
}

namespace OwnS3.Replication.Transport;

// JSON (camelCase): {"originHostId":"...","events":[{"seqNo":1,"type":"object-put",...}]}
public sealed record NotificationBatchRequest(string OriginHostId, IReadOnlyList<ReplicationEvent> Events);
public sealed record NotificationAcceptedResponse(long AcceptedToSeqNo);
public sealed record ReplicationListingRequest(string Bucket, string? Marker, int MaxKeys);
public sealed record ReplicationListingResponse(IReadOnlyList<OwnS3.Storage.ReplicationListEntry> Entries,
    string? NextMarker);

public interface IReplicationTransport
{
    // POST /replication/notifications → 200 {acceptedToSeqNo} | 429 (→ null, вызывающий бэкоффит).
    Task<long?> SendNotificationsAsync(TargetRecord target, NotificationBatchRequest batch, CancellationToken ct);
    // GET /replication/object/{bucket}/{key+}: 200-объект (Body+заголовки modtime/sha/etag/origin/
    // contentType/x-amz-meta-*) | 200-tombstone (TombstoneUnixMs) | 404 (ключ неизвестен).
    Task<FetchResult> FetchObjectAsync(TargetRecord target, string bucket, string key, CancellationToken ct);
    Task<ReplicationListingResponse> ListAsync(TargetRecord target, string bucket, string? marker,
        int maxKeys, CancellationToken ct);
    // GET /replication/listing без bucket — состав бакетов соседа (backfill reconcile, Task 13).
    Task<IReadOnlyList<(string Name, long CreationUnixMs)>> ListingBucketsAsync(TargetRecord target, CancellationToken ct);
}
public sealed record FetchResult(Stream? Body, long ModTimeUnixMs, string ContentSha256, string ETag,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata, long? TombstoneUnixMs);
```

`ReplicationHttpClient(IReplicationTransport-реализация)`: `HttpClient` инжектится (тесты подменяют `HttpMessageHandler`); подпись всех запросов SigV4Signer'ом с accessKey/secretKey цели; таймаут запроса 10 с; тело фетча — потоковым (HttpCompletionOption.ResponseHeadersRead).

**Вход:** Task 3; Protocol-верификатор прочитан.

**Действие:** signer (HMAC-цепочка SigV4Core + CanonicalRequestBuilder — переиспользовать, не дублировать канонизацию); транспорт.

- [ ] **Шаг 1. Тест (fail) — roundtrip-подлинка:**

```csharp
[Fact]
public void OutgoingSignature_PassesOwnVerifier()
{
    // Arrange — подписываем исходящий POST /replication/notifications с JSON-телом
    var payload = SigV4Core.HexSha256("..."u8.ToArray());
    // Act
    var (auth, amzDate, _) = SigV4Signer.Sign(secret, access, "POST", "/replication/notifications", "",
        [("host", "neighbor:9000"), ("x-amz-date", amzDate), ("x-amz-content-sha256", payload)], payload, at);
    // Assert — СОБСТВЕННЫЙ SigV4HeaderVerifier подтверждает подпись (взаимная совместимость граней)
    var model = BuildRequestModel("/replication/notifications", auth, amzDate, payload);
    var result = new SigV4HeaderVerifier(TimeProvider.System).Verify(model, _ => secret);
    result.Should().BeOfType<SigV4Result.Ok>();
}
```

Плюс кейсы: FetchObject-парсинг заголовков `x-owns3-repl-*`/`x-amz-meta-*`; tombstone-заголовок; 404 → FetchResult с null-Body и null-Tombstone; ListingBuckets-парсинг.

- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(protocol,replication): исходящая SigV4Signer + IReplicationTransport (подписанные notifications/fetch/listing/buckets)`.

**Выход:** подписанный транспорт mesh (3.4).

**Проверка:** юниты `FullyQualifiedName~SigV4Signer` зелёные.

**Spec:** 3.4 (грань), юниты «подлинка исходящих SigV4», Ф3.

---

## Task 8: Политика `replication` в матрице доступа (Ф4)

**Files:**
- Modify: `src/OwnS3.App/Access/AccessPolicy.cs` (+ `Replication`), `AccessKeyRegistry.cs` (ParsePolicy), `OperationAccessMatrix.cs`
- Test: `src/tests/OwnS3.UnitTests/OperationAccessMatrixTests.cs` (расширение), `AccessKeyRegistryTests.cs` (расширение)

**Interfaces:**
- Produces: `AccessPolicy.Replication`; `OperationAccessMatrix.IsAllowed(Replication, любая из 21 операции) == false` (грань `/replication/*` авторизуется отдельно в S3Middleware — не через матрицу).

**Вход:** Task 0 (канон 05 §3 дополнен строкой).

**Действие:** enum + парсер `"replication"` + матрица (`AccessPolicy.Replication => false` — явная ветка switch). A13-инвариант: replication-ключ не проходит НИ ОДНУ из 21 операции.

- [ ] **Шаг 1. Тесты (fail):** policy-строка "replication" парсится; IsAllowed(replication, каждая из 21 операции) == false; admin на replication-грани — rejecting делается в Task 9 (там грань); read-only/read-write/admin на 21 операцию не изменились (регресс-кейс).
- [ ] **Шаг 2–4.** FAIL → реализация → PASS (юниты OwnS3 целиком).
- [ ] **Шаг 5.** Коммит: `t35(app): политика replication — только replication-грань, 21 операция закрыта (матрица 05 §3)`.

**Выход:** peer-креды инсталляции (A13).

**Проверка:** юниты зелёные.

**Spec:** 3.4 (политика), A13, Ф4.

---

## Task 9: Replication-грань в App — приём уведомлений, фетч-отдача, листинг (Ф4)

**Files:**
- Create: `src/OwnS3.Replication/Inbound/FetchQueue.cs`, `src/OwnS3.Replication/Inbound/NotificationInbox.cs`
- Create: `src/OwnS3.App/Handlers/ReplicationHandlers.cs`
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (шаг 2.4 до роутинга 21 операций), `src/OwnS3.App/Program.cs` (DI)
- Test: `src/tests/OwnS3.IntegrationTests/Api/ReplicationScenarios.cs` (WAF); `src/tests/OwnS3.UnitTests/Replication/NotificationInboxTests.cs` (юнит дедупа)

**Interfaces:**
- Consumes: `IReplicaStore`, `ReplicaApplier`, `TombstoneStore` (Task 2/6), `OwnS3TestClient` (подписанные запросы).
- Produces:

```csharp
namespace OwnS3.Replication.Inbound;

// Задача фетча (ключ, о котором уведомил источник; modTime — для метрик лага).
public sealed record FetchTask(string Bucket, string Key, long NotifiedModMs);

// Bounded in-memory очередь per-source (Channel, ёмкость ReplicationOptions.FetchQueueMax).
public sealed class FetchQueue(int capacity)
{
    public bool TryEnqueue(FetchTask task);          // false — полна
    public bool TryDequeue(out FetchTask task);
    public int Depth { get; }
    public long OldestEnqueuedUnixMs { get; }        // лаг данных (метрика), 0 — пуста
}

// Очереди по ИМЕНИ соседа-источника (sourceName = TargetRecord.Name приёмника;
// имя определяется по accessKey подписи батча — канон 3.1/07 §2 «идентификация соседа
// по его accessKey»; ключ не найден в targets → ReplicationHandlers отвечает 403).
public sealed class FetchQueueRegistry(ReplicationOptions options)
{
    public FetchQueue ForSource(string sourceName);  // создаётся по требованию
    public IReadOnlyCollection<string> Sources { get; }
}

// Точки инкремента счётчиков исходов приёма — единый контракт inbox'а (здесь)
// и фетчера (Task 12). Реализация: no-op-заглушка (до wiring'а Task 12) и боевой
// ReplicationMetrics (Task 12: серии applied_total{result}/fetched/conflicts).
public interface IOutcomeCounters
{
    void Applied(string sourceName);    // каждое применённое событие (самодостаточные inbox'а И apply-ветка фетчера)
    void Skipped(string sourceName);    // LWW-предфильтр-skip inbox'а (вкл. дедуп-повторы) И skip-ветка фетчера
    void Conflict(string sourceName);   // флаг конфликта LWW: компаратор предфильтра (inbox) И ReplicaApplyResult.Conflict применения (фетчер)
    void Fetched(string sourceName);    // успешный фетч тела объекта (только фетчер)
}

// Приёмник уведомлений: дедуп по (sourceName, seqNo) — per-source lastAccepted;
// повторы (seqNo <= lastAccepted) — skip без повторного применения;
// самодостаточные применяет ReplicaApplier'ом; object-put — LWW-предфильтр: локально
// (запись|tombstone) не старее → skip, старее/нет → fetch-очередь источника;
// очередь полна → null (429). Исходы пишет в IOutcomeCounters (интерфейс определён
// здесь, реализация — Task 12; DI этой задачи подставляет no-op-заглушку):
// applied — каждое применённое событие (самодостаточные), skipped — LWW-предфильтр-skip
// (в т.ч. дедуп-повторы), conflict — флаг Conflict компаратора предфильтра (равные
// modTime, разные sha). Эти точки работают и в WAF-конфигурации с фиктивным соседом
// (без фетча) — на них опираются ассерты счётчиков статуса Task 14.
public sealed class NotificationInbox(ReplicaApplier applier, IReplicaStore store,
    TombstoneStore tombstones, FetchQueueRegistry queues, IOutcomeCounters counters,
    TimeProvider timeProvider)
{
    // null — 429 (приёмный буфер переполнен); иначе acceptedToSeqNo = последний
    // поставленный (применённый|в очередь) seqNo батча; повторы ≤ lastAccepted — skip без применения.
    public long? Handle(string sourceName, NotificationBatchRequest batch);
}
```

`ReplicationHandlers` (App): три эндпоинта (после аутентификации SigV4: accessKey подписи ищется в `ReplicationTargets.Snapshot` — так определяется `sourceName` соседа; валидный replication-ключ, не числящийся соседом → 403, A13):
- `POST /replication/notifications` — JSON-тело `NotificationBatchRequest`; `NotificationInbox.Handle(sourceName, batch)` → 200 `{acceptedToSeqNo}`; null → 429; malformed JSON → 400.
- `GET /replication/object/{bucket}/{key+}` — `IReplicaStore.ReadKeyState`: есть → `OpenObjectForReplication`: 200 + тело + `x-owns3-repl-{modtime(unix-ms),sha256,etag,origin(HostId)}`, `Content-Type`, `x-amz-meta-*`; нет → `TombstoneStore.GetDeleteTime`: есть → 200 + `x-owns3-repl-tombstone: <unix-ms>` (без тела); нет → 404.
- `GET /replication/listing?bucket=&marker=&max-keys=` — `ListReplicationEntries` (дефолт max-keys 1000, cap 1000) → JSON `{entries:[{key,modTimeUnixMs,contentSha256,size}], nextMarker}` (ветка без bucket — состав бакетов — добавляется в Task 13).

S3Middleware шаг 2.4 (после OPTIONS, до `/admin` — или рядом, порядок не критичен): `path.StartsWithSegments("/replication")` → SigV4 (тот же S3Authenticator, `S3Operation.None`) → политика строго `AccessPolicy.Replication` (иначе 403 AccessDenied, включая admin/root — A13) → диспетчеризация по (method, path). Неизвестный replication-путь → 400 InvalidArgument.

**Вход:** Task 6, 7, 8.

**Действие:** очередь, inbox (исходы через параметр `IOutcomeCounters` — интерфейс задаётся здесь как контракт приёмных счётчиков, реализация/боевой класс — Task 12; в этой задаче DI регистрирует no-op-заглушку `NullOutcomeCounters`, тесты используют счётчик-заглушку), хендлеры, ветка middleware, DI-регистрации.

- [ ] **Шаг 1. Юниты NotificationInbox (fail)** — `src/tests/OwnS3.UnitTests/Replication/NotificationInboxTests.cs` на temp-томе (реальные `ReplicaStore`/`TombstoneStore`/`ReplicaApplier` из Tasks 2/6, AAA):

```csharp
[Fact]
public void Handle_DuplicateBatch_SkipsReapplication_AndRepeatsAck()
{
    // Arrange — батч bucket-create + object-delete seqNo 1..2 от источника "peer-a"
    // Act — Handle дважды одним и тем же батчем
    // Assert — второй вызов: тот же acceptedToSeqNo=2; применение не продублировано
    // (DeleteObject по отсутствующему ключу идемпотентен — проверяем через счётчик
    // обращений к applier: обёртка-счётчик вокруг ReplicaApplier либо InternalsVisibleTo-хук)
}

[Fact]
public void Handle_SelfSufficientApplied_IncrementsAppliedCounter()
{
    // Arrange — батч bucket-create (самодостаточное) + счётчик-заглушка IOutcomeCounters
    // Act — Handle батча
    // Assert — counters.Applied == 1 (точка инкремента inbox — гарантия ассертов Task 14)
}

[Fact]
public void Handle_LwwPrefilterSkip_IncrementsSkippedCounter()
{
    // Arrange — локальный ключ новее уведомления object-put
    // Act — Handle батча
    // Assert — counters.Skipped == 1; ключ не перезаписан
}

[Fact]
public void Handle_GapInSeqNo_AppliesAnyway() { /* seqNo 5 после lastAccepted=1 — применяется, acceptedToSeqNo=5 (пропущенные закроет reconcile) */ }

[Fact]
public void Handle_FetchQueueFull_ReturnsNull() { /* capacity=1, два put-ключа старее локальных → второй не влез → null (429) */ }
```

- [ ] **Шаг 2. WAF-тесты (fail)** — `OwnS3AppFactory` + собственный replication-ключ (добавить в фабрику `ReplicationAccessKey/SecretKey` с policy=replication) И `OwnS3:Replication:Targets:0` = фиктивный сосед `{name:"peer-a", endpoint:"http://127.0.0.1:9", accessKey: ReplicationAccessKey, secretKey: ...}` — accessKey соседа совпадает с ключом подписи, чтобы `sourceName` резолвился (исполнители рассылки в WAF не поднимаются — координатор Task 12 ещё нет); подпись `OwnS3TestClient.SendSignedAsync`:

```csharp
[Fact]
public async Task Notifications_SelfSufficientEvents_AppliedFromNotification()
{
    // Arrange — батч: bucket-create + object-delete (нет данных локально)
    var batch = new { originHostId = "mesh-a", events = new[] {
        new { seqNo = 1L, type = "bucket-create", bucket = "b", key = "", modTimeUnixMs = T, ... },
        new { seqNo = 2L, type = "object-delete", bucket = "b", key = "gone", ... } } };

    // Act
    var response = await client.SendSignedAsync("POST", "/replication/notifications",
        body: Json(batch), credentials: ReplicationCreds);

    // Assert — 200 + acceptedToSeqNo=2; бакет создан (через S3-грань writer'а ListBuckets)
    response.StatusCode.Should().Be(HttpStatusCode.OK);
    (await JsonOf(response)).AcceptedToSeqNo.Should().Be(2);
}

[Fact]
public async Task Notifications_ObjectPut_LwwPrefilter_QueuesOrSkips()
{
    // Arrange — локальный ключ с modTime новее уведомления
    // Act — батч object-put со старым modTime
    // Assert — 200 + acceptedToSeqNo; ключ НЕ перезаписан (skip без фетча — проверяем поведением: ключ остался прежним)
}

[Fact]
public async Task Notifications_FetchQueueFull_Returns429() { /* capacity=1 через конфиг, батч из 2 put-ключей старее локальных → второй не влез → 429 */ }

[Fact]
public async Task Notifications_DuplicateBatch_IdempotentAccept()
{
    // Arrange — тот же батч дважды
    // Act — повторный POST
    // Assert — 200 с тем же acceptedToSeqNo, применение не продублировано (состояние неизменно)
}

[Fact]
public async Task ObjectFetch_ReturnsHeadersBody_Tombstone_And404() { /* три ветки: объект (заголовки+sha тела), удалённый ключ после клиентского DELETE (tombstone-заголовок), неизвестный ключ (404) */ }

[Fact]
public async Task Listing_PagesByMarker() { /* 3 ключа, max-keys=2 → nextMarker → вторая страница */ }

[Fact]
public async Task ReplicationEdge_PolicyIsolation()
{
    // Arrange / Act — запросы грани ключами read-only, read-write, admin; replication-ключом,
    // НЕ числящимся в targets (второй replication-ключ фабрики без target-записи); replication-ключом на PutObject
    // Assert — все 403 AccessDenied (A13: грань — только peer-креды известных соседей;
    // replication-ключ не проходит в 21 операцию)
}
```

- [ ] **Шаг 3.** Прогон юнитов `--filter FullyQualifiedName~NotificationInbox` и WAF `--filter FullyQualifiedName~Api.Replication` → FAIL.
- [ ] **Шаг 4.** Реализация очереди/inbox/хендлеров/ветки middleware → обе серии PASS; регресс: `--filter FullyQualifiedName~Api` (существующие сценарии не задеты).
- [ ] **Шаг 5.** Коммит: `t35(app,replication): replication-грань — приём уведомлений (дедуп по источочнику/LWW-предфильтр/429), фетч-отдача с tombstone, листинг`.

**Выход:** грань приёма/отдачи работает под SigV4 с изоляцией политик; дедуп покрыт юнитом; точки инкремента счётчиков исходов приёма определены (inbox — самодостаточные applied / предфильтр skipped+conflict, через `IOutcomeCounters`).

**Проверка:** юниты NotificationInbox + WAF-сценарии `Api.Replication` зелёные; A13-инвариант покрыт.

**Spec:** 3.4 (вся таблица операций, дедуп), A13, «Дополнительно — Юниты: дедуп уведомлений», Ф4; точки счётчиков — 3.1/3.8.

---

## Task 10: События в 7 хендлерах + env-трансляция + DI (Ф4)

**Files:**
- Create: `src/OwnS3.Replication/ReplicationEventSink.cs`
- Modify: `src/OwnS3.App/Handlers/ObjectHandlers.cs` (PutObject, DeleteObject, DeleteObjects, CopyObject), `MultipartHandlers.cs` (CompleteMultipartUpload), `BucketHandlers.cs` (CreateBucket, DeleteBucket), `src/OwnS3.App/Program.cs`, `src/OwnS3.App/OwnS3Options.cs` (+ `ReplicationOptions Replication`)
- Test: `src/tests/OwnS3.IntegrationTests/Api/ReplicationEventsScenarios.cs` (WAF)

**Interfaces:**
- Consumes: `OutboxJournal` (Task 4), `ReplicationTargets` (Task 3), `PutResult/CompleteResult` (Task 1).
- Produces:

```csharp
namespace OwnS3.Replication;

// Синхронный sink пути записи: НЕТ соседей → no-op (ноль IO, Existing-регресс);
// есть → Append+fsync (мс) в outbox. originHostId — OWNS3_HOST_ID (дефолт — имя контейнера, как HostId).
public sealed class ReplicationEventSink(OutboxJournal journal, ReplicationTargets targets,
    TombstoneStore tombstones, string originHostId) : IReplicationEventSink
{
    public void ObjectPut(string bucket, string key, long modTimeUnixMs, string etag, long size, string contentSha256);
    public void ObjectDeleted(string bucket, string key, long deleteTimeUnixMs); // время коммита удаления — вызывающий (TimeProvider)
    public void BucketCreated(string bucket, long creationUnixMs);
    public void BucketDeleted(string bucket, long deleteUnixMs);
    public void ClientObjectDeleted(string bucket, string key, long deleteTimeUnixMs); // событие + tombstone-запись (fetch-ответы «ключ удалён»)
}
public interface IReplicationEventSink { ... } // реализация + NullReplicationEventSink (неактивна)
```

Хендлеры (вставка ПОСЛЕ успешного коммит-поинта, ДО ответа клиенту):
- `PutObjectHandler`: после `PutObjectAsync` → `sink.ObjectPut(bucket, key, result.LastModified.ToUnixTimeMilliseconds(), result.ETag, result.Size, result.ContentSha256Hex)`.
- `CompleteMultipartUploadHandler`: после `CompleteMultipartUploadAsync` → аналогично из расширенного `CompleteResult`.
- `CopyObjectHandler`: после `CopyObjectAsync` → из `PutResult`.
- `DeleteObjectHandler`: после `DeleteObjectAsync` → `sink.ClientObjectDeleted(bucket, key, now)` (событие + локальный tombstone).
- `DeleteObjectsHandler`: для каждого `r.Deleted` → событие на каждый ключ.
- `CreateBucketHandler` / `DeleteBucketHandler`: после коммита → `BucketCreated/BucketDeleted` (время из `TimeProvider` — хендлеры берут TimeProvider из DI, не `DateTimeOffset.UtcNow`).

Env-трансляция Program.cs (по образцу `OWNS3_ACCESS_KEYS__*`): скаляры `OWNS3_REPLICATION_{OUTBOX_BUDGET_BYTES,INFLIGHT_WINDOW,PARALLELISM,MAX_RATE_BYTES,BACKOFF_MAX_SEC,DISK_FREE_MIN_BYTES,TOMBSTONE_RETENTION_HOURS,RECONCILE_INTERVAL_HOURS,FETCH_QUEUE_MAX}` → `OwnS3:Replication:*`; массив `OWNS3_REPLICATION_TARGETS__<i>__{NAME,ENDPOINT,ACCESSKEY,SECRETKEY}` → `OwnS3:Replication:Targets:<i>:*` (скан до первого пропуска индекса).

DI: `ReplicationTargets.Load(...)` на `.owns3.sys/replication/` тома (путь — от StorageModeInfo.DataRoots[0] + `.owns3.sys`; каталог раскладки создать), `OutboxJournal`, `TombstoneStore`, `ReplicaApplier`, `FetchQueueRegistry`, `NotificationInbox`, `IReplicationEventSink` (активен/Null по `targets.Active`).

**Вход:** Task 1–9.

**Действие:** sink + вставки в 7 хендлеров + env + DI.

- [ ] **Шаг 1. WAF-тесты (fail):**
  - без соседей: PUT/DELETE/Create/Delete → в `.owns3.sys/replication/` НЕТ каталога `outbox` (Existing-регресс; путь тома известен из `OwnS3AppFactory.TempVolumeDir`);
  - с соседом (in-memory конфиг `OwnS3:Replication:Targets:0:*` → фиктивный endpoint, notifier ещё не запущен — Task 12; здесь только события): PUT → в `outbox/seg-*.log` бинарно читается событие object-put c корректными etag/sha/size/modTime (декодировать `OutboxEntryCodec` в тесте);
  - multipart-complete и copy кладут object-put; DeleteObjects кладёт по событию на ключ; бакеты — bucket-create/bucket-delete;
  - приём реплики (POST /replication/notifications c object-put) НЕ порождает событий в outbox (анти-петля, принцип 5).
- [ ] **Шаг 2–4.** FAIL → реализация → PASS (вся Api-серия).
- [ ] **Шаг 5.** Коммит: `t35(app): события репликации в 7 хендлерах после коммит-поинта + env OWNS3_REPLICATION_* + DI (анти-петля; без соседей — байт-в-байт регресс)`.

**Выход:** журнал наполняется клиентскими операциями; приём реплик событий не ставит.

**Проверка:** WAF-серия Api зелёная; регресс-кейс «без соседей» зелёный.

**Spec:** 3.2 (таблица событий), принципы 5/1, Existing-регресс, Ф4.

---

## Task 11: Notifier per-target — окно, бэкофф, метрики доставки (Ф5)

**Files:**
- Create: `src/OwnS3.Replication/Notifier/NotificationDispatcher.cs`
- Create: `src/OwnS3.Replication/ReplicationMetrics.cs` (создание класса: исходящие серии link_online, queue_depth, delivery_lag_seconds, notified_total, retries_total; приёмные fetch-серии/счётчики исходов добавляет Task 12, reconcile.position — Task 13, доведение словаря до полного + экспорт-инстанцирование — Task 15)
- Test: `src/tests/OwnS3.UnitTests/Replication/NotificationDispatcherTests.cs`

**Interfaces:**
- Consumes: `OutboxJournal`, `TargetCursors`, `IReplicationTransport`, `TimeProvider`.
- Produces:

```csharp
namespace OwnS3.Replication.Notifier;

// Исполнитель per-target (фон): цикл — ReadBatch(watermark+1, окно) → пусто: ждать новых
// (сигнал Append/PollPeriod 500мс) → SendNotifications → 200: Advance(watermark=acceptedToSeqNo),
// персист дебаунсом; null/исключение: бэкофф эксп 1с→BackoffMaxSec (успех — сброс).
// Стартер нового соседа: watermark = LastSeqNo (уведомления с конца журнала, 3.1).
// Счётчики/лаги пишет в ReplicationMetrics (link_online, queue_depth, delivery_lag,
// notified_total{result}, retries_total — target = target.Name).
public sealed class NotificationDispatcher(TargetRecord target, OutboxJournal journal,
    TargetCursors cursors, IReplicationTransport transport, ReplicationOptions options,
    ReplicationMetrics metrics, TimeProvider timeProvider, ILogger<NotificationDispatcher> logger)
{
    public Task RunAsync(CancellationToken ct);   // пока не cancel
    public bool Online { get; }                   // метрика link_online
}
```

`ReplicationMetrics` (по образцу `OwnS3Metrics`, Meter "ownS3"): инструменты таблицы 3.8 — имена: `ownS3.replication.link.online{target}`, `ownS3.replication.queue.depth{target}`, `ownS3.replication.delivery.lag.seconds{target}`, `ownS3.replication.notified.total{target,result}`, `ownS3.replication.retries.total{target}` (экспорт → `ownS3_replication_*`). Счётчики исходов приёма (applied/skipped/conflict/fetched) класс получит в Task 12 (реализация `IOutcomeCounters`); inbox остаётся на no-op-заглушке до wiring'а Task 12.

**Вход:** Task 4, 5, 7.

**Действие:** dispatcher + метрики-класс; тесты — фейковый `IReplicationTransport` + `FakeTimeProvider`-подобный контроль времени (если пакета нет — собственный `ManualTimeProvider` в тестах: маленький класс, TimeProvider-наследник с ручным сдвигом).

- [ ] **Шаг 1. Тесты (fail):**
  - окно: журнал 100 событий, окно 64 → первый батч ≤64; acceptedToSeqNo=64 → второй батч 36; acceptedToSeqNo=100 → тишина (нет отправок сверх окна);
  - 429 (null) → повтор от того же watermark (at-least-once), эксп-бэкофф растёт 1→2→4…, после успеха сброс;
  - повторная отправка неподтверждённого батча после «рестарта» (новый dispatcher на тех же journal/cursors) — с того же watermark;
  - новый сосед (курсор -1) → первая отправка от LastSeqNo (конец журнала);
  - метрики: queue_depth = LastSeqNo − watermark; delivery_lag = now − modTime старейшего неподтверждённого.
- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(replication): notifier per-target — окно in-flight, acceptedToSeqNo, эксп-бэкофф, метрики доставки`.

**Выход:** рассылка уведомлений с backpressure (3.6).

**Проверка:** юниты `FullyQualifiedName~NotificationDispatcher` зелёные.

**Spec:** 3.6, 3.8 (исходящие метрики), Ф5.

---

## Task 12: Фетчер per-source, цикл обслуживания, маркер чистого завершения, координатор (Ф5)

**Files:**
- Create: `src/OwnS3.Replication/Fetching/ObjectFetcher.cs`, `src/OwnS3.Replication/ReplicationState.cs`, `src/OwnS3.Replication/ReplicationMaintenance.cs`, `src/OwnS3.Replication/ReplicationCoordinator.cs`
- Modify: `src/OwnS3.App/Program.cs` (AddHostedService<ReplicationCoordinator>), `OwnS3.Replication/ReplicationMetrics.cs` (+fetch-серии и реализация `IOutcomeCounters` — источник данных для снапшота статуса Task 14; wiring: координатор подменяет no-op-заглушку inbox'а (Task 9) на боевой ReplicationMetrics; подписка на `ReplicaApplier.ConflictDetected` — ТОЛЬКО лог-событие конфликта, БЕЗ инкремента метрики: конфликт считается по результатам применения в точках inbox/фетчера)
- Test: `src/tests/OwnS3.UnitTests/Replication/{ObjectFetcherTests,ReplicationStateTests,ReplicationMaintenanceTests}.cs`

**Interfaces:**
- Consumes: `FetchQueueRegistry`, `NotificationInbox` (Task 9), `IReplicationTransport`, `ReplicaApplier`, `IReplicaStore`, `TombstoneStore`, `OutboxJournal`, `TargetCursors`, `ReplicationTargets`.
- Produces:

```csharp
namespace OwnS3.Replication;

// Реализация IOutcomeCounters (контракт определён в Task 9 вместе с inbox'ом):
// ReplicationMetrics — внутренние long-счётчики per-source, питают серии
// ownS3.replication.applied.total{source,result}, fetched.total, conflicts_total
// и снапшот статуса (Task 14). No-op-заглушка (без серий) стоит в inbox'е до wiring'а
// этой задачи — координатор подменяет её на боевой инстанс при старте.

namespace OwnS3.Replication.Fetching;

// Фетчер per-source: очередь → (disk-free ≥ DiskFreeMinBytes? нет → пауза: метрика
// fetch.paused{source}=1, ожидание улучшения/поллинг 5с) → FetchObjectAsync →
// 200-объект: sha-сверка на лету делегирована ApplyObjectAsync (expectedSha из заголовка);
// tombstone → ReplicaApplier.ApplyDelete; 404 → ничего. Ошибка → бэкофф 1с→60с.
// Rate: MaxRateBytes (0=безлимит) — троттлинг чтения Stream (TokenBucket). Parallelism:
// N воркеров над одной очередью (порядок не гарантируется, LWW-идемпотентен).
// Исходы пишет в IOutcomeCounters ПО РЕЗУЛЬТАТУ СВОЕГО применения (не по общему
// событию аплайера — оно без sourceName и мультиплится на per-source фетчеры):
// applied/skipped — ReplicaApplyResult.Applied, conflict — ReplicaApplyResult.Conflict,
// fetched — успешное тело.
public sealed class ObjectFetcher(TargetRecord source, FetchQueue queue, IReplicationTransport transport,
    ReplicaApplier applier, IOutcomeCounters counters, Func<long> getFreeDiskBytes,
    ReplicationOptions options, TimeProvider timeProvider, ILogger<ObjectFetcher> logger)
{
    public Task RunAsync(CancellationToken ct);
    public bool Paused { get; }
    public Task DrainAsync(CancellationToken ct); // доработать текущую очередь до пустоты (graceful)
}

// .owns3.sys/replication/state.json {"shutdownClean": bool}
public sealed class ReplicationState(string path)
{
    public bool WasCleanShutdown();      // читает при старте; отсутствующий/битый → false (пустой том — тоже false: стартовый reconcile покрывает и триггер «пустой том», 3.7)
    public void MarkDirty();             // при старте — сразу
    public void MarkCleanAndPersist();   // после drain + fsync-точки (graceful stop)
}

namespace OwnS3.Replication;

// Цикл обслуживания журнала и tombstones (канон 07 §4/§7/§8-а; spec 3.3/3.5/3.9).
// Отдельный класс — тестируем без хоста; координатор зовёт RunMaintenancePassAsync
// периодически (60 с) и однократно при старте.
public sealed class ReplicationMaintenance(OutboxJournal journal, TargetCursors cursors,
    ReplicationTargets targets, TombstoneStore tombstones, ReplicationOptions options,
    TimeProvider timeProvider, ILogger<ReplicationMaintenance> logger)
{
    // 1) штатный тримминг: Trim(min(watermark приёма АКТИВНЫХ соседей)); соседей нет → Trim(long.MaxValue-1)
    //    только при активной репликации (иначе журнал и так пуст);
    // 2) если journal.BudgetExceeded → ForceTrimToBudget() + лог-событие
    //    «потеряны уведомления для отстающих соседей (принудительный тримминг)» (алерт-метрика уже в журнале);
    // 3) retention-чистка tombstones: Cleanup(now, options.TombstoneRetentionHours) + лог при удалённых >0.
    public Task RunMaintenancePassAsync(CancellationToken ct);
}

// Hosted-сервис App: старт — targets.Snapshot → по соседу: NotificationDispatcher + ObjectFetcher;
// TargetsChanged → остановить исполнителей убранных/запустить добавленных (курсоры сохраняются);
// цикл обслуживания — ReplicationMaintenance.RunMaintenancePassAsync при старте и таймером 60 с;
// graceful stop (StopAsync): drain fetch-очередей (бюджет 30с), cursors.Flush(),
// state.MarkCleanAndPersist(); стартовый reconcile при WasCleanShutdown()==false —
// хук в ReconcileRunner (Task 13, вызов из старта).
public sealed class ReplicationCoordinator(...) : BackgroundService { ... }
```

disk-free: `new DriveInfo(dataRoot).AvailableFreeSpace` (single-режим — корень тома; EC — min по дискам).

**Вход:** Task 9, 11 (метрики-класс существует).

**Действие:** фетчер, стейт, maintenance, координатор; ReplicationMetrics: fetch-серии `ownS3.replication.fetch.{queue.depth,lag.seconds,paused}{source}`, `ownS3.replication.fetched.total{source}`, `ownS3.replication.applied.total{source,result}`, `ownS3.replication.conflicts_total` + реализация `IOutcomeCounters` (внутренние long-счётчики per-source); wiring координатора: боевой metrics в inbox (замена no-op-заглушки) и фетчеры; подписка `ReplicaApplier.ConflictDetected` — только лог-событие конфликта (R4: факт слияния журналируется), БЕЗ инкремента метрики. Счётчики исходов инкрементируются ТОЛЬКО точками применения по результату своего вызова, каждый со своим sourceName: NotificationInbox (самодостаточные события → applied; LWW-предфильтр/дедуп-повтор → skipped; равные modTime с разными sha → conflict по компаратору) и ObjectFetcher (`ReplicaApplyResult`: Applied → applied, !Applied → skipped, Conflict → conflict; успешное тело → fetched). Task 14 снапшотит эти long-счётчики для статуса.

- [ ] **Шаг 1. Тесты (fail):**
  - фетч-применение: фейк-транспорт отдаёт тело+заголовки → ApplyObject вызван с исходными метаданными; sha-мисматч (заголовок ≠ тела) → повтор (бэкофф), локально не применено;
  - tombstone-ответ → ApplyDelete; 404 → пропуск без изменений;
  - пауза: getFreeDiskBytes возвращает < min → фетчи не идут, Paused==true; место появилось → продолжение с того же элемента очереди;
  - drain: очередь 3 элемента, транспорт быстрый → DrainAsync завершается пустой очередью; MarkCleanAndPersist → WasCleanShutdown()==true у нового экземпляра;
  - стейт: отсутствие файла → WasCleanShutdown()==false;
  - счётчики исходов (IOutcomeCounters-заглушка-счётчик в тестах фетчера): применение → Applied+Fetched растут; skip-ветка (локально новее) → Skipped растёт, Applied нет; конфликт-ветка (равные modTime, разные sha — победитель по sha) → Conflict растёт по флагу `ReplicaApplyResult.Conflict` (событие ConflictDetected при этом — только лог);
  - **цикл обслуживания** (`ReplicationMaintenanceTests`):
    - сосед ACKнул seqNo 2 (cursors), сегмент 1..2 закрыт → `RunMaintenancePassAsync` → сегмент удалён (штатный тримминг по min watermark активных соседей);
    - два соседа: min(watermark) удерживает сегмент, пока отстающий не ACKнул → после его ACK тримминг проходит;
    - `BudgetExceeded==true` (малый бюджет, наполненный журнал, сосед offline) → проход делает ForceTrimToBudget (TotalBytes в бюджете) и пишет лог-событие принудительного тримма (проверка через IEnumerable-хук логгера/ILogger-фейк);
    - tombstone старше retention (задан options.TombstoneRetentionHours) удалён проходом, свежий остался.
- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(replication,app): фетчер per-source (rate/parallelism/пауза по месту), счётчики исходов IOutcomeCounters (inbox+fetcher), цикл обслуживания (тримминг журнала/бюджет/retention tombstones), маркер чистого завершения, ReplicationCoordinator (drain/graceful)`.

**Выход:** доставка данных приёмником с backpressure по диску; счётчики исходов инкрементируются во всех точках применения (включая WAF-режим без фетча — inbox); штатные чистки журнала/tombstones назначены фоновому циклу; graceful/грязный рестарт (3.3/3.5/3.9).

**Проверка:** юниты `FullyQualifiedName~Replication` зелёные; WAF-серия Api зелёная (координатор не мешает: без соседей — неактивен).

**Spec:** 3.5, 3.3 (тримминг/бюджет), 3.9 (hosted-чистки), 3.1/3.8 (счётчики исходов приёма), потоки 4/9, Ф5.

---

## Task 13: Reconcile / backfill (Ф5)

**Files:**
- Create: `src/OwnS3.Replication/Reconcile/ReconcileRunner.cs`
- Modify: `src/OwnS3.Replication/ReplicationCoordinator.cs` (триггеры), `ReplicationMetrics.cs` (+reconcile.position), `src/OwnS3.App/Handlers/ReplicationHandlers.cs` (ветка listing без bucket)
- Test: `src/tests/OwnS3.UnitTests/Replication/ReconcileRunnerTests.cs`

**Interfaces:**
- Produces:

```csharp
namespace OwnS3.Replication.Reconcile;

// Гарантийный контур: постраничный ListAsync соседа → LWW-сравнение со своей записью
// (ReadKeyState) → старее/отсутствует → fetch-задача в очередь источника
// (FetchQueueRegistry.ForSource(sourceName).TryEnqueue) → новее → ничего.
// Маркер reconcile/<sourceName>.marker (файл, персист после каждой страницы).
// Backfill состава бакетов: ListingBucketsAsync соседа → недостающие ApplyBucketCreate
// с CreationDate соседа (только в сторону «у соседа есть, у нас нет»).
public sealed class ReconcileRunner(ReplicationTargets targets, IReplicaStore store, ReplicaApplier applier,
    FetchQueueRegistry queues, IReplicationTransport transport, string reconcileDir,
    ReplicationOptions options, TimeProvider timeProvider, ILogger<ReconcileRunner> logger)
{
    public Task RunForSourceAsync(string sourceName, CancellationToken ct); // один проход; идущий — await
    public bool IsRunning(string sourceName);                               // параллельный не создаётся
    public void Trigger(string sourceName);                                 // админ/событие
    public long ProcessedKeys(string sourceName);                           // метрика reconcile.position
}
```

Триггеры координатора — ВСЕ ПЯТЬ канона 07 §9 (spec 3.7), явно:
1. добавление соседа (`TargetsChanged`: reconcile по НОВОМУ соседу);
2. старт на пустом томе при существующем составе — реализуется стартовым reconcile: на пустом томе нет `state.json` → `WasCleanShutdown()==false` → стартовый проход по всем соседям (код-комментарий в `ReplicationCoordinator` фиксирует это покрытие явно);
3. грязная остановка (`WasCleanShutdown()==false`) — стартовый проход по всем соседям;
4. периодически `ReconcileIntervalHours` (таймер координатора);
5. админ-команда `POST /admin/replication/reconcile` (Task 14 → `Trigger` по всем).

`GET /replication/listing` без `bucket` (или пустой) возвращает `{buckets:[{name, creationDateUnixMs}]}` — состав бакетов для backfill; с `bucket` — постраничный листинг ключей. Транспорт — `ListingBucketsAsync` (Task 7), ветка добавляется в `ReplicationHandlers`.

**Вход:** Task 12.

**Действие:** runner + ветка listing-без-bucket + подключение триггеров в координатор.

- [ ] **Шаг 1. Тесты (fail):**
  - полный проход: фейк-транспорт листит 3 ключа (1 отсутствует локально, 1 старее, 1 новее) → fetch-задачи для 2, новее не тронут; после прогона (фетчер-фейк применяет) — недостающий появился;
  - маркер: проход по 2 страницы → маркер = nextMarker первой; рестарт runner'а продолжает со второй;
  - IsRunning — повторный Trigger при идущем проходе не порождает второй;
  - backfill бакетов: сосед имеет бакет, локально нет → бакет создан с CreationDate соседа.
- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(replication): reconcile/backfill — листинг+LWW+маркер, все 5 триггеров (вкл. пустой том через стартовый reconcile), сверка состава бакетов (листинг без bucket)`.

**Выход:** гарантийный контур сходимости (3.7, R4).

**Проверка:** юниты Replication зелёные.

**Spec:** 3.7 (все 5 триггеров), A4/A5/A10/A11-механика, Ф5.

---

## Task 14: Админ-грань replication — targets/status/reconcile/sync-report + снапшот счётчиков (Ф5)

**Files:**
- Create: `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs`
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (админ-ветка: `/admin/replication/*`), `src/OwnS3.App/Program.cs` (DI), `src/OwnS3.Replication/ReplicationMetrics.cs` (+снапшот-метод `SnapshotCounters` — представление счётчиков, заведённых Tasks 11–12, для status-JSON)
- Test: `src/tests/OwnS3.IntegrationTests/Api/ReplicationAdminScenarios.cs` (WAF)

**Interfaces:**
- Consumes: `ReplicationTargets`, `TargetCursors`, `OutboxJournal`, `ReconcileRunner`, координатор (Task 12), `ReplicationMetrics` (класс существует с Task 11; счётчики исходов наполняются с Task 12 во всех точках применения — inbox И фетчер).
- Produces:

```csharp
// В ReplicationMetrics (добавляется ЗДЕСЬ — status-хендлеру нужен готовый снапшот):
// чтение внутренних long-счётчиков, которые с Task 11/12 питают серии
// notified_total{result}/fetched_total/applied_total{result}/conflicts_total.
public sealed record SourceCounters(long NotifiedAccepted, long NotifiedBackoff, long NotifiedRetry,
    long Fetched, long Applied, long Skipped, long Conflict);
public SourceCounters SnapshotCounters(string neighborName); // neighborName фигурирует и как {target}, и как {source}
```

Эндпоинты (SigV4, права admin/root — прочим 403):
  - `PUT /admin/replication/targets` — тело `{targets:[{name, endpoint, accessKey, secretKey}]}`; валидация (имена уникальны, endpoint `scheme://host:port`); `ReplicationTargets.Replace` (персист tmp+rename) → координатор перестраивает исполнителей на лету; ответ **202 Accepted** (идемпотентен).
  - `GET /admin/replication/status` — JSON (spec 3.1: стейт + счётчики исходов): `active`, `outbox: {bytes, budgetBytes, budgetExceeded, lastSeqNo}` и по каждому соседу `{name, online, acceptedSeqNo, queueDepth, deliveryLagSeconds, fetchQueueDepth, fetchLagSeconds, fetchPaused, reconcileRunning, reconcilePosition, counters: {notifiedAccepted, notifiedBackoff, notifiedRetry, fetched, applied, skipped, conflict}}` — counters из `ReplicationMetrics.SnapshotCounters`.
  - `POST /admin/replication/reconcile` — `ReconcileRunner.Trigger` по всем соседям; **202** (идущий продолжается, параллельный не создаётся).
  - `GET /admin/replication/sync-report?bucket=&prefix=&marker=` — JSON `{bucket, prefix, marker, firstKey, firstModTime, lastKey, lastModTime, count, totalSize, aggregateHash}`: листинг СВОЕГО тома от marker (до 1000 ключей, prefix-фильтр), `aggregateHash` = XOR 64-бит FNV-1a пар (key, modTimeUnixMs) — порядко-независимая свёртка (метод `SyncReportHash` в Replication-моделях).

**Вход:** Task 12, 13.

**Действие:** снапшот-метод в ReplicationMetrics + хендлеры + расширение admin-ветки middleware (список ресурсов `/admin/replication/{targets,status,reconcile,sync-report}`, права admin — существующая проверка admin-ветки уже отсекает не-admin; replication-ключ на админ-грани → 403 — покрыть тестом).

- [ ] **Шаг 1. WAF-тесты (fail):**
  - PUT targets: 202; файл targets.json персистился (перечитан); повторный PUT того же списка — 202 без побочных эффектов; невалидное тело (дубль имени) → 400;
  - GET status: без соседей `active:false`; с соседом и принятым батчем — структура полей, ВКЛЮЧАЯ счётчики исходов. Ассерты достижимы в WAF-конфигурации с фиктивным соседом БЕЗ живого фетча — точки инкремента Task 12: приём батча с самодостаточным событием (bucket-create) → `counters.applied ≥ 1` (инкремент в NotificationInbox); приём object-put со старым modTime → `counters.skipped ≥ 1` (LWW-предфильтр inbox); фетчер-точки в этом тесте не требуются (наполняются в mesh-E2E Task 16–20);
  - POST reconcile → 202; повтор → 202 (без второго прохода — IsRunning);
  - sync-report: N ключей → count/totalSize/first/last/aggregateHash; ДВА набора с одинаковыми (key,modTime) в РАЗНОМ порядке вставки (два бакета/две фабрики) → aggregateHash равен (порядко-независимость); с marker — страница от маркера;
  - права: replication-ключ на все 4 эндпоинта → 403; writer → 403 (A13-зеркало).
- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(app): админ-грань replication — targets(202,hot-apply)/status(стейт+счётчики исходов через SnapshotCounters)/reconcile(202)/sync-report(XOR-hash)`.

**Выход:** управление составом mesh без даунтайма + сверка листингов (3.1, 3.8); счётчики исходов наблюдаемы в статусе.

**Проверка:** WAF `Api.ReplicationAdmin` зелёные; вся Api-серия зелёная.

**Spec:** 3.1 (админ-грань, статус со счётчиками исходов), 3.8 (sync-report), A9, Ф5.

---

## Task 15: Метрики репликации — полный словарь + экспорт (Ф5)

**Files:**
- Modify: `src/OwnS3.Replication/ReplicationMetrics.cs` (доведение до полной таблицы 3.8 — серии Tasks 11–13 уже существуют, добавляются недостающие gauge/инструменты и ObservableGauge-обвязка), `src/OwnS3.App/Program.cs` (инстанцирование при активной репликации по образцу `OwnS3EcMetrics`), финальный wiring во всех компонентах
- Test: `src/tests/OwnS3.IntegrationTests/Api/MetricsReplicationScenarios.cs` (WAF: /metrics содержит серии)

**Interfaces:**
- Produces: все 14 метрик таблицы 3.8 в экспорте Prometheus (`ownS3_replication_*`, словарь имён — spec 3.8 дословно): `link_online{target}`, `queue_depth{target}`, `delivery_lag_seconds{target}`, `notified_total{target,result}`, `fetch_queue_depth{source}`, `fetch_lag_seconds{source}`, `fetch_paused{source}`, `fetched_total{source}`, `applied_total{source,result}`, `retries_total{target}`, `conflicts_total`, `reconcile_position{source}`, `outbox_bytes`, `outbox_budget_exceeded`. Снапшот счётчиков для статуса уже введён Task 14 (`SnapshotCounters`) — эта задача его не меняет.

**Вход:** Task 11–14 (компоненты-источники и снапшот существуют).

**Действие:** довести ReplicationMetrics до полного словаря (ObservableGauge + Counter по образцу OwnS3Metrics), подключить во все компоненты, экспорт при `targets.Active` (неактивна — серии отсутствуют, Existing-регресс).

- [ ] **Шаг 1. WAF-тест (fail):** с соседом в конфиге после PUT-а (событие) и приёма батча /metrics содержит `ownS3_replication_outbox_bytes`, `ownS3_replication_queue_depth`, `ownS3_replication_applied_total`; без соседей — ни одной `ownS3_replication_`-серии (регресс).
- [ ] **Шаг 2–4.** FAIL → реализация → PASS.
- [ ] **Шаг 5.** Коммит: `t35(replication): полный словарь метрик 3.8 (лаги уведомлений и данных раздельно, конфликты, бюджет) — экспорт при активной репликации`.

**Выход:** честная наблюдаемость (R5, принцип 10); счётчики исходов — в /metrics (здесь) и в статусе (Task 14).

**Проверка:** WAF-метрики-сценарий зелёной; вся Api-серия зелёная.

**Spec:** 3.8, R5, A7-механика, Ф5.

---

## Task 16: Mesh-E2E контур (окружение) (Ф6)

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/OwnS3MeshE2eEnvironment.cs`, `MeshClients.cs`
- Modify: — (образ уже собирается `OwnS3E2eImage.BuildOnceAsync` — переиспользовать)

**Interfaces:**
- Consumes: `OwnS3E2eImage` (сборка owns3:e2e), каноны `docs/e2e-isolation.md`/`docs/e2e-launch.md`.
- Produces (для Task 17–20):

```csharp
namespace OwnS3.IntegrationTests.E2e.Replication;

// Контур mesh: сеть os3m-en-{runId}, N (2–5) контейнеров os3m-n{i}-{runId} (образ owns3:e2e),
// тома os3m-v{i}-{runId}, динамические хост-порты (зонд FreePort), OWNS3_HOST_ID=os3m-{i}-{runTag},
// root-пара + replication-ключ (единый peer-комплект контура) + writer-ключ.
// targets через env: полный mesh (каждый знает всех, endpoint=http://os3m-n{j}-{runId}:9000),
// либо пусто — конфигурируется StartAsync'ом (параметр targets: full-mesh | none + configureNode).
// Телеметрия: [PHASE]/phases.log/логи+inspect всех контейнеров ДО удалений; MarkFailed —
// остановить без удаления + README-cleanup.txt; teardown при любом исходе: контейнеры →
// тома (ретраи ≤30с) → сеть → АССЕРТ ЧИСТОТЫ по runId (контейнеры/тома/сети).
// Retry старта ×3 (полное удаление попытки). Wait /healthz ≤30с на каждый узел.
public sealed class OwnS3MeshE2eEnvironment : IAsyncDisposable
{
    public static Task<OwnS3MeshE2eEnvironment> StartAsync(string slug, int nodes,
        Action<int, ContainerBuilder>? configureNode = null, bool fullMeshTargets = true,
        CancellationToken ct = default);
    public static Task RunScenarioAsync(string slug, int nodes,
        Func<OwnS3MeshE2eEnvironment, Task> body, ...);             // телеметрия-обёртка (прецедент OwnS3E2eEnvironment)
    public string NodeName(int i);                                   // os3m-n{i}-{runId}
    public string NodeHostUrl(int i);                                // http://localhost:{dynamicPort} (клиенты с хоста)
    public string NodeInternalUrl(int i);                            // http://os3m-n{i}-{runId}:9000 (для targets)
    // Сетевые изоляции (A2/A4): disconnect/reconnect контейнера от сети окружения.
    public Task DisconnectNodeAsync(int i, CancellationToken ct);    // docker network disconnect {net} {container}
    public Task ReconnectNodeAsync(int i, CancellationToken ct);
    // Управление контейнером (A1/A11): docker stop/start (не remove!). graceSec — docker stop -t
    // (дефолт 10; сценарий A11(б) передаёт 30, чтобы graceful-drain уложился до SIGKILL — в пределах канона ≤30 с).
    public Task StopNodeAsync(int i, int graceSec = 10, CancellationToken ct = default);
    public Task StartNodeAsync(int i, CancellationToken ct);
}

// MeshClients: клиентские операции против узла — AWS SDK (IAmazonS3 на NodeHostUrl,
// ForcePathStyle, per-node клиент) + raw-SigV4-клиент админ-грани (подпись
// TestSigV4Signer — уже линкуется в IntegrationTests).
public static class MeshClients
{
    public static Task PutObjectAsync(int node, string bucket, string key, byte[] body, ...);   // SDK
    public static Task<byte[]> GetObjectAsync(int node, string bucket, string key, ...);        // SDK
    public static Task DeleteObjectAsync / DeleteObjectsAsync / InitiateMultipart+UploadPart+CompleteAsync(int node, ...);
    public static Task<HttpResponseMessage> AdminPutTargetsAsync(int node, IReadOnlyList<TargetRecord> targets, ...);
    public static Task<string> AdminGetStatusAsync(int node, ...);          // JSON-тело статуса
    public static Task AdminReconcileAsync(int node, ...);                  // POST 202
    public static Task<string> AdminSyncReportAsync(int node, string bucket, string? marker, ...); // JSON отчёт
    public static Task<string> GetMetricsAsync(int node, ...);              // /metrics текст (парсинг серий — хелпером FindMetric(text, name, labels))
}
```

**Вход:** Task 15; образ собирается; `OwnS3E2eCollection`-паттерн (новая коллекция `owns3-mesh-e2e`, DisableParallelization).

**Действие:** окружение + клиенты; env-переопределения per-node (параметр `configureNode(i, builder)` даёт сценарию `WithEnvironment(...)` до старта — для A10/A14).

- [ ] **Шаг 1.** Реализовать окружение (это инфраструктурный код — smoke-тест = Шаг 2).
- [ ] **Шаг 2. Smoke-сценарий (первый E2e.Replication-тест):** `MeshSmokeScenarios`:
  - контур 2 узла; PUT в узел-0 (SDK) → поллинг (шаг ≤5 с в первых циклах; сходимость — ≤5 мин поллингом с шагом ≤30 с): GET с узла-1 отдаёт объект (доставка уведомление→фетч);
  - teardown-ассерты чистоты (в DisposeAsync) — зелёные.
- [ ] **Шаг 3.** Прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release --filter FullyQualifiedName~E2e.Replication.MeshSmoke` → PASS; после серии — контрольная зачистка: `docker ps -a --filter name=os3` (пусто), `docker network ls | grep -c os3m` (0; при остатках — `docker network prune -f`, разобраться по логам).
- [ ] **Шаг 4.** Коммит: `t35(e2e): mesh-контур OwnS3MeshE2eEnvironment (2-5 узлов, изоляция guid, динамические порты, полный teardown+чистота, network-изоляции) + smoke`.

**Выход:** изолированный E2E-контур mesh (Ф6-база).

**Проверка:** smoke-сценарий зелёной на Release-docker; чистота после серии.

**Spec:** 7 (методика), Ф6.

---

## Task 17: E2E R1/R3 — запись без штрафов (A1, A2, A3)

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/WriteIsolationScenarios.cs` (коллекция `owns3-mesh-e2e`)

**Вход:** Task 16.

**Действие (все — `RunScenarioAsync`, тайминги Stopwatch, AAA):**
- **A1** `[Theory(InlineData(2) / 3 / 5)]`: поднять контур; stop docker-контейнеров ВСЕХ соседей узла-писателя (`StopNodeAsync` — TARGET-природа отказа); серия 10 PUT (SDK, ~1 КБ) в узел-писатель → суммарно ≤1 с, все 200, 0 блокировок (каждый PUT — свой HTTP с таймаутом 5 с; ни один не превысил);
- **A2**: контур 3; DISCONNECT писателя от сети (`DisconnectNodeAsync(0)` — изолят, TIMEOUT-природа для исходящих); серия 10 PUT в узел-0 → ≤1 с; reconnect в finally-ветке сценария;
- **A3**: узел изолирован (тот же disconnect): `/healthz` ≤1 с (несколько проб), GET ранее записанного объекта работает, PUT+GET новых работают.

- [ ] **Шаг 1.** Написать сценарии (каждый — свой контур: Theory-кейс = отдельный запуск `RunScenarioAsync`).
- [ ] **Шаг 2.** Прогон `--filter FullyQualifiedName~E2e.Replication.WriteIsolation` → PASS; телеметрия: фазы >60 с имеют `[PHASE]`-отчёт.
- [ ] **Шаг 3.** Зачистка серии (п. Шага 3 Task 16) + коммит: `t35(e2e): A1-A3 — 10 PUT ≤1с при stop целей и сетевом изоляте писателя; S3-грань изолированного жива (R1/R3)`.

**Выход:** приёмка R1/R3 автоматизирована.

**Проверка:** серия зелёная на Release-docker; бюджеты таймингов в ассертах.

**Spec:** 7 (A1–A3), R1/R3, Ф6.

---

## Task 18: E2E сходимость — раскол/сращение, LWW-конфликт, метрики, удаления (A4, A6, A7, A8)

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/ConvergenceScenarios.cs`

**Вход:** Task 16, 17.

**Действие:**
- **A4**: контур 4; записать данные; раскол пополам (disconnect n0,n1 от сети — попарная изоляция половин); в обеих половинах — записи (взаимные расхождения); срастание (reconnect всех); поллинг сходимости ≤5 мин (шаг ≤30 с): GET каждого ключа на ВСЕХ узлах равен; БЕЗ рестартов контейнеров;
- **A6**: в расколе PUT одного ключа в обе половины (разные тела); срастание → все узлы отдают ОДИНАКОВОЕ тело (LWW-победитель); в логах обеих сторон (docker logs) есть запись конфликта (grep по ключу/«conflict»); метрика `ownS3_replication_conflicts_total` ≥1 на /metrics обоих;
- **A7**: при живом расхождении (до сращения): `/metrics` источника показывает `ownS3_replication_delivery_lag_seconds > 0` и/или `queue_depth > 0`; приёмника — `ownS3_replication_fetch_lag_seconds > 0`/`fetch_queue_depth > 0` (по обстоятельствам сценария); `sync-report` пары (bucket) — aggregateHash расходится; после догона — совпадает;
- **A8**: DELETE одного ключа + DeleteObjects пачки на узле-0 → поллинг ≤5 мин: ключей нет на всех узлах (GET 404); tombstone-поведение: после повторной доставки старого PUT (вручную POST /replication/notifications с старым modTime через peer-кред) ключ НЕ воскрешён.

- [ ] **Шаг 1.** Сценарии.
- [ ] **Шаг 2.** Прогон `--filter FullyQualifiedName~E2e.Replication.Convergence` → PASS.
- [ ] **Шаг 3.** Зачистка серии + коммит: `t35(e2e): A4/A6/A7/A8 — срастание ≤5мин без рестартов, LWW-конфликт журналируется с обеих сторон, раздельные лаги+sync-report, tombstone-сходимость (R4/R5)`.

**Выход:** приёмка R4/R5-сходимости.

**Проверка:** серия зелёная; ассерты таймингов/метрик конкретны.

**Spec:** 7 (A4, A6–A8), R4/R5, Ф6.

---

## Task 19: E2E членство и рестарты (A5, A9, A11)

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/MembershipScenarios.cs`

**Вход:** Task 16, 18.

**Действие:**
- **A5**: контур 2 с данными (записать в n0; n1 стартует БЕЗ targets); `PUT /admin/replication/targets` на n1 (сосед n0) и на n0 (сосед n1) — живой контур; backfill: n1 забирает существующее (поллинг ≤5 мин); во время backfill PUT в n0 продолжаются без остановки (серия PUT параллельно поллингу — тайминги не деградируют: каждый ≤1 с);
- **A9**: контур 4 (4-й узел поднят без targets); PUT targets добавляет его на трёх живых → применяется без рестартов; S3-грань жива во время применения (параллельные GET 200); новый узел получает backfill; вывод узла: PUT targets без него → его notifier'ы остановлены (статус online=false), запись на оставшихся работает;
- **A11**: три ветки:
  - (а) рестарт ИСТОЧНИКА (docker stop/start n0 при наполненном outbox, сосед жив) → до-рассылка, дедуп гасит повторы, сходимость;
  - (б) чистый рестарт ПРИЁМНИКА: ПРЕДУСЛОВИЕ — приёмник синхронизирован (статус `fetchQueueDepth == 0` перед рестартом — drain мгновенный); `StopNodeAsync(i, graceSec: 30)` (docker stop -t 30 — graceful-drain успевает до SIGKILL, в пределах канона ≤30 с) → start → без стартового reconcile (state.json clean; проверка: данные на месте, потерь нет);
  - (в) грязный рестарт ПРИЁМНИКА: пауза фетчей через огромный `OWNS3_REPLICATION_DISK_FREE_MIN_BYTES` на приёмнике (configureNode) → очередь копится → `docker kill` (SIGKILL, без drain) → перезапуск с нормальным disk-free → дыры закрыты СТАРТОВЫМ reconcile (грязный маркер) ≤5 мин.

- [ ] **Шаг 1.** Сценарии.
- [ ] **Шаг 2.** Прогон `--filter FullyQualifiedName~E2e.Replication.Membership` → PASS.
- [ ] **Шаг 3.** Зачистка + коммит: `t35(e2e): A5/A9/A11 — backfill нового узла, hot-targets без даунтайма, рестарты источника/приёмника (drain vs крах→стартовый reconcile) (R2/R4/R6)`.

**Выход:** приёмка R6 (состав) и R2 (рестарты).

**Проверка:** серия зелёная.

**Spec:** 7 (A5, A9, A11), R2/R4/R6, Ф6.

---

## Task 20: E2E границы — бюджет, большие объекты, изоляция peer-доступа, место (A10, A12, A13, A14)

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/BoundaryScenarios.cs`

**Вход:** Task 16, 19.

**Действие:**
- **A10**: контур 2, у источника `OWNS3_REPLICATION_OUTBOX_BUDGET_BYTES=65536` (малый, через configureNode); сосед disconnect; серия PUT (~сотня событий) → запись не отказывает (все 200); `/metrics` источника: `ownS3_replication_outbox_budget_exceeded 1`; журнал удержан бюджетом (`outbox.bytes ≤` бюджет+активный сегмент — через admin/status); reconnect + `POST /admin/replication/reconcile` на приёмнике → сходимость ≤5 мин (уведомления потеряны триммингом, данные вернул reconcile); в логах источника — событие принудительного тримма;
- **A12**: контур 2; multipart 6 частей × 1 ГиБ нулей (SDK UploadPart из повторяемого стрима; ИТОГО >5 ГБ — превыше PutObject-лимита) в n0 → complete → поллинг ≤5 мин: n1 GET отдаёт полный размер, sha равен (клиентская сверка чанками);
- **A13**: контур 1 узел (mesh-min) с ключами ro/rw/admin/replication: каждый из трёх клиентских ключей на `/replication/notifications` → 403; replication-ключ на PutObject/ListObjects → 403;
- **A14**: контур 2, у ПРИЁМНИКА `OWNS3_REPLICATION_DISK_FREE_MIN_BYTES` больше фактического свободного места тома (например 1 ТиБ, через configureNode); PUT в источник + приём уведомлений (ACK идёт: статус источника queue_depth→0 / delivery_lag→0 после ACK) → фетчи на паузе (`ownS3_replication_fetch_paused 1`); запись источника не деградирует (серия 10 PUT ≤1 с); перезапуск приёмника с нормальным watermark → фетчи продолжаются, сходимость ≤5 мин.

- [ ] **Шаг 1.** Сценарии.
- [ ] **Шаг 2.** Прогон `--filter FullyQualifiedName~E2e.Replication.Boundary` → PASS (A12 — самый долгий: контроль фаз >60 с по телеметрии).
- [ ] **Шаг 3.** Зачистка + коммит: `t35(e2e): A10/A12/A13/A14 — bounded-бюджет+принудительный тримминг+reconcile, >5ГБ потоково с sha-сверкой, изоляция peer-политик, пауза по месту (R1/R5/R6)`.

**Выход:** приёмочная матрица 7 закрыта полностью.

**Проверка:** вся `E2e.Replication` серия зелёная одним прогоном; чистота после.

**Spec:** 7 (A10, A12–A14), Ф6.

---

## Task 21: Поставка — deploy/env/compose-инструкции + полный регресс (Ф7)

**Files:**
- Modify: `deploy/.env.example` (новая секция «Репликация ownS3» после блока OWNS3_*: все `OWNS3_REPLICATION_*` с дефолтами и комментариями; пример `OWNS3_REPLICATION_TARGETS__0__*`)
- Modify: `deploy/docker-compose.yml` (закомментированный блок-инструкция mesh-профиля по образцу `owns3-ec`: `owns3-m1`/`owns3-m2` — тот же образ `owns3:dev`, свои тома, `OWNS3_REPLICATION_TARGETS__0__*` друг на друга, профиль `owns3-mesh`; в базовый подъём НЕ входит)

**Вход:** Task 0–20.

**Действие:** env-секция + compose-инструкции (mesh-профиль закомментирован — подъем опционален, как `owns3-ec`); smoke-опции стенда НЕ добавляем (приёмка — docker-E2E; решение плана в рамках «по решению плана» спеки 3.10).

- [ ] **Шаг 1.** Внести правки deploy.
- [ ] **Шаг 2. Полный регресс (каждая серия — дождаться финальной строки, ЗАЧИСТКА между сериями):**
  1. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj -c Release` → зелёные;
  2. зачистка: `docker ps -a --filter name=os3` пусто, `docker network ls | grep os3` пусто;
  3. `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release` (ПОЛНЫЙ: Api + все E2e включая Replication и TLS-серии) → зелёные;
  4. зачистка (п.2) + `docker network prune -f` страховочно при осиротевших;
  5. мерж-гейт PgWorker-стека (канон задач, трогающих docker-контур): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` → зелёный;
  6. зачистка финальная.
- [ ] **Шаг 3.** Коммит: `t35(deploy): секция OWNS3_REPLICATION_* в .env.example + compose mesh-инструкции; полный регресс зелёный`.

**Выход:** поставка документирована; все серии зелёные.

**Проверка:** команды Шага 2 — зелёные финальные строки; чистота docker между/после.

**Spec:** 3.10, Ф7, Existing-регресс, мерж-гейты.

---

## Task 22: Сквозные измерения E2E — A15 (латентность репликации) + время восстановления связи (A4) (Ф6-дополнение)

> Доработка ПОВЕРХ исполненных Task 0–21 (код репликации и E2E-серии в ветке): измерительные шаги; рантайм-контракты не меняются. Зафиксированное отступление от первоначальной формулировки «рантайм не меняется»: в ходе реализации A15 вскрыта латентность poll-цикла фетчера — константа `DiskPausePoll` сокращена 5 с → 250 мс (ece30558/5e4fae93); контракт не менялся (интервал poll в arch не был канонизирован), порог A15 ≤5 с выполнен.

**Files:**
- Modify: `arch/owns3/07-replication.md` (§10 — новый абзац «Сквозные измерения в E2E» дословно из spec 3.8: не рантайм-метрики узлов — end-to-end латентность репликации и время восстановления связи после срастания измеряются самой тестовой серией, публикуются фактическими значениями `[METRIC]`-строками в журнале прогона по канону `docs/e2e-launch.md` (по образцу `[PHASE]`), попадают в артефакты телеметрии; пороги — раздел 7: A15 ≤5 с, A4 ≤60 с; `delivery_lag`/`fetch_lag` сверяются тестом со своими измерениями)
- Modify: `src/tests/OwnS3.IntegrationTests/E2e/Replication/OwnS3MeshE2eEnvironment.cs` (хелпер `LogMetricAsync(string name, string value)`: `[METRIC] name=<value>` в Console + дописывание в `metrics.log` ArtifactsDir — по образцу `LogPhaseAsync`)
- Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/ReplicationLatencyScenarios.cs` (A15, коллекция `owns3-mesh-e2e`)
- Modify: `src/tests/OwnS3.IntegrationTests/E2e/Replication/ConvergenceScenarios.cs` (кейс A4 — измерительные шаги поверх существующего сценария срастания)

**Interfaces:**
- Consumes: исполненные Task 16–20 — контур `OwnS3MeshE2eEnvironment` (`DisconnectNodeAsync`/`ReconnectNodeAsync`, ArtifactsDir), `MeshClients` (`PutObjectAsync`/`HeadObject`/`AdminGetStatusAsync`/`GetMetricsAsync`), рантайм-серии `ownS3_replication_delivery_lag_seconds` / `ownS3_replication_fetch_lag_seconds`.
- Produces: `OwnS3MeshE2eEnvironment.LogMetricAsync(name, value)` — телеметрия `[METRIC]` + файл `metrics.log` в артефактах прогона (для A15/A4 и будущих серий).

**Вход:** Tasks 0–21 исполнены (код в ветке, `E2e/Replication/*` существуют); spec расширен: A15, A4-измерение, 3.8-абзац, поток 5, Ф6, «Дополнительно — Сквозные измерения E2E».

**Действие:**
- **A15 — сквозная латентность (спокойный контур, `ReplicationLatencyScenarios`, 2 узла, AAA):** серия 10 PUT (~1 КБ, уникальные ключи) в узел-0; для каждого объекта: `t0` = момент 200 OK источника (Stopwatch от завершения `PutObjectAsync`); поллинг HeadObject узла-1 с шагом ≤1 с (бюджет 5 с, промежуточные проверки без sleep >1 с) до ответа с modTime/sha256 источника (Last-Modified/ETag равны зафиксированным в ответе PUT); `latencyMs = t_applied − t0`; `LogMetricAsync("repl-e2e-latency-ms", latencyMs)` по каждому объекту; по серии — `[METRIC] repl-e2e-latency-ms min/max/median=<v>` (три строки); ассерт: каждый объект ≤5000 мс (превышение — провал с разбором по логам, НЕ рост таймаута). Сверка честности R5: после серии поллинг (бюджет 10 с) до `ownS3_replication_delivery_lag_seconds == 0` (источник) и `ownS3_replication_fetch_lag_seconds == 0` (приёмник) — тестовое «применено» подтверждено рантайм-метриками; фактические значения лагов фиксируются `[METRIC] repl-runtime-delivery-lag-seconds=<v>` / `repl-runtime-fetch-lag-seconds=<v>`.
- **A4 — время восстановления связи (в существующем кейсе срастания `ConvergenceScenarios`):** в расколе — фиксация `ownS3_replication_link_online{target} == 0` у источника и снапшот `acceptedSeqNo` (AdminGetStatus); `t_heal` = момент завершения `ReconnectNodeAsync` (фактическое восстановление docker-сети); от `t_heal` — поллинг с шагом ≤5 с (бюджет 60 с) признака первого успешного репликационного взаимодействия: `link_online == 1` И `acceptedSeqNo` строго вырос сверх снапшота на t_heal (уведомление принято — watermark двинулся); вспомогательный материал разбора — docker-логи/inspect узлов (телеметрия окружения снимает их в ArtifactsDir ДО удалений); `LogMetricAsync("repl-recover-after-heal-ms", ms)`; ассерт ≤60000 мс (потолок бэкоффа `OWNS3_REPLICATION_BACKOFF_MAX_SEC` + запас; превышение — провал с разбором, НЕ увеличением таймаута). Существующие ассерты сходимости кейса A4 не меняются.

- [ ] **Шаг 1 (arch-first).** §10 главы 07 — абзац «Сквозные измерения в E2E»; коммит `t35(arch): 07 §10 — сквозные измерения E2E ([METRIC] repl-e2e-latency/repl-recover-after-heal, пороги A15 ≤5с / A4 ≤60с)`.
- [ ] **Шаг 2.** `LogMetricAsync` в `OwnS3MeshE2eEnvironment` (Console + `metrics.log` в ArtifactsDir, по образцу `LogPhaseAsync`).
- [ ] **Шаг 3.** Сценарий A15 `ReplicationLatencyScenarios` (серия PUT → пер-объектный поллинг → [METRIC] → min/max/median → ассерты → сверка лагов).
- [ ] **Шаг 4.** Измерительные шаги A4 в `ConvergenceScenarios` (снапшоты до раскола → t_heal → поллинг признака → [METRIC] → ассерт ≤60 с).
- [ ] **Шаг 5.** Прогон измерительных серий: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~E2e.Replication.ReplicationLatency|FullyQualifiedName~E2e.Replication.Convergence"` → PASS; контроль: `[METRIC]`-строки в выводе и `metrics.log` в `/tmp/pgw-e2e-artifacts-<guid>/`; зачистка серии (`docker ps -a --filter name=os3` пусто, `docker network ls | grep os3m` пусто). Затем — **полный регресс всей серии `E2e.Replication`** (Task 22 правит общие файлы тест-инфраструктуры — `OwnS3MeshE2eEnvironment` (база серий Task 17–20) и `ConvergenceScenarios`; критерий приёмки spec «Регресс: существующие серии зелёные» обязан выполняться в КОНЕЧНОМ состоянии ветки, полный прогон Task 21 Шаг 2 выполнен ДО Task 22; образец — Task 21 Шаг 2): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj -c Release --filter FullyQualifiedName~E2e.Replication` → зелёная финальная строка; зачистка после серии.
- [ ] **Шаг 6.** Коммит: `t35(e2e): сквозные измерения — A15 repl-e2e-latency-ms (≤5с, per-object+min/max/median, сверка delivery_lag/fetch_lag) + A4 repl-recover-after-heal-ms (≤60с, от reconnect до первого репликационного взаимодействия)`.

**Выход:** обе величины запроса (скорость репликации, скорость восстановления связи) измеряются тестовой серией и публикуются фактическими значениями в журнале прогона; пороги — ассерты (превышение = провал с разбором).

**Проверка:** обе измерительные серии зелёные; вся серия `E2e.Replication` зелёная в конечном состоянии ветки (полный регресс Шага 5); в артефактах прогона — `[METRIC] repl-e2e-latency-ms` (каждый объект + min/max/median), `repl-runtime-*-lag-seconds`, `repl-recover-after-heal-ms`; чистота docker после серий.

**Spec:** 7 (A15; A4 — измерение восстановления связи), 3.8 («Сквозные измерения в E2E»), 4-поток 5 (измерение времени восстановления), Ф6 (`[METRIC]`), «Дополнительно — Сквозные измерения E2E (телеметрия)», R5.

---

## Финальный гейт задачи (после Task 22, до запроса мержа)

1. Самопроверка покрытия приёмочной матрицы: A1–A14 ↔ Task 17–20, A15 ↔ Task 22, измерение времени восстановления в A4 ↔ Task 22 (таблица соответствия в PR-описании мержа).
2. Канон: arch-глава 07 смержена ВМЕСТЕ с кодом (arch-first в истории коммитов — Task 0 первый).
3. Roadmap: удаление тега `t35-owns3-replication` из `arch/roadmap/owns3.md` — мерж-коммитом в main (правила `arch/roadmap/README.md`).
4. Мерж в `main` — ТОЛЬКО по явной команде пользователя (базовые правила §7).

---

## Self-review плана (выполнен; обновлён по ревью Фазы 4 раунды 1–3 + расширение spec A15/A4-измерение → Task 22)

- **Покрытие spec:** 3.1→Task 3/14; 3.2→Task 10; 3.3→Task 4/5 (цикл обслуживания — Task 12 `ReplicationMaintenance`); 3.4→Task 7/9; 3.5→Task 6/12 (retention-чистка — цикл Task 12); 3.6→Task 11; 3.7→Task 13 (все 5 триггеров; «пустой том» — через стартовый reconcile по отсутствующему `state.json`, зафиксировано в каноне 07 §9 и код-комментарии координатора); 3.8→Task 11/15 + sync-report Task 14; 3.1 счётчики исходов в статусе→Task 14 (снапшот `SnapshotCounters` и WAF-ассерты counters — в Task 14; контракт точек инкремента `IOutcomeCounters` определён в Task 9, реализация/wiring — Task 12: inbox — самодостаточные applied / предфильтр skipped+conflict, фетчер — apply/skip/conflict/fetched по `ReplicaApplyResult`; ассерты Task 14 достижимы в WAF с фиктивным соседом через inbox-точки, инверсии порядка и вечного FAIL нет); 3.9→карта проектов (Task 3 создаёт проект, границы зависимостей соблюдены: Replication→Storage+Protocol, App стыкует; hosted-чистки журнала/tombstones — Task 12); 3.10→Task 21; потоки 4→Task 19; раздел 6 границы — Task 0 §12; раздел 7 A1–A14→Task 17–20, A15→Task 22, A4-измерение восстановления→Task 22 (расширение кейса срастания ConvergenceScenarios), 3.8-абзац «Сквозные измерения в E2E»→Task 22 (arch-шаг: 07 §10 + `[METRIC]`-телеметрия), Ф6 «включая сквозные измерения `[METRIC]`»→Task 22, «Дополнительно — Сквозные измерения E2E (телеметрия)»→Task 22, 3.9-строка IntegrationTests (вкл. сквозные измерения)→Task 22, поток 5 (время восстановления — измеряется)→Task 22; юнит-список из «Дополнительно»→Tasks 4–7/11–13, дедуп уведомлений — юнит `NotificationInboxTests` в Task 9; регресс→Task 21.
- **Типовая консистентность:** `ReplicationEvent` используется в журнале/транспорте; `IReplicaStore`-сигнатуры Task 2 потреблены Tasks 6/9/13; `NotificationAcceptedResponse.AcceptedToSeqNo` = `TargetCursors.Advance`-аргумент; `FetchTask` определён в Task 9 и потреблён Tasks 12/13 (очереди per-source через `FetchQueueRegistry`, `sourceName` резолвится по accessKey подписи — канон 3.1/07 §2); дедуп-ключ по всей цепочке (канон 07 §6, Task 9) — (источник по accessKey, seqNo), `originHostId` — диагностика; `IOutcomeCounters` определён в Task 9 (контракт inbox'а и фетчера), реализован в Task 12 на `ReplicationMetrics` (wiring координатора подменяет no-op-заглушку); инкременты inbox'а работают с Task 9, наполняются после wiring'а Task 12; conflict инкрементируется только точками применения (компаратор предфильтра inbox / `ReplicaApplyResult.Conflict` фетчера), событие `ConflictDetected` — только лог (без инкремента, нет мульти-счёта при ≥2 источниках); `ReplicaApplyResult {Applied, Conflict}` — возврат `ReplicaApplier.ApplyObjectAsync` (Task 6), питает `applied_total{result}`/конфликты; `ReplicationMetrics` пробрасывается в конструкторы NotificationDispatcher (Task 11) и через `IOutcomeCounters` в inbox/фетчер — все точки инкремента описаны; `ReplicationMaintenance` принимает `ReplicationOptions` (retention из `options.TombstoneRetentionHours`); пакеты Task 3 — только существующие PackageVersion (`Options.ConfigurationExtensions`, `Logging.Abstractions`); порядок Tasks 14/15 по счётчикам: снапшот и ассерты counters в Task 14, полный словарь/экспорт — Task 15 (снапшот не меняет); Task 11 Files синхронен карте файлов (fetch-серии/счётчики — Task 12, reconcile.position — Task 13).
- **Каноны:** динамические порты (Task 16), полный teardown+ассерт чистоты (Task 16), телеметрия `[PHASE]`/MarkFailed (Task 16) и `[METRIC]` сквозных измерений по канону `docs/e2e-launch.md` (Task 22: metrics.log в ArtifactsDir), таймауты ≤30 с/поллинг срастания ≤30 с шагом (Tasks 16–20; A11(б) — предусловие «очередь пуста» + `docker stop -t 30`; Task 22: A15-поллинг шаг ≤1 с бюджет 5 с, A4-поллинг шаг ≤5 с бюджет 60 с; пороги — ассерты, превышение = провал с разбором, НЕ рост таймаутов), зачистка сетей между сериями (Tasks 16–22), E2E-образ без исходников (переиспользование OwnS3E2eImage), TreatWarningsAsErrors/центральный packages (Global Constraints), AAA (все тест-шаги).
