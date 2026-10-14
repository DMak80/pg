# t33-s3-sync-connectivity — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** процесс F `SyncProbeProcess` в Active-ветке тика S3Worker — периодический (гейт `SyncIntervalSec`) и по-командный (заявка API) зонд связности/синхронности ownS3-кластеров: ping-матрица настроенных линков (`connectivity`) и сверка содержимого bucket между нодами (`sync`), оба ключа — канонический JSON arch/23 §3; реализация `POST /api/s3/clusters/{c}/sync-probe` (202-заявка).

**Архитектура:** зонд — читатель факта, не надзиратель: опрашивает готовые грани ownS3 (`POST /admin/replication/probe`, `GET /admin/replication/sync-report`, `GET /admin/replication/status`, ListObjectsV2) через единственную точку транспорта `IOwnS3NodeClient`, пишет два ключа наблюдения в etcd под клэймом `<C>`, сносит заявку по исполнении. Свёртка `sync-report` (равные страницы) → количественный diff листингов при расхождении; вердикты synced/lagging/diverged с критерием прогресса по прошлому значению ключа. Никакого лечения — доводка состава остаётся зоной G.

**Стек:** .NET 10 (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), AWSSDK.S3 (ListObjectsV2), SigV4-подпись root-кредами профиля, xunit.v3 + FluentAssertions (AAA-комментарии в тестах), testcontainers (интеграция/E2E).

**Spec:** `docs/superpowers/2026-10-14-t33-s3-sync-connectivity/spec.md` — план аргументируется от spec; исполнители читают оба документа.

---

## Глобальные ограничения

- Все пути ниже — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t33-s3-sync-connectivity`.
- Каноны: arch/23-s3-clusters.md §3/§8 (форматы ключей, `/s3worker/probes/<C>`), arch/24-s3worker.md §1.1/§5 F/§7/§8 (эндпоинт, процесс, метрики, конфиг), arch/owns3/07-replication.md §2/§10 (грани probe/sync-report/status). Расхождение с arch — блокирующее замечание.
- Комментарии/документация — по-русски; идентификаторы — английские; тесты — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Поллинг-инвариант: каждый вызов грани ownS3 и каждый etcd-вызов ≤ 7.5 с (клиент `owns3-node` уже 7.5 c; AWSSDK-клиент per-node — `Timeout=7.5`, `MaxErrorRetry=0`); никаких внутренних retry-циклов — ретраи тиками.
- Значения секретов (root-пара и т.п.) никогда не попадают в журнал/метрики/ключи.
- Порядок полей канонических JSON — строго arch/23 §3 (см. примеры ниже); snake_case в etcd, camelCase в HTTP-гранях ownS3.
- Компиляция: `TreatWarningsAsErrors=true` — `dotnet build` без предупреждений.
- Тесты не хардкодят хост-порты; таймауты ожиданий — поллинг `WaitPhaseAsync` с бюджетом (E2E); после каждой тестовой серии — зачистка контура (E2E-фикстура делает это сама; юниты/интеграция не поднимают docker-контуров, кроме etcd-контейнеров фикстур, которые сносят себя).
- Метрики — Meter `S3Worker` (как `mesh_*`), имена `s3worker_sync_probe_*`.
- Каждый шаг с командой сборки/тестов выполняется до перехода к следующему; коммит — в конце задачи (feature-ветка, коммитить свободно).

## Карта изменений (структура файлов)

| Файл | Действие | Ответственность |
|---|---|---|
| `src/S3Worker.Core/OwnS3/OwnS3NodeClient.cs` | правка | DTO зонда + 3 новых метода + расширение модели статуса |
| `src/S3Worker.Core/OwnS3/SigV4GetSigner.cs` | правка | canonical query в подписи (нужна для `sync-report?bucket=&marker=`) |
| `src/S3Worker.Etcd/Parsing/S3SnapshotParser.cs` | правка | `connectivity`/`sync` — известные листы (не unknownKeys) |
| `src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs` | правка | ключи `ConnectivityKey`/`SyncKey`/`ProbeRequestKey`, канонические сериализаторы, парсер прошлого sync |
| `src/S3Worker.Provisioning/Processes/SyncProbeProcess.cs` | создание | процесс F (§3.2–§3.5) |
| `src/S3Worker.Provisioning/Processes/DeprovisioningProcess.cs` | правка | X3 сносит `/s3worker/probes/<C>` |
| `src/S3Worker.App/Options.cs` | правка | `Thresholds.SyncIntervalSec=60`, `Thresholds.SyncMaxPages=20` |
| `src/S3Worker.App/appsettings.json` | правка | те же значения |
| `src/S3Worker.App/S3StartupChecks.cs` | правка | валидация новых порогов |
| `src/S3Worker.App/Loops/S3ClusterProcesses.cs` | правка | F в Active-ветке после G |
| `src/S3Worker.App/Api/Operations/ClusterHandlers.cs` | правка | реализация `SyncProbeHandler` |
| `src/S3Worker.App/Api/ApiModule.cs` | правка | маршрут sync-probe: гварды 404/409/503, 202 |
| `src/S3Worker.App/Program.cs` | правка | DI `SyncProbeProcess`, `SyncProbeHandler` |
| `src/tests/S3Worker.UnitTests/...` | правка/создание | тесты клиента, хелперов, процесса, парсера; расширение `FakeNodeClient` |
| `src/tests/S3Worker.IntegrationTests/...` | создание | API-тесты sync-probe; etcd-контур зонда |
| `src/tests/S3Worker.IntegrationTests/E2e/S3E2eSyncProbeTests.cs` | создание | docker-E2E §8.3 |

Модель снапшота (`S3ClusterSnapshot`) НЕ расширяется (spec §3.8).

---

## Задача 1. Клиент грани: DTO, SigV4 canonical query, методы зонда

**Файлы:**
- Modify: `src/S3Worker.Core/OwnS3/SigV4GetSigner.cs`
- Modify: `src/S3Worker.Core/OwnS3/OwnS3NodeClient.cs`
- Test: `src/tests/S3Worker.UnitTests/OwnS3/OwnS3NodeClientTests.cs` (дополнить), `src/tests/S3Worker.UnitTests/OwnS3/SigV4GetSignerTests.cs` (дополнить)

**Интерфейсы (Consumes/Produces):**
- Produces (используют Задача 3 и тесты):
  - `record OwnS3ProbeTarget(string Name, bool Reachable, long? RttMs = null)`
  - `record OwnS3ProbeReport(bool Active, IReadOnlyList<OwnS3ProbeTarget> Targets)`
  - `record OwnS3SyncReportPage(int Count, string? FirstKey, long FirstModTime, string? LastKey, long LastModTime, long TotalSize, string AggregateHash, string? NextMarker)`
  - `record OwnS3ListEntry(string Key, long ModTimeUnixMs, long Size)`
  - `record OwnS3ListPage(IReadOnlyList<OwnS3ListEntry> Entries, string? NextContinuationToken)`
  - `record OwnS3ReplicationNeighbor(string Name, long FetchQueueDepth, double? FetchLagSeconds, bool FetchPaused, bool ReconcileRunning)`
  - `OwnS3ReplicationStatus` — третий позиционный параметр `IReadOnlyList<OwnS3ReplicationNeighbor> Neighbors = []` (существующие вызовы `new(active, names)` компилируются; `NeighborNames` сохраняется — G не меняется, spec §3.6.4)
  - Методы `IOwnS3NodeClient`:
    ```csharp
    Task<Result<OwnS3ProbeReport>> ProbeAsync(string host, int port, OwnS3NodeSecrets creds, CancellationToken ct);
    Task<Result<OwnS3SyncReportPage>> GetSyncReportPageAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, string? marker, CancellationToken ct);
    Task<Result<OwnS3ListPage>> ListObjectEntriesAsync(string host, int port, OwnS3NodeSecrets creds, string bucket, string? continuationToken, CancellationToken ct);
    ```
  - `SigV4GetSigner.SignRequest(..., string? canonicalQuery = null)` — новая опциональная параметром-последняя (null/пусто — прежняя пустая query, обратная совместимость).

**Обоснование canonical query (найдено при изучении кода):** серверный верификатор `OwnS3.Protocol/Auth/CanonicalRequestBuilder.Build` включает `model.RawQuery` в canonical request, а текущий `SigV4GetSigner.SignRequest` подписывает пустую query — GET `/admin/replication/sync-report?bucket=…&marker=…` получил бы `SignatureDoesNotMatch`. `S3Worker.Core` уже ссылается на `OwnS3.Protocol` (ProjectReference) — roundtrip-тест возможен.

- [ ] **Шаг 1.1. SigV4GetSigner: canonical query**

`SignRequest` получает параметр `string? canonicalQuery = null` (после `now`); в canonical request третья строка — `canonicalQuery ?? ""` вместо всегда-пустой. Остальное (scope/headers/payload `UNSIGNED-PAYLOAD`) без изменений. Публичный хелпер канонизации пар:

```csharp
// Каноническая query (RFC 3986, пары отсортированы по имени Ordinal):
// точка совместимости с CanonicalRequestBuilder сервера (значения null/пусто не шлются).
public static string CanonicalQuery(params (string Name, string? Value)[] pairs)
    => string.Join("&", pairs
        .Where(p => !string.IsNullOrEmpty(p.Value))
        .OrderBy(p => p.Name, StringComparer.Ordinal)
        .Select(p => $"{Uri.EscapeDataString(p.Name)}={Uri.EscapeDataString(p.Value!)}"));
```

- [ ] **Шаг 1.2. Roundtrip-тест подписи с query (FAIL → реализация уже в 1.1; тест пишется первым)**

В `SigV4GetSignerTests.cs` — тест: `SignRequest("GET", "/admin/replication/sync-report", "h:1", "ak", "sk", canonicalQuery: "bucket=b&marker=m%2Fnext")` → собрать `S3RequestModel { Method="GET", RawPath=..., RawQuery="bucket=b&marker=m/next", Host="h:1", Headers=[host,x-amz-date,x-amz-content-sha256] }` → `new SigV4HeaderVerifier(FixedTime-подобный TimeProvider).Verify(model, _ => "sk")` → `Ok`. Плюс негативный: подпись БЕЗ query на запросе С query → `SignatureDoesNotMatch`. Использовать `OwnS3.Protocol.Requests.S3RequestModel`/`S3HeaderCollection.FromPairs` (см. `SigV4Signer.cs` — прецедент сборки модели). Время фиксировать `at:` и тем же instant в verifier нельзя — verifier берёт UtcNow; подпись с `now: DateTimeOffset.UtcNow` проходит ±15 мин skew.

- [ ] **Шаг 1.3. DTO в OwnS3NodeClient.cs**

Записи из блока «Интерфейсы» выше, XML-doc по-русски с указанием канона (owns3/07 §2/§10). `OwnS3ReplicationStatus`:

```csharp
/// <summary>Состав mesh из GET /admin/replication/status (owns3/07 §10):
/// G — имена соседей (NeighborNames, проекция Neighbors); зонд F — per-neighbor
/// счётчики доставки (fetchQueueDepth и др.). Neighbors=[] по умолчанию —
/// обратная совместимость существующих вызовов.</summary>
public sealed record OwnS3ReplicationStatus(
    bool Active, IReadOnlyList<string> NeighborNames,
    IReadOnlyList<OwnS3ReplicationNeighbor> Neighbors = []);
```

- [ ] **Шаг 1.4. Метод ProbeAsync (интерфейс + реализация)**

`POST http://{host}:{port}/admin/replication/probe` без тела, SigV4 root-креды (`SignAdmin("POST", ..., body: Array.Empty<byte>())`). 200 → парсинг `{active, targets:[{name, reachable, rttMs?}]}` (camelCase; `rttMs` может отсутствовать/null → `null`). Транспорт/не-2xx → `Failed(ApplicationException $"probe {host}:{port}: ...")` — транзиент, нода «не опрошена» (spec §3.6.1). Образец обработки исключений — как `GetReplicationStatusAsync`.

- [ ] **Шаг 1.5. Метод GetSyncReportPageAsync**

`GET http://{host}:{port}/admin/replication/sync-report?bucket=<b>&marker=<m>` — query собирать `SigV4GetSigner.CanonicalQuery(("bucket", bucket), ("marker", string.IsNullOrEmpty(marker) ? null : marker))`; URL — `$"http://{host}:{port}/admin/replication/sync-report?{query}"`; подпись `SignAdmin("GET", path, hostHeader, creds)` с `canonicalQuery: query` (расширить приватный `SignAdmin` параметром, по умолчанию null). Парсинг ответа 200: `count`(int), `firstKey`(string?), `firstModTime`(long), `lastKey`(string?), `lastModTime`(long), `totalSize`(long), `aggregateHash`(string), `nextMarker`(string?; null ИЛИ пусто → конец). Не-2xx/транспорт/битый JSON → Failed.

- [ ] **Шаг 1.6. Метод ListObjectEntriesAsync**

По образцу `EnsureBucketAsync` (AWSSDK, `NewS3Client`: ForcePathStyle, `Timeout=7.5`, `MaxErrorRetry=0`):

```csharp
var response = await s3.ListObjectsV2Async(new ListObjectsV2Request
{
    BucketName = bucket,
    ContinuationToken = continuationToken, // null допустим
    MaxKeys = 1000,
}, ct);
```

Маппинг: `Entries = response.S3Objects.Select(o => new OwnS3ListEntry(o.Key, o.LastModified.ToUnixTimeMilliseconds(), o.Size))`; `NextContinuationToken = string.IsNullOrEmpty(response.NextContinuationToken) ? null : response.NextContinuationToken`. `AmazonS3Exception`/таймаут/прочее → Failed (диагностика `listing {host}:{port}/{bucket}: …`).

- [ ] **Шаг 1.7. Расширение GetReplicationStatusAsync**

В существующем парсинге `neighbors[]` — кроме `name` читать `fetchQueueDepth` (long; null/нет → 0), `fetchLagSeconds` (double?; null ок), `fetchPaused` (bool; null → false), `reconcileRunning` (bool; null → false) → `Neighbors`. `NeighborNames` — из собранного списка (`neighbors.Select(n => n.Name)`). Запись без `neighbors`-поля → `Neighbors = []` (толерантность).

- [ ] **Шаг 1.8. Тесты клиента (AAA)**

Дополнить `OwnS3NodeClientTests.cs` (мок `StubHandler` для JSON-граней, `LoopbackServer` для AWSSDK-веток — образцы уже в файле):
1. `Probe_ПарсингОтвета` — 200 `{active:true,targets:[{name:"east2",reachable:true,rttMs:12},{name:"west1",reachable:false}]}` → `Targets[1].RttMs == null`, `Reachable==false`.
2. `Probe_НеОтветилаНода_Failed` — 500 → `IsSuccess==false`, сообщение содержит `probe`.
3. `SyncReport_СтраницаИКонец` — 200 c `nextMarker:"k2"` → страница; `nextMarker:""`/отсутствует → `NextMarker == null`; query фактического запроса содержит `bucket=` и `marker=` (защёлка `Requests`).
4. `SyncReport_НеДостигнут_Failed` — 503 → Failed.
5. `ListEntries_СтраницаИКонец` (LoopbackServer c XML ListObjectsV2: два объекта + `NextContinuationToken`; вторая страница без токена) → записи `(key, modTimeUnixMs, size)`, вторая страница `NextContinuationToken == null`.
6. `ReplicationStatus_PerNeighborИNeighborNames` — 200 с двумя neighbors (разные `fetchQueueDepth`) → `Neighbors.Count==2`, `NeighborNames` — те же имена в порядке ответа.

- [ ] **Шаг 1.9. Сборка и прогон юнитов клиента**

```
dotnet build src/S3Worker.slnx -c Debug
dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~OwnS3NodeClientTests|FullyQualifiedName~SigV4GetSignerTests"
```
Ожидание: PASS, build без предупреждений.

- [ ] **Шаг 1.10. Регрессия G (NeighborNames)**

```
dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~MeshGovernanceTests"
```
Ожидание: PASS (модель статуса расширялась обратно совместимо).

- [ ] **Шаг 1.11. Коммит**

```
git add src/S3Worker.Core src/tests/S3Worker.UnitTests
git commit -m "t33: клиент грани ownS3 — probe/sync-report/listing + SigV4 canonical query"
```

---

## Задача 2. Ключи, канонические сериализаторы, парсер снапшота, X3

**Файлы:**
- Modify: `src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs`
- Modify: `src/S3Worker.Etcd/Parsing/S3SnapshotParser.cs`
- Modify: `src/S3Worker.Provisioning/Processes/DeprovisioningProcess.cs` (X3 + probes)
- Test: `src/tests/S3Worker.UnitTests/Provisioning/S3ProcessCommonTests.cs` (дополнить), `src/tests/S3Worker.UnitTests/Etcd/` (парсер — найти существующий файл парсер-тестов и дополнить), `src/tests/S3Worker.UnitTests/Provisioning/DeprovisioningProcessTests.cs` (дополнить один кейс)

**Интерфейсы (Produces; использует Задача 3):**

```csharp
// S3ProcessCommon:
public static string ConnectivityKey(string cluster)    // /s3/clusters/<C>/connectivity
public static string SyncKey(string cluster)            // /s3/clusters/<C>/sync
public static string ProbeRequestKey(string cluster)    // /s3worker/probes/<C>

public sealed record SyncPairValue(string From, string To, long Missing, long Extra, long? LagBytes, long? LagObjects);

// Порядок полей канона arch/23 §3; links-узлы и targets — Ordinal; пары — (From,To) Ordinal.
public static string SerializeConnectivity(string state, long checkedUnix,
    IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> links, string? probeError = null)
public static string SerializeSync(string state, long checkedUnix,
    IReadOnlyList<SyncPairValue> pairs, string? lastError = null)

// Прошлое значение connectivity/sync для гейта и прогресс-критерия:
// битый JSON → null (гейт «ключа нет» — прогон перепишет).
public static long? ParseCheckedUnix(string? json)
// (from,to) → missing+extra прошлого прогона; пары без полей missing/extra не попадают.
public static IReadOnlyDictionary<(string From, string To), long>? ParseSyncDiscrepancies(string? json)
```

- [ ] **Шаг 2.1. S3ProvisioningOptions: поля зонда**

Добавить в конец записи `S3ProvisioningOptions` два поля с дефолтами (существующие позиционные вызовы в `S3ClusterFixture`/`MeshGovernanceTests` не ломаются) + XML-doc (arch/24 §8; SyncMaxPages — бюджет страниц сверки на сторону, spec §3.3):

```csharp
int SyncIntervalSec = 60,
int SyncMaxPages = 20)
```

Обновить `Default`: `new(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups", SyncIntervalSec: 60, SyncMaxPages: 20)`. Поля нужны Задаче 3 (процесс читает их) — поэтому шаг здесь, до создания процесса.

- [ ] **Шаг 2.2. Ключи-хелперы** — три метода по образцу `RotationKey` (одна строка на ключ, XML-doc со ссылкой arch/23 §3/§8).

- [ ] **Шаг 2.3. Сериализаторы (Utf8JsonWriter, как `SerializeCanonicalConfig`)**

`SerializeConnectivity` — поля: `state`, `checked_unix`, `links` (объект: узел → объект target → `"ok"|"offline"`; узлы и targets в Ordinal), опционально `probe_error` (только при непустом). Пример результата (spec §3.2):

```json
{"state":"OK","checked_unix":1760156900,
 "links":{"east1":{"east2":"ok"},"east2":{"east1":"ok"}}}
```

`SerializeSync` — поля: `state`, `checked_unix`, `pairs` (массив объектов `from`,`to`,`missing`,`extra`, опционально `lag_bytes` (при `missing>0`), `lag_objects` (при >0); порядок пар — (From,To) Ordinal), опционально `last_error`. Пример (spec §3.3):

```json
{"state":"LAGGING","checked_unix":1760156900,
 "pairs":[{"from":"east1","to":"east2","missing":3,"extra":0,
           "lag_bytes":1048576,"lag_objects":3}]}
```

- [ ] **Шаг 2.4. Парсеры прошлого значения** — `ParseCheckedUnix`: `JsonDocument`, поле `checked_unix` (number → long; иначе null); не-JSON/не-объект → null. `ParseSyncDiscrepancies`: массив `pairs` → словарь `(from,to) → missing+extra` (нечисловые/отсутствующие поля → пара пропускается); отсутствие/битость → null.

- [ ] **Шаг 2.5. Парсер снапшота: connectivity/sync — известные листы**

В `S3SnapshotParser.Parse`, в `switch (segments[4])` добавить (рядом с `endpoints`):

```csharp
// Ключи наблюдения зонда F (arch/23 §3): известные листы — не unknownKeys,
// в снапшот тика не входят (зонд читает их напрямую при гейте/прогрессе).
case "connectivity" when segments.Length == 5:
case "sync" when segments.Length == 5:
    break;
```

Обновить XML-doc класса (убрать упоминание «в т.ч. connectivity/sync — t33» из комментария unknownKeys).

- [ ] **Шаг 2.6. X3 сносит заявку probes**

В `DeprovisioningProcess` (фаза X3, массив ключей рядом с `WorkKey`/`RotationKey`) добавить `S3ProcessCommon.ProbeRequestKey(cluster)`.

- [ ] **Шаг 2.7. Тесты (AAA)**

1. `S3ProcessCommonTests`: `Connectivity_КаноническийПорядокПолей` — строгая строка:
   ```
   {"state":"DEGRADED","checked_unix":1760156900,"links":{"east1":{"east2":"offline"},"east2":{"east1":"ok"}},"probe_error":"east3: таймаут"}
   ```
   (ассерт полного `==`, вход — те же данные в словаре; проверка Ordinal-сортировки: подать узлы/таргеты в обратном порядке — строка та же).
2. `Sync_КаноническийПорядокПолей` — строгая строка из примера выше + вариант SYNCED без опциональных полей:
   `{"state":"SYNCED","checked_unix":1760156900,"pairs":[{"from":"east1","to":"east2","missing":0,"extra":0}]}` и `{"state":"SYNCED","checked_unix":1760156900,"pairs":[]}`.
3. `ParseCheckedUnix_Гейт` — валидный → число; битый/нет поля → null.
4. `ParseSyncDiscrepancies_Прогресс` — pairs с missing/extra → суммы; битый → null.
5. Парсер снапшота: кластер с ключами `connectivity`/`sync` → НЕ в `UnknownKeys` (счётчик 0), снапшот кластера строится; прочий неизвестный лист — по-прежнему unknownKeys.
6. `DeprovisioningProcessTests`: после X3 в FakeEtcd нет `/s3worker/probes/<C>` (сид заявки до прогона; после — `Store.ContainsKey == false`; вместе с существующими work/rotations-ассертами).

- [ ] **Шаг 2.8. Сборка и прогон**

```
dotnet build src/S3Worker.slnx -c Debug
dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~S3ProcessCommonTests|FullyQualifiedName~SnapshotParser|FullyQualifiedName~DeprovisioningProcessTests"
```
Ожидание: PASS. (Если файл парсер-тестов назван иначе — найти `grep -rl "S3SnapshotParser" src/tests/S3Worker.UnitTests`.)

- [ ] **Шаг 2.9. Коммит**

```
git add src/S3Worker.Provisioning src/S3Worker.Etcd src/tests/S3Worker.UnitTests
git commit -m "t33: ключи/сериализаторы connectivity+sync, парсер тихо, X3 уносит заявку probes"
```

---

## Задача 3. SyncProbeProcess (процесс F) + юнит-тесты

**Файлы:**
- Create: `src/S3Worker.Provisioning/Processes/SyncProbeProcess.cs`
- Modify: `src/tests/S3Worker.UnitTests/Fakes/Fakes.cs` (расширение `FakeNodeClient`)
- Test: `src/tests/S3Worker.UnitTests/Provisioning/SyncProbeTests.cs` (создать)

**Интерфейсы (Consumes/Produces):**
- Consumes: `IOwnS3NodeClient.{ProbeAsync, GetSyncReportPageAsync, ListObjectEntriesAsync, GetReplicationStatusAsync}` (Задача 1); `S3ProcessCommon.{ConnectivityKey, SyncKey, ProbeRequestKey, SerializeConnectivity, SerializeSync, ParseCheckedUnix, ParseSyncDiscrepancies, GetWithFailoverAsync, EnsureClaimed, ParsePortAlloc}` (Задача 2); `ClaimStore`/`WorkJournal`/`SecretProfileStore`; `S3ProvisioningOptions` с полями `SyncIntervalSec`/`SyncMaxPages` (добавлены Шагом 2.1).
- Produces: `public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)`; `public const string Op = "sync-probe"` — вызывает Задача 4 (ветка тика).

**Конструктор (по образцу `MeshGovernanceProcess`, но БЕЗ docker/PkiStore):**

```csharp
public sealed class SyncProbeProcess(
    IEtcdGateway gateway,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    SecretProfileStore secrets,
    IOwnS3NodeClient nodeClient,
    S3ProvisioningOptions options,
    TimeProvider clock,
    Meter meter)
```

Счётчики (Meter `S3Worker`, имена spec §3.9): `_runs("s3worker_sync_probe_runs_total")`, `_links("s3worker_sync_probe_links_total")`, `_pairs("s3worker_sync_probe_pairs_total")` — тег `result` (`ok|failed|idle` / `ok|offline` / `synced|lagging|diverged`).

- [ ] **Шаг 3.1. Скелет тика (§3.5 шаги 1–2)**

```csharp
public async Task<Result> TickAsync(S3ClusterSnapshot snap, CancellationToken ct)
{
    var cluster = snap.Cluster;
    var claimed = S3ProcessCommon.EnsureClaimed(claims, cluster, Op);   // S5
    if (!claimed.IsSuccess) return claimed;

    // 0 RUNNING → idle (до гейта; по букве §3.5 п.1) + del заявки: «исполнена — нечем зондировать».
    var running = snap.Nodes.Where(n => n.State == S3NodeStates.Running).Select(n => n.Node)
        .OrderBy(n => n, StringComparer.Ordinal).ToList();
    if (running.Count == 0)
        return await IdleAsync(cluster, deleteRequest: true, warning: null, ct);

    // Креды профиля: грани недостижимы без root-кредов — диагностический skip (образец G).
    if (!secrets.TryGet(snap.Config?.SecretProfile ?? "", out var creds, out var secretDiagnostic))
        return await IdleAsync(cluster, deleteRequest: false, warning: $"профиль секретов недоступен: {secretDiagnostic}", ct);

    // Заявка + прошлый connectivity (гейт). Отказ чтения etcd → Failed (ретрай тиком, заявка жива).
    var request = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, S3ProcessCommon.ProbeRequestKey(cluster), ct);
    if (!request.IsSuccess) return Fail(cluster, request.Error!, "read-probe-request");
    var pastConnectivity = await S3ProcessCommon.GetWithFailoverAsync(gateway, endpoints, S3ProcessCommon.ConnectivityKey(cluster), ct);
    if (!pastConnectivity.IsSuccess) return Fail(cluster, pastConnectivity.Error!, "read-connectivity");

    var now = clock.GetUtcNow().ToUnixTimeSeconds();
    var pastChecked = S3ProcessCommon.ParseCheckedUnix(pastConnectivity.Value?.Value);
    var gated = request.Value is not null
        || pastChecked is null
        || now - pastChecked.Value >= options.SyncIntervalSec;
    if (!gated)
        return Result.Success(); // тихий тик: journal НЕ пишем (§3.5 п.2)

    return await RunAsync(cluster, snap, running, creds!, request.Value is not null, now, ct);
}
```

`IdleAsync`: journal `WritePhaseAsync(cluster, Op, "idle", claims.InstanceId, warning, ct)`; при `deleteRequest` — del `ProbeRequestKey` failover (отказ del → `Result.Failed` — заявка обязана быть снесена, ретрай тиком); метрика `_runs.Add(1, result="idle")`; Success. `Fail`: journal-запись `phase="probe-failed"` + `last_error` (`CancellationToken.None` — образец `MeshGovernanceProcess.Fail`); метрика `_runs.Add(1, result="failed")`; `Result.Failed`.

- [ ] **Шаг 3.2. Прогон: probe-опрос и матрица (§3.2, §3.3-пары)**

`RunAsync` (шаги; все warnings копятся в `List<string> warnings`, ошибки опроса — в `List<string> probeErrors`):

1. portalloc: чтение+`ParsePortAlloc` (образец `MeshGovernanceProcess.ReadPortAllocAsync`); отказ → `Fail(..., "portalloc")`. Записи нет у RUNNING-ноды → `warnings.Add($"{node}: адреса нет в portalloc — транзиент")`, нода не опрашивается.
2. Последовательно по RUNNING-нодам (Ordinal): `nodeClient.ProbeAsync(AdvertisedHost ?? addr.Host, addr.S3Port, creds, ct)`; Failed → `probeErrors.Add($"{node}: {msg}")` — строки ноды в матрице нет (§3.2 п.4), реконструкции нет.
3. Матрица `SortedDictionary<string, SortedDictionary<string, string>>`: для каждого ответа ноды X — только её ФАКТИЧЕСКИЕ targets: `links[X][t.Name] = t.Reachable ? "ok" : "offline"`; метрики `_links.Add(1, result="ok"/"offline")` по каждому линку. `rttMs` игнорируется.
4. Агрегат connectivity:
   - всех ответивших нулевое число И `running.Count > 0` → `DOWN` + `probe_error` (обязательно; §3.2 п.5);
   - матрица пуста (никому не настроено) → `OK`;
   - все линки `ok` → `OK`; часть `offline` → `DEGRADED`; живых нет → `DOWN`.
   - `probe_error` = `string.Join("; ", probeErrors)` при непустом, усечение до 300 символов (`s.Length > 300 ? s[..300] : s`).
5. Пары sync: направленные записи из ответов (X → t.Name); пара `{a,b}` ⟺ запись в любую сторону; `from = Ordinal-min(a,b)`, `to = max`; уникальные, сортировка (from,to) Ordinal.

- [ ] **Шаг 3.3. Прогон: lag_objects (статусы приёмников)**

Только если пары есть. Кэш `Dictionary<string, IReadOnlyList<OwnS3ReplicationNeighbor>> statusCache`: `GetReplicationStatusAsync(приёмника to)` один раз на ноду (Failed → `warnings.Add($"status {to}: {msg} — транзиент")`, кэш-промах). `lagObjects(p) = statusCache[to]?.FirstOrDefault(n => n.Name == from)?.FetchQueueDepth` — null при неизвестности (поле не пишется).

- [ ] **Шаг 3.4. Прогон: сверка пар (§3.3)**

Для каждой пары — счётчики страниц `pagesFrom`/`pagesTo` (бюджет `options.SyncMaxPages` на сторону, суммарно на sync-report + listing этой пары):

1. **Прет-чек sync-report**: цикл от `marker = null`; на итерации `pagesFrom++`/`pagesTo++`; превысили бюджет → пара НЕ сверена (см. п.4). Запрос обеим сторонам; любой Failed → `warnings.Add($"sync-report {p.from}/{p.to}: {msg} — транзиент")`, пара не сверена.
2. Страницы равны ⟺ равны ВСЕ поля `OwnS3SyncReportPage` (count, firstKey, firstModTime, lastKey, lastModTime, totalSize, aggregateHash) И `nextMarker` обеих одинаков (обе null/пусто ИЛИ равны). Равны и не конец → следующая итерация от общего `nextMarker`. Обе кончились (nextMarker пуст у обеих) → пара `synced` (`missing=0, extra=0`).
3. Расхождение (первая неравная страница ИЛИ одна кончилась, другая нет) → **diff листингов**: полные постраничные `ListObjectEntriesAsync` обеих сторон (токен от null; страницы в те же счётчики бюджета); любой Failed → пара не сверена (warning). Сеты `HashSet<(string Key, long Mod)>`; `missing = fromSet.Except(toSet)`, `extra = toSet.Except(fromSet)` (элемент сверки `(key, modTimeUnixMs)` — одинаковый ключ с другим modTime попадает в ОБА множества); `lag_bytes = Σ size(missing)` при `missing>0`.
4. Пара «не сверена» (бюджет/отказы) → НЕ попадает в `pairs`, `last_error.Sync += $"листинг пары {from}↔{to} превышает бюджет страниц"` (бюджет) либо текст отказа; `uncovTotal++`.
5. Прошлое значение: `S3ProcessCommon.ParseSyncDiscrepancies(pastSyncJson)` — читается ОДИН раз в начале `RunAsync` (failover-чтение `SyncKey`; отказ чтения → `Fail`, т.к. прогресс-критерий и гейт уже пройдены — spec §3.10 идемпотентность не требует записи при нечитаемом etcd).

- [ ] **Шаг 3.5. Вердикты и агрегат sync (§3.3)**

- `synced` — `missing==0 ∧ extra==0`.
- `lagging` — расхождения И (`lag_objects > 0` ИЛИ `past[(from,to)] существует и > missing+extra`).
- `diverged` — расхождения И `lag_objects == 0` (или неизвестен) И (прошлое есть И `missing+extra >= past`). Прошлого нет (первый прогон) → `lagging` (не diverged).
- Агрегат: есть `diverged` → `DIVERGED`; иначе есть `lagging` ИЛИ `uncovTotal > 0` → `LAGGING`; иначе `SYNCED` (0 пар — тоже SYNCED).
- Метрики `_pairs.Add(1, result=вердикт)` по каждой свёрнутой паре.

- [ ] **Шаг 3.6. Запись ключей, del заявки, journal**

Один `checkedUnix` на обе половины:

1. `SerializeConnectivity(state, checkedUnix, links, probeError)` → put failover (по endpoints, первый успешный); отказ → `Fail(cluster, err, "write-connectivity")`.
2. `SerializeSync(state, checkedUnix, pairs, lastError)` → put failover; отказ → `Fail(..., "write-sync")`.
3. Заявка была → del `ProbeRequestKey` failover; отказ del → `Fail(..., "delete-probe-request")` (заявку сносим только после успеха записи — §3.4).
4. journal `phase="probed"`, `lastError = warnings.Count > 0 ? Join("; ", warnings) : null`; метрика `_runs.Add(1, result="ok")`; Success.

- [ ] **Шаг 3.7. FakeNodeClient: расширение (Fakes.cs)**

```csharp
// Зонд F (t33): программируемые probe/sync-report/listing по S3-порту ноды + журнал вызовов.
public readonly Dictionary<int, Result<S3Worker.Core.OwnS3.OwnS3ProbeReport>> ProbeByPort = [];
public readonly Dictionary<int, Dictionary<string?, S3Worker.Core.OwnS3.OwnS3SyncReportPage>> SyncReportPages = []; // marker(null) → страница
public readonly Dictionary<int, IReadOnlyList<S3Worker.Core.OwnS3.OwnS3ListEntry>> Listings = [];
public readonly List<(string Op, int Port, string? Arg)> ProbeCalls = [];
```

Реализации трёх методов: `ProbeAsync` → журнал + `ProbeByPort.TryGetValue(port)` (нет → Failed `"probe не запрограммирован"`); `GetSyncReportPageAsync` → журнал + страница по `(port, marker)` (нет → Failed); `ListObjectEntriesAsync` → журнал + ОДНА страница из `Listings[port]` с `NextContinuationToken=null` (для многостраничных diff-тестов достаточно одной страницы — бюджет тестируется цепочкой sync-report). Статус: `StatusByPort` уже есть — расширить существующим полем типа `Result<OwnS3ReplicationStatus>` при необходимости вернуть `Neighbors` (добавить `StatusResultByPort` словарь; `NeighborNames`-ветка не трогается).

- [ ] **Шаг 3.8. Юнит-тесты §8.1 (SyncProbeTests.cs, AAA; Rig по образцу MeshGovernanceTests)**

Rig: `FakeEtcd` + `FakeNodeClient` + `FixedTimeProvider` + `WorkJournal` + `ClaimStore`(claim "demo") + `SecretProfileStore`(профиль `backups-main`) + `new SyncProbeProcess(etcd, endpoints, claims, journal, secrets, nodeClient, new S3ProvisioningOptions(..., SyncIntervalSec: 60, SyncMaxPages: 20), clock, new Meter("S3Worker"))`; сид portalloc `SeedPortAlloc` (копия хелпера из MeshGovernanceTests); `Snap(nodes...)` — снапшот Active-кластера. Тесты (все — с комментарием `// AAA`):

1. **Матрица** (§8.1.1):
   - `Матрица_ЗеркальныеТаргеты_OkПолнаяМатрица` — probe east1/east2 с взаимными reachable targets → connectivity JSON: `{"state":"OK","checked_unix":<now>,"links":{"east1":{"east2":"ok"},"east2":{"east1":"ok"}}}` (строгий ассерт значения ключа из FakeEtcd).
   - `Матрица_ОдинOffline_Degraded` — у east2 target east1 `reachable=false` → DEGRADED, `"east2":{"east1":"offline"}`.
   - `Матрица_ВсеОффлайн_Down`.
   - `Матрица_ТаргетоВНет_OkПустаяМатрица` — оба ответа `targets:[]` → `{"state":"OK","checked_unix":<now>,"links":{}}`, sync SYNCED pairs `[]`.
   - `Матрица_ОтказПробы_СтрокиНет_ProbeError` — `ProbeByPort[18001] = Failed` → в links ключа `east1` нет; у east2 его линк на east1 по его ответу (`reachable=false` → DEGRADED); `probe_error` содержит `east1`.
   - `Матрица_НольОтветивших_DownPlusProbeError` — оба Failed → DOWN + `probe_error` непуст.
2. **Пары** (§8.1.2): `Пары_ЗеркальныеОднаПара` — одна `{min,max}`; `Пары_НезеркальныйТаргет_ОднаПара` (east1→east2 есть, обратно нет); `Пары_НетПар_Synced`.
3. **Сверка** (§8.1.3):
   - `Сверка_РавныеСтраницы_Synced00` — две равные страницы (nextMarker → вторая пустая) → SYNCED `missing:0,extra:0`.
   - `Сверка_РасхождениеНаВторойСтранице_DiffЛистингов` — первая равна, вторая различается → Listings: у east1 3 записи, у east2 1 общая + `"missing":2` + `lag_bytes` = сумма размеров; `extra:0`.
   - `Сверка_ТотЖеКлючДругойModTime_ИMissingИExtra` — ключ `k` с разными modTime в Listings сторон → `missing:1, extra:1`.
4. **Вердикты** (§8.1.4): `lagging` при `StatusResultByPort[to].Neighbors[0].FetchQueueDepth=5`; `lagging` по сокращению (сид прошлого sync со `missing:5` → текущий diff `missing:2`, очередь 0/пуста); `diverged` (прошлое `missing:2`, сейчас `missing:2`, очередь 0); первый прогон без прошлого и пустая очередь → `lagging`.
5. **Бюджет** (§8.1.5): `SyncReportPages` — цепочка из 21+ страницы (все равные, nextMarker непуст) при `SyncMaxPages: 20` → пары `[]`, `last_error` содержит `превышает бюджет страниц`, state LAGGING (несвёрнутая пара).
6. **Гейт/заявка** (§8.1.6):
   - `Гейт_СвежийКлючБезЗаявки_ТихийТик` — сид connectivity с `checked_unix = now` → после тика `Etcd.Ops` не содержит `put:/s3/clusters/demo/connectivity|sync`, journal-ключа записи нет.
   - `Гейт_Истёк_Прогон` — `checked_unix = now - 61` → put есть.
   - `Заявка_ПрогонИDel` — сид `/s3worker/probes/demo` → прогон + после тика ключа заявки нет.
   - `Заявка_FailedПрогонаЖива` — `PutFault` по connectivity-ключу → `result.IsSuccess == false`, заявка жива; снять fault → следующий тик пишет ключи и сносит заявку.
7. **Канонические JSON** (§8.1.7) — покрыты строгими ассертами тестов 1/3 (значения ключей строкой).
8. **0 RUNNING** (§3.5 п.1): снапшот с нодами не-RUNNING → journal `op=sync-probe, phase=idle`, заявка (сид) снесена, ключи не пишутся.
9. **Профиль недоступен** — `SecretProfileStore` без профиля → idle + warning в journal, заявка жива.

- [ ] **Шаг 3.9. Сборка и прогон**

```
dotnet build src/S3Worker.slnx -c Debug
dotnet test src/tests/S3Worker.UnitTests -c Debug --filter "FullyQualifiedName~SyncProbeTests"
```
Ожидание: PASS.

- [ ] **Шаг 3.10. Коммит**

```
git add src/S3Worker.Provisioning src/tests/S3Worker.UnitTests
git commit -m "t33: SyncProbeProcess — матрица связности, сверка пар, вердикты, гейт и заявка"
```

---

## Задача 4. Конфигурация, DI, ветка тика

**Файлы:**
- Modify: `src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs` (`S3ProvisioningOptions`)
- Modify: `src/S3Worker.App/Options.cs`, `src/S3Worker.App/appsettings.json`, `src/S3Worker.App/S3StartupChecks.cs`
- Modify: `src/S3Worker.App/Program.cs`, `src/S3Worker.App/Loops/S3ClusterProcesses.cs`
- Test: юнит-тест не нужен (проброс конфигурации); проверка — сборка + интеграционные тесты Задачи 5/6.

**Интерфейсы:**
- Produces: `SyncProbeProcess` в DI; F исполняется в Active-ветке. (Поля `S3ProvisioningOptions.SyncIntervalSec/SyncMaxPages` добавлены Шагом 2.1.)

- [ ] **Шаг 4.1. Options.cs** — в `ThresholdsOptions`:

```csharp
/// <summary>Период зонда F (arch/24 §8): гейт по checked_unix ключа connectivity.</summary>
public int SyncIntervalSec { get; set; } = 60;

/// <summary>Бюджет страниц сверки на сторону пары (spec §3.3; 20 = 20 000 объектов).</summary>
public int SyncMaxPages { get; set; } = 20;
```

- [ ] **Шаг 4.2. appsettings.json** — в `Thresholds`: `"SyncIntervalSec": 60, "SyncMaxPages": 20`.

- [ ] **Шаг 4.3. S3StartupChecks.Thresholds** — добавить `&& o.Thresholds.SyncIntervalSec > 0 && o.Thresholds.SyncMaxPages > 0` (fail-fast отрицательных порогов — канон arch/24 §8).

- [ ] **Шаг 4.4. Program.cs**:

1. `ToProvisioningOptions` — проброс: `..., opts.Docker.ContourNetwork, opts.Thresholds.SyncIntervalSec, opts.Thresholds.SyncMaxPages)`.
2. DI после `MeshGovernanceProcess`:

```csharp
builder.Services.AddSingleton(sp => new S3Worker.Provisioning.Processes.SyncProbeProcess(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value.Etcd.Endpoints,
    sp.GetRequiredService<ClaimStore>(),
    sp.GetRequiredService<WorkJournal>(),
    sp.GetRequiredService<S3Worker.Provisioning.SecretProfileStore>(),
    sp.GetRequiredService<S3Worker.Core.OwnS3.IOwnS3NodeClient>(),
    ToProvisioningOptions(sp.GetRequiredService<IOptions<S3WorkerOptions>>().Value),
    sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<System.Diagnostics.Metrics.Meter>()));
```

- [ ] **Шаг 4.5. S3ClusterProcesses** — в конструктор добавить `SyncProbeProcess syncProbe`; в `case S3ClusterKind.Active:` после mesh (обновить комментарий ветки: `C → D → E → G → F`):

```csharp
var meshed = await mesh.TickAsync(snap, ct);
if (!meshed.IsSuccess)
    return meshed;
// Зонд F — последний в Active-ветке (arch/24 §5): видит состав mesh,
// доведённым тиком G; пишет connectivity/sync (arch/23 §3).
return await syncProbe.TickAsync(snap, ct);
```

- [ ] **Шаг 4.6. Сборка + быстрые регрессии юнитов**

```
dotnet build src/S3Worker.slnx -c Debug
dotnet test src/tests/S3Worker.UnitTests -c Debug
```
Ожидание: PASS (весь юнит-набор — конструкторы S3ProvisioningOptions с дефолтами не сломали существующие риги).

- [ ] **Шаг 4.7. Коммит**

```
git add src/S3Worker.Provisioning src/S3Worker.App
git commit -m "t33: конфиг SyncIntervalSec/SyncMaxPages, DI и ветка тика C→D→E→G→F"
```

---

## Задача 5. HTTP API sync-probe + интеграционные тесты

**Файлы:**
- Modify: `src/S3Worker.App/Api/Operations/ClusterHandlers.cs` (заменить заглушку `SyncProbeHandler`)
- Modify: `src/S3Worker.App/Api/ApiModule.cs` (маршрут), `src/S3Worker.App/Program.cs` (DI хендлера)
- Test: `src/tests/S3Worker.IntegrationTests/Api/SyncProbeApiTests.cs` (создать)

**Интерфейсы:**
- Consumes: `S3ApiHelpers.GetConfigAsync/CallAsync`, `S3ProcessCommon.ProbeRequestKey`, `S3ClusterNotFoundException`/`S3ClusterNotActiveException` (уже есть), `ApiModule.RequestedBy` (приватный — используется внутри ApiModule).
- Produces: `SyncProbeHandler(IEtcdGateway, string[], TimeProvider).HandleAsync(string cluster, string requestedBy, CancellationToken) → Task<Result<bool>>` (true — заявка поставлена, false — уже жива; оба → 202).

- [ ] **Шаг 5.1. Реализация хендлера** (по образцу `PostRotationHandler`):

```csharp
// POST /api/s3/clusters/{c}/sync-probe: 202-заявка /s3worker/probes/<C>
// put-if-absent (arch/24 §1.1, arch/23 §8); already → 202 идемпотентно
// (заявка первого заявителя не перезаписывается); кластера нет → 404;
// state=NOT_INITIALIZED/TO_REMOVE → 409; результат — ключи arch/23 §3
// (исполняет зонд F ближайшим тиком держателя клэйма).
public sealed class SyncProbeHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<bool>> HandleAsync(string cluster, string requestedBy, CancellationToken ct)
    {
        var config = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, cluster, ct);
        if (!config.IsSuccess)
            return Result<bool>.Failed(config.Error!);
        if (config.Value is null)
            return Result<bool>.Failed(new S3ClusterNotFoundException(cluster));
        if (config.Value.Value.Contains("\"state\"", StringComparison.Ordinal))
            return Result<bool>.Failed(new S3ClusterNotActiveException(cluster, "NOT_INITIALIZED/TO_REMOVE"));

        var key = S3ProcessCommon.ProbeRequestKey(cluster);
        var value = System.Text.Json.JsonSerializer.Serialize(
            new ProbeRequest(time.GetUtcNow().ToUnixTimeSeconds(), requestedBy));
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, value, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        return Result<bool>.Success(txn.Value.Succeeded); // true → заявка поставлена; false → уже жива
    }

    private sealed record ProbeRequest(
        [property: System.Text.Json.Serialization.JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: System.Text.Json.Serialization.JsonPropertyName("requested_by")] string RequestedBy);
}
```

- [ ] **Шаг 5.2. Маршрут ApiModule** — заменить блок `sync-probe`:

```csharp
// POST /api/s3/clusters/{cluster}/sync-probe — 202 (заявка /s3worker/probes/<C>,
// put-if-absent; already → 202 идемпотентно) / 404 / 409 / 503.
endpoints.MapPost("/api/s3/clusters/{cluster}/sync-probe", async (
    string cluster, HttpRequest http, SyncProbeHandler handler, CancellationToken ct) =>
{
    var result = await handler.HandleAsync(cluster, RequestedBy(http), ct);
    IResult response;
    if (result.IsSuccess)
        response = Results.Accepted();
    else
    {
        response = result.Error switch
        {
            S3ClusterNotFoundException => Results.Problem(
                statusCode: StatusCodes.Status404NotFound, title: "Cluster not found",
                detail: result.Error.Message),
            S3ClusterNotActiveException => Results.Problem(
                statusCode: StatusCodes.Status409Conflict, title: "Cluster not active",
                detail: result.Error.Message),
            _ => Results.Problem(
                statusCode: StatusCodes.Status503ServiceUnavailable, title: "Etcd write failed",
                detail: result.Error!.Message),
        };
    }

    metrics.Track("sync-probe", GetStatusCode(response));
    return response;
});
```

- [ ] **Шаг 5.3. Program.cs DI** — заменить `AddSingleton<SyncProbeHandler>()` на фабрику с зависимостями (по образцу `PostRotationHandler`: gateway, endpoints из `IOptions<S3WorkerOptions>`, `TimeProvider`).

- [ ] **Шаг 5.4. Интеграционные API-тесты (§8.1.9; SyncProbeApiTests.cs, коллекция `S3ApiCollection`)**

Риг: `[Collection(S3ApiCollection.Name)]`; хелпер сида config через `fx.Etcd.Gateway.PutAsync` (канонический JSON `{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}`; вариант с `"state":"NOT_INITIALIZED"`). Тесты (AAA):

1. `SyncProbe_КластераНет_404`.
2. `SyncProbe_НеАктивен_409` (config с state).
3. `SyncProbe_202ИPutIfAbsent` — POST → 202; в etcd ключ `/s3worker/probes/<c>` == `{"requested_unix":<не 0>,"requested_by":"e2e-op"}` (заголовок `X-Requested-By: e2e-op`).
4. `SyncProbe_ПовторПриЖивой_202БезПерезаписи` — второй POST (другой `X-Requested-By`) → 202; значение ключа НЕ изменилось (`requested_by` первого сохранился; сверить всю строку до второго вызова и после).

- [ ] **Шаг 5.5. Сборка и прогон**

```
dotnet build src/S3Worker.slnx -c Debug
dotnet test src/tests/S3Worker.IntegrationTests -c Debug --filter "FullyQualifiedName~SyncProbeApiTests"
```
Ожидание: PASS (etcd-контейнер фикстуры поднимается/сносится фикстурой).

- [ ] **Шаг 5.6. Коммит**

```
git add src/S3Worker.App src/tests/S3Worker.IntegrationTests
git commit -m "t33: POST sync-probe — 202-заявка put-if-absent, гварды 404/409"
```

---

## Задача 6. Интеграционные etcd-тесты зонда (§8.2)

**Файлы:**
- Create: `src/tests/S3Worker.IntegrationTests/S3/SyncProbeEtcdTests.cs`

**Интерфейсы:**
- Consumes: `S3EtcdFixture` (etcd-контейнер, `Gateway`, `Endpoint`, `TestPrefix`), `SyncProbeProcess` (прямой инстанс с стабом клиента), `S3ApiFactory` (заявка через API), `ClaimStore`, `WorkJournal`, `SecretProfileStore`.

- [ ] **Шаг 6.1. Стаб клиента в тесте** — приватный класс `StubNodeClient : IOwnS3NodeClient` в файле теста: полноценная реализация интерфейса, где методы зонда программируются словарями по порту (аналог FakeNodeClient, но локальный — IntegrationTests не видит internal-фейки UnitTests), а остальные методы — `Task.FromResult(Result.Success())`-заглушки (не вызываются). Профиль секретов — `SecretProfileStore` на словаре (по образцу `S3ClusterFixture`).

- [ ] **Шаг 6.2. Сценарий 1 (§8.2.1): заявка через API → тик → ключи + снос заявки**

```
// Arrange: etcd-фикстура + WAF S3ApiFactory(etcd) с сида config (Active, 2 ноды RUNNING
// в /s3/ — сид ключей nodes/*/state=RUNNING напрямую Gateway) + portalloc сид + StubNodeClient
// с взаимными probe-ответами и равными sync-report-страницами (конец сразу).
// Act: POST /api/s3/clusters/<c>/sync-probe → 202; затем один тик SyncProbeProcess.TickAsync.
// Assert: ключи /s3/clusters/<c>/connectivity и /sync существуют, парсятся как JSON,
// state OK/SYNCED, checked_unix > 0; заявка /s3worker/probes/<c> отсутствует.
```

- [ ] **Шаг 6.3. Сценарий 2 (§8.2.2): периодический гейт**

```
// Arrange: тот же риг; FakeClock — Microsoft.Extensions.TimeProvider.Testing не подключён —
// собственный FixedTime (TimeProvider-наследник, поле Utc).
// Act/Assert: первый тик (ключей нет) — прогон (put обоих ключей есть);
// второй тик немедленно (checked_unix свеж) — put'ов нет;
// сдвиг Utc += SyncIntervalSec+1 — третий тик — прогон.
```

- [ ] **Шаг 6.4. Сценарий 3 (§8.2.3)** — уже покрыт юнитом `Заявка_FailedПрогонаЖива` (FakeEtcd.PutFault, Задача 3): в этом файле не дублировать; в шапке класса — комментарий-ссылка.

- [ ] **Шаг 6.5. Сборка и прогон**

```
dotnet build src/S3Worker.slnx -c Debug
dotnet test src/tests/S3Worker.IntegrationTests -c Debug --filter "FullyQualifiedName~SyncProbeEtcdTests"
```
Ожидание: PASS.

- [ ] **Шаг 6.6. Коммит**

```
git add src/tests/S3Worker.IntegrationTests
git commit -m "t33: etcd-интеграция зонда — заявка API → тик → канонические ключи, гейт по времени"
```

---

## Задача 7. Docker-E2E (Release, §8.3)

**Файлы:**
- Create: `src/tests/S3Worker.IntegrationTests/E2e/S3E2eSyncProbeTests.cs`

**Интерфейсы (Consumes):** `S3E2eEnvironment` (StartAsync/RunDockerAsync/WaitPhaseAsync/MarkFailed/CreateApiHttpClient/Gateway/EtcdEndpoint/ClusterTag/CollectDiagnosticsAsync), хелперы `S3E2eScaleTests.{WaitWorkerAsync, CreateClusterAsync, WaitClusterActiveAsync, DeleteClusterAndWaitCleanAsync, GetOrNullAsync, Json}`, AWSSDK-клиент по порту из portalloc (паттерн `S3E2eMeshTests.S3Client` — приватный копией в новом файле). Гейт `PGW_TEST_DOCKER=1`; каноны изоляции/телеметрии уже в фикстуре (guid-имена, динамические порты, teardown+ассерт чистоты, `/tmp/pgw-e2e-artifacts-<guid>/`).

Общий каркас сценария (копия паттерна MeshTests): `await using var fx = await S3E2eEnvironment.StartAsync("<slug>"); ... try { ... } catch { fx.MarkFailed(); throw; }`. Чтение ключей зонда — `Json(await S3E2eScaleTests.GetOrNullAsync(fx, $"/s3/clusters/{cluster}/connectivity|sync", ct))`. Прогон по команде: `POST /api/s3/clusters/{cluster}/sync-probe` → `WaitPhaseAsync("probed", ...)` по условию «ключи обновились» (поллинг 500 мс, бюджет 60 с — тик держателя 1 с по env фикстуры).

- [ ] **Шаг 7.1. Сценарий «Штатная синхронность» (§8.3.1)**

```
// Arrange: кластер 2 инстанса (east1, east2) + links {east1,east2}; объект в bucket
// через S3Client(port east1); дождаться доставки (WaitPhaseAsync "replicated" — образец MeshTests).
// Act: POST sync-probe → 202; WaitPhaseAsync "probed": оба ключа существуют и свежи.
// Assert: connectivity — state OK, links[east1][east2]=ok и links[east2][east1]=ok;
// sync — SYNCED, одна пара {from:east1,to:east2,missing:0,extra:0} (без lag-полей);
// заявка probes снесена (GetOrNullAsync → null).
// Финал: DeleteClusterAndWaitCleanAsync.
```

- [ ] **Шаг 7.2. Сценарий «По команде» (§8.3.2)**

```
// Arrange: тот же подъём (кластер с линком, объекты доставлены, первый прогон уже был
// — ключи есть с checked_unix T1).
// Act: POST sync-probe (X-Requested-By: e2e-1) → 202; сразу повторный POST (e2e-2) → 202;
// WaitPhaseAsync "probed2": checked_unix обоих ключей > T1.
// Assert: заявка снесена; оба ответа — 202 (идемпотентность already — requested_by
// первого заявителя проверен интеграционным тестом Задачи 5, здесь — контракт 202).
```

- [ ] **Шаг 7.3. Сценарий «Недоступная нода» (§8.3.3)**

```
// Arrange: живой кластер с линком и объектом.
// Act 1: docker stop s3w-<c>-east2 (fx.RunDockerAsync(["stop", ...])); POST sync-probe → 202.
// Assert 1 (до UNREACHABLE — NodeDeadSec=15 у фикстуры): у east1 его линк
// links[east1][east2]="offline" (probe-опрос самого east1 прошёл, пинг соседа мёртв);
// state DEGRADED; строки east2 в links НЕТ (probe-вызов east2 не отвечает — таймаут).
// Act 2: WaitPhaseAsync "unreachable": nodes/east2/state == UNREACHABLE (надзор C);
// POST sync-probe → 202.
// Assert 2: connectivity без строки east2 (state по оставшимся линкам — линков
// настроенных у east1 больше нет в его targets после stop? НЕТ — targets east1 не
// меняются от stop; строки east1 остаются с его фактическими targets: линк
// east1→east2 offline → DEGRADED; sync: пары {east1,east2} остаются (состав targets
// east1), сверка невозможна — листинг east2 недоступен → пара не сверена, sync LAGGING
// с last_error). Проверить фактические JSON-поля и зафиксировать в ассертах по канону.
// Teardown: docker start east2 не нужен — DeleteClusterAndWaitCleanAsync сносит всё.
```

ВНИМАНИЕ исполнителю: перед фиксацией ассертов шага Assert 2 сверься со spec §8.3.3 («после UNREACHABLE connectivity без её строки, sync без её пар») — расхождение spec/факта (пары строятся из targets ОТВЕТИВШИХ нод: east1 держит target east2 ⇒ пара остаётся) разрешать по канону arch/23 §3 (пары — «объединение targets»; UNREACHABLE-нода не опрашивается, её записей нет, но запись east1→east2 существует ⇒ пара есть и не сверяется). Если факт отличается от ожидания — НЕ подгонять ассерты: собрать телеметрию (`fx.CollectDiagnosticsAsync`) и остановиться на разборе.

- [ ] **Шаг 7.4. Сценарий «Отставание → догон» (§8.3.4)**

```
// Arrange: кластер с линком; docker stop east2; запись объекта в east1 (PutObject).
// Act 1: docker start east2; WaitPhaseAsync "running": nodes/east2/state == RUNNING
// (health-возврат надзором; бюджет NodeBootSec не участвует — контейнер жив).
// Act 2: POST sync-probe → 202; WaitPhaseAsync "lagging": sync.state == LAGGING,
// pairs[0].missing > 0, lag_objects > 0 (fetch-очередь приёмника east2 непуста).
// Act 3: WaitPhaseAsync "delivered": GetObject у east2 возвращает тело (бюджет 150 с — R4,
// образец MeshTests "replicated"); POST sync-probe → 202; WaitPhaseAsync "synced":
// sync.state == SYNCED, missing == 0.
// Assert: во всех точках connectivity-матрица живых нод ok (связность не рвётся
// доставкой) — сверить после финального прогона.
```

- [ ] **Шаг 7.5. Сценарий «Deny-инстанс» (§8.3.5)**

```
// Arrange: кластер с линком и доставленным объектом; POST nodes/east2/deny → 202;
// WaitPhaseAsync "isolated": репликация-статус east1 не содержит east2 (доводка G
// снесла связь; поллинг статуса east1 — паттерн ReplicationStatusOrNullAsync из MeshTests).
// Act: POST sync-probe → 202; WaitPhaseAsync "probed".
// Assert: connectivity — state OK, links == {} (линков нет — пустой состав = OK);
// sync — SYNCED, pairs == [] (отсутствие репликации — не дефект зонда).
```

- [ ] **Шаг 7.6. Чистота контура (§8.3.6)** — обеспечена фикстурой (teardown при любом исходе, ассерты чистоты etcd-префиксов/контейнеров/томов/сети); сценарии обязаны завершаться `DeleteClusterAndWaitCleanAsync` в `try` и `fx.MarkFailed()` в `catch` — как в существующих E2E.

- [ ] **Шаг 7.7. Прогон E2E (свежий Release — фикстура собирает сама, [PHASE]-метки в журнале)**

```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~S3E2eSyncProbeTests
```
Ожидание: PASS всех сценариев; после серии — убедиться (журнал прогона) в ассертах чистоты фикстуры. При падении — разбор по артефактам `/tmp/pgw-e2e-artifacts-<guid>/` БЕЗ перезапуска (правила телеметрии AGENTS.md).

- [ ] **Шаг 7.8. Регрессия существующих S3Worker-E2E**

```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~S3E2e"
```
Ожидание: PASS (G/ротация/надзор не затронуты; unknownKeys-шум от connectivity/sync исчез — счётчик не растёт).

- [ ] **Шаг 7.9. Коммит**

```
git add src/tests/S3Worker.IntegrationTests
git commit -m "t33: docker-E2E зонда — штатная синхронность, по команде, недоступная нода, отставание, deny"
```

---

## Задача 8. Финальная верификация

**Файлы:** без новых правок (только проверки; выявленные проблемы — фикс + повтор).

- [ ] **Шаг 8.1. Полная сборка Release без предупреждений**

```
dotnet build src/PgWorker.slnx -c Release
```
Ожидание: 0 warnings/error (`TreatWarningsAsErrors`).

- [ ] **Шаг 8.2. Все юниты S3Worker**

```
dotnet test src/tests/S3Worker.UnitTests -c Release
```

- [ ] **Шаг 8.3. Все интеграционные S3Worker** (после финальной строки прогона — убедиться, что контейнеры фикстур снесены; `docker ps | grep s3w-` пуст при отсутствии стендовых)

```
dotnet test src/tests/S3Worker.IntegrationTests -c Release --filter "FullyQualifiedName!~E2e"
```

- [ ] **Шаг 8.4. Кейс-маркер E2E на свежем Release** (канон AGENTS.md для задач, трогающих код воркеров)

```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~S3E2eSyncProbeTests
```

- [ ] **Шаг 8.5. Регрессии прочих доменов** — код вне `S3Worker.*` не менялся (Shared/OwnS3-грани только читаются); прогнать юниты решения целиком:

```
dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName!~E2e&FullyQualifiedName!~IntegrationTests"
```

- [ ] **Шаг 8.6. Итоговый коммит** (если были фиксы)

```
git add -A
git commit -m "t33: фиксы по итогам финальной верификации"
```

---

## Соответствие задач требованиям spec

| Требование spec | Задача/шаг |
|---|---|
| §3.2 connectivity: ping-матрица, агрегаты, probe_error | 3.2, тесты 3.8-1 |
| §3.3 sync: пары, свёртка, diff, лаг, вердикты, бюджет | 3.3–3.5, тесты 3.8-2..5 |
| §3.4 триггеры: период + заявка put-if-absent/del | 3.1, 5.1, тесты 3.8-6, 5.4 |
| §3.5 процесс F в тике | 3.1–3.6, 4.5 |
| §3.6 клиент грани (4 пункта) | 1.4–1.7, тесты 1.8 |
| §3.7 API sync-probe | 5.1–5.3, тесты 5.4 |
| §3.8 парсер + ключи-хелперы | 2.2, 2.5, тесты 2.7-5 |
| §3.9 метрики sync_probe_* | 3.1 (счётчики), 3.2/3.5/3.6 (точки инкремента) |
| §3.10 идемпотентность/X3/клэйм/multi-host | 2.6, 3.1 (EnsureClaimed), 3.6 |
| §5 состав изменений по проектам | 1 (Core), 2 (Etcd/Provisioning-хелперы), 3 (Provisioning-процесс), 4–5 (App), 5–7 (тесты) |
| §6 фазы реализации | 1→2→3→4/5→6→7→8 |
| §8.1 юнит-критерии | 3.8 (1–8), 5.4 (9) |
| §8.2 интеграционные | 6.2–6.3 (+3.8-6 для «Failed → заявка жива») |
| §8.3 E2E | 7.1–7.6 |
| §8.4 регрессии | 1.10, 4.6, 7.8, 8.x |

## Открытые допущения (проверены по коду worktree)

1. `SigV4GetSigner` подписывал пустую query — для `sync-report?bucket=&marker=` обязательна канонизация query (Задача 1.1–1.2), серверный `CanonicalRequestBuilder` включает RawQuery в подпись — подтверждено чтением кода.
2. `SyncMaxPages` положен в `ThresholdsOptions` рядом с `SyncIntervalSec` (spec §5.4 перечисляет оба значения одной строкой после упоминания секции Thresholds); в arch/24 §8 SyncIntervalSec уже канонизирован, SyncMaxPages — budgets-параметр зонда этой задачи, arch не описывает и не запрещает.
3. Ошибка G прерывает Active-ветку до F (ранний return, как сегодня для C→D→E→G): зонд повторит заявку следующим тиком — соответствует §3.4 («заявка живёт до успеха»).
4. Scenario §8.3.3 шаг Assert 2 (пары после UNREACHABLE) — канон arch/23 §3 (пара существует при записи хотя бы у одной опрошенной стороны) может расходиться с краткой формулировкой §8.3.3 («sync без её пар»: без её ЗАПИСЕЙ); исполнителю дано указание сверяться с каноном и не подгонять ассерты.
