# Valkey-домен AdminPanel (t03-valkey-panel) — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** четвёртый домен AdminPanel — valkey: домен-снапшот `ValkeySnapshot` из etcd, REST API (инспекция + 5 мутаций-прокси в API ValkeyWorker), React-панель, live-проба PING (RESP-миниклиент), 8 алертов, грань «Воркеры» (третья карточка), стендовая интеграция (профиль `valkey`, сид `demo`, чек `51-valkey-api.sh`).

**Архитектура:** перенос механики Kafka-домена панели 1:1 с усечениями (одна нода `node1`, нет топиков/ребалансов/lifecycle/CA): `ValkeySnapshotRefresher` (тик 3 с, 4 KV-чтения) → `ValkeySnapshotStore`; мутации — `WorkerApiGateway` → HTTP API ValkeyWorker (mTLS); проба — `ValkeyProbeLoop` (тик 15 с, PING по admin-кредам из internal-стора `ValkeySecretsStore`); панель в etcd valkey-домена не пишет ничего.

**Технологии:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), React+Mantine+TanStack Query, bash+jq, Testcontainers. Новых внешних пакетов НЕТ (RESP-миниклиент — своя копия по образцу `src/ValkeyWorker.Core/Valkey/ValkeyConnection.cs`).

**Spec:** `docs/superpowers/2026-09-17-t03-valkey-panel/spec.md` (в этом же каталоге).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t03-valkey-panel` (ветка `feat-t03-valkey-panel`). Все пути ниже — от корня worktree; ВСЕ команды выполнять из корня worktree.

## Глобальные ограничения (из spec §2/§7; обязательны для каждой задачи)

- Канон: `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md` §1.1, панельные проекции `arch/adminpanel/02-etcd-contract.md` §11/§2.3.3/§9.9, `arch/adminpanel/03-panels.md` §8, `arch/adminpanel/04-local-stand.md` §1/§2.4/§3 — arch-правки УЖЕ в ветке (Task 1 фиксирует их коммитом ДО кода).
- Образец — Kafka-домен: где механика совпадает, код/структура переносятся 1:1 с заменой домена; код `src/{PgWorker,KafkaWorker,ValkeyWorker}.*` и `Shared.*` НЕ трогается (кроме правок `WorkerApiGateway`/`WorkerCertService`/`WorkerHealthPoller`/`GetWorkersQuery` — файлов `AdminPanel.*`).
- Панель в etcd valkey-домена НЕ пишет: читает `/valkey/clusters/` + `/valkeyworker/{rotations,api}/` + ключ `/workers/api_tls/valkeyworker`; мутации — только прокси в API воркера (коды 1:1, ProblemDetails как есть, недоступность → собственный 503).
- Секреты: `app_user`/`app_password` не читаются вовсе; `admin_*` — только internal-стор проб; в DTO/UI/API креды не отдаются никогда.
- Толерантность парсеров (arch/20 §5): битый JSON → parseError без исключения; незнакомое `state` → Active; пустой `endpoints` → null; неполные креды → кластер без пробы; транспортный провал любого KV-чтения роняет тик refresher'а.
- Валидации/дефолты (02 §11.3): maxmemory def 536870912 (512 MiB) ≥ 1; policy — 8 значений, def `allkeys-lru`; cpu 0.01..64 def 1; memGi/diskGi 1..65536 def 1/10; инвариант R3 `maxmemoryBytes < memGi GiB`. Панель НЕ валидирует на сервере (воркер — источник истины); фронт дублирует для UX.
- Сборка 0 warnings (`TreatWarningsAsErrors=true`); документация/комментарии — русские; идентификаторы — английские; тесты — AAA-комментарии.
- Тесты: динамические порты (testcontainers), guid-изоляция, полный teardown + ассерт чистоты, таймауты ≤ 100 с; после КАЖДОЙ docker-серии — зачистка контейнеров/сетей (`docker network prune -f`, страховочный гейт AGENTS.md).
- Локальные образы `adminpanel:dev`/`valkeyworker:dev` в registry `192.168.0.1:5000` НЕ класть.
- Каждая задача завершается зелёной сборкой и коммитом в ветке.

---

### Task 1: Коммит arch-правок (arch-first)

**Files:**
- Commit: `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `arch/adminpanel/{01-architecture,02-etcd-contract,03-panels,04-local-stand}.md`, `docs/superpowers/2026-09-17-t03-valkey-panel/spec.md` (весь каталог задачи).

**Interfaces:** нет (документация).

- [x] **Шаг 1: Проверить состав незакоммиченных правок**

Выполнить из корня worktree:
```bash
git status --short
```
Ожидание: изменены ровно 6 arch-файлов (`arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `arch/adminpanel/01-architecture.md`, `arch/adminpanel/02-etcd-contract.md`, `arch/adminpanel/03-panels.md`, `arch/adminpanel/04-local-stand.md`) + untracked `docs/superpowers/2026-09-17-t03-valkey-panel/`. Расхождений с таблицей spec §1.2 быть не должно (правки уже внесены и одобрены на гейте user-review вместе со spec).

- [x] **Шаг 2: Закоммитить arch-правки + spec отдельным коммитом**

```bash
git add arch/20-valkey-clusters.md arch/21-valkeyworker.md \
  arch/adminpanel/01-architecture.md arch/adminpanel/02-etcd-contract.md \
  arch/adminpanel/03-panels.md arch/adminpanel/04-local-stand.md \
  docs/superpowers/2026-09-17-t03-valkey-panel/
git commit -m "arch(t03): valkey-домен панели — adminpanel/02 §11+§2.3.3+§9.9, 03 §8, 04 стенд, 01 упоминания, arch/21 §1.1, arch/20 §3 + spec задачи"
```

**Выход:** arch-канон valkey-домена зафиксирован в ветке ДО кода (порядок коммитов фиксирует arch-first, spec §10.9).
**Проверка:** `git log --oneline -1` показывает коммит; `git status --short` пуст.
**Spec:** §1.2, §6.1, §10.9.

---

### Task 2: Модель домена + парсер + фикстуры (TDD)

**Files:**
- Create: `src/AdminPanel.Core/Valkey/ValkeySnapshot.cs`
- Create: `src/AdminPanel.Etcd/Parsing/ValkeyParser.cs`
- Create: `src/tests/AdminPanel.UnitTests/EtcdFixtures/Valkey/clusters-canonical.json`, `EtcdFixtures/Valkey/clusters-tolerance.json`, `EtcdFixtures/Valkey/rotations.json`
- Test: `src/tests/AdminPanel.UnitTests/ValkeyParserTests.cs`, `src/tests/AdminPanel.UnitTests/ValkeyModelTests.cs`

**Interfaces (produces, используют Task 3/5/6/7):**
```csharp
namespace AdminPanel.Core.Valkey;
public sealed record ValkeySnapshot(...);                  // поля — см. Шаг 1
public sealed record ValkeyClusterInfo(...);
public sealed record ValkeyNodeInfo(...);
public sealed record ValkeyRotationTicket(string Cluster, string Role, long RequestedUnix, string? RequestedBy);
public sealed record ValkeyProbeResult(string Cluster, string Node, bool Live, long CheckedUnix, string? Error);
public enum ValkeyClusterState { Active, NotInitialized, ToRemove }
public static class ValkeyClusterStates { public static ValkeyClusterState Parse(string? raw); }

namespace AdminPanel.Etcd.Parsing;
public sealed record ValkeyClustersParseResult(
    IReadOnlyList<AdminPanel.Core.Valkey.ValkeyClusterInfo> Clusters,
    IReadOnlyList<AdminPanel.Core.KeyParseError> Errors,
    int UnknownKeyCount);
public sealed record ValkeyRotationsParseResult(
    IReadOnlyList<AdminPanel.Core.Valkey.ValkeyRotationTicket> Tickets,
    IReadOnlyList<AdminPanel.Core.KeyParseError> Errors);
public static class ValkeyParser
{
    public static ValkeyClustersParseResult ParseClusters(Shared.Etcd.Client.Kv[] kvs);   // IReadOnlyList<Kv>
    public static ValkeyRotationsParseResult ParseRotations(Shared.Etcd.Client.Kv[] kvs);
}
```

- [ ] **Шаг 1: Написать модель домена `ValkeySnapshot.cs`**

Создать `src/AdminPanel.Core/Valkey/ValkeySnapshot.cs` — порт `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` с усечением (нет topics/rebalances/reassignments/regens/lifecycle/groups/brokers-role). Тело файла:

```csharp
using AdminPanel.Core;

namespace AdminPanel.Core.Valkey;

// Домен-снапшот Valkey (arch/02 §11.1): отдельный от EtcdSnapshot/KafkaSnapshot —
// своя механика тика, теми же настройками endpoints. Immutable; refresher строит
// новый и атомарно заменяет в ValkeySnapshotStore.
public sealed record ValkeySnapshot(
    DateTimeOffset BuiltAtUtc,
    bool EtcdReachable,
    int ConsecutiveFailures,
    IReadOnlyList<ValkeyClusterInfo> Clusters,
    IReadOnlyList<ValkeyRotationTicket> Rotations,     // /valkeyworker/rotations/ (arch/20 §3)
    IReadOnlyList<WorkerEndpoint> WorkerEndpoints,     // живые ключи /valkeyworker/api/ (arch/02 §2.3.3)
    IReadOnlyList<WorkerHealth> WorkerHealth,          // опрос /healthz живых инстансов
    IReadOnlyList<ValkeyProbeResult> Probes,           // live-PING пробы (§4.6 spec)
    IReadOnlyList<Alert> Alerts,                       // ValkeyAlertEngine (arch/03 §8.4)
    IReadOnlyList<KeyParseError> ParseErrors,          // битые JSON valkey-ключей (arch/20 §5)
    int UnknownKeyCount,
    WorkerApiCert? WorkerApiCert = null);              // целевой серт /workers/api_tls/valkeyworker (arch/02 §9.9)

// Кластер /valkey/clusters/<C>/ (arch/20 §2): config + state + факт (nodes/endpoints).
public sealed record ValkeyClusterInfo(
    string Name,
    ValkeyClusterState State,                // Active|NotInitialized|ToRemove; отсутствие state = Active
    int Nodes,                               // всегда 1 (v1)
    long MaxmemoryBytes,
    string MaxmemoryPolicy,
    long? CreatedUnix,
    string? Endpoints,                       // null/пусто — воркер не дописал (алерт у Active)
    IReadOnlyList<ValkeyNodeInfo> NodesList, // node1 (v1 — один элемент)
    ValkeyRotationTicket? Rotation = null);  // живая заявка ротации (джойн по кластеру)

// Нода node<k>: state — raw-строка (NOT_INITIALIZED|PROVISIONING|RUNNING|UNREACHABLE|
// REMOVING|TO_REMOVE; толерантно к новым); Live — из PING-пробы (null — проба молчит).
public sealed record ValkeyNodeInfo(
    string Name,
    string? State,
    decimal? Cpu,
    int? MemGi,
    int? DiskGi,
    bool? Live = null,
    string? ProbeError = null);

// Заявка ротации /valkeyworker/rotations/<C> (arch/20 §3): role app|admin + аудит.
public sealed record ValkeyRotationTicket(
    string Cluster, string Role, long RequestedUnix, string? RequestedBy);

// Результат live-пробы кластера (spec §4.6): одна нода в v1.
public sealed record ValkeyProbeResult(
    string Cluster, string Node, bool Live, long CheckedUnix, string? Error);

// Состояние кластера: config.state (arch/20 §2); отсутствие = Active.
public enum ValkeyClusterState
{
    Active,
    NotInitialized,
    ToRemove,
}

// Маппинг config.state → enum (arch/20 §5: незнакомое значение — толерантно, Active).
public static class ValkeyClusterStates
{
    public static ValkeyClusterState Parse(string? raw) => raw switch
    {
        "NOT_INITIALIZED" => ValkeyClusterState.NotInitialized,
        "TO_REMOVE" => ValkeyClusterState.ToRemove,
        _ => ValkeyClusterState.Active,
    };
}
```

- [ ] **Шаг 2: Написать фикстуры парсера**

Создать `src/tests/AdminPanel.UnitTests/EtcdFixtures/Valkey/clusters-canonical.json` — канонические примеры arch/20 §2.1 дословно (приёмочный критерий, spec §4.2):

```json
[
  { "key": "/valkey/clusters/cache/config", "value": "{\"nodes\":1,\"maxmemory_bytes\":536870912,\"maxmemory_policy\":\"allkeys-lru\",\"created_unix\":1756500000,\"state\":\"NOT_INITIALIZED\"}", "modRevision": 1 },
  { "key": "/valkey/clusters/cache/nodes/node1/state", "value": "NOT_INITIALIZED", "modRevision": 2 },
  { "key": "/valkey/clusters/cache/nodes/node1/resources", "value": "{\"cpu\":\"2\",\"mem\":\"4Gi\",\"disk\":\"40Gi\"}", "modRevision": 3 },
  { "key": "/valkey/clusters/live/config", "value": "{\"nodes\":1,\"maxmemory_bytes\":536870912,\"maxmemory_policy\":\"allkeys-lru\",\"created_unix\":1756500000}", "modRevision": 4 },
  { "key": "/valkey/clusters/live/endpoints", "value": "host.docker.internal:17001", "modRevision": 5 },
  { "key": "/valkey/clusters/live/nodes/node1/state", "value": "RUNNING", "modRevision": 6 },
  { "key": "/valkey/clusters/live/nodes/node1/resources", "value": "{\"cpu\":\"1\",\"mem\":\"1Gi\",\"disk\":\"10Gi\"}", "modRevision": 7 },
  { "key": "/valkey/clusters/live/admin_user", "value": "admin", "modRevision": 8 },
  { "key": "/valkey/clusters/live/admin_password", "value": "0123456789abcdef0123456789abcdef", "modRevision": 9 },
  { "key": "/valkey/clusters/live/app_user", "value": "app", "modRevision": 10 },
  { "key": "/valkey/clusters/live/app_password", "value": "ffffffffffffffffffffffffffffffff", "modRevision": 11 },
  { "key": "/valkey/clusters/dying/config", "value": "{\"nodes\":1,\"maxmemory_bytes\":1073741824,\"maxmemory_policy\":\"noeviction\",\"created_unix\":1756500000,\"state\":\"TO_REMOVE\"}", "modRevision": 12 },
  { "key": "/valkey/clusters/unknown/future_feature", "value": "{}", "modRevision": 13 }
]
```

`EtcdFixtures/Valkey/clusters-tolerance.json` — строки таблицы arch/20 §5 (все 6, spec §5.1):

```json
[
  { "key": "/valkey/clusters/bad/config", "value": "{\"nodes\":1,", "modRevision": 1 },
  { "key": "/valkey/clusters/weird/config", "value": "{\"nodes\":1,\"maxmemory_bytes\":1,\"maxmemory_policy\":\"allkeys-lru\",\"created_unix\":1,\"state\":\"FUTURE_STATE\"}", "modRevision": 2 },
  { "key": "/valkey/clusters/noep/config", "value": "{\"nodes\":1,\"maxmemory_bytes\":1,\"maxmemory_policy\":\"noeviction\",\"created_unix\":1}", "modRevision": 3 },
  { "key": "/valkey/clusters/noep/endpoints", "value": "   ", "modRevision": 4 },
  { "key": "/valkey/clusters/badres/config", "value": "{\"nodes\":1,\"maxmemory_bytes\":1,\"maxmemory_policy\":\"noeviction\",\"created_unix\":1}", "modRevision": 5 },
  { "key": "/valkey/clusters/badres/nodes/node1/resources", "value": "{\"cpu\":\"x\",\"mem\":\"4\",\"disk\":\"40Gb\"}", "modRevision": 6 },
  { "key": "/valkey/clusters/half/admin_user", "value": "admin", "modRevision": 7 }
]
```

`EtcdFixtures/Valkey/rotations.json`:

```json
[
  { "key": "/valkeyworker/rotations/live", "value": "{\"role\":\"app\",\"requested_unix\":1756500123,\"requested_by\":\"seed\"}", "modRevision": 1 },
  { "key": "/valkeyworker/rotations/other", "value": "{\"role\":\"admin\",\"requested_unix\":1756500456}", "modRevision": 2 },
  { "key": "/valkeyworker/rotations/broken", "value": "{oops", "modRevision": 3 },
  { "key": "/valkeyworker/rotations/nofield", "value": "{\"role\":\"app\"}", "modRevision": 4 }
]
```

Фикстуры копируются в выходной каталог: в `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj` уже есть `None-ItemGroup` для `EtcdFixtures/**` (проверить `grep -n EtcdFixtures src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`; glob `EtcdFixtures/**` покроет подпапку `Valkey/` — еслиItemGroup точечный по файлам, добавить строку `<None Include="EtcdFixtures/**" CopyToOutputDirectory="PreserveNewest" />` по образцу существующих).

- [ ] **Шаг 3: Написать падающие тесты парсера (`ValkeyParserTests.cs`)**

Порт `src/tests/AdminPanel.UnitTests/KafkaParserTests.cs` (AAA-комментарии, xUnit). Ключевые тесты (все обязательны):

```csharp
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd.Parsing;
using Shared.Etcd.Client;

namespace AdminPanel.UnitTests;

// Парсер valkey-домена: канон arch/20 §2.1 (приёмочные) + толерантность §5.
public sealed class ValkeyParserTests
{
    // Arrange: канонические фикстуры §2.1. Act: ParseClusters. Assert: поля дословно.
    [Fact]
    public void ParseClusters_Canonical_BuildsModel()
    {
        var kvs = EtcdFixtures.LoadKv("Valkey/clusters-canonical.json");

        var result = ValkeyParser.ParseClusters(kvs);

        // cache: заявка NOT_INITIALIZED (config со state), нода без live.
        var cache = Assert.Single(result.Clusters, c => c.Name == "cache");
        Assert.Equal(ValkeyClusterState.NotInitialized, cache.State);
        Assert.Equal(1, cache.Nodes);
        Assert.Equal(536870912L, cache.MaxmemoryBytes);
        Assert.Equal("allkeys-lru", cache.MaxmemoryPolicy);
        Assert.Equal(1756500000L, cache.CreatedUnix);
        Assert.Null(cache.Endpoints);
        var node = Assert.Single(cache.NodesList);
        Assert.Equal("node1", node.Name);
        Assert.Equal("NOT_INITIALIZED", node.State);
        Assert.Equal(2m, node.Cpu);
        Assert.Equal(4, node.MemGi);
        Assert.Equal(40, node.DiskGi);

        // live: Active-config БЕЗ поля state; endpoints фактом.
        var live = Assert.Single(result.Clusters, c => c.Name == "live");
        Assert.Equal(ValkeyClusterState.Active, live.State);
        Assert.Equal("host.docker.internal:17001", live.Endpoints);
        Assert.Equal("RUNNING", Assert.Single(live.NodesList).State);

        // dying: TO_REMOVE сохранён.
        Assert.Equal(ValkeyClusterState.ToRemove,
            Assert.Single(result.Clusters, c => c.Name == "dying").State);

        // app_*/admin_* пропущены МОЛЧА: в модель не попадают и unknownKeys не растят.
        Assert.Equal(1, result.UnknownKeyCount); // только future_feature
        Assert.Empty(result.Errors);
    }

    // Arrange: битый config/weird state/пустой endpoints/битые ресурсы.
    // Act: ParseClusters. Assert: parseError-записи без исключений; странное state → Active.
    [Fact]
    public void ParseClusters_Tolerance_TableRows()
    {
        var result = ValkeyParser.ParseClusters(EtcdFixtures.LoadKv("Valkey/clusters-tolerance.json"));

        // битый JSON config → кластер-скелет + parseError.
        Assert.Contains(result.Errors, e => e.Key == "/valkey/clusters/bad/config");
        // незнакомое state → Active.
        Assert.Equal(ValkeyClusterState.Active,
            Assert.Single(result.Clusters, c => c.Name == "weird").State);
        // пустой/пробельный endpoints → null (нет — как отсутствие).
        Assert.Null(Assert.Single(result.Clusters, c => c.Name == "noep").Endpoints);
        // resources: cpu не число → parseError; неканонический суффикс mem/disk → null-поля (не ошибка формата Gi).
        var badRes = Assert.Single(result.Clusters, c => c.Name == "badres");
        var res = Assert.Single(badRes.NodesList);
        Assert.Null(res.Cpu);
        Assert.Null(res.MemGi);
        Assert.Null(res.DiskGi);
        // частичные креды (один admin_user) — не ошибка парсера: ignored-ключ.
        Assert.DoesNotContain(result.Errors, e => e.Key.Contains("half", StringComparison.Ordinal));
    }

    // Arrange: rotations.json. Act: ParseRotations. Assert: role raw-строка; битый JSON → parseError.
    [Fact]
    public void ParseRotations_RawRole_And_BrokenJson()
    {
        var result = ValkeyParser.ParseRotations(EtcdFixtures.LoadKv("Valkey/rotations.json"));

        Assert.Equal(2, result.Tickets.Count);
        var app = Assert.Single(result.Tickets, t => t.Cluster == "live");
        Assert.Equal("app", app.Role);
        Assert.Equal("seed", app.RequestedBy);
        var admin = Assert.Single(result.Tickets, t => t.Cluster == "other");
        Assert.Equal("admin", admin.Role); // raw-строка, толерантно к новым значениям
        Assert.Null(admin.RequestedBy);
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/rotations/broken");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/rotations/nofield");
    }
}
```

- [ ] **Шаг 4: Убедиться, что тесты падают**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~ValkeyParser"
```
Ожидание: ошибка компиляции (`ValkeyParser`/`ValkeySnapshot` не найдены).

- [ ] **Шаг 5: Написать `ValkeyParser.cs` (минимальная реализация)**

Создать `src/AdminPanel.Etcd/Parsing/ValkeyParser.cs` — порт `KafkaParser.cs` с усечением. Полная структура (вместо TODO — реализация сразу):

```csharp
using System.Globalization;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора префикса /valkey/clusters/ (arch/02 §11.1).
public sealed record ValkeyClustersParseResult(
    IReadOnlyList<ValkeyClusterInfo> Clusters,
    IReadOnlyList<KeyParseError> Errors,
    int UnknownKeyCount);

// Результат разбора очереди ротаций /valkeyworker/rotations/ (arch/20 §3).
public sealed record ValkeyRotationsParseResult(
    IReadOnlyList<ValkeyRotationTicket> Tickets,
    IReadOnlyList<KeyParseError> Errors);

// Парсер valkey-домена: чистые функции Kv[] → модель, битые значения не бросают
// исключений — порождают KeyParseError (порт KafkaParser; arch/20 §5).
public static class ValkeyParser
{
    private sealed class NodeAcc(string name)
    {
        public readonly string Name = name;
        public string? State;
        public string? ResourcesRaw;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;
        public string? ConfigRaw;
        public string? Endpoints;
        public readonly Dictionary<string, NodeAcc> Nodes = [];
    }

    public static ValkeyClustersParseResult ParseClusters(IReadOnlyList<Kv> kvs)
    {
        var errors = new List<KeyParseError>();
        var unknown = 0;
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/valkey/clusters/<C>/leaf…" → ["", "valkey", "clusters", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5 || segments[1] != "valkey" || segments[2] != "clusters"
                || segments[3].Length == 0)
            {
                unknown++;
                continue;
            }

            var acc = GetOrAdd(accs, segments[3], static name => new ClusterAcc(name));
            switch (segments[4])
            {
                case "config" when segments.Length == 5:
                    acc.ConfigRaw = kv.Value;
                    break;

                case "endpoints" when segments.Length == 5:
                    // Пустой/пробельный → отсутствует (arch/20 §5).
                    acc.Endpoints = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    break;

                // Креды: панель app_* не читает вовсе, admin_* — только refresher в
                // secrets-стор (§4.4); здесь — expected-skip без unknownKeys (02 §11.1).
                case "app_user" or "app_password" or "admin_user" or "admin_password"
                    when segments.Length == 5:
                    break;

                case "nodes" when segments.Length == 7
                    && segments[5].Length > 0
                    && segments[6] is "state" or "resources":
                {
                    var node = GetOrAdd(acc.Nodes, segments[5], static name => new NodeAcc(name));
                    if (segments[6] == "state")
                        node.State = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
                    else
                        node.ResourcesRaw = kv.Value;
                    break;
                }

                default:
                    // система развивается — неизвестный ключ не ошибка, только счётчик (arch/20 §5)
                    unknown++;
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(c => c.Name, StringComparer.Ordinal)
            .Select(acc => BuildCluster(acc, errors))
            .ToList();

        return new ValkeyClustersParseResult(clusters, errors, unknown);
    }

    // Ротации: {"role","requested_unix","requested_by"}; role — raw-строка (толерантно);
    // битый JSON / нет requested_unix → parseError-запись (ключ не трогаем).
    public static ValkeyRotationsParseResult ParseRotations(IReadOnlyList<Kv> kvs)
    {
        var tickets = new List<ValkeyRotationTicket>();
        var errors = new List<KeyParseError>();
        foreach (var kv in kvs)
        {
            // "/valkeyworker/rotations/<C>" → ["", "valkeyworker", "rotations", <C>]
            var segments = kv.Key.Split('/');
            if (segments.Length != 4 || segments[3].Length == 0)
            {
                errors.Add(new(kv.Key, "ожидается /valkeyworker/rotations/<cluster>"));
                continue;
            }

            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                var root = doc.RootElement;
                var requested = JsonValues.ReadLong(root, "requested_unix");
                if (requested is null)
                {
                    errors.Add(new(kv.Key, "нет поля requested_unix"));
                    continue;
                }

                tickets.Add(new ValkeyRotationTicket(
                    segments[3],
                    JsonValues.ReadString(root, "role") ?? "",
                    requested.Value,
                    JsonValues.ReadString(root, "requested_by")));
            }
            catch (JsonException e)
            {
                errors.Add(new(kv.Key, $"битый JSON: {e.Message}"));
            }
        }

        return new(tickets, errors);
    }

    private static ValkeyClusterInfo BuildCluster(ClusterAcc acc, List<KeyParseError> errors)
    {
        var (nodes, maxmemory, policy, createdUnix, state) = ParseConfig(acc.Name, acc.ConfigRaw, errors);
        var nodeList = acc.Nodes
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(pair => BuildNode(acc.Name, pair.Value, errors))
            .ToList();
        return new ValkeyClusterInfo(
            acc.Name, state, nodes, maxmemory, policy, createdUnix, acc.Endpoints, nodeList);
    }

    // Ключа config нет — кластер-скелет из прочих ключей; не ошибка парсера.
    private static (
        int Nodes, long MaxmemoryBytes, string MaxmemoryPolicy, long? CreatedUnix,
        ValkeyClusterState State)
        ParseConfig(string cluster, string? raw, List<KeyParseError> errors)
    {
        if (raw is null)
            return (0, 0, "", null, ValkeyClusterState.Active);

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            return (
                AsInt(JsonValues.ReadLong(root, "nodes")),
                JsonValues.ReadLong(root, "maxmemory_bytes") ?? 0,
                JsonValues.ReadString(root, "maxmemory_policy") ?? "",
                JsonValues.ReadLong(root, "created_unix"),
                ValkeyClusterStates.Parse(JsonValues.ReadString(root, "state")));
        }
        catch (JsonException)
        {
            errors.Add(new KeyParseError($"/valkey/clusters/{cluster}/config", "битый JSON config"));
            return (0, 0, "", null, ValkeyClusterState.Active);
        }
    }

    // resources: cpu — decimal invariant; mem/disk — "<int>Gi" → int; неканонический
    // суффикс/число → поле null (не ошибка — заявка неполна); cpu не число → parseError.
    private static ValkeyNodeInfo BuildNode(string cluster, NodeAcc acc, List<KeyParseError> errors)
    {
        decimal? cpu = null;
        int? memGi = null, diskGi = null;
        if (acc.ResourcesRaw is not null)
        {
            try
            {
                using var doc = JsonDocument.Parse(acc.ResourcesRaw);
                var root = doc.RootElement;
                var cpuRaw = JsonValues.ReadString(root, "cpu");
                if (cpuRaw is not null
                    && decimal.TryParse(cpuRaw, NumberStyles.Number, CultureInfo.InvariantCulture, out var cpuValue))
                    cpu = cpuValue;
                else
                    errors.Add(new KeyParseError(
                        $"/valkey/clusters/{cluster}/nodes/{acc.Name}/resources", "поле cpu не число"));

                memGi = ParseGi(JsonValues.ReadString(root, "mem"));
                diskGi = ParseGi(JsonValues.ReadString(root, "disk"));
            }
            catch (JsonException)
            {
                errors.Add(new KeyParseError(
                    $"/valkey/clusters/{cluster}/nodes/{acc.Name}/resources", "битый JSON resources"));
            }
        }

        return new ValkeyNodeInfo(acc.Name, acc.State, cpu, memGi, diskGi);
    }

    private static int? ParseGi(string? raw)
        => raw is not null && raw.EndsWith("Gi", StringComparison.Ordinal)
           && int.TryParse(raw[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    private static int AsInt(long? value) => value is null ? 0 : (int)value.Value;

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
```

- [ ] **Шаг 6: Написать `ValkeyModelTests.cs` (state-маппинг)**

```csharp
using AdminPanel.Core.Valkey;

namespace AdminPanel.UnitTests;

// Модель valkey-домена: толерантный state-маппинг (arch/20 §5).
public sealed class ValkeyModelTests
{
    // Arrange: сырые значения state. Act: Parse. Assert: канон + незнакомое → Active.
    [Theory]
    [InlineData("NOT_INITIALIZED", ValkeyClusterState.NotInitialized)]
    [InlineData("TO_REMOVE", ValkeyClusterState.ToRemove)]
    [InlineData(null, ValkeyClusterState.Active)]
    [InlineData("", ValkeyClusterState.Active)]
    [InlineData("SOMETHING_NEW", ValkeyClusterState.Active)]
    public void Parse_State_MapsTolerantly(string? raw, ValkeyClusterState expected)
    {
        var actual = ValkeyClusterStates.Parse(raw);
        Assert.Equal(expected, actual);
    }
}
```

- [ ] **Шаг 7: Прогнать тесты — зелёные**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Valkey"
```
Ожидание: PASS (ValkeyParserTests 3 + ValkeyModelTests 5).

- [ ] **Шаг 8: Полная сборка + коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Core/Valkey/ src/AdminPanel.Etcd/Parsing/ValkeyParser.cs \
  src/tests/AdminPanel.UnitTests/ValkeyParserTests.cs src/tests/AdminPanel.UnitTests/ValkeyModelTests.cs \
  src/tests/AdminPanel.UnitTests/EtcdFixtures/Valkey/ src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj
git commit -m "feat(valkey-panel): модель домена ValkeySnapshot + парсер (канон arch/20 §2.1/§5) + фикстуры"
```

**Выход:** модель + парсер + приёмочные фикстуры; `ValkeyParser`/`ValkeyClusterStates` готовы для Task 3.
**Проверка:** шаг 7 зелёный, сборка 0 warnings.
**Spec:** §4.1, §4.2, §5.1 (ValkeyParserTests/ValkeyModelTests), §6.2.

---

### Task 3: Refresher + сторы + регистрации (TDD)

**Files:**
- Create: `src/AdminPanel.Core/Valkey/ValkeyAlerting/{IValkeyAlertEngine,ValkeyAlertEngine,ValkeyAlertsOptions}.cs` (минимальный скелет — каталог наполняет Task 5)
- Create: `src/AdminPanel.Etcd/ValkeySnapshotStore.cs`, `src/AdminPanel.Etcd/ValkeySecretsStore.cs`, `src/AdminPanel.Etcd/ValkeyPanelOptions.cs`, `src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs`, `src/AdminPanel.Etcd/ValkeyProbeReader.cs`
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs` (метод `AddValkey()`)
- Modify: `src/AdminPanel.Api/Program.cs` (строка `.AddValkey()` после `.AddKafka()`)
- Test: `src/tests/AdminPanel.UnitTests/ValkeyRefresherTests.cs`

**Interfaces (produces; используют Task 4/5/6/7):**
```csharp
namespace AdminPanel.Etcd;
public interface IValkeySnapshotReader { AdminPanel.Core.Valkey.ValkeySnapshot? Current { get; } }
public interface IValkeySnapshotStore : IValkeySnapshotReader
{ new AdminPanel.Core.Valkey.ValkeySnapshot? Current { get; } void Replace(AdminPanel.Core.Valkey.ValkeySnapshot snapshot); }
public sealed class ValkeySnapshotStore : IValkeySnapshotStore;
public sealed record ValkeyClusterSecrets(string Cluster, string AdminUser, string AdminPassword);
public interface IValkeySecretsStore
{ IReadOnlyDictionary<string, ValkeyClusterSecrets> Current { get; } void Replace(IReadOnlyDictionary<string, ValkeyClusterSecrets> secrets); }
public sealed class ValkeySecretsStore : IValkeySecretsStore;
public interface IValkeyProbeReader { IReadOnlyList<AdminPanel.Core.Valkey.ValkeyProbeResult>? Current { get; } }
[Config("AdminPanel:Valkey")] public class ValkeyPanelOptions { public double RefreshIntervalSeconds { get; set; } = 3; }
public sealed class ValkeySnapshotRefresher(...) : BackgroundService
{ public Task<Result> RefreshOnceAsync(CancellationToken ct); public static IReadOnlyList<ValkeyClusterInfo> MergeProbes(...); }

namespace AdminPanel.Core.Valkey.ValkeyAlerting;
public interface IValkeyAlertEngine
{ IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? previous); }
[Config("AdminPanel:ValkeyAlerts")] public class ValkeyAlertsOptions { public int FreshProvisioningSeconds { get; set; } = 60; }
```

- [ ] **Шаг 1: Написать падающие тесты тика (`ValkeyRefresherTests.cs`)**

Порт `src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs` (изучить его приёмы: fake-gateway с KV-ответами, `FixedTimeProvider`). Обязательные тесты:

```csharp
// Arrange: KV-gateway с ключами 4 префиксов (канонические значения + ключ
// /valkeyworker/api/i1 + ключ серта /workers/api_tls/valkeyworker).
// Act: RefreshOnceAsync. Assert: снапшот собран: кластеры/ротации/endpoints/cert.
[Fact] public void RefreshOnce_Canonical_BuildsSnapshot()

// Arrange: один из range-запросов (любой префикс) отвечает ошибкой.
// Act: RefreshOnceAsync. Assert: Result неуспешен; прежний снапшот в сторе
// (EtcdReachable=false, ConsecutiveFailures=+1, данные прежние).
[Fact] public void RefreshOnce_KvFail_FailsTickKeepsPrevious()

// Arrange: полный набор admin_user+admin_password и частичный (только admin_user).
// Act: RefreshOnceAsync. Assert: полный — в IValkeySecretsStore; частичный — пропущен без ошибки.
[Fact] public void RefreshOnce_Secrets_FullAndPartialSets()

// Arrange: в IValkeyProbeReader лежит живая проба кластера live (Live=true, Error=null)
// и ошибка для cache. Act: RefreshOnceAsync. Assert: NodesList[].Live/ProbeError
// переносятся в снапшот; кластер без пробы — Live=null.
[Fact] public void RefreshOnce_MergesProbeResults()

// Arrange: кластеры без проб + список ValkeyProbeResult (живая/ошибка/чужой кластер).
// Act: прямой вызов ValkeySnapshotRefresher.MergeProbes (public static; spec §5.1
// «мердж live-проб» в модельных тестах). Assert: Live/ProbeError в нужных нодах,
// прочие — без изменений, чужой кластер игнорируется.
[Fact] public void MergeProbes_Direct_MergesLiveAndError()

// Arrange: активная ротация кластера live. Act: RefreshOnceAsync.
// Assert: ValkeyClusterInfo.Rotation заполнен (джойн по имени кластера).
[Fact] public void RefreshOnce_JoinsRotationTicket()
```

Реализация — по механике `KafkaRefresherTests.cs` (заглянуть в него для fake-гв: как подменяют `IEtcdGateway`; повторить 1:1). Тесты не компилируются (нет классов).

- [ ] **Шаг 2: Убедиться, что падают (компиляция)**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~ValkeyRefresher"
```

- [ ] **Шаг 3: Написать сторы, опции, читатель проб**

`src/AdminPanel.Etcd/ValkeySnapshotStore.cs` — порт `KafkaSnapshotStore.cs` 1:1:
```csharp
using AdminPanel.Core.Valkey;

namespace AdminPanel.Etcd;

// Читатели valkey-домена (инспекция API, проба) — без блокировок.
public interface IValkeySnapshotReader
{
    // До первого тика снапшота нет — потребители показывают «загрузку».
    ValkeySnapshot? Current { get; }
}

// Хранилище текущего снапшота valkey (порт KafkaSnapshotStore): писатель один —
// ValkeySnapshotRefresher; атомарная замена volatile-ссылки.
public interface IValkeySnapshotStore : IValkeySnapshotReader
{
    new ValkeySnapshot? Current { get; }

    void Replace(ValkeySnapshot snapshot);
}

// Регистрация — явно в ModuleExtensions.AddValkey() (симметрия AddKafka).
public sealed class ValkeySnapshotStore : IValkeySnapshotStore
{
    private volatile ValkeySnapshot? _current;

    public ValkeySnapshot? Current => _current;

    public void Replace(ValkeySnapshot snapshot) => _current = snapshot;
}
```

`src/AdminPanel.Etcd/ValkeySecretsStore.cs` — порт `KafkaSecretsStore.cs` без CaPem:
```csharp
namespace AdminPanel.Etcd;

// Per-cluster admin-креды (arch/02 §11.1): панель читает admin_user/admin_password
// ТОЛЬКО для live-проб PING; в модель ValkeyClusterInfo/UI/API не выносит никогда.
// app-креды панель не читает вовсе (роль приложений).
public sealed record ValkeyClusterSecrets(string Cluster, string AdminUser, string AdminPassword);

// Внутренний стор кредов: заполняет ValkeySnapshotRefresher при тике, читает
// valkey-проба (spec §4.6). Значение пароля не покидает этот контур.
public interface IValkeySecretsStore
{
    IReadOnlyDictionary<string, ValkeyClusterSecrets> Current { get; }

    void Replace(IReadOnlyDictionary<string, ValkeyClusterSecrets> secrets);
}

public sealed class ValkeySecretsStore : IValkeySecretsStore
{
    private volatile IReadOnlyDictionary<string, ValkeyClusterSecrets> _current =
        new Dictionary<string, ValkeyClusterSecrets>();

    public IReadOnlyDictionary<string, ValkeyClusterSecrets> Current => _current;

    public void Replace(IReadOnlyDictionary<string, ValkeyClusterSecrets> secrets) => _current = secrets;
}
```

`src/AdminPanel.Etcd/ValkeyPanelOptions.cs`:
```csharp
using Shared.Core.DI;

namespace AdminPanel.Etcd;

// [Config]-POCO valkey-домена панели: секция AdminPanel:Valkey (arch/02 §11).
// Endpoints etcd — общие с pg-циклом через AdminPanel:Etcd (EtcdOptions).
[Config("AdminPanel:Valkey")]
public class ValkeyPanelOptions
{
    // Тик ValkeySnapshotRefresher. <= 0 — fallback 3 c (симметрия kafka, arch/02 §4).
    public double RefreshIntervalSeconds { get; set; } = 3;
}
```

`src/AdminPanel.Etcd/ValkeyProbeReader.cs`:
```csharp
using AdminPanel.Core.Valkey;

namespace AdminPanel.Etcd;

// Live-пробы valkey (PING): Etcd-сборка знает только интерфейс; реализация-адаптер
// над реальным стором проб — в AdminPanel.Probes (паттерн IKafkaProbeReader).
public interface IValkeyProbeReader
{
    IReadOnlyList<ValkeyProbeResult>? Current { get; }
}
```

- [ ] **Шаг 4: Минимальный скелет alert-движка (наполняет Task 5)**

`src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertsOptions.cs`:
```csharp
using Shared.Core.DI;

namespace AdminPanel.Core.Valkey.ValkeyAlerting;

// [Config]-POCO порогов valkey-алертов: секция AdminPanel:ValkeyAlerts
// (arch/03 §8.4). Регистрация — автоскан AddCore().
[Config("AdminPanel:ValkeyAlerts")]
public class ValkeyAlertsOptions
{
    // valkey-node-not-running: PROVISIONING младше N секунд не алертится
    // (штатный подъём ноды — critical-шум неуместен; порт KafkaAlertsOptions).
    public int FreshProvisioningSeconds { get; set; } = 60;
}
```

`src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertEngine.cs` (Task 5 заменит тело `Enumerate` на каталог 8 kinds):
```csharp
using AdminPanel.Core;
using AdminPanel.Core.Alerting;
using Shared.Core.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Core.Valkey.ValkeyAlerting;

// Чистая функция (ValkeySnapshot next, prev) → Alert[] — каталог arch/03 §8.4.
// sinceUnix — по стабильному id из prev.Alerts (механика KafkaAlertEngine);
// сортировка severity → kind → target.
public interface IValkeyAlertEngine
{
    IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? previous);
}

[InjectAsSingleton(typeof(IValkeyAlertEngine))]
public sealed class ValkeyAlertEngine(IOptions<ValkeyAlertsOptions> options) : IValkeyAlertEngine
{
    private static readonly IComparer<AlertSeverity> SeverityDescending =
        Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x));

    private readonly ValkeyAlertsOptions _options = options.Value;

    public IReadOnlyList<Alert> Evaluate(ValkeySnapshot next, ValkeySnapshot? previous)
    {
        var nowUnix = next.BuiltAtUtc.ToUnixTimeSeconds();
        return
        [
            .. Enumerate(next, previous)
               .Select(a => a with { SinceUnix = ResolveSince(a, previous, nowUnix) })
               .OrderBy(a => a.Severity, SeverityDescending)
               .ThenBy(a => a.Kind, StringComparer.Ordinal)
               .ThenBy(a => a.Target, StringComparer.Ordinal),
        ];
    }

    // Каталог 8 kinds — наполняется задачей алертов (TDD); до неё движок пуст.
    private IEnumerable<Alert> Enumerate(ValkeySnapshot next, ValkeySnapshot? previous)
    {
        yield break;
    }

    // sinceUnix: prev нет → null; id был в prev → перенос; новый → now (pg-механика).
    private static long? ResolveSince(Alert alert, ValkeySnapshot? previous, long nowUnix)
    {
        if (previous is null)
            return null;
        var before = previous.Alerts.FirstOrDefault(a => a.Id == alert.Id);
        return before is null ? nowUnix : before.SinceUnix;
    }
}
```

- [ ] **Шаг 5: Написать `ValkeySnapshotRefresher.cs`**

Порт `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` 1:1 (sticky+failover, первый тик сразу, `RefreshOnceAsync` публичен). Ключевые отличия от образца (4 range-чтения вместо 8; secrets только admin-пара; мердж проб; ротационный джойн):

```csharp
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Core.Valkey.ValkeyAlerting;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd;

// Единственный писатель valkey-снапшота (arch/02 §11): тик RefreshIntervalSeconds,
// range /valkey/clusters/ + /valkeyworker/rotations/ + /valkeyworker/api/ и
// точечный ключ /workers/api_tls/valkeyworker на активном endpoint (sticky +
// failover, опции общие с pg-циклом EtcdOptions). Транспортный провал любого
// чтения роняет тик: прежние данные, EtcdReachable=false, счётчик отказов.
// Регистрация — явно в ModuleExtensions.AddValkey().
public sealed class ValkeySnapshotRefresher(
    IEtcdGateway gateway,
    IValkeyAlertEngine alertEngine,
    IValkeySnapshotStore store,
    IValkeySecretsStore secretsStore,
    IOptions<EtcdOptions> etcdOptions,
    IOptions<ValkeyPanelOptions> valkeyOptions,
    TimeProvider time,
    ILogger<ValkeySnapshotRefresher> logger,
    IValkeyProbeReader? probeReader = null) : BackgroundService
{
    private string? _activeEndpoint;
    private bool _endpointsWarned;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var seconds = valkeyOptions.Value.RefreshIntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Valkey:RefreshIntervalSeconds <= 0 — использую 3 c");
            seconds = 3;
        }

        // Первый тик сразу: панель набирает данные со старта (симметрия pg/kafka).
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            try
            {
                await RefreshOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро одного тика — публично для unit/integration-тестов без хоста.
    public async Task<Result> RefreshOnceAsync(CancellationToken ct)
    {
        var endpoints = etcdOptions.Value.Endpoints.Where(IsValidEndpoint).ToArray();
        if (!_endpointsWarned && endpoints.Length == 0)
        {
            logger.LogWarning("AdminPanel:Etcd:Endpoints не задан или невалиден — valkey-данные недоступны");
            _endpointsWarned = true;
        }

        var now = time.GetUtcNow();
        var previous = store.Current;

        if (endpoints.Length == 0)
            return FailTick(previous, now, "AdminPanel:Etcd:Endpoints не задан или невалиден");

        var active = _activeEndpoint is not null && endpoints.Contains(_activeEndpoint)
            ? _activeEndpoint
            : endpoints[0];

        var clustersKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Clusters, ct);
        var rotationsKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.Rotations, ct);
        var workerApiKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApi, ct);
        var certKv = await RangeWithFailoverAsync(endpoints, active, Prefixes.WorkerApiCert, ct);
        if (!clustersKv.IsSuccess || !rotationsKv.IsSuccess || !workerApiKv.IsSuccess || !certKv.IsSuccess)
            return FailTick(previous, now, "KV-чтения etcd не удались");

        _activeEndpoint = active;

        var clusters = ValkeyParser.ParseClusters(clustersKv.Value);
        var rotations = ValkeyParser.ParseRotations(rotationsKv.Value);
        var workerApi = WorkerEndpointsParser.Parse(workerApiKv.Value);
        // Префикс-запрос точечный: ровно один ключ /workers/api_tls/valkeyworker.
        var certParsed = WorkerCertParser.Parse(Prefixes.WorkerApiCert, certKv.Value.FirstOrDefault());

        // Креды проб: admin-пара — internal-стор; в модель кластера не попадает
        // (arch/02 §11.1). Частичный набор — не ошибка (ensure воркера в процессе).
        secretsStore.Replace(ReadSecrets(clustersKv.Value));

        var rotationsByCluster = rotations.Tickets
            .GroupBy(t => t.Cluster, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

        var built = new ValkeySnapshot(
            now,
            EtcdReachable: true,
            ConsecutiveFailures: 0,
            MergeProbes(
                [.. clusters.Clusters.Select(c => c with
                {
                    Rotation = rotationsByCluster.GetValueOrDefault(c.Name),
                })],
                probeReader?.Current),
            rotations.Tickets,
            workerApi.Endpoints,
            [],                         // WorkerHealth вносит health-поллер успешным тиком (Task 4)
            previous?.Probes ?? [],     // пробы переживают отказ etcd (симметрия pg/kafka)
            Alerts: [],
            [.. clusters.Errors, .. rotations.Errors, .. workerApi.Errors,
                .. WorkerCertParser.ErrorsOf(certParsed)],
            clusters.UnknownKeyCount,
            WorkerApiCert: certParsed.Cert);

        store.Replace(built with { Alerts = alertEngine.Evaluate(built, previous) });
        return Result.Success();
    }

    // Мердж live-проб: Live/ProbeError в ноды кластеров; проба молчит о кластере —
    // etcd-данные как есть (Live=null). Публичен для юнит-тестов модели (spec §5.1).
    public static IReadOnlyList<ValkeyClusterInfo> MergeProbes(
        IReadOnlyList<ValkeyClusterInfo> clusters,
        IReadOnlyList<ValkeyProbeResult>? probes)
    {
        if (probes is null || probes.Count == 0)
            return clusters;

        var byCluster = probes
            .GroupBy(p => p.Cluster, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
        return [.. clusters.Select(c => byCluster.TryGetValue(c.Name, out var probe)
            ? c with
            {
                NodesList = [.. c.NodesList.Select(n =>
                    n.Name == probe.Node ? n with { Live = probe.Live, ProbeError = probe.Error } : n)],
            }
            : c)];
    }

    // Failover: один проход по endpoints по кругу от активного (pg-механика).
    private async Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(
        string[] endpoints, string active, string prefix, CancellationToken ct)
    {
        var start = Array.IndexOf(endpoints, active);
        Exception? last = null;
        for (var i = 0; i < endpoints.Length; i++)
        {
            var endpoint = endpoints[(start + i) % endpoints.Length];
            var result = await gateway.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
            {
                _activeEndpoint = endpoint;
                return result;
            }

            last = result.Error!;
        }

        return Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException(
            $"все endpoints не ответили на range {prefix}: {last?.Message}"));
    }

    // Отказ тика: прежние данные/BuiltAtUtc, Reachable=false, счётчик растёт.
    private Result FailTick(ValkeySnapshot? previous, DateTimeOffset now, string reason)
    {
        var error = Result.Failed(new EtcdUnreachableException(reason));
        var failed = previous
            ?? new ValkeySnapshot(now, EtcdReachable: false, ConsecutiveFailures: 0,
                [], [], [], [], [], [], [], 0);
        failed = failed with { EtcdReachable = false, ConsecutiveFailures = failed.ConsecutiveFailures + 1 };

        // Алерты пересчитываются и на отказном тике (pg-семантика §4).
        store.Replace(failed with { Alerts = alertEngine.Evaluate(failed, previous) });
        return error;
    }

    // Креды проб: "/valkey/clusters/<C>/admin_user|admin_password" → стор;
    // полный набор → запись, частичный — пропуск без ошибки (ensure воркера).
    private static IReadOnlyDictionary<string, ValkeyClusterSecrets> ReadSecrets(IReadOnlyList<Kv> kvs)
    {
        var users = new Dictionary<string, string>();
        var passwords = new Dictionary<string, string>();
        foreach (var kv in kvs)
        {
            // "/valkey/clusters/<C>/admin_user" → ["", "valkey", "clusters", <C>, "admin_user"]
            var segments = kv.Key.Split('/');
            if (segments.Length != 5)
                continue;
            switch (segments[4])
            {
                case "admin_user":
                    users[segments[3]] = kv.Value;
                    break;
                case "admin_password":
                    passwords[segments[3]] = kv.Value;
                    break;
            }
        }

        var secrets = new Dictionary<string, ValkeyClusterSecrets>();
        foreach (var cluster in users.Keys.Union(passwords.Keys).OrderBy(n => n, StringComparer.Ordinal))
        {
            var user = users.GetValueOrDefault(cluster) ?? string.Empty;
            var password = passwords.GetValueOrDefault(cluster) ?? string.Empty;
            if (user.Length == 0 || password.Length == 0)
                continue;

            secrets[cluster] = new ValkeyClusterSecrets(cluster, user, password);
        }

        return secrets;
    }

    private static bool IsValidEndpoint(string endpoint)
        => Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
            && uri.Scheme is "http" or "https"
            && !string.IsNullOrEmpty(uri.Host);

    private static class Prefixes
    {
        public const string Clusters = "/valkey/clusters/";
        public const string Rotations = "/valkeyworker/rotations/";
        public const string WorkerApi = "/valkeyworker/api/";
        public const string WorkerApiCert = "/workers/api_tls/valkeyworker";
    }
}
```

- [ ] **Шаг 6: Регистрация `AddValkey()` + Program.cs**

В `src/AdminPanel.Etcd/ModuleExtensions.cs` добавить метод (после `AddKafka`):
```csharp
    // Модуль valkey-домена (t03, arch/02 §11): hosted-service refresher'а + стор
    // снапшота + стор кредов проб. Отдельный HttpClient не заводится — транспорт
    // общий с pg/kafka-циклами (IEtcdGateway/«etcd», EtcdOptions).
    public static IServiceCollection AddValkey(this IServiceCollection services)
    {
        services.AddSingleton<ValkeySnapshotStore>();
        services.AddSingleton<IValkeySnapshotStore>(sp => sp.GetRequiredService<ValkeySnapshotStore>());
        services.AddSingleton<IValkeySnapshotReader>(sp => sp.GetRequiredService<ValkeySnapshotStore>());

        services.AddSingleton<ValkeySecretsStore>();
        services.AddSingleton<IValkeySecretsStore>(sp => sp.GetRequiredService<ValkeySecretsStore>());

        services.AddSingleton<ValkeySnapshotRefresher>();
        services.AddHostedService(sp => sp.GetRequiredService<ValkeySnapshotRefresher>());
        return services;
    }
```

В `src/AdminPanel.Api/Program.cs` после строки `.AddKafka()` добавить:
```csharp
   .AddValkey() // t03: valkey-домен: refresher + сторы (arch/02 §11)
```

- [ ] **Шаг 7: Тесты зелёные + сборка + коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Valkey"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Core/Valkey/ src/AdminPanel.Etcd/ValkeySnapshotStore.cs \
  src/AdminPanel.Etcd/ValkeySecretsStore.cs src/AdminPanel.Etcd/ValkeyPanelOptions.cs \
  src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs src/AdminPanel.Etcd/ValkeyProbeReader.cs \
  src/AdminPanel.Etcd/ModuleExtensions.cs src/AdminPanel.Api/Program.cs \
  src/tests/AdminPanel.UnitTests/ValkeyRefresherTests.cs
git commit -m "feat(valkey-panel): ValkeySnapshotRefresher + сторы + AddValkey() (4 префикса, secrets, мердж проб)"
```

**Выход:** refresher тикает 4 префикса, сторы/опции/скелет движка, домен зарегистрирован в хосте.
**Проверка:** фильтр `~Valkey` зелёный; сборка 0 warnings (скелет движка пуст, но DI-граф валиден — автоскан AddCore подхватит).
**Spec:** §4.3, §4.4, §5.1 (ValkeyRefresherTests), §6.3.

---

### Task 4: Инфраструктура воркеров — valkeyworker третьим воркером

**Files:**
- Create: `src/AdminPanel.Etcd/Workers/ValkeyWorkerHealthStore.cs`
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs` (DI + `ResolveEndpoints`)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerCertService.cs` (3 гварда + текст исключения)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs` (DI + valkey-блок в `RunOnceAsync`)
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs` (`TrustedWorkerThumbprints` + valkey)
- Modify: `src/AdminPanel.Api/Operations/WorkersModule.cs` (`GetWorkersQueryHandler` третья карточка)
- Modify: `src/AdminPanel.Api/Operations/WorkersCommands.cs` (`LiveHosts` case + гвард рестарта)
- Test: `src/tests/AdminPanel.UnitTests/Workers/ValkeyWorkersInfraTests.cs` (новый)

**Interfaces (consumes):** `IValkeySnapshotStore`/`IValkeySnapshotReader` (Task 3). **Produces:** `"valkeyworker"` валиден во всех общих точках (gateway/certs/health/workers-грань) — используют Task 7 (мутации) и Task 9 (чек 51).

- [ ] **Шаг 1: Написать падающие тесты**

`src/tests/AdminPanel.UnitTests/Workers/ValkeyWorkersInfraTests.cs` (посмотреть существующие `Workers/*Tests` в `src/tests/AdminPanel.UnitTests/Workers/` для приёмов):

```csharp
// Arrange: снапшок valkey с живым WorkerEndpoints(https://vwk:8080, id "i1").
// Act: GetWorkersQueryHandler.Handle. Assert: третья карточка Worker=="valkeyworker",
// инстанс i1, TargetCert из WorkerApiCert снапшота.
[Fact] public void GetWorkers_ValkeySnapshot_AddsThirdCard()

// Arrange: gateway с IValkeySnapshotStore(живой ключ) и пустыми pg/kafka.
// Act: SendAsync("valkeyworker",...). Assert: не кидает ArgumentOutOfRange,
// запрос уходит на URL живого ключа (стаб-хендлер фиксирует).
[Fact] public void Gateway_Valkeyworker_ResolvesEndpoints()

// Arrange: пустой IValkeySnapshotStore. Act: SendAsync("valkeyworker",...).
// Assert: WorkerApiUnavailableException (→ 503 панели).
[Fact] public void Gateway_Valkeyworker_NoEndpoints_ThrowsUnavailable()

// Arrange: WorkerCertService с IValkeySnapshotStore(health [i1=Degraded]).
// Act: WorkerHealthPoller.RunOnceAsync. Assert: IValkeyWorkerHealthStore.Current
// содержит i1 с Degraded (по образцу kafka-блока — стаб HttpClient).
[Fact] public void HealthPoller_ValkeyEndpoints_ProbesValkeyWorker()
```

- [ ] **Шаг 2: Убедиться, что падают**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~ValkeyWorkersInfra"
```

- [ ] **Шаг 3: Реализовать расширения**

1. `ValkeyWorkerHealthStore.cs` (новый, порт `KafkaWorkerHealthStore.cs`):
```csharp
using AdminPanel.Core;
using Shared.Core.DI;

namespace AdminPanel.Etcd.Workers;

// Стор результатов опроса /healthz инстансов ValkeyWorker (t03; arch/adminpanel/02
// §2.3.3): poller пишет, valkey-refresher вносит готовым в снапшот (паттерн
// KafkaWorkerHealthStore: volatile-замена, KV-тик не блокируется).
[InjectAsSingleton(typeof(IValkeyWorkerHealthStore))]
public sealed class ValkeyWorkerHealthStore : IValkeyWorkerHealthStore
{
    private volatile IReadOnlyList<WorkerHealth>? _current;

    public IReadOnlyList<WorkerHealth>? Current => _current;

    public void Replace(IReadOnlyList<WorkerHealth> health) => _current = health;
}

// Читатель результатов опроса (refresher вносит их успешным тиком).
public interface IValkeyWorkerHealthStore
{
    IReadOnlyList<WorkerHealth>? Current { get; }

    void Replace(IReadOnlyList<WorkerHealth> health);
}
```

2. `WorkerApiGateway.cs`: добавить в конструктор `IValkeySnapshotStore valkeyStore`; `ResolveEndpoints`:
```csharp
        "pgworker" => pgStore.Current?.PgWorkerEndpoints,
        "kafkaworker" => kafkaStore.Current?.WorkerEndpoints,
        "valkeyworker" => valkeyStore.Current?.WorkerEndpoints,
        _ => throw new ArgumentOutOfRangeException(nameof(worker), worker, "ожидался pgworker|kafkaworker|valkeyworker"),
```

3. `WorkerCertService.cs`: заменить во всех ТРЁХ гвардах (строки ~163, ~183, ~206: `GenerateAndPutAsync`, `PutAsync`, `DeleteAsync`):
```csharp
        if (worker is not ("pgworker" or "kafkaworker" or "valkeyworker"))
```
и текст `WorkerNotFoundException`: `$"неизвестный воркер {worker} (ожидался pgworker|kafkaworker|valkeyworker)"`.

4. `WorkerHealthPoller.cs`: в конструктор добавить `IValkeySnapshotReader valkeySnapshotReader, IValkeyWorkerHealthStore valkeyStore`; в конец `RunOnceAsync` — valkey-блок (копия kafka-блока):
```csharp
        // ValkeyWorker-инстансы (t03; arch/adminpanel/02 §2.3.3): тот же тик/клиент/
        // семантика — 200 → Healthy, 503 → Degraded, сетевой сбой → Unreachable;
        // /healthz за mTLS тем же клиентским сертом.
        var valkeyEndpoints = valkeySnapshotReader.Current?.WorkerEndpoints ?? [];
        var valkeyAt = time.GetUtcNow();
        var valkeyResults = await Task.WhenAll(valkeyEndpoints.Select(e => ProbeAsync(e, valkeyAt, ct)));
        valkeyStore.Replace([.. valkeyResults.OrderBy(r => r.InstanceId, StringComparer.Ordinal)]);
```

5. `ModuleExtensions.cs` (Etcd), в `TrustedWorkerThumbprints` добавить:
```csharp
            if (sp.GetRequiredService<IValkeySnapshotStore>().Current?.WorkerApiCert is { } vwk)
                thumbs.Add(vwk.Thumbprint);
```

6. `WorkersModule.cs` `GetWorkersQueryHandler`: добавить DI `IValkeySnapshotReader valkey`; после kafka-карточки — третья:
```csharp
            new("valkeyworker",
                (v?.WorkerEndpoints ?? []).Select(e => Instance(e, v?.WorkerApiCert,
                    v?.WorkerHealth?.FirstOrDefault(h => h.InstanceId == e.InstanceId))).ToList(),
                Cert(v?.WorkerApiCert)),
```

7. `WorkersCommands.cs`: в `LiveHosts` — case `"valkeyworker" => valkey?.WorkerEndpoints ?? []` (добавить DI-параметр `IValkeySnapshotReader valkey` в `GenerateWorkerApiCertCommandHandler`); в `RestartWorkerCommandHandler` гвард: `if (c.Worker is not ("pgworker" or "kafkaworker" or "valkeyworker"))`.

- [ ] **Шаг 4: Внести WorkerHealth в refresher-тик (перенос из стора)**

В `src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs` (Task 3): добавить DI-параметр `IValkeyWorkerHealthStore workerHealthStore` (после `logger`, перед optional `probeReader`) и в сборке снапшота заменить `WorkerHealth: []` на:
```csharp
            workerHealthStore.Current ?? [],   // health-проб воркера вносит успешный тик (t03; arch/02 §2.3.3)
```
(симметрия `KafkaSnapshotRefresher`; отказный тик НЕ мерджит — комментарий образца).

- [ ] **Шаг 5: Тесты зелёные + сборка + коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Valkey"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Etcd/Workers/ src/AdminPanel.Etcd/ModuleExtensions.cs \
  src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs src/AdminPanel.Api/Operations/WorkersModule.cs \
  src/AdminPanel.Api/Operations/WorkersCommands.cs src/tests/AdminPanel.UnitTests/Workers/ValkeyWorkersInfraTests.cs
git commit -m "feat(valkey-panel): valkeyworker в общем каркасе воркеров — gateway/certs/health/грань Воркеры"
```

**Выход:** `WorkerApiGateway`/`WorkerCertService`/`WorkerHealthPoller`/`GetWorkersQuery` знают `valkeyworker`; kafka-поведение не изменилось.
**Проверка:** фильтр `~Valkey` зелёный; полный прогон юнитов не сломан: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug` (все зелёные).
**Spec:** §4.5, §1 (п.6), §6.4.

---

### Task 5: Алерты ValkeyAlertEngine (TDD) + сводные

**Files:**
- Modify: `src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertEngine.cs` (наполнить каталог — Тело `Enumerate` + приватные helpers)
- Modify: `src/AdminPanel.Api/Inspection/AlertsQuery.cs` (объединение 3 движков)
- Modify: `src/AdminPanel.Api/Inspection/OverviewQuery.cs` (`OverviewValkeyDto` + поле `Valkey`)
- Test: `src/tests/AdminPanel.UnitTests/ValkeyAlertRulesTests.cs` (новый); правки существующих тестов overview/alerts при необходимости (`InspectionMappersTests.cs` и соседние — добавить valkey-случаи по месту).

**Interfaces (produces):** kinds `valkey-*` в `ValkeySnapshot.Alerts` (использует чек 51); `OverviewDto.Valkey` (фронт Task 8).

- [ ] **Шаг 1: Написать падающие тесты всех 8 kinds**

`src/tests/AdminPanel.UnitTests/ValkeyAlertRulesTests.cs` (порт стиля `KafkaAlertRulesTests.cs`; AAA). Обязательные кейсы:

```csharp
// Arrange: NOT_INITIALIZED-кластер. Act: Evaluate. Assert: kind valkey-cluster-not-initialized, severity info.
[Fact] NotInitialized_Info()
// Arrange: TO_REMOVE-кластер. Assert: valkey-cluster-to-remove, info.
[Fact] ToRemove_Info()
// Arrange: Active-кластер, нода UNREACHABLE. Assert: valkey-node-not-running, critical.
[Fact] NodeNotRunning_Critical()
// Arrange: Active, нода PROVISIONING; prev-снапшот тик назад тоже PROVISIONING,
// разница BuiltAtUtc < FreshProvisioningSeconds. Assert: алерта НЕТ (fresh-окно).
[Fact] NodeFreshProvisioning_Suppressed()
// Arrange: то же, но prev старше окна (или prev нет → fresh). Assert: алерт ЕСТЬ; и гашение:
// нода стала RUNNING → алерта нет.
[Fact] NodeProvisioningStale_Raises()/NodeRecovered_Clears()
// Arrange: Active без endpoints (null). Assert: valkey-endpoints-missing, critical.
[Fact] EndpointsMissing_Critical()
// Arrange: живая заявка ротации кластера live. Assert: valkey-rotation-pending, info,
// details role/requestedBy; заявка исчезла → алерт гаснет.
[Fact] RotationPending_Info()/RotationGone_Clears()
// Arrange: ParseErrors с ключом. Assert: valkey-key-malformed, warning.
[Fact] KeyMalformed_Warning()
// Arrange: WorkerEndpoints пуст. Assert: worker-api-unreachable, critical, target "valkeyworker".
[Fact] WorkerApiUnreachable_Critical()
// Arrange: WorkerHealth [{InstanceId=i1, Degraded}]. Assert: worker-unhealthy, warning, target "valkeyworker/i1".
[Fact] WorkerUnhealthy_Warning()
// Arrange: алерт жил в prev (SinceUnix=T). Act: Evaluate с prev. Assert: SinceUnix перенесён (стабильный id kind:target).
[Fact] SinceUnix_StableAcrossTicks()
```

- [ ] **Шаг 2: Убедиться, что падают**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~ValkeyAlertRules"
```

- [ ] **Шаг 3: Наполнить `Enumerate` каталогом 8 kinds**

Тексты — русские, Hint/Remedy по образцу `KafkaAlertEngine` (worker-auto/operator-api/operator-runbook). Полное тело `Enumerate` + helpers (заменяет скелет Task 3):

```csharp
    // Каталог arch/03 §8.4: все 8 kinds. Ротационный алерт — только у живого
    // кластера (заявка удаляется исполнением или демонтажем — вечного pending нет).
    private IEnumerable<Alert> Enumerate(ValkeySnapshot next, ValkeySnapshot? previous)
    {
        // worker-api-unreachable (critical): нет живых ключей /valkeyworker/api/
        // (arch/02 §2.3.3) — valkey-мутации панели 503; чтение не страдает.
        if (next.WorkerEndpoints.Count == 0)
            yield return new Alert(
                "worker-api-unreachable:valkeyworker",
                AlertSeverity.Critical,
                "worker-api-unreachable",
                "valkeyworker",
                "API ValkeyWorker недоступен: живых ключей /valkeyworker/api/ нет — valkey-мутации из панели 503; чтение данных не страдает",
                null,
                null,
                Hint: "воркер ставит lease-ключ при старте; ключа нет = воркер не поднялся или умер ≤15 c назад",
                Remedy: AlertRemedy.OperatorRunbook,
                RemedyText: "запустите контейнер воркера (профиль valkey стендовой compose), проверьте /healthz и ValkeyWorker:Api:AdvertiseUrl");

        // worker-unhealthy (warning): живой ключ, /healthz ≠ 200.
        foreach (var w in next.WorkerHealth.Where(w => w.Status != WorkerHealthStatus.Healthy))
        {
            var what = w.Status == WorkerHealthStatus.Degraded
                ? $"/healthz отвечает не-200 ({w.Detail ?? "degraded"})"
                : $"недостижим по URL lease-ключа ({w.Detail ?? "network error"})";
            yield return new Alert(
                $"worker-unhealthy:valkeyworker/{w.InstanceId}",
                AlertSeverity.Warning,
                "worker-unhealthy",
                $"valkeyworker/{w.InstanceId}",
                $"инстанс ValkeyWorker {w.InstanceId} нездоров: {what}",
                new Dictionary<string, string>
                {
                    ["url"] = w.Url,
                    ["checked_unix"] = w.CheckedAtUtc.ToUnixTimeSeconds().ToString(),
                },
                null,
                "lease-ключ жив, но health-проба процесса плохая; docker-healthcheck гасит контейнер — за этим последует исчезновение lease и critical worker-api-unreachable",
                AlertRemedy.OperatorRunbook,
                "смотрите docker logs valkeyworker и /healthz напрямую; поднимите зависимость (etcd/docker) или перезапустите контейнер воркера");
        }

        foreach (var cluster in next.Clusters)
        {
            switch (cluster.State)
            {
                case ValkeyClusterState.NotInitialized:
                    yield return new Alert(
                        $"valkey-cluster-not-initialized:{cluster.Name}",
                        AlertSeverity.Info,
                        "valkey-cluster-not-initialized",
                        cluster.Name,
                        $"кластер {cluster.Name} заявлен (NOT_INITIALIZED): нода не поднята",
                        null, null,
                        "кластер заявлен (config.state=NOT_INITIALIZED): provisioning воркера поднимет ноду и переведёт state в ACTIVE",
                        AlertRemedy.WorkerAuto,
                        "дождитесь provisioning ноды — воркер снимет NOT_INITIALIZED; висит дольше обычного — смотрите journal воркера");
                    break;
                case ValkeyClusterState.ToRemove:
                    yield return new Alert(
                        $"valkey-cluster-to-remove:{cluster.Name}",
                        AlertSeverity.Info,
                        "valkey-cluster-to-remove",
                        cluster.Name,
                        $"кластер {cluster.Name} в удалении (TO_REMOVE): воркер демонтирует",
                        null, null,
                        "кластер в удалении (config.state=TO_REMOVE): воркер демонтирует ноду и уберёт префикс /valkey/clusters/<C>",
                        AlertRemedy.WorkerAuto,
                        "воркер демонтирует кластер сам; висит — проверьте journal воркера (контейнер мог не удалиться)");
                    break;
                case ValkeyClusterState.Active:
                    foreach (var alert in ActiveClusterAlerts(cluster, previous, next))
                        yield return alert;
                    break;
            }
        }

        // valkey-rotation-pending (info): только заявки живых кластеров.
        var alive = next.Clusters.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var rotation in next.Rotations.Where(r => alive.Contains(r.Cluster)))
            yield return new Alert(
                $"valkey-rotation-pending:{rotation.Cluster}",
                AlertSeverity.Info,
                "valkey-rotation-pending",
                rotation.Cluster,
                $"ротация {rotation.Role}-пароля кластера {rotation.Cluster} заявлена, исполняется воркером (окно двух паролей, без рестартов)",
                new Dictionary<string, string>
                {
                    ["role"] = rotation.Role,
                    ["requestedBy"] = rotation.RequestedBy ?? "unknown",
                    ["requestedUnix"] = rotation.RequestedUnix.ToString(),
                },
                null,
                "заявка ротации жива (ключ /valkeyworker/rotations/<C>): воркер исполняет окно двух паролей E1–E3 и снимет ключ; отмены из панели нет (arch/20 §3)",
                AlertRemedy.WorkerAuto,
                "ротацию исполняет воркер, ключ исчезнет; висит — воркер буксует, проверьте journal");

        // valkey-key-malformed (warning): parseError-записи (arch/20 §5).
        foreach (var error in next.ParseErrors)
            yield return new Alert(
                $"valkey-key-malformed:{error.Key}",
                AlertSeverity.Warning,
                "valkey-key-malformed",
                error.Key,
                $"valkey-ключ не разобран: {error.Key}",
                new Dictionary<string, string> { ["reason"] = error.Reason },
                null,
                "valkey-ключ не разобран парсером панели: битое значение не попадает в модель — UI слеп к ключу; формат значений valkey-домена — канон arch/20",
                AlertRemedy.OperatorRunbook,
                "устраните источник битой записи (внешний писатель) и приведите значение к канону arch/20; повторный тик распарсит ключ");
    }

    // valkey-endpoints-missing + valkey-node-not-running (только Active-кластер).
    private IEnumerable<Alert> ActiveClusterAlerts(
        ValkeyClusterInfo cluster, ValkeySnapshot? previous, ValkeySnapshot next)
    {
        if (string.IsNullOrEmpty(cluster.Endpoints))
            yield return new Alert(
                $"valkey-endpoints-missing:{cluster.Name}",
                AlertSeverity.Critical,
                "valkey-endpoints-missing",
                cluster.Name,
                $"Active-кластер {cluster.Name} без endpoints — дискавери клиентов невозможно",
                null, null,
                "Active-кластер без endpoints: endpoints дописывает воркер по факту подъёма ноды — без них клиенты не найдут инстанс; каждый Active-кластер обязан иметь endpoints",
                AlertRemedy.WorkerAuto,
                "воркер допишет endpoints по факту provisioning; висит — нода недоступна воркеру, проверьте контейнер vwk-<C>-node1");

        var prevCluster = previous?.Clusters.FirstOrDefault(c => c.Name == cluster.Name);
        foreach (var node in cluster.NodesList)
        {
            if (node.State is null or "RUNNING")
                continue;

            // fresh-PROVISIONING (arch/03 §8.4): подъём только начался — не алертим
            // (порт IsFreshProvisioning kafka: PROVISIONING наблюдался и тик назад,
            // но окно FreshProvisioningSeconds ещё не истекло).
            if (node.State == "PROVISIONING"
                && IsFreshProvisioning(node, prevCluster, previous, next, _options.FreshProvisioningSeconds))
                continue;

            yield return new Alert(
                $"valkey-node-not-running:{cluster.Name}/{node.Name}",
                AlertSeverity.Critical,
                "valkey-node-not-running",
                $"{cluster.Name}/{node.Name}",
                $"нода {node.Name} кластера {cluster.Name} не RUNNING: {node.State}",
                new Dictionary<string, string> { ["state"] = node.State },
                null,
                "нода не в RUNNING: надзор воркера обязан привести ноду в RUNNING (рестарт/пересоздание контейнера); каждая заявленная нода обязана быть жива в Active-кластере",
                AlertRemedy.WorkerAuto,
                "воркер supervises ноду (restart/пересоздание контейнера); висит — проверьте контейнер vwk-<C>-node1 на стенде");
        }
    }

    private static bool IsFreshProvisioning(
        ValkeyNodeInfo node,
        ValkeyClusterInfo? prevCluster,
        ValkeySnapshot? previous,
        ValkeySnapshot next,
        int freshSeconds)
    {
        // Нет prev / в prev нода была не PROVISIONING → статус только что начался.
        if (previous is null || prevCluster is null)
            return true;
        var prevNode = prevCluster.NodesList.FirstOrDefault(n => n.Name == node.Name);
        if (prevNode?.State != "PROVISIONING")
            return true;

        // PROVISIONING наблюдался и тик назад: fresh, пока разница BuiltAtUtc < окна.
        return next.BuiltAtUtc - previous.BuiltAtUtc < TimeSpan.FromSeconds(freshSeconds);
    }
```

- [ ] **Шаг 4: Сводные `AlertsQuery` + `OverviewQuery`**

`AlertsQuery.cs`: хендлеру добавить DI `IValkeySnapshotReader valkeyStore`; merge — три движка (заменить метод `Merge`):
```csharp
    // Merge: единая сортировка severity → kind → target (механика движков);
    // kind уже различает valkey-* (arch/03 §8.1).
    private static IReadOnlyList<Alert> Merge(
        IReadOnlyList<Alert> pg, IReadOnlyList<Alert>? kafka, IReadOnlyList<Alert>? valkey)
        => valkey is null && kafka is null
            ? pg
            : [.. pg.Concat(kafka ?? []).Concat(valkey ?? [])
                .OrderBy(a => a.Severity, Comparer<AlertSeverity>.Create((x, y) => y.CompareTo(x)))
                .ThenBy(a => a.Kind, StringComparer.Ordinal)
                .ThenBy(a => a.Target, StringComparer.Ordinal)];
```
(вызов в `Handle` — `Merge(snapshot.Alerts, kafkaStore.Current?.Alerts, valkeyStore.Current?.Alerts)`; комментарий над классом дополнить: «объединяет алерты pg-, kafka- и valkey-движков»).

`OverviewQuery.cs`: добавить DTO и поле:
```csharp
// Сводка valkey-домена (arch/03 §8.1): из ValkeySnapshot + valkey-алертов.
public sealed record OverviewValkeyDto(
    int ClustersTotal,
    int ClustersCritical);
```
В `OverviewDto` добавить параметр `OverviewValkeyDto? Valkey` (после `Kafka`); в `OverviewMapper.Map` — `null` с комментарием «valkey-сводка — хендлер дополняет из ValkeySnapshot (t03)»; маппер valkey — МЕТОДОМ ТОГО ЖЕ `OverviewMapper` (public static, как `Map`/`MapKafka`-паттерн файла — прямой юнит-тест без хоста; spec §5.1 «ValkeyQuery-мапперы (summary/details/overview)»); в `OverviewQueryHandler` добавить DI `IValkeySnapshotReader valkeyStore` и `with { Valkey = OverviewMapper.MapValkey(valkeyStore.Current) }`:
```csharp
    // valkey-сводка: кластеры + critical-алерты ТОЛЬКО кластерных kinds
    // valkey-node-not-running/valkey-endpoints-missing (arch/03 §8.1 — НЕ
    // worker-api-unreachable: сознательное отличие от MapKafka, считающего
    // все critical — фиксируется юнит-тестом в Task 7); null до первого тика
    // valkey-refresher'а. Public static — юнит-тесты (как OverviewMapper.Map).
    public static OverviewValkeyDto? MapValkey(Core.Valkey.ValkeySnapshot? valkey)
        => valkey is null
            ? null
            : new OverviewValkeyDto(
                valkey.Clusters.Count,
                valkey.Alerts.Count(a => a.Severity == Core.AlertSeverity.Critical
                    && a.Kind is "valkey-node-not-running" or "valkey-endpoints-missing"));
```

- [ ] **Шаг 5: Тесты зелёные + сборка + коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Valkey"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Inspection"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertEngine.cs \
  src/AdminPanel.Api/Inspection/AlertsQuery.cs src/AdminPanel.Api/Inspection/OverviewQuery.cs \
  src/tests/AdminPanel.UnitTests/ValkeyAlertRulesTests.cs src/tests/AdminPanel.UnitTests/InspectionMappersTests.cs
git commit -m "feat(valkey-panel): ValkeyAlertEngine — 8 kinds (arch/03 §8.4) + /api/alerts и /api/overview включают valkey"
```

**Выход:** все 8 kinds зажигаются/гаснут; сводные эндпоинты включают valkey.
**Проверка:** фильтры `~Valkey` и `~Inspection` зелёные; сборка 0 warnings.
**Spec:** §4.7, §4.8 (overview/alerts), §5.1 (ValkeyAlertRulesTests), §6.5.

---

### Task 6: Live-пробы — RESP-миниклиент + петля (TDD)

**Files:**
- Create: `src/AdminPanel.Probes/Valkey/{IValkeyProbeClient,ValkeyConnection,ValkeyProbeLoop}.cs` (стор `IValkeyProbeStore`/`ValkeyProbeStore` — в том же файле `ValkeyProbeLoop.cs`, паттерн kafka)
- Modify: `src/AdminPanel.Probes/ProbesOptions.cs` (подсекция `Valkey`)
- Modify: `src/AdminPanel.Probes/ModuleExtensions.cs` (регистрации + адаптер `IValkeyProbeReader`)
- Test: `src/tests/AdminPanel.UnitTests/ProbesValkey/{ValkeyConnectionTests,ValkeyProbeLoopTests}.cs` (новые)

**Interfaces (consumes):** `IValkeySnapshotReader` (Task 3), `IValkeySecretsStore` (Task 3), `HostMapResolver` (существует), `ValkeyProbeResult` (Task 2). **Produces:** `IValkeyProbeStore.Current: IReadOnlyList<ValkeyProbeResult>?` — через адаптер попадает в `IValkeyProbeReader` → мердж refresher'а (уже написан в Task 3).

- [ ] **Шаг 1: Написать падающие тесты RESP-клиента**

`src/tests/AdminPanel.UnitTests/ProbesValkey/ValkeyConnectionTests.cs` — против эфемерного TCP-сервера-заглушки (динамический порт! `TcpListener(IPAddress.Loopback, 0)` → `port = ((IPEndPoint)listener.LocalEndpoint).Port`; AAA):

```csharp
// Arrange: TCP-заглушка, читает AUTH-кадр, отвечает +OK, читает PING, отвечает +PONG.
// Act: PingAsync. Assert: успех (Live=true).
[Fact] PingAsync_AuthOkPong_Live()

// Arrange: заглушка на AUTH отвечает -WRONGPASS. Act: PingAsync.
// Assert: Result неуспешен, ошибка содержит "AUTH".
[Fact] PingAsync_AuthFail_Error()

// Arrange: заглушка принимает соединение и молчит. Act: PingAsync с таймаутом 200 мс.
// Assert: Result неуспешен (таймаут), не исключение.
[Fact] PingAsync_Silence_TimesOut()

// Arrange: порт без слушателя. Act: PingAsync. Assert: Result неуспешен (connection refused).
[Fact] PingAsync_Refused_Fails()
```
Реализация заглушки: ручной разбор RESP-кадра в тесте (прочитать строки до `\r\n`) или захардкоженный сценарий `NetworkStream`-ом — по вкусу исполнителя, БЕЗ внешних пакетов.

- [ ] **Шаг 2: Убедиться, что падают**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~ProbesValkey"
```

- [ ] **Шаг 3: Реализовать `IValkeyProbeClient` + `ValkeyConnection`**

`src/AdminPanel.Probes/Valkey/IValkeyProbeClient.cs`:
```csharp
namespace AdminPanel.Probes.Valkey;

// Цель PING-пробы: адрес уже разрешён HostMapResolver'ом (host:port) + креды.
public sealed record ValkeyProbeTarget(string Host, int Port, string AdminUser, string AdminPassword);

// Клиент live-пробы: одна проба = одно короткоживущее соединение AUTH+PING.
public interface IValkeyProbeClient
{
    Task<Shared.Core.Result> PingAsync(ValkeyProbeTarget target, TimeSpan timeout, CancellationToken ct);
}
```

`src/AdminPanel.Probes/Valkey/ValkeyConnection.cs` — копия паттерна `src/ValkeyWorker.Core/Valkey/ValkeyConnection.cs` (namespace панели; ссылок на сборки воркеров НЕТ — код копируется) с усечением до AUTH+PING:
```csharp
using System.Net.Sockets;
using System.Text;
using Shared.Core;

namespace AdminPanel.Probes.Valkey;

// RESP-миниклиент панели (spec §4.6): копия паттерна ValkeyWorker.Core/Valkey/
// ValkeyConnection.cs с усечением до AUTH+PING. Одна проба = одно короткоживущее
// TCP-соединение; таймаут connect+команда; ретраев нет — следующий тик петли
// и есть ретрай (симметрия refresher'а). Ошибка сети/протокола → Result.Failed.
public sealed class ValkeyConnection : IValkeyProbeClient
{
    public async Task<Result> PingAsync(ValkeyProbeTarget target, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);
            using var client = new TcpClient();
            await client.ConnectAsync(target.Host, target.Port, cts.Token);

            // BufferedStream один на соединение: буфер переживает чтение AUTH.
            using var stream = new BufferedStream(client.GetStream(), 8192);

            var auth = await Resp.WriteAndReadAsync(
                stream, ["AUTH", target.AdminUser, target.AdminPassword], cts.Token);
            if (auth is not string)
                return FailedReply("AUTH", auth, target);

            var reply = await Resp.WriteAndReadAsync(stream, ["PING"], cts.Token);
            // +PONG — строковый кадр; всё прочее (включая -ERR) — не жив.
            return auth is not null && reply is "PONG"
                ? Result.Success()
                : FailedReply("PING", reply, target);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — не «нода молчит»
        }
        catch (OperationCanceledException ex)
        {
            return Result.Failed(new TimeoutException(
                $"valkey {target.Host}:{target.Port} не ответил за {timeout.TotalSeconds:F1} c (PING)", ex));
        }
        catch (Exception ex)
        {
            return Result.Failed(new ApplicationException(
                $"valkey {target.Host}:{target.Port} PING: {ex.Message}", ex));
        }
    }

    private static Result FailedReply(string command, object? reply, ValkeyProbeTarget target)
        => reply is RespError error
            ? Result.Failed(new ApplicationException($"{command}: {error.Message}"))
            : Result.Failed(new ApplicationException(
                $"{command}: valkey {target.Host}:{target.Port} — неожиданный ответ сервера"));

    /// <summary>Ошибка протокола (-ERR…): отдельный тип — клиент переводит в Failed.</summary>
    internal sealed record RespError(string Message);

    /// <summary>Парсер/писатель RESP-кадров (internal — юнит-тесты; копия воркерской).</summary>
    internal static class Resp
    {
        // … ПОЛНАЯ копия класса Resp из ValkeyWorker.Core/Valkey/ValkeyConnection.cs
        // (WriteAndReadAsync, ReadReplyAsync, ReadBulkAsync, ReadArrayAsync,
        // ReadLineAsync, ExpectLFAsync, ReadCrlfAsync, ReadExactlyAsync, ReadByteAsync)
        // без изменений — код в панели свой, внешних ссылок нет.
    }
}
```
(при копировании сохранить поведение побайтового чтения до полного кадра; PONG-ответ сервера — simple string `+PONG`.)

- [ ] **Шаг 4: Написать падающие тесты петли**

`src/tests/AdminPanel.UnitTests/ProbesValkey/ValkeyProbeLoopTests.cs` (фейк `IValkeyProbeClient`: in-memory флаг «отвечает», счётчик AUTH-кредов — spec §4.6):
```csharp
// Arrange: снапшот: Active-кластер live с endpoints + полный набор кредов в сторе.
// Act: RunOnceAsync. Assert: в IValkeyProbeStore запись Live=true, Node="node1".
[Fact] Loop_ActiveClusterWithCreds_Probes()

// Arrange: Active-кластер без кредов (частичный набор) и без endpoints.
// Act: RunOnceAsync. Assert: кластер НЕ пробится — записи с его именем нет.
[Fact] Loop_NoCredsOrNoEndpoints_Skips()

// Arrange: креды есть, фейк отвечает ошибкой. Act: RunOnceAsync.
// Assert: запись Live=false, Error заполнен; креды НЕ попадают в Error.
[Fact] Loop_ProbeError_LiveFalseWithError()

// Arrange: NOT_INITIALIZED-кластер. Assert: не пробится.
[Fact] Loop_NotInitialized_Skips()
```

- [ ] **Шаг 5: Реализовать опции + петлю**

`ProbesOptions.cs` — добавить вложенный POCO (секция `AdminPanel:Probes:Valkey`):
```csharp
    // Valkey-PING-пробы (t03, arch/03 §8): RESP-миниклиент панели.
    public ValkeyProbesOptions Valkey { get; set; } = new();
```
и в конец файла:
```csharp
// Параметры valkey-проб (spec §4.6): тик 15 c, таймаут одной пробы 3 c.
public sealed class ValkeyProbesOptions
{
    public bool Enabled { get; set; } = true;

    public double IntervalSec { get; set; } = 15;

    public double TimeoutSec { get; set; } = 3;
}
```

`src/AdminPanel.Probes/Valkey/ValkeyProbeLoop.cs`:
```csharp
using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes.Valkey;

// Стор результатов valkey-проб: писатель один — ValkeyProbeLoop; читают
// ValkeySnapshotRefresher (мердж Live/ProbeError) и инспекция.
public interface IValkeyProbeStore
{
    IReadOnlyList<ValkeyProbeResult>? Current { get; }

    void Replace(IReadOnlyList<ValkeyProbeResult> results);
}

public sealed class ValkeyProbeStore : IValkeyProbeStore
{
    private volatile IReadOnlyList<ValkeyProbeResult>? _current;

    public IReadOnlyList<ValkeyProbeResult>? Current => _current;

    public void Replace(IReadOnlyList<ValkeyProbeResult> results) => _current = results;
}

// Фоновый тик valkey-проб (spec §4.6): для каждого Active-кластера с endpoints
// и полным набором admin-кредов — PING (AUTH admin). Ошибка/нет кредов →
// Live=false+Error / кластер без пробы. Пробы не блокируют KV-тик (переносит
// успешный тик refresher'а — симметрия kafka). Креды в результаты не попадают.
public sealed class ValkeyProbeLoop(
    IValkeySnapshotReader snapshotReader,
    IValkeySecretsStore secrets,
    IValkeyProbeClient client,
    IValkeyProbeStore store,
    IOptions<ProbesOptions> probesOptions,
    TimeProvider time,
    ILogger<ValkeyProbeLoop> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var valkey = probesOptions.Value.Valkey;
        if (!valkey.Enabled)
        {
            logger.LogInformation("AdminPanel:Probes:Valkey: проба выключена — тик не запускается");
            return;
        }

        var seconds = valkey.IntervalSec;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Probes:Valkey:IntervalSec <= 0 — использую 15 c");
            seconds = 15;
        }

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            try
            {
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро тика — публично для unit-тестов без хоста.
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var at = time.GetUtcNow();
        var snapshot = snapshotReader.Current;
        if (snapshot is null)
            return; // valkey-снапшота ещё нет — пробать нечего

        var timeout = TimeSpan.FromSeconds(
            probesOptions.Value.Valkey.TimeoutSec > 0 ? probesOptions.Value.Valkey.TimeoutSec : 3);
        var hostMap = probesOptions.Value.HostMap;
        var results = new List<ValkeyProbeResult>();

        foreach (var cluster in snapshot.Clusters.Where(c =>
                     c.State == ValkeyClusterState.Active && !string.IsNullOrEmpty(c.Endpoints)))
        {
            // nodes=1: единственный адрес endpoints (spec §4.4); HostMapResolver —
            // порядок arch/02 §6 (адрес из etcd → override HostMap → прямое).
            var address = cluster.Endpoints!.Split(',')[0].Trim();
            var parts = address.Split(':');
            if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
                continue;

            var resolved = HostMapResolver.Resolve(hostMap, parts[0], port);

            if (!secrets.Current.TryGetValue(cluster.Name, out var creds))
                continue; // неполные креды — кластер без пробы (live=null в DTO)

            var target = new ValkeyProbeTarget(
                resolved[..resolved.LastIndexOf(':')],
                int.Parse(resolved[(resolved.LastIndexOf(':') + 1)..]),
                creds.AdminUser,
                creds.AdminPassword);
            var probe = await client.PingAsync(target, timeout, ct);
            var node = cluster.NodesList.FirstOrDefault()?.Name ?? "node1";
            results.Add(new ValkeyProbeResult(
                cluster.Name, node, probe.IsSuccess,
                at.ToUnixTimeSeconds(),
                probe.IsSuccess ? null : probe.Error!.Message));
        }

        results.Sort((a, b) => string.CompareOrdinal(a.Cluster, b.Cluster));
        store.Replace(results);
    }
}
```

- [ ] **Шаг 6: Регистрации в `src/AdminPanel.Probes/ModuleExtensions.cs`**

Внутрь `AddProbes()` добавить (рядом с kafka-блоком):
```csharp
        // Valkey-проба (t03, arch/03 §8): отдельный тик PING, состояние — свой стор
        // (в снапшот вносит ValkeySnapshotRefresher через IValkeyProbeReader).
        services.AddSingleton<Valkey.IValkeyProbeStore, Valkey.ValkeyProbeStore>();
        services.AddSingleton<Valkey.IValkeyProbeClient, Valkey.ValkeyConnection>();
        services.AddSingleton<Valkey.ValkeyProbeLoop>();
        services.AddHostedService(sp => sp.GetRequiredService<Valkey.ValkeyProbeLoop>());
        // Адаптер проб-стора для Etcd-читателя: Etcd не ссылается на Probes-сборку.
        services.AddSingleton<AdminPanel.Etcd.IValkeyProbeReader>(sp =>
            new ValkeyProbeReaderAdapter(sp.GetRequiredService<Valkey.IValkeyProbeStore>()));
```
и в конец файла:
```csharp
// Адаптер valkey-проб-стора для Etcd-читателя (паттерн ProbeReaderAdapter kafka).
internal sealed class ValkeyProbeReaderAdapter(Valkey.IValkeyProbeStore store)
    : AdminPanel.Etcd.IValkeyProbeReader
{
    public IReadOnlyList<AdminPanel.Core.Valkey.ValkeyProbeResult>? Current => store.Current;
}
```

- [ ] **Шаг 7: Тесты зелёные + сборка + коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Valkey"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
git add src/AdminPanel.Probes/ src/tests/AdminPanel.UnitTests/ProbesValkey/
git commit -m "feat(valkey-panel): RESP-миниклиент + ValkeyProbeLoop (PING по admin-креду, тик 15 c)"
```

**Выход:** `live`/`probeError` наполняются петлёй и мержатся в снапшот; креды не покидают стор.
**Проверка:** фильтр `~Valkey` зелёный (вкл. ProbesValkey); сборка 0 warnings.
**Spec:** §4.6, §4.4, §5.1 (ProbesValkey), §6.6.

---

### Task 7: REST API — инспекция + 5 мутаций-прокси + интеграционные тесты

**Files:**
- Create: `src/AdminPanel.Api/Inspection/ValkeyQuery.cs`
- Create: `src/AdminPanel.Api/Operations/Valkey/{ValkeyOperationsModule,ValkeyCommands,ValkeyRequests}.cs`
- Modify: `src/AdminPanel.Api/Inspection/InspectionModule.cs` (метод `MapValkeyInspectionApi`)
- Modify: `src/AdminPanel.Api/Program.cs` (`MapValkeyInspectionApi()` + `MapValkeyOperationsApi()`)
- Test: `src/tests/AdminPanel.IntegrationTests/ValkeyApiTests.cs` (+ фикстура `ValkeyApiFixture` внутри файла), `src/tests/AdminPanel.IntegrationTests/ValkeySnapshotIntegrationTests.cs`

**Interfaces (consumes):** `IValkeySnapshotStore/Reader`, `IValkeyProbeStore`, `IWorkerApiGateway`, `InspectionModule.SnapshotNotReadyException`, `WorkerProblemDetails`, `WorkerProxy.SendAsync<T>`. **Produces (REST, arch/03 §8.1):** `GET /api/valkey/clusters`, `GET /api/valkey/clusters/{cluster}`, `POST /api/valkey/clusters` (201), `DELETE /api/valkey/clusters/{c}` (202), `PUT .../config` (200), `PUT .../nodes/{node}/resources` (200), `POST .../password/rotate` (202).

- [ ] **Шаг 1: Инспекция `ValkeyQuery.cs` + мапперы**

Новый файл (порт `KafkaQuery.cs`, DTO — arch/03 §8.2):

```csharp
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Inspection;

// Запросы инспекции valkey-домена (arch/03 §8.1): сводный список и детали.
public sealed record ValkeyClustersQuery : IQuery<IReadOnlyList<ValkeyClusterSummaryDto>>;

public sealed record ValkeyClusterDetailsQuery(string Cluster) : IQuery<ValkeyClusterDto>;

// Сводная строка списка кластеров (arch/03 §8.2).
public sealed record ValkeyClusterSummaryDto(
    string Name,
    string State,
    int NodesTotal,
    int NodesRunning,
    string? Endpoints,
    bool RotationPending,
    long MaxmemoryBytes,
    string MaxmemoryPolicy);

// Детали кластера: config, нода, ротация (arch/03 §8.2).
public sealed record ValkeyClusterDto(
    string Name,
    string State,
    int NodesTotal,
    long MaxmemoryBytes,
    string MaxmemoryPolicy,
    long? CreatedUnix,
    string? Endpoints,
    IReadOnlyList<ValkeyNodeDto> NodesList,
    ValkeyRotationDto? Rotation);

// Нода node1: state raw + ресурсы + live из PING-пробы (null — проба молчит).
public sealed record ValkeyNodeDto(
    string Name,
    string? State,
    decimal? Cpu,
    int? MemGi,
    int? DiskGi,
    bool? Live,
    string? ProbeError);

// Живая заявка ротации (бейдж UI).
public sealed record ValkeyRotationDto(string Role, long RequestedUnix, string? RequestedBy);

// Core → DTO: чистые функции (arch/03 §8.2; camelCase-зеркало модели).
public static class ValkeyMappers
{
    public static IReadOnlyList<ValkeyClusterSummaryDto> MapSummaries(ValkeySnapshot snapshot)
        => [.. snapshot.Clusters.Select(c => new ValkeyClusterSummaryDto(
            c.Name,
            StateName(c.State),
            c.NodesList.Count,
            c.NodesList.Count(n => n.State == "RUNNING"),
            c.Endpoints,
            c.Rotation is not null,
            c.MaxmemoryBytes,
            c.MaxmemoryPolicy))];

    public static ValkeyClusterDto MapDetails(ValkeyClusterInfo cluster)
        => new(
            cluster.Name,
            StateName(cluster.State),
            cluster.NodesList.Count,
            cluster.MaxmemoryBytes,
            cluster.MaxmemoryPolicy,
            cluster.CreatedUnix,
            cluster.Endpoints,
            [.. cluster.NodesList.Select(n => new ValkeyNodeDto(
                n.Name, n.State, n.Cpu, n.MemGi, n.DiskGi, n.Live, n.ProbeError))],
            cluster.Rotation is null
                ? null
                : new ValkeyRotationDto(cluster.Rotation.Role, cluster.Rotation.RequestedUnix, cluster.Rotation.RequestedBy));

    public static string StateName(ValkeyClusterState state) => state switch
    {
        ValkeyClusterState.NotInitialized => "NOT_INITIALIZED",
        ValkeyClusterState.ToRemove => "TO_REMOVE",
        _ => "ACTIVE",
    };
}

// Список: valkey-снапшот → сводки (отказ «снапшота нет» — 503-семантика pg/kafka).
[InjectAsScoped]
public sealed class ValkeyClustersQueryHandler(IValkeySnapshotReader store)
    : IQueryHandler<ValkeyClustersQuery, IReadOnlyList<ValkeyClusterSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<ValkeyClusterSummaryDto>>> Handle(
        ValkeyClustersQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<ValkeyClusterSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<ValkeyClusterSummaryDto>>.Success(ValkeyMappers.MapSummaries(snapshot)));
    }
}

// Детали: 404 кластера нет в снапшоте (парсер собирает даже неполные префиксы).
[InjectAsScoped]
public sealed class ValkeyClusterDetailsQueryHandler(IValkeySnapshotReader store)
    : IQueryHandler<ValkeyClusterDetailsQuery, ValkeyClusterDto>
{
    public ValueTask<Result<ValkeyClusterDto>> Handle(ValkeyClusterDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<ValkeyClusterDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        var cluster = snapshot.Clusters.FirstOrDefault(c => c.Name == query.Cluster);
        return ValueTask.FromResult(cluster is null
            ? Result<ValkeyClusterDto>.Failed(new ValkeyClusterNotFound(query.Cluster))
            : Result<ValkeyClusterDto>.Success(ValkeyMappers.MapDetails(cluster)));
    }
}

// Кластер отсутствует в valkey-снапшоте — 404 (детали).
public sealed class ValkeyClusterNotFound(string cluster)
    : Exception($"valkey-кластер {cluster} не найден в снапшоте");
```

В `InspectionModule.cs` добавить `MapValkeyInspectionApi` (порт kafka-блока — сверить точный маппинг исключений с существующим `MapKafkaInspectionApi`):

```csharp
    // GET /api/valkey/clusters[...] — инспекция valkey-домена из ValkeySnapshot (arch/03 §8.1).
    public static IEndpointRouteBuilder MapValkeyInspectionApi(this IEndpointRouteBuilder endpoints)
    {
        // GET /api/valkey/clusters — сводный список (arch/03 §8.1).
        endpoints.MapGet("/api/valkey/clusters", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<ValkeyClustersQuery, IReadOnlyList<ValkeyClusterSummaryDto>>(
                new ValkeyClustersQuery(), ct);
            return result.IsSuccess
                ? Results.Ok(result.Value)
                : InspectionError(result);
        });

        // GET /api/valkey/clusters/{cluster} — детали; 404 кластера нет, прочее — 503.
        endpoints.MapGet("/api/valkey/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<ValkeyClusterDetailsQuery, ValkeyClusterDto>(
                new ValkeyClusterDetailsQuery(cluster), ct);
            return result.IsSuccess
                ? Results.Ok(result.Value)
                : InspectionError(result);
        });

        return endpoints;
    }

    // Error-ветка инспекции valkey: 404 неизвестного кластера, 503 снапшота
    // (механика kafka-ветки MapKafkaInspectionApi — сверить и повторить).
    private static IResult InspectionError<T>(Result<T> result) => result.Error switch
    {
        ValkeyClusterNotFound notFound => Results.Problem(
            statusCode: StatusCodes.Status404NotFound, title: "Not found", detail: notFound.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Valkey snapshot not ready",
            detail: result.Error!.Message),
    };
```
(если в `InspectionModule` уже есть приватный общий Error-хелпер — переиспользовать его и добавить только кейс `ValkeyClusterNotFound`; имя `InspectionError` не должно конфликтовать — сверить с существующими хелперами модуля.)

- [ ] **Шаг 2: Юнит-тесты мапперов и прокси-команд**

`src/tests/AdminPanel.UnitTests/Operations/Valkey/ValkeyOperationsTests.cs` (порт kafka-аналогов в `src/tests/AdminPanel.UnitTests/Operations/`):

```csharp
// Arrange: снапшот с Active-кластером (нода RUNNING, live=true, ротация app).
// Act: MapSummaries/MapDetails. Assert: nodesRunning=1, state ACTIVE, rotation.role="app",
// nodesList[].live=true; креды в DTO отсутствуют (нет полей).
[Fact] Mappers_Snapshot_ToDto()

// Arrange: valkey-снапшота нет (null — до первого тика refresher'а).
// Act: OverviewMapper.MapValkey(null). Assert: null (сводка отсутствует, фронт
// показывает «—», не 0 — кластеры не «пропали»).
[Fact] MapValkey_NullSnapshot_ReturnsNull()

// Arrange: снапшот с 1 кластером; Alerts: valkey-node-not-running (critical),
// valkey-endpoints-missing (critical), worker-api-unreachable (critical,
// target valkeyworker), valkey-rotation-pending (info).
// Act: OverviewMapper.MapValkey(snapshot). Assert: ClustersTotal == 1,
// ClustersCritical == 2 — worker-api-unreachable НЕ считается (сознательное
// отличие от MapKafka, arch/03 §8.1; зафиксировано тестом).
[Fact] MapValkey_CountsOnlyClusterCriticalKinds()

// Arrange: стаб IWorkerApiGateway отвечает 200+JSON тела воркера.
// Act: 5 команд (create/delete/config/resources/rotate). Assert: путь/метод/тело
// запроса к "valkeyworker" верны (стаб фиксирует), DTO десериализованы, rotate
// передаёт requestedBy → X-Requested-By.
[Fact] Commands_ProxyToValkeyWorker_WithExactPaths()

// Arrange: стаб кидает WorkerApiUnavailableException. Act: команда.
// Assert: Result неуспешен (модуль вернёт 503).
[Fact] Commands_WorkerUnavailable_Fails()
```

- [ ] **Шаг 3: Команды-прокси + модуль мутаций**

`src/AdminPanel.Api/Operations/Valkey/ValkeyRequests.cs` — DTO запросов (зеркало API воркера t02, `src/ValkeyWorker.App/Api/Operations/ValkeyLimits.cs`):
```csharp
namespace AdminPanel.Api.Operations.Valkey;

// Тела запросов — зеркало DTO ValkeyWorker (arch/21 §1.1; панель НЕ валидирует —
// сервер источник истины, фронт дублирует для UX; spec §4.9).
public sealed record CreateValkeyClusterRequest(
    string? Name,
    int? Nodes = null,
    long? MaxmemoryBytes = null,
    string? MaxmemoryPolicy = null,
    ValkeyResourcesUpdateRequest? Resources = null);

public sealed record ValkeyResourcesUpdateRequest(
    decimal? Cpu = null,
    int? MemGi = null,
    int? DiskGi = null);

public sealed record ValkeyConfigUpdateRequest(
    long? MaxmemoryBytes = null,
    string? MaxmemoryPolicy = null);

public sealed record RotateValkeyPasswordRequest(string? Role);
```

`src/AdminPanel.Api/Operations/Valkey/ValkeyCommands.cs` — 5 команд (DTO ответов — поля воркера t02):
```csharp
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Shared.Core.DI;

namespace AdminPanel.Api.Operations.Valkey;

// ===== Valkey-мутации — прокси в API ValkeyWorker (arch/02 §11.2, arch/21 §1.1):
// панель не пишет в etcd; воркер валидирует и пишет. Оператор (requestedBy) —
// только rotate (заявка с аудитом) — заголовком X-Requested-By. =====

// 1. Создание кластера (02 §11.2-1).
public sealed record CreateValkeyClusterCommand(CreateValkeyClusterRequest Request)
    : ICommand<ValkeyClusterCreatedDto>;

// Ответ 201 POST /api/valkey/clusters (arch/03 §8.2; поля — DTO воркера t02).
public sealed record ValkeyClusterCreatedDto(
    string Name, string State, int Nodes, long MaxmemoryBytes,
    string MaxmemoryPolicy, string Cpu, string MemGi, string DiskGi);

[InjectAsScoped]
public sealed class CreateValkeyClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<CreateValkeyClusterCommand, ValkeyClusterCreatedDto>
{
    public async ValueTask<Result<ValkeyClusterCreatedDto>> Handle(
        CreateValkeyClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyClusterCreatedDto>(
            api, "valkeyworker", HttpMethod.Post, "/api/valkey/clusters",
            command.Request, requestedBy: null, ct);
}

// 2. Удаление — TO_REMOVE, демонтаж асинхронный (02 §11.2-2): панель отвечает 202.
public sealed record DeleteValkeyClusterCommand(string Cluster) : ICommand<ValkeyClusterDeletedDto>;

public sealed record ValkeyClusterDeletedDto(string Cluster);

[InjectAsScoped]
public sealed class DeleteValkeyClusterCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<DeleteValkeyClusterCommand, ValkeyClusterDeletedDto>
{
    public async ValueTask<Result<ValkeyClusterDeletedDto>> Handle(
        DeleteValkeyClusterCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyClusterDeletedDto>(
            api, "valkeyworker", HttpMethod.Delete, $"/api/valkey/clusters/{command.Cluster}",
            body: null, requestedBy: null, ct);
}

// 3. Конфиг-мутация maxmemory_* (02 §11.2-3; converge D применит).
public sealed record UpdateValkeyConfigCommand(string Cluster, ValkeyConfigUpdateRequest Request)
    : ICommand<ValkeyConfigUpdatedDto>;

public sealed record ValkeyConfigUpdatedDto(string Cluster, long MaxmemoryBytes, string MaxmemoryPolicy);

[InjectAsScoped]
public sealed class UpdateValkeyConfigCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateValkeyConfigCommand, ValkeyConfigUpdatedDto>
{
    public async ValueTask<Result<ValkeyConfigUpdatedDto>> Handle(
        UpdateValkeyConfigCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyConfigUpdatedDto>(
            api, "valkeyworker", HttpMethod.Put, $"/api/valkey/clusters/{command.Cluster}/config",
            command.Request, requestedBy: null, ct);
}

// 4. Ресурсы ноды (02 §11.2-4; автоконверге надзора — пересоздание контейнера).
public sealed record UpdateValkeyResourcesCommand(
    string Cluster, string Node, ValkeyResourcesUpdateRequest Request)
    : ICommand<ValkeyResourcesUpdatedDto>;

public sealed record ValkeyResourcesUpdatedDto(string Cluster, string Node, string Cpu, string MemGi, string DiskGi);

[InjectAsScoped]
public sealed class UpdateValkeyResourcesCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<UpdateValkeyResourcesCommand, ValkeyResourcesUpdatedDto>
{
    public async ValueTask<Result<ValkeyResourcesUpdatedDto>> Handle(
        UpdateValkeyResourcesCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyResourcesUpdatedDto>(
            api, "valkeyworker", HttpMethod.Put,
            $"/api/valkey/clusters/{command.Cluster}/nodes/{command.Node}/resources",
            command.Request, requestedBy: null, ct);
}

// 5. Заявка ротации пароля app|admin (02 §11.2-5; окно двух паролей, без рестартов).
public sealed record RotateValkeyPasswordCommand(string Cluster, string Role, string RequestedBy)
    : ICommand<ValkeyPasswordRotatedDto>;

public sealed record ValkeyPasswordRotatedDto(
    string Cluster, string Role, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RotateValkeyPasswordCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateValkeyPasswordCommand, ValkeyPasswordRotatedDto>
{
    public async ValueTask<Result<ValkeyPasswordRotatedDto>> Handle(
        RotateValkeyPasswordCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<ValkeyPasswordRotatedDto>(
            api, "valkeyworker", HttpMethod.Post,
            $"/api/valkey/clusters/{command.Cluster}/password/rotate",
            new RotateValkeyPasswordRequest(command.Role), command.RequestedBy, ct);
}
```

`src/AdminPanel.Api/Operations/Valkey/ValkeyOperationsModule.cs` — порт `KafkaOperationsModule` (Error-хендлер общий — копия ветки):
```csharp
using System.Security.Claims;
using AdminPanel.Api.Inspection;
using AdminPanel.Etcd.Workers;
using Shared.Core.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations.Valkey;

// Модуль valkey-мутаций (arch/03 §8.1) — ПРОКСИ в API ValkeyWorker (arch/21 §1.1):
// панель не пишет в etcd; успех — DTO воркера, ошибки — ProblemDetails как есть,
// недоступность API — собственный 503. Успех не дёргает refresher: следующий
// тик (3 c) подхватывает новые ключи (arch/02 §4).
public static class ValkeyOperationsModule
{
    public static IEndpointRouteBuilder MapValkeyOperationsApi(this IEndpointRouteBuilder endpoints)
    {
        // POST /api/valkey/clusters — создание (02 §11.2-1): 201.
        endpoints.MapPost("/api/valkey/clusters", async (
            CreateValkeyClusterRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<CreateValkeyClusterCommand, ValkeyClusterCreatedDto>(
                new CreateValkeyClusterCommand(request), ct);
            return result.IsSuccess
                ? Results.Created($"/api/valkey/clusters/{result.Value.Name}", result.Value)
                : Error(result);
        });

        // DELETE /api/valkey/clusters/{cluster} — TO_REMOVE (02 §11.2-2): 202
        // (демонтаж асинхронный — процесс B воркера), без тела.
        endpoints.MapDelete("/api/valkey/clusters/{cluster}", async (
            string cluster, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteValkeyClusterCommand, ValkeyClusterDeletedDto>(
                new DeleteValkeyClusterCommand(cluster), ct);
            return result.IsSuccess ? Results.Accepted() : Error(result);
        });

        // PUT /api/valkey/clusters/{cluster}/config — maxmemory_* (02 §11.2-3): 200.
        endpoints.MapPut("/api/valkey/clusters/{cluster}/config", async (
            string cluster, ValkeyConfigUpdateRequest request, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateValkeyConfigCommand, ValkeyConfigUpdatedDto>(
                new UpdateValkeyConfigCommand(cluster, request), ct);
            return result.IsSuccess ? Results.Ok(result.Value) : Error(result);
        });

        // PUT /api/valkey/clusters/{cluster}/nodes/{node}/resources (02 §11.2-4): 200.
        endpoints.MapPut("/api/valkey/clusters/{cluster}/nodes/{node}/resources", async (
            string cluster, string node, ValkeyResourcesUpdateRequest request,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UpdateValkeyResourcesCommand, ValkeyResourcesUpdatedDto>(
                new UpdateValkeyResourcesCommand(cluster, node, request), ct);
            return result.IsSuccess ? Results.Ok(result.Value) : Error(result);
        });

        // POST /api/valkey/clusters/{cluster}/password/rotate — заявка (02 §11.2-5):
        // 202; оператор сессии — X-Requested-By (арх/02 §11.2).
        endpoints.MapPost("/api/valkey/clusters/{cluster}/password/rotate", async (
            string cluster, RotateValkeyPasswordRequest request, ClaimsPrincipal user,
            IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RotateValkeyPasswordCommand, ValkeyPasswordRotatedDto>(
                new RotateValkeyPasswordCommand(cluster, request.Role ?? "", user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Results.Accepted((string?)null, result.Value) : Error(result);
        });

        return endpoints;
    }

    // Error-ветка прокси (общая с pg/kafka-модулями): недоступность API воркера →
    // собственный 503 панели; ProblemDetails воркера — телом как есть.
    private static IResult Error(Result result) => result.Error switch
    {
        WorkerApiUnavailableException unavailable => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "API воркера недоступен",
            detail: unavailable.Message),
        WorkerProblemDetails problem => Results.Text(
            problem.Body, "application/problem+json", System.Text.Encoding.UTF8, problem.StatusCode),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Etcd write failed",
            detail: result.Error!.Message),
    };
}
```

В `Program.cs` после `MapKafkaOperationsApi()`:
```csharp
app.MapValkeyInspectionApi();  // t03: инспекция valkey-домена (arch/03 §8.1)
app.MapValkeyOperationsApi();  // t03: valkey-мутации (arch/02 §11.2)
```

- [ ] **Шаг 4: Юниты зелёные + сборка**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~Valkey"
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
```

- [ ] **Шаг 5: Интеграционные тесты — `ValkeySnapshotIntegrationTests.cs`**

Новый файл (порт `EtcdSnapshotIntegrationTests.cs` — изучить его фикстуру etcd-testcontainers + `PanelHostBuilder`; собственный etcd-контейнер на класс, guid-префиксы, teardown):
```csharp
// Фикстура: EtcdContainerFixture-паттерн (динамический порт), сид канонических
// ключей valkey через EtcdGateway.PutAsync (guid-префикс кластеров <guid>c1/c2).
// Тесты (AAA):
// 1) RefreshOnceAsync на сид-ключах → снапшот: кластеры/ротации/endpoints/серт;
//    секреты admin-пары в IValkeySecretsStore (полный набор), частичный — пропущен.
// 2) Битые ключи (guid-кластер с "{oops" в config) → ParseErrors без исключения,
//    UnknownKeyCount для неизвестного leaf.
// 3) Teardown-ассерт чистоты: etcd-ключей guid-префикса нет (etcdctl del --prefix
//    в DisposeAsync) + контейнер удалён (DisposeAsync фикстуры).
// Таймауты ≤ 100 c.
```
Механику сид-записи взять из `EtcdSeed.cs`/`EtcdSnapshotIntegrationTests.cs` (HttpClient POST /v3/kv/put с base64, БЕЗ новых пакетов).

- [ ] **Шаг 6: Интеграционные тесты — `ValkeyApiTests.cs`**

Новый файл: `WebApplicationFactory` на РЕАЛЬНОМ etcd-контейнере (сид: живой ключ `/valkeyworker/api/<guid>` + ключи кластеров) + ПОДМЕНЁННЫЙ `HttpMessageHandler` именованного клиента `"workers"` (сквозной путь: панель → WorkerApiGateway → HttpClient → заглушка API воркера):
```csharp
// Подмена handler'а в ConfigureTestServices (после AddEtcd — перекрывает mTLS):
//   services.AddHttpClient(WorkerApiGateway.HttpClientName)
//       .ConfigurePrimaryHttpMessageHandler(() => stub);
// StubHttpMessageHandler: очередь ответов (код, тело) + фиксация запросов
// (метод/путь/тело/X-Requested-By) — assert'ы маппинга 1:1.
//
// Тесты (AAA):
// 1) GET /api/valkey/clusters → 200, сводки сида (nodesRunning/rotationPending).
// 2) GET /api/valkey/clusters/<c> → 200 (детали, live=null), 404 неизвестного, 503 без снапшота.
// 3) GET /api/workers → содержит карточку "valkeyworker" с инстансом сида.
// 4) POST /api/valkey/clusters → 201 (Location /api/valkey/clusters/<name>, DTO);
//    стаб отвечает 409 ProblemDetails → панель отдаёт 409 телом как есть;
//    стаб недоступен (все URL throw) → 503 панели «API воркера недоступен».
// 5) DELETE → 202; PUT config → 200; PUT resources → 200; POST rotate → 202
//    (стаб фиксирует X-Requested-By: admin после login).
// 6) Креды в ответах отсутствуют: сериализованные DTO не содержат admin_password.
// Fixture: IAsyncLifetime — etcd-контейнер (динамический порт) + factory
// (EnsureBuilt по PanelHostBuilder); DisposeAsync — factory + контейнер (теardown
// при любом исходе). Login — как WorkersApiTests (cookie admin).
```

- [ ] **Шаг 7: Интеграционные тесты зелёные + зачистка серии + коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter "FullyQualifiedName~Valkey"
# зачистка после docker-серии (страховочный гейт AGENTS.md):
docker ps -aq --filter "label=org.testcontainers" --format '{{.ID}}' | xargs -r docker rm -f
docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug   # вся серия не сломана
# повторная зачистка после полной серии (см. выше)
git add src/AdminPanel.Api/ src/tests/AdminPanel.UnitTests/Operations/Valkey/ \
  src/tests/AdminPanel.IntegrationTests/ValkeyApiTests.cs \
  src/tests/AdminPanel.IntegrationTests/ValkeySnapshotIntegrationTests.cs
git commit -m "feat(valkey-panel): REST API valkey-домена — инспекция + 5 мутаций-прокси + интеграционные тесты"
```

**Выход:** эндпоинты 03 §8.1 работают; коды 1:1 с воркером; креды не выходят.
**Проверка:** фильтры `~Valkey` в юнитах и интеграции зелёные; после серий — ни остаточных контейнеров, ни сетей (`docker network ls | grep -c pgw` → 0 лишних).
**Spec:** §4.8, §4.9, §5.1 (Operations/Valkey), §5.2, §6.7.

---

### Task 8: Фронтенд — страницы Valkey + роуты/меню

**Files:**
- Modify: `frontend/src/api/dto.ts` (+ valkey-типы), `frontend/src/api/queries.ts` (+ fetch-функции)
- Create: `frontend/src/pages/ValkeyClustersPage.tsx`
- Create: `frontend/src/pages/valkey-cluster/{ValkeyClusterDetailsPage,CreateValkeyClusterModal,DeleteValkeyClusterButton,EditClusterConfigModal,EditNodeResourcesModal,RotatePasswordButton}.tsx`
- Modify: `frontend/src/App.tsx` (роуты), `frontend/src/layout/AppLayout.tsx` (пункт меню)

**Interfaces (consumes):** REST Task 7 (camelCase-DTO 03 §8.2). **Produces:** роуты `/valkey`, `/valkey/:cluster`.

- [ ] **Шаг 1: Типы и запросы**

В `dto.ts` (по образцу kafka-секции, ~строка 395) добавить:
```typescript
// Канон состояния valkey-кластера: config.state (arch/20 §2); отсутствие = ACTIVE.
export type ValkeyClusterStateName = 'ACTIVE' | 'NOT_INITIALIZED' | 'TO_REMOVE';

// GET /api/valkey/clusters — сводный список (arch/03 §8.2).
export interface ValkeyClusterSummaryDto {
  name: string;
  state: ValkeyClusterStateName;
  nodesTotal: number;
  nodesRunning: number;
  endpoints: string | null;
  rotationPending: boolean;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
}

// GET /api/valkey/clusters/{cluster} — детали (arch/03 §8.2).
export interface ValkeyClusterDto {
  name: string;
  state: ValkeyClusterStateName;
  nodesTotal: number;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
  createdUnix: number | null;
  endpoints: string | null;
  nodesList: ValkeyNodeDto[];
  rotation: ValkeyRotationDto | null;
}

export interface ValkeyNodeDto {
  name: string;
  state: string | null;
  cpu: number | null;
  memGi: number | null;
  diskGi: number | null;
  // Из PING-пробы (только факт живости); null — проба молчит/кредов нет.
  live: boolean | null;
  probeError: string | null;
}

export interface ValkeyRotationDto {
  role: string;
  requestedUnix: number;
  requestedBy: string | null;
}

// POST /api/valkey/clusters — тело и ответ (arch/03 §8.2/§8.3.1).
export interface CreateValkeyClusterRequestDto {
  name: string;
  maxmemoryBytes?: number;
  maxmemoryPolicy?: string;
  resources?: { cpu?: number; memGi?: number; diskGi?: number };
}

export interface ValkeyClusterCreatedDto {
  name: string;
  state: string;
  nodes: number;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
  cpu: string;
  memGi: string;
  diskGi: string;
}

// PUT config / PUT resources / POST rotate — тела и ответы.
export interface ValkeyConfigUpdateRequestDto {
  maxmemoryBytes?: number;
  maxmemoryPolicy?: string;
}
export interface ValkeyConfigUpdatedDto {
  cluster: string;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
}
export interface ValkeyResourcesRequestDto {
  cpu?: number;
  memGi?: number;
  diskGi?: number;
}
export interface ValkeyResourcesUpdatedDto {
  cluster: string;
  node: string;
  cpu: string;
  memGi: string;
  diskGi: string;
}
export interface ValkeyRotateRequestDto {
  role: 'app' | 'admin';
}
export interface ValkeyPasswordRotatedDto {
  cluster: string;
  role: string;
  requestedUnix: number;
  requestedBy: string;
}
```
В `OverviewDto` (строки ~143) добавить `valkey: OverviewValkeyDto | null;` + `export interface OverviewValkeyDto { clustersTotal: number; clustersCritical: number; }`.

В `queries.ts` (по образцу kafka-функций):
```typescript
// ===== Valkey-домен (arch/03 §8.1) =====
export const valkeyQueryKeys = {
  clusters: ['valkey-clusters'] as const,
  cluster: (name: string) => ['valkey-clusters', name] as const,
};
export function fetchValkeyClusters(): Promise<ValkeyClusterSummaryDto[]> {
  return apiFetch<ValkeyClusterSummaryDto[]>('/api/valkey/clusters');
}
export function fetchValkeyClusterDetails(name: string): Promise<ValkeyClusterDto> {
  return apiFetch<ValkeyClusterDto>(`/api/valkey/clusters/${encodeURIComponent(name)}`);
}
export function createValkeyCluster(request: CreateValkeyClusterRequestDto): Promise<ValkeyClusterCreatedDto> {
  return apiFetch<ValkeyClusterCreatedDto>('/api/valkey/clusters', { method: 'POST', body: request });
}
export function deleteValkeyCluster(cluster: string): Promise<void> {
  return apiFetch<void>(`/api/valkey/clusters/${encodeURIComponent(cluster)}`, { method: 'DELETE' });
}
export function updateValkeyConfig(cluster: string, request: ValkeyConfigUpdateRequestDto): Promise<ValkeyConfigUpdatedDto> {
  return apiFetch<ValkeyConfigUpdatedDto>(`/api/valkey/clusters/${encodeURIComponent(cluster)}/config`, { method: 'PUT', body: request });
}
export function updateValkeyNodeResources(cluster: string, node: string, request: ValkeyResourcesRequestDto): Promise<ValkeyResourcesUpdatedDto> {
  return apiFetch<ValkeyResourcesUpdatedDto>(`/api/valkey/clusters/${encodeURIComponent(cluster)}/nodes/${encodeURIComponent(node)}/resources`, { method: 'PUT', body: request });
}
export function rotateValkeyPassword(cluster: string, role: 'app' | 'admin'): Promise<ValkeyPasswordRotatedDto> {
  return apiFetch<ValkeyPasswordRotatedDto>(`/api/valkey/clusters/${encodeURIComponent(cluster)}/password/rotate`, { method: 'POST', body: { role } });
}
```

- [ ] **Шаг 2: Страница списка `ValkeyClustersPage.tsx`**

Порт `KafkaClustersPage.tsx` (таблица: Кластер/Состояние/Нода (running/total)/Endpoints/maxmemory+policy/Пометки; бейдж ротации `role + возраст` из details? — в summary только флаг: бейдж «ротация»; `canMutate`-логика на details). maxmemory человекочитаемо: `formatBytes` — если в проекте нет утилиты, локальная функция: `>= 1GiB → "512 MiB"/"2 GiB"`. Колонки:

| Кластер | Состояние (бейджи как kafka: ACTIVE зелёный/«не инициализирован»/«к удалению») | Нода `running/total` | Endpoints (monospace, сокращённо) | maxmemory (MiB/GiB) + policy | Пометки (бейдж «ротация») |
Кнопка «Создать кластер» → `CreateValkeyClusterModal`.

- [ ] **Шаг 3: Модал создания `CreateValkeyClusterModal.tsx`**

Порт `CreateKafkaClusterModal.tsx` (Mantine; поля 03 §8.3.1): имя; maxmemory в **MiB** (число, def 512); policy — Select из 8 канонических значений `['allkeys-lru','allkeys-lfu','volatile-lru','volatile-lfu','allkeys-random','volatile-random','volatile-ttl','noeviction']`, def `allkeys-lru`; группа «Ресурсы ноды»: cpu (decimal, def 1), memGi (def 1), diskGi (def 10). Клиентская валидация-зеркало 02 §11.3 — тело функции:

```typescript
// Валидация-зеркало 02 §11.3 (сервер — источник истины, это UX); maxmemory —
// в MiB (UI), в API уходит maxmemoryBytes = MiB * 1048576.
function validateCreate(name: string, mib: number, policy: string, cpu: number, memGi: number, diskGi: number): string | null {
  if (!/^[a-z][a-z0-9_]{0,62}$/.test(name)) return 'имя: [a-z][a-z0-9_]{0,62} (строчные, без дефиса)';
  if (!Number.isInteger(mib) || mib < 1) return 'maxmemory: целое ≥ 1 MiB';
  if (!KNOWN_POLICIES.includes(policy)) return 'maxmemoryPolicy: выберите из списка';
  if (cpu < 0.01 || cpu > 64) return 'cpu: 0.01..64 ядер';
  if (!Number.isInteger(memGi) || memGi < 1 || memGi > 65536) return 'memGi: целое 1..65536';
  if (!Number.isInteger(diskGi) || diskGi < 1 || diskGi > 65536) return 'diskGi: целое 1..65536';
  // Инвариант R3: maxmemoryBytes < mem-лимит (иначе OOM-килл контейнера).
  if (mib * 1048576 >= memGi * 1073741824) return 'maxmemory обязан быть меньше mem-лимита — иначе OOM-килл (R3)';
  return null;
}
```
(`KNOWN_POLICIES` — массив 8 значений выше.) Проблема сервера — ProblemDetails в теле формы (`errors`); двойной клик — блокировка. Успех: `queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.clusters })`.

- [ ] **Шаг 4: Страница деталей + вкладка Нода + модалы**

`ValkeyClusterDetailsPage.tsx` (порт `KafkaClusterDetailsPage.tsx`): шапка — имя, state-бейджи, endpoints (monospace), createdUnix; кнопки (все, кроме просмотра, — только при `canMutate = state === 'ACTIVE'`; при TO_REMOVE скрыты):
- «Изменить конфиг» → `EditClusterConfigModal` (maxmemory MiB + policy select; предупреждение R3);
- «Сменить app-пароль» / «Сменить admin-пароль» → `RotatePasswordButton` (общий компонент с пропом `role`; предупреждение «после применения подключения со старым паролем отвергаются до перечитывания кредов — окно двух паролей»; 409 «уже запрошена» — текстом из ProblemDetails);
- «Удалить кластер» → `DeleteValkeyClusterButton` (красная, Popconfirm-подтверждение; успех → navigate('/valkey')).
Вкладка **Нода** (одна, без Tabs-обёртки можно — как проще по образцу BrokersTab): name/state-бейдж/resources (`cpu / memGi Gi / diskGi Gi`)/live (бейдж: `live=true` зелёный «жив», `false` красный «не отвечает» + probeError tooltip, `null` серый «проба молчит»); кнопка «Изменить ресурсы» → `EditNodeResourcesModal` (cpu/mem/disk; подпись «применяется пересозданием контейнера — кеш восполним; disk — инфо-поле»). Ротационный бейдж в шапке: `role + возраст` (`Date.now()/1000 - requestedUnix` в минутах).

- [ ] **Шаг 5: Роуты и меню**

`App.tsx`: импорты страниц + в children после kafka-роутов:
```typescript
      { path: 'valkey', element: <ValkeyClustersPage /> },
      { path: 'valkey/:cluster', element: <ValkeyClusterDetailsPage /> },
```
`AppLayout.tsx`: после `{ to: '/kafka', label: 'Kafka' }` добавить `{ to: '/valkey', label: 'Valkey' }`.

- [ ] **Шаг 6: Сборка фронта + коммит**

```bash
cd frontend && npm run build   # tsc typecheck + vite build
cd .. && git add frontend/src/
git commit -m "feat(valkey-panel): UI /valkey — список, детали+Нода, 5 модалов мутаций, роуты и меню"
```

**Выход:** страницы/роуты/меню; валидация-зеркало 02 §11.3 вкл. R3.
**Проверка:** `npm run build` без ошибок TS; (визуальная проверка — на стенде в Task 9).
**Spec:** §4.10, §6.8.

---

### Task 9: Стенд — сервис valkeyworker + серты + сид + чек 51 + чистка

**Files:**
- Modify: `deploy/tls/gen.sh` (SAN `DNS:valkeyworker` в server-серт + переген серта старых пакетов)
- Modify: `dev-stand/adminpanel/docker-compose.yml` (сервис `valkeyworker`, профиль `valkey`; том `vw-snapshots`)
- Modify: `dev-stand/adminpanel/checks/{00-up.sh,05-seed.sh,90-down.sh}`
- Create: `dev-stand/adminpanel/checks/51-valkey-api.sh`

**Interfaces (consumes):** образ `valkeyworker:dev` (сборка `docker/ValkeyWorker.Dockerfile`, curl внутри образа для healthcheck); TLS-пакет `deploy/tls/`; API панели Task 7. **Produces:** полный стенд с профилем `valkey` — E2E-гейт задачи.

- [ ] **Шаг 1: TLS — SAN серверного серта покрывает `valkeyworker`**

Полный новый текст `deploy/tls/gen.sh` (идемпотентность сохранена: при живом пакете перегенерируется ТОЛЬКО server-серт без valkey-SAN — CA и клиентские серты не трогаются):

```bash
#!/usr/bin/env bash
# Per-install API TLS-пакет (t03, arch/14 §1.1 / arch/16 §1.1 / arch/21 §1.1):
# ЕДИНАЯ CA kfw-install-ca на воркеров. Серверные серты: server (kafkaworker +
# valkeyworker — один серт, SAN покрывает обоих), pgserver (pgworker); клиентские:
# panel, seed, prometheus, healthcheck. Идемпотентен: при существующем ca.pem не
# делает ничего, КРОМЕ перегенерации server-серта старых пакетов без DNS:valkeyworker
# (t03). Ротация CA — вручную: rm ca.* и перезапуск. Файлы в git не попадают
# (deploy/tls/.gitignore).
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
SERVER_SAN="DNS:kafkaworker,DNS:valkeyworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
DAYS=3650

if [ ! -f ca.pem ]; then
  openssl genrsa -out ca.key 4096 2>/dev/null
  openssl req -x509 -new -nodes -key ca.key -sha256 -days "$DAYS" \
    -subj "/CN=kfw-install-ca" -out ca.pem
fi

issue() { # name cn eku san
  local name="$1" cn="$2" eku="$3" san="$4"
  openssl genrsa -out "$name.key" 2048 2>/dev/null
  openssl req -new -key "$name.key" -subj "/CN=$cn" -out "$name.csr"
  local ext="basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=$eku"
  [ -n "$san" ] && ext="$ext
subjectAltName=$san"
  openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -sha256 -out "$name.crt" 2>/dev/null \
    -extfile <(printf '%s\n' "$ext")
  rm -f "$name.csr"
}

# t03: старый server-серт без DNS:valkeyworker — перегенерируем (CA жив).
if [ ! -f server.crt ] || ! openssl x509 -in server.crt -noout -text 2>/dev/null | grep -q 'DNS:valkeyworker'; then
  issue server kafkaworker serverAuth "$SERVER_SAN"
fi

if [ ! -f pgserver.crt ]; then
  # серверный pgworker (SAN покрывает compose-DNS, localhost, host-gateway — R13)
  issue pgserver pgworker serverAuth "DNS:pgworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
fi

# клиентские (различимость в журналах сервера, независимый отзыв)
[ -f panel.crt ]      || issue panel      panel      clientAuth ""
[ -f seed.crt ]       || issue seed       seed       clientAuth ""
[ -f prometheus.crt ] || issue prometheus prometheus clientAuth ""
[ -f healthcheck.crt ] || issue healthcheck healthcheck clientAuth ""
chmod 600 ca.key ./*.key
echo "✓ TLS-пакет kfw-install-ca: ca.pem, server.* (kafkaworker+valkeyworker), pgserver.*, panel.*, seed.*, prometheus.*, healthcheck.*"
```

- [ ] **Шаг 2: Compose-сервис `valkeyworker`**

В `dev-stand/adminpanel/docker-compose.yml` после сервиса `kafkaworker` добавить (том `vw-snapshots` — в блок volumes):

```yaml
  # Живой ValkeyWorker (t03; arch/04 §2.4): профиль valkey — только явно.
  # Управляет docker-хостом через сокет; контейнеры нод vwk-* поднимает на
  # хосте стенда (portalloc 17000–17999: на одном docker-хосте один valkey-контур).
  # API-порт на хост НЕ публикуется: панель и чеки ходят через панель/панельную
  # сеть (compose-DNS valkeyworker:8080); сид — 05-seed.sh (exec curl внутрь).
  valkeyworker:
    build:
      context: ../..
      dockerfile: docker/ValkeyWorker.Dockerfile
    image: valkeyworker:dev
    container_name: as-valkeyworker
    restart: unless-stopped
    profiles: ["valkey"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - vw-snapshots:/snapshots
      # mTLS HTTP API (arch/21 §1.1): per-install пакет (deploy/tls/gen.sh) —
      # server-серт с SAN DNS:valkeyworker (t03), клиентский CA общий.
      - ../../deploy/tls:/tls:ro
    extra_hosts:
      - "local:host-gateway"
    environment:
      ValkeyWorker__Etcd__Endpoints__0: http://etcd:2379
      ValkeyWorker__AdvertisedClientHost: host.docker.internal
      ValkeyWorker__Api__AdvertiseUrl: https://valkeyworker:8080
      VWK_API_TLS_CERT_PATH: /tls/server.crt
      VWK_API_TLS_KEY_PATH: /tls/server.key
      VWK_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
      ValkeyWorker__Api__EnableSeedEndpoint: "true"
    depends_on: [etcd]
```
и в `volumes:` блока — `  vw-snapshots:`.

- [ ] **Шаг 3: `00-up.sh` — профиль valkey + wait + сид**

В `00-up.sh`:
1. Обе строки `docker compose --profile full --profile kafka --profile metrics up -d --build` (блоки подъёма/ретрая) → добавить `--profile valkey` (итог: `--profile full --profile kafka --profile valkey --profile metrics`).
2. Комментарий-шапку дополнить «+ valkeyworker».
3. После блока «7) kafkaworker жив» добавить шаг 7b (по 04 §3 — heartbeat живого ключа):

```bash
# 7b) valkeyworker жив (t03): heartbeat lease-ключ /valkeyworker/api/* — его
#     ждут панель (WorkerEndpoints) и чек 51 (мутации через панель→воркер).
for i in $(seq 1 60); do
  [ -n "$(docker compose exec -T etcd etcdctl get /valkeyworker/api/ --prefix --keys-only 2>/dev/null | head -1)" ] && break
  sleep 1
done
[ -n "$(docker compose exec -T etcd etcdctl get /valkeyworker/api/ --prefix --keys-only 2>/dev/null | head -1)" ] \
  || { echo "❌ valkeyworker не ожил за 60 c (docker compose logs valkeyworker)"; exit 1; }
echo "  valkeyworker жив (heartbeat /valkeyworker/api/*)"

# 7c) valkey-сид (t03): демо-кластер demo наливается ЧЕРЕЗ API живого воркера —
#     метрика spec §8.1: после ПОЛНОГО 00-up.sh панель /valkey уже показывает
#     demo (Active, RUNNING, endpoints, live) — без отдельного запуска чека.
#     05-seed.sh идемпотентен (SeedDemoHandler: живой config → 200 no-op),
#     wait до Active — внутри seed-функции; прецедент — pg-контур (00-up.sh
#     сам наливает pg-сид через API pgworker). Воркер продолжает жить.
"$PWD/checks/05-seed.sh" valkey
```

- [ ] **Шаг 4: `05-seed.sh` — блок `seed_valkey`**

1. Сигнатура режимов: `pg | kafka | valkey | all` — обновить usage-строку и диспетчер:
```bash
[ "$MODE" = pg ] || [ "$MODE" = kafka ] || [ "$MODE" = valkey ] || [ "$MODE" = all ] \
  || { echo "usage: 05-seed.sh [pg|kafka|valkey|all]"; exit 1; }
[ "$MODE" = kafka ] || [ "$MODE" = valkey ] || seed_pg
[ "$MODE" = pg ] || [ "$MODE" = valkey ] || seed_kafka
[ "$MODE" = pg ] || [ "$MODE" = kafka ] || seed_valkey
```
2. Функция (порт наливает ЗАЯВКУ — её доигрывает живой воркер, 04 §2.4; curl — ВНУТРИ контейнера, порт не публикуется):

```bash
seed_valkey() {
  docker compose --profile valkey up -d valkeyworker >/dev/null 2>&1
  # mTLS-курл внутри образа воркера (curl установлен HEALTHCHECK'ом; серты /tls).
  vwk_curl() {
    docker compose exec -T valkeyworker curl -fsS -m 3 \
      --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key "$@"
  }
  for i in $(seq 1 60); do vwk_curl https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
  vwk_curl https://localhost:8080/healthz >/dev/null \
    || { echo "❌ valkeyworker не ожил (:8080/healthz по mTLS из контейнера)"; exit 1; }
  echo "  valkey-сид: $(vwk_curl -X POST https://localhost:8080/api/seed/demo)"
  # Заявка доигрывается Reconcile-циклом воркера: ждём Active-конфиг demo
  # (config без state) ≤ бюджета тиков (NodeBootSec-граница 120 c + запас).
  for i in $(seq 1 150); do
    cfg="$(docker compose exec -T etcd etcdctl get /valkey/clusters/demo/config --print-value-only </dev/null 2>/dev/null)"
    [ -n "$cfg" ] && ! echo "$cfg" | grep -q '"state"' && break
    sleep 1
  done
  cfg="$(docker compose exec -T etcd etcdctl get /valkey/clusters/demo/config --print-value-only </dev/null 2>/dev/null)"
  [ -n "$cfg" ] && ! echo "$cfg" | grep -q '"state"' \
    || { echo "❌ демо-кластер demo не стал Active за 150 c (docker compose logs valkeyworker; контейнер vwk-demo-node1?)"; exit 1; }
  echo "  демо-кластер demo Active (воркер доиграл заявку; vwk-demo-node1 жив)"
}
```

- [ ] **Шаг 5: Чек `51-valkey-api.sh` (E2E-гейт)**

Новый файл (bash+jq, стиль `50-kafka-api.sh`), полный каркас:

```bash
#!/usr/bin/env bash
# 51-valkey-api.sh (t03, arch/04 §3; мерж-гейт задачи): valkey-домен против
# ЖИВОГО воркера (профиль valkey): сид demo (05-seed.sh valkey — заявка,
# доигранная воркером) → панель видит кластер (live-PING) → полный цикл
# мутаций ЧЕРЕЗ панель→прокси→API воркера с RunTag-именем → чистота после
# delete. Финал: демо-контур остаётся (сид живёт — полная система).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
TAG="v51$(date +%s)"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# etcd-хелперы (как 55-kafka-e2e.sh): чтение фактов мимо панели — для ожиданий.
etcd_key() { docker compose exec -T etcd etcdctl get "$1" --print-value-only </dev/null 2>/dev/null; }
etcd_has() { docker compose exec -T etcd etcdctl get "$1" --print-value-only </dev/null 2>/dev/null | grep -q .; }

# Arrange: сид через API живого воркера (поднимает valkeyworker, ждёт demo Active).
"$PWD/checks/05-seed.sh" valkey

for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' \
  || { echo "❌ login admin/admin не прошёл"; exit 1; }
api()  { curl -fsS -b "$JAR" "$BASE$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -b "$JAR" "$@"; }

# 1) Панель видит сид demo: сводка ACTIVE + нода RUNNING + endpoints (список),
#    детали с live=true (PING-проба по admin-креду из etcd; тики 3 c + 15 c).
# null при отсутствии demo: jq -e на null-входе даст false → цикл ждёт (не пустой вывод!).
demo_summary() { api /api/valkey/clusters | jq -c '[.[] | select(.name == "demo")][0]'; }
for i in $(seq 1 20); do
  demo_summary | jq -e '.state == "ACTIVE" and .nodesRunning == 1 and (.endpoints | length > 0)' >/dev/null 2>&1 && break
  sleep 2
done
demo_summary | jq -e '.state == "ACTIVE" and .nodesRunning == 1' >/dev/null \
  || { echo "❌ demo не ACTIVE/RUNNING в сводке"; exit 1; }
for i in $(seq 1 20); do
  api /api/valkey/clusters/demo | jq -e '.nodesList[0].live == true' >/dev/null 2>&1 && break
  sleep 2
done
api /api/valkey/clusters/demo | jq -e '.nodesList[0].live == true' >/dev/null \
  || { echo "❌ live=true не появился (PING-проба; AdminPanel__Probes__Valkey?)"; exit 1; }
echo "  сид demo: ACTIVE, нода RUNNING, live=true (PING)"

# 2) Создание кластера через панель: 201 → NOT_INITIALIZED → RUNNING ≤ бюджета
#    тиков воркера (NodeBootSec 120 c + запас на portalloc/загрузку образа).
body="{\"name\":\"$TAG\",\"maxmemoryBytes\":268435456,\"maxmemoryPolicy\":\"volatile-lru\",\"resources\":{\"cpu\":1,\"memGi\":1,\"diskGi\":10}}"
c="$(code -X POST "$BASE/api/valkey/clusters" -H 'Content-Type: application/json' -d "$body")"
[ "$c" = 201 ] || { echo "❌ create = $c, ожидался 201"; exit 1; }
for i in $(seq 1 150); do
  api "/api/valkey/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/valkey/clusters/$TAG" | jq -e '.state == "ACTIVE" and .nodesList[0].state == "RUNNING"' >/dev/null \
  || { echo "❌ $TAG не достиг ACTIVE/RUNNING за 300 c (docker compose logs valkeyworker)"; exit 1; }
echo "  create $TAG -> 201; RUNNING достигнут"

# 3) Конфиг-мутация: 200; converge D применяет БЕЗ рестарта контейнера
#    (container ID неизменен); панель видит новые значения ≤ пары тиков.
cid_before="$(docker inspect -f '{{.Id}}' "vwk-$TAG-node1" 2>/dev/null || true)"
c="$(code -X PUT "$BASE/api/valkey/clusters/$TAG/config" -H 'Content-Type: application/json' -d '{"maxmemoryBytes":134217728}')"
[ "$c" = 200 ] || { echo "❌ PUT config = $c"; exit 1; }
for i in $(seq 1 15); do
  api "/api/valkey/clusters/$TAG" | jq -e '.maxmemoryBytes == 134217728' >/dev/null 2>&1 && break
  sleep 2
done
api "/api/valkey/clusters/$TAG" | jq -e '.maxmemoryBytes == 134217728' >/dev/null \
  || { echo "❌ maxmemory не применился в панельных данных"; exit 1; }
cid_after="$(docker inspect -f '{{.Id}}' "vwk-$TAG-node1" 2>/dev/null || true)"
[ -n "$cid_before" ] && [ "$cid_before" = "$cid_after" ] \
  || { echo "❌ контейнер пересоздан при конфиг-мутации (ожидался converge без рестарта)"; exit 1; }
echo "  config-мутация: converge без рестарта, панель видит 128 MiB"

# 4) Ресурсы: 200 → автоконверге пересоздаёт контейнер (PROVISIONING → RUNNING).
c="$(code -X PUT "$BASE/api/valkey/clusters/$TAG/nodes/node1/resources" -H 'Content-Type: application/json' -d '{"cpu":2,"memGi":2,"diskGi":20}')"
[ "$c" = 200 ] || { echo "❌ PUT resources = $c"; exit 1; }
for i in $(seq 1 150); do
  api "/api/valkey/clusters/$TAG" | jq -e '.nodesList[0].state == "RUNNING"' >/dev/null 2>&1 \
    && docker inspect -f '{{.Id}}' "vwk-$TAG-node1" 2>/dev/null | grep -qv "$cid_after" && break
  sleep 2
done
docker inspect "vwk-$TAG-node1" >/dev/null 2>&1 \
  || { echo "❌ контейнер vwk-$TAG-node1 не поднялся после resources"; exit 1; }
echo "  resources: контейнер пересоздан, RUNNING"

# 5) Ротации app+admin: 202, заявка исполняется (ключ /valkeyworker/rotations/$TAG
#    исчезает тиком воркера).
for role in app admin; do
  c="$(code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d "{\"role\":\"$role\"}")"
  [ "$c" = 202 ] || { echo "❌ rotate $role = $c, ожидался 202"; exit 1; }
  for i in $(seq 1 15); do ! etcd_has "/valkeyworker/rotations/$TAG" && break; sleep 2; done
  etcd_has "/valkeyworker/rotations/$TAG" && { echo "❌ заявка ротации $role не исполнена за 30 c"; exit 1; }
done

# 5b) Двойная ротация (409): два ПАРАЛЛЕЛЬНЫХ POST до тика воркера — клэйм-txn
#     version==0 пропускает ровно один: ожидаем пару {202, 409}.
c1="$(mktemp)"; c2="$(mktemp)"
code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d '{"role":"app"}' >"$c1" &
p1=$!
code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d '{"role":"app"}' >"$c2" &
p2=$!
wait "$p1" "$p2"
r1="$(cat "$c1")"; r2="$(cat "$c2")"; rm -f "$c1" "$c2"
{ [ "$r1" = 202 ] && [ "$r2" = 409 ]; } || { [ "$r1" = 409 ] && [ "$r2" = 202 ]; } \
  || { echo "❌ двойная ротация: $r1/$r2, ожидалось 202+409"; exit 1; }
for i in $(seq 1 15); do ! etcd_has "/valkeyworker/rotations/$TAG" && break; sleep 2; done
etcd_has "/valkeyworker/rotations/$TAG" && { echo "❌ заявка двойной ротации не исполнена"; exit 1; }

# 5c) Негатив роли: role=wrong → 400 (валидирует воркер).
c="$(code -X POST "$BASE/api/valkey/clusters/$TAG/password/rotate" -H 'Content-Type: application/json' -d '{"role":"wrong"}')"
[ "$c" = 400 ] || { echo "❌ rotate role=wrong = $c, ожидался 400"; exit 1; }
echo "  ротации app+admin: 202 → исполнены; двойная → 409; role=wrong → 400"

# 6) 409-ветка создания: дубль имени занят.
c="$(code -X POST "$BASE/api/valkey/clusters" -H 'Content-Type: application/json' -d "{\"name\":\"$TAG\"}")"
[ "$c" = 409 ] || { echo "❌ дубль имени = $c, ожидался 409"; exit 1; }
echo "  дубль имени $TAG -> 409"

# 7) Алерты домена: worker-api-unreachable нет при живом воркере; alerts содержит
#    только реальные (проверка отсутствия critical worker-api-unreachable valkey).
api /api/alerts | jq -e 'any(.[]; .kind == "worker-api-unreachable" and .target == "valkeyworker") | not' >/dev/null \
  || { echo "❌ ложный worker-api-unreachable при живом воркере"; exit 1; }
echo "  /api/alerts: valkey-грань чиста при живом воркере"

# 8) Удаление: 202 → демонтаж воркером → кластер исчезает из панели; чистота:
#    ни контейнера vwk-$TAG-*, ни ключей /valkey/clusters/$TAG/.
c="$(code -X DELETE "$BASE/api/valkey/clusters/$TAG")"
[ "$c" = 202 ] || { echo "❌ delete = $c, ожидался 202"; exit 1; }
for i in $(seq 1 150); do
  ! docker inspect "vwk-$TAG-node1" >/dev/null 2>&1 \
    && ! etcd_has "/valkey/clusters/$TAG/config" && break
  sleep 2
done
docker inspect "vwk-$TAG-node1" >/dev/null 2>&1 && { echo "❌ контейнер vwk-$TAG-node1 не удалён"; exit 1; }
etcd_has "/valkey/clusters/$TAG/config" && { echo "❌ ключи /valkey/clusters/$TAG/ не удалены"; exit 1; }
api /api/valkey/clusters | jq -e "any(.[]; .name == \"$TAG\") | not" >/dev/null \
  || { echo "❌ $TAG не исчез из панельного списка"; exit 1; }
echo "  delete -> 202; чистота: контейнера и ключей нет, из панели исчез"

# Финал: демо-контур остаётся (сид живёт; чистка — 90-down.sh).
api /api/valkey/clusters | jq -e 'any(.[]; .name == "demo")' >/dev/null \
  || { echo "❌ демо-кластер demo пропал"; exit 1; }
echo "✓ 51-valkey-api: полный цикл valkey-домена через панель — все шаги зелёные"
```

Сделать исполняемым: `chmod +x dev-stand/adminpanel/checks/51-valkey-api.sh`.

- [ ] **Шаг 6: `90-down.sh` — valkey-профиль + демо-контейнер**

Полный новый текст:
```bash
#!/usr/bin/env bash
# Разбор стенда; -v — стереть и данные (вкл. etcd-data; spec t10 §7.6).
# Профили kafka+valkey ОБЯЗАТЕЛЬНЫ в down: воркеры (restart: unless-stopped)
# иначе переживают down с закешированным negative-DNS умершего etcd (t03:
# valkeyworker + демо-контейнер vwk-demo-node1 — контейнер воркера удаляем
# руками: он создан на docker-хосте, compose его не знает).
set -euo pipefail
cd "$(dirname "$0")/.."
# демо-контейнер сида valkey (arch/04 §2.4): создан воркером вне compose.
docker rm -f vwk-demo-node1 >/dev/null 2>&1 || true
if [ "${1:-}" = "-v" ]; then
  docker compose --profile full --profile kafka --profile valkey down -v --remove-orphans
  echo "✓ стенд разобран (данные стёрты)"
else
  docker compose --profile full --profile kafka --profile valkey down --remove-orphans
  echo "✓ стенд разобран (etcd-data сохранён)"
fi
```

- [ ] **Шаг 7: Прогон стенда (E2E-гейт) + зачистка**

Предусловие: свежие образы (00-up пересобирает инкрементально), registry-образ `valkey/valkey:9.1.2` на хосте (`dev-stand/images/pull-images.sh` при необходимости).
```bash
bash dev-stand/adminpanel/checks/00-up.sh
# Метрика spec §8.1: после ПОЛНОГО 00-up.sh (сид — шагом 7c) панель уже видит
# демо-кластер demo: Active, нода RUNNING, endpoints (live=true даст тик пробы
# ≤15 c — его досматривает чек 51).
J=$(mktemp)
curl -fsS -c "$J" -o /dev/null -X POST http://localhost:5050/api/auth/login \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}'
curl -fsS -b "$J" http://localhost:5050/api/valkey/clusters \
  | jq -e '[.[] | select(.name == "demo")][0] | .state == "ACTIVE" and .nodesRunning == 1 and (.endpoints | length > 0)' \
  || { echo "❌ demo не виден после 00-up.sh (метрика spec §8.1)"; exit 1; }
rm -f "$J"
bash dev-stand/adminpanel/checks/51-valkey-api.sh
bash dev-stand/adminpanel/checks/90-down.sh
docker network prune -f   # страховочный гейт: осиротевших сетей нет
docker ps -a --format '{{.Names}}' | grep -c 'vwk-\|as-valkeyworker' || echo 0   # → 0
```
Каждый шаг — финальная строка `✓`; упавший сценарий разбирается ПО ЛОГАМ (docker logs valkeyworker/adminpanel; перезапуск упавшего — только после анализа, правило телеметрии AGENTS.md).

```bash
git add deploy/tls/gen.sh dev-stand/adminpanel/
git commit -m "feat(valkey-panel): стенд — сервис valkeyworker (профиль valkey), SAN серта, сид demo, чек 51, чистка 90"
```

**Выход:** полный стенд поднимается с valkey-контуром; чек 51 зелёный на свежем образе панели (метрика успеха §8).
**Проверка:** шаг 7 (все команды зелёные; после 90-down — ни `vwk-*`, ни `as-valkeyworker`, сетей нет).
**Spec:** §4.11, §5.3, §6.9, §8 (метрики 1–4, 8).

---

### Task 10: Финальный прогон + мерж-гейт (roadmap)

**Files:**
- Modify (при мерже): `arch/roadmap/valkey.md` — снять тег `t03-valkey-panel`.

- [ ] **Шаг 1: Полный прогон юнитов панели**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug
```
Ожидание: все зелёные (вкл. новые Valkey*-тесты).

- [ ] **Шаг 2: Полный прогон интеграции панели + зачистка**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug
docker ps -aq --filter "label=org.testcontainers" --format '{{.ID}}' | xargs -r docker rm -f
docker network prune -f
docker network ls | grep -c 'pgw-\|kfw-net' || echo 0   # только ожидаемые (0 осиротевших)
```

- [ ] **Шаг 3: Release-сборка (0 warnings) + Release-прогон юнитов**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build
```

- [ ] **Шаг 4: Свежий E2E на Release-образе (мерж-гейт)**

Стенд собирает образ панели из Release автоматически (00-up.sh --build инкрементальный); повторить Task 9 шаг 7 (00-up → 51 → 90-down + зачистка). Docker-E2E PgWorker (`Scale_AddEmptyShard`) НЕ требуется — код воркеров не трогался (spec §5.3).

- [ ] **Шаг 5: Мерж-гейт — roadmap-правка ТЕМ ЖЕ мерж-коммитом**

При мерже ветки в `main` (по явной команде пользователя): удалить пункт `t03-valkey-panel` из `arch/roadmap/valkey.md` тем же коммитом мержа (правила `arch/roadmap/README.md`; стрелок `← t03-valkey-panel` у других пунктов нет — проверить `grep -rn "t03-valkey-panel" arch/roadmap/` → единственное вхождение сам пункт). Сам, без команды и вопросов. Никаких пометок «закрыта» — история в git и `docs/superpowers/`.

**Выход:** задача готова к мержу; критерии приёмки spec §10 закрыты.
**Проверка:** шаги 1–4 зелёные; в мерж-коммите roadmap-тег снят.
**Spec:** §6.10, §9, §10.

---

## Самопроверка (выполнена автором плана)

1. **Покрытие spec → задачи:** §1.2/§6.1 arch-коммит → Task 1; §4.1 модель → Task 2; §4.2 парсер → Task 2; §4.3 refresher → Task 3; §4.4 secrets → Task 3; §4.5 инфраструктура воркеров → Task 4; §4.7 алерты → Task 5; §4.8 инспекция/сводные → Task 5+7; §4.6 пробы → Task 6; §4.9 мутации → Task 7; §4.10 фронт → Task 8; §4.11 стенд → Task 9; §5 тесты → Tasks 2/3/5/6/7/9; §9 roadmap → Task 10. Ограничения §7 отражены в «Глобальных ограничениях» (креды, без новых пакетов, без правки воркеров, без хардкод-портов).
2. **Мелкие решения зафиксированы:** скелет `ValkeyAlertEngine` создаётся в Task 3 (DI refresher'а), наполняется в Task 5; `WorkerHealth`-перенос в refresher — Task 4 шаг 4; seed/чеки ходят в API воркера `docker compose exec` (порт на хост не публикуется — curl есть в образе воркера); SAN — общий server-серт kfw+vwk с перегенерацией старых пакетов (идемпотентность сохранена). Правки ревью Фазы 4: полный `00-up.sh` сам наливает valkey-сид (шаг 7c → метрика spec §8.1: после голого подъёма панель `/valkey` уже показывает `demo` Active/RUNNING/endpoints); overview-маппер valkey — `public static OverviewMapper.MapValkey` с прямыми юнит-тестами (null → null; critical считаются только кластерные kinds — отличие от `MapKafka` зафиксировано тестом).
3. **Типы согласованы:** `ValkeySnapshot`/`ValkeyClusterInfo`/`ValkeyNodeInfo`/`ValkeyRotationTicket`/`ValkeyProbeResult` (Task 2) используются без переименований в Tasks 3–7; DTO REST (Task 7) зеркалят `dto.ts` (Task 8); kinds алертов совпадают в Task 5 и чеке 51; префиксы `/valkey/clusters/`, `/valkeyworker/{rotations,api}/`, `/workers/api_tls/valkeyworker` едины во всех задачах.
