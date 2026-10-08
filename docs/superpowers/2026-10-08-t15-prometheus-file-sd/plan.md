# t15-prometheus-file-sd — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Метрики реальных Patroni-нод кластеров PgWorker собираются Prometheus'ом стенда: мини-сервис `sd-generator` тиком читает `/pgworker/portalloc/` из etcd (read-only, с failover по endpoints) и атомарно пишет file_sd JSON в volume Prometheus; джоба `patroni-nodes`, алерты и панели на нативные `patroni_*`.

**Architecture:** Новый независимый .NET mini-сервис `src/Metrics.SdGenerator` (console + hosted service, Kestrel только для самонаблюдения `/metrics`) в профиле `metrics` dev-стенда. Контракт etcd НЕ меняется — генератор «немой читатель» portalloc (как панель). Чистая функция маппинга `Kv → file_sd-группы`, атомарная запись при diff (tmp+rename, byte-compare), консервативная свежесть (ошибка etcd не трогает файл). Приёмка: unit → integration (живой etcd) → docker-E2E (реальный кластер + настоящий Prometheus). arch-first: правки arch/18 до кода (Ф0).

**Tech Stack:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), System.Diagnostics.Metrics + OTel Prometheus-экспортёр (уже в `Shared.Metrics`), xUnit v3 + FluentAssertions + Testcontainers; Prometheus file_sd (JSON-группы `targets`+`labels`).

**Spec:** `docs/superpowers/2026-10-08-t15-prometheus-file-sd/spec.md` (в этом же каталоге; план не спорит со spec — executors читают оба).

## Global Constraints

- Работа ТОЛЬКО в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t15-prometheus-file-sd` (ветка `feat-t15-prometheus-file-sd`); команды ниже — из корня worktree (пути относительные).
- .NET 10, `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`** (`src/Directory.Build.props` наследуется) — сборка обязана быть 0 warnings.
- Централизованное версионирование (CPM, `src/Directory.Packages.props`): НОВЫХ пакетов нет — OTel/xunit/FluentAssertions/Testcontainers уже запинены.
- Контракт etcd НЕ меняется (spec §2): генератор — read-only потребитель `/pgworker/portalloc/<C>`; ноль новых etcd-ключей, ноль записей.
- Код PgWorker/KafkaWorker/ValkeyWorker/AdminPanel НЕ меняется (spec §5). Единственное исключение — тестовая инфраструктура `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` (сборка e2e-образа генератора + публичный доступ к сети контура) — это не код воркеров.
- Лейблы конечны (spec §2, M1): у таргетов только `cluster`, `shard`, `node`; самонаблюдение — серия без лейблов.
- Порты docker-контейнеров в тестах — ТОЛЬКО динамические (`assignRandomHostPort: true` + `GetMappedPublicPort`, либо `E2eEnvironment.ReserveWindowPort`); никаких литералов портов в ассертах.
- Каждый интеграционный/E2E тест полностью чистит за собой (teardown при любом исходе + ассерт чистоты); телеметрия E2E: `[PHASE]`-метки фаз, docker-логи в teardown до удаления, `MarkFailed()` для упавших.
- Образ `sdgenerator:dev` / `sdgenerator:e2e` — локально собираемые, в registry `192.168.0.1:5000` НЕ класть; `images.txt` НЕ меняется (базы `aspnet:10.0`, `prom/prometheus:v3.14.0`, `etcd:v3.5.21` уже зеркалированы).
- E2E-образ: publish НА ХОСТЕ, в контейнер — только publish-вывод (runtime-слой, без sdk-стадии и `COPY src/`); вывод сборки — с `[PHASE]`-меткой и таймингом.
- `BrokerBootSec`-подобные таймауты интеграционных фикстур ≤ 100 с; ожидания в тестах — поллом ≤ 500 мс с общим бюджетом.
- Комментарии/доки — русский; идентификаторы — английские; тесты — с AAA-комментариями.
- Коммиты в стиле репо: `feat(t15): …` / `test(t15): …` / `docs(t15): …` (описание на русском).
- Перезапуск упавших тестов «для выяснения» запрещён: сначала анализ логов/артефактов (`/tmp/pgw-e2e-artifacts-<guid>/`, `/tmp/pgw-e2e-static-*.log`).

## Карта файлов

| Файл | Ответственность |
|---|---|
| `arch/18-metrics.md` | Ф0: §2.5 словарь `patroni_*`, §5.2 джобы `patroni-nodes`/`sd-generator`, §5.4 реализованный паттерн, §6 приёмка, §8 конфиг `SdGenerator:*` |
| `src/Metrics.SdGenerator/Metrics.SdGenerator.csproj` | новый mini-сервис (Worker SDK, FrameworkReference AspNetCore для Kestrel) |
| `src/Metrics.SdGenerator/SdGeneratorOptions.cs` | `[Config]`-опции + нормализация интервала |
| `src/Metrics.SdGenerator/TargetMapping.cs` | чистая функция portalloc → file_sd-группы + сериализация JSON |
| `src/Metrics.SdGenerator/SdFileWriter.cs` | атомарная запись файла при diff (tmp+rename) |
| `src/Metrics.SdGenerator/SdGeneratorMetrics.cs` | константы `MeterName`/`LastSuccessInstrument` (каркас Task 2 — компилируемость Program.cs) + ObservableGauge `sd_generator.last_success_timestamp_seconds` (Meter через ctor — DI-канон; реализация Task 5) |
| `src/Metrics.SdGenerator/EtcdFailover.cs` | копия паттерна failover (без ProjectReference на воркеров) |
| `src/Metrics.SdGenerator/SdGeneratorLoop.cs` | тик: failover-Range → Map → Write; консервативная свежесть |
| `src/Metrics.SdGenerator/SdGeneratorHostedService.cs` | BackgroundService с PeriodicTimer |
| `src/Metrics.SdGenerator/Program.cs` | HostApplicationBuilder-точка входа: DI, Kestrel `/metrics`, hosted service |
| `src/Metrics.SdGenerator/appsettings.json` | дефолты `SdGenerator:*` |
| `docker/Metrics.SdGenerator.Dockerfile` | runtime-слой, COPY publish-вывода (контекст = каталог publish) |
| `dev-stand/adminpanel/docker-compose.yml` | сервис `sd-generator` (профиль metrics) + volume `prometheus-sd` + маунт в prometheus |
| `dev-stand/adminpanel/checks/00-up.sh` | publish генератора на хосте перед compose up (с [PHASE]) |
| `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` | джобы `patroni-nodes` (file_sd), `sd-generator` (static) |
| `dev-stand/adminpanel/metrics/prometheus/rules.yml` | +3 алерта в группу `pg` |
| `dev-stand/adminpanel/metrics/grafana/dashboards/pg.json` | +4 панели «real» |
| `dev-stand/adminpanel/checks/65-metrics.sh` | строгий all-up БЕЗ patroni-nodes + условный шаг 2.1; счётчик алертов ≥ 21, самоскрейп sd-generator |
| `src/tests/Metrics.SdGenerator.UnitTests/*` | unit: маппинг, writer, loop, метрика, интервал |
| `src/tests/Metrics.SdGenerator.IntegrationTests/*` | integration: живой etcd (testcontainers), цикл/файл/свежесть |
| `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` | публичный `EnsureSdImageAsync` (сборка `sdgenerator:e2e`, лениво — из сценария); публичный доступ к сети контура |
| `src/tests/PgWorker.IntegrationTests/E2e/E2ePatroniFileSdScenarios.cs` | docker-E2E: кластер → таргеты up → серии → демонтаж → исчезли |
| `src/PgWorker.slnx` | +3 проекта (svc, unit, integration) |
| `arch/roadmap/reliability.md`, `arch/roadmap/reliability-report.md` | мерж-гейт: снятие t15 + перенос строки + сводка N |

## Контракт типов (единственное место определения; задачи повторяют сигнатуры)

```csharp
// SdGeneratorOptions.cs
public sealed class SdGeneratorOptions
{
    public const int DefaultRefreshIntervalSec = 15;
    public EtcdOptions Etcd { get; set; } = new();
    public int RefreshIntervalSec { get; set; } = DefaultRefreshIntervalSec;
    public string OutputPath { get; set; } = "/sd/patroni-nodes.json";
    public MetricsOptions Metrics { get; set; } = new();   // Shared.Metrics.MetricsOptions
    public sealed class EtcdOptions { public string[] Endpoints { get; set; } = []; }
    // <=0 → 15 (паттерн §4.2 valkey; warning-лог — в Program.cs при старте)
    public static int NormalizeInterval(int value)
        => value <= 0 ? DefaultRefreshIntervalSec : value;
}

// TargetMapping.cs
public sealed record SdTargetGroup(string Target, string Cluster, string Shard, string Node);
public static class TargetMapping
{
    public const string PortallocPrefix = "/pgworker/portalloc/";
    // Kv[] → группы; warn(cluster, error) вызывается на битые/незнакомые записи (skip)
    public static IReadOnlyList<SdTargetGroup> Map(IReadOnlyList<Kv> kvs, Action<string, string> warn);
    // Детерминированный file_sd JSON: [{"targets":["h:p"],"labels":{cluster,shard,node}}…]
    public static string Serialize(IReadOnlyList<SdTargetGroup> groups);
}

// SdFileWriter.cs
public sealed class SdFileWriter(string path)
{
    /// true — файл перезаписан (tmp+rename); false — контент не изменился (mtime не тронут)
    public bool WriteIfChanged(string content);
}

// SdGeneratorMetrics.cs — Meter приходит через ctor (канон репо: DI-Meter из
// AddAppMetrics, паттерн KafkaWorker.App; standalone/юниты — свой). Meter в
// Dispose НЕ диспозим — он принадлежит владельцу/DI (канон WorkerMetricsInstrumentation).
public sealed class SdGeneratorMetrics(System.Diagnostics.Metrics.Meter meter, TimeProvider time) : IDisposable
{
    public const string MeterName = "SdGenerator";
    public const string LastSuccessInstrument = "sd_generator.last_success_timestamp_seconds";
    public long? LastSuccessUnix { get; }        // null — серия не эмитится (до первого успеха)
    public void MarkSuccess();                   // обновляет стейт ObservableGauge
}

// SdGeneratorLoop.cs
public sealed class SdGeneratorLoop(
    IEtcdGateway etcd, string[] endpoints, SdFileWriter writer,
    SdGeneratorMetrics metrics, ILogger<SdGeneratorLoop> logger)
{
    /// true — успешный тик (пустой префикс = успех); false — ошибка чтения
    /// (warning-лог, файл и last_success НЕ тронуты)
    public async Task<bool> TickAsync(CancellationToken ct);
}

// SdGeneratorHostedService.cs
internal sealed class SdGeneratorHostedService(
    SdGeneratorOptions options, SdGeneratorLoop loop, ILogger<SdGeneratorHostedService> logger)
    : BackgroundService;   // PeriodicTimer(TimeSpan.FromSeconds(NormalizeInterval(RefreshIntervalSec)))
```

Свой DTO парсинга portalloc (генератор НЕ ссылается на PgWorker.Core — независимость):

```csharp
// TargetMapping.cs (вложенный): {"<shard>/<node>": {"host","pg","patroni","doorman"}}
private sealed record PortallocEntry(string Host, int Pg, int Patroni, int Doorman);
```

---

### Task 1: Ф0 — контракт arch/18 (arch-first, до кода)

**Вход (предусловие):** spec одобрен; worktree чист (кроме `docs/superpowers/…t15…/spec.md`).

**Действие (Files):** Modify `arch/18-metrics.md` — пять правок по spec §3.1. Тексты вставок:

1. **§2.5** — заменить абзац «Узлы, создаваемые PgWorker в per-cluster сетях…» и дополнить секцию словарём нативных серий (после существующего текста об эмуляторах):

```markdown
Словарь реальных Patroni-нод — нативные серии REST `/metrics` (spilo,
Patroni 4.x); канон-минимум, на который пишутся дашборд/алерты:
`patroni_master`, `patroni_replica`, `patroni_sync_standby`,
`patroni_timeline`, `patroni_xlog_replay_timestamp`, `patroni_version`,
`patroni_postgres_running` (лейблы `scope`, `name` — сам Patroni).
Два словаря сосуществуют: эмуляторный `pg_replica_lag_seconds` (стенд без
PgWorker-кластеров) и нативный `patroni_*` (реальные ноды); фактический
набор фиксирует docker-E2E (§6). Таргеты реальных нод — file_sd из
portalloc (§5.2 `patroni-nodes`, §5.4): узлы PgWorker скрейпятся по
host-публикациям портов Patroni, per-cluster сети для этого не нужны.
```

2. **§5.2** — две новые строки таблицы джоб:

```markdown
| `patroni-nodes` | file_sd `/etc/prometheus/sd/patroni-nodes.json` (scheme http — Patroni REST без TLS, t22 вне скоупа; источник файла — генератор §5.4 из `/pgworker/portalloc/`) | §2.5 нативные `patroni_*` |
| `sd-generator` | static: имя сети стенда `sd-generator:8080` (профиль `metrics`) | самонаблюдение генератора §5.4 |
```

3. **§5.4** — заменить «file_sd из etcd-снапшота — опция будущих задач» на реализованный паттерн:

```markdown
Прод-мультихост: Prometheus рядом с docker-хостами, таргеты нод — из
advertise-адресов portalloc; паттерн реализован в стенде: file_sd-генератор
`sd-generator` (профиль `metrics`, `src/Metrics.SdGenerator`) тиком читает
`/pgworker/portalloc/` (read-only, без клэймов) и пишет
`sd/patroni-nodes.json` в volume Prometheus; таргет ноды = advertised host +
patroni-порт (host-публикация) — на мульти-хосте адреса честны, достижимость
скрейпа — зона сетевой политики прода. Граница: Kafka/Valkey-ноды без HTTP
metrics-эндпоинта наблюдаются доменными сериями коллекторов воркеров
(§2.3/§2.6); расширение словаря их нод — отдельные задачи.
```

4. **§6** — дополнить пункт «Тестирование» строками приёмки t15 (unit-маппинг; integration-цикл с etcd-фикстурой; docker-E2E «кластер → таргет up → демонтаж → таргет исчез», фиксирует фактический набор `patroni_*`).

5. **§8** — дополнить блок конфигурации:

```
SdGenerator:Etcd:Endpoints[]           # узлы HA-контура
SdGenerator:RefreshIntervalSec=15      # <=0 → 15 + warning
SdGenerator:OutputPath=/sd/patroni-nodes.json
SdGenerator:Metrics { Enabled=true, Path="/metrics" }
```

- [ ] Step 1: Внести пять правок в `arch/18-metrics.md` (тексты выше; вычитка тона окружающего текста).
- [ ] Step 2: Коммит: `git add arch/18-metrics.md docs/superpowers/2026-10-08-t15-prometheus-file-sd/ && git commit -m "docs(t15): контракт наблюдаемости реальных Patroni-нод (arch/18 §2.5/§5.2/§5.4/§6/§8) + spec"`.

**Выход:** контракт в истории; точка отсчёта код-задач.

**Проверка:** `git show --stat HEAD` — один коммит arch+spec; `grep -c "patroni-nodes" arch/18-metrics.md` ≥ 3.

**Spec:** §3.1 (таблица правок), §2 (arch-first).

---

### Task 2: Каркас `src/Metrics.SdGenerator` + опции (unit на нормализацию интервала)

**Вход:** Task 1 закоммичен.

**Действие (Files):**
- Create `src/Metrics.SdGenerator/Metrics.SdGenerator.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk.Worker">
    <ItemGroup>
        <InternalsVisibleTo Include="Metrics.SdGenerator.UnitTests"/>
    </ItemGroup>
    <ItemGroup>
        <FrameworkReference Include="Microsoft.AspNetCore.App"/>
    </ItemGroup>
    <ItemGroup>
        <ProjectReference Include="..\Shared.Core\Shared.Core.csproj"/>
        <ProjectReference Include="..\Shared.Etcd\Shared.Etcd.csproj"/>
        <ProjectReference Include="..\Shared.Metrics\Shared.Metrics.csproj"/>
    </ItemGroup>
</Project>
```

- Create `src/Metrics.SdGenerator/SdGeneratorOptions.cs` — сигнатуры из контракта типов (включая `NormalizeInterval`); XML-doc комментарии по-русски.
- Create `src/Metrics.SdGenerator/appsettings.json`:

```json
{
  "Logging": { "LogLevel": { "Default": "Information", "Microsoft.Hosting.Lifetime": "Information" } },
  "SdGenerator": {
    "Etcd": { "Endpoints": [ "http://localhost:2379" ] },
    "RefreshIntervalSec": 15,
    "OutputPath": "/sd/patroni-nodes.json",
    "Metrics": { "Enabled": true, "Path": "/metrics" }
  }
}
```

- Create `src/Metrics.SdGenerator/SdGeneratorMetrics.cs` — минимальный каркас: только константы из контракта типов (реализация ObservableGauge/`MarkSuccess`/Dispose — Task 5). Файл нужен уже в Task 2: Program.cs ниже ссылается на `SdGeneratorMetrics.MeterName`, без класса гейт Task 2 (`dotnet build` → PASS) падает на CS0103:

```csharp
namespace Metrics.SdGenerator;

// Каркас Task 2: только константы — на MeterName ссылается Program.cs
// (AddAppMetrics); ctor(meter, time), LastSuccessUnix, MarkSuccess, Dispose — Task 5.
public sealed class SdGeneratorMetrics
{
    public const string MeterName = "SdGenerator";
    public const string LastSuccessInstrument = "sd_generator.last_success_timestamp_seconds";
}
```

- Create `src/Metrics.SdGenerator/Program.cs` — минимальный каркас (цикл подключит Task 5):

```csharp
using Metrics.SdGenerator;
using Shared.Etcd.Client;
using Shared.Metrics;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<SdGeneratorOptions>(builder.Configuration.GetSection("SdGenerator"));
// Самонаблюдение (arch/18 §5.2 job sd-generator): AddAppMetrics при Enabled=true
// регистрирует OTel-провайдер и DI-Meter с именем SdGenerator; инструментарий
// (Task 5) берёт этот Meter из DI (канон KafkaWorker.App). Enabled=false —
// провайдера/Meter в DI нет, SdGeneratorMetrics создаст свой (пишем «в никуда»).
builder.Services.AddAppMetrics(SdGeneratorMetrics.MeterName,
    builder.Configuration.GetSection("SdGenerator:Metrics"));
builder.Services.AddHttpClient("etcd");
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));
// TODO(t15 Task 5): SdFileWriter, SdGeneratorMetrics, SdGeneratorLoop, HostedService
var app = builder.Build();
app.MapAppMetrics();
app.Run();
```

- Create `src/tests/Metrics.SdGenerator.UnitTests/Metrics.SdGenerator.UnitTests.csproj` (копия структуры `src/tests/Shared.Metrics.UnitTests/*.csproj`: xunit.v3, FluentAssertions, coverlet; ProjectReference на `../../Metrics.SdGenerator/Metrics.SdGenerator.csproj`).
- Modify `src/PgWorker.slnx`: в `/common/` проект `Metrics.SdGenerator/Metrics.SdGenerator.csproj`; в `/tests/` — `tests/Metrics.SdGenerator.UnitTests/...`.

- [ ] Step 1: Создать тесты нормализации (AAA):

```csharp
public class SdGeneratorOptionsTests
{
    [Theory]
    [InlineData(0, 15)] [InlineData(-5, 15)] [InlineData(1, 1)] [InlineData(15, 15)] [InlineData(60, 60)]
    public void NormalizeInterval_ClampsNonPositive(int given, int expected)
    {
        // Act
        var actual = SdGeneratorOptions.NormalizeInterval(given);
        // Assert
        actual.Should().Be(expected);
    }
}
```

- [ ] Step 2: `dotnet test src/tests/Metrics.SdGenerator.UnitTests -c Release` — FAIL (проекта нет) → создать csproj/опции/каркас/slnx-строки → PASS.
- [ ] Step 3: `dotnet build src/PgWorker.slnx -c Release` — 0 errors, 0 warnings.
- [ ] Step 4: `cd src/Metrics.SdGenerator && dotnet run` вручную НЕ поднимать (правило: PgWorker всегда в докере; генератор тоже поставляется образом — Task 6); вместо этого smoke: `ASPNETCORE_URLS=http://127.0.0.1:18099 dotnet src/Metrics.SdGenerator/bin/Release/net10.0/Metrics.SdGenerator.dll & sleep 3; curl -fsS http://127.0.0.1:18099/metrics | head -3; kill %1` — ожидание: HTTP 200, `# HELP`/`# TYPE` (или пустой экспорт до первого инструмента — после Task 5 серия появится).
- [ ] Step 5: Коммит `feat(t15): каркас Metrics.SdGenerator (опции, Kestrel /metrics, slnx)`.

**Выход:** собирающийся сервис-каркас с `/metrics`; опции с нормализацией.

**Проверка:** шаги 2–4 зелёные; `grep -c "Metrics.SdGenerator" src/PgWorker.slnx` = 2.

**Spec:** §3.2 (мини-сервис, `[Config]`-паттерн), §3.5 (конфигурация), §6.1 (дефолт `<=0` → 15).

---

### Task 3: Маппинг `TargetMapping` (чистая функция, TDD)

**Вход:** Task 2.

**Действие (Files):**
- Create `src/Metrics.SdGenerator/TargetMapping.cs`: `SdTargetGroup`, `Map`, `Serialize`, DTO `PortallocEntry` (контракт типов). Правила `Map`:
  - ключ `/pgworker/portalloc/<C>` (кластер = последний сегмент ключа; чужие префиксы игнорируются фильтром `key.StartsWith(PortallocPrefix)`);
  - JSON-значение — словарь `"<shard>/<node>" → {host,pg,patroni,doorman}`; поле `object` и любые незнакомые поля игнорируются (толерантность панели: state-значения развиваются);
  - на каждую запись с `patroni > 0` — группа `Target = $"{host}:{patroni}"`;
  - `patroni <= 0` (усыптлённые) — пропуск без warning (штатная семантика, arch/14 §2.4);
  - битый JSON / не-словарь / нечисловые порты — пропуск записи ключа ЦЕЛИКОМ + `warn(cluster, error)`;
  - `host` пустой — как битая запись (warn+skip);
  - порядок групп детерминирован: сортировка по (Cluster, Shard, Node);
  - пустой список Kv → пустой список (serialize → `[]`).
- `Serialize`: `JsonSerializer` без отступов, порядок полей фиксорован: `targets`, `labels{cluster,shard,node}`.

- Test `src/tests/Metrics.SdGenerator.UnitTests/TargetMappingTests.cs` — кейсы (каждый — отдельный `[Fact]` с AAA-комментарием; `Kv` из `Shared.Etcd.Client`, `ModRevision: 1`):

| Кейс | Вход (Kv) | Ожидание |
|---|---|---|
| Одна нода | ключ `/pgworker/portalloc/c1`, value `{"shard1/shard1a":{"host":"h1","pg":5432,"patroni":8008,"doorman":6432}}` | 1 группа `("h1:8008","c1","shard1","shard1a")` |
| Усыптлённая нода | `...{"patroni":0}...` (вторая запись словаря) | группы нет, warn НЕ вызван |
| Битый JSON | value `{"shard1/` | 0 групп, warn вызван с `c1` |
| Незнакомое поле | value с доп. `"object":"x","extra":true` | группа есть |
| Пустой префикс | `[]` Kv | `Serialize → "[]"` |
| Детерминизм | 2 кластера × 2 шарда × 2 ноды в перемешанном порядке | группы отсортированы (Cluster,Shard,Node); два вызова на один вход — равные списки |
| Формат | одна группа | JSON: `[{"targets":["h1:8008"],"labels":{"cluster":"c1","shard":"shard1","node":"shard1a"}}]` |
| Чужой ключ | `/pgworker/portallocX/c1` | 0 групп, warn НЕ вызван |

- [ ] Step 1: Написать тесты таблицы → `dotnet test src/tests/Metrics.SdGenerator.UnitTests -c Release` FAIL (типа нет).
- [ ] Step 2: Реализовать `TargetMapping` → тесты PASS.
- [ ] Step 3: Коммит `feat(t15): маппинг portalloc → file_sd-таргеты (чистая функция)`.

**Выход:** детерминированный маппинг с толерантностью к битым записям.

**Проверка:** все `[Fact]` зелёные; `dotnet build src/PgWorker.slnx -c Release` 0 warnings.

**Spec:** §3.2 (маппинг), §6.1 (unit-критерии 1).

---

### Task 4: Атомарная запись `SdFileWriter` (TDD)

**Вход:** Task 3.

**Действие (Files):**
- Create `src/Metrics.SdGenerator/SdFileWriter.cs` — контракт типов; реализация: `File.Exists` + byte-compare (`File.ReadAllBytes` vs `Encoding.UTF8.GetBytes(content)`) → при равенстве вернуть `false`; иначе записать `path + ".tmp"` → `File.Move(tmp, path, overwrite: true)` → `true`.

- Test `src/tests/Metrics.SdGenerator.UnitTests/SdFileWriterTests.cs` (temp-каталог `Path.Combine(Path.GetTempPath(), Guid...)`, cleanup в finally):
  - `Write_CreatesFile_WhenAbsent`: первый вызов `true`, файл существует, контент совпал.
  - `Write_Skips_WhenUnchanged`: два вызова подряд одним контентом — второй `false`; `File.GetLastWriteTimeUtc` НЕ изменился (снимок между вызовами).
  - `Write_Rewrites_WhenChanged`: контент A → B: `true`, файл = B; `.tmp`-файла не осталось (`Directory.GetFiles(dir, "*.tmp")` пусто).

- [ ] Step 1: тесты → FAIL. Step 2: реализация → PASS. Step 3: коммит `feat(t15): атомарная запись file_sd при diff (tmp+rename, mtime не дёргается)`.

**Выход:** безопасная запись, не будоражащая mtime.

**Проверка:** unit зелёные; в репо нет `*.tmp` хвостов.

**Spec:** §2 («запись файла минимальна»), §3.2.

---

### Task 5: Метрика самонаблюдения + цикл `SdGeneratorLoop` + hosted service + wiring

**Вход:** Tasks 2–4.

**Действие (Files):**
- Modify `src/Metrics.SdGenerator/SdGeneratorMetrics.cs` — расширить каркас Task 2 (только константы) до контракта типов. **Meter приходит через ctor-параметр**, внутри класса `new Meter(...)` НЕ создаётся: при `Enabled=true` `AddAppMetrics` уже зарегистрировал в DI Meter с тем же именем (`Shared.Metrics/MetricsModuleExtensions.cs:30` — `new Meter(serviceName)` + `services.AddSingleton(meter)`), канон репо — писать в DI-Meter (`sp.GetRequiredService<Meter>()`, `src/KafkaWorker.App/Program.cs:37–41`); два живых Meter с одним именем в процессе — не канон. `ObservableGauge<double>(LastSuccessInstrument, …)` создаётся на переданном meter из стейта `long? _lastSuccessUnix` (null — измерений нет, серия не эмитится); `MarkSuccess()` пишет `time.GetUtcNow().ToUnixTimeSeconds()`; `Dispose` — только прекращает эмиссию гейджа (флаг: колбэк после Dispose не отдаёт серии), **Meter НЕ диспозим** — он принадлежит владельцу/DI (канон `WorkerMetricsInstrumentation.cs:604–606`).
- Create `src/Metrics.SdGenerator/EtcdFailover.cs` — копия паттерна (не ProjectReference — прецедент дублей воркеров), адаптация без `EtcdWriteUnavailableException`:

```csharp
using Shared.Core;
namespace Metrics.SdGenerator;

// Failover по endpoints (паттерн EtcdFailover воркеров): первый успешный
// выигрывает; все недоступны — последняя ошибка наружу (тик = warning-лог).
internal static class EtcdFailover
{
    public static async Task<Result<T>> CallAsync<T>(string[] endpoints, Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }
        return last!;
    }
}
```

- Create `src/Metrics.SdGenerator/SdGeneratorLoop.cs` — контракт типов. `TickAsync`:
  1. `var range = await EtcdFailover.CallAsync(endpoints, ep => etcd.RangeAsync(ep, TargetMapping.PortallocPrefix, ct));`
  2. `!range.IsSuccess` → `logger.LogWarning("чтение portalloc не удалось: {Error}", …)`, `return false` — файл/метрика НЕ тронуты (консервативная свежесть);
  3. `var groups = TargetMapping.Map(range.Value, (c, e) => logger.LogWarning("пропуск portalloc {Cluster}: {Error}", c, e));`
  4. `writer.WriteIfChanged(TargetMapping.Serialize(groups))`;
  5. `metrics.MarkSuccess()` (пустой префикс = успех) → `return true`.
- Create `src/Metrics.SdGenerator/SdGeneratorHostedService.cs` — `BackgroundService`: интервал `SdGeneratorOptions.NormalizeInterval(options.Value.RefreshIntervalSec)`; при `RefreshIntervalSec <= 0` — один warning-лог на старте («интервал <=0 → 15», паттерн valkey); цикл `PeriodicTimer`, тик = `loop.TickAsync(ct)` (исключение из тика — catch + warning, хост жив; отмена — выход).
- Modify `src/Metrics.SdGenerator/Program.cs` — заменить TODO-блок на DI-регистрацию: `SdFileWriter` (из `IOptions<SdGeneratorOptions>.Value.OutputPath`), `SdGeneratorMetrics` — `sp => new SdGeneratorMetrics(sp.GetService<System.Diagnostics.Metrics.Meter>() ?? new Meter(SdGeneratorMetrics.MeterName), TimeProvider.System)` (DI-Meter при `Enabled=true`; fallback свой при `Enabled=false` — `AddAppMetrics` тогда Meter в DI не кладёт, пишем «в никуда», хост не падает), `SdGeneratorLoop`, `AddHostedService<SdGeneratorHostedService>`.

- Test `src/tests/Metrics.SdGenerator.UnitTests/SdGeneratorLoopTests.cs` — фейк `IEtcdGateway` (ручная реализация интерфейса в тестах: словарь возвращаемых `Result` по endpoint) + `SdGeneratorMetrics` со своим `new Meter("sd-unit")` (standalone-тестируемость — через ctor-параметр) и `TimeProvider.System` (пакет `Microsoft.Extensions.TimeProvider.Testing` НЕ подключён — ассертить `LastSuccessUnix` ∈ [now-5, now]). Кейсы:

| Кейс | Фейк возвращает | Ассерты |
|---|---|---|
| Успех пишет файл и метрику | 1 ключ portalloc (как Task 3) | `TickAsync` → true; файл = Serialize(групп); `LastSuccessUnix` > null |
| Пустой префикс = успех | `[]` | true; файл `[]`; метрика обновилась |
| Ошибка etcd — всё стоит | Failed на все endpoints | false; файл НЕ создан (или контент прежний); `LastSuccessUnix` прежний |
| Failover на второй endpoint | ep1 Failed, ep2 успех | true; файл написан |
| Запись только при diff | два тика подряд одинаковый range | mtime файла не изменился (снимок между тиками) |

- Test `src/tests/Metrics.SdGenerator.UnitTests/SdGeneratorMetricsTests.cs`: конструируется со своим Meter; `MarkSuccess` дважды — `LastSuccessUnix` не убывает; новый `MeterListener` не нужен (свойство — стейт; имя инструмента проверяет константа + integration/E2E против реального scrape — чек 65/E2E).

- [ ] Step 1: тесты (loop+metrics) → FAIL. Step 2: реализация трёх классов (EtcdFailover, Loop, HostedService) + расширение `SdGeneratorMetrics` + wiring Program.cs → PASS. Step 3: `dotnet build src/PgWorker.slnx -c Release` 0 warnings; smoke из Task 2 теперь отдаёт `sd_generator_last_success_timestamp_seconds` только после тика (с мёртвым etcd серии нет — корректно). Step 4: коммит `feat(t15): цикл генератора с failover, консервативной свежестью и самонаблюдением`.

**Выход:** работающий сервис целиком (code-complete генератора).

**Проверка:** unit зелёные; smoke: `SdGenerator__Etcd__Endpoints__0=http://127.0.0.1:1 SdGenerator__RefreshIntervalSec=2 SdGenerator__OutputPath=/tmp/sd-test.json dotnet …dll` → в логе warning «чтение portalloc не удалось», процесс жив, файл не создан (Ctrl-C/kill по timeout).

**Spec:** §2 (консервативная свежесть, таргеты из реплицированной истины), §3.2 (цикл/маппинг/самонаблюдение), §6.1.

---

### Task 6: Образ `docker/Metrics.SdGenerator.Dockerfile`

**Вход:** Task 5.

**Действие (Files):**
- Create `docker/Metrics.SdGenerator.Dockerfile` (канон E2E-образов — `PgWorker.Wal.E2E.Dockerfile`):

```dockerfile
# syntax=docker/dockerfile:1

# Образ SdGenerator (t15, arch/18 §5.4): dotnet publish НА ХОСТЕ (инкрементально,
# секунды), в контейнер — ТОЛЬКО publish-вывод (runtime-слой, без sdk/исходников).
# Контекст сборки — каталог артефактов (узкий; канон AGENTS.md):
#   dotnet publish src/Metrics.SdGenerator/Metrics.SdGenerator.csproj \
#     -c Release -o artifacts/sd-generator/publish
#   docker build -f docker/Metrics.SdGenerator.Dockerfile -t sdgenerator:dev \
#     artifacts/sd-generator/publish
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY . ./
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "Metrics.SdGenerator.dll"]
```

- [ ] Step 1: publish на хосте: `dotnet publish src/Metrics.SdGenerator/Metrics.SdGenerator.csproj -c Release -o artifacts/sd-generator/publish --nologo` (каталог под `.gitignore`-строкой `artifacts/`).
- [ ] Step 2: `docker build -f docker/Metrics.SdGenerator.Dockerfile -t sdgenerator:dev artifacts/sd-generator/publish` — успех.
- [ ] Step 3: smoke контейнера (мёртвый etcd, динамический порт): `docker run --rm -d --name sd-smoke -p 0:8080 -e SdGenerator__Etcd__Endpoints__0=http://127.0.0.1:1 -e SdGenerator__OutputPath=/sd/patroni-nodes.json sdgenerator:dev; P=$(docker port sd-smoke 8080/tcp | head -1 | cut -d: -f2); sleep 3; curl -fsS http://localhost:$P/metrics | head -2; docker logs sd-smoke 2>&1 | grep -c "чтение portalloc" ≥ 1; docker rm -f sd-smoke`.
- [ ] Step 4: коммит `feat(t15): runtime-образ SdGenerator (publish на хосте)`.

**Выход:** собираемый локальный образ; в registry НЕ кладётся.

**Проверка:** шаги 1–3 зелёные; `docker images sdgenerator` — тег `dev` есть; в `dev-stand/images/images.txt` НЕТ новых строк.

**Spec:** §3.5 (образ), §5 (ограничение registry).

---

### Task 7: Стенд — compose-сервис `sd-generator` + publish-шаг `00-up.sh`

**Вход:** Task 6.

**Действие (Files):**
- Modify `dev-stand/adminpanel/docker-compose.yml` — в блок мониторинга (после `prometheus`), по spec §3.3:

```yaml
  # file_sd-генератор реальных Patroni-нод (t15, arch/18 §5.4): тик читает
  # /pgworker/portalloc/ (read-only) и пишет таргеты в volume Prometheus.
  # Локально собираемый образ — в registry НЕ класть (канон AGENTS.md).
  sd-generator:
    build:
      context: ../../artifacts/sd-generator/publish
      dockerfile: ../../docker/Metrics.SdGenerator.Dockerfile
    image: sdgenerator:dev
    container_name: as-sd-generator
    restart: unless-stopped
    profiles: ["metrics"]
    volumes:
      - prometheus-sd:/sd
    environment:
      SdGenerator__Etcd__Endpoints__0: http://etcd1:2379
      SdGenerator__Etcd__Endpoints__1: http://etcd2:2379
      SdGenerator__Etcd__Endpoints__2: http://etcd3:2379
      SdGenerator__OutputPath: /sd/patroni-nodes.json
    depends_on: [etcd1, etcd2, etcd3]
```

  В сервис `prometheus` добавить маунт к существующим volumes: `- prometheus-sd:/etc/prometheus/sd:ro`; в корневые `volumes:` добавить `prometheus-sd:`.
  (Если compose не примет dockerfile за пределами context — вариант из spec §3.3: `context: ../..` + `dockerfile: docker/Metrics.SdGenerator.Dockerfile` и в Dockerfile `COPY artifacts/sd-generator/publish/ ./`; проверить `docker compose config` и выбрать рабочий, зафиксировав комментарий в YAML.)
- Modify `dev-stand/adminpanel/checks/00-up.sh` — перед `compose up` (после TLS-пакета):

```bash
# SdGenerator (t15): publish НА ХОСТЕ → compose build пакует только вывод
# (канон E2E-образов; сборка не «тихая» — [PHASE] и тайминг).
echo ">>> [PHASE] publish Metrics.SdGenerator ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/Metrics.SdGenerator/Metrics.SdGenerator.csproj" \
  -c Release -o "$ROOT/artifacts/sd-generator/publish" --nologo \
  || { echo "❌ publish SdGenerator не удался"; exit 1; }
echo ">>> [PHASE] publish SdGenerator готов ($(date +%H:%M:%S))"
```

- [ ] Step 1: Внести правки compose + 00-up.sh.
- [ ] Step 2: Валидация без подъёма: `bash -n dev-stand/adminpanel/checks/00-up.sh`; `cd dev-stand/adminpanel && docker compose --profile metrics config -q`.
- [ ] Step 3: Коммит `feat(t15): сервис sd-generator в профиле metrics + publish на хосте в 00-up.sh`.

**Выход:** полный стенд поднимает генератор; volume стыкуется с Prometheus.

**Проверка:** шаг 2 без ошибок; (живой подъём — гейт Task 12).

**Spec:** §3.3, §6.4.

---

### Task 8: `prometheus.yml` (2 джобы) + `rules.yml` (3 алерта) + чек 65

**Вход:** Task 7.

**Действие (Files):**
- Modify `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` — добавить в конец `scrape_configs`:

```yaml
  - job_name: patroni-nodes      # реальные ноды PgWorker (t15, arch/18 §2.5/§5.4)
    scheme: http                 # Patroni REST без TLS — t22 вне скоупа
    file_sd_configs:
      - files: ["/etc/prometheus/sd/patroni-nodes.json"]
        refresh_interval: 30s
  - job_name: sd-generator       # самонаблюдение генератора (arch/18 §5.4)
    static_configs: [{targets: ["sd-generator:8080"]}]
```

- Modify `dev-stand/adminpanel/metrics/prometheus/rules.yml` — в группу `pg` (после `PgReplicaLagHigh`) три правила по spec §3.4:

```yaml
      - alert: PatroniNodeDown
        expr: up{job="patroni-nodes"} == 0
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Patroni-нода {{ $labels.instance }} ({{ $labels.cluster }}/{{ $labels.shard }}/{{ $labels.node }}) недоскрейпится 5мин"
          description: "нода в rebuild — штатный сценарий (~90с), порог терпит; runbook — arch/18 §2.5"
      - alert: PatroniReplicaLagHigh
        expr: time() - patroni_xlog_replay_timestamp > 30
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "реплика {{ $labels.name }} ({{ $labels.scope }}) отстаёт >30с"
          description: "зеркало PgReplicaLagHigh на нативных сериях; серия есть только у реплик; runbook — arch/18 §2.5"
      - alert: SdGeneratorStalled
        expr: time() - sd_generator_last_success_timestamp_seconds > 300
        for: 0m
        labels: {severity: warning}
        annotations:
          summary: "file_sd-генератор не тикает >5мин"
          description: "фиксированный порог ≥3×RefreshIntervalSec(15с); runbook — arch/18 §5.4"
```

- Modify `dev-stand/adminpanel/checks/65-metrics.sh`:
  - **шаг 2 (строгий «все up», строки 64–76): исключить `job="patroni-nodes"` из jq-фильтров** — и из `up_count`, и из `total`, и из `bad` (иначе `total` останется больше и чек зависнет/упадёт):

```bash
# 2) все scrape-джобы up, КРОМЕ patroni-nodes (t15): down-таргеты этой джобы —
#    легитимное состояние (rebuild ноды ~90с — arch/18 §2.5; остановленный
#    эмулятор демо-кластера; устаревший file_sd при лежачем etcd) — её проверка
#    условная, шаг 2.1; остальные джобы (включая sd-generator) — строго все up.
up_count=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job!="patroni-nodes" and .health=="up")] | length')
total=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job!="patroni-nodes")] | length')
# цикл ожидания и ассерт bad — те же, но bad тоже с исключением:
bad=$(curl -fsS "$PROM/api/v1/targets" | jq -r '[.data.activeTargets[] | select(.labels.job!="patroni-nodes" and .health!="up")] | .[] | .labels.job+"/"+.labels.instance' | tr '\n' ' ')
```

  - **новый шаг 2.1 (после «все up»)** — file_sd валиден + условная проверка patroni-nodes (без строгого all-up: живость канала = ≥1 up; единичные down — зона алерта `PatroniNodeDown` (`for: 5m`), не чека):

```bash
# 2.1) patroni-nodes (t15): файл file_sd существует/валиден; таргеты — условно:
#      ≥1 up = канал скрейпа жив; единичные down не роняют чек (rebuild/остановка).
docker exec as-prometheus sh -c 'test -s /etc/prometheus/sd/patroni-nodes.json' \
  || { echo "  ❌ /etc/prometheus/sd/patroni-nodes.json отсутствует/пуст (жив ли as-sd-generator? docker logs as-sd-generator)"; exit 1; }
docker exec as-prometheus cat /etc/prometheus/sd/patroni-nodes.json | jq -e 'type=="array"' >/dev/null \
  || { echo "  ❌ file_sd patroni-nodes.json — не JSON-массив"; exit 1; }
pn_total=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job=="patroni-nodes")] | length')
pn_up=$(curl -fsS "$PROM/api/v1/targets" | jq '[.data.activeTargets[] | select(.labels.job=="patroni-nodes" and .health=="up")] | length')
if [ "$pn_total" -eq 0 ]; then
  echo "  patroni-nodes: кластеров PgWorker на стенде нет — file_sd пуст (корректно)"
elif [ "$pn_up" -ge 1 ]; then
  echo "  patroni-nodes: $pn_up/$pn_total up (единичные down — зона алерта PatroniNodeDown)"
else
  echo "  ❌ patroni-nodes: все $pn_total таргетов down (file_sd устарел? Patroni-REST нод живы?)"; exit 1
fi
```

  - шаг 4: порог `[ "$rules" -ge 21 ]` + текст «(18 + 3 t15: patroni-nodes)»;
  - шаг 3 (серии): добавить цикл-поиск `sd_generator_last_success_timestamp_seconds` (по образцу valkey-цикла: 30×2 с, обязательна при живом профиле metrics).

- [ ] Step 1: Правки трёх файлов.
- [ ] Step 2: Валидация конфигов прометеуса его же инструментом (образ уже локален): `docker run --rm -v "$PWD/dev-stand/adminpanel/metrics/prometheus:/cfg:ro" --entrypoint promtool prom/prometheus:v3.14.0 check config /cfg/prometheus.yml` — «SUCCESS».
- [ ] Step 3: `bash -n dev-stand/adminpanel/checks/65-metrics.sh`.
- [ ] Step 4: Коммит `feat(t15): джобы patroni-nodes/sd-generator, 3 алерта группы pg, чек 65 → 21 рул + условный patroni-nodes`.

**Выход:** конфигурация мониторинга готова; чек знает новые серии/джобы и терпит легитимные down-таргеты patroni-nodes.

**Проверка:** шаги 2–3 зелёные; `grep -c "alert:" dev-stand/adminpanel/metrics/prometheus/rules.yml` = 21; в шаге 2 чека 65 jq-фильтры up_count/total/bad содержат `select(.labels.job!="patroni-nodes"`, а условная проверка — только в шаге 2.1.

**Spec:** §3.2 (джоба sd-generator), §3.3 (джоба patroni-nodes), §3.4 (алерты), §6.4 (чек: ≥21, patroni-nodes при наличии up — условно, самоскрейп).

---

### Task 9: Дашборд `pg.json` — 4 новые панели

**Вход:** Task 8.

**Действие (Files):** Modify `dev-stand/adminpanel/metrics/grafana/dashboards/pg.json` — дополнить (существующие 3 панели не трогать) рядами ниже (gridPos y=9), datasource-стиль скопировать из соседних панелей файла:

| Панель | type | expr |
|---|---|---|
| «Patroni nodes up (real)» | stat | `up{job="patroni-nodes"}` (legend `{{cluster}}/{{shard}}/{{node}}`) |
| «Patroni role (real)» | timeseries | `patroni_master` / `patroni_replica` / `patroni_sync_standby` (legend `{{scope}}/{{name}}`) |
| «Replica replay lag, s (real)» | timeseries | `time() - patroni_xlog_replay_timestamp` (legend `{{scope}}/{{name}}`) |
| «Timeline (real)» | timeseries | `patroni_timeline` (legend `{{scope}}/{{name}}`) |

- [ ] Step 1: Внести панели (валидный JSON; id/ui IsPackable — новые id max+1..+4).
- [ ] Step 2: `jq empty dev-stand/adminpanel/metrics/grafana/dashboards/pg.json` — валидность.
- [ ] Step 3: Коммит `feat(t15): панели реальных Patroni-нод в pg.json`.

**Выход:** оператор видит реальные ноды рядом с эмуляторными панелями.

**Проверка:** jq-валидность; `python3 -c "import json;d=json.load(open('dev-stand/adminpanel/metrics/grafana/dashboards/pg.json'));print(len(d['panels']))"` → 7.

**Spec:** §3.4 (дашборд), решение 3 (эмуляторные панели не меняются).

---

### Task 10: Integration-тесты `src/tests/Metrics.SdGenerator.IntegrationTests` (живой etcd)

**Вход:** Tasks 5–6; локальный docker жив (`PGW_TEST_DOCKER`).

**Действие (Files):**
- Create `src/tests/Metrics.SdGenerator.IntegrationTests/Metrics.SdGenerator.IntegrationTests.csproj` — копия структуры PgWorker.IntegrationTests (Testcontainers, xunit.v3, FluentAssertions) + ProjectReference на генератор; `xunit.runner.json` НЕ нужен (без E2E-параллелизма, тестов мало).
- Create `EtcdFixture.cs` — копия паттерна `src/tests/PgWorker.IntegrationTests/Etcd/EtcdFixture.cs` (quay.io/coreos/etcd:v3.5.21, `WithPortBinding(2379, assignRandomHostPort: true)`, POST-ретрай готовности 30×1 с), С ОДНОЙ АДАПТАЦИЕЙ: старт отложен из `IAsyncLifetime` в явный `StartAsync` (нужен сценарий «мёртвый порт → ожил»):

```csharp
public sealed class EtcdFixture : IAsyncDisposable
{
    public EtcdFixture(int? hostPort = null) { /* ctor копии */ }
    public string Endpoint { get; private set; } = "";
    public EtcdGateway Gateway { get; }
    public Task StartAsync(CancellationToken ct);   // старт контейнера + WaitReady
    public ValueTask DisposeAsync();                // контейнер + http
}
```

- Create тесты (`SdGeneratorIntegrationTests.cs`, AAA-комментарии; общий хелпер `LoopAsync()` — собирает `SdGeneratorLoop` с реальным `EtcdGateway`, `OutputPath` в temp, `SdGeneratorMetrics` (свой `new Meter("sd-it")` — ctor-параметр), `NullLogger`):

| Тест | Сценарий | Ассерты |
|---|---|---|
| `Put_Portalloc_TargetAppears` | `put /pgworker/portalloc/c1 = {"shard1/shard1a":{"host":"h1","pg":1,"patroni":8008,"doorman":0}}` → `TickAsync` | true; файл содержит `"h1:8008"` и все три лейбла; `LastSuccessUnix` not null |
| `Delete_TargetDisappears` | put → тик → `DeleteAsync` ключа → тик | второй файл = `[]` |
| `EtcdDown_FileAndLastSuccessUntouched_ThenCatchUp` | fixture НЕ стартована (порт из `ReserveHostPort()`): тик на мёртвый порт → put невозможен; файл pre-written `X`, lastSuccess=null | тик false; файл байт-в-байт `X`; lastSuccess null; затем `fixture.StartAsync` на том же порту → put → тик → true, файл обновлён («догоняет») |
| `EmptyPrefix_EmptyArray_IsSuccess` | пустой etcd → тик | true; файл `[]`; lastSuccess not null |
| `HostedService_RefreshesWithinInterval` | `SdGeneratorHostedService` (RefreshIntervalSec=1) `StartAsync`; put; `WaitForAsync(файл содержит, 10 s)` | файл появился без ручного тика; `StopAsync` в finally |

- [ ] Step 1: csproj + fixture + тесты; `dotnet build src/PgWorker.slnx -c Release` (slnx: +integration-проект в `/tests/`).
- [ ] Step 2: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/Metrics.SdGenerator.IntegrationTests -c Release` — все зелёные; фикстуры DisposeAsync (testcontainers чистит).
- [ ] Step 3: Зачистка серии: `docker ps -a --filter name=/testcontainers/ -q | wc -l` → 0 (ryuk подобрал); осиротевших сетей нет: `docker network ls | grep -c sd-` → 0.
- [ ] Step 4: Коммит `test(t15): integration-цикл генератора на живом etcd (put/del/свежесть/догон)`.

**Выход:** поведение тика доказано на настоящем etcd.

**Проверка:** шаги 2–3 зелёные.

**Spec:** §6.2 (integration-критерии), §4 Ф3.

---

### Task 11: docker-E2E `E2ePatroniFileSd` (реальный кластер + настоящий Prometheus)

**Вход:** Task 10; Release-бинарь собирается; docker жив.

**Действие (Files):**
- Modify `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`:
  1. `public const string SdImage = "sdgenerator:e2e";` + `private static bool _sdImageReady;` + **`public static async Task EnsureSdImageAsync(CancellationToken ct)`** — точная копия `EnsureWalImageAsync` (E2eEnvironment.cs:1088) с заменами: проект `src/Metrics.SdGenerator/Metrics.SdGenerator.csproj`, вывод `artifacts/e2e/sdgenerator`, `-f docker/Metrics.SdGenerator.Dockerfile`, тег `SdImage`, флаг `_sdImageReady`, лог-файлы `/tmp/pgw-e2e-static-process-sd-e2e-{publish,build}.log`; `[PHASE]`-метки и `StaticGate` (сериализация сборок) сохранены. Вызов — **НЕ в `EnsureStaticAsync`/`StartOnceAsync`** (прецедент wal вызывается из `StartOnceAsync` внутри ветки `if (withMinio)` — E2eEnvironment.cs:424–427; общий путь заставил бы КАЖДУЮ E2E-серию, включая кейс-маркер `Scale_AddEmptyShard` гейта Task 12, платить сборкой `sdgenerator:e2e`), а **из самого сценария `E2ePatroniFileSd`** — лениво, перед стартом контейнера (метод для этого публичный; `_root` к моменту вызова уже инициализирован — сценарий зовёт после `E2eEnvironment.StartAsync`, где `EnsureStaticAsync` отработал).
  2. Публичный доступ к сети контура для контейнеров сценария:

```csharp
    /// <summary>Docker-сеть окружения — контейнерам сценариев (t15: sd-generator
    /// и тестовый Prometheus в одном контуре с etcd окружения).</summary>
    public INetwork Net => _net;
```

- Create `src/tests/PgWorker.IntegrationTests/E2e/E2ePatroniFileSdScenarios.cs` — структура сценария (канон класса: `DockerTrait.SkipIfUnavailable()`, `await using var fx = await E2eEnvironment.StartAsync("patroni-file-sd", ct: ct)`, `try/catch → fx.MarkFailed()`, копии хелперов `SeedClusterAsync`/`ProvisionedAsync`/`SetToRemoveAsync` из `E2eScaleScenarios`/`E2eScenarios`):
  1. **Контур мониторинга:** `await E2eEnvironment.EnsureSdImageAsync(ct);` (ленивая сборка образа — только этой серией); `IVolume sdVol = new VolumeBuilder().WithName($"pgw-sd-{fx.ClusterTag}").Build(); await sdVol.CreateAsync(ct);` тестовый `prometheus.yml` (в `fx.ArtifactsDir`; scrape_interval 3 s, evaluation 3 s; джоба `patroni-nodes` file_sd `/etc/prometheus/sd/patroni-nodes.json` refresh 3 s; джоба `sd-generator` static `sd-generator:8080`); контейнеры (оба `WithNetwork(fx.Net)`, `WithExtraHost("host.docker.internal", "host-gateway")`):
     - `sd-gen`: образ `E2eEnvironment.SdImage`, **`WithNetworkAliases("sd-generator")`** — DNS-имя `sd-generator` из static-таргета prometheus.yml резолвится в docker-сети только по имени/alias контейнера, а testcontainers присваивает контейнеру случайное имя (прецедент: `WithNetworkAliases("e2e-minio")` для MinIO/mc — E2eEnvironment.cs:404); без алиаса самоскрейп не соберётся и ассерт серии `sd_generator_last_success_timestamp_seconds` упадёт. Далее: volume → `/sd` (rw), env `SdGenerator__Etcd__Endpoints__0 = http://host.docker.internal:<порт etcd из fx.EtcdEndpoints>`, `SdGenerator__OutputPath=/sd/patroni-nodes.json`, `SdGenerator__RefreshIntervalSec=2`;
     - `prom`: `prom/prometheus:v3.14.0`, volume → `/etc/prometheus/sd` (ro), bind-mount конфига → `/etc/prometheus/prometheus.yml` (ro), `WithPortBinding(9090, assignRandomHostPort: true)`; старт и `GetMappedPublicPort(9090)`.
  2. **Кластер:** `SeedClusterAsync(cluster)` → `fx.StartHostAsync("s1")` → `WaitForAsync(ProvisionedAsync, 360 s)` (как `Scale_AddEmptyShard`).
  3. **Таргеты up:** `WaitForAsync`: `GET http://localhost:<promPort>/api/v1/targets` (JsonDocument) → `activeTargets` с `labels.job == "patroni-nodes"`: ≥ 4 (2 шарда × 2 реплики), все `health == "up"`, `scrapeUrl` содержит `host.docker.internal:` и фактический patroni-порт нод (порты читать из `/pgworker/portalloc/<C>` через `fx.Gateway`, не хардкод).
  4. **Канон-минимум серий (M3-факт):** `GET /api/v1/query?query=<имя>` для всех 7: `patroni_master`, `patroni_replica`, `patroni_sync_standby`, `patroni_timeline`, `patroni_xlog_replay_timestamp`, `patroni_version`, `patroni_postgres_running` — каждая `data.result` непуста; плюс `sd_generator_last_success_timestamp_seconds` непуста (самоскрейп генератора — через network-alias из п.1).
  5. **Демонтаж:** `SetToRemoveAsync(cluster)` → `WaitForAsync`: portalloc-ключ `/pgworker/portalloc/<C>` исчез (`GetAsync` → null) → таргеты `patroni-nodes` в `/api/v1/targets` = 0.
  6. **Teardown (finally, любой исход):** stop/rm `prom`, `sd-gen` (docker-логи в `fx.ArtifactsDir` ПЕРЕД удалением — канон телеметрии); `sdVol.DisposeAsync()`; ассерт чистоты: `docker volume ls` без `pgw-sd-<tag>`; окружение сносит своё (`fx` await using). `[PHASE]`-строки перед каждым долгим ожиданием (сборка образа, провижининг, таргеты, демонтаж).

- [ ] Step 1: Правки `E2eEnvironment.cs` (публичный `EnsureSdImageAsync` + сеть) — `dotnet build src/PgWorker.slnx -c Release` 0 warnings.
- [ ] Step 2: Сценарий целиком.
- [ ] Step 3: Прогон: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2ePatroniFileSd` — зелёный (E2eFixture соберёт Release сам; первый прогон дольше — сборка образа, это фиксируется `[PHASE]`).
- [ ] Step 4: Зачистка после серии: `docker ps -a --filter name=pgw- -q | wc -l` → 0; `docker volume ls -q | grep -c pgw-sd` → 0; `docker network ls | grep -c pgw-net` → 0 (или только чужие живые прогоны).
- [ ] Step 5: Коммит `test(t15): docker-E2E file_sd — кластер → таргеты up → серии → демонтаж → чисто`.

**Выход:** сквозная приёмка t15 на реальном контуре.

**Проверка:** шаг 3 зелёный; артефакты в `/tmp/pgw-e2e-artifacts-<guid>/` содержат логи обоих контейнеров.

**Spec:** §4 Ф4, §6.3, §6.5 (канонизация словаря фактом).

---

### Task 12: Мерж-гейт трека reliability (полный прогон + roadmap одним коммитом)

**Вход:** Tasks 1–11 закоммичены; юнит/интеграция/E2E локально зелёные.

**Действие (Files):** прогон всего + финальные документные правки.

- [ ] Step 1: Сборка: `dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- [ ] Step 2: Юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter 'FullyQualifiedName~UnitTests'` — зелёные. Зачистка между сериями (контроль `docker ps`/networks — канон AGENTS.md).
- [ ] Step 3: Интеграции t15: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Metrics.SdGenerator.IntegrationTests` — зелёные; зачистка серии (Task 10 Step 3).
- [ ] Step 4: E2E на свежем Release (новый сервис в контуре стенда — кейс-маркер обязателен): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` и `... --filter FullyQualifiedName~E2ePatroniFileSd` — зелёные (сборка `sdgenerator:e2e` происходит только в серии E2ePatroniFileSd — ленивый вызов); после КАЖДОЙ серии — зачистка (`docker ps -a --filter name=pgw-`, `docker network prune -f` при осиротевших `pgw-net-*`).
- [ ] Step 5: Стенд: `bash dev-stand/adminpanel/checks/00-up.sh` — полный подъём зелёный (профиль metrics поднял `as-sd-generator`); `bash dev-stand/adminpanel/checks/65-metrics.sh` — зелёный (21 рул, серия `sd_generator_last_success_timestamp_seconds` в TSDB, файл `/etc/prometheus/sd/patroni-nodes.json` валиден, строгий all-up — без patroni-nodes, шаг 2.1 — условно). Любая фаза > 60 с — собрать логи и `[PHASE]`-отчёт «почему долго».
- [ ] Step 6: Roadmap-гейт (тем же мерж-коммитом, что и код — правки сейчас, коммит на мерже):
  - `arch/roadmap/reliability.md`: удалить пункт `t15-prometheus-file-sd` (строка списка N-трека; `←`-ссылок на t15 нет — проверить `grep -n t15`).
  - `arch/roadmap/reliability-report.md`: (а) таблица «Осталось» — строку `t15-prometheus-file-sd | file_sd для скрейпа реальных нод | P3 | N` удалить; (б) таблица «Сделано в рамках трека» — добавить строку вида: `| t15-prometheus-file-sd | — (мерж-коммит t15) | независимый file_sd-канал наблюдения реальных Patroni-нод (характеристика N): сервис sd-generator (профиль metrics, read-only читатель /pgworker/portalloc/) атомарно пишет таргеты host:patroni в volume Prometheus, джоба patroni-nodes скрейпит нативные patroni_* (канон-минимум 7 серий фиксирован docker-E2E), алерты PatroniNodeDown/PatroniReplicaLagHigh/SdGeneratorStalled + панели pg.json; граница — Kafka/Valkey-ноды остаются на доменных сериях коллекторов |`; (в) сводка N: убрать «метрики реальных нод не собираются», дописать «реальные Patroni-ноды скрейпятся Prometheus по file_sd из portalloc (sd-generator, независим от панели/воркеров)»; (г) «Открытые разрывы»: убрать «прод-ноды недоскрейпимы (`t15`);», добавить «Kafka/Valkey-ноды без HTTP metrics-эндпоинта — только доменные серии коллекторов (расширение — будущие задачи)».
- [ ] Step 7: Финальный статус ветки: `git status` чист; дальше — merge-gate пользователя (мерж в main и пуш — ТОЛЬКО по явной просьбе).

**Выход:** ветка готова к мержу; roadmap/report синхронны мерж-коммиту.

**Проверка:** все шаги 1–5 зелёные; `grep -rn "t15" arch/roadmap/reliability.md` → пусто; в reliability-report.md t15 — только в «Сделано».

**Spec:** §6.4, §6.6 (мерж-гейт одним коммитом), §4 Ф5.

---

## Self-Review (выполнен при написании)

- **Покрытие спеки:** §3.1→Task 1; §3.2 (цикл/маппинг/самонаблюдение/джоба)→Tasks 2–5, 8; §3.3→Tasks 7–8; §3.4→Tasks 8–9; §3.5→Task 2/6; §6.1→Tasks 3–5; §6.2→Task 10; §6.3→Task 11; §6.4→Tasks 7–8, 12 (проверка patroni-nodes в чеке 65 — условная, без строгого all-up: down-таргеты джобы легитимны, живость канала = ≥1 up при наличии таргетов); §6.5→Tasks 11–12 (серия — E2E+чек 65); §6.6→Task 12. Ограничения §5 — в Global Constraints (код воркеров не трогаем, registry, images.txt, patroni=0, single-host extra_hosts).
- **Placeholder-скан:** TBD/TODO нет; TODO-комментарий в Program.cs Task 2 — временный артефакт каркаса, закрывается Task 5 (шаг wiring).
- **Консистентность типов:** сигнатуры Tasks 3–5 совпадают с контрактом типов (WriteIfChanged/Map/Serialize/TickAsync/MarkSuccess/NormalizeInterval); `SdGeneratorMetrics` принимает Meter через ctor (DI-канон: DI-Meter из `AddAppMetrics`, fallback свой при `Enabled=false`; Meter не диспозим — канон `WorkerMetricsInstrumentation`); класс появляется уже в Task 2 с одними константами `MeterName`/`LastSuccessInstrument` (Program.cs и его `AddAppMetrics(SdGeneratorMetrics.MeterName, …)` компилируются на гейте Task 2 — без CS0103), полную реализацию по контракту получает в Task 5; имена env `SdGenerator__*` едины в Task 2/6/7/11.
- **E2E-инфраструктура:** сборка `sdgenerator:e2e` — лениво из сценария `E2ePatroniFileSd` (публичный `EnsureSdImageAsync`), чужие серии и кейс-маркер гейта её не платят; static-таргет `sd-generator:8080` в тестовом prometheus.yml резолвится через `WithNetworkAliases("sd-generator")` (testcontainers даёт контейнеру случайное имя); чек 65 шаг 2 считает строгость только на джобах без `patroni-nodes` (up_count/total/bad — с исключением), условная проверка джобы — целиком в шаге 2.1.
