# PgWorker — план реализации (Фаза 3 dev-flow)

> **Для исполнителей:** план выполняется пошагово (subagent-driven или
> executing-plans). Шаги помечены чекбоксами (`- [ ]`). Каждый шаг
> самодостаточен: точные пути, код, команды проверки. Тесты — с
> AAA-комментариями (Arrange/Act/Assert), текст комментариев — русский.
> Ревизия 2: внесены правки по итогам ревью Фазы 4 (замечания 1–6).

**Цель:** построить по спецификации backend-сервис PgWorker (.NET 10):
оркестратор кластеров PostgreSQL через etcd + docker (plain/swarm), с
provisioning/deprovisioning, контролем нод, эвакуацией бакетов и
координацией инстансов в etcd.

**Архитектура:** цикл опроса etcd (poll, 5 с) → классификация кластеров →
пер-кластерные lease-клэймы → идемпотентные процессы-машины состояний;
управление docker — собственный тонкий клиент Engine API (plain: контейнеры
per-host, swarm: сервисы+spread); нода кластера = один контейнер
`pgworker-node` (Spilo+pg_doorman+HAProxy+supervisord). Всё значимое
состояние — в etcd (journal/фазы/portalloc).

**Стек:** .NET 10 (`net10.0`, `Nullable=enable`, `TreatWarningsAsErrors=true`),
CPM, `.slnx`; Npgsql, Polly 8, Microsoft.Extensions.*; тесты xunit.v3 +
FluentAssertions + Testcontainers. Версии — из `../AdminPanel/src/Directory.Packages.props`.

**Spec:** `docs/superpowers/2026-08-23-pgworker-backend/spec.md` (в этом же
worktree; дальше — «spec §N», решения «Д1–Д9»).

## Глобальные ограничения (действуют в каждой задаче)

- Сборка: `dotnet build src/PgWorker.slnx -c Release` — 0 warnings
  (`TreatWarningsAsErrors=true`), иначе задача не засчитана.
- Пакеты: только CPM (`src/Directory.Packages.props`), версии как в
  AdminPanel (Npgsql 10.0.3, xunit.v3 3.2.2, FluentAssertions 7.2.1,
  Testcontainers 4.14.0, Microsoft.Extensions.* 10.0.9) + Polly 8.7.0,
  Polly.Contrib.WaitAndRetry 1.1.1 (как в Puzzle).
- Язык: идентификаторы — английские; доки/комментарии/сообщения — русские.
- Тесты: AAA-комментарии (`// Arrange`, `// Act`, `// Assert`).
- Работаем только в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-pgworker`
  (ветка `feat-pgworker`); `/Users/demakaev/ZCodeProject/pg` — read-only
  источник копирования. Коммит — в конце каждой задачи.
- etcd-контракт: ключи и форматы — spec §4; мутации `/clusters/` — только
  txn с compare; мутации — только держателем клэйма.
- Копируемые источники (точные пути):
  - Result/DI/retry/health: `/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/`
  - образец worker-цикла: `/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App.Bus/Consumer/BusConsumerHostedService.cs` (соседний проект `Infrastructure.App.Bus`, не подпапка)
  - etcd-клиент/парсеры: `/Users/demakaev/ZCodeProject/AdminPanel/src/AdminPanel.Etcd/`
  - etcd-фикстуры: `/Users/demakaev/ZCodeProject/AdminPanel/src/tests/AdminPanel.UnitTests/EtcdFixtures/*.json` + загрузчик `…/EtcdFixtures.cs`
  - lease-скрипт мастер-ключа: `/Users/demakaev/ZCodeProject/pg/arch/stand/sidecar/rolecheck.py`
  - эталоны конфигов: `/Users/demakaev/ZCodeProject/pg/arch/configs/{postgres/pg.env,haproxy/haproxy.cfg}`

## Структура файлов (карта решения)

```
worktree/
├── src/
│   ├── PgWorker.slnx
│   ├── Directory.Build.props            # общие свойства сборки
│   ├── Directory.Packages.props         # CPM
│   ├── NuGet.Config                     # копия AdminPanel
│   ├── PgWorker.Core/                   # модель, планировщики, шаблоны, Result, DI, Retry
│   ├── PgWorker.Etcd/                   # gateway-клиент, парсеры, ClaimStore, WorkJournal
│   ├── PgWorker.Docker/                 # Engine API клиент, драйверы plain/swarm
│   ├── PgWorker.Provisioning/           # процессы, SQL/Patroni-слои, SnapshotJob
│   ├── PgWorker.App/                    # host, циклы, health, appsettings
│   └── tests/
│       ├── PgWorker.UnitTests/          # Core/Etcd/Docker/Provisioning unit
│       └── PgWorker.IntegrationTests/   # etcd (Testcontainers), docker-trait, e2e
├── docker/
│   ├── PgWorker.Dockerfile              # образ сервиса
│   └── node/Dockerfile                  # образ pgworker-node
├── deploy/docker-compose.yml            # запуск PgWorker
├── dev-stand/                           # compose (etcd) + сид-скрипт
└── arch/…                               # deliverables §13 (задачи 1–3)
```

---

## Блок A — arch/-правки (spec §13, до кода)

### Задача 1: Дока `arch/14-pgworker.md`

**Вход:** spec утверждён; arch/ в worktree соответствует основному checkout.
**Действие:** создать `arch/14-pgworker.md` — перенести из spec содержание в
формат доков arch (как 11-bucket-sharding.md): роль и границы, контракт etcd
(§4 spec: таблицы читаемых/пишемых ключей, новые `/pgworker/*`, финальное
состояние кластера после provisioning — Д1), модель развертывания (§5:
образ узлы, plain/swarm, placement/порты), процессы (§6.4: таблица
`nodes/<n>/state`, фазы P0–P5/D0–D3/E0–E4), надёжность (§7), связь с панелью
(AdminPanel 02 §9) и скриптами. Заголовок в стиле соседних док; в конце —
«Дальше» со ссылкой на README.
**Выход:** канонический документ PgWorker, на который ссылается код.
**Проверка:** `grep -c "pgworker" arch/14-pgworker.md` > 0; все ключи из
spec §4 присутствуют: `grep -o "/pgworker/[a-z]*" arch/14-pgworker.md | sort -u`
выдаёт leader/claims/work/evacuations/portalloc/instances.
**Spec:** §13.1, §4, §5, §6.4 (Д1–Д6).

- [x] Шаг 1: написать `arch/14-pgworker.md` (структура выше; таблицы значений
  `nodes/<n>/state` и форматы JSON `/pgworker/*` — копия таблиц spec §4.3/§6.4).
- [x] Шаг 2: самопроверка консистентности с spec: для каждого ключа из
  `grep -oE "/(clusters|service|pgworker)/[a-zA-Z/<>*-]*" docs/superpowers/2026-08-23-pgworker-backend/spec.md | sort -u`
  убедиться, что ключ упомянут в 14-й доке или осознанно исключён.
- [x] Шаг 3: `git add arch/14-pgworker.md && git commit -m "arch: 14-pgworker — дока PgWorker (контракт etcd, процессы, развертывание)"`.

### Задача 2: Правки `arch/11-bucket-sharding.md` и `arch/README.md`

**Вход:** задача 1 слита (14-я дока существует).
**Действие:**
- `arch/11-bucket-sharding.md` §2 (раздел «Ключи одного кластера», после
  абзаца о fail-open): абзац-указатель: координация воркеров оркестратора
  PgWorker — в отдельном префиксе `/pgworker/` (вне читаемого панелью
  снапшота), контракт — в [14](14-pgworker.md).
- `arch/11-bucket-sharding.md` §4.5, перед «### init-cluster.sh»: абзац
  «Декларативный provisioning»: панель AdminPanel заявляет кластер
  (`state=NOT_INITIALIZED`, структура §9.2 её контракта) — PgWorker
  (14-я дока) поднимает ноды/схемы и переводит в рабочее состояние;
  скрипты остаются ручным путём для уже поднятых кластеров.
- `arch/README.md`: добавить в индекс строку `14-pgworker.md` рядом с 13-й.
**Выход:** arch/ консистентен с новым сервисом.
**Проверка:** `grep -n "14-pgworker" arch/11-bucket-sharding.md arch/README.md`
— ссылки есть в обоих.
**Spec:** §13.2, §13.3.

- [x] Шаг 1: внести правки в 11-ю доку (два абзаца выше — текст готов).
- [x] Шаг 2: правка `arch/README.md` (индекс).
- [x] Шаг 3: `git add arch/11-bucket-sharding.md arch/README.md && git commit -m "arch: указатели на PgWorker (11 §2/§4.5, README)"`.

### Задача 3: Создание `arch/roadmap/`

**Вход:** задачи 1–2 слиты.
**Действие:** создать `arch/roadmap/README.md` — правила ведения (теги
`tNN-slug`, `←`-зависимости, мерж-гейт: слито в main → тег удаляется тем же
коммитом) — по образцу AdminPanel `arch/roadmap/README.md`; создать
`arch/roadmap/pgworker.md` с треками/задачами из spec §2 (out of scope):
  - `t01-move-bucket-csharp` — полный C#-порт move-bucket (P1–P8) ← зависит от базовых процессов (уже в main);
  - `t02-per-cluster-secrets` — генерация/ротация секретов per-cluster (Д7);
  - `t03-docker-tls-ssh` — TLS к Docker API / SSH-туннели;
  - `t04-metrics` — Prometheus-метрики;
  - `t05-quarantine-merge` — слияние/восстановление данных карантинного шарда;
  - `t06-shard-autoscaling` — add/remove-shard из панели с оркестрацией PgWorker.
**Выход:** отложенные задачи зафиксированы, roadmap-канон создан.
**Проверка:** `ls arch/roadmap/` → README.md + хотя бы один трек-файл;
`grep -c "t0" arch/roadmap/pgworker.md` ≥ 6.
**Spec:** §2 (out of scope), §13.4.

- [ ] Шаг 1: `arch/roadmap/README.md` + `arch/roadmap/pgworker.md` (список выше).
- [ ] Шаг 2: `git add arch/roadmap && git commit -m "arch: roadmap — отложенные задачи PgWorker (t01–t06)"`.

---

## Блок B — каркас решения

### Задача 4: Решение, проекты, общие свойства

**Вход:** arch-правки слиты; dotnet SDK 10 доступен (`dotnet --version`).
**Действие:**
- `src/Directory.Build.props` — копия `/Users/demakaev/ZCodeProject/AdminPanel/src/Directory.Build.props` (net10.0, latest, nullable, warnings-as-errors, IsPackable=false).
- `src/Directory.Packages.props` — копия AdminPanel + строки:
  ```xml
  <PackageVersion Include="Npgsql" Version="10.0.3" />
  <PackageVersion Include="Polly" Version="8.7.0" />
  <PackageVersion Include="Polly.Contrib.WaitAndRetry" Version="1.1.1" />
  ```
  (остальные — Microsoft.Extensions.*, xunit.v3 3.2.2, FluentAssertions 7.2.1,
  Testcontainers 4.14.0 — как в AdminPanel; etcd-контейнер — plain
  `Testcontainers` + образ `quay.io/coreos/etcd:v3.5.21`).
- `src/NuGet.Config` — копия AdminPanel.
- `.editorconfig` (корень worktree; в референсах нет — решение фазы plan):
  ```ini
  root = true
  [*.cs]
  indent_style = space
  indent_size = 4
  charset = utf-8
  end_of_line = lf
  insert_final_newline = true
  ```
- 5 пустых проектов + 2 тестовых (Class Library / xunit):
  `src/PgWorker.Core`, `src/PgWorker.Etcd` (ref Core), `src/PgWorker.Docker`
  (ref Core), `src/PgWorker.Provisioning` (ref Core, Etcd, Docker),
  `src/PgWorker.App` (ref все; exe, `Program.cs` с пустым host-builder),
  `src/tests/PgWorker.UnitTests` (ref Core, Etcd, Docker, Provisioning),
  `src/tests/PgWorker.IntegrationTests` (ref все).
- `src/PgWorker.slnx` — по образцу AdminPanel (папки /common/,
  /core/, /etcd/, /docker/, /provisioning/, /app/, /tests/).
**Выход:** решение собирается; скелет на месте.
**Проверка:** `dotnet build src/PgWorker.slnx -c Release` → 0 warnings, 0 errors.
**Spec:** §6.1.

- [ ] Шаг 1: `dotnet new classlib -n PgWorker.Core -o src/PgWorker.Core -f net10.0` и аналогично остальные (App — `dotnet new worker -n PgWorker.App` как exe-заготовка; тесты — `dotnet new xunit3`).
- [ ] Шаг 2: прописать ProjectReference по карте выше; пакеты в csproj — только `ProjectReference` + `<PackageReference />` без версий (CPM).
- [ ] Шаг 3: файлы props/NuGet.Config/.editorconfig/slnx (содержимое выше).
- [ ] Шаг 4: `dotnet build src/PgWorker.slnx -c Release` — зелёная сборка.
- [ ] Шаг 5: `git add -A src .editorconfig && git commit -m "feat: каркас решения PgWorker (slnx, CPM, проекты)"`.

---

## Блок C — PgWorker.Core (модель и планировщики)

### Задача 5: Result, DI, Retry — каркас из Puzzle + доменная модель

**Вход:** задача 4 собрана.
**Действие:**
- Скопировать из `/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/`:
  - `Result.cs` → `src/PgWorker.Core/Result.cs` (namespace `PgWorker.Core`);
  - папку `DI/` (7 файлов: InjectAs, Config, DiTypeBehaviour,
    AutoRegistration*, ServiceCollectionExtensions, ServiceProviderExtensions)
    → `src/PgWorker.Core/DI/` (namespace `PgWorker.Core.DI`);
  - `Retry/IRetryConfig.cs` + `Retry/RetryPolicies.cs` →
    `src/PgWorker.Core/Retry/` — адаптация под Polly 8 (решение фазы plan:
    v8 API вместо v7 `Policy.Handle`):
    ```csharp
    namespace PgWorker.Core.Retry;

    public interface IRetryConfig { int RetryCount { get; } int FirstRetryDelayInSec { get; } }

    public static class RetryPolicies
    {
        // Джиттер-ретрай (Polly 8): DecorrelatedJitterBackoffV2, как в Puzzle.
        public static ResiliencePipeline<HttpResponseMessage> HttpRetry(
            int retryCount, TimeSpan medianFirstRetryDelay) =>
            new ResiliencePipelineBuilder<HttpResponseMessage>()
               .AddRetry(new RetryStrategyOptions<HttpResponseMessage>
               {
                   MaxRetryAttempts = retryCount,
                   BackoffType = DelayBackoffType.Exponential,
                   UseJitter = true,
                   Delay = medianFirstRetryDelay,
                   ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                       .Handle<HttpRequestException>()
                       .Handle<TaskCanceledException>(),
               }).Build();

        // Аналог для Npgsql-ошибок (Transient-классы 57xxx/53xxx/connection).
        public static ResiliencePipeline SqlRetry(int retryCount, TimeSpan medianFirstRetryDelay) =>
            new ResiliencePipelineBuilder()
               .AddRetry(new RetryStrategyOptions
               {
                   MaxRetryAttempts = retryCount,
                   UseJitter = true,
                   Delay = medianFirstRetryDelay,
                   ShouldHandle = new PredicateBuilder()
                       .Handle<NpgsqlException>()
                       .Handle<TaskCanceledException>(),
               }).Build();
    }
    ```
- Доменная модель `src/PgWorker.Core/Model/`:
  ```csharp
  namespace PgWorker.Core.Model;

  public enum ClusterState { Active, NotInitialized, ToRemove }      // config.state: отсутствует=Active
  public enum NodeState { NotInitialized, Provisioning, Running, Rebuilding, Unreachable, Quarantined, Removing }
  public enum BucketMoveState { NotInitialized, Syncing, Frozen, Aborting }

  public sealed record ClusterConfig(string Cluster, int Buckets, string DbName,
      long? CreatedUnix, ClusterState State);

  public sealed record NodeSpec(string Shard, string Name, NodeState State);        // "shard1","shard1a"
  public sealed record ShardSpec(string Name, int Replicas, string? Dsn, string? Master,
      IReadOnlyList<NodeSpec> Nodes);

  public sealed record BucketRoute(int Id, string? Owner, BucketMoveState? Status); // Status=null → ACTIVE

  public sealed record ClusterSnapshot(ClusterConfig Config, IReadOnlyList<ShardSpec> Shards,
      IReadOnlyList<BucketRoute> Routing);                                           // все N бакетов

  public sealed record NodePorts(int Pg, int Patroni, int Doorman);
  public sealed record NodeAddress(string Host, NodePorts Ports);                    // host + выделенные порты
  public sealed record EtcdEndpoints(IReadOnlyList<string> Http);                    // http://host:2379 — для lease-скрипта ноды
  ```
**Выход:** общий каркас + модель, на которые ссылаются все слои.
**Проверка:** сборка зелёная; unit-тест на Result не нужен (копия
проверенного), но добавь smoke: `src/tests/PgWorker.UnitTests/ResultTests.cs`
с 2 AAA-тестами (`From` при успехе/ошибке) — прогон
`dotnet test src/PgWorker.slnx --filter ResultTests`.
**Spec:** §6.1 (Core), Д9.

- [ ] Шаг 1: копии файлов + namespace-замены (`sed` или вручную; в Result.cs
  заменить `PuzzleServer.Infrastructure.App` → `PgWorker.Core`; DI → `PgWorker.Core.DI`).
- [ ] Шаг 2: `RetryPolicies` под Polly 8 (код выше).
- [ ] Шаг 3: модель (`Model/Domain.cs` одним файлом — записи выше).
- [ ] Шаг 4: `ResultTests.cs` (AAA), `dotnet test` — зелёный.
- [ ] Шаг 5: `git add -A && git commit -m "feat(core): Result/DI/Retry из Puzzle + доменная модель"`.

### Задача 6: PlacementPlanner (анти-аффинити)

**Вход:** задача 5.
**Действие:** `src/PgWorker.Core/Planning/PlacementPlanner.cs`:
```csharp
namespace PgWorker.Core.Planning;

public sealed record HostInfo(string Name, int UsedSlots);   // занятость (ноды всех кластеров)
public sealed record NodePlacement(string Shard, string Node, string Host);
public sealed record PlacementPlan(IReadOnlyList<NodePlacement> Nodes);

public static class PlacementPlanner
{
    // Анти-аффинити (spec §6.3): ноды одного шарда — на разных хостах, если
    // hosts.Count >= replicas; иначе — равномерно least-loaded.
    // Детерминизм: сортировка хостов по (UsedSlots, Name), шард/ноды — по имени.
    public static PlacementPlan Plan(IReadOnlyList<Model.ShardSpec> shards, IReadOnlyList<HostInfo> hosts);
}
```
Алгоритм: для каждого шарда (по имени) — для каждой ноды (по имени):
кандидаты = хосты, ещё не занятые этим шардом в текущем плане, отсортированные
по (UsedSlots+план, Name); если все заняты шардом — наименее загруженный.
Тесты (`tests`: `UnitTests/Planning/PlacementPlannerTests.cs`):
- 3 хоста, шард replicas=3 → все ноды на разных хостах;
- 1 хост, replicas=2 → обе ноды на нём (равномерность невозможна);
- 2 хоста, replicas=3 → распределение 2+1, минимум повторов;
- UsedSlots учитывается: перегруженный хост не выбирается первым;
- детерминизм: одинаковый вход → одинаковый выход.
**Выход:** план размещения кластера.
**Проверка:** `dotnet test src/PgWorker.slnx --filter PlacementPlanner`.
**Spec:** §6.3, Д5 (анти-аффинити «если топология позволяет»).

- [ ] Шаг 1: тест-файл с 5 AAA-тестами выше → запуск → красный (типа нет).
- [ ] Шаг 2: реализация `PlacementPlanner.Plan` → зелёный.
- [ ] Шаг 3: `git add -A && git commit -m "feat(core): PlacementPlanner — анти-аффинити нод шарда"`.

### Задача 7: PortAllocator

**Вход:** задача 5.
**Действие:** `src/PgWorker.Core/Planning/PortAllocator.cs`:
```csharp
namespace PgWorker.Core.Planning;

public static class PortAllocator
{
    // Тройка портов ноды: pg=base, patroni=base+3000, doorman=base+1500 (spec §6.3).
    // existing — закреплённые за нодами адреса (/pgworker/portalloc/<C>), busy —
    // фактическая занятость (host,port) по данным docker.
    // Закреплённое и свободное — переиспользуем; конфликт/нет свободного базового
    // в [rangeFrom,rangeTo) — следующий base; всё занято → Result.Failed.
    public static Core.Result<IReadOnlyDictionary<string, NodeAddress>> Allocate(
        PlacementPlan plan,
        IReadOnlyDictionary<string, NodeAddress> existing,
        IReadOnlySet<(string Host, int Port)> busy,
        int rangeFrom, int rangeTo);
}
```
Тесты (`UnitTests/Planning/PortAllocatorTests.cs`): закреплённое
переиспользуется; новое = первый свободный base с шагом 1 (все 3 порта
свободны на хосте ноды); конфликт busy → сдвиг; исчерпание диапазона →
`Result.Failed`; offsets корректны (base/ base+1500/ base+3000).
**Выход:** закрепляемая адресация нод (DSN/конфиги/портпаблиши).
**Проверка:** `dotnet test src/PgWorker.slnx --filter PortAllocator`.
**Spec:** §6.3, Д5.

- [ ] Шаг 1: AAA-тесты (5 кейсов выше) → красный.
- [ ] Шаг 2: реализация → зелёный.
- [ ] Шаг 3: commit `"feat(core): PortAllocator — выделение и закрепление портов нод"`.

### Задача 8: EvacuationPlanner

**Вход:** задача 5.
**Действие:** `src/PgWorker.Core/Planning/EvacuationPlanner.cs`:
```csharp
namespace PgWorker.Core.Planning;

public sealed record EvacuationAssignment(int BucketId, string FromShard, string ToShard);

public static class EvacuationPlanner
{
    // Аварийная эвакуация (spec §6.4 D): живые шарды получают бакеты умершего
    // сбалансированно (round-robin по возрастанию id); guard'ы:
    //  - бакет со статусом SYNCING/FROZEN/ABORTING → Result.Failed (незавершённый переезд);
    //  - живых шардов нет → Result.Failed;
    //  - бакет без owner → пропускается (дыра карты — не наша забота здесь).
    public static Core.Result<IReadOnlyList<EvacuationAssignment>> Plan(
        IReadOnlyList<Model.BucketRoute> routing, string deadShard,
        IReadOnlyList<string> aliveShards);
}
```
Тесты: раскладка поровну round-robin; блокировка при Frozen-статусе; отказ
без живых; дыра (owner=null) пропущена.
**Выход:** план эвакуации для BucketEvacuator.
**Проверка:** `dotnet test src/PgWorker.slnx --filter EvacuationPlanner`.
**Spec:** §6.4 D, Д6.

- [ ] Шаг 1: AAA-тесты → красный.
- [ ] Шаг 2: реализация → зелёный.
- [ ] Шаг 3: commit `"feat(core): EvacuationPlanner — план аварийной эвакуации"`.

### Задача 9: Генераторы конфигов ноды (Spilo env, doorman, haproxy)

**Вход:** задача 5 (модель), эталоны `arch/configs/postgres/pg.env`
(SPILO_CONFIGURATION), `arch/configs/haproxy/haproxy.cfg`,
`arch/stand/sidecar/rolecheck.py`.
**Действие:** `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`:
```csharp
namespace PgWorker.Core.Templates;

public sealed record ShardTopology(string Cluster, string Shard, string Scope,
    IReadOnlyDictionary<string, NodeAddress> Nodes);   // scope = "<C>-<X>"
public sealed record InstallSecrets(string SuPassword, string StandbyPassword,
    string AppPassword, string BucketAdminPassword, string MoverPassword); // Д7: env PgWorker

public static class SpiloEnvBuilder
{
    // ENV контейнера pgworker-node. SPILO_CONFIGURATION — YAML-строка с эталонными
    // значениями (P11 ttl=5/loop_wait=2/retry_timeout=3; P3 wal_level=logical,
    // sync_replication_slots on репликах, max_slot_wal_keep_size; P15
    // max_connections=60, max_wal_senders=max_replication_slots=10) — за основу
    // взять SPILO_CONFIGURATION из arch/configs/postgres/pg.env, поправив
    // wal_level: logical. Callback: on_role_change → lease-скрипт мастер-ключа
    // (ключ /clusters/<C>/shards/<X>/master, P11).
    public static IReadOnlyDictionary<string, string> Build(
        ShardTopology topology, EtcdEndpoints etcd, InstallSecrets secrets);
}

public static class DoormanConfigBuilder
{
    // Единственный пул <dbname> → 127.0.0.1:5432, pool_mode=transaction,
    // max_db_connections=55, TLS sslmode=require (P13/P14/P15/P17).
    public static string Build(string dbname);
}

public static class HaproxyConfigBuilder
{
    // По эталону arch/configs/haproxy/haproxy.cfg, только write-фронтенд :5432:
    // backend-серверы = все ноды шарда: server <node> <host>:<pgPort> check
    // port <patroniPort>, httpchk GET /primary (P2). Read-фронтенд и stats — не нужны.
    public static string Build(ShardTopology topology);
}
```
`EtcdEndpoints` и `NodeAddress` — из `PgWorker.Core.Model` (задача 5).
Тесты (`UnitTests/Templates/NodeConfigBuildersTests.cs`): в SPILO-строке
присутствуют `ttl: 5`, `loop_wait: 2`, `wal_level: logical`,
`max_connections: "60"`; env содержит `SCOPE=<C>-<X>` и `ETCD_HOSTS`;
doorman: `pool_mode = "transaction"`, `max_db_connections = 55`, dbname-пул;
haproxy: строки `server shard1a … check port <patroniPort>` для всех нод +
`option httpchk GET /primary`; секреты попадают в env, но НИКОГДА не в
doorman/haproxy-конфиги.
**Выход:** детерминированные конфиги контейнера ноды.
**Проверка:** `dotnet test src/PgWorker.slnx --filter NodeConfigBuilders`.
**Spec:** §5.1, §6.1 (Core/шаблоны), P2/P3/P11/P13/P14/P15/P17, Д4.

- [ ] Шаг 1: AAA-тесты → красный.
- [ ] Шаг 2: три билдера (YAML/конфиг — интерполяция строк по эталонам) → зелёный.
- [ ] Шаг 3: commit `"feat(core): генераторы конфигов ноды (Spilo env, doorman, haproxy)"`.

---

## Блок D — PgWorker.Etcd

### Задача 10: Gateway-клиент (адаптация AdminPanel + txn/lease/snapshot)

**Вход:** задачи 4–5.
**Действие:** скопировать из
`/Users/demakaev/ZCodeProject/AdminPanel/src/AdminPanel.Etcd/Client/`:
`Kv.cs`, `IEtcdGateway.cs`, `EtcdGateway.cs` (namespace `PgWorker.Etcd.Client`;
модель `EtcdStatusPayload`/`EtcdMember`/`EtcdAlarm` — не нужны, вырезать).
Расширить контракт (spec §4):
```csharp
namespace PgWorker.Etcd.Client;

public interface IEtcdGateway
{
    Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct);
    Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct);
    Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct);
    Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct);
    Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct);

    Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct);
    Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct);
    Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct); // один цикл

    Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct);        // P12
}

public sealed record TxnRequest(IReadOnlyList<TxnCompare> Compare,
    IReadOnlyList<TxnOp> Success, IReadOnlyList<TxnOp> Failure);
public abstract record TxnOp { public sealed record Put(string Key, string Value, long? Lease) : TxnOp;
    public sealed record Delete(string Key, bool Prefix) : TxnOp; }
public sealed record TxnCompare(string Key, TxnTarget Target, TxnPredicate Pred, string Arg, long Num)
// Target: Version|Value|ModRevision; Pred: Equal|Greater; для Value — Arg (строка),
// для Version/ModRevision — Num.
public sealed record TxnResult(bool Succeeded);
```
Реализация `EtcdGateway` — HttpClient + base64, как AdminPanel (метод
`RangeAsync` и каркас берём оттуда; новый `/v3/kv/txn` c
`requestPut`/`requestDeleteRange`, `/v3/lease/*`, `/v3/snapshot/save` —
бинарный ответ `response.Content.ReadAsByteArrayAsync`).
Тесты (`UnitTests/Etcd/EtcdGatewayTests.cs`, мок `HttpMessageHandler`):
- range-запрос кодирует prefix+range_end base64;
- txn-тело содержит compare VERSION=0 и requestPut с lease;
- lease-grant парсит `{"ID":"123"}` (строковое число!);
- snapshot читает байты; 500 → Result.Failed.
**Выход:** полный примитивный API etcd для всех верхних слоёв.
**Проверка:** `dotnet test src/PgWorker.slnx --filter EtcdGateway`.
**Spec:** §4 (транспорт/txn/lease), Д8.

- [ ] Шаг 1: копия 3 файлов клиента + вырезка лишнего + namespace.
- [ ] Шаг 2: AAA-тесты на мок-хендлере (4 выше) → красный.
- [ ] Шаг 3: расширение интерфейса и реализации (txn/lease/snapshot) → зелёный.
- [ ] Шаг 4: commit `"feat(etcd): gateway-клиент с txn-compare/lease/snapshot"`.

### Задача 11: ClusterSnapshotParser

**Вход:** задачи 5, 10. Источники фикстур (проверено, фактические пути):
JSON-файлы `/Users/demakaev/ZCodeProject/AdminPanel/src/tests/AdminPanel.UnitTests/EtcdFixtures/*.json`
(`clusters-full.json`, `service-full.json`, `clusters-degenerate.json`,
`service-unmatched.json`, `stand-nodes.json`) + C#-загрузчик
`…/AdminPanel.UnitTests/EtcdFixtures.cs` (грузит `{"key","value","modRevision"}[]`
в `IReadOnlyList<Kv>` из выходного каталога).
**Действие:** `src/PgWorker.Etcd/Parsing/ClusterSnapshotParser.cs` (за основу —
AdminPanel `Parsing/ClustersParser.cs` + `ServiceParser.cs`, модель наша):
```csharp
namespace PgWorker.Etcd.Parsing;

public static class ClusterSnapshotParser
{
    // kvs префикса /clusters/ → кластеры (config+shards+nodes+routing+status).
    // Толерантность: битый JSON ключа → ключ пропущен (ParseError-запись в лог-список
    // out-параметра), неизвестные ключи игнорируются; state="NOT_INITIALIZED"/"TO_REMOVE",
    // отсутствие поля → Active.
    public static Result<IReadOnlyList<Model.ClusterSnapshot>> ParseClusters(
        IReadOnlyList<Kv> kvs, out IReadOnlyList<string> parseErrors);

    // kvs префикса /service/ → scope-функция: initialized? leaderName?
    public sealed record HaScopeState(string Scope, bool Initialized, string? LeaderName);
    public static IReadOnlyList<HaScopeState> ParseService(IReadOnlyList<Kv> kvs);
}
```
Тесты: полный кластер из фикстур панели (config NOT_INITIALIZED, 2 шарда,
nodes, routing+status NOT_INITIALIZED); config без state → Active; битый
JSON → в parseErrors, остальное живо; routing без status → Status=null;
`/pgworker/…` ключи в kvs не ломают парсинг.
**Выход:** снапшот кластеров для цикла и процессов.
**Проверка:** `dotnet test src/PgWorker.slnx --filter ClusterSnapshotParser`.
**Spec:** §4.1, Д1.

- [ ] Шаг 1: перенести фикстуры: скопировать JSON-файлы AdminPanel (список
  выше) в `src/tests/PgWorker.UnitTests/EtcdFixtures/` (csproj —
  `None`+`CopyToOutputDirectory` для каталога); скопировать `EtcdFixtures.cs`
  в `src/tests/PgWorker.UnitTests/` и адаптировать (namespace
  `PgWorker.UnitTests`, наш `PgWorker.Etcd.Client.Kv`).
- [ ] Шаг 2: AAA-тесты (5 кейсов выше) на загруженных фикстурах → красный.
- [ ] Шаг 3: парсеры → зелёный.
- [ ] Шаг 4: commit `"feat(etcd): парсер /clusters/ и /service/ в доменную модель"`.

### Задача 12: ClaimStore (клэймы/лидерство) + WorkJournal

**Вход:** задачи 5, 10.
**Действие:** `src/PgWorker.Etcd/Coordination/ClaimStore.cs`:
```csharp
namespace PgWorker.Etcd.Coordination;

public sealed class ClaimStore(string[] endpoints, IEtcdGateway gateway, TimeProvider clock) : IAsyncDisposable
{
    public string InstanceId { get; } = Guid.NewGuid().ToString("N")[..12];

    // Пер-кластерный клэйм (spec §4.3): txn compare version(/pgworker/claims/<C>)==0
    // → put lease TTL 15с {"instance","since_unix"}; false → занят.
    public Task<Result<bool>> TryClaimClusterAsync(string cluster, CancellationToken ct);

    // Глобальный лидер: /pgworker/leader, тот же примитив.
    public Task<Result<bool>> TryBecomeLeaderAsync(CancellationToken ct);

    public bool IsMine(string cluster);                  // + живой lease
    public bool IsLeader { get; }
    public Task ReleaseClusterAsync(string cluster, CancellationToken ct); // del + revoke

    // Стартует фоновый keepalive-цикл (тик 5с): все мои lease + instance-ключ
    // /pgworker/instances/<id> (lease). При провале keepalive — клэйм считается
    // потерянным (следующий тик пере-захватывает), лидерство аналогично.
    public Task StartAsync(CancellationToken ct);
    public ValueTask DisposeAsync();                      // revoke всех lease
}
```
Плюс `src/PgWorker.Etcd/Coordination/WorkJournal.cs` — обёртка над ключами
`/pgworker/work/<C>` и `/pgworker/evacuations/<C>/<X>` (spec §4.3; решение
фазы plan №8 — WorkJournal в слое Etcd рядом с ClaimStore: чистая
etcd-обёртка, доступна integration-тестам блока D):
```csharp
namespace PgWorker.Etcd.Coordination;

public sealed record WorkState(string Op, string Phase, string Instance, long UpdatedUnix, string? LastError);
public sealed record EvacuationJournal(IReadOnlyDictionary<int, string> Buckets, // bucketId → новый владелец
    string Reason, long EvacuatedUnix, string State, long? ReturnedUnix);

public sealed class WorkJournal(IEtcdGateway gateway, string[] endpoints)
{
    // /pgworker/work/<C>: camelCase-JSON {"op","phase","instance","updated_unix","last_error"}.
    public Task<Result> WritePhaseAsync(string cluster, string op, string phase,
        string instance, string? lastError, CancellationToken ct);
    public Task<Result<WorkState?>> ReadAsync(string cluster, CancellationToken ct);
    // /pgworker/evacuations/<C>/<X> — журнал эвакуации (spec §4.3).
    public Task<Result> WriteEvacuationAsync(string cluster, string shard, EvacuationJournal j, CancellationToken ct);
    public Task<Result<EvacuationJournal?>> ReadEvacuationAsync(string cluster, string shard, CancellationToken ct);
}
```
Тесты (`UnitTests/Etcd/CoordinationTests.cs`, мок `IEtcdGateway`):
- ClaimStore: захват вызывает txn с compare version==0; занятость
  (Succeeded=false) → false; keepalive-тик продлевает все live-lease;
  Release удаляет ключ; потеря keepalive → IsMine=false;
- WorkJournal (round-trip на моке): WritePhaseAsync шлёт put с
  camelCase-ключом `/pgworker/work/shop`; ReadAsync десериализует
  op/phase/instance/last_error без потерь.
**Выход:** координационный слой Etcd (клэймы + журнал процессов).
**Проверка:** `dotnet test src/PgWorker.slnx --filter "ClaimStore|WorkJournal"`.
**Spec:** §4.3, §6.2, Д2.

- [ ] Шаг 1: AAA-тесты ClaimStore (5 кейсов) + WorkJournal (2 кейса) → красный.
- [ ] Шаг 2: реализация обоих классов → зелёный.
- [ ] Шаг 3: commit `"feat(etcd): ClaimStore + WorkJournal — координация и журнал /pgworker/*"`.

### Задача 13: Integration: etcd (Testcontainers) — клэймы, txn, контракт форматов §4.2/§4.3

**Вход:** задачи 10–12; docker доступен на машине CI/исполнения.
**Действие:** `src/tests/PgWorker.IntegrationTests/Etcd/EtcdCoordinationTests.cs`
(fixture: `quay.io/coreos/etcd:v3.5.21`, порт 2379):
- AAA: два ClaimStore → взаимное исключение (первый true, второй false);
- AAA: истечение клэйма: grant TTL 2с, Dispose первого, sleep 3с → второй захватывает;
- AAA: txn-compare VALUE: конкурентный flip routing отклоняется при чужом значении;
- AAA: lease-put истекает (ключ исчезает после TTL);
- AAA: snapshot save возвращает непустой массив байтов.

`src/tests/PgWorker.IntegrationTests/Etcd/EtcdContractTests.cs` — повторение
утверждений spec §4.2/§4.3 (критерий приёмки §11.8) на реальном etcd:
- AAA: сид кластера в стиле панели (02 §9.1: config NOT_INITIALIZED,
  shards/replicas, nodes, routing/status NOT_INITIALIZED, request_*) →
  `ClusterSnapshotParser` выдаёт корректный снапшот;
- AAA: WorkJournal round-trip против реального etcd:
  `WritePhaseAsync("shop", "provision", "planned", "inst-1", null)` →
  `ReadAsync("shop")` возвращает WorkState с теми же op/phase/instance;
- AAA: portalloc round-trip: `PutAsync("/pgworker/portalloc/shop", json)` с
  JSON `{"shard1/shard1a":{"host":"h1","pg":15432,"patroni":18008,"doorman":16432}}`
  → `GetAsync` → десериализация в `Dictionary<string, NodeAddress>` даёт
  исходные host/порты — структура переживает запись/перечитывание.
**Выход:** доказанная работа координации и всех трёх форматов `/pgworker/*`
(клэймы, work-journal, portalloc) на реальном etcd — полное покрытие §11.8.
**Проверка:** `dotnet test src/PgWorker.slnx --filter FullyQualifiedName~PgWorker.IntegrationTests.Etcd`.
**Spec:** §4.2, §4.3, §9, §11.8.

- [ ] Шаг 1: фикстура etcd-контейнера (общая `EtcdFixture` с WithPortBinding(2379)).
- [ ] Шаг 2: 5 координационных AAA-тестов → зелёные.
- [ ] Шаг 3: 3 контрактных AAA-теста (сид панели; WorkJournal round-trip;
  portalloc round-trip) → зелёные.
- [ ] Шаг 4: commit `"test(etcd): integration — клэймы/txn/lease/snapshot + контракт форматов §4.2/§4.3"`.

---

## Блок E — PgWorker.Docker

### Задача 14: Тонкий клиент Docker Engine API

**Вход:** задачи 4–5.
**Действие:** `src/PgWorker.Docker/Engine/`:
```csharp
namespace PgWorker.Docker.Engine;

public interface IDockerEngine : IAsyncDisposable
{
    Task<Result> PingAsync(CancellationToken ct);
    Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct);
    Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct);
    Task<Result> StartContainerAsync(string idOrName, CancellationToken ct);
    Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct);
    Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct);
    Task<Result> RemoveVolumeAsync(string name, CancellationToken ct);
    // swarm:
    Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct);
    Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct);
    Task<Result> RemoveServiceAsync(string name, CancellationToken ct);
    Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct);
    Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct); // publish-порты хоста
}

public sealed record DockerContainer(string Id, string[] Names, string State, string Image);
public sealed record DockerSwarmNode(string Id, string Hostname, string State, int RunningTasks);
public sealed record DockerTask(string Id, string NodeId, string State, string? Host, int? PublishedPort);
public sealed record PortMap(int ContainerPort, int HostPort);                 // tcp
public sealed record ContainerSpec(string Image, IReadOnlyDictionary<string, string> Env,
    string VolumeName, string VolumeDest, IReadOnlyList<PortMap> Ports, string Hostname,
    double? CpuCores, long? MemoryBytes, string? Label);
public sealed record ServiceSpec(string Name, ContainerSpec Template, string NodeConstraint); // node.id==<id>

public sealed class DockerEngineFactory
{
    // endpoint: "unix:///var/run/docker.sock" | "tcp://host:2375".
    // unix: SocketsHttpHandler.ConnectCallback → UnixDomainSocketEndPoint.
    // API-версия фиксируется v1.44 (решение фазы plan; docker >= 23).
    public IDockerEngine Create(string endpoint);
}
```
Реализация `DockerEngine` : HttpClient + JSON (`System.Text.Json`):
`GET/POST /v1.44/...` по докам Engine API (containers/json с
`filters={"name":["<prefix>"]}`; create `?name=`; stop `?t=`;
rm `?force=1`; volumes/{name} DELETE; nodes; services/create;
tasks?filters={"service"...}). `BusyPortsAsync` — из `containers/json?all=1`
+ `tasks`: собрать host-порты publish.
Тесты (`UnitTests/Docker/DockerEngineTests.cs`, мок HttpMessageHandler,
фикстуры-JSON из реальных ответов Engine API):
- unix endpoint строит handler с ConnectCallback (проверяем фабрику, не сокет);
- ListContainers парсит Names с ведущим `/`;
- CreateContainer шлёт env/порты/volume в теле (сверка JSON);
- 404 на rm → Result.Success (идемпотентность);
- 409 «already exists» на create → Result.Success (идемпотентность);
- BusyPorts собирает уникальные пары.
**Выход:** примитивы docker для драйверов.
**Проверка:** `dotnet test src/PgWorker.slnx --filter DockerEngine`.
**Spec:** §5.2–5.3, Д3.

- [ ] Шаг 1: контракт (файлы выше, пустая реализация) + сборка.
- [ ] Шаг 2: AAA-тесты на моках → красный.
- [ ] Шаг 3: реализация → зелёный.
- [ ] Шаг 4: commit `"feat(docker): тонкий клиент Engine API (containers/services/nodes/ports)"`.

### Задача 15: Драйверы кластера (plain / swarm)

**Вход:** задачи 9, 14.
**Действие:** `src/PgWorker.Docker/Drivers/ClusterDriver.cs`:
```csharp
namespace PgWorker.Docker.Drivers;

public interface IClusterDriver
{
    // Живые хосты для PlacementPlanner: plain — конфиг (UsedSlots по числу
    // контейнеров pgw-*), swarm — ListNodes (running tasks).
    Task<Result<IReadOnlyList<HostInfo>>> GetHostsAsync(CancellationToken ct);

    // Занятые host:port (для PortAllocator).
    Task<Result<IReadOnlySet<(string, int)>>> GetBusyPortsAsync(CancellationToken ct);

    // Идемпотентно создать ноду (plain: container pgw-<C>-<X>-<n> + volume
    // pgw-<C>-<X>-<n>-data; swarm: service с constraint node.id==<id>, publish
    // mode=host). env/конфиги — из NodeConfigBuilders; существующий объект
    // сверяется по имени и не пересоздаётся.
    Task<Result> EnsureNodeAsync(ShardTopology topology, string nodeName, NodeAddress addr,
        InstallSecrets secrets, EtcdEndpoints etcd, CancellationToken ct);

    // Остановить и удалить ноду + volume (404 = успех). swarm: service rm.
    Task<Result> RemoveNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    // Только остановить контейнер ноды (эвакуация E3: карантин вернувшегося
    // шарда — данные на месте, нода не удаляется). 404 = успех.
    Task<Result> StopNodeAsync(string cluster, string shard, string nodeName, CancellationToken ct);

    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(string cluster, CancellationToken ct); // имена pgw-<C>-*
}

public sealed class PlainClusterDriver(IReadOnlyList<HostEndpoint> hosts, DockerEngineFactory f,
    bool enableDoorman) : IClusterDriver;
public sealed class SwarmClusterDriver(string managerEndpoint, DockerEngineFactory f,
    bool enableDoorman) : IClusterDriver;
public sealed record HostEndpoint(string Name, string Endpoint);
```
Размещение ноды по хосту: plain — DockerEngine конкретного хоста из
`addr.Host`; swarm — constraint `node.id==<id>` (id ищем по Hostname==addr.Host).
Флаг `EnableDoorman` (spec §12 R1, из Options): false → doorman-конфиг не
генерируется, порт 6432 не публикуется (узел без пулера — компромисс для
стенда/сборки образа без doorman).
Тесты (`UnitTests/Docker/ClusterDriverTests.cs`, мок IDockerEngine):
- EnsureNode при существующем контейнере не создаёт второй раз;
- create-тело содержит env из SpiloEnvBuilder и volume;
- RemoveNode: stop→rm(force)→volume rm; 404 на любом шаге — успех;
- StopNode: stop без rm/volume (карантин E3);
- swarm: service spec с constraint и publish mode host (проверяем тело);
- GetHosts: UsedSlots = число контейнеров префикса.
**Выход:** унифицированное управление нодой в обоих режимах (включая
stop-only для карантина).
**Проверка:** `dotnet test src/PgWorker.slnx --filter ClusterDriver`.
**Spec:** §5.2, §5.3, §6.4 E3, Д3, Д4, Д5.

- [ ] Шаг 1: AAA-тесты → красный.
- [ ] Шаг 2: реализация двух драйверов → зелёный.
- [ ] Шаг 3: commit `"feat(docker): драйверы plain/swarm — создание/остановка/удаление нод"`.

### Задача 16: Integration: docker (trait DockerAvailable)

**Вход:** задача 15; docker доступен.
**Действие:** `src/tests/PgWorker.IntegrationTests/Docker/DockerDriverTests.cs`
(все тесты начинаются с `DockerTrait.SkipIfUnavailable()` — helper:
`Assert.SkipWhen(Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1", "docker-тесты выключены")`):
- AAA: create+start контейнера `alpine sleep 60` с publish-портом → в списке;
- AAA: повторный Ensure с тем же именем — без ошибки, контейнер один;
- AAA: RemoveNode — контейнер и volume исчезли; повторный вызов — успех;
- AAA: StopNode — контейнер в состоянии exited, volume на месте; повтор — успех;
- AAA: BusyPorts отражает занятый publish-порт.
**Выход:** драйвер доказан на живом docker.
**Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx --filter FullyQualifiedName~DockerDriverTests`.
**Spec:** §9, §11.1 (docker-серия).

- [ ] Шаг 1: `DockerTrait` helper + 5 AAA-тестов.
- [ ] Шаг 2: прогон с `PGW_TEST_DOCKER=1` — зелёный (без переменной — skipped).
- [ ] Шаг 3: commit `"test(docker): integration-тесты драйвера на живом docker"`.

---

## Блок F — PgWorker.Provisioning

### Задача 17: ShardProbe (Patroni REST)

**Вход:** задачи 10–11; WorkJournal уже в слое Etcd (задача 12).
**Действие:** `src/PgWorker.Provisioning/Probes/ShardProbe.cs`:
```csharp
namespace PgWorker.Provisioning.Probes;

public sealed record PatroniMember(string Name, string Role, string State); // role: master|replica

public sealed class ShardProbe(HttpClient http)
{
    // GET http://<host>:<patroniPort>/cluster (timeout 3с) — состояние нод шарда.
    public Task<Result<IReadOnlyList<PatroniMember>>> GetClusterAsync(NodeAddress node, CancellationToken ct);

    // Живость конкретной ноды: GET /cluster 200.
    public Task<bool> IsAliveAsync(NodeAddress node, CancellationToken ct);
}
```
Тесты (`UnitTests/Provisioning/ShardProbeTests.cs`, мок HttpMessageHandler,
Patroni-фикстура — образец `ProbesFixtures/patroni-cluster.json` из AdminPanel):
- парсинг `{"members":[{"name":"shard1a","role":"master","state":"running"},…]}`
  → список PatroniMember;
- 500 → Result.Failed; timeout (TaskCanceledException) → Result.Failed;
- IsAliveAsync: 200 → true, 500 → false.
**Выход:** пробы Patroni для процессов и ReconcileLoop.
**Проверка:** `dotnet test src/PgWorker.slnx --filter ShardProbe`.
**Spec:** §6.4 C, P11.

- [ ] Шаг 1: скопировать `patroni-cluster.json` в `src/tests/PgWorker.UnitTests/ProbesFixtures/` (CopyToOutputDirectory).
- [ ] Шаг 2: AAA-тесты (3 кейса) → красный.
- [ ] Шаг 3: реализация → зелёный.
- [ ] Шаг 4: commit `"feat(provisioning): ShardProbe — Patroni REST пробы"`.

### Задача 18: DatabaseProvisioner (SQL-слой)

**Вход:** задачи 5, 10; Npgsql-пакет подключён.
**Действие:** `src/PgWorker.Provisioning/Sql/DatabaseProvisioner.cs`:
```csharp
namespace PgWorker.Provisioning.Sql;

public sealed class DatabaseProvisioner
{
    // Все операции идемпотентны; подключение — к master-ноде шарда (:pgPort),
    // user=postgres, пароль из InstallSecrets (Д7). DSN без etcd — пароли
    // только из памяти.
    public static string BuildCreateDatabaseSql(string dbname);        // SELECT для проверки + CREATE DATABASE IF-нет (через pg_database)
    public static string BuildRolesSql(InstallSecrets s);             // CREATE ROLE app/bucket_admin/bucket_mover LOGIN + GRANT'ы §4 доки 11
    public static string BuildSchemasSql(string dbname, IEnumerable<int> bucketIds); // CREATE SCHEMA IF NOT EXISTS bucket_i + GRANT USAGE
    public async Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct); // ExecuteNonQuery с SqlRetry
}
```
Роли точно по доке 11 §4: `bucket_mover` с `REPLICATION` и `GRANT SELECT ON
ALL TABLES IN SCHEMA`; app-роль — INSERT/UPDATE/DELETE/TRUNCATE + USAGE,
UPDATE на sequences (гранты — при создании схем, пустых таблиц нет).
Тесты (unit, без БД — проверяем генерацию SQL-текстов): `CREATE SCHEMA IF
NOT EXISTS bucket_7`, гранты для всех ролей, `CREATE DATABASE` guard через
`pg_database`, идемпотентность (`IF NOT EXISTS`/`NOT EXISTS`).
**Выход:** SQL-механика инициализации БД/ролей/схем.
**Проверка:** `dotnet test src/PgWorker.slnx --filter DatabaseProvisioner`.
**Spec:** §6.4 A (P2.3–P2.4), P1/P5 (роли), Д7.

- [ ] Шаг 1: AAA-тесты на SQL-тексты → красный.
- [ ] Шаг 2: реализация → зелёный.
- [ ] Шаг 3: commit `"feat(provisioning): DatabaseProvisioner — БД/роли/схемы (идемпотентный SQL)"`.

### Задача 19: ProvisioningProcess (машина состояний P0–P5)

**Вход:** задачи 6–7, 9, 12 (ClaimStore + WorkJournal), 15, 17, 18.
**Действие:** `src/PgWorker.Provisioning/Processes/ProvisioningProcess.cs`:
```csharp
namespace PgWorker.Provisioning.Processes;

public interface IClusterProcess
{
    // Один такт процесса: доводит кластер насколько возможно за вызов;
    // состояние фаз — в /pgworker/work/<C> + nodes state (spec §6.4).
    Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct);
}
public enum ProcessOutcome { InProgress, Done, Failed }

public sealed class ProvisioningProcess( // все зависимости через конструктор:
    IEtcdGateway etcd, string[] endpoints, IClusterDriver driver,
    DatabaseProvisioner db, ShardProbe probe, ClaimStore claims, WorkJournal journal,
    PlacementOptions placementOpts, InstallSecrets secrets) : IClusterProcess
```
Фазы (spec §6.4 A; каждая идемпотентна, перед фазой — `IsMine(cluster)` и
перечитывание `config` — если state сменился на TO_REMOVE посреди работы,
процесс безопасно прекращается, кластер подхватит Deprovisioning (spec §12 R6)):
P0 journal(op=provision); P1 план: `PlacementPlanner`+`PortAllocator` +
сохранение `/pgworker/portalloc/<C>` (txn compare version==0 — только
создание; далее переиспользование); P2.1 EnsureNode для всех нод всех
шардов + `nodes/<n>/state=PROVISIONING`; P2.2 ожидание: scope initialized +
leader в `/service/<C>-<X>/` + probe жив (PatroniBootTimeoutSec 600) →
`RUNNING`; P2.3–P2.4 через master (из master-ключа): БД/роли/схемы по
routing шарда; P2.5 `shards/X/dsn` (multi-host: `host=h1,h2
port=pg1,pg2 dbname=<C> user=bucket_admin`); P3 del всех
`status/bucket_<i>` (пакетами ≤128 через txn с пустым compare); P4 txn
(compare config.mod_revision) put config без `state`; P5 снапшот (делегат
`Func<CancellationToken,Task>` от SnapshotJob) + journal phase=done.
Guard входа (spec §6.4 A): полный набор ключей панели (config, replicas,
nodes, routing всех N) — иначе journal phase=waiting-keys, InProgress.
Тесты (`UnitTests/Provisioning/ProvisioningProcessTests.cs`, все зависимости
мок-интерфейсы): 
- тик на чистом NOT_INITIALIZED кл-ре: порядок вызовов (EnsureNode×ноды →
  nodes state PROVISIONING → …), один тик без живого Patroni → InProgress,
  journal=waiting-patroni;
- второй тик при живом Patroni (мок probe) → DONE: dsn записан, статус-ключи
  удалены, config перезаписан без state (мок txn-вызов с compare
  ModRevision);
- повторный тик после DONE → никаких новых EnsureNode (идемпотентность по
  узлу: state=RUNNING + контейнер есть);
- guard: нет routing-ключей → waiting-keys, docker не трогаем.
**Выход:** процесс provisioning — главная машина.
**Проверка:** `dotnet test src/PgWorker.slnx --filter ProvisioningProcess`.
**Spec:** §6.4 A, Д1, Д5.

- [ ] Шаг 1: интерфейс IClusterProcess + AAA-тесты (4 сценария) → красный.
- [ ] Шаг 2: реализация фаз P0–P5 (каждая — private-метод, вход через journal) → зелёный.
- [ ] Шаг 3: commit `"feat(provisioning): ProvisioningProcess P0–P5"`.

### Задача 20: DeprovisioningProcess (D0–D3)

**Вход:** задача 19 (общий каркас процессов).
**Действие:** `src/PgWorker.Provisioning/Processes/DeprovisioningProcess.cs`
(конструктор аналогичен 19, + ClaimStore): D0 journal(op=deprovision);
D1 RemoveNode всех нод (по nodes-ключам + `ListNodeObjects` для сирот) +
`nodes/<n>/state=REMOVING`; D2 del prefix `/clusters/<C>/` + точечные
`/service/<C>-shard<k>/request_*` + del prefix `/service/<C>-<X>/` (guard:
docker-объектов нет) + `/pgworker/portalloc/<C>` и `/pgworker/work/<C>`;
D3 снапшот; успех = `GetAsync(config)` → ключа нет + явное освобождение
клэйма `claims.ReleaseClusterAsync(<C>)` (del `/pgworker/claims/<C>` +
revoke lease) — клэйм не висит до TTL (spec §6.4 D2 включает
`/pgworker/{portalloc,work,claims}/<C>*`).
Тесты: полный тик удаляет всё (моки: список вызовов RemoveNode/del);
сироты-контейнеры (nodes-ключей нет, docker вернул имена) — удаляются;
частичный отказ docker (Failed) → journal phase=removing-nodes, повторный
тик продолжает; после Done — ReleaseClusterAsync вызван (клэйм снят сразу,
не по TTL); config-ключ отсутствует → Done.
**Выход:** безопасное удаление кластера.
**Проверка:** `dotnet test src/PgWorker.slnx --filter DeprovisioningProcess`.
**Spec:** §6.4 B, §4.2.

- [ ] Шаг 1: AAA-тесты (5 кейсов, включая снятие клэйма после Done) → красный.
- [ ] Шаг 2: реализация → зелёный.
- [ ] Шаг 3: commit `"feat(provisioning): DeprovisioningProcess D0–D3 (со снятием клэйма)"`.

### Задача 21: NodeSupervisor + MasterKeyReconciler

**Вход:** задачи 12, 15, 17.
**Действие:** `src/PgWorker.Provisioning/Processes/NodeSupervisor.cs`:
```csharp
public sealed class NodeSupervisor(/* etcd, driver, probe, journal (WorkJournal), claims, thresholds, clock */)
{
    // Тик надзора (spec §6.4 C). Возвращает решения для ReconcileLoop.
    public Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct);
    // Событие для цикла: шард мёртв целиком дольше ShardDeadSec → эвакуация.
    public IReadOnlyList<string> DeadShards { get; }
}
```
Логика: сверка декларации (каждой плановой ноде — контейнер/сервис; нет →
EnsureNode, state PROVISIONING); probe всех нод; нода молчит >
NodeDeadSec(90) И не лидер(scope leader ≠ её name) И живых ≥2 → rebuild
(RemoveNode+EnsureNode с тем же addr, state REBUILDING); весь шард молчит +
master-ключа нет > ShardDeadSec(300) → шард попадает в DeadShards (эвакуатор
запускает ReconcileLoop — задача 23). Пороговое время трекается в
`/pgworker/work/<C>`: `{"unreachable": {"<X>/<n>": first_seen_unix}}`
(решение фазы plan №4: значение `nodes/<n>/state` — плоская строка по
контракту панели).
`MasterKeyReconciler` (P11): для каждого шарда: `GET /primary`-проба нод →
фактический primary; расхождение с master-ключом (или ключа нет при живом
primary) → lease-grant TTL 5 + put `host:6432` (host ноды, порт doorman) +
продление в этом же тике не нужно (ключ перепишет callback; сверка —
только при рассинхроне).
Тесты: снесённый контейнер → EnsureNode вызван; мёртвая не-лидер нода с
живым кворумом → rebuild (RemoveNode+EnsureNode, state REBUILDING);
мёртвый лидер → ничего (Patroni сам); весь шард мёртв → шард в DeadShards;
reconciler: ключ = реплика → перезапись по факту primary; ключ
корректен → мутаций нет.
**Выход:** штатный надзор + P11-контур.
**Проверка:** `dotnet test src/PgWorker.slnx --filter "NodeSupervisor|MasterKeyReconciler"`.
**Spec:** §6.4 C, P11, §7.

- [ ] Шаг 1: AAA-тесты (6 кейсов) → красный.
- [ ] Шаг 2: реализация → зелёный.
- [ ] Шаг 3: commit `"feat(provisioning): NodeSupervisor + MasterKeyReconciler (P11)"`.

### Задача 22: BucketEvacuator + SnapshotJob

**Вход:** задачи 8, 12 (WorkJournal), 14–15 (IClusterDriver — stop/карантин
E3), 18.
**Действие:**
- `src/PgWorker.Provisioning/Processes/BucketEvacuator.cs` (spec §6.4 E):
  E0 guard'ы (шард мёртв ≥ порога — триггер от NodeSupervisor; ни один
  бакет не SYNCING/FROZEN/ABORTING; живые есть) → journal
  `/pgworker/evacuations/<C>/<X>` с планом EvacuationPlanner + снапшот «до»;
  E1 схемы на целевых шардах (DatabaseProvisioner.BuildSchemasSql);
  E2 по каждому бакету txn (compare routing=`<deadShard>`) put routing=`<to>`
  (сравнение не сошлось → перечитать, зафиксировать конфликт в journal);
  E3 nodes state=QUARANTINED; при возврате REST-живости (следующие тики) —
  `driver.StopNodeAsync` нод (контейнер остановлен, volume/данные на месте),
  journal `state=QUARANTINED, returned_unix`; данные никогда не удаляются;
  E4 journal DONE + снапшот «после».
- `src/PgWorker.Provisioning/Snapshots/SnapshotJob.cs`:
  ```csharp
  public sealed class SnapshotJob(IEtcdGateway etcd, string[] endpoints, string dir)
  {
      // /v3/snapshot/save → файл <dir>/snapshot-<yyyyMMdd-HHmmss>.db; чистит
      // старше RetentionFiles (10). Возвращает путь.
      public Task<Result<string>> TakeAsync(CancellationToken ct);
  }
  ```
Тесты: эвакуатор-тик пишет journal ДО SQL-вызовов (порядок моков); flip
отклонён при чужом routing (мок txn false) → journal conflict; схемы
создаются на целевых шардах; возврат шарда → StopNodeAsync-вызовы (без
RemoveNode — данные целы); SnapshotJob пишет файл и чистит старые (tmp-каталог).
**Выход:** аварийная эвакуация + снапшоты.
**Проверка:** `dotnet test src/PgWorker.slnx --filter "BucketEvacuator|SnapshotJob"`.
**Spec:** §6.4 D/E, Д6, P12.

- [ ] Шаг 1: AAA-тесты → красный.
- [ ] Шаг 2: реализация → зелёный.
- [ ] Шаг 3: commit `"feat(provisioning): BucketEvacuator + SnapshotJob (P12)"`.

---

## Блок G — PgWorker.App

### Задача 23: Options, DI, циклы, Program

**Вход:** блоки D–F собраны и протестированы.
**Действие:**
- `src/PgWorker.App/Options.cs` (примеры значений — в appsettings.json
  задачи 25):
  ```csharp
  namespace PgWorker.App;

  public sealed class PgWorkerOptions
  {
      public EtcdOptions Etcd { get; set; } = new();          // Endpoints: []
      public DockerOptions Docker { get; set; } = new();      // Mode, Hosts, SwarmManager, PortRange, Images, EnableDoorman
      public LoopsOptions Loops { get; set; } = new();        // ScanIntervalSec=5, KeepaliveSec=5, SnapshotIntervalMin=360, ErrorDelayMs=2000
      public ThresholdsOptions Thresholds { get; set; } = new(); // NodeDeadSec=90, ShardDeadSec=300, PatroniBootSec=600
      public ParallelismOptions Parallelism { get; set; } = new(); // MaxClusters=4
      public SnapshotOptions Snapshots { get; set; } = new(); // Dir="/snapshots", RetentionFiles=10
  }
  ```
- `src/PgWorker.App/Loops/ReconcileLoop.cs` (за образец —
  `/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App.Bus/Consumer/BusConsumerHostedService.cs`):
  ```csharp
  internal sealed class ReconcileLoop(/* options, gateway, parser, claims, processes */) : BackgroundService
  {
      protected override async Task ExecuteAsync(CancellationToken stoppingToken)
      {
          while (!stoppingToken.IsCancellationRequested)
          {
              // Тик (spec §6.2): снапшот → клэйм → процесс → следующий тик = ретрай.
              var outcome = await TickAsync(stoppingToken);
              if (!outcome.IsSuccess)
              {
                  logger.LogError(outcome.Error, "тик ReconcileLoop не прошёл");
                  await Task.Delay(config.Loops.ErrorDelayMs, stoppingToken); // 2с
              }
              else
                  await Task.Delay(config.Loops.ScanIntervalSec * 1000, stoppingToken);
          }
      }
      // TickAsync: range /clusters/ + /service/ → ParseClusters → параллельно
      // (SemaphoreSlim MaxClusters) по каждому кластеру: классификация →
      // TryClaimClusterAsync → процесс.TickAsync(snap). Эвакуационные события
      // NodeSupervisor.DeadShards → BucketEvacuator для шарда.
  }
  ```
  Классификация: `config.state=NOT_INITIALIZED` → ProvisioningProcess;
  `TO_REMOVE` → DeprovisioningProcess; иначе → NodeSupervisor.
- `src/PgWorker.App/Loops/KeepaliveLoop.cs` — тик ClaimStore.StartAsync-цикла
  (продление) + instance-ключ.
- `src/PgWorker.App/Loops/SnapshotLoop.cs` — только при лидерстве
  (`ClaimStore.IsLeader`): SnapshotJob по интервалу.
- `src/PgWorker.App/Program.cs`: Host-builder, конфиг appsettings+env
  (`PGW_*` для секретов), DI: gateway HttpClient, ClaimStore, WorkJournal,
  драйвер по `Docker.Mode` (Plain: Hosts → движки; Swarm: manager),
  процессы, циклы.
Тесты: классификация кластеров (unit — таблица: NOT_INITIALIZED→Provisioning,
TO_REMOVE→Deprovisioning, Active→Supervisor); ReconcileLoop на моках: тик
обрабатывает один кластер и вызывает нужный процесс; DeadShards →
вызов BucketEvacuator; параллелизм не превышает SemaphoreSlim (2 потока ×
лимит 1).
**Выход:** работающий сервис-хост.
**Проверка:** `dotnet build src/PgWorker.slnx -c Release` +
`dotnet test src/PgWorker.slnx --filter "ReconcileLoop|ClassificationTests"`.
**Spec:** §6.2, §10.

- [ ] Шаг 1: Options + AAA unit-тесты классификации/цикла → красный.
- [ ] Шаг 2: циклы + Program → зелёный.
- [ ] Шаг 3: commit `"feat(app): циклы Reconcile/Keepalive/Snapshot + Program"`.

### Задача 24: Health checks

**Вход:** задача 23.
**Действие:** скопировать
`/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/HealthChecks/{HealthCheckAbstract.cs,IHealthCheckService.cs}`
→ `src/PgWorker.App/HealthChecks/` (namespace-замена). Реализовать
`PgWorkerHealth : HealthCheckAbstract` (spec §8): etcd-reachable (последний
Range-тик), docker-hosts (последний Ping каждого), loops-alive (время
последнего тика каждого цикла), claims (счётчик удерживаемых),
snapshot-freshness (возраст последнего снапшота). Map в Program:
`app.MapHealthChecks("/healthz")` (Microsoft.Extensions.Diagnostics.HealthChecks).
Тесты: fake-провайдер состояний → все секции отдаются; недоступный etcd →
Degraded.
**Выход:** наблюдаемость MVP.
**Проверка:** `dotnet test src/PgWorker.slnx --filter Health`.
**Spec:** §8.

- [ ] Шаг 1: копия + реализация + AAA-тесты → красный → зелёный.
- [ ] Шаг 2: commit `"feat(app): health checks (/healthz: etcd/docker/loops/claims/snapshots)"`.

---

## Блок H — поставка и e2e

### Задача 25: Поставка: Dockerfile'ы, compose, appsettings

**Вход:** задача 23 собрана.
**Действие:**
- `src/PgWorker.App/appsettings.json` — полный пример (spec §10): все секции
  Options + `Docker.Hosts` образец + `Images.Node=pgworker-node:dev`.
- `docker/PgWorker.Dockerfile` — multi-stage (SDK → final `mcr.microsoft.com/dotnet/aspnet:10.0`),
  копия по образцу `/Users/demakaev/ZCodeProject/AdminPanel/Dockerfile`.
- `docker/node/Dockerfile` — образ `pgworker-node` (Д4):
  `FROM ghcr.io/zalando/spilo-16:3.3-p3`; установка haproxy (apt) +
  pg_doorman (бинарник из ARG DOORMAN_URL — переменная сборки, версия пин);
  `supervisord.conf` (patroni уже стартует Spilo; добавляем doorman, haproxy);
  копия `master-lease.py` (адаптация `rolecheck.py`: env `PGW_ETCD`,
  `PGW_MASTER_KEY`, `PGW_NODE_HOST`, `PGW_DOORMAN_PORT`; LEASE_TTL=5,
  KEEPALIVE=1с; демон стартует при role=master, гаснет при replica).
- `deploy/docker-compose.yml`: сервис `pgworker` (образ PgWorker; тома:
  `/var/run/docker.sock:/var/run/docker.sock`, `pgw-snapshots:/snapshots`;
  env `PGW_*` секреты; restart: unless-stopped).
Тесты: не требуются (сборочные артефакты); проверка — сборка.
**Выход:** поставка по spec §10.
**Проверка:** `docker build -f docker/PgWorker.Dockerfile -t pgworker:dev .` — успех.
**Spec:** §10, Д4, Д7.

- [ ] Шаг 1: appsettings.json + Dockerfile сервиса (образец AdminPanel).
- [ ] Шаг 2: node-Dockerfile + master-lease.py (адаптация rolecheck.py) + supervisord.conf.
- [ ] Шаг 3: compose; локальная сборка образа — успех.
- [ ] Шаг 4: commit `"build: поставка — Dockerfile сервиса и узла, compose, appsettings"`.

### Задача 26: dev-stand + e2e (критерии приёмки §11)

**Вход:** задачи 13, 16, 25 (образ pgworker-node собран: `docker build -f docker/node/Dockerfile -t pgworker-node:dev .`).
**Действие:**
- `dev-stand/compose.yaml`: etcd (`quay.io/coreos/etcd:v3.5.21`, порт 2379,
  volume) — внешний слой; PgWorker не в стендe (тесты запускают хост).
- `dev-stand/seed.sh` (или C#-фикстура): сид создания кластера в стиле
  панели 02 §9.1 (кластер `shop`, N=6, S=2, replicas=2, request_*).
- `src/tests/PgWorker.IntegrationTests/E2e/E2eScenarios.cs` (все с
  `DockerTrait.SkipIfUnavailable()` + сборка node-образа в фикстуре):
  - **AC2 provisioning e2e**: сид → запуск хоста `PgWorker.App` (Process или
    `WebApplicationFactory`-эквивалент Host) → wait до DONE → проверки etcd:
    dsn у шардов, nodes=RUNNING, статус-ключей нет, config без state;
    `docker ps` — 4 контейнера pgw-shop-* на разных «хостах» (локально —
    один docker-хост, порты разные); схемы/роли в PG (SQL-проверка через
    Npgsql к master);
    **O2-проверка (spec §12 O2)**: прочитать ключ
    `/clusters/shop/shards/shard1/dsn` из etcd и выполнить probe
    `SELECT 1` через Npgsql **по этому multi-host DSN как записан** (host
    перечислением + port перечислением, без пароля в ключе) с добавлением
    `Password=<bucket_admin из сид-секрета>` — доказывает, что формат
    multi-host DSN с разными портами работает с Npgsql.
  - **AC3 takeover**: kill процесса-инстанса №1 посреди provisioning →
    инстанс №2 доносит (second host process) → без дублей контейнеров.
  - **AC4 deprovisioning**: PUT TO_REMOVE → wait → контейнеров/volume нет,
    префикс `/clusters/shop/` пуст, `/service/shop-*` пуст; клэйм
    `/pgworker/claims/shop` снят сразу (не ждём TTL).
  - **AC5 failover/rebuild**: `docker stop` лидера → master-ключ обновился
    ≤10с; остановленный пересоздан (state REBUILDING→RUNNING); реплика
    догоняет (`pg_is_in_recovery()`).
  - **AC6 эвакуация**: stop всех контейнеров shard2 → после ShardDeadSec
    (в тесте 5с — опции) routing бакетов shard2 → shard1, схемы созданы,
    journal эвакуации заполнен, снапшоты до/после; `docker start` нод
    shard2 → контейнеры остановлены PgWorker (stop, не rm), QUARANTINED.
  - **AC7 клэймы**: два инстанса: журнал/`/pgworker/claims` — кластер
    обрабатывает один; снапшоты снимает только лидер.
**Выход:** все критерии приёмки §11 автодоказаны (включая O2).
**Проверка:** `PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx --filter E2e`.
**Spec:** §11 (полный список, включая §11.8 через задачу 13), §9, §12 O2.

- [ ] Шаг 1: dev-stand compose + сид (кластер shop 6×2×2).
- [ ] Шаг 2: E2E-фикстура (node-образ, хост-инстанс PgWorker, этcd-клиент
  теста) + сценарий AC2 (provisioning + O2 multi-host DSN probe) — зелёный.
- [ ] Шаг 3: сценарии AC3/AC4 (takeover, deprovisioning) — зелёные.
- [ ] Шаг 4: сценарии AC5/AC6/AC7 — зелёные.
- [ ] Шаг 5: commit `"test(e2e): сценарии приёмки spec §11 на dev-stand"`.

### Задача 27: Финальная верификация и self-review

**Вход:** все задачи 1–26.
**Действие:**
- Полная сборка: `dotnet build src/PgWorker.slnx -c Release` — 0 warnings.
- Полный прогон: `dotnet test src/PgWorker.slnx -c Release` (unit всегда;
  integration — `PGW_TEST_DOCKER=1` при доступном docker) — зелёные.
- Self-review против spec: чек-лист разделов §4–§11 → каждый пункт закрыт
  задачей (таблица в commit-message не нужна; сверка глазами + правки при
  пробеле).
- README-строка в `arch/14-pgworker.md` при расхождениях фазы исполнения
  (если что-то реализовано иначе — правка доки тем же коммитом).
**Выход:** фича готова к ревью перед мержем.
**Проверка:** команды выше зелёные; `grep -c "TODO\|TBD" src -r` == 0.
**Spec:** весь.

- [ ] Шаг 1: build + test полные.
- [ ] Шаг 2: self-review spec-чеклиста; правки-при-пробеле.
- [ ] Шаг 3: финальный commit `"chore: финальная верификация PgWorker (build+tests green)"`.

---

## Решения фазы plan (не покрыто spec напрямую — принято здесь)

1. **Polly 8 API** вместо v7-стиля Puzzle: `RetryPolicies` переписаны на
   `ResiliencePipeline` (код в задаче 5); версии пакетов — из Puzzle (8.7.0).
2. **Docker API version pinned v1.44** (docker ≥ 23): без negotiate —
   стабильный минимум для нужных endpoint'ов; при 404 от старого docker —
   понятная ошибка.
3. **`PGW_TEST_DOCKER=1`** — переменная включения docker/e2e-серий; без неё
   тесты пропускаются (`Assert.skipWhen`) — CI без docker остаётся зелёным.
4. **Время недоступности нод** трекается в `/pgworker/work/<C>` (поле
   `unreachable`), т.к. значение `nodes/<n>/state` — плоская строка по
   контракту панели.
5. **Локальный dev-stand без docker-host'ов**: e2e на одном docker-хосте
   (режим plain, Hosts=[local]) — анти-аффинити вырождается в «порты
   разные», что и проверяем; отдельный two-host стенд — опционально руками.
6. **SnapshotLoop интервал по умолчанию 6 ч** (360 мин) — из spec §10.
7. **Не реализуем watch**: poll 5 с (Д8); watch не появляется нигде в плане.
8. **WorkJournal перенесён из Provisioning (задача 17) в слой Etcd (задача
   12, `PgWorker.Etcd/Coordination/`)** — правка ревью №1: integration-тесты
   форматов §4.3 (задача 13) требуют WorkJournal до блока F; класс — чистая
   etcd-обёртка над `/pgworker/*`, ему место рядом с ClaimStore. Задача 17
   осталась только про ShardProbe.
9. **Трактовка O2-проверки (AC2)**: подключение невозможно без пароля, а
   пароли в etcd не хранятся (P12/P17) — probe выполняется по DSN из ключа
   `shards/X/dsn` как записан (multi-host, без пароля) + `Password` из
   сид-секрета `bucket_admin`; проверяется именно формат multi-host DSN с
   разными портами (суть O2).
10. **Добавлен `StopNodeAsync` в IClusterDriver** (задача 15): эвакуация E3
    требует остановки контейнеров карантина без удаления (данные на месте) —
    отдельный метод вместо злоупотребления RemoveNode.

## Журнал ревизий плана

- **Ревизия 2 (по ревью Фазы 4, CHANGES_REQUESTED):**
  1. Задача 13: добавлены контрактные AAA-тесты §11.8 — WorkJournal
     round-trip против реального etcd и portalloc round-trip (формат
     `{"<shard>/<node>":{host,pg,patroni,doorman}}`); WorkJournal перенесён
     в задачу 12 (решение №8), чтобы тесты блока D не зависели от блока F.
  2. Задача 26 (AC2): добавлена O2-проверка — probe `SELECT 1` через Npgsql
     по multi-host DSN из ключа `shards/X/dsn` (трактовка — решение №9).
  3. Исправлен путь образца worker-цикла: `…/PuzzleServer.Infrastructure.App.Bus/Consumer/BusConsumerHostedService.cs`
     (в глобальных источниках и задаче 23).
  4. Задача 11: исправлены источники фикстур — JSON-файлы
     `…/src/tests/AdminPanel.UnitTests/EtcdFixtures/*.json` + C#-загрузчик
     `EtcdFixtures.cs` (копия + адаптация namespace/Kv).
  5. Задача 20 (D2/D3): явное снятие клэйма `ReleaseClusterAsync(<C>)` после
     успеха deprovisioning + тест «клэйм снят сразу, не по TTL».
  6. Задача 22: «Вход» дополнен задачами 14–15 (driver); в E3 —
     `StopNodeAsync` (новый метод IClusterDriver, задача 15), тест stop-без-rm.
