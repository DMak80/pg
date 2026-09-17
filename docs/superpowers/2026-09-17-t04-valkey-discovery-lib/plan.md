# t04-valkey-discovery-lib — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Клиентская дискавери-библиотека HA.Valkey в репозитории Puzzle — только читатель etcd-префикса `/valkey/clusters/<C>/`, иммутабельный снапшот + фоновая актуализация + fail-open + `GetClientConfig()` plain-полями.

**Architecture:** Зеркальная копия HA.Kafka (1:1 по структуре и семантике, docs/01.19), отличия только в доменной модели (нет topics) и наборе читаемых ключей. Транспорт — общий слой HA.Etcd (`IEtcdClient`). Roadmap-правки и spec/plan — в feature-ветке pg-worktree; код и канон docs/01.21 — в feature-ветке `t04-valkey-discovery-lib` репозитория Puzzle.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xunit v3 + FluentAssertions, Testcontainers (etcd `quay.io/coreos/etcd:v3.5.21`). Внешних пакетов НЕ добавляется.

**Spec:** `docs/superpowers/2026-09-17-t04-valkey-discovery-lib/spec.md` (worktree pg). Канон контракта: `arch/20-valkey-clusters.md` §2/§4/§5 (в worktree pg). Образец реализации: `../Puzzle` → `src/PuzzleServer.Infrastructure.App.HA.Kafka` + `docs/01.19-ha-kafka.md`.

## Global Constraints

- Два репозитория: **pg-worktree** `/Users/demakaev/ZCodeProject/worktrees/t04-valkey-discovery-lib` (ветка `t04-valkey-discovery-lib`, уже существует) — roadmap/spec/plan; **Puzzle** `/Users/demakaev/ZCodeProject/Puzzle` — код + канон docs/01.21, в НОВОЙ ветке `t04-valkey-discovery-lib`, создаваемой от `main`.
- Коммиты в feature-ветки — свободно; **мерж в main и пуши — ТОЛЬКО по явной просьбе пользователя**.
- В рабочей копии Puzzle висит staged `src/global.json` (не относится к t04): НЕ коммитить его, НЕ сбрасывать; везде точечный `git add <файлы задачи>`.
- Библиотека **не делает ни одной etcd-мутации**: только `POST /v3/kv/range`, `POST /v3/watch`, `POST /v3/cluster/member/list` (последний — только в members-режимах). Фиксируется интеграционным тестом.
- Формат имени кластера: `^[a-z][a-z0-9_]{0,62}$` (arch/20 §1, без дефиса).
- Дефолты опций (как у HA.Kafka): `Mode=WatchLongPoll`, `RequestTimeoutMs=2000`, `WatchWindowMs=1000`, `WatchReopenDelayMs=100`, `WatchErrorDelayMs=1000`, `PollIntervalMs=1000`, `MembersMode=Poll`, `MembersPollIntervalMs=30000`, `MembersMinIntervalMs=1000`, `BootstrapTimeoutSec=15`.
- `Ssl=false` — v1 без TLS; константа `ValkeyClientConfig.SslValue = false`; после t06 читатель обязан остаться совместимым (`ca_pem` → unknownKeys).
- Язык: документация/комментарии — русские, идентификаторы — английские; тесты — с AAA-комментариями.
- Сборка: `dotnet build src/PuzzleServer.Api.slnx` (из `/Users/demakaev/ZCodeProject/Puzzle`) — 0 warnings; `Directory.Packages.props` НЕ трогается.
- Docker-серии: после КАЖДОЙ серии интеграционных тестов — контроль зачистки контейнеров; host-порт etcd-фикстуры — параметр, дефолт `32496` (заняты: 32490, 32495, 32500–32509); таймауты ожидания контейнера короткие (готовность ≤ 30 с POST-ретраями).
- Любой sleep/ожидание в шагах агента — ≤ 30 с; интеграционная серия > 5 минут — онлайн-анализ логов по канону AGENTS.base.

---

### Шаг 1: Roadmap-декомпозиция (Фаза 0, docs-коммит ДО кода)

**Связь со spec:** §4 Фаза 0, §6 п.5 (roadmap t04 уточнён + t08 добавлен до кода).

**Вход (предусловие):** worktree pg на ветке `t04-valkey-discovery-lib`; spec одобрен (гейт user-review пройден); тег `t08` в `arch/roadmap/*.md` свободен (проверено: заняты t02–t07).

**Действие:** Правка `/Users/demakaev/ZCodeProject/worktrees/t04-valkey-discovery-lib/arch/roadmap/valkey.md`:

1. Заменить пункт `t04-valkey-discovery-lib` (текущий текст заканчивается словами «интеграция с клиентским модулем Puzzle — по образцу интеграции HA.Kafka в `Infrastructure.App.Kafka`.») на:

```markdown
- **`t04-valkey-discovery-lib`** — клиентская
  дискавери-библиотека в Puzzle: `PuzzleServer.Infrastructure.App.HA.Valkey`
  — только читатель `/valkey/clusters/<C>/` (снапшот endpoints/креды/state,
  watch+poll актуализация, fail-open, `GetClientConfig()` с plain-полями для
  StackExchange.Redis). Объём — только библиотека; интеграция в клиентский
  модуль Valkey выделена в `t08-valkey-client-integration` (клиентского
  модуля Valkey/Redis в Puzzle пока нет — как у kafka интеграция была t10).
```

2. После пункта `t06-valkey-tls` дописать новый пункт (нумерация — следующий свободный тег трека):

```markdown
- **`t08-valkey-client-integration`** (`← t04-valkey-discovery-lib`) —
  интеграция HA.Valkey в клиентский модуль Valkey приложения
  (StackExchange.Redis) по образцу связки HA.Kafka →
  `Infrastructure.App.Kafka` (docs/01.16 §1a): клиентский модуль в HaDb-режиме
  регистрирует `AddHaValkey(...).AddValkeyCluster(<Valkey:Cluster>)`,
  соединительные параметры берёт из снапшота `GetClientConfig()`, ротация
  `app_password`/смена endpoints доставляется событием `Updated`, fail-open
  без параметров. Берётся в работу, когда клиентский модуль Valkey появится
  в Puzzle.
```

**Выход:** roadmap отражает декомпозицию t04 → t04 + t08; код ещё не начат (arch-first).

**Шаги исполнения:**

- [ ] Внести обе правки в `arch/roadmap/valkey.md` (worktree pg).
- [ ] Самопроверка: `grep -n "t08-valkey-client-integration\|← t04" arch/roadmap/valkey.md` показывает новый пункт и зависимость; пункт t04 не содержит «интеграция с клиентским модулем Puzzle — по образцу».

**Проверка:** 

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t04-valkey-discovery-lib
git diff --stat                          # только arch/roadmap/valkey.md
git add arch/roadmap/valkey.md
git commit -m "docs(roadmap): t04-valkey-discovery-lib — уточнение объёма (только библиотека) + новый пункт t08-valkey-client-integration (декомпозиция до кода)"
```

---

### Шаг 2: Ветка Puzzle + канон docs/01.21 (Фаза 1, docs-коммит)

**Связь со spec:** §1 п.1 (git-организация), §2 п.5 (arch-first: канон до/вместе с кодом), §4 Фаза 1.

**Вход (предусловие):** Шаг 1 закоммичен; репозиторий `/Users/demakaev/ZCodeProject/Puzzle` на `main`, чистый (кроме staged `src/global.json` — не трогать).

**Действие:**

1. Создать feature-ветку в Puzzle:

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git checkout -b t04-valkey-discovery-lib
```

2. Создать `/Users/demakaev/ZCodeProject/Puzzle/docs/01.21-ha-valkey.md` — полный текст:

````markdown
# 01.21 — HA.Valkey (дискавери valkey-кластеров из etcd)

> Проект **`PuzzleServer.Infrastructure.App.HA.Valkey`**
> Namespace: `PuzzleServer.Infrastructure.App.HA.Valkey` (+ `.Model`, `.Parsing`, `.Refresh`)
> Назад: [01 — Инфраструктура](01-infrastructure.md) · Смежно: [01.18 HA.Etcd](01.18-ha-etcd.md) (транспорт), [01.19 HA.Kafka](01.19-ha-kafka.md) (образец 1:1)

Клиентский дискавери valkey-кластеров: читает `/valkey/clusters/<C>/` из etcd
(HTTP JSON gateway `/v3/*` через общий слой
[HA.Etcd](01.18-ha-etcd.md)), держит иммутабельный `ValkeyClusterSnapshot`
в памяти и актуализирует его в фоне. Источник контракта — репозиторий
PgWorker (`arch/20-valkey-clusters.md` §4–§5: точки дискавери и толерантность
читателей). Библиотека — **только читатель**; пишет ValkeyWorker. Внешних
пакетов не тянет — параметры клиента отдаются plain-полями. Интеграция в
клиентский модуль Valkey (StackExchange.Redis) — отдельная задача
`t08-valkey-client-integration` (roadmap pg); сейчас клиентского модуля
Valkey в Puzzle нет.

## Подключение

```csharp
services.AddHaValkey(configuration)
        .AddValkeyCluster("cache");
```

- Первый вызов `AddHaValkey(configuration)` регистрирует модуль: опции
  (секция `HaValkey` + валидация при старте), typed etcd-клиент (таймаут из
  `HaValkey:RequestTimeoutMs`), ротация endpoints, сигнальщик по `Mode`,
  store + refresher (AutoRegistration сборки), health-check `HaValkeyCheck`,
  members-монитор (`MembersMode=Off` — монитора нет в DI).
- Каждый `AddValkeyCluster("name")` добавляет заявку-кластер (флуент-паттерн):
  PostConfigure наполняет `HaValkeyOptions.Clusters`. Кластеры секцией
  конфига НЕ задаются — только заявками в коде (пережиток `HaValkey:Clusters`
  в конфиге игнорируется).
- Fail-fast: повторный `AddHaValkey`; `AddValkeyCluster` без модуля; пустое
  имя; невалидный формат `^[a-z][a-z0-9_]{0,62}$` (arch/20 §1); дубликат;
  старт без единой заявки; пустые `EtcdEndpoints`; неположительные интервалы.

`Get(cluster)` — мгновенно из кэша (без сети); `RefreshAsync` — форс-рефетч
(один range по префиксу); событие `Updated` — только при фактическом
изменении содержимого. Незаявленный кластер / снапшот ещё не собран →
`Result.Failed`.

## Режимы актуализации

| Режим | Механика | Настройки (дефолты) |
|---|---|---|
| `WatchLongPoll` (по умолчанию) | Короткоживущие watch-стримы `/v3/watch` — по одному на префикс кластера `/valkey/clusters/<C>/`; первое событие/Compacted → сигнал → полный рефетч; `start_revision` = `Revision` снапшота своего кластера (пропусков между окнами нет); compact → форс-рефетч и сброс ревизии; сбой окна → сигнал + ротация endpoint | `WatchWindowMs=1000`, `WatchReopenDelayMs=100`, `WatchErrorDelayMs=1000` |
| `Poll` | `PeriodicTimer` → полный рефетч всех префиксов | `PollIntervalMs=1000` |

Общее: `RequestTimeoutMs=2000`, `BootstrapTimeoutSec=15` (бюджет
bootstrap-рефетча при старте; провал не роняет старт), members —
`MembersMode=Poll`/`MembersPollIntervalMs=30000`/`MembersMinIntervalMs=1000`
(семантика [01.18 HA.Etcd](01.18-ha-etcd.md)). Сигналы коалесцируются:
пачка сигналов за время прохода — один дополнительный проход. Health:
`Inited` (хотя бы один успешный рефетч), `Working` (успех за последние 3
интервала режима), `StatusError` (Failed при ≥2 рефетч-сбоях подряд).

## Модель снапшота

`ValkeyClusterSnapshot`: `Cluster`, `State` (raw-строка `config.state`;
null = Active), `Endpoints` (endpoints как есть, `"h1:p1,h2:p2"`; null =
ключа нет/пустой-пробельный), `App` (`ValkeyAppSecret` — app_user+app_password
только полным набором обоих ключей; неполный → null; `HasAppSecret`),
`FetchedAtUtc`, `Revision` (max mod_revision — start_revision watch-окон).
Коллекционных полей нет (topics в домене отсутствует — arch/20 §2).

Вычислитель: `GetClientConfig()` → `ValkeyClientConfig` — plain-параметры
клиента без зависимости от StackExchange.Redis: `Endpoints`, `Username`,
`Password`, `Ssl=false` (v1 без TLS; `ssl=true` + CA — после
`t06-valkey-tls`); **null при отсутствии endpoints ИЛИ секрета**.

Пароль редацирован: `ValkeyAppSecret.ToString()`/`ValkeyClientConfig.ToString()`
дают `Password = ***` — фиксируется тестами.

**Семантика равенства**: событие `Updated` стреляет по `SameContent` —
структурному сравнению `Cluster`/`State`/`Endpoints`/`App` без
`FetchedAtUtc`/`Revision`. Коллекций нет, поэтому сравнение чисто
строкочное (паттерн ha-db, docs/01.17; проще kafka-образца — там Topics).

## Читаемые ключи

Один префикс на кластер — `/valkey/clusters/<C>/` (range и watch):

| Ключ | Значение | В снапшот |
|---|---|---|
| `config` | JSON; читается только `state` (raw-строкой; отсутствие поля = Active) | `State` |
| `endpoints` | plain `"h1:p1,h2:p2,..."`; пустой/пробельный → как отсутствующий | `Endpoints` |
| `app_user` / `app_password` | plain-креды ACL | `App` — только полным набором обоих |
| `admin_user` / `admin_password` | креды администратора (писатель — ValkeyWorker) | вне клиентского подмножества §4 — пропуск молча |
| `nodes/<k>/state`, `nodes/<k>/resources` | состояние/ресурсы нод | вне клиентского подмножества — пропуск молча |

Фильтры: битый JSON `config` → parseError (лог warning, укороченное
редактированное значение) + state=null — Active-ветка, кластер жив;
неизвестные ключи внутри `/valkey/` → лог + счётчик unknownKeys (в т.ч.
будущий `ca_pem` после t06 — обратная совместимость читателя).

## Грабли

- **fail-open**: при недоступности всех endpoints `Get` отдаёт последний
  снапшот без ограничения времени; `RefreshAsync` → Failed (кэш не трогаем);
  health деградирует; возврат etcd — восстановление первым окном/тиком;
- неполный набор кредов → `App = null` → `GetClientConfig() = null`
  (потребитель обязан проверить);
- `State` — raw-строка: незнакомое значение приходит как есть (трактовка —
  дело потребителя);
- `Updated` стреляет только при изменении содержимого: put тем же значением
  (в т.ч. ревизия растёт) событие НЕ даёт;
- смена `app_password` (ротация arch/21 §5 E) = событие `Updated` → новый
  пароль в `GetClientConfig()`;
- библиотека использует только чтение: `/v3/kv/range` + `/v3/watch` по
  префиксу кластера (+ `/v3/cluster/member/list` в members-режимах);
  фиксируется интеграционным тестом по журналу трафика.

## Интеграция в клиентский модуль (t08 — плановая)

Клиентский модуль Valkey (StackExchange.Redis) в Puzzle пока не существует;
по образцу связки HA.Kafka → `Infrastructure.App.Kafka` (docs/01.16 §1a)
интеграция выделена в задачу `t08-valkey-client-integration` (roadmap pg,
`← t04-valkey-discovery-lib`): модуль в HaDb-режиме будет регистрировать
`AddHaValkey(...).AddValkeyCluster(<Valkey:Cluster>)` и строить
`ConfigurationOptions` из `GetClientConfig()`.
````

3. В `/Users/demakaev/ZCodeProject/Puzzle/docs/01-infrastructure.md` добавить строку в таблицу «Документы» ПОСЛЕ строки `01.20 — Metrics`:

```markdown
| [01.21 — HA.Valkey](01.21-ha-valkey.md) | `PuzzleServer.Infrastructure.App.HA.Valkey` | Дискавери valkey-кластеров из etcd (контракт pg/arch/20 §4–§5): кэш-снапшот, режимы WatchLongPoll/Poll, событие `Updated`, plain-параметры клиента (ACL-креды, `ssl=false`), заявки `AddValkeyCluster`. |
```

**Выход:** ветка `t04-valkey-discovery-lib` в Puzzle; канон 01.21 + строка индекса закоммичены docs-коммитом ДО кода.

**Шаги исполнения:**

- [ ] Создать ветку (команда выше); убедиться `git branch --show-current` → `t04-valkey-discovery-lib`.
- [ ] Записать `docs/01.21-ha-valkey.md` (текст выше, дословно).
- [ ] Добавить строку в `docs/01-infrastructure.md` (после 01.20).
- [ ] Коммит (точечный add; `src/global.json` НЕ добавлять):

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
git add docs/01.21-ha-valkey.md docs/01-infrastructure.md
git commit -m "docs: 01.21-ha-valkey — канон дискавери-библиотеки HA.Valkey (arch/20 §4-§5, образец 01.19); строка в индексе 01-infrastructure"
git show --stat HEAD        # ровно 2 файла, global.json нет
```

**Проверка:** `git show --stat HEAD` показывает ровно два doc-файла; `git status` — staged `src/global.json` остался нетронутым.

---

### Шаг 3: Каркас проекта (csproj, slnx, опции, реестр, исключение)

**Связь со spec:** §3.1 (структура проекта), §3.2 (опции + дефолты), §4 Фаза 2.

**Вход (предусловие):** Шаг 2 закоммичен; ветка Puzzle `t04-valkey-discovery-lib` активна.

**Действие:** создать проект `src/PuzzleServer.Infrastructure.App.HA.Valkey` (TFM наследуется из `Directory.Build.props`, как у HA.Kafka — в csproj не указывать).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/PuzzleServer.Infrastructure.App.HA.Valkey.csproj`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyOptions.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyClusterRegistry.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyException.cs`
- Modify: `src/PuzzleServer.Api.slnx` (папка `/Infrastructure/`, строка после HA.Kafka)

**Шаги исполнения:**

- [ ] Создать csproj (копия HA.Kafka, заменены имена сборок):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="Microsoft.Extensions.Configuration.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks"/>
        <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Http"/>
        <PackageReference Include="Microsoft.Extensions.Logging.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Options"/>
        <PackageReference Include="Microsoft.Extensions.Options.ConfigurationExtensions"/>
    </ItemGroup>
    <ItemGroup>
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App.HA.Etcd\PuzzleServer.Infrastructure.App.HA.Etcd.csproj"/>
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App\PuzzleServer.Infrastructure.App.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <InternalsVisibleTo Include="PuzzleServer.UnitTests"/>
    </ItemGroup>

</Project>
```

- [ ] Создать `HaValkeyOptions.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Valkey;

/// <summary>Режим фоновой актуализации (образец HA.Kafka, docs/01.19)</summary>
public enum HaValkeyRefreshMode
{
    /// <summary>Короткоживущие watch-стримы /v3/watch: событие по префиксу кластера → форс-рефетч</summary>
    WatchLongPoll,

    /// <summary>Периодический полный рефетч всех префиксов</summary>
    Poll,
}

/// <summary>Режим слежения за составом etcd-кластера (семантика docs/01.18)</summary>
public enum HaValkeyMembersMode
{
    /// <summary>Периодический member/list + внеплановый опрос при отказе активного endpoint</summary>
    Poll,

    /// <summary>member/list только после отказа активного endpoint</summary>
    OnFailure,

    /// <summary>Состав фиксирован конфигом — монитора нет в DI вовсе</summary>
    Off,
}

/// <summary>Конфигурация секции "HaValkey". Кластеры секцией НЕ задаются — только заявками AddValkeyCluster.</summary>
public class HaValkeyOptions
{
    public HaValkeyRefreshMode Mode { get; set; } = HaValkeyRefreshMode.WatchLongPoll;

    /// <summary>Seed-адреса etcd (HTTP JSON gateway): точка первого соединения и вечный алиас</summary>
    public string[] EtcdEndpoints { get; set; } = [];

    /// <summary>Наполняется PostConfigure из реестра заявок AddValkeyCluster; секцией не задаётся</summary>
    public string[] Clusters { get; set; } = [];

    public int RequestTimeoutMs { get; set; } = 2000;

    // --- WatchLongPoll ---
    public int WatchWindowMs { get; set; } = 1000;
    public int WatchReopenDelayMs { get; set; } = 100;
    public int WatchErrorDelayMs { get; set; } = 1000;

    // --- Poll ---
    public int PollIntervalMs { get; set; } = 1000;

    // --- Members (node discovery) ---
    public HaValkeyMembersMode MembersMode { get; set; } = HaValkeyMembersMode.Poll;
    public int MembersPollIntervalMs { get; set; } = 30_000;
    public int MembersMinIntervalMs { get; set; } = 1_000;

    // --- Общее ---
    public int BootstrapTimeoutSec { get; set; } = 15;
}
```

- [ ] Создать `HaValkeyClusterRegistry.cs`:

```csharp
using System.Text.RegularExpressions;

namespace PuzzleServer.Infrastructure.App.HA.Valkey;

/// <summary>
/// Реестр заявок valkey-кластеров (паттерн ConfigurationTopologyRegistry):
/// заявки дают флуент-вызовы AddValkeyCluster после AddHaValkey; PostConfigure
/// переносит их в HaValkeyOptions.Clusters.
/// </summary>
public sealed partial class HaValkeyClusterRegistry
{
    // Имя кластера — ^[a-z][a-z0-9_]{0,62}$ (arch/20 §1: как pg/kafka, без дефиса)
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterName();

    private readonly List<string> _clusters = [];

    public IReadOnlyList<string> Clusters => _clusters;

    public void Add(string cluster)
    {
        if (string.IsNullOrWhiteSpace(cluster))
            throw new InvalidOperationException("HA.Valkey: имя valkey-кластера не задано");
        if (!ClusterName().IsMatch(cluster))
            throw new InvalidOperationException(
                $"HA.Valkey: недопустимое имя кластера '{cluster}' (нужен ^[a-z][a-z0-9_]{{0,62}}$, arch/20)");
        if (_clusters.Contains(cluster))
            throw new InvalidOperationException($"HA.Valkey: кластер '{cluster}' уже заявлен");
        _clusters.Add(cluster);
    }
}
```

- [ ] Создать `HaValkeyException.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Valkey;

/// <summary>Ошибки библиотеки HA.Valkey для Result.Failed</summary>
public sealed class HaValkeyException(string message) : Exception(message);
```

- [ ] Добавить в `src/PuzzleServer.Api.slnx` внутри `<Folder Name="/Infrastructure/">`, строкой после `PuzzleServer.Infrastructure.App.HA.Kafka`:

```xml
        <Project Path="PuzzleServer.Infrastructure.App.HA.Valkey/PuzzleServer.Infrastructure.App.HA.Valkey.csproj" />
```

**Выход:** пустой проект библиотеки в решении; опции/реестр/исключение готовы к использованию позже.

**Шаги исполнения (проверки):**

- [ ] Сборка: `cd /Users/demakaev/ZCodeProject/Puzzle && dotnet build src/PuzzleServer.Api.slnx` — успех, 0 warnings.
- [ ] Коммит:

```bash
git add src/PuzzleServer.Infrastructure.App.HA.Valkey/PuzzleServer.Infrastructure.App.HA.Valkey.csproj \
        src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyOptions.cs \
        src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyClusterRegistry.cs \
        src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyException.cs \
        src/PuzzleServer.Api.slnx
git commit -m "feat: HA.Valkey — каркас проекта (опции секции HaValkey, реестр заявок, исключение), подключение в slnx"
```

---

### Шаг 4: Модель снапшота (TDD)

**Связь со spec:** §3.3 (модель, GetClientConfig, редакция пароля, Ssl=false), §6 п.2 (критерии юнит-тестов модели).

**Вход (предусловие):** Шаг 3 закоммичен (проект в решении).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/Model/ValkeyAppSecret.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/Model/ValkeyClientConfig.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/Model/ValkeyClusterSnapshot.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Valkey/ModelTests.cs`
- Modify: `src/PuzzleServer.UnitTests/PuzzleServer.UnitTests.csproj` (ProjectReference)

**Interfaces (Produces):** `ValkeyClusterSnapshot(string Cluster, string? State, string? Endpoints, ValkeyAppSecret? App, DateTimeOffset FetchedAtUtc, long Revision)` с `bool HasAppSecret` и `ValkeyClientConfig? GetClientConfig()`; `ValkeyAppSecret(string Username, string Password)`; `ValkeyClientConfig(string Endpoints, string Username, string Password, bool Ssl)` с `const bool SslValue = false`.

**Шаги исполнения:**

- [ ] 1. Добавить в `src/PuzzleServer.UnitTests/PuzzleServer.UnitTests.csproj` (в ItemGroup ProjectReference, рядом с HA.Kafka):

```xml
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App.HA.Valkey\PuzzleServer.Infrastructure.App.HA.Valkey.csproj"/>
```

- [ ] 2. Написать падающий тест `src/PuzzleServer.UnitTests/HA/Valkey/ModelTests.cs`:

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;
using Xunit;

namespace PuzzleServer.UnitTests.HA.Valkey;

// Модель снапшота: null-кейсы GetClientConfig, plain-поля, редакция пароля
// (спека §3.3, arch/20 §4)
public class ModelTests
{
    private static readonly ValkeyAppSecret Secret =
        new("app", "abcdefghijklmnopqrstuvwxyz012345");

    private static ValkeyClusterSnapshot Snapshot(
        string? endpoints = "host.docker.internal:17001",
        ValkeyAppSecret? app = null)
        => new("cache", null, endpoints, app ?? Secret, DateTimeOffset.UtcNow, 7);

    [Fact]
    public void GetClientConfig_Full_PlainFields_SslFalse()
    {
        // Arrange — полный набор: endpoints + секрет
        var snapshot = Snapshot();

        // Act
        var config = snapshot.GetClientConfig();

        // Assert — plain-поля по arch/20 §4: ssl=false в v1
        config.Should().NotBeNull();
        config!.Endpoints.Should().Be("host.docker.internal:17001");
        config.Username.Should().Be("app");
        config.Password.Should().Be("abcdefghijklmnopqrstuvwxyz012345");
        config.Ssl.Should().BeFalse();
        ValkeyClientConfig.SslValue.Should().BeFalse();
    }

    [Fact]
    public void GetClientConfig_Null_WhenNoEndpoints()
    {
        // Arrange — ключа endpoints нет (null)
        var snapshot = Snapshot(endpoints: null);

        // Act
        var config = snapshot.GetClientConfig();

        // Assert — потребитель обязан проверить null (arch/20 §4)
        config.Should().BeNull();
    }

    [Fact]
    public void GetClientConfig_Null_WhenNoSecret()
    {
        // Arrange — неполный набор кредов → App = null
        var snapshot = Snapshot(app: null);

        // Act / Assert
        snapshot.HasAppSecret.Should().BeFalse();
        snapshot.GetClientConfig().Should().BeNull();
    }

    [Fact]
    public void State_DefaultIsActive_RawStringSurvives()
    {
        // Arrange — State: null = Active; незнакомое значение — raw-строкой (§5)
        var active = new ValkeyClusterSnapshot("cache", null, "h:1", Secret, DateTimeOffset.UtcNow, 1);
        var weird = new ValkeyClusterSnapshot("cache", "SOMETHING_NEW", "h:1", Secret, DateTimeOffset.UtcNow, 1);

        // Act / Assert
        active.State.Should().BeNull();
        weird.State.Should().Be("SOMETHING_NEW");
    }

    [Fact]
    public void SecretToString_RedactsPassword()
    {
        // Arrange / Act
        var text = Secret.ToString();

        // Assert — пароль не светится в логах/дампах (спека §3.3)
        text.Should().NotContain("abcdefghijklmnopqrstuvwxyz012345");
        text.Should().Contain("Password = ***");
        text.Should().Contain("Username = app");
    }

    [Fact]
    public void ClientConfigToString_RedactsPassword()
    {
        // Arrange
        var config = Snapshot().GetClientConfig()!;

        // Act
        var text = config.ToString();

        // Assert
        text.Should().NotContain("abcdefghijklmnopqrstuvwxyz012345");
        text.Should().Contain("Password = ***");
        text.Should().Contain("Endpoints = host.docker.internal:17001");
    }
}
```

- [ ] 3. Прогнать — падает: `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.HA.Valkey"` → ошибки компиляции (типы не определены).
- [ ] 4. Создать `Model/ValkeyAppSecret.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Valkey.Model;

/// <summary>
/// Per-cluster ACL-креды приложения (app_user + app_password, arch/20 §4).
/// В снапшот попадают только полным набором обоих ключей.
/// </summary>
public sealed record ValkeyAppSecret(string Username, string Password)
{
    // Редакция секрета: пароль не светится в логах/дампах
    public override string ToString() => $"{nameof(ValkeyAppSecret)} {{ Username = {Username}, Password = *** }}";
}
```

- [ ] 5. Создать `Model/ValkeyClientConfig.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Valkey.Model;

/// <summary>
/// Plain-параметры клиента valkey (arch/20 §4): без зависимости от пакетов —
/// потребитель (будущий клиентский модуль Valkey, t08) собирает из них
/// ConfigurationOptions StackExchange.Redis.
/// </summary>
public sealed record ValkeyClientConfig(
    string Endpoints,
    string Username,
    string Password,
    bool Ssl)
{
    /// <summary>ssl контракта (arch/20 §4: v1 без TLS; ssl=true + CA — после t06)</summary>
    public const bool SslValue = false;

    // Редакция секрета: пароль не светится
    public override string ToString()
        => $"{nameof(ValkeyClientConfig)} {{ Endpoints = {Endpoints}, "
           + $"Username = {Username}, Password = ***, Ssl = {Ssl} }}";
}
```

- [ ] 6. Создать `Model/ValkeyClusterSnapshot.cs`:

```csharp
namespace PuzzleServer.Infrastructure.App.HA.Valkey.Model;

/// <summary>
/// Иммутабельный снапшот valkey-кластера из etcd. State — raw-строка
/// config.state (null = Active, arch/20 §5); Revision — max(mod_revision)
/// ответа, start_revision следующего watch-окна. Коллекционных полей нет
/// (topics в домене отсутствует — arch/20 §2), поэтому структурное сравнение
/// содержимого (условие корректности события Updated) — чисто строковое,
/// ValkeyDiscoveryStore.SameContent (паттерн ha-db, docs/01.17).
/// </summary>
public sealed record ValkeyClusterSnapshot(
    string Cluster,
    string? State,
    string? Endpoints,
    ValkeyAppSecret? App,
    DateTimeOffset FetchedAtUtc,
    long Revision)
{
    public bool HasAppSecret => App is not null;

    // null при отсутствии endpoints ИЛИ секрета — потребитель обязан проверить (arch/20 §4)
    public ValkeyClientConfig? GetClientConfig()
        => Endpoints is null || App is null
            ? null
            : new ValkeyClientConfig(
                Endpoints,
                App.Username,
                App.Password,
                ValkeyClientConfig.SslValue);
}
```

- [ ] 7. Прогнать: `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.HA.Valkey"` — 6/6 PASS.
- [ ] 8. Коммит:

```bash
git add src/PuzzleServer.Infrastructure.App.HA.Valkey/Model \
        src/PuzzleServer.UnitTests/HA/Valkey/ModelTests.cs \
        src/PuzzleServer.UnitTests/PuzzleServer.UnitTests.csproj
git commit -m "feat: HA.Valkey — модель снапшота (ValkeyClusterSnapshot/ValkeyAppSecret/ValkeyClientConfig, GetClientConfig ssl=false) + unit-тесты"
```

**Выход:** модель с редацией пароля; юнит-тесты зелёные.

**Проверка:** фильтр-прогон `UnitTests.HA.Valkey` зелёный; `dotnet build src/PuzzleServer.Api.slnx` — 0 warnings.

---

### Шаг 5: Парсер /valkey/clusters/<C>/ (TDD)

**Связь со spec:** §3.4 (таблица ключей и поведение), §6 п.2 (юнит-критерии по arch/20 §2.1), §7 (риск ca_pem → unknownKeys уже сейчас).

**Вход (предусловие):** Шаг 4 закоммичен (модель существует).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/Parsing/ValkeyClusterParser.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Valkey/ValkeyClusterParserTests.cs`

**Interfaces (Produces):** `ValkeyClusterData Parse(string cluster, IReadOnlyList<EtcdKv> kvs, out IReadOnlyList<string> parseErrors, out IReadOnlyList<string> unknownKeys)`; `record ValkeyClusterData(string? State, string? Endpoints, ValkeyAppSecret? App)`. Потребляет `EtcdKv` из HA.Etcd.

**Шаги исполнения:**

- [ ] 1. Написать падающий тест `ValkeyClusterParserTests.cs`:

```csharp
using FluentAssertions;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Valkey.Parsing;
using Xunit;

namespace PuzzleServer.UnitTests.HA.Valkey;

// Парсер /valkey/clusters/<C>/ — канонические значения arch/20 §2.1 и толерантность §5
public class ValkeyClusterParserTests
{
    // Arrange-хелпер: kv по относительному ключу (префикс /valkey/clusters/cache/)
    private static EtcdKv Kv(string relativeKey, string value, long modRevision = 1)
        => new($"/valkey/clusters/cache/{relativeKey}", value, modRevision);

    [Fact]
    public void Parse_CanonicalActiveCluster_EndpointsSecret()
    {
        // Arrange — канон arch/20 §2.1: Active-config БЕЗ state + endpoints + креды
        var kvs = new[]
        {
            Kv("config", """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}"""),
            Kv("endpoints", "host.docker.internal:17001"),
            Kv("app_user", "app"),
            Kv("app_password", "abcdefghijklmnopqrstuvwxyz012345"),
        };

        // Act
        var data = ValkeyClusterParser.Parse("cache", kvs, out var parseErrors, out var unknownKeys);

        // Assert
        data.State.Should().BeNull(); // нет поля state = Active (arch/20 §2)
        data.Endpoints.Should().Be("host.docker.internal:17001");
        data.App.Should().NotBeNull();
        data.App!.Username.Should().Be("app");
        data.App.Password.Should().Be("abcdefghijklmnopqrstuvwxyz012345");
        parseErrors.Should().BeEmpty();
        unknownKeys.Should().BeEmpty();
    }

    [Fact]
    public void Parse_ClusterStates_RawString()
    {
        // Arrange — заявочный config с state / TO_REMOVE / незнакомое значение (§2.1, §5)
        var notInitialized = new[] { Kv("config", """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"NOT_INITIALIZED"}""") };
        var toRemove = new[] { Kv("config", """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}""") };
        var future = new[] { Kv("config", """{"nodes":1,"created_unix":1,"state":"SOMETHING_NEW"}""") };

        // Act
        var a = ValkeyClusterParser.Parse("cache", notInitialized, out _, out _);
        var b = ValkeyClusterParser.Parse("cache", toRemove, out _, out _);
        var c = ValkeyClusterParser.Parse("cache", future, out _, out _);

        // Assert — незнакомое state толерантно, raw-строкой (§5)
        a.State.Should().Be("NOT_INITIALIZED");
        b.State.Should().Be("TO_REMOVE");
        c.State.Should().Be("SOMETHING_NEW");
    }

    [Theory]
    [InlineData("app_user")]
    [InlineData("app_password")]
    public void Parse_IncompleteSecret_AppIsNull(string presentKey)
    {
        // Arrange — неполный набор кредов: секрета нет (§4, §5)
        var kvs = new[] { Kv(presentKey, "value") };

        // Act
        var data = ValkeyClusterParser.Parse("cache", kvs, out _, out _);

        // Assert
        data.App.Should().BeNull();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_EmptyEndpoints_TreatedAsMissing(string endpoints)
    {
        // Arrange — пустой/пробельный endpoints = отсутствующий (§5)
        var kvs = new[] { Kv("endpoints", endpoints) };

        // Act
        var data = ValkeyClusterParser.Parse("cache", kvs, out _, out _);

        // Assert
        data.Endpoints.Should().BeNull();
    }

    [Fact]
    public void Parse_BrokenConfigJson_ParseErrorActiveBranch()
    {
        // Arrange — битый JSON: ключ пропускается, парсер не падает (§5);
        // кластер остаётся жив (Active-ветка), endpoints читаются
        var kvs = new[]
        {
            Kv("config", "{not-json"),
            Kv("endpoints", "host.docker.internal:17001"),
        };

        // Act
        var data = ValkeyClusterParser.Parse("cache", kvs, out var parseErrors, out _);

        // Assert
        data.State.Should().BeNull();
        data.Endpoints.Should().Be("host.docker.internal:17001");
        parseErrors.Should().ContainSingle().Which.Should().Contain("config");
    }

    [Fact]
    public void Parse_AdminAndNodesSkippedSilently()
    {
        // Arrange — admin-креды и nodes/* — известные ключи ВНЕ клиентского
        // подмножества §4: молча, не unknownKeys (§5)
        var kvs = new[]
        {
            Kv("admin_user", "admin"),
            Kv("admin_password", "ZYXWVUTSRQPONMLKJIHGFEDCBA987654"),
            Kv("nodes/node1/state", "RUNNING"),
            Kv("nodes/node1/resources", """{"cpu":"2","mem":"4Gi","disk":"40Gi"}"""),
            Kv("endpoints", "host.docker.internal:17001"),
        };

        // Act
        var data = ValkeyClusterParser.Parse("cache", kvs, out var parseErrors, out var unknownKeys);

        // Assert
        data.Endpoints.Should().Be("host.docker.internal:17001");
        parseErrors.Should().BeEmpty();
        unknownKeys.Should().BeEmpty();
    }

    [Fact]
    public void Parse_UnknownKeys_Counter_IncludingFutureCaPem()
    {
        // Arrange — неизвестные ключи: лог + счётчик unknownKeys, парсер не
        // падает (§5); будущий ca_pem после t06 попадает сюда же (§3 риски)
        var kvs = new[]
        {
            Kv("something/new", "value"),
            Kv("ca_pem", "-----BEGIN CERTIFICATE-----"),
            Kv("endpoints", "host.docker.internal:17001"),
        };

        // Act
        var data = ValkeyClusterParser.Parse("cache", kvs, out _, out var unknownKeys);

        // Assert
        data.Endpoints.Should().Be("host.docker.internal:17001");
        unknownKeys.Should().HaveCount(2);
        unknownKeys.Should().Contain("/valkey/clusters/cache/ca_pem");
    }

    [Fact]
    public void Parse_ForeignPrefixSkipped()
    {
        // Arrange — чужой ключ в ответе не бывает (range по префиксу), но
        // защита по сегментам обязательна (паттерн kafka-парсера)
        var kvs = new[] { new EtcdKv("/valkey/clusters/other/endpoints", "h:1", 1) };

        // Act
        var data = ValkeyClusterParser.Parse("cache", kvs, out _, out _);

        // Assert — чужой кластер мимо
        data.Endpoints.Should().BeNull();
    }
}
```

- [ ] 2. Прогнать — падает (тип `ValkeyClusterParser` не определён).
- [ ] 3. Создать `Parsing/ValkeyClusterParser.cs`:

```csharp
using System.Text.Json;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;

namespace PuzzleServer.Infrastructure.App.HA.Valkey.Parsing;

/// <summary>Разобранные данные кластера (без Revision/FetchedAtUtc — их добавляет store)</summary>
public sealed record ValkeyClusterData(
    string? State,
    string? Endpoints,
    ValkeyAppSecret? App);

// Чистые функции разбора range-ответа /valkey/clusters/<C>/ (arch/20 §2, §4–§5).
// Сегменты: key.Split('/') с ведущим пустым — факт-ключи кластера = 5 сегментов
// (/valkey/clusters/<C>/<leaf>), nodes/<k>/<...> — 7. Разбор — JsonDocument с
// ручным чтением state: JsonSerializerOptions НЕ заводить без использования —
// CS0414 под TreatWarningsAsErrors=true (паттерн kafka-парсера).
public static class ValkeyClusterParser
{
    public static ValkeyClusterData Parse(
        string cluster,
        IReadOnlyList<EtcdKv> kvs,
        out IReadOnlyList<string> parseErrors,
        out IReadOnlyList<string> unknownKeys)
    {
        var errors = new List<string>();
        var unknown = new List<string>();
        string? state = null;
        string? endpoints = null;
        string? appUser = null;
        string? appPassword = null;

        foreach (var kv in kvs)
        {
            var segments = kv.Key.Split('/');
            // ожидаем /valkey/clusters/<C>/...: [0]="", [1]="valkey", [2]="clusters", [3]=C
            if (segments.Length < 5 || segments[1] != "valkey" || segments[2] != "clusters"
                || segments[3] != cluster)
            {
                continue; // чужой ключ в ответе не бывает (range по префиксу), на всякий случай — мимо
            }

            switch (segments[4])
            {
                case "config" when segments.Length == 5:
                    state = ParseConfigState(kv.Value, errors);
                    break;
                case "endpoints" when segments.Length == 5:
                    endpoints = NormalizeEndpoints(kv.Value);
                    break;
                case "app_user" when segments.Length == 5:
                    appUser = kv.Value;
                    break;
                case "app_password" when segments.Length == 5:
                    appPassword = kv.Value;
                    break;
                case "admin_user":
                case "admin_password":
                case "nodes":
                    break; // известные ключи вне клиентского подмножества §4 — молча (§5)
                default:
                    unknown.Add(kv.Key); // в т.ч. будущий ca_pem после t06 — читатель совместим
                    break;
            }
        }

        parseErrors = errors;
        unknownKeys = unknown;

        return new ValkeyClusterData(
            state,
            endpoints,
            // креды — только полным набором обоих ключей (arch/20 §4)
            appUser is not null && appPassword is not null ? new ValkeyAppSecret(appUser, appPassword) : null);
    }

    // config: только state (raw-строкой, §5); остальное клиенту не нужно (§4)
    private static string? ParseConfigState(string value, List<string> errors)
    {
        try
        {
            using var doc = JsonDocument.Parse(value);
            return doc.RootElement.TryGetProperty("state", out var state)
                && state.ValueKind == JsonValueKind.String
                    ? state.GetString()
                    : null;
        }
        catch (JsonException)
        {
            errors.Add($"config: битый JSON ({Redact(value)})");
            return null; // битый config → Active-ветка, кластер в снапшоте жив (§5)
        }
    }

    // Пустой/пробельный endpoints — как отсутствующий (§5)
    private static string? NormalizeEndpoints(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value;

    // Значение ключа в ошибки — укороченное и без подозрительных данных
    private static string Redact(string value)
        => value.Length <= 40 ? value : value[..40] + "…";
}
```

- [ ] 4. Прогнать: `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.HA.Valkey"` — все PASS (модель + парсер).
- [ ] 5. Коммит:

```bash
git add src/PuzzleServer.Infrastructure.App.HA.Valkey/Parsing \
        src/PuzzleServer.UnitTests/HA/Valkey/ValkeyClusterParserTests.cs
git commit -m "feat: HA.Valkey — парсер /valkey/clusters/<C>/ (config.state raw, endpoints, креды; admin/nodes молча; unknownKeys) + unit-тесты по arch/20 §2.1"
```

**Выход:** парсер с полной толерантностью §5; юнит-тесты зелёные.

**Проверка:** фильтр-прогон зелёный; в тестах покрыты все строки таблицы §3.4 spec.

---

### Шаг 6: Стор дискавери (TDD)

**Связь со spec:** §3.5 (IValkeyDiscoveryStore/ValkeyDiscoveryStore: Get/RefreshAsync/Updated/SameContent/fail-open), §3.3 (SameContent без FetchedAtUtc/Revision), §6 п.4 (Updated не стреляет на put тем же содержимым).

**Вход (предусловие):** Шаг 5 закоммичен (парсер + модель существуют).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/ValkeyDiscoveryStore.cs`
- Create: `src/PuzzleServer.UnitTests/HA/Valkey/FakeEtcdClient.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Valkey/ValkeyDiscoveryStoreTests.cs`

**Interfaces (Produces):**

```csharp
public interface IValkeyDiscoveryStore
{
    Result<ValkeyClusterSnapshot> Get(string cluster);
    event Action<ValkeyClusterSnapshot>? Updated;
    Task<Result<ValkeyClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct);
}
```

`[InjectAsSingleton] ValkeyDiscoveryStore(IEtcdClient, EtcdEndpointRotation, IOptions<HaValkeyOptions>, ILogger<ValkeyDiscoveryStore>)`.

**Шаги исполнения:**

- [ ] 1. Скопировать `src/PuzzleServer.UnitTests/HA/Kafka/FakeEtcdClient.cs` → `src/PuzzleServer.UnitTests/HA/Valkey/FakeEtcdClient.cs` с заменами (все вхождения): namespace `PuzzleServer.UnitTests.HA.Kafka` → `PuzzleServer.UnitTests.HA.Valkey`; комментарий «префикс кластера: /kafka/clusters/<C>/» → «/valkey/clusters/<C>/»; заголовочный комментарий «для unit-тестов HA.Kafka» → «HA.Valkey». Логика — без изменений (Range-словарь, счётчики, отказы, watch-канал).
- [ ] 2. Написать падающий тест `ValkeyDiscoveryStoreTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Valkey;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;
using Xunit;

namespace PuzzleServer.UnitTests.HA.Valkey;

// ValkeyDiscoveryStore: кэш + событие только при ИЗМЕНЕНИИ СОДЕРЖИМОГО +
// RefreshAsync + fail-open (спека §3.5, §3.3; arch/20 §5)
public class ValkeyDiscoveryStoreTests
{
    // Заготовка kv-набора (архетип arch/20 §2.1: Active-кластер)
    private static IReadOnlyList<EtcdKv> ClusterKvs(string endpoints = "h1:6379", long modRevision = 10)
        => new[]
        {
            new EtcdKv("/valkey/clusters/cache/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""", modRevision),
            new EtcdKv("/valkey/clusters/cache/endpoints", endpoints, modRevision),
            new EtcdKv("/valkey/clusters/cache/app_user", "app", modRevision),
            new EtcdKv("/valkey/clusters/cache/app_password", "abcdefghijklmnopqrstuvwxyz012345", modRevision),
        };

    private static ValkeyDiscoveryStore BuildStore(FakeEtcdClient client, params string[] clusters)
    {
        var options = Microsoft.Extensions.Options.Options.Create(new HaValkeyOptions { Clusters = clusters });
        return new ValkeyDiscoveryStore(
            client, new EtcdEndpointRotation(["http://etcd:2379"]), options,
            NullLoggerFactory.Instance.CreateLogger<ValkeyDiscoveryStore>());
    }

    [Fact]
    public async Task RefreshAsync_ThenGet_ReturnsSnapshotWithoutNetwork()
    {
        // Arrange
        var client = new FakeEtcdClient();
        client.SetRange("/valkey/clusters/cache/", ClusterKvs());
        var store = BuildStore(client, "cache");

        // Act
        var refresh = await store.RefreshAsync("cache", CancellationToken.None);
        var rangeCallsAfterRefresh = client.RangeCalls;
        var get = store.Get("cache");

        // Assert — Get не ходит в сеть (спека §3.5)
        refresh.IsSuccess.Should().BeTrue();
        get.IsSuccess.Should().BeTrue();
        get.Value.Endpoints.Should().Be("h1:6379");
        get.Value.GetClientConfig().Should().NotBeNull();
        client.RangeCalls.Should().Be(rangeCallsAfterRefresh);
    }

    [Fact]
    public async Task Get_Failed_ForUndeclaredCluster()
    {
        // Arrange
        var store = BuildStore(new FakeEtcdClient(), "cache");

        // Act
        var get = store.Get("other");

        // Assert — незаявленный кластер → Result.Failed с HaValkeyException
        get.IsSuccess.Should().BeFalse();
        get.Error.Should().BeOfType<HaValkeyException>();
        (await store.RefreshAsync("other", CancellationToken.None)).IsSuccess.Should().BeFalse();
    }

    [Fact]
    public void Get_Failed_WhenSnapshotNotReady()
    {
        // Arrange — заявлен, но bootstrap не завершён
        var store = BuildStore(new FakeEtcdClient(), "cache");

        // Act
        var get = store.Get("cache");

        // Assert
        get.IsSuccess.Should().BeFalse();
        get.Error.Should().BeOfType<HaValkeyException>();
    }

    [Fact]
    public async Task Updated_FiresOnlyOnContentChange()
    {
        // Arrange
        var client = new FakeEtcdClient();
        client.SetRange("/valkey/clusters/cache/", ClusterKvs());
        var store = BuildStore(client, "cache");
        var fired = 0;
        store.Updated += _ => fired++;

        // Act
        await store.RefreshAsync("cache", CancellationToken.None);   // первое появление → событие
        await store.RefreshAsync("cache", CancellationToken.None);   // тот же ответ, ревизия та же → НЕТ события
        client.SetRange("/valkey/clusters/cache/", ClusterKvs("h9:6379", 11));
        await store.RefreshAsync("cache", CancellationToken.None);   // изменение → событие

        // Assert
        fired.Should().Be(2);
        store.Get("cache").Value.Endpoints.Should().Be("h9:6379");
    }

    [Fact]
    public async Task Updated_NotFired_WhenSameContent_RefetchedWithHigherRevision()
    {
        // Arrange — ГЛАВНЫЙ тест SameContent: put ТЕМ ЖЕ значением (ревизия
        // растёт) — события НЕТ; FetchedAtUtc/Revision не участвуют (спека §3.3)
        var client = new FakeEtcdClient();
        var store = BuildStore(client, "cache");
        var fired = 0;
        store.Updated += _ => fired++;

        // Act — новый kv-набор с ВЫРОСШЕЙ mod_revision, содержимое то же
        client.SetRange("/valkey/clusters/cache/", ClusterKvs());
        await store.RefreshAsync("cache", CancellationToken.None);
        client.SetRange("/valkey/clusters/cache/", ClusterKvs(modRevision: 99));
        await store.RefreshAsync("cache", CancellationToken.None);

        // Assert — событие ровно один раз (только первое появление снапшота)
        fired.Should().Be(1);
        store.Get("cache").Value.Revision.Should().Be(99); // ревизия обновилась — start_revision watch-окон
    }

    [Fact]
    public async Task RefreshAsync_Failure_KeepsLastSnapshot_FailOpen()
    {
        // Arrange
        var client = new FakeEtcdClient();
        client.SetRange("/valkey/clusters/cache/", ClusterKvs());
        var store = BuildStore(client, "cache");
        await store.RefreshAsync("cache", CancellationToken.None);

        // Act — одноразовый сбой следующего range
        client.SimulateRangeFailure();
        var failed = await store.RefreshAsync("cache", CancellationToken.None);
        var get = store.Get("cache");

        // Assert — fail-open: кэш живёт, RefreshAsync → Failed (спека §3.5)
        failed.IsSuccess.Should().BeFalse();
        get.IsSuccess.Should().BeTrue();
        get.Value.Endpoints.Should().Be("h1:6379");
    }

    [Fact]
    public async Task Updated_SignalsPasswordRotation()
    {
        // Arrange — сценарий ротации arch/21 §5 E: put NEW → событие, новый пароль
        var client = new FakeEtcdClient();
        client.SetRange("/valkey/clusters/cache/", ClusterKvs());
        var store = BuildStore(client, "cache");
        await store.RefreshAsync("cache", CancellationToken.None);
        ValkeyClientConfig? latest = null;
        store.Updated += s => latest = s.GetClientConfig();

        // Act
        client.SetRange("/valkey/clusters/cache/", new[]
        {
            new EtcdKv("/valkey/clusters/cache/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""", 12),
            new EtcdKv("/valkey/clusters/cache/endpoints", "h1:6379", 12),
            new EtcdKv("/valkey/clusters/cache/app_user", "app", 12),
            new EtcdKv("/valkey/clusters/cache/app_password", "ZYXWVUTSRQPONMLKJIHGFEDCBA987654", 12),
        });
        await store.RefreshAsync("cache", CancellationToken.None);

        // Assert
        latest.Should().NotBeNull();
        latest!.Password.Should().Be("ZYXWVUTSRQPONMLKJIHGFEDCBA987654");
    }

    [Fact]
    public async Task RefreshAsync_ParsesTolerantly_AndLogsUnknownKeys()
    {
        // Arrange — битый config + неизвестный ключ: снапшот собирается (§5)
        var client = new FakeEtcdClient();
        client.SetRange("/valkey/clusters/cache/", new[]
        {
            new EtcdKv("/valkey/clusters/cache/config", "{not-json", 1),
            new EtcdKv("/valkey/clusters/cache/endpoints", "h1:6379", 1),
            new EtcdKv("/valkey/clusters/cache/app_user", "app", 1),
            new EtcdKv("/valkey/clusters/cache/app_password", "abcdefghijklmnopqrstuvwxyz012345", 1),
            new EtcdKv("/valkey/clusters/cache/ca_pem", "-----BEGIN CERTIFICATE-----", 1),
        });
        var store = BuildStore(client, "cache");

        // Act
        var refresh = await store.RefreshAsync("cache", CancellationToken.None);

        // Assert — толерантность: кластер жив (Active-ветка), unknownKeys не роняют рефетч
        refresh.IsSuccess.Should().BeTrue();
        refresh.Value.State.Should().BeNull();
        refresh.Value.GetClientConfig().Should().NotBeNull();
    }
}
```

- [ ] 3. Прогнать — падает (тип `ValkeyDiscoveryStore` не определён).
- [ ] 4. Создать `ValkeyDiscoveryStore.cs` (полный код; по структуре — копия `KafkaDiscoveryStore` без Topics):

```csharp
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;
using PuzzleServer.Infrastructure.App.HA.Valkey.Parsing;

namespace PuzzleServer.Infrastructure.App.HA.Valkey;

// Публичный контракт кэша дискавери (спека §3.5)
public interface IValkeyDiscoveryStore
{
    Result<ValkeyClusterSnapshot> Get(string cluster);
    event Action<ValkeyClusterSnapshot>? Updated;
    Task<Result<ValkeyClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct);
}

// Кэш снапшотов по кластерам: Get — мгновенно, RefreshAsync — полный рефетч
// с атомарной заменой и событием только при фактическом изменении содержимого.
[InjectAsSingleton]
public sealed class ValkeyDiscoveryStore(
    IEtcdClient etcdClient,
    EtcdEndpointRotation rotation,
    IOptions<HaValkeyOptions> options,
    ILogger<ValkeyDiscoveryStore> logger) : IValkeyDiscoveryStore
{
    private readonly object _sync = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly Dictionary<string, ValkeyClusterSnapshot> _snapshots = new();

    public event Action<ValkeyClusterSnapshot>? Updated;

    public Result<ValkeyClusterSnapshot> Get(string cluster)
    {
        lock (_sync)
        {
            if (!_snapshots.TryGetValue(cluster, out var snapshot))
                return Result<ValkeyClusterSnapshot>.Failed(new HaValkeyException(
                    IsDeclared(cluster)
                        ? $"снапшот valkey-кластера {cluster} ещё не готов (bootstrap не завершён)"
                        : $"valkey-кластер {cluster} не заявлен (AddValkeyCluster)"));
            return Result<ValkeyClusterSnapshot>.Success(snapshot);
        }
    }

    public async Task<Result<ValkeyClusterSnapshot>> RefreshAsync(string cluster, CancellationToken ct)
    {
        if (!IsDeclared(cluster))
            return Result<ValkeyClusterSnapshot>.Failed(
                new HaValkeyException($"valkey-кластер {cluster} не заявлен (AddValkeyCluster)"));

        await _refreshGate.WaitAsync(ct);
        try
        {
            var fetched = await FetchAsync(cluster, ct);
            fetched.Apply(snapshot => Publish(cluster, snapshot));
            return fetched;
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private bool IsDeclared(string cluster)
        => options.Value.Clusters.Contains(cluster);

    // Один проход = один range на одном endpoint (консистентность ревизии), ротация при отказе
    private async Task<Result<ValkeyClusterSnapshot>> FetchAsync(string cluster, CancellationToken ct)
    {
        var endpoint = rotation.GetActive();
        var prefix = $"/valkey/clusters/{cluster}/";

        var kvs = await etcdClient.RangeAsync(endpoint, prefix, ct);
        if (!kvs.IsSuccess)
        {
            rotation.ReportFailure(endpoint);
            logger.LogWarning(kvs.Error, "HA.Valkey: range {Prefix} на {Endpoint} провалился", prefix, endpoint);
            return Result<ValkeyClusterSnapshot>.Failed(kvs.Error!);
        }

        rotation.ReportSuccess(endpoint);
        var revision = kvs.Value.Count == 0 ? 0 : kvs.Value.Max(kv => kv.ModRevision);
        var data = ValkeyClusterParser.Parse(cluster, kvs.Value, out var parseErrors, out var unknownKeys);
        foreach (var error in parseErrors)
            logger.LogWarning("HA.Valkey: кластер {Cluster}: {Error}", cluster, error);
        if (unknownKeys.Count > 0)
            logger.LogWarning("HA.Valkey: кластер {Cluster}: неизвестные ключи ({Count}): {Keys}",
                cluster, unknownKeys.Count, string.Join(", ", unknownKeys));

        return Result<ValkeyClusterSnapshot>.Success(new ValkeyClusterSnapshot(
            cluster, data.State, data.Endpoints, data.App,
            DateTimeOffset.UtcNow, revision));
    }

    // Замена снапшота; Updated стреляет только при изменении содержимого.
    // FetchedAtUtc/Revision обновляются всегда — start_revision следующего watch-окна.
    private void Publish(string cluster, ValkeyClusterSnapshot snapshot)
    {
        Action<ValkeyClusterSnapshot>? handlers = null;
        lock (_sync)
        {
            var changed = !_snapshots.TryGetValue(cluster, out var old) || !SameContent(old, snapshot);
            _snapshots[cluster] = snapshot;
            if (changed)
                handlers = Updated;
        }

        if (handlers is null)
            return;

        foreach (var handler in handlers.GetInvocationList().Cast<Action<ValkeyClusterSnapshot>>())
        {
            try
            {
                handler(snapshot);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "HA.Valkey: подписчик Updated кластера {Cluster} бросил исключение", cluster);
            }
        }
    }

    // СТРУКТУРНОЕ сравнение содержимого без FetchedAtUtc/Revision (спека §3.3).
    // Коллекционных полей нет (topics в домене отсутствует — arch/20 §2):
    // сравнение чисто строковое; App — record со строковыми полями (структурно).
    private static bool SameContent(ValkeyClusterSnapshot a, ValkeyClusterSnapshot b)
        => a.Cluster == b.Cluster
           && a.State == b.State
           && a.Endpoints == b.Endpoints
           && a.App == b.App;
}
```

- [ ] 5. Прогнать: `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.HA.Valkey"` — все PASS.
- [ ] 6. Коммит:

```bash
git add src/PuzzleServer.Infrastructure.App.HA.Valkey/ValkeyDiscoveryStore.cs \
        src/PuzzleServer.UnitTests/HA/Valkey/FakeEtcdClient.cs \
        src/PuzzleServer.UnitTests/HA/Valkey/ValkeyDiscoveryStoreTests.cs
git commit -m "feat: HA.Valkey — стор дискавери (Get/RefreshAsync/Updated по SameContent, fail-open) + unit-тесты"
```

**Выход:** рабочий стор с fail-open и семантикой Updated.

**Проверка:** фильтр-прогон зелёный; тест `Updated_NotFired_WhenSameContent_RefetchedWithHigherRevision` фиксирует §6 п.4 spec.

---

### Шаг 7: Сигнальщики + шина актуализации (TDD)

**Связь со spec:** §3.5 (refresher: bootstrap/коалесценция/health; сигнальщики WatchLongPoll и Poll), §2 п.2 (копии HA.Kafka).

**Вход (предусловие):** Шаг 6 закоммичен (стор + FakeEtcdClient существуют).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/Refresh/IHaValkeyRefreshSignaler.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/Refresh/HaValkeyPollRefreshSignaler.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/Refresh/HaValkeyWatchLongPollSignaler.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/ValkeyDiscoveryRefresher.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Valkey/HaValkeyPollRefreshSignalerTests.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Valkey/HaValkeyWatchLongPollSignalerTests.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Valkey/ValkeyDiscoveryRefresherTests.cs`

**Interfaces (Produces):** `IHaValkeyRefreshSignaler { Task RunAsync(ChannelWriter<object> signals, CancellationToken ct); }`; `[InjectAsSingleton] ValkeyDiscoveryRefresher(IValkeyDiscoveryStore, IHaValkeyRefreshSignaler, IOptions<HaValkeyOptions>, ILogger<ValkeyDiscoveryRefresher>) : BackgroundService, IHealthCheckService` с `bool Inited`, `bool Working`, `Result StatusError`, `IValkeyDiscoveryStore Store`.

**Шаги исполнения:**

- [ ] 1. Скопировать `HA.Kafka/Refresh/IHaKafkaRefreshSignaler.cs` → `Refresh/IHaValkeyRefreshSignaler.cs` с заменами: namespace `...HA.Kafka.Refresh` → `...HA.Valkey.Refresh`; `IHaKafkaRefreshSignaler` → `IHaValkeyRefreshSignaler`; комментарии «KafkaDiscoveryRefresher» → «ValkeyDiscoveryRefresher», «IKafkaDiscoveryStore» → «IValkeyDiscoveryStore». Тело интерфейса то же: `Task RunAsync(ChannelWriter<object> signals, CancellationToken ct);`.
- [ ] 2. Скопировать `HA.Kafka/Refresh/HaKafkaPollRefreshSignaler.cs` → `Refresh/HaValkeyPollRefreshSignaler.cs` с заменами: namespace → `.HA.Valkey.Refresh`; `HaKafkaPollRefreshSignaler` → `HaValkeyPollRefreshSignaler`; `IHaKafkaRefreshSignaler` → `IHaValkeyRefreshSignaler`; `HaKafkaOptions` → `HaValkeyOptions`. Тело без изменений (`PeriodicTimer(PollIntervalMs)` → `signals.TryWrite`).
- [ ] 3. Скопировать `HA.Kafka/Refresh/HaKafkaWatchLongPollSignaler.cs` → `Refresh/HaValkeyWatchLongPollSignaler.cs` с заменами (все вхождения): namespace → `.HA.Valkey.Refresh`; `HaKafkaWatchLongPollSignaler` → `HaValkeyWatchLongPollSignaler`; `IHaKafkaRefreshSignaler` → `IHaValkeyRefreshSignaler`; `IKafkaDiscoveryStore` → `IValkeyDiscoveryStore`; `HaKafkaOptions` → `HaValkeyOptions`; строка префикса `"/kafka/clusters/{cluster}/"` → `"/valkey/clusters/{cluster}/"`; строки логов «HA.Kafka:» → «HA.Valkey:». Логика (окна, per-cluster start_revision, Compacted, ротация) — без изменений.
- [ ] 4. Скопировать `HA.Kafka/KafkaDiscoveryRefresher.cs` → `ValkeyDiscoveryRefresher.cs` с заменами (все вхождения): namespace → `.HA.Valkey`; using-строки `.HA.Kafka.Model`/`.HA.Kafka.Refresh` → `.HA.Valkey.Model`/`.HA.Valkey.Refresh`; `KafkaDiscoveryRefresher` → `ValkeyDiscoveryRefresher`; `IKafkaDiscoveryStore`/`IKafkaDiscoveryStore.Store` → `IValkeyDiscoveryStore`; `IHaKafkaRefreshSignaler` → `IHaValkeyRefreshSignaler`; `HaKafkaOptions` → `HaValkeyOptions`; `HaKafkaRefreshMode` → `HaValkeyRefreshMode`; `HaKafkaException` → `HaValkeyException`; `KafkaClusterSnapshot` → `ValkeyClusterSnapshot`; строки логов «HA.Kafka:» → «HA.Valkey:». Единственное содержательное отличие — лог первого снапшота (топиков нет); заменить в `OnRefreshSuccess`:

```csharp
        if (!_firstSuccessLogged)
        {
            _firstSuccessLogged = true;
            logger.LogInformation(
                "HA.Valkey: первый снапшот кластера {Cluster}: endpoints {Endpoints}, ревизия {Revision}",
                cluster, snapshot.Endpoints, snapshot.Revision);
        }
```

- [ ] 5. Скопировать тесты с заменами (все вхождения имён и префиксов; AAA-комментарии сохранить):
  - `UnitTests/HA/Kafka/HaKafkaPollRefreshSignalerTests.cs` → `HA/Valkey/HaValkeyPollRefreshSignalerTests.cs`: namespace → `.HA.Valkey`; `HaKafkaPollRefreshSignaler` → `HaValkeyPollRefreshSignaler`; `HaKafkaOptions` → `HaValkeyOptions`; `IHaKafkaRefreshSignaler` → `IHaValkeyRefreshSignaler`. Тест-тела те же (тики → сигналы; отмена → выход).
  - `UnitTests/HA/Kafka/HaKafkaWatchLongPollSignalerTests.cs` → `HA/Valkey/HaValkeyWatchLongPollSignalerTests.cs`: замены имён как выше + `HaKafkaWatchLongPollSignaler` → `HaValkeyWatchLongPollSignaler`, `KafkaDiscoveryStore` → `ValkeyDiscoveryStore`, `IKafkaDiscoveryStore` → `IValkeyDiscoveryStore`, `HaKafkaException` → `HaValkeyException`, `KafkaClusterSnapshot` → `ValkeyClusterSnapshot`; префиксы и ключи: `"/kafka/clusters/events/"` → `"/valkey/clusters/cache/"`; кластер `"events"` → `"cache"`; kv-набор в `WindowReopens_WithSnapshotRevision_AsStartRevision` заменить на valkey-archетип:

```csharp
        client.SetRange("/valkey/clusters/cache/", new[]
        {
            new EtcdKv("/valkey/clusters/cache/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""", 7),
            new EtcdKv("/valkey/clusters/cache/endpoints", "h1:6379", 7),
        });
```

  и ассерты `c.Prefix == "/valkey/clusters/cache/"`. Событие засева: `client.EnqueueWatchEvent(new EtcdWatchEvent(WatchEventType.Put, "/valkey/clusters/cache/endpoints", 42));`. Вложенные классы `EmptyStore`/`FailingWatchEtcdClient` копируются с заменой типов на valkey.
  - `UnitTests/HA/Kafka/KafkaDiscoveryRefresherTests.cs` → `HA/Valkey/ValkeyDiscoveryRefresherTests.cs`: замены имён как выше (`KafkaDiscoveryRefresher` → `ValkeyDiscoveryRefresher`, `ManualSignaler : IHaValkeyRefreshSignaler`, `HaValkeyOptions`, `ValkeyDiscoveryStore`); хелпер `ClusterKvs`:

```csharp
    private static IReadOnlyList<EtcdKv> ClusterKvs(string endpoints)
        => new[] { new EtcdKv("/valkey/clusters/cache/endpoints", endpoints, 1) };
```

  кластер `"events"` → `"cache"`; префиксы `"/kafka/clusters/events/"` → `"/valkey/clusters/cache/"`; ассерты `BootstrapServers` → `Endpoints`. Сценарии те же: bootstrap-деградация при лежащем etcd; сигнал → рефетч → новый снапшот; коалесценция шторма в один проход; StatusError после ≥2 провалов и сброс успехом.
- [ ] 6. Сборка и прогон: `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.HA.Valkey"` — все PASS (модель, парсер, стор, шина, сигнальщики).
- [ ] 7. Коммит:

```bash
git add src/PuzzleServer.Infrastructure.App.HA.Valkey \
        src/PuzzleServer.UnitTests/HA/Valkey
git commit -m "feat: HA.Valkey — шина актуализации (bootstrap с бюджетом, коалесценция, health) + сигнальщики WatchLongPoll/Poll + unit-тесты"
```

**Выход:** фоновая актуализация обоих режимов с health-семантикой.

**Проверка:** фильтр-прогон зелёный; тест коалесценции (`SignalStorm_DuringSlowPass_CoalescesIntoSingleRefetch`) проходит.

---

### Шаг 8: Подключение модуля — AddHaValkey/AddValkeyCluster + health-check (TDD)

**Связь со spec:** §3.1 (ModuleExtensions, HaValkeyHealthCheck), §3.2 (регистрация, fail-fast, members-монитор, AutoRegistration), §6 п.2 (fail-fast валидации).

**Вход (предусловие):** Шаг 7 закоммичен (стор/шина/сигнальщики существуют).

**Files:**
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyHealthCheck.cs`
- Create: `src/PuzzleServer.Infrastructure.App.HA.Valkey/ModuleExtensions.cs`
- Test: `src/PuzzleServer.UnitTests/HA/Valkey/ModuleRegistrationTests.cs`

**Interfaces (Produces):** `IServiceCollection AddHaValkey(this IServiceCollection, IConfiguration)`; `IServiceCollection AddValkeyCluster(this IServiceCollection, string cluster)`; health-check с именем `"HaValkeyCheck"`; секция конфига `"HaValkey"`.

**Шаги исполнения:**

- [ ] 1. Создать `HaValkeyHealthCheck.cs`:

```csharp
using PuzzleServer.Infrastructure.App.HealthChecks;

namespace PuzzleServer.Infrastructure.App.HA.Valkey;

// Health-check шины актуализации (паттерн HaDbCheck/HaKafkaCheck: пустой класс-наследник)
public class HaValkeyHealthCheck(ValkeyDiscoveryRefresher service)
    : HealthCheckAbstract<ValkeyDiscoveryRefresher>(service)
{
}
```

- [ ] 2. Написать падающий тест `ModuleRegistrationTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Valkey;
using PuzzleServer.Infrastructure.App.HA.Valkey.Refresh;
using Xunit;

namespace PuzzleServer.UnitTests.HA.Valkey;

// Регистрация модуля HA.Valkey: флуент-заявки кластеров, PostConfigure,
// fail-fast, выбор сигнальщика по Mode (спека §3.2)
public class ModuleRegistrationTests
{
    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        // ILogger<> — нужен атрибутным singleton'ам модуля (store/refresher)
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        return services;
    }

    private static IConfiguration Config(Dictionary<string, string?>? extra = null)
    {
        var data = new Dictionary<string, string?>
        {
            ["HaValkey:EtcdEndpoints:0"] = "http://etcd:2379",
        };
        foreach (var (k, v) in extra ?? [])
            data[k] = v;
        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    [Fact]
    public void AddValkeyCluster_FillsOptionsClusters_ThroughPostConfigure()
    {
        // Arrange
        var services = Services();

        // Act
        services.AddHaValkey(Config()).AddValkeyCluster("cache").AddValkeyCluster("pending");
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<HaValkeyOptions>>().Value;

        // Assert
        options.Clusters.Should().Equal("cache", "pending");
    }

    [Theory]
    [InlineData("")]              // пустое имя
    [InlineData("Cache")]         // заглавная буква
    [InlineData("ca-che")]        // дефис (arch/20 §1: без дефиса)
    [InlineData("cache")]         // дубликат (второй AddValkeyCluster("cache"))
    public void AddValkeyCluster_FailFast_OnBadName(string cluster)
    {
        // Arrange
        var services = Services();
        services.AddHaValkey(Config()).AddValkeyCluster("cache");

        // Act
        var act = () => services.AddValkeyCluster(cluster);

        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AddValkeyCluster_FailFast_WhenModuleNotRegistered()
    {
        // Arrange
        var services = Services();

        // Act
        var act = () => services.AddValkeyCluster("cache");

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*AddHaValkey*");
    }

    [Fact]
    public void AddHaValkey_FailFast_OnSecondCall()
    {
        // Arrange
        var services = Services();
        services.AddHaValkey(Config());

        // Act
        var act = () => services.AddHaValkey(Config());

        // Assert
        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void AddHaValkey_FailFast_WhenNoClustersDeclared()
    {
        // Arrange — старт без единой заявки запрещён (спека §3.2)
        var services = Services();
        services.AddHaValkey(Config());

        // Act — резолв сервиса, чья фабрика валидирует опции (EtcdEndpointRotation)
        using var provider = services.BuildServiceProvider();
        var act = () => provider.GetRequiredService<EtcdEndpointRotation>();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*кластер*заявлен*");
    }

    [Fact]
    public void AddHaValkey_FailFast_OnEmptyEndpointsAndBadIntervals()
    {
        // Arrange — секция без endpoints; секция с неположительным интервалом
        var noEndpoints = new ConfigurationBuilder().AddInMemoryCollection(
            new Dictionary<string, string?> { ["HaValkey:PollIntervalMs"] = "1000" }).Build();
        var badInterval = new ConfigurationBuilder().AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["HaValkey:EtcdEndpoints:0"] = "http://etcd:2379",
                ["HaValkey:PollIntervalMs"] = "0",
            }).Build();
        var services1 = Services();
        services1.AddHaValkey(noEndpoints).AddValkeyCluster("cache");
        var services2 = Services();
        services2.AddHaValkey(badInterval).AddValkeyCluster("cache");

        // Act
        using var p1 = services1.BuildServiceProvider();
        using var p2 = services2.BuildServiceProvider();
        var act1 = () => p1.GetRequiredService<EtcdEndpointRotation>();
        var act2 = () => p2.GetRequiredService<EtcdEndpointRotation>();

        // Assert
        act1.Should().Throw<InvalidOperationException>().WithMessage("*EtcdEndpoints*");
        act2.Should().Throw<InvalidOperationException>().WithMessage("*PollIntervalMs*");
    }

    [Fact]
    public void AddHaValkey_AutoRegistration_ResolvesStoreAndRefresher()
    {
        // Arrange — атрибутные регистрации: Handle со своей коллекцией, как в
        // AddHaDbModuleTests (глобальный UseBehaviour статичен на процесс)
        var services = Services();

        // Act
        services.AddHaValkey(Config()).AddValkeyCluster("cache");
        new AutoRegistrationDiTypeBehaviour(services).Handle(typeof(ModuleExtensions).Assembly.GetTypes());
        using var provider = services.BuildServiceProvider();

        // Assert — AutoRegistration сборки поднял [InjectAsSingleton]-сервисы (спека §3.2)
        provider.GetRequiredService<IValkeyDiscoveryStore>().Should().NotBeNull();
        provider.GetRequiredService<ValkeyDiscoveryRefresher>().Should().NotBeNull();
    }

    [Fact]
    public void Mode_Poll_ResolvesPollSignaler()
    {
        // Arrange
        var services = Services();
        services.AddHaValkey(Config(new() { ["HaValkey:Mode"] = "Poll" })).AddValkeyCluster("cache");

        // Act
        using var provider = services.BuildServiceProvider();
        var signaler = provider.GetRequiredService<IHaValkeyRefreshSignaler>();

        // Assert — режим Poll выбирает poll-сигнальщик
        signaler.Should().BeOfType<HaValkeyPollRefreshSignaler>();
    }

    [Fact]
    public void Mode_WatchLongPoll_ResolvesWatchSignaler()
    {
        // Arrange — дефолтный режим; watch-сигнальщик тянет IValkeyDiscoveryStore
        // из атрибутной регистрации — Handle
        var services = Services();
        services.AddHaValkey(Config()).AddValkeyCluster("cache");
        new AutoRegistrationDiTypeBehaviour(services).Handle(typeof(ModuleExtensions).Assembly.GetTypes());

        // Act
        using var provider = services.BuildServiceProvider();
        var signaler = provider.GetRequiredService<IHaValkeyRefreshSignaler>();

        // Assert
        signaler.Should().BeOfType<HaValkeyWatchLongPollSignaler>();
    }

    [Fact]
    public void AddHaValkey_Defaults_WatchLongPollAndIntervals()
    {
        // Arrange
        var services = Services();

        // Act
        services.AddHaValkey(Config()).AddValkeyCluster("cache");
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<HaValkeyOptions>>().Value;

        // Assert — дефолты спеки §3.2 (как у HA.Kafka)
        options.Mode.Should().Be(HaValkeyRefreshMode.WatchLongPoll);
        options.WatchWindowMs.Should().Be(1000);
        options.BootstrapTimeoutSec.Should().Be(15);
        options.MembersMode.Should().Be(HaValkeyMembersMode.Poll);
    }

    [Fact]
    public void AddHaValkey_MembersModeOff_NoMonitorInDi()
    {
        // Arrange — Off: монитора нет в DI вовсе (спека §3.2)
        var services = Services();
        services.AddHaValkey(Config(new() { ["HaValkey:MembersMode"] = "Off" })).AddValkeyCluster("cache");

        // Act
        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetService<EtcdMembersMonitor>();

        // Assert
        monitor.Should().BeNull();
    }
}
```

- [ ] 3. Прогнать — падает (`AddHaValkey` не определён).
- [ ] 4. Создать `ModuleExtensions.cs`:

```csharp
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Valkey.Refresh;

namespace PuzzleServer.Infrastructure.App.HA.Valkey;

public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    /// <summary>
    /// Регистрирует HA.Valkey-модуль (спека §3.2): опции (секция HaValkey,
    /// валидация), общий etcd-клиент из HA.Etcd (таймаут из
    /// HaValkey:RequestTimeoutMs), ротация, сигнальщик по Mode, health-check
    /// "HaValkeyCheck", members-монитор. Кластеры задаются ЗАЯВКАМИ:
    /// .AddValkeyCluster("name") после этого вызова.
    /// </summary>
    public static IServiceCollection AddHaValkey(this IServiceCollection services, IConfiguration configuration)
    {
        if (services.Any(d => d.ServiceType == typeof(HaValkeyClusterRegistry)))
            throw new InvalidOperationException("HA.Valkey: модуль уже зарегистрирован (повторный AddHaValkey)");

        var registry = new HaValkeyClusterRegistry();
        services.AddSingleton(registry);
        services.Configure<HaValkeyOptions>(configuration.GetSection("HaValkey"));
        // Кластеры — из реестра заявок; пережиток HaValkey:Clusters в конфиге игнорируется
        services.AddOptions<HaValkeyOptions>().PostConfigure(options => options.Clusters = [.. registry.Clusters]);

        // Typed client /v3/*: имя = тип интерфейса (для декораторов в тестах),
        // без глобального таймаута (watch-стрим живёт дольше любого фиксированного;
        // range берёт RequestTimeoutMs per-request из HaValkey-опций)
        services.AddHttpClient(typeof(IEtcdClient).FullName!)
            .ConfigureHttpClient(http => http.Timeout = Timeout.InfiniteTimeSpan)
            .AddTypedClient<IEtcdClient>((http, sp) => new EtcdHttpClient(http,
                sp.GetRequiredService<IOptions<HaValkeyOptions>>().Value.RequestTimeoutMs));

        services.AddHealthChecks().AddCheck<HaValkeyHealthCheck>("HaValkeyCheck");
        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<HaValkeyOptions>>().Value;
            ValidateOptions(options);
            return new EtcdEndpointRotation(options.EtcdEndpoints);
        });

        // Сигнальщик выбирается режимом при старте
        services.AddSingleton<IHaValkeyRefreshSignaler>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<HaValkeyOptions>>().Value;
            return options.Mode switch
            {
                HaValkeyRefreshMode.Poll => new HaValkeyPollRefreshSignaler(options),
                HaValkeyRefreshMode.WatchLongPoll => new HaValkeyWatchLongPollSignaler(
                    sp.GetRequiredService<IEtcdClient>(),
                    sp.GetRequiredService<EtcdEndpointRotation>(),
                    sp.GetRequiredService<IValkeyDiscoveryStore>(),
                    sp.GetRequiredService<IOptions<HaValkeyOptions>>(),
                    sp.GetRequiredService<ILogger<HaValkeyWatchLongPollSignaler>>()),
                _ => throw new InvalidOperationException($"HA.Valkey: Mode={options.Mode} не поддерживается"),
            };
        });

        // Members-режим читается прямым биндом секции (IOptions недоступен на этапе регистрации)
        var membersMode = configuration.GetSection("HaValkey").Get<HaValkeyOptions>()?.MembersMode
                          ?? HaValkeyMembersMode.Poll;
        if (membersMode != HaValkeyMembersMode.Off)
        {
            services.AddSingleton(sp => new EtcdMembersMonitor(
                sp.GetRequiredService<IEtcdClient>(),
                sp.GetRequiredService<EtcdEndpointRotation>(),
                new EtcdMembersMonitorOptions(
                    membersMode == HaValkeyMembersMode.Poll ? EtcdMembersMode.Poll : EtcdMembersMode.OnFailure,
                    sp.GetRequiredService<IOptions<HaValkeyOptions>>().Value.MembersPollIntervalMs,
                    sp.GetRequiredService<IOptions<HaValkeyOptions>>().Value.MembersMinIntervalMs,
                    sp.GetRequiredService<IOptions<HaValkeyOptions>>().Value.EtcdEndpoints),
                sp.GetRequiredService<ILogger<EtcdMembersMonitor>>()));
            services.AddHostedService(sp => sp.GetRequiredService<EtcdMembersMonitor>());
        }

        // AutoRegistration сборки: [InjectAsSingleton] ValkeyDiscoveryStore/ValkeyDiscoveryRefresher
        return services.AutoRegistration(Assembly);
    }

    /// <summary>
    /// Заявка valkey-кластера: КАЖДЫЙ вызов добавляет имя в реестр.
    /// Fail-fast: модуль не зарегистрирован, пустое имя, неверный формат (arch/20 §1), дубликат.
    /// </summary>
    public static IServiceCollection AddValkeyCluster(this IServiceCollection services, string cluster)
    {
        var registry = services.FirstOrDefault(d => d.ServiceType == typeof(HaValkeyClusterRegistry))
            ?.ImplementationInstance as HaValkeyClusterRegistry
            ?? throw new InvalidOperationException(
                "HA.Valkey: сначала зарегистрируйте модуль вызовом AddHaValkey(configuration)");
        registry.Add(cluster);
        return services;
    }

    // Полная валидация опций: endpoints, интервалы, хотя бы один кластер
    private static void ValidateOptions(HaValkeyOptions options)
    {
        if (options.EtcdEndpoints.Length == 0)
            throw new InvalidOperationException("HA.Valkey: HaValkey:EtcdEndpoints не задан (нужен хотя бы один etcd endpoint)");
        if (options.Clusters.Length == 0)
            throw new InvalidOperationException("HA.Valkey: ни один valkey-кластер не заявлен (AddValkeyCluster)");
        foreach (var (name, value) in new[]
                 {
                     ("RequestTimeoutMs", options.RequestTimeoutMs),
                     ("WatchWindowMs", options.WatchWindowMs),
                     ("WatchReopenDelayMs", options.WatchReopenDelayMs),
                     ("WatchErrorDelayMs", options.WatchErrorDelayMs),
                     ("PollIntervalMs", options.PollIntervalMs),
                     ("MembersPollIntervalMs", options.MembersPollIntervalMs),
                     ("MembersMinIntervalMs", options.MembersMinIntervalMs),
                     ("BootstrapTimeoutSec", options.BootstrapTimeoutSec),
                 })
        {
            if (value <= 0)
                throw new InvalidOperationException($"HA.Valkey: HaValkey:{name} должен быть > 0 (сейчас {value})");
        }
    }
}
```

- [ ] 5. Прогнать: `dotnet test src/PuzzleServer.UnitTests --filter "FullyQualifiedName~UnitTests.HA.Valkey"` — все PASS.
- [ ] 6. Коммит:

```bash
git add src/PuzzleServer.Infrastructure.App.HA.Valkey/HaValkeyHealthCheck.cs \
        src/PuzzleServer.Infrastructure.App.HA.Valkey/ModuleExtensions.cs \
        src/PuzzleServer.UnitTests/HA/Valkey/ModuleRegistrationTests.cs
git commit -m "feat: HA.Valkey — подключение модуля (AddHaValkey/AddValkeyCluster, fail-fast, members-монитор, health-check HaValkeyCheck) + unit-тесты регистрации"
```

**Выход:** библиотека функционально завершена; DI-контракт публичен.

**Проверка:** фильтр-прогон зелёный (все файлы `src/PuzzleServer.UnitTests/HA/Valkey/`); `dotnet build src/PuzzleServer.Api.slnx` — 0 warnings.

---

### Шаг 9: Интеграционные тесты против реального etcd (Фаза 4)

**Связь со spec:** §3.6 (интеграционные сценарии), §6 п.3/п.4/п.6 (критерии: оба режима, ротация, fail-open, read-only-трафик, зачистка), §5 п.7 (порт вне 32490/32495).

**Вход (предусловие):** Шаг 8 закоммичен; docker доступен (`docker info` отвечает); образ `quay.io/coreos/etcd:v3.5.21` присутствует локально (иначе `docker pull quay.io/coreos/etcd:v3.5.21`).

**Files:**
- Modify: `src/PuzzleServer.IntegrationTests/PuzzleServer.IntegrationTests.csproj` (ProjectReference)
- Create: `src/PuzzleServer.IntegrationTests/HA/Valkey/ValkeyEtcdFixture.cs`
- Create: `src/PuzzleServer.IntegrationTests/HA/Valkey/ValkeyDiscoveryIntegrationTests.cs`

**Interfaces (Consumes):** публичный API модуля (AddHaValkey/AddValkeyCluster/IValkeyDiscoveryStore/Updated/GetClientConfig), `AutoRegistrationDiTypeBehaviour`.

**Шаги исполнения:**

- [ ] 1. Добавить в `PuzzleServer.IntegrationTests.csproj` (рядом с HA.Kafka-ссылкой):

```xml
        <ProjectReference Include="..\PuzzleServer.Infrastructure.App.HA.Valkey\PuzzleServer.Infrastructure.App.HA.Valkey.csproj"/>
```

- [ ] 2. Скопировать `IntegrationTests/HA/Kafka/KafkaEtcdFixture.cs` → `HA/Valkey/ValkeyEtcdFixture.cs` с заменами: namespace → `PuzzleServer.IntegrationTests.HA.Valkey`; имя класса `KafkaEtcdFixture` → `ValkeyEtcdFixture`; дефолт порта `32490` → `32496` (заняты 32490/32495 kafka-фикстурами и 32500–32509 kafka-зондом); комментарий про порты обновить: «порт — параметр (дефолт 32496: xunit параллелит коллекции, 32490/32495 заняты kafka-фикстурами)». Механика без изменений: generic-контейнер etcd v3.5.21, фиксированный host-порт (рестарт на том же endpoint для fail-open), POST-ретрай готовности ≤ 30 с, PutAsync/DeleteAsync/DeletePrefixAsync прямым HTTP.
- [ ] 3. Создать `ValkeyDiscoveryIntegrationTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PuzzleServer.Infrastructure.App.DI;
using PuzzleServer.Infrastructure.App.HA.Etcd;
using PuzzleServer.Infrastructure.App.HA.Valkey;
using PuzzleServer.Infrastructure.App.HA.Valkey.Model;
using Xunit;

namespace PuzzleServer.IntegrationTests.HA.Valkey;

// Полный цикл дискавери в обоих режимах против реального etcd + фиксация
// read-only трафика (спека §3.6, §6 п.3-4)
public class ValkeyDiscoveryIntegrationTests : IAsyncLifetime
{
    private readonly ValkeyEtcdFixture _etcd = new();

    public static TheoryData<string> Modes() => new() { "WatchLongPoll", "Poll" };

    private IConfiguration Config(string mode, Dictionary<string, string?>? extra = null)
    {
        var data = new Dictionary<string, string?>
        {
            ["HaValkey:Mode"] = mode,
            ["HaValkey:EtcdEndpoints:0"] = _etcd.Endpoint,
            ["HaValkey:MembersMode"] = "Off",          // монитору тут нечего открывать
            ["HaValkey:WatchWindowMs"] = "300",
            ["HaValkey:PollIntervalMs"] = "200",
        };
        foreach (var (k, v) in extra ?? [])
            data[k] = v;
        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    private async Task<ServiceProvider> StartAsync(string mode, RecordingHandler? recording = null)
    {
        var services = new ServiceCollection()
            .AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(NullLogger<>));
        if (recording is not null)
        {
            // Декоратор трафика на typed client IEtcdClient (имя = FullName типа)
            services.AddSingleton(recording);
            services.AddHttpClient(typeof(IEtcdClient).FullName!)
                .AddHttpMessageHandler<RecordingHandler>();
        }
        services.AddHaValkey(Config(mode)).AddValkeyCluster("cache");
        // Атрибутные регистрации модуля: Handle со своей коллекцией
        new AutoRegistrationDiTypeBehaviour(services).Handle(typeof(ModuleExtensions).Assembly.GetTypes());
        var provider = services.BuildServiceProvider();
        // Стартуем ВСЕ hosted-сервисы (ValkeyDiscoveryRefresher — bootstrap)
        foreach (var hosted in provider.GetServices<IHostedService>())
            await hosted.StartAsync(CancellationToken.None);
        return provider;
    }

    private async Task SeedClusterAsync(string password = "abcdefghijklmnopqrstuvwxyz012345")
    {
        // Засев через ПРЯМОЙ HTTP (конечный etcd-клиент теста — НЕ библиотека);
        // arch/20 §2.1: Active-config без state + endpoints + креды + admin/nodes
        await _etcd.PutAsync("/valkey/clusters/cache/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
        await _etcd.PutAsync("/valkey/clusters/cache/endpoints", "host.docker.internal:17001");
        await _etcd.PutAsync("/valkey/clusters/cache/app_user", "app");
        await _etcd.PutAsync("/valkey/clusters/cache/app_password", password);
        await _etcd.PutAsync("/valkey/clusters/cache/admin_user", "admin");
        await _etcd.PutAsync("/valkey/clusters/cache/admin_password", "ZYXWVUTSRQPONMLKJIHGFEDCBA987654");
        await _etcd.PutAsync("/valkey/clusters/cache/nodes/node1/state", "RUNNING");
        await _etcd.PutAsync("/valkey/clusters/cache/nodes/node1/resources",
            """{"cpu":"2","mem":"4Gi","disk":"40Gi"}""");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task FullCycle_SnapshotEventsRotationFailOpen(string mode)
    {
        // Arrange
        await _etcd.InitializeAsync();
        await SeedClusterAsync();

        // Act / Assert — 1. bootstrap: снапшот собран, admin/nodes молча
        var provider = await StartAsync(mode);
        using var __ = provider;
        var store = provider.GetRequiredService<IValkeyDiscoveryStore>();
        (await WaitUntilAsync(() => store.Get("cache").IsSuccess, TimeSpan.FromSeconds(5)))
            .Should().BeTrue("bootstrap должен собрать снапшот");
        var snapshot = store.Get("cache").Value;
        snapshot.State.Should().BeNull(); // Active-config без state
        snapshot.GetClientConfig()!.Endpoints.Should().Be("host.docker.internal:17001");
        snapshot.GetClientConfig()!.Username.Should().Be("app");
        snapshot.GetClientConfig()!.Ssl.Should().BeFalse();

        // 2. изменение endpoints → событие Updated (порог: Watch ≤ 2 с, Poll ≤ ~1 с)
        var updated = new TaskCompletionSource<ValkeyClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "cache") updated.TrySetResult(s); };
        await _etcd.PutAsync("/valkey/clusters/cache/endpoints", "host.docker.internal:17011");
        var winner = await Task.WhenAny(updated.Task, Task.Delay(TimeSpan.FromSeconds(3)));
        winner.Should().Be(updated.Task, $"изменение endpoints должно прийти в режиме {mode}");
        (await updated.Task).Endpoints.Should().Be("host.docker.internal:17011");

        // 3. ротация пароля (arch/21 §5 E): put NEW → событие → новый пароль в конфиге
        var rotated = new TaskCompletionSource<ValkeyClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "cache") rotated.TrySetResult(s); };
        await _etcd.PutAsync("/valkey/clusters/cache/app_password", "ZYXWVUTSRQPONMLKJIHGFEDCBA987654");
        await Task.WhenAny(rotated.Task, Task.Delay(TimeSpan.FromSeconds(3)));
        store.Get("cache").Value.GetClientConfig()!.Password
            .Should().Be("ZYXWVUTSRQPONMLKJIHGFEDCBA987654");

        // 4. fail-open: остановка etcd → кэш живёт неограниченно
        await _etcd.StopEtcdAsync();
        store.Get("cache").IsSuccess.Should().BeTrue("кэш живёт при лежащем etcd (fail-open)");
        store.Get("cache").Value.Endpoints.Should().Be("host.docker.internal:17011");

        // 5. возврат etcd → восстановление первым окном/тиком
        await _etcd.StartEtcdAsync();
        await _etcd.PutAsync("/valkey/clusters/cache/endpoints", "host.docker.internal:17012");
        var recovered = new TaskCompletionSource<ValkeyClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "cache") recovered.TrySetResult(s); };
        await Task.WhenAny(recovered.Task, Task.Delay(TimeSpan.FromSeconds(8)));
        store.Get("cache").Value.Endpoints.Should().Be("host.docker.internal:17012");
    }

    [Fact]
    public async Task SameValuePut_DoesNotFireUpdated()
    {
        // Arrange
        await _etcd.InitializeAsync();
        await SeedClusterAsync();
        var provider = await StartAsync("WatchLongPoll");
        using var __ = provider;
        var store = provider.GetRequiredService<IValkeyDiscoveryStore>();
        await WaitUntilAsync(() => store.Get("cache").IsSuccess, TimeSpan.FromSeconds(5));
        var fired = 0;
        store.Updated += _ => fired++;

        // Act — переписываем те же ключи ТЕМИ ЖЕ значениями (ревизии растут)
        await _etcd.PutAsync("/valkey/clusters/cache/endpoints", "host.docker.internal:17001");
        await _etcd.PutAsync("/valkey/clusters/cache/app_password", "abcdefghijklmnopqrstuvwxyz012345");
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Assert — событие по содержимому, не по ревизии (спека §6 п.4)
        fired.Should().Be(0);
    }

    [Fact]
    public async Task KeyDelete_DegradesClientConfig()
    {
        // Arrange — удаление app_password → App = null → GetClientConfig() = null
        await _etcd.InitializeAsync();
        await SeedClusterAsync();
        var provider = await StartAsync("WatchLongPoll");
        using var __ = provider;
        var store = provider.GetRequiredService<IValkeyDiscoveryStore>();
        await WaitUntilAsync(() => store.Get("cache").IsSuccess, TimeSpan.FromSeconds(5));
        var degraded = new TaskCompletionSource<ValkeyClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "cache") degraded.TrySetResult(s); };

        // Act
        await _etcd.DeleteAsync("/valkey/clusters/cache/app_password");
        var winner = await Task.WhenAny(degraded.Task, Task.Delay(TimeSpan.FromSeconds(3)));

        // Assert — снапшот жив, конфиг деградировал (arch/20 §5: неполный набор кредов)
        winner.Should().Be(degraded.Task);
        store.Get("cache").IsSuccess.Should().BeTrue();
        store.Get("cache").Value.GetClientConfig().Should().BeNull();
    }

    [Fact]
    public async Task MalformedValues_Tolerant()
    {
        // Arrange — битый config-JSON и пустой endpoints: событие/деградация без падений
        await _etcd.InitializeAsync();
        await SeedClusterAsync();
        var provider = await StartAsync("WatchLongPoll");
        using var __ = provider;
        var store = provider.GetRequiredService<IValkeyDiscoveryStore>();
        await WaitUntilAsync(() => store.Get("cache").IsSuccess, TimeSpan.FromSeconds(5));
        var changed = new TaskCompletionSource<ValkeyClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Updated += s => { if (s.Cluster == "cache") changed.TrySetResult(s); };

        // Act — битый config (Active-ветка) + пробельный endpoints (как отсутствующий)
        await _etcd.PutAsync("/valkey/clusters/cache/config", "{not-json");
        await _etcd.PutAsync("/valkey/clusters/cache/endpoints", "   ");
        var winner = await Task.WhenAny(changed.Task, Task.Delay(TimeSpan.FromSeconds(3)));

        // Assert — State = Active, Endpoints = null → конфиг null; кластер жив (§5)
        winner.Should().Be(changed.Task);
        var snapshot = store.Get("cache").Value;
        snapshot.State.Should().BeNull();
        snapshot.Endpoints.Should().BeNull();
        snapshot.GetClientConfig().Should().BeNull();
        snapshot.HasAppSecret.Should().BeTrue();
    }

    [Fact]
    public async Task Traffic_IsReadOnly_RangeWatchMemberListOnly()
    {
        // Arrange — декорирующий DelegatingHandler на typed client IEtcdClient
        await _etcd.InitializeAsync();
        await SeedClusterAsync();
        var recording = new RecordingHandler();
        var provider = await StartAsync("WatchLongPoll", recording);
        using var __ = provider;
        var store = provider.GetRequiredService<IValkeyDiscoveryStore>();
        await WaitUntilAsync(() => store.Get("cache").IsSuccess, TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromSeconds(1)); // пара watch-окон поверх bootstrap

        // Act — журнал трафика собран RecordingHandler'ом
        var entries = recording.Entries.ToList();

        // Assert — только read-only RPC etcd: range + watch (+ member/list в
        // members-режимах; здесь MembersMode=Off); НИЧЕГО не пишем (спека §2 п.1)
        entries.Should().NotBeEmpty();
        entries.Should().OnlyContain(e => e is "POST /v3/kv/range" or "POST /v3/watch" or "POST /v3/cluster/member/list");
        entries.Should().Contain(e => e == "POST /v3/kv/range");
        entries.Should().Contain(e => e == "POST /v3/watch");
    }

    // Счётчик запросов «METHOD path» сквозь весь typed client IEtcdClient
    private sealed class RecordingHandler : DelegatingHandler
    {
        public List<string> Entries { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Entries.Add($"{request.Method.Method} {request.RequestUri?.AbsolutePath}");
            return base.SendAsync(request, cancellationToken);
        }
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(100);
        }
        return condition();
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => _etcd.DisposeAsync();
}
```

- [ ] 4. Прогон серии (docker; до старта зафиксировать baseline контейнеров):

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
docker ps -a --filter "ancestor=quay.io/coreos/etcd:v3.5.21" -q | sort > /tmp/t04-etcd-before.txt
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.IntegrationTests --filter "FullyQualifiedName~IntegrationTests.HA.Valkey"
```

Ожидание: все тесты PASS (6 прогонов: FullCycle ×2 режима + SameValuePut + KeyDelete + MalformedValues + Traffic). Серия > 5 минут — онлайн-анализ `docker logs <container>` без остановки прогона (канон AGENTS.base).

- [ ] 5. Зачистка после серии (обязательная):

```bash
docker ps -a --filter "ancestor=quay.io/coreos/etcd:v3.5.21" -q | sort > /tmp/t04-etcd-after.txt
diff /tmp/t04-etcd-before.txt /tmp/t04-etcd-after.txt   # расхождений быть НЕ должно
```

Если остаточные контейнеры фикстуры есть — `docker rm -f <id>` (только свои) и разобраться, почему teardown не сработал, до коммита.

- [ ] 6. Коммит:

```bash
git add src/PuzzleServer.IntegrationTests/PuzzleServer.IntegrationTests.csproj \
        src/PuzzleServer.IntegrationTests/HA/Valkey
git commit -m "test: HA.Valkey — интеграционные тесты против testcontainers-etcd (полный цикл обоих режимов, ротация, деградация ключа, fail-open, read-only трафик)"
```

**Выход:** интеграционное покрытие категорий приёмки §6 п.3–4 и 6.

**Проверка:** серия зелёная; diff зачистки пуст; никаких хардкод-портов в expects, кроме параметра фикстуры 32496.

---

### Шаг 10: Синхронизация доков и финальные гейты (Фаза 5)

**Связь со spec:** §4 Фаза 5, §6 п.1/п.5/п.7 (build 0 warnings, доки синхронны, code-review, мерж-гейты).

**Вход (предусловие):** Шаги 1–9 закоммичены в обоих репозиториях.

**Действие/Шаги исполнения:**

- [ ] 1. Сверка канона: перечитать `docs/01.21-ha-valkey.md` против фактического кода (файлы из Шагов 3–8); любые расхождения — править ДОКУМЕНТ или признать код неверным (второе — возврат на соответствующий шаг). Типовые точки сверки: дефолты опций, набор читаемых ключей, семантика Updated/fail-open, «только range/watch/member-list».
- [ ] 2. Полная сборка решения: `cd /Users/demakaev/ZCodeProject/Puzzle && DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx` — 0 errors, 0 warnings; пакеты не тронуты: `git diff --stat -- src/Directory.Packages.props` пуст (рабочее дерево чисто) И `git diff --stat main...HEAD -- src/Directory.Packages.props` пуст (ветка не меняла файл — новых пакетов нет; файл лежит в `src/`).
- [ ] 3. Полная юнит-серия: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests` — все зелёные (вкл. ранее существовавшие; регрессий нет).
- [ ] 4. Интеграционная серия HA.Valkey повторно (после возможных правок доков/кода): команды Шага 9 п.4 + зачистка п.5. Между сериями — только поочерёдно, никогда поверх незачищенной (канон AGENTS pg).
- [ ] 5. Code-review: запросить ревью (skill superpowers:requesting-code-review) на оба диффа: pg-worktree (roadmap + spec/plan каталог) и Puzzle (ветка t04-valkey-discovery-lib). Замечания — исправить, серии из п.3–4 перепрогнать.
- [ ] 6. Состояние мерж-гейта (НЕ исполняется до явной просьбы пользователя — фиксация в финальном отчёте):
  - мерж ветки `t04-valkey-discovery-lib` в `main` Puzzle и мерж pg-worktree в `main` pg — только по явной просьбе;
  - в момент мержа pg: тем же мерж-коммитом снять тег `t04-valkey-discovery-lib` из `arch/roadmap/valkey.md` — пункт t04 целиком И `← t04-valkey-discovery-lib` из зависимостей пункта t08 (мерж-гейт roadmap, AGENTS.md pg).

**Выход:** задача в состоянии «готова к ревью/мержу»; все гейты пройдены.

**Проверка (итоговая, по критериям приёмки §6):**

```bash
cd /Users/demakaev/ZCodeProject/Puzzle
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PuzzleServer.Api.slnx                       # 0 warnings, новых пакетов нет
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.UnitTests                        # все зелёные
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PuzzleServer.IntegrationTests --filter "FullyQualifiedName~IntegrationTests.HA.Valkey"   # зелёные, оба режима
docker ps -a --filter "ancestor=quay.io/coreos/etcd:v3.5.21" -q | wc -l                 # = baseline (зачистка)
# Гейт «roadmap ДО кода» (spec §6 п.5 — правка ДО кода, а НЕ первый коммит ветки:
# в pg-ветке до неё уже лежат коммиты spec/plan):
cd /Users/demakaev/ZCodeProject/worktrees/t04-valkey-discovery-lib \
  && git log --format="%h %cI %s" --reverse -- arch/roadmap/valkey.md | head -1   # timestamp roadmap-коммита (Шаг 1)
cd /Users/demakaev/ZCodeProject/Puzzle \
  && git log --format="%h %cI %s" --reverse --grep="^feat" | head -1               # timestamp первого КОД-коммита Puzzle-ветки
# условие гейта: roadmap-коммит Шага 1 старше первого feat-коммита Puzzle
```

---

## Самопроверка плана (выполнено автором)

1. **Spec coverage:** §1 объём (только библиотека) → Шаги 3–8; §2 п.1 read-only → Шаг 9 `Traffic_IsReadOnly`; §2 п.2 копия HA.Kafka → Шаги 3, 6–8 (механика) + 4–5 (домен); §2 п.3 толерантность → Шаги 5, 9; §2 п.4 без пакетов → Шаги 3 (csproj), 10 (гейт); §2 п.5 arch-first → Шаги 1, 2; §2 п.6 язык/AAA → все тесты с AAA-комментариями; §3.1–3.6 → Шаги 3–9 один к одному; §4 фазы → Шаги 1–10 в порядке; §5 ограничения → Global Constraints + Шаг 9 (порты/зачистка); §6 критерии → проверки Шагов 4–10; §7 риски → ca_pem в Шагах 5, 6; порт-коллизия → 32496 в Шаге 9; §8 open questions — нет.
2. **Placeholder scan:** TBD/TODO нет; все копии заданы точными таблицами замен с исходными путями; код новых файлов приведён полностью.
3. **Type consistency:** `ValkeyAppSecret(Username, Password)` / `ValkeyClientConfig(Endpoints, Username, Password, Ssl)` / `SslValue` / `IValkeyDiscoveryStore` / `IHaValkeyRefreshSignaler` единообразны во всех шагах; префикс `/valkey/clusters/<C>/` одинаков в парсере, сторе, сигнальщике и тестах.
