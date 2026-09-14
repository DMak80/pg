# t08-unify-adminpanel-duplicates — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** устранить дубли кода AdminPanel/PgWorker/KafkaWorker переносом в общие сборки `Shared.Core`, `Shared.Etcd`, `Shared.Tls` (паттерн `/common/` + `Shared.Metrics`) без изменения поведения систем и внешних контрактов.

**Архитектура:** три новые сборки в solution-папке `/common/` слн `src/PgWorker.slnx`. `Shared.Core` — Puzzle-каркас (attribute-DI, `Result`, CQRS, Contexts, Traces, HealthChecks, Retry) из канонических копий as-is. `Shared.Etcd` — etcd-клиент HTTP JSON gateway с union-API воркеров и панели (txn-фабрики `NotExists`/`ValueEqual`/`ModRevisionEqual`/`TxnRequest.Of` — эквивалент прямой формы, spec §4.2). `Shared.Tls` — хелперы mTLS-граней. Все три системы переключаются на общие сборки, копии-дубли удаляются, `AdminPanel.Infrastructure` ликвидируется (код — в фазе B, сборка и slnx-строка — в фазе E).

**Тех-стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), CPM (`src/Directory.Packages.props` — все нужные версии уже есть, новых записей НЕ добавлять), xunit.v3 + FluentAssertions, Testcontainers.

**Spec:** `docs/superpowers/2026-09-14-t08-unify-adminpanel-duplicates/spec.md` (исполнитель читает spec и этот план вместе; план аргументируется от spec).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/refactor-t08-unify-adminpanel-duplicates`, ветка `refactor-t08-unify-adminpanel-duplicates`. Все команды ниже выполняются от корня worktree.

## Global Constraints (из spec, обязательны для каждой задачи)

- Поведение систем НЕ меняется: перенос типов без изменения логики; etcd-ключи (`arch/adminpanel/02`), REST API, docker-образы, конфигурация — нетронуты (spec §1, §3.2).
- Любое обнаруженное расхождение семантики дублей — остановить перенос и рассмотреть, не «подтихо выбрать одну версию» (spec §3.2).
- Общие сборки не зависят от доменных `PgWorker.*`/`KafkaWorker.*`/`AdminPanel.*` (spec §3.3).
- Новых NuGet-пакетов и записей в CPM — нет (spec §4.1, §5.В5).
- После каждой фазы — полный `dotnet build src/PgWorker.slnx -c Release` (0 warnings как errors; spec §7).
- Комментарии/доки — русский; идентификаторы — английские (spec §3.7).
- Тесты: динамические порты, полный teardown, зачистка контейнеров и сетей между сериями — правила AGENTS.md без изменений (spec §4.5).
- Телеметрия E2E — по `docs/e2e-launch.md`: упавший сценарий НЕ перезапускается без анализа логов (spec §8 Фаза F.6).
- Коммит после каждой задачи (feature-ветка — можно свободно). Мерж в `main` — только по явной просьбе пользователя.

## Механика миграции namespace (общая для фаз B/C/D)

Новых namespace-правок у потребителей — минимум, тремя приёмами:

1. **Глобальный using `Shared.Core` в csproj** (`<Using Include="Shared.Core" />`) — покрывает `Result` (самый частый тип). Старые using `using PgWorker.Core;`/`using KafkaWorker.Core;` НЕ трогаем: namespace жив (Model/Planning/… остаются), дубль using не ошибка. Паттерн `<Using Include=…/>` уже легализован в тестовых csproj (`KafkaWorker.UnitTests.csproj`: `<Using Include="Xunit"/>`).
2. **Точечная замена префикса** perl-ом (одна команда на namespace; это bulk-rename, не ручные правки):
   ```bash
   # панель (фаза B):
   git grep -lz 'using AdminPanel.Infrastructure.DI;' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*' | xargs -0 perl -pi -e 's/^using AdminPanel\.Infrastructure\.DI;/using Shared.Core.DI;/'
   git grep -lz 'using AdminPanel.Infrastructure.CQRS;' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*' | xargs -0 perl -pi -e 's/^using AdminPanel\.Infrastructure\.CQRS;/using Shared.Core.CQRS;/'
   git grep -lz 'using AdminPanel.Infrastructure.Contexts;' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*' | xargs -0 perl -pi -e 's/^using AdminPanel\.Infrastructure\.Contexts;/using Shared.Core.Contexts;/'
   git grep -lz 'using AdminPanel.Infrastructure.Traces;' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*' | xargs -0 perl -pi -e 's/^using AdminPanel\.Infrastructure\.Traces;/using Shared.Core.Traces;/'
   git grep -lz 'using AdminPanel.Infrastructure.HealthChecks;' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*' | xargs -0 perl -pi -e 's/^using AdminPanel\.Infrastructure\.HealthChecks;/using Shared.Core.HealthChecks;/'
   # удаление using корневого namespace (Result приходит глобальным using):
   git grep -lz '^using AdminPanel.Infrastructure;$' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*' | xargs -0 perl -ni -e 'print unless /^using AdminPanel\.Infrastructure;$/'
   # воркеры, HealthChecks (фаза B):
   git grep -lz 'using PgWorker.App.HealthChecks;' -- 'src' | xargs -0 perl -pi -e 's/^using PgWorker\.App\.HealthChecks;/using Shared.Core.HealthChecks;/'
   git grep -lz 'using KafkaWorker.App.HealthChecks;' -- 'src' | xargs -0 perl -pi -e 's/^using KafkaWorker\.App\.HealthChecks;/using Shared.Core.HealthChecks;/'
   # etcd-клиент (фаза C):
   git grep -lz '^using PgWorker.Etcd.Client;' -- 'src' | xargs -0 perl -pi -e 's/^using PgWorker\.Etcd\.Client;/using Shared.Etcd.Client;/'
   git grep -lz '^using KafkaWorker.Etcd.Client;' -- 'src' | xargs -0 perl -pi -e 's/^using KafkaWorker\.Etcd\.Client;/using Shared.Etcd.Client;/'
   git grep -lz '^using AdminPanel.Etcd.Client;' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*' | xargs -0 perl -pi -e 's/^using AdminPanel\.Etcd\.Client;/using Shared.Etcd.Client;/'
   ```
   (git grep по glob'ам `src/AdminPanel.*` разворачивается шеллом в директории: `src/AdminPanel.Api …AdminPanel.Core …AdminPanel.Etcd …AdminPanel.Infrastructure …AdminPanel.Probes`; для `src/tests/AdminPanel.*` — два тестовых проекта. Фаза B выполняется ДО удаления остатков кода `AdminPanel.Infrastructure` в Task 4 Step 4 — заменам всё равно, какие файлы существуют.)
3. **Точечные правки вызовов** — только перечисленные в spec §2.2/§4.2/§4.3 точки (WorkerCertService, SnapshotJob ×2) плюс расширение тестовых фейков `IEtcdGateway` до union-API.

Точные текущие объёмы (проверено grep 2026-09-14): `using AdminPanel.Infrastructure.DI;` — 99 файлов, `.CQRS;` — 27, `.Contexts;` — 1, `.Traces;` — 3, `.HealthChecks;` — 2, `using AdminPanel.Infrastructure;` — 51; `using PgWorker.Etcd.Client;` — 102, `using KafkaWorker.Etcd.Client;` — 46, `using AdminPanel.Etcd.Client;` — 30; `using PgWorker.App.HealthChecks;` / `using KafkaWorker.App.HealthChecks;` — по 5.

---

## Фаза A — arch-first (контракт до кода)

### Task 1: Обновление arch/-доков и roadmap-заглушка t09

**Files:**
- Modify: `arch/adminpanel/01-architecture.md` (§1 «Направление зависимостей», ~стр. 81–82; §2 таблица проектов, ~стр. 89)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§1 «Транспорт», первый пункт списка)
- Modify: `docs/adminpanel/01-framework.md` (шапка + пути в тексте)
- Modify: `docs/adminpanel/INDEX.md` (строка таблицы «01 — Каркас»)
- Modify: `arch/18-metrics.md` (~стр. 41 и ~стр. 214 — упоминания `AdminPanel.Infrastructure`)
- Modify: `arch/roadmap/pgworker.md` (новый пункт `t09-unify-worker-duplicates`)

**Interfaces:** — (доки).

**Вход:** spec утверждён; worktree чистый (git status пустой).

- [ ] **Step 1: `arch/adminpanel/01-architecture.md` §1** — заменить строку направления зависимостей:
  ```
  - **Направление зависимостей**: `Api → (Core, Etcd, Probes)`; `Etcd → (Core,
    Shared.Etcd, Shared.Tls)`; `Probes → Core`; `Core → Shared.Core, Shared.Etcd`
    (/common/: каркас; транспортные records etcd `EtcdMember`/`EtcdAlarm`/
    `EtcdAlarmType` — общие из `Shared.Etcd`, HTTP-транспорт домену не известен).
  ```
  (было: `Api → (Core, Etcd, Probes, Infrastructure)`; `Etcd → Core`; `Probes → Core`; `Core → Infrastructure`). Формулировка соответствует spec §4.4 (диаграмма: `AdminPanel.Core → Shared.Core, Shared.Etcd`).
- [ ] **Step 2: `arch/adminpanel/01-architecture.md` §2, строка таблицы `AdminPanel.Infrastructure`** — заменить на строку общих сборок:
  ```
  | `/common/` (Shared.Core, Shared.Etcd, Shared.Tls) | Общие сборки монорепо (паттерн `Shared.Metrics`, шаблон `../Puzzle` `Infrastructure.App`): `Shared.Core` — каркас (attribute-DI, `Result`, CQRS, Contexts, Traces, HealthChecks-базис, Retry); `Shared.Etcd` — etcd-клиент HTTP JSON gateway `/v3/*` (вкл. транспортные records `EtcdMember`/`EtcdAlarm`); `Shared.Tls` — хелперы mTLS-граней. Бывший `AdminPanel.Infrastructure` (t08) |
  ```
  Строку `AdminPanel.Etcd` в той же таблице скорректировать: «Клиент etcd — общая сборка `Shared.Etcd` (IEtcdGateway); у панели: парсеры ключей …, `SnapshotRefresher`, `SnapshotStore`» (текст роли сохранить, упоминание собственного клиента убрать). Строку `AdminPanel.Core` дополнить: «…, транспортные records etcd (`EtcdMember`/`EtcdAlarm`) — общие из `Shared.Etcd`».
- [ ] **Step 3: `arch/adminpanel/02-etcd-contract.md` §1** — в первый пункт («Клиент — HttpClient против gRPC-gateway etcd…») добавить после первого предложения: «Клиент — общая сборка `Shared.Etcd` (используется и воркерами PgWorker/KafkaWorker); контракт ключей и формат запросов этим переносом не меняются».
- [ ] **Step 4: `docs/adminpanel/01-framework.md`** — шапку `> Подсистема: src/AdminPanel.Infrastructure` заменить на `> Подсистема: src/Shared.Core (/common/; бывший AdminPanel.Infrastructure, t08)`; в тексте пути `src/AdminPanel.Infrastructure/DI/InjectAs.cs` → `src/Shared.Core/DI/InjectAs.cs`; раздел про граблю статического кеша сборок сохранить дословно (она остаётся актуальной).
- [ ] **Step 5: `docs/adminpanel/INDEX.md`** — в строке `| [01 — Каркас](01-framework.md) | AdminPanel.Infrastructure | …` подсистему заменить на `Shared.Core (/common/)`.
- [ ] **Step 6: `arch/18-metrics.md`** — в строках ~41 и ~214 заменить `AdminPanel.Infrastructure` на `Shared.Core (бывший AdminPanel.Infrastructure)` (смысл: порт базы метрик делался с паттерна панели, теперь каркас общий).
- [ ] **Step 7: `arch/roadmap/pgworker.md`** — после пункта `t08-unify-adminpanel-duplicates` добавить пункт-заглушку:
  ```
  - **`t09-unify-worker-duplicates`** — Pg↔Kfw-дубли вне панельного контура (осознанно
    не тронуты t08): Coordination `ClaimStore`/`PortAllocLock`/`WorkJournal`,
    `SnapshotJob`, `Writing/{PlanPut,ValidationError}`, `Planning/{PlacementPlanner,
    PortAllocator}` — перенос с параметризацией префиксов ключей и моделей журнала
    (PgWorker несёт `RetrySeries`/`EvacuationJournal`).
  ```
- [ ] **Step 8: Проверка** — `git diff --stat` показывает только 6 перечисленных файлов; `grep -rn "AdminPanel.Infrastructure" arch/ docs/ --exclude-dir=superpowers` — остаются только формулировки «бывший AdminPanelInfrastructure/t08» (осознанные отсылки к истории), не живые ссылки на сборку как на текущую.
- [ ] **Step 9: Commit**
  ```bash
  git add arch/adminpanel/01-architecture.md arch/adminpanel/02-etcd-contract.md docs/adminpanel/01-framework.md docs/adminpanel/INDEX.md arch/18-metrics.md arch/roadmap/pgworker.md
  git commit -m "docs(t08): arch-first — общие сборки Shared.Core/Etcd/Tls в arch/adminpanel (Core → Shared.Core+Shared.Etcd: транспортные records), 18-metrics, docs/adminpanel; roadmap-заглушка t09 (Pg↔Kfw-дубли)"
  ```

**Выход:** arch/-канон описывает целевую структуру ДО кода. **Проверка:** Step 8. **Spec:** §3.1 (arch-first), §8 Фаза A.

---

## Фаза B — `Shared.Core` (Д2–Д7)

### Task 2: Сборка Shared.Core — перенос каркаса as-is

**Files:**
- Create: `src/Shared.Core/Shared.Core.csproj`
- Create: `src/Shared.Core/ModuleExtensions.cs`
- Create (git mv из канонических копий, ниже): `src/Shared.Core/DI/*` (8 файлов), `Result.cs`, `CQRS/*` (5), `Contexts/ServiceProviderHelper.cs`, `Traces/Tracing.cs`, `HealthChecks/{HealthCheckAbstract,IHealthCheckService}.cs`, `Retry/{IRetryConfig,RetryPolicies}.cs`
- Modify: `src/PgWorker.slnx` (папка `/common/`)

**Interfaces (Produces — для последующих задач):**
- `namespace Shared.Core`: `Result`, `Result<T>`, `ResultSuccess(Error)`, `ResultError(Error)`, `ResultExtensions` (as-is из `AdminPanel.Infrastructure/Result.cs`)
- `namespace Shared.Core.DI`: `[InjectAs(Singleton|Scoped|Transient)]`, `[Config]`, `DiTypeBehaviour`, `AutoRegistration*`, `ServiceCollectionExtensions.AutoRegistration(...)`, `ServiceProviderExtensions`, `UseDiBehavioursExtensions.UseDiBehaviours(...)`
- `namespace Shared.Core.CQRS`: `IQuery<T>`, `IQueryHandler<TQ,TR>`, `ICommand<T>`, `ICommandHandler<TC,TR>`, `IHandler` (+ internal `Handler`)
- `namespace Shared.Core.Contexts`: `IServiceProviderHelper`
- `namespace Shared.Core.Traces`: `Tracing` (Init/Activity/ActivityVT/ActivityT)
- `namespace Shared.Core.HealthChecks`: `HealthCheckAbstract<T>`, `IHealthCheckService`
- `namespace Shared.Core.Retry`: `IRetryConfig`, `RetryPolicies.{HttpRetry,GeneralRetry}`
- `Shared.Core.ModuleExtensions.AddSharedCore(this IServiceCollection)`

**Вход:** Task 1 закоммичен.

- [ ] **Step 1: csproj** — создать `src/Shared.Core/Shared.Core.csproj` (пакеты — версии уже в CPM, spec §4.1):
  ```xml
  <Project Sdk="Microsoft.NET.Sdk">

      <ItemGroup>
          <PackageReference Include="Microsoft.Extensions.Configuration.Abstractions"/>
          <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions"/>
          <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions"/>
          <PackageReference Include="Microsoft.Extensions.Options.ConfigurationExtensions"/>
          <PackageReference Include="Polly"/>
      </ItemGroup>

      <ItemGroup>
          <InternalsVisibleTo Include="Shared.Core.UnitTests"/>
      </ItemGroup>

  </Project>
  ```
- [ ] **Step 2: Перенос файлов as-is (git mv + замена namespace одним perl):**
  ```bash
  mkdir -p src/Shared.Core/{DI,CQRS,Contexts,Traces,HealthChecks,Retry}
  # DI — канон панель (самый полный, 8 файлов вкл. UseDiBehavioursExtensions):
  git mv src/AdminPanel.Infrastructure/DI/*.cs src/Shared.Core/DI/
  # Result/CQRS/Contexts/Traces — панельные копии (единственные):
  git mv src/AdminPanel.Infrastructure/Result.cs src/Shared.Core/Result.cs
  git mv src/AdminPanel.Infrastructure/CQRS/*.cs src/Shared.Core/CQRS/
  git mv src/AdminPanel.Infrastructure/Contexts/ServiceProviderHelper.cs src/Shared.Core/Contexts/
  git mv src/AdminPanel.Infrastructure/Traces/Tracing.cs src/Shared.Core/Traces/
  # HealthChecks — канон PgWorker.App (самый документированный):
  git mv src/PgWorker.App/HealthChecks/HealthCheckAbstract.cs src/Shared.Core/HealthChecks/
  git mv src/PgWorker.App/HealthChecks/IHealthCheckService.cs src/Shared.Core/HealthChecks/
  # Retry — канон KafkaWorker.Core (содержит оба общих метода HttpRetry+GeneralRetry):
  git mv src/KafkaWorker.Core/Retry/IRetryConfig.cs src/Shared.Core/Retry/
  git mv src/KafkaWorker.Core/Retry/RetryPolicies.cs src/Shared.Core/Retry/
  # Замена namespace в перенесённых файлах:
  perl -pi -e 's/^namespace AdminPanel\.Infrastructure(\.\w+)?;/namespace Shared.Core$1;/' src/Shared.Core/DI/*.cs src/Shared.Core/CQRS/*.cs src/Shared.Core/Contexts/*.cs src/Shared.Core/Traces/*.cs src/Shared.Core/Result.cs
  perl -pi -e 's/^namespace PgWorker\.App\.HealthChecks;/namespace Shared.Core.HealthChecks;/' src/Shared.Core/HealthChecks/*.cs
  perl -pi -e 's/^namespace KafkaWorker\.Core\.Retry;/namespace Shared.Core.Retry;/' src/Shared.Core/Retry/*.cs
  # Внутренние using перенесённых файлов:
  perl -pi -e 's/using AdminPanel\.Infrastructure(\.\w+)?;/using Shared.Core$1;/' src/Shared.Core/DI/*.cs src/Shared.Core/CQRS/*.cs src/Shared.Core/Contexts/*.cs src/Shared.Core/Traces/*.cs
  perl -pi -e 's/^using PgWorker\.Core;$/using Shared.Core;/' src/Shared.Core/HealthChecks/IHealthCheckService.cs
  ```
  Примечания: `KafkaWorker.Core/Retry/RetryPolicies.cs` содержит только `using System.Net.Http; using Polly; using Polly.Retry;` (namespace-using отсутствует — проверка: `grep -n '^using' src/Shared.Core/Retry/RetryPolicies.cs`). В `IHealthCheckService.cs` после замены `using PgWorker.Core;` → `using Shared.Core;` тип `Result` резолвится; в CQRS-файлах using на `Contexts/DI/Traces` переписываются последним perl. Панельные `AdminPanel.Infrastructure/HealthChecks/*.cs` и `ModuleExtensions.cs` на этом шаге НЕ трогаются — их удаляет Task 4 (первый полный sln-билд — только Task 6).
- [ ] **Step 3: ModuleExtensions** — создать `src/Shared.Core/ModuleExtensions.cs` (замена панельного `AddInfrastructure`; регистрирует internal `Handler` из CQRS):
  ```csharp
  using System.Reflection;
  using Microsoft.Extensions.DependencyInjection;
  using Shared.Core.DI;

  namespace Shared.Core;

  // Модуль каркаса: регистрирует все типы сборки через attribute-DI
  // (замена панельного AdminPanel.Infrastructure.AddInfrastructure, t08).
  public static class ModuleExtensions
  {
      private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

      public static IServiceCollection AddSharedCore(this IServiceCollection services)
          => services.AutoRegistration(Assembly);
  }
  ```
- [ ] **Step 4: slnx** — в `src/PgWorker.slnx` внутрь `<Folder Name="/common/">` после строки Shared.Metrics добавить:
  ```xml
          <Project Path="Shared.Core/Shared.Core.csproj" />
  ```
- [ ] **Step 5: Сборка** — `dotnet build src/Shared.Core/Shared.Core.csproj -c Release`: 0 errors/warnings.
  Примечание: на этом шаге панель/воркеры ещё не переключены, а в `AdminPanel.Infrastructure` остались файлы (`ModuleExtensions.cs`, `HealthChecks/*`), чьи namespace ссылаются на перенесённые типы — поэтому собираем ИМЕННО проект, не слн (первый полный слн-билд — Task 6, после чистки Task 4).
- [ ] **Step 6: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): сборка Shared.Core — перенос каркаса as-is (DI/Result/CQRS/Contexts/Traces из AdminPanel.Infrastructure, HealthChecks из PgWorker.App, Retry из KafkaWorker.Core)"
  ```

**Выход:** компилируемая сборка-каркас. **Проверка:** Step 5. **Spec:** §4.1, §3.3–§3.4.

### Task 3: Shared.Core.UnitTests — переезд тестов каркаса

**Files:**
- Create: `src/tests/Shared.Core.UnitTests/Shared.Core.UnitTests.csproj`
- Create: `src/tests/Shared.Core.UnitTests/GlobalUsings.cs`
- Move: `src/tests/AdminPanel.UnitTests/{ResultTests,AutoRegistrationTests,CQRSTests,TestHost}.cs` → `src/tests/Shared.Core.UnitTests/`
- Modify: `src/PgWorker.slnx`

**Interfaces (Consumes):** `Shared.Core` (Task 2).

**Вход:** Task 2 закоммичен.

- [ ] **Step 1: csproj** (по образцу `Shared.Metrics.UnitTests`; `Microsoft.Extensions.Configuration` и `.DependencyInjection` — для TestHost, версии в CPM):
  ```xml
  <Project Sdk="Microsoft.NET.Sdk">

      <ItemGroup>
          <Using Include="Xunit"/>
      </ItemGroup>

      <ItemGroup>
          <PackageReference Include="coverlet.collector">
              <PrivateAssets>all</PrivateAssets>
              <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
          </PackageReference>
          <PackageReference Include="FluentAssertions"/>
          <PackageReference Include="Microsoft.Extensions.Configuration"/>
          <PackageReference Include="Microsoft.Extensions.DependencyInjection"/>
          <PackageReference Include="Microsoft.NET.Test.Sdk"/>
          <PackageReference Include="xunit.runner.visualstudio">
              <PrivateAssets>all</PrivateAssets>
              <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
          </PackageReference>
          <PackageReference Include="xunit.v3"/>
      </ItemGroup>

      <ItemGroup>
          <ProjectReference Include="..\..\Shared.Core\Shared.Core.csproj"/>
      </ItemGroup>

  </Project>
  ```
- [ ] **Step 2: GlobalUsings.cs:**
  ```csharp
  // Глобальные using тестового проекта: FluentAssertions во всех тест-файлах.
  global using FluentAssertions;
  ```
- [ ] **Step 3: Переезд тестов as-is + правки namespace:**
  ```bash
  git mv src/tests/AdminPanel.UnitTests/ResultTests.cs src/tests/AdminPanel.UnitTests/AutoRegistrationTests.cs src/tests/AdminPanel.UnitTests/CQRSTests.cs src/tests/AdminPanel.UnitTests/TestHost.cs src/tests/Shared.Core.UnitTests/
  perl -pi -e 's/^namespace AdminPanel\.UnitTests;/namespace Shared.Core.UnitTests;/' src/tests/Shared.Core.UnitTests/*.cs
  perl -pi -e 's/using AdminPanel\.Infrastructure(\.\w+)?;/using Shared.Core$1;/' src/tests/Shared.Core.UnitTests/*.cs
  perl -ni -e 'print unless /^using AdminPanel\.Infrastructure;$/' src/tests/Shared.Core.UnitTests/*.cs
  perl -ni -e 'print unless /^using FluentAssertions;$/' src/tests/Shared.Core.UnitTests/*.cs   # FluentAssertions — глобально из GlobalUsings.cs
  ```
  В `TestHost.cs` заменить вызов `services.AddInfrastructure();` → `services.AddSharedCore();` и `Tracing.Init("AdminPanel.UnitTests")` → `Tracing.Init("Shared.Core.UnitTests")` (Edit вручную; комментарий «Скан сборки каркаса: ServiceProviderHelper, IHandler…» сохранить).
- [ ] **Step 4: slnx** — в `<Folder Name="/tests/">` добавить `<Project Path="tests/Shared.Core.UnitTests/Shared.Core.UnitTests.csproj" />`.
- [ ] **Step 5: Прогон** — `dotnet test src/tests/Shared.Core.UnitTests -c Release`: все тесты PASS (ResultTests + AutoRegistrationTests + CQRSTests; WAF не нужен — чистый DI-хост TestHost).
- [ ] **Step 6: Commit**
  ```bash
  git add -A && git commit -m "test(t08): Shared.Core.UnitTests — переезд ResultTests/AutoRegistrationTests/CQRSTests из AdminPanel.UnitTests (as-is)"
  ```

**Выход:** каркас покрыт тестами в общем контуре. **Проверка:** Step 5. **Spec:** §4.5.

### Task 4: Переключение панели на Shared.Core + удаление остатков кода AdminPanel.Infrastructure

**Files:**
- Modify: `src/AdminPanel.Core/AdminPanel.Core.csproj`, `src/AdminPanel.Etcd/AdminPanel.Etcd.csproj`, `src/AdminPanel.Probes/AdminPanel.Probes.csproj`, `src/AdminPanel.Api/AdminPanel.Api.csproj`, `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`, `src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj`
- Modify: `src/AdminPanel.Api/Program.cs` (AddInfrastructure → AddSharedCore)
- Delete: `src/AdminPanel.Infrastructure/ModuleExtensions.cs` (роль — `AddSharedCore` из Shared.Core), `src/AdminPanel.Infrastructure/HealthChecks/HealthCheckAbstract.cs`, `src/AdminPanel.Infrastructure/HealthChecks/IHealthCheckService.cs` (третья копия дубля Д6; канон — в `Shared.Core/HealthChecks` из Task 2)
- Modify (perl-массштаб из «Механики»): ~180 файлов `src/AdminPanel.*` + `src/tests/AdminPanel.*`

**Interfaces (Consumes):** `Shared.Core` (Task 2).

**Вход:** Task 3 закоммичен.

- [ ] **Step 1: Ссылки и глобальные using.** В `AdminPanel.Core.csproj` заменить `ProjectReference … AdminPanel.Infrastructure.csproj` на:
  ```xml
      <ProjectReference Include="..\Shared.Core\Shared.Core.csproj"/>
  ```
  и добавить в тот же ItemGroup-набор:
  ```xml
      <Using Include="Shared.Core"/>
  ```
  В `AdminPanel.Etcd.csproj`, `AdminPanel.Probes.csproj` добавить `<Using Include="Shared.Core"/>`. В `AdminPanel.Api.csproj` удалить `ProjectReference … AdminPanel.Infrastructure.csproj`, добавить `ProjectReference … ..\Shared.Core\Shared.Core.csproj` и `<Using Include="Shared.Core"/>`. В `AdminPanel.UnitTests.csproj` заменить ссылку `AdminPanel.Infrastructure` → `Shared.Core` + `<Using Include="Shared.Core"/>`. В `AdminPanel.IntegrationTests.csproj` добавить `<Using Include="Shared.Core"/>` (фейки используют `Result`).
- [ ] **Step 2: Program.cs** — `src/AdminPanel.Api/Program.cs`: заменить `.AddInfrastructure()` на `.AddSharedCore()` (строка ~31); using-строки покроет Step 3.
- [ ] **Step 3: Массовые замены using** — выполнить perl-команды «панель (фаза B)» из раздела «Механика миграции» (DI/CQRS/Contexts/Traces/HealthChecks + удаление корневого). В `src/AdminPanel.Etcd/Client/*` корневой using удаляется тем же perl, `Result` приходит из глобального using csproj — фактическая компиляция проверяется сборкой Step 6.
- [ ] **Step 4: Удаление остатков кода AdminPanel.Infrastructure.** Потребители уже переведены на `Shared.Core.*` (Step 3):
  ```bash
  git rm src/AdminPanel.Infrastructure/ModuleExtensions.cs
  git rm src/AdminPanel.Infrastructure/HealthChecks/HealthCheckAbstract.cs src/AdminPanel.Infrastructure/HealthChecks/IHealthCheckService.cs
  ```
  Обоснование: `ModuleExtensions.AddInfrastructure` больше не вызывается (Program.cs → `AddSharedCore`, панельный TestHost переехал в Task 3), а панельные HealthChecks — забытая третья копия Д6 (spec §8 Фаза B: «HealthChecks ×3» удаляются; канон уже в Shared.Core из Task 2). После этого шага от `AdminPanel.Infrastructure` остаётся csproj без единого .cs — пустая сборка собирается зелёно и остаётся в slnx до фазы E (удаление сборки и slnx-строки — Task 12, по spec §8 Фаза E «удалить AdminPanel.Infrastructure»).
- [ ] **Step 5: Проверка пустоты:** `find src/AdminPanel.Infrastructure -name '*.cs' | wc -l` → 0 (только csproj).
- [ ] **Step 6: Сборка панели** — `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj -c Release`: 0 warnings. Если CS0246/CS0104 — точечно докомплектовать using (список файлов из ошибки компилятора).
- [ ] **Step 7: Юниты панели** — `dotnet test src/tests/AdminPanel.UnitTests -c Release`: PASS (TestHost уже переехал; AutoRegistration-кеш — per-process, поведение не менялось).
- [ ] **Step 8: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): панель на Shared.Core — ссылки/глобальные using, AddSharedCore; остатки кода AdminPanel.Infrastructure удалены (ModuleExtensions, HealthChecks-копия; сборка-пустышка до фазы E)"
  ```

**Выход:** панель собирается и юниты зелёные на общем каркасе; в `AdminPanel.Infrastructure` нет кода. **Проверка:** Steps 5–7. **Spec:** §4.1, §4.4 (строка /admin/), §3.5, §8 Фаза B (HealthChecks ×3).

### Task 5: Переключение PgWorker на Shared.Core

**Files:**
- Modify: `src/PgWorker.Core/PgWorker.Core.csproj` (+Shared.Core, +Using; −пакеты, ставшие общими)
- Create: `src/PgWorker.Core/Retry/SqlRetryPolicies.cs` (локальный Npgsql-ретрай)
- Delete: `src/PgWorker.Core/DI/` (7 файлов — отдельная воркерская копия; канон уже перенесён из панели в Task 2), `src/PgWorker.Core/Result.cs`, `src/PgWorker.Core/Retry/{IRetryConfig,RetryPolicies}.cs`, `src/PgWorker.App/HealthChecks/{HealthCheckAbstract,IHealthCheckService}.cs`, `src/tests/PgWorker.UnitTests/ResultTests.cs`
- Modify: `src/PgWorker.Etcd/PgWorker.Etcd.csproj`, `src/PgWorker.Docker/PgWorker.Docker.csproj`, `src/PgWorker.Provisioning/PgWorker.Provisioning.csproj`, `src/PgWorker.Moves/PgWorker.Moves.csproj`, `src/PgWorker.Backups/PgWorker.Backups.csproj`, `src/PgWorker.App/PgWorker.App.csproj`, `src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, `src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj` (+`<Using Include="Shared.Core"/>`)
- Modify: `src/PgWorker.Provisioning/Sql/DatabaseProvisioner.cs` (2 вызова), `src/PgWorker.Moves/Sql/NpgsqlMoveSqlExecutor.cs` (3 вызова): `RetryPolicies.SqlRetry` → `SqlRetryPolicies.SqlRetry`

**Interfaces (Consumes):** `Shared.Core` (Task 2).

**Вход:** Task 4 закоммичен.

- [ ] **Step 1: Локальный SqlRetry.** Создать `src/PgWorker.Core/Retry/SqlRetryPolicies.cs` (Npgsql остаётся у воркера — spec §4.1):
  ```csharp
  using Npgsql;
  using Polly;
  using Polly.Retry;

  namespace PgWorker.Core.Retry;

  // SQL-ретрай воркера (Npgsql): HttpRetry/GeneralRetry — общие в Shared.Core.Retry
  // (t08), локальный класс переименован из RetryPolicies, чтобы не пересекаться
  // именем с общим.
  public static class SqlRetryPolicies
  {
      // Транзиентные ошибки Npgsql (обрывы соединения, timeout) и отмены задач;
      // политику применяет вызывающий код к коротким операциям.
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
              })
             .Build();
  }
  ```
- [ ] **Step 2: Вызовы SqlRetry** — в `DatabaseProvisioner.cs` (строки ~141, ~163) и `NpgsqlMoveSqlExecutor.cs` (строки ~23, ~45, ~74) заменить `RetryPolicies.SqlRetry(` → `SqlRetryPolicies.SqlRetry(` (using `PgWorker.Core.Retry` уже есть).
- [ ] **Step 3: Удаление дублей PgWorker:**
  ```bash
  git rm -r src/PgWorker.Core/DI src/PgWorker.Core/Retry/IRetryConfig.cs src/PgWorker.Core/Retry/RetryPolicies.cs
  git rm src/PgWorker.Core/Result.cs
  git rm src/PgWorker.App/HealthChecks/HealthCheckAbstract.cs src/PgWorker.App/HealthChecks/IHealthCheckService.cs
  git rm src/tests/PgWorker.UnitTests/ResultTests.cs   # 39-строчный smoke-дубль; полный ResultTests — в Shared.Core.UnitTests
  ```
- [ ] **Step 4: csproj-правки.** `PgWorker.Core.csproj`: добавить `ProjectReference ..\Shared.Core\Shared.Core.csproj` + `<Using Include="Shared.Core"/>`; удалить PackageReference `Microsoft.Extensions.Configuration.Abstractions`, `Microsoft.Extensions.DependencyInjection.Abstractions`, `Microsoft.Extensions.Options.ConfigurationExtensions`, `Polly` (все использования ушли в Shared.Core; Npgsql остаётся). Остальным csproj (список Files) добавить `<Using Include="Shared.Core"/>`.
- [ ] **Step 5: HealthChecks-using** — perl «воркеры, HealthChecks» из «Механики» (PgWorker.App.HealthChecks → Shared.Core.HealthChecks, 5 файлов: `PgWorkerHealth.cs`, `ServiceProbes.cs`, тесты).
- [ ] **Step 6: Сборка+юниты** — `dotnet build src/PgWorker.App/PgWorker.App.csproj -c Release` → 0 warnings; `dotnet test src/tests/PgWorker.UnitTests -c Release` → PASS.
- [ ] **Step 7: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): PgWorker на Shared.Core — DI/Result/Retry/HealthChecks-дубли удалены; SqlRetry локально (SqlRetryPolicies)"
  ```

**Выход:** PgWorker-контур на общем каркасе. **Проверка:** Step 6. **Spec:** §4.1 (SqlRetry — локально), §4.4 (строка /core/), §4.5.

### Task 6: Переключение KafkaWorker на Shared.Core + гейт фазы B

**Files:**
- Delete: `src/KafkaWorker.Core/{DI,Retry}/` (7+2 файла), `src/KafkaWorker.Core/Result.cs`, `src/KafkaWorker.App/HealthChecks/{HealthCheckAbstract,IHealthCheckService}.cs`, `src/tests/KafkaWorker.UnitTests/ResultTests.cs`
- Modify: `src/KafkaWorker.Core/KafkaWorker.Core.csproj` (+Shared.Core,+Using; −4 общих пакета), `src/KafkaWorker.{Etcd,Docker,Provisioning,App}/*.csproj` + `src/tests/KafkaWorker.{UnitTests,IntegrationTests}/*.csproj` (+`<Using Include="Shared.Core"/>`)

**Вход:** Task 5 закоммичен.

- [ ] **Step 1: Удаление дублей:**
  ```bash
  git rm -r src/KafkaWorker.Core/DI src/KafkaWorker.Core/Retry
  git rm src/KafkaWorker.Core/Result.cs
  git rm src/KafkaWorker.App/HealthChecks/HealthCheckAbstract.cs src/KafkaWorker.App/HealthChecks/IHealthCheckService.cs
  git rm src/tests/KafkaWorker.UnitTests/ResultTests.cs   # smoke-дубль
  ```
- [ ] **Step 2: csproj** — как в Task 5 Step 4 (у `KafkaWorker.Core` общие пакеты удаляются, Npgsql тут не было).
- [ ] **Step 3: HealthChecks-using** — perl для `KafkaWorker.App.HealthChecks` (5 файлов).
- [ ] **Step 4: Гейт фазы B (полный слн):**
  ```bash
  dotnet build src/PgWorker.slnx -c Release
  ```
  0 warnings/errors. Примечание: `AdminPanel.Infrastructure` ещё в решении — это csproj БЕЗ единого .cs-файла (код перенесён в Task 2 либо удалён в Task 4), пустая сборка собирается зелёно; удаление сборки и slnx-строки — фаза E (Task 12).
- [ ] **Step 5: Все юнит-серии:**
  ```bash
  dotnet test src/tests/Shared.Core.UnitTests -c Release
  dotnet test src/tests/Shared.Metrics.UnitTests -c Release
  dotnet test src/tests/PgWorker.UnitTests -c Release
  dotnet test src/tests/KafkaWorker.UnitTests -c Release
  dotnet test src/tests/AdminPanel.UnitTests -c Release
  ```
  Все PASS (серии запускаются последовательно, docker не нужен).
- [ ] **Step 6: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): KafkaWorker на Shared.Core; гейт фазы B — слн Release + все юниты зелёные"
  ```

**Выход:** фаза B завершена (Д2–Д7 закрыты; все три копии HealthChecks удалены). **Проверка:** Steps 4–5. **Spec:** §8 Фаза B, §9.2, §9.3 (частично).

---

## Фаза C — `Shared.Etcd` (Д1)

### Task 7: Сборка Shared.Etcd (union-API) + объединённые тесты

**Files:**
- Create: `src/Shared.Etcd/Shared.Etcd.csproj`
- Move: `src/PgWorker.Etcd/Client/*` → `src/Shared.Etcd/Client/*` (база — воркерская копия; Pg↔Kfw идентичны, diff — только namespace)
- Create: `src/Shared.Etcd/Client/EtcdModels.cs` (панельные records + Revision-расширение)
- Create: `src/tests/Shared.Etcd.UnitTests/` (csproj + GlobalUsings + EtcdGatewayTests.cs)
- Move: `src/tests/PgWorker.UnitTests/Etcd/EtcdGatewayTests.cs` → `src/tests/Shared.Etcd.UnitTests/EtcdGatewayTests.cs`
- Delete: `src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs` (после переноса кейсов)
- Modify: `src/PgWorker.slnx`

**Interfaces (Produces):**
- `namespace Shared.Etcd.Client`: `IEtcdGateway` (union-API, ниже), `EtcdGateway` (+`const string HttpClientName = "etcd"`), `Kv`, `EtcdHttpException`, `EtcdUnreachableException`, `TxnTarget`, `TxnPredicate`, `TxnCompare` (+синтаксические фабрики `NotExists`/`ValueEqual`/`ModRevisionEqual` — as-is воркерские, семантика закреплена spec §4.2), `TxnOp.Put/Delete`, `TxnRequest` (+фабрика `Of`), `TxnResult`, `EtcdStatusPayload` (+`ulong? Revision`), `EtcdMember`, `EtcdAlarm`, `EtcdAlarmType`

**Вход:** Task 6 закоммичен.

- [ ] **Step 1: csproj** — `src/Shared.Etcd/Shared.Etcd.csproj`:
  ```xml
  <Project Sdk="Microsoft.NET.Sdk">

      <ItemGroup>
          <PackageReference Include="Microsoft.Extensions.Http"/>
          <ProjectReference Include="..\Shared.Core\Shared.Core.csproj"/>
      </ItemGroup>

  </Project>
  ```
- [ ] **Step 2: Перенос воркерской копии:**
  ```bash
  mkdir -p src/Shared.Etcd/Client
  git mv src/PgWorker.Etcd/Client/EtcdGateway.cs src/PgWorker.Etcd/Client/IEtcdGateway.cs src/PgWorker.Etcd/Client/Kv.cs src/Shared.Etcd/Client/
  perl -pi -e 's/^namespace PgWorker\.Etcd\.Client;/namespace Shared.Etcd.Client;/' src/Shared.Etcd/Client/*.cs
  # Result в Shared.Etcd.Client не входит в охватывающий namespace — прямой обмен using:
  perl -pi -e 's/^using PgWorker\.Core;$/using Shared.Core;/' src/Shared.Etcd/Client/EtcdGateway.cs src/Shared.Etcd/Client/IEtcdGateway.cs
  ```
- [ ] **Step 3: Union-API в IEtcdGateway.cs.** Сигнатуры — воркерские без изменений, кроме Status + два панельных метода (spec §4.2). Итоговый состав интерфейса (doc-комментарии сохранить из воркерской копии; для новых — из панельной):
  ```csharp
  using Shared.Core;

  namespace Shared.Etcd.Client;

  // Клиент etcd через HTTP JSON gateway /v3/*: union воркеров (Pg/Kfw) и панели (t08).
  public interface IEtcdGateway
  {
      Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct);
      Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct);
      Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct);
      Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct);
      Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct);
      Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct);
      Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct);
      Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct);
      Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct);
      // Статус: полный payload (панель) + Revision из header.revision (воркеры — compaction).
      Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct);
      Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct);
      Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct);
      Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct);
      Task<Result> DefragmentAsync(string endpoint, CancellationToken ct);
  }
  ```
  Тело файла (TxnTarget/TxnPredicate/TxnCompare с фабриками `NotExists`/`ValueEqual`/`ModRevisionEqual`, TxnOp/TxnRequest с `Of`/TxnResult) — as-is воркерское; фабрики — закреплённый spec §4.2 («Синтаксические фабрики txn-моделей») эквивалент прямой формы с той же сериализацией в `CompareToDto`. Панельные `TxnCompare(Key,Version,ModRevision)`/`KvPut` НЕ переносятся (замены — фабрики и `TxnOp.Put`).
- [ ] **Step 4: Панельные модели.** Создать `src/Shared.Etcd/Client/EtcdModels.cs` (перенос as-is из `AdminPanel.Core/EtcdStatus.cs` + payload с Revision из `AdminPanel.Etcd/Client/IEtcdGateway.cs`; источники удаляются в Task 9):
  ```csharp
  namespace Shared.Etcd.Client;

  // Данные status-ответа без контекста endpoint (url/latency добавляет refresher панели).
  // Revision — из header.revision (int64 → ulong-decimal-строка protojson; воркеры
  // используют для compaction, t08).
  public sealed record EtcdStatusPayload(
      string? Version,
      long? DbSizeBytes,
      ulong? LeaderMemberId,
      ulong? RaftIndex,
      ulong? RaftTerm,
      ulong? Revision);

  // Член etcd-кластера из /v3/cluster/member/list.
  public sealed record EtcdMember(
      ulong Id,
      string? Name,
      IReadOnlyList<string> PeerUrls,
      IReadOnlyList<string> ClientUrls);

  // Активная тревога из /v3/maintenance/alarm.
  public sealed record EtcdAlarm(ulong MemberId, EtcdAlarmType Type);

  // Значения enum-поля alarm в gateway: 0/1/2.
  public enum EtcdAlarmType
  {
      None = 0,
      NoSpace = 1,
      Corrupt = 2,
  }
  ```
- [ ] **Step 5: EtcdGateway.cs — панельные методы в общую реализацию.** (а) добавить `EtcdUnreachableException` (as-is панельный, после `EtcdHttpException`); (б) `public const string HttpClientName = "etcd";` внутрь класса; (в) заменить `StatusAsync` и добавить `MemberListAsync`/`AlarmAsync`:
  ```csharp
  public async Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
  {
      var result = await Result<StatusResponse>.FromAsync(
          async () => await PostAsync<StatusResponse>(endpoint, "/v3/maintenance/status", new { }, ct));
      return result.Map(r => new EtcdStatusPayload(
          r.Version, r.DbSize, r.Leader, r.RaftIndex, r.RaftTerm, r.Header?.Revision));
  }

  public async Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
  {
      var result = await Result<MemberListResponse>.FromAsync(
          async () => await PostAsync<MemberListResponse>(endpoint, "/v3/cluster/member/list", new { }, ct));
      return result.Map(r => (IReadOnlyList<EtcdMember>)(r.Members ?? [])
          .Select(m => new EtcdMember(m.Id, m.Name, m.PeerUrls ?? [], m.ClientUrls ?? []))
          .ToList());
  }

  public async Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
  {
      var result = await Result<AlarmResponse>.FromAsync(
          async () => await PostAsync<AlarmResponse>(endpoint, "/v3/maintenance/alarm", new { }, ct));
      return result.Map(r => (IReadOnlyList<EtcdAlarm>)(r.Alarms ?? [])
          .Select(a => new EtcdAlarm(a.MemberId, a.Type))
          .ToList());
  }
  ```
  (г) DTO: `StatusResponse` расширить полями панели (header остаётся):
  ```csharp
  private sealed class StatusResponse
  {
      [JsonPropertyName("header")]
      public StatusHeader? Header { get; set; }

      [JsonPropertyName("version")]
      public string? Version { get; set; }

      [JsonPropertyName("dbSize")]
      public long? DbSize { get; set; }

      [JsonPropertyName("leader")]
      public ulong? Leader { get; set; }

      [JsonPropertyName("raftIndex")]
      public ulong? RaftIndex { get; set; }

      [JsonPropertyName("raftTerm")]
      public ulong? RaftTerm { get; set; }
  }
  ```
  и добавить DTO `MemberListResponse`/`MemberDto`/`AlarmResponse`/`AlarmDto` as-is из панельного `EtcdGateway.cs`. Панельный атрибут `[InjectAsSingleton(typeof(IEtcdGateway))]` на класс НЕ переносить (регистрацию делает `AddHttpClient<EtcdGateway>` панели; воркеры атрибут не сканируют — spec §4.2).
- [ ] **Step 6: slnx** — `/common/` + `<Project Path="Shared.Etcd/Shared.Etcd.csproj" />`; `/tests/` + `tests/Shared.Etcd.UnitTests/…`.
- [ ] **Step 7: Тесты.** Создать `src/tests/Shared.Etcd.UnitTests/`:
  - `Shared.Etcd.UnitTests.csproj` — копия `Shared.Core.UnitTests.csproj` с заменой ProjectReference на `..\..\Shared.Etcd\Shared.Etcd.csproj` (пакеты `Microsoft.Extensions.Configuration`/`.DependencyInjection` не нужны — чистый HttpClient-тест; TestHost не переносится).
  - `GlobalUsings.cs`:
    ```csharp
    // Глобальные using тестового проекта.
    global using FluentAssertions;
    global using Shared.Core;
    global using Shared.Etcd.Client;
    ```
  - Перенести воркерский файл тестов и вычистить using:
    ```bash
    git mv src/tests/PgWorker.UnitTests/Etcd/EtcdGatewayTests.cs src/tests/Shared.Etcd.UnitTests/EtcdGatewayTests.cs
    perl -pi -e 's/^namespace PgWorker\.UnitTests\.Etcd;/namespace Shared.Etcd.UnitTests;/' src/tests/Shared.Etcd.UnitTests/EtcdGatewayTests.cs
    perl -ni -e 'print unless /^using PgWorker\.Core;$/ || /^using PgWorker\.Etcd\.Client;$/ || /^using Xunit;$/' src/tests/Shared.Etcd.UnitTests/EtcdGatewayTests.cs
    ```
  - Дополнить файл панельными кейсами из `src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs` (переписать на union-API, AAA-комментарии сохранить): `Range_DecodesBase64Kvs`, `Range_MissingKvs_EmptyList`, `Status_ParsesFields` (конструктор payload теперь 6-арный — дообновить), `MemberList_ParsesUrls`, `Alarm_MapsAlarmType`, `HttpError_ReturnsFailed`, `NetworkError_ReturnsFailed`, `Txn_CompareFailed_MapsSucceededFalse`, `Put_RequestHasBase64KeyValue`, `Delete_Prefix_RequestHasKeyAndRangeEnd`. Панельный кейс `Txn_CompareAndPuts_RequestHasBase64Bodies` переезжает как `Txn_RequestHasBase64Bodies` с сигнатурой через фабрики: `TxnAsync(endpoint, TxnRequest.Of([TxnCompare.NotExists("/k")], [new TxnOp.Put("/k","v",null)]), ct)` (ассерты тела запроса — прежние).
  - НОВЫЙ кейс №1 (регрессия Revision; семантика до/после сверяется ревью тела StatusAsync — Step 5):
    ```csharp
    [Fact]
    public async Task Status_ParsesRevision_FromHeader()
    {
        // Arrange — header.revision приходит int64-decimal-строкой (protojson).
        var handler = new FakeHandler(_ => Json(
            """{"header":{"revision":"42"},"version":"3.5.21","leader":"1"}"""));
        var gateway = NewGateway(handler);

        // Act
        var status = await gateway.StatusAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        status.IsSuccess.Should().BeTrue();
        status.Value.Revision.Should().Be(42);
        status.Value.Version.Should().Be("3.5.21");
    }
    ```
  - НОВЫЙ кейс №2 (маппинг фабрик — spec §4.2 «Синтаксические фабрики txn-моделей»: `NotExists` = compare `version == 0`, `TxnRequest.Of` ≡ прямая форма — та же сериализация в `CompareToDto`):
    ```csharp
    [Fact]
    public async Task Txn_FactoryNotExists_SerializesVersionZeroCompare()
    {
        // Arrange — фабрика NotExists(key) эквивалентна прямой форме
        // new TxnCompare(key, TxnTarget.Version, TxnPredicate.Equal, "", 0) (spec §4.2).
        var handler = new FakeHandler(_ => Json("""{"succeeded":true}"""));
        var gateway = NewGateway(handler);

        // Act
        await gateway.TxnAsync("http://etcd:2379",
            TxnRequest.Of([TxnCompare.NotExists("/k")], [new TxnOp.Put("/k", "v", null)]),
            CancellationToken.None);

        // Assert: target=VERSION(0), result=EQUAL(0), version=0; success-put без lease.
        var body = JsonDocument.Parse(handler.Requests.Should().ContainSingle().Subject.Body).RootElement;
        var compare = body.GetProperty("compare")[0];
        compare.GetProperty("target").GetInt32().Should().Be(0);
        compare.GetProperty("result").GetInt32().Should().Be(0);
        compare.GetProperty("version").GetInt32().Should().Be(0);
        var put = body.GetProperty("success")[0].GetProperty("request_put");
        put.GetProperty("key").GetString().Should().Be("L2s=");
        put.GetProperty("value").GetString().Should().Be("dg==");
    }
    ```
  - Удалить исходник-дубль: `git rm src/tests/AdminPanel.UnitTests/EtcdGatewayTests.cs`.
- [ ] **Step 8: Прогон** — `dotnet test src/tests/Shared.Etcd.UnitTests -c Release`: PASS (воркерские + панельные кейсы + Revision + фабрики).
- [ ] **Step 9: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): Shared.Etcd — union-API IEtcdGateway (воркерская база + MemberList/Alarm/StatusFull+Revision, фабрики §4.2); объединённые EtcdGatewayTests + кейсы Revision и маппинга фабрик"
  ```

**Выход:** общий etcd-клиент с протокольными тестами (вкл. фабрики txn). **Проверка:** Step 8. **Spec:** §4.2 (union-API + «Синтаксические фабрики txn-моделей»), §2.2, §4.5.

### Task 8: Воркеры на Shared.Etcd (Client удалён, SnapshotJob ×2)

**Files:**
- Delete: `src/KafkaWorker.Etcd/Client/` (3 файла), `src/PgWorker.Etcd/Client/` (уже пуст после git mv Task 7 — проверить)
- Modify: `src/PgWorker.Etcd/PgWorker.Etcd.csproj`, `src/KafkaWorker.Etcd/KafkaWorker.Etcd.csproj` (+ProjectReference Shared.Etcd)
- Modify (perl): ~102+46 файлов using
- Modify: `src/PgWorker.Provisioning/Snapshots/SnapshotJob.cs:81`, `src/KafkaWorker.Etcd/SnapshotJob.cs:81`
- Modify: тестовые фейки `IEtcdGateway` в `src/tests/PgWorker.UnitTests/` (`Api/FakeEtcdGateway.cs`, `Etcd/CoordinationTests.cs`, `App/TestSupport.cs`, `Provisioning/Fakes.cs`, `Provisioning/ClusterSecretEnsurerTests.cs`, `Backups/WalStatusWriterTests.cs`, `Writing/WorkJournalPhaseEventTests.cs`) и `src/tests/KafkaWorker.UnitTests/` (`Provisioning/Fakes.cs`, `Writing/WorkJournalPhaseEventTests.cs`)

**Interfaces (Consumes):** `Shared.Etcd.Client` (Task 7).

**Вход:** Task 7 закоммичен.

- [ ] **Step 1: Копия Kfw и ссылки:**
  ```bash
  git rm -r src/KafkaWorker.Etcd/Client
  ```
  В `PgWorker.Etcd.csproj` и `KafkaWorker.Etcd.csproj` добавить:
  ```xml
      <ProjectReference Include="..\Shared.Etcd\Shared.Etcd.csproj"/>
  ```
- [ ] **Step 2: Массовый using** — perl «etcd-клиент (фаза C)» для PgWorker/KafkaWorker из «Механики» (148 файлов).
- [ ] **Step 3: SnapshotJob ×2 — revision из payload.** В обоих файлах (`src/PgWorker.Provisioning/Snapshots/SnapshotJob.cs`, `src/KafkaWorker.Etcd/SnapshotJob.cs`, метод `MaintainAsync`, строка ~81) заменить:
  ```csharp
  // было:
  revision = status.Value;
  // стало:
  revision = (long?)status.Value.Revision;
  ```
  (семантика прежней воркерской реализации `(long)r.Header!.Revision`; null в payload невозможен на живом etcd — header обязателен).
- [ ] **Step 4: Тестовые фейки до union-API.** В каждом файле-фейке (список Files; классы `: IEtcdGateway`): (а) `StatusAsync` →
  ```csharp
  public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
      => Task.FromResult(Result<EtcdStatusPayload>.Success(
          new EtcdStatusPayload(null, null, null, null, null, 1)));
  ```
  (б) добавить заглушки:
  ```csharp
  public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
      => Task.FromResult(Result<IReadOnlyList<EtcdMember>>.Success([]));

  public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
      => Task.FromResult(Result<IReadOnlyList<EtcdAlarm>>.Success([]));
  ```
  Семантика существующих фейков (Store, txn-compare) не меняется.
- [ ] **Step 5: Сборка+юниты воркеров:**
  ```bash
  dotnet build src/PgWorker.slnx -c Release
  dotnet test src/tests/PgWorker.UnitTests -c Release
  dotnet test src/tests/KafkaWorker.UnitTests -c Release
  ```
- [ ] **Step 6: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): воркеры на Shared.Etcd — Client-копии удалены, SnapshotJob revision из payload, фейки до union-API"
  ```

**Выход:** воркерский etcd-контур на общем клиенте. **Проверка:** Step 5. **Spec:** §4.2 (правки потребителей), §4.5 (интеграции остаются на местах), §5.В2 (точечная правка SnapshotJob).

### Task 9: Панель на Shared.Etcd (WorkerCertService txn-протокол §9.9 через фабрики §4.2)

**Files:**
- Delete: `src/AdminPanel.Etcd/Client/` (3 файла)
- Modify: `src/AdminPanel.Etcd/AdminPanel.Etcd.csproj` (+ProjectReference Shared.Etcd), `src/AdminPanel.Core/AdminPanel.Core.csproj` (+ProjectReference Shared.Etcd — только ради records EtcdMember/EtcdAlarm/EtcdAlarmType), `src/tests/AdminPanel.{UnitTests,IntegrationTests}/*.csproj` (+ProjectReference Shared.Etcd при прямых ссылках)
- Modify: `src/AdminPanel.Core/EtcdStatus.cs` (удалить EtcdMember/EtcdAlarm/EtcdAlarmType — переехали в EtcdModels.cs; добавить `using Shared.Etcd.Client;`)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerCertService.cs` (2 вызова), `src/AdminPanel.Etcd/ModuleExtensions.cs` (using)
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs` — **правки тела НЕ требуются**: сигнатуры `StatusAsync` (payload, расширенный `Revision` — панелью не потребляется), `MemberListAsync`, `AlarmAsync` совпали с прежними панельными; меняется только using (Step 3). Подтверждается компиляцией Step 6. Файл зафиксирован здесь явно как точка внимания spec §2.2 (`SnapshotRefresher.cs:96-97,185`) — для сверки диффа на гейте Task 16.
- Modify (perl): 30 файлов using + панели-фейки: `src/tests/AdminPanel.UnitTests/{SnapshotRefresherTests,KafkaRefresherTests}.cs`, `src/tests/AdminPanel.UnitTests/Workers/WorkerCertServiceTests.cs`, `src/tests/AdminPanel.IntegrationTests/AuthTests.cs`

**Interfaces (Consumes):** `Shared.Etcd.Client` (Task 7), вкл. фабрики `TxnCompare.NotExists`/`TxnRequest.Of` (spec §4.2).

**Вход:** Task 8 закоммичен.

- [ ] **Step 1: Ссылки.** `AdminPanel.Etcd.csproj`: + `ProjectReference ..\Shared.Etcd\Shared.Etcd.csproj`. `AdminPanel.Core.csproj`: + `ProjectReference ..\Shared.Etcd\Shared.Etcd.csproj` (обоснование: транспортные records `EtcdMember`/`EtcdAlarm`/`EtcdAlarmType` — общие из Shared.Etcd по spec §4.2/§4.4; домен по-прежнему не знает HTTP-транспорта — отражено в arch/adminpanel/01 в Task 1).
- [ ] **Step 2: Удаление панельной копии и доменных дублей records:**
  ```bash
  git rm -r src/AdminPanel.Etcd/Client
  ```
  В `src/AdminPanel.Core/EtcdStatus.cs`: удалить определения `EtcdMember`, `EtcdAlarm`, `EtcdAlarmType` (строки ~27–43; уже переехали в `Shared.Etcd/Client/EtcdModels.cs` в Task 7), добавить `using Shared.Etcd.Client;` в шапку файла. `EtcdStatus`/`EtcdEndpoint` остаются.
- [ ] **Step 3: Массовый using** — perl «etcd-клиент (фаза C)» для AdminPanel (30 файлов; покрывает и `SnapshotRefresher.cs`, и `ModuleExtensions.cs`). Плюс точечно: типы `EtcdMember`/`EtcdAlarm`/`EtcdAlarmType` исчезли из `AdminPanel.Core` (Step 2) — файлам-потребителям records (spec §2.1: `EtcdStatusQuery`, `EtcdAlarmRule`, `SnapshotBuilder`; точный список: `git grep -l 'EtcdMember\|EtcdAlarmType' -- 'src/AdminPanel.*' 'src/tests/AdminPanel.*'`) добавить `using Shared.Etcd.Client;`. Оставшиеся пропуски покажет компиляция Step 6 (CS0246 — докомплектовать using по списку из ошибки).
- [ ] **Step 4: WorkerCertService — txn-протокол §9.9 дословно (через фабрики spec §4.2: `NotExists(key)` = compare `version == 0`, `TxnRequest.Of` — эквивалент прямой формы `new TxnRequest(Compare: […], Success: […])` с той же сериализацией в `CompareToDto`).** `GenerateAndPutAsync` (строки ~168–172), было:
  ```csharp
  var txn = await WithEtcdAsync(endpoint => gateway.TxnAsync(
      endpoint,
      [new TxnCompare(KeyOf(worker), Version: 0)],
      [new KvPut(KeyOf(worker), value)],
      ct));
  ```
  стало:
  ```csharp
  var txn = await WithEtcdAsync(endpoint => gateway.TxnAsync(
      endpoint,
      TxnRequest.Of(
          [TxnCompare.NotExists(KeyOf(worker))],
          [new TxnOp.Put(KeyOf(worker), value, null)]),
      ct));
  ```
  `PutAsync` (строки ~197–199), было: `gateway.PutAsync(endpoint, KeyOf(worker), SerializePayload(...), ct)`, стало:
  ```csharp
  var put = await WithEtcdAsync(endpoint => gateway.PutAsync(
      endpoint, KeyOf(worker), SerializePayload(certPem, keyPem, updatedBy), lease: null, ct));
  ```
  Комментарий в `DeleteAsync` (строки ~211+) про «IEtcdGateway.TxnAsync несёт только puts» обновить: общий `TxnRequest` теперь умеет delete-ветки, но атомарный txn «compare version>0 → delete» остаётся неиспользованным осознанно (TOCTOU-комментарий сохранить, причину переформулировать: «расширение протокола удаления — изменение контракта arch/adminpanel/02, вне t08»).
- [ ] **Step 5: Панельные фейки до union-API.** Файлы: `SnapshotRefresherTests.cs` (1 фейк), `KafkaRefresherTests.cs` (2 фейка), `WorkerCertServiceTests.cs`, `AuthTests.cs`. В каждом: реализовать полный union-интерфейс — существующие методы `RangeAsync`/`StatusAsync`/`MemberListAsync`/`AlarmAsync` остаются (сигнатуры совпали), `TxnAsync(endpoint, compares, puts, ct)` заменить на:
  ```csharp
  public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
  {
      // compare version==0/!=0 по семантике фейка (как раньше); puts применяются в success.
      var succeeded = req.Compare.All(c => c.Target == TxnTarget.Version
          && (!Store.ContainsKey(c.Key) && c.Num == 0 || Store.ContainsKey(c.Key) && c.Num != 0));
      if (succeeded)
          foreach (var op in req.Success)
              if (op is TxnOp.Put put)
                  Store[put.Key] = put.Value;
      return Task.FromResult(Result<TxnResult>.Success(new TxnResult(succeeded)));
  }
  ```
  (по образцу `PgWorker.UnitTests/Api/FakeEtcdGateway.cs`; внутреннее хранилище фейка — как было), `PutAsync` → сигнатура с `lease`, добавить заглушки `GetAsync`/`LeaseGrantAsync`/`LeaseRevokeAsync`/`LeaseKeepaliveAsync`/`SnapshotSaveAsync`/`CompactAsync`/`DefragmentAsync` (успех/`NotSupportedException("fake")` — как в воркерском фейке). Панельный `SnapshotRefresherTests`-фейк хранит txn-историю для ассертов — сверить семантику сравнения до/после (spec §7: «сигнатуры сравнить до/после»).
- [ ] **Step 6: Сборка+юниты панели** (компиляция подтверждает и отсутствие необходимых правок тела `SnapshotRefresher.cs`):
  ```bash
  dotnet build src/PgWorker.slnx -c Release
  dotnet test src/tests/AdminPanel.UnitTests -c Release
  ```
  PASS — в т.ч. `WorkerCertServiceTests` (регрессия txn §9.9, spec §7).
- [ ] **Step 7: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): панель на Shared.Etcd — Client удалён, WorkerCertService на TxnRequest.Of/NotExists (§9.9 через фабрики §4.2), фейки до union-API; SnapshotRefresher — только using"
  ```

**Выход:** фаза C завершена (Д1 закрыт). **Проверка:** Step 6. **Spec:** §4.2 (правки потребителей + фабрики), §2.2 (SnapshotRefresher — точка внимания без правок тела), §7 (риски 1–2 закрыты тестами).

---

## Фаза D — `Shared.Tls` (Д8)

### Task 10: Сборка Shared.Tls

**Files:**
- Create: `src/Shared.Tls/Shared.Tls.csproj`, `src/Shared.Tls/TlsEnv.cs`, `src/Shared.Tls/TlsChain.cs`, `src/Shared.Tls/TlsMaterial.cs`
- Modify: `src/PgWorker.slnx`

**Interfaces (Produces):** `namespace Shared.Tls`:
- `TlsEnv.ApplyEnvOverrides((string Env, string Key)[] bindings, ConfigurationManager configuration, Func<string, string?>? getenv = null)`
- `TlsChain.ValidateChain(X509Certificate? certificate, X509Certificate2 ca)`
- `TlsMaterial.LoadPemPair(string certPem, string keyPem)`, `TlsMaterial.LoadPem(string pem)`, `TlsMaterial.ReadPemFile(string? path)`

**Вход:** Task 9 закоммичен.

- [ ] **Step 1: csproj:**
  ```xml
  <Project Sdk="Microsoft.NET.Sdk">

      <ItemGroup>
          <PackageReference Include="Microsoft.Extensions.Configuration"/>
      </ItemGroup>

  </Project>
  ```
- [ ] **Step 2: Три файла (тела — дословно из канонических копий, свёрстаны в один хелпер-класс на файл):**
  ```csharp
  // src/Shared.Tls/TlsEnv.cs
  using Microsoft.Extensions.Configuration;

  namespace Shared.Tls;

  // Перенос env-секретов → конфиг-дерево (PEM-значения и _PATH-файлы); наборы пар
  // остаются у потребителей (t08: три копии env→config слиты).
  public static class TlsEnv
  {
      public static void ApplyEnvOverrides(
          (string Env, string Key)[] bindings,
          ConfigurationManager configuration,
          Func<string, string?>? getenv = null)
      {
          getenv ??= Environment.GetEnvironmentVariable;
          foreach (var (env, key) in bindings)
          {
              var value = getenv(env);
              if (!string.IsNullOrWhiteSpace(value))
                  configuration[key] = value;
          }
      }
  }
  ```
  ```csharp
  // src/Shared.Tls/TlsChain.cs
  using System.Security.Cryptography.X509Certificates;

  namespace Shared.Tls;

  // Валидация цепочки серта против приватной per-install CA (t08: 4 копии слиты;
  // сигнатура — как DockerTlsMaterial.ValidateChain: принимает X509Certificate?).
  public static class TlsChain
  {
      public static bool ValidateChain(X509Certificate? certificate, X509Certificate2 ca)
      {
          var cert2 = certificate as X509Certificate2
              ?? (certificate is null ? null : new X509Certificate2(certificate));
          if (cert2 is null)
              return false;
          using var chain = new X509Chain();
          // Per-install приватная CA не публикует CRL/OCSP — онлайн-проверка отзыва
          // всегда падала бы и отвергала валидные клиентские серты.
          chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
          chain.ChainPolicy.CustomTrustStore.Add(ca);
          chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
          return chain.Build(cert2);
      }
  }
  ```
  ```csharp
  // src/Shared.Tls/TlsMaterial.cs
  using System.Security.Cryptography.X509Certificates;

  namespace Shared.Tls;

  // Загрузка PEM-материала (t08: копии из ApiTlsEndpoints/TlsEndpoints/WorkerTlsHandler/DockerEngine слиты).
  public static class TlsMaterial
  {
      // PFX round-trip: ключ из CreateFromPem эфемерный (не экспортируемый) —
      // SslStream (macOS) не может его использовать без ре-импорта.
      public static X509Certificate2 LoadPemPair(string certPem, string keyPem)
      {
          var pem = X509Certificate2.CreateFromPem(certPem, keyPem);
          return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
      }

      // CA-сертификат; на macOS — ре-импорт через PFX (паттерн WorkerTlsHandler).
      public static X509Certificate2 LoadPem(string caPem)
      {
          var ca = X509Certificate2.CreateFromPem(caPem);
          return OperatingSystem.IsMacOS()
              ? X509CertificateLoader.LoadPkcs12(ca.Export(X509ContentType.Pkcs12), null)
              : ca;
      }

      // PATH-дуализм: null — файла нет/не задан.
      public static string? ReadPemFile(string? path)
          => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
  }
  ```
- [ ] **Step 3: slnx** — `/common/` + `<Project Path="Shared.Tls/Shared.Tls.csproj" />`.
- [ ] **Step 4: Сборка** — `dotnet build src/Shared.Tls/Shared.Tls.csproj -c Release`: 0 warnings.
- [ ] **Step 5: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): Shared.Tls — TlsEnv/TlsChain/TlsMaterial (слияние 4 копий mTLS-хелперов)"
  ```

**Выход:** общие TLS-хелперы. **Проверка:** Step 4. **Spec:** §4.3.

### Task 11: Перевод 4 точек на Shared.Tls

**Files:**
- Modify: `src/PgWorker.App/Api/ApiTlsEndpoints.cs`, `src/KafkaWorker.App/Api/TlsEndpoints.cs` (+`src/PgWorker.App/PgWorker.App.csproj`, `src/KafkaWorker.App/KafkaWorker.App.csproj` +ProjectReference Shared.Tls)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs` (+`src/AdminPanel.Etcd/AdminPanel.Etcd.csproj` +ProjectReference Shared.Tls)
- Modify: `src/PgWorker.Docker/Engine/DockerEngine.cs` (+`src/PgWorker.Docker/PgWorker.Docker.csproj` +ProjectReference Shared.Tls)

**Interfaces (Consumes):** `Shared.Tls` (Task 10).

**Вход:** Task 10 закоммичен.

- [ ] **Step 1: `ApiTlsEndpoints.cs` (PgWorker.App).** В шапку `using Shared.Tls;`. `ApplyEnvOverrides` становится делегатом (EnvBindings остаются per-app):
  ```csharp
  public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
      => TlsEnv.ApplyEnvOverrides(EnvBindings, configuration, getenv);
  ```
  `ClientCertificateValidation = (certificate, _, _) => TlsChain.ValidateChain(certificate, clientCa)` (было `ValidateChain(certificate, clientCa)`); `LoadCertificatePemPair(...)` → `TlsMaterial.LoadPemPair(...)` (строки ~70, ~138); `LoadClientCa`:
  ```csharp
  private static X509Certificate2? LoadClientCa(TlsOptions tls)
  {
      var caPem = tls.ClientCaPem ?? TlsMaterial.ReadPemFile(tls.ClientCaPath);
      return caPem is null ? null : TlsMaterial.LoadPem(caPem);
  }
  ```
  Тело `LoadClientCa` заменено на делегат (выше), приватные `ValidateChain`, `LoadCertificatePemPair`, `ReadFile` удалить. Kestrel-настройка (`ConfigureMtls`, `ResolvePort`, managed-cert из etcd, `ApiTlsSetup`) — НЕ трогать (spec §4.3).
- [ ] **Step 2: `TlsEndpoints.cs` (KafkaWorker.App)** — те же замены симметрично (строки ~94 `ValidateChain`, ~109 `LoadCertificatePemPair`, ~124 `LoadClientCa`, ~135 `ReadFile`, ~30 `ApplyEnvOverrides`).
- [ ] **Step 3: `WorkerTlsHandler.cs` (AdminPanel.Etcd).** `using Shared.Tls;`; `ApplyEnvOverrides` → делегат в `TlsEnv`; в `Build`: `ReadFile` → `TlsMaterial.ReadPemFile`, pair-загрузку → `TlsMaterial.LoadPemPair`, CA → `TlsMaterial.LoadPem`, вызов `ValidateChain(cert2, ca)` → `TlsChain.ValidateChain(cert2, ca)`; удалить приватные `ValidateChain`/`ReadFile`. Сам `Build` с thumbprint-доверием остаётся панельным (spec §2.3).
- [ ] **Step 4: `DockerEngine.cs` (PgWorker.Docker).** В `DockerTlsMaterial.Load`: `ReadFile` → `TlsMaterial.ReadPemFile`, `CreateFromPem`+PFX → `TlsMaterial.LoadPemPair(certPem, keyPem)`, CA-блок → `TlsMaterial.LoadPem(caPem)`. Публичная обёртка остаётся и делегирует (spec §9.1: допустимая обёртка):
  ```csharp
  public static bool ValidateChain(X509Certificate? certificate, X509Certificate2 ca)
      => TlsChain.ValidateChain(certificate, ca);
  ```
  удалить приватный `ReadFile`.
- [ ] **Step 5: csproj** — `+<ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>` в 4 csproj (App×2, AdminPanel.Etcd, PgWorker.Docker).
- [ ] **Step 6: Гейт фазы D (существующие env-тесты обязаны остаться зелёными — ассерты на `PGW_*`/`KFW_*`/`WORKERS_PANEL_TLS_*` НЕ меняются, spec §8 Фаза D):**
  ```bash
  dotnet build src/PgWorker.slnx -c Release
  dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~ApiTlsEnvBindings|FullyQualifiedName~DockerTlsOptions"
  dotnet test src/tests/KafkaWorker.UnitTests -c Release --filter "FullyQualifiedName~TlsEnvBindings"
  dotnet test src/tests/AdminPanel.UnitTests -c Release --filter "FullyQualifiedName~WorkerTlsHandler"
  dotnet test src/tests/PgWorker.UnitTests -c Release --filter "FullyQualifiedName~Health"
  dotnet test src/tests/KafkaWorker.UnitTests -c Release --filter "FullyQualifiedName~Health"
  ```
- [ ] **Step 7: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): 4 mTLS-точки на Shared.Tls (ApiTlsEndpoints, TlsEndpoints, WorkerTlsHandler, DockerEngine-обёртка); env-тесты зелёные без правок ассертов"
  ```

**Выход:** фаза D завершена (Д8 закрыт). **Проверка:** Step 6. **Spec:** §4.3, §8 Фаза D.

---

## Фаза E — чистка

### Task 12: Удаление AdminPanel.Infrastructure, slnx-чистка, grep-гейт

**Files:**
- Delete: `src/AdminPanel.Infrastructure/` (к этому моменту — csproj без единого .cs-файла: DI/Result/CQRS/Contexts/Traces перенесены в Task 2, ModuleExtensions.cs и HealthChecks-копия удалены в Task 4)
- Modify: `src/PgWorker.slnx`
- Modify: `src/Shared.Metrics/{MetricsOptions,MetricsModuleExtensions}.cs` (комментарии «копия осознанная» — обновить)
- Modify (по находкам grep): перекрёстные ссылки в `docs/`/`arch/`

**Вход:** Task 11 закоммичен.

- [ ] **Step 1: Удаление сборки:**
  ```bash
  git rm -r src/AdminPanel.Infrastructure
  ```
  В `src/PgWorker.slnx` удалить строку `<Project Path="AdminPanel.Infrastructure/AdminPanel.Infrastructure.csproj" />`. Проверить: `git grep -n 'AdminPanel.Infrastructure' -- src | grep -v docs` — пусто.
- [ ] **Step 2: Греп-гейт дублей (критерий приёмки №1, spec §9.1):**
  ```bash
  grep -rn "class EtcdGateway\|class Result\b\|class InjectAsAttribute\|class HealthCheckAbstract\|ValidateChain\|class Tracing\|interface IHandler" src --include='*.cs' \
    | grep -v "^src/Shared\." \
    | grep -v "TlsChain\.ValidateChain(\|public static bool ValidateChain(\|DockerTlsMaterial\.ValidateChain("
  ```
  Ожидание: пусто (допустимые строки: вызовы/определение делегирующей обёртки `DockerTlsMaterial.ValidateChain` в `DockerEngine.cs` и её вызов на строке ~74; локальный `SqlRetryPolicies` в паттерн не входит). Если что-то найдено — забытая копия: вернуть перенос (не оставлять на потом).
- [ ] **Step 3: Ревизия комментариев** — `src/Shared.Metrics/MetricsOptions.cs:2` и `MetricsModuleExtensions.cs:2`: «копия с AdminPanel.Infrastructure осознанная» → «порт с паттерна Puzzle (бывший AdminPanel.Infrastructure, унифицирован t08)».
- [ ] **Step 4: Ревизия доков** — `grep -rn "AdminPanel.Infrastructure\|AdminPanel.Etcd/Client\|PgWorker.Core/DI\|KafkaWorker.Core/DI" docs arch README.md AGENTS.md --exclude-dir=superpowers`: все живые ссылки либо уже обновлены (Task 1), либо обновить сейчас (упоминания «бывший»/«t08» — оставить). Отдельно: `AGENTS.md` корня — фраза про осознанные дубли НЕ трогается (снимается мерж-гейтом, §10).
- [ ] **Step 5: Полная проверка:**
  ```bash
  dotnet build src/PgWorker.slnx -c Release
  dotnet test src/tests/Shared.Core.UnitTests -c Release
  dotnet test src/tests/Shared.Etcd.UnitTests -c Release
  dotnet test src/tests/AdminPanel.UnitTests -c Release
  ```
- [ ] **Step 6: Commit**
  ```bash
  git add -A && git commit -m "refactor(t08): AdminPanel.Infrastructure удалён (csproj-пустышка после фазы B); grep-гейт дублей чист; комментарии/доки выверены (фаза E)"
  ```

**Выход:** в `src/` ровно одна копия каждого типа Д1–Д8; сборки-пустышки нет. **Проверка:** Steps 2, 5. **Spec:** §8 Фаза E, §9.1.

---

## Фаза F — верификация и гейты

### Task 13: Полный build + все юнит-серии (F.1–F.2)

**Вход:** Task 12 закоммичен.

- [ ] **Step 1:**
  ```bash
  DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
  ```
  0 warnings (TreatWarningsAsErrors).
- [ ] **Step 2: Юнит-серии последовательно:**
  ```bash
  dotnet test src/tests/Shared.Core.UnitTests -c Release
  dotnet test src/tests/Shared.Etcd.UnitTests -c Release
  dotnet test src/tests/Shared.Metrics.UnitTests -c Release
  dotnet test src/tests/PgWorker.UnitTests -c Release
  dotnet test src/tests/KafkaWorker.UnitTests -c Release
  dotnet test src/tests/AdminPanel.UnitTests -c Release
  ```
  Все PASS. (Юниты docker не поднимают — зачистка контейнеров не требуется.)
- [ ] **Step 3: Commit** (если были правки) — `git add -A && git commit -m "chore(t08): фаза F.1-2 — полный Release build и юнит-серии зелёные"` (при отсутствии правок коммит не нужен).

**Выход:** F.1–F.2 закрыты. **Проверка:** Steps 1–2. **Spec:** §8 Фаза F.1–2, §9.2–9.3.

### Task 14: Интеграционные серии docker (F.3) — с зачисткой между сериями

**Вход:** Task 13 зелёный. Docker-хост поднят; перед серией: `docker ps` — пусто (или только осознанный стенд), `docker network ls | grep -c 'kfw-net\|pgw-'` → при остатках от прошлых прогонов `docker network prune -f` (страховочный гейт AGENTS.md).

- [ ] **Step 1: PgWorker-интеграции (полные):**
  ```bash
  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release
  ```
  Дождаться финальной строки. Зачистка:
  ```bash
  docker ps -aq --filter "name=pgw-" | xargs -r docker rm -f
  docker network prune -f
  docker ps -a | wc -l   # ожидание 0
  docker network ls | grep -c 'kfw-net\|pgw-' || true   # 0
  ```
- [ ] **Step 2: KafkaWorker-интеграции (полные):**
  ```bash
  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Release
  ```
  + та же зачистка после финальной строки.
- [ ] **Step 3: AdminPanel-интеграции:**
  ```bash
  DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Release
  ```
  + зачистка. PASS — в т.ч. `WorkersApiTests` (панельные мутации сертификатов через новый txn) и `EtcdSnapshotIntegrationTests` (MemberList/Alarm/Status через общий клиент).
- [ ] **Step 4: Правки (если падения)** — по правилам `docs/e2e-launch.md`: разбор логов ДО какого-либо перезапуска; упавший сценарий не перезапускать без анализа и формулировки причин. Коммит правок отдельным шагом.
- [ ] **Step 5: Commit** (при правках): `git commit -m "fix(t08): <причина> — правки по итогам интеграционных серий"`.

**Выход:** F.3 закрыт. **Проверка:** три серии зелёные, зачистка после каждой подтверждена пустым `docker ps -a`/`network ls`. **Spec:** §8 Фаза F.3, §4.5, AGENTS.md (зачистка).

### Task 15: E2E-гейт на свежем Release (F.4)

**Вход:** Task 14 зелёный, зачищен.

- [ ] **Step 1: Кейс-маркер:**
  ```bash
  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
  ```
  E2eFixture сам собирает Release (инкрементальный no-op; `PGW_TEST_E2E_NOBUILD=1` НЕ использовать). Зачистка после финальной строки (Task 14 Step 1).
- [ ] **Step 2: Полный E2E-прогон** (обязателен: тронуты `src/PgWorker.Core`, `PgWorker.Etcd`, `PgWorker.Provisioning/Snapshots` — AGENTS.md; фильтр по namespace серий):
  ```bash
  DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~PgWorker.IntegrationTests.E2e"
  ```
  Перед прогоном сверить состав фильтра: `dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~PgWorker.IntegrationTests.E2e" --list-tests | head -40` — в списке все классы `src/tests/PgWorker.IntegrationTests/E2e/*.cs`. Телеметрия — по `docs/e2e-launch.md` (`[PHASE]`-строки, артефакты `/tmp/pgw-e2e-artifacts-<guid>/`, `MarkFailed()` не удаляет контур). Зачистка после финальной строки.
- [ ] **Step 3: Падения** — разбор по артефактам телеметрии без перезапуска (AGENTS.md, `docs/e2e-launch.md`); ручная зачистка по `README-cleanup.txt` после разбора; повторный прогон — только после анализа и фиксации причин.
- [ ] **Step 4: Commit** (при правках).

**Выход:** F.4 закрыт. **Проверка:** маркер + полный E2E зелёные на свежем Release. **Spec:** §8 Фаза F.4, §9.3; AGENTS.md (E2E-гейт, телеметрия).

### Task 16: Docker-образы и финальный отчёт (F.5)

**Вход:** Task 15 зелёный, зачищен.

- [ ] **Step 1: Образы (локально собираемые, в registry НЕ класть):**
  ```bash
  docker build -f docker/PgWorker.Dockerfile -t pgworker:dev .
  docker build -f docker/KafkaWorker.Dockerfile -t kafkaworker:dev .
  docker build -f docker/AdminPanel.Dockerfile -t adminpanel:dev .
  ```
  (`COPY src/ ./src/` в Dockerfile подтянет новые `/common/`-проекты автоматически; пути publish-команд не менялись — проверить, что publish прошёл, образы создались: `docker images | grep -E 'pgworker|adminpanel|kafkaworker'`.)
- [ ] **Step 2: Compose-конфиги валидны (правок в них НЕ делаем):**
  ```bash
  docker compose -f deploy/docker-compose.yml config >/dev/null && echo OK
  docker compose -f dev-stand/adminpanel/docker-compose.yml config >/dev/null && echo OK
  ```
- [ ] **Step 3: Финальный self-check критериев приёмки (spec §9):** (1) grep-гейт Task 12 Step 2 — чист; (2) build Release зелёный; (3) все серии зелёные (Tasks 13–15); (4) изменения кода вызовов ограничены точками §2.2/§4.2/§4.3 (`git diff main...HEAD --stat` сверить с картой ниже); (5) образы собираются, `deploy/`/`dev-stand/` не менялись (`git diff main...HEAD --name-only -- deploy dev-stand` — пусто); (6) arch/доки фазы A соответствуют коду; (7) мерж-гейт §10 — к исполнению при мерже (ниже).
- [ ] **Step 4: Commit** (при правках). План выполнения завершён — доложить пользователю итог (серия зелёных, диффстат, соответствие критериям).

**Выход:** F.5 закрыт; задача готова к ревью и мерж-гейту. **Проверка:** Steps 1–3. **Spec:** §8 Фаза F.5, §9.1–9.7.

---

## Мерж-гейт (spec §10) — исполняется ТЕМ ЖЕ мерж-коммитом в `main`

По явной просьбе пользователя о мерже (не в worktree, не заранее):

1. Удалить пункт `t08-unify-adminpanel-duplicates` из `arch/roadmap/pgworker.md` (пункт `t09-unify-worker-duplicates` из Task 1 остаётся; `←`-зависимостей у t08 нет — проверено grep по `arch/roadmap/`).
2. Из корневого `AGENTS.md` убрать фразу «Дубли кода с PgWorker (`AdminPanel.Etcd`, `AdminPanel.Infrastructure`) — осознанные, унификация в roadmap (`t08-unify-adminpanel-duplicates`)».
3. История задачи — `docs/superpowers/2026-09-14-t08-unify-adminpanel-duplicates/` (spec+plan уже там).
4. Мерж через ревью (`superpowers:requesting-code-review` / `finishing-a-development-branch` по флоу).

## Карта ожидаемого диффа по коду вызовов (для сверки на гейтах)

| Точка | Файл | Правка |
|---|---|---|
| txn §9.9 (generate) | `src/AdminPanel.Etcd/Workers/WorkerCertService.cs:168` | `TxnAsync(TxnRequest.Of([NotExists],[Put]))` — фабрики spec §4.2 |
| put без lease | `src/AdminPanel.Etcd/Workers/WorkerCertService.cs:197` | `PutAsync(..., lease: null, ct)` |
| revision compaction | `src/PgWorker.Provisioning/Snapshots/SnapshotJob.cs:81` | `status.Value.Revision` |
| revision compaction | `src/KafkaWorker.Etcd/SnapshotJob.cs:81` | `status.Value.Revision` |
| SqlRetry ×5 | `DatabaseProvisioner.cs` (2), `NpgsqlMoveSqlExecutor.cs` (3) | `SqlRetryPolicies.SqlRetry` |
| Status payload | `Shared.Etcd` (новый код) | `EtcdStatusPayload` + `Revision` |
| TLS ×4 | `ApiTlsEndpoints`/`TlsEndpoints`/`WorkerTlsHandler`/`DockerEngine` | вызовы `TlsEnv`/`TlsChain`/`TlsMaterial` |
| Панельная композиция | `AdminPanel.Api/Program.cs:31` | `AddSharedCore()` |
| SnapshotRefresher (точка внимания §2.2) | `src/AdminPanel.Etcd/SnapshotRefresher.cs` | **правки тела не требуются** (сигнатуры совпали, Revision панелью не потребляется) — только using |
| Тестовые фейки | PgWorker.UnitTests (7), KafkaWorker.UnitTests (2), AdminPanel (4) | до union-API |
| Остальное | ~370 файлов | только using/namespace (perl-замены) |
