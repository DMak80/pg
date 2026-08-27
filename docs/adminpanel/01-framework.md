# 01 — Каркас: attribute-DI, CQRS, Result

> Назад: [docs/README.md](README.md) · Подсистема: `src/AdminPanel.Infrastructure`
> (скопирован из `../Puzzle`, обрезан до read-only: без Bus/Outbox/миграций).
> Контракт слоёв: [arch/01](../arch/01-architecture.md) §1–2.

Как пользоваться (99% случаев):

1. Сервис помечается `[InjectAsScoped]`/`[InjectAsSingleton]`/`[InjectAsTransient]`,
   конфигурация — `[Config]`; в `IServiceCollection` вручную ничего не добавляется.
2. Модуль проекта (`ModuleExtensions.Add<Module>`) вызывает
   `services.AutoRegistration(Assembly)`; корень композиции — `Program.cs`:
   `UseDiBehaviours(configuration)` → `AddInfrastructure()` → `AddApi()` → `AddCore()`
   → `AddEtcd()` → `AddProbes()`.
3. Query: `IQuery<T>` + `IQueryHandler<TQ,TR>`, вызов через `IHandler.HandleQuery`
   (внутри — scope из root-провайдера и Activity-трассировка).
4. Ошибки — `Result`/`Result<T>` (`Bind`/`Map`/`Match`/`From…`), не исключения.

## Регистрация сервисов: `[InjectAs...]`

`src/AdminPanel.Infrastructure/DI/InjectAs.cs`; поведение —
`AutoRegistrationDiTypeBehaviour`:

| Атрибут | Lifetime |
|---|---|
| `[InjectAsSingleton(params Type[] interfaces)]` | Singleton |
| `[InjectAsScoped(params Type[] interfaces)]` | Scoped |
| `[InjectAsTransient(params Type[] interfaces)]` | Transient |

Регистрируется concrete-тип + каждый интерфейс как forward на concrete (`sp =>
sp.GetService(type)`), т.е. интерфейс и класс разрешаются в один экземпляр. Если
`interfaces` НЕ задан — регистрируются **все** интерфейсы типа; если задан — только
перечисленные (ограничение контактов).

`BackgroundService` — авто-хостинг: класс с `[InjectAsSingleton(typeof(IHostedService))]`
запускается хостом без `AddHostedService` (пример: `ProbeOrchestrator`). Singleton
обязателен.

## Конфигурация: `[Config]`

`[Config]` / `[Config("Section")]` на POCO с parameterless-конструктором (примеры:
`EtcdOptions`, `ProbesOptions`, `AuthOptions`, `AlertsOptions`). Значения биндятся
из `IConfiguration` секцией `AdminPanel:*`; в appsettings/env — `AdminPanel__*`
(env-разделитель `__`, см. [arch/01](../arch/01-architecture.md) §6).

## CQRS (только queries) и Result

- `IQuery<T>` — маркер; `IQueryHandler<in TQ, TR>` — обработчик
  (`Task<Result<TR>> Handle(TQ, ct)`); диспетчер `IHandler` (`[InjectAsTransient]`)
  открывает scope при вызове из корневого провайдера и оборачивает выполнение
  в Activity (`Tracing.ActivityT`, `Tracing.Init` в Program).
- `Result` (`Result.cs`): `IsSuccess`, комбинаторы `Bind/BindAsync/Map/MapAsync/
  Apply/Match`, фабрики `Result.From(action)`/`FromAsync`, `Result<T>.FromValue`;
  implicit-конверсия из `Exception`. Мутаций нет — read-only панель обошлась без
  command-инфраструктуры референса.

## Health-checks

`Program.cs`: `AddCheck("self", …, ["live"])` и `AddCheck<EtcdHealthCheck>("etcd")`;
`/api/healthz` фильтрует по тегу `live` — живость панели не зависит от etcd (arch/03
§1). `EtcdHealthCheck` отражает состояние `SnapshotRefresher` (Unhealthy после
отказных тиков). Базис hosted-сервисов — `HealthChecks/HealthCheckAbstract.cs`
(референс 01.12).

## Чек-лист «добавить сервис/query»

1. Класс + интерфейс; атрибут lifetime (по умолчанию scoped; singleton — stateless/
   кэши/фоновые; transient — лёгкие short-lived).
2. Фоновый сервис: `BackgroundService` + `[InjectAsSingleton(typeof(IHostedService))]`.
3. Настройки: POCO + `[Config]`, секция `AdminPanel:*`, дефолты — в `appsettings.json`.
4. Query: record `IQuery<T>` + handler `IQueryHandler<TQ,TR>`; вызов только через
   `IHandler`; ошибки — `Result`, не throw.
5. Сборка модуля уже покрыта `AutoRegistration(Assembly)` — отдельная регистрация
   не нужна.

## Грабли

- **Статический кеш сборок** (`DI/ServiceCollectionExtensions.cs`: `_assemblies`,
  `_behaviours`): `AutoRegistration` дедуплицирует сборки на процесс — **второй
  DI-хост в том же процессе не получает регистраций** (урок t02 §14 → t03 §15).
  Поэтому: один `WebApplicationFactory` на тестовую сборку (коллекция `api`,
  `AuthWebFactory`), а харнессы без attribute-DI конструируют модули напрямую
  (`EtcdTestHarness` — `new Gateway/new Refresher/Options.Create`).
- **Порядок**: `UseDiBehaviours(…)` строго до первого `AutoRegistration` — поведения
  учитываются только уже добавленные.
- **NU1903 как ошибка**: обновление CPM-пакета может уронить сборку предупреждением
  об уязвимости (прецедент: `Microsoft.AspNetCore.OpenApi` 10.0.9 → 10.0.11, t01);
  после любого обновления `Directory.Packages.props` — полный `dotnet build`.
- **`[InjectAs…]` ищется только на самом классе** (не наследуется) — базовые
  generic-хендлеры помечать в каждой реализации или регистрировать явно.
