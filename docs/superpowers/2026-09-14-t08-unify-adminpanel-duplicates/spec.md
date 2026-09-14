# t08-unify-adminpanel-duplicates — унификация дублей кода AdminPanel с PgWorker (spec)

- **Дата**: 2026-09-14
- **Roadmap**: `arch/roadmap/pgworker.md`, тег `t08-unify-adminpanel-duplicates` (снимается тем же коммитом мержа в `main` — мерж-гейт; см. §10)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/refactor-t08-unify-adminpanel-duplicates`
- **Тип**: структурный рефакторинг (дедупликация кода). **Поведение систем не меняется** — условие roadmap-пункта: «поведение обеих систем не меняется (тесты зелёные)».
- **Шаблон**: `../Puzzle` (каркас `Infrastructure.App`, docs-описания подсистем) — использован как образец организации общих библиотек.

## 1. Цель

Устранить дубли кода, накопившиеся после переноса AdminPanel в монорепо
(2026-08-27) и появления KafkaWorker: три системы (AdminPanel, PgWorker,
KafkaWorker) несут побайтово идентичные (или разошедшиеся косметикой) копии
одних и тех же подсистем. Дубли переводятся на общие сборки `Shared.*` по
действующему паттерну решения (`/common/` + `Shared.Metrics`); копии
удаляются. Ни один внешний контракт (etcd-ключи, REST API, docker-образы,
конфигурация) не меняется.

Состав общих библиотек определён исполнителем по результатам фактического
сравнения кода (поручение пользователя); обоснование границ — §4–§6.

## 2. Исходное состояние (проверено по коду worktree)

### 2.1. Карта дублей

| # | Подсистема | Копии | Степень расхождения |
|---|---|---|---|
| Д1 | **etcd-клиент** (`Client/`: `EtcdGateway`, `IEtcdGateway`, `Kv`, txn-модели, `EtcdHttpException`) | `PgWorker.Etcd/Client`, `KafkaWorker.Etcd/Client`, `AdminPanel.Etcd/Client` | Pg↔Kfw **идентичны** (только namespace; проверено diff). Панельная копия — урезанный API + свои методы (см. §2.2) |
| Д2 | **Puzzle-каркас: attribute-DI** (`DI/`: `InjectAs`, `ConfigAttribute`, `DiTypeBehaviour`, `AutoRegistration*`, `ServiceCollectionExtensions`, `ServiceProviderExtensions`) | `PgWorker.Core/DI`, `KafkaWorker.Core/DI`, `AdminPanel.Infrastructure/DI` | **Идентичны** во всех трёх (diff пустой после замены namespace). В панели есть 8-й файл `UseDiBehavioursExtensions.cs` |
| Д3 | **`Result`-монада** (`Result.cs`) | `PgWorker.Core`, `KafkaWorker.Core`, `AdminPanel.Infrastructure` | **Идентичны** |
| Д4 | **Retry** (`RetryPolicies`, `IRetryConfig`) | `PgWorker.Core/Retry`, `KafkaWorker.Core/Retry` | `IRetryConfig` идентичен; `HttpRetry` идентичен; `SqlRetry` (Npgsql) есть только в PgWorker |
| Д5 | **CQRS-контракты** (`IQuery`, `IQueryHandler`, `ICommand`, `ICommandHandler`, `IHandler`) | `AdminPanel.Infrastructure/CQRS` | одна копия (использует только панель: `AdminPanel.Api/Operations/*`) |
| Д6 | **HealthChecks-базис** (`HealthCheckAbstract`, `IHealthCheckService`) | `AdminPanel.Infrastructure/HealthChecks`, `PgWorker.App/HealthChecks`, `KafkaWorker.App/HealthChecks` | идентичны по логике (воркерские чуть с doc-комментариями) |
| Д7 | **Traces** (`Tracing.cs`), **Contexts** (`ServiceProviderHelper.cs`) | `AdminPanel.Infrastructure` | по одной копии (использует только панель) |
| Д8 | **TLS-группа mTLS-граней** (см. §2.3) | `PgWorker.App/Api/ApiTlsEndpoints.cs`, `KafkaWorker.App/Api/TlsEndpoints.cs`, `AdminPanel.Etcd/Workers/WorkerTlsHandler.cs`, `PgWorker.Docker/Engine/DockerEngine.cs` (внутренний `ValidateChain`) | env→config PEM-биндинги, `ValidateChain` (4 копии), PEM/PATH-загрузка, PFX round-trip — повторяются; Kestrel-настройка граней разошлась сознательно |

Транспортные records панели `EtcdMember`/`EtcdAlarm`/`EtcdAlarmType` сегодня
определены в `AdminPanel.Core/EtcdStatus.cs` (потребители: `EtcdAlarmRule`,
`EtcdStatusQuery`, `SnapshotBuilder`) — при объединении клиента переезжают в
`Shared.Etcd` (§4.2/§4.4).

### 2.2. Расхождение API etcd-клиентов (важно для объединения)

`IEtcdGateway` воркеров (Pg/Kfw, идентичны): `RangeAsync`, `GetAsync`,
`PutAsync(key,value,lease,ct)`, `DeleteAsync`, `TxnAsync(TxnRequest)`
(модели `TxnCompare(Key,Target,Pred,Arg,Num)`, `TxnOp.Put/Delete`,
`TxnRequest`, `TxnResult`), `LeaseGrantAsync`, `LeaseRevokeAsync`,
`LeaseKeepaliveAsync`, `SnapshotSaveAsync`, `StatusAsync → Result<long>`
(revision), `CompactAsync`, `DefragmentAsync`.

`IEtcdGateway` панели: `RangeAsync`, `StatusAsync → Result<EtcdStatusPayload>`
(version/dbSize/leader/raftIndex/raftTerm — **без revision**),
`MemberListAsync`, `AlarmAsync`, `TxnAsync(compares: TxnCompare(Key,Version,ModRevision), puts: KvPut)`,
`PutAsync(key,value,ct)` (без lease), `DeleteAsync`; свой
`EtcdUnreachableException`.

Точки вызова, требующие правок при объединении (полный список по grep):

- Панель: `SnapshotRefresher.cs:96-97,185` (MemberList/Alarm/Status→payload);
  `WorkerCertService.cs:168,197` (TxnAsync со старой моделью, PutAsync без
  lease — протокол §9.9: txn `compare version(ключ)==0` + put).
- Воркеры: `PgWorker.Provisioning/Snapshots/SnapshotJob.cs:81` и
  `KafkaWorker.Etcd/SnapshotJob.cs:81` (`StatusAsync → revision` для
  compaction); остальное (GetAsync/PutAsync/TxnAsync/Lease*) — сигнатуры
  общие, изменений кода вызовов не требуется.

### 2.3. TLS-группа: что именно дублируется

- **env→config перенос PEM-секретов** (`(string Env, string Key)[] EnvBindings`
  + `ApplyEnvOverrides(ConfigurationManager, getenv?)`): три копии с разными
  наборами пар (`PGW_API_TLS_*`, `KFW_API_TLS_*`, `WORKERS_PANEL_TLS_*`).
- **`ValidateChain(X509Certificate?, X509Certificate2 ca)`** — CustomRootTrust
  + NoCheck: 4 копии (`ApiTlsEndpoints:111`, `TlsEndpoints:94`,
  `WorkerTlsHandler:80`, `DockerEngine:173`).
- **Загрузка PEM**: `LoadCertificatePemPair` (CreateFromPem + PFX round-trip
  для macOS SslStream), `LoadClientCa`/`LoadPem` (+macOS ре-импорт),
  `ReadFile` (PATH-дуализм) — в обоих App-гранях и `WorkerTlsHandler`.
- **Не дублируется** (остаётся per-app): настройка Kestrel-грани
  (`ConfigureMtls` + `ResolvePort` у PgWorker развились дальше: managed-cert
  из etcd `/workers/api_tls/*`, `ApiTlsSetup`), клиентский handler панели
  `WorkerTlsHandler.Build` (client-cert + доверие по thumbprint из снапшотов).

### 2.4. Существующая инфраструктура общих сборок

- Solution `src/PgWorker.slnx`, папка `/common/`: `Directory.Build.props`,
  `Directory.Packages.props` (CPM), `Shared.Metrics` — действующий паттерн
  общей сборки (потребители: PgWorker.App, KafkaWorker.App, AdminPanel.Api).
- Тесты общих сборок: `src/tests/Shared.Metrics.UnitTests`.

## 3. Принципы

1. **arch-first**: контракт/структура описываются в `arch/` ДО кода (фаза A,
   §8). Канон панели: `arch/adminpanel/` (вкл. `02-etcd-contract.md`).
2. **Поведение не меняется**: перенос типов в общие сборки без изменения
   логики; контракт etcd (`arch/adminpanel/02`) и REST API — нетронуты.
   Любое обнаруженное расхождение семантики дублей — остановить перенос и
   рассмотреть (не «подтихо выбрать одну версию»).
3. **Паттерн `Shared.*`** в `/common/` слн — как `Shared.Metrics`; общие
   сборки не зависят от доменных (`PgWorker.*`/`KafkaWorker.*`/`AdminPanel.*`).
4. **Puzzle — шаблон каркаса**: состав подсистем каркаса и их границы
   соответствуют `../Puzzle/docs/01-infrastructure.md` (DI 01.01, Contexts
   01.02, CQRS 01.03, Retry 01.06, Traces 01.07, Result 01.08, HealthChecks
   01.12); порт уже выполнен, переносятся существующие копии as-is.
5. **Минимальный diff у потребителей**: замены namespace не порождают сотен
   правок — глобальные using на уровне проекта (решение механики — в плане).
6. **TreatWarningsAsErrors=true**: после каждого переноса — чистка using и
   полный `dotnet build`.
7. **Язык**: комментарии/доки — русский; идентификаторы — английские.

## 4. Решение: состав общих библиотек

Три новые сборки в `/common/` слн. Обоснование выбора «новые Shared.*, а не
канонизация `PgWorker.Core`/`PgWorker.Etcd`» (буквальный вариант roadmap):
панель, ссылаясь на `PgWorker.Core`, транзитивно получила бы `Npgsql` и
доменные модели воркера (Planning/Templates/Writing), а `PgWorker.Etcd` —
Coordination-код координации воркеров; кроме того, половина дублей — копии
KafkaWorker, для которых канонизация на PgWorker-проекты симметрично грязна.
Отдельная общая сборка — нулевая цена (в решении уже есть паттерн) и чистые
зависимости. Roadmap-механика соблюдена: «панель получает ProjectReference
на общие сборки, дубли удаляются».

### 4.1. `Shared.Core` — Puzzle-каркас (объединение Д2–Д7)

| Состав (namespace `Shared.Core.*`) | Источник | Потребители |
|---|---|---|
| `DI/` — 7 идентичных файлов + `UseDiBehavioursExtensions.cs` | `AdminPanel.Infrastructure/DI` (канон: самый полный) | все три системы |
| `Result.cs` | любая копия (идентичны) | все три системы |
| `CQRS/` — 5 файлов | `AdminPanel.Infrastructure/CQRS` | панель |
| `Contexts/ServiceProviderHelper.cs` | `AdminPanel.Infrastructure` | панель |
| `Traces/Tracing.cs` | `AdminPanel.Infrastructure` | панель |
| `HealthChecks/` — `HealthCheckAbstract`, `IHealthCheckService` | `PgWorker.App/HealthChecks` (самый документированный) | все три системы |
| `Retry/` — `IRetryConfig`, `RetryPolicies` (`HttpRetry`, `GeneralRetry`) | `KafkaWorker.Core/Retry` (содержит оба общих метода; `HttpRetry` идентичен во всех копиях) | воркеры |

- **`SqlRetry` (Npgsql) остаётся в `PgWorker.Core/Retry`** (локальный файл):
  `Shared.Core` не должен тащить `Npgsql` (панели не нужен).
- Пакеты: `Microsoft.Extensions.Configuration.Abstractions`,
  `Microsoft.Extensions.DependencyInjection.Abstractions`,
  `Microsoft.Extensions.Options.ConfigurationExtensions`,
  `Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions`, `Polly`
  (все версии уже в CPM — новых записей в `Directory.Packages.props` нет).
- **Семантика статического кеша сборок `AutoRegistration` не меняется**
  (грабля из `docs/adminpanel/01-framework.md`: дедупликация сборок на
  процесс; WAF-коллекции тестов уже построены вокруг этого).

### 4.2. `Shared.Etcd` — etcd-клиент HTTP JSON gateway (Д1)

- Состав: `Client/` — `EtcdGateway`, `IEtcdGateway` (**union-API**, ниже),
  `Kv`, `EtcdHttpException`, `EtcdUnreachableException`, txn-модели
  (`TxnCompare`, `TxnTarget`, `TxnPredicate`, `TxnOp`, `TxnRequest`,
  `TxnResult`), `EtcdStatusPayload`, `EtcdMember`, `EtcdAlarm(+Type)`.
  Records `EtcdMember`/`EtcdAlarm`/`EtcdAlarmType` переезжают из
  `AdminPanel.Core/EtcdStatus.cs` (см. §2.1, §4.4) — HTTP-транспорт домену
  панели при этом остаётся неизвестен (домен берёт только транспортные
  records).
- База — воркерская копия (Pg/Kfw идентичны); панельные методы
  (`MemberListAsync`, `AlarmAsync`, статус-метаданные) добавлены в общий
  интерфейс и реализацию (DTO protojson: `members`/`memberID`/`peerURLs`/
  `clientURLs`/`alarms`/`alarm`).
- **Union-API `IEtcdGateway`** (сигнатуры — как у воркеров, кроме Status):
  - `RangeAsync`, `GetAsync`, `PutAsync(key, value, lease, ct)`,
    `DeleteAsync`, `TxnAsync(TxnRequest)`, `LeaseGrantAsync`,
    `LeaseRevokeAsync`, `LeaseKeepaliveAsync`, `SnapshotSaveAsync`,
    `CompactAsync`, `DefragmentAsync` — без изменений;
  - `StatusAsync → Result<EtcdStatusPayload>` — payload **расширяется полем
    `Revision`** (из `header.revision`, ulong-decimal-строка protojson);
    воркеры берут revision из payload (правка `SnapshotJob` ×2: строка 81);
  - `MemberListAsync → Result<IReadOnlyList<EtcdMember>>`,
    `AlarmAsync → Result<IReadOnlyList<EtcdAlarm>>` — новые для воркеров
    (ими не вызываются, панельные).
- Правки потребителей:
  - панель `WorkerCertService`: `TxnAsync(compares, puts)` → `TxnAsync(new
    TxnRequest(Compare: [TxnCompare(Key, TxnTarget.Version, Equal, Arg, Num)],
    Success: [TxnOp.Put(...)]))`; `PutAsync(..., lease: null, ct)`.
    Семантика §9.9 (создание под `compare version==0`) сохраняется дословно;
  - `EtcdGateway.HttpClientName = "etcd"` и регистрация typed-клиента
    (панельный `ModuleExtensions.AddEtcd`) — без изменений.
- **Синтаксические фабрики txn-моделей** (допустимый эквивалент прямой
  формы): `Shared.Etcd` предоставляет фабрики `TxnCompare.NotExists(key)`
  (compare `version == 0` — создание §9.9), `TxnCompare.ValueEqual(key,
  value)` (compare `value ==`), `TxnCompare.ModRevisionEqual(key, rev)`
  (compare `mod_revision ==`) и `TxnRequest.Of(compares, successOps)`.
  Панельная замена txn-вызовов может использовать фабрики — например,
  `TxnRequest.Of([TxnCompare.NotExists(key)], [TxnOp.Put(key, value, null)])`
  — как эквивалент прямой конструкции `new TxnRequest(Compare: [...],
  Success: [...])` с именованными параметрами: та же семантика protojson
  (§9.9), та же сериализация в `CompareToDto`; прямая форма остаётся
  валидной. Фабрики — сахар над моделями, доступны всем потребителям
  `Shared.Etcd` (воркерская Coordination при желании переходит на них без
  изменения поведения).
- Зависимости: `ProjectReference → Shared.Core`; пакет
  `Microsoft.Extensions.Http`.
- **Coordination НЕ переносится** (`ClaimStore`, `PortAllocLock`,
  `WorkJournal` остаются в `PgWorker.Etcd`/`KafkaWorker.Etcd`): их копии
  параметризованы префиксами ключей и доменными моделями журнала (PgWorker
  несёт `RetrySeries`/`EvacuationJournal`) — это не побайтовый дубль, а
  общая схема; вынос — отдельная задача (§7, В2).

### 4.3. `Shared.Tls` — хелперы mTLS-граней (Д8)

- Состав (namespace `Shared.Tls`):
  - `TlsEnv.ApplyEnvOverrides((Env, Key)[] bindings, ConfigurationManager, getenv?)`
    — перенос env→config (наборы пар остаются у потребителей);
  - `TlsChain.ValidateChain(X509Certificate2?, X509Certificate2)` — единая
    замена 4 копий (CustomRootTrust + NoCheck, приватная CA без CRL);
  - `TlsMaterial.LoadPemPair(certPem, keyPem)` (PFX round-trip),
    `TlsMaterial.LoadPem(pem)` (macOS-ре-импорт), `TlsMaterial.ReadPemFile(path)`.
- Пакет: `Microsoft.Extensions.Configuration` (для `ConfigurationManager`;
  уже в CPM).
- Потребители обновляются на хелперы, сохраняя свою конфигурацию граней:
  `ApiTlsEndpoints` (PgWorker.App), `TlsEndpoints` (KafkaWorker.App),
  `WorkerTlsHandler` (AdminPanel.Etcd; сам `Build` с thumbprint-доверием
    остаётся панельным), `DockerEngine` (PgWorker.Docker; его публичная
  обёртка `ValidateChain` остаётся, делегирует в `TlsChain`).
- Kestrel-настройка (`ConfigureMtls`, `ResolvePort`, managed-cert из etcd) —
  per-app, НЕ переносится (PgWorker-грань эволюционировала отдельно, t03/§9.9).

### 4.4. Целевая структура решений и ссылок

```
/common/   Shared.Metrics | Shared.Core | Shared.Etcd | Shared.Tls
             Shared.Etcd → Shared.Core;  Shared.Tls ( standalone )
/core/     PgWorker.Core     → Shared.Core (теряет DI/, Result.cs, Retry/-дубли; домен остаётся)
/etcd/     PgWorker.Etcd     → PgWorker.Core, Shared.Etcd (теряет Client/)
/kafka/    KafkaWorker.Core  → Shared.Core (аналогично PgWorker.Core)
           KafkaWorker.Etcd  → KafkaWorker.Core, Shared.Etcd (теряет Client/)
/admin/    AdminPanel.Core   → Shared.Core, Shared.Etcd
                              (Shared.Core — вместо AdminPanel.Infrastructure;
                               Shared.Etcd — транспортные records
                               EtcdMember/EtcdAlarm/EtcdAlarmType, переезжающие
                               из AdminPanel.Core/EtcdStatus.cs; HTTP-транспорт
                               домену не известен — только records)
           AdminPanel.Etcd   → AdminPanel.Core, Shared.Etcd, Shared.Tls
                              (теряет Client/; Workers/, Parsing/ остаются)
           AdminPanel.Api    → AdminPanel.{Core,Etcd,Probes}, Shared.Core
           AdminPanel.Probes → как сейчас + транзитивно Shared.*
           AdminPanel.Infrastructure — УДАЛЯЕТСЯ (все файлы переехали)
/tests/    + Shared.Core.UnitTests, Shared.Etcd.UnitTests
           (по образцу Shared.Metrics.UnitTests)
```

- `PgWorker.App/HealthChecks/`, `KafkaWorker.App/HealthChecks/` — удаляются
  (базис из `Shared.Core`); их WAF/loops-код меняет только using.
- Механика миграции namespace (`using PgWorker.Core;` в ~75 файлах,
  `using KafkaWorker.Core;` в ~52, `using AdminPanel.Infrastructure` в ~104):
  глобальные using на уровне csproj (`<Using Include="Shared.Core" />` и
  т.п.), точный перечень — в плане; тестовые проекты — аналогично.

### 4.5. Перенос тестов

- `AdminPanel.UnitTests/{ResultTests,AutoRegistrationTests,CQRSTests}.cs` →
  `tests/Shared.Core.UnitTests` (переезд as-is, namespace-правки).
- `AdminPanel.UnitTests/EtcdGatewayTests.cs` +
  `PgWorker.UnitTests/Etcd/EtcdGatewayTests.cs` → объединяются в
  `tests/Shared.Etcd.UnitTests` (протокольные кейсы: base64, prefix-end,
  txn-protojson, int64-decimal, MemberList/Alarm/StatusFull DTO).
- Интеграционные и контрактовые тесты (`PgWorker.IntegrationTests/Etcd/*`,
  `KafkaWorker.IntegrationTests/Etcd/*`, `AdminPanel.IntegrationTests/*`) —
  остаются на местах, обновляются только namespace/сигнатуры (SnapshotJob
  revision, панельные txn-модели). Правила тестов AGENTS.md (динамические
  порты, полный teardown, зачистка сетей) — без изменений.
- `WorkerTlsHandlerTests`, `ApiTlsEnvBindingsTests`, `DockerTlsOptionsTests`,
  `HealthTests` (обоих воркеров) — остаются по месту.

## 5. Что НЕ входит в скоуп (осознанно)

- **В1. Coordination-дубли воркеров** (`ClaimStore`/`PortAllocLock`/
  `WorkJournal` Pg↔Kfw): расходятся доменно (префиксы ключей, модели
  журнала). Вынос с параметризацией — отдельная задача roadmap (добавить
  тегом `t9X` в фазе A, §8).
- **В2. Прочие Pg↔Kfw-дубли вне панельного контура**: `SnapshotJob`
  (сам перенос файла), `Writing/{PlanPut,ValidationError}`,
  `Planning/{PlacementPlanner,PortAllocator}`, `FakeEtcdGateway`-подобные
  фейки тестов. Причина: не относятся к дублям AdminPanel (тема t08) и
  несут доменные различия; трогать runtime воркеров сверх необходимого —
  риск без выгоды текущей задаче. Исключение — точечная правка вызова
  `StatusAsync` в `SnapshotJob`×2 (фаза C, смена сигнатуры на payload с
  `Revision`): это следствие объединения клиента, не дедупликация файла.
- **В3. Реорганизация `PgWorker.Core`/`KafkaWorker.Core`** (вынос каркаса из
  доменных проектов в отдельные воркерские Infrastructure-сборки) —
  перегенерация структуры сверх нужного; ссылка на `Shared.Core` решает
  дедупликацию.
- **В4. Перенос панельных парсеров/снапшотов** (`AdminPanel.Etcd/Parsing`,
  `Snapshot*`) в общие сборки — потребителей нет кроме панели.
- **В5. Новые NuGet-пакеты** — не добавляются (CPM не меняется).

## 6. Отклонения от буквы roadmap-пункта (обоснование)

Roadmap: «перевод панели на `PgWorker.Etcd` … перевод на `PgWorker.Core`».
Исполнено по существу (панель получает ProjectReference на общие сборки,
дубли удаляются), но каноном назначены новые `Shared.Etcd`/`Shared.Core`, а
не проекты PgWorker:

1. `PgWorker.Core` несёт домен воркера + `Npgsql` — панель получила бы
   лишнюю тяжёлую зависимость; `Shared.Core` зависит только от
   abstractions+Polly.
2. Половина дублей — копии KafkaWorker; канонизация на PgWorker-проекты
   создала бы асимметрию «воркер-канон/воркер-потребитель».
3. Паттерн уже легализован в решении (`Shared.Metrics` в `/common/`) и в
   Puzzle (каркас — отдельная сборка `Infrastructure.App` от домена).

## 7. Риски и меры

| Риск | Мера |
|---|---|
| Кеш сборок `AutoRegistration` (второй DI-хост в процессе не получает регистраций) | Код DI переносится as-is, поведение кеша не меняется; WAF-коллекции тестов (`api`, `AuthWebFactory`) — индикатор регрессии на гейте фазы B |
| Регрессия txn-протокола панели §9.9 (compare version==0 + put) при переходе на `TxnRequest` | Правка только в `WorkerCertService` (2 вызова), кейс покрыт `WorkerCertServiceTests` + `WorkersApiTests` (integration); сигнатуры сравнить до/после; фабрики `NotExists`/`TxnRequest.Of` — эквивалент прямой формы (§4.2), сериализация `CompareToDto` не меняется |
| Регрессия compaction-цикла воркеров (revision из payload) | Правка `SnapshotJob`×2 (строка 81), покрыто интеграционными сериями Etcd + E2E (фаза F) |
| `TreatWarningsAsErrors` роняет сборку на осиротевших using | После каждой фазы — полный `dotnet build src/PgWorker.slnx -c Release` |
| Слияние панельной `EtcdUnreachableException` с воркерскими исключениями изменит обработку ошибок | Типы переносятся as-is в `Shared.Etcd` (оба), catch-сайты не меняются |
| Осколки дублей после переноса (забытая копия) | Критерий приёмки №1 (grep-гейт, §9); фаза чистки E |
| Образы панель/воркеры не собираются после смены ссылок | Фаза F: сборка docker-образов `pgworker:dev` (deploy) и панели (docker/AdminPanel.Dockerfile) |

## 8. Фазы (скелет для плана)

Каждая фаза заканчивается зелёными build+unit своего контура; порядок
обязателен.

- **Фаза A — arch-first (контракт до кода)**. Обновить:
  `arch/adminpanel/01-architecture.md` (§1 направление зависимостей:
  `Core → Shared.Core, Shared.Etcd`; §2 таблица проектов: строка
  `AdminPanel.Infrastructure` заменяется на ссылку на `/common/`, описания
  `Etcd`/`Api` корректируются, транспортные records etcd — общие из
  `Shared.Etcd`); `arch/adminpanel/02-etcd-contract.md` §1 (транспорт —
  общий клиент `Shared.Etcd`; контракт ключей НЕ меняется);
  `docs/adminpanel/01-framework.md` (каркас = `Shared.Core`, грабли
  сохраняются); `arch/18-metrics.md`:41,214 (упоминания
  `AdminPanel.Infrastructure`); в `arch/roadmap/pgworker.md` добавить
  пункт-заглушку `t9X` про Coordination/прочие Pg↔Kfw-дубли (В1/В2).
- **Фаза B — `Shared.Core`**: создать сборку, перенести Д2–Д7 (§4.1),
  переключить все три системы (ссылки + глобальные using), удалить дубли
  (`*/DI`, `Result.cs` ×3, `Retry`-дубли, HealthChecks ×3, панельные
  CQRS/Traces/Contexts переехали), перенести тесты (§4.5). `SqlRetry`
  остаётся в `PgWorker.Core`.
- **Фаза C — `Shared.Etcd`**: создать сборку, union-API (§4.2), перенести
  Client/×3 (включая records `EtcdMember`/`EtcdAlarm`/`EtcdAlarmType` из
  `AdminPanel.Core/EtcdStatus.cs`), правки вызовов (§2.2): панель
  `WorkerCertService` (прямая форма или фабрики §4.2), `SnapshotRefresher`
  (payload + Revision); воркеры `SnapshotJob`×2. Тесты: объединение
  gateway-тестов (§4.5), обновить `FakeEtcdGateway` и интеграционные
  фикстуры (namespace).
- **Фаза D — `Shared.Tls`**: создать сборку (§4.3), перевести 4 точки
  (ApiTlsEndpoints, TlsEndpoints, WorkerTlsHandler, DockerEngine) на
  хелперы. Тесты env-биндингов остаются по месту и обязаны остаться
  зелёными (асерты на `PGW_*`/`KFW_*`/`WORKERS_PANEL_TLS_*` не меняются).
- **Фаза E — чистка**: удалить `AdminPanel.Infrastructure`, обновить
  `PgWorker.slnx` (папка `/common/`, `/admin/`, `/tests/`), финальный grep на
  остатки дублей (§9 п.1), ревизия доков (`docs/adminpanel/INDEX.md` и
  перекрёстные ссылки).
- **Фаза F — верификация и гейты**:
  1. `dotnet build src/PgWorker.slnx -c Release` (0 warnings как errors);
  2. все юнит-серий: `dotnet test` по проектам (в т.ч. новые
     `Shared.Core.UnitTests`, `Shared.Etcd.UnitTests`);
  3. интеграционные серии (docker): PgWorker, KafkaWorker, AdminPanel — с
     правилами зачистки контейнеров/сетей после каждой серии (AGENTS.md);
  4. **E2E-гейт** (тронуты `src/PgWorker.Core`, `PgWorker.Etcd`,
     `PgWorker.Provisioning/Snapshots`): полный docker-E2E прогон PgWorker
     на свежем Release (`PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c
     Release` серии E2eFixture; минимум — маркер
     `--filter FullyQualifiedName~Scale_AddEmptyShard`, полный — обязателен
     из-за правки provisioning-снапшотов); интеграционные KafkaWorker —
     полные;
  5. сборка docker-образов `pgworker:dev` и панели (docker-compose config
     валиден, образы билдятся);
  6. телеметрия E2E — по постоянным правилам (`docs/e2e-launch.md`):
     упавший сценарий не перезапускается без анализа логов.

## 9. Критерии приёмки

1. **Дедупликация**: после мержа в `src/` ровно одна копия каждого типа
   Д1–Д8: grep-гейт — `grep -r "class EtcdGateway\|class Result\b\|class
   InjectAsAttribute\|class HealthCheckAbstract\|ValidateChain\|class
   Tracing\|interface IHandler"` по `src/` находит определения только в
   `Shared.*` (+ допустимые обёртки: `DockerEngine.ValidateChain` —
   делегат в `TlsChain`, локальный `SqlRetry`).
2. `dotnet build src/PgWorker.slnx -c Release` — зелёный (0 warnings).
3. Все тестовые серии зелёные: юниты (все проекты), интеграции
   (PgWorker/KafkaWorker/AdminPanel, docker), E2E-гейт фазы F.4.
4. Поведение не изменилось: etcd-контракты (`arch/adminpanel/02`, arch/14,
   arch/15/16) и REST API панель/воркеры — без изменений; изменения кода
   вызовов ограничены перечисленными точками (§2.2, §4.2, §4.3).
5. Docker-образы `pgworker:dev` и панели собираются; `deploy/` и
   `dev-stand/` конфигурации не менялись (кроме отсутствующих правок путей
   проектов, если сборка образов ссылается на csproj — проверить в фазе F).
6. arch/-доки и доки обновлены (фаза A отражена в коде; §8 Фаза A —
   перечень).
7. Roadmap-гейт исполнен (§10).

## 10. Мерж-гейт (по правилам `arch/roadmap/README.md` и AGENTS.md)

Тем же мерж-коммитом в `main`:

- удалить пункт `t08-unify-adminpanel-duplicates` из
  `arch/roadmap/pgworker.md` (и из `←`-зависимостей, если где-то упомянут);
- убрать из корневого `AGENTS.md` фразу «Дубли кода с PgWorker
  (`AdminPanel.Etcd`, `AdminPanel.Infrastructure`) — осознанные, унификация
  в roadmap (`t08-unify-adminpanel-duplicates`)» — дубли устранены;
- добавить в `arch/roadmap/pgworker.md` пункт `t9X-unify-worker-duplicates`
  (Coordination `ClaimStore`/`PortAllocLock`/`WorkJournal`, `SnapshotJob`,
  `Writing`/`Planning`-дубли Pg↔Kfw — перенос с параметризацией префиксов
  ключей; зависимость `←` не ставится: к моменту добавления t08 уже снят);
- история задачи — `docs/superpowers/2026-09-14-t08-unify-adminpanel-duplicates/`.

## 11. Open questions

Нет: состав общих библиотек делегирован исполнителю («сделай все сам»),
развиток, меняющих архитектуру или объём на порядок, не обнаружено —
архитектурное направление единственное (общие сборки по действующему
паттерну `Shared.Metrics` и Puzzle-шаблону).
