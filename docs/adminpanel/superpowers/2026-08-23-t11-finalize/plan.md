# t11-finalize — план реализации (финализация AdminPanel)

> **Для агентских исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development
> (рекомендуется) или superpowers:executing-plans — выполнять по задачам; шаги отмечаются
> чекбоксами (`- [ ]`).

**Цель:** финализировать репозиторий — docs/ в стиле Puzzle, README корня, стабилизация
t90-флака, многостадийный Dockerfile + .dockerignore, зелёный полный прогон, закрытие
roadmap (t11, t90).

**Архитектура:** работа чисто документационно-инфраструктурная: функциональный код
(C#/TS/compose/чеки) не меняется; два точечных исключения оговорены spec §2 — тестовый
код t90-фикса (Задача 3) и случай Dockerfile-блокера (не ожидается). Всё строится поверх
слитых t01–t10.

**Стек:** .NET 10 (`TreatWarningsAsErrors=true`, CPM, .slnx), React+Vite+TS7+Mantine,
docker (dockerfile:1, node:22-alpine, sdk/aspnet 10.0), bash+jq (e2e-чеки).

**Spec:** `docs/superpowers/2026-08-23-t11-finalize/spec.md` — план аргументируется от
spec; исполнители читают оба файла. Arch-правки spec §3 уже в ветке (коммит `33e039e`).

## Глобальные ограничения

- Рабочий каталог всех команд: `/Users/demakaev/ZCodeProject/worktrees/feat-t11-finalize`
  (worktree ветки `feat-t11-finalize`; мерж в `main` — вне этого плана).
- Язык всех документов и комментариев — русский; идентификаторы, команды, пути —
  английские (как в коде).
- Функциональный код не менять; правки только: docs/, README.md, Dockerfile,
  .dockerignore, arch/roadmap/*, тестовый файл t90 (Задача 3).
- `TreatWarningsAsErrors=true` — любые правки должны собираться с 0 warnings.
- Один Task = один коммит вида `t11: <суть>` (кроме Задачи 5 — верификационная, дерева
  не меняет; и Задачи 7 — контрольная).
- Коммит в feature-ветке — свободно (AGENTS.base); без push.

---

### Задача 1: docs/ — индекс + 5 документов подсистем

**Файлы:**
- Create: `docs/README.md`, `docs/01-framework.md`, `docs/02-etcd-snapshot.md`,
  `docs/03-probes-alerts.md`, `docs/04-frontend.md`, `docs/05-dev-stand.md`.

**Интерфейсы:**
- Consumes: факты кода t01–t10 (перечислены в текстах); ссылочная целостность на
  `arch/*.md` (уже существуют).
- Produces: `docs/README.md` — цель ссылки из `arch/README.md` (внесена коммитом
  `33e039e`; после этой задачи становится валидной). Разделы-якоря документов
  используются README корня (Задача 2).

- [ ] **Шаг 1.1. Понимание Входа**

Вход: spec §5 (состав, стиль, объём 80–150 строк/документ); образцы стиля
`/Users/demakaev/ZCodeProject/Puzzle/docs/01-infrastructure.md` (индекс) и
`/Users/demakaev/ZCodeProject/Puzzle/docs/01.01-di.md` (шапка «Назад», «Кратко»,
таблицы, «Чек-лист», «Грабли»). Каждая «грабля» — фактическая, из истории t01–t10.

- [ ] **Шаг 1.2. Действие: создать `docs/README.md`**

```markdown
# docs/ — практические документы подсистем AdminPanel

Здесь — **как устроен код и что сломается при изменении**: чек-листы и грабли из
опыта задач t01–t10. Контракт (что и почему строим) — в [`../arch/`](../arch/README.md):
arch — источник истины, docs — практики; при расхождении правится arch, затем docs.
История задач (spec/plan по каждой) — в [`superpowers/`](superpowers/). Каркас
Infrastructure скопирован из референса `../Puzzle` (его docs — родитель стиля и
механик DI/CQRS/Result).

## Документы

| Документ | Подсистема | Назначение |
|---|---|---|
| [01 — Каркас](01-framework.md) | `AdminPanel.Infrastructure` | attribute-DI, CQRS-queries + `Result`, модульная композиция, health-checks; грабля статического кеша сборок. |
| [02 — etcd-снапшот](02-etcd-snapshot.md) | `AdminPanel.Etcd` | HTTP JSON gateway `/v3/*`, парсеры, `SnapshotRefresher`/`SnapshotStore`; инвариант «API не ходит в etcd на запрос». |
| [03 — Пробы и алерты](03-probes-alerts.md) | `AdminPanel.Probes` + `Core/Alerting` | Patroni/SQL live-пробы, HostMap, `AlertEngine` — 24 правила. |
| [04 — Фронтенд](04-frontend.md) | `frontend/` | Сборка SPA в wwwroot, api-слой, polling, guard; TS7-css и registry-грабли. |
| [05 — Dev-стенд](05-dev-stand.md) | `dev-stand/` | Профили quick/full, сид, patroni-эмуляторы с lease, e2e-чеки и их порядок. |

## Соглашения

- Новый документ подсистемы: `NN-slug.md`, следующий свободный NN; строка в таблицу
  выше; шапка `> Назад: [docs/README.md](README.md)`; финал документа — разделы
  «Чек-лист при изменениях» и «Грабли».
- Грабли пишем только пережитые (ссылка на задачу/коммит); предположения — не грабли.
```

- [ ] **Шаг 1.3. Действие: создать `docs/01-framework.md`**

```markdown
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
```

- [ ] **Шаг 1.4. Действие: создать `docs/02-etcd-snapshot.md`**

```markdown
# 02 — etcd-клиент и снапшот

> Назад: [docs/README.md](README.md) · Подсистема: `src/AdminPanel.Etcd`.
> Контракт ключей/модели: [arch/02](../arch/02-etcd-contract.md) — здесь только
> реализация и её грабли.

Кратко: `SnapshotRefresher` (тик 3 c, `EtcdOptions.RefreshIntervalSeconds`) —
единственный писатель; строит immutable `EtcdSnapshot` и атомарно кладёт в
`SnapshotStore.Current` (volatile-swap). API читает только стор. Живой цикл:
endpoint-status → range `/clusters/` + `/service/` + `/cluster/nodes/` →
member/list + alarm → парсеры → `ProbeEnricher` → `AlertEngine` → `store.Replace`.

## Gateway: HTTP JSON `/v3/*`

`Client/IEtcdGateway.cs` (реализация `EtcdGateway`): `RangeAsync(endpoint, prefix)`,
`StatusAsync`, `MemberListAsync`, `AlarmAsync` — POST JSON, ключи/значения base64.
Sticky+failover: активный endpoint держится, отказ → следующий (тест
`Refresher_Failover_DeadFirstEndpoint`: мёртвый `http://localhost:1` + живой).

Парсеры (`Etcd/Parsing/`): `ClustersParser` (`config`/`shards`/`routing`/`status`/
`heals`), `ServiceParser` (`leader`/`members`/`optime`), `StandNodesParser`
(`/cluster/nodes/`), `DsnParser` (multi-host DSN шарда). Префиксы — константы
`SnapshotRefresher.Prefixes` (`/clusters/`, `/service/`, `/cluster/nodes/`).

## Снапшот и отказы

`EtcdSnapshot` — immutable record: `Etcd`-статус, `Clusters[]`, `HaScopes[]`,
`Probes`, `Alerts`, `StandNodes`, `BuiltAtUtc`. Отказный тик: прежний снапшот
сохраняется (тот же экземпляр), `ConsecutiveFailures` растёт, `Etcd.Reachable=false`;
алерт `etcd-unreachable` — с порога 2 отказов. `EtcdHealthCheck` (readiness-семантика)
не входит в liveness `/api/healthz`.

## Чек-лист «добавить ключ/поле снапшота»

1. Контракт: правка [arch/02](../arch/02-etcd-contract.md) (формат ключа, семантика) —
   первой.
2. Модель: поле в `Core` (`ClusterInfo`/`ShardInfo`/…), immutable.
3. Парсер: чтение ключа/поля (`Parsing/*`, толерантный `JsonValues`).
4. Сид синхронно в 3 местах: `seed.sh` (стенд), `EtcdSeed` (integration),
   `EtcdFixtures/*.json` (unit) — расхождение ломает тесты и e2e.
5. DTO API + фронт (arch/03 §2 → `api/dto.ts`) — по потребности.
6. Тесты: unit-парсер (fixture-JSON), integration-сценарий на живом etcd.

## Грабли

- **API не ходит в etcd на запрос** — только `SnapshotStore.Current`; иначе латентность
  etcd ломает UI, а отказ etcd — панель (инвариант arch/01 §1).
- **Числа int64 из gateway — decimal-строки** (`mod_revision`, `dbSize`, `raftTerm`,
  lease-ID): DTO читаются `System.Text.Json` с `JsonNumberHandling.
  AllowReadingFromString|WriteAsString` (t03 §3.17); lease-ID — десятичная строка
  (урок rolecheck `../pg`).
- **«Тесты недоступности»**: `http://localhost:1` даёт мгновенный connection refused —
  сценарий отказа не флакает по таймауту (t03).
- **Мутации сида в тестах** — только в классе со **своим** контейнером
  (`EtcdRoutingMutationTests`): перевладение routing в общем контейнере меняет
  ACTIVE-раскладку и ломает ожидания инвентаря соседних тестов класса (t90,
  «лишний bucket_0»).
- **Пустые `Endpoints`** — не падение: снапшот пуст, панель и healthz живы (норма
  для старта без ENV).
```

- [ ] **Шаг 1.5. Действие: создать `docs/03-probes-alerts.md`**

```markdown
# 03 — Live-пробы и алерты

> Назад: [docs/README.md](README.md) · Подсистемы: `src/AdminPanel.Probes` +
> `src/AdminPanel.Core/Alerting`. Контракт: [arch/02](../arch/02-etcd-contract.md)
> §4/§6, [arch/03](../arch/03-panels.md) §4–5.

Кратко: `ProbeOrchestrator` (`BackgroundService`, `[InjectAsSingleton(typeof(IHostedService))]`)
раз в `Probes.IntervalSeconds` (15 c) берёт цели из текущего снапшота и гонит пробы
параллельно; результаты — в `IProbeStateStore`; следующий KV-тик refresher'а
обогащает снапшот (`ProbeEnricher`). Обе пробы выключены (`PatroniEnabled`=
`SqlEnabled`=false) — цикл не запускается вовсе.

## Пробы

- **Patroni REST** (`PatroniRestProbe`): `GET http://<host>:8008/cluster` на каждый
  member scope'а; ответ парсит `PatroniClusterParser` (роль/state/timeline/lag).
- **SQL** (`SqlProbe`): Npgsql по DSN шарда из etcd (`host=s1a,s1b port=5432 …`);
  **один коннект на шард**; каталог arch/03 §5 (слоты, sync-standby, подписки,
  инвентарь `bucket_%`); пароль — `Probes.Password` (DSN его не несёт).
- **HostMap** (`HostMapResolver.Resolve(hostMap, host, port)`): override адреса
  «etcd-адрес ноды `host:port`» → «достижимый с панели»; точное совпадение,
  применяется к каждой цели до подключения. В `appsettings*.json` ключ словаря —
  `host__port` (`:` в ключах режут конфиг-провайдеры .NET, урок t10); в памяти/ENV —
  канонический `host:port`, он приоритетен при наличии обоих.

## AlertEngine — 24 правила

`Core/Alerting/Rules/*` — все `[InjectAsSingleton(typeof(IAlertRule))]`, движок
(`AlertEngine`, `[InjectAsSingleton(typeof(IAlertEngine))]`) собирает
`IEnumerable<IAlertRule>` через DI. Id = `kind:target` (стабилен), `SinceUnix`
переносится из предыдущего снапшота («присутствует с…»), сортировка: severity ↓,
затем kind/target (Ordinal). Пороги — `AdminPanel:Alerts` (`AlertsOptions`).

| Группа | Правила (kind) |
|---|---|
| etcd-здоровье (5) | `etcd-unreachable`, `etcd-endpoint-down`, `etcd-no-quorum`, `etcd-alarm`, `snapshot-stale` |
| шардирование/переезды (11) | `cluster-incomplete`, `key-malformed`, `shard-no-master`, `bucket-no-routing`, `bucket-lost`, `bucket-out-of-range`, `move-stale`, `move-frozen-long`, `move-aborting`, `move-flipped-status-stuck`, `inventory-mismatch` |
| HA/слоты (7) | `shard-no-leader`, `ha-member-not-streaming`, `replica-lag-high`, `sync-standby-missing`, `slot-lag-high`, `slot-invalidation-risk`, `slot-wal-lost` |
| пробы (1) | `probe-failed` |

(`inventory-mismatch` сверяет инвентарь SQL-пробы с routing, только ACTIVE-бакеты.)

## Чек-лист «добавить правило/поле пробы»

1. Контракт arch/03 §4 (kind, severity, условие) — первой; порог — в `AlertsOptions`
   + `appsettings.json`.
2. Правило: `Rules/<Kind>Rule.cs : IAlertRule` (+ `[InjectAsSingleton(typeof(IAlertRule))]`);
   `Evaluate(snapshot, ctx)` возвращает алерты со стабильным `kind:target`.
3. Unit-сценарий: fixture-снапшот (`TestSnapshots`) → ожидаемые алерты; при правке
   порога — `AlertTestRules`.
4. Порог/поле наружу: DTO (`AlertsDto`/`HaDto`…), фронт `api/dto.ts` — по arch/03 §2.
5. Живой прогон: интеграционный сценарий или чек стенда (20-alerts) на появление/гашение.

## Грабли

- **`TargetSessionAttributes=read-write`** (Npgsql 10) работает **только на multi-host
  DSN**; read-only-защита — сессионный `SET` **после** выбора мастера (t06): обе
  степени нужны, ни одну не выкидывать.
- **Пробы мимо DSN из etcd не настроить** адрес хоста руками: только HostMap (прод —
  пуст; стенд — `appsettings.Development.json`, порты 5433–5436/8011–8022).
- **Тайминги ожиданий**: тик проб 15 c — e2e-чеки ждут поля проб с запасом (≤40 c),
  алерты — ≤2 KV-тиков; «не дождались за 5 c» — не баг, а недостаток таймаута.
- **`probe-failed` ≠ пустые данные**: отказ пробы оставляет etcd-часть (поля null),
  SQL-поля в UI скрываются с пометкой (arch/01 §8).
- **HostMap в тестах**: интеграционные проверки резолва — на обоих форматах ключа
  (`host:port` и `host__port`, `HostMapResolverTests`).
```

- [ ] **Шаг 1.6. Действие: создать `docs/04-frontend.md`**

```markdown
# 04 — Фронтенд: каркас SPA

> Назад: [docs/README.md](README.md) · Подсистема: `frontend/` (React+Vite+TS7+Mantine,
  вне .slnx). Контракт: [arch/01](../arch/01-architecture.md) §5, [arch/03](../arch/03-panels.md).

Кратко: `npm run build` кладёт бандл в `src/AdminPanel.Api/wwwroot` (vite `outDir`
вне корня проекта — `emptyOutDir: true` обязателен), Kestrel раздаёт его без auth и
делает SPA-fallback; `npm run dev` — vite:5173 с proxy `/api` → `http://localhost:5000`
(cookie same-origin, CORS не нужен). Данные — только polling (TanStack Query,
`refetchInterval` из контекста), WebSocket/SSE нет.

## Слои

- `api/client.ts`: `apiFetch<T>` + `ApiError`; 401 → редирект `/login?from=…`
  (кроме формы логина), 429 — `Retry-After`; ProblemDetails → `title/detail`.
- `api/dto.ts`: типы DTO (camelCase — как JSON API); `api/queries.ts`: queryKeys +
  fetch-функции.
- `polling/PollingContext.tsx`: `'2'|'5'|'15'|'off'`, default `'5'`, localStorage
  `adminpanel.pollingInterval` (невалидное → default); `usePollingIntervalMs()` →
  `number | false`.
- `main.tsx`: `MantineProvider` (dark) → `QueryClientProvider` (retry без 401,
  refetchOnWindowFocus: false) → `PollingProvider` → `RouterProvider`;
  `@mantine/core/styles.css` импортируется первым.
- `layout/AppLayout.tsx`: guard через session-query (`GET /api/auth/me`), AppShell,
  StaleBadge, PollingToggle; страницы `pages/`.

## Чек-лист «добавить страницу/эндпоинт-клиент»

1. DTO: поля в `api/dto.ts` (camelCase, строго по arch/03 §2 — не по C#-типам).
2. Запрос: queryKey + fetch в `api/queries.ts`; polling-интервал — через
   `usePollingIntervalMs()` в `refetchInterval`.
3. Страница: `pages/<Name>Page.tsx`, маршрут в `App.tsx` (+ подсветка навигации по
   префиксу для деталей), LoadState для загрузки/ошибки.
4. `npm run typecheck` зелёный; при изменении бандла для локальной проверки —
   `npm run build` и перезапуск/обновление Kestrel.

## Грабли

- **TS7 и css-импорт**: typescript 7 (tsgo) проверяет side-effect-импорты строже —
  `@mantine/core/styles.css` требует ambient-декларацию `vite-env.d.ts`
  (`declare module '*.css'`; t07, коммит f4edda4). Удалять `vite-env.d.ts` нельзя.
- **`.npmrc` с публичным registry обязателен**: дефолтный registry окружения может
  быть приватным — `npm ci` в Docker/чистом окружении идёт через
  `registry.npmjs.org` из `frontend/.npmrc` (t07, c0c5ac9). Копировать в сборке
  вместе с package*.json.
- **wwwroot не в git** (артефакт vite): «SPA не отдаётся» в свежем клоне — это
  warning в логе хоста и `npm run build`, не поломка API.
- **Корневой `tsconfig.json` — только для IDE** (`files: []` + references); CLI-проверки
  всегда с явным `-p tsconfig.app.json`/`tsconfig.node.json` (скрипты package.json).
- **Node-версия**: engines `>=22.12` (peer vite 8); локально/Docker — node 22.
```

- [ ] **Шаг 1.7. Действие: создать `docs/05-dev-stand.md`**

````markdown
# 05 — Dev-стенд и e2e

> Назад: [docs/README.md](README.md) · Подсистема: `dev-stand/` (docker compose,
> проект `adminpanel-stand`). Канон: [arch/04](../arch/04-local-stand.md);
> быстрый старт — `dev-stand/README.md`.

Кратко: quick-профиль (по умолчанию) — etcd + идемпотентный сид контроль-плейна;
full — + 4 PG-ноды (2 шарда: мастер+реплика) и 4 patroni-эмулятора `hc*`
(master-lease TTL 5 c в etcd). Панель всегда на хосте (`dotnet run`, :5000);
compose-адреса проб маппятся `HostMap` на хост-порты 5433–5436/8011–8022.

## Состав

- `docker-compose.yml`: `etcd` (2379), `seed` (alpine + etcdctl из distroless-образа:
  официальный etcd без shell), `s1a/s1b/s2a/s2b` (5432→5433–5436, физреплики,
  self-healing мастеров), `hc1a…hc2b` (8008→8011–8022, python-эмуляторы Patroni:
  `/cluster`, `/primary`, `/replica`; пишут `master`/`leader`/`optime`/`members`/
  `/cluster/nodes/*` c lease, пока жива PG ноды).
- `seed.sh`: значения = `EtcdSeed` интеграционных тестов (= unit-фикстуры
  `EtcdFixtures/*.json`), времена статусов динамические от `now`.
- `checks/`: `00-up.sh` (full-up + wait-healthy + БД demo + 13 схем + sync-names),
  `10-smoke-api.sh`, `20-alerts.sh`, `30-failover.sh`, `40-live-probes.sh`,
  `90-down.sh [-v]`.

## E2E-прогон (порядок важен)

```bash
# терминал 1: панель
dotnet run --project src/AdminPanel.Api
# терминал 2:
cd dev-stand
checks/90-down.sh -v                      # чистое состояние (обязательно)
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
checks/90-down.sh -v                      # разбор
```

30-й делает failover s1 (мастер s1b, s1a rejoin-ится репликой) — 40-й рассчитан на
эту топологию. Quick-режим: `90-down.sh -v && docker compose up -d` → зелёные
10/20 (quick-ветка); 30/40 требуют full.

## Чек-лист «изменить стенд»

1. Данные сида: `seed.sh` + `EtcdSeed` + `EtcdFixtures/*.json` — синхронно
   (расхождение = зелёный стенд и красные тесты, и наоборот).
2. Новая аномалия для UI/алертов: статус-ключ в сидe + ожидание в 20-alerts;
   динамические времена — от `now`, чтобы аномалии были «живыми».
3. Топология/порты: только через arch/04 §1 (порты зафиксированы контрактом) +
   `HostMap` в `appsettings.Development.json`.
4. Повторный прогон — всегда с `90-down.sh -v`.

## Грабли

- **Повторный прогон без `-v` флакает**: lease-ключи (master/members/nodes) после
  остановки эмуляторов протухают, идемпотентный сид их не восстанавливает; full →
  quick — тоже только с `-v` (t10).
- **SyncRep-ловушка после promote**: коммиты висят без реплики — 30-й чек снимает
  `synchronous_standby_names` сразу после promote и возвращает после rejoin (урок
  `../pg`).
- **Контейнеры `as-*`** (container_name): не конфликтуют со стендом `../pg`, который
  порты на хост не публикует; имена сервисов (`etcd`, `s1a`…) — канон, на них DSN и
  скрипты.
- **Официальный etcd-образ distroless** (нет shell) — seed-образ это `alpine:3.20` +
  скопированный `etcdctl` 3.5.21.
- **Тики против TTL**: панель 3 c / пробы 15 c / lease 5 c — все ожидания чеков
  ретраи с запасом (≤15 c API, ≤40 c пробы); lease-гашение ассертится в etcd до
  проверки панели.
````

- [ ] **Шаг 1.8. Проверка**

Run: `ls docs/*.md && grep -c 'Грабли' docs/0*.md && grep -rn 'TBD\|TODO\|FIXME' docs/ || true`
Expected: 6 файлов README+01..05; «Грабли» в каждом из 01–05; grep по TBD/TODO/FIXME — пусто.
Дополнительно: пройти глазами, что каждый путь к файлу кода в docs существует
(например `ls frontend/vite.config.ts src/AdminPanel.Etcd/Client/IEtcdGateway.cs`).

- [ ] **Шаг 1.9. Коммит**

```bash
git add docs/
git commit -m "t11: docs/ — индекс + 5 документов подсистем (каркас, etcd-снапшот, пробы/алерты, фронт, стенд; чек-листы и грабли t01–t10)"
```

**Выход:** `docs/` с 6 файлами; ссылка из `arch/README.md` валидна.
**Spec:** §5 (все 6 пунктов), §2 («arch — контракт, docs — практики»).

---

### Задача 2: README.md корня

**Файлы:**
- Modify (полная замена содержимого): `README.md`.

**Интерфейсы:**
- Consumes: `docs/` (Задача 1), `arch/`, `dev-stand/README.md`, команды, проверенные
  в t01–t10.
- Produces: точка входа репозитория; используется финальным контролем (Задача 7,
  сверка команд).

- [ ] **Шаг 2.1. Вход**

Текущий README — статус t01-скелета (spec §4: полная замена). Все команды ниже —
действующие (проверены в t01–t10; полный прогон Задачи 5 подтвердит).

- [ ] **Шаг 2.2. Действие: заменить `README.md` содержимым**

````markdown
# AdminPanel

Read-only панель администрирования шардированных HA-кластеров PostgreSQL
(инспектируемая система — репозиторий `../pg`): etcd-контроль-плейн,
кластеры/шарды/бакеты/переезды/heals, HA (Patroni), live-пробы и алерты.
Панель ничего не мутирует: ни одной операции записи в etcd/PG.

Стек: .NET 10 (Minimal API, warnings как ошибки, CPM, `.slnx`) + React/Vite/TS
(Mantine, TanStack Query); снапшот-модель из etcd (тик 3 c), опциональные live-пробы
Patroni REST/SQL (тик 15 c), 24 правила алертов.

## Карта репозитория

| Путь | Что там |
|---|---|
| [`arch/`](arch/README.md) | Контракт (источник истины): [архитектура](arch/01-architecture.md), [etcd-контракт](arch/02-etcd-contract.md), [панели/API](arch/03-panels.md), [dev-стенд](arch/04-local-stand.md), [roadmap](arch/roadmap/README.md) |
| [`docs/`](docs/README.md) | Практические документы подсистем: чек-листы и грабли t01–t10 |
| `src/AdminPanel.Api` | Host: Program.cs (модульная композиция), auth, REST `/api/*`, `/api/healthz`, раздача SPA |
| `src/AdminPanel.Core` | Домен снапшота + `AlertEngine` (24 правила) |
| `src/AdminPanel.Etcd` | etcd-клиент (HTTP JSON gateway), парсеры, `SnapshotRefresher`/`SnapshotStore` |
| `src/AdminPanel.Probes` | Live-пробы Patroni REST/SQL, `HostMapResolver` |
| `src/AdminPanel.Infrastructure` | Каркас из референса `../Puzzle`: attribute-DI, CQRS, `Result`, health-checks |
| `src/tests/` | Unit (xunit v3 + FluentAssertions) + Integration (Testcontainers: etcd, postgres:18) |
| [`frontend/`](frontend/package.json) | SPA (React+Vite+TS+Mantine); сборка в `src/AdminPanel.Api/wwwroot` |
| [`dev-stand/`](dev-stand/README.md) | Docker-стенд quick/full (etcd + шардированная PG + patroni-эмуляторы) и e2e-чеки |
| `Dockerfile`, `.dockerignore` | Многостадийная сборка образа (node → publish → runtime) |
| `docs/superpowers/` | История задач (spec/plan по каждой) |

## Быстрый старт (стенд)

```bash
# терминал 1 — панель (http://localhost:5000, логин admin/admin из appsettings.Development.json)
dotnet run --project src/AdminPanel.Api

# терминал 2 — стенд full (etcd+сид+2 PG-шарда+эмуляторы); quick: docker compose up -d
cd dev-stand && checks/00-up.sh

open http://localhost:5000
```

Без стенда панель тоже стартует (`curl http://localhost:5000/api/healthz` →
`{"status":"ok"}`), но данных нет: единственное подключение к данным — etcd
(`AdminPanel:Etcd:Endpoints`).

## Сборка и тесты

```bash
dotnet build src/AdminPanel.slnx     # 0 warnings (warnings как ошибки)
dotnet test src/AdminPanel.slnx      # нужен Docker: integration — Testcontainers
cd frontend && npm ci && npm run build   # tsc-typecheck + бандл в wwwroot
cd frontend && npm run dev           # либо dev-режим: vite:5173, proxy /api → :5000
```

## Контейнер

```bash
docker build -t adminpanel .

docker run -d --name adminpanel -p 8080:8080 \
  -e AdminPanel__Etcd__Endpoints__0=http://host.docker.internal:2379 \
  -e AdminPanel__Auth__Username=admin \
  -e AdminPanel__Auth__Password=admin \
  -e AdminPanel__Auth__AllowHttp=true \
  adminpanel
# HEALTHCHECK встроен (GET /api/healthz); из контейнера стенд на хосте —
# через host.docker.internal (Linux: --add-host=host.docker.internal:host-gateway).
```

Прод-настройки — только ENV (`AdminPanel__*`; секции arch/01 §6): etcd-endpoints
(обязательно), auth (`PasswordHash` PBKDF2 либо `Password`), probes (отключение,
`HostMap`, SQL-пароль). Секретов в образе и appsettings.json нет: без пароля логин
отключён (fail-closed). Пробы из контейнера против стенда по умолчанию выключайте
(`AdminPanel__Probes__PatroniEnabled=false`, `AdminPanel__Probes__SqlEnabled=false`)
— стендовые адреса из etcd из контейнера не резолвятся.

## E2E-стенда

```bash
cd dev-stand
checks/90-down.sh -v && checks/00-up.sh && checks/10-smoke-api.sh \
  && checks/20-alerts.sh && checks/30-failover.sh && checks/40-live-probes.sh
```

Порядок важен (30-й меняет топологию s1, 40-й на неё рассчитан); повтор — только с
`90-down.sh -v`. Подробности: [`dev-stand/README.md`](dev-stand/README.md).

## Документация и правила

- Контракт и правила ведения: [`arch/README.md`](arch/README.md), [`AGENTS.md`](AGENTS.md).
- Практики подсистем (чек-листы, грабли): [`docs/README.md`](docs/README.md).
````

- [ ] **Шаг 2.3. Проверка**

Run: `grep -c '##' README.md && ls dev-stand/checks/00-up.sh frontend/package.json Dockerfile 2>/dev/null; echo $?`
Expected: непустой README с разделами; `ls` до Dockerfile вернёт ошибку (файл появится
в Задаче 4) — это ожидаемо на данном шаге, раздел «Карта» уже упоминает Dockerfile;
после Задачи 4 повторить `ls Dockerfile .dockerignore` → оба существуют.

- [ ] **Шаг 2.4. Коммит**

```bash
git add README.md
git commit -m "t11: README корня — карта репо, быстрый старт стенда, сборка/тесты, контейнер, ссылки на arch/docs"
```

**Выход:** README корня соответствует состоянию t01–t10 + поставке.
**Spec:** §4 (все 7 подразделов).

---

### Задача 3: t90 — стабилизация флака `Refresher_EnrichesSnapshot_FromProbeState`

**Файлы:**
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs`
  (удалить один тест-метод и добавить новый класс в конец файла).

**Интерфейсы:**
- Consumes: `EtcdContainerFixture` (IClassFixture → свой контейнер на класс),
  `EtcdTestHarness.NewRefresher(store, endpoint)`, `EtcdSeed.PutAsync`.
- Produces: класс `EtcdRoutingMutationTests` (упомянут в docs/02 «Грабли», Задача 1).

**Диагноз (почему order-dependent).** Оба теста — в одном классе
`EtcdSnapshotIntegrationTests`, т.е. на одном `EtcdContainerFixture` и одном сиде:
- `Refresher_SecondTick_PicksUpChanges` **мутирует** общий etcd:
  `routing/bucket_0: s1 → s2` (навсегда, сид пишется один раз при старте фикстуры);
- `Refresher_EnrichesSnapshot_FromProbeState` ожидает, что ACTIVE-routing s1 = чётные
  `bucket_0..14` (инвентарь теста «8/8»): на чистом сиде s1 без статусных 3 и 11 —
  это ровно 8 чётных. После мутации bucket_0 уходит в s2 → инвентарь содержит
  «лишний bucket_0» → `inventory-mismatch` → assert
  `NotContain(a => a.Kind == "inventory-mismatch")` падает. Порядок внутри класса
  xunit не гарантирует — флак только при SecondTick раньше Enriches (полный прогон).

**Решение (минимально, по существу):** мутационный тест — в отдельный класс со своим
контейнером (готовой прецедент в этом же файле: `EtcdFailureTests` изолирует
`StopAsync`). Продуктовый код не трогаем.

- [ ] **Шаг 3.1. Действие: удалить тест из общего класса**

В `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` удалить
целиком метод (строки с `[Fact] public async Task Refresher_SecondTick_PicksUpChanges()`
по закрывающую скобку перед `[Fact] public async Task Refresher_Failover_DeadFirstEndpoint`):

```csharp
[Fact]
public async Task Refresher_SecondTick_PicksUpChanges()
{
    // Arrange
    var store = new SnapshotStore();
    var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
    await refresher.RefreshOnceAsync(CancellationToken.None);

    // Act — перевладение routing bucket_0 шарду s2
    await EtcdSeed.PutAsync(fixture.Endpoint, "/clusters/demo/buckets/routing/bucket_0", "s2", CancellationToken.None);
    await refresher.RefreshOnceAsync(CancellationToken.None);

    // Assert
    store.Current!.Clusters.Single().Buckets.Single(b => b.Id == 0).Owner.Should().Be("s2");
}
```

- [ ] **Шаг 3.2. Действие: добавить класс в конец файла**

```csharp
// Мутационные сценарии — отдельный класс со СВОИМ контейнером (по образцу EtcdFailureTests):
// перевладение routing bucket_0 меняет ACTIVE-раскладку s1 и ломает ожидание инвентаря
// "чётные bucket_0..14" в EnrichesSnapshot-тесте общего класса (t90: order-dependent
// флак "лишний bucket_0" → inventory-mismatch). Мутации сида — только здесь.
public class EtcdRoutingMutationTests(EtcdContainerFixture fixture) : IClassFixture<EtcdContainerFixture>
{
    [Fact]
    public async Task Refresher_SecondTick_PicksUpChanges()
    {
        // Arrange
        var store = new SnapshotStore();
        var refresher = EtcdTestHarness.NewRefresher(store, fixture.Endpoint);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Act — перевладение routing bucket_0 шарду s2
        await EtcdSeed.PutAsync(fixture.Endpoint, "/clusters/demo/buckets/routing/bucket_0", "s2", CancellationToken.None);
        await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert
        store.Current!.Clusters.Single().Buckets.Single(b => b.Id == 0).Owner.Should().Be("s2");
    }
}
```

- [ ] **Шаг 3.3. Проверка: сборка + точечный запуск**

Run: `dotnet build src/AdminPanel.slnx && dotnet test src/tests/AdminPanel.IntegrationTests --filter "FullyQualifiedName~EtcdRoutingMutationTests|FullyQualifiedName~EtcdSnapshotIntegrationTests"`
Expected: build 0 warnings; тесты обоих классов зелёные (свой контейнер на класс).

- [ ] **Шаг 3.4. Проверка: полный integration-прогон ×3 (доказательство стабильности)**

Run (трижды, подряд):
`for i in 1 2 3; do dotnet test src/tests/AdminPanel.IntegrationTests || break; done`
Expected: три зелёных прогона подряд (Docker запущен). Раньше на полном прогоне
падал `Refresher_EnrichesSnapshot_FromProbeState` — теперь порядок нерелевантен
(мутация изолирована контейнером).

- [ ] **Шаг 3.5. Коммит**

```bash
git add src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs
git commit -m "t11: t90 — мутационный etcd-тест в отдельном контейнере (EtcdRoutingMutationTests); флак inventory-mismatch общего класса устранён (полный прогон ×3 зелёный)"
```

**Выход:** order-зависимость устранена; integration стабилен.
**Spec:** §7.4, §8 (закрытие t90), §2 (правка только тестов).

---

### Задача 4: Dockerfile + .dockerignore + smoke контейнера

**Файлы:**
- Create: `Dockerfile`, `.dockerignore`.

**Интерфейсы:**
- Consumes: `frontend/` (npm-скрипты, `.npmrc`, engines node >=22.12; vite outDir
  `../src/AdminPanel.Api/wwwroot`), `src/` (slnx-проекты, `NuGet.Config` публичный),
  quick-стенд (etcd :2379 на хосте), `/api/healthz` (liveness).
- Produces: образ `adminpanel` (порт 8080, `USER app`, HEALTHCHECK); используется
  README (Задача 2, раздел уже написан) и Задачей 7.

Технические решения (проверены): пользователь `app` (uid 1654) существует в
`mcr.microsoft.com/dotnet/aspnet:10.0` (проверено `docker run --rm … id app`);
`curl` в базовом образе отсутствует — ставим одной строкой; бандл frontend-стадии:
`WORKDIR /src` + копия `frontend/` в `/src` → vite outDir
`../src/AdminPanel.Api/wwwroot` резолвится в `/src/AdminPanel.Api/wwwroot`.

- [ ] **Шаг 4.1. Действие: создать `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1

# Стадия 1 — фронт: SPA-бандл (engines node >=22.12; registry — публичный, см. frontend/.npmrc).
FROM node:22-alpine AS frontend
WORKDIR /src
COPY frontend/.npmrc frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
# vite build: outDir ../src/AdminPanel.Api/wwwroot от корня frontend → /src/AdminPanel.Api/wwwroot
RUN npm run build

# Стадия 2 — бэкенд: publish (NuGet.Config/CPM/Build.props — внутри src/, источники публичные).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS publish
WORKDIR /src
COPY src/ ./src/
RUN dotnet publish src/AdminPanel.Api/AdminPanel.Api.csproj -c Release -o /app --nologo

# Стадия 3 — runtime: один процесс, один порт, не-root, HEALTHCHECK на liveness.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
# curl отсутствует в базовом образе — нужен только для HEALTHCHECK.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=publish /app ./
COPY --from=frontend /src/AdminPanel.Api/wwwroot ./wwwroot
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER app
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/api/healthz || exit 1
ENTRYPOINT ["dotnet", "AdminPanel.Api.dll"]
```

- [ ] **Шаг 4.2. Действие: создать `.dockerignore`**

```
# Контекст сборки: только frontend/ и src/ + Dockerfile (spec t11 §6.2).
.git
.dev-flow
.idea
.vs
.vscode
.DS_Store
**/*.md
arch
dev-stand
docs
graphify-out
**/bin
**/obj
**/node_modules
src/AdminPanel.Api/wwwroot
```

- [ ] **Шаг 4.3. Проверка: сборка образа**

Run: `docker build -t adminpanel . 2>&1 | tail -5`
Expected: `Successfully tagged adminpanel:latest` / `naming to docker.io/library/adminpanel`
без предупреждений о больших файлах контекста; в логе видны стадии frontend (npm ci,
vite build) и publish (`Restore`+`Publish` succeeded, 0 warnings — Release-сборка с
`TreatWarningsAsErrors=true`).

- [ ] **Шаг 4.4. Проверка: smoke против quick-станда**

Run:
```bash
cd dev-stand && checks/90-down.sh -v >/dev/null 2>&1; docker compose up -d && cd ..
docker rm -f adminpanel-smoke 2>/dev/null; docker run -d --name adminpanel-smoke -p 18080:8080 \
  --add-host=host.docker.internal:host-gateway \
  -e AdminPanel__Etcd__Endpoints__0=http://host.docker.internal:2379 \
  -e AdminPanel__Auth__Username=admin -e AdminPanel__Auth__Password=admin \
  -e AdminPanel__Auth__AllowHttp=true \
  -e AdminPanel__Probes__PatroniEnabled=false -e AdminPanel__Probes__SqlEnabled=false \
  adminpanel
sleep 3    # старт Kestrel в контейнере до первой curl-проверки
curl -sf http://localhost:18080/api/healthz
curl -sf http://localhost:18080/ | head -c 200
curl -s -c /tmp/t11jar -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}' http://localhost:18080/api/auth/login -o /dev/null -w '%{http_code}\n'
curl -s -b /tmp/t11jar http://localhost:18080/api/overview | head -c 300
# HEALTHCHECK: первый прогон — ~30 с от старта (--start-period не сдвигает первый чек,
# лишь не засчитывает неудачи) → ждём healthy ретраями, а не разовым inspect (иначе `starting`).
for i in $(seq 1 24); do
  [ "$(docker inspect --format '{{.State.Health.Status}}' adminpanel-smoke)" = healthy ] && break
  sleep 5
done
docker inspect --format '{{.State.Health.Status}}' adminpanel-smoke   # ожидание: healthy
```
Expected: healthz → `{"status":"ok"}`; `/` → начало `index.html` (SPA отдаётся —
бандл попал в образ); login → `204`; `/api/overview` → JSON с данными сида (cluster
demo); Health → `healthy` — финальный inspect после ретрай-цикла (≤ ~2 мин; разовый
inspect сразу после `docker run` дал бы ложный `starting` — первый чек только через
~30 с). Интервалы HEALTHCHECK в Dockerfile не менять — контракт spec §6.1/arch §7.
Разбор: `docker rm -f adminpanel-smoke && cd dev-stand && checks/90-down.sh -v`.

- [ ] **Шаг 4.5. Коммит**

```bash
git add Dockerfile .dockerignore
git commit -m "t11: многостадийный Dockerfile + .dockerignore — node(npm ci+build) → sdk(publish) → aspnet runtime; 8080, USER app, HEALTHCHECK /api/healthz; smoke против quick-станда зелёный"
```

**Выход:** образ собирается и здоров, SPA+API+данные сида работают из контейнера.
**Spec:** §6 (6.1–6.2), §7.6; arch/01 §7 (контракт внесён в `33e039e`).

---

### Задача 5: Полный прогон (верификационная, коммита нет)

**Файлы:** — (дерево не меняется; при случайных изменениях — `git status` до начала
и в конце).

**Интерфейсы:** Consumes: всё предыдущее. Produces: зафиксированный зелёный прогон
(основа финального ревью, Задача 7).

- [ ] **Шаг 5.0. Вход**: `git status --short` — пусто (все Task 1–4 закоммичены).

- [ ] **Шаг 5.1. Сборка Debug+Release**

Run: `dotnet build src/AdminPanel.slnx 2>&1 | tail -3 && dotnet build src/AdminPanel.slnx -c Release 2>&1 | tail -3`
Expected: обе — `Предупреждений: 0`, `Ошибок: 0`.

- [ ] **Шаг 5.2. Все тесты ×2**

Run: `for i in 1 2; do dotnet test src/AdminPanel.slnx --nologo || break; done`
Expected: два зелёных прогона (unit + integration, Docker запущен); 0 failed.

- [ ] **Шаг 5.3. Фронт**

Run: `cd frontend && npm ci --silent && npm run build 2>&1 | tail -4 && ls ../src/AdminPanel.Api/wwwroot/index.html && cd ..`
Expected: tsc-проверки без ошибок, vite build OK, `index.html` создан
(каталог в `.gitignore` — `git status` остаётся чистым).

- [ ] **Шаг 5.4. e2e стенда (панель на хосте)**

Run (терминал 1 — панель):
```bash
nohup dotnet run --project src/AdminPanel.Api >/tmp/adminpanel-t11.log 2>&1 &
```
Run (терминал 2):
```bash
cd dev-stand
# [spec §7.3] хостовая раздача SPA: GET / отдаёт index.html из wwwroot (бандл Шага 5.3)
curl -sf http://localhost:5000/ | head -c 200
checks/90-down.sh -v
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
checks/90-down.sh -v
```
Expected: `curl http://localhost:5000/` → начало `index.html` (SPA отдаётся хостом);
каждый чек завершается `OK`/exit 0; в конце — разбор стенда.
Панель остановить: `pkill -f 'AdminPanel.Api' || kill %1`.

- [ ] **Шаг 5.5. Выход**: дерево чистое (`git status --short` — пусто; wwwroot и
логи игнорируются). Все результаты зафиксировать для сводки ревью (Задача 7).

**Spec:** §7.1–7.5 (docker-smoke уже в Задаче 4.4).

---

### Задача 6: roadmap-деливерабл (мерж-гейт)

**Файлы:**
- Delete: `arch/roadmap/infra.md`, `arch/roadmap/ha.md`.
- Modify: `arch/roadmap/README.md` (таблица треков).

**Интерфейсы:**
- Consumes: правила `arch/roadmap/README.md` (пункт слит → удаляется тем же
  мерж-коммитом; прецедент удаления опустевшего трека — `stand.md`, коммит 47ac22b);
  закрытие t90 — Задача 3.
- Produces: roadmap без t11/t90; в треках остаются только etcd/sharding/frontend.

- [ ] **Шаг 6.1. Вход**: Задачи 1–5 выполнены (t11 фактически сделан, t90 закрыт).

- [ ] **Шаг 6.2. Действие**: удалить файлы `arch/roadmap/infra.md` и
`arch/roadmap/ha.md` (в первом останется пустой список после удаления пункта t11,
во втором — после удаления пункта t90; по правилам «только несделанные задачи»
опустевший трек удаляется целиком).

- [ ] **Шаг 6.3. Действие**: в `arch/roadmap/README.md` сократить таблицу треков до:

```markdown
| Файл | Направление |
|---|---|
| [etcd.md](etcd.md) | etcd-клиент, снапшот, инспекция etcd, базовые алерты |
| [sharding.md](sharding.md) | инспекция кластеров/шардов/бакетов/переездов/heals |
| [frontend.md](frontend.md) | React-панели |
```

(строки `infra.md` «каркас решения, аутентификация, сборка/поставка» и `ha.md`
«HA: /service/, Patroni/SQL live-пробы, HA-алерты» удалить; остальные части
`arch/roadmap/README.md` не менять.)

- [ ] **Шаг 6.4. Проверка**

Run: `grep -rn 't11-\|t90-\|infra\.md\|ha\.md' arch/ ; echo "exit=$?"`
Expected: вывод пуст (`exit=1`) — ни пунктов, ни ссылок на удалённые треки.

- [ ] **Шаг 6.5. Коммит**

```bash
git add -A arch/roadmap/
git commit -m "t11: roadmap — t11-finalize и t90-fix-probe-enrich-flaky закрыты; треки infra/ha опустели и удалены (правило README, прецедент stand.md)"
```

**Выход:** roadmap чист; история — в git и docs/superpowers.
**Spec:** §8.

---

### Задача 7: Финальный контроль (без коммита)

**Файлы:** — .

- [ ] **Шаг 7.1. Чистота и история**

Run: `git status --short; git log --oneline -7`
Expected: статус пуст; последние коммиты — Task 1–6 + `33e039e` (spec+arch).

- [ ] **Шаг 7.2. Сверка деливерабл со spec §10**

Чек-лист (по каждому — да/нет, источник):
1. Прогоны §7 зелёные (выводы Задач 4–5).
2. README/docs соответствуют коду: команды запускаются, ссылки ведут, числа верны
   (24 правила алертов; чеки 00/10/20/30/40/90; порты 5000/8080/2379/5433–5436/
   8011–8022; node 22).
   Run: `grep -rn '](' README.md docs/README.md | head -30` — проверить каждую
   относительную ссылку `ls`-ом по целевому файлу.
3. Образ: собирается, `healthy`, отдаёт SPA и API (Задача 4.4).
4. arch-правки §3 и roadmap §8 применены (`git show 33e039e --stat`; Задача 6).
5. Функциональный код не менялся: `git diff --stat 33e039e..HEAD -- src/ frontend/ dev-stand/`
   — единственный изменённый файл вне docs/arch/README:
   `src/tests/AdminPanel.IntegrationTests/EtcdSnapshotIntegrationTests.cs` (t90,
   зафиксированное исключение spec §2).
   Run: `git diff --name-only 33e039e..HEAD | grep -v '^docs/\|^arch/\|^README.md$\|^Dockerfile$\|^.dockerignore$'`
   Expected: только путь t90-теста.

- [ ] **Шаг 7.3. Выход**: ветка готова к ревью по superpowers:requesting-code-review
(фаза 4 dev-flow): diff целиком, внимание ревьюера — на пункты 5 (отсутствие
функциональных правок) и полноту «граблей» в docs. Мерж в `main` — по гейту
пользователя.

---

## Самопроверка плана (выполнена автором)

- **Покрытие spec:** §3 arch — уже в `33e039e`; §4 README — Задача 2; §5 docs —
  Задача 1; §6 Dockerfile/.dockerignore — Задача 4; §7 (7.1–7.3 build/test/фронт —
  Задача 5; 7.4 t90 — Задача 3; 7.5 e2e — Задача 5; 7.6 docker-smoke — Задача 4);
  §8 roadmap — Задача 6; §10 ревью-условия — Задача 7 (5 пунктов). Пропусков нет.
- **Плейсхолдеры:** полные тексты всех деливераблов включены; «TBD/TODO» нет;
  фолбэков не осталось (пользователь `app` и пути бандла проверены заранее).
- **Согласованность имён:** класс `EtcdRoutingMutationTests` одинаков в Задаче 3 и
  docs/02 (Задача 1); HEALTHCHECK/порт 8080 согласованы между Dockerfile, README и
  arch/01 §7; коммиты вида `t11: …`.
