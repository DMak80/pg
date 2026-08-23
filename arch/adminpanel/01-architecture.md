# 01. Общая архитектура AdminPanel

Read-only панель администрирования шардированных HA-кластеров PostgreSQL
(репозиторий `../pg`). Четыре зоны инспекции: **etcd**, **шардирование**
(кластеры/шарды/бакеты/переезды/heals), **HA** (лидеры/члены/реплики/лаги),
**алерты**. Операций нет: ни одного эндпоинта, мутирующего etcd или PG.

## 1. Слои и потоки данных

```
                 ┌──────────────────────────────────────────────────────┐
                 │ browser: React SPA (Vite+TS, статика из wwwroot)     │
                 │  polling REST API каждые 5 c (2/5/15/off)            │
                 └───────────────▲──────────────────────────────────────┘
                                 │ HTTP JSON, cookie-сессия
                 ┌───────────────┴──────────────────────────────────────┐
                 │ AdminPanel.Api — ASP.NET Core Minimal API host       │
                 │  /api/auth/*, /api/overview|etcd|clusters|ha|alerts  │
                 │  CQRS queries → SnapshotStore (чтение из памяти)     │
                 └───────────────▲──────────────────────────────────────┘
                                 │ immutable EtcdSnapshot (atomic swap)
        ┌────────────────────────┴───────────────┐
        │ SnapshotRefresher (BackgroundService)  │  тик ~3 c
        │  1) endpoint-status по всем endpoints  │
        │  2) range /clusters/ + /service/       │──► etcd (HTTP JSON
        │  3) member/list, alarm                 │    gateway /v3/*)
        │  4) парсеры → EtcdSnapshot             │
        │  5) AlertEngine(Snapshot) → Alert[]    │
        └────────────────────────┬───────────────┘
                                 │ опционально, отдельный тик ~15 c
        ┌────────────────────────┴───────────────┐
        │ Probes (Patroni REST :8008, Npgsql)    │──► PG-ноды и HAProxy
        │  обогащают снапшот полями runtime      │    шардов
        └────────────────────────────────────────┘
```

Правила потоков:

- **API не ходит в etcd на запрос** — только читает текущий снапшот из
  `SnapshotStore` (singleton, атомарная замена ссылки). Скорость UI не зависит
  от латентности etcd, а отказ etcd не роняет панель: снапшок остаётся со
  штампом `lastRefreshUtc` и алертом «данные устарели».
- **Refresher — единственный писатель снапшота**; пробы пишут в него же
  (отдельным тиком, реже). Всё, что видит пользователь, — производные от
  снапшота: DTO для API, алерты, badge «stale».
- **Направление зависимостей**: `Api → (Core, Etcd, Probes, Infrastructure)`;
  `Etcd → Core`; `Probes → Core`; `Core → Infrastructure`. Домен снапшота
  (`Core`) не знает про HTTP и etcd-клиента.

## 2. Проекты решения (`src/AdminPanel.slnx`, формат .slnx)

| Проект | Роль |
|---|---|
| `AdminPanel.Infrastructure` | Каркас, скопированный из референса `../Puzzle` и обрезанный под read-only: `Result`-монада, attribute-DI (`[InjectAs*]`, `[Config]`, `AutoRegistration`), CQRS (`IQuery<T>`/`IQueryHandler`, `IHandler`-диспетчер), health-check базис. Без Bus/Outbox/Kafka/миграций — панели не нужны |
| `AdminPanel.Core` | Домен снапшота: `EtcdSnapshot` и его модели (`ClusterInfo`, `ShardInfo`, `BucketInfo`, `HaScope`, `Alert`, …), `AlertEngine` (чистая функция `Snapshot → Alert[]`), парсинг scope `<C>-<X>` |
| `AdminPanel.Etcd` | Клиент etcd через HTTP JSON gateway (`IEtcdGateway`), парсеры ключей `/clusters/`, `/service/`, `/cluster/nodes/` в модель Core, `SnapshotRefresher`, `SnapshotStore` |
| `AdminPanel.Probes` | Опциональные live-пробы: Patroni REST `:8008` (`/cluster`), SQL через Npgsql (read-only к `pg_catalog`/`pg_stat_*`). Обогащение снапшота полями runtime |
| `AdminPanel.Api` | Host: `Program.cs` (модульная композиция ~50 строк), auth-модуль, REST-эндпоинты, раздача SPA из `wwwroot`, `/api/healthz` |
| `frontend/` | React+Vite+TS (не dotnet-проект); `npm run build` кладёт бандл в `src/AdminPanel.Api/wwwroot` |
| `tests/AdminPanel.UnitTests` | xunit v3 + FluentAssertions: парсеры etcd-ключей, `AlertEngine`, auth-логика, DTO-мапперы |
| `tests/AdminPanel.IntegrationTests` | Testcontainers (etcd + postgres:18) + `WebApplicationFactory`: refresher против реального etcd, API-смоук, пробы |

Общие файлы в `src/` (как в Puzzle): `Directory.Build.props`
(`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`,
`net10.0`), `Directory.Packages.props` (CPM), `NuGet.Config` (package source
mapping), `.editorconfig`. Версии пакетов стартуют от референса (xunit.v3,
FluentAssertions, Testcontainers, Npgsql, Microsoft.Extensions.*); новые
зависимости — только через CPM.

## 3. Модель снапшота (обзор; полный контракт — 02)

`EtcdSnapshot` — immutable record, единый «слепок» всего, что панель знает:

- `EtcdStatus` — endpoints (reachable/latency/version/dbSize/raftTerm),
  members (+leader), alarms, кворум-признак, `lastRefreshUtc`,
  `consecutiveFailures`;
- `Clusters[]` — по каждому кластеру `<C>`: константы (`config`), шарды
  (`dsn`, `replicas`, master-lease), бакеты (`routing` + `status`),
  журнал `heals`;
- `HaScopes[]` — по каждому `/service/<scope>`: leader, members
  (role/state/лаг — из Patroni-пробы, если включена), optime, связь
  scope → (cluster, shard);
- `Probes[]` — результаты live-проб ( Patroni/SQL: ok/error/latency),
  `Runtime`-поля по шардам (слоты, sync-standby, подписки, inventory
  бакетов) — только при включённых пробах;
- `Alerts[]` — вычислено `AlertEngine` на этом тике.

Идентичность алертов (стабильный `id` вида `kind:target`) позволяет
фронтенду показывать «присутствует с …» без хранения истории.

## 4. Аутентификация

- Один администратор: `Username` + `Password`/`PasswordHash` из настроек
  (`[Config]`, `AdminPanel:Auth:*`). БД пользователей отсутствует.
  Пароль: для dev допускается plain `Password`; для прода — PBKDF2-SHA256
  `PasswordHash` (формат `$pbkdf2-sha256$i$salt-b64$hash-b64`), сравнение
  constant-time. Если заданы оба — используется hash.
- ASP.NET Core Cookie Authentication: cookie `adminpanel_session`, HttpOnly,
  SameSite=Lax, sliding-истечение `SessionHours` (по умолчанию 8 ч);
  `Secure=true`, если не `AllowHttp=true` (стенд).
- `POST /api/auth/login` (JSON, rate-limit 5/мин на IP), `POST /api/auth/logout`,
  `GET /api/auth/me`. Всё под `/api/*`, кроме `login` и `healthz`, требует
  cookie → иначе 401. Статика (SPA-бандл) раздаётся без авторизации — в нём
  секретов нет, данные приходят только через API.
- Никаких refresh-token/JWT: одна cookie, один админ.

## 5. Фронтенд

- **Стек**: React + Vite + TypeScript; UI — Mantine (таблицы, бейджи, тёмная
  тема без emotion); данные — TanStack Query; маршрутизация — React Router.
  Сборка `frontend/` → `src/AdminPanel.Api/wwwroot` (vite `outDir`), ASP.NET
  Core раздаёт статику и SPA-fallback на `index.html`.
- **Обновление — только polling**: TanStack Query `refetchInterval` 5 c
  (переключатель в UI: 2/5/15/off, default 5 c; выбор сохраняется в
  localStorage). WebSocket/SSE сознательно нет: данные
  и так производные от тиков refresher'а, а меньше движущихся частей.
- **Dev-режим**: `vite dev` с proxy `/api` → `http://localhost:5000`
  (Kestrel), CORS не нужен.
- **Каркас SPA** (t07): `frontend/src` — общий API-клиент (fetch-обёртка с
  обработкой 401/ProblemDetails + типы DTO из [03](03-panels.md) §2),
  layout с навигацией и страницей Login; остальные панели — заглушки,
  наполняются задачами t08/t09. Guard: layout при монтировании проверяет
  `GET /api/auth/me`; любой 401 от API (кроме запроса самой формы логина)
  → редирект на `/login`.
- **Раздача SPA**: статику и SPA-fallback (`index.html` на неизвестных
  путях) хост отдаёт без авторизации; неизвестные пути `/api/*` при этом —
  404, а не SPA-fallback. Если wwwroot пуст (бандл не собран) — хост
  стартует с предупреждением в лог, `/api/*` работает.
- **Страницы**: Login, Overview (дашборд), etcd, Clusters (список → детали:
  шарды/бакеты/переезды/heals), HA (список scope'ов → детали), Alerts.
  Спецификация панелей — [03-panels.md](03-panels.md).

## 6. Конфигурация (`[Config]`-POCO, `AdminPanel:*`)

| Секция | Ключи (по умолчанию) | Назначение |
|---|---|---|
| `AdminPanel:Etcd` | `Endpoints` (обязательно), `RefreshInterval` (3 c), `RequestTimeout` (2 c) | единственное обязательное подключение к данным |
| `AdminPanel:Probes` | `PatroniEnabled` (true), `SqlEnabled` (true), `Interval` (15 c), `Timeout` (3 c), `Password` (для SQL; DSN берётся из etcd), `HostMap` (пусто; словарь «etcd-адрес ноды `host:port`» → «адрес, достижимый с хоста панели») | live-пробы; отключаются целиком; `HostMap` — override адресов проб для локальных стендов ([02](02-etcd-contract.md) §6, [04](04-local-stand.md) §2.3) |
| `AdminPanel:Auth` | `Username`, `Password`, `PasswordHash`, `SessionHours` (8), `AllowHttp` (false) | аутентификация |
| `AdminPanel:Alerts` | `StaleMoveSeconds` (600), `FrozenSeconds` (60), `ReplicaLagBytes` (16 МБ) | пороги алертов |

Секреты (SQL-пароль, пароль админа) — env-переменными поверх `appsettings.json`
(`AdminPanel__Probes__Password` и т.п.), в git их нет.

## 7. Сборка и запуск

- Backend: `dotnet build src/AdminPanel.slnx`; тесты `dotnet test`
  (нужен Docker — Testcontainers). Приложение: `dotnet run --project
  src/AdminPanel.Api` (отдаёт и SPA из wwwroot, если бандл собран).
- Frontend: `cd frontend && npm ci && npm run build` (prod-бандл в wwwroot)
  или `npm run dev` (проксирует на Kestrel).
- Контейнер (поставка): многостадийный `Dockerfile` в корне репо —
  `node:22-alpine` (сборка SPA, `npm ci && npm run build`) →
  `sdk:10.0` (`dotnet publish -c Release`) → `aspnet:10.0` (runtime);
  один процесс, один порт 8080 (`ASPNETCORE_HTTP_PORTS`, `EXPOSE`),
  не-root пользователь, `HEALTHCHECK` на `GET /api/healthz`; прод-настройки
  — только ENV поверх образа (`AdminPanel__Etcd__Endpoints__0`,
  `AdminPanel__Auth__*`, `AdminPanel__Probes__*`; секретов в образе нет,
  auth fail-closed). Бандл SPA собирается в образе (wwwroot в git
  отсутствует); контекст сборки ограничен `frontend/` + `src/`
  (`.dockerignore`).
- Локальный стенд с данными — [04-local-stand.md](04-local-stand.md):
  `docker compose` поднимает etcd + шардированную PG и сеет контроль-плейн.

## 8. Обработка отказов (сводно)

| Отказ | Поведение панели |
|---|---|
| etcd недоступен (все endpoints) | снапшот не обновляется, `EtcdStatus.reachable=false`, алерт critical «etcd unreachable», UI-бейдж stale с возрастом данных |
| Часть endpoints etcd мертва | endpoint-таблица показывает их красными, KV-чтения идут на живой (sticky + failover) |
| Нет кворума etcd | `/v3/maintenance/status` отвечает, но `header` без raft-данных/ошибки → алерт critical «no quorum» (по статусу leader/raftTerm и ошибкам `errors[]`) |
| Протух master-lease шарда | ключа `/clusters/<C>/shards/X/master` нет при живом `dsn` → алерт critical (P11) |
| Patroni REST недоступен | поля пробы `null`, `Probes[]` фиксирует ошибку, алерт warning «probe failed»; etcd-часть HA остаётся |
| SQL-проба недоступна | аналогично; SQL-поля (слоты/лаги) скрыты в UI с пометкой |

## 9. Что сознательно НЕ делаем (YAGNI)

- Мутации (move/abort/heal, patronictl, switchover) — вне зоны панели
  навсегда; это runbook-операции `../pg`.
- WebSocket/SSE-пуш, история метрик и графики — polling и «текущее состояние»
  достаточно (P21 просит дашборд, не Prometheus).
- Пользователи/роли/аудит — один админ из настроек.
- Собственная БД панели — состояние панели полностью выводимо из etcd за один
  тик.
