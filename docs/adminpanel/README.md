# AdminPanel

Read-only панель администрирования шардированных HA-кластеров PostgreSQL
(инспектируемая система — репозиторий pg (этот монорепозиторий)): etcd-контроль-плейн,
кластеры/шарды/бакеты/переезды/heals, HA (Patroni), live-пробы и алерты.
Панель read-only по отношению ко всему, кроме единственной мутации —
создание кластера `POST /api/clusters` (запись структуры в etcd,
arch/02 §9); все прочие операции записи в etcd/PG по-прежнему запрещены.

Стек: .NET 10 (Minimal API, warnings как ошибки, CPM, `.slnx`) + React/Vite/TS
(Mantine, TanStack Query); снапшот-модель из etcd (тик 3 c), опциональные live-пробы
Patroni REST/SQL (тик 15 c), 25 правил алертов.

## Карта репозитория

| Путь | Что там |
|---|---|
| [`arch/`](../../arch/adminpanel/README.md) | Контракт (источник истины): [архитектура](../../arch/adminpanel/01-architecture.md), [etcd-контракт](../../arch/adminpanel/02-etcd-contract.md), [панели/API](../../arch/adminpanel/03-panels.md), [dev-стенд](../../arch/adminpanel/04-local-stand.md), [roadmap](../../arch/adminpanel/roadmap/README.md) |
| [`docs/`](INDEX.md) | Практические документы подсистем: чек-листы и грабли t01–t10 |
| `src/AdminPanel.Api` | Host: Program.cs (модульная композиция), auth, REST `/api/*`, `/api/healthz`, раздача SPA |
| `src/AdminPanel.Core` | Домен снапшота + `AlertEngine` (25 правил) |
| `src/AdminPanel.Etcd` | etcd-клиент (HTTP JSON gateway), парсеры, `SnapshotRefresher`/`SnapshotStore` |
| `src/AdminPanel.Probes` | Live-пробы Patroni REST/SQL, `HostMapResolver` |
| `src/AdminPanel.Infrastructure` | Каркас из референса `Puzzle`: attribute-DI, CQRS, `Result`, health-checks |
| `src/tests/` | Unit (xunit v3 + FluentAssertions) + Integration (Testcontainers: etcd, postgres:18) |
| [`frontend/`](frontend/package.json) | SPA (React+Vite+TS+Mantine); сборка в `src/AdminPanel.Api/wwwroot` |
| [`dev-stand/adminpanel/`](../../dev-stand/adminpanel/README.md) | Docker-стенд quick/full (etcd + шардированная PG + patroni-эмуляторы) и e2e-чеки |
| `Dockerfile`, `.dockerignore` | Многостадийная сборка образа (node → publish → runtime) |
| `docs/adminpanel/superpowers/` | История задач (spec/plan по каждой) |

## Быстрый старт (стенд)

```bash
# терминал 1 — панель (http://localhost:5050, логин admin/admin из appsettings.Development.json)
dotnet run --project src/AdminPanel.Api

# терминал 2 — стенд full (etcd+сид+2 PG-шарда+эмуляторы); quick: docker compose up -d
cd dev-stand/adminpanel && checks/00-up.sh

open http://localhost:5050
```

Без стенда панель тоже стартует (`curl http://localhost:5050/api/healthz` →
`{"status":"ok"}`), но данных нет: единственное подключение к данным — etcd
(`AdminPanel:Etcd:Endpoints`).

## Сборка и тесты

```bash
dotnet build src/PgWorker.slnx     # 0 warnings (warnings как ошибки)
dotnet test src/PgWorker.slnx      # нужен Docker: integration — Testcontainers
cd frontend && npm ci && npm run build   # tsc-typecheck + бандл в wwwroot
cd frontend && npm run dev           # либо dev-режим: vite:5173, proxy /api → :5050
```

## Контейнер

```bash
docker build -f docker/AdminPanel.Dockerfile -t adminpanel .

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
cd dev-stand/adminpanel
checks/90-down.sh -v && checks/00-up.sh && checks/10-smoke-api.sh \
  && checks/15-cluster-create.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
```

Порядок важен (30-й меняет топологию s1, 40-й на неё рассчитан); повтор — только с
`90-down.sh -v`. Подробности: [`dev-stand/README.md`](../../dev-stand/adminpanel/README.md).

## Документация и правила

- Контракт и правила ведения: [`arch/README.md`](../../arch/adminpanel/README.md), [`AGENTS.md`](../../AGENTS.md).
- Практики подсистем (чек-листы, грабли): [`INDEX.md`](INDEX.md).
