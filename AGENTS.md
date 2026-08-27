# AGENTS.md — PgWorker

**Обязательно прочитать и следовать до понимания что пользователь хочет.** Базовые рабочие правила (вопросы→ждать ответ, git worktree, spec→plan→код, ревью перед `main`, язык) — в [`../AGENTS.base.md`](../AGENTS.base.md).

Здесь — только специфика проекта.

⚠️ **Обязательное правило.** По явной просьбе пользователя работать в `main` — действуем без dev-flow и worktree: никаких `spec.md`/`plan.md` и гейтов, просто делаем в `main` то, что просит пользователь; коммит и пуш — только по его отдельному требованию. Без такой просьбы — обычный dev-flow с worktree.

.NET 10, C# (`LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`**). Централизованное версионирование пакетов через `Directory.Packages.props`.

⚠️ **PgWorker ВСЕГДА запускается в докере** (через `deploy/docker-compose.yml`), никогда как хост-процесс `dotnet run`. Стенд поднимается так: etcd через `dev-stand/compose.yaml`, PgWorker через `deploy/docker-compose.yml` (сборка образа `pgworker:dev` + env-секреты).

**AdminPanel** (панель администрирования шардированных кластеров; перенесена из отдельного репозитория AdminPanel — архив вне монорепо, 2026-08-27, с сохранением истории): код — `src/AdminPanel.*` (solution-папка `/admin/`), канон — `arch/adminpanel/` (вкл. etcd-контракт `02-etcd-contract.md`), практики — `docs/adminpanel/`. Запуск — **хост-процессом** (исключение из docker-правила выше): `cd frontend && npm ci && npm run build` (SPA-бандл → `src/AdminPanel.Api/wwwroot`), затем `dotnet run --project src/AdminPanel.Api` с `AdminPanel__Probes__Password` (порт 5050, cookie-логин). Dev-стенд панели — `dev-stand/adminpanel/` (профили quick/full, e2e-чеки `checks/`). Дубли кода с PgWorker (`AdminPanel.Etcd`, `AdminPanel.Infrastructure`) — осознанные, унификация в roadmap (`t08-unify-adminpanel-duplicates`).

**Нужно использовать** подходы к реализации из проекта `../Puzzle` у него есть папка с описанием `docs` откуда можно брать описание. Можно копировать код из этого проекта.

**Roadmap (отложенные/планируемые задачи) ведётся в [`arch/roadmap/`](arch/roadmap/README.md)** — canonical source. В этом файле оставлен **только указатель** (ниже); новые отложенные задачи дописывать в `arch/roadmap/`, а не сюда.

⚠️ **Roadmap — только несделанные задачи.** Задача слита в `main` → её тег удаляется из `arch/roadmap/*.md` **тем же коммитом** (мерж-гейт): из списков и из `←`-зависимостей других пунктов. Сам, без команды и без вопросов. Никаких пометок «закрыта», «реализована», «Волна N закрыта» — нигде; история — в git и `docs/superpowers/`. Правила ведения — в [`arch/roadmap/README.md`](arch/roadmap/README.md).

## Roadmap

Backlog живёт в [`arch/roadmap/`](arch/roadmap/README.md). Обозначения (теги `tNN-slug`, `←`-зависимости) и правило ведения — в [`arch/roadmap/README.md`](arch/roadmap/README.md). Треки по направлениям: **infra** (каркас, auth, поставка), **etcd** (клиент, снапшот, инспекция etcd), **sharding** (кластеры/шарды/бакеты/переезды), **ha** (Patroni/live-пробы, HA-алерты), **frontend** (React-панели), **stand** (dev-стенд, e2e). Каждый пункт — отдельный spec→plan→код (dev-flow); порядок номеров тегов = порядок исполнения.
