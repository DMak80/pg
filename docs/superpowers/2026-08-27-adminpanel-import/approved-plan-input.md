# Одобренный план (вход для спецификации)

Утверждён пользователем в plan-mode 2026-08-27. Все развилки уже закрыты решениями пользователя — переоткрывать их не нужно.

## Зафиксированные решения
- **История**: сохранить 190 коммитов через `git filter-repo` (у AdminPanel нет remote — иначе история теряется при удалении папки).
- **Структура**: влить в `src/` pg — один `PgWorker.slnx` (13 проектов), общий `Directory.Build.props`/`Directory.Packages.props`/`NuGet.Config`.
- **Дубли кода** (etcd-клиент, Puzzle-каркас): не трогаем, унификация — новая задача в `arch/roadmap/`.
- **Доки**: `arch/` AdminPanel → `pg/arch/adminpanel/`, `docs/`+README → `pg/docs/adminpanel/`; старую папку `../AdminPanel` оставляем как архив (не удаляем, не трогаем).

## Маппинг путей (основа и для filter-repo, и для проверок)
| AdminPanel | → pg |
|---|---|
| `src/*` (7 csproj) | `src/*` как есть |
| `frontend/` | `frontend/` |
| `arch/` | `arch/adminpanel/` |
| `docs/`, `README.md` | `docs/adminpanel/` |
| `Dockerfile` | `docker/AdminPanel.Dockerfile` |
| `.dev-flow/`, `src/AdminPanel.slnx`, `src/Directory.*.props`, `src/NuGet.Config`, `.gitignore`, `AGENTS.md`, `dev-stand/` | **не переносятся** |

Пересечений путей с существующим деревом pg нет — merge пройдёт без файловых конфликтов; общие инфраструктурные файлы администрируются pg-версией.

## Шаги

### 1. Подготовка (dev-flow: worktree + feature-ветка)
- Убедиться в чистоте обоих репо; зафиксировать HEAD pg.
- Установить `git-filter-repo` (brew), если нет.
- Создать worktree pg с веткой `feature/adminpanel-import`.

### 2. Перенос истории
- `git clone --no-local ../AdminPanel /tmp/adminpanel-import` (filter-repo требует свежий клон).
- Применить filter-repo: path-renames из таблицы + исключение непереносимых путей. Побочный эффект: коммиты, менявшие только исключённые файлы, станут пустыми и будут отброшены — история станет чуть короче 190, это ожидаемо.
- Проверить результат: `git log --stat`, выборочная сверка деревьев последних коммитов.

### 3. Вливание
- `git remote add adminpanel-import /tmp/adminpanel-import && git fetch`.
- `git merge --allow-unrelated-histories` (в feature-ветке; конфликтов быть не должно по построению).

### 4. Инфраструктура (отдельные коммиты)
- `Directory.Packages.props`: добавить пакеты AdminPanel (`Microsoft.AspNetCore.Mvc.Testing` 10.0.9, `Microsoft.AspNetCore.OpenApi` 10.0.11, `xunit.runner.visualstudio` 3.1.5; сверить все остальные версии — расхождения решить в пользу актуальных).
- `.gitignore`: перенести правила wwwroot/node_modules из `.gitignore` AdminPanel.
- `PgWorker.slnx`: `dotnet sln add` 7 проектов (solution-папки `/admin/` + тесты в `/tests/`).
- Ссылки в доках pg: `../AdminPanel/arch/…` → `arch/adminpanel/…` (grep по репо).
- `AGENTS.md` pg: секция про AdminPanel (сборка фронта, запуск: `npm run build` + `dotnet run` c `AdminPanel__Probes__Password`, порт 5050, cookie-логин).
- `arch/roadmap/`: новая задача унификации дублей (etcd-клиент `AdminPanel.Etcd` → `PgWorker.Etcd`, Puzzle-каркас `Infrastructure` → `Core`), номер и формат — по правилам `arch/roadmap/README.md`.
- Сверить `dev-stand/` AdminPanel с `pg/dev-stand`: уникального нет — ничего не делать; есть — вынести на обсуждение.

### 5. Верификация
- `dotnet build` всего решения (warnings-as-errors — оба проекта уже под этим режимом).
- `dotnet test` PgWorker.UnitTests (357) + AdminPanel.UnitTests.
- `npm ci && npm run build` в `frontend/` (typecheck через tsc).
- Интеграционные тесты (Testcontainers) и запуск панели на стенде — **только с отдельного согласия**.

### 6. Ревью и мерж
- Diff на ревью перед `main`; мерж в `main` и пуш — только по явной просьбе.

## Что НЕ делаем
- Старую папку `../AdminPanel` не удаляем и не меняем.
- `deploy/docker-compose.yml` не трогаем (панель остаётся хост-процессом; её Dockerfile просто переезжает).
- Код обоих проектов не рефакторим — перенос строго механический.

## Риски
- filter-repo перепишет хэши коммитов AdminPanel — ссылки на `ae25346` в сообщениях коммитов pg останутся просто текстом (не резолвятся). Неизбежно и безопасно.
- Временная копия работает в `/tmp` — оригинал не под угрозой; `main` pg не переписывается (обычный merge-коммит).
