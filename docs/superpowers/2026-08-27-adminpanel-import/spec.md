# Спека: перенос AdminPanel в монорепозиторий pg

- Дата: 2026-08-27
- Статус: утверждена пользователем (user-review пройден; решение о README/INDEX внесено 2026-08-27, §1.3.7)
- Ветка/worktree: `feat-adminpanel-import` (`/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-import`)
- Вход: `approved-plan-input.md` (решения утверждены в plan-mode), `research-notes.md` (исследование), уточнения пользователя от 2026-08-27 (см. §1.3)

## 1. Контекст и цель

### 1.1. Контекст

`AdminPanel` — отдельный git-репозиторий (`/Users/demakaev/ZCodeProject/AdminPanel`) без remote: 190 коммитов, HEAD `ae25346` (2026-08-26), рабочее дерево чистое. Панель — React 19 + Vite 8 фронтенд и ASP.NET-бэкенд (7 csproj), владелец канонического etcd-контракта (`arch/02-etcd-contract.md`), с которым работает PgWorker. Зависимостей от pg в коде нет (только через etcd-контракт), namespace-ы `AdminPanel.*` / `PgWorker.*` не пересекаются.

pg — монорепозиторий PgWorker: `src/PgWorker.slnx` (XML), общий `src/Directory.Build.props` (net10.0, `TreatWarningsAsErrors=true`), CPM (`src/Directory.Packages.props`), `src/NuGet.Config`. Пересечений переносимых путей с текущим деревом pg нет — merge проходит без файловых конфликтов.

### 1.2. Цель

Перенести AdminPanel в монорепозиторий pg **с полным сохранением git-истории** (через `git filter-repo`), влить проекты в `src/` под единый solution/CPM, собрать документацию в `arch/adminpanel/` и `docs/adminpanel/`. После переноса вся разработка панели ведётся в pg; старая папка `../AdminPanel` остаётся нетронутым архивом (репозиторий без remote — иначе история была бы потеряна).

Задача **не меняет поведенческий контракт системы**: контракты данных/etcd не трогаются, код не рефакторится. `arch/`-слой затрагивается только переносом документов (`arch/adminpanel/`) и добавлением roadmap-задачи унификации дублей.

### 1.3. Решения (все закрыты, не переоткрывать)

Утверждены пользователем в plan-mode (см. `approved-plan-input.md`):

1. **История**: сохранить через `git filter-repo` (у AdminPanel нет remote).
2. **Структура**: влить в `src/` pg — один `PgWorker.slnx`, общий `Directory.Build.props`/`Directory.Packages.props`/`NuGet.Config`.
3. **Дубли кода** (etcd-клиент, Puzzle-каркас): не трогать, унификация — новая задача в `arch/roadmap/`.
4. **Доки**: `arch/` → `pg/arch/adminpanel/`, `docs/` + `README.md` → `pg/docs/adminpanel/`; старая папка `../AdminPanel` остаётся архивом.

Дополнительно закрыты вопросами от 2026-08-27:

5. **`dev-stand/` AdminPanel** — уникальный контент (compose с профилями quick/full: PG-шарды s1/s2, Patroni-эмуляторы hc1/hc2; checks/ e2e-скрипты; sidecar-эмулятор). Решение: **переносится** в `dev-stand/adminpanel/` с историей — dev-цикл панели (live-пробы, failover, e2e) остаётся воспроизводимым из монорепо; стенды сосуществуют по задумке (префиксы `as-` vs `pgw-`).
6. **`.dockerignore`** AdminPanel: **не переносится** (панель — хост-процесс, образ не собирается; Dockerfile переносится как артефакт поставки; dockerignore добавим при реальной сборке образа).
7. **Коллизия README** (обнаружена при планировании): в AdminPanel есть и корневой `README.md`, и `docs/README.md`; маппинг «docs/ → docs/adminpanel/» отображает оба в `docs/adminpanel/README.md`. Решение: **`docs/README.md` переносится как `docs/adminpanel/INDEX.md`**, корневой `README.md` — как `docs/adminpanel/README.md`; внутренние ссылки «Назад: docs/README.md» тематических доков 01–05 переправляются на `INDEX.md`.

Фактическая сверка 2026-08-27 (поправка к плану): `xunit.runner.visualstudio 3.1.5` **уже есть** в `src/Directory.Packages.props` pg — реально добавить только 2 пакета (см. §4.3).

## 2. Принципы

1. **Механический перенос.** Код, контент и конфиги панели переносятся как есть. Никакого рефакторинга, переименований, «улучшений». Правки после merge — только инфраструктура (solution, CPM, .gitignore) и ссылки/пути в документации.
2. **История — часть артефакта.** filter-repo переписывает пути и отбрасывает ставшие пустыми коммиты (менявшие только непереносимые файлы) — итоговое число коммитов меньше 190, это ожидаемо и корректно. Хэши перепишутся: старые ссылки (например `ae25346`) в сообщениях pg останутся просто текстом — неизбежно и безопасно.
3. **Инфраструктуру администрирует pg.** Общие файлы (`Directory.Build.props`, `Directory.Packages.props`, `NuGet.Config`, `.gitignore`, solution) существуют в единственной pg-версии; AdminPanel-копии не переносятся. Настройки Build.props идентичны по факту (research-notes §pg/AdminPanel) — сборка под общим props поведенчески не меняется.
4. **Две модели запуска сохраняются.** PgWorker — всегда docker (`deploy/docker-compose.yml`, не трогаем). AdminPanel — хост-процесс (`npm run build` + `dotnet run --project src/AdminPanel.Api` c `AdminPanel__Probes__Password`, порт 5050, cookie-логин). Перенос Dockerfile панели в `docker/` — сохранение артефакта, не подключение к compose.
5. **Канон etcd-контракта не меняется.** `arch/adminpanel/02-etcd-contract.md` переезжает байт-в-байт; в `arch/14-pgworker.md` правится только указание источника канона (было «репозиторий AdminPanel» — станет путь внутри pg). Сами контракты данных/etcd — без изменений.

## 3. Целевая структура

### 3.1. Маппинг путей (основа для filter-repo и проверок)

| AdminPanel | → pg | Комментарий |
|---|---|---|
| `src/AdminPanel.Api/`, `src/AdminPanel.Core/`, `src/AdminPanel.Etcd/`, `src/AdminPanel.Probes/`, `src/AdminPanel.Infrastructure/` | те же пути | как есть |
| `src/tests/AdminPanel.UnitTests/`, `src/tests/AdminPanel.IntegrationTests/` | те же пути | как есть |
| `frontend/` | `frontend/` | включая `.npmrc`, `package-lock.json`; `node_modules` не в git |
| `arch/` | `arch/adminpanel/` | включая канон `02-etcd-contract.md` |
| `docs/` | `docs/adminpanel/` | **кроме** `docs/README.md` → `docs/adminpanel/INDEX.md` (§1.3.7) |
| `README.md` | `docs/adminpanel/README.md` | главный README панели |
| `Dockerfile` | `docker/AdminPanel.Dockerfile` | артефакт поставки, не подключается к compose |
| `dev-stand/` | `dev-stand/adminpanel/` | решение пользователя 2026-08-27 |
| `.dev-flow/`, `.gitignore`, `.dockerignore`, `AGENTS.md` | **не переносятся** | инфраструктура pg / нерелевантно после переезда |
| `src/AdminPanel.slnx`, `src/Directory.Build.props`, `src/Directory.Packages.props`, `src/NuGet.Config`, `src/.editorconfig` | **не переносятся** | общие файлы администрируются pg-версиями (для `.editorconfig` — при необходимости pg заведёт свой отдельно) |

Переносятся каталоги проектов целиком (все файлы `src/*/*.csproj`, содержимое проектов — включая `appsettings*.json` панели), а не корень `src/`: в корне `src/` панели лежат 5 непереносимых файлов (`AdminPanel.slnx`, `Directory.Build.props`, `Directory.Packages.props`, `NuGet.Config`, `.editorconfig`) — включение каталога `src/` целиком недопустимо. Каталоги `frontend/` (вкл. `.npmrc`), `arch/`, `docs/`, `dev-stand/` едут целиком.

### 3.2. Целевое дерево pg (фрагмент, после переноса)

```
pg/
├── AGENTS.md                      # + секция про AdminPanel
├── .gitignore                     # + правила wwwroot/node_modules панели
├── arch/
│   ├── 14-pgworker.md             # правка указателя источника канона
│   ├── adminpanel/                # ← arch/ AdminPanel (02-etcd-contract.md и др.)
│   └── roadmap/pgworker.md        # + t08-unify-adminpanel-duplicates
├── dev-stand/
│   ├── compose.yaml, seed.sh      # стенд PgWorker (не трогаем)
│   └── adminpanel/                # ← dev-stand/ AdminPanel (compose, checks/, sidecar/, seed)
├── docker/
│   ├── PgWorker.Dockerfile        # существует
│   └── AdminPanel.Dockerfile      # ← Dockerfile AdminPanel
├── docs/
│   ├── superpowers/…              # доки pg (ссылки переправлены)
│   └── adminpanel/                # ← docs/ + README.md AdminPanel
│       ├── README.md              # ← корневой README панели
│       ├── INDEX.md               # ← docs/README.md панели (§1.3.7)
│       ├── 01…05-*.md
│       └── superpowers/
├── frontend/                      # ← frontend/ AdminPanel
└── src/
    ├── PgWorker.slnx              # + 7 проектов AdminPanel
    ├── Directory.Build.props      # единый (не меняется)
    ├── Directory.Packages.props   # + 2 пакета
    ├── AdminPanel.*/…             # 5 прод-проектов
    └── tests/                     # PgWorker.* + AdminPanel.*
```

Solution-папки в `PgWorker.slnx`: прод-проекты панели (5) — в новую папку `/admin/`; тесты (2) — в существующую `/tests/`.

## 4. Компоненты изменения

### 4.1. Перенос истории (filter-repo)

1. Свежий клон (filter-repo требует): `git clone --no-local /Users/demakaev/ZCodeProject/AdminPanel /tmp/adminpanel-import`.
2. Применить `git filter-repo --paths-from-file` со строками-включениями: **7 каталогов проектов** (`src/AdminPanel.Api/`, `src/AdminPanel.Core/`, `src/AdminPanel.Etcd/`, `src/AdminPanel.Infrastructure/`, `src/AdminPanel.Probes/`, `src/tests/AdminPanel.UnitTests/`, `src/tests/AdminPanel.IntegrationTests/` — каталог `src/` целиком НЕ включается: тянет 5 непереносимых файлов из корня), а также `frontend/`, `arch/`, `docs/`, `dev-stand/`, `README.md`, `Dockerfile`; и строками-переименованиями по таблице §3.1, включая `docs/README.md ==> docs/adminpanel/INDEX.md`. Порядок директив критичен (переименования применяются построчно к уже переименованным путям).
3. Зафиксировать число коммитов после filter-repo (`git rev-list --count HEAD`) — эталон для проверки вливания.
4. Проверка результата: `git log --stat` выборочно; сверка дерева последнего коммита с эталонными счётчиками файлов по каждому перенесённому пути (все переносимые пути на месте, непереносимых — включая `src/.editorconfig` — нет).

### 4.2. Вливание

В feature-ветке `feat-adminpanel-import` (worktree уже существует):

1. `git remote add adminpanel-import /tmp/adminpanel-import && git fetch adminpanel-import`.
2. `git merge --allow-unrelated-histories adminpanel-import/main` — конфликтов быть не должно по построению маппинга (пересечений путей нет, включая `src/`: непереносимые файлы верхнего уровня панели не приезжают, add/add на `Directory.*.props`/`NuGet.Config` невозможен).
3. Сверка сразу после merge (до инфраструктурных коммитов §4.3–§4.4): `git rev-list --count <pg-base>..HEAD` = эталон из §4.1.3 **плюс 2** (1 merge-коммит + 1 коммит входных документов задачи из фазы 0, §5), где `<pg-base>` — зафиксированный в фазе 0 HEAD main pg.
4. Cleanup: удалить remote `adminpanel-import` и каталог `/tmp/adminpanel-import` — только после успешной верификации §7.

### 4.3. Инфраструктура pg (каждый пункт — отдельный коммит)

1. **`src/Directory.Packages.props`** — добавить ровно 2 пакета: `Microsoft.AspNetCore.Mvc.Testing` 10.0.9, `Microsoft.AspNetCore.OpenApi` 10.0.11 (версии — как в AdminPanel; `xunit.runner.visualstudio 3.1.5` уже в pg, остальные версии совпадают по фактической сверке diff от 2026-08-27; пакеты pg, которых нет у панели — `M.E.Hosting`, `Polly`, `Polly.Contrib.WaitAndRetry` — остаются).
2. **`.gitignore`** — добавить блок «AdminPanel» (по образцу `.gitignore` AdminPanel): `node_modules/`, `dist/`, `src/AdminPanel.Api/wwwroot/` с комментарием, что wwwroot — SPA-бандл vite-сборки (`outDir` фронта), поставка собирает его заново. Сейчас в `.gitignore` pg таких правил нет.
3. **`src/PgWorker.slnx`** — добавить 7 проектов: 5 прод-проектов в solution-папку `/admin/`, `AdminPanel.UnitTests` и `AdminPanel.IntegrationTests` — в `/tests/`.
4. **Ссылки в доках** (grep-аудит по всему репо, правим только пути — не контент):
   - `docs/superpowers/2026-08-23-pgworker-backend/spec.md` (3 вхождения): `../AdminPanel/arch/02-etcd-contract.md` → `arch/adminpanel/02-etcd-contract.md`; `../AdminPanel/src/AdminPanel.Etcd` → `src/AdminPanel.Etcd`;
   - `docs/superpowers/2026-08-23-pgworker-backend/plan.md` (1): `../AdminPanel/src/Directory.Packages.props` → `src/Directory.Packages.props` (pg-версия администрирует версии);
   - `arch/14-pgworker.md` (строка 7): «контракт панели — репозиторий AdminPanel, `arch/02-etcd-contract.md` §9» → указание на `arch/adminpanel/02-etcd-contract.md` §9 внутри pg;
   - **внутренние ссылки перенесённых доков** (`docs/adminpanel/**` кроме `superpowers/`, `arch/adminpanel/**`, `dev-stand/adminpanel/**`):
     - «Назад: docs/README.md» в тематических доках 01–05 → `INDEX.md` (§1.3.7);
     - относительные ссылки на `arch/NN-*.md` из `docs/adminpanel/` → `../../arch/adminpanel/…`; на `dev-stand/README.md` → `../../dev-stand/adminpanel/README.md`;
     - текстовые пути старой структуры по паттернам: `dev-stand/…` → `dev-stand/adminpanel/…` (вкл. `docker-compose.yml` стенда и bash-блоки `cd dev-stand`), `docs/superpowers/…` панели → `docs/adminpanel/superpowers/…`, `../pg/…` → пути внутри pg, `../Puzzle` → `Puzzle`, `src/AdminPanel.slnx` в командах README → `src/PgWorker.slnx`;
     - исторические документы задач (`docs/adminpanel/superpowers/**`) не редактируются — это архив прошлых задач.

### 4.4. AGENTS.md и roadmap

1. **`AGENTS.md` pg** — секция про AdminPanel: код в `src/AdminPanel.*` (solution-папка `/admin/`), доки `arch/adminpanel/` + `docs/adminpanel/`, сборка фронта (`cd frontend && npm ci && npm run build`, outDir = `src/AdminPanel.Api/wwwroot`), запуск панели как хост-процесса (`dotnet run --project src/AdminPanel.Api` с `AdminPanel__Probes__Password`, порт 5050, cookie-логин admin), стенд разработки `dev-stand/adminpanel/`, отсылка к t08 по дублям.
2. **`arch/roadmap/pgworker.md`** — новая задача **`t08-unify-adminpanel-duplicates`** (следующий свободный номер после `t07`): унификация дублей кода — `AdminPanel.Etcd/Client/{EtcdGateway,IEtcdGateway,Kv}.cs` (урезанный аналог `PgWorker.Etcd/Client`, без Coordination) → `PgWorker.Etcd`, Puzzle-каркас `AdminPanel.Infrastructure` (attribute-DI, CQRS, Result, Traces) → `PgWorker.Core`. Формат записи — по правилам `arch/roadmap/README.md`. Задача добавляется, не исполняется — мерж-гейт удаления её не касается.

### 4.5. Что НЕ меняется

- `deploy/docker-compose.yml` и `docker/PgWorker.Dockerfile` — не трогаем.
- `dev-stand/compose.yaml`, `dev-stand/seed.sh` (стенд PgWorker) — не трогаем.
- Код обоих проектов, `src/Directory.Build.props`, `src/NuGet.Config` — без изменений.
- appsettings панели — переносятся как есть (включая большой `appsettings.Development.json` с HostMap стенда).

## 5. Фазы исполнения

| Фаза | Содержимое | Результат |
|---|---|---|
| 0. Подготовка | Чистота обоих репо (`git status`), фиксация фактического HEAD pg main (эталон — `8c33327`); `git-filter-repo` установлен (brew), иначе установить; worktree `feat-adminpanel-import` уже создан; коммит входных документов задачи | Предпосылки подтверждены |
| 1. Перенос истории | §4.1: клон в `/tmp`, filter-repo (`--paths-from-file`), эталонный счётчик, сверка деревьев | `/tmp/adminpanel-import` с переписанной историей |
| 2. Вливание | §4.2: remote, fetch, merge `--allow-unrelated-histories`, сверка счётчика | Все файлы панели в ветке, история едина |
| 3. Инфраструктура | §4.3–§4.4: 6 отдельных коммитов (CPM, .gitignore, slnx, ссылки, AGENTS.md, roadmap) | Монорепо собирается как единое целое |
| 4. Верификация | §7: build/test/npm + инспекционные проверки | Все критерии приёмки зелёные; cleanup `/tmp` |
| 5. Ревью и мерж | Diff-ревью перед `main`; мерж в `main` и пуш — только по явной просьбе пользователя | По отдельной команде |

## 6. Ограничения и что НЕ делаем

1. Старую папку `/Users/demakaev/ZCodeProject/AdminPanel` не удаляем и не меняем (архив; работа только с клоном).
2. `main` pg не переписывается: перенос — обычный merge-коммит в feature-ветке; push/мерж — только по явной просьбе.
3. Код не рефакторим, дубли не унифицируем (это `t08-unify-adminpanel-duplicates` в roadmap).
4. Интеграционные тесты (Testcontainers) и запуск docker-стендов — только с отдельного согласия пользователя.
5. Контракты данных/etcd и поведение обеих систем не меняются.
6. Непереносимые пути (§3.1) в pg не появляются; их AdminPanel-версии остаются только в архиве.
7. Русский язык для документации, английский для идентификаторов — как в конвенциях pg.

## 7. Критерии приёмки

Все команды выполняются в корне worktree `feat-adminpanel-import` (если не указано иное). Интеграционные тесты и docker-стенды в критерии не входят (п. 6.4).

1. **Сборка решения зелёная** (warnings-as-errors): `dotnet build src/PgWorker.slnx` — успех, 0 warnings; в выводе присутствуют все проекты PgWorker.* и AdminPanel.*.
2. **Юнит-тесты обеих систем зелёные**: `dotnet test src/tests/PgWorker.UnitTests` и `dotnet test src/tests/AdminPanel.UnitTests` — успех (базовый ориентир PgWorker: 357 тестов).
3. **Фронтенд собирается**: `cd frontend && npm ci && npm run build` — успех (включает `tsc --noEmit` ×2 + `vite build`); после сборки `git status --porcelain src/AdminPanel.Api/wwwroot` пуст (артефакт покрыт .gitignore).
4. **История AdminPanel в pg**: `git log --oneline --follow -- src/AdminPanel.Api/Program.cs` показывает исходные коммиты панели (авторы и даты сохранены); `git rev-list --count <pg-base>..HEAD` ≥ эталона filter-repo из §4.1.3 (эталон + merge-коммит + коммит входных доков + инфраструктурные коммиты; эталон < 190 — пустые коммиты отброшены).
5. **Оригинал не тронут**: `git -C /Users/demakaev/ZCodeProject/AdminPanel status` — чистое дерево, HEAD `ae25346`.
6. **Ссылок на старый репозиторий нет** (кроме входных документов самой задачи, которые фиксируют историю обсуждения): `grep -rn '\.\./AdminPanel' --include='*.md' . | grep -v 'docs/superpowers/2026-08-27-adminpanel-import/'` — пусто.
7. **Solution содержит проекты панели**: `dotnet sln src/PgWorker.slnx list` — в списке 7 проектов `AdminPanel.*`; в slnx есть solution-папка `/admin/`, тесты панели в `/tests/`.
8. **Стенд панели перенесён**: `ls dev-stand/adminpanel/checks/00-up.sh` существует; `dev-stand/compose.yaml` (стенд PgWorker) не изменён.
9. **Roadmap-задача на месте**: `grep -n 't08-unify-adminpanel-duplicates' arch/roadmap/pgworker.md` находит запись с описанием унификации (Etcd-клиент и Infrastructure).
10. **Доки на месте**: `arch/adminpanel/02-etcd-contract.md`, `docs/adminpanel/README.md` и `docs/adminpanel/INDEX.md` существуют; выборочная проверка относительных ссылок в перенесённых доках (не менее: `docs/adminpanel/README.md`, `docs/adminpanel/01-framework.md`, `dev-stand/adminpanel/README.md`) — пути указывают на существующие файлы.

## 8. Риски и компромиссы

- **Переписанные хэши**: ссылки на коммиты AdminPanel (например `ae25346`) в старых сообщениях pg не резолвятся — остаются текстом. Неизбежно, безопасно.
- **Потеря части коммитов**: коммиты, менявшие только непереносимые файлы (slnx, props, .gitignore и т.п.), станут пустыми и будут отброшены filter-repo. Ожидаемо; эталонный счётчик §4.1.3 делает потерю контролируемой.
- **Порт 2379 публикуют оба стенда** (`dev-stand/compose.yaml` pg и `dev-stand/adminpanel/docker-compose.yml`): одновременный запуск конфликтует по порту хоста. Это существующее поведение (комментарий стендов предполагает раздельный запуск), переносом не меняется; при необходимости решается в будущих задачах (не в этой).
- **Правки путей в живых доках панели** (README, INDEX, 01–05, arch/adminpanel, dev-stand/adminpanel): содержимое слегка расходится с оригиналами архива — принято; в монорепо доки должны быть навигаемы, контент (смысл, формулировки) не меняется.

## 9. Открытые вопросы

Нет — все развилки закрыты (§1.3): четыре решены в plan-mode, три (dev-stand, .dockerignore, README/INDEX) — вопросами от 2026-08-27, поправка по пакетам подтверждена фактической сверкой.
