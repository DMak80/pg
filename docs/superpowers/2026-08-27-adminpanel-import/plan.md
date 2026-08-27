# План: перенос AdminPanel в монорепозиторий pg (с историей)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Перенести репозиторий AdminPanel (190 коммитов) в монорепозиторий pg через `git filter-repo` с сохранением истории, влить 7 csproj в `src/PgWorker.slnx`, собрать доки в `arch/adminpanel/` + `docs/adminpanel/`, обновить инфраструктуру (CPM, .gitignore, ссылки, AGENTS.md, roadmap).

**Architecture:** Механический перенос без рефакторинга: (1) клон AdminPanel в `/tmp` + `git filter-repo --paths-from-file` (белый список путей + переименования), (2) `merge --allow-unrelated-histories` в feature-ветку, (3) 6 инфраструктурных коммитов в pg-файлах. Код панели не меняется; `../AdminPanel` остаётся архивом.

**Tech Stack:** git + git-filter-repo (brew), .NET 10 / CPM / `.slnx` (dotnet 10), npm (Node >= 22.12), BSD sed (macOS: `sed -i ''`) и BSD grep (поддерживает `--exclude-dir`).

**Spec:** `docs/superpowers/2026-08-27-adminpanel-import/spec.md` — план аргументируется от спеки; исполнитель читает оба документа. Ссылки «spec §N» — на её разделы.

**Ревью plan↔spec:** раунд 1 (findings 1–4) — счётчик docs/adminpanel=43; dev-stand-паттерны для `docs/adminpanel/0*.md`; `docker-compose` для arch/04; тексты линков и `../pg`/`../Puzzle` в 0*.md. Раунд 2 (findings A–B) — перенос src/ явным перечислением 7 проектов (каталог целиком тянул 5 непереносимых файлов, вкл. `.editorconfig`; эталоны 193/300); контроли Task 7 Шага 6 ограничены живыми доками (`--exclude-dir=superpowers`, точечные файлы стенда).

## Global Constraints (из spec §2, §6)

- Язык документации — русский; идентификаторы/команды — как есть.
- Код и контент панели не рефакторим; правки после merge — только инфраструктура и пути в доках.
- Оригинал `/Users/demakaev/ZCodeProject/AdminPanel` — только чтение (работаем с клоном в `/tmp`).
- `main` pg не трогаем; коммиты — только в `feat-adminpanel-import`; push — только по явной просьбе пользователя.
- Интеграционные тесты (Testcontainers) и docker-стенды — НЕ входят в план (только по отдельному согласию пользователя).
- `TreatWarningsAsErrors=true` — все сборки должны быть зелёными с 0 warnings.
- Historical-доки панели `docs/adminpanel/superpowers/**` не редактируются и не сканируются контролами (архив задач).

**Рабочие переменные (использовать во всех командах):**

```bash
WT=/Users/demakaev/ZCodeProject/worktrees/feat-adminpanel-import   # worktree, ветка feat-adminpanel-import
AP_SRC=/Users/demakaev/ZCodeProject/AdminPanel                     # оригинал (только чтение!)
TMP_CLONE=/tmp/adminpanel-import                                   # свежий клон для filter-repo
PG_BASE=8c33327                                                    # HEAD main pg (проверить в Task 0)
```

**Эталонные счётчики AdminPanel** (сняты 2026-08-27, `git ls-files`): всего **300** переносимых файлов: `src/` **193** — только 7 каталогов проектов (Api 27, Core 44, Etcd 19, Infrastructure 20, Probes 9, tests/UnitTests 55, tests/IntegrationTests 19; файлы верхнего уровня `src/` — `AdminPanel.slnx`, `Directory.Build.props`, `Directory.Packages.props`, `NuGet.Config`, `.editorconfig` — НЕ переносятся), `frontend/` 41, `arch/` 9, `dev-stand/` 13, `Dockerfile` 1, `docs/` 42 — **счётчик исходного репо**; целевой `docs/adminpanel/` = **43** (42 файла `docs/` + корневой `README.md`, ставший `docs/adminpanel/README.md`). Коммитов: 190.

---

### Task 0: Подготовка окружения и коммит входных документов

**Files:**
- Commit: `docs/superpowers/2026-08-27-adminpanel-import/` (spec.md, plan.md, approved-plan-input.md, research-notes.md)

**Вход:** worktree `feat-adminpanel-import` существует; спека утверждена.

**Выход:** предпосылки подтверждены, входные документы задачи в git.

**Связь со spec:** §5 фаза 0.

- [ ] **Шаг 1: Проверить чистоту обоих репозиториев и зафиксировать pg-base**

```bash
git -C "$WT" status --porcelain
# Ожидание: только '?? docs/superpowers/2026-08-27-adminpanel-import/' — ничего другого

git -C /Users/demakaev/ZCodeProject/AdminPanel status --porcelain && git -C /Users/demakaev/ZCodeProject/AdminPanel rev-parse --short HEAD
# Ожидание: пусто (чисто), затем ae25346

git -C /Users/demakaev/ZCodeProject/pg rev-parse --short main
# Ожидание: 8c33327. Если отличается — записать фактическое значение в PG_BASE (используется в Task 2).

git -C "$WT" log --oneline -1
# Ожидание: тот же коммит, что и main pg (ветка fresh от main)
```

- [ ] **Шаг 2: Установить git-filter-repo, если отсутствует**

```bash
command -v git-filter-repo || brew install git-filter-repo
git filter-repo --version
# Ожидание: версия печатается (например, 2.45.0 или новее)
```

- [ ] **Шаг 3: Проверить Node/npm для шага фронта (>= 22.12)**

```bash
node --version && npm --version
# Ожидание: v22.12+ (на стенде разработки 2026-08-27: v26.7.0 / 12.0.2)
```

- [ ] **Шаг 4: Закоммитить входные документы задачи**

```bash
cd "$WT"
git add docs/superpowers/2026-08-27-adminpanel-import/
git commit -m "docs(adminpanel-import): spec + plan + входные материалы переноса AdminPanel в монорепо"
git log --oneline -1
# Ожидание: новый коммит; git status чист
```

---

### Task 1: Перенос истории (git filter-repo)

**Files:**
- Create: `/tmp/adminpanel-paths.txt` (директивы filter-repo)
- Create: `/tmp/adminpanel-import/` (свежий клон — результат фильтрации)

**Interfaces:**
- Produces: `AP_COUNT` — эталонное число коммитов после фильтрации (используется в Task 2); клон `/tmp/adminpanel-import` с переписанной историей (используется в Task 2).

**Вход:** Task 0 пройден (filter-repo установлен, оригинал чист).

**Выход:** клон с историей только по переносимым путям, дерево сверенo с эталонами.

**Связь со spec:** §4.1 (маппинг §3.1, коллизия README — §1.3.7).

- [ ] **Шаг 1: Свежий клон и эталон «до»**

```bash
rm -rf "$TMP_CLONE"
git clone --no-local "$AP_SRC" "$TMP_CLONE"
git -C "$TMP_CLONE" rev-list --count HEAD
# Ожидание: 190 (зафиксировать фактически)
git -C "$TMP_CLONE" ls-files | wc -l
# Ожидание: 308 (все файлы, включая непереносимые)
git -C "$TMP_CLONE" ls-files src | grep -vE '^src/[^/]+/'
# Ожидание: ровно 5 строк — src/.editorconfig, src/AdminPanel.slnx, src/Directory.Build.props,
#            src/Directory.Packages.props, src/NuGet.Config (не переносятся — spec §3.1)
```

- [ ] **Шаг 2: Создать файл директив filter-repo**

Создать `/tmp/adminpanel-paths.txt` с содержимым (порядок строк критичен — директивы применяются построчно сверху вниз к уже переименованным путям; правило INDEX должно стоять между `docs/==>docs/adminpanel/` и `README.md==>…`, иначе оба README столкнутся — spec §1.3.7):

```
# Выбор (включить целиком; literal по умолчанию, каталог — с завершающим слэшем).
# src/ НЕ включаем каталогом: в его корне 5 непереносимых файлов (slnx, Build.props,
# Packages.props, NuGet.Config, .editorconfig) — включаем только 7 каталогов проектов.
src/AdminPanel.Api/
src/AdminPanel.Core/
src/AdminPanel.Etcd/
src/AdminPanel.Infrastructure/
src/AdminPanel.Probes/
src/tests/AdminPanel.UnitTests/
src/tests/AdminPanel.IntegrationTests/
frontend/
arch/
docs/
dev-stand/
README.md
Dockerfile

# Переименования (порядок критичен)
docs/==>docs/adminpanel/
docs/adminpanel/README.md==>docs/adminpanel/INDEX.md
README.md==>docs/adminpanel/README.md
arch/==>arch/adminpanel/
dev-stand/==>dev-stand/adminpanel/
Dockerfile==>docker/AdminPanel.Dockerfile
```

Проверка вручную перед запуском: `docs/README.md` → (правило 1) `docs/adminpanel/README.md` → (правило 2) `docs/adminpanel/INDEX.md` — и правило 3 (корневой `README.md` → `docs/adminpanel/README.md`) уже не найдёт совпадений с результатом правила 2.

- [ ] **Шаг 3: Применить filter-repo**

```bash
cd "$TMP_CLONE"
git filter-repo --paths-from-file /tmp/adminpanel-paths.txt
```

Ожидание: filter-repo отрабатывает без ошибок; предупреждения о переписанных хэшах — норма. Ошибок вида «collide»/«collision» быть не должно (порядок директив исключает).

- [ ] **Шаг 4: Сверить эталонный счётчик коммитов**

```bash
git -C "$TMP_CLONE" rev-list --count HEAD
# Ожидание: число МЕНЬШЕ 190 (пустые коммиты отброшены) — записать как AP_COUNT (spec §4.1.3, §8)
```

- [ ] **Шаг 5: Сверить дерево с эталонными счётчиками файлов**

```bash
cd "$TMP_CLONE"
git ls-files | wc -l                                   # Ожидание: 300
git ls-files src | wc -l                               # Ожидание: 193 (7 проектов, без файлов верхнего уровня src/)
git ls-files frontend | wc -l                          # Ожидание: 41
git ls-files arch/adminpanel | wc -l                   # Ожидание: 9
git ls-files docs/adminpanel | wc -l                   # Ожидание: 43 (42 файла docs/ + корневой README.md)
git ls-files dev-stand/adminpanel | wc -l              # Ожидание: 13
git ls-files docs/adminpanel/README.md                 # Ожидание: docs/adminpanel/README.md
git ls-files docs/adminpanel/INDEX.md                  # Ожидание: docs/adminpanel/INDEX.md (бывш. docs/README.md)
git ls-files docker/AdminPanel.Dockerfile              # Ожидание: docker/AdminPanel.Dockerfile
git ls-files | grep -cE '^(\.dev-flow/|\.gitignore$|\.dockerignore$|AGENTS\.md$|src/AdminPanel\.slnx$|src/Directory\.Build\.props$|src/Directory\.Packages\.props$|src/NuGet\.Config$|src/\.editorconfig$)'
# Ожидание: 0 — непереносимые пути (вкл. src/.editorconfig) отсутствуют
git ls-files '*.csproj'
# Ожидание: 7 строк — AdminPanel.Api/Core/Etcd/Infrastructure/Probes + tests/AdminPanel.{UnitTests,IntegrationTests}
```

Сверка суммы: 193 + 41 + 9 + 43 + 13 + 1 (Dockerfile) = 300.

- [ ] **Шаг 6: Выборочная проверка истории**

```bash
git -C "$TMP_CLONE" log --oneline | head -5
# Ожидание: верхний коммит — бывш. HEAD ae25346 (новый хэш), тема та же
git -C "$TMP_CLONE" log --oneline --follow -- src/AdminPanel.Api/Program.cs | wc -l
# Ожидание: > 0 — история файлов панели сохранилась
```

Если что-то не сошлось — клон удалить (`rm -rf "$TMP_CLONE" /tmp/adminpanel-paths.txt`) и повторить Шаг 1 с исправленными директивами; merge не начинать до полной сверки.

---

### Task 2: Вливание истории в feat-adminpanel-import

**Files:** — (только git-операции в worktree)

**Interfaces:**
- Consumes: `/tmp/adminpanel-import` из Task 1; `AP_COUNT` из Task 1 Шага 4; `PG_BASE` из Task 0.

**Вход:** Task 1 пройден полностью; worktree чист.

**Выход:** все 300 файлов панели в ветке `feat-adminpanel-import`, история едина.

**Связь со spec:** §4.2.

- [ ] **Шаг 1: Подключить клон как remote и fetch**

```bash
cd "$WT"
git remote add adminpanel-import "$TMP_CLONE"
git fetch adminpanel-import
git rev-list --count adminpanel-import/main
# Ожидание: AP_COUNT (то же число, что в Task 1 Шаг 4)
```

- [ ] **Шаг 2: Merge неродственных историй**

```bash
cd "$WT"
git merge --allow-unrelated-histories adminpanel-import/main \
  -m "merge: перенос AdminPanel с историей (filter-repo; docs/superpowers/2026-08-27-adminpanel-import)"
```

Ожидание: merge проходит БЕЗ конфликтов — пересечений путей нет, включая `src/` (файлы верхнего уровня `src/` панели не переносились, add/add на `Directory.*.props`/`NuGet.Config` невозможен — spec §1.1, §3.1). Если конфликт всё же возник — остановиться и разобрать каждый файл против таблицы маппинга spec §3.1; не «разруливать» правками кода.

- [ ] **Шаг 3: Сверить счётчик ветки**

```bash
cd "$WT"
git rev-list --count "$PG_BASE"..HEAD
# Ожидание: AP_COUNT + 2 (AP_COUNT коммитов панели + 1 merge-коммит + 1 коммит входных доков из Task 0)
git status --porcelain
# Ожидание: пусто
```

- [ ] **Шаг 4: Сверить ключевые пути в worktree**

```bash
cd "$WT"
ls src/AdminPanel.Api/AdminPanel.Api.csproj src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj \
   arch/adminpanel/02-etcd-contract.md docs/adminpanel/README.md docs/adminpanel/INDEX.md \
   dev-stand/adminpanel/checks/00-up.sh docker/AdminPanel.Dockerfile frontend/package.json
# Ожидание: все пути существуют
ls src/AdminPanel.slnx src/.editorconfig 2>/dev/null; echo "absent-ok"
# Ожидание: 'ls: ... No such file or directory' для обоих + 'absent-ok' — непереносимые не приехали
```

Remote `adminpanel-import` и `/tmp`-каталог НЕ удалять до Task 10 (cleanup после верификации — spec §4.2.4).

---

### Task 3: CPM — добавить 2 пакета (коммит 1/6)

**Files:**
- Modify: `src/Directory.Packages.props` (после строки `<PackageVersion Include="FluentAssertions" … />`, перед блоком `Microsoft.Extensions.*` — файл в алфавитном порядке)

**Вход:** Task 2 завершён.

**Выход:** пакеты панели доступны под CPM.

**Связь со spec:** §4.3.1 (поправка: `xunit.runner.visualstudio` уже есть — §1.3).

- [ ] **Шаг 1: Вставить две строки**

В `src/Directory.Packages.props` между строками `FluentAssertions` и `Microsoft.Extensions.Configuration` вставить (с сохранением отступа в 4 пробела):

```xml
    <PackageVersion Include="Microsoft.AspNetCore.Mvc.Testing" Version="10.0.9" />
    <PackageVersion Include="Microsoft.AspNetCore.OpenApi" Version="10.0.11" />
```

- [ ] **Шаг 2: Проверить restore**

```bash
cd "$WT/src"
dotnet restore PgWorker.slnx
# Ожидание: успех; предупреждений об отсутствующих версиях нет
```

- [ ] **Шаг 3: Коммит**

```bash
cd "$WT"
git add src/Directory.Packages.props
git commit -m "build(cpm): пакеты AdminPanel — AspNetCore.Mvc.Testing 10.0.9, AspNetCore.OpenApi 10.0.11"
```

---

### Task 4: .gitignore — правила фронта панели (коммит 2/6)

**Files:**
- Modify: `.gitignore` (в конец файла)

**Вход:** Task 2 завершён (wwwroot-артефакт появится в Task 10 при npm build).

**Выход:** артефакты vite/npm панели игнорируются.

**Связь со spec:** §4.3.2 (образец — `.gitignore` AdminPanel, строки 450–453; `.dev-flow/` в pg уже есть).

- [ ] **Шаг 1: Добавить блок в конец .gitignore**

```bash
cd "$WT"
cat >> .gitignore <<'EOF'

# AdminPanel: фронтенд (npm/vite) — SPA-бандл в wwwroot собирается поставкой заново
node_modules/
dist/
src/AdminPanel.Api/wwwroot/
EOF
tail -6 .gitignore
```

- [ ] **Шаг 2: Проверка**

```bash
cd "$WT"
mkdir -p src/AdminPanel.Api/wwwroot && touch src/AdminPanel.Api/wwwroot/probe.js
git status --porcelain src/AdminPanel.Api/wwwroot
# Ожидание: пусто (покрыто ignore)
rm -rf src/AdminPanel.Api/wwwroot
```

- [ ] **Шаг 3: Коммит**

```bash
git add .gitignore
git commit -m "chore(gitignore): node_modules/dist/wwwroot AdminPanel (vite-сборка)"
```

---

### Task 5: PgWorker.slnx — 7 проектов панели (коммит 3/6)

**Files:**
- Modify: `src/PgWorker.slnx` (новая Folder `/admin/` после `/app/`; +2 `<Project>` в `/tests/`)

**Вход:** Task 3 и Task 4 завершены.

**Выход:** solution собирает 15 проектов (8 pg + 7 панели).

**Связь со spec:** §4.3.3, §3.2.

Править XML напрямую (детерминированнее `dotnet sln add --solution-folder`, который может создать папку с именем без слэшей рядом с существующей `/tests/`).

- [ ] **Шаг 1: Вставить Folder /admin/ (после блока `/app/`, строки ~21–23)**

```xml
    <Folder Name="/admin/">
        <Project Path="AdminPanel.Api/AdminPanel.Api.csproj" />
        <Project Path="AdminPanel.Core/AdminPanel.Core.csproj" />
        <Project Path="AdminPanel.Etcd/AdminPanel.Etcd.csproj" />
        <Project Path="AdminPanel.Infrastructure/AdminPanel.Infrastructure.csproj" />
        <Project Path="AdminPanel.Probes/AdminPanel.Probes.csproj" />
    </Folder>
```

- [ ] **Шаг 2: Добавить 2 проекта в Folder `/tests/` (после `PgWorker.IntegrationTests`)**

```xml
        <Project Path="tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj" />
        <Project Path="tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj" />
```

- [ ] **Шаг 3: Проверка состава и сборка**

```bash
cd "$WT"
dotnet sln src/PgWorker.slnx list | grep -c 'AdminPanel'
# Ожидание: 7
dotnet build src/PgWorker.slnx
# Ожидание: успех, 0 warnings/errors (TreatWarningsAsErrors=true); в выводе — проекты PgWorker.* и AdminPanel.*
```

Если сборка упала по коду панели — это регресс переноса (не наша правка): сверить файл с оригиналом `diff -r "$AP_SRC/src/AdminPanel.Api" src/AdminPanel.Api` и устранить расхождение в дереве (не в коде).

- [ ] **Шаг 4: Коммит**

```bash
git add src/PgWorker.slnx
git commit -m "build(slnx): 7 проектов AdminPanel (/admin/ + /tests/)"
```

---

### Task 6: Ссылки в доках pg (часть коммита 4/6)

**Files:**
- Modify: `docs/superpowers/2026-08-23-pgworker-backend/spec.md` (строки 8, 10, 110)
- Modify: `docs/superpowers/2026-08-23-pgworker-backend/plan.md` (строка 23)
- Modify: `arch/14-pgworker.md` (строка 7)

**Вход:** Task 2 завершён (перенесённые доки существуют).

**Выход:** в доках pg нет путей `../AdminPanel/*`; arch/14 указывает на канон внутри pg.

**Связь со spec:** §4.3.4 (первые три пункта), §7.6.

- [ ] **Шаг 1: Правки 4 вхождений в docs/superpowers/2026-08-23-pgworker-backend/**

```bash
cd "$WT"
F=docs/superpowers/2026-08-23-pgworker-backend
sed -i '' -e 's|`../AdminPanel/arch/02-etcd-contract.md`|`arch/adminpanel/02-etcd-contract.md`|'  "$F/spec.md"   # строка 8
sed -i '' -e 's|`../AdminPanel/src/AdminPanel.Etcd`|`src/AdminPanel.Etcd`|g'                            "$F/spec.md"   # строки 10, 110
sed -i '' -e 's|`../AdminPanel/src/Directory.Packages.props`|`src/Directory.Packages.props`|'            "$F/plan.md"   # строка 23
```

- [ ] **Шаг 2: Правка указателя канона в arch/14-pgworker.md**

```bash
cd "$WT"
sed -i '' 's|панели — репозиторий AdminPanel, `arch/02-etcd-contract.md` §9)|панели — `arch/adminpanel/02-etcd-contract.md` §9, перенесён из репозитория AdminPanel)|' arch/14-pgworker.md
```

- [ ] **Шаг 3: Проверка (коммит НЕ делать — продолжение в Task 7)**

```bash
cd "$WT"
grep -rn '\.\./AdminPanel' --include='*.md' . | grep -v 'docs/superpowers/2026-08-27-adminpanel-import/'
# Ожидание: пусто (входные документы задачи исключены — там ссылки легитимны, spec §7.6)
```

---

### Task 7: Пути в переносимых доках панели (завершение коммита 4/6)

**Files (Modify, все — перенесённые на Шаге 2):**
- `docs/adminpanel/README.md`, `docs/adminpanel/INDEX.md`, `docs/adminpanel/0*.md`
- `arch/adminpanel/*.md`
- `dev-stand/adminpanel/README.md`, `dev-stand/adminpanel/docker-compose.yml`

**Вход:** Task 6 Шаги 1–2 сделаны.

**Выход:** живые доки панели навигабельны из pg; исторические `docs/adminpanel/superpowers/**` не тронуты.

**Связь со spec:** §4.3.4 (последний пункт), §1.3.7 (INDEX), §7.10; контент не меняем — только пути (spec §2.1).

- [ ] **Шаг 1: docs/adminpanel/README.md (бывш. корневой README)**

```bash
cd "$WT"
sed -i '' \
  -e 's|](arch/|](../../arch/adminpanel/|g' \
  -e 's|](docs/README.md)|](INDEX.md)|g' \
  -e 's|\[docs/README\.md\]|[INDEX.md]|g' \
  -e 's|](dev-stand/README.md)|](../../dev-stand/adminpanel/README.md)|g' \
  -e 's|](AGENTS.md)|](../../AGENTS.md)|g' \
  -e 's|`docs/superpowers/`|`docs/adminpanel/superpowers/`|g' \
  -e 's|`dev-stand/`|`dev-stand/adminpanel/`|g' \
  -e 's|src/AdminPanel\.slnx|src/PgWorker.slnx|g' \
  -e 's|cd dev-stand$|cd dev-stand/adminpanel|' \
  -e 's|cd dev-stand &&|cd dev-stand/adminpanel \&\&|' \
  -e 's|docker build -t adminpanel \.|docker build -f docker/AdminPanel.Dockerfile -t adminpanel .|' \
  -e 's|`../pg`|pg (этот монорепозиторий)|g' \
  -e 's|`../Puzzle`|`Puzzle`|g' \
  docs/adminpanel/README.md
```

Паттерн `\[docs/README\.md\]` чинит текст линка (например строка 93 `[`docs/README.md`](…)` — сам URL меняет предыдущий паттерн, текст — этот).

- [ ] **Шаг 2: docs/adminpanel/INDEX.md (бывш. docs/README.md)**

```bash
sed -i '' \
  -e 's|](../arch/README.md)|](../../arch/adminpanel/README.md)|' \
  -e 's|[docs/README.md](README.md)|[INDEX.md](INDEX.md)|' \
  -e 's|`../Puzzle`|`Puzzle`|' \
  docs/adminpanel/INDEX.md
```

Второй паттерн — пример шапки в соглашениях (строка 23): конвенция «Назад» теперь указывает на INDEX.md.

- [ ] **Шаг 3: docs/adminpanel/0N-*.md (тематические 01–05)**

```bash
sed -i '' \
  -e 's|[docs/README.md](README.md)|[INDEX.md](INDEX.md)|' \
  -e 's|](../arch/|](../../arch/adminpanel/|g' \
  -e 's|`dev-stand/README.md`|`dev-stand/adminpanel/README.md`|g' \
  -e 's|`dev-stand/`|`dev-stand/adminpanel/`|g' \
  -e 's|cd dev-stand$|cd dev-stand/adminpanel|' \
  -e 's|cd dev-stand &&|cd dev-stand/adminpanel \&\&|' \
  -e 's|`../pg`|pg (этот монорепозиторий)|g' \
  -e 's|\.\./Puzzle|Puzzle|g' \
  docs/adminpanel/0*.md
grep -n 'Назад' docs/adminpanel/0*.md
# Ожидание: 5 строк, все с [INDEX.md](INDEX.md)
```

Паттерны `cd dev-stand$`/`cd dev-stand &&` закрывают bash-блоки (например `05-dev-stand.md:31`); `` `dev-stand/` `` — подсистему в шапке 05; `../pg`/`../Puzzle` — упоминания референсов в 01/02/05.

- [ ] **Шаг 4: arch/adminpanel/*.md (канон панели)**

```bash
sed -i '' \
  -e 's|\.\./pg/arch/|arch/|g' \
  -e 's|\.\./pg arch/|arch/|g' \
  -e 's|`../pg`|pg (этот монорепозиторий)|g' \
  -e 's|\.\./Puzzle|Puzzle|g' \
  -e 's|](../docs/README.md)|](../../docs/adminpanel/INDEX.md)|' \
  -e 's|docs/superpowers/|docs/adminpanel/superpowers/|g' \
  arch/adminpanel/*.md
# Стенд-пути в каноне стенда (04-local-stand.md) — точечно:
# (паттерны «cd dev-stand» — только с явным окончанием ($, &&, ;), иначе в цепочке -e
#  они удвоят adminpanel/ в путях, уже заменённых предыдущими выражениями)
sed -i '' \
  -e 's|dev-stand/docker-compose|dev-stand/adminpanel/docker-compose|g' \
  -e 's|dev-stand/checks/|dev-stand/adminpanel/checks/|g' \
  -e 's|dev-stand/seed|dev-stand/adminpanel/seed|g' \
  -e 's|dev-stand/sidecar|dev-stand/adminpanel/sidecar|g' \
  -e 's|dev-stand/compose|dev-stand/adminpanel/compose|g' \
  -e 's|`dev-stand/`|`dev-stand/adminpanel/`|g' \
  -e 's|cd dev-stand$|cd dev-stand/adminpanel|' \
  -e 's|cd dev-stand &&|cd dev-stand/adminpanel \&\&|' \
  -e 's|cd dev-stand;|cd dev-stand/adminpanel;|' \
  arch/adminpanel/04-local-stand.md
```

Паттерн `dev-stand/docker-compose` стоит первым — файл стенда называется `docker-compose.yml` (arch/04:9), а не `compose*`.

- [ ] **Шаг 5: dev-stand/adminpanel/README.md и docker-compose.yml**

```bash
sed -i '' \
  -e 's|`arch/04-local-stand.md`|`../../arch/adminpanel/04-local-stand.md`|' \
  -e 's|docs/superpowers/|docs/adminpanel/superpowers/|g' \
  -e 's|cd dev-stand$|cd dev-stand/adminpanel|' \
  -e 's|cd dev-stand &&|cd dev-stand/adminpanel \&\&|' \
  -e 's|`../pg`|pg (этот монорепозиторий)|g' \
  dev-stand/adminpanel/README.md
sed -i '' \
  -e 's|\.\./pg arch/|arch/|g' \
  -e 's|\.\./pg|pg (монорепозиторий)|g' \
  dev-stand/adminpanel/docker-compose.yml
```

- [ ] **Шаг 6: Контроль остатков и выверка диффа (только живые доки)**

Контроли сканируют только правимые живые доки: `docs/adminpanel` с `--exclude-dir=superpowers` (исторические spec/plan по правилам не редактируются — вхождения там легитимны), `arch/adminpanel` и точечно README/docker-compose стенда (код стенда `checks/*.sh`, `sidecar/` не правился и не сканируется).

```bash
cd "$WT"
grep -rn '\.\./pg\|\.\./Puzzle' docs/adminpanel --exclude-dir=superpowers arch/adminpanel dev-stand/adminpanel/README.md dev-stand/adminpanel/docker-compose.yml
# Ожидание: пусто
grep -rEn 'dev-stand/(checks|seed|sidecar|compose|docker-)' docs/adminpanel --exclude-dir=superpowers arch/adminpanel dev-stand/adminpanel/README.md | grep -v 'dev-stand/adminpanel'
# Ожидание: пусто
grep -c 'docs/README.md' docs/adminpanel/README.md docs/adminpanel/INDEX.md
# Ожидание: 0 и 0
git diff --stat -- docs/adminpanel arch/adminpanel dev-stand/adminpanel
# Ожидание: только перечисленные выше файлы (README, INDEX, 01–05, arch/*, dev-stand README+compose);
# docs/adminpanel/superpowers/** в diff ОТСУТСТВУЮТ
```

Затем просмотреть `git diff` каждого файла: правки — только путей; формулировки не менять. Остаточные упоминания старых путей, не покрытые sed, добить точечными Edit (контекст: карта репозитория README, таблицы arch/README).

- [ ] **Шаг 7: Выборочная проверка живых ссылок (spec §7.10)**

```bash
cd "$WT/docs/adminpanel"
test -f ../../arch/adminpanel/02-etcd-contract.md && test -f INDEX.md && test -f ../../dev-stand/adminpanel/README.md && test -f ../../AGENTS.md && echo LINKS-OK
# Ожидание: LINKS-OK (все цели markdown-ссылок README существуют)
cd ../../arch/adminpanel && test -f ../../docs/adminpanel/INDEX.md && echo ARCH-LINKS-OK
# Ожидание: ARCH-LINKS-OK
```

- [ ] **Шаг 8: Коммит 4/6 (ссылки — единый коммит Task 6+7)**

```bash
cd "$WT"
git add docs/superpowers/2026-08-23-pgworker-backend arch/14-pgworker.md \
        docs/adminpanel arch/adminpanel dev-stand/adminpanel/README.md dev-stand/adminpanel/docker-compose.yml
git commit -m "docs: переправить пути после переноса AdminPanel (arch/adminpanel, docs/adminpanel, dev-stand/adminpanel)"
```

---

### Task 8: AGENTS.md — секция AdminPanel (коммит 5/6)

**Files:**
- Modify: `AGENTS.md` (вставка между абзацем «⚠️ PgWorker ВСЕГДА запускается в докере…» и абзацем «**Нужно использовать** подходы…»)

**Вход:** Task 7 завершён (структура путей зафиксирована).

**Выход:** инструкции по сборке/запуску панели в AGENTS.md.

**Связь со spec:** §4.4.1.

- [ ] **Шаг 1: Вставить секцию**

Точный текст для вставки (отдельным абзацем):

```markdown
**AdminPanel** (панель администрирования шардированных кластеров; перенесена из `../AdminPanel` 2026-08-27 с сохранением истории): код — `src/AdminPanel.*` (solution-папка `/admin/`), канон — `arch/adminpanel/` (вкл. etcd-контракт `02-etcd-contract.md`), практики — `docs/adminpanel/`. Запуск — **хост-процессом** (исключение из docker-правила выше): `cd frontend && npm ci && npm run build` (SPA-бандл → `src/AdminPanel.Api/wwwroot`), затем `dotnet run --project src/AdminPanel.Api` с `AdminPanel__Probes__Password` (порт 5050, cookie-логин). Dev-стенд панели — `dev-stand/adminpanel/` (профили quick/full, e2e-чеки `checks/`). Дубли кода с PgWorker (`AdminPanel.Etcd`, `AdminPanel.Infrastructure`) — осознанные, унификация в roadmap (`t08-unify-adminpanel-duplicates`).
```

- [ ] **Шаг 2: Проверка и коммит**

```bash
cd "$WT"
grep -n 'AdminPanel' AGENTS.md
# Ожидание: секция на месте, остальной текст не тронут
git add AGENTS.md
git commit -m "docs(agents): секция AdminPanel — сборка, запуск, стенд, отсылка к t08"
```

---

### Task 9: Roadmap — t08-unify-adminpanel-duplicates (коммит 6/6)

**Files:**
- Modify: `arch/roadmap/pgworker.md` (новый пункт после `t07-move-bucket-ui`)

**Вход:** Task 8 завершён.

**Выход:** задача унификации дублей в backlog (не исполняется в этой задаче).

**Связь со spec:** §4.4.2; формат — по `arch/roadmap/README.md` (тег `tNN-slug` — следующий свободный после t07; описание результата).

- [ ] **Шаг 1: Добавить пункт в конец раздела «## Задачи»**

```markdown
- **`t08-unify-adminpanel-duplicates`** — унификация дублей кода после переноса
  AdminPanel в монорепо (2026-08-27): etcd-клиент `AdminPanel.Etcd/Client/`
  (`EtcdGateway`/`IEtcdGateway`/`Kv` — урезанный аналог `PgWorker.Etcd/Client`,
  без Coordination) → перевод панели на `PgWorker.Etcd`; Puzzle-каркас
  `AdminPanel.Infrastructure` (attribute-DI, CQRS, `Result`, Traces) → перевод
  на `PgWorker.Core`. Механика: панель получает ProjectReference на общие
  сборки, дубли удаляются; поведение обеих систем не меняется (тесты зелёные).
```

- [ ] **Шаг 2: Проверка и коммит**

```bash
cd "$WT"
grep -n 't08-unify-adminpanel-duplicates' arch/roadmap/pgworker.md
# Ожидание: 1 вхождение с описанием (spec §7.9)
git add arch/roadmap/pgworker.md
git commit -m "docs(roadmap): t08-unify-adminpanel-duplicates — унификация дублей AdminPanel/PgWorker"
```

---

### Task 10: Верификация по критериям приёмки spec §7 + cleanup

**Files:** — (только проверки и cleanup).

**Вход:** Tasks 0–9 завершены.

**Выход:** все 10 критериев приёмки spec §7 зелёные; временные ресурсы удалены.

**Связь со spec:** §7 (все пункты), §4.2.4 (cleanup).

- [ ] **Шаг 1: Сборка решения (spec §7.1)**

```bash
cd "$WT"
dotnet build src/PgWorker.slnx
# Ожидание: успех, 0 warnings; в выводе PgWorker.* и AdminPanel.*
```

- [ ] **Шаг 2: Юнит-тесты обеих систем (spec §7.2)**

```bash
cd "$WT"
dotnet test src/tests/PgWorker.UnitTests
# Ожидание: успех (~357 тестов)
dotnet test src/tests/AdminPanel.UnitTests
# Ожидание: успех. Интеграционные (AdminPanel.IntegrationTests, PgWorker.IntegrationTests) — НЕ запускать (spec §6.4)
```

- [ ] **Шаг 3: Фронтенд (spec §7.3)**

```bash
cd "$WT/frontend"
npm ci
npm run build
# Ожидание: успех (tsc --noEmit ×2 + vite build)
cd "$WT"
git status --porcelain src/AdminPanel.Api/wwwroot
# Ожидание: пусто (Task 4 покрыл ignore)
```

- [ ] **Шаг 4: История и оригинал (spec §7.4–7.5)**

```bash
cd "$WT"
git log --oneline --follow -- src/AdminPanel.Api/Program.cs | head
# Ожидание: исходные коммиты панели (авторы/даты сохранены)
git rev-list --count "$PG_BASE"..HEAD
# Ожидание: AP_COUNT + 8 (merge-коммит + входные доки Task 0 + 6 инфраструктурных коммитов)
git -C "$AP_SRC" status --porcelain; git -C "$AP_SRC" rev-parse --short HEAD
# Ожидание: пусто; ae25346 — оригинал не тронут
```

- [ ] **Шаг 5: Ссылки и структура (spec §7.6–7.10)**

```bash
cd "$WT"
grep -rn '\.\./AdminPanel' --include='*.md' . | grep -v 'docs/superpowers/2026-08-27-adminpanel-import/'
# Ожидание: пусто
dotnet sln src/PgWorker.slnx list | grep -c AdminPanel
# Ожидание: 7
ls dev-stand/adminpanel/checks/00-up.sh arch/adminpanel/02-etcd-contract.md docs/adminpanel/README.md docs/adminpanel/INDEX.md
# Ожидание: все существуют
git diff main..HEAD --stat -- dev-stand/compose.yaml dev-stand/seed.sh deploy/docker-compose.yml docker/PgWorker.Dockerfile
# Ожидание: пусто — инфраструктура PgWorker не тронута (spec §4.5)
```

- [ ] **Шаг 6: Cleanup (только после зелёных Шагов 1–5)**

```bash
cd "$WT"
git remote remove adminpanel-import
rm -rf /tmp/adminpanel-import /tmp/adminpanel-paths.txt
git remote -v && ls /tmp/adminpanel-import 2>/dev/null; echo "cleanup done"
# Ожидание: remote отсутствует, /tmp пуст; 'cleanup done'
git status --porcelain
# Ожидание: пусто (wwwroot покрыт ignore)
```

---

### Task 11: Итоговый отчёт для ревью (гейт main-агента)

**Files:** — (без изменений кода).

**Вход:** Task 10 зелёный.

**Выход:** сводка для diff-ревью перед `main`.

**Связь со spec:** §5 фаза 5 (мерж/пуш — только по явной просьбе пользователя; НЕ делать).

- [ ] **Шаг 1: Собрать отчёт**

```bash
cd "$WT"
git log --oneline "$PG_BASE"..HEAD | head -15
git diff --stat "$PG_BASE"..HEAD | tail -5
```

В отчёте main-агенту отразить: AP_COUNT (фактическое), результаты всех проверок §7 (по одному на пункт), список инфраструктурных коммитов, отклонения от плана (если были).

- [ ] **Шаг 2: НЕ делать самостоятельно**

Не мержить в `main`, не push, не запускать интеграционные тесты и docker-стенды — всё это только по отдельному явному согласию/просьбе пользователя (spec §5, §6).

---

## Опционально (в план НЕ входит; только по отдельному согласию пользователя)

- Интеграционные тесты: `dotnet test src/tests/AdminPanel.IntegrationTests`, `dotnet test src/tests/PgWorker.IntegrationTests` (Testcontainers, нужен Docker).
- Подъём dev-стенда панели: `cd dev-stand/adminpanel && docker compose up -d` (порт 2379 конфликтует со стендом pg при одновременном запуске — spec §8).
- Ручная проверка панели: `dotnet run --project src/AdminPanel.Api` + `open http://localhost:5050`.

## Self-review плана

- Покрытие spec: §4.1 → Task 1; §4.2 → Task 2; §4.3.1–3 → Tasks 3–5; §4.3.4 → Tasks 6–7; §4.4 → Tasks 8–9; §7 → Task 10; §5 фаза 5 → Task 11; §3.1/§1.3.7 (INDEX) → Task 1 Шаг 2 + Task 7 Шаги 2–3. Пропусков нет.
- Placeholder-скан: все шаги содержат конкретные команды/тексты; «TBD» нет.
- Согласованность: эталонные счётчики едины во всех задачах — `src/` = 193 (7 каталогов проектов: 27+44+19+20+9+55+19; 5 файлов верхнего уровня src/ — slnx, Build.props, Packages.props, NuGet.Config, .editorconfig — не переносятся, поэтому в `--paths-from-file` каталог `src/` НЕ включается), целевой `docs/adminpanel/` = 43 (в исходном репо `docs/` = 42; 42 + корневой README.md), сумма 300 = 193+41+9+43+13+1; формула ревизий в Task 2 (AP_COUNT+2: merge + входные доки) согласована с коммитом Task 0 и с spec §4.2.3; в Task 10 — AP_COUNT+8 (merge + доки + 6 инфраструктурных); sed-паттерны «cd dev-stand» везде с явным окончанием строки/`&&`/`;` (не удваивают adminpanel/ в цепочках -e); контроли Task 7 Шага 6 сканируют только живые доки (`docs/adminpanel --exclude-dir=superpowers`, `arch/adminpanel`, точечно `dev-stand/adminpanel/README.md` + `docker-compose.yml`) по набору `dev-stand/(checks|seed|sidecar|compose|docker-)` — исторические superpowers и код стенда в контроли не попадают.
- Ревью Фазы 4: раунд 1 (findings 1–4) устранён; раунд 2: finding A — белый список src/ сужен с каталога до 7 проектов (исключены 5 непереносимых файлов верхнего уровня, вкл. `src/.editorconfig`; эталоны 193/300; в Task 2 добавлена проверка отсутствия `src/AdminPanel.slnx`/`src/.editorconfig`), finding B — контроли Шага 6 ограничены живыми доками.
