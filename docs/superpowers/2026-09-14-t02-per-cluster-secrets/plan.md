# t02-per-cluster-secrets — roadmap-чистка хвоста мерж-гейта 5bcd467: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Выполнить невыполненный хвост мерж-гейта merge `5bcd467` — снять слитые теги `t02-per-cluster-secrets` и `t07-kafka-ca-rotation` из `arch/roadmap/`, добавить отложенный пункт `t02-external-secret-manager` и починить пример тега в `arch/roadmap/README.md`.

**Architecture:** Docs-only правка ровно трёх файлов `arch/roadmap/` одним коммитом в ветке задачи. Код, тесты, сервисные arch-контракты, deploy/, стенд — не трогаются. Тестовый цикл задачи — grep-гейты и `git diff` (кода нет, TDD неприменима).

**Tech Stack:** Markdown (`arch/roadmap/*.md`), git, grep.

**Spec:** [`docs/superpowers/2026-09-14-t02-per-cluster-secrets/spec.md`](./spec.md) (в worktree `/Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets`).

## Global Constraints

- **Docs-only**: меняются только `arch/roadmap/pgworker.md`, `arch/roadmap/kafkaworker.md`, `arch/roadmap/README.md` (spec §5.4, §7). Код (`src/**`), тесты, arch-контракты сервисов (14/15/16/19, adminpanel/02), deploy/, стенд — без изменений.
- **Один docs-only коммит** в ветке задачи (spec §6, фаза 2): удаление `t02-per-cluster-secrets` и добавление `t02-external-secret-manager` — тем же коммитом (слот t02 освобождается тем же коммитом, spec §5.1).
- **Никаких пометок «закрыта/реализована»** в roadmap (spec §3.1): слитое удаляется, история живёт в git и `docs/superpowers/`.
- **Коммит в feature-ветку — свободно; мерж в `main` и пуш — только по явной просьбе пользователя** (AGENTS.base §6; spec §7).
- Сборка/E2E/docker не требуются: код не тронут (spec §7).
- Язык правок — русский; теги задач и идентификаторы — английские (spec §3.4).
- Артефакты флоу (`spec.md`, `plan.md` в `docs/superpowers/2026-09-14-t02-per-cluster-secrets/`) — законная часть ветки задачи и НЕ входят в «три файла» критерия приёмки 4: проверка §8.4 выполняется с исключением `docs/superpowers` (spec-коммит `5eb1683` уже в ветке — буквальное «в diff только три файла» физически недостижимо и противоречило бы канону dev-flow «spec/plan лежат в ветке задачи»).
- Все bash-команды исполняются из worktree: рабочая директория между вызовами сбрасывается — в командах ниже всегда явный `cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && ...` или абсолютные пути.
- Базовое состояние (проверено при составлении плана, 2026-09-14): ветка `t02-per-cluster-secrets`, HEAD `5eb1683` (spec-коммит, родитель — `main` `b76a3f9`), рабочее дерево чистое; теги встречаются ровно в трёх местах — `pgworker.md:9`, `kafkaworker.md:10`, `README.md:8`; `←`-зависимостей на оба тега нет.

---

### Task 1: `arch/roadmap/pgworker.md` — снять `t02-per-cluster-secrets`, добавить `t02-external-secret-manager`

**Files:**
- Modify: `arch/roadmap/pgworker.md:9-12` (полный путь `/Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets/arch/roadmap/pgworker.md`)

**Interfaces:**
- Consumes: spec §5.1 (точная формулировка нового пункта); текущее состояние файла (блок `t02-per-cluster-secrets` — первый пункт раздела «## Задачи», далее `t05-quarantine-merge`, `t08-unify-adminpanel-duplicates`).
- Produces: `pgworker.md`, в котором первый пункт «## Задачи» — `t02-external-secret-manager`, тега `t02-per-cluster-secrets` нет; пункты t05/t08 нетронуты. На это состояние опираются проверки Task 4 (критерии приёмки §8.1 и §8.3).

**Вход (предусловие):** ветка `t02-per-cluster-secrets` в worktree, рабочее дерево чистое (`git status --short` пуст); `arch/roadmap/pgworker.md` в состоянии `main` (строки 9–12 — блок `t02-per-cluster-secrets`).

**Действие:** одна Edit-замена: блок старого пункта (4 строки) заменяется на блок нового пункта (7 строк, текст — дословно из spec §5.1). Старый блок стоит первым после «## Задачи», новый обязан встать первым же — замена блока на блок сохраняет позицию.

**Выход:** `pgworker.md` соответствует spec §5.1: снятый тег отсутствует, отложенная задача добавлена первой, порядок t02→t05→t08 сохранён.

**Проверка:** grep-команды шага 3 (новый пункт на месте первого, старого тега нет, t05/t08 живы).

**Связь со spec:** §5.1 (обе правки файла), §2 (имя `t02-external-secret-manager` — решение пользователя), §4 строка 10 таблицы / §8.3 (закрываемая часть хвоста мерж-гейта).

- [ ] **Step 1: Убедиться в базовом состоянии**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && git status --short && git log --oneline -1
```
Expected: `git status --short` — пусто; HEAD — `5eb1683` (или последующий коммит плана; главное — ветка `t02-per-cluster-secrets`, `git branch --show-current` = `t02-per-cluster-secrets`).

- [ ] **Step 2: Выполнить замену блока (Edit)**

Edit `/Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets/arch/roadmap/pgworker.md`:

`old_string` (строки 9–12, дословно):
```markdown
- **`t02-per-cluster-secrets`** — ротация секретов per-cluster (смена без
  остановки записи), генерация per-cluster `bucket_mover`, интеграция с
  secret-manager. Генерация per-cluster app-секрета в etcd сделана
  (2026-08-28, feat-etcd-password-field).
```

`new_string` (дословно из spec §5.1):
```markdown
- **`t02-external-secret-manager`** — интеграция с внешним secret-manager:
  публикация/чтение per-install/per-cluster секретов внешним SM поверх
  etcd-канона (per-cluster app/bucket_admin/mover + backup — arch/14 §4/§5 I,
  arch/19 §7; слито 2026-09-06, merge 5bcd467). Решение пользователя
  (2026-09-06): etcd остаётся единственным хранилищем per-cluster секретов
  до этой интеграции.
```

- [ ] **Step 3: Проверить результат правки**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  grep -n "t02-external-secret-manager" arch/roadmap/pgworker.md && \
  grep -n "t02-per-cluster-secrets" arch/roadmap/pgworker.md ; echo "old-tag-grep-exit=$?" ; \
  grep -n "t05-quarantine-merge\|t08-unify-adminpanel-duplicates" arch/roadmap/pgworker.md
```
Expected: `t02-external-secret-manager` найден (первое вхождение — первый пункт после «## Задачи», до `t05`); `t02-per-cluster-secrets` — нет вывода, `old-tag-grep-exit=1`; `t05-quarantine-merge` и `t08-unify-adminpanel-duplicates` — оба найдены. Коммит НЕ делаем — единый коммит всех трёх файлов в Task 4.

---

### Task 2: `arch/roadmap/kafkaworker.md` — снять `t07-kafka-ca-rotation`, оставить пустой раздел «## Задачи»

**Files:**
- Modify: `arch/roadmap/kafkaworker.md:8-13` (полный путь `/Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets/arch/roadmap/kafkaworker.md`)

**Interfaces:**
- Consumes: spec §5.2; текущее состояние файла (блок `t07-kafka-ca-rotation` — единственный пункт, строки 10–13, файл кончается им).
- Produces: `kafkaworker.md` существует, раздел «## Задачи» пуст (как у `arch/roadmap/backup.md`, строка 14 — последний содержательный заголовок). На это опирается критерий приёмки §8.2 (Task 4).

**Вход (предусловие):** Task 1 выполнен (или независим — правки разных файлов); файл в состоянии `main`.

**Действие:** одна Edit-замена: заголовок «## Задачи» + блок `t07` целиком заменяются на один заголовок «## Задачи» (пустой раздел в конце файла — прецедент `backup.md`).

**Выход:** `kafkaworker.md` — преамбула трека + пустой «## Задачи»; тега `t07-kafka-ca-rotation` нет.

**Проверка:** grep-команды шага 2.

**Связь со spec:** §5.2 (снятие второго тега мерж-гейта `5bcd467`), §4 (заключение: CaRotator слит тем же merge — тег снимается по правилу «Roadmap — только несделанные задачи»), §8.2.

- [ ] **Step 1: Выполнить удаление блока (Edit)**

Edit `/Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets/arch/roadmap/kafkaworker.md`:

`old_string` (дословно, строки 8–13):
```markdown
## Задачи

- **`t07-kafka-ca-rotation`** — ротация per-cluster CA и серверных сертификатов
  (окно двойного доверия CA/серт-версий в env, rolling-пересоздание брокеров;
  отложено из t03-kafka: серты долгоживущие — 10 лет; зависит от канона
  безопасности arch/16 §2.3 и `BrokerCertificateCache`).
```

`new_string`:
```markdown
## Задачи
```

- [ ] **Step 2: Проверить результат правки**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  grep -n "t07-kafka-ca-rotation" arch/roadmap/kafkaworker.md ; echo "tag-grep-exit=$?" ; \
  tail -n 2 arch/roadmap/kafkaworker.md
```
Expected: `t07-kafka-ca-rotation` — нет вывода, `tag-grep-exit=1`; `tail` показывает, что файл заканчивается строкой `## Задачи` (после неё — только перевод строки). Файл существует и не пуст (`test -s arch/roadmap/kafkaworker.md && echo OK`). Коммит НЕ делаем — единый коммит в Task 4.

---

### Task 3: `arch/roadmap/README.md` — пример тега в правиле ведения заменить на живой

**Files:**
- Modify: `arch/roadmap/README.md:8` (полный путь `/Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets/arch/roadmap/README.md`)

**Interfaces:**
- Consumes: spec §5.3; текущее состояние файла (строка 8 правила «Тег задачи» содержит пример `t02-per-cluster-secrets`).
- Produces: README без ссылок на несуществующий тег; полный grep `t02-per-cluster-secrets` по `arch/roadmap/` становится пустым (критерий §8.1, Task 4).

**Вход (предусловие):** Task 1 выполнен (иначе grep-гейт §8.1 не станет пустым); файл в состоянии `main`.

**Действие:** одна Edit-замена в строке 8: `t02-per-cluster-secrets` → `t05-quarantine-merge`. Больше в README ничего не меняется.

**Выход:** иллюстрация в правилах ведения ссылается на живой тег `t05-quarantine-merge`; во всём `arch/roadmap/` тег `t02-per-cluster-secrets` не встречается.

**Проверка:** grep-команды шага 2.

**Связь со spec:** §5.3, §8.1 («включая README-пример: заменён на живой тег»).

- [ ] **Step 1: Выполнить замену примера (Edit)**

Edit `/Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets/arch/roadmap/README.md`:

`old_string` (дословно, строка 8):
```markdown
- **Тег задачи**: `tNN-slug` (например, `t02-per-cluster-secrets`), NN — порядок
```

`new_string`:
```markdown
- **Тег задачи**: `tNN-slug` (например, `t05-quarantine-merge`), NN — порядок
```

- [ ] **Step 2: Проверить результат правки**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  grep -n "t05-quarantine-merge" arch/roadmap/README.md && \
  grep -rn "t02-per-cluster-secrets" arch/roadmap/ ; echo "grep-exit=$?"
```
Expected: `t05-quarantine-merge` найден в строке 8 README; рекурсивный grep по `arch/roadmap/` — нет вывода, `grep-exit=1` (тега больше нет нигде, включая README). Коммит НЕ делаем — единый коммит в Task 4.

---

### Task 4: Верификация по критериям §8 + единый docs-only коммит

**Files:**
- Коммит: `arch/roadmap/pgworker.md`, `arch/roadmap/kafkaworker.md`, `arch/roadmap/README.md` (никаких новых файлов).

**Interfaces:**
- Consumes: конечные состояния трёх файлов из Task 1–3; ветка `t02-per-cluster-secrets` (HEAD — spec-коммит `5eb1683` + возможно коммит плана от гейта user-review).
- Produces: один docs-only коммит в ветке задачи, содержащий ровно три roadmap-файла; ветка готова к ревью и (после явной просьбы пользователя) мержу в `main`.

**Вход (предусловие):** Task 1, 2, 3 выполнены; правки ещё не закоммичены (`git status --short` показывает три изменённых файла `arch/roadmap/`).

**Действие:** прогон всех grep/`git`-гейтов критериев приёмки §8 (1–4), затем один коммит трёх файлов; контрольная проверка коммита.

**Выход:** хвост мерж-гейта `5bcd467` закрыт в ветке; roadmap соответствует правилу «только несделанные задачи».

**Проверка:** шаги 1–5 ниже (все гейты — до коммита; шаги 6–8 — коммит и контроль после).

**Связь со spec:** §6 фазы 2–3 (roadmap-правка + верификация), §8 целиком; AGENTS.base §6 (коммит в feature-ветку свободно, мерж в `main` — только по явной просьбе).

- [ ] **Step 1: Гейт §8.1 — тега `t02-per-cluster-secrets` нет во всём `arch/roadmap/`**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  grep -rn "t02-per-cluster-secrets" arch/roadmap/ ; echo "exit=$?"
```
Expected: нет вывода, `exit=1`.

- [ ] **Step 2: Гейт §8.2 — тега `t07-kafka-ca-rotation` нет; `kafkaworker.md` жив с пустым «## Задачи»**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  grep -rn "t07-kafka-ca-rotation" arch/roadmap/ ; echo "tag-exit=$?" ; \
  test -f arch/roadmap/kafkaworker.md && echo "file-exists" ; \
  tail -n 1 arch/roadmap/kafkaworker.md
```
Expected: `tag-exit=1` без вывода; `file-exists`; последняя строка файла — `## Задачи` (пустой раздел, как `backup.md`).

- [ ] **Step 3: Гейт §8.3 — `t02-external-secret-manager` на месте, t05/t08 не тронуты**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  grep -c "t02-external-secret-manager" arch/roadmap/pgworker.md && \
  grep -n "t05-quarantine-merge\|t08-unify-adminpanel-duplicates" arch/roadmap/pgworker.md && \
  sed -n '7,10p' arch/roadmap/pgworker.md
```
Expected: count = 1; `t05-quarantine-merge` и `t08-unify-adminpanel-duplicates` найдены; вывод строк 7–10 показывает «## Задачи», за которым сразу идёт `- **`t02-external-secret-manager`** —` (новый пункт — первый).

- [ ] **Step 4: Гейт §8.4 — diff с `main` содержит только три roadmap-файла (+ артефакты флоу)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  git diff main...HEAD --stat && echo "--- только roadmap, вне флоу-артефактов:" && \
  git diff main...HEAD --stat -- ':!docs/superpowers'
```
Expected: полный diff — только `arch/roadmap/{pgworker,kafkaworker,README}.md` (+ `docs/superpowers/2026-09-14-t02-per-cluster-secrets/` — spec/plan, артефакты флоу, допустимы по канону dev-flow); diff с исключением `docs/superpowers` — ровно три файла `arch/roadmap/`. Любой файл вне этих путей — стоп, разбираться (правка вне границ §5.4).

- [ ] **Step 5: Гейт чистоты рабочего дерева перед коммитом**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && git status --short
```
Expected: ровно три строки ` M arch/roadmap/pgworker.md`, ` M arch/roadmap/kafkaworker.md`, ` M arch/roadmap/README.md` (плюс ничего сверх; незакоммиченный plan.md, если он ещё не в гейте плана, — сначала закоммитить отдельно от roadmap-коммита, в него не включать).

- [ ] **Step 6: Единственный docs-only коммит (удаление t02 + добавление t02-external-secret-manager — тем же коммитом)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  git add arch/roadmap/pgworker.md arch/roadmap/kafkaworker.md arch/roadmap/README.md && \
  git commit -m "docs(t02): roadmap-чистка — хвост мерж-гейта merge 5bcd467: тег t02-per-cluster-secrets снят из arch/roadmap/pgworker.md, первым пунктом трека добавлена отложенная t02-external-secret-manager (имя зафиксировано merge-сообщением 5bcd467, пункт не был добавлен; слот t02 освобождён тем же коммитом — порядок номеров сохранён); тег t07-kafka-ca-rotation снят из arch/roadmap/kafkaworker.md (CaRotator слит тем же merge 5bcd467: окна двойного доверия, rolling-пересоздание, API /api/kafka/clusters/{C}/ca/rotate, панель+UI; тесты CaRotatorTests/CaRotationTests зелёные), раздел «Задачи» остаётся пустым по прецеденту backup.md; пример тега в arch/roadmap/README.md заменён с несуществующего t02-per-cluster-secrets на живой t05-quarantine-merge"
```
Expected: коммит создан; `git add` — только три явных пути (никаких `git add -A`).

- [ ] **Step 7: Контроль коммита и дерева**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  git show --stat --oneline HEAD | head -n 8 && git status --short && echo "clean"
```
Expected: в коммите ровно три файла `arch/roadmap/*.md`; `git status --short` пуст.

- [ ] **Step 8: Финальный протокол гейтов (свидетельство критериев §8.1–§8.4 после коммита)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/t02-per-cluster-secrets && \
  grep -rn "t02-per-cluster-secrets\|t07-kafka-ca-rotation" arch/roadmap/ ; echo "stale-tags-exit=$?" ; \
  git diff main...HEAD --stat -- ':!docs/superpowers' | tail -n 1
```
Expected: `stale-tags-exit=1` (обоих тегов нет); итоговая строка — `3 files changed` (три roadmap-файла). Далее — обычный dev-flow: ревью и мерж в `main` только по явной просьбе пользователя (AGENTS.base §6).

---

## Соответствие план ↔ spec (self-review)

| Требование spec | Задача/шаг |
|---|---|
| §5.1 pgworker.md: снять t02, добавить t02-external-secret-manager первым | Task 1 (Step 2 — точные old/new) |
| §5.2 kafkaworker.md: снять t07, пустой «## Задачи» | Task 2 (Step 1–2) |
| §5.3 README.md: пример тега → t05-quarantine-merge | Task 3 (Step 1–2) |
| §5.4 границы: ничего сверх трёх файлов | Task 4 Step 4–5 (diff-гейты) |
| §6 фаза 2: один docs-only коммит | Task 4 Step 6 |
| §6 фаза 3: grep-гейты, сборка не требуется | Task 4 Step 1–4 (сборки нет нигде) |
| §8.1–§8.4 критерии приёмки | Task 4 Step 1–4, 8 |
| §3.1/§7: без пометок «закрыта», код/контракты не тронуты | Global Constraints + Task 4 Step 4 |
| §2 решения пользователя (имя t02-external-secret-manager, оба тега одним мерж-гейтом) | Task 1 (имя), Task 1+2+6 (один коммит) |
| §8.5 spec↔arch↔roadmap не расходятся | Аудит уже в spec §4; правки только приближают roadmap к правилу — доп. задач не требуется |

Пробелы покрытия: отсутствуют. Плейсхолдеры: отсутствуют (все правки даны дословно). Расхождения имён: нет (`t02-external-secret-manager`, `t05-quarantine-merge`, `t08-unify-adminpanel-duplicates` сверены с текущими файлами).
