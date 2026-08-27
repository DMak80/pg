# План финализации задачи «Проектирование AdminPanel» (архив documentation-only)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** довести задачу проектирования AdminPanel до конца: верифицировать комплектность и консистентность `arch/`, `arch/roadmap/`, `spec.md` и правки `AGENTS.md`, устранить найденные пробелы и закоммитить все артефакты в ветку `feat-arch-design`.

**Architecture:** задача документационная — кода нет и не появится. План состоит из проверок (каждая — конкретная bash-команда с эталонным ожидаемым результатом), точечных правок по результатам проверок и финального коммита. Источник истины — spec.md; все проверки апеллируют к его разделам.

**Tech Stack:** bash (grep/awk/ls/wc), git. Никакого кода не пишем.

**Spec:** [`docs/superpowers/2026-08-22-arch-design/spec.md`](spec.md) — план аргументирует от спеки; исполнителю читать spec целиком перед началом.

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-arch-design` — все команды ниже выполняются от его корня.

## Global Constraints

Из spec §2, §4, §6 (дословно):

- Deliverables — **только документация**: `arch/*`, `arch/roadmap/*`, `spec.md`, правка `AGENTS.md`. Кода нет; новые файлы вне этих путей не создаём (кроме самого `plan.md`).
- Документация на русском, идентификаторы — английские; **без TBD/TODO/placeholder** (spec §6.6).
- Roadmap: **только несделанные задачи**, тег вида `tNN-slug`, `←`-зависимости; порядок номеров = порядок исполнения (spec §7).
- В коммит **не попадает** `.dev-flow/` — состояние флоу, не артефакт задачи.
- Мерж в `main` — НЕ часть этого плана (за main-агентом); план заканчивается коммитом в `feat-arch-design`.
- Новых проектных решений не изобретать: любое расхождение устраняется приведением к spec/arch, а не наоборот.

---

### Task 1: Комплектность артефактов (все файлы из spec существуют)

**Files:**
- Verify: `arch/README.md`, `arch/01-architecture.md`, `arch/02-etcd-contract.md`, `arch/03-panels.md`, `arch/04-local-stand.md`, `arch/roadmap/README.md`, `arch/roadmap/{infra,etcd,sharding,ha,frontend,stand}.md`, `docs/superpowers/2026-08-22-arch-design/{spec.md,research/puzzle-report.md,research/pg-report.md}`
- Modify: (нет — только при находке пропуска, см. Шаг 3)

**Interfaces:**
- Consumes: spec §3 (список контрактов), §6.1 (критерий приёмки «создан arch/ …»).
- Produces: подтверждённый полный список файлов для Task 6 (коммит).

- [ ] **Шаг 1: Проверить существование всех файлов спеки одной командой**

**Вход:** spec §3 перечисляет: `arch/README.md`, `arch/01-architecture.md`, `arch/02-etcd-contract.md`, `arch/03-panels.md`, `arch/04-local-stand.md`, `arch/roadmap/README.md` + 6 треков; плюс сам `spec.md` и два research-отчёта (упомянуты в spec §1).
**Действие:** выполнить:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && ls -1 \
  arch/README.md arch/01-architecture.md arch/02-etcd-contract.md \
  arch/03-panels.md arch/04-local-stand.md \
  arch/roadmap/README.md arch/roadmap/infra.md arch/roadmap/etcd.md \
  arch/roadmap/sharding.md arch/roadmap/ha.md arch/roadmap/frontend.md \
  arch/roadmap/stand.md \
  docs/superpowers/2026-08-22-arch-design/spec.md \
  docs/superpowers/2026-08-22-arch-design/research/puzzle-report.md \
  docs/superpowers/2026-08-22-arch-design/research/pg-report.md
```
**Выход:** перечень из 15 строк либо ошибка `No such file or directory`.
**Проверка:** команда завершается с кодом 0 и печатает ровно 15 путей; отклонений нет. Если какой-то файл отсутствует — переход к Шагу 3.
**Spec:** §3 (компоненты/контракты), §6.1.

- [ ] **Шаг 2: Сверить git-статус с ожидаемым набором изменений**

**Вход:** worktree, в котором Фаза 1 создала: изменённый `AGENTS.md`, новые `arch/`, `docs/`, служебный `.dev-flow/`.
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && git status --short
```
**Выход:** строки статуса.
**Проверка (критерий):** присутствуют ` M AGENTS.md`, `?? arch/`, `?? docs/` и `?? .dev-flow/`; никаких других изменённых/неотслеживаемых путей нет. Если есть посторонние пути — остановиться и разобраться (не добавлять их в коммит Task 6).
**Spec:** §6.5 (AGENTS.md правится), Global Constraints (`.dev-flow/` не коммитим).

- [ ] **Шаг 3: При пропуске файла — восстановить его из spec**

**Вход:** вывод Шага 1 с ошибкой отсутствия (если её не было — шаг пропустить, отметив «n/a»).
**Действие:** отсутствующий файл создаётся заново по описанию соответствующего раздела spec (§3 — состав, §5 — решения) и структуре соседних документов `arch/`. Содержимое не выдумывать: каждый факт брать из spec §5 или research-отчётов `research/{puzzle-report,pg-report}.md`.
**Выход:** файл существует и непуст.
**Проверка:** повторить команду Шага 1 — код 0, 15 путей.
**Spec:** §3, §5.

---

### Task 2: Консистентность roadmap (теги, зависимости, покрытие)

**Files:**
- Verify: `arch/roadmap/*.md` (7 файлов), сверка с `docs/superpowers/2026-08-22-arch-design/spec.md` §7
- Modify: `arch/roadmap/*.md` (только при расхождении)

**Interfaces:**
- Consumes: spec §6.2 (покрытие тем), §7 (эталон зависимостей), `arch/roadmap/README.md` (правила: тег `tNN-slug`, `←`, удаление тем же мерж-коммитом).
- Produces: верифицированный roadmap, на который указывает AGENTS.md.

**Эталон (из spec §7 — сверивать буквально с этим):**

| Тег | Зависимости |
|---|---|
| `t01-skeleton` | — |
| `t02-auth` | `t01-skeleton` |
| `t03-etcd-snapshot` | `t01-skeleton` |
| `t04-etcd-api` | `t02-auth`, `t03-etcd-snapshot` |
| `t05-sharding-api` | `t04-etcd-api` |
| `t06-ha-api` | `t04-etcd-api` |
| `t07-frontend-base` | `t02-auth`, `t04-etcd-api` |
| `t08-frontend-clusters` | `t05-sharding-api`, `t07-frontend-base` |
| `t09-frontend-ha` | `t06-ha-api`, `t07-frontend-base` |
| `t10-dev-stand` | `t06-ha-api` |
| `t11-finalize` | `t08-frontend-clusters`, `t09-frontend-ha`, `t10-dev-stand` |

- [ ] **Шаг 1: Извлечь фактические теги и проверить их уникальность**

**Вход:** файлы треков; задачи записаны строками вида «- `tNN-slug` — …».
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
grep -rhoE '^- `t[0-9]{2}-[a-z0-9-]+`' arch/roadmap/*.md | sort | uniq -d
```
**Выход:** список дубликатов тегов.
**Проверка (критерий):** вывод **пуст** (все теги уникальны). Непустой вывод = расхождение, чинить в Шаге 4.
**Spec:** §6.1 (теги `tNN-slug`), `arch/roadmap/README.md` (правила).

- [ ] **Шаг 2: Сверить список и количество тегов с эталоном**

**Вход:** эталонная таблица выше (11 задач — spec §6.2, §7).
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
grep -rhoE '^- `t[0-9]{2}-[a-z0-9-]+`' arch/roadmap/*.md | sed 's/^- `//;s/`$//' | sort
```
**Выход:** отсортированный список тегов.
**Проверка (критерий):** ровно 11 строк, в точности: `t01-skeleton, t02-auth, t03-etcd-snapshot, t04-etcd-api, t05-sharding-api, t06-ha-api, t07-frontend-base, t08-frontend-clusters, t09-frontend-ha, t10-dev-stand, t11-finalize`. Любое отличие (лишний/недостающий тег, другой slug) — расхождение, Шаг 4.
**Spec:** §6.2, §7.

- [ ] **Шаг 3: Сверить зависимости `←` с эталоном (ручная сверка по выводу grep)**

**Вход:** зависимости записаны в строках-пунктах задач как «- `tXX-yyy` ← `tAA-bbb`[, `tCC-ddd`]». Важно: тег может встречаться в файле и внутри чужой строки `←`, поэтому сверяются **только строки-пункты** (начинающиеся с `- \``).
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && grep -rn '←' arch/roadmap/*.md
```
и вручную сверить каждую строку-пункт с эталонной таблицей выше (задач 11, зависимостей мало — ручная сверка надёжнее скрипта).
**Выход:** таблица «тег → фактические зависимости».
**Проверка (критерий):** каждая строка-пункт совпадает с эталоном (у `t01` зависимостей нет вообще; у `t11` — три). Дополнительные проверки: (а) ни одна зависимость не указывает вперёд по номеру (нет `← t05…` в t02–t04 и т.п. — порядок = исполнение, spec §7); (б) каждый упоминаемый в `←` тег существует (устанавливается Шагом 2).
**Spec:** §7, `arch/roadmap/README.md`.

- [ ] **Шаг 4: При расхождении — привести roadmap к эталону spec**

**Вход:** вывод Шагов 1–3 с отклонениями (если их не было — шаг пропустить, «n/a»).
**Действие:** правка `arch/roadmap/*.md`: дубликат/лишний тег — удалить пункт; неверная зависимость — заменить на эталонную из таблицы; пропуск зависимости — дописать. Формулировки описаний задач не менять (они из Фазы 1). Обратно-совместимо с правилом «удаление тега тем же мерж-коммитом» — не оставлять упоминаний исправляемого тега в других пунктах, если он удалялся.
**Выход:** изменённые файлы треков.
**Проверка:** повторить Шаги 1–3 — все критерии зелёные.
**Spec:** §6.2, §7.

- [ ] **Шаг 5: Проверить покрытие обязательных тем (по строкам-пунктам, не по упоминаниям)**

**Вход:** spec §6.2 задаёт обязательный состав (скелет; auth; etcd-клиент+снапшот; API инспекции etcd; API шардирования; API HA+алерты; фронтенд ×3; dev-стенд+e2e; финализация). Каждой теме соответствует слаг тега: `skeleton, auth, etcd-snapshot, etcd-api, sharding-api, ha-api, frontend-base, frontend-clusters, frontend-ha, dev-stand, finalize` — 11 слагов, один к одному с темами §6.2.
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && for w in skeleton auth etcd-snapshot etcd-api sharding-api ha-api frontend-base frontend-clusters frontend-ha dev-stand finalize; do printf '%s: ' "$w"; grep -rl "^- \`t[0-9]\{2\}-$w\`" arch/roadmap/ | wc -l; done
```
Анкер `^- \`` (начало строки-пункта) отсекает упоминания тега в чужих `←`-зависимостях, которые требуют spec §7 и которые обязаны встречаться в других файлах.
**Выход:** строка «слаг: количество файлов» на каждый из 11 слагов.
**Проверка (критерий):** каждая строка `: 1` — тема объявлена задачей ровно в одном треке.
**Spec:** §6.2.

---

### Task 3: Консистентность arch-документов (ссылки, числа, модель↔DTO)

**Files:**
- Verify: `arch/*.md`, `arch/roadmap/*.md`
- Modify: конкретный файл, в котором найдено расхождение

**Interfaces:**
- Consumes: spec §5 (решения и числа: тик 3 с, пробы 15 с, UI 5 с; 24 алерта; 11 эндпоинтов; poll вместо watch; gateway `/v3/*`), arch/02 §3 (C#-модель), arch/03 §1–2 (API/DTO).
- Produces: внутренне согласованный контракт arch/.

- [ ] **Шаг 1: Проверить все междокументные markdown-ссылки**

**Вход:** документы ссылаются друг на друга относительными путями (`01-architecture.md`, `../02-etcd-contract.md`, `roadmap/README.md` и т.п.).
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
{ grep -rhoE '\]\([^)#]+\.md' arch/*.md | sed 's/](//' | sort -u | while read -r f; do [ -f "arch/$f" ] || echo "BROKEN(arch): $f"; done; \
  grep -rhoE '\]\([^)#]+\.md' arch/roadmap/*.md | sed 's/](//' | sort -u | while read -r f; do [ -f "arch/roadmap/$f" ] || echo "BROKEN(roadmap): $f"; done; }
```
**Выход:** список битых ссылок (может быть пуст).
**Проверка (критерий):** вывод пуст — все ссылки разрешаются. Каждая `BROKEN`-строка = правка в Шаге 5.
**Spec:** §3 (состав arch/), §6.1.

- [ ] **Шаг 2: Сверить ключевые числа**

**Вход:** числа зафиксированы в spec §5 и должны совпадать во всех упоминаниях в arch/.
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
echo '— интервал refresher (ожидаемо 3 с в 01 §6 и 02 §4):'; grep -n 'RefreshInterval' arch/01-architecture.md arch/02-etcd-contract.md; \
echo '— интервал проб (ожидаемо 15 с в 01 §6 и 02 §4):'; grep -n 'Interval.*15\|15 с' arch/01-architecture.md arch/02-etcd-contract.md; \
echo '— polling UI (ожидаемо 5 с в 01 §5 и 03):'; grep -n 'refetchInterval\|5 с' arch/01-architecture.md arch/03-panels.md; \
echo '— число алертов (24 kind в 03 §4):'; awk -F'|' '/^\| `/ {n=split($2,a,"`"); for(i=1;i<=n;i++) if(a[i] ~ /^[a-z0-9-]+$/ && a[i]!="") c++} END{print c}' arch/03-panels.md; \
echo '— число API-эндпоинтов (11 строк в 03 §1):'; grep -cE '^\| `(GET|POST)' arch/03-panels.md; \
echo '— «24 алертов» в spec:'; grep -n '24 алерт' docs/superpowers/2026-08-22-arch-design/spec.md
```
**Выход:** вывод всех проверок.
**Проверка (критерий):** `RefreshInterval` — 3 с (по умолчанию) в обоих файлах; probes — 15 с; polling — 5 с; awk печатает `24`; grep -c печатает `11`; spec содержит «24 алерт». Примечание для awk: строка `slot-lag-high / slot-wal-lost` несёт два kind — awk это учитывает. При любом несовпадении — Шаг 5.
**Spec:** §5 (решения №2, №5, №6), §6.3, §6.4.

- [ ] **Шаг 3: Сверить эталонный каталог алертов (24 kind)**

**Вход:** каталог arch/03 §4.
**Действие:** прочитать таблицу алертов в `arch/03-panels.md` (§4) и сверить с эталонным списком: `etcd-unreachable, etcd-no-quorum, etcd-endpoint-down, etcd-alarm, snapshot-stale, shard-no-master, shard-no-leader, move-stale, move-frozen-long, move-aborting, move-flipped-status-stuck, bucket-lost, bucket-no-routing, bucket-out-of-range, cluster-incomplete, key-malformed, ha-member-not-streaming, replica-lag-high, slot-lag-high, slot-wal-lost, slot-invalidation-risk, sync-standby-missing, inventory-mismatch, probe-failed`. Дополнительно машинно:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
awk -F'|' '/^\| `/ {n=split($2,a,"`"); for(i=1;i<=n;i++) if(a[i] ~ /^[a-z0-9-]+$/ && a[i]!="") print a[i]}' arch/03-panels.md | sort > /tmp/ap_alerts.txt && wc -l < /tmp/ap_alerts.txt
```
**Выход:** файл `/tmp/ap_alerts.txt` со списком kind.
**Проверка (критерий):** `wc -l` = 24; ручная сверка со списком выше — идентичны (нет переименованных/потерянных). Алерты, упомянутые в задачах roadmap (`t04`: etcd-часть; `t05`: `shard-no-master`, `move-*`, `bucket-*`; `t06`: `shard-no-leader`, `ha-*`, `replica-lag-high`, `slot-*`, `sync-standby-missing`, `inventory-mismatch`, `probe-failed`), существуют в каталоге:
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && for a in shard-no-master shard-no-leader sync-standby-missing inventory-mismatch move-stale bucket-lost; do printf '%s: ' "$a"; grep -c "\`$a\`" arch/03-panels.md; done
```
каждая печать ≥ 1.
**Spec:** §6.4 (каталог покрывает P21/P23), arch/03 §4.

- [ ] **Шаг 4: Сверить модель снапшота (02 §3) с DTO (03 §2)**

**Вход:** arch/02 §3 (C#-records) и arch/03 §2 (DTO).
**Действие:** для каждой пары проверить наличие имени в обоих файлах (camelCase в DTO соответствует PascalCase поля):
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
for pair in 'MasterAddress:masterAddress' 'ReplicasDeclared:replicasDeclared' 'BucketsCount:bucketsCount' 'LeaderName:leaderName' 'OptimeLeader:optimeLeader' 'MoveInfo:move{' 'ShardRuntime:runtime' 'BuiltAtUtc:snapshotAgeMs'; do c="${pair%%:*}"; d="${pair##*:}"; printf '%s→%s: 02=%s 03=%s\n' "$c" "$d" "$(grep -c "$c" arch/02-etcd-contract.md)" "$(grep -c "$d" arch/03-panels.md)"; done
```
**Выход:** строки «поле→поле: 02=N 03=N».
**Проверка (критерий):** в каждой строке оба счётчика ≥ 1 (поля согласованы между моделью и DTO). Пара `BuiltAtUtc→snapshotAgeMs` — производная (возраст), наличие обоих обязательно. Нули = расхождение, Шаг 5.
**Spec:** §6.3 (контракт 02/03 согласованы), arch/02 §3, arch/03 §2.

- [ ] **Шаг 5: Устранить найденные расхождения (ссылки/числа/модель)**

**Вход:** вывод Шагов 1–4 (если все зелёные — шаг «n/a», пропустить).
**Действие:** точечно править тот файл, где расхождение: битая ссылка → правильный относительный путь; неверное число → значение из spec §5 (источник истины — spec, не наоборот); отсутствующее поле модели/DTO → дописать по описанию в spec §5 и соседнему документу. Формат правки — тот же markdown-стиль; русский язык, идентификаторы английские.
**Выход:** правки в arch/*.md.
**Проверка:** повторить Шаги 1–4 этого task'а — все критерии зелёные.
**Spec:** §5, §6.3, §6.4.

---

### Task 4: AGENTS.md и критерии приёмки spec

**Files:**
- Verify: `AGENTS.md`, `docs/superpowers/2026-08-22-arch-design/spec.md` §6
- Modify: `AGENTS.md` (только если остались следы старых треков)

**Interfaces:**
- Consumes: spec §5 (решение №12), §6.5.
- Produces: AGENTS.md, указывающий на реальные треки AdminPanel.

- [ ] **Шаг 1: Убедиться, что шаблонные треки Solana/EVM удалены**

**Вход:** AGENTS.md, раздел «## Roadmap».
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && grep -in 'solana\|evm\|кросс-сетев\|шлюз' AGENTS.md
```
**Выход:** строки-совпадения или пусто.
**Проверка (критерий):** вывод **пуст**. Непустой — Шаг 3.
**Spec:** §5 (№12), §6.5.

- [ ] **Шаг 2: Убедиться, что AGENTS.md указывает на реальные треки и файлы**

**Вход:** раздел «## Roadmap» должен перечислять треки infra/etcd/sharding/ha/frontend/stand и ссылаться на `arch/roadmap/README.md`.
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && grep -o 'infra\|etcd\|sharding\|ha\|frontend\|stand' AGENTS.md | sort | uniq -c && grep -c 'arch/roadmap/README.md' AGENTS.md
```
**Выход:** счётчики треков и число ссылок на roadmap README.
**Проверка (критерий):** каждый из шести треков встречается ≥ 1 раза; `arch/roadmap/README.md` — ≥ 2 ссылок (указатель в теле + в правиле ведения). Все перечисленные в AGENTS.md файлы треков существуют (проверено в Task 1).
**Spec:** §6.5.

- [ ] **Шаг 3: При находке старых следов — дочистить AGENTS.md**

**Вход:** непустой вывод Шага 1 или несоответствие Шага 2 (иначе «n/a»).
**Действие:** заменить остатки текста про Solana/EVM формулировкой про настоящие треки (образец — текущая формулировка раздела «## Roadmap» из Фазы 1: треки infra/etcd/sharding/ha/frontend/stand, теги `tNN-slug`, правило ведения в `arch/roadmap/README.md`).
**Выход:** исправленный AGENTS.md.
**Проверка:** повторить Шаги 1–2 — оба критерия зелёные.
**Spec:** §5 (№12), §6.5.

- [ ] **Шаг 4: Пройти чек-лист критериев приёмки spec §6.1–6.6**

**Вход:** spec §6.
**Действие:** вручную отметить каждый пункт:
- 6.1: arch/ с контрактами 01–04 + roadmap (README + 6 треков, 11 задач с `←`) — подтверждено Task 1 и Task 2;
- 6.2: покрытие тем — подтверждено Task 2 Шаг 5;
- 6.3: контракт 02 покрывает `/clusters/` (config, shards/dsn|replicas|master, routing, status, heals), `/service/` (leader, members, config, optime, initialize), `/cluster/nodes/`, кластерные метаданные (status/member list/alarm) — проверить глазами оглавление 02 (§2.1–2.4);
- 6.4: контракт 03 определяет API/DTO всех панелей и алерты P21 (протухший lease, не-ACTIVE статусы, лаги/safe_wal_size/wal_status, sync-standby) и P23 (routing в никуда) — подтверждено Task 3 Шаги 2–3;
- 6.5: AGENTS.md — подтверждено Шагами 1–2 этого task'а;
- 6.6: русский/английский, без TBD — проверяется в Task 5 Шаге 1.
**Выход:** заполненный чек-лист (в отчёте исполнения).
**Проверка (критерий):** все шесть пунктов «да»; если какой-то «нет» — вернуться к соответствующему task'у.
**Spec:** §6 целиком.

---

### Task 5: Финальный sweep (placeholder'ы, язык, чистота)

**Files:**
- Verify: все `arch/**`, `docs/superpowers/2026-08-22-arch-design/**`, `AGENTS.md`

**Interfaces:**
- Consumes: spec §6.6.
- Produces: чистый набор артефактов, готовый к коммиту.

- [ ] **Шаг 1: Поиск placeholder'ов во всех артефактах**

**Вход:** все документы задачи, включая сам план (в нём слова TBD/TODO встречаются только как запреты — их исключаем из выдачи фильтром ниже).
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
grep -rn 'TBD\|TODO\|FIXME\|XXX\|待定\|заполнить позже\|implement later\|fill in' \
  arch/ AGENTS.md docs/superpowers/2026-08-22-arch-design/spec.md docs/superpowers/2026-08-22-arch-design/plan.md \
  | grep -vE 'новые TBD|без TBD|TBD/TODO|grep -rn'
```
Исключения фильтра — строки, где TBD/TODO упомянуты как запрет или как текст самой команды сканирования (Global Constraints, критерии spec §6.6, формулировки Шагов 3.5/4.4/5.1 этого плана); эти строки планом предусмотрены и дефектом не являются. Реальный placeholder (например, «TBD: доделать») ни под одну альтернативу фильтра не попадает.
**Выход:** список совпадений.
**Проверка (критерий):** вывод пуст. Каждая строка-находка — устранить на месте (дописать конкретику из spec/research) и перезапустить команду до пустого результата.
**Spec:** §6.6.

- [ ] **Шаг 2: Проверить языковое правило на выборке**

**Вход:** правило «документация/комментарии — русский, идентификаторы — английские» (spec §2).
**Действие:** глазами просмотреть заголовки и первые абзацы `arch/01…04`, `arch/roadmap/*`, `AGENTS.md`: тексты на русском; имена тегов `tNN-slug`, kind'ы алертов, пути, C#-идентификаторы — английские/латиница.
**Выход:** заключение.
**Проверка (критерий):** русскоязычные документы с английскими идентификаторами; смешения нет (кириллица внутри идентификаторов — ошибка, исправить).
**Spec:** §2, §6.6.

- [ ] **Шаг 3: Подтвердить, что кодовая база не тронута**

**Вход:** задача documentation-only (spec §2, Global Constraints).
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && git status --short
```
**Выход:** статус.
**Проверка (критерий):** изменения только: `AGENTS.md`, `arch/**`, `docs/**` (+ `.dev-flow/` вне коммита). Никаких `src/`, `frontend/`, `dev-stand/`, `*.cs`, `*.ts`, `package.json` и т.п.
**Spec:** §2, §6.

---

### Task 6: Коммит артефактов в `feat-arch-design`

**Files:**
- Commit: `AGENTS.md`, `arch/`, `docs/superpowers/2026-08-22-arch-design/` (включая `spec.md`, `plan.md`, `research/`)

**Interfaces:**
- Consumes: зелёные результаты Tasks 1–5.
- Produces: коммит в feature-ветке, готовый к ревью и мержу (мерж — за main-агентом).

- [ ] **Шаг 1: Подтвердить ветку**

**Вход:** worktree feat-arch-design.
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && git branch --show-current
```
**Выход:** имя ветки.
**Проверка (критерий):** `feat-arch-design`. Иначе — СТОП, к main-агенту (не переключать ветки самовольно).
**Spec:** контекст задачи (флоу, worktree).

- [ ] **Шаг 2: Добавить артефакты (явными путями, без `-A`)**

**Вход:** чистый набор из Task 5.
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
git add AGENTS.md arch docs/superpowers/2026-08-22-arch-design && git status --short
```
**Выход:** staged-список.
**Проверка (критерий):** staged только файлы этих трёх путей; `.dev-flow/` НЕ staged (остаётся `?? .dev-flow/`).
**Spec:** Global Constraints.

- [ ] **Шаг 3: Коммит**

**Вход:** staged-изменения.
**Действие:**
```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-arch-design && \
git commit -m "docs(arch): спецификация и контракт AdminPanel — arch/, roadmap, spec, AGENTS

- arch/01-04: архитектура, контракт чтения etcd, панели/REST API (24 алерта), dev-стенд
- arch/roadmap/: 6 треков, 11 задач t01-t11 с зависимостями
- docs/superpowers/2026-08-22-arch-design/: spec.md, plan.md, research
- AGENTS.md: настоящие треки roadmap (infra/etcd/sharding/ha/frontend/stand)"
```
**Выход:** коммит.
**Проверка (критерий):** `git log --oneline -1` показывает новый коммит; `git status --short` после — только `?? .dev-flow/`.
**Spec:** контекст задачи (коммит в feature-ветку свободно разрешён).

- [ ] **Шаг 4: Итоговый отчёт исполнителя**

**Вход:** все шаги плана.
**Действие:** собрать краткий отчёт: результаты каждой проверки (зелёное/исправлено), список закоммиченных файлов, хеш коммита.
**Выход:** отчёт main-агенту (мерж в `main` — за ним).
**Проверка (критерий):** отчёт содержит хеш коммита и подтверждение всех критериев spec §6.1–6.6.
**Spec:** §6.

---

## Самопроверка плана (выполнена автором)

- **Покрытие spec:** §6.1→Task 1; §6.2→Task 2; §6.3/§6.4→Task 3; §6.5→Task 4; §6.6→Task 5; контекст (коммит в ветку)→Task 6. Пробелов нет.
- **Placeholder-скан:** шаги содержат конкретные команды и эталонные значения; шаги-заглушки отсутствуют (шаги «n/a» явно обусловлены критерием «если находок нет»).
- **Консистентность имён:** эталоны тегов/зависимостей/24 kind'ов скопированы из финальной spec и arch Фазы 1 дословно.
- **Учёт ревью Фазы 4:** (1) Task 2 Шаг 5 — grep анкерован на начало строки-пункта `^- \``, упоминания в `←` не считаются; (2) Task 5 Шаг 1 — исключение расширено (`новые TBD|без TBD|TBD/TODO|grep -rn`), критерий «пусто» достижим; (3) Task 2 Шаг 3 — сломанная ERE-команда `[^\n]` и неиспользуемые /tmp-файлы убраны, заменено на `grep -rn '←'` + ручную сверку по строкам-пунктам; (4) ссылки уточнены: «11 задач — spec §6.2» (без «8–12»), интервал проб — «01 §6 и 02 §4»; (5) при верификации фикса (1) найдено и исправлено: список тем Шага 5 приведён к 11 полным слагам тегов (`sharding-api`, `ha-api` вместо укороченных `sharding`, `ha`) — прогон даёт 11×`: 1`; обе команды плана (Шаг 1 Task 5 и Шаг 5 Task 2) выполнены против актуальных артефактов — критерии достижимы.
