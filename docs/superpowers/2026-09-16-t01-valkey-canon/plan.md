# t01-valkey-canon: канон Valkey-домена — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Зафиксировать арх-канон Valkey-домена — два новых канон-документа `arch/20-valkey-clusters.md` (контракт etcd `/valkey/` + координация `/valkeyworker/` + клиентский дискавери) и `arch/21-valkeyworker.md` (оркестратор ValkeyWorker), плюс указатели и roadmap-правки (перенос трека + новая задача `t06-valkey-tls`). Без кода, тестов и стенда — вся реализация в t02–t06.

**Architecture:** arch-first по образцу задачи t01-backup-canon: сначала предусловие — перенос незакоммиченных `arch/roadmap/valkey.md` + `arch/roadmap/README.md` из рабочей копии главного репозитория в ветку (снапшот-коммит), затем два канона по структуре эталонов `arch/15-kafka-clusters.md` / `arch/16-kafkaworker.md` (содержание — дословная разработка spec §3/§4: формулировки kafka-домена переносятся 1:1, отличия standalone-кеша фиксируются явно), затем указатели (`arch/README.md`, шапка трека) и новая задача `t06-valkey-tls`. Финал — гейт ревью пользователем и roadmap-мерж-гейт (снятие тега `t01-valkey-canon`).

**Tech Stack:** только Markdown-документация (`arch/`), git (ветка `docs-t01-valkey-canon` в worktree). Никакого кода, сборок, docker и тестов — проверки шагов текстовые (grep/diff) и линк-чеки markdown-ссылок.

**Spec:** [`docs/superpowers/2026-09-16-t01-valkey-canon/spec.md`](spec.md) — план аргументирует от spec; исполнитель читает BOTH (spec даёт полное содержание §3/§4, план — порядок и критерии).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon`, ветка `docs-t01-valkey-canon` (создана от кончика `main` ec920fd; untracked — только каталог этой задачи). Главный репозиторий `/Users/demakaev/ZCodeProject/pg` — источник переноса, НЕ редактируется.

## Global Constraints

(Из spec §2/§7, AGENTS.md, AGENTS.base.md — действуют для каждой задачи.)

- **t01 — только канон-документы**: никакой реализации ValkeyWorker/кода/тестов/deploy/dev-стенда (t02), панели и правок `arch/adminpanel/*` (t03), библиотеки Puzzle (t04), метрик (t05), TLS (t06), зеркалирования образа/`images.txt` (t02).
- **Изменения только в** `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `arch/README.md`, `arch/roadmap/valkey.md`, `arch/roadmap/README.md`, `docs/superpowers/2026-09-16-t01-valkey-canon/` — больше ничего (критерий приёмки spec §8.6). Другие каноны не трогаем.
- **Рабочая копия главного репозитория (`/Users/demakaev/ZCodeProject/pg`) не редактируется** — из неё только читаем два файла для снапшота (Task 1).
- **Образец-канон**: где Valkey-домен совпадает с kafka (state-семантика, координация, txn-протоколы, толерантность читателей) — формулировки переносятся 1:1 из arch/15/16; отличия (standalone, ACL-креды, кеш восполним, без TLS v1) — фиксируются явно (spec §2.2).
- **Решения пользователя** (spec §1, обязательны в канонах): nodes-поддерево с `nodes=1`; ACL-модель кред `admin`/`app` с ensure и ротацией без рестартов через окно двух паролей; без TLS в v1 + roadmap-пункт `t06-valkey-tls`.
- **Язык**: документация — русский, идентификаторы — английские; стиль и тон — окружающих arch/15/16 (те же обороты, та же ширина строк ~78–80, те же форматы таблиц).
- **Префиксы `/valkey/` и `/valkeyworker/` пишет ТОЛЬКО ValkeyWorker** (панель и сиды — через его HTTP API); порты 17000–17999; имена контейнеров `vwk-<C>-node<k>`; образ `valkey/valkey:<пин>` (spec §1).
- **Коммиты**: в feature-ветке после каждой задачи (свободно, AGENTS.base п.6); мерж в `main` — только по явной просьбе пользователя.
- **Мерж-гейт roadmap**: тег `t01-valkey-canon` удаляется из `arch/roadmap/valkey.md` (пункт + `←`-зависимости t02–t06) тем же мерж-коммитом (Task 6).
- **Порядок grep-чеков «t01-valkey-canon»**: до Task 6 упоминание `t01-valkey-canon` в `arch/roadmap/valkey.md` — НОРМА (пункт задачи и зависимости t02/t03/t04/t06); полный чек «нет упоминаний по `arch/roadmap/`» — только в Task 6 после снятия.

---

### Task 1: Перенос roadmap-трека в ветку (снапшот, предусловие)

**Вход (предусловие):** ветка `docs-t01-valkey-canon` чистая (untracked — только `docs/superpowers/2026-09-16-t01-valkey-canon/`); в главном репозитории `/Users/demakaev/ZCodeProject/pg` существуют незакоммиченные `arch/roadmap/valkey.md` (untracked, 47 строк, задачи t01–t05) и модификация `arch/roadmap/README.md` (ровно +1 строка — valkey в таблице треков); в ветке обоих файлов нет.

**Действие (файлы/изменения):** скопировать байт-в-байт текущее содержимое обоих файлов из рабочей копии главного репозитория в worktree и закоммитить снапшот — БЕЗ каких-либо правок поверх (шапка-ссылка и t06 — Task 4, не здесь).

**Files:**
- Create: `arch/roadmap/valkey.md` (копия `/Users/demakaev/ZCodeProject/pg/arch/roadmap/valkey.md`)
- Modify: `arch/roadmap/README.md` (заменить версией из `/Users/demakaev/ZCodeProject/pg/arch/roadmap/README.md`)

**Interfaces:**
- Consumes: рабочая копия repo_root (только чтение).
- Produces: объект roadmap-правок для Task 4 (шапка трека, t06) и Task 6 (мерж-гейт): в ветке теперь есть `arch/roadmap/valkey.md` с пунктами t01–t05 и `arch/roadmap/README.md` со строкой valkey в таблице треков.

- [ ] **Step 1: Проверить предусловие** — в ветке файлов нет, в repo_root есть:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
  git status --short && ls arch/roadmap/ && \
  git -C /Users/demakaev/ZCodeProject/pg status --short arch/roadmap/
```

Expected: worktree — только `?? docs/superpowers/2026-09-16-t01-valkey-canon/`, в `arch/roadmap/` НЕТ `valkey.md`; repo_root — ` M arch/roadmap/README.md` и `?? arch/roadmap/valkey.md`.

- [ ] **Step 2: Скопировать оба файла байт-в-байт**

```bash
cp /Users/demakaev/ZCodeProject/pg/arch/roadmap/valkey.md \
   /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon/arch/roadmap/valkey.md && \
cp /Users/demakaev/ZCodeProject/pg/arch/roadmap/README.md \
   /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon/arch/roadmap/README.md
```

- [ ] **Step 3: Проверить идентичность и отсутствие других расхождений трека**

```bash
diff /Users/demakaev/ZCodeProject/pg/arch/roadmap/valkey.md \
     /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon/arch/roadmap/valkey.md && \
diff /Users/demakaev/ZCodeProject/pg/arch/roadmap/README.md \
     /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon/arch/roadmap/README.md && \
for f in backup.md kafkaworker.md pgworker.md; do \
  diff /Users/demakaev/ZCodeProject/pg/arch/roadmap/$f \
       /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon/arch/roadmap/$f || exit 1; done && echo OK
```

Expected: `OK` (обе копии идентичны; остальные файлы трека не разошлись).

- [ ] **Step 4: Коммит снапшота** (только эти два файла, spec-каталог не трогаем):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git add arch/roadmap/valkey.md arch/roadmap/README.md && \
git commit -m "docs(t01): перенос valkey-трека roadmap из рабочей копии главного репозитория (снапшот valkey.md + строка трека в README), байт-в-байт, без правок — предусловие t01 (spec §5.1)"
```

**Выход:** в ветке существуют `arch/roadmap/valkey.md` (t01–t05) и `arch/roadmap/README.md` со строкой valkey; снапшот зафиксирован коммитом.

**Проверка (критерий достижения цели шага):**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git show --stat HEAD | grep -E "arch/roadmap/(valkey|README).md" && \
grep -c "valkey.md" arch/roadmap/README.md && grep -c "t0[1-5]-valkey" arch/roadmap/valkey.md
```

Expected: коммит содержит ровно 2 файла; `grep -c "valkey.md" arch/roadmap/README.md` = 1; `grep -c "t0[1-5]-valkey" arch/roadmap/valkey.md` = 5 (пункты t01–t05, каждый на своей строке).

**Связь со spec:** §5.1 (предусловие), §6 фаза 1, критерий приёмки §8.4.

---

### Task 2: Канон `arch/20-valkey-clusters.md`

**Вход (предусловие):** Task 1 закоммичен; номера 20/21 в `arch/` свободны (`ls arch/ | grep "^2[01]-"` — пусто); исполнитель ПРОЧИТАЛ эталоны `arch/15-kafka-clusters.md` (структура: шапка+имена → §1 транспорт → §2 таблица ключей → §2.1 примеры → §4 координация → §5 дискавери → §6 сбои) и `arch/16-kafkaworker.md` (на него ссылаемся), spec §3.

**Действие (файлы/изменения):** создать `arch/20-valkey-clusters.md` — контракт Valkey-домена по структуре arch/15. Содержание — развёртка spec §3 в связный канонный текст (как план → канон в t01-backup-canon): формулировки kafka-домена переносятся из 15 буквально, valkey-отличия фиксируются явно. Без ссылок на spec/план — канон самодостаточен (на него будут ссылаться t02–t06).

**Files:**
- Create: `arch/20-valkey-clusters.md`

**Interfaces:**
- Consumes: spec §3.1–§3.6; эталоны arch/15 §1/§2/§2.1/§4/§5/§6 (формулировки), arch/17 (S-принципы по ссылкам), arch/11 §2 (аналогия pg-дискавери).
- Produces: канон, на который ссылаются `arch/21` (Task 3), указатели (Task 4) и задачи t02–t06. Номера разделов — предмет ссылок: §1 транспорт, §2 ключи кластера, §2.1 канонические примеры, §3 координация `/valkeyworker/`, §4 клиентский дискавери, §5 обработка сбоев. (Нумерация НЕ как в 15: у valkey нет топикового §3, поэтому координация — §3, дискавери — §4, сбои — §5.)

**Обязательная структура документа** (заголовок + разделы; тезисы — материал развёртки, всё из spec §3, ничего не добавлять от себя):

```markdown
# 20. Valkey-кластеры: контракт etcd (контроль-плейн + дискавери) ★

Шапка-аннотация (по образцу 15, до §1): канон ключей Valkey-домена:
контроль-плейн кластеров `/valkey/` (декларирует панель AdminPanel через
HTTP API воркера — исполняет ValkeyWorker, канон
[21-valkeyworker.md](21-valkeyworker.md)), координация воркера
`/valkeyworker/` и **клиентский дискавери** (приложение читает
endpoints/ACL-креды исключительно отсюда — аналогия pg
dsn/`app_password` и kafka endpoints,
[11-bucket-sharding.md](11-bucket-sharding.md) §2). Назначение домена —
**разделяемый кеш набора инстансов приложения**: топология standalone,
`nodes=1` (реплики/sentinel/cluster — вне канона,
[roadmap/valkey.md](roadmap/valkey.md)).

Имена (как 15): кластер `<C>` — `^[[a-z][a-z0-9_]{0,62}$` (как pg/kafka;
без дефиса); ноды `node1..nodeN` (имя генерирует панель, `node<max+1>`,
≤ 9); в v1 всегда `nodes=1` — нода `node1`.

## 1. Транспорт: HTTP JSON gateway `/v3/*`
— Спецификация §3.1, дословная адаптация 15 §1: HttpClient против
gRPC-gateway etcd (JSON+base64), POST /v3/kv/range|put|txn, /v3/lease/*;
один общий etcd-кластер со стендом (ссылки adminpanel/02 §1, arch/14 §3 —
как в 15). Poll, без watch — тик воркера 5 с / панели 3 с. Два новых
корневых префикса: /valkey/ и /valkeyworker/ (панель читает
избирательно — §3).

## 2. Ключи кластера `/valkey/clusters/<C>/`
— Таблица 4 колонки (Ключ | Формат значения | Пишет | Примечание) —
8 строк из spec §3.2: config / nodes/node<k>/state /
nodes/node<k>/resources / endpoints / app_user / app_password /
admin_user / admin_password. Формулировки колонок — стиль 15 §2:
— `config`: JSON {"nodes":N,"maxmemory_bytes":M,"maxmemory_policy":P,
  "created_unix":T,"state"?}; state — только у невыполненных заявок,
  отсутствие = Active (семантика pg/kafka); nodes фиксируется при
  создании (=1 в v1, реплики — roadmap); maxmemory_policy — 8 значений
  Valkey (перечислить все), канон-дефолт allkeys-lru; оба поля —
  mutable-конфиги (converge без рестартов).
— `nodes/node<k>/state`: 6 состояний; NOT_INITIALIZED/TO_REMOVE — только
  панель (one-way), остальные — воркер; TO_REMOVE — маркер демонтажа
  ноды; в v1 (nodes=1) демонтаж ноды = демонтаж кластера через
  config.state=TO_REMOVE.
— `nodes/node<k>/resources`: {"cpu","mem","disk"}; disk — инфо-поле;
  инвариант валидации maxmemory_bytes < mem-лимит (риск R3 arch/21).
— `endpoints`: "h1:p1,h2:p2,..."; воркер, RMW; точка дискавери клиентов.
— креды app/admin: формат, кто пишет (ensure txn put-if-absent /
  ротация), права ACL (app: ~* +@read +@write; admin: +@all), в UI/API
  панели не отдаются.
— Абзац после таблицы: неизвестные ключи внутри /valkey/ — не ошибка,
  лог + счётчик unknownKeys (как pg/kafka).
— Подраздел «Отличия от kafka-таблицы [15](15-kafka-clusters.md) §2»
  (3 пункта, явно): нет role (standalone, нет ролей нод); нет
  ca_pem/ca_key/ca_next_* (TLS — t06-valkey-tls, после t06 добавятся
  ca_pem/ca_key); нет topics/ (Valkey ключи данных хранит сам,
  контроль-плейн не реестрирует).

### 2.1. Канонические примеры значений (критерий приёмки парсеров)
— Спецификация §3.3, формат 15 §2.1: три JSON config (заявка
  NOT_INITIALIZED; Active без state; заявка удаления TO_REMOVE) +
  endpoints "host.docker.internal:17001" (advertised-правило 21 §2,
  порт из /valkeyworker/portalloc/<C>) + app_user "app" /
  admin_user "admin" / пароли 32 симв [A-Za-z0-9].

## 3. Координация воркера `/valkeyworker/`
— Вводная: порт схемы /kafkaworker/ (15 §4) 1:1, свой префикс.
— Таблица 8 ключей из spec §3.4: leader / claims/<C> / work/<C> /
  portalloc/<C> / locks/portalloc / instances/<id> / api/<id> /
  rotations/<C>. Форматы значений полностью из spec §3.4 (вкл. JSON
  work-журнала, portalloc-мэппинг node<k>:{host,client}, api-JSON
  {url,instance,since_unix,cert_thumbprint?}, rotations-JSON
  {role,requested_unix,requested_by}).
— Абзац про rotations: ОДИН ключ с полем role (не два, как у kafka —
  там разделение наследие JAAS; здесь ротация без рестартов единая для
  обеих ролей); клэйм-txn version==0, del воркером по завершении или
  панелью (отмена).
— Абзац: панель читает из /valkeyworker/ ТОЛЬКО rotations/ (реализация
  t03); остальное не читает и не пишет.
— Абзац: реализация — переиспользование Shared.Etcd (ClaimStore /
  PortAllocLock / WorkJournal, keyPrefix="/valkeyworker"; t02, новый
  код не пишется).

## 4. Клиентский дискавери (приложения)
— Спецификация §3.5, формат 15 §5: приложение читает из etcd и только
  из него: (1) endpoints → адреса; (2) app_user + app_password →
  ACL-креды; (3) config → только state (raw-строка, отсутствие =
  Active). GetClientConfig() внешней библиотеки (t04, образец HA.Kafka
  docs/01.19-ha-kafka.md в Puzzle) — plain-поля для
  StackExchange.Redis: endpoints/username/password/ssl=false;
  неполный набор кредов → App = null → GetClientConfig() = null.
— Абзац про TLS: в v1 отсутствует осознанно (доверенная закрытая
  docker-сеть + ACL-креды); шифрование/аутентификация сервера —
  t06-valkey-tls; контракт при этом не меняется — добавится только
  ca_pem (обратная совместимость читателя).

## 5. Обработка сбоев (толерантность читателей)
— Таблица 6 строк из spec §3.6 (порт 15 §6 один в один): битый JSON →
  parseError + warning valkey-key-malformed; неизвестный ключ →
  unknownKeys; Active без endpoints → критический
  valkey-endpoints-missing; неполные креды → null; незнакомое
  config.state → Active-ветка с raw-строкой; пустой endpoints →
  отсутствующий (тот же алерт).
```

- [ ] **Step 1: Прочитать эталоны** — `arch/15-kafka-clusters.md` целиком, `arch/16-kafkaworker.md` шапку+§2 (для корректных ссылок), spec §3. Критерий входа: понимаешь структуру 15 и все термины spec.

- [ ] **Step 2: Написать документ** — создать `arch/20-valkey-clusters.md` по обязательной структуре выше (все разделы, все 8 строк таблицы §2, все 8 строк таблицы §3, все примеры §2.1, все 6 строк таблицы §5). Стиль/ширина строк — как 15.

- [ ] **Step 3: Структурная самопроверка разделов**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
grep -n '^#\{1,3\} ' arch/20-valkey-clusters.md
```

Expected: ровно заголовок + §1, §2, §2.1, §3, §4, §5 (6 заголовков `##`/`###` после титульного, нумерация 1–5 как в Interfaces).

- [ ] **Step 4: Чек полноты ключевых сущностей (критерий приёмки spec §8.1)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
for t in maxmemory_bytes maxmemory_policy allkeys-lru noeviction \
         nodes/node<k>/state nodes/node<k>/resources endpoints app_password admin_password \
         '/valkeyworker/rotations' '/valkeyworker/portalloc' '/valkeyworker/locks/portalloc' \
         '"role"' GetClientConfig ssl=false valkey-key-malformed valkey-endpoints-missing \
         unknownKeys parseError t06-valkey-tls host.docker.internal 17001; do \
  grep -qF "$t" arch/20-valkey-clusters.md && echo "OK: $t" || echo "MISSING: $t"; done
```

Expected: все строки `OK`, ни одной `MISSING`.

- [ ] **Step 5: Линк-чек относительных ссылок**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
grep -oE '\]\([^)#]+\.md' arch/20-valkey-clusters.md | sed 's/](//' | sort -u | \
while read -r t; do [ -f "arch/$t" ] || echo "BROKEN: $t"; done; echo DONE
```

Expected: `DONE` без строк BROKEN (файл лежит в `arch/`, цели вида `21-valkeyworker.md` появятся после Task 3 — если ссылка на 21 уже стоит, BROKEN на этом шаге допустим только для `21-valkeyworker.md` и закрывается Task 3; проверь повторно в Task 5).

- [ ] **Step 6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git add arch/20-valkey-clusters.md && \
git commit -m "docs(t01): канон arch/20-valkey-clusters.md — контракт etcd /valkey/ (контроль-плейн + ACL-креды), координация /valkeyworker/, клиентский дискавери, толерантность читателей (по структуре arch/15)"
```

**Выход:** канон `arch/20-valkey-clusters.md` существует, покрывает spec §3 полностью, структурно и стилистически соответствует arch/15.

**Проверка:** Steps 3–5 зелёные; дополнительно контрольный смысловой чек — таблица §2 содержит ровно 8 строк-ключей и подраздел отличий от kafka с 3 пунктами:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
grep -c '^| `' arch/20-valkey-clusters.md
```

Expected: 8 (таблица §2) + 8 (таблица §3) + 6 (таблица §5) = 22 (если в §2/§3 формат строки начинается с `| \``; иначе сверь вручную по подсчёту строк таблиц).

**Связь со spec:** цель §1.1, содержание §3 (§3.1–§3.6), решения пользователя §1 (nodes=1, ACL, без TLS), критерии приёмки §8.1, §8.5.

---

### Task 3: Канон `arch/21-valkeyworker.md`

**Вход (предусловие):** Task 2 закоммичен (ссылки 20 ↔ 21 стыкуются); номер 21 свободен; исполнитель ПРОЧИТАЛ `arch/16-kafkaworker.md` целиком (структура-эталон: шапка → §1 роль → §1.1 HTTP API → §2 модель размещения → §3 контракт etcd (§3.1 читаемые/§3.2 пишемые) → §4 секреты → §5 процессы A–… → §6 надёжность → §7 наблюдаемость → §8 конфигурация → §9 риски) и spec §4.

**Действие (файлы/изменения):** создать `arch/21-valkeyworker.md` — канон оркестратора по структуре arch/16. Содержание — развёртка spec §4; таблицы контракта — зеркало arch/20 (Task 2) в формате 16 §3.

**Files:**
- Create: `arch/21-valkeyworker.md`

**Interfaces:**
- Consumes: spec §4.1–§4.9; arch/20 (Task 2 — зеркало контракта); arch/16 §1.1/§2/§3/§4/§5(H)/§7/§8/§9 (формулировки и форматы); `arch/adminpanel/02-etcd-contract.md` §9.9 (ключ `/workers/api_tls/*`); arch/18 §2.2 (метрики), arch/17 (S5/S6/S7, E9).
- Produces: канон-заготовка для t02 (реализация), t03 (панель), t05 (метрики), t06 (TLS). Разделы: §1 роль/границы (+§1.1 HTTP API), §2 модель размещения, §3 контракт etcd (§3.1/§3.2), §4 секреты, §5 процессы A–E, §6 надёжность, §7 наблюдаемость, §8 конфигурация, §9 риски R1–R7.

**Обязательная структура документа** (тезисы — материал развёртки из spec §4, ничего не добавлять):

```markdown
# 21. ValkeyWorker: оркестратор Valkey-кластеров ★

Шапка (по образцу 16, до §1): **ValkeyWorker** — фоновый сервис (.NET 10),
исполнительная сторона декларативного контракта
[20-valkey-clusters.md](20-valkey-clusters.md): панель заявляет кластер
через HTTP API воркера (§1.1; state=NOT_INITIALIZED) — воркер поднимает
standalone-контейнер Valkey, обеспечивает per-cluster ACL-креды, пишет
факт и снимает state; TO_REMOVE — полный демонтаж. Ответственность
изменений etcd: /valkey/, /valkeyworker/ пишет ТОЛЬКО ValkeyWorker.

Пять процессов: 1. Provisioning (A, V0–V5); 2. Deprovisioning (B,
X0–X3); 3. NodeSupervisor (C, надзор); 4. ConfigConverger (D);
5. PasswordRotator (E). Свойства: несколько инстансов одновременно
(lease-клэймы 20 §3); takeover ≤ TTL 15 с + тик; идемпотентность;
значимое состояние переживает смерть контроллера (etcd).

Границы (что НЕ входит): TLS клиентских подключений (t06-valkey-tls —
per-cluster CA, tls-port, ключ ca_pem); реплики/sentinel/cluster;
коллектор доменных метрик INFO и дашборд (t05, arch/18 §2.2/§4);
панель valkey-домена (t03); клиентская библиотека Puzzle (t04);
persistence RDB/AOF — off (кеш восполним); квоты томов — томов нет.

## 1. Роль в системе и разделение ответственности
— Спецификация §4.1: развёртка шапки (роль, ответственность изменений,
схема взаимодействия панель→API→воркер→docker/etcd в стиле 16 §1).

### 1.1. HTTP API воркера (мутации панели, сиды)
— Кратко по образцу 16 §1.1: mTLS-грань по канону воркеров (t03),
ключ /valkeyworker/api/<id> (ставит сам инстанс, формат — 20 §3);
панель — декларатор через HTTP API, etcd домена не пишет; сиды
(EnableSeedEndpoint) — паттерн 16 §1.1. Детальные контракты
эндпоинтов — t03 (в t01 фиксируется только грань и её transport).

## 2. Модель размещения
— Спецификация §4.2 (9 пунктов, каждый — как подпункт/абзац в стиле
16 §2): образ valkey/valkey:<пин> (Images:Node, без кастомного образа;
зеркало 192.168.0.1:5000 — t02, ранбук); нода = контейнер
vwk-<C>-node<k>, restart unless-stopped; ACL при старте — аргументы
--user default off --user admin on ><пароль> ~* +@all --user app on
><пароль> ~* +@read +@write (ротация на живой ноде — ACL SETUSER,
без пересоздания); maxmemory аргументами + converge; persistence off
(без volume, --save ""/--appendonly no — детали t02; потеря контейнера
= холодный старт кеша, документированное поведение); per-cluster сеть
НЕ создаётся (нет inter-node трафика, клиентский доступ — host-порт;
домен не порождает *-net-* объектов — урок инцидента t05 про kfw-net);
порты 17000–17999 (1 клиентский порт на
ноду, 6379→host-порт, закрепление portalloc; занятость = docker-публикации
∪ portalloc всех чужих кластеров; довыделение под locks/portalloc,
t90-паттерн); advertised-правило (16 §2.1):
ValkeyWorker:AdvertisedClientHost, иначе имя docker-хоста размещения;
обязан резолвиться клиентами, стенды — host.docker.internal; лимиты
из resources (cpu/mem, disk — инфо; инвариант maxmemory_bytes <
mem-лимит); сам воркер — контейнер с docker.sock, deploy по образцу
KafkaWorker (сборка — t02).

## 3. Контракт etcd
— Вводная: зеркало [20](20-valkey-clusters.md) §2/§3, формат
16 §3. Смежный ключ вне префикса — /workers/api_tls/valkeyworker
(серверный серт mTLS-грани; пишет ТОЛЬКО панель, воркер читает при
старте — adminpanel/02 §9.9; применяется с t02).

### 3.1. Читаемые ключи
— Таблица: config (целиком, вкл. state-заявки), nodes/node<k>/state
(только NOT_INITIALIZED/TO_REMOVE от панели — они же пишутся панелью
через API), nodes/node<k>/resources, /valkeyworker/rotations/<C>,
/workers/api_tls/valkeyworker.

### 3.2. Пишемые ключи
— Таблица: nodes/node<k>/state (PROVISIONING/RUNNING/UNREACHABLE/
REMOVING), endpoints (RMW), app_user/app_password/admin_user/
admin_password (ensure + ротация), config без state (txn по
mod_revision), координация /valkeyworker/* (claims, work, portalloc,
locks, instances, api), del --prefix /valkey/clusters/<C>/ + del
/valkeyworker/{claims,work,portalloc,rotations}/<C>* при демонтаже.

## 4. Секреты
— Спецификация §4.4: per-cluster в etcd, генерирует воркер (ensure txn
put-if-absent): app ("app", 32 симв) — приложения; admin ("admin",
32 симв) — воркер/панель-пробы; ротация — процесс E. Env-секреты
per-install — только TLS HTTP API: VWK_API_TLS_{CERT,KEY,CLIENT_CA}
(паттерн 16 §4); зона доверия контроль-плейна, парольная защита кеша
достаточна для домашнего контура, TLS-транспорт — t06.

## 5. Процессы (машины состояний)
— Состояния ноды: NOT_INITIALIZED → PROVISIONING → RUNNING;
UNREACHABLE; REMOVING; TO_REMOVE (one-way). Классификация тика:
config.state=NOT_INITIALIZED → A; TO_REMOVE → B; иначе Active: C →
D → E; всё под живым клэймом <C>; journal-before-manipulations;
без kafka-специфики (нет премиграций — домен рождается с ACL-каноном).

### A. ProvisioningProcess (V0–V5)
— Спецификация §4.5 A: V0 claim+journal; V1 план+порт-аллокация под
locks/portalloc (не взял → waiting-portalloc-lock, следующий тик);
V2 ensure секретов (проигрыш txn → re-read); V3 контейнер (аргументы
ACL/maxmemory, лимиты, host-порт) + state=PROVISIONING; существующий
re-run — сверка и пропуск; V4 PING с admin-кредом (бюджет NodeBootSec,
транзиент-толерантно) → RUNNING; V5 endpoints + txn config без state;
journal done; гонка TO_REMOVE — перечитывание config перед фазами.

### B. DeprovisioningProcess (X0–X3)
— X0 claim+journal; X1 docker: контейнер vwk-<C>-* (404 = ок; сначала
docker, потом etcd; томов нет); X2 etcd: del --prefix /valkey/... +
очистка координации ВКЛЮЧАЯ заявки ротаций; X3 снятие клэйма
(del + revoke lease).

### C. NodeSupervisor (надзор)
— Сверка декларации с docker-фактом + PING-проба (admin-кред):
снесённый контейнер (docker-факт) → пересоздание; молчание дольше
NodeDeadSec → UNREACHABLE + пересоздание без тома (данных не жалко);
слепая проба — никаких действий (S7); одно пересоздание за тик;
кеш неприкосновенен на etcd-уровне (надзор не чистит ключи домена);
лестница E9: нода без portalloc → реконструкция из inspect
(published-порт+host, put-if-absent под locks/portalloc) → новая
аллокация.

### D. ConfigConverger
— Active-ветка: CONFIG GET maxmemory/maxmemory-policy vs декларация →
CONFIG SET без рестартов (маппинг maxmemory_bytes→maxmemory,
maxmemory_policy→maxmemory-policy); ACL-план: ACL LIST vs канон
(admin/app, default off) → идемпотентный ACL SETUSER-converge;
расхождение maxmemory_bytes ≥ mem-лимита → journal-warning
(ответственность оператора, образец R7 arch/16).

### E. PasswordRotator (окно двух паролей, без рестартов; роли app|admin)
— Заявка /valkeyworker/rotations/<C> (role); NEW = 32 симв:

E1 ACL SETUSER <role> >NEW    — оба пароля (OLD+NEW) валидны
E2 ОДНА txn: [compare value(<role>_password)==OLD][put NEW; del заявки]
E3 ACL SETUSER <role> <OLD    — удаление старого пароля

— Отказ между фазами безопасен (повтор тика доигрывает); отличие от
kafka 16 §5 H — без пересозданий (ACL runtime); пересоздание надзором
в окне безопасно (аргументы из etcd-актуальных кредов, E1 уже
закоммитил NEW); роли изолированы; битая заявка — del с journal
(панель до того получает 409).

## 6. Надёжность
— Спецификация §4.6: идемпотентность (каждый шаг перепроверяет факт);
takeover (состояние в etcd; двойной контроллер невозможен); атомарность
txn (mod_revision, version==0, RMW); снапшоты /valkey/+/valkeyworker/
лидером регулярно + до/после prov/deprov (SnapshotJob Shared.Etcd,
retention); Polly jitter; отказ etcd — контроль-плейн заморожен, живые
ноды и клиенты (fail-open) не страдают.

## 7. Наблюдаемость
— Спецификация §4.7: /healthz честный (t09-канон: последнее состояние
цикла, структура всегда); секции etcd-reachable, docker-hosts,
loops-alive, claims, snapshot-freshness; SocketsHttpHandler +
IPv4-first (16 §7); панель опрашивает /healthz по URL из api/<id>;
метрики — каркас arch/18 §2.2 (ValkeyWorker в Meter-именах),
коллектор INFO — t05; diag-ключи: work/<C>, nodes/node<k>/state.

## 8. Конфигурация (appsettings + env-оверрайды)
— Блок кода из spec §4.8 ДОСЛОВНО (ValkeyWorker:Etcd/Docker{PortRange
17000-17999, Images:Node}/Loops/Thresholds{NodeBootSec=120,
NodeDeadSec=90}/Parallelism/Snapshots/AdvertisedClientHost/Api{Tls} +
строка про env VWK_API_TLS_{CERT,KEY,CLIENT_CA} в стиле 16 §8).

## 9. Риски
— Таблица R1–R7 из spec §4.9 дословно (образ: | # | Риск | Митигация |,
7 строк: R1 сторонний образ, R2 порт-коллизии, R3 maxmemory ≥ mem →
OOM, R4 отказ между E1–E3, R5 без TLS, R6 потеря кеша, R7 пароли в
etcd).
```

- [ ] **Step 1: Прочитать эталоны** — `arch/16-kafkaworker.md` целиком (особенно §1.1, §2, §3, §5 H, §8, §9), `arch/adminpanel/02-etcd-contract.md` §9.9, spec §4.

- [ ] **Step 2: Написать документ** — создать `arch/21-valkeyworker.md` по обязательной структуре (все 9 разделов + §1.1, §3.1/§3.2, процессы A–E с фазами V0–V5/X0–X3/E1–E3, конфиг-блок, таблица рисков R1–R7).

- [ ] **Step 3: Структурная самопроверка разделов**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
grep -n '^#\{1,3\} ' arch/21-valkeyworker.md
```

Expected: титул + §1, §1.1, §2, §3, §3.1, §3.2, §4, §5, A–E, §6, §7, §8, §9 (соответствие списку Interfaces).

- [ ] **Step 4: Чек полноты ключевых сущностей (критерий приёмки spec §8.2)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
for t in V0 V5 X0 X3 E1 E3 vwk- 'valkey/valkey' 17000 17999 \
         ACL\ SETUSER allkeys-lru NodeBootSec NodeDeadSec NodeSupervisor ConfigConverger \
         PasswordRotator Provisioning Deprovisioning maxmemory_bytes \
         /workers/api_tls/valkeyworker VWK_API_TLS_ host.docker.internal \
         AdvertisedClientHost unless-stopped put-if-absent mod_revision \
         SnapshotJob R7 t02 t03 t04 t05 t06; do \
  grep -qF "$t" arch/21-valkeyworker.md && echo "OK: $t" || echo "MISSING: $t"; done
```

Expected: все `OK`, ни одной `MISSING`.

- [ ] **Step 5: Зеркальность с arch/20** — каждый ключ из 20 §2/§3 упомянут в §3.1/§3.2 с корректной стороной (читает/пишет); сверка вручную по таблицам двух канонов (расхождение = ошибка).

- [ ] **Step 6: Линк-чек обоих канонов** (ссылка 20→21 теперь обязана резолвиться):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
for f in arch/20-valkey-clusters.md arch/21-valkeyworker.md; do \
  grep -oE '\]\([^)#]+\.md' "$f" | sed 's/](//' | sort -u | \
  while read -r t; do [ -f "arch/$t" ] || echo "BROKEN: $f -> $t"; done; done; echo DONE
```

Expected: `DONE` без строк BROKEN для обоих файлов.

- [ ] **Step 7: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git add arch/21-valkeyworker.md && \
git commit -m "docs(t01): канон arch/21-valkeyworker.md — оркестратор ValkeyWorker (роль/границы, модель размещения, контракт-зеркало, ACL-секреты, процессы A–E, надёжность/наблюдаемость/конфигурация/риски; по структуре arch/16)"
```

**Выход:** канон `arch/21-valkeyworker.md` существует, покрывает spec §4 полностью, зеркалит arch/20, соответствует структуре arch/16.

**Проверка:** Steps 3–6 зелёные; контрольный подсчёт рисков `grep -c '^| R[1-7] \| | R[1-7] ' arch/21-valkeyworker.md` — 7 строк R1–R7.

**Связь со spec:** цель §1.2, содержание §4 (§4.1–§4.9), решения пользователя §1 (ACL-ротация без рестартов, без TLS), критерии приёмки §8.2, §8.5.

---

### Task 4: Указатели + roadmap (шапка трека, задача t06-valkey-tls)

**Вход (предусловие):** Task 1–3 закоммичены (обе ссылки шапки ведут на существующие файлы); исполнитель прочитал текущий `arch/README.md` (структура до `19-backups.md`; список «Дальше» — 16 пунктов) и `arch/roadmap/backup.md` шапку (образец «канон появился»), spec §1.3/§5.2/§5.3.

**Действие (файлы/изменения):** три правки — (1) `arch/README.md`: строки 20/21 в «Структуру репозитория» + пункты в «Дальше»; (2) шапка `arch/roadmap/valkey.md`: ссылка на канон (по образцу backup.md); (3) новый пункт `t06-valkey-tls` в конец списка задач `arch/roadmap/valkey.md`.

**Files:**
- Modify: `arch/README.md` (блок «Структура репозитория» после строк `19-backups.md`; список «Дальше» после пункта 16)
- Modify: `arch/roadmap/valkey.md` (шапка после 1-го абзаца; конец списка задач после `t05`)

**Interfaces:**
- Consumes: каноны Task 2/3 (существование файлов для ссылок); снапшот Task 1 (valkey.md в ветке).
- Produces: `t06-valkey-tls` — roadmap-задача с зависимостью `← t01-valkey-canon` (снимается в Task 6).

- [ ] **Step 1: arch/README.md — структура.** После блока `19-backups.md` (строки 109–111 текущего файла) вставить с тем же форматом выравнивания:

```markdown
├── 20-valkey-clusters.md      ← ★ Valkey-кластера: контракт etcd /valkey/ + координация /valkeyworker/
│                                  + клиентский дискавери (endpoints/ACL-креды app)
├── 21-valkeyworker.md         ← ★ ValkeyWorker: оркестратор Valkey-кластеров
│                                  (provisioning/deprovisioning/надзор/converge/ротация кред)
```

- [ ] **Step 2: arch/README.md — «Дальше».** После пункта 16 (19-backups.md, конец списка) добавить пункты 17 и 18:

```markdown
17. [20-valkey-clusters.md](20-valkey-clusters.md) — Valkey-кластера:
    контракт etcd `/valkey/` (контроль-плейн кластеров, координация
    `/valkeyworker/`) и клиентский дискавери (endpoints + ACL-креды).
18. [21-valkeyworker.md](21-valkeyworker.md) — оркестратор ValkeyWorker:
    декларативный жизненный цикл Valkey-кластеров (standalone-кеш,
    ACL-креды, converge, ротация без рестартов; задачи — [roadmap/valkey.md](roadmap/valkey.md)).
```

- [ ] **Step 3: Шапка `arch/roadmap/valkey.md` (spec §5.2).** В первый абзац шапки (после списка канон-образцов, перед строкой «Порядок: …») вставить предложение по образцу backup-трека:

```markdown
Канон домена — [../20-valkey-clusters.md](../20-valkey-clusters.md)
(контракт etcd) и [../21-valkeyworker.md](../21-valkeyworker.md)
(оркестратор).
```

- [ ] **Step 4: Новая задача `t06-valkey-tls` (spec §5.3, требование пользователя — ПОДРОБНОЕ описание).** В конец списка «## Задачи» (после пункта t05) добавить пункт дословно из spec §5.3 (перенести блок цитаты целиком, оформив как элемент списка `- ` с переносами строк шириной ~78 как у соседних пунктов; текст НЕ сокращать и не переформулировать):

```markdown
- **`t06-valkey-tls`** `← t01-valkey-canon` — TLS клиентских подключений
  Valkey-кластеров (образец — kafka t03, arch/16 §2.3). **Что нужно
  сделать**: per-cluster CA (`ca_pem`/`ca_key` в
  `/valkey/clusters/<C>/`, ensure воркером при provisioning, подпись
  серверного сертификата ноды CN=`node<k>` + SAN advertised-хоста);
  tls-port контейнера (порт из portalloc; plain-порт закрывается);
  дискавери-ключ `ca_pem` для внешнего читателя — доверие клиентов
  StackExchange.Redis (`GetClientConfig()` получает `ssl=true` + CA);
  advertised/SAN-правило по 16 §2.1; окно двойного доверия при ротации
  CA — по потребности (образец CaRotator 16 §5 K). **Зачем**:
  шифрование клиентского трафика и аутентификация сервера (сейчас —
  доверенная docker-сеть + ACL-креды: пароли ходят по сети открыто в
  пределах закрытого контура). **Почему отложено**: кеш восполним и не
  содержит данных дороже секрета доступа; v1 живёт в доверенной
  закрытой docker-сети домашней установки (enterprise-защиты не нужны
  — AGENTS базовые правила п.8); контракт кред/endpoints не меняется —
  добавятся только CA-ключи, внешняя библиотека t04 совместима без
  переделок (обратная совместимость дискавери arch/20 §4).
```

- [ ] **Step 5: Проверка указателей и ссылок**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
grep -n "20-valkey-clusters\|21-valkeyworker" arch/README.md && \
grep -n "t06-valkey-tls" arch/roadmap/valkey.md && \
grep -c "t0[1-6]-valkey" arch/roadmap/valkey.md && \
grep -oE '\]\([^)#]+\.md' arch/README.md arch/roadmap/valkey.md | sed 's/.*](//' | sort -u | \
while read -r t; do [ -f "arch/$t" ] || [ -f "arch/roadmap/$t" ] || echo "BROKEN: $t"; done; echo DONE
```

Expected: `arch/README.md` — 2 вхождения (структура) + 2 в «Дальше» (итого ≥ 4 строк вывода grep -n); `t06-valkey-tls` найден; grep -c = 6 (t01–t06); `DONE` без BROKEN (относительные пути проверять от каталога файла: arch/README.md → от `arch/`, valkey.md → от `arch/roadmap/`; команда выше проверяет оба корня).

- [ ] **Step 6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git add arch/README.md arch/roadmap/valkey.md && \
git commit -m "docs(t01): указатели arch/README.md (20/21 в структуру и «Дальше»), шапка valkey-трека со ссылкой на канон, новая задача t06-valkey-tls (TLS клиентских подключений — отложено с обоснованием)"
```

**Выход:** все указатели согласованы: arch/README.md ведёт на 20/21, шапка трека ссылается на канон, в треке 6 задач (t01–t06) с корректными зависимостями.

**Проверка:** Step 5 зелёный; критерий приёмки spec §8.3 (указатели согласованы) + §8.4 (t06 с полным описанием и `← t01-valkey-canon`).

**Связь со spec:** цель §1.3/§1.4, §5.2, §5.3, фаза §6.4, критерии §8.3/§8.4.

---

### Task 5: Самопроверка по критериям приёмки + история задачи + ГЕЙТ ревью

**Вход (предусловие):** Task 1–4 закоммичены; все файлы на месте.

**Действие (файлы/изменения):** финальная сверка результата против spec §8 (пункты 1–6), проверка границы изменений, коммит истории задачи (spec + план), остановка на гейте ревью пользователя.

**Files:**
- Modify: `docs/superpowers/2026-09-16-t01-valkey-canon/` (коммит spec.md + plan.md — сейчас untracked)

**Interfaces:**
- Produces: ветка, готовая к ревью пользователем (гейт dev-flow, spec §6 фаза 5); после ревью — к мержу (Task 6 входит тем же мержем).

- [ ] **Step 1: Чек-лист критериев приёмки spec §8 (пункты 1–6)** — пройти ПО ТЕКСТУ канонов, каждое утверждение подтвердить (найдено/нет); расхождение — исправить до коммита:

  - §8.1: arch/20 покрывает транспорт+имена, таблицу 8 ключей с nodes=1 и всеми полями config, примеры, координацию 1:1 + единый rotations с role, дискавери (endpoints+app-креды+state, GetClientConfig, ssl=false + отсылка к t06), толерантность (parseError/`valkey-key-malformed`, `unknownKeys`, `valkey-endpoints-missing`, неполные креды → null, state raw-строка).
  - §8.2: arch/21 фиксирует роль/границы + A–E, модель размещения (образ, `vwk-<C>-node<k>`, без volume, без per-cluster сети, 17000–17999, ACL-старт, advertised), зеркало контракта, секреты admin+app, состояния (PING-ready, docker→etcd, S7/E9, converge, окно двух паролей), §6–§9.
  - §8.3: указатели arch/README (структура + «Дальше») и шапка трека → 20/21.
  - §8.4: перенос снапшота выполнен (Task 1); t06 с «что нужно / зачем / почему отложено» и `← t01-valkey-canon`.
  - §8.5: три решения пользователя отражены (nodes=1 — 20 §2; ACL admin+app с ensure и ротацией без рестартов — 20 §2 + 21 §4/§5 E; без TLS v1 с отсылками к t06 — 20 §4 + 21 границы/риски).
  - §8.6 (граница изменений) — Step 2.

- [ ] **Step 2: Граница изменений — ничего вне разрешённого**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git diff --stat main..HEAD && git status --short
```

Expected: изменения ТОЛЬКО в `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `arch/README.md`, `arch/roadmap/valkey.md`, `arch/roadmap/README.md` (+ после Step 3 — `docs/superpowers/2026-09-16-t01-valkey-canon/`); никакого кода, тестов, deploy, dev-stand, других канонов. Если что-то лишнее — убрать/откатить до коммита истории.

- [ ] **Step 3: Коммит истории задачи (spec + план)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git add docs/superpowers/2026-09-16-t01-valkey-canon/ && \
git commit -m "docs(t01): spec и план valkey-канона — история задачи (решения: nodes=1, ACL-креды admin+app, без TLS v1 + t06-valkey-tls)"
```

- [ ] **Step 4: ГЕЙТ — остановка и ревью пользователем.** Исполнитель/координатор ОСТАНАВЛИВАЕТСЯ: каноны предъявляются пользователю на ревью (dev-flow user-review; spec §6 фаза 5). Мерж в `main` и Task 6 — ТОЛЬКО после явного одобрения и по отдельной просьбе пользователя (AGENTS.base п.6). Никаких самовольных продолжений.

**Выход:** ветка полная (2 канона + указатели + roadmap + история), самопроверка §8.1–§8.6 пройдена, ревью запрошено.

**Проверка:** Steps 1–2 без расхождений; `git log --oneline main..HEAD` — 5 коммитов (Task 1–5).

**Связь со spec:** §6 фазы 2–5, критерии §8.1–§8.6 (кроме §8.7 — Task 6).

---

### Task 6: Мерж-гейт — снятие тега `t01-valkey-canon` из roadmap

**Вход (предусловие):** ревью пользователя пройдено (Task 5 Step 4), пользователь явно потребовал мерж в `main`. Исполняется ПОСЛЕДНИМ коммитом ветки перед мержем (попадает в `main` тем же мержем — практика проекта, см. t10: `chore(tNN): мерж-гейт…` непосредственно перед merge-коммитом).

**Действие (файлы/изменения):** в `arch/roadmap/valkey.md`: удалить пункт `t01-valkey-canon` (буллет из 10 строк — «канон Valkey-домена: arch/20… converge)») и снять подстроку `← t01-valkey-canon` из зависимостей t02, t03, t04, t06 (t05 зависит от t02 — НЕ трогаем). После удаления t06 становится первой строкой списка зависимостей вида `← t01-valkey-canon` — просто убрать хвост `` `← t01-valkey-canon` `` у четырёх пунктов.

**Files:**
- Modify: `arch/roadmap/valkey.md`

**Interfaces:**
- Consumes: roadmap из Task 1 + Task 4 (пункты t01–t06).
- Produces: roadmap без выполненной задачи — правило мерж-гейта AGENTS.md/`arch/roadmap/README.md` («тем же мерж-коммитом: и из списка, и из ←-зависимостей; сам, без команды и без вопросов»).

- [ ] **Step 1: Удалить пункт t01 и снять зависимости** — редактирование `arch/roadmap/valkey.md`: удалить буллет `- **t01-valkey-canon** …` целиком; в пунктах t02/t03/t04/t06 убрать ` ← t01-valkey-canon` (в обратных кавычках с пробелом-префиксом, как в файле).

- [ ] **Step 2: Проверка полноты снятия**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
grep -rn "t01-valkey-canon" arch/roadmap/ ; echo "exit=$?"
```

Expected: вывод пуст, `exit=1` (упоминаний в roadmap нет; упоминания в `docs/superpowers/…` и `arch/20`/`arch/21` НЕ трогаем — история и канон живут отдельно).

- [ ] **Step 3: Контроль целостности трека после правки**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
grep -n '^\- \*\*`t0' arch/roadmap/valkey.md
```

Expected: ровно 5 пунктов — t02 (без ←), t03 (без ←), t04 (без ←), t05 (`← t02-valkey-worker` — сохранена), t06 (без ←); формат строк не сломан.

- [ ] **Step 4: Коммит мерж-гейта**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t01-valkey-canon && \
git add arch/roadmap/valkey.md && \
git commit -m "chore(t01): мерж-гейт — тег t01-valkey-canon снят из roadmap (пункт удалён, зависимости t02–t06 освобождены; правило arch/roadmap/README.md)"
```

**Выход:** roadmap чист от выполненной задачи; ветка готова к мержу в `main`.

**Проверка:** Steps 2–3 зелёные; критерий приёмки spec §8.7 выполнен тем же мержем.

**Связь со spec:** §5.4, §6 фаза 6, критерий §8.7.

---

## Самопроверка плана (выполнена автором)

1. **Покрытие spec:** §5.1/§6.1 → Task 1; §3 (всё) → Task 2; §4 (всё) → Task 3; §1.3/§5.2/§5.3/§6.4 → Task 4; §6.5 + §8.1–§8.6 → Task 5; §5.4/§6.6/§8.7 → Task 6. Решения пользователя §1 — в скелетах Task 2/3/4 и чек-строках (Step 4 обоих канонов, Task 5 Step 1 §8.5). Ограничения §7 — Global Constraints. Пробелов нет.
2. **Плейсхолдеры:** тезисы скелетов дают конкретику (имена ключей, состояния, фазы, значения); текст t06 — дословный; конфиг-блок §8 и блок E1–E3 — дословно из spec. «Развёртка в стиле эталона» здесь — инструкция формата, а не отложенное содержание (материал указан секцией spec).
3. **Согласованность имён:** `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `t06-valkey-tls`, префиксы `/valkey/`/`/valkeyworker/`, `vwk-<C>-node<k>`, `VWK_API_TLS_*`, разделы 20 §1–§5 / 21 §1–§9 — одинаковы во всех задачах.
