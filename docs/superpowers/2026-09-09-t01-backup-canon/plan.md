# t01-backup-canon: канон подсистемы бэкапов + код-каркас — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Зафиксировать арх-канон подсистемы бэкапов PG-шардов (`arch/19-backups.md`) и положить код-каркас (конфиг `PgWorker:Backups`, модель/парсер etcd `/pgworker/backups/*`, MinIO в dev-станде) — без процессной логики (вся механика — t02–t07).

**Architecture:** arch-first: сначала канон `arch/19-backups.md` + указатели (README, arch/14 §границ, adminpanel/02 §2.3.1, roadmap-шапка), затем отражение в коде — options-секция с fail-fast-валидацией и толерантный парсер etcd-префикса в `PgWorker.Etcd` (по образцу `ClusterSnapshotParser`), затем стенд: сервис `as-minio` + env-контракт `PGW_BACKUP_S3_*` в deploy. Поведение воркера не меняется: `Enabled=false` по умолчанию.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3 + FluentAssertions, docker compose (MinIO), без новых NuGet-пакетов (S3 SDK — только t02+).

**Spec:** [`docs/superpowers/2026-09-09-t01-backup-canon/spec.md`](spec.md)

## Global Constraints

(Из spec §2/§6 и AGENTS.md — действуют для каждой задачи.)

- **t01 ничего не исполняет**: никаких джобов/агентов/образа `pgworker-backup`, слотов, S3-клиента (SDK-пакеты НЕ тянем), ретенции, restore, UI панели, API-эндпинтов бэкапов.
- **Поведение воркера не меняется**: `PgWorker:Backups:Enabled=false` по умолчанию; новый код — только опции и модель данных.
- Ноды (Spilo) не меняются: без `archive_command`, без WAL-G-env, без S3-кредов в нодах.
- S3-креды — per-install, ТОЛЬКО env воркера (не etcd, не git): `PGW_BACKUP_S3_*`.
- .NET 10, `TreatWarningsAsErrors=true`, centralized packages (`Directory.Packages.props`) — новых пакетов нет.
- Документация — русский язык, идентификаторы — английские; тесты — с AAA-комментариями (`// Arrange`, `// Act`, `// Assert`).
- Префикс `/pgworker/backups/*` пишет ТОЛЬКО PgWorker (держатель клэйма `<C>`); панель читает.
- Порты 9000/9001 — фиксированные порты ТОЛЬКО dev-стенда (в тестах хардкодов портов нет; testcontainers-MinIO — t02+ с динамическими портами).
- После КАЖДОЙ тестовой серии с docker — зачистка: контейнеры (кроме `as-*`/`adminpanel` живого стенда) + `docker network prune -f`. Порядок серий по AGENTS.md: юниты → интеграция → E2E, зачистка после каждой.
- Работа — в worktree `feat-t01-backup-canon` (уже создан); коммит после каждой задачи; ветку не мержим сами.
- Мерж-гейт roadmap: удаление `t01-backup-canon` из `arch/roadmap/backup.md` (пункт + `←`-зависимости) — тем же мерж-коммитом (Task 10).
- **Порядок чеков «17-backups»/«t01»:** упоминание «`arch/17-backups.md`» живёт в пункте `t01-backup-canon` файла `arch/roadmap/backup.md` до мерж-гейта (Task 10). До Task 10 grep-чеки по «17-backups» выполняются с исключением `arch/roadmap/backup.md`; полный чек «17-backups → пусто по всему `arch/`» и «t01-backup-canon → пусто по `arch/roadmap/`» — только в Task 10 Step 3, после удаления пункта.

**Уточнение по месту as-minio (разрешение противоречия spec §4.3).** Spec пишет «в `dev-stand/compose.yaml` (входит в полный стенд 00-up.sh)» — но по AGENTS.md `dev-stand/compose.yaml` («стенд части», имена `pgw-stand-*`) в полный стенд НЕ входит, а `00-up.sh` поднимает `dev-stand/adminpanel/docker-compose.yml` (проект `adminpanel-stand`, контейнеры `as-*`). Оба сигнала spec — префикс имени `as-minio` и «входит в полный стенд 00-up.sh» — указывают на `dev-stand/adminpanel/docker-compose.yml`. MinIO кладём туда (Task 5).

**Уточнение по месту BackupsOptions (отклонение от буквы spec §4.2).** Spec говорит «`BackupsOptions` (PgWorker.Core, паттерн существующих Options)» — но фактически ВСЕ существующие Options воркера (включая `PgWorkerOptions` и вложенные `MovesOptions`/`ApiOptions`/`TlsOptions` и т.д.) живут в `src/PgWorker.App/Options.cs`, а не в `PgWorker.Core` (в Core — доменная модель без конфигурации). Кладём `BackupsOptions` в `src/PgWorker.App/Options.cs` рядом с `PgWorkerOptions` — по фактическому паттерну кодовой базы («паттерн существующих Options» из spec); потребители пока только App (Program.cs), t02+ достанут через `IOptions<PgWorkerOptions>` как остальные секции.

---

### Task 1: Канон `arch/19-backups.md`

**Вход (предусловие):** worktree `feat-t01-backup-canon`, spec прочитан; номера 17/18 в `arch/` заняты (`17-synchronization-principles.md`, `18-metrics.md`) — берём 19.

**Действие (файлы/изменения):** создать новый `arch/19-backups.md` — канон подсистемы бэкапов в стиле arch/14–16/18 (шапка с курсивом-аннотацией и «Границы (что НЕ входит)», нумерованные разделы). Содержание — перенос архитектурных решений из spec §3 (развёрнуто в связный текст, без ссылок на spec — это канон, на него будут ссылаться t02–t07).

**Files:**
- Create: `arch/19-backups.md`

**Interfaces (для следующих задач и задач t02–t07):**
- Produces: канон — единый источник архитектуры подсистемы; на него ссылаются указатели (Task 2), `BackupsOptions` (Task 3), `BackupsParser` (Task 4), стенд MinIO (Task 5), env-контракт (Task 6). Разделы канона нумеруются — в комментариях кода и указателях используем ссылки вида `arch/19 §4` (контракт etcd), `arch/19 §6` (HA/лимиты), `arch/19 §7` (секреты), `arch/19 §9` (конфигурация).

**Обязательная структура документа** (заголовок + разделы; тезисы ниже — материал для развёртывания, всё из spec §3/§7 — ничего не добавлять от себя и не помечать «додумать»):

```markdown
# 19. Подсистема бэкапов шардов: PgWorker-оркестрация (канон) ★

**Шапка:** PgWorker — хозяин бэкапов шардов: сам создаёт, ретраит/пересоздаёт,
чистит, сигнализирует в панель. Ноды (Spilo) бэкапами не управляют. Механика —
базовые утилиты PostgreSQL (`pg_basebackup`, `pg_receivewal`) + оркестрация
воркером; WAL-G/pgBackRest НЕ встраиваются (параллельное использование
оператором не поддерживается и не смешивается). Хранилище — S3-совместимое
объектное (dev-стенд MinIO, прод — любое S3, вкл. облако). Реализация —
t02–t07 по разделам §8; t01 (этот документ) код не исполняет.

Границы (что НЕ входит): UI/алерты панели (т02+), API-эндпоинты бэкапов
(t02+), управление нодами/HA-контуром (бэкапы — сторонние клиенты нод),
файловое том-хранилище docker-хоста (решение — S3).

## 1. Роль и границы
- Роль в системе: защита данных шардированных кластеров (полные + WAL),
  восстановление (t05) — единый оркестратор PgWorker.
- Схема взаимодействия (ASCII): PgWorker → docker-контейнеры джобов/агентов →
  PG-ноды (реплика/мастер) и S3; PgWorker → etcd `/pgworker/backups/*`;
  AdminPanel читает etcd (алерты t02+).
- Принципы: arch-first; единый etcd-контур и паттерны arch/17 (идемпотентность
  каждого шага, journal-before-manipulations, transient vs permanent);
  PgWorker — единственный писатель префикса; S3-креды per-install только env.

## 2. Механика полных бэкапов (реализация — t02)
- Инструмент: `pg_basebackup` (plain-формат, `-X stream`,
  `--checkpoint=spread`, `--manifest-checksums=SHA256`).
- Исполнитель: ephemeral docker-контейнер джоба `pgw-backup-full-<C>-<X>-<id>`,
  запускает PgWorker; образ — лёгкий `pgworker-backup` (postgres-клиентские
  утилиты + S3-uploader; отдельный Dockerfile в `docker/`, сборка — t02).
- Источник: реплика шарда (sync-standby из Patroni `GET /cluster`); реплика
  недоступна/отстала → fallback на мастер (резолв мастера — существующий
  `ShardEndpoints`, arch/14 §5 F). Источник фиксируется в etcd-статусе
  (`node`, `role`).
- Пайплайн джоба: staging-каталог (ephemeral volume) → `pg_basebackup -D
  <staging>` → потокная загрузка в S3 файлами 1:1 объектами (включая
  `backup_manifest`), WAL-сегменты набора `-X stream` — в общий WAL-префикс
  шарда → атомарная фиксация статуса в etcd (§4).
- Идентификатор: `id = YYYYMMDDHHMMSSZ` (UTC старта, сортируемый); коллизия в
  пределах шарда — суффикс `-2`, `-3`…; детерминизм имён — как у
  `pgw-<C>-<X>-<n>` (arch/14 §6).
- Подключение: адресация из portalloc (pg-порты нод) в том же namespace
  адресов, что панель (advertised-правило arch/14 §2.4 п.5); БД-роль — §7.

## 3. Online-WAL: pg_receivewal-агент (реализация — t03)
- Агент: long-running контейнер `pgw-backup-wal-<C>-<X>` (тот же образ
  `pgworker-backup`), запускает и супервизирует PgWorker. Внутри —
  `pg_receivewal --slot=<slot> -D <staging>` + S3-шиппер: закрытый сегмент →
  upload `wal/<segment>` → удаление из staging; `.partial` — по канону
  `pg_receivewal`; `.history`-файлы ЗАГРУЖАЮТСЯ обязательно (PITR через смену
  timeline).
- Слот: один на шард, имя `pgw_bkp_<C>_<X>`; длиннее NAMEDATALEN(63) →
  `pgw_bkp_` + sha1(`<C>/<X>`)[:16]. Слот создаёт агент, держит WAL на мастере
  до подтверждения приёма — защита непрерывности цепочки.
- Подключение: к мастеру шарда (резолв `ShardEndpoints`); смена мастера →
  рестарт агента с нового мастера, догон цепочки, склейка timeline через
  `.history`.
- Инвариант непрерывности: от стартовой точки каждого COMPLETED полного
  бэкапа до `last_uploaded` нет дыр (контроль имён сегментов + TLI-переходы
  через history; проверка — t03/t04, база — парсер имён в каркасной модели
  t01 не входит, фиксируется каноном).

## 4. Контракт etcd `/pgworker/backups/*`
- Пишет ТОЛЬКО PgWorker под клэймом `<C>` (операции бэкапов — под тем же
  клэймом, что остальные процессы кластера); панель читает (контекст —
  arch/adminpanel/02 §2.3.1; UI/алерты — t02+).
- Таблица ключей (дословно из spec §3.4):
  - `/pgworker/backups/<C>/policy` — per-cluster политика:
    `{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":86400,"verify":{"on_create":true}}`;
    пишет воркер (приём через API — t02/t06); отсутствует → дефолт
    `PgWorker:Backups:Policy`.
  - `/pgworker/backups/<C>/<X>/full/<id>` — статус полного:
    `{"state":"PLANNED|RUNNING|UPLOADING|COMPLETED|FAILED|DELETING","node":"<n>","role":"replica|master","started_unix","finished_unix"?,"wal_start_segment","size_bytes"?,"error"?,"verify":{"state":"PENDING|OK|FAILED","checked_unix"?}}`.
  - `/pgworker/backups/<C>/<X>/wal` — состояние WAL-потока шарда:
    `{"state":"ACTIVE|DEGRADED|STOPPED","slot":"<slot>","master_node","chain_start_segment","last_received_segment","last_uploaded_segment","last_uploaded_unix","lag_segments"?,"error"?}`.
- Правила: transient-сбой → статус с `error` + ретраи тиками (t02/t03);
  permanent-отказ → фиксация причины + алерт. `DELETING` — транзитная фаза
  ретенции (t06; запрет удалять последний валидный полный — guard t06).
- Deprovisioning кластера (D2, arch/14 §5 B) чистит etcd-префикс
  `/pgworker/backups/<C>/` тем же `del --prefix`; объекты S3 НЕ удаляются
  автоматически (данные дороже места): префикс S3 без etcd-владельца = orphan,
  его видит супервизор (t07: алерт + политика возраста/ручной разбор);
  восстановление удалённого кластера из S3 — runbook t05 (симметрия R4
  arch/14: воркер не уничтожает потенциально ценные данные автоматикой).

## 5. Хранилище S3: layout
- Один bucket на установку (`PgWorker:Backups:S3:Bucket`); префиксы per-cluster/
  per-shard (диаграмма из spec §3.3):
  `s3://<bucket>/<C>/<X>/full/<id>/` — файлы pg_basebackup 1:1 (вкл.
  backup_manifest, pg_wal/); `s3://<bucket>/<C>/<X>/wal/<segment>` — WAL;
  `wal/00000002.history` — timeline-истории (обязательны).
- Ключ объекта = путь файла; `backup_manifest` в корне `full/<id>/` — база
  `pg_verifybackup` (t04: скачивание в staging → verify). Никаких tar-обёрток;
  WAL-сегмент = один объект. Layout одинаков для MinIO и облака;
  регион/endpoint — конфиг.

## 6. Источник, HA-контур и ресурсные лимиты
- Реплика-источник: штатно полный бэкап не трогает мастер; fallback на мастер
  — журнал-факт (`role=master`) + I/O одной операции (не потоковой). Смерть
  источника посреди бэкапа → джоб убивается, статус FAILED, переснятие
  (окно/алерты — t02).
- Слот на мастере: WAL копится, пока агент не принял; потолок —
  `max_slot_wal_keep_size` (уже в каноне нод, arch/14 §2.1 P3/P4): смерть
  агента → WAL в пределах лимита → по исчерпании слот инвалидируется PG →
  цепочка рвётся → алерт t03 + переснятие полного (t07). Диск мастера
  переполнить нельзя.
- Лимиты джоба/агента: `PgWorker:Backups:Agent { Cpu, Mem }` →
  `HostConfig.NanoCPUs/Memory` (образец: request_* нод, arch/14 §2.4 п.4);
  staging volume — ephemeral с квотой `Staging:QuotaBytes` (guard «нет места»
  → FAILED + алерт, t02).
- Patroni/HA не затрагивается: бэкапы — сторонние клиенты нод; изменений в
  конфиги нод/HA-контура подсистема не вносит.

## 7. Секреты
- Per-install, env воркера (не etcd, не git): `PGW_BACKUP_S3_ENDPOINT`,
  `PGW_BACKUP_S3_REGION` (опц., MinIO не требует), `PGW_BACKUP_S3_BUCKET`,
  `PGW_BACKUP_S3_ACCESS_KEY`, `PGW_BACKUP_S3_SECRET_KEY` — конфиг-биндинг
  `PgWorker:Backups:S3` (группа транспортных секретов arch/14 §4 п.3);
  передача агенту/джобу — env контейнера (t02/t03).
- Per-cluster БД-роль `backup_exec` (LOGIN + REPLICATION): пароль per-cluster
  `/clusters/<C>/backup_password` — по образцу t02-секретов (ensure
  put-if-absent, ротация в общем тикете §9.8, 32 симв `[A-Za-z0-9]`);
  реализация ensure/ротации — t02. Отдельная от bucket_mover (иная зона
  доверия/ротации).

## 8. Карта задач (Дальше)
- Таблица t02–t07 (дословно из spec §3.7): t02 джоб полного+планировщик+
  статусы/ретраи+суточный алерт+роль/ensure backup_exec; t03 WAL-агент+слот+
  загрузка+непрерывность+алерты; t04 pg_verifybackup+полнота цепочки+статусы;
  t05 восстановление полный+WAL (PITR)+runbook; t06 GFS-ретенция+чистка WAL+
  квоты+защита последнего; t07 reconcile S3↔etcd+сироты+reconnect+рестарт-
  устойчивость.

## 9. Конфигурация
- Секция `PgWorker:Backups` (каркас t01): `Enabled=false`, `S3 { Endpoint,
  Region, Bucket, AccessKey, SecretKey, PathStyle=true }`, `Policy {
  Retention { Days=7, Weeks=4, Months=6 }, FullMaxAgeSec=86400,
  VerifyOnCreate=true }`, `Staging { Dir=/backup-staging, QuotaBytes }`,
  `Agent { Cpu, Mem }`. Валидация старта: `Enabled=true` при пустых S3-полях —
  fail-fast.

## 10. Риски
- Таблица рисков из spec §7 (все 7 строк: детали vs t02–t07, MinIO-стенд,
  компрометация S3-кредов, слот при смерти агента, длинные имена, staging-
  квота, расхождение spec↔arch) — каждая с закрытием.
```

- [ ] **Step 1: Создать `arch/19-backups.md`** по структуре выше. Стиль — как у `arch/18-metrics.md`/`arch/14-pgworker.md`: шапка `# 19. … ★`, жирный вводный абзац, «Ключевые свойства» и «Границы (что НЕ входит)», затем разделы. Все значения (ключи etcd, layout S3, имена, дефолты, лимиты) — дословно из тезисов выше. ASCII-диаграммы — в стиле arch/14 §1. Ссылки внутри документа на arch/14/17 и adminpanel/02 — относительные (`14-pgworker.md`, `adminpanel/02-etcd-contract.md`).

- [ ] **Step 2: Проверить покрытие тем roadmap.** Каждая тема roadmap-задачи t01 обязана иметь раздел: механика полные (§2) + WAL (§3), layout S3 (§5), контракт etcd (§4), источник (§2/§6), HA-контур и лимиты (§6), секреты (§7), карта t02–t07 (§8), конфигурация (§9), риски (§10). Run: `grep -c '^## ' arch/19-backups.md` → ожидаемо `10`. `grep -n "TODO\|TBD\|заполнить" arch/19-backups.md` → пусто.

- [ ] **Step 3: Проверить согласованность с существующим каноном.** Run: `grep -rn "17-backups" arch/ | grep -v "arch/roadmap/backup.md"` → пусто — упоминание «17-backups» остаётся ТОЛЬКО в пункте `t01-backup-canon` файла `arch/roadmap/backup.md` (живёт до мерж-гейта Task 10; нигде больше его не плодим). Run: `grep -c "pg_basebackup\|pg_receivewal\|pg_verifybackup" arch/19-backups.md` → ≥3 (все три утилиты названы).

- [ ] **Step 4: Commit.**

```bash
git add arch/19-backups.md
git commit -m "arch(19): канон подсистемы бэкапов шардов (t01-backup-canon)

Механика pg_basebackup+pg_receivewal-агент, layout S3, контракт
/pgworker/backups/*, источник реплика/fallback, HA+лимиты, секреты,
карта t02-t07. Без процессной логики — t01 фиксирует решения."
```

**Выход (что становится готовым):** канон `arch/19-backups.md` — источник правды для t02–t07; все арх-решения spec §3 зафиксированы.

**Проверка (критерий):** файл существует; 10 разделов; grep-чеки Step 2–3 зелёные.

**Связь со spec:** §1.1, §3 (весь), §7 — критерий приёмки 1.

---

### Task 2: Указатели на канон (arch/README, arch/14 §границ, adminpanel/02 §2.3.1, roadmap-шапка)

**Вход (предусловие):** Task 1 слит (канон существует по пути `arch/19-backups.md`).

**Действие (файлы/изменения):** четыре минимальные правки указателей.

**Files:**
- Modify: `arch/README.md` (дерево структуры ~строка 106 + список «Дальше» ~строка 199)
- Modify: `arch/14-pgworker.md` (абзац «Границы (что НЕ входит): …» ~строки 48–58)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§2.3.1 ~строки 105–116)
- Modify: `arch/roadmap/backup.md` (шапка ~строки 6–8)

**Interfaces:**
- Consumes: `arch/19-backups.md` (Task 1).
- Produces: согласованные указатели — требование критерия приёмки 2.

- [ ] **Step 1: `arch/README.md` — дерево структуры.** После блока `18-metrics.md` (строки с `18-metrics.md ← ★ единая телеметрия…`) вставить:

```
├── 19-backups.md               ← ★ подсистема бэкапов шардов: PgWorker-оркестрация
│                                  (pg_basebackup + pg_receivewal-агент, S3/MinIO,
│                                   контракт /pgworker/backups/*, t02–t07)
```

- [ ] **Step 2: `arch/README.md` — список «Дальше».** Фактический последний пункт списка «Дальше» — 15 (`17-synchronization-principles.md`). После пункта 15 добавить пункт 16:

```markdown
16. [19-backups.md](19-backups.md) — подсистема бэкапов шардов: механика
    (полные `pg_basebackup`, online-WAL `pg_receivewal`-агент), layout S3,
    контракт `/pgworker/backups/*` (задачи t02–t07 — [roadmap/backup.md](roadmap/backup.md)).
```

- [ ] **Step 3: `arch/14-pgworker.md` — §границ.** В абзаце, начинающемся `Границы (что НЕ входит): панельные кнопки ЯВНЫХ переездов бакетов — roadmap`, после перечисления перед `Транспортная безопасность` добавить пункт о бэкапах. Итоговый фрагмент абзаца (в конец перечисления, после `тонкий RBAC Engine API (authz-плагины Docker CE) —` перед `[roadmap/pgworker.md](roadmap/pgworker.md)`): вставить `подсистема бэкапов шардов (хозяин — PgWorker, механика и хранилище — отдельный канон [19-backups.md](19-backups.md)) — `.

- [ ] **Step 4: `arch/adminpanel/02-etcd-contract.md` — §2.3.1.** В §2.3.1: (а) в вводном предложении «четыре ключа-семейства» заменить на «пять ключа-семейств»; (б) добавить строку в таблицу ключей (после строки `/pgworker/api/<id>`):

```markdown
| `/pgworker/backups/<C>/…` | JSON-статусы полных/WAL (канон — [19-backups.md](../19-backups.md) §4) | — (t01: только контракт) | подсистема бэкапов (arch/19): панель ЧИТАЕТ статусы полных и WAL-цепочек; UI и алерты («нет валидного полного за окно суток», «разрыв/отставание WAL-цепочки») — t02/t03; пишет префикс ТОЛЬКО PgWorker |
```

(Линк `[19-backups.md](../19-backups.md)` — относительный из `arch/adminpanel/` в `arch/`.)

- [ ] **Step 5: `arch/roadmap/backup.md` — шапка.** Строку «Канон подсистемы появится с `t01-backup-canon` (будущий `../17-backups.md`); контекст сервиса — [../14-pgworker.md](../14-pgworker.md).» заменить на: «Канон подсистемы — [../19-backups.md](../19-backups.md); контекст сервиса — [../14-pgworker.md](../14-pgworker.md).» ВАЖНО: БЕЗ упоминания тега `t01-backup-canon` в новой шапке — тег живёт только в пункте задачи (удаляется мерж-гейтом Task 10); упоминание тега в шапке сломало бы grep-чек мерж-гейта (`grep -rn "t01-backup-canon" arch/roadmap/` → пусто, Task 10 Step 3). Формулировка «канон появился: [../19-backups.md]» без тега — именно то, что требует spec §4.1.

- [ ] **Step 6: Проверить согласованность (чеки этапа Task 2).**
  - Run: `grep -rn "19-backups.md" arch/ | grep -v "^arch/19-backups.md"` → минимум 5 попаданий (README: дерево + «Дальше»; 14-pgworker; adminpanel/02 — текст и линк; roadmap/backup).
  - Run: `grep -rn "17-backups" arch/ | grep -v "arch/roadmap/backup.md"` → пусто (вне пункта t01 файла `arch/roadmap/backup.md` упоминаний «17-backups» нет; сам пункт живёт до мерж-гейта — его чистит Task 10).
  - Run: `grep -n "будущий ../17" arch/roadmap/backup.md` → пусто (устаревшая фраза шапки удалена Step 5).
  - Run: `grep -n "пять ключа-семейства" arch/adminpanel/02-etcd-contract.md` → 1 попадание.
  - Полный чек «17-backups → пусто по всему `arch/`» на этом этапе НЕ выполняется — он перенесён в Task 10 Step 3.

- [ ] **Step 7: Commit.**

```bash
git add arch/README.md arch/14-pgworker.md arch/adminpanel/02-etcd-contract.md arch/roadmap/backup.md
git commit -m "arch: указатели на канон бэкапов (19-backups.md) — README, 14-границы, adminpanel/02 §2.3.1, roadmap-шапка"
```

**Выход:** все указатели согласованы с каноном; устаревшая фраза «будущий 17-backups.md» удалена из шапки roadmap.

**Проверка:** grep-чеки Step 6 зелёные (с исключением roadmap-пункта t01 — см. Global Constraints «Порядок чеков»).

**Связь со spec:** §4.1, §2 п.1 (мерж-гейт правит ссылку на 17) — критерий приёмки 2.

---

### Task 3: `BackupsOptions` с fail-fast-валидацией + wiring в App

**Вход (предусловие):** Task 1–2 слиты (канон и ссылки существуют — XML-doc ссылаются на `arch/19`).

**Действие (файлы/изменения):** секция опций `PgWorker:Backups` в существующем `Options.cs` (паттерн соседних Options; место — по фактическому паттерну кодовой базы, см. уточнение в Global Constraints), предикат валидации в `Program.cs`, дефолтная секция в `appsettings.json`, юнит-тесты.

**Files:**
- Modify: `src/PgWorker.App/Options.cs` (свойство в `PgWorkerOptions` + 5 новых классов в конец файла)
- Modify: `src/PgWorker.App/Program.cs` (~строки 39–45: цепочка `.Validate(...)`)
- Modify: `src/PgWorker.App/appsettings.json` (секция `PgWorker`)
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` (создать)

**Interfaces:**
- Consumes: канон arch/19 §4/§6/§7/§9 (дефолты и состав секции).
- Produces (для Task 6 и t02+):
  - `PgWorkerOptions.Backups : BackupsOptions` (namespace `PgWorker.App`);
  - `BackupsOptions { bool Enabled; BackupsS3Options S3; BackupsPolicyOptions Policy; BackupsStagingOptions Staging; BackupsAgentOptions Agent; bool IsValid(); }`
  - `BackupsS3Options { string Endpoint; string? Region; string Bucket; string AccessKey; string SecretKey; bool PathStyle=true; }`
  - `BackupsPolicyOptions { BackupsRetentionOptions Retention; long FullMaxAgeSec=86400; bool VerifyOnCreate=true; }`, `BackupsRetentionOptions { int Days=7; int Weeks=4; int Months=6; }`
  - `BackupsStagingOptions { string Dir="/backup-staging"; long? QuotaBytes; }`
  - `BackupsAgentOptions { double? Cpu; long? Mem; }`

- [ ] **Step 1: Написать падающий тест (TDD).** Создать `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs`:

```csharp
using PgWorker.App;

namespace PgWorker.UnitTests.App;

// Каркас конфигурации бэкапов (arch/19 §9, t01): default выключен и валиден,
// включение требует полный S3-комплект — fail-fast старта (образец TLS
// arch/14 §2.2.1). Процессной логики в t01 нет — только options.
public class BackupsOptionsTests
{
    [Fact]
    public void Default_DisabledAndValid_PathStyleTrue()
    {
        // Arrange — дефолтная секция (appsettings без Backups).
        var options = new BackupsOptions();

        // Act / Assert — поведение воркера не меняется: подсистема выключена.
        options.Enabled.Should().BeFalse();
        options.IsValid().Should().BeTrue();
        options.S3.PathStyle.Should().BeTrue(); // клиент MinIO-режима (arch/19 §5)
    }

    [Fact]
    public void Enabled_EmptyS3_Invalid()
    {
        // Arrange — включили, но S3-комплект не задан.
        var options = new BackupsOptions { Enabled = true };

        // Act / Assert — Program.cs ValidateOnStart уронит старт.
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Enabled_CompleteS3_Valid()
    {
        // Arrange — полный per-install S3-комплект (env-секреты arch/19 §7).
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Endpoint = "http://host.docker.internal:9000",
                Region = null,
                Bucket = "pgworker-backups",
                AccessKey = "minioadmin",
                SecretKey = "minioadmin",
            },
        };

        // Act / Assert
        options.IsValid().Should().BeTrue();
    }

    [Fact]
    public void Enabled_MissingSecretKey_Invalid()
    {
        // Arrange — частичная конфигурация: endpoint/bucket/key есть, секрета нет.
        var options = new BackupsOptions
        {
            Enabled = true,
            S3 = new BackupsS3Options
            {
                Endpoint = "http://host.docker.internal:9000",
                Bucket = "pgworker-backups",
                AccessKey = "minioadmin",
            },
        };

        // Act / Assert
        options.IsValid().Should().BeFalse();
    }

    [Fact]
    public void Defaults_PolicyStagingAgent_CanonValues()
    {
        // Arrange / Act — дефолты каркаса (арх/19 §4/§6/§9).
        var options = new BackupsOptions();

        // Assert — GFS 7/4/6, суточное окно, verify при создании; staging-
        // каталог; квота и лимиты джобов — null (без лимита, образец request_*
        // нод arch/14 §2.4 п.4).
        options.Policy.Retention.Days.Should().Be(7);
        options.Policy.Retention.Weeks.Should().Be(4);
        options.Policy.Retention.Months.Should().Be(6);
        options.Policy.FullMaxAgeSec.Should().Be(86400);
        options.Policy.VerifyOnCreate.Should().BeTrue();
        options.Staging.Dir.Should().Be("/backup-staging");
        options.Staging.QuotaBytes.Should().BeNull();
        options.Agent.Cpu.Should().BeNull();
        options.Agent.Mem.Should().BeNull();
        options.S3.Endpoint.Should().BeEmpty();
    }
}
```

- [ ] **Step 2: Прогнать — убедиться, что падает.** Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter FullyQualifiedName~BackupsOptionsTests`. Expected: FAIL — ошибка компиляции `BackupsOptions` не существует (CS0246).

- [ ] **Step 3: Реализовать опции.** В `src/PgWorker.App/Options.cs`:

(а) В `PgWorkerOptions` после свойства `Metrics` добавить:

```csharp
    /// <summary>Подсистема бэкапов шардов (arch/19, t01): каркас конфигурации;
    /// процессная логика — t02–t07. Default Enabled=false.</summary>
    public BackupsOptions Backups { get; set; } = new();
```

(б) В конец файла — классы:

```csharp
/// <summary>Подсистема бэкапов шардов (arch/19, t01): каркас без процессной
/// логики (джобы/агенты — t02–t07); Enabled=false — поведение воркера не
/// меняется.</summary>
public sealed class BackupsOptions
{
    /// <summary>Вкл/выкл подсистемы (t01: только каркас; потребители — t02+).</summary>
    public bool Enabled { get; set; }

    public BackupsS3Options S3 { get; set; } = new();

    public BackupsPolicyOptions Policy { get; set; } = new();

    public BackupsStagingOptions Staging { get; set; } = new();

    public BackupsAgentOptions Agent { get; set; } = new();

    /// <summary>Fail-fast старта (образец TLS arch/14 §2.2.1): Enabled=true
    /// обязан иметь полный S3-комплект; false — подсистема не активна.</summary>
    public bool IsValid()
        => !Enabled
           || (!string.IsNullOrWhiteSpace(S3.Endpoint)
               && !string.IsNullOrWhiteSpace(S3.Bucket)
               && !string.IsNullOrWhiteSpace(S3.AccessKey)
               && !string.IsNullOrWhiteSpace(S3.SecretKey));
}

/// <summary>S3-хранилище бэкапов (arch/19 §5/§7): per-install креды — ТОЛЬКО
/// env воркера (PGW_BACKUP_S3_*), не etcd/не git; PathStyle=true — клиент
/// MinIO-режима.</summary>
public sealed class BackupsS3Options
{
    public string Endpoint { get; set; } = "";

    /// <summary>Регион (опц.; MinIO не требует — null).</summary>
    public string? Region { get; set; }

    public string Bucket { get; set; } = "";

    public string AccessKey { get; set; } = "";

    public string SecretKey { get; set; } = "";

    public bool PathStyle { get; set; } = true;
}

/// <summary>Дефолт per-cluster политики: ключ
/// /pgworker/backups/&lt;C&gt;/policy отсутствует → эти значения (arch/19 §4).</summary>
public sealed class BackupsPolicyOptions
{
    public BackupsRetentionOptions Retention { get; set; } = new();

    public long FullMaxAgeSec { get; set; } = 86400;

    public bool VerifyOnCreate { get; set; } = true;
}

/// <summary>GFS-ретенция полных бэкапов (дни/недели/месяцы, t06).</summary>
public sealed class BackupsRetentionOptions
{
    public int Days { get; set; } = 7;

    public int Weeks { get; set; } = 4;

    public int Months { get; set; } = 6;
}

/// <summary>Staging джобов/агентов бэкапов (arch/19 §6): ephemeral volume с
/// квотой (guard «нет места» → FAILED, реализация t02; null — без квоты).</summary>
public sealed class BackupsStagingOptions
{
    public string Dir { get; set; } = "/backup-staging";

    public long? QuotaBytes { get; set; }
}

/// <summary>Ресурсные лимиты джоба/агента бэкапов (arch/19 §6) →
/// HostConfig.NanoCPUs/Memory; null — без лимита (образец request_* нод,
/// arch/14 §2.4 п.4). Cpu — ядра, Mem — байты.</summary>
public sealed class BackupsAgentOptions
{
    public double? Cpu { get; set; }

    public long? Mem { get; set; }
}
```

- [ ] **Step 4: Прогнать тест опций.** Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter FullyQualifiedName~BackupsOptionsTests`. Expected: PASS (5 тестов).

- [ ] **Step 5: Wiring в Program.cs.** В `src/PgWorker.App/Program.cs` цепочку валидации (строки с `.Validate(o => o.Api.Tls.AllowInsecureHttp …)`) дополнить перед `.ValidateOnStart();`:

```csharp
    // Подсистема бэкапов (arch/19, t01): fail-fast включения без S3-комплекта;
    // default Enabled=false — подсистема не активна, поведение не меняется.
    .Validate(o => o.Backups.IsValid(),
        "PgWorker:Backups: Enabled=true требует непустые PgWorker:Backups:S3:Endpoint/Bucket/AccessKey/SecretKey (env PGW_BACKUP_S3_*, arch/19 §7)")
```

- [ ] **Step 6: Дефолтная секция в appsettings.json.** В `src/PgWorker.App/appsettings.json` заменить блок `"Api"` (внутри `"PgWorker"`) — добавить после него секцию Backups (S3-креды в конфиге НЕ размещаем — только env). Было:

```json
    "Api": {
      "AdvertiseUrl": "",
      "EnableSeedEndpoint": false,
      "Tls": { "AllowInsecureHttp": false }
    }
```

Стало:

```json
    "Api": {
      "AdvertiseUrl": "",
      "EnableSeedEndpoint": false,
      "Tls": { "AllowInsecureHttp": false }
    },
    "Backups": {
      "Enabled": false
    }
```

- [ ] **Step 7: Сборка решения без warnings.** Run: `dotnet build src/PgWorker.slnx -c Debug`. Expected: Build succeeded, 0 warnings (TreatWarningsAsErrors — упадёт при warning).

- [ ] **Step 8: Commit.**

```bash
git add src/PgWorker.App/Options.cs src/PgWorker.App/Program.cs src/PgWorker.App/appsettings.json src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs
git commit -m "feat(pgworker): каркас конфигурации бэкапов PgWorker:Backups (t01, arch/19)

Enabled=false default (поведение воркера не меняется), S3/Policy/Staging/
Agent-секции, fail-fast Enabled=true без S3-комплекта; юнит-тесты."
```

**Выход:** валидируемая секция `PgWorker:Backups` в App; тесты зелёные; поведение не изменилось (default `Enabled=false`).

**Проверка:** Steps 4, 7 зелёные; в `appsettings.json` секция есть, S3-кредов нет.

**Связь со spec:** §2 п.5, §4.2 (options + валидация; место — см. уточнение в Global Constraints), §5 п.2 — критерий приёмки 3 (часть), 5 (часть).

---

### Task 4: Модель и парсер etcd `/pgworker/backups/*` в PgWorker.Etcd

**Вход (предусловие):** Task 3 слит (юнит-проект уже ссылается на PgWorker.Etcd — новых ссылок не надо).

**Действие (файлы/изменения):** модель-records + статический парсер по образцу `ClusterSnapshotParser` (толерантность: битое → parseErrors с пропуском, неизвестное → игнор), фикстура, тесты.

**Files:**
- Create: `src/PgWorker.Etcd/Parsing/BackupsModel.cs`
- Create: `src/PgWorker.Etcd/Parsing/BackupsParser.cs`
- Create: `src/tests/PgWorker.UnitTests/EtcdFixtures/backups-full.json`
- Test: Create: `src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs`

**Interfaces:**
- Consumes: `Kv(string Key, string Value, ulong ModRevision)` из `PgWorker.Etcd.Client`; `Result<T>` из `PgWorker.Core`; канон arch/19 §4.
- Produces (для t02+ — потребители модели):
  - `BackupsParser.Parse(IReadOnlyList<Kv> kvs, out IReadOnlyList<string> parseErrors) : Result<IReadOnlyList<ClusterBackups>>` (namespace `PgWorker.Etcd.Parsing`);
  - records: `ClusterBackups(string Cluster, BackupPolicy? Policy, IReadOnlyDictionary<string, ShardBackups> Shards)`, `ShardBackups(IReadOnlyList<FullBackupState> Full, WalStreamState? Wal)`, `FullBackupState(string Id, FullBackupStatus State, string Node, BackupSourceRole Role, long StartedUnix, long? FinishedUnix, string WalStartSegment, long? SizeBytes, string? Error, BackupVerify? Verify)`, `WalStreamState(WalStreamStatus State, string Slot, string MasterNode, string ChainStartSegment, string LastReceivedSegment, string LastUploadedSegment, long? LastUploadedUnix, long? LagSegments, string? Error)`, `BackupPolicy(int RetentionDays, int RetentionWeeks, int RetentionMonths, long FullMaxAgeSec, bool VerifyOnCreate)`, `BackupVerify(BackupVerifyStatus State, long? CheckedUnix)`;
  - enums: `FullBackupStatus { Planned, Running, Uploading, Completed, Failed, Deleting }`, `BackupSourceRole { Replica, Master }`, `WalStreamStatus { Active, Degraded, Stopped }`, `BackupVerifyStatus { Pending, Ok, Failed }`.

Правила парсинга (arch/19 §4 + образец снапшот-парсеров): неизвестные ключи префикса — игнор; битый JSON / неизвестное `state`/`role` / отсутствие обязательного поля — запись в `parseErrors` и пропуск ЗАПИСИ (остальное парсится); `policy` отсутствует → `Policy=null` без ошибки; `wal` отсутствует → `Wal=null`; `verify` отсутствует → `Verify=null`; битый `verify.state` → ошибка + `Verify=null` (запись жива); кластеры/шарды сортируются по имени (Ordinal), полные — по `Id` (Ordinal). `Result` всегда Success (ошибки — через `parseErrors`, как `ParseClusters`).

- [ ] **Step 1: Написать падающий тест + фикстуру (TDD).** Создать фикстуру `src/tests/PgWorker.UnitTests/EtcdFixtures/backups-full.json`:

```json
[
  {"key":"/pgworker/backups/demo/policy","value":"{\"retention\":{\"days\":14,\"weeks\":8,\"months\":12},\"full_max_age_sec\":43200,\"verify\":{\"on_create\":false}}","modRevision":10},
  {"key":"/pgworker/backups/demo/s1/full/20260908030000Z","value":"{\"state\":\"COMPLETED\",\"node\":\"pgw-demo-s1-2\",\"role\":\"replica\",\"started_unix\":1757290800,\"finished_unix\":1757294400,\"wal_start_segment\":\"000000010000000000000042\",\"size_bytes\":104857600,\"verify\":{\"state\":\"OK\",\"checked_unix\":1757294500}}","modRevision":50},
  {"key":"/pgworker/backups/demo/s1/full/20260909030000Z","value":"{\"state\":\"FAILED\",\"node\":\"pgw-demo-s1-2\",\"role\":\"replica\",\"started_unix\":1757377200,\"wal_start_segment\":\"0000000100000000000000AA\",\"error\":\"staging: no space left on device\"}","modRevision":70},
  {"key":"/pgworker/backups/demo/s1/wal","value":"{\"state\":\"ACTIVE\",\"slot\":\"pgw_bkp_demo_s1\",\"master_node\":\"pgw-demo-s1-1\",\"chain_start_segment\":\"000000010000000000000042\",\"last_received_segment\":\"0000000100000000000000C3\",\"last_uploaded_segment\":\"0000000100000000000000C2\",\"last_uploaded_unix\":1757377213,\"lag_segments\":1}","modRevision":80},
  {"key":"/pgworker/backups/demo/s2/full/20260908030000Z","value":"{\"state\":\"UPLOADING\",\"node\":\"pgw-demo-s2-2\",\"role\":\"replica\",\"started_unix\":1757290800,\"wal_start_segment\":\"0000000100000000000000010\"}","modRevision":60},
  {"key":"/pgworker/backups/shop/policy","value":"{\"retention\":","modRevision":90},
  {"key":"/pgworker/backups/shop/s1/full/20260907030000Z-2","value":"{\"state\":\"DELETING\",\"node\":\"pgw-shop-s1-1\",\"role\":\"master\",\"started_unix\":1757118000,\"wal_start_segment\":\"000000010000000000000001\"}","modRevision":100},
  {"key":"/pgworker/backups/shop/s1/wal","value":"{\"state\":\"DEGRADED\",\"slot\":\"pgw_bkp_shop_s1\",\"master_node\":\"pgw-shop-s1-1\",\"chain_start_segment\":\"000000010000000000000001\",\"last_received_segment\":\"000000010000000000000005\",\"last_uploaded_segment\":\"000000010000000000000003\",\"last_uploaded_unix\":1757119000,\"lag_segments\":2,\"error\":\"upload: connection reset\"}","modRevision":110},
  {"key":"/pgworker/backups/shop/s1/unknown-leaf","value":"x","modRevision":120},
  {"key":"/pgworker/backups/shop/s1/full/broken-json","value":"{\"state\":","modRevision":130},
  {"key":"/clusters/demo/config","value":"{}","modRevision":1}
]
```

Создать тест `src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs`:

```csharp
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Etcd;

// Парсер /pgworker/backups/* (arch/19 §4, t01): полный набор ключей,
// битые значения — parseErrors + пропуск записи, неизвестные ключи и чужие
// префиксы — игнор (толерантность снапшот-парсеров).
public class BackupsParserTests
{
    [Fact]
    public void Parse_FullFixture_ClustersPolicyFullsWal()
    {
        // Arrange — два кластера: demo (policy + 2 полных + WAL), shop (битый
        // policy, DELETING-full с суффиксом коллизии, DEGRADED WAL).
        var kvs = EtcdFixtures.LoadKv("backups-full.json");

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — кластеры отсортированы по имени; demo разобран полностью.
        result.IsSuccess.Should().BeTrue();
        var demo = result.Value.Should().Contain(c => c.Cluster == "demo").Subject;
        demo.Policy.Should().NotBeNull();
        demo.Policy!.RetentionDays.Should().Be(14);
        demo.Policy.RetentionWeeks.Should().Be(8);
        demo.Policy.RetentionMonths.Should().Be(12);
        demo.Policy.FullMaxAgeSec.Should().Be(43200);
        demo.Policy.VerifyOnCreate.Should().BeFalse();
        var s1 = demo.Shards.Should().ContainKey("s1").WhoseValue;
        s1.Full.Should().HaveCount(2);
        s1.Full[0].Id.Should().Be("20260908030000Z"); // сортировка по Id (Ordinal)
        s1.Full[0].State.Should().Be(FullBackupStatus.Completed);
        s1.Full[0].Role.Should().Be(BackupSourceRole.Replica);
        s1.Full[0].Node.Should().Be("pgw-demo-s1-2");
        s1.Full[0].StartedUnix.Should().Be(1757290800);
        s1.Full[0].FinishedUnix.Should().Be(1757294400);
        s1.Full[0].WalStartSegment.Should().Be("000000010000000000000042");
        s1.Full[0].SizeBytes.Should().Be(104857600);
        s1.Full[0].Verify.Should().NotBeNull();
        s1.Full[0].Verify!.State.Should().Be(BackupVerifyStatus.Ok);
        s1.Full[0].Verify.CheckedUnix.Should().Be(1757294500);
        s1.Full[1].State.Should().Be(FullBackupStatus.Failed);
        s1.Full[1].Error.Should().Contain("no space");
        s1.Full[1].Verify.Should().BeNull(); // verify отсутствует → null
        s1.Wal.Should().NotBeNull();
        s1.Wal!.State.Should().Be(WalStreamStatus.Active);
        s1.Wal.Slot.Should().Be("pgw_bkp_demo_s1");
        s1.Wal.MasterNode.Should().Be("pgw-demo-s1-1");
        s1.Wal.ChainStartSegment.Should().Be("000000010000000000000042");
        s1.Wal.LastReceivedSegment.Should().Be("0000000100000000000000C3");
        s1.Wal.LastUploadedSegment.Should().Be("0000000100000000000000C2");
        s1.Wal.LastUploadedUnix.Should().Be(1757377213);
        s1.Wal.LagSegments.Should().Be(1);
        demo.Shards.Should().ContainKey("s2")
            .WhoseValue.Full[0].State.Should().Be(FullBackupStatus.Uploading);
        // demo разобран без ошибок (битые значения есть только у shop — тест 2).
        errors.Should().NotContain(e => e.Contains("/pgworker/backups/demo"));
    }

    [Fact]
    public void Parse_MalformedValues_SkippedWithErrors()
    {
        // Arrange — битые policy и full у shop; demo жив.
        var kvs = EtcdFixtures.LoadKv("backups-full.json");

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — битые записи в parseErrors и пропущены; живые на месте.
        result.IsSuccess.Should().BeTrue();
        errors.Should().Contain(e => e.Contains("/pgworker/backups/shop/policy"));
        errors.Should().Contain(e => e.Contains("/pgworker/backups/shop/s1/full/broken-json"));
        var shop = result.Value.Should().Contain(c => c.Cluster == "shop").Subject;
        shop.Policy.Should().BeNull(); // битый policy → null (дефолт — у потребителя)
        shop.Shards["s1"].Full.Should().ContainSingle(); // остался только DELETING
        shop.Shards["s1"].Full[0].Id.Should().Be("20260907030000Z-2"); // суффикс коллизии
        shop.Shards["s1"].Full[0].State.Should().Be(FullBackupStatus.Deleting);
        shop.Shards["s1"].Full[0].Role.Should().Be(BackupSourceRole.Master); // fallback-факт
    }

    [Fact]
    public void Parse_UnknownKeysAndForeignPrefix_Ignored()
    {
        // Arrange — unknown-leaf внутри префикса + чужой /clusters/-ключ.
        var kvs = EtcdFixtures.LoadKv("backups-full.json");

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — ни ошибки, ни записи: неизвестное — игнор (обратная
        // совместимость), /clusters/ — не наш префикс.
        result.IsSuccess.Should().BeTrue();
        errors.Should().NotContain(e => e.Contains("unknown-leaf"));
        errors.Should().NotContain(e => e.Contains("/clusters/"));
        result.Value.Should().HaveCount(2); // demo + shop, /clusters/demo не стал кластером
    }

    [Fact]
    public void Parse_NoPolicyNoWal_NullsWithoutErrors()
    {
        // Arrange — кластер без policy и wal: только один full.
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/solo/x1/full/20260908030000Z",
                "{\"state\":\"PLANNED\",\"node\":\"pgw-solo-x1-2\",\"role\":\"replica\",\"started_unix\":1757290800,\"wal_start_segment\":\"000000010000000000000001\"}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — отсутствующие опциональные ключи — не ошибка.
        result.IsSuccess.Should().BeTrue();
        errors.Should().BeEmpty();
        var solo = result.Value.Should().ContainSingle().Subject;
        solo.Cluster.Should().Be("solo");
        solo.Policy.Should().BeNull();
        solo.Shards["x1"].Wal.Should().BeNull();
        solo.Shards["x1"].Full.Should().ContainSingle()
            .Which.State.Should().Be(FullBackupStatus.Planned);
    }

    [Fact]
    public void Parse_UnknownState_RecordSkippedWithError()
    {
        // Arrange — неизвестное state (будущая версия воркера писала новое).
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/c1/x1/full/20260908030000Z",
                "{\"state\":\"ARCHIVED\",\"node\":\"n1\",\"role\":\"replica\",\"started_unix\":1,\"wal_start_segment\":\"000000010000000000000001\"}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — запись пропущена с диагностикой, не исключение.
        result.IsSuccess.Should().BeTrue();
        errors.Should().Contain(e => e.Contains("state"));
        result.Value.Should().ContainSingle().Which.Shards["x1"].Full.Should().BeEmpty();
    }
}
```

- [ ] **Step 2: Прогнать — убедиться, что падает.** Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter FullyQualifiedName~BackupsParserTests`. Expected: FAIL — CS0103/CS0246 (`BackupsParser` не существует).

- [ ] **Step 3: Реализовать модель.** Создать `src/PgWorker.Etcd/Parsing/BackupsModel.cs`:

```csharp
namespace PgWorker.Etcd.Parsing;

// Модель подсистемы бэкапов (arch/19 §4, t01): префикс /pgworker/backups/*
// пишет ТОЛЬКО PgWorker (клэйм <C>), панель читает (02 §2.3.1); потребители в
// воркере — t02+. Значения-строки etcd → типизированные records.

/// <summary>Состояние полного бэкапа (etcd state, arch/19 §4).</summary>
public enum FullBackupStatus
{
    Planned,
    Running,
    Uploading,
    Completed,
    Failed,
    Deleting,
}

/// <summary>Источник полного бэкапа: штатно реплика, fallback — мастер
/// (журнал-факт I/O одной операции, arch/19 §6).</summary>
public enum BackupSourceRole
{
    Replica,
    Master,
}

/// <summary>Состояние WAL-потока шарда (arch/19 §4).</summary>
public enum WalStreamStatus
{
    Active,
    Degraded,
    Stopped,
}

/// <summary>Статус проверки полного (pg_verifybackup — t04, arch/19 §8).</summary>
public enum BackupVerifyStatus
{
    Pending,
    Ok,
    Failed,
}

/// <summary>Per-cluster политика бэкапов (ключ
/// /pgworker/backups/&lt;C&gt;/policy); отсутствует → дефолт конфига
/// PgWorker:Backups:Policy у потребителя (t02+).</summary>
/// <param name="RetentionDays">GFS: хранить N дневных (t06).</param>
/// <param name="RetentionWeeks">GFS: N недельных.</param>
/// <param name="RetentionMonths">GFS: N месячных.</param>
/// <param name="FullMaxAgeSec">Окно суточного алерта «нет валидного полного» (t02).</param>
/// <param name="VerifyOnCreate">Проверять полный сразу после создания (t04).</param>
public sealed record BackupPolicy(
    int RetentionDays, int RetentionWeeks, int RetentionMonths,
    long FullMaxAgeSec, bool VerifyOnCreate);

/// <summary>Результат проверки полного: состояние + время последней проверки.</summary>
public sealed record BackupVerify(BackupVerifyStatus State, long? CheckedUnix);

/// <summary>Один полный бэкап шарда (ключ
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/full/&lt;id&gt;, id=YYYYMMDDHHMMSSZ
/// сортируемый, коллизия — суффикс -2/-3, arch/19 §2).</summary>
public sealed record FullBackupState(
    string Id,
    FullBackupStatus State,
    string Node,
    BackupSourceRole Role,
    long StartedUnix,
    long? FinishedUnix,
    string WalStartSegment,
    long? SizeBytes,
    string? Error,
    BackupVerify? Verify);

/// <summary>WAL-поток шарда (ключ /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal):
/// агент pg_receivewal (t03); инвариант непрерывности chain_start →
/// last_uploaded без дыр (arch/19 §3).</summary>
public sealed record WalStreamState(
    WalStreamStatus State,
    string Slot,
    string MasterNode,
    string ChainStartSegment,
    string LastReceivedSegment,
    string LastUploadedSegment,
    long? LastUploadedUnix,
    long? LagSegments,
    string? Error);

/// <summary>Бэкапы одного шарда: полные (сортированы по Id) + WAL-поток
/// (null — ключа нет: агент не поднимался, t03).</summary>
public sealed record ShardBackups(
    IReadOnlyList<FullBackupState> Full,
    WalStreamState? Wal);

/// <summary>Бэкапы кластера: политика (null — дефолт конфига) + шарды.</summary>
public sealed record ClusterBackups(
    string Cluster,
    BackupPolicy? Policy,
    IReadOnlyDictionary<string, ShardBackups> Shards);
```

- [ ] **Step 4: Реализовать парсер.** Создать `src/PgWorker.Etcd/Parsing/BackupsParser.cs`:

```csharp
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Etcd.Parsing;

// Парсер префикса /pgworker/backups/ в модель подсистемы бэкапов (arch/19
// §4, t01) по образцу ClusterSnapshotParser: чистые функции Kv[] → модель;
// битые значения — в parseErrors с пропуском записи (не исключение), неизвестные
// ключи — игнор (обратная совместимость). Подключение к снапшоту воркера —
// чтение без изменений поведения (потребители — t02+).
public static class BackupsParser
{
    public static Result<IReadOnlyList<ClusterBackups>> Parse(
        IReadOnlyList<Kv> kvs, out IReadOnlyList<string> parseErrors)
    {
        var errors = new List<string>();
        var accs = new Dictionary<string, ClusterAcc>();

        foreach (var kv in kvs)
        {
            // "/pgworker/backups/<C>/…" → ["", "pgworker", "backups", <C>, …]
            var segments = kv.Key.Split('/');
            if (segments.Length < 5
                || segments[1] != "pgworker"
                || segments[2] != "backups"
                || segments[3].Length == 0)
            {
                continue; // чужой префикс (в т.ч. /clusters/) — не наша забота
            }

            var acc = GetOrAdd(accs, segments[3], static name => new ClusterAcc(name));
            switch (segments.Length)
            {
                // "/pgworker/backups/<C>/policy"
                case 5 when segments[4] == "policy":
                    acc.PolicyRaw = kv.Value;
                    break;

                // "/pgworker/backups/<C>/<X>/full/<id>"
                case 7 when segments[4].Length > 0
                    && segments[5] == "full"
                    && segments[6].Length > 0:
                    GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc())
                        .Fulls.Add((segments[6], kv.Value));
                    break;

                // "/pgworker/backups/<C>/<X>/wal"
                case 6 when segments[4].Length > 0 && segments[5] == "wal":
                    GetOrAdd(acc.Shards, segments[4], static _ => new ShardAcc()).WalRaw = kv.Value;
                    break;

                default:
                    // система развивается — неизвестный ключ не ошибка, просто игнор
                    break;
            }
        }

        var clusters = accs.Values
            .OrderBy(a => a.Name, StringComparer.Ordinal)
            .Select(a => BuildCluster(a, errors))
            .ToList();

        parseErrors = errors;
        return Result<IReadOnlyList<ClusterBackups>>.Success(clusters);
    }

    private sealed class ShardAcc
    {
        public readonly List<(string Id, string Raw)> Fulls = [];

        public string? WalRaw;
    }

    private sealed class ClusterAcc(string name)
    {
        public readonly string Name = name;

        public string? PolicyRaw;

        public readonly Dictionary<string, ShardAcc> Shards = [];
    }

    private static ClusterBackups BuildCluster(ClusterAcc acc, List<string> errors)
    {
        var policy = TryParsePolicy(acc.Name, acc.PolicyRaw, errors);
        var shards = acc.Shards
            .OrderBy(pair => pair.Key, StringComparer.Ordinal)
            .ToDictionary(
                pair => pair.Key,
                pair => new ShardBackups(
                    pair.Value.Fulls
                        .Select(f => TryParseFull(acc.Name, pair.Key, f.Id, f.Raw, errors))
                        .Where(f => f is not null)
                        .Select(f => f!)
                        .OrderBy(f => f.Id, StringComparer.Ordinal)
                        .ToList(),
                    TryParseWal(acc.Name, pair.Key, pair.Value.WalRaw, errors)));
        return new ClusterBackups(acc.Name, policy, shards);
    }

    // policy: отсутствует → null без ошибки (дефолт — PgWorker:Backups:Policy);
    // отсутствующие поля — дефолты канона (толерантность будущих версий).
    private static BackupPolicy? TryParsePolicy(string cluster, string? raw, List<string> errors)
    {
        if (raw is null)
            return null;

        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var days = 7;
            var weeks = 4;
            var months = 6;
            if (root.TryGetProperty("retention", out var retention)
                && retention.ValueKind == JsonValueKind.Object)
            {
                days = (int?)ReadLong(retention, "days") ?? days;
                weeks = (int?)ReadLong(retention, "weeks") ?? weeks;
                months = (int?)ReadLong(retention, "months") ?? months;
            }

            var verifyOnCreate = true;
            if (root.TryGetProperty("verify", out var verify)
                && verify.ValueKind == JsonValueKind.Object
                && verify.TryGetProperty("on_create", out var flag))
                verifyOnCreate = flag.ValueKind is JsonValueKind.True or JsonValueKind.String
                    && flag.ToString() is "true" or "True";

            return new BackupPolicy(
                days, weeks, months,
                ReadLong(root, "full_max_age_sec") ?? 86400,
                verifyOnCreate);
        }
        catch (JsonException)
        {
            errors.Add($"/pgworker/backups/{cluster}/policy: битый JSON");
            return null;
        }
    }

    // full/<id>: обязательны state/node/role/started_unix/wal_start_segment;
    // неизвестное state/role — пропуск записи с диагностикой; verify —
    // опциональный (битный verify.state → Verify=null, запись жива).
    private static FullBackupState? TryParseFull(
        string cluster, string shard, string id, string raw, List<string> errors)
    {
        var key = $"/pgworker/backups/{cluster}/{shard}/full/{id}";
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var state = ReadString(root, "state") switch
            {
                "PLANNED" => FullBackupStatus.Planned,
                "RUNNING" => FullBackupStatus.Running,
                "UPLOADING" => FullBackupStatus.Uploading,
                "COMPLETED" => FullBackupStatus.Completed,
                "FAILED" => FullBackupStatus.Failed,
                "DELETING" => FullBackupStatus.Deleting,
                _ => (FullBackupStatus?)null,
            };
            var role = ReadString(root, "role") switch
            {
                "replica" => BackupSourceRole.Replica,
                "master" => BackupSourceRole.Master,
                _ => (BackupSourceRole?)null,
            };
            var startedUnix = ReadLong(root, "started_unix");
            var node = ReadString(root, "node");
            var walStart = ReadString(root, "wal_start_segment");
            if (state is null || role is null || startedUnix is null
                || string.IsNullOrEmpty(node) || string.IsNullOrEmpty(walStart))
            {
                errors.Add($"{key}: битый JSON или неизвестное state/role, обязательное поле отсутствует");
                return null;
            }

            BackupVerify? verify = null;
            if (root.TryGetProperty("verify", out var verifyEl)
                && verifyEl.ValueKind == JsonValueKind.Object)
            {
                var verifyState = ReadString(verifyEl, "state") switch
                {
                    "PENDING" => BackupVerifyStatus.Pending,
                    "OK" => BackupVerifyStatus.Ok,
                    "FAILED" => BackupVerifyStatus.Failed,
                    _ => (BackupVerifyStatus?)null,
                };
                if (verifyState is null)
                    errors.Add($"{key}: неизвестное verify.state — verify пропущен");
                else
                    verify = new BackupVerify(verifyState.Value, ReadLong(verifyEl, "checked_unix"));
            }

            return new FullBackupState(
                id, state.Value, node, role.Value, startedUnix.Value,
                ReadLong(root, "finished_unix"), walStart,
                ReadLong(root, "size_bytes"), ReadString(root, "error"), verify);
        }
        catch (JsonException)
        {
            errors.Add($"{key}: битый JSON");
            return null;
        }
    }

    // wal: обязательны state/slot/master_node/цепочка сегментов/last_uploaded_unix;
    // опциональны lag_segments/error.
    private static WalStreamState? TryParseWal(
        string cluster, string shard, string? raw, List<string> errors)
    {
        if (raw is null)
            return null; // нет ключа — агент не поднимался (t03), не ошибка

        var key = $"/pgworker/backups/{cluster}/{shard}/wal";
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            var state = ReadString(root, "state") switch
            {
                "ACTIVE" => WalStreamStatus.Active,
                "DEGRADED" => WalStreamStatus.Degraded,
                "STOPPED" => WalStreamStatus.Stopped,
                _ => (WalStreamStatus?)null,
            };
            var slot = ReadString(root, "slot");
            var masterNode = ReadString(root, "master_node");
            var chainStart = ReadString(root, "chain_start_segment");
            var lastReceived = ReadString(root, "last_received_segment");
            var lastUploaded = ReadString(root, "last_uploaded_segment");
            var lastUploadedUnix = ReadLong(root, "last_uploaded_unix");
            if (state is null || string.IsNullOrEmpty(slot) || string.IsNullOrEmpty(masterNode)
                || string.IsNullOrEmpty(chainStart) || string.IsNullOrEmpty(lastReceived)
                || string.IsNullOrEmpty(lastUploaded) || lastUploadedUnix is null)
            {
                errors.Add($"{key}: битый JSON или неизвестное state, обязательное поле отсутствует");
                return null;
            }

            return new WalStreamState(
                state.Value, slot, masterNode, chainStart, lastReceived,
                lastUploaded, lastUploadedUnix,
                ReadLong(root, "lag_segments"), ReadString(root, "error"));
        }
        catch (JsonException)
        {
            errors.Add($"{key}: битый JSON");
            return null;
        }
    }

    // Толерантное чтение полей JSON-значений: строки-числа, отсутствующие поля
    // (копия хелперов ClusterSnapshotParser — общие утилиты не рефакторим в t01).
    private static string? ReadString(JsonElement root, string name)
        => root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty(name, out var element)
            && element.ValueKind is JsonValueKind.String or JsonValueKind.Number
            ? element.ToString()
            : null;

    private static long? ReadLong(JsonElement root, string name)
    {
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty(name, out var element))
            return null;

        return element.ValueKind switch
        {
            JsonValueKind.Number => element.TryGetInt64(out var value) ? value : null,
            JsonValueKind.String when long.TryParse(element.GetString(), out var value) => value,
            _ => null,
        };
    }

    private static TValue GetOrAdd<TKey, TValue>(Dictionary<TKey, TValue> dictionary, TKey key, Func<TKey, TValue> factory)
        where TKey : notnull
    {
        if (!dictionary.TryGetValue(key, out var value))
        {
            value = factory(key);
            dictionary[key] = value;
        }

        return value;
    }
}
```

- [ ] **Step 5: Прогнать тесты парсера.** Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj --filter FullyQualifiedName~BackupsParserTests`. Expected: PASS (5 тестов).

- [ ] **Step 6: Прогнать весь юнит-проект (регрессий нет).** Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`. Expected: PASS — все тесты (существующие + 10 новых), 0 упавших.

- [ ] **Step 7: Commit.**

```bash
git add src/PgWorker.Etcd/Parsing/BackupsModel.cs src/PgWorker.Etcd/Parsing/BackupsParser.cs \
  src/tests/PgWorker.UnitTests/Etcd/BackupsParserTests.cs src/tests/PgWorker.UnitTests/EtcdFixtures/backups-full.json
git commit -m "feat(pgworker-etcd): модель и парсер префикса /pgworker/backups/* (t01, arch/19 §4)

Толерантный парсер по образцу снапшот-парсеров: битое — parseErrors+пропуск,
неизвестное — игнор; юнит-тесты с фикстурой."
```

**Выход:** типизированная модель бэкапов + парсер в `PgWorker.Etcd`; чтение снапшота не тронуто (без изменений поведения воркера).

**Проверка:** Steps 5–6 зелёные; `grep -rn "BackupsParser" src/PgWorker.App` → пусто (в App подключений нет — t02+).

**Связь со spec:** §4.2 (модель/парсер + юнит-тесты), §3.4 — критерий приёмки 3.

---

### Task 5: MinIO в dev-станде (`as-minio`) + сид bucket в `00-up.sh`

**Вход (предусловие):** Task 1 слит (канон для ссылок в комментариях). Порты 9000/9001 в конфигах стенда/deploy свободны.

**Действие (файлы/изменения):** сервис `minio` (container_name `as-minio`, профиль `full`) в compose полного стенда + шаг создания bucket в `00-up.sh` + заметка в README стендовой папки. Место — `dev-stand/adminpanel/docker-compose.yml` (см. уточнение в Global Constraints: «входит в полный стенд 00-up.sh» + префикс `as-` = проект `adminpanel-stand`; `dev-stand/compose.yaml` — стенд части, в полный подъём не входит).

**Files:**
- Modify: `dev-stand/adminpanel/docker-compose.yml` (новый сервис после `etcd`, до `s1a`; + volume `as-minio-data`)
- Modify: `dev-stand/adminpanel/checks/00-up.sh` (новый шаг 1a после «1) etcd жив»)
- Modify: `dev-stand/adminpanel/README.md` (строка профиля full + секция «MinIO»)

**Interfaces:**
- Consumes: канон arch/19 §5 (layout/bucket).
- Produces: контейнер `as-minio` (S3 API `:9000`, консоль `:9001`, креды стендовые `minioadmin`/`minioadmin`), volume `as-minio-data` (имя — по spec §4.3), bucket `pgworker-backups` — потребитель Task 6 (env стенда) и t02+ (S3-клиент воркера).

- [ ] **Step 1: Проверить, что порты 9000/9001 не заняты в конфигах.** Run: `grep -rn "9000\|9001" dev-stand deploy --include="*.yml" --include="*.yaml" | grep -v metrics` → пусто (метрики: 9090/9093 — не пересекаются). И Run: `lsof -nP -iTCP:9000 -iTCP:9001 -sTCP:LISTEN` → пусто или занято несущественным (если занято docker-стендом прошлых прогонов — сначала разобрать стенд: `bash dev-stand/adminpanel/checks/90-down.sh`).

- [ ] **Step 2: Сервис в compose.** В `dev-stand/adminpanel/docker-compose.yml` после сервиса `etcd` (перед комментарием «Шард 1: s1a…») вставить:

```yaml
  # MinIO — S3-хранилище бэкапов (arch/19 §5, t01): каркас подсистемы в
  # стенде; PgWorker (deploy-проект) ходит публикацией host:9000. Креды —
  # стендовые дефолты (ТОЛЬКО локальный стенд; прод — per-install S3, arch/19
  # §7). Bucket pgworker-backups создаёт 00-up.sh (mc mb, идемпотентно).
  minio:
    image: minio/minio:latest
    container_name: as-minio
    profiles: ["full"]
    ports:
      - "9000:9000"  # S3 API
      - "9001:9001"  # консоль
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: ["server", "/data", "--console-address", ":9001"]
    volumes:
      - as-minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5
```

И в секцию `volumes:` в конце файла добавить `as-minio-data:` (после `etcd-data:`).

- [ ] **Step 3: Сид bucket в 00-up.sh.** В `dev-stand/adminpanel/checks/00-up.sh` после блока «1) etcd жив» (строки с `echo "  etcd ready"`) и ПЕРЕД блоком «1b) PgWorker» вставить:

```bash
# 1a) MinIO (S3 бэкапов, arch/19): healthy + стендовый bucket pgworker-backups
#      (идемпотентный сид mc mb --ignore-existing; креды — стендовые дефолты).
for i in $(seq 1 60); do curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1 \
  || { echo "  ❌ as-minio не стал здоровым за 60 c (docker compose logs minio)"; exit 1; }
minio_net="$(docker inspect as-minio -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker run --rm --network "$minio_net" minio/mc:latest \
  sh -c "mc alias set standup http://as-minio:9000 minioadmin minioadmin >/dev/null && mc mb --ignore-existing standup/pgworker-backups >/dev/null" \
  || { echo "  ❌ bucket pgworker-backups не создан (mc против as-minio)"; exit 1; }
echo "  as-minio жив, bucket pgworker-backups готов (:9000 API / :9001 консоль)"
```

- [ ] **Step 4: README-заметка.** В `dev-stand/adminpanel/README.md`: (а) строку таблицы профилей `| full | + s1a/s1b, s2a/s2b, hc1a/hc1b, hc2a/hc2b | live-пробы, failover, e2e |` заменить на `| full | + s1a/s1b, s2a/s2b, hc1a/hc1b, hc2a/hc2b, minio (бэкапы, :9000/:9001) | live-пробы, failover, e2e |`; (б) после секции «Мониторинг (профиль metrics)» добавить:

```markdown
## MinIO (подсистема бэкапов, arch/19)

Профиль `full`: `as-minio` — S3 API `:9000`, консоль `:9001` (стендовые креды
`minioadmin`/`minioadmin`, только локальный стенд); bucket `pgworker-backups`
создаёт `00-up.sh` (`mc mb --ignore-existing`, идемпотентно). Воркер
(deploy-проект) ходит публикацией `host.docker.internal:9000`; включение
подсистемы — с t02 (env-блок `PGW_BACKUP_S3_*` в `deploy/.env.example`).
```

- [ ] **Step 5: Проверить compose-синтаксис.** Run: `docker compose -f dev-stand/adminpanel/docker-compose.yml --profile full config >/dev/null && echo OK`. Expected: `OK` (валидный compose, сервис minio в профиле full). Дополнительно: `docker compose -f dev-stand/adminpanel/docker-compose.yml --profile full config | grep -c "as-minio-data"` → `2` (ссылка в сервисе + объявление volume).

- [ ] **Step 6: Commit.**

```bash
git add dev-stand/adminpanel/docker-compose.yml dev-stand/adminpanel/checks/00-up.sh dev-stand/adminpanel/README.md
git commit -m "feat(stand): as-minio в полном стенде + сид bucket pgworker-backups (t01, arch/19)

Профиль full, :9000 API / :9001 консоль, стендовые креды, healthcheck;
00-up.sh идемпотентно создаёт bucket через mc."
```

**Выход:** полный стенд поднимается с MinIO; bucket создаётся скриптом (проверка подъёмом — Task 9).

**Проверка:** Step 5 `OK`.

**Связь со spec:** §4.3 (dev-stand), §5 п.3 — критерий приёмки 4 (часть).

---

### Task 6: deploy — env-блок `PGW_BACKUP_S3_*` и проброс в контейнер воркера

**Вход (предусловие):** Task 3 слит (секция `PgWorker:Backups` существует и валидируется — пустые S3 при `Enabled=false` валидны).

**Действие (файлы/изменения):** закомментированный блок секретов в `.env.example`, мягкий проброс env в compose-сервис `pgworker`.

**Files:**
- Modify: `deploy/.env.example` (блок в конец файла)
- Modify: `deploy/docker-compose.yml` (environment сервиса `pgworker`)

**Interfaces:**
- Consumes: `BackupsOptions` (Task 3) — env-имена маппятся в `PgWorker__Backups__*`; канон arch/19 §7.
- Produces: env-контракт per-install: `PGW_BACKUPS_ENABLED`, `PGW_BACKUP_S3_ENDPOINT`, `PGW_BACKUP_S3_REGION`, `PGW_BACKUP_S3_BUCKET`, `PGW_BACKUP_S3_ACCESS_KEY`, `PGW_BACKUP_S3_SECRET_KEY` — потребители: стенд (Task 9: дефолты пусто → `Enabled=false`), t02+ (включение).

- [ ] **Step 1: Блок в `.env.example`.** В конец `deploy/.env.example` добавить:

```bash

# Подсистема бэкапов шардов (arch/19, t01 — каркас; процессная логика t02+):
# S3-хранилище per-install, креды ТОЛЬКО env — не etcd/не git (группа
# транспортных секретов arch/14 §4 п.3). Стенд: as-minio (00-up.sh создаёт
# bucket pgworker-backups), креды — стендовые дефолты MinIO. Включается с t02:
# Enabled=true требует полного S3-комплекта (fail-fast старта воркера).
#PGW_BACKUPS_ENABLED=false
#PGW_BACKUP_S3_ENDPOINT=http://host.docker.internal:9000
#PGW_BACKUP_S3_REGION=
#PGW_BACKUP_S3_BUCKET=pgworker-backups
#PGW_BACKUP_S3_ACCESS_KEY=minioadmin
#PGW_BACKUP_S3_SECRET_KEY=minioadmin
```

- [ ] **Step 2: Проброс в compose.** В `deploy/docker-compose.yml` в `environment:` сервиса `pgworker` после строки `PgWorker__Api__EnableSeedEndpoint: ${PGW_API_ENABLE_SEED:-false}` добавить:

```yaml
      # Подсистема бэкапов (arch/19, t01): каркас — Enabled=false default;
      # S3-креды per-install только env (PGW_BACKUP_S3_*, deploy/.env.example).
      # Пустые значения валидны при Enabled=false (валидация старта воркера).
      PgWorker__Backups__Enabled: ${PGW_BACKUPS_ENABLED:-false}
      PgWorker__Backups__S3__Endpoint: ${PGW_BACKUP_S3_ENDPOINT:-}
      PgWorker__Backups__S3__Region: ${PGW_BACKUP_S3_REGION:-}
      PgWorker__Backups__S3__Bucket: ${PGW_BACKUP_S3_BUCKET:-}
      PgWorker__Backups__S3__AccessKey: ${PGW_BACKUP_S3_ACCESS_KEY:-}
      PgWorker__Backups__S3__SecretKey: ${PGW_BACKUP_S3_SECRET_KEY:-}
```

(Мягкие дефолты `:-` обязательны: жёсткие `:?` сломали бы существующие стенды без бэкапов.)

- [ ] **Step 3: Проверить compose без deploy/.env.** Run: `docker compose -f deploy/docker-compose.yml config 2>/dev/null | grep -c "PgWorker__Backups"` → `6` (без env-файла все дефолты — false/пусто, compose валиден; compose использует дефолты `${VAR:-}` без `--env-file`).

- [ ] **Step 4: Commit.**

```bash
git add deploy/.env.example deploy/docker-compose.yml
git commit -m "feat(deploy): env-контракт бэкапов PGW_BACKUP_S3_* + проброс PgWorker__Backups__* в воркер (t01, arch/19 §7)"
```

**Выход:** per-install env-контракт бэкапов в deploy; воркер получает секцию (пусто/`Enabled=false` — без изменения поведения).

**Проверка:** Step 3 → `6`.

**Связь со spec:** §4.3 (deploy), §3.6 — критерий приёмки 4 (часть).

---

### Task 7: Полный юнит-прогон + сборка решения (поведение не изменилось)

**Вход (предусловие):** Tasks 3–6 слиты (весь код-каркас в ветке).

**Действие:** контрольный прогон юнитов PgWorker + сборка всего решения Debug/Release.

**Files:** без изменений (только проверки; падение = возврат в соответствующую задачу).

- [ ] **Step 1: Юниты PgWorker целиком.** Run: `dotnet test src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj -c Debug`. Expected: PASS — 0 упавших (существующие + 10 новых t01).

- [ ] **Step 2: Сборка решения Release (для интеграции/E2E Task 8).** Run: `dotnet build src/PgWorker.slnx -c Release`. Expected: Build succeeded, 0 warnings/errors.

- [ ] **Step 3: Юниты остальных воркеров не задеты.** Run: `dotnet test src/tests/KafkaWorker.UnitTests/KafkaWorker.UnitTests.csproj -c Debug`. Expected: PASS (мы не трогали общие сборки KafkaWorker; проверка «существующие юниты зелёные»).

**Выход:** подтверждено — код-каркас не сломал ни один юнит-проект.

**Проверка:** Steps 1–3 зелёные.

**Связь со spec:** §5 п.4, §8 п.5 — «существующие юниты PgWorker зелёные».

---

### Task 8: Интеграционная серия + E2E-маркер `Scale_AddEmptyShard` на свежем Release + docker-зачистки

**Вход (предусловие):** Task 7 зелёный; docker-хост доступен. Гейт AGENTS.md: тронуты `PgWorker.Etcd`/`PgWorker.App` → docker-E2E на свежем Release обязателен; критерий приёмки 5 spec требует и «существующие юниты/интеграция PgWorker зелёные» — поэтому серия = интеграция → E2E (порядок серий AGENTS.md: юниты → интеграция → E2E, зачистка после КАЖДОЙ).

**Действие:** зачистить поле → прогнать интеграционный проект → зачистить серию → прогнать E2E-маркер → зачистить серию (контейнеры + сети; стенд `as-*`/`adminpanel`, если поднят, — НЕ трогать).

**Files:** без изменений.

- [ ] **Step 1: Зачистка перед сериями.** Если dev-стенд поднят (`docker ps --format '{{.Names}}' | grep -c '^as-'` > 0) — оставить его контейнеры; удалить всё прочее + осиротевшие сети:

```bash
for id in $(docker ps -aq); do
  name="$(docker inspect -f '{{.Name}}' "$id")"
  case "$name" in /as-*|/adminpanel) : ;; *) docker rm -f "$id" ;; esac
done
docker network prune -f
```

Если стенда нет — ожидаемо `docker ps -aq | wc -l` → `0` после зачистки.

- [ ] **Step 2: Интеграционная серия PgWorker.** Run (Release — собран Task 7 Step 2; фикстуры поднимают docker-контейнеры с динамическими портами, таймауты фикстур ≤100 с не меняем):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj -c Release
```

Expected: PASS — 0 упавших (существующая интеграция PgWorker зелёная на коде с каркасом бэкапов; `Enabled=false` — поведение воркера не изменилось).

- [ ] **Step 3: Зачистка после интеграционной серии.** Повторить команды Step 1 (контейнеры кроме `as-*`/`adminpanel` + `docker network prune -f`). Проверка: `docker ps -aq --filter name=pgw- | wc -l` → `0` (при живом стенде `as-*` остаются; при пустом хосте `docker ps -aq | wc -l` → `0`). Никогда не запускать E2E поверх незачищенной интеграции (AGENTS.md).

- [ ] **Step 4: E2E-маркер (команда AGENTS.md дословно).** Run (из корня worktree):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```

Expected: PASS — все тесты фильтра (E2eFixture собирает Release сам; таймауты фикстур ≤100 с — падение должно быть быстрым).

- [ ] **Step 5: Зачистка после E2E-серии.** Повторить команды Step 1 (контейнеры кроме `as-*`/`adminpanel` + `docker network prune -f` — сети `pgw-*-net`/`kfw-net-*` движка ryuk'ом не подбираются, осиротевшие сети исчерпают пул подсетей и уронят следующую серию, в т.ч. стенд-проверку Task 9). Проверка: `docker ps -aq --filter name=pgw- | wc -l` → `0` (при живом стенде `as-*` остаются; при пустом хосте `docker ps -aq | wc -l` → `0`).

**Выход:** критерий приёмки 5 закрыт полностью: интеграция PgWorker зелёная + E2E-гейт AGENTS.md на свежем Release — поведение воркера с каркасом бэкапов (`Enabled=false`) не изменилось; поле чистое для Task 9.

**Проверка:** Steps 2, 4 PASS; Steps 1/3/5 — чистое поле.

**Связь со spec:** §5 п.4, §8 п.5 (юниты/интеграция/E2E-маркер + зачистка).

---

### Task 9: Стенд-проверка: полный подъём с `as-minio` и bucket

**Вход (предусловие):** Tasks 5–6 слиты; Task 8 завершён (поле чистое, docker-хост свободен от тестовых серий).

**Действие:** поднять полный стенд `00-up.sh`, проверить as-minio + bucket + живость PgWorker (воркер с новой секцией Backups стартует), разобрать стенд.

**Files:** без изменений (только проверки; падение = возврат в Task 5/6).

- [ ] **Step 1: Полный подъём.** Run: `bash dev-stand/adminpanel/checks/00-up.sh` (внутри: compose full+kafka+metrics, pgworker из deploy — пересборка образа `pgworker:dev` с кодом каркаса). Expected: последняя строка `✓ стенд поднят (полная система: панель + PG + kafka + PgWorker + мониторинг, контур один)`; среди строк шагов — `as-minio жив, bucket pgworker-backups готов (:9000 API / :9001 консоль)`. Воркер ожил на `/healthz` — значит `PgWorker:Backups` с пустыми S3-дефолтами не уронила валидацию старта (`Enabled=false`).

- [ ] **Step 2: Проверить MinIO и bucket напрямую.** Run:

```bash
curl -fsS http://localhost:9000/minio/health/live >/dev/null && echo "minio-live"
docker inspect --format '{{.State.Health.Status}}' as-minio   # ожидаемо: healthy
docker run --rm --network "$(docker inspect as-minio -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')" \
  minio/mc:latest sh -c "mc alias set s http://as-minio:9000 minioadmin minioadmin >/dev/null && mc ls --json s/pgworker-backups" \
  && echo "bucket-ok"
```

Expected: `minio-live`, `healthy`, вывод `mc ls` (пустой JSON-список или created-запись) + `bucket-ok`.

- [ ] **Step 3: Идемпотентность сида.** Повторно: `bash dev-stand/adminpanel/checks/00-up.sh`. Expected: тот же успех (`mc mb --ignore-existing` не падает на существующем bucket; полный прогон идемпотентен).

- [ ] **Step 4: Разбор стенда + зачистка.** Run: `bash dev-stand/adminpanel/checks/90-down.sh` (затем, если поднимался deploy-воркер: `docker rm -f deploy-pgworker-1 2>/dev/null || true`) и зачистку из Task 8 Step 1 (удалить прочие контейнеры + `docker network prune -f`). Expected: чистое поле (`docker ps -aq | wc -l` → `0` или только намеренно оставленные контейнеры).

**Выход:** доказано — стенд поднимается с `as-minio`, bucket создаётся идемпотентно, воркер со свежей конфигурацией жив.

**Проверка:** Steps 1–3 успешны, Step 4 — чистое поле.

**Связь со spec:** §4.3, §8 п.4 («стенд поднимается с as-minio») + §8 п.5 (docker-зачистка после серий).

---

### Task 10: Мерж-гейт roadmap (правки тем же мерж-коммитом)

**Вход (предусловие):** Tasks 1–9 завершены, ветка готова к мержу в `main`.

**Действие:** удалить задачу `t01-backup-canon` из `arch/roadmap/backup.md` — пункт И `←`-зависимости t02–t07 (правила `arch/roadmap/README.md`). Правки делаются в ветке последним коммитом — мерж-коммит принесёт их в `main` (правило «тем же мерж-коммитом»).

**Files:**
- Modify: `arch/roadmap/backup.md`

- [ ] **Step 1: Удалить пункт t01.** Полностью удалить блок списка «**`t01-backup-canon`** — арх-канон подсистемы бэкапов (новый `arch/17-backups.md`) + spec-скелет: … Зависимостей нет.» (строки ~15–22; шапка уже ссылается на arch/19 из Task 2 и НЕ содержит тега t01 — Task 2 Step 5, поэтому её НЕ трогаем). Вместе с пунктом уходит ПОСЛЕДНЕЕ упоминание «17-backups» в `arch/`.

- [ ] **Step 2: Снять `←`-зависимости.** Два фрагмента «было → стало»:

(а) пункт `t02-backup-full-daily` — убрать хвост `← `t01-backup-canon`.`; остальной текст пункта не меняется.

(б) пункт `t03-backup-wal-stream` — фрагмент в конце пункта переформатировать БЕЗ скобок и стрелки (висячая скобка недопустима). Было:

```markdown
← `t01-backup-canon` (исполнима параллельно с t02: доставка WAL не
  требует готового полного, привязка «нужного» диапазона — задача t06).
```

Стало:

```markdown
Исполнима параллельно с t02: доставка WAL не требует готового полного,
  привязка «нужного» диапазона — задача t06.
```

Зависимости t04–t07 (`← t02…`, `← t03…`) не трогаем — t01 там нет.

- [ ] **Step 3: Проверить чистоту roadmap и arch (полные чеки — только здесь, после удаления пункта).**
  - Run: `grep -rn "t01-backup-canon" arch/roadmap/` → пусто (пункт удалён Step 1, шапка тега не содержит — Task 2 Step 5; в `arch/` вне roadmap тег не встречается; docs/superpowers — история, не трогаем).
  - Run: `grep -rn "17-backups" arch/` → пусто (пункт t01 roadmap был последним носителем «17-backups»; чек, отложенный из Task 1 Step 3 / Task 2 Step 6).

- [ ] **Step 4: Commit (последний в ветке до мержа).**

```bash
git add arch/roadmap/backup.md
git commit -m "chore(roadmap): снять t01-backup-canon (канон и каркас слиты) — мерж-гейт правил roadmap"
```

**Выход:** roadmap содержит только несделанные задачи (t02–t07) без зависимостей от t01; arch полностью свободен от «17-backups» и тега t01.

**Проверка:** Step 3 — оба grep'а пусты.

**Связь со spec:** §4.4 — критерий приёмки 6.

---

## Самопроверка плана (выполнена автором; ревизии после code review)

**Покрытие spec:** §1.1/§3 → Task 1; §4.1 → Task 2; §4.2 (options+валидация; место — см. уточнение в Global Constraints) → Task 3; §4.2 (модель/парсер+тесты) → Task 4; §4.3 (MinIO+сид) → Task 5; §4.3 (deploy env) → Task 6; §5 п.4 → Tasks 7–9 (юниты → интеграция+E2E с зачистками → стенд); §4.4 → Task 10; §8 критерии 1–6 → Tasks 1, 2, 3+4, 5+6, 7+8+9, 10 соответственно (критерий 5: юниты — Task 7, интеграция + E2E-маркер + зачистки — Task 8). Ограничения §6 — в Global Constraints. Пропусков нет.

**Ссылки на arch/19:** везде согласованы со структурой канона из Task 1 (10 разделов): контракт etcd → §4, Online-WAL → §3, S3 layout → §5, HA/лимиты → §6, секреты → §7, карта задач → §8, конфигурация → §9. Ссылки на «spec §N» (например, «дословно из spec §3.4» в тезисах Task 1) — это ссылки на SPEC, не на arch/19, и остаются как есть.

**Порядок чеков «17-backups»/«t01» (двухфазность):** упоминание «`arch/17-backups.md`» живёт в пункте `t01-backup-canon` файла `arch/roadmap/backup.md` до мерж-гейта — поэтому чеки Tasks 1/2 выполняются с исключением `arch/roadmap/backup.md` (Task 1 Step 3, Task 2 Step 6), а полные чеки «17-backups → пусто по `arch/`» и «t01-backup-canon → пусто по `arch/roadmap/`» — только в Task 10 Step 3 после удаления пункта. Шапка `arch/roadmap/backup.md` (Task 2 Step 5) сформулирована БЕЗ тега t01 («канон — [../19-backups.md]»), чтобы grep-чек мерж-гейта был выполним без правки шапки в Task 10.

**Типовая консистентность:** `BackupsOptions.IsValid()` используется в Program.cs (Task 3 Step 5) и тестируется (Task 3 Step 1); сигнатура `BackupsParser.Parse(IReadOnlyList<Kv>, out IReadOnlyList<string>) : Result<IReadOnlyList<ClusterBackups>>` едина между Step 1 (тест) и Step 4 (реализация) Task 4; env-имена `PGW_BACKUP_S3_*`/`PGW_BACKUPS_ENABLED` едины в Tasks 6, 9 (комментарии) и arch/19 §7; volume `as-minio-data` — по spec §4.3 (Tasks 5 Step 2/5).

**Плейсхолдеры:** код тестов/реализаций/команда/фикстура даны полностью; для arch/19 дана полная структура с тезисами (материал — дословно из spec §3, включая фразу про парсер имён сегментов в §3), без TBD/«додумать».
