# План: вынос ownS3 из монорепо pg в самостоятельный репозиторий ../OwnS3

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ownS3 (код, тесты, канон, документация) становится самостоятельным репозиторием `/Users/demakaev/ZCodeProject/OwnS3` (сборка/тесты/образ `owns3:dev` — там), а монорепо pg рвёт кодовую связь (`ProjectReference` на `OwnS3.Protocol`) через внутреннюю клиентскую копию и переключается на готовый внешний образ. Ветка актуализирована merge'ом main (ca7a0c5b): новые owns3-изменения main (t36 — пул S3 ownS3 в бэкапах pg/E2E-контур «Вариант A»/ренейм Minio→S3 в панели; t91 — заголовочные checksum PutObject + кластер-health `/s3/health/cluster`) переносятся в OS3 задачей синхронизации (Task 5.5) ДО приёмки и initial commit.

**Architecture:** единая задача, один dev-flow (подход A spec §7): в ветке `refactor-owns3-extract` worktree pg выполняются и наполнение `../OwnS3` (файловые копии as-is + синхронизация main-изменений + initial commit в отдельный git), и чистка pg (arch-first: сначала канон, затем код). Порядок фаз: Ф0 канон → Ф1 наполнение+синхронизация+приёмка+initial commit `../OwnS3` → Ф2 разрыв связи (клиентская копия протокола в `S3Worker.Core`) → Ф3 чистка pg → Ф4 приёмка обоих репо → Ф5 ревью/мерж.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), xunit.v3, Testcontainers, docker compose, bash.

**Spec:** `docs/superpowers/2026-10-16-owns3-extract/spec.md` (в этом же каталоге; все решения подтверждены пользователем).

## Global Constraints

- Все команды и скрипты — ТОЛЬКО в фоне (`run_in_background: true`), task_id фиксировать; sleep-поллинг не дольше 30 с (AGENTS.base.md §4, §12).
- Копия = копия: никакого рефакторинга кода при переносе, только механические правки путей/ссылок сборки (spec §4.2). Синхронизация main-изменений (Task 5.5) — тем же правилом: идемпотентный rsync поверх наполнения, без ручных доработок кода.
- Канон переносится as-is, адаптация только путей/ссылок; arch-first — сначала перенос/правки канона, затем код (spec §4.1).
- `owns3:dev` — локально собираемый образ, в registry `192.168.0.1:5000` не кладётся никогда; внешние образы — из локального registry (spec §4.6).
- E2E/docker-прогоны: полная самоочистка, guid-изоляция, динамические порты, `BrokerBootSec` ≤ 100 с; после КАЖДОЙ docker-серии — зачистка остатков (контейнеры/сети/тома), следующая серия не запускается поверх незачищенной (AGENTS.md pg).
- Сборка образов для docker — publish на хосте + `COPY` publish-вывода; без sdk-стадий и исходников в контексте (кроме продовых Dockerfile поставки — их здесь нет).
- Комментарии/документация — по-русски, идентификаторы — на английском.
- Коммиты pg — только в feature-ветку `refactor-owns3-extract` worktree (свободно); мерж в `main` и push — ТОЛЬКО по отдельному требованию пользователя. `../OwnS3` — свой git, initial commit по гейту Task 6.
- Тег задачи в `arch/roadmap/` не заводился (refactor, не tNN) — roadmap-гейт мержа: убедиться, что в `arch/roadmap/*.md` нет тега этой задачи (grep `owns3-extract` пуст).
- В рамках задачи НЕ выполняются: перенумерация `arch/` pg, переименование сборок/namespace'ов, изменение версий пакетов, чистка `Directory.Packages.props` (spec §9).
- Счётчики количества тестов нигде не заявляются: серии pg/OS3 изменились (t36/t91 добавили сценарии) — критерий только «зелёный прогон».

## Обозначения путей

- **WT** — worktree pg: `/Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract` (ветка `refactor-owns3-extract`; HEAD = merge-коммит `ca7a0c5b` — main влит поверх нашего коммита ф0 `b4b95f75`).
- **OS3** — новый репозиторий: `/Users/demakaev/ZCodeProject/OwnS3` (git init сделан, ветка `main`, коммитов нет; наполнён задачами 2–5 по коду ДО merge).
- **PG-MAIN** — `/Users/demakaev/ZCodeProject/pg` (главный worktree pg; НЕ трогаем, мерж — отдельный гейт).

Все команды ниже — с абсолютными путями (cwd между вызовами сбрасывается). macOS: `sed -i ''`, `rsync -a`.

## Статус выполнения (фиксация на момент актуализации плана)

| Задача | Статус | Артефакт |
|---|---|---|
| Task 1 (Ф0 канон) | ВЫПОЛНЕНА | коммит pg `b4b95f75` + merge `ca7a0c5b` (DU-конфликты arch/owns3/{02,03,05}, roadmap/owns3 разрешены удалением — актуальные версии main лежат в стороне main: `git show ca7a0c5b^2:<путь>`; UU: arch/24, roadmap/s3 склеены). Примечание: main принёс одну НОВУЮ owns3-href мимо нашей sed-правки — `arch/19-backups.md:758`, чинится Task 11 Step 2 |
| Tasks 2–5 (Ф1 наполнение OS3) | ВЫПОЛНЕНЫ | OS3 наполнен по коду ДО merge main (код/тесты/Shared-дубли/build-файлы/slnx, docker+deploy, docs+история, AGENTS/README/runbook); коммитов в OS3 нет |
| Task 5.5 (синхронизация main-изменений) | ВЫПОЛНЕНА | rsync поверх наполнения (diff -rq пуст, 5 новых файлов на месте, System.IO.Hashing в csproj/props); главы 02/03/05 + roadmap/owns3 из `ca7a0c5b^2` с sed-адаптацией (вкл. паттерн adminpanel); аудит ссылок пуст; `dotnet build src/OwnS3.slnx` зелёный |
| Task 6 (приёмка + initial commit) | ВЫПОЛНЕНА | юниты+Api зелёные; docker-E2E Release зелёный 416/416 (оба падения прошлого исполнителя — Metrics_SeriesPresent и A7 — на синхронизированном коде проходят); зачистка чиста; standalone-контур на свежем образе healthz 200 (стартовая гонка первого curl — прогретый повтор 200), teardown чист; самостоятельность пусто; initial commit OS3 `eb1504b` |
| Tasks 7–15 | НЕ НАЧАТЫ | — |

Пометки `[x]` в задачах 1–5 — выполнено до актуализации; строки/номера там — исторические (на момент исполнения). Исполнитель продолжает с Task 5.5.

## Правило адаптации ссылок (фиксация трактовки spec §10.2)

Ссылки **pg → OS3** — конвенция `../OwnS3/...` (понятийный указатель соседнего репозитория, формулировка spec §1/§6.1; в git-мире межрепозиторные ссылки не вычислимы). Ссылки **OS3 → pg** (канон ownS3 ссылается на каноны pg) — рабочие относительные пути от расположения файла: из `OS3/arch/*.md` — `../../pg/arch/...`, из `OS3/arch/<подкаталог>/*.md` (owns3/, roadmap/) — `../../../pg/arch/...`. Ссылки на референс MinIO (`../../../minio`) — та же глубина от обоих репо (оба лежат в `ZCodeProject/`), правки не требуют.

Whitelist-обоснование (подтверждено ревью): критерий spec §10.2 «ни путей вида `../pg`» буквально ограничен **сборкой/скриптами/конфигурацией**; `arch/*.md` — канон-документация, не сборка/скрипты/конфигурация, исторические упоминания в `docs/superpowers/` spec допускает явно. Документные canon-ссылки OS3→pg (`../../pg/…`, `../../../pg/…`) допустимы; приёмка §10.2 на них не распространяется. Целостность этих ссылок проверяется аудитом: Task 5.5 Step 5 (полный, после замены глав новыми версиями main).

---

## Состав клиентской копии протокола (зафиксирован по факту кодовой базы)

S3Worker (основной код: `S3Worker.Core/OwnS3/{OwnS3NodeClient,SigV4GetSigner,PkiMaterial}.cs`, `S3Worker.Provisioning/{InstanceEnvComposer,PkiStore}.cs`) использует из `OwnS3.Protocol`: `SigV4Core` (HexSha256), `PeerRequestSigner`, `PeerCertificate`, `PeerCertificateAuthority`, `PeerTrustAnchor`. Тесты S3Worker дополнительно: `SigV4HeaderVerifier`+`SigV4Result`, `S3ErrorCode`, `S3RequestModel`, `S3HeaderCollection`, `PeerRequestVerifier`+`PeerVerifyResult`. Транзитивные зависимости по `using`-строкам дают полный состав — **15 файлов**:

| Откуда (`src/OwnS3.Protocol/`) | Куда (`src/S3Worker.Core/OwnS3/Protocol/`) |
|---|
| `Auth/SigV4Core.cs` | `Auth/SigV4Core.cs` |
| `Auth/SigV4HeaderVerifier.cs` | `Auth/SigV4HeaderVerifier.cs` |
| `Auth/AuthorizationHeaderParser.cs` | `Auth/AuthorizationHeaderParser.cs` |
| `Auth/CanonicalRequestBuilder.cs` | `Auth/CanonicalRequestBuilder.cs` |
| `Auth/PayloadHashMode.cs` | `Auth/PayloadHashMode.cs` |
| `PeerAuth/PeerCertificate.cs` | `PeerAuth/PeerCertificate.cs` |
| `PeerAuth/PeerCertificateAuthority.cs` | `PeerAuth/PeerCertificateAuthority.cs` |
| `PeerAuth/PeerRequestSigner.cs` | `PeerAuth/PeerRequestSigner.cs` |
| `PeerAuth/PeerRequestVerifier.cs` | `PeerAuth/PeerRequestVerifier.cs` |
| `PeerAuth/PeerTrustAnchor.cs` | `PeerAuth/PeerTrustAnchor.cs` |
| `Errors/S3ErrorCode.cs` | `Errors/S3ErrorCode.cs` |
| `Errors/S3ProtocolException.cs` | `Errors/S3ProtocolException.cs` |
| `Requests/S3RequestModel.cs` | `Requests/S3RequestModel.cs` |
| `Requests/S3HeaderCollection.cs` | `Requests/S3HeaderCollection.cs` |
| `Uris/UriEncoding.cs` | `Uris/UriEncoding.cs` |

НЕ копируются (не используются S3Worker и его тестами): `Auth/{AwsChunkedReader,AwsChunkedReadingContext,Crc32C,ChecksumConventions,PresignedRequestVerifier,S3PresignedOperations,SigV4Signer}.cs`, `Requests/S3PathParser.cs`, `Validation/*`, `Xml/*`. Таблица стабильна после merge main: diff `2897d19a..main` по `src/OwnS3.Protocol` затрагивает только `Auth/AwsChunkedReader.cs` (трейлерный путь переведён на `ChecksumConventions`) и добавляет `Auth/ChecksumConventions.cs` (НОВЫЙ, t91) — ни один не входит в копию и не нужен её using-зависимостям; diff по `src/tests/S3Worker.*` и `src/S3Worker.*` пуст. Ни один из 15 копируемых файлов не использует `System.IO.Hashing` и не использует `Shared.Core` — значит в `S3Worker.Core.csproj` добавлять пакеты НЕ нужно, только удалить `ProjectReference` на `OwnS3.Protocol`. Namespace `OwnS3.Protocol.*` в копии сохраняется (spec §6.2, минимум диффа).

## Переносимые каталоги docs/superpowers (13) и остающиеся (8)

Переносятся в OS3: `2026-10-10-t35-owns3-canon`, `2026-10-10-t36-owns3-protocol`, `2026-10-10-t37-owns3-storage-objects`, `2026-10-10-t38-owns3-multipart`, `2026-10-10-t39-owns3-e2e`, `2026-10-11-t35-owns3-replication`, `2026-10-11-t40-owns3-erasure-coding`, `2026-10-11-t41-owns3-minio-migration`, `2026-10-11-t42-owns3-tls`, `2026-10-13-owns3-ca-identity`, `2026-10-13-t37-owns3-replication-ping`, `2026-10-13-t38-owns3-result-migration`, `2026-10-14-owns3-denylist-replace`.

Остаются в pg: `2026-10-11-t31-s3-clusters-canon`, `2026-10-11-t32-s3-cluster-provisioning`, `2026-10-14-t33-s3-sync-connectivity`, `2026-10-14-t40-s3-cluster-governance`, `2026-10-15-t34-s3-panel`, `2026-10-16-owns3-extract` (эта задача — история pg-репо), а также пришедшие merge'ем main `2026-10-15-t36-s3-pool-integration` и `2026-10-17-t91-owns3-checksum-verify`. Осознанный остаток (периметр приказа закрытый: код+тесты+arch-канон): новые каталоги истории задач НЕ переносятся — t36 — задача pg (пул S3 в бэкапах pg, E2E-контур PgWorker, панель), t91 выполнялась в репо pg; история разработки остаётся в git-логе pg (spec §2.3). Они покрываются исключением `docs/superpowers/` в grep-критерии Task 14 Step 5 (4).

## ВСЕ сборщики ownS3-образа в тестах pg (полный перечень: 5)

После merge main сборщиков ПЯТЬ: два в S3Worker, два в PgWorker, один в AdminPanel. Все ломаются удалением `src/OwnS3.App` + `docker/OwnS3.Dockerfile` (Task 8) и все переводятся на внешний `owns3:dev` (Task 10):

1. **S3Worker, E2e-серия**: `src/tests/S3Worker.IntegrationTests/E2e/S3E2eEnvironment.cs` — const `OwnS3Image = "owns3:dev"`, `OwnS3PublishDir`, publish+build в `BuildImagesAsync` (строки ~402–429). Diff main пуст — строки соответствуют плану.
2. **S3Worker, S3-серия** (интеграции за `PGW_TEST_DOCKER=1`): `src/tests/S3Worker.IntegrationTests/S3/S3OwnS3Image.cs` — класс сборки тега `owns3:e2e-it` (publish `src/OwnS3.App/OwnS3.App.csproj` строка 38, build `docker/OwnS3.Dockerfile` строка 45, поиск корня репо по `docker/OwnS3.Dockerfile` строка 67); потребитель — `S3/S3ClusterFixture.cs` (свойство `OwnS3Image` ~72, литерал `"owns3:e2e-it"` в `S3ProvisioningOptions` ~77, вызов `BuildOnceAsync` ~109). Diff main пуст.
3. **PgWorker, E2E-контур t36 («Вариант A»)**: `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs` — const `OwnS3Image = "owns3:e2e"` (строка 56), `EnsureOwnS3ImageAsync` (строки ~1727–1772: publish `src/OwnS3.App/OwnS3.App.csproj` + build `docker/OwnS3.Dockerfile`, тег `owns3:e2e`); потребители — E2e-сценарии бэкапов (`E2eBackup*`, `E2eSupervisor*`, `E2eRetention*`, `E2eEtcdSnapshotExport*`; контейнер `e2e-owns3`).
4. **PgWorker, интеграции бэкапов**: `src/tests/PgWorker.IntegrationTests/Backups/OwnS3.cs` — класс собственной ноды Fact'а; `private const string Image = E2eEnvironment.OwnS3Image` (наследует тег), свой сборщик `EnsureImageAsync` (строки ~93–137: publish+build). По приказу файл остаётся в pg — значит, перевод на внешний образ обязателен.
5. **AdminPanel, docker-интеграции**: `src/tests/AdminPanel.IntegrationTests/S3NodeContainerFixture.cs` — const `Image = "owns3:e2e"` (строка 31), свой сборщик `EnsureImageAsync` (строки 78–125: publish `src/OwnS3.App/OwnS3.App.csproj` строка 97, build `-f docker/OwnS3.Dockerfile` строка 113), комментарии с тегом `owns3:e2e` на строках 16, 76, 132. Файл остаётся в pg — перевод на внешний образ обязателен.

---

### Task 0: Гейт старта

**Files:** ничего не создаёт.

**Вход:** ветка `refactor-owns3-extract` с влитым main; spec одобрен; план актуализирован.

- [ ] **Step 1.** Проверить состояние worktree:
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract status --short --branch
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract log --oneline -3
  ```
  Ожидание: `## refactor-owns3-extract`; HEAD = `ca7a0c5b Merge branch 'main' into refactor-owns3-extract`; изменений нет (допустим untracked `docs/superpowers/2026-10-16-owns3-extract/` — spec/plan этой задачи, не трогать).
- [ ] **Step 2.** Проверить цель: `git -C /Users/demakaev/ZCodeProject/OwnS3 status --short --branch` → `## No commits yet on main` (наполнение untracked — так и должно быть до initial commit Task 6); `ls /Users/demakaev/ZCodeProject/OwnS3` → `AGENTS.md README.md arch artifacts deploy docker docs src`.
- [ ] **Step 3.** Прочитать `../AGENTS.base.md` (ссылка из корня WT) — базовые правила обязательны для исполнителя.

**Выход:** подтверждена отправная точка: ф0+merge в ветке, OS3 наполнен и без коммитов.
**Проверка:** команды Steps 1–2 вернули ожидаемое.
**Spec:** §2.3 (исходное состояние цели), §8 Фаза 1.

---

### Task 1: Фаза 0 — перенос канона в OS3/arch и правки arch/ pg (arch-first) — ВЫПОЛНЕНО

**Статус: ВЫПОЛНЕНО до актуализации.** Коммит pg `b4b95f75`; поверх влит main (`ca7a0c5b`): DU-конфликты `arch/owns3/{02-operations,03-protocol,05-access-config}.md` и `arch/roadmap/owns3.md` разрешены подтверждённым удалением из pg (наша ветка их уже перенесла); UU `arch/24-s3worker.md` и `arch/roadmap/s3.md` склеены (наши `../OwnS3`-ссылки + контент main: t36-пункт roadmap удалён как слитый, остался `s3-external-targets`; в 24 — href `../OwnS3` + текст main «канон adminpanel/02 §12»). Актуальные ВЕРСИИ main этих четырёх файлов (с изменениями t36/t91) в WT отсутствуют — они лежат в стороне main и переносятся в OS3 задачей **Task 5.5 Steps 3–4**. ⚠️ Известный дефект после merge: проверка Step 8 по текущему WT НЕ пуста — `arch/19-backups.md:758` содержит висячую owns3-href, пришедшую из main мимо нашей sed-правки Step 6 (grep `](owns3/` по arch/ даёт ровно одно вхождение); правка — **Task 11 Step 2** (второй sed). Шаги ниже — исполненные (строки исторические).

**Files:**
- Create (OS3): `arch/README.md`, `arch/01-owns3.md`, `arch/owns3/{01..07}*.md`, `arch/roadmap/README.md`, `arch/roadmap/owns3.md`.
- Delete (WT): `arch/22-owns3.md`, `arch/owns3/` (каталог), `arch/roadmap/owns3.md`.
- Modify (WT): `arch/README.md`, `arch/19-backups.md`, `arch/23-s3-clusters.md`, `arch/24-s3worker.md`, `arch/adminpanel/02-etcd-contract.md`, `arch/adminpanel/04-local-stand.md`, `arch/roadmap/README.md`, `arch/roadmap/s3.md`.

**Вход:** Task 0 пройден.
**Действия:**

- [x] **Step 1. Перенести главы канона as-is:**
  ```bash
  mkdir -p /Users/demakaev/ZCodeProject/OwnS3/arch/owns3 /Users/demakaev/ZCodeProject/OwnS3/arch/roadmap
  rsync -a /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/owns3/ /Users/demakaev/ZCodeProject/OwnS3/arch/owns3/
  cp /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/roadmap/owns3.md /Users/demakaev/ZCodeProject/OwnS3/arch/roadmap/owns3.md
  ```
- [x] **Step 2. `arch/22-owns3.md` → `OS3/arch/01-owns3.md`** (копия + адаптация ссылок; глубина — два уровня вверх, до `ZCodeProject/`):
  ```bash
  cp /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/22-owns3.md /Users/demakaev/ZCodeProject/OwnS3/arch/01-owns3.md
  sed -i '' \
    -e 's|(19-backups.md)|(../../pg/arch/19-backups.md)|g' \
    -e 's|(roadmap/s3.md)|(../../pg/arch/roadmap/s3.md)|g' \
    -e 's|(23-s3-clusters.md)|(../../pg/arch/23-s3-clusters.md)|g' \
    -e 's|(24-s3worker.md)|(../../pg/arch/24-s3worker.md)|g' \
    /Users/demakaev/ZCodeProject/OwnS3/arch/01-owns3.md
  ```
  Заголовок первой строки: `# 22. ownS3: ...` → `# 01. ownS3: ...` (правка вручную, одна строка). Четыре паттерна закрывают ВСЕ markdown-ссылки на pg: `19-backups.md` и тройку со строк 61–62 исходника (`roadmap/s3.md`, `23-s3-clusters.md`, `24-s3worker.md` — в pg остались, глубина не адаптирована без правки); каждого имени ровно одно вхождение в файле (grep на момент плана). Ссылки `(owns3/NN-*.md)` (главы рядом) и `(roadmap/owns3.md)` (трек рядом) остаются валидными — НЕ трогать.
- [x] **Step 3. Адаптировать ссылки глав и roadmap-трека** (глубина из `OS3/arch/owns3/` и `OS3/arch/roadmap/` — ТРИ уровня вверх: `../../../pg/...`; см. «Правило адаптации ссылок»):
  ```bash
  # Главы: ссылки на канон pg + внутренние ссылки на переименованную вводную
  # и на трек s3 (в OS3 arch/roadmap/s3.md не существует).
  sed -i '' \
    -e 's|(../18-metrics.md)|(../../../pg/arch/18-metrics.md)|g' \
    -e 's|(../19-backups.md)|(../../../pg/arch/19-backups.md)|g' \
    -e 's|(../22-owns3.md)|(../01-owns3.md)|g' \
    -e 's|(../roadmap/s3.md)|(../../../pg/arch/roadmap/s3.md)|g' \
    /Users/demakaev/ZCodeProject/OwnS3/arch/owns3/*.md
  # Трек ownS3: ссылка на трек s3 (остался в pg).
  sed -i '' 's|(s3.md)|(../../../pg/arch/roadmap/s3.md)|g' /Users/demakaev/ZCodeProject/OwnS3/arch/roadmap/owns3.md
  ```
  Ссылки `../owns3/06`/`../owns3/07` внутри `arch/roadmap/owns3.md` корректны уже (структура та же) — не трогать. Ссылки глав на `../../docs/…` (только `arch/owns3/07-replication.md:8,10` — grep на момент плана) остаются БЕЗ правки: глубина совпадает (`OS3/arch/owns3/` → `OS3/docs/`), файлы появились в Task 4. Правка `(../22-owns3.md)` → `(../01-owns3.md)` закрывает ссылку вводной из `01-overview.md:3`; `(../roadmap/s3.md)` — ссылку `01-overview.md:49`.
- [x] **Step 4. `OS3/arch/README.md`** — точка входа канона OwnS3 (новый, краткий):
  ```
  # arch/ — канон ownS3

  Единственный канонический источник устройства ownS3 (spec-first): код
  зеркалит его; меняя контракт — сначала arch/, затем код.

  - [01-owns3.md](01-owns3.md) — вводная: собственное S3-хранилище, критерий
    правильности — стандарт S3 API; MinIO — референс.
  - [owns3/](owns3/) — главы канона: 01 overview, 02 operations, 03 protocol,
    04 storage, 05 access-config, 06 erasure, 07 replication.
  - [roadmap/README.md](roadmap/README.md) — правила ведения отложенных задач;
    трек ownS3 — [roadmap/owns3.md](roadmap/owns3.md).

  Смежные каноны (клиенты ownS3) живут в репозитории pg (`../../pg/arch/`).
  ```
- [x] **Step 5. `OS3/arch/roadmap/README.md`** — копия `WT/arch/roadmap/README.md` с адаптацией: таблица треков сводится к одной строке `owns3.md`; в тексте правил заменить упоминания остальных треков pg (`pgworker/kafka/valkey/s3/backup/reliability`) на общую формулировку «треки задач ownS3»; ссылки на `../`-документы pg — по факту содержимого.
- [x] **Step 6. Правки arch/ pg (spec §6.1).** Удаление + sed-правки + ручные правки 19/23/24/adminpanel/roadmap (детали — в git-истории коммита `b4b95f75`):
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract rm -r arch/22-owns3.md arch/owns3 arch/roadmap/owns3.md
  sed -i '' 's|](owns3/|](../OwnS3/arch/owns3/|g' \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/19-backups.md \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/23-s3-clusters.md \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/24-s3worker.md
  sed -i '' 's|docs/owns3-migration\.md|../OwnS3/docs/owns3-migration.md|g' /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/19-backups.md
  sed -i '' 's|docs/owns3-replication-requirements\.md|../OwnS3/docs/owns3-replication-requirements.md|g; s|docs/s3-replication\.md|../OwnS3/docs/s3-replication.md|g' \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/23-s3-clusters.md \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/24-s3worker.md
  sed -i '' 's|arch/owns3/|../OwnS3/arch/owns3/|g' \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/adminpanel/02-etcd-contract.md \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/adminpanel/04-local-stand.md
  sed -i '' \
    -e 's|arch/owns3/|../OwnS3/arch/owns3/|g' \
    -e 's|\.\./owns3/|../OwnS3/arch/owns3/|g' \
    -e 's|docs/owns3-replication-requirements\.md|../OwnS3/docs/owns3-replication-requirements.md|g' \
    -e 's|docs/s3-replication\.md|../OwnS3/docs/s3-replication.md|g' \
    -e 's|arch/22|../OwnS3/arch/01-owns3.md|g' \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/roadmap/s3.md
  ```
  Плюс ручные правки: `arch/23-s3-clusters.md` — «образ `owns3:dev` — внешний, собирается репозиторием `../OwnS3`» и «`CreateCa`/`IssueServer` клиентской копии peer-PKI ownS3 (`S3Worker.Core/OwnS3/Protocol/PeerAuth`; канон — `../OwnS3/arch/owns3/07` §2)»; `arch/roadmap/README.md` — удалена строка трека `owns3.md`; `arch/README.md` — удалён блок 22-owns3. Осознанный остаток: текстовые шорткаты «owns3/NN §X» (без `](…)`) в 23/24 — нумерация глав сохранена, шорткаты указывают на те же главы в `../OwnS3/arch/owns3/`.
- [x] **Step 7. Аудит целостности ссылок канона OS3** (с временным whitelist `../../docs/` — до Task 4) — пусто.
- [x] **Step 8. Проверка ссылок arch/ pg** (все оставшиеся вхождения содержат `../OwnS3`) — пусто НА МОМЕНТ ИСПОЛНЕНИЯ; после merge main непуста (`arch/19-backups.md:758` — новая href из main), закрывается Task 11 Step 2.
- [x] **Step 9. Коммит pg:** `owns3-extract(ф0): ...` = `b4b95f75`.

**Выход:** исполнен и закоммичен; merge `ca7a0c5b` сохранил чистку (актуализация версий глав — Task 5.5; висячая href 19:758 — Task 11).
**Проверка:** `grep -rn "arch/owns3\|22-owns3\|owns3-migration\|owns3-replication-requirements\|docs/s3-replication\|](owns3/\|](\.\./owns3/" arch/ | grep -v "../OwnS3"` в WT — единственное оставшееся вхождение до правки: `arch/19-backups.md:758`; после Task 11 Step 2 — пусто.
**Spec:** §8 Фаза 0, §6.1, §4.1, §10.6.

---

### Task 2: Фаза 1 — код, тесты, Shared-дубли, build-файлы, OwnS3.slnx в OS3 — ВЫПОЛНЕНО

**Статус: ВЫПОЛНЕНО до актуализации** (по коду ДО merge main; код t36/t91 докатывает **Task 5.5 Step 1** — идемпотентный повтор тех же rsync-команд поверх).

**Files:**
- Create (OS3): `src/OwnS3.{App,Pki,Protocol,Replication,Storage}/`, `src/tests/OwnS3.{UnitTests,IntegrationTests}/`, `src/Shared.{Core,Metrics,Tls}/`, `src/tests/Shared.{Core,Metrics}.UnitTests/`, `src/Directory.Build.props`, `src/Directory.Packages.props`, `src/NuGet.Config`, `src/OwnS3.slnx`.

**Вход:** Task 1 (канон уже в OS3).
**Действия:**

- [x] **Step 1. Копировать проекты и build-файлы** (без bin/obj; csproj-файлы не правились — структура `src/` та же, относительные `ProjectReference` валидны):
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  for p in OwnS3.App OwnS3.Pki OwnS3.Protocol OwnS3.Replication OwnS3.Storage Shared.Core Shared.Metrics Shared.Tls; do
    rsync -a --exclude bin --exclude obj "src/$p" /Users/demakaev/ZCodeProject/OwnS3/src/
  done
  mkdir -p /Users/demakaev/ZCodeProject/OwnS3/src/tests
  for t in OwnS3.UnitTests OwnS3.IntegrationTests Shared.Core.UnitTests Shared.Metrics.UnitTests; do
    rsync -a --exclude bin --exclude obj "src/tests/$t" /Users/demakaev/ZCodeProject/OwnS3/src/tests/
  done
  cp src/Directory.Build.props src/Directory.Packages.props src/NuGet.Config /Users/demakaev/ZCodeProject/OwnS3/src/
  ```
  (`Shared.Etcd`/`Shared.Docker` и их тесты НЕ копируются — ownS3 без etcd и docker.sock; `Directory.Packages.props` копируется as-is, чистка версий запрещена spec §9.)
- [x] **Step 2. Создать `OS3/src/OwnS3.slnx`** (по образцу `PgWorker.slnx`): папки `/common/` (Directory.Build.props, Directory.Packages.props, Shared.Core/Metrics/Tls), `/owns3/` (5 проектов OwnS3.*), `/tests/` (4 тестовых проекта) — полный XML в git-истории или `cat /Users/demakaev/ZCodeProject/OwnS3/src/OwnS3.slnx`.
- [x] **Step 3. Сборка:** `dotnet build src/OwnS3.slnx` — `Build succeeded` (повторяется в Task 5.5 Step 6 после синхронизации).

**Выход:** собирающийся solution OS3 (актуализация кода — Task 5.5).
**Проверка:** Step 3 зелёный (на момент исполнения).
**Spec:** §5 (структура), §5.1 (slnx), §2.1 (периметр кода), §4.2 (без рефакторинга).

---

### Task 3: Фаза 1 — docker/ и deploy/ OS3 (образ + standalone-контур) — ВЫПОЛНЕНО

**Статус: ВЫПОЛНЕНО до актуализации.** `docker/OwnS3.Dockerfile` в diff main НЕ менялся — синхронизация не требуется; образ, собранный этим таском, устареет после Task 5.5 — обязательная пересборка в Task 6 Step 4 (и Task 12 Step 2).

**Files:**
- Create (OS3): `docker/OwnS3.Dockerfile`, `deploy/build-image.sh`, `deploy/docker-compose.yml`, `deploy/.env.example`, `deploy/tls/gen.sh`, `deploy/tls/.gitignore`, `.gitignore`.

**Вход:** Task 2 (сборка OS3 зелёная).
**Действия:**

- [x] **Step 1. Dockerfile as-is** (`cp WT/docker/OwnS3.Dockerfile OS3/docker/`).
- [x] **Step 2. `OS3/deploy/build-image.sh`** — единственный способ собрать `owns3:dev`: publish НА ХОСТЕ + docker build c `[PHASE]`+таймингом (полный текст — в git-истории; файл на месте).
- [x] **Step 3. `OS3/deploy/docker-compose.yml`** — standalone-контур: сервис + erasure-профиль `owns3-ec`; без etcd, без docker.sock (полный текст — в git-истории).
- [x] **Step 4. `OS3/deploy/.env.example`** — dev-значения c `:?`-fail-fast.
- [x] **Step 5. `OS3/deploy/tls/gen.sh`** + `deploy/tls/.gitignore`.
- [x] **Step 6. `OS3/.gitignore`** (bin/obj/artifacts/TestResults/deploy/.env).
- [x] **Step 7. Сборка образа:** `bash deploy/build-image.sh` — образ `owns3:dev` создан (устареет после Task 5.5).

**Выход:** `owns3:dev` собирается скриптом OS3; standalone-контур описан.
**Проверка:** `docker image inspect owns3:dev` exit 0 (на момент исполнения).
**Spec:** §5.2, §4.5, §4.6.

---

### Task 4: Фаза 1 — docs/ и история задач в OS3 (копии) — ВЫПОЛНЕНО

**Статус: ВЫПОЛНЕНО до актуализации.** `docs/owns3-migration.md`, `docs/owns3-replication-requirements.md`, `docs/s3-replication.md` в diff main НЕ менялись (проверено: diff `2897d19a..main` по этим путям пуст) — синхронизация docs не требуется. Новые каталоги истории (t36/t91) НЕ переносятся (см. «Переносимые каталоги» — осознанный остаток).

**Files:**
- Create (OS3): `docs/owns3-migration.md`, `docs/owns3-replication-requirements.md`, `docs/s3-replication.md`, `docs/superpowers/<13 каталогов>` (список в шапке плана).

**Вход:** Task 1 (архив канона уже в OS3).

- [x] **Step 1. Копировать доки и историю** (13 каталогов списком из шапки; копии as-is — исторические упоминания pg внутри архива допустимы, spec §10.2).
- [x] **Step 2. Повторный полный аудит ссылок канона OS3 — без whitelist** — пусто.

**Выход:** документация и история задач продублированы в OS3 (удаление из pg — Task 8).
**Проверка:** `ls OS3/docs/superpowers | wc -l` → 13; аудит пуст.
**Spec:** §2.1 (доки/история), §7-A (единая задача).

---

### Task 5: Фаза 1 — AGENTS.md и README.md OS3 — ВЫПОЛНЕНО

**Статус: ВЫПОЛНЕНО до актуализации.** Файлы на месте (`OS3/AGENTS.md`, `OS3/README.md`, `OS3/docs/runbook.md`); правки не требуют (merge main их не касался — это новые файлы OS3).

**Files:**
- Create (OS3): `AGENTS.md`, `README.md`, `docs/runbook.md`.

**Вход:** Tasks 2–4 (наполнение завершено).

- [x] **Step 1. `OS3/AGENTS.md`** — копия AGENTS.md pg, исправленная по spec §5.3 (полный текст — в git-истории).
- [x] **Step 2. `OS3/README.md`** — назначение/quick start/потребителям (spec §5.4).
- [x] **Step 3. `OS3/docs/runbook.md`** — минимальный ранбук (registry, NuGet-ленты, сборка owns3:dev).

**Выход:** OS3 имеет свои рабочие правила и входную документацию.
**Проверка:** файлы существуют; ссылки в README указывают на существующие пути.
**Spec:** §5.3, §5.4, §5, §9.

---

### Task 5.5: Фаза 1 — синхронизация OS3 с owns3-изменениями main (t36/t91)

**Files:**
- Modify (OS3): `src/OwnS3.App/{Handlers/AdminHandlers.cs, Handlers/HashingBodyStream.cs, Handlers/ObjectHandlers.cs, Handlers/OperationValidation.cs, OwnS3.App.csproj, Pipeline/S3Middleware.cs, Program.cs}`, `src/OwnS3.Protocol/Auth/{AwsChunkedReader.cs, ChecksumConventions.cs (новый)}`, `src/OwnS3.Storage/Drives/{ErasureXlDrive.cs, SingleXlDrive.cs}`, `src/tests/OwnS3.UnitTests/{App/HashingBodyStreamChecksumTests.cs (новый), HandlersValidationTests.cs}`, `src/tests/OwnS3.IntegrationTests/{Api/BodyIntegrityScenarios.cs, Api/ClusterHealthScenarios.cs (новый), Api/ClusterHealthEcScenarios.cs (новый), E2e/AwsSdkSimpleScenarios.cs, E2e/HealthMetricsScenarios.cs}`, `arch/owns3/{02-operations.md, 03-protocol.md, 05-access-config.md}`, `arch/roadmap/owns3.md`.

**Вход:** Tasks 2–5 (наполнение OS3 по коду ДО merge); merge `ca7a0c5b` влит (код/тесты в WT актуальны; arch-версии main доступны через `git show ca7a0c5b^2:<путь>` — в WT эти файлы удалены ф0).

**Действия:**

- [ ] **Step 1. Синхронизировать код и тесты** — идемпотентный повтор rsync-цикла Task 2 Step 1 ПОВЕРХ существующего наполнения (rsync без `--delete` обновит изменённое и добавит новое; периметр diff `2897d19a..main`: 11 файлов кода + 7 тестов — 2 юнит + 5 интеграционных, из них 4 новых):
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  for p in OwnS3.App OwnS3.Pki OwnS3.Protocol OwnS3.Replication OwnS3.Storage Shared.Core Shared.Metrics Shared.Tls; do
    rsync -a --exclude bin --exclude obj "src/$p" /Users/demakaev/ZCodeProject/OwnS3/src/
  done
  for t in OwnS3.UnitTests OwnS3.IntegrationTests Shared.Core.UnitTests Shared.Metrics.UnitTests; do
    rsync -a --exclude bin --exclude obj "src/tests/$t" /Users/demakaev/ZCodeProject/OwnS3/src/tests/
  done
  cp src/Directory.Build.props src/Directory.Packages.props src/NuGet.Config /Users/demakaev/ZCodeProject/OwnS3/src/
  ```
  Shared-проекты и build-файлы в diff main не менялись — копирование идемпотентно (нулевое изменение); включены для гарантии полного совпадения.
- [ ] **Step 2. Контроль полноты синхронизации кода:**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  for p in OwnS3.App OwnS3.Pki OwnS3.Protocol OwnS3.Replication OwnS3.Storage Shared.Core Shared.Metrics Shared.Tls; do
    diff -rq --exclude=bin --exclude=obj "src/$p" "/Users/demakaev/ZCodeProject/OwnS3/src/$p"
  done
  for t in OwnS3.UnitTests OwnS3.IntegrationTests Shared.Core.UnitTests Shared.Metrics.UnitTests; do
    diff -rq --exclude=bin --exclude=obj "src/tests/$t" "/Users/demakaev/ZCodeProject/OwnS3/src/tests/$t"
  done
  ls /Users/demakaev/ZCodeProject/OwnS3/src/OwnS3.Protocol/Auth/ChecksumConventions.cs \
     /Users/demakaev/ZCodeProject/OwnS3/src/tests/OwnS3.UnitTests/App/HashingBodyStreamChecksumTests.cs \
     /Users/demakaev/ZCodeProject/OwnS3/src/tests/OwnS3.IntegrationTests/Api/BodyIntegrityScenarios.cs \
     /Users/demakaev/ZCodeProject/OwnS3/src/tests/OwnS3.IntegrationTests/Api/ClusterHealthScenarios.cs \
     /Users/demakaev/ZCodeProject/OwnS3/src/tests/OwnS3.IntegrationTests/Api/ClusterHealthEcScenarios.cs
  grep -n "System.IO.Hashing" /Users/demakaev/ZCodeProject/OwnS3/src/OwnS3.App/OwnS3.App.csproj /Users/demakaev/ZCodeProject/OwnS3/src/Directory.Packages.props
  ```
  Ожидание: оба `diff -rq` пусто (никаких «Files differ»/«Only in» — при расхождении остановиться и разобрать, не править руками); все 5 новых файлов существуют; grep: csproj — `PackageReference Include="System.IO.Hashing"` (~строка 17), props — `PackageVersion ... Version="10.0.12"` (~строка 33; версия была в pg и до main — копия Task 2 уже содержала её, изменений props не требуется).
- [ ] **Step 3. Заменить главы канона и трек версиями main** (в WT удалены ф0+merge — источник: сторона main merge-коммита `ca7a0c5b^2`):
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  for f in 02-operations.md 03-protocol.md 05-access-config.md; do
    git show ca7a0c5b^2:arch/owns3/$f > /Users/demakaev/ZCodeProject/OwnS3/arch/owns3/$f
  done
  git show ca7a0c5b^2:arch/roadmap/owns3.md > /Users/demakaev/ZCodeProject/OwnS3/arch/roadmap/owns3.md
  ```
  (Главы 01/04/06/07 и `01-owns3.md` в main не менялись — уже актуальны в OS3.)
- [ ] **Step 4. Адаптация ссылок новых версий** (ТЕ ЖЕ правила, что Task 1 Steps 2–3, + НОВЫЙ паттерн adminpanel — ссылка t91 в `05-access-config.md:129`; глубина из `OS3/arch/owns3/` — три уровня вверх):
  ```bash
  sed -i '' \
    -e 's|(../18-metrics.md)|(../../../pg/arch/18-metrics.md)|g' \
    -e 's|(../19-backups.md)|(../../../pg/arch/19-backups.md)|g' \
    -e 's|(../22-owns3.md)|(../01-owns3.md)|g' \
    -e 's|(../roadmap/s3.md)|(../../../pg/arch/roadmap/s3.md)|g' \
    -e 's|(../adminpanel/|(../../../pg/arch/adminpanel/|g' \
    /Users/demakaev/ZCodeProject/OwnS3/arch/owns3/02-operations.md \
    /Users/demakaev/ZCodeProject/OwnS3/arch/owns3/03-protocol.md \
    /Users/demakaev/ZCodeProject/OwnS3/arch/owns3/05-access-config.md
  sed -i '' 's|(s3.md)|(../../../pg/arch/roadmap/s3.md)|g' /Users/demakaev/ZCodeProject/OwnS3/arch/roadmap/owns3.md
  ```
  Пояснения (grep по версиям main на момент плана): `05:129` — `[adminpanel/02](../adminpanel/02-etcd-contract.md)` (кластер-health → ClusterOk-проба панели) → пятый паттерн ведёт в `../../../pg/arch/adminpanel/`; `05:143` — `(../18-metrics.md)` → первый паттерн; `02:16` — `(../roadmap/owns3.md)` — трек ВНУТРИ OS3, валиден, НЕ трогать; minio-ссылки `(../../../minio/...)` (02/03, roadmap) — та же глубина от OS3, НЕ трогать; `(../owns3/06/07-...)` в roadmap — внутренние, НЕ трогать; `(s3.md)` в roadmap:16 — паттерн трека. Осознанный остаток (текстовые, не-href — тот же принцип шорткатов Task 1 Step 6): `` `docs/owns3-migration.md` `` (02, 05:210, roadmap:16 — файл существует в OS3/docs, указатель корректен), `` `docs/e2e-isolation.md` / `docs/e2e-launch.md` `` (05:218 — каноны E2E репо pg; текстовый указатель без правки: механическая правка текста канона запрещена принципом «копия = копия»).
- [ ] **Step 5. Полный аудит целостности ссылок канона OS3** (без whitelist — доки в OS3 с Task 4; покрывает и ссылки новых версий глав):
  ```bash
  cd /Users/demakaev/ZCodeProject/OwnS3 && find arch -name "*.md" | while read -r f; do
    d="$(dirname "$f")"
    grep -hoE '\]\([^)#]+\)' "$f" | sed 's/^](//; s/)$//' | grep -v '^http' | sort -u | while read -r p; do
      [ -e "$d/$p" ] || echo "Висячая ссылка: $f -> $p"
    done
  done
  ```
  Ожидание: пусто.
- [ ] **Step 6. Сборка OS3 (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/OwnS3 && dotnet build src/OwnS3.slnx
  ```
  Ожидание: `Build succeeded`, 0 Error.

**Выход:** OS3 содержит актуальный код/тесты/канон main (t36/t91); коммитов нет (initial commit — Task 6, только по завершённой приёмке; WT не менялся — коммитита pg нет).
**Проверка:** Steps 2/5/6 — пусто/пусто/зелёная.
**Spec:** §4.1 (канон as-is + адаптация ссылок), §4.2 (копия = копия), §2.1 (периметр), §9 (порядок: initial commit после синхронизации).

---

### Task 6: Фаза 1 — приёмка OS3 и initial commit

**Files:** ничего нового; проверочные прогоны + первый коммит OS3.

**Вход:** Task 5.5 завершён (наполнение синхронизировано с main).

- [ ] **Step 1. Юниты + Api-интеграции (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/OwnS3 && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/OwnS3.slnx
  ```
  Ожидание: все тесты зелёные (docker-сценарии без `PGW_TEST_DOCKER` пропускаются); в покрытие входят новые сценарии t91 — `HashingBodyStreamChecksumTests`, `HandlersValidationTests`, `BodyIntegrityScenarios`; счётчики не фиксируются.
- [ ] **Step 2. Docker-E2E (в фоне; каноны: guid-изоляция, самоочистка, динамические порты):**
  ```bash
  cd /Users/demakaev/ZCodeProject/OwnS3 && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/OwnS3.slnx -c Release
  ```
  Ожидание: зелёный прогон (E2e-сценарии сами self-build'ят образ из src OS3 — актуальность кода гарантирована сборкой); дождаться финальной строки; после серии — зачистка остатков (`docker ps -a`, `docker network ls`; мусор — `docker network prune -f`).
- [ ] **Step 3. Зачистка после серии:** `docker ps -a` — нет остаточных тестовых контейнеров; `docker network ls` — нет осиротевших сетей прогона.
- [ ] **Step 4. Standalone-контур (образ пересобрать ОБЯЗАТЕЛЬНО — артефакты Task 3 Step 7 устарели после синхронизации Task 5.5):**
  ```bash
  bash /Users/demakaev/ZCodeProject/OwnS3/deploy/build-image.sh
  cd /Users/demakaev/ZCodeProject/OwnS3/deploy && [ -f .env ] || cp .env.example .env
  docker compose up -d
  PORT="$(grep -E '^OWNS3_HOST_PORT=' .env | tail -1 | cut -d= -f2)"; PORT="${PORT:-9000}"
  curl -fsS "http://localhost:${PORT}/healthz"
  docker compose down -v
  ```
  Ожидание: обе `[PHASE]`-фазы сборки прошли; healthz отвечает; после `down -v` — контейнеров/томов контура нет.
- [ ] **Step 5. Проверка самостоятельности (критерий spec §10.2; трактовка — см. «Правило адаптации ссылок» в шапке плана: whitelist для canon-ссылок OS3→pg документных):
  ```bash
  grep -rn "\.\./pg" /Users/demakaev/ZCodeProject/OwnS3/src /Users/demakaev/ZCodeProject/OwnS3/deploy /Users/demakaev/ZCodeProject/OwnS3/docker 2>/dev/null | grep -v bin/ | grep -v obj/
  grep -rn "ProjectReference" /Users/demakaev/ZCodeProject/OwnS3/src --include="*.csproj" | grep -v "OwnS3\.\|Shared\."
  ```
  Ожидание: оба пусто. Canon-ссылки `../../pg/arch/…`/`../../../pg/arch/…` в `OS3/arch/` и `docs/runbook.md` — допустимы (документация, whitelist); исторические упоминания в `docs/superpowers/` — допустимы (spec §10.2); `../AGENTS.base.md`, `../minio`, `../Puzzle` — допустимы (общее окружение, spec §4.3).
- [ ] **Step 6. Initial commit OS3 (после приёмки, до чистки pg — spec §9; коммит содержит актуальный код main благодаря Task 5.5):**
  ```bash
  git -C /Users/demakaev/ZCodeProject/OwnS3 add -A
  git -C /Users/demakaev/ZCodeProject/OwnS3 commit -m "owns3: initial snapshot — вынос из монорепо pg (канон arch/ с правками t36/t91, код src/, тесты, deploy/build-image.sh + standalone-контур, docs/ и история задач)"
  git -C /Users/demakaev/ZCodeProject/OwnS3 status --short
  ```

**Выход:** OS3 дееспособен (код main) и зафиксирован первым коммитом в `main`.
**Проверка:** Steps 1–5 зелёные; Step 6 — коммит создан, статус чист.
**Spec:** §8 Фаза 1, §10.1, §10.2, §9 (порядок коммитов).

---

### Task 7: Фаза 2 — клиентская копия протокола в S3Worker.Core, разрыв ProjectReference

**Files:**
- Create (WT): `src/S3Worker.Core/OwnS3/Protocol/{Auth,PeerAuth,Errors,Requests,Uris}/` — 15 файлов (таблица в шапке плана).
- Modify (WT): `src/S3Worker.Core/S3Worker.Core.csproj` (удалить `ProjectReference` на `OwnS3.Protocol`).

**Вход:** Task 6 (OS3 зафиксирован — код протокола доступен в новом репо).

- [ ] **Step 1. Скопировать 15 файлов протокола** (as-is, namespace `OwnS3.Protocol.*` сохраняется):
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/src/OwnS3.Protocol
  DST=/Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/src/S3Worker.Core/OwnS3/Protocol
  mkdir -p "$DST/Auth" "$DST/PeerAuth" "$DST/Errors" "$DST/Requests" "$DST/Uris"
  cp Auth/SigV4Core.cs Auth/SigV4HeaderVerifier.cs Auth/AuthorizationHeaderParser.cs Auth/CanonicalRequestBuilder.cs Auth/PayloadHashMode.cs "$DST/Auth/"
  cp PeerAuth/PeerCertificate.cs PeerAuth/PeerCertificateAuthority.cs PeerAuth/PeerRequestSigner.cs PeerAuth/PeerRequestVerifier.cs PeerAuth/PeerTrustAnchor.cs "$DST/PeerAuth/"
  cp Errors/S3ErrorCode.cs Errors/S3ProtocolException.cs "$DST/Errors/"
  cp Requests/S3RequestModel.cs Requests/S3HeaderCollection.cs "$DST/Requests/"
  cp Uris/UriEncoding.cs "$DST/Uris/"
  ```
  Источник — актуальный WT (после merge): ни один из 15 файлов не менялся в main (diff только `AwsChunkedReader` + новый `ChecksumConventions` — оба вне копии), состав и using-зависимости стабильны.
- [ ] **Step 2. Удалить ProjectReference** из `src/S3Worker.Core/S3Worker.Core.csproj`: убрать блок
  ```xml
  <!-- Криптобиблиотека peer-PKI ownS3 (spec §5.1, §3.2): переиспользование,
       копирование крипты запрещено (PkiStore — выпуск/чтение PKI per-cluster). -->
  <ProjectReference Include="..\OwnS3.Protocol\OwnS3.Protocol.csproj"/>
  ```
  и заменить комментарием:
  ```xml
  <!-- Клиентская копия протокола ownS3 (OwnS3/Protocol/: SigV4-ядро, peer-PKI,
       модель запроса; канон — ../OwnS3/arch/owns3/03 и 07): развивается
       независимо, обязана соответствовать канону OwnS3. -->
  ```
  Пакеты НЕ добавляются (копии не нужны ни `System.IO.Hashing`, ни `Shared.Core` сверх уже имеющейся ссылки на `Shared.Core`).
- [ ] **Step 3. Сборка pg (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract && dotnet build src/PgWorker.slnx
  ```
  Ожидание: `Build succeeded` (компиляция ловит недобор классов).
- [ ] **Step 4. Тесты S3Worker (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx --filter "FullyQualifiedName~S3Worker.UnitTests"
  ```
  Ожидание: зелёные (в т.ч. SigV4GetSignerTests/OwnS3NodeClientTests/PkiStoreTests — верификаторы теперь из копии).
- [ ] **Step 5. Коммит pg:**
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract add -A src/S3Worker.Core/
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract commit -m "owns3-extract(ф2): клиентская копия OwnS3.Protocol в S3Worker.Core (15 файлов, namespace сохранён); ProjectReference на OwnS3.Protocol удалён — единственная кодовая связь pg→OwnS3 разорвана"
  ```

**Выход:** S3Worker собирается и проходит тесты на внутренней копии протокола.
**Проверка:** Steps 3–4 зелёные; `grep -n "OwnS3.Protocol" src/S3Worker.Core/S3Worker.Core.csproj` — пусто.
**Spec:** §6.2, §8 Фаза 2, §10.5 (юниты на копии).

---

### Task 8: Фаза 3 — удаление OwnS3-проектов/тестов/доков из pg, правка slnx и Dockerfile

**Files:**
- Delete (WT): `src/OwnS3.{App,Pki,Protocol,Replication,Storage}/`, `src/tests/OwnS3.{UnitTests,IntegrationTests}/`, `docker/OwnS3.Dockerfile`, `docs/owns3-migration.md`, `docs/owns3-replication-requirements.md`, `docs/s3-replication.md`, `docs/superpowers/<13 owns3-каталогов>` (НЕ трогать пришедшие merge'ем `2026-10-15-t36-s3-pool-integration`, `2026-10-17-t91-owns3-checksum-verify` — остаются в pg).
- Modify (WT): `src/PgWorker.slnx`.

**Вход:** Tasks 6–7 (OS3 зафиксирован; копия протокола работает).

- [ ] **Step 1. Удалить код и тесты OwnS3 из pg:**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  git rm -r src/OwnS3.App src/OwnS3.Pki src/OwnS3.Protocol src/OwnS3.Replication src/OwnS3.Storage src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests docker/OwnS3.Dockerfile
  rm -rf src/OwnS3.App src/OwnS3.Pki src/OwnS3.Protocol src/OwnS3.Replication src/OwnS3.Storage src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests artifacts/owns3
  ```
- [ ] **Step 2. Правка `src/PgWorker.slnx` (структура после merge проверена):** удалить блок `<Folder Name="/owns3/"> … </Folder>` — 5 строк проектов (строки 62–66: Protocol/Storage/Replication/App/Pki) — и две строки в `/tests/` (строки 84–85: `tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj`, `tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj`). Остальное — без изменений. **Индексировать правку:** `git add src/PgWorker.slnx` (Steps 1/3 индексируют только удаления — без add правка slnx не попадёт в коммит Step 5).
- [ ] **Step 3. Удалить перенесённые доки и историю:**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  git rm docs/owns3-migration.md docs/owns3-replication-requirements.md docs/s3-replication.md
  git rm -r docs/superpowers/2026-10-10-t35-owns3-canon docs/superpowers/2026-10-10-t36-owns3-protocol docs/superpowers/2026-10-10-t37-owns3-storage-objects docs/superpowers/2026-10-10-t38-owns3-multipart docs/superpowers/2026-10-10-t39-owns3-e2e docs/superpowers/2026-10-11-t35-owns3-replication docs/superpowers/2026-10-11-t40-owns3-erasure-coding docs/superpowers/2026-10-11-t41-owns3-minio-migration docs/superpowers/2026-10-11-t42-owns3-tls docs/superpowers/2026-10-13-owns3-ca-identity docs/superpowers/2026-10-13-t37-owns3-replication-ping docs/superpowers/2026-10-13-t38-owns3-result-migration docs/superpowers/2026-10-14-owns3-denylist-replace
  ```
- [ ] **Step 4. Сборка полного solution (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract && dotnet build src/PgWorker.slnx
  ```
  Ожидание: зелёная (slnx больше не знает OwnS3-проектов).
- [ ] **Step 5. Коммит pg:**
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract add src/PgWorker.slnx
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract commit -m "owns3-extract(ф3): удаление проектов/тестов/Dockerfile OwnS3 и перенесённых доков/истории из pg; slnx чист (папка /owns3/ и тесты OwnS3 удалены)"
  ```

**Выход:** в pg нет OwnS3-проектов, сборка зелёная; правка slnx в коммите.
**Проверка:** `ls src/ | grep OwnS3` — пусто; Step 4 зелёный; `git -C WT show --stat HEAD | grep slnx` — есть.
**Spec:** §6.3, §6.7, §10.3.

---

### Task 9: Фаза 3 — deploy-compose и 00-up.sh pg (внешний образ + fail-fast)

**Files:**
- Modify (WT): `deploy/docker-compose.yml`, `dev-stand/adminpanel/checks/00-up.sh`.

**Вход:** Task 8. Строки — по состоянию WT после merge `ca7a0c5b` (сервис `owns3` на строках 346–372; t36 добавил ключ `OWNS3_ACCESS_KEYS__1_*` для панели — env-блок НЕ трогать).

- [ ] **Step 1. `deploy/docker-compose.yml`, сервис `owns3` (строки ~346–352):** удалить `build:`-блок
  ```yaml
      build:
        context: ..
        dockerfile: docker/OwnS3.Dockerfile
        args:
          OWNS3_PUBLISH_DIR: artifacts/owns3/publish
  ```
  оставить `image: owns3:dev`; комментарий перед сервисом (строки ~341–345) дополнить строкой: «образ — ВНЕШНИЙ: собирается репозиторием `../OwnS3` (`deploy/build-image.sh` там); pg его не собирает». Шапочную docs-ссылку (строка ~344, `docs/owns3-migration.md`) заменить на `../OwnS3/docs/owns3-migration.md` (sed Step 2 в Task 11 доведёт идемпотентно).
- [ ] **Step 2. Комментарии канона в deploy-compose** (вхождения `arch/owns3/` в строках ~341, 361, 373–374, 382 и далее — файл правится этим таском, веду ссылки к новому адресу):
  ```bash
  sed -i '' 's|arch/owns3/|../OwnS3/arch/owns3/|g' /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/deploy/docker-compose.yml
  ```
- [ ] **Step 3. Комментарии профилей owns3-ec/mesh (строки ~382–384):** фразу «build НЕ дублируется: профиль поднимается на образе, собранном сервисом owns3 / 00-up.sh» заменить на «build НЕ дублируется: профиль поднимается на внешнем образе owns3:dev (репозиторий ../OwnS3)». Env-блоки, тома, contour-override, t36-добавления (ключ панели `OWNS3_ACCESS_KEYS__1_*`) — БЕЗ изменений. **Осознанный остаток (не правится):** `deploy/docker-compose.contour.yml`, `deploy/.env.example` (4 вхождения `arch/owns3/`), `deploy/tls/gen.sh`, чеки `46-owns3.sh`/`47-owns3-ec.sh` содержат текстовые упоминания `arch/owns3/NN §X` — остаются как есть (spec §6.4: «без изменений»; канон ищется в `../OwnS3/arch/` и git-истории pg).
- [ ] **Step 4. `00-up.sh` (строки по WT после merge):** удалить publish-фазу OwnS3 (блок строк ~75–81: комментарий `# ownS3 (t39, …)` + `echo ">>> [PHASE] publish OwnS3.App …"` + `dotnet publish … OwnS3.App.csproj …` + `echo ">>> [PHASE] publish OwnS3 готов …"`); в шапке-комментарии (строка 5: «[PHASE]: SdGenerator, OwnS3, wal-агент…») убрать `OwnS3` из перечня; после блока «Arrange: инструменты хоста» (строки 45–~50, `for bin in docker jq curl; do … done`) добавить fail-fast:
  ```bash
  # ownS3: ВНЕШНИЙ образ (репозиторий ../OwnS3) — pg его не собирает; нет
  # образа — падаем сразу с подсказкой (сборка: ../OwnS3/deploy/build-image.sh).
  docker image inspect owns3:dev >/dev/null 2>&1 \
    || { echo "❌ нет образа owns3:dev — собери в ../OwnS3: bash ../OwnS3/deploy/build-image.sh"; exit 1; }
  ```
  Строку `up -d --build --force-recreate pgworker pgworker-2 owns3 s3worker s3worker-2` (строка ~213) НЕ менять: `--build` у сервисов без `build:`-секции — noop, remaining build-сервисы собираются как раньше. t36-блоки ниже (строки ~222–241: ожидание healthz owns3 + создание bucket `pgworker-etcd-snapshots`; строка ~487: сид пула `backups`) — НЕ трогать: они работают с внешним образом одинаково.
- [ ] **Step 5. Коммит pg:**
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract add -A deploy/ dev-stand/adminpanel/checks/00-up.sh
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract commit -m "owns3-extract(ф3): deploy-compose owns3 на готовом внешнем образе (build: удалён, ссылки канона → ../OwnS3); 00-up.sh — fail-fast по отсутствию owns3:dev с подсказкой ../OwnS3"
  ```

**Выход:** контур pg поднимается на внешнем образе; без образа — понятный фейл.
**Проверка:** `docker compose -f deploy/docker-compose.yml config` (валидация YAML, в фоне); блок сервиса owns3 без `build:`:
```bash
sed -n '/^  owns3:/,/^  [a-z]/p' /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/deploy/docker-compose.yml | grep -n "build:"
```
Ожидание: пусто — печать блока сервиса owns3 (от `  owns3:` до следующей строки сервиса `^  [a-z]`, т.е. `  owns3-ec:`) не содержит `build:`. Не `grep "build:" | grep -B2 owns3`: строки `build:` не содержат подстроки «owns3» — такая проверка пуста тривиально и не ловит забытый блок.
**Spec:** §6.4, §10.4.

---

### Task 10: Фаза 3 — ВСЕ ПЯТЬ сборщиков ownS3-образа в тестах pg на внешний owns3:dev

**Files:**
- Modify (WT): `src/tests/S3Worker.IntegrationTests/E2e/S3E2eEnvironment.cs`, `src/tests/S3Worker.IntegrationTests/S3/S3OwnS3Image.cs`, `src/tests/S3Worker.IntegrationTests/S3/S3ClusterFixture.cs`, `src/tests/PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`, `src/tests/PgWorker.IntegrationTests/Backups/OwnS3.cs`, `src/tests/AdminPanel.IntegrationTests/S3NodeContainerFixture.cs`.

**Вход:** Task 9 (deploy переключён; после Task 8 исходники OwnS3.App/Dockerfile в pg уже удалены — правка обязательна для собираемости docker-серий ВСЕХ трёх тестовых проектов). E2e-контур t36 PgWorker, фикстура бэкапов и панельная фикстура S3-ноды пришли merge'ем main и собирают `owns3:e2e` publish'ем из удаляемого src — перевод на внешний образ обязателен, иначе Task 8 ломает docker-серии PgWorker и AdminPanel.

- [ ] **Step 1. `S3Worker.IntegrationTests/E2e/S3E2eEnvironment.cs`:**
  - удалить константу `OwnS3PublishDir` (строка ~32: `artifacts/e2e/owns3`);
  - заменить оба места сборки owns3-образа на проверку наличия внешнего образа:
    - в `BuildImagesAsync` noBuild-ветка: вызов `EnsureImageAsync(OwnS3Image, …publish/build…)` заменить на `EnsureImageAsync(OwnS3Image, "bash ../OwnS3/deploy/build-image.sh")`;
    - в основной ветке: удалить owns3-блок (`owns3T0`, `[PHASE] e2e publish OwnS3.App`, `dotnet publish … OwnS3.App.csproj`, `docker build … docker/OwnS3.Dockerfile`) и добавить перед сборкой s3worker тот же вызов `await EnsureImageAsync(OwnS3Image, "bash ../OwnS3/deploy/build-image.sh");`;
  - обновить комментарии: doc-комментарий класса (строка ~18: «пересборка КАЖДЫЙ прогон — S8») и комментарий над `BuildImagesAsync` (строки ~402–405) изложить как: «owns3:dev — ВНЕШНИЙ статический ассет (репозиторий ../OwnS3, freshness контролирует разработчик пересборкой `deploy/build-image.sh` там; pg его не собирает — fail-fast с командой); s3worker:e2e-{runId} — self-build, свежесть каждого прогона». Комментарий ~187 «Статические ассеты процесса: owns3:dev (S8 — свежесть каждого прогона)» — согласовать с той же формулировкой.
- [ ] **Step 2. `S3Worker.IntegrationTests/S3/S3OwnS3Image.cs` — переписать целиком** (было: сборка тега `owns3:e2e-it` через publish+build; становится: проверка внешнего `owns3:dev`). Новый полный текст файла:
  ```csharp
  using System.Diagnostics;

  namespace S3Worker.IntegrationTests.S3;

  // Внешний образ owns3:dev (репозиторий ../OwnS3, сборка — deploy/build-image.sh
  // там): pg его не собирает — проверка наличия раз на процесс тест-хоста,
  // fail-fast с командой сборки. Freshness контролирует разработчик
  // пересборкой в ../OwnS3 (устаревший образ маскирует регрессии).
  internal static class S3OwnS3Image
  {
      public const string Tag = "owns3:dev";

      private static readonly SemaphoreSlim Gate = new(1, 1);
      private static bool _ready;

      public static async Task EnsureOnceAsync(string artifactsDir, CancellationToken ct)
      {
          if (_ready)
              return;
          await Gate.WaitAsync(ct);
          try
          {
              if (_ready)
                  return;
              var inspect = new ProcessStartInfo("docker", $"image inspect {Tag}")
              {
                  UseShellExecute = false,
                  RedirectStandardOutput = true,
                  RedirectStandardError = true,
                  CreateNoWindow = true,
              };
              using var process = Process.Start(inspect)!;
              await process.WaitForExitAsync(ct);
              if (process.ExitCode != 0)
                  throw new InvalidOperationException(
                      $"образ {Tag} отсутствует — собери в ../OwnS3: bash ../OwnS3/deploy/build-image.sh");
              Phase(artifactsDir, $"внешний образ {Tag} найден (репозиторий ../OwnS3)");
              _ready = true;
          }
          finally
          {
              Gate.Release();
          }
      }

      private static void Phase(string artifactsDir, string message)
      {
          Console.WriteLine($"[PHASE] {message}");
          File.AppendAllText(Path.Combine(artifactsDir, "phases.log"), $"{DateTime.UtcNow:O} {message}\n");
      }
  }
  ```
- [ ] **Step 3. `S3Worker.IntegrationTests/S3/S3ClusterFixture.cs`** (потребитель):
  - комментарий и свойство (~строки 69–72): «Образ ownS3 (owns3:e2e-it, сборка раз на процесс)» → «Образ ownS3 — внешний owns3:dev (репозиторий ../OwnS3; проверка раз на процесс)»; свойство `OwnS3Image => S3OwnS3Image.Tag` остаётся (Tag теперь `owns3:dev`);
  - литерал тега в `S3ProvisioningOptions` (~строка 77): `"owns3:e2e-it"` → `"owns3:dev"`;
  - вызов в `InitializeAsync` (~строка 109): `await S3OwnS3Image.BuildOnceAsync(artifactsDir, ct);` → `await S3OwnS3Image.EnsureOnceAsync(artifactsDir, ct);`.
- [ ] **Step 4. `PgWorker.IntegrationTests/E2e/E2eEnvironment.cs`** (E2E-контур t36 «Вариант A»):
  - константа (строка 56): `public const string OwnS3Image = "owns3:e2e";` → `"owns3:dev"`;
  - `EnsureOwnS3ImageAsync` (строки ~1727–1772): doc-комментарий (t36: «.NET-сервис — publish НА ХОСТЕ… docker/OwnS3.Dockerfile…») заменить на: «Образ ownS3 — ВНЕШНИЙ статический ассет (репозиторий ../OwnS3, сборка — deploy/build-image.sh там): pg его не собирает; freshness контролирует разработчик пересборкой в ../OwnS3 (устаревший образ маскирует регрессии; урок t09)»;
  - тело метода сократить: сохранить ранние выходы `IsInnerRunner` и `_owns3ImageReady` и обёртку `StaticGate`; ВЕЗДЕ (и в prepare-фазе, и в noBuild `PGW_TEST_E2E_NOBUILD=1`, и в раннере) — одна проверка: `if (!await DockerImageExistsAsync(OwnS3Image, ct)) throw new ApplicationException("образ owns3:dev отсутствует в docker — собери в ../OwnS3: bash ../OwnS3/deploy/build-image.sh");` затем `_owns3ImageReady = true;`. Удалить ветвление `PreparingPhase`/noBuild и весь publish/build-блок (`outDir artifacts/e2e/owns3`, `[PHASE] e2e-image …` publish/build-фазы, два `RunProcessAsync`). Комментарий «Раннер: образ гарантированно собран фазой 1» заменить формулировкой внешнего ассета. Вызовы `EnsureOwnS3ImageAsync` не меняются.
- [ ] **Step 5. `PgWorker.IntegrationTests/Backups/OwnS3.cs`** (фикстура ноды Fact):
  - `private const string Image = E2eEnvironment.OwnS3Image;` — остаётся (тег придёт `owns3:dev` из Step 4);
  - `EnsureImageAsync` (строки ~93–137) заменить целиком на проверку наличия внешнего образа (по образцу Step 2 — `ProcessStartInfo("docker", $"image inspect {Image}")`, fail-fast `ApplicationException` с текстом «образ owns3:dev отсутствует — собери в ../OwnS3: bash ../OwnS3/deploy/build-image.sh», `_imageReady = true` под `ImageGate`); комментарий метода («Локальная сборка ownS3…») заменить на формулировку внешнего образа; удалить ставшие неиспользуемыми `FindRepoRoot`/`Tail` (перед удалением — grep по файлу: если используются другими методами, оставить).
- [ ] **Step 6. `AdminPanel.IntegrationTests/S3NodeContainerFixture.cs`** (панельная фикстура S3-ноды, t36) — тот же перевод по образцу Step 5:
  - константа (строка 31): `public const string Image = "owns3:e2e";` → `"owns3:dev"`;
  - `EnsureImageAsync` (строки 78–125) заменить целиком на проверку наличия внешнего образа: `ProcessStartInfo("docker", $"image inspect {Image}")` с fail-fast (текст — «образ owns3:dev отсутствует — собери в ../OwnS3: bash ../OwnS3/deploy/build-image.sh»), `_imageReady = true` под существующим `ImageGate`; комментарий метода (строки 76–77: «Локальная сборка owns3:e2e (publish НА ХОСТЕ → образ publish-COPY…») заменить на формулировку внешнего образа;
  - комментарии с тегом `owns3:e2e` на строках 16 и 132 — согласовать с формулировкой внешнего `owns3:dev` (строка 132 — комментарий над `RunToolAsync`, упоминающий инцидент t36 при сборке образа);
  - `RunToolAsync` (хост-инструмент с параллельной дренировкой пайпов) — после замены сборки на inspect может остаться неиспользуемым: grep по файлу; используется другими методами — оставить как есть, не используется — удалить вместе с publish/build-хвостом.
- [ ] **Step 7. Сборка всех трёх тестовых проектов (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract && dotnet build src/tests/S3Worker.IntegrationTests/S3Worker.IntegrationTests.csproj && dotnet build src/tests/PgWorker.IntegrationTests/PgWorker.IntegrationTests.csproj && dotnet build src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj
  ```
- [ ] **Step 8. Контрольный grep (все пять сборщиков):**
  ```bash
  grep -rn "OwnS3.App\|OwnS3.Dockerfile\|owns3:e2e" /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/src/tests/
  ```
  Ожидание: пусто (ни publish-путей, ни Dockerfile, ни старых тегов `owns3:e2e-it`/`owns3:e2e`, включая S3NodeContainerFixture; каталогов `tests/OwnS3.*` уже нет после Task 8).
- [ ] **Step 9. Коммит pg:**
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract add -A src/tests/S3Worker.IntegrationTests/ src/tests/PgWorker.IntegrationTests/ src/tests/AdminPanel.IntegrationTests/
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract commit -m "owns3-extract(ф3): все пять сборщиков образа в тестах pg (S3Worker E2e + S3-серия; PgWorker E2E-контур t36 + фикстура бэкапов OwnS3; AdminPanel S3NodeContainerFixture) — внешний owns3:dev с fail-fast; теги owns3:e2e-it/owns3:e2e упразднены"
  ```

**Выход:** ни один тест pg не собирает owns3-образ из src pg.
**Проверка:** Step 7 зелёная; Step 8 пусто.
**Spec:** §6.5 (E2E-окружение S3Worker — принцип распространён на пришедшие t36-контуры PgWorker и AdminPanel), §10.5.

---

### Task 11: Фаза 3 — AGENTS.md pg, висячая href в arch/19 и ссылки на перенесённые доки

**Files:**
- Modify (WT): `AGENTS.md`, `arch/19-backups.md`, `docs/runbook.md`, `dev-stand/adminpanel/README.md`, `dev-stand/adminpanel/docker-compose.yml`, `deploy/docker-compose.yml`, `docker/PgWorker.Wal.Dockerfile`.

**Вход:** Tasks 8–10. По WT после merge: owns3-упоминаний в AGENTS.md НЕТ (grep пуст — вставка чистая); в `docs/runbook.md:210` — текстовая ссылка на удаляемую `docs/owns3-migration.md` (пришла с t36) — без правки ломает критерий Task 14 Step 5 (4); в `arch/19-backups.md:758` — висячая owns3-href, пришедшая из main мимо sed-правки Task 1 Step 6 (наша ветка правила файл до merge; main добавил новую href в другой строке — merge принял её без конфликта; grep `](owns3/` по arch/ даёт ровно это одно вхождение) — противоречие spec §10.6, чинится здесь.

- [ ] **Step 1. `AGENTS.md` pg** — блок registry-правил (строка 43): после предложения «⚠️ **Локально собираемые образы (`pgworker*:dev`, `adminpanel:dev`, `pgworker-node:*`, …) в registry класть КАТЕГОРИЧЕСКИ запрещено.**» (перед «Новый внешний образ в проекте…») добавить:
  ```
  ⚠️ `owns3:dev` — ВНЕШНИЙ образ: собирается репозиторием `../OwnS3`
  (`deploy/build-image.sh` там), pg его не собирает (стенд/E2E — fail-fast с
  подсказкой); в registry не класть.
  ```
  Прочие блоки pg — без изменений.
- [ ] **Step 2. Ссылки на перенесённые доки + висячая href архива.** Первый sed — docs-ссылки (полный список по grep; правка только этих вхождений; `docs/runbook.md` — текстовое упоминание в таблице образов, строка 210, пришло с t36):
  ```bash
  sed -i '' 's|docs/owns3-migration\.md|../OwnS3/docs/owns3-migration.md|g' \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/README.md \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/docker-compose.yml \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/deploy/docker-compose.yml \
    /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/docs/runbook.md
  ```
  Второй sed — висячая href `arch/19-backups.md:758` (паттерн тот же, что применяла Task 1 Step 6 к 19/23/24; вхождение `](owns3/` без `../OwnS3` в arch/ единственное — строка 758 «разносит tombstone — [owns3/07-replication.md](owns3/07-replication.md)»):
  ```bash
  sed -i '' 's|](owns3/|](../OwnS3/arch/owns3/|g' /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/arch/19-backups.md
  ```
  (deploy-compose доком-ссылку получает и в Task 9 Step 1 — sed идемпотентно доводит все файлы; чеки 46/47, `.env.example`, `tls/gen.sh`, `docker-compose.contour.yml`, комментарии кода S3Worker/AdminPanel — НЕ трогать: spec «без изменений»/«копия = копия», остаток зафиксирован в Task 9 Step 3.)
- [ ] **Step 3. `docker/PgWorker.Wal.Dockerfile`** (строка ~4: комментарий «канон E2E-образов, по образцу docker/OwnS3.Dockerfile» — файл-образец удалён Task 8): заменить фрагмент `по образцу docker/OwnS3.Dockerfile` на `по образцу ../OwnS3/docker/OwnS3.Dockerfile`. Правка только комментария.
- [ ] **Step 4. Контрольные grep'ы (перенесённые доки + перезапуск проверки Task 1 Step 8 по текущему WT):**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  grep -rn "owns3-migration\|owns3-replication-requirements\|docs/s3-replication" --include="*.md" --include="*.sh" --include="*.yml" . | grep -v "../OwnS3" | grep -v docs/superpowers/
  grep -rn "](owns3/" arch/ | grep -v "../OwnS3"
  ```
  Ожидание: оба пусто (второй — закрывает висячую 19:758).
- [ ] **Step 5. Коммит pg:**
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract add -A
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract commit -m "owns3-extract(ф3): AGENTS.md pg — owns3:dev внешний (../OwnS3); ссылки на перенесённые доки (adminpanel README/compose, deploy-compose, runbook) ведут в ../OwnS3; arch/19: висячая owns3-href (main-merge) → ../OwnS3; Wal.Dockerfile — образец образа в ../OwnS3"
  ```

**Выход:** правила и ссылки pg соответствуют новой топологии; висячих owns3-href в arch/ нет.
**Проверка:** Step 4 оба пусто.
**Spec:** §6.6, §6.7, §10.6, §10.7.

---

### Task 12: Фаза 4 — полный стенд pg на внешнем образе

**Files:** ничего нового; прогоны.

**Вход:** Tasks 8–11 (чистка pg завершена), Task 6 (образ собирался) — перед прогоном пересобрать образ свежим.

- [ ] **Step 1. Страховочная зачистка docker перед серией** (гейт AGENTS.md): `docker ps -a` — нет чужих остатков; `docker network ls` — нет осиротевших сетей; мусор — `docker network prune -f`.
- [ ] **Step 2. Свежий образ (в фоне):**
  ```bash
  bash /Users/demakaev/ZCodeProject/OwnS3/deploy/build-image.sh
  ```
- [ ] **Step 3. Полный стенд (в фоне, серия чеков по порядку; 45-backups-storage.sh — чек t36, проверяет пул бэкапов ownS3 — нода пула `s3w-backups-node1` поднимается S3Worker'ом из того же внешнего `owns3:dev`; прямой потребитель выноса):**
  ```bash
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/00-up.sh
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/10-smoke-api.sh
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/45-backups-storage.sh
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/46-owns3.sh
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/47-owns3-ec.sh
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/75-s3worker.sh
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/80-s3-panel.sh
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/90-down.sh
  ```
  Ожидание: все чеки зелёные (в т.ч. 45/46/47 — внешний образ и его потребители в контуре pg; чек 46 после t91 включает пробу кластер-health), down — чистый.

**Выход:** стенд pg полностью работает на образе из OS3.
**Проверка:** финальные строки чеков `✓`; после 90-down — стенд снесён.
**Spec:** §8 Фаза 4, §10.4.

---

### Task 13: Фаза 4 — отрицательный тест: стенд без образа

**Files:** ничего; прогоны.

**Вход:** Task 12 (стенд проверен и снесён).

- [ ] **Step 1. Спрятать образ:**
  ```bash
  docker tag owns3:dev owns3:dev-bak && docker rmi owns3:dev
  ```
- [ ] **Step 2. Запуск 00-up.sh (в фоне) — ожидание падения:**
  ```bash
  bash /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract/dev-stand/adminpanel/checks/00-up.sh; echo "EXIT=$?"
  ```
  Ожидание: быстрый ненулевой выход с сообщением `❌ нет образа owns3:dev — собери в ../OwnS3: bash ../OwnS3/deploy/build-image.sh`.
- [ ] **Step 3. Восстановить образ:**
  ```bash
  docker tag owns3:dev-bak owns3:dev && docker rmi owns3:dev-bak && docker image inspect owns3:dev >/dev/null && echo OK
  ```

**Выход:** fail-fast работает и понятен.
**Проверка:** вывод Step 2 содержит подсказку; Step 3 — `OK`.
**Spec:** §6.4 (fail-fast), §10.4.

---

### Task 14: Фаза 4 — финальные прогоны и grep-критерии приёмки

**Files:** ничего; прогоны/проверки.

**Вход:** Tasks 12–13. Серии pg изменились (t36/t91 добавили сценарии) — счётчики нигде не заявляются, критерий «зелёный прогон».

- [ ] **Step 1. Полный тест pg (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release
  ```
  Ожидание: зелёный (критерий §10.3; docker-сценарии за `PGW_TEST_DOCKER=1` в этом прогоне пропускаются — docker-серии ниже отдельными шагами).
- [ ] **Step 2. Docker-E2E S3Worker с внешним owns3:dev (в фоне):**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.E2e"
  ```
  Ожидание: зелёный; дождаться финальной строки; зачистка остатков (паттерн Step 1 из Task 12) — следующая серия только после неё.
- [ ] **Step 3. Docker-серия интеграций S3Worker.S3** (S3ClusterFixture на внешнем образе — приёмка правки Task 10 Steps 2–3):
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract && DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.S3"
  ```
  Ожидание: зелёный; после — зачистка остатков (серия за серией, не поверх).
- [ ] **Step 4. Полный тест OS3 (в фоне):** повтор `dotnet test src/OwnS3.slnx` (юниты+Api) — зелёный (критерий §10.1; сценарии t91 в покрытии).
- [ ] **Step 5. Grep-критерии:**
  ```bash
  cd /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract
  # 1) ни одной ProjectReference на OwnS3.*
  grep -rn "ProjectReference" src --include="*.csproj" | grep "OwnS3"
  # 2) OwnS3 в src/ остаётся ТОЛЬКО в: клиентской копии S3Worker, коде
  #    AdminPanel и тестах PgWorker.IntegrationTests. Последние —
  #    легитимные потребители внешнего owns3:dev (t36: E2E-контур
  #    бэкапов/etcd-слепков, фикстура OwnS3, юниты бэкап-конфигурации —
  #    7 файлов: E2e/E2eEnvironment.cs, E2e/E2eFixture.cs,
  #    E2e/E2eEtcdSnapshotExportScenarios.cs, Backups/OwnS3.cs,
  #    Backups/WalReceiverIntegrationTests.cs,
  #    Backups/EtcdSnapshotSinkTests.cs, Backups/BackupS3Tests.cs);
  #    работают с ownS3 только через образ и S3 API (по приказу остаются
  #    в pg). Основной код PgWorker.*/AdminPanel.* чист от OwnS3.
  grep -rln "OwnS3" src --include="*.cs" | grep -v "S3Worker" | grep -v AdminPanel | grep -v "PgWorker.IntegrationTests"
  # 3) удаляемых сущностей не существует
  ls src/OwnS3.App src/OwnS3.Protocol src/tests/OwnS3.UnitTests arch/owns3 arch/22-owns3.md arch/roadmap/owns3.md docs/owns3-migration.md docs/s3-replication.md docker/OwnS3.Dockerfile 2>&1
  # 4) owns3-упоминания в docs/arch — только легитимные указатели на ../OwnS3.
  #    История задач docs/superpowers/ исключена: остающиеся каталоги
  #    S3Worker-задач (t31–t34, t40-governance, эта задача) И пришедшие
  #    merge'ем t36-s3-pool-integration / t91-owns3-checksum-verify
  #    упоминают arch/owns3 и перенесённые доки БЕЗ ../OwnS3 — это
  #    легитимный остаток (исторические документы не переписываются;
  #    spec §10.2, AGENTS.md pg: история — в git и docs/superpowers/).
  grep -ril owns3 docs/ arch/ | grep -v '^docs/superpowers/' | xargs grep -l "arch/owns3\|docs/owns3\|docs/s3-replication" 2>/dev/null | xargs grep -L "../OwnS3" 2>/dev/null
  ```
  Ожидания: (1) пусто; (2) пусто — OwnS3 вне {клиентская копия S3Worker, AdminPanel, PgWorker.IntegrationTests} не встречается; (3) все `No such file or directory`; (4) пусто — за вычетом исключённой истории `docs/superpowers/` (легитимный остаток, зафиксирован в комментарии цепочки; `docs/runbook.md` закрыт правкой Task 11 Step 2, висячая href arch/19 — правкой Task 11 Step 2).
- [ ] **Step 6. Roadmap-гейт:** `grep -rn "owns3-extract" arch/roadmap/` — пусто (тег не заводился; если вдруг есть — удалить тем же коммитом).

**Выход:** все критерии spec §10 подтверждены прогонами, включая docker-серию S3Worker.S3 на внешнем образе.
**Проверка:** Steps 1–5 зелёные/пустые.
**Spec:** §10 (все пункты), §8 Фаза 4.

---

### Task 15: Фаза 5 — подготовка к ревью и мержу

**Files:** ничего нового.

**Вход:** Task 14 полностью зелёный.

- [ ] **Step 1. Итоговый статус ветки:**
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract status --short
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract log --oneline main..
  git -C /Users/demakaev/ZCodeProject/OwnS3 log --oneline
  ```
  Ожидание: worktree чист; в ветке 7 коммитов поверх точки ветвления: `b4b95f75` (ф0), `ca7a0c5b` (merge main), Task 7 (ф2), Tasks 8/9/10/11 (ф3 × 4); OS3 — один initial commit (код main с t36/t91).
- [ ] **Step 2. Сводка диффа для ревью** (перенос-копии читаются по спискам файлов):
  ```bash
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract diff main..HEAD --stat -- arch/ | tail -5
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract diff main..HEAD --stat -- src/ | tail -5
  git -C /Users/demakaev/ZCodeProject/worktrees/refactor-owns3-extract diff main..HEAD --stat -- deploy/ dev-stand/ docker/ AGENTS.md docs/ | tail -5
  ```
  (За точку сравнения с main — учитывать merge: дифф `main..HEAD` после merge показывает только пост-merge коммиты; полный дифф ветки — `git diff 2897d19a..HEAD`.)
- [ ] **Step 3.** Дальше — по гейту dev-flow: ревью диффа пользователем, мерж в `main` и push — ТОЛЬКО по явному требованию (AGENTS.base.md §7).

**Выход:** ветка готова к ревью/мержу.
**Проверка:** Step 1 чист.
**Spec:** §8 Фаза 5.

---

## Self-review (покрытие spec)

| Spec | Задача |
|---|---|
| §5 структура OS3 (arch/src/docker/deploy/AGENTS/README) | Tasks 1–5 (выполнены) |
| Синхронизация OS3 с main (t36/t91: код+тесты+канон) | Task 5.5 |
| §5.1 OwnS3.slnx | Task 2 Step 2 (выполнено) |
| §5.2 deploy OS3 (build-image/compose/env/tls) | Task 3 (выполнено) |
| §5.3 AGENTS.md OS3 | Task 5 Step 1 (выполнено) |
| §5.4 README.md OS3 | Task 5 Step 2 (выполнено) |
| §6.1 arch/ pg | Task 1 (выполнено; merge сохранил; висячая href 19:758 — Task 11) |
| §6.2 клиентская копия протокола | Task 7 |
| §6.3 удаление проектов + slnx | Task 8 |
| §6.4 deploy/00-up.sh pg | Tasks 9, 13 |
| §6.5 E2E-окружения на внешний образ (S3Worker + пришедшие t36-контуры PgWorker и AdminPanel) | Task 10 |
| §6.6 AGENTS.md pg | Task 11 Step 1 |
| §6.7 docs-ссылки pg (вкл. runbook t36 и arch/19:758) | Tasks 1, 8, 11 |
| §8 фазы 0–5 | Tasks 1, 2–6 (синхр. 5.5), 7, 8–11, 12–14, 15 |
| §10.1–10.7 приёмка | Tasks 6, 14 |
| §9 порядок коммитов (initial OS3 с кодом main до чистки pg) | Task 5.5 → Task 6 → Task 8 |

Placeholder-скан: полные тексты всех НОВЫХ файлов приведены в задачах (S3OwnS3Image.cs — Task 10 Step 2); выполненные Tasks 1–5 ссылаются на git-историю и фактические файлы OS3 (перечисление содержимого не требуется — исполнение завершено). Команды копирования/sed/сборок — точные, строки сверены с WT после merge `ca7a0c5b`: deploy-compose сервис owns3 346–372 (build-блок ~348–351, docs-ссылка ~344, фраза про профиль ~383–384), 00-up.sh publish-фаза 75–81 / Arrange 45–50 / up-строка ~213 / t36-блоки 222–241 и ~487 не трогать, slnx /owns3/ 62–66 и тесты 84–85, AGENTS.md registry-абзац 43 (owns3-упоминаний сейчас нет — вставка чистая), runbook 210, arch/19-backups 758. Синхронизация Task 5.5: rsync-повтор Task 2 Step 1 идемпотентен (Shared/build-файлы в diff main пусты — копирование нулевое, включено для гарантии); периметр — 11 файлов кода + 7 тестов (2 юнит + 5 интеграционных), из них 4 новых; контроль — `diff -rq` обеих пар каталогов + наличие 5 новых файлов + grep System.IO.Hashing (csproj ~17, props 33 — версия 10.0.12 была и до main); канон — `git show ca7a0c5b^2:…` (в WT удалены ф0+merge); sed-адаптация = правила Task 1 Steps 2–3 + НОВЫЙ паттерн `(../adminpanel/` → `(../../../pg/arch/adminpanel/` (05:129, t91); ссылки `../roadmap/owns3.md` (02:16), `../../../minio` — валидны из OS3 без правки; текстовые `` `docs/…` `` (05:210/218, roadmap:16) — осознанный остаток. Аудит Task 5.5 Step 5 — полный без whitelist. Таблица 15 файлов протокола стабильна: diff main по Protocol = только AwsChunkedReader (изменён) + ChecksumConventions (новый), оба вне копии; diff по S3Worker-код/тестам пуст. ПЯТЬ сборщиков owns3-образа (шапка) закрыты Task 10: S3Worker-пара по старым шагам; PgWorker-пара (E2eEnvironment.cs: константа 56 + EnsureOwnS3ImageAsync 1727–1772; Backups/OwnS3.cs: EnsureImageAsync 93–137, Image наследует константу) и AdminPanel S3NodeContainerFixture.cs (константа 31 + EnsureImageAsync 78–125 + комментарии тега 16/76/132; RunToolAsync — по факту использования) — переведены на внешний owns3:dev с fail-fast; контрольный grep ловит оба старых тега (`owns3:e2e` покрывает и `owns3:e2e-it` как подстроку). runbook.md добавлен в sed Task 11 Step 2 (текстовая ссылка t36 — иначе критерий (4) Task 14 падает; проверено прогоном цепочки по текущему WT). Висячая href `arch/19-backups.md:758` (пришла из main мимо sed Task 1 Step 6; единственное вхождение `](owns3/` по arch/) чинится вторым sed Task 11 Step 2; Task 11 Step 4 повторяет проверку Task 1 Step 8 по текущему WT — оба grep'а пусты. Критерий (2) Task 14 Step 5 адаптирован под t36: `PgWorker.IntegrationTests` исключён из цепочки с обоснованием (7 файлов — легитимные потребители внешнего owns3:dev, остаются в pg по приказу); проверено прогоном по текущему WT: без исключения список непуст, с исключением — пуст (после Task 8). Чек 45-backups-storage.sh добавлен в прогон Task 12 (пул бэкапов ownS3 — потребитель внешнего образа). Осознанные остатки: superpowers t36/t91 остаются в pg (периметр приказа закрытый; покрыты исключением docs/superpowers/ в (4)); текстовые шорткаты «owns3/NN §X» в arch/23/24 (Task 1 Step 6); упоминания `arch/owns3` в contour/env/tls/чеках (Task 9 Step 3); docker-E2E серии PgWorker/AdminPanel за `PGW_TEST_DOCKER=1` в Task 14 не гоняются (приказ: «полный test валиден как есть»; перевод всех пяти сборщиков проверен сборкой Step 7 Task 10 и grep Step 8). Типовая консистентность: имя образа везде `owns3:dev` (теги `owns3:e2e-it`/`owns3:e2e` упразднены), скрипт сборки — `../OwnS3/deploy/build-image.sh`, ссылки канона pg→OS3 — `../OwnS3/...`, OS3→pg — рабочие относительные `../../pg`/`../../../pg` (аудит Task 5.5 Step 5). Счётчики тестов не заявляются нигде.
