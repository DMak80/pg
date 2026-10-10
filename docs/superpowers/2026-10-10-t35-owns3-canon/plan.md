# t35-owns3-canon — план реализации (канон ownS3: arch/owns3/ + roadmap)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Наполнить канон ownS3 до полноты, достаточной для реализации t36–t39 «по канону, без досказок»: входная точка `arch/22-owns3.md` + пять глав `arch/owns3/01…05-*.md` + отражение в `arch/roadmap/owns3.md` (цель-замещение MinIO, имя `.owns3.sys`, новые задачи t41/t42) и указатель в `arch/README.md`.

**Architecture:** arch-only задача — код не пишется, только `arch/**` и указатели. Структура канона — подпапка `arch/owns3/` с пятью главами (прецедент `arch/adminpanel/`); `arch/22-owns3.md` — короткая входная точка в списке `arch/`. Задачи 1–2 (входная точка, глава 01) — обязательные предшественники; задачи 3–6 (главы 02–05, фазы 3–6) независимы друг от друга — их порядок допускает перестановку (spec §4); задача 7 (roadmap-отражение готового канона) — последняя; задача 8 — финальный гейт по критериям приёмки spec §6.

**Tech Stack:** Markdown (стиль соседних канонов `arch/`: §-нумерация, таблицы решений, ASCII-схемы); справочники при написании — S3 API Reference (знания исполнителя) и исходники MinIO `/Users/demakaev/ZCodeProject/minio` (только чтение); проверки — grep/test/ls/git в worktree.

**Spec:** `docs/superpowers/2026-10-10-t35-owns3-canon/spec.md` (исполняется вместе с планом; аргумент плана — от спеки, формулировки — из её §3).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon`, ветка `docs-t35-owns3-canon` (feature-ветка: коммиты свободны, по одной на задачу; мерж в `main` — только через ревью dev-flow).

## Global Constraints

- **Только arch/roadmap/README-указатели** (spec §1.3, §6.9): изменения — исключительно в `arch/22-owns3.md`, `arch/owns3/**`, `arch/roadmap/owns3.md`, `arch/README.md` и `docs/superpowers/2026-10-10-t35-owns3-canon/**` (spec/plan). `src/**`, `deploy/**`, `dev-stand/**`, любые другие каталоги — не трогать.
- **Имена служебных сущностей ownS3 — собственные** (решение пользователя 8): служебный контур — `.owns3.sys`; файл формата тома — `volume.json`. Осознанный (закрытый) список имён MinIO-происхождения: `xl.meta`, `part.N`, `uploads.json`, sha256-путь multipart — и только они. Строка `.minio.sys` не встречается ни в одном новом/правимом документе ownS3-канона; имена файлов MinIO (`cmd/*.go`, `format.json`, `encodeDirObject`) допустимы только как явные референс-указатели.
- **Критерий каждого решения** (spec §2): стандарт S3 API / референс MinIO (точечный, `../../../minio` от файлов `arch/owns3/`) / простота — критерий указывается у каждого зафиксированного решения (таблица решений главы 01; в остальных главах — кратко в месте фиксации).
- **Язык и стиль**: русский текст, английские идентификаторы (имена операций, заголовки, env, коды ошибок); структура и тон — как у соседних канонов `arch/`; никаких «TBD»/«TODO»/плейсхолдеров; без исторических пассажей и атрибуций задач (правило AGENTS.md: документы описывают текущее/планируемое состояние).
- **Заголовки операций в главе 02** — единый формат `### <Operation>` (например, `### PutObject`), группы — `## N. <Группа>`: на это завязаны проверки полноты grep'ом.
- **Относительные ссылки** из `arch/owns3/*.md`: соседние каноны — `../19-backups.md`, `../18-metrics.md`; трек ownS3 — `../roadmap/owns3.md`; параллельный трек s3 — `../roadmap/s3.md`; входная точка — `../22-owns3.md`; референс MinIO — `../../../minio`. Внутренние ссылки (`../…` внутри репозитория, включая `../roadmap/s3.md`) проверяются `test -e` из worktree. Упоминания deploy-артефактов (`deploy/.env.example`, `images.txt`) — текстом, без маркдаун-ссылок. Ссылка `../../../minio` разрешается из **основного чекаута** (`/Users/demakaev/ZCodeProject/pg`, куда документы попадут после мержа), а worktree лежит на уровень глубже — из worktree `../../../minio` НЕ разрешается: цель этой ссылки проверять только абсолютным путём (`test -d /Users/demakaev/ZCodeProject/minio`), а форму ссылки в документах — текстово (`grep` по `../../../minio`); `test -e ../../../minio` из worktree не применять и ссылку под worktree не «чинить» (`../../../../minio` был бы верен в worktree и сломан в `main`).
- **Коммит на задачу** (spec §4): каждая задача завершается отдельным коммитом в feature-ветке; сообщения — префикс `arch:` либо уточнённый `arch/<путь>:` (как в задачах: `arch/owns3/02:`, `arch/roadmap:`), для spec/plan — `docs:`; по-русски, с `(t35)` в конце.
- **Сверка с референсом**: при написании глав исполнитель читает указанные файлы `/Users/demakaev/ZCodeProject/minio/cmd/*.go` (карта референса — во введении `arch/roadmap/owns3.md`); перенос механики целиком не делается, отступления от MinIO фиксируются явно с критерием.

---

## Задача 1: `arch/22-owns3.md` — входная точка канона (фаза 1)

**Spec:** §3.1, §6.1; решения пользователя 1, 7, 8.
**Вход:** spec утверждён; worktree чист, кроме непроиндексированной папки `docs/superpowers/2026-10-10-t35-owns3-canon/` (spec; не трогать до задачи 8).
**Files:** Rewrite: `arch/22-owns3.md` (текущая заготовка ~23 строк заменяется целиком).

- [ ] **Шаг 1. Переписать `arch/22-owns3.md`** структурой ровно из spec §3.1:

  1. Заголовок `# 22. ownS3: собственное S3-хранилище ★` (сохранить).
  2. **Миссия**: ownS3 — собственное S3-совместимое объектное хранилище на C#/.NET 10 (проекты `src/OwnS3.*`); **целевая замена MinIO подсистемы бэкапов после приёмки t39** (клиенты переключаются env-endpoint'ом, код клиентов не меняется); целевые клиенты — AWS SDK .NET подсистемы бэкапов (arch/19) и `mc`; критерий правильности — стандарт S3 API.
  3. **Принцип «стандарт S3, а не копия MinIO»**: сохранить суть текущего раздела; референс — `../../../minio` (от `arch/22-owns3.md` — `../../minio`); единственное, что берётся у MinIO сознательно, — xl-раскладка хранения на диске; служебный контур — **`.owns3.sys`**, имена служебных сущностей задаёт канон ownS3; побайтовая совместимость `xl.meta` не требуется.
  4. **Границы начального этапа**: 22 операции (объекты/листинги/бакеты/multipart); бакеты без versioning; single-drive без erasure coding и репликации; обращение к вне-наборным граням — единый канонизированный отказ (глава 02); path-style only, HTTP-only; последующие этапы — трек.
  5. **Указатель глав** (одной строкой на главу, относительные ссылки `owns3/0N-*.md`):
     - `owns3/01-overview.md` — место в системе, клиенты, состав проектов, durability-ограничения, таблица ключевых решений;
     - `owns3/02-operations.md` — полный контракт 22 операций + общие семантики (ETag, conditional, Range);
     - `owns3/03-protocol.md` — SigV4 (заголовочная/presigned/чанковая), XML-схемы, формат ошибок, транспорт;
     - `owns3/04-storage.md` — xl-хранение single-drive: раскладка, `xl.meta`, `.owns3.sys`, атомарность, чистки;
     - `owns3/05-access-config.md` — учётки, матрица прав, конфиг/env, health/метрики, запуск в докере.
  6. **Ссылка на трек**: `arch/roadmap/owns3.md` (порядок t35–t40, новые задачи после t39).
  Без исторических пассажей, без «наполнение задачей» (канон уже наполнен — фазы 2–6).

- [ ] **Шаг 2. Самопроверка файла**: заголовки-указатели дают 5 строк; упоминается `.owns3.sys` и не встречается `.minio.sys`; нет TBD/TODO; миссия содержит «целевая замена MinIO подсистемы бэкапов».

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
grep -c 'owns3/0[1-5]-' arch/22-owns3.md                 # → 5
grep -n '\.minio\.sys' arch/22-owns3.md                  # → пусто
grep -niE 'TBD|TODO|FIXME' arch/22-owns3.md              # → пусто
grep -n 'целевая замена MinIO' arch/22-owns3.md          # → 1 строка
grep -n '\.\./\.\./\.\./minio' arch/22-owns3.md          # → пусто (трёхуровневая форма — дефект: arch/22-owns3.md лежит на уровень выше arch/owns3/, корректная глубина — ../../minio)
grep -c '\.\./\.\./minio' arch/22-owns3.md               # ≥ 1 (референс-ссылка правильной формы; цель — /Users/demakaev/ZCodeProject/minio из основного чекаута, из worktree test -e не применять)
```

- [ ] **Шаг 3. Commit**

```bash
git add arch/22-owns3.md && git commit -m "arch: 22-owns3 — входная точка канона ownS3: миссия (целевая замена MinIO после t39), границы этапа, указатель глав arch/owns3/01-05 (t35)"
```

**Выход:** входная точка канона готова и ссылается на главы (создаются в задачах 2–6; до их создания ссылки временно висячие — устраняется финальным гейтом задачи 8).
**Проверка:** команды шага 2 все зелёные (5 / пусто / пусто / 1).
**Связь со spec:** §3.1 (артефакт), §6.1 (критерий приёмки), решения пользователя 1, 7, 8.

---

## Задача 2: `arch/owns3/01-overview.md` — обзор и структура решения (фаза 2)

**Spec:** §3.2, §6.2; решения пользователя 3, 5, 6, 7.
**Вход:** задача 1 закоммичена.
**Files:** Create: `arch/owns3/01-overview.md` (каталог `arch/owns3/` создаётся этим файлом).

- [ ] **Шаг 1. Раздел «Место в системе»** (spec §3.2 п. 1): ASCII-диаграмма «клиенты → ownS3 (докер-контейнер, том данных)»; клиенты — подсистема бэкапов (write: воркер/джобы бэкапов; read: restore/verify/drill — по `arch/19-backups.md`, клиент остаётся S3-агностичным) и `mc` (операции); ownS3 — standalone-сервис: без etcd-контракта, без provisioning-проектов, не воркер; отношение к MinIO — целевая замена после t39 (переключение клиентов env-endpoint'ом), трек s3 — ссылка `../roadmap/s3.md` (t31–t34, MinIO-кластера) — параллельный на переходный период.

- [ ] **Шаг 2. Раздел «Границы начального этапа»** (spec §3.2 п. 2): 22 операции (перечислить группами, детальные контракты — глава 02); бакеты без versioning (ListObjectVersions — unversioned-семантика); single-drive без EC/репликации; перечень вне-наборных граней (сабресурсы объекта: ACL/tagging/retention/legal-hold; конфигурации бакета: versioning/lifecycle/replication/encryption/…; S3-Express, notification, select) — единый отказ по главе 02; ссылка на трек для последующих этапов (t40 — erasure coding).

- [ ] **Шаг 3. Раздел «Состав решения»** (spec §3.2 п. 3): четыре строки с границами ответственности:
  - `src/OwnS3.Protocol` — чистые типы протокола: канонизация/проверка SigV4 (заголовочная, presigned, чанковая потоковая), XML-схемы запросов/ответов, формат S3-ошибок, ETag-семантика; без ASP.NET и без диска;
  - `src/OwnS3.Storage` — xl-модель хранения: раскладка каталогов, чтение/запись `xl.meta`, `.owns3.sys`, атомарный коммит, multipart на диске, чистки; без HTTP;
  - `src/OwnS3.App` — хост Kestrel: роутинг 22 операций, хендлеры, конфиг/env (`OwnS3:*` / `OWNS3_*`), health/метрики (канон `arch/18-metrics.md`, каркас `Shared.Metrics`), стыковка Protocol×Storage;
  - тесты `src/tests/OwnS3.UnitTests` (тест-векторы подписи, XML) и `src/tests/OwnS3.IntegrationTests` (контурные).
  Плюс явные границы зависимостей: `Protocol` и `Storage` не зависят от `App` и друг от друга; `App` — от обоих; общие библиотеки `Shared.*` — по мере надобности.

- [ ] **Шаг 4. Раздел «Ключевые решения»** (spec §3.2 п. 4): таблица «Решение / Почему / Критерий» со строками: path-style only (решение 3); HTTP-only на начальном этапе (решение 6); собственный формат `xl.meta`; root из env + статические access keys (решение 4); xl-раскладка хранения по механике MinIO (собственные имена служебного контура); три проекта решения (решение 5). Критерий каждой строки — «стандарт S3» / «референс MinIO» / «простота».

- [ ] **Шаг 5. Раздел «Запуск»** (spec §3.2 п. 5): ownS3 всегда запускается в докере (никогда хост-процессом — как PgWorker); том данных — единственный persistent-volume; env-секреты (root, ключи) из env-файла по образцу `deploy/.env.example` воркеров (env-шаблон ownS3 создаётся в t39 — канон фиксирует состав секрета в главе 05); правила E2E-образа: сборка .NET на хосте, в образ COPY только publish-вывод, локально собираемый образ в registry не кладётся, базовый runtime-образ — из существующих `images.txt` (новых внешних образов нет). Детальная модель dev-стенда — t39; здесь — только правила образа.

- [ ] **Шаг 6. Раздел «Durability и отказы»** (spec §3.2 п. 6): явный ответ на вопрос «что теряется при отключении машины X»: single-drive том — отказ диска/машины = потеря всех данных ownS3 на нём; если ownS3 уже обслуживает бэкапы — они недоступны на время отказа (или навсегда при потере диска); осознанное ограничение начального этапа; путь митигации — t40 (erasure coding) и последующие задачи (репликация); локальный том — единственная копия, гарантий durability нет.

- [ ] **Шаг 7. Проверка главы**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
grep -c '^## ' arch/owns3/01-overview.md                 # ≥ 6 разделов
grep -n '\.minio\.sys' arch/owns3/01-overview.md         # → пусто
grep -niE 'TBD|TODO|FIXME' arch/owns3/01-overview.md     # → пусто
grep -En 'целевая замена|t40' arch/owns3/01-overview.md  # durability и замещение упомянуты (-E: \| в BRE — GNU-расширение, BSD grep трактует литерально)
cd arch/owns3 && for p in ../19-backups.md ../18-metrics.md ../roadmap/owns3.md ../roadmap/s3.md; do test -e "$p" || echo "BROKEN: $p"; done   # → пусто (внутренние ссылки, включая трек s3, разрешаются из worktree)
grep -c '\.\./\.\./\.\./minio' 01-overview.md          # ≥ 1 (форма ссылки на референс — текстово)
test -d /Users/demakaev/ZCodeProject/minio || echo "MISSING REF DIR"   # → пусто (цель референс-ссылки; из worktree ../../../minio не разрешается — НЕ «чинить», см. Global Constraints)
```

- [ ] **Шаг 8. Commit**

```bash
git add arch/owns3/01-overview.md && git commit -m "arch/owns3/01: обзор — место в системе, границы этапа, состав трёх проектов, таблица решений, durability-ограничение (t35)"
```

**Выход:** глава 01 канона готова: обзор, состав решения, правила образа, ограничение durability.
**Проверка:** команды шага 7 зелёные; таблица решений содержит 6 строк из шага 4.
**Связь со spec:** §3.2 (артефакт), §6.2 (критерий), решения пользователя 3, 5, 6, 7.

---

## Задача 3: `arch/owns3/02-operations.md` — контракт 22 операций (фаза 3)

**Spec:** §3.3, §6.3; решения пользователя 2, 3.
**Вход:** задачи 1–2 закоммичены.
**Files:** Create: `arch/owns3/02-operations.md`.
**Референс при написании (карта трека):** `cmd/object-handlers.go` (+ `cmd/object-handlers-common.go`), `cmd/bucket-handlers.go`, `cmd/bucket-listobjects-handlers.go`, `cmd/object-multipart-handlers.go` — референс-хендлер указывается в контракте каждой операции (файл из этого списка).

- [ ] **Шаг 1. Вводная часть главы**: назначение; **единый шаблон контракта** (из spec §3.3, семь пунктов): (1) назначение 1–2 предложения + референс-хендлер MinIO; (2) HTTP: метод, path-style URI (`/{bucket}`, `/{bucket}/{key+}`), обязательные/опциональные query-параметры; (3) заголовки запроса (специфичные: `Content-MD5`, `x-amz-*`, conditional, `x-amz-copy-source`, `x-amz-metadata-directive`, `x-amz-storage-class`); (4) тело запроса (XML-схема при наличии); (5) успех: код, заголовки ответа, тело/XML-схема; (6) ошибки: коды S3 и условия; (7) семантика: атомарность/идемпотентность, ETag-правило, conditional, видимость в листингах. Соглашение об именах заголовков: контракты используют `### <Operation>`.

- [ ] **Шаг 2. Раздел «Общие семантики»** (вынесены из контрактов, не дублируются):
  - **ETag-правила**: простой PUT — hex-MD5 содержимого; multipart — составной `<N>-<md5(concat part-ETag'ов)>`; ETag у CopyObject — семантика по стандарту (ответ CopyObjectResult, заголовок ETag возвращается);
  - **Conditional-запросы**: `If-Match`/`If-None-Match`/`If-Modified-Since`/`If-Unmodified-Since`; комбинации и приоритет по стандарту (If-Match + If-None-Match/If-Modified-Since → приоритет If-Match; If-None-Match + If-Modified-Since → приоритет If-None-Match; зафиксировать таблицей), исходы 304 (Not Modified) / 412 (PreconditionFailed);
  - **Range**: `bytes=a-b`, суффиксный `bytes=-N`, множественный диапазон — поведение зафиксировать по сверке с референсом и стандартом (ожидаемо: не поддерживается, ответ 200 полным объектом — зафиксировать точно); `If-Range`; 206 Partial Content / 416 InvalidRange;
  - **Лимиты размеров** (условие кода `EntityTooLarge` — таблица маппинга главы 03): простой PUT — объект ≤ 5 ГБ по стандарту (превышение → `EntityTooLarge`; большие объекты загружаются multipart); `UploadPart` — часть ≤ 5 ГБ (превышение → `EntityTooLarge`);
  - **Вне-наборные грани**: перечень сабресурсов и конфигураций бакета из задачи 2 (шаг 2) — единый отказ `NotImplemented` (501), код и сообщение зафиксировать (база — S3-стандарт; точное поведение — по референсу MinIO с фиксацией текста `Message`);
  - **Идемпотентность DeleteObject**: отсутствие ключа — успех (204).

- [ ] **Шаг 3. Группа «Объекты» (7 контрактов)**: `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject`, `GetObjectAttributes`. Обязательные фиксации: PutObject — `Content-MD5` (опционален, при наличии проверяется, несовпадение → BadDigest), `x-amz-meta-*` (произвольные user-metadata), `x-amz-storage-class` игнорируется (фиксация: принимается, влияние отсутствует), тело может идти чанковой подписью (глава 03), успех 200 + `ETag`; GetObject/HeadObject — Range, conditional, заголовки ответа (`ETag`, `Last-Modified`, `Content-Type`, `Content-Length`, `x-amz-meta-*`, `Accept-Ranges`), различие Get (тело) / Head (без тела); DeleteObjects — запросный XML `Delete` (до 1000 ключей за запрос — фиксация по стандарту, превышение → MalformedXML), ответ `DeleteResult` (Deleted/Error на каждый ключ); CopyObject — `x-amz-copy-source` (только внутренняя копия), `x-amz-metadata-directive` COPY/REPLACE, copy-условия (conditional на источник), ответ `CopyObjectResult`; GetObjectAttributes — роутинг по query-параметру `attributes` (стандарт: `GET /{key}?attributes` — дискриминатор операции, обязателен; его отправляют стандартные клиенты, включая AWS SDK .NET; `partNumber`-варианта нет), состав запрашиваемых атрибутов — заголовок `x-amz-object-attributes` (поддерживаемые значения зафиксировать: `ETag`, `ObjectSize`, `StorageClass`, `ObjectParts` — минимальный набор, достаточный целевым клиентам); заголовки ответа: `Last-Modified`, `ETag` (дублируется в теле — схема `GetObjectAttributesOutput`, глава 03); `ResponseExpires` (`response-expires`) операция не возвращает (в отличие от GetObject) — зафиксировать явно; сверить по референсу `cmd/object-handlers.go`.

- [ ] **Шаг 4. Группа «Листинги» (3 контракта)**: `ListObjects` (v1: `marker`, `NextMarker`), `ListObjectsV2` (`list-type=2`, `continuation-token`, `start-after`, `KeyCount`, `fetch-owner` — Owner-элемент не возвращается без запроса и содержит заполнитель при запросе: зафиксировать), `ListObjectVersions` (unversioned: возвращает текущие объекты, без delete markers, без `VersionId`-полей — зафиксировать точную схему ответа). Общая семантика листингов (один раздел на группу): фильтрация `prefix` (ключи, начинающиеся с префикса; отсутствующий/пустой — весь бакет; ключевой параметр целевых клиентов — листинг цепочек бэкапов по префиксу); лексикографический порядок ключей (по UTF-8 байтам); `max-keys` (дефолт 1000, максимум 1000, 0 — пустой ответ с `IsTruncated=false`); `IsTruncated` + механизм продолжения (v1 — `NextMarker`/последний ключ; V2 — `NextContinuationToken`); `delimiter` → `CommonPrefixes`; `encoding-type=url` (единственное значение; без параметра — без кодирования).

- [ ] **Шаг 5. Группа «Бакеты» (5 контрактов)**: `CreateBucket` (семантика существования: бакет уже есть → 409 `BucketAlreadyOwnedByYou` — владельческой модели нет, зафиксировать, сверить по референсу `cmd/bucket-handlers.go`; `LocationConstraint` тела/`x-amz-bucket-region` принимаются и игнорируются — фиксация), `DeleteBucket` (только пустой; непустой → 409 `BucketNotEmpty`; несуществующий → 404 `NoSuchBucket`; идемпотентность отсутствия — 404), `HeadBucket` (200/404 без тела), `ListBuckets` (`ListAllMyBucketsResult`, Owner — заполнитель: зафиксировать), `GetBucketLocation` (`LocationConstraint` — фиксированное значение `us-east-1` (пустое значение элемента по стандарту означает us-east-1; зафиксировать возвращаемое пустым элементом — сверить по референсу `cmd/bucket-handlers.go`)).

- [ ] **Шаг 6. Группа «Multipart» (7 контрактов)**: `CreateMultipartUpload` (uploadId — формат зафиксировать: UUID v4; ответ `InitiateMultipartUploadResult`), `UploadPart` (`partNumber` 1–10000, вне диапазона → 400 `InvalidArgument`; ETag части — hex-MD5 тела; без `partNumber`/`uploadId` → `InvalidArgument`/`NoSuchUpload`), `UploadPartCopy` (`x-amz-copy-source` + `x-amz-copy-source-range`; ответ `CopyPartResult`), `CompleteMultipartUpload` (запросный XML-манифест `CompleteMultipartUpload/Part/{PartNumber,ETag}`: минимум 1 часть, максимум 10000, порядок строго возрастания `PartNumber` иначе `InvalidPartOrder`; несовпадение ETag/номера → `InvalidPart`; ответ `CompleteMultipartUploadResult` с составным ETag `<N>-<md5-all-ETag'ов>`), `AbortMultipartUpload` (несуществующий uploadId → 404 `NoSuchUpload`; повторный Abort — зафиксировать: успех при живущей загрузке, `NoSuchUpload` после), `ListParts` (`ListPartsResult`, `max-parts`, `part-number-marker`), `ListMultipartUploads` (`ListMultipartUploadsResult`, `prefix`/`delimiter`, фильтры по загрузкам).

- [ ] **Шаг 7. Проверка полноты и единства шаблона**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
for op in PutObject GetObject HeadObject DeleteObject DeleteObjects CopyObject GetObjectAttributes ListObjects ListObjectsV2 ListObjectVersions CreateBucket DeleteBucket HeadBucket ListBuckets GetBucketLocation CreateMultipartUpload UploadPart UploadPartCopy CompleteMultipartUpload AbortMultipartUpload ListParts ListMultipartUploads; do grep -qx "### $op" arch/owns3/02-operations.md || echo "MISSING: $op"; done   # → пусто (все 22; -x — целая строка, иначе GetObject ложно проходит по GetObjectAttributes и т.п.)
grep -Ec 'cmd/object-handlers|cmd/bucket-handlers|cmd/bucket-listobjects-handlers|cmd/object-multipart-handlers' arch/owns3/02-operations.md   # ≥ 22 (референс у каждой операции; -E — альтернация для BSD grep)
grep -n '\.minio\.sys' arch/owns3/02-operations.md       # → пусто
grep -niE 'TBD|TODO|FIXME' arch/owns3/02-operations.md   # → пусто
```

  Плюс ручная сверка: каждый контракт содержит все 7 пунктов шаблона шага 1; общие семантики не дублируются внутри контрактов (ссылки на раздел «Общие семантики»).

- [ ] **Шаг 8. Commit**

```bash
git add arch/owns3/02-operations.md && git commit -m "arch/owns3/02: полный контракт 22 операций (объекты/листинги/бакеты/multipart) + общие семантики ETag/conditional/Range/вне-наборные грани (t35)"
```

**Выход:** глава 02 — полный контракт всех 22 операций по единому шаблону; t36–t38 реализуют операции без досказок.
**Проверка:** цикл шага 7 без вывода MISSING; счётчик референсов ≥ 22; шаблон един (ручная сверка).
**Связь со spec:** §3.3 (артефакт и состав 22 операций), §6.3 (критерий), решение пользователя 2 (полный контракт).

---

## Задача 4: `arch/owns3/03-protocol.md` — протокол (фаза 4)

**Spec:** §3.4, §6.4; решения пользователя 3, 6.
**Вход:** задачи 1–2 закоммичены (глава не зависит от 02; в тексте ссылки на «глава 02» для кодов ошибок операций).
**Files:** Create: `arch/owns3/03-protocol.md`.
**Референс при написании:** `cmd/auth-handler.go`, `cmd/signature-v4.go`, `cmd/signature-v4-parser.go`, `cmd/signature-v4-utils.go`, `cmd/streaming-signature-v4.go`.

- [ ] **Шаг 1. Раздел «SigV4: заголовочная подпись»** (spec §3.4 п. 1): canonical request (метод; path-style canonical URI — каждое сегментное URI-кодирование; canonical query string — сортировка по ключу; canonical headers — нижний регистр, сортировка, `host` обязателен; `x-amz-content-sha256` в подписанном списке); string-to-sign (алгоритм `AWS4-HMAC-SHA256`, `x-amz-date`, credential scope `<date>/<region>/s3/aws4_request` — зафиксировать region-заполнитель, принимаемый из credential, без гео-семантики); деривация signing key (цепочка HMAC: `AWS4<secret>` → date → region → service → signing); обязательные заголовки `x-amz-date` (формат `yyyyMMdd'T'HHmmss'Z'`) и `Host`; допустимый clock skew ±15 минут (нарушение → `RequestTimeTooSkewed`); допустимые значения `x-amz-content-sha256`: hex-SHA256 тела / `UNSIGNED-PAYLOAD` / `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`; несовпадение подписи → `SignatureDoesNotMatch`; анонимный запрос → `AccessDenied` (public-доступа нет).

- [ ] **Шаг 2. Раздел «Presigned URL»** (spec §3.4 п. 2): query-параметры `X-Amz-Algorithm=AWS4-HMAC-SHA256`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-SignedHeaders`, `X-Amz-Signature`; подпись тела не участвует (`UNSIGNED-PAYLOAD`-семантика); максимум `X-Amz-Expires` — 604800 секунд (7 дней; превышение/отрицательное → `AuthorizationQueryParametersError`); просроченный presigned → `AccessDenied`; перечень операций, разрешённых через presigned (зафиксировать ровно потребности клиентов бэкапов и `mc`): `GetObject`, `PutObject`, `DeleteObject`, `HeadObject`, `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`; прочие операции через presigned → `AuthorizationQueryParametersError` (зафиксировать).

- [ ] **Шаг 3. Раздел «Чанковая потоковая подпись»** (spec §3.4 п. 3): `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD` + `Content-Encoding: aws-chunked` + обязательный `x-amz-decoded-content-length`; фрейминг: `[hex-размер чанка];chunk-signature=<подпись>\r\n<данные>\r\n`, финальный `0;chunk-signature=<подпись>` (без данных после); цепочка подписей: seed = подпись исходного запроса → string-to-sign чанка N включает подпись чанка N-1 → сверка каждого чанка, разрыв цепочки → `SignatureDoesNotMatch` (зафиксировать код и момент отказа); trailers `x-amz-trailer`: зафиксировать по референсу `cmd/streaming-signature-v4.go` и потребностям AWS SDK .NET минимальный поддерживаемый набор (ожидаемо `x-amz-checksum-*` — зафиксировать точно: какие принимаются и сверяются, прочие → отказ с каким кодом); неподдерживаемые значения `x-amz-content-sha256` (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`, варианты `StreamBody-…`) — не поддерживаются (зафиксировать отказ `InvalidRequest`/`NotImplemented` по результату сверки со стандартом — записать точно).

- [ ] **Шаг 4. Раздел «XML-схемы»** (spec §3.4 п. 4): namespace `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`; элементы всех XML-ответов (перечислить схемами-образцами): листинги v1 (`ListBucketResult` + `Contents/Key/LastModified/ETag/Size/Owner?`), V2 (`ListBucketResult` + `KeyCount`/`StartAfter`/`ContinuationToken`/`NextContinuationToken`), versions (`ListVersionsResult`, unversioned-вид — глава 02), `DeleteResult`, `InitiateMultipartUploadResult`, `CompleteMultipartUploadResult`, `ListPartsResult`, `ListMultipartUploadsResult`, `LocationConstraint`, `ListAllMyBucketsResult`, `CopyObjectResult`, `CopyPartResult`, `GetObjectAttributesOutput` (ETag/ObjectParts/StorageClass/ObjectSize — операция из набора 22, возвращающая XML); запросные XML (`Delete`, `CompleteMultipartUpload`-манифест): валидация, некорректный XML → `MalformedXML` (400); кодирование спецсимволов (`&<>`, числовые ссылки); `Content-Type: application/xml` у всех XML-ответов.

- [ ] **Шаг 5. Раздел «Формат ошибок»** (spec §3.4 п. 5): канонический XML `<Error><Code/><Message/><Resource/><RequestId/><HostId/></Error>` (зафиксировать `HostId` — статичный идентификатор инстанса); маппинг внутренних сбоев на S3-коды — таблица со списком: `NoSuchBucket`, `NoSuchKey`, `BucketAlreadyExists` (не эмитируется ownS3: владельческой модели нет — CreateBucket отвечает `BucketAlreadyOwnedByYou`; в таблице с явной пометкой «не используется, справочный алиас»), `BucketAlreadyOwnedByYou`, `BucketNotEmpty`, `InvalidRange`, `PreconditionFailed`, `NotModified`, `EntityTooLarge` (условие — лимиты размеров главы 02: простой PUT и часть multipart ≤ 5 ГБ), `InvalidPart`, `InvalidPartOrder`, `MalformedXML`, `AuthorizationHeaderMalformed`, `AuthorizationQueryParametersError`, `SignatureDoesNotMatch`, `AccessDenied`, `RequestTimeTooSkewed`, `BadDigest`, `NoSuchUpload`, `InvalidArgument`, `InvalidBucketName` (нарушение правил имени бакета 3–63 — фиксация шага 6), `InvalidRequest` (неподдерживаемые значения `x-amz-content-sha256` — если выбран в шаге 3), `NotImplemented`, `InternalError` (условия каждой — ссылка на операцию главы 02 или условие протокола; перечень — обязательный минимум: все коды, зафиксированные в шагах 3 и 6, включаются в таблицу — глава 03 единая точка маппинга); `RequestId` — генерация (UUID) на каждый запрос + запись в лог запроса.

- [ ] **Шаг 6. Раздел «Транспорт и стиль»** (spec §3.4 п. 6): path-style only — виртуальные хосты не поддерживаются: host-header обрабатывается как обычный host (без извлечения бакета), бакет всегда из пути; в подписи — тоже path-style (зафиксировать: virtual-host style у клиента не работает против ownS3, `mc`/SDK настраиваются на path-style); HTTP/1.1 — основной; HTTP/2 при Kestrel — зафиксировать поведение (принимается, семантика операций неизменна); лимиты: ключ объекта ≤ 1024 байт (в UTF-8; превышение → 400), имя бакета 3–63 символа (строчные `a-z`, `0-9`, `-`; начало/конец буквой или цифрой; нарушение → `InvalidBucketName`); `Expect: 100-continue` — поддерживается Kestrel'ом штатно (фиксация).

- [ ] **Шаг 7. Проверка главы**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
for t in 'AWS4-HMAC-SHA256' 'UNSIGNED-PAYLOAD' 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' 'X-Amz-Algorithm' 'aws-chunked' 'x-amz-decoded-content-length' 'chunk-signature' 's3.amazonaws.com/doc/2006-03-01/' 'RequestId' '604800' '1024' 'SignatureDoesNotMatch'; do grep -q "$t" arch/owns3/03-protocol.md || echo "MISSING: $t"; done   # → пусто
grep -n '\.minio\.sys' arch/owns3/03-protocol.md         # → пусто
grep -niE 'TBD|TODO|FIXME' arch/owns3/03-protocol.md     # → пусто
```

- [ ] **Шаг 8. Commit**

```bash
git add arch/owns3/03-protocol.md && git commit -m "arch/owns3/03: протокол — SigV4 трёх режимов (заголовочная/presigned/чанковая), XML-схемы, формат ошибок с маппингом, path-style/HTTP-only, лимиты (t35)"
```

**Выход:** глава 03 — полное протокольное описание; t36 реализует подпись/XML/ошибки без досказок.
**Проверка:** цикл шага 7 без MISSING; все три режима SigV4 описаны с фреймингом и цепочкой подписей.
**Связь со spec:** §3.4 (артефакт), §6.4 (критерий), решения пользователя 3, 6.

---

## Задача 5: `arch/owns3/04-storage.md` — xl-хранение single-drive (фаза 5)

**Spec:** §3.5, §6.5; решения пользователя 8.
**Вход:** задачи 1–2 закоммичены (глава не зависит от 03).
**Files:** Create: `arch/owns3/04-storage.md`.
**Референс при написании:** `cmd/xl-storage.go` (раскладка/кодирование имён), `cmd/xl-storage-format-v2.go` (устройство xl.meta — образец, не формат), `cmd/erasure-object.go` (запись через tmp + атомарный rename-коммит), `cmd/format-erasure.go` (format-тома xl-single), `cmd/erasure-multipart.go` (multipart на диске).

- [ ] **Шаг 1. Раздел «Раскладка тома»** (spec §3.5 п. 1): ASCII-дерево каталогов тома — перенести из spec §3.5 п. 1 дословно:

  ```
  <том>/
  ├── .owns3.sys/
  │   ├── volume.json                ← формат/идентификация тома (см. п. 3)
  │   ├── tmp/                       ← staging записи (tmp-префикс guid)
  │   ├── multipart/<sha256(bucket/object)>/<uploadID>/   ← части multipart + uploads.json
  │   ├── .trash/                    ← отложенное удаление (rename вместо rm)
  │   ├── buckets/<bucket>/          ← служебные метаданные бакета
  │   └── config/                    ← конфиг и ключи (глава 05)
  ├── <bucket>/                      ← бакеты — каталоги первого уровня
  │   └── <путь объекта (кодированный)>/
  │       ├── xl.meta                ← метаданные объекта
  │       └── <dataDir-uuid>/part.1  ← данные (одна часть для простого PUT)
  ```

  Имена: `dataDir` — UUID; части — `part.N`, N с 1; схема кодирования имени объекта в путь каталогов — по образцу MinIO `encodeDirObject` (сегментное кодирование слэшей и спецсимволов, ограничение длины пути файловой системы; зафиксировать конкретную схему: слэши ключа → вложенные каталоги, спецсимволы — percent-кодирование сегмента; сверить по `cmd/xl-storage.go`).

- [ ] **Шаг 2. Раздел «Формат `xl.meta`»** (spec §3.5 п. 2): собственный бинарный формат по образцу устройства MinIO xl-meta-v2 (НЕ msgp, побайтовая совместимость не требуется): header — magic-байты (зафиксировать: ASCII `OWS3` + uint8 версия формата, стартовая версия 1) + записи-версии; поля записи: versionId (UUID), размер объекта, modTime (UTC, фиксировать точность — мс), metadata (contentType, user-metadata словарь, etag, headers), checksum данных — зафиксировать алгоритм **SHA-256** (критерий: целостность бэкапов при чтении; сверка при каждом чтении данных, несовпадение → InternalError с диагностикой в лог); зарезервированная расширяемость: новые поля записи и EC-поля (Erasure-набор) — через версию формата (t40), запись содержит поле «версия записи»; порядок записи: текущий `xl.meta` → `xl.meta.bkp` → новый файл + атомарный rename (по образцу MinIO; фиксация: `xl.meta.bkp` — страховочная копия, используется диагностикой, не клиентами).

- [ ] **Шаг 3. Раздел «`volume.json` тома»** (spec §3.5 п. 3): собственное имя формата тома (референс MinIO — `format.json`): поля — magic/версия формата, uuid тома, режим `XL Single`; проверка при старте: отсутствие/чужой magic/несовместимая версия → отказ старта с диагностикой в лог и ненулевым exit (по образцу `cmd/format-erasure.go`); создание на первом старте пустого тома (зафиксировать: первый старт создаёт `.owns3.sys/` + `volume.json`, повторный — валидирует).

- [ ] **Шаг 4. Раздел «Жизненный цикл записи»** (spec §3.5 п. 4): данные → `.owns3.sys/tmp/<guid>/…` (part-файлы + предварительный `xl.meta`) → fsync файлов и каталога → атомарный rename целевого каталога объекта в bucket-дерево (коммит); перезапись существующего объекта — новый dataDir-UUID, старый каталог-данных переименовывается в `.trash/` (метаданные `xl.meta` заменяются в том же коммите); delete — rename каталога объекта в `.trash/`; видимость: объект появляется в Get/Head/List только после rename; сбой на любом шаге оставляет мусор только в `tmp/`/`.trash/`.

- [ ] **Шаг 5. Раздел «Multipart на диске»** (spec §3.5 п. 5): `.owns3.sys/multipart/<sha256(bucket/object)>/<uploadID>/` — `part.N` загруженных частей + `xl.meta` загрузки; журнал активных загрузок `uploads.json` в каталоге `<sha256(bucket/object)>/` — записи: uploadID, время инициации, access key создателя (для матрицы видимости главы 05); Complete — сборка частей rename'ами в целевой объект + составной ETag + новая запись `xl.meta`; Abort — rename каталога загрузки в `.trash/`; чистка брошенных загрузок по возрасту — порог зафиксировать: **24 часа** (критерий: референс MinIO purge по умолчанию 24 ч; потребности клиентов бэкапов — загрузки живут минуты).

- [ ] **Шаг 6. Раздел «Восстановление и чистки»** (spec §3.5 п. 6): старт — валидация `volume.json` (шаг 3), полное содержимое `tmp/` удаляется (безусловно — это незакоммиченный staging), чистка `.trash/` по порогу возраста — зафиксировать: **1 час** (критерий: простота; содержимое `.trash/` никогда не читается клиентами); фон: периодическая чистка `.trash/` и брошенных multipart-загрузок теми же порогами; мусор в `tmp/`/`.trash/` не виден клиентам по построению раскладки.

- [ ] **Шаг 7. Раздел «Согласованность»** (spec §3.5 п. 7): read-after-write коммита (rename в одной ФС атомарен); листинги читают те же каталоги — согласованы с коммитами без отдельного механизма; без versioning: перезапись атомарно заменяет видимые данные (новый dataDir + новый `xl.meta` одним коммитом).

- [ ] **Шаг 8. Проверка главы**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
for t in '.owns3.sys' 'volume.json' 'tmp/' 'multipart/' '.trash/' 'buckets/' 'config/' 'xl.meta' 'part.N' 'uploads.json' 'dataDir' 'SHA-256' 'rename' 'encodeDirObject' 'XL Single'; do grep -q "$t" arch/owns3/04-storage.md || echo "MISSING: $t"; done   # → пусто
grep -n '\.minio\.sys' arch/owns3/04-storage.md          # → пусто
[ "$(grep -c 'format\.json' arch/owns3/04-storage.md)" -le 1 ] || echo "TOO MANY format.json"   # → пусто; единственное допустимое употребление — референс-примечание «(референс MinIO — `format.json`)» из шага 3 — сверить контекст вручную, как имя сущности ownS3 не используется
grep -niE 'TBD|TODO|FIXME' arch/owns3/04-storage.md      # → пусто
```

- [ ] **Шаг 9. Commit**

```bash
git add arch/owns3/04-storage.md && git commit -m "arch/owns3/04: xl-хранение single-drive — раскладка .owns3.sys, собственный xl.meta (SHA-256), volume.json, атомарный rename-коммит, multipart на диске, чистки (t35)"
```

**Выход:** глава 04 — модель хранения; t37/t38 реализуют диск и multipart без досказок.
**Проверка:** цикл шага 8 без MISSING; пороги чисток зафиксированы (24 ч multipart, 1 ч `.trash/`); имена служебного контура — только собственные.
**Связь со spec:** §3.5 (артефакт), §6.5 (критерий), решение пользователя 8.

---

## Задача 6: `arch/owns3/05-access-config.md` — учётки, конфиг, эксплуатация (фаза 6)

**Spec:** §3.6, §6.2 (п. 5–6 главы 01 связаны), §6.6; решения пользователя 4, 6.
**Вход:** задачи 1–2 закоммичены (ссылки на главы 03/04).
**Files:** Create: `arch/owns3/05-access-config.md`.

- [ ] **Шаг 1. Раздел «Root-учётка»** (spec §3.6 п. 1): env `OWNS3_ROOT_USER` / `OWNS3_ROOT_PASSWORD`; минимальные длины зафиксировать: user ≥ 3, password ≥ 8 (критерий: референс MinIO); оба обязательны, отсутствие/короткость — отказ старта (fail-fast); полный доступ (роль admin).

- [ ] **Шаг 2. Раздел «Статические access keys»** (spec §3.6 п. 2): источник — конфигурация (зафиксировать: секция `OwnS3:AccessKeys` appsettings/env `OWNS3_*` — массив записей `{accessKey, secretKey, policy}`; критерий: простота, единый env-механизм с воркерами); право `policy` — `read-only` / `read-write` / `admin`; материализация в `.owns3.sys/config/` при первом старте и при изменении конфигурации (файл ключей тома); secret в файле локального тома допускается — зафиксировать явно с комментарием о границах (том локальный, multi-host durability правил не даёт; ротация — переконфигурация env + рестарт).

- [ ] **Шаг 3. Раздел «Матрица прав»** (spec §3.6 п. 3): таблица «роль × операции»:
  - `read-only`: `GetObject`, `HeadObject`, `ListObjects` (v1), `ListObjectsV2`, `ListObjectVersions`, `HeadBucket`, `ListBuckets`, `GetBucketLocation`, `ListMultipartUploads`, `ListParts` — только загрузки, созданные этим же access key (владелец в `uploads.json`, глава 04);
  - `read-write`: всё `read-only` (видит все загрузки) + `PutObject`, `DeleteObject`, `DeleteObjects`, `CopyObject`, `GetObjectAttributes`, полный multipart-цикл `CreateMultipartUpload`/`UploadPart`/`UploadPartCopy`/`CompleteMultipartUpload`/`AbortMultipartUpload`;
  - `admin`: всё `read-write` + `CreateBucket`, `DeleteBucket`.
  Отказ в правах — `AccessDenied` (403) в формате ошибок главы 03. Пояснение к границе: справочник бакетов (`Head`/`List`/`GetBucketLocation`) доступен всем ролям — нужен целевым клиентам; мутации бакетов — только admin.

- [ ] **Шаг 4. Раздел «Конфиг и env»** (spec §3.6 п. 4): полная таблица «env → назначение → дефолт»: `OWNS3_SERVER_PORT` (порт HTTP-грани; дефолт **9000** — S3-конвенция), `OWNS3_DATA_DIR` (путь тома данных; дефолт **`/data`** — докер-конвенция), `OWNS3_ROOT_USER` (нет дефолта, обязательно), `OWNS3_ROOT_PASSWORD` (нет дефолта, обязательно), ключи `OWNS3_ACCESS_KEYS__*` (секция `OwnS3:AccessKeys`; пусто = только root); соответствующая секция `OwnS3:*` appsettings (`OwnS3:Server:Port`, `OwnS3:DataDir`, `OwnS3:Root*`, `OwnS3:AccessKeys`); префикс `OWNS3_*` — единая конвенция имён.

- [ ] **Шаг 5. Раздел «Health и метрики»** (spec §3.6 п. 5): `/healthz` — liveness: том смонтирован и доступен на запись + `volume.json` валиден (200/503); Prometheus-метрики по канону `arch/18-metrics.md` (каркас `Shared.Metrics`), минимальный состав зафиксировать: счётчик запросов по операциям и кодам (`ownS3_requests_total{operation,code}`), гистограмма длительности (`ownS3_request_duration_seconds{operation}`), занятость тома (`ownS3_disk_used_bytes`, `ownS3_disk_total_bytes`); расширенный состав — задачами эксплуатации, не каноном t35.

- [ ] **Шаг 6. Раздел «Запуск в докере»** (spec §3.6 п. 6): образ — сборка .NET на хосте (`dotnet publish`) + runtime-слой с `COPY` publish-вывода (правило E2E-образов AGENTS.md; прод-Dockerfile поставки — отдельный процесс, не канон t35); том данных — bind/volume на `OWNS3_DATA_DIR`; env-секреты через env-файл (шаблон `.env.example` по образцу deploy воркеров создаётся в t39; канон фиксирует состав: root-пара + статические ключи); локально собираемый образ в registry `192.168.0.1:5000` не кладётся; базовый runtime-образ — из существующих `images.txt` (новых внешних образов нет); детальная модель dev-стенд-сервиса — t39.

- [ ] **Шаг 7. Проверка главы**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
for t in 'OWNS3_ROOT_USER' 'OWNS3_ROOT_PASSWORD' 'OWNS3_SERVER_PORT' 'OWNS3_DATA_DIR' '9000' 'read-only' 'read-write' 'admin' 'AccessDenied' '/healthz' '.owns3.sys/config/' '192.168.0.1:5000'; do grep -q "$t" arch/owns3/05-access-config.md || echo "MISSING: $t"; done   # → пусто
grep -n '\.minio\.sys' arch/owns3/05-access-config.md   # → пусто
grep -niE 'TBD|TODO|FIXME' arch/owns3/05-access-config.md   # → пусто
```

- [ ] **Шаг 8. Commit**

```bash
git add arch/owns3/05-access-config.md && git commit -m "arch/owns3/05: root из env + статические ключи, матрица прав read-only/read-write/admin, env-таблица (порт 9000), health/метрики, докер-запуск (t35)"
```

**Выход:** глава 05 — доступ и эксплуатация; t36 (аутентификация) и t39 (стенд/health) работают без досказок.
**Проверка:** цикл шага 7 без MISSING; матрица прав покрывает все 22 операции + root.
**Связь со spec:** §3.6 (артефакт), §6.6 (критерий), решения пользователя 4, 6.

---

## Задача 7: roadmap-отражение и указатели (фаза 7)

**Spec:** §3.7, §3.8, §6.7, §6.8; решения пользователя 7, 8.
**Вход:** задачи 1–6 закоммичены (канон готов — трек ссылается на него).
**Files:** Modify: `arch/roadmap/owns3.md`, `arch/README.md`. `arch/roadmap/README.md` — без изменений (описание трека уже соответствует; проверить).

- [ ] **Шаг 1. Введение трека — цель-замещение** (spec §3.7 п. 1): во вводную часть `arch/roadmap/owns3.md` (после первого абзаца) добавить абзац: ownS3 — целевое S3-хранилище подсистемы бэкапов; после приёмки t39 вытесняет MinIO (клиенты переключаются env-endpoint'ом, без изменений кода клиентов); миграция данных MinIO→ownS3 — задача `t41-owns3-minio-migration`; трек `s3.md` (t31–t34, MinIO-кластера) живёт параллельно на переходный период. Без исторических пассажей.

- [ ] **Шаг 2. Замена имён служебного контура** (spec §3.7 п. 2, инвариант «расхождение имени контура в roadmap и каноне недопустимо»): в `arch/roadmap/owns3.md` заменить каждое вхождение `.minio.sys` → `.owns3.sys`. Фактические места на момент плана: введение «служебный контур `.minio.sys` (формат тома, multipart, tmp/.trash)», формулировка t35 «служебный контур `.minio.sys`», формулировка t37 «запись через `.minio.sys/tmp`», формулировка t38 «раскладка `.minio.sys/multipart/...`» — заменить все четыре (в t37/t38 тоже: перечисление spec §3.7 п. 2 — минимальный набор, а инвариант того же пункта требует отсутствия расхождений в файле трека; оставленные `.minio.sys` в t37/t38 стали бы расхождением roadmap-канон). Упоминания референс-файлов MinIO и механики раскладки не меняются.

- [ ] **Шаг 3. Согласование формулировки t39 с правилом образов** (spec §3.7 п. 3): в пункте `t39-owns3-e2e` трека — строка 95 на момент плана: «dev-стенд сервис в докере (образ в локальный registry, env-секреты), health/метрики;» — заменить фразу «образ в локальный registry» на «образ — по правилам E2E-образов (сборка .NET на хосте, publish-COPY; локально собираемый образ в registry не кладётся)» — дословно из spec §3.7 п. 3. Формулировку t35 («запуск в докере (образ — правила локального registry)», строка 62) не трогать: это иная фраза (с искомой не совпадает), сохранена в шапке spec как цитата задачи.

- [ ] **Шаг 4. Новые задачи трека** (spec §3.7 п. 4–5): после пункта `t40-owns3-erasure-coding` дописать:

  - **`t41-owns3-minio-migration`** ← `t39-owns3-e2e` — перевод подсистемы бэкапов с MinIO на ownS3: выбор пути (перезаливка цепочек бэкапов / `mc mirror` / новый цикл ретенции), гонка двух хранилищ на переход, критерий готовности к отказу от MinIO; состав фиксируется спекой t41.
  - **`t42-owns3-tls`** ← `t39-owns3-e2e` — TLS-опция S3-грани: per-install сертификат, режим HTTP-only для стенда, доверие CA у клиентов (SDK бэкапов, `mc`).

- [ ] **Шаг 5. Пункт t35 остаётся** (spec §3.7 п. 6) в списке до мержа (снимается мерж-гейтом — стандартное правило трека); ссылка t35 на `../22-owns3.md` остаётся валидной (входная точка канона).

- [ ] **Шаг 6. `arch/README.md`** (spec §3.8): дополнить строку `22-owns3.md` указателем на главы — в дереве «Структура репозитория» комментарий строки дополнить текстом «главы канона — owns3/» (без маркдаун-ссылки: дерево внутри код-блока, ссылки там не рендерятся, соседние комментарии — простой текст); в списке «Дальше» пункт 19 дополнить фразой про главы с рабочей маркдаун-ссылкой `[owns3/](owns3/)` (главы `01…05`). Больше ничего в README не менять.

- [ ] **Шаг 7. Проверки**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
grep -n '\.minio\.sys' arch/roadmap/owns3.md             # → пусто
grep -n '\.owns3\.sys' arch/roadmap/owns3.md             # ≥ 4 (введение, t35, t37, t38)
grep -En 't41-owns3-minio-migration|t42-owns3-tls' arch/roadmap/owns3.md   # обе задачи присутствуют (-E — альтернация для BSD grep)
grep -c 't41-owns3-minio-migration.*t39-owns3-e2e' arch/roadmap/owns3.md   # → 1 (пункт и зависимость — на одной строке; голая проверка ← t39 ловила бы и существующий t40)
grep -c 't42-owns3-tls.*t39-owns3-e2e' arch/roadmap/owns3.md               # → 1
grep -n '](owns3/)' arch/README.md                       # ≥ 1 — маркдаун-ссылка на подпапку глав (шаг 5); голый 'owns3/' недискриминативен — строка roadmap/owns3.md уже есть в README
grep -c 'cmd/' arch/roadmap/owns3.md                     # карта референса не сломана (> 0)
grep -n 'образ в локальный registry' arch/roadmap/owns3.md   # → пусто (формулировка t39 заменена — критерий spec §6 п. 7)
grep -c 'по правилам E2E-образов' arch/roadmap/owns3.md  # ≥ 1 (новая формулировка t39 на месте)
git diff --stat                                          # только arch/roadmap/owns3.md и arch/README.md
```

- [ ] **Шаг 8. Commit**

```bash
git add arch/roadmap/owns3.md arch/README.md && git commit -m "arch/roadmap: owns3 — цель-замещение MinIO во введении, служебный контур .owns3.sys во всём треке, формулировка t39 согласована с правилом E2E-образов, новые задачи t41-миграция и t42-TLS ← t39; arch/README — указатель глав канона (t35)"
```

**Выход:** трек отражает канон (цель, имена, формулировка t39 по правилам образов, задачи t41/t42); указатели расставлены.
**Проверка:** команды шага 7 зелёные; `grep -n 't35-owns3-canon' arch/roadmap/owns3.md` — пункт t35 на месте.
**Связь со spec:** §3.7 п. 1–6, §3.8, §6.7, §6.8; решения пользователя 7, 8.

---

## Задача 8: финальный гейт — сверка критериев приёмки spec §6

**Spec:** §6 (все критерии), §1.3 (границы).
**Вход:** задачи 1–7 закоммичены.
**Files:** без новых артефактов; правки только при найденных расхождениях.

- [ ] **Шаг 1. Граница изменений** (§6.9 — контролем по git status, diff не видит untracked):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
# закоммиченные артефакты задач 1–7 (только они — в diff; spec/plan коммитятся в шаге 4)
git diff --name-only main...HEAD   # ровно: arch/22-owns3.md, arch/owns3/01…05-*.md,
                                   # arch/roadmap/owns3.md, arch/README.md
# незакоммиченное/untracked: spec/plan ещё не в индексе, прочего мусора быть не должно
git status --porcelain             # ровно одна строка: ?? docs/superpowers/2026-10-10-t35-owns3-canon/
```

  Никаких `src/**`, `deploy/**`, `dev-stand/**`, других каталогов — ни в diff, ни в status. Лишние закоммиченные файлы — выяснить и откатить (`git checkout -- <путь>` / `git rm`); лишние untracked — удалить.

- [ ] **Шаг 2. Автоматические сверки**:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/docs-t35-owns3-canon
# плейсхолдеры отсутствуют во всех документах канона
grep -rniE 'TBD|TODO|FIXME' arch/22-owns3.md arch/owns3/ arch/roadmap/owns3.md   # → пусто
# имя служебного контура нигде не MinIO
grep -rn '\.minio\.sys' arch/22-owns3.md arch/owns3/ arch/roadmap/owns3.md       # → пусто
# все 22 операции — полные контракты
for op in PutObject GetObject HeadObject DeleteObject DeleteObjects CopyObject GetObjectAttributes ListObjects ListObjectsV2 ListObjectVersions CreateBucket DeleteBucket HeadBucket ListBuckets GetBucketLocation CreateMultipartUpload UploadPart UploadPartCopy CompleteMultipartUpload AbortMultipartUpload ListParts ListMultipartUploads; do grep -qx "### $op" arch/owns3/02-operations.md || echo "MISSING: $op"; done   # → пусто (-x — целая строка)
# указатель входной точки ведёт на существующие главы
for f in 01-overview 02-operations 03-protocol 04-storage 05-access-config; do test -f "arch/owns3/$f.md" || echo "MISSING: $f.md"; done   # → пусто
# все референс-файлы MinIO, упомянутые в новых документах, существуют
grep -oh 'cmd/[a-z0-9-]*\.go' arch/22-owns3.md arch/owns3/*.md | sort -u | while read f; do test -e "/Users/demakaev/ZCodeProject/minio/$f" || echo "MISSING REF: $f"; done   # → пусто
# внутренние относительные ссылки из глав разрешаются из worktree
cd arch/owns3 && for p in ../19-backups.md ../18-metrics.md ../roadmap/owns3.md ../roadmap/s3.md ../22-owns3.md; do test -e "$p" || echo "BROKEN: $p"; done   # → пусто
# ссылки на референс MinIO: форма — текстово (только ../../../minio, изолированные формы других глубин отсутствуют), цель — абсолютным путём
grep -rnE '(^|[^/.])\.\./\.\./minio' *.md    # → пусто (изолированная 2-уровневая форма отсутствует; [^/.] отсекает подстроку внутри корректной ../../../minio)
grep -rn '\.\./\.\./\.\./\.\./minio' *.md    # → пусто (4-уровневая форма отсутствует)
grep -rln '\.\./\.\./\.\./minio' *.md        # ≥ 1 файл (ссылки присутствуют корректной формой)
test -d /Users/demakaev/ZCodeProject/minio || echo "MISSING REF DIR"   # → пусто (цель референс-ссылок; ../../../minio из worktree не разрешается — test -e не применять, см. Global Constraints)
# форма референс-ссылки из входной точки (arch/22-owns3.md): файл на уровень выше глав, корректна ../../minio; трёхуровневая — дефект
grep -n '\.\./\.\./\.\./minio' ../22-owns3.md               # → пусто
grep -c '\.\./\.\./minio' ../22-owns3.md                    # ≥ 1
```

- [ ] **Шаг 3. Ручная сверка по чек-листу §6** (таблица соответствия):

  | Критерий §6 | Где гарантируется |
  |---|---|
  | 6.1 входная точка | задача 1 |
  | 6.2 глава 01 (состав проектов, durability, таблица решений) | задача 2 |
  | 6.3 глава 02 (22 контракта, общие семантики, unversioned) | задача 3 |
  | 6.4 глава 03 (SigV4 ×3, XML, ошибки, path-style/HTTP-only, лимиты) | задача 4 |
  | 6.5 глава 04 (раскладка, xl.meta, volume.json, rename-коммит, multipart, чистки) | задача 5 |
  | 6.6 глава 05 (root, ключи, матрица, env, health, докер) | задача 6 |
  | 6.7 roadmap (цель-замещение, `.owns3.sys`, формулировка t39 — по правилам E2E-образов, фразы «образ в локальный registry» нет, t41/t42 ← t39, t35 в списке) | задача 7 |
  | 6.8 указатели (arch/README, относительные ссылки `../../../minio`) | задачи 7–8 |
  | 6.9 только arch/roadmap/README + docs/superpowers | шаг 1 |
  | 6.10 стиль (русский/идентификаторы, без TBD, критерии решений) | все задачи + шаг 2 |

  При любом несоответствии — исправить в соответствующем файле и закоммитить `arch: owns3 — правки перекрёстной сверки канона (t35)`; при полном соответствии — коммит не нужен.

- [ ] **Шаг 4. Commit spec/plan задачи** (финальная фиксация артефактов dev-flow в ветке):

```bash
git add docs/superpowers/2026-10-10-t35-owns3-canon && git commit -m "docs: spec + plan задачи канона ownS3 (t35)"
git status --porcelain   # → пусто (рабочее дерево чисто; теперь и git diff --name-only main...HEAD показывает полную границу: артефакты задач 1–7 + docs/superpowers/2026-10-10-t35-owns3-canon/)
```

**Выход:** канон согласован и полон против spec §6; ветка готова к ревью dev-flow (мерж в `main` — только после одобрения).
**Проверка:** все команды шагов 1–2 без вывода ошибок; чек-лист шага 3 закрыт построчно.
**Связь со spec:** §6 целиком (критерии приёмки), §4 (фазы/коммиты).

---

## Самопроверка плана (выполнена при написании)

- **Покрытие spec**: §3.1→задача 1; §3.2→задача 2; §3.3→задача 3; §3.4→задача 4; §3.5→задача 5; §3.6→задача 6; §3.7+§3.8→задача 7; §6→задача 8. Решения пользователя 1–8 распределены: 1→з.1, 2→з.3, 3→з.2/4, 4→з.6, 5→з.2, 6→з.2/4/6, 7→з.1/2/7, 8→з.1/5/7. НЕ-цели §1.3 — Global Constraints (только arch/**, arch/19 не трогаем — на него только ссылки).
- **Плейсхолдеры**: конкретные значения зафиксированы везде, где spec оставлял «фиксируется» и решение однозначно вытекает из spec/референса (uploadId=UUID v4, checksum=SHA-256, пороги 24 ч/1 ч, порт 9000, `/data`, us-east-1, длины root 3/8, лимиты 1024/3–63/1000/10000, размеры объект/часть 5 ГБ); спорные места (множественный Range, trailers, CreateBucket-семантика) снабжены указанием «зафиксировать по сверке с референсом и стандартом» с ожидаемым исходом — исполнитель фиксирует в главе конкретику, не оставляя TBD.
- **Единообразие имён**: `.owns3.sys`, `volume.json`, `xl.meta`, `part.N`, `uploads.json`, `dataDir`, проекты `OwnS3.{Protocol,Storage,App}`, env `OWNS3_*` — одинаково во всех задачах; формат заголовков операций `### <Operation>` согласован между задачей 3 (написание) и проверками задач 3/8.
