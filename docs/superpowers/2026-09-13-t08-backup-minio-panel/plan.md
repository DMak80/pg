# t08-backup-minio-panel — план реализации (грань «Хранилище бэкапов» в AdminPanel)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Дать оператору в AdminPanel read-only грань «Хранилище бэкапов»: live-инвентарь MinIO/S3 (дерево `<C>/<X>`, размеры, счётчики), health MinIO (live/cluster/drives), место/квота (etcd-ключ `storage` + live-факт), сверка S3↔etcd «глазами человека» (полные без ключа, ключи без объектов, сироты с джойном на реестр воркера) и on-demand постраничный list объектов — без единой пишущей операции в S3.

**Architecture:** Arch-first (фаза 0 — правки `arch/adminpanel/{01,02,03,04}` + `arch/19` §3.5/§5 + `docs/adminpanel/05`). Затем: MinIO-клиент и фоновый инвентарь-тик — новая подпапка `AdminPanel.Probes/S3/` (образец `Kafka/`), результат — в `MinioInventoryStore` (singleton) и далее в `EtcdSnapshot.MinioStorage` refresher'ом (образец `WorkerHealth`); агрегация `MinioInventory.Build` и сверка `MinioReconciler` — чистые функции Core; алерт `backup-s3-unreachable` — новое правило AlertEngine; три GET-эндпоинта `/api/backups/*` — CQRS-queries над снапшотом (+ `IMinioS3` только для on-demand objects); React-страница `/backups-storage` + подстраница деталей шарда; dev-стенд — env панели в compose + чек `45-backups-storage.sh`.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), AWSSDK.S3 3.7.511.8 (уже в CPM, новая ссылка в `AdminPanel.Probes`/`AdminPanel.IntegrationTests`), xUnit v3 + FluentAssertions, Testcontainers (MinIO + etcd), React+Vite+TS (Mantine, TanStack Query).

**Spec:** `docs/superpowers/2026-09-13-t08-backup-minio-panel/spec.md` — план аргументируется от спека; исполнители читают оба документа. Канон подсистемы бэкапов — `arch/19-backups.md`; канон панели — `arch/adminpanel/`.

## Global Constraints

- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — сборка без ворнингов или не сборка. Версии пакетов — только CPM (`Directory.Packages.props`); AWSSDK.S3 уже там, новых пакетов НЕ вводим.
- **Панель в S3 read-only на уровне кода**: `IMinioS3` содержит только `ListBuckets`/`ListPage`/health — ни put, ни delete, ни admin-API. Панель не пишет в S3 и в etcd из этой грани никогда.
- Креды per-install только env (`AdminPanel:Backups:S3:*`): в git их нет; ни один DTO/ответ API не несёт AccessKey/SecretKey (AC8).
- Код `src/PgWorker.*`/`src/KafkaWorker.*` НЕ меняется (E2E-мерж-гейт воркеров не применяется). Существующие парсер `/pgworker/backups/`, правила t02–t07, `SnapshotRefresher` — только расширение.
- Тесты: docker-порты динамические (`WithPortBinding(..., assignRandomHostPort: true)` + `GetMappedPublicPort`) — никаких литералов-портов; готовность MinIO ≤ 45 c; таймауты фикстур ≤ 100 c; каждая фикстура чистит себя в `DisposeAsync` при любом исходе + ассерт «контейнер исчез»; после КАЖДОЙ тестовой серии — `docker rm -f $(docker ps -aq)` (кроме стендовых `as-*`/`adminpanel`) + `docker network prune -f`.
- Русский язык комментариев/доков, английский — идентификаторы. Тесты — AAA-комментарии (Arrange/Act/Assert).
- Панель ВСЕГДА в докере; доступ к MinIO — из docker-сети (`minio:9000`), никаких хост-процессов.
- UI — Mantine/TanStack Query, тёмная тема, общий polling-переключатель; форм ввода НЕ добавляем (счётчик «форм ввода» arch/03 §3 не растёт).
- Каждая задача плана завершается коммитом; работаем в worktree `feat-t08-backup-minio-panel` (ветка уже существует), пуш/мерж — по флоу dev-flow.
- Мерж-гейт (финальный гейт задачи, spec §5 фаза 5): `dotnet test` панели зелёный + `npm run typecheck`/`build` зелёные + чек 45 на полном стенде + зачистка серий по AGENTS + roadmap-тег `t08-backup-minio-panel` снят из `arch/roadmap/backup.md` тем же мерж-коммитом.

### Решения (автономно) — приняты исполнителем плана, зафиксированы здесь

1. **`ClusterBackupsInfo` += опциональный `ShardsFulls`, `WalStreamInfo` += опциональный `LastUploadedSegment`** (оба — новые параметры со значением по умолчанию, обратная совместимость): Reconciler (spec §4.4) и детали шарда (§4.6) требуют per-full etcd-факты (state/verify/size) и etcd `last_uploaded_segment`, которых в панельной модели не было. Паттерн расширений t03–t06 — все новые поля этой модели добавлялись опциональными параметрами (прецеденты `Shards`, `DeletingFulls`, `ShardVerifyFailures`, `ShardsRestores`). Это «расширение, не переделка» — инвариант spec §2 соблюдён. **Осознанное отступление от буквы spec §4.4** («существующие [records сверки] не меняются»): без этих двух опциональных полей нести per-full etcd-факты и `last_uploaded_segment` в модели НЕГДЕ — ни одна существующая позиция/семантика записей не меняется (только новые параметры с default, все существующие конструктивные вызовы компилируются без правок); запрет §4.4 на изменение существующих записей сверки сохраняется в полном объёме.
2. **Модели инвентаря/health/сверки — в `AdminPanel.Core`** (`MinioStorageInfo.cs`, сверки — в `BackupInfo.cs`), интерфейс `IMinioInventoryStore` — в Core (реализация — в Probes): снапшот несёт `Health`/дерево, а `Core` не ссылается на `Probes` (направление зависимостей arch/01 §1). Прецедент — `IWorkerHealthStore` в Core, реализация вне; DI-регистрация реализации из `Probes.ModuleExtensions` (прецедент `IKafkaSecretsStore`).
3. **`IMinioS3`/`MinioS3Client`/`MinioOptions`/`MinioInventoryLoop`/`MinioInventoryStore` — в `AdminPanel.Probes/S3/`** (новая подпапка по образцу `Kafka/`), `MinioS3Client` — осознанный дубль `PgWorker.Backups.BackupS3` (панель не ссылается на `PgWorker.*`; унификация — `t08-unify-adminpanel-duplicates` в roadmap).
4. **Маркер configured до первого тика**: при заданном `Endpoint` `MinioInventoryLoop.RunOnceAsync` первым делом вносит в стор `MinioStorageInfo(Configured: true, UpdatedAtUnix: 0, …)` (если стор пуст) — API различает «не настроено» (`MinioStorage == null` → `configured:false`) и «настроено, инвентарь ещё собирается». При пустом `Endpoint` тик не стартует, стор пуст (AC1).
5. **Сверка вычисляется в query** (`MinioReconciler.Reconcile` — чистая функция над снапшотом), в снапшоте не хранится: дёшево, детерминировано, соответствует «API не ходит во внешние системы на запрос» (инвентарь уже в снапшоте).
6. **`EtcdSeed` — отдельный список `Backups` + метод `SeedBackupsAsync`**, НЕ расширение `Demo`: `Demo` сидаUsed существующими интеграционными тестами с ассертами по снапшоту — расширение сломало бы соседние серии.
7. **Интеграционные тесты MinIO-грани — своя `BackupsWebFactory`** (не `AuthWebFactory`): реальный `SnapshotStore` + реальный `SnapshotRefresher`/`MinioInventoryLoop`, но hosted-сервисы сняты — тесты двигают тики вручную (`RefreshOnceAsync`/`RunOnceAsync`, прецедент спека §3.10).
8. **WAL-сверка — факты без вердикта**: etcd `last_uploaded_segment`/`last_uploaded_unix` vs S3-последний объект (имя + lastModified); сортировка имён сегментов Ordinal (24-hex имена при равном TLI лексикографически упорядочены; cross-TLI — показываем как факт, вердикт не ставим — spec §4.4).
9. **Env-байндинг простых листьев** `AdminPanel__Backups__S3__*` — тот же класс, что рабочие `AdminPanel__Probes__Kafka__*` (урок WORKERTLS касался POCO с path-полями); митигация — интеграционный тест читает `IOptions<MinioOptions>` через хост (Task 9), при обнаружении проблемы — env-перекладка в `Program.cs` по образцу `WorkerTlsHandler.ApplyEnvOverrides` тем же коммитом.
10. **Пункт навигации** — метка «Хранилище бэкапов», маршрут `/backups-storage`.
11. **Гвард `prefix` эндпоинта objects — двухступенчатый** (разрешает противоречие spec: §4.6 допускает любой `<C>/…` по regex-паттерну, но AC7 требует `prefix=freepath/` → 400 при том, что `freepath` матчит `^[a-z][a-z0-9_]{0,62}$`): ступень 1 — форма: пусто или первый сегмент матчит regex (быстрый 400 на мусор); ступень 2 — принадлежность: первый сегмент — имя кластера из снапшота ИЛИ кластер S3-дерева инвентаря (сироты просматриваемы), иначе 400 «префикс вне грани». Так `demo/s1/…` и `ghost-shard/s9/…` валидны, `freepath/` и `Bad/x` — 400 (AC7 проходит, «защита от произвольного листинга» — полная). Foreign-корни (вне `<C>/<X>`) в objects не пускаем — их факты уже в сводке.

---

### Task 0: Фаза 0 — arch-правки (arch-first, до кода)

**Files:**
- Modify: `arch/adminpanel/01-architecture.md` (§1, §2, §6, §8)
- Modify: `arch/adminpanel/02-etcd-contract.md` (§2.3.1, новая §2.5, §3)
- Modify: `arch/adminpanel/03-panels.md` (§1, §2, §3, §4)
- Modify: `arch/adminpanel/04-local-stand.md` (env панели, чек 45)
- Modify: `arch/19-backups.md` (§3.5, §5)
- Modify: `docs/adminpanel/05-dev-stand.md` (чек 45 в порядке чеков)

**Interfaces:**
- Consumes: spec §5 «Фаза 0».
- Produces: канон, по которому пишутся все дальнейшие задачи: три эндпоинта `/api/backups/*` (03 §1), DTO (03 §2), панель UI (03 §3), kind `backup-s3-unreachable` (03 §4), §2.5 «MinIO/S3 бэкапов — live-чтение панели», `EtcdSnapshot.MinioStorage` (02 §3), §3.5 «отображение статуса restore — грань t08; запуск — команда оператора», §5 «панель читает bucket read-only».

**Вход (предусловие):** worktree чист, spec прочитан.

- [ ] **Step 1: `arch/adminpanel/01-architecture.md`**

  §1: в ASCII-схему после блока `Probes (Patroni REST :8008, Npgsql)` добавить блок:

  ```
                 ┌────────────────────────┴───────────────┐
                 │ MinIO live-тик (AdminPanel.Probes/S3,  │  тик 60 c
                 │  AdminPanel:Backups)                   │──► MinIO/S3 bucket
                 │  ListBuckets + health + list-v2 bucket │    бэкапов установки
                 │  → MinioInventoryStore → снапшот       │    (read-only!)
                 └────────────────────────────────────────┘
  ```

  и в «Правила потоков» (после пункта про refresher) добавить пункт: «**MinIO live-тик панели** (t08): отдельный `BackgroundService` `MinioInventoryLoop` пишет инвентарь bucket'а бэкапов в `MinioInventoryStore`; `SnapshotRefresher` вносит готовое состояние в `EtcdSnapshot.MinioStorage` (по образцу `WorkerHealth`) — KV-тик не блокируется; API не ходит в MinIO на запрос, КРОМЕ on-demand `GET /api/backups/objects` (постраничный list-v2 — большой объём, тиком не тянется)». §2: строку таблицы `AdminPanel.Probes` дополнить: «…, S3/MinIO-инвентарь бэкапов (t08: read-only обёртка AWSSDK.S3 + health-эндпоинты)». §6: после секции `Probes` добавить строку «`Backups` — `S3 {Endpoint, Region?, Bucket, AccessKey, SecretKey, PathStyle=true}, IntervalSec=60, TimeoutSec=5`; пустой `Endpoint` — грань выключена (тик не стартует, API `configured=false`); креды только env (arch/19 §7)». §8: добавить строку «MinIO недоступен → инвентарь устаревает (`inventoryUpdatedUnix` не растёт), алерт warning `backup-s3-unreachable` после 2 неудачных тиков; etcd-часть грани (статусы/квота/сироты) продолжает работать».

- [ ] **Step 2: `arch/adminpanel/02-etcd-contract.md` §2.3.1 + §2.5 + §3**

  §2.3.1, строка таблицы `/pgworker/backups/storage`: фразу «в UI не отображается (t08 — грань MinIO)» заменить на «отображается в грани "Хранилище бэкапов" (t08: карточка «Место» — used/quota/вердикт воркера + штамп live-инвентаря)». В строке `/pgworker/backups/<C>/…` фразу «UI-грань бэкапов — t08» заменить на «UI-грань бэкапов — t08 (статусы полных/WAL/restore джойнятся с S3-инвентарём в сверке грани)».

  После §2.4 добавить новую секцию:

  ```
  ### 2.5. MinIO/S3 бэкапов — live-чтение панели (t08)

  Грань «Хранилище бэкапов» читает объектное хранилище бэкапов НАПРЯМУЮ (не через
  etcd): per-install креды — env `AdminPanel:Backups:S3:*` (arch/19 §7, в git их
  нет; DTO несёт только endpoint/bucket). Панель в S3 ТОЛЬКО ЧИТАЕТ — контракт
  уровня кода: интерфейс клиента содержит лишь ListBuckets/ListObjectsV2 и
  публичные health-эндпоинты `/minio/health/live`|`/minio/health/cluster` (200/503;
  drives-поля тела при наличии — толерантный парс); пишущие/удаляющие операции —
  только воркер (arch/19 §4/§5). Инвентарь-тик 60 c (`IntervalSec`): ListBuckets +
  health + ОДИН полный list-v2 bucket → агрегаты дерева `<C>/<X>` в стор →
  снапшот `EtcdSnapshot.MinioStorage` (§3); построчные объекты — on-demand
  `GET /api/backups/objects` (пагинация, единственный прямой выход на запрос).
  Сверка S3↔etcd — отображение (полные без ключа / ключи без объектов / сироты с
  джойном на реестр /pgworker/backups/orphans): находит и удаляет сироты
  супервизор воркера (arch/19 §4) — панель ничего не удаляет.
  ```

  §3: в C#-блок снапшота после строки `IReadOnlyList<WorkerHealth> WorkerHealth,` добавить строку `MinioStorageInfo? MinioStorage,   // §2.5: live-инвентарь MinIO (t08), null — грань не настроена` и после блока `WorkerHealth` — краткие сигнатуры: `sealed record MinioStorageInfo(bool Configured, string Endpoint, string Bucket, MinioHealth? Health, IReadOnlyList<string> Buckets, long UsedBytes, long ObjectCount, IReadOnlyList<MinioClusterNode> Clusters, IReadOnlyList<string> ForeignPrefixes, long UpdatedAtUnix, int ConsecutiveFailures, string? LastError)`.

- [ ] **Step 3: `arch/adminpanel/03-panels.md` §1 + §2 + §3 + §4**

  §1: в таблицу эндпоинтов после `GET /api/ha/{scope}` добавить три строки:

  ```
  | `GET /api/backups/storage` | грань «Хранилище бэкапов»: `configured` (false → только это поле+причина), endpoint/bucket (без ключей), health, место (etcd-ключ `storage` + live-инвентарь), дерево кластеров/шардов, сироты (реестр воркера + сверка панели), `inventoryUpdatedUnix`/`inventoryError` (t08, 02 §2.5) |
  | `GET /api/backups/storage/{cluster}/{shard}` | детали шарда: полные (id/size/objectCount/lastModified + etcd state/verify/size + статус сверки), WAL (S3-факт + etcd-статус), активный restore (бейдж), 404 — нет ни в etcd, ни в S3-дереве |
  | `GET /api/backups/objects?prefix=&maxKeys=200&continuationToken=` | on-demand list-v2 (единственный выход в MinIO на запрос): `prefix` пуст или `<кластерный паттерн>/…` **и** первый сегмент — кластер снапшота или S3-дерева инвентаря (иначе 400 — защита от произвольного листинга), `maxKeys` 1..1000 (иначе 400), `nextContinuationToken` |
  ```

  §2: в DTO-раздел добавить абзац: «Грань «Хранилище бэкапов» (t08): `BackupStorageDto`, `BackupShardStorageDto`, `BackupObjectsPageDto` (+ вложенные `BackupFullDto`, `BackupWalDto`, `BackupOrphanDto`, `MinioHealthDto`); camelCase; S3-ключи (AccessKey/SecretKey) НЕ отдаются никогда».

  §3: в таблицу панелей после строки **Alerts** добавить строку:

  ```
  | **Хранилище бэкапов** | `/backups-storage` (t08, read-only): карточки Health (api/live/cluster + drives), Место (used/quota, прогресс-бар, state-бейдж OK/WARN/CRIT — вердикт воркера + штамп live), Buckets; таблица «Кластеры → шарды» (размер, полные шт., WAL-сегменты шт., пометки сверки); клик → детали шарда `/backups-storage/:cluster/:shard`: таблица полных (id/размер/дата/etcd state+verify/сверка), блок WAL (etcd-статус + S3-факт), бейдж активного restore, «Объекты» с on-demand пагинацией («Загрузить ещё»); блок «Осиротевшие префиксы» (реестр воркера OBSERVED/DELETING+TTL или «панель видит, в реестре нет»); `configured=false` — заглушка «не настроено (AdminPanel:Backups:S3)». Без форм ввода |
  ```

  §4: добавить строку в КОНЕЦ таблицы каталога алертов (в таблице 03 §4 сейчас нет ни одного `backup-*` kind — бэкап-алерты t02–t07 каталогизированы в arch/19 §4; новая строка — первая backup-строка 03 §4, самосогласованность канона сохранена): `| backup-s3-unreachable | warning | грань настроена && инвентарь-тик MinIO падал ≥ 2 подряд (configured && consecutiveFailures ≥ 2); снимается первым успешным тиком | MinIO live-тик (02 §2.5) |`.

- [ ] **Step 4: `arch/19-backups.md` §3.5 + §5; `arch/adminpanel/04-local-stand.md`; `docs/adminpanel/05-dev-stand.md`**

  `arch/19` §3.5, первый абзац: фразу «Воркер валидирует и сам пишет заявку-статус в etcd (§4); UI — t08.» заменить на «Воркер валидирует и сам пишет заявку-статус в etcd (§4); отображение статуса restore (state/phase/error бейджем в деталях шарда) — грань t08 панели ([adminpanel/03](adminpanel/03-panels.md) §3); ЗАПУСК restore — команда оператора в HTTP API воркера по runbook (runbook backup-restore.md), из UI панель restore не запускает.»

  `arch/19` §5, в конец первого маркированного списка добавить пункт: «**Панель читает bucket read-only** (t08): live-инвентарь/health/сверка в грани «Хранилище бэкапов» ([adminpanel/02](adminpanel/02-etcd-contract.md) §2.5); пишущие/удаляющие операции S3 — ТОЛЬКО воркер (ретенция §4, супервизор t07).»

  `arch/adminpanel/04` §1: в перечисление сервисов/env панели дополнить: сервис `adminpanel` получает env `AdminPanel__Backups__S3__Endpoint=http://minio:9000`, `...__Bucket=pgworker-backups`, `...__AccessKey/SecretKey=minioadmin` (стендовые дефолты as-minio; прод — per-install env) — панель и as-minio в одной compose-сети, `minio` резолвится напрямую (docker-only панели); §3: добавить чек `45-backups-storage.sh` (налив mc-контейнером тестовых объектов → `curl /api/backups/storage`: configured/health/дерево).

  `docs/adminpanel/05-dev-stand.md`: в «Состав»-список `checks/` добавить `45-backups-storage.sh` между 40-м и 90-м (в порядке чеков).

- [ ] **Step 5: Проверка и коммит**

  Run: `grep -c "backups/storage\|backups/objects" arch/adminpanel/03-panels.md` → ≥ 3; `grep -c "MinioStorage\|§2.5" arch/adminpanel/02-etcd-contract.md` → ≥ 3; `grep -c "read-only" arch/19-backups.md` → ≥ 1.
  Markdown сборкой не проверяется — визуальная сверка таблиц с соседними строками.

```bash
git add arch/adminpanel/01-architecture.md arch/adminpanel/02-etcd-contract.md arch/adminpanel/03-panels.md arch/adminpanel/04-local-stand.md arch/19-backups.md docs/adminpanel/05-dev-stand.md
git commit -m "docs(arch): t08-backup-minio-panel — канон грани «Хранилище бэкапов» (adminpanel/01-04, arch/19 §3.5/§5, docs/05): MinIO live-тик, §2.5 read-only-контракт, эндпоинты/DTO/UI, kind backup-s3-unreachable, restore-статус в грани"
```

**Выход:** канон обновлён; весь дальнейший код аргументируется от него. **Проверка:** grep-ассерты шага 5. **Связь со spec:** §5 фаза 0; §3.9 (правка arch/19 §3.5).

---

### Task 1: Core-модели MinIO-грани + `EtcdSnapshot.MinioStorage`

**Files:**
- Create: `src/AdminPanel.Core/MinioStorageInfo.cs`
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs:5-21` (новый optional-параметр `MinioStorage`)
- Modify: `src/AdminPanel.Core/BackupInfo.cs` (`BackupFullInfo`, `ClusterBackupsInfo.ShardsFulls`, `WalStreamInfo.LastUploadedSegment`, типы сверки)

**Interfaces:**
- Consumes: arch/02 §2.5+§3 (Task 0).
- Produces (для всех последующих задач): типы ниже — имена точные.

**Вход (предусловие):** Task 0 смержён в ветку.

- [ ] **Step 1: `MinioStorageInfo.cs` — модели инвентаря и health**

```csharp
namespace AdminPanel.Core;

// Live-инвентарь MinIO-грани «Хранилище бэкапов» (t08, arch/adminpanel/02 §2.5).
// Заполняет MinioInventoryLoop (Probes/S3) через IMinioInventoryStore; снапшот
// вносит refresher. Все метрики — факты, без вердиктов (вердикты — воркер).

/// <summary>Drives-поля тела /minio/health/cluster (свежие MinIO); null-поля —
/// в теле нет; сам Drives null — тела/полей не было вовсе (только статус-код).</summary>
public sealed record MinioDrives(
    long? HealthyDrives, long? OfflineDrives, long? HealingDrives, long? TotalDrives);

/// <summary>Health MinIO: ApiOk — ListBuckets прошёл (API жив + креды валидны),
/// LiveOk — /minio/health/live 200, ClusterOk — /minio/health/cluster 200
/// (null — эндпоинт не отвечает/не поддерживается).</summary>
public sealed record MinioHealth(
    bool ApiOk, string? ApiError, bool LiveOk, bool? ClusterOk, MinioDrives? Drives);

/// <summary>Один полный бэкап в S3-дереве: префикс full/&lt;id&gt;/.</summary>
public sealed record MinioFullNode(
    string Id, long SizeBytes, long ObjectCount, long LastModifiedUnix);

/// <summary>WAL-часть шарда в S3: сегменты wal/&lt;segment&gt; и истории wal/&lt;TLI&gt;.history.</summary>
public sealed record MinioWalNode(
    long SegmentCount, long HistoryCount, long SizeBytes, long LastModifiedUnix);

public sealed record MinioShardNode(
    string Cluster, string Shard, long SizeBytes,
    IReadOnlyList<MinioFullNode> Fulls, MinioWalNode? Wal);

public sealed record MinioClusterNode(
    string Cluster, long SizeBytes, IReadOnlyList<MinioShardNode> Shards);

/// <summary>Агрегат одного прогона list-v2 всего bucket (MinioInventory.Build).</summary>
public sealed record MinioInventory(
    IReadOnlyList<MinioClusterNode> Clusters, long UsedBytes, long ObjectCount,
    IReadOnlyList<string> Buckets, IReadOnlyList<string> ForeignPrefixes);

/// <summary>Состояние грани в снапшоте: null — AdminPanel:Backups:S3 не настроен
/// (configured=false); Configured=true + UpdatedAtUnix=0 — первый тик ещё шёл.</summary>
public sealed record MinioStorageInfo(
    bool Configured, string Endpoint, string Bucket,
    MinioHealth? Health, IReadOnlyList<string> Buckets,
    long UsedBytes, long ObjectCount,
    IReadOnlyList<MinioClusterNode> Clusters, IReadOnlyList<string> ForeignPrefixes,
    long UpdatedAtUnix, int ConsecutiveFailures, string? LastError);

/// <summary>Стор инвентаря: loop пишет, SnapshotRefresher вносит готовым в
/// снапшот (паттерн IWorkerHealthStore — KV-тик не блокируется).</summary>
public interface IMinioInventoryStore
{
    MinioStorageInfo? Current { get; }

    void Replace(MinioStorageInfo state);
}
```

- [ ] **Step 2: `EtcdSnapshot` + `BackupInfo.cs` — расширения**

  `EtcdSnapshot.cs`: после `BackupOrphansInfo? BackupOrphans = null` добавить параметр `MinioStorageInfo? MinioStorage = null` (t08, nullable — грань не настроена).

  `BackupInfo.cs` — новые record'ы и optional-параметры (существующие не трогаем, Решение 1):

```csharp
/// <summary>Один etcd-ключ полного /pgworker/backups/&lt;C&gt;/&lt;X&gt;/full/&lt;id&gt; (t08):
/// полный факт state/verify/size для сверки с S3 и деталей шарда.</summary>
public sealed record BackupFullInfo(
    string Id, string State, string? Error,
    long StartedUnix, long? FinishedUnix, long? SizeBytes,
    string? VerifyState, long? VerifyCheckedUnix, string? VerifyError);
```

  В `ClusterBackupsInfo` последним параметром: `IReadOnlyDictionary<string, IReadOnlyList<BackupFullInfo>>? ShardsFulls = null` (t08: все etcd-полные per-shard — вход MinioReconciler; null = парсер t08 их не собрал). В `WalStreamInfo` последним параметром: `string? LastUploadedSegment = null` (t08: etcd-факт wal-сверки).

  Типы сверки (новые records, в конец `BackupInfo.cs`):

```csharp
/// <summary>Статус одного полного в сверке S3↔etcd (t08, arch/02 §2.5):
/// Ok — объекты+ключ; S3Only — объекты без ключа; EtcdOnly — ключ без объектов
/// (COMPLETED/FAILED); InProgress — активный PLANNED/RUNNING/UPLOADING без
/// объектов (ожидаемо); Deleting — DELETING-ключ + остатки объектов.</summary>
public enum BackupFullReconcileStatus { Ok, S3Only, EtcdOnly, InProgress, Deleting }

public sealed record BackupFullReconcile(
    string Cluster, string Shard, string Id,
    BackupFullReconcileStatus Status,
    long? SizeBytes, long? ObjectCount, long? LastModifiedUnix, // S3-факт (null — объектов нет)
    string? EtcdState, string? VerifyState, long? EtcdSizeBytes); // etcd-факт (null — ключа нет)

/// <summary>Сирота по сверке ПАНЕЛИ (префикс <C>/<X> без владельца в /clusters/):
/// рядом — факт реестра воркера (InWorkerRegistry/RegistryState/FirstSeenUnix).</summary>
public sealed record BackupOrphanPrefix(
    string Prefix, string Kind, long SizeBytes,
    bool InWorkerRegistry, string? RegistryState, long? FirstSeenUnix);

/// <summary>WAL-сверка — только факты, без вердикта (spec §4.4).</summary>
public sealed record BackupWalReconcile(
    string Cluster, string Shard,
    string? EtcdLastSegment, long? EtcdLastUnix,
    string? S3LastObject, long? S3LastModifiedUnix);

/// <summary>Итог MinioReconciler.Reconcile — чистая функция над снапшотом.</summary>
public sealed record BackupReconcileInfo(
    IReadOnlyList<BackupFullReconcile> Fulls,
    IReadOnlyList<BackupOrphanPrefix> OrphanPrefixes,
    IReadOnlyList<BackupWalReconcile> Wal);
```

- [ ] **Step 3: Сборка**

  Run: `dotnet build src/AdminPanel.Core/AdminPanel.Core.csproj -c Release`
  Expected: Build succeeded, 0 warnings (TreatWarningsAsErrors). Существующие конструктивные вызовы `EtcdSnapshot`/`ClusterBackupsInfo`/`WalStreamInfo` компилируются без правок (все новые параметры optional).

- [ ] **Step 4: Коммит**

```bash
git add src/AdminPanel.Core/MinioStorageInfo.cs src/AdminPanel.Core/EtcdSnapshot.cs src/AdminPanel.Core/BackupInfo.cs
git commit -m "feat(adminpanel): t08 — модели MinIO-грани в Core (MinioStorageInfo, IMinioInventoryStore, EtcdSnapshot.MinioStorage) + etcd-факты per-full (BackupFullInfo, WalStreamInfo.LastUploadedSegment) и типы сверки"
```

**Выход:** типы Core, на которые ссылаются Tasks 2–8. **Проверка:** сборка Core. **Связь со spec:** §4.3 (модели), §4.4 (типы сверки), AC4/AC9 (etcd-факты per-full).

---

### Task 2: `MinioInventory.Build` — агрегация list-v2 в дерево

**Files:**
- Create: `src/AdminPanel.Core/MinioInventory.cs`
- Test: `src/tests/AdminPanel.UnitTests/MinioInventoryTests.cs`

**Interfaces:**
- Consumes: `MinioInventory`, `MinioClusterNode/ShardNode/FullNode/WalNode` (Task 1).
- Produces: `public static MinioInventory Build(IReadOnlyList<MinioObject> objects)`; входной record `public sealed record MinioObject(string Key, long SizeBytes, long LastModifiedUnix)` (в `MinioInventory.cs`).

**Вход (предусловие):** Task 1; layout S3 — arch/19 §5 (`<C>/<X>/full/<id>/…`, `<C>/<X>/wal/<segment>`, `<C>/<X>/wal/<TLI>.history`).

- [ ] **Step 1: Тесты агрегации (AAA)** — `MinioInventoryTests.cs`

  Кейсы (каждый — отдельный `[Fact]`, FluentAssertions):
  1. `Build_	fulls_wal_history` — объекты `demo/s1/full/20260913a/base.tar` (100), `demo/s1/full/20260913a/pg_wal/seg` (16), `demo/s1/wal/000000010000000000000001` (16), `demo/s1/wal/000000010000000000000002` (16), `demo/s1/wal/00000002.history` (4) → кластер demo, шард s1: `Fulls[0] = {Id=20260913a, SizeBytes=116, ObjectCount=2}`, `Wal = {SegmentCount=2, HistoryCount=1, SizeBytes=36}`, `UsedBytes=152`, `ObjectCount=5`, `ForeignPrefixes` пуст.
  2. `Build_foreign_prefix` — объект `loose/file.bin` (10) (меньше 2 сегментов пути) → кластеров нет, `ForeignPrefixes = ["loose"]`, `UsedBytes=10`.
  3. `Build_two_clusters_shards` — `a/s1/...`, `a/s2/...`, `b/s1/...` → 2 кластера, размеры/шарды раздельно, сортировка кластеров/шардов Ordinal.
  4. `Build_last_modified_max` — `LastModifiedUnix` full/wal = max по объектам.
  5. `Build_shard_extras_counted` — объект `demo/s1/other.txt` внутри шарда вне full/wal → в `ShardNode.SizeBytes` входит, в Fulls/Wal — нет (без вердикта).

- [ ] **Step 2: Запуск тестов — красные**

  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release --filter FullyQualifiedName~MinioInventoryTests`
  Expected: FAIL — тип `MinioInventory.Build` не существует (CS0103/ошибка компиляции).

- [ ] **Step 3: Реализация `MinioInventory.cs`**

  Чистая функция: сплит ключа по `/`; сегментов пути < 2 → foreign-корень (первый сегмент, dedup, сортировка Ordinal); иначе группа `<cluster>/<shard>`; внутри: 3-й сегмент `full` → full id = 4-й сегмент (агрегат size/count/maxLastModified по 5+ сегментам), `wal` → имя 4-го сегмента: суффикс `.history` → HistoryCount, иначе SegmentCount (`.partial` — тоже сегмент-факт); прочее внутри шарда — только в SizeBytes шарда. `UsedBytes`/`ObjectCount` — суммы по всем объектам. Итог сортирован: кластеры/шарды/fulls — Ordinal.

- [ ] **Step 4: Тесты зелёные + коммит**

  Run: тот же фильтр → Expected: PASS.

```bash
git add src/AdminPanel.Core/MinioInventory.cs src/tests/AdminPanel.UnitTests/MinioInventoryTests.cs
git commit -m "feat(adminpanel): t08 — MinioInventory.Build: агрегация list-v2 в дерево <C>/<X> (fulls/wal/history/foreign), юниты"
```

**Выход:** агрегатная функция инвентарь-тика (Task 6). **Проверка:** юнит-фильтр зелёный. **Связь со spec:** §4.3, AC2 (юниты Build).

---

### Task 3: `BackupsParser` — сбор etcd-фактов per-full + `last_uploaded_segment`

**Files:**
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs` (ветка full-ключа ~строка 244, ветка wal-ключа ~строка 155, `BackupsParseResult`)
- Test: `src/tests/AdminPanel.UnitTests/BackupsParserTests.cs` (дополнение)

**Interfaces:**
- Consumes: `BackupFullInfo`, `ClusterBackupsInfo.ShardsFulls`, `WalStreamInfo.LastUploadedSegment` (Task 1).
- Produces: `BackupsParseResult.Clusters[i].ShardsFulls` — все etcd-полные per-shard; `WalStreamInfo.LastUploadedSegment`.

**Вход (предусловие):** Task 1; формат ключей — arch/19 §4 (таблица full/wal).

- [ ] **Step 1: Тесты (AAA)** — дополнить `BackupsParserTests.cs`

  1. `Parse_full_key_collects_full_info` — KV `/pgworker/backups/demo/s1/full/20260913a` = `{"state":"COMPLETED","node":"s1a","role":"replica","started_unix":100,"finished_unix":200,"wal_start_segment":"...","size_bytes":123,"verify":{"state":"OK","checked_unix":201}}` → `ShardsFulls["s1"]` содержит `BackupFullInfo { Id="20260913a", State="COMPLETED", SizeBytes=123, VerifyState="OK", VerifyCheckedUnix=201 }`.
  2. `Parse_full_key_minimal_failed` — FAILED-ключ без size/verify → `BackupFullInfo` с `SizeBytes=null, VerifyState=null, Error` из `error`.
  3. `Parse_wal_last_uploaded_segment` — wal-ключ с `"last_uploaded_segment":"00000001000000000000000A"` → `WalStreamInfo.LastUploadedSegment` = это значение; wal-ключ без поля → null (толерантно).
  4. `Parse_multiple_fulls_same_shard` — 2 full-ключа одного шарда → оба в списке (сортировка по Id Ordinal).
  5. `Parse_full_key_unknown_state_tolerated` — full-ключ с `"state":"WEIRD"` → `BackupFullInfo.State == "WEIRD"` (state читается как есть — терпимо к незнакомым значениям; НОВОГО пути отказа не вводим, см. Step 3).

- [ ] **Step 2: Красные**

  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release --filter FullyQualifiedName~BackupsParserTests`
  Expected: FAIL (новые ассерты).

- [ ] **Step 3: Реализация**

  В ветке full-ключа (существующий разбор state/verify остаётся) добавить сбор `BackupFullInfo` в новый словарь `fulls[cluster][shard]`: `state` читается КАК ЕСТЬ (raw-строка в `BackupFullInfo.State`) — текущая ветка full-ключа state валидации НЕ делает (KeyParseError только на битый JSON/неизвестное `verify.state`/DELETING без `started_unix`, `BackupsParser.cs:244-331`), и новой валидации/пути отказа НЕ вводим — незнакомые state'ы терпимы (воркер — писатель, панель — толерантный читатель, паттерн панели); в конце — в `ClusterBackupsInfo` как `ShardsFulls` (пустой словарь → `null`). В ветке wal-ключа — читать `last_uploaded_segment` (строка, опционально) в новый параметр `WalStreamInfo`.

- [ ] **Step 4: Зелёные + коммит**

  Run: тот же фильтр → PASS; затем `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release` → ALL PASS (существующие парсер-тесты не сломаны).

```bash
git add src/AdminPanel.Etcd/Parsing/BackupsParser.cs src/tests/AdminPanel.UnitTests/BackupsParserTests.cs
git commit -m "feat(adminpanel): t08 — BackupsParser собирает etcd-факты per-full (ShardsFulls) и last_uploaded_segment для сверки"
```

**Выход:** etcd-вход Reconciler. **Проверка:** юниты парсера. **Связь со spec:** §4.4 (входы сверки), AC4/AC9.

---

### Task 4: `MinioReconciler` — чистая сверка S3↔etcd

**Files:**
- Create: `src/AdminPanel.Core/MinioReconciler.cs`
- Test: `src/tests/AdminPanel.UnitTests/MinioReconcilerTests.cs`

**Interfaces:**
- Consumes: `MinioStorageInfo`, `MinioInventory`-дерево, `ClusterBackupsInfo.ShardsFulls/Shards`, `ClusterInfo`, `BackupOrphansInfo`, `WalStreamInfo.LastUploadedSegment` (Tasks 1–3).
- Produces: `public static BackupReconcileInfo Reconcile(MinioStorageInfo minio, IReadOnlyList<ClusterBackupsInfo> backups, IReadOnlyList<ClusterInfo> clusters, BackupOrphansInfo? orphanRegistry)`.

**Вход (предусловие):** Tasks 1–3.

- [ ] **Step 1: Тесты (AAA)** — все ветви AC4/AC5:

  1. `Reconcile_ok_full` — S3 `full/20260913a` + etcd COMPLETED → `Ok` с обоими фактами.
  2. `Reconcile_s3_only` — S3 `full/gone/` без etcd-ключа → `S3Only` (`EtcdState=null`).
  3. `Reconcile_etcd_only_completed` — etcd COMPLETED без объектов → `EtcdOnly` (`SizeBytes=null`).
  4. `Reconcile_active_no_objects_is_in_progress` — etcd PLANNED (и отдельно RUNNING) без объектов → `InProgress`, НЕ `EtcdOnly`.
  5. `Reconcile_deleting_with_leftovers` — etcd DELETING + остатки объектов → `Deleting`.
  6. `Reconcile_orphan_prefix_with_registry` — S3-префикс `ghost/s9/` без кластера `ghost` в `clusters` + запись `{"prefix":"ghost/s9","state":"OBSERVED","first_seen_unix":1}` в реестре → `OrphanPrefix { Prefix="ghost/s9", InWorkerRegistry=true, RegistryState="OBSERVED", FirstSeenUnix=1 }`.
  7. `Reconcile_orphan_prefix_not_in_registry` — то же без записи в реестре (и `orphanRegistry=null`) → `InWorkerRegistry=false`.
  8. `Reconcile_registry_entry_without_s3` — запись реестра без S3-факта (воркер уже удалил) → в OrphanPrefixes НЕ попадает (реестр без факта — не сирота панели; расхождение увидит оператор по TTL-строке реестра) — зафиксировать ассертом.
  9. `Reconcile_wal_facts` — etcd `LastUploadedSegment="..01"`, S3 последний сегмент `..02` → `BackupWalReconcile` несёт оба факта (вердикта нет — только поля).
  10. `Reconcile_shard_of_known_cluster_not_orphan` — `demo/s1` при живом кластере demo → не сирота.

- [ ] **Step 2: Красные**

  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release --filter FullyQualifiedName~MinioReconcilerTests`
  Expected: FAIL (тип не существует).

- [ ] **Step 3: Реализация**

  Чистая функция, без IO. Множество владельцев: `clusters` (имена) × `cluster.Shards` (имена). Обход S3-дерева: каждая пара `(C, X)` без владельца → `OrphanPrefix(Kind="shard")`; кластер в дереве целиком без владельца → дополнительно `Kind="cluster"` на префикс `<C>` (по образцу реестра воркера: kind shard|cluster — шардовый префикс достаточен, кластеровый добавляем только если в дереве нет ни одного шарда-владельца кластера). Обход объединения id полных: S3-полные ∪ etcd `ShardsFulls`; статус по правилам §4.4 (активные PLANNED/RUNNING/UPLOADING без объектов → InProgress). WAL: последний объект wal-префикса — max по имени Ordinal (segments; истории в «последний» не идут) vs etcd-факты. Джойн сирот с реестром — по `Prefix` (Ordinal). Сортировки Ordinal.

- [ ] **Step 4: Зелёные + полный юнит-прогон + коммит**

  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release` → ALL PASS.

```bash
git add src/AdminPanel.Core/MinioReconciler.cs src/tests/AdminPanel.UnitTests/MinioReconcilerTests.cs
git commit -m "feat(adminpanel): t08 — MinioReconciler: сверка S3-дерева с etcd (Ok/S3Only/EtcdOnly/InProgress/Deleting, сироты с джойном на реестр, wal-факты), юниты всех ветвей"
```

**Выход:** сверка для API (Task 8). **Проверка:** юнит-фильтр. **Связь со spec:** §4.4, AC4, AC5 (юнит-часть).

---

### Task 5: Probes/S3 — `MinioOptions` + `IMinioS3`/`MinioS3Client`

**Files:**
- Create: `src/AdminPanel.Probes/S3/MinioOptions.cs`
- Create: `src/AdminPanel.Probes/S3/MinioS3Client.cs`
- Modify: `src/AdminPanel.Probes/AdminPanel.Probes.csproj` (+= `<PackageReference Include="AWSSDK.S3"/>`)
- Modify: `src/AdminPanel.Probes/ModuleExtensions.cs` (регистрация)
- Modify: `src/AdminPanel.Api/Program.cs` (fail-fast валидация)
- Modify: `src/AdminPanel.Api/appsettings.json` (секция `Backups`)
- Test: `src/tests/AdminPanel.UnitTests/MinioOptionsTests.cs`

**Interfaces:**
- Consumes: `MinioHealth`/`MinioDrives` (Core, Task 1).
- Produces:

```csharp
namespace AdminPanel.Probes.S3;

[Config("AdminPanel:Backups")]
public class MinioOptions
{
    public MinioS3Options S3 { get; set; } = new();
    public int IntervalSec { get; set; } = 60;   // период инвентарь-тика
    public int TimeoutSec { get; set; } = 5;     // HTTP-таймаут list/health

    public sealed class MinioS3Options
    {
        public string Endpoint { get; set; } = "";
        public string? Region { get; set; }
        public string Bucket { get; set; } = "";
        public string AccessKey { get; set; } = "";
        public string SecretKey { get; set; } = "";
        public bool PathStyle { get; set; } = true;
    }

    public bool IsConfigured => !string.IsNullOrWhiteSpace(S3.Endpoint);

    /// <summary>Fail-fast старта (образец PgWorker:Backups): Endpoint задан при
    /// пустых Bucket/AccessKey/SecretKey или IntervalSec/TimeoutSec <= 0 — бросок.</summary>
    public void EnsureValid() { /* InvalidOperationException с именем поля */ }
}

/// <summary>Страница list-v2 (on-demand пагинация — ContinuationToken наружу).</summary>
public sealed record MinioObjectInfo(string Key, long SizeBytes, long LastModifiedUnix);
public sealed record S3Page(IReadOnlyList<MinioObjectInfo> Items, string? NextContinuationToken);

public interface IMinioS3
{
    Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct);
    Task<Result<S3Page>> ListPageAsync(string? prefix, string? continuationToken, int maxKeys, CancellationToken ct);
    Task<MinioHealth> GetHealthAsync(CancellationToken ct);
}
```

  Реализация `MinioS3Client` — read-only обёртка AWSSDK.S3 по образцу `PgWorker.Backups.BackupS3` (Решение 3): `AmazonS3Config { ServiceURL = Endpoint, ForcePathStyle = PathStyle, AuthenticationRegion = Region }` + `BasicAWSCredentials`; `ListPageAsync` — один `ListObjectsV2Async` (BucketName, Prefix, ContinuationToken, MaxKeys; IsTruncated → NextContinuationToken); health — именованный HttpClient `"minio-health"` (без SigV4): `GET {endpoint}/minio/health/live` и `/minio/health/cluster`, 200 → true, 503/иное → false/`null` (cluster не отвечает), тело cluster парсим толерантно (`healthyDrives`/`offlineDrives`/`healingDrives`/`totalDrives` — long?, отсутствующие поля → null, битый JSON → Drives=null); `ApiOk` = `ListBucketsAsync.IsSuccess`. **Пишущих методов в классе НЕТ.**

**Вход (предусловие):** Tasks 0–1; AWSSDK.S3 в CPM.

- [ ] **Step 1: Тест `MinioOptionsTests` (AAA)** — `EnsureValid` бросает: (a) Endpoint + пустой Bucket; (b) Endpoint + пустые AccessKey/SecretKey; (c) `IntervalSec=0`; (d) `TimeoutSec=-1`; (e) НЕ бросает при пустом Endpoint (грань выключена — валидации нет); (f) НЕ бросает на полном валидном наборе. Красный → реализация → зелёный.

- [ ] **Step 2: csproj + реализация клиента + DI**

  `AdminPanel.Probes.csproj`: добавить `<PackageReference Include="AWSSDK.S3"/>`. `MinioS3Client.cs`: интерфейс+реализация выше; `IAsyncDisposable` (dispose AmazonS3Client); health-HttpClient — через `IHttpClientFactory` (имя `MinioS3Client.HealthHttpClientName = "minio-health"`), таймаут из `IOptions<MinioOptions>.TimeoutSec`. `ModuleExtensions.AddProbes`: `services.AddHttpClient(MinioS3Client.HealthHttpClientName)` c таймаутом; `services.AddSingleton<MinioS3Client>(); services.AddSingleton<IMinioS3>(sp => sp.GetRequiredService<MinioS3Client>());` (клиент создаётся всегда, ходит только при конфигурации).

- [ ] **Step 3: Program.cs fail-fast + appsettings.json**

  `Program.cs`, после auth-проверки: 

```csharp
// t08: fail-fast конфига MinIO-грани (пустой Endpoint — грань выключена, не ошибка).
var minio = app.Services.GetRequiredService<IOptions<AdminPanel.Probes.S3.MinioOptions>>().Value;
if (minio.IsConfigured)
    minio.EnsureValid();
```

  `appsettings.json`, внутрь `"AdminPanel"`: `"Backups": { "S3": { "Endpoint": "", "Region": null, "Bucket": "", "AccessKey": "", "SecretKey": "", "PathStyle": true }, "IntervalSec": 60, "TimeoutSec": 5 }`.

- [ ] **Step 4: Сборка+тесты + коммит**

  Run: `dotnet build src/AdminPanel.slnx` не нужен — `dotnet build src/PgWorker.slnx -c Release` слишком широко; собирать `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj -c Release` (тянет Probes/Core) → 0 warnings; `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release --filter FullyQualifiedName~MinioOptionsTests` → PASS.

```bash
git add src/AdminPanel.Probes/S3/ src/AdminPanel.Probes/AdminPanel.Probes.csproj src/AdminPanel.Probes/ModuleExtensions.cs src/AdminPanel.Api/Program.cs src/AdminPanel.Api/appsettings.json src/tests/AdminPanel.UnitTests/MinioOptionsTests.cs
git commit -m "feat(adminpanel): t08 — MinioOptions (fail-fast) и read-only MinioS3Client (AWSSDK.S3 list-v2 + health HttpClient), DI, appsettings"
```

**Выход:** клиент для loop (Task 6) и objects-эндпоинта (Task 8). **Проверка:** сборка Api + юниты опций. **Связь со spec:** §4.1 (конфиг), §4.2 (клиент), §2 (read-only контракт уровня кода).

---

### Task 6: `MinioInventoryLoop` + `MinioInventoryStore` + внесение в снапшот

**Files:**
- Create: `src/AdminPanel.Probes/S3/MinioInventoryStore.cs` (реализация `Core.IMinioInventoryStore`)
- Create: `src/AdminPanel.Probes/S3/MinioInventoryLoop.cs`
- Modify: `src/AdminPanel.Probes/ModuleExtensions.cs` (регистрация: store-синглтон как `Core.IMinioInventoryStore`, loop как hosted)
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs` (внесение `MinioStorage`)
- Test: `src/tests/AdminPanel.UnitTests/MinioInventoryLoopTests.cs`
- Test: `src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs` (дополнение)

**Interfaces:**
- Consumes: `IMinioS3` (Task 5), `MinioInventory.Build` (Task 2), `IMinioInventoryStore`/`MinioStorageInfo` (Task 1).
- Produces: `MinioInventoryLoop.RunOnceAsync(CancellationToken): Task` — публичное ядро тика (для интеграционных тестов); `MinioInventoryStore` — потокобезопасный singleton-стор (`Interlocked`/`lock` замена ссылки).

**Вход (предусловие):** Tasks 2, 5.

- [ ] **Step 1: Тесты loop (AAA, стаб `IMinioS3`)** — `MinioInventoryLoopTests`:

  1. `RunOnce_success_populates_store` — стаб отдаёт buckets + страницу объектов → стор: `Configured=true`, `Health.ApiOk=true`, дерево кластера, `ConsecutiveFailures=0`, `UpdatedAtUnix>0`.
  2. `RunOnce_failure_keeps_previous_and_counts` — первый успешный тик, затем стаб бросает → прежний инвентарь живёт (`UsedBytes` прежний, `UpdatedAtUtc` прежний), `ConsecutiveFailures=1`, `LastError` заполнен; ещё сбой → `ConsecutiveFailures=2`.
  3. `RunOnce_success_resets_failures` — после сбоев успех → `ConsecutiveFailures=0`.
  4. `RunOnce_not_configured_noop` — `MinioOptions` с пустым Endpoint → `RunOnceAsync` не зовёт клиент (стаб с счётчиком вызовов = 0), стор остаётся null.
  5. `RunOnce_writes_configured_marker_before_first_inventory` — свежий стор + конфигурация задана + клиент бросает → в сторе `Configured=true, UpdatedAtUnix=0, ConsecutiveFailures=1` (маркер Решения 4).

- [ ] **Step 2: Реализация loop/store**

  `MinioInventoryLoop : BackgroundService` (конструктор: `IMinioS3`, `IMinioInventoryStore`, `IOptions<MinioOptions>`, `TimeProvider`, `ILogger`): `ExecuteAsync` — если `!IsConfigured` → log Info «грань выключена» и return; иначе первый `RunOnceAsync` сразу + `PeriodicTimer(IntervalSec)` (образец `ProbeOrchestrator`/`SnapshotRefresher`). `RunOnceAsync`: гвард конфигурации; маркер в пустой стор; (1) `GetHealthAsync`, (2) `ListBucketsAsync`, (3) полный list-v2 постранично через `ListPageAsync(prefix: null, token, 1000)` до исчерпания, (4) `MinioInventory.Build(objects)`; успех → `Replace(MinioStorageInfo{... ConsecutiveFailures=0, LastError=null})`; любой сбой шага → `Replace(прошлое with ConsecutiveFailures+1, LastError, прежние данные)` (аналог FailTick refresher'а).

- [ ] **Step 3: `SnapshotRefresher` вносит `MinioStorage`**

  В конструктор добавить параметр `AdminPanel.Core.IMinioInventoryStore minioStore`. В `RefreshOnceAsync` после `ProbeEnricher.Apply(...) with { WorkerHealth = ... }` расширить: `with { MinioStorage = minioStore.Current }`; в `FailTick` — `MinioStorage = previous?.MinioStorage` (инвентарь переживает отказ etcd, как WorkerHealth). В `ModuleExtensions.AddProbes` зарегистрировать `services.AddSingleton<Core.IMinioInventoryStore>(sp => sp.GetRequiredService<S3.MinioInventoryStore>()); services.AddSingleton<S3.MinioInventoryLoop>(); services.AddHostedService(sp => sp.GetRequiredService<S3.MinioInventoryLoop>());` (прецедент `KafkaProbeLoop`).

- [ ] **Step 4: Тесты + сборка решения панели + коммит**

  `SnapshotRefresherTests`: дополнить существующие конструктивы refresher'а заглушкой сторa (запасной `MinioStorageInfo`) и добавить `[Fact]`: успешный `RefreshOnceAsync` переносит `store.Current` в `snapshot.MinioStorage`; FailTick сохраняет прежний.
  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release` → ALL PASS; `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj -c Release` → 0 warnings.

```bash
git add src/AdminPanel.Probes/S3/MinioInventoryStore.cs src/AdminPanel.Probes/S3/MinioInventoryLoop.cs src/AdminPanel.Probes/ModuleExtensions.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/tests/AdminPanel.UnitTests/MinioInventoryLoopTests.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs
git commit -m "feat(adminpanel): t08 — инвентарь-тик MinIO (loop+store, ConsecutiveFailures/устаревающий инвентарь) и внесение EtcdSnapshot.MinioStorage refresher'ом"
```

**Выход:** снапшот несёт live-инвентарь (AC2-механика). **Проверка:** юниты loop+refresher. **Связь со spec:** §4.3, §3.1 (гибрид), AC1 (тик не поднимается без Endpoint).

---

### Task 7: `BackupS3UnreachableRule` — алерт

**Files:**
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupS3UnreachableRule.cs`
- Test: `src/tests/AdminPanel.UnitTests/BackupS3UnreachableRuleTests.cs`

**Interfaces:**
- Consumes: `EtcdSnapshot.MinioStorage` (Task 1); образец — `EtcdUnreachableRule` (порог-константа) и `BackupOrphanRule` (Hint/Remedy).
- Produces: kind `backup-s3-unreachable`, warning; регистрация — автоскан `[InjectAsSingleton(typeof(IAlertRule))]` (новых настроек нет — spec §3.7).

**Вход (предусловие):** Task 1.

- [ ] **Step 1: Тесты (AAA, по образцу `BackupOrphanRuleTests`)**

  1. `Evaluate_no_minio_storage_no_alert` — `MinioStorage=null` → пусто (грань выключена).
  2. `Evaluate_configured_under_threshold_no_alert` — `Configured=true, ConsecutiveFailures=1` → пусто.
  3. `Evaluate_two_failures_warning` — `ConsecutiveFailures=2` → один warning, `Kind="backup-s3-unreachable"`, target = `endpoint/bucket`, поле `consecutiveFailures`, Hint упоминает bucket/воркера, `Remedy=AlertRemedy.OperatorRunbook`.
  4. `Evaluate_success_resets` — `ConsecutiveFailures=0` → пусто (снимается первым успешным тиком).

- [ ] **Step 2: Реализация**

```csharp
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupS3UnreachableRule : IAlertRule
{
    public const string KindName = "backup-s3-unreachable";
    public const int Threshold = 2; // каталог 03 §4: >= 2 тиков (константа, не настройка)

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var minio = snapshot.MinioStorage;
        if (minio is not { Configured: true } || minio.ConsecutiveFailures < Threshold)
            yield break;
        yield return new Alert(
            $"{KindName}:{minio.Endpoint}/{minio.Bucket}",
            AlertSeverity.Warning, KindName, $"{minio.Endpoint}/{minio.Bucket}",
            $"панель не может листить bucket бэкапов {minio.Bucket}: {minio.ConsecutiveFailures} подряд неудачных инвентарь-тиков ({minio.LastError})",
            new Dictionary<string, string> { ["consecutiveFailures"] = minio.ConsecutiveFailures.ToString(), ["bucket"] = minio.Bucket },
            SinceUnix: null, // проставляет AlertEngine
            Hint: "панель не может листить bucket бэкапов: endpoint/креды/сеть — бэкапы воркера под угрозой, смотрите логи PgWorker и MinIO",
            Remedy: AlertRemedy.OperatorRunbook,
            RemedyText: "проверьте доступность MinIO (endpoint AdminPanel:Backups:S3:Endpoint, креды, docker-сеть) и логи PgWorker; после восстановления алерт гаснет первым успешным тиком");
    }
}
```

- [ ] **Step 3: Зелёные + коммит**

  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release --filter FullyQualifiedName~BackupS3UnreachableRule` → PASS; полный юнит-прогон → ALL PASS.

```bash
git add src/AdminPanel.Core/Alerting/Rules/BackupS3UnreachableRule.cs src/tests/AdminPanel.UnitTests/BackupS3UnreachableRuleTests.cs
git commit -m "feat(adminpanel): t08 — правило backup-s3-unreachable (warning, configured && consecutiveFailures>=2), юниты"
```

**Выход:** алерт AC3. **Проверка:** юнит-фильтр. **Связь со spec:** §4.5, §3.7.

---

### Task 8: REST API — queries/DTO/эндпоинты `/api/backups/*`

**Files:**
- Create: `src/AdminPanel.Api/Inspection/BackupStorageQuery.cs` (query'и + DTO + handlers + мапперы, по образцу `KafkaQuery.cs`)
- Modify: `src/AdminPanel.Api/Inspection/InspectionModule.cs` (`MapBackupsInspectionApi`)
- Modify: `src/AdminPanel.Api/Program.cs` (`app.MapBackupsInspectionApi()`)
- Test: `src/tests/AdminPanel.UnitTests/BackupStorageQueryTests.cs` (гварды + мапперы)

**Interfaces:**
- Consumes: снапшот (`ISnapshotReader`), `MinioReconciler` (Task 4), `IMinioS3` (Task 5 — только on-demand), `MinioOptions` (для 503-семантики objects при незаданном bucket).
- Produces: контракты arch/03 §1 (Task 0):

```csharp
public sealed record BackupStorageQuery : IQuery<BackupStorageDto>;
public sealed record BackupShardStorageQuery(string Cluster, string Shard) : IQuery<BackupShardStorageDto>;
public sealed record BackupObjectsQuery(string? Prefix, int? MaxKeys, string? ContinuationToken)
    : IQuery<BackupObjectsPageDto>;

// DTO (camelCase JSON; ключи S3 НЕ отдаются — AC8):
public sealed record MinioHealthDto(bool ApiOk, string? ApiError, bool LiveOk, bool? ClusterOk,
    long? HealthyDrives, long? OfflineDrives, long? HealingDrives, long? TotalDrives);
public sealed record BackupStorageDto(
    bool Configured, string? NotConfiguredReason, string? Endpoint, string? Bucket,
    MinioHealthDto? Health,
    BackupStorageEtcdDto? Etcd,            // ключ /pgworker/backups/storage: UsedBytes/QuotaBytes/UsedPercent/State/UpdatedUnix
    long? LiveUsedBytes,                    // инвентарь
    IReadOnlyList<BackupClusterStorageDto> Clusters,
    IReadOnlyList<BackupOrphanDto> Orphans, // реестр воркера + сверка панели, слитые
    long InventoryUpdatedUnix, string? InventoryError);
public sealed record BackupClusterStorageDto(string Cluster, long SizeBytes,
    IReadOnlyList<BackupShardSummaryDto> Shards);
public sealed record BackupShardSummaryDto(string Cluster, string Shard, long SizeBytes,
    int FullsCount, long WalSegmentCount, bool HasS3Only, bool Orphan); // пометки сверки для таблицы
public sealed record BackupOrphanDto(string Prefix, string Kind, long SizeBytes,
    bool InWorkerRegistry, string? RegistryState, long? FirstSeenUnix, long? TtlLeftSec);
public sealed record BackupShardStorageDto(
    string Cluster, string Shard,
    IReadOnlyList<BackupFullDto> Fulls,      // id/sizeBytes/objectCount/lastModifiedUnix + etcdState/verifyState/etcdSizeBytes/reconcile
    BackupWalDto? Wal,                       // etcd state/lastSegment/lastUnix + s3 segmentCount/historyCount/sizeBytes/lastModifiedUnix/lastObject
    BackupRestoreBadgeDto? ActiveRestore,    // state/phase/error (без кнопок — arch/19 §3.5)
    string? ReconcileNote);                  // сводная пометка (напр. «объекты без ключа: 2»)
public sealed record BackupFullDto(string Id, long? SizeBytes, long? ObjectCount, long? LastModifiedUnix,
    string? EtcdState, string? VerifyState, long? EtcdSizeBytes, string Reconcile); // Ok|S3Only|EtcdOnly|InProgress|Deleting
public sealed record BackupWalDto(string? EtcdState, string? EtcdLastSegment, long? EtcdLastUnix,
    long S3SegmentCount, long S3HistoryCount, long S3SizeBytes, long S3LastModifiedUnix, string? S3LastObject);
public sealed record BackupObjectsPageDto(
    IReadOnlyList<BackupObjectDto> Items, string? NextContinuationToken);
public sealed record BackupObjectDto(string Key, long SizeBytes, long LastModifiedUnix);
```

  Гварды `BackupObjectsQuery`: ступень 1 (форма) — `prefix` пуст/null ИЛИ первый сегмент (до первого `/`) матчит `^[a-z][a-z0-9_]{0,62}$` — иначе 400; ступень 2 (принадлежность, в handler'е по снапшоту — Решение 11) — первый сегмент ∈ {имена кластеров снапшота} ∪ {кластеры S3-дерева `MinioStorage`} — иначе 400 «префикс вне грани (не кластер и не объект инвентаря)»; `maxKeys` default 200, диапазон 1..1000 — иначе 400. Отказы гварда — исключение `InvalidBackupPrefixException`/`InvalidBackupMaxKeysException` → эндпоинт мапит в 400 ProblemDetails. Семантики: `snapshot == null` → 503 `SnapshotNotReadyException`; `MinioStorage == null` → сводка `{ Configured=false, NotConfiguredReason="AdminPanel:Backups:S3:Endpoint не задан" }` (200); детали шарда: нет ни в etcd-Backups, ни в S3-дереве → 404 `BackupShardNotFound`; objects: грань не настроена → 503 ProblemDetails; транспортный сбой `IMinioS3` → 502/503 ProblemDetails с ошибкой клиента.

**Вход (предусловие):** Tasks 1–7.

- [ ] **Step 1: Юнит-тесты (AAA)** — `BackupStorageQueryTests`:

  1. Гвард prefix (ступень 1, статический `BackupObjectsQuery.ValidatePrefixForm`): валидны `""`, `null`, `"demo/s1/full/"`; невалидны (400) `"Bad/x"` (заглавная), `"1demo/x"` (цифра первой), `"-demo/x"`, `"DEMO/x"`. Ступень 2 (`ValidatePrefixKnown`, фикстура снапшота с кластером `demo` + S3-деревом `demo`/`ghost-shard`): валидны `"demo/…"`, `"ghost-shard/s9/…"` (сирота из дерева); невалиден `"freepath/"` (матчит форму, но не существует ни в кластерах, ни в дереве — AC7).
  2. Гвард maxKeys: `null → 200`, `1/1000` ок, `0/1001/-5` → ошибка.
  3. Маппер `configured=false`: снапшот без MinioStorage → DTO только `Configured=false` + причина, кластеры пусты.
  4. Маппер сводки: фикстура снапшота (MinioStorage с деревом + BackupStorage(WARN) + orphans-реестр + reconcile с одним сиротом) → `Orphans` слиты: запись реестра `ghost/s9 OBSERVED` + `TtlLeftSec>0`; `Etcd.State="WARN"`; `LiveUsedBytes` = UsedBytes инвентаря.
  5. Маппер деталей: etcd full (COMPLETED+verify OK) + S3-факт → `BackupFullDto{ Reconcile="Ok" }`; активный restore (PLANNED) → `ActiveRestore{ State="PLANNED" }`.

- [ ] **Step 2: Реализация `BackupStorageQuery.cs`**

  Handlers `[InjectAsScoped]` по образцу `KafkaClusterDetailsQueryHandler`: `BackupStorageQueryHandler(ISnapshotReader)` — маппинг снапшота + `MinioReconciler.Reconcile` (сироты: реестр ∪ панельная сверка по Prefix; TTL: `FirstSeenUnix + AlertsOptions.Backups.OrphanTtlSec − now` при `OrphanTtlSec>0`, иначе null); `BackupShardStorageQueryHandler(ISnapshotReader)` — джойн `ShardsFulls`/`Shards(wal)`/`ShardsRestores` с S3-узлом, 404-семантика; `BackupObjectsQueryHandler(IMinioS3, IOptions<MinioOptions>, ISnapshotReader)` — гварды уже в эндпоинте; зовёт `ListPageAsync`, DTO без кредов.

- [ ] **Step 3: `InspectionModule.MapBackupsInspectionApi` + Program.cs**

  Три `MapGet` по образцу существующих: `/api/backups/storage` (ResultToHttp), `/api/backups/storage/{cluster}/{shard}` (404 через `BackupShardNotFound`), `/api/backups/objects` — ступень 1 (regex-форма prefix, диапазон maxKeys) inline до query (образец валидации `state` в `/api/clusters/{cluster}`); ступень 2 (принадлежность prefix кластерам/дереву) — в handler'е через `InvalidBackupPrefixException` → эндпоинт мапит в 400. `Program.cs`: строка `app.MapBackupsInspectionApi();` после `MapInspectionApi()`.

- [ ] **Step 4: Тесты+сборка + коммит**

  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release` → ALL PASS; `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj -c Release` → 0 warnings.

```bash
git add src/AdminPanel.Api/Inspection/BackupStorageQuery.cs src/AdminPanel.Api/Inspection/InspectionModule.cs src/AdminPanel.Api/Program.cs src/tests/AdminPanel.UnitTests/BackupStorageQueryTests.cs
git commit -m "feat(adminpanel): t08 — REST API грани: GET /api/backups/storage|storage/{c}/{s}|objects (queries+DTO, 400-гварды prefix/maxKeys, 404/503-семантика), юниты гвардов и мапперов"
```

**Выход:** контракт для frontend (Task 12) и интеграций (Tasks 9–11). **Проверка:** юниты + сборка. **Связь со spec:** §4.6, AC6 (DTO), AC7 (гварды), AC8 (без ключей — ассерт в Task 11).

---

### Task 9: Интеграционные фикстуры MinIO + AC1 (configured=false)

**Files:**
- Create: `src/tests/AdminPanel.IntegrationTests/MinioContainerFixture.cs`
- Modify: `src/tests/AdminPanel.IntegrationTests/EtcdSeed.cs` (список `Backups` + `SeedBackupsAsync`)
- Create: `src/tests/AdminPanel.IntegrationTests/BackupsWebFactory.cs`
- Modify: `src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj` (+= `<PackageReference Include="AWSSDK.S3"/>`)
- Test: `src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs` (AC1)

**Interfaces:**
- Consumes: образец `OwnMinio` (PgWorker.IntegrationTests) — образ `minio/minio:RELEASE.2025-09-07T16-13-09Z`, `MINIO_ROOT_USER/PASSWORD=minioadmin`, порт 9000 random; `EtcdContainerFixture`; `AuthWebFactory` (паттерн конфигурации).
- Produces: `MinioContainerFixture { string HostEndpoint; Task StopAsync(); }` (контейнер `apm-minio-{guid}`, bucket `apm-backups-{guid}` прямым AWSSDK-клиентом фикстуры, wait `/minio/health/live` ≤ 45 c, `DisposeAsync` при любом исходе + ассерт «`docker ps -a` не содержит apm-minio-{guid}»); `EtcdSeed.Backups` — ключи demo (сверх существующего `EtcdSeed.Demo` — оба сида наливает тест: `SeedAsync` даёт `/clusters/demo/…`-владельца, `SeedBackupsAsync` — бэкапы): policy, `demo/s1/full/b1` COMPLETED(size 100, verify OK), `demo/s1/full/b2` PLANNED, `demo/s1/wal` ACTIVE(last_uploaded_segment=…01), `demo/s1/restore/r1` PLANNED (backup_id=b1, source=demo/s1, target=latest, requested_unix=now−10, phase=downloading — бейдж активного restore, вход AC9), `demo/s2/full/b1` COMPLETED, `ghost-shard/s9/wal` (сирота — кластера `ghost-shard` в `/clusters/` НЕТ), `/pgworker/backups/storage` (WARN, used/quota), `/pgworker/backups/orphans` (ghost-shard/s9 OBSERVED) — времена `now`-относительные, формат ключа restore — arch/19 §4; `BackupsWebFactory : WebApplicationFactory<Program>` — `UseSetting` etcd-эндпоинт + MinIO-опции + Auth admin/adminpw + `RemoveAll<IHostedService>()` (тики — вручную), НО реальный `SnapshotStore` (никаких TestSnapshotStore) + реальный `SnapshotRefresher`/`MinioInventoryLoop` из `Services`.

**Вход (предусловие):** Tasks 5–8; перед серией: `dev-stand/images/pull-images.sh` (образы из registry 192.168.0.1:5000).

- [ ] **Step 1: `MinioContainerFixture`** — по образцу `OwnMinio` (гвид-имя `apm-minio-{guid}`: панельная серия отличается от pgw-*), `IAsyncLifetime`: старт (wait health/live 45 c) → bucket прямым `AmazonS3Client`; `StopAsync()` для AC3; `DisposeAsync`: dispose контейнера при любом исходе + `docker ps -a --filter name=apm-minio-{guid}` пуст (FluentAssertions). Хелпер `SeedObjectsAsync(IReadOnlyList<(string key, string content)>)` — `PutObjectAsync` прямым клиентом (сеянные объекты AC2).

- [ ] **Step 2: `EtcdSeed.Backups` + `SeedBackupsAsync(endpoint, ct)`** — `Demo` НЕ трогаем (Решение 6); значения канон arch/19 §4 (вкл. `restore/r1` PLANNED+phase=downloading — бейдж AC9); отдельные ключи для сироты: кластер `ghost-shard` в `/clusters/` отсутствует.

- [ ] **Step 3: `BackupsWebFactory`** — конфиг через `UseSetting` (env-подобные значения — Решение 9: заодно проверяем байндинг простых листьев): `AdminPanel:Backups:S3:Endpoint/AccessKey/SecretKey/Bucket`, `IntervalSec=60`, `TimeoutSec=5`; статический метод `WithoutMinio()` для AC1 (без настроек). Инжект реального `TimeProvider` — ок.

- [ ] **Step 4: Тест AC1 (AAA)** — `BackupsStorageApiTests.Storage_Disabled_WhenEndpointEmpty`:

```csharp
// Arrange: хост без AdminPanel:Backups (BackupsWebFactory.WithoutMinio), login-по образцу ApiTestLogin
// Act: GET /api/backups/storage с cookie
// Assert: 200; json configured == false; кластеры пусты; inventoryUpdatedUnix == 0;
//         алерт backup-s3-unreachable в /api/alerts отсутствует (MinioStorage null);
//         MinioInventoryLoop.RunOnceAsync — no-op (стаб не нужен: сервис без конфигурации).
```

  Плюс тест env-байндинга (Решение 9): `Options_Bind_FromConfiguration` — хост с настройками MinIO: `factory.Services.GetRequiredService<IOptions<MinioOptions>>().Value.S3.Endpoint == заданному` (UseSetting-эквивалент env-листьев).

- [ ] **Step 5: Прогон + зачистка серии + коммит**

  Run: `PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release --filter "FullyQualifiedName~BackupsStorageApiTests"`
  Expected: PASS. Затем страховочная зачистка серии (контейнеры apm-* ушли сами — ассерт фикстуры; сети: `docker network prune -f`; стендовые `as-*` не трогать).

```bash
git add src/tests/AdminPanel.IntegrationTests/MinioContainerFixture.cs src/tests/AdminPanel.IntegrationTests/EtcdSeed.cs src/tests/AdminPanel.IntegrationTests/BackupsWebFactory.cs src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs
git commit -m "test(adminpanel): t08 — MinioContainerFixture (apm-*, динамический порт, teardown+ассерт чистоты), EtcdSeed.Backups, BackupsWebFactory; AC1 configured=false + env-байндинг MinioOptions"
```

**Выход:** фикстуры для Tasks 10–11. **Проверка:** AC1 зелёный. **Связь со spec:** §3.10 (фикстуры), AC1, §3.4 (митигация байндинга).

---

### Task 10: Интеграционные: инвентарь/дерево/health/алерт/квота (AC2, AC3, AC6)

**Files:**
- Test: `src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs` (дополнение)

**Interfaces:**
- Consumes: фикстуры Task 9; `MinioInventoryLoop.RunOnceAsync`/`SnapshotRefresher.RefreshOnceAsync` из `factory.Services`.
- Produces: зелёные AC2/AC3/AC6 (интеграционный уровень).

**Вход (предусловие):** Task 9 зелёный; серия зачищена.

- [ ] **Step 1: `Storage_Summary_Tree_Health_And_Quota` (AC2+AC6, AAA)**

  Arrange: `EtcdContainerFixture` (класс-фикстура) + `await EtcdSeed.SeedBackupsAsync(...)`; `MinioContainerFixture` + `SeedObjectsAsync`: `demo/s1/full/b1/base.tar`(100), `demo/s1/full/b1/pg_wal/000000010000000000000001`(16), `demo/s1/wal/000000010000000000000002`(16), `demo/s1/wal/00000002.history`(4), `ghost-shard/s9/full/b9/base.tar`(50) (корневой кластер `ghost-shard` — БЕЗ префикса `demo/`: ключ = путь S3, сирота живёт на верхнем уровне bucket), `loose/file.bin`(10); `BackupsWebFactory` с обоими эндпоинтами; `await refresher.RefreshOnceAsync()` + `await loop.RunOnceAsync()`.
  Act: `GET /api/backups/storage`.
  Assert (jq-подобно, `JsonElement`): `configured==true`; `health.apiOk==true && health.liveOk==true && health.clusterOk==true`; `buckets` содержит bucket фикстуры; дерево: кластер demo/шард s1 (`fulls` через детали — здесь сводка: sizeBytes==136, walSegmentCount==1), `ghost-shard` тоже в дереве (сиротство — не скрываем); `foreignPrefixes` содержит `loose`; `liveUsedBytes==196`; `etcd.usedBytes/quotaBytes/state=="WARN"` (сид); `inventoryUpdatedUnix>0`.

- [ ] **Step 2: `Storage_StopMinio_ApiDown_Alert_AfterTwoTicks` (AC3, AAA)**

  Arrange: как Step 1 (успешный первый тик). Act: `await minio.StopAsync()`; `loop.RunOnceAsync()` ×2; `refresher.RefreshOnceAsync()`. Assert: `GET /api/backups/storage` → `health.apiOk==false && health.liveOk==false` (устаревающий инвентарь: `inventoryUpdatedUnix` прежний, `inventoryError` не пуст); `GET /api/alerts` → есть `kind=="backup-s3-unreachable"` warning. Teardown фикстуры — при любом исходе (xUnit IClassFixture + IAsyncLifetime).

- [ ] **Step 3: Прогон + зачистка + коммит**

  Run: тот же csproj, фильтр `FullyQualifiedName~BackupsStorageApiTests` → ALL PASS; после серии — `docker ps -aq | wc -l` (0 либо только стендовые `as-*`) + `docker network prune -f`.

```bash
git add src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs
git commit -m "test(adminpanel): t08 — интеграции AC2/AC3/AC6: дерево/health/foreign на сеянных объектах, stop-MinIO → apiOk=false + backup-s3-unreachable после 2 тиков, квота WARN рядом с live"
```

**Выход:** AC2/AC3/AC6 закрыты интеграционно. **Проверка:** фильтр зелёный. **Связь со spec:** AC2, AC3, AC6.

---

### Task 11: Интеграционные: детали шарда/objects/секреты/сироты (AC4-инт, AC5-инт, AC7, AC8, AC9)

**Files:**
- Test: `src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs` (дополнение) — при росте файла > ~400 строк выделить `BackupsShardApiTests.cs`/`BackupsObjectsApiTests.cs` в той же папке.

**Interfaces:**
- Consumes: фикстуры Tasks 9–10.
- Produces: зелёные AC5(инт)/AC7/AC8/AC9 (AC4 — юниты Task 4).

**Вход (предусловие):** Task 10 зелёный.

- [ ] **Step 1: `ShardDetails_JoinsEtcdAndS3` (AC9, AAA)** — сид Task 10 + доп. объект `demo/s1/full/gone/base.tar` (объекты без ключа) → `GET /api/backups/storage/demo/s1`: full `b1` → `reconcile=="Ok"`, `etcdState=="COMPLETED"`, `verifyState=="OK"`, `sizeBytes==116`, `objectCount==2`; full `gone` → `reconcile=="S3Only"`, `etcdState==null`; full `b2` (PLANNED без объектов, из сида) → `reconcile=="InProgress"`; `wal` → etcd-факт (state ACTIVE, lastSegment из сида) + S3-факт (segmentCount==1, historyCount==1); `activeRestore` (из restore-ключа сида Task 9) → `state=="PLANNED"` И `phase=="downloading"` (бейдж активной заявки — AC9-часть «активный restore state/phase»); 404: `GET /api/backups/storage/demo/nope` → 404.

- [ ] **Step 2: `Objects_Pagination_Guards_And_Auth` (AC7, AAA)**

  Arrange: сид ≥ 4 объектов под `demo/s1/`. Act/Assert: `GET /api/backups/objects?prefix=demo/s1/&maxKeys=2` → 2 items + `nextContinuationToken` != null; повтор с токеном → следующие 2, токен null (конец); `GET /api/backups/objects?prefix=freepath/` → 400 (кластера `freepath` нет ни в etcd, ни в S3-дереве — AC7); `?prefix=Bad%2Fx` → 400 (форма); `?maxKeys=1001` → 400; `?maxKeys=0` → 400; без cookie → 401; `?prefix=ghost-shard/s9/` → 200 (сирота просматриваема — Решение 11).

- [ ] **Step 3: `Responses_Contain_NoSecrets` (AC8, AAA)** — сериализовать ответы всех трёх эндпоинтов (сводка/детали/objects) в строку → `JsonSerializer.Serialize` тела; Assert: строка НЕ содержит `minioadmin`, `AccessKey`, `SecretKey`, `accessKey`, `secretKey` (case-sensitive проверки по каноничному camelCase + значение сида).

- [ ] **Step 4: `Orphans_Block_JoinsRegistryAndPanel` (AC5-инт, AAA)** — сид: ключ `orphans` с `ghost-shard/s9 OBSERVED first_seen_unix=now-100`; объекты `ghost-shard/s9/full/b9/...` (Task 10). Assert в `/api/backups/storage`: блок сирот содержит `prefix=="ghost-shard/s9"`, `inWorkerRegistry==true`, `registryState=="OBSERVED"`, `firstSeenUnix>0`, `ttlLeftSec>0`; плюс сиротка панели без записи в реестре (добавить объекты `noowner/s1/wal/x` и НЕ писать в реестр) → `inWorkerRegistry==false`.

- [ ] **Step 5: Прогон всей интеграционной серии панели + зачистка + коммит**

  Run: `PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release` → ALL PASS (вся панель, не только новые). Зачистка серии по AGENTS (`docker rm -f $(docker ps -aq)` минус стендовые `as-*`; `docker network prune -f`; `docker ps -aq | wc -l` → 0/только as-*).

```bash
git add src/tests/AdminPanel.IntegrationTests/BackupsStorageApiTests.cs
git commit -m "test(adminpanel): t08 — интеграции AC5/AC7/AC8/AC9: джойн деталей шарда, пагинация+гварды+401 objects, отсутствие секретов во всех DTO, сироты реестр+панель"
```

**Выход:** интеграционная часть грани закрыта. **Проверка:** вся серия панели зелёная. **Связь со spec:** AC5, AC7, AC8, AC9.

---

### Task 12: Frontend — страница «Хранилище бэкапов»

**Files:**
- Modify: `frontend/src/api/dto.ts` (+ типы `BackupStorageDto`/`BackupShardStorageDto`/`BackupObjectsPageDto` + вложенные — зеркало C#-DTO Task 8, camelCase)
- Modify: `frontend/src/api/queries.ts` (+ `backupsQueryKeys { storage, shard: (c,s) => [...], objects: (prefix, token) => [...] }`, `fetchBackupsStorage()`, `fetchBackupShardStorage(cluster, shard)`, `fetchBackupObjectsPage(prefix, maxKeys, continuationToken?)`)
- Create: `frontend/src/pages/BackupsStoragePage.tsx`
- Create: `frontend/src/pages/backups-storage/BackupsShardDetailsPage.tsx`
- Modify: `frontend/src/App.tsx` (маршруты `backups-storage`, `backups-storage/:cluster/:shard`)
- Modify: `frontend/src/layout/AppLayout.tsx` (nav-пункт «Хранилище бэкапов» — Решение 10)

**Interfaces:**
- Consumes: REST-контракт Task 8; паттерны страниц: `EtcdPage.tsx` (карточки+таблица), `HaScopeDetailsPage.tsx` (подстраница деталей), `PollingContext` (общий переключатель 5 c — `usePolling()`), `LoadState.tsx`.
- Produces: UI по arch/03 §3 (Task 0): карточки Health/Место/Бuckets; таблица «Кластеры → шарды» с пометками сверки и переходом; детали шарда (полные/WAL/restore-бейдж/объекты «Загрузить ещё»); блок «Осиротевшие префиксы»; заглушка `configured=false`.

**Вход (предусловие):** Task 8 (контракт стабилен); `npm install` в `frontend/` актуален (`package-lock.json` не меняется — новых пакетов нет).

- [ ] **Step 1: dto.ts + queries.ts** — типы 1:1 с C#-DTO (Task 8); fetch-функции по образцу `fetchClusterDetails` (`encodeURIComponent`); для objects — параметры в `URLSearchParams`.

- [ ] **Step 2: `BackupsStoragePage.tsx`** — `useQuery(backupsQueryKeys.storage, fetchBackupsStorage, { refetchInterval: intervalMs })` (образец `EtcdPage`):
  - `configured=false` → `<Alert>`-заглушка «Хранилище бэкапов не настроено (AdminPanel:Backups:S3)», больше контента нет.
  - Карточки (Mantine `Card`/`Group`/`Progress`/`Badge`): Health (`apiOk`/`liveOk`/`clusterOk` бейджи + drives-строка при наличии полей), Место (progress `etcd.usedBytes/quotaBytes`, бейдж OK/WARN/CRIT цветами `green/yellow/red`, подпись live `usedBytes` + `inventoryUpdatedUnix` возраст), Buckets (список).
  - Таблица `Table.ScrollContainer` «Кластеры → шарды»: cluster/shard, sizeBytes (форматирование — образец из `utils/` при наличии, иначе локальный `formatBytes`), полные шт., WAL-сегменты шт., пометки сверки (`HasS3Only` → значок «объекты без ключа», `Orphan` → «сирота»); строки — `Link`/`navigate` на `/backups-storage/:cluster/:shard`.
  - Блок «Осиротевшие префиксы»: таблица prefix/kind/размер/реестр (`registryState`+`firstSeenUnix`+`ttlLeftSec` → «удаление по TTL через …») или «панель видит, в реестре нет».
  - `inventoryError` → `Alert` «инвентарь устаревает».

- [ ] **Step 3: `BackupsShardDetailsPage.tsx`** — `useParams` + `useQuery(backupsQueryKeys.shard)`: таблица полных (id/размер/дата (`lastModifiedUnix`)/`etcdState`+`verifyState` бейдж/`reconcile` бейдж с цветовыми маппингами Ok=green, S3Only=yellow, EtcdOnly=red, InProgress=blue, Deleting=gray); блок WAL (etcd-статус + S3-факт строкой); `activeRestore` → `Badge` «Restore: {state} {phase}» + error-текст; блок «Объекты»: `useQuery(backupsQueryKeys.objects(prefix=`${cluster}/${shard}/`), fetchBackupObjectsPage, { enabled: false })` — начальная загрузка по кнопке/авто один раз, «Загрузить ещё» → `fetchNextPage`-паттерн через `useQuery` с `continuationToken` из state (стек страниц в `useState`, on-demand — polling НЕ трогает).

- [ ] **Step 4: Маршруты + навигация** — `App.tsx`: `{ path: 'backups-storage', element: <BackupsStoragePage /> }`, `{ path: 'backups-storage/:cluster/:shard', element: <BackupsShardDetailsPage /> }` (внутрь children AppLayout, рядом с `clusters`); `AppLayout.tsx`: nav-элемент `{ to: '/backups-storage', label: 'Хранилище бэкапов' }` после «Кластеры».

- [ ] **Step 5: Typecheck + build + коммит**

  Run: `npm --prefix frontend run typecheck` → без ошибок; `npm --prefix frontend run build` → vite build OK (бандл в `src/AdminPanel.Api/wwwroot` — коммитим только исходники, wwwroot по gitignore-контуру проекта).

```bash
git add frontend/src/api/dto.ts frontend/src/api/queries.ts frontend/src/pages/BackupsStoragePage.tsx frontend/src/pages/backups-storage/BackupsShardDetailsPage.tsx frontend/src/App.tsx frontend/src/layout/AppLayout.tsx
git commit -m "feat(frontend): t08 — страница «Хранилище бэкапов»: карточки health/место/buckets, дерево кластер→шард с пометками сверки, детали шарда (полные/WAL/restore/объекты on-demand), блок сирот, заглушка configured=false"
```

**Выход:** грань в UI (AC10-часть «UI показывает грань»). **Проверка:** typecheck+build зелёные. **Связь со spec:** §4.7, AC6 (прогресс-бар/бейдж — ручная проверка на стенде Task 13), AC10.

---

### Task 13: Dev-стенд — env панели в compose + чек `45-backups-storage.sh`

**Files:**
- Modify: `dev-stand/adminpanel/docker-compose.yml` (сервис `adminpanel`, блок `environment`)
- Create: `dev-stand/adminpanel/checks/45-backups-storage.sh` (chmod +x)
- Modify: `dev-stand/adminpanel/README.md` (строка о чеке 45 — если README перечисляет чеки)

**Interfaces:**
- Consumes: compose-сервис `minio` (профиль `full`, `MINIO_ROOT_USER=minioadmin`), bucket `pgworker-backups` создаёт `00-up.sh`; образ `minio/mc:RELEASE.2025-08-13T08-35-41Z` уже в `dev-stand/images/images.txt` (взят из registry — `pull-images.sh`); стиль чеков — `10-smoke-api.sh`.
- Produces: работающая грань на стенде + curl-чек AC10.

**Вход (предусловие):** Tasks 5–8 (бэкенд), Task 12 (UI); стенд поднимается `checks/00-up.sh` (full-профиль с as-minio).

- [ ] **Step 1: compose env** — в `adminpanel.environment` добавить (простые листья — Решение 9; рядом с существующими `AdminPanel__*`):

```yaml
      # t08: грань «Хранилище бэкапов» — read-only инвентарь MinIO из сети
      # стенда (minio:9000 — compose-DNS; панель всегда в докере). Креды —
      # стендовые дефолты as-minio; прод — per-install env (arch/19 §7).
      AdminPanel__Backups__S3__Endpoint: http://minio:9000
      AdminPanel__Backups__S3__Bucket: pgworker-backups
      AdminPanel__Backups__S3__AccessKey: minioadmin
      AdminPanel__Backups__S3__SecretKey: minioadmin
```

- [ ] **Step 2: чек `45-backups-storage.sh`** — по скелету `10-smoke-api.sh` (`set -euo pipefail`, `BASE`, cookie-JAR, функции `api`, ожидание панели):
  1. Гвард: as-minio жив (`docker compose ps minio` / health-live curl `http://localhost:9000/minio/health/live`) — иначе «запусти checks/00-up.sh (full-профиль)».
  2. Налив mc-контейнером — ТОЧНО по прецеденту `00-up.sh:63-70` (у образа mc ENTRYPOINT `["mc"]` — shell зовём через `--entrypoint /bin/sh`; сеть — из inspect as-minio, не хардкод имени сети; образ `minio/mc:RELEASE.2025-08-13T08-35-41Z` уже в `dev-stand/images/images.txt`, тянется из локального registry по `docs/runbook.md`):

```bash
minio_net="$(docker inspect as-minio -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker run --rm --entrypoint /bin/sh --network "$minio_net" minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c "mc alias set a http://as-minio:9000 minioadmin minioadmin >/dev/null \
      && echo data | mc pipe a/pgworker-backups/demo/s1/full/20260913a/base.tar \
      && echo wal  | mc pipe a/pgworker-backups/demo/s1/wal/000000010000000000000001"
```

  (идемпотентно: перезапись объектов ок; `as-minio:9000` — container_name, резолвится в сети compose, как в 00-up.sh).
  3. Ждать инвентарь-тик (≤ 70 c ретраем; панель reconfigure не нужна — env при подъёме) → `api /api/backups/storage | jq -e '.configured == true and .health.apiOk == true and .health.liveOk == true and .health.clusterOk == true'`.
  4. Дерево: `jq -e 'any(.clusters[]?; .cluster == "demo")'` и в нём шард s1 (`any(.clusters[] | select(.cluster=="demo") | .shards[]; .shard == "s1" and .fullsCount >= 1)`.
  5. objects: `api '/api/backups/objects?prefix=demo/s1/&maxKeys=1' | jq -e '(.items | length) == 1'`.
  6. Секреты: `api /api/backups/storage | jq -e '((tostring) | contains("SecretKey") or contains("AccessKey")) | not'`.
  7. Финал `echo "✓ backups-storage зелёный"`.

- [ ] **Step 3: Прогон чека на полном стенде + ручная проверка UI + коммит**

  Порядок: `cd dev-stand/adminpanel && checks/90-down.sh -v && checks/00-up.sh && checks/10-smoke-api.sh && checks/45-backups-storage.sh` → зелёный. Ручная проверка UI (AC10): браузер `http://localhost:5050` → nav «Хранилище бэкапов»: карточки health/место, дерево demo/s1 с налитым полным, детали шарда — таблица полных + «Объекты» пагинация. Разбор стенда: `checks/90-down.sh -v` (чистота; `as-minio-data` уходит с `-v`).

```bash
git add dev-stand/adminpanel/docker-compose.yml dev-stand/adminpanel/checks/45-backups-storage.sh dev-stand/adminpanel/README.md
git commit -m "feat(stand): t08 — env AdminPanel__Backups__S3__* панели в compose (minio:9000) и чек 45-backups-storage.sh (mc-налив, configured/health/дерево/objects/без-секретов)"
```

**Выход:** AC10 (стенд+чек+UI). **Проверка:** чек 45 зелёный на свежем стенде. **Связь со spec:** §4.8, §3.11, AC10.

---

### Task 14: Финальная верификация + roadmap-гейт

**Files:**
- Modify: `arch/roadmap/backup.md` (снять тег `t08-backup-minio-panel` — пункт и `←`-зависимости; тем же мерж-коммитом — правило AGENTS/roadmap)

**Interfaces:**
- Consumes: все задачи.
- Produces: мерж-гейт по spec §5 фаза 5 / AC11.

**Вход (предусловие):** Tasks 0–13 закоммичены.

- [ ] **Step 1: Полный прогон юнитов панели**

  Run: `dotnet test src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj -c Release`
  Expected: ALL PASS, 0 warnings.

- [ ] **Step 2: Полный прогон интеграций панели**

  Зачистка перед серией: `docker ps -aq` пуст (или только стендовые `as-*`) + `docker network prune -f`.
  Run: `PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj -c Release`
  Expected: ALL PASS. После — зачистка серии (`docker rm -f $(docker ps -aq)` минус `as-*`; `docker network prune -f`; `docker ps -aq | wc -l` → 0/только as-*).

- [ ] **Step 3: Frontend + сборка Api**

  Run: `npm --prefix frontend run typecheck && npm --prefix frontend run build` → OK; `dotnet build src/AdminPanel.Api/AdminPanel.Api.csproj -c Release` → 0 warnings.

- [ ] **Step 4: Roadmap-гейт**

  `arch/roadmap/backup.md`: удалить пункт `t08-backup-minio-panel` (текст задачи) и все `← t08-backup-minio-panel`-зависимости других пунктов (grep по файлам `arch/roadmap/`). Попадает в мерж-коммит ветки (последний коммит задачи).

- [ ] **Step 5: Self-review против AC + финальный коммит**

  Пройти по AC1–AC11 спека: каждый — либо зелёный тест (задачи выше), либо ручная проверка на стенде (AC6-UI/AC10 — Task 13). Расхождений нет → финальный коммит roadmap-правки:

```bash
git add arch/roadmap/
git commit -m "docs(roadmap): снять t08-backup-minio-panel (мерж-гейт — задача слита в ветку, реализация в arch/adminpanel/02 §2.5 + arch/19 §5)"
```

**Выход:** ветка готова к ревью/мержу. **Проверка:** Steps 1–3 зелёные, roadmap чист. **Связь со spec:** §5 фаза 5, AC11.

---

## Self-review плана (исполнен автором плана)

- **Покрытие spec:** §5 фазы 0–5 ↔ Tasks 0, 1–7 (фаза 1), 8–11 (фаза 2), 12 (фаза 3), 13 (фаза 4), 14 (фаза 5). AC1→T9; AC2→T2/T10; AC3→T7/T10; AC4→T4; AC5→T4/T11; AC6→T8/T10(+UI T12/T13); AC7→T8/T11; AC8→T8/T11/T13; AC9→T3/T8/T11 (вкл. интеграционный ассерт `activeRestore.state/phase` на restore-ключе сида); AC10→T13; AC11→T14. Границы §1 (нет мутаций S3/etcd, нет restore-кнопок, нет admin-API, PgWorker-код не трогаем) — отражены в Global Constraints и Tasks 5/8/12.
- **Противоречие spec §4.6 vs AC7** (`prefix=freepath/` матчит regex, но AC7 ждёт 400) — разрешено Решением 11 (двухступенчатый гвард: форма + принадлежность к кластерам/дереву); AC7 проходит буквально; arch-строка эндпоинта objects (Task 0) фиксирует ОБЕ ступени.
- **Типы:** имена Tasks 1→2→4→6→8 сквозные (`MinioStorageInfo`, `IMinioInventoryStore`, `MinioInventory.Build`, `MinioReconciler.Reconcile`, `IMinioS3`, `BackupReconcileInfo`); DTO Task 8 ↔ dto.ts Task 12 — camelCase-зеркало.
- **Заполненность:** шаги содержат конкретные файлы/контракты/команды; «TBD» нет.
- **Ревью Фазы 4 (все 6 замечаний учтены):** (1) restore-ключ `demo/s1/restore/r1` в сид T9 + интеграционный ассерт `activeRestore` в T11 Step 1; (2) налив mc в чеке 45 — `--entrypoint /bin/sh` + сеть из `docker inspect as-minio` (прецедент `00-up.sh:63-70`); (3) якорь arch/03 §4 — строка `backup-s3-unreachable` в КОНЕЦ таблицы (backup-* видов в 03 §4 нет, они в arch/19 §4); (4) Task 3 — незнакомый `state` полного читается как есть, нового пути отказа нет (+тест-кейс `Parse_full_key_unknown_state_tolerated`); (5) arch-строка objects фиксирует обе ступени гварда; (6) Решение 1 — явная оговорка об осознанном отступлении от буквы spec §4.4.
