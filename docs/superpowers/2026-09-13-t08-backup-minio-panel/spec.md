# t08-backup-minio-panel — грань «Хранилище бэкапов» в AdminPanel (spec)

Дата: 2026-09-13. Канон подсистемы бэкапов — [`arch/19-backups.md`](../../../arch/19-backups.md)
(§4 контракт etcd/сироты, §5 layout S3, §7 секреты); канон панели —
[`arch/adminpanel/`](../../../arch/adminpanel/README.md) (особенно
[02-etcd-contract.md](../../../arch/adminpanel/02-etcd-contract.md) §2.3.1 и
[03-panels.md](../../../arch/adminpanel/03-panels.md)). Roadmap-текст задачи —
`arch/roadmap/backup.md` (снимается из roadmap мерж-гейтом этого же коммита).
Контекст реализации бэкапов (t02–t07) — код `src/PgWorker.Backups/`; панель —
`src/AdminPanel.*` + `frontend/`.

Задача выполняется в автономном режиме: открытые вопросы решены исполнителем
по канону и существующему коду — пары «вопрос → решение» в §3.

---

## 1. Цель

Дать оператору грань «Хранилище бэкапов» в AdminPanel — окно в MinIO/S3
объектное хранилище бэкапов PG-шардов (вариант «объектное хранилище» закрыт
каноном arch/19 §5). Сегодня в S3 живут только воркер (пишет/чистит) и джобы;
человек видит хранилище только через `mc`/консоль MinIO вне панели, а
etcd-состояние бэкапов (полные/WAL/restore/сироты) — только через алерты.
Конкретно:

- **Подключение панели к MinIO** по per-install кредам из env-секретов
  (симметрия PgWorker: `PgWorker:Backups:S3` → `AdminPanel:Backups:S3`):
  панель работает в докере, из сети docker-стенда резолвит MinIO напрямую
  (стенд: `http://minio:9000` — имя сервиса compose-сети).
- **Просмотр содержимого**: бакеты установки, дерево префиксов
  per-cluster/per-shard (`full/<id>/`, `wal/`), размеры и счётчики, построчный
  просмотр объектов (пагинация, on-demand).
- **Занятое место/квоты**: etcd-ключ `/pgworker/backups/storage` (каноничное
  `used_bytes`/`quota_bytes`/вердикт воркера) + live-факт из S3-инвентаря.
- **Health MinIO (диски)**: публичные эндпоинты `/minio/health/live` и
  `/minio/health/cluster` (200/503; drives-поля из тела при наличии).
- **Сверка фактического содержимого хранилища с etcd-состоянием бэкапов —
  «глазами человека»**: полные в S3 без etcd-ключа, etcd-ключи без объектов,
  WAL-поток vs объекты, префиксы без владельца (сироты) — с джойном на реестр
  `/pgworker/backups/orphans` супервизора (t07). Панель ТОЛЬКО ПОКАЗЫВАЕТ:
  находит и удаляет сироты автоматически супервизор бэкапов (arch/19 §4);
  панель не выполняет в S3 ни одной пишущей/удаляющей операции — никогда.

Границы (что НЕ входит): любые мутации S3 и etcd из грани (нет кнопок
удаления/запуска бэкапов/restore); запуск restore из UI (arch/19 §3.5 —
команда оператора в HTTP API воркера; в грани показывается только СТАТУС
restore из etcd); управление MinIO-сервером (пользователи/политики/диски);
новые воркерные механики; E2E PgWorker-мерж-гейт (код воркеров не меняется).

## 2. Принципы

- **Arch-first**: правки контракта панели — первой фазой в
  `arch/adminpanel/{01,02,03}*.md` и `arch/19-backups.md` (перечень — §5,
  фаза 0), затем код; spec↔arch расхождений нет.
- **Панель в S3 read-only**: клиент панели выполняет только
  `ListBuckets`/`ListObjectsV2`/`GET /minio/health/*` — ни put, ни delete,
  ни admin-API; код обёртки не содержит пишущих методов вовсе (контракт
  уровня кода, не дисциплины).
- **Один источник истины по-доменно**: статусы/сироты/квота — etcd-снапшот
  (уже читается, t02–t07); фактическое содержимое — live-инвентарь MinIO
  (собственный тик панели); сверка — чистая функция над обоими. Панель не
  дублирует вердикты воркера (использует ключи `storage`/`orphans` как есть).
- **API не ходит во внешние системы на запрос** (инвариант панели,
  docs/adminpanel/02): сводка/дерево/health — из снапшота (инвентарь вносится
  тиком refresher'а из стора по образцу `WorkerHealth`); единственное
  осознанное исключение — постраничный list объектов (on-demand, большой
  объём, тянуть тиком нельзя).
- **Креды per-install только env** (arch/19 §7): в git/gitignore-контуре их
  нет; API/UI не отдают ключи никогда (DTO несёт только endpoint/bucket).
- **Образцы кода**: фоновый тик — `ProbeOrchestrator`/`WorkerHealthPoller`;
  стор — `WorkerHealthStore`; S3-обёртка — `PgWorker.Backups.BackupS3`
  (AWSSDK.S3 из CPM, дубль панели осознанный — унификация
  `t08-unify-adminpanel-duplicates`); MinIO-фикстура тестов — `OwnMinio`
  (PgWorker.IntegrationTests).
- **Не трогаем работающее**: парсер `/pgworker/backups/` (t02–t07), правила
  алертов бэкапов, `SnapshotRefresher` — только расширение (новое поле
  снапшота, новое правило, новые эндпоинты).

## 3. Открытые вопросы и принятые решения (автономный режим)

| # | Вопрос | Решение |
|---|---|---|
| 1 | Данные грани — etcd-снапшот или живой MinIO? | **Гибрид.** Статусы полных/WAL/restore, policy, ключ `storage` (место/квота/вердикт), реестр `orphans` — из etcd-снапшота (уже читается). Фактическое дерево/размеры/health — live-инвентарь MinIO отдельным фоновым тиком `AdminPanel:Backups:IntervalSec` (default 60 c): ListBuckets + health + ОДИН полный list-v2 bucket (keys+sizes, пагинация) → агрегаты дерева в стор. Построчные объекты — on-demand (§4.6). Полный list раз в 60 с — тот же паттерн, что storage-монитор воркера (arch/19 §4, раз в 600 c); объём метаданных мал (строки ключей), S3-нагрузка пренебрежима |
| 2 | Куда поместить MinIO-клиент и тик? | **`AdminPanel.Probes/S3/`** (новая подпапка по образцу `Kafka/`): `MinioOptions`, `MinioS3Client` (тонкая read-only обёртка AWSSDK.S3 + HttpClient health), `MinioInventoryLoop` (BackgroundService), `MinioInventoryStore` (singleton). AWSSDK.S3 3.7.511.8 уже в CPM — новая зависимость не вводится. Дубль воркерного клиента осознанный (панель не ссылается на PgWorker.*) |
| 3 | Как панель узнаёт креды/endpoint? | **Секция `AdminPanel:Backups`** (`S3 {Endpoint, Region?, Bucket, AccessKey, SecretKey, PathStyle=true}, IntervalSec=60, TimeoutSec=5`), значения — env поверх appsettings (стенд: `AdminPanel__Backups__S3__*` в compose). Пустой `Endpoint` → грань выключена: тик не стартует, API отвечает `configured=false`, страница показывает «Хранилище не настроено», алерт молчит. Симметрия `PgWorker:Backups:S3` (arch/19 §7). Отдельный `Enabled`-флаг не вводится — пустой endpoint и есть выключение |
| 4 | Не споткнёмся ли об env-байндинг (урок WORKERTLS из compose)? | Простые листья `AdminPanel__Backups__S3__Endpoint` — тот же класс байндинга, что рабочие `AdminPanel__Probes__Kafka__TimeoutSeconds`/`AdminPanel__Auth__Username` (урок касался Get\<T\> конкретного POCO с path-полями). Митигация: интеграционный тест читает `IOptions<MinioOptions>` через хост (фикстура задаёт env-подобные значения конфигурации) + при обнаружении проблемы — env-перекладка в `Program.cs` (образец `WorkerTlsHandler.EnvBindings`) тем же коммитом |
| 5 | Health «диски» без admin-API? | **Публичные эндпоинты** `GET {endpoint}/minio/health/live` и `/minio/health/cluster` (документированный контракт MinIO; 00-up.sh уже использует live): live — живость процесса, cluster — 200 = все диски онлайн / 503 = degraded; тело (JSON с drives-полями `healthyDrives`/`offlineDrives`/`healingDrives`/`totalDrives` в свежих MinIO) парсим толерантно — есть поля → показываем, нет → только статус-код. Плюс `ListBuckets` как факт «API жив + креды валидны». MinIO Admin API (`mc admin`) НЕ используем: SigV4-admin-протокол вне AWSSDK.S3 и вне необходимости |
| 6 | Сверка — заново или реестр воркера? | **Оба факта рядом.** Панель считает свою сверку чистой функцией (`MinioReconciler`, Core): S3-префикс `full/<id>/` без etcd-ключа → «объекты без ключа»; etcd-ключ без объектов → «ключ без объектов» (кроме активных PLANNED/RUNNING — «идёт, объектов ещё нет»); префикс `<C>/<X>/` без владельца в `/clusters/` → «сирота (по сверке панели)»; wal: `last_uploaded_segment`/`last_uploaded_unix` из etcd vs последний сегмент объекта. Джойн с реестром `orphans` (t07): сироты панели маркируются «в реестре воркера: OBSERVED/DELETING, TTL…» либо «в реестре нет» (воркер мёртв/не дошёл — глазами человека и увидено). Расхождение панели и супервизора — не алерт, а видимый факт |
| 7 | Нужно ли новое правило AlertEngine? | **Да, одно: `backup-s3-unreachable`** (warning): конфиг задан + инвентарь-тик падал подряд ≥ 2 (`consecutiveFailures`, образец `etcd-unreachable`). Hint: «панель не может листить bucket бэкапов: endpoint/креды/сеть — бэкапы воркера под угрозой, смотрите логи PgWorker и MinIO»; Remedy=`OperatorRunbook`. Правила t02–t07 не меняются; квота/сироты/цепочки уже покрыты (`backup-storage-quota`, `backup-orphan`, …) |
| 8 | Инвентарь в EtcdSnapshot или отдельный стор на конце API? | **В снапшот** — по образцу `WorkerHealth`: `MinioInventoryLoop` пишет в `MinioInventoryStore`, `SnapshotRefresher` вносит готовое состояние в `EtcdSnapshot` новым полем `MinioStorage` (nullable — не настроен). Тогда AlertEngine и все queries читают единый снапшот; on-demand objects — единственный прямой выход (§4.6) |
| 9 | Показывать ли restore-заявки в грани? | **Да, бейджем статуса** в деталях шарда (данные уже в снапшоте: `RestoreOperationInfo`, t05): state/phase/error активной заявки. Кнопки запуска/управления НЕТ — запуск restore остаётся командой в HTTP API воркера по runbook (roadmap t08 мутаций не требует; «UI — t08» из arch/19 §3.5 трактуем как отображение статуса). Фиксируется правкой arch/19 §3.5 в фазе 0 |
| 10 | Тестовый MinIO для интеграции панели? | **Собственная фикстура** `AdminPanel.IntegrationTests/MinioContainerFixture` по образцу `OwnMinio` (PgWorker.IntegrationTests): guid-имя `apm-minio-{guid}`, порт `assignRandomHostPort: true`, готовность — POST-free GET `/minio/health/live` (бюджет ≤ 45 c — правило быстрых падений), bucket создаёт тест прямым AWSSDK-клиентом, teardown `DisposeAsync` при любом исходе + проверка «контейнер исчез». etcd-часть — существующая `EtcdContainerFixture` + расширение `EtcdSeed` ключами бэкапов |
| 11 | Чек dev-стенда? | **Новый `dev-stand/adminpanel/checks/45-backups-storage.sh`**: полный стенд (full-профиль с as-minio) → налить mc-контейнером пару тестовых объектов в `pgworker-backups` (префикс сида демо-кластера) → `curl /api/backups/storage`: configured=true, health ok, дерево содержит префикс; `docker compose ... down -v` в 90-down остаётся механизмом чистки. Браузерная проверка UI — ручная (панельные чеки — curl-уровень, прецедент 10-smoke-api) |
| 12 | Нужен ли PgWorker docker-E2E (`Scale_AddEmptyShard`) в мерж-гейте? | **Нет**: задача не трогает `src/PgWorker.*` (условие AGENTS — задачи, меняющие код воркеров). Гейт задачи: `dotnet test` панели (юниты+интеграция) зелёный + чек 45 на стенде + зачистка серий по AGENTS |
| 13 | Квота — редактируется из панели? | **Нет** — отображение etcd-ключа `storage` (воркер — писатель; квота — конфиг `PgWorker:Backups:Quota`). «Квоты» в roadmap = видимость, не управление |
| 14 | Интервал/объём тика | `IntervalSec=60` (полный list — не патрон-проба; 60 c достаточно для грани просмотра), `TimeoutSec=5` на HTTP-вызов тика; on-demand objects: `maxKeys` default 200, максимум 1000 (потолок S3) |

## 4. Структура и компоненты

### 4.1. Конфигурация (`AdminPanel:Backups`, проект Probes)

```
AdminPanel:Backups {
  S3 { Endpoint, Region?, Bucket, AccessKey, SecretKey, PathStyle=true }
  IntervalSec = 60     # период инвентарь-тика
  TimeoutSec  = 5      # HTTP-таймаут list/health-вызовов
}
```

`MinioOptions` (`[Config]`-POCO, AdminPanel.Probes): валидация старта —
`IntervalSec <= 0` или `TimeoutSec <= 0` при заданном `Endpoint` → fail-fast
(образец PgWorker:Backups); `Endpoint` задан при пустых
`Bucket/AccessKey/SecretKey` → fail-fast. appsettings.json — секция с пустыми
значениями (грань выключена по умолчанию, как `Etcd:Endpoints` до настройки).

### 4.2. S3-клиент панели (`AdminPanel.Probes/S3/MinioS3Client.cs`)

Тонкая read-only обёртка (AWSSDK.S3, `ForcePathStyle=true` — MinIO и облако
одним клиентом; дубль `PgWorker.Backups.BackupS3` осознанный):

```csharp
public interface IMinioS3
{
    Task<Result<IReadOnlyList<string>>> ListBucketsAsync(CancellationToken ct);
    // list-v2 по префиксу с пагинацией (ключи+размеры+lastModified; для on-demand —
    // ContinuationToken наружу)
    Task<Result<S3Page>> ListPageAsync(string? prefix, string? continuationToken,
        int maxKeys, CancellationToken ct);
    // health: GET /minio/health/live | /minio/health/cluster (HttpClient, без SigV4)
    Task<MinioHealth> GetHealthAsync(CancellationToken ct);
}
```

`MinioHealth` = `{ ApiOk(bool, ошибка ListBuckets), LiveOk(bool),
ClusterOk(bool? — null = эндпоинт не отвечает/не поддерживает),
Drives(MinioDrives? — healthyDrives/offlineDrives/healingDrives/totalDrives,
толерантный парс тела) }`. Пишущих методов в интерфейсе и реализации нет.

### 4.3. Инвентарь-тик (`MinioInventoryLoop` + `MinioInventoryStore`)

`BackgroundService` (образец `ProbeOrchestrator`/`WorkerHealthPoller`):
каждый `IntervalSec` — (1) `GetHealthAsync`, (2) `ListBucketsAsync`,
(3) полный list-v2 bucket постранично (ключи+размеры; один прогон),
(4) агрегация чистой функцией `MinioInventory.Build(objects)` (Core):

```
MinioClusterNode { Cluster, SizeBytes, Shards[MinioShardNode] }
MinioShardNode   { Cluster, Shard, SizeBytes,
                   Fulls[ { Id, SizeBytes, ObjectCount, LastModifiedUnix } ],
                   Wal { SegmentCount, HistoryCount, SizeBytes, LastModifiedUnix } }
+ UsedBytes (сумма), ObjectCount, Buckets[], ForeignPrefixes (корневые
  префиксы не формы "<C>/<X>" — информация, без вердикта)
```

Результат + health + `ConsecutiveFailures`/`LastError` — атомарная замена в
`MinioInventoryStore` (singleton, образец `WorkerHealthStore`). Сбой тика
(любой шаг) → счётчик +1, прежний инвентарь живёт (устаревающий, со штампом
`UpdatedAtUtc`); успех → счётчик 0. Первый тик — сразу на старте (образец
ProbeOrchestrator). Тик не блокирует KV-refresher.

### 4.4. Снапшот и сверка (Core)

- `EtcdSnapshot` += `MinioStorageInfo? MinioStorage` (вносится refresher'ом
  из стора — по образцу `WorkerHealth`): `{ Configured, Buckets, Health,
  UsedBytes, ObjectCount, Clusters[], ForeignPrefixes, UpdatedAtUtc,
  ConsecutiveFailures, LastError? }`.
- **`MinioReconciler`** — чистая функция
  `(MinioStorageInfo, IReadOnlyList<ClusterBackupsInfo>, IReadOnlyList<ClusterInfo>, BackupOrphansInfo?) → BackupReconcileInfo`:
  - per-shard полные: статус каждого `full/<id>` — `Ok` (объекты+ключ),
    `S3Only` (объекты, ключа нет — мусор упавших upload'ов/гигиены),
    `EtcdOnly` (ключ есть, объектов нет; активные PLANNED/RUNNING — ожидаемо,
    помечается «идёт»), `Deleting` (DELETING-ключ + остатки объектов —
    доводка ретенции);
  - префиксы `<C>/<X>/` без владельца в `/clusters/` → `OrphanPrefix`:
    `{ Prefix, Kind(shard|cluster), SizeBytes, InWorkerRegistry(bool),
    RegistryState(OBSERVED|DELETING?), FirstSeenUnix? }`;
  - wal: etcd `last_uploaded_segment`/`unix` vs S3-факт (последний объект) —
    расхождение показывается, вердикта не ставит.
- Модель `BackupInfo.cs` расширяется типами сверки (только новые records,
  существующие не меняются).

### 4.5. Алерт (Core/Alerting)

`BackupS3UnreachableRule` (`Rules/BackupS3UnreachableRule.cs`, каталог
03 §4): `configured=true && ConsecutiveFailures >= 2` → warning
`backup-s3-unreachable`, target = endpoint+bucket; Hint/Remedy — §3.7;
снимается первым успешным тиком. Юнит — по образцу `BackupOrphanRuleTests`.

### 4.6. REST API (AdminPanel.Api/Inspection)

Все — GET, cookie-auth (как остальное `/api/*`), ProblemDetails для ошибок:

| Метод+путь | Назначение |
|---|---|
| `GET /api/backups/storage` | сводка: `configured` (false → только это поле+причина), endpoint/bucket (без ключей), health, место (`etcd` из ключа `storage`: used/quota/state/updated + `live` из инвентаря), дерево кластеров (размер/шарды/счётчики), сироты (реестр воркера + сверка панели, слитые), `inventoryUpdatedUnix`, `inventoryError` |
| `GET /api/backups/storage/{cluster}/{shard}` | детали шарда: полные (id, sizeBytes, objectCount, lastModified + etcd state/verify/size), wal (сегменты/истории/размер/last + etcd wal-статус), активный restore (бейдж-данные), сверка per-full. 404 — кластера/шарда нет ни в etcd, ни в S3-дереве |
| `GET /api/backups/objects?prefix=&maxKeys=200&continuationToken=` | on-demand постраничный list-v2 (единственный выход наружу на запрос): ключ, размер, lastModified; `prefix` пуст или начинается с `<C>/…`, где `<C>` — паттерн имён кластеров `^[a-z][a-z0-9_]{0,62}$` (400 иначе — защита от произвольного листинга); `maxKeys` 1..1000 (400 вне диапазона); nextContinuationToken в ответе |

DTO (03 §2 пополняется): `BackupStorageDto`, `BackupShardStorageDto`,
`BackupObjectsPageDto` (+ вложенные `BackupFullDto`, `BackupWalDto`,
`BackupOrphanDto`, `MinioHealthDto`). camelCase, ключи S3 НЕ отдаются.
Реализация — CQRS-queries над снапшотом (+ `IMinioS3` для objects),
маппинг 1:1 по образцу `KafkaQuery`.

### 4.7. UI (frontend)

Страница **«Хранилище бэкапов»** (`/backups-storage`, пункт навигации):
- Карточки: Health (api/live/cluster + drives-поля при наличии),
  Место (used/quota, прогресс-бар, state-бейдж OK/WARN/CRIT — etcd-вердикт
  воркера + штамп live-инвентаря), Buckets.
- Таблица «Кластеры → шарды»: cluster/shard, размер, полные (шт.),
  WAL-сегменты (шт.), пометки сверки (значок «есть объекты без ключа» /
  «сирота»), клик → детали шарда.
- Детали шарда (подстраница `/backups-storage/:cluster/:shard`): таблица
  полных (id, размер, дата, etcd state + verify-бейдж, статус сверки), блок
  WAL (etcd-статус цепочки + S3-факт), бейдж активного restore, блок
  «Объекты» — таблица с кнопкой «Загрузить ещё» (on-demand пагинация,
  префикс деталей шарда).
- Блок «Осиротевшие префиксы»: префикс, kind, размер, реестр воркера
  (OBSERVED/DELETING + first_seen/TTL) или «панель видит, в реестре нет».
- Не настроено (`configured=false`) — заглушка «Хранилище бэкапов не
  настроено (AdminPanel:Backups:S3)». Polling — общий переключатель (5 c);
  on-demand objects — по действию.

DTO — `api/dto.ts`, запросы — `api/queries.ts`, маршрут/навигация —
`App.tsx`/`AppLayout` (чек-лист docs/adminpanel/04).

### 4.8. Dev-стенд и секреты

- `dev-stand/adminpanel/docker-compose.yml` (сервис `adminpanel`, профиль
  по умолчанию): env `AdminPanel__Backups__S3__Endpoint=http://minio:9000`,
  `...__Bucket=pgworker-backups`, `...__AccessKey/SecretKey=minioadmin` —
  стендовые дефолты (тот же класс значений, что `MINIO_ROOT_USER` as-minio;
  прод — per-install env, ключи в git не попадают). Панель и as-minio — одна
  compose-сеть, имя `minio` резолвится напрямую (docker-only панели, AGENTS).
- Чек `checks/45-backups-storage.sh` (§3.11). `00-up.sh` не меняется (bucket
  уже создаёт). Прод-поставка: документирование env в
  `docker/AdminPanel.Dockerfile`-упоминании не требуется (ENV поверх образа —
  практика 01 §7).

## 5. Фазы реализации (порядок; фаза 0 — обязательная первая)

**Фаза 0 — arch-правки (до кода)**:
- `arch/adminpanel/01-architecture.md`: §1 — поток «MinIO live-тик»
  (инвентарь-стор → снапшот) на схеме и в правилах; §2 — строка
  `AdminPanel.Probes` пополняется S3/MinIO-пробой; §6 — секция
  `AdminPanel:Backups`; §8 — строка «MinIO недоступен» (инвентарь устаревает,
  алерт warning).
- `arch/adminpanel/02-etcd-contract.md`: §2.3.1 — строки `storage`/`orphans`:
  «в UI не отображается (t08 — грань MinIO)» → «отображается в грани
  "Хранилище бэкапов" (t08)»; новая секция «§2.5. MinIO/S3 бэкапов —
  live-чтение панели (t08)»: конфиг-секреты, read-only-контракт, инвентарь-тик
  60 c, on-demand objects, сверка = отображение (удаление — супервизор
  воркера, arch/19 §4); §3 — `EtcdSnapshot.MinioStorage`.
- `arch/adminpanel/03-panels.md`: §1 — три эндпоинта таблицы; §2 — DTO;
  §3 — панель UI «Хранилище бэкапов» (read-only, без форм — счётчик «форм
  ввода» не растёт); §4 — kind `backup-s3-unreachable`.
- `arch/adminpanel/04-local-stand.md`: env панели стенда
  (`AdminPanel__Backups__S3__*`, minio-сервис — источник).
- `arch/19-backups.md`: §3.5 — «UI — t08» → «отображение статуса restore —
  грань t08 панели; запуск — команда оператора в API воркера (runbook)»;
  §5 — примечание «панель читает bucket read-only (инвентарь/health/сверка,
  t08); пишущие/удаляющие операции — только воркер».
- `docs/adminpanel/05-dev-stand.md`: чек 45 в порядке чеков (практики).

**Фаза 1 — бэкенд Core+Probes**: `MinioOptions` (+валидация),
`IMinioS3`/`MinioS3Client`, `MinioInventoryLoop`/`MinioInventoryStore`,
модель `MinioStorageInfo` + `MinioInventory.Build`, `MinioReconciler`,
`BackupS3UnreachableRule`, `EtcdSnapshot.MinioStorage` (внесение
refresher'ом), DI-склейка. Юниты: Build-агрегация по образцу объектов
(вкл. foreign-префиксы/wal/history), Reconciler (все статусы §4.4 — чистые
функции, AAA), правило алерта, валидация опций.

**Фаза 2 — API**: queries + DTO + эндпоинты Inspection (сводка/детали
шарда/on-demand objects с 400-гвардом prefix), маппинг кредов — никуда.
Интеграционные: `MinioContainerFixture` (свой MinIO, динамический порт,
bucket прямым AWSSDK-клиентом) + `EtcdContainerFixture`/`EtcdSeed` (ключи
полных/wal/storage/orphans) + `WebApplicationFactory`: дерево/health/сверка
на сеянных объектах; stop MinIO → 2 тика → алерт `backup-s3-unreachable`
(ручной прогон тика `RunOnceAsync`-образцом); objects-пагинация (seeds
maxKeys=2 → 2 страницы); `configured=false` → сводка без дерева. Teardown
фикстуры при любом исходе + чистота (контейнер исчез).

**Фаза 3 — frontend**: DTO/queries/страница/детали/навигация (§4.7);
`npm run typecheck` + `npm run build`.

**Фаза 4 — dev-стенд**: env панели в compose, чек 45, ручная проверка UI на
полном стенде; докатка docs/adminpanel/05 при граблях.

**Фаза 5 — гейт**: `dotnet test src/AdminPanel.*` зелёный
(TreatWarningsAsErrors); зачистка docker-серий по AGENTS (контейнеры+сети,
стенд as-* не трогать); roadmap-тег `t08-backup-minio-panel` снимается из
`arch/roadmap/backup.md` тем же мерж-коммитом.

## 6. Ограничения

- Панель ВСЕГДА в докере; доступ к MinIO — из docker-сети стенда
  (`minio:9000`), никаких хост-процессов.
- Креды/endpoint — только env-конфиг; в API/UI ключи не отдаются; в git их
  нет. Панель в S3 только читает (контракт уровня кода §2).
- Тесты: docker-порты динамические (`assignRandomHostPort` +
  `GetMappedPublicPort`), никаких литералов-портов; готовность MinIO ≤ 45 c;
  общие таймауты фикстур ≤ 100 c; зачистка контейнеров/сетей после каждой
  серии (`docker rm -f` + `docker network prune -f`, стенд `as-*` не трогать)
  — интеграционные фикстуры чистят себя сами (`IAsyncLifetime`), ручная
  зачистка — страховочный гейт.
- `TreatWarningsAsErrors=true`, `Nullable=enable`, `LangVersion=latest`;
  версии пакетов — только CPM (AWSSDK.S3 уже там; новых пакетов нет).
- Код PgWorker/KafkaWorker не меняется (E2E-мерж-гейт воркеров не
  применяется; правка только тестово-независимых compose/чеков стенда панели).
- Спецификация UI соответствует стеку Mantine/TanStack Query (тёмная тема,
  polling; WebSocket нет). Форм ввода не добавляется.

## 7. Критерии приёмки

1. **AC1 (конфиг/skip)**: без `AdminPanel:Backups:S3:Endpoint` панель
   стартует, `GET /api/backups/storage` → `{configured:false}`, тик не
   поднимается, алерт не горит, страница — заглушка (интеграция).
2. **AC2 (инвентарь)**: MinIO с bucket `pgworker-backups` и сеянными
   объектами (`<C>/<X>/full/<id>/...`, `wal/<segment>`, `wal/<tli>.history`,
   посторонний корневой префикс) → после тика сводка отдаёт: buckets, health
   (api ok/live ok/cluster ok), дерево `<C>/<X>` с размерами/счётчиками
   (full sizeBytes/objectCount, wal segmentCount/historyCount), foreign-
   префикс без вердикта; `usedBytes` = сумме (интеграция + юниты Build).
3. **AC3 (health/диски)**: live-эндпоинт отвечает, cluster-эндпоинт 200 →
   `clusterOk=true`; остановленный MinIO → `apiOk=false`, `liveOk=false`,
   алерт после 2 неудачных тиков (интеграция stop-контейнера).
4. **AC4 (сверка полных)**: объекты `full/<id>/` без etcd-ключа → статус
   `S3Only`; ключ COMPLETED без объектов → `EtcdOnly`; активный
   PLANNED/RUNNING без объектов → «идёт» (юниты Reconciler — все ветви,
   AAA-комментарии).
5. **AC5 (сироты)**: префикс `<C>/<X>/` без кластера-владельца: при записи в
   etcd реестра `orphans` — строка блока сирот с `OBSERVED/DELETING`+TTL;
   без записи — «панель видит, в реестре нет» (юнит + интеграция).
6. **AC6 (место/квота)**: ключ `/pgworker/backups/storage` (used/quota/WARN)
   отображается в DTO рядом с live-инвентарём; прогресс-бар и state-бейдж —
   на странице (интеграция DTO + ручная проверка UI).
7. **AC7 (on-demand objects)**: `GET /api/backups/objects` с `maxKeys=2` →
   страница + `nextContinuationToken`, вторая страница продолжает список;
   `prefix=freepath/` → 400; несанкционированный (без cookie) → 401
   (интеграция).
8. **AC8 (секреты)**: ни один DTO/ответ API не содержит access/secret key
   (ассерт по сериализации всех трёх эндпоинтов — интеграция grep-подобной
   проверкой полей).
9. **AC9 (детали шарда)**: etcd-статусы (full state/verify, wal-статус,
   активный restore state/phase) джойнятся с S3-фактами в
   `BackupShardStorageDto` (интеграция на сид-наборе).
10. **AC10 (стенд)**: полный стенд + чек `45-backups-storage.sh` зелёный
    (configured=true, health ok, дерево с налитым префиксом); UI на стенде
    показывает грань (ручная проверка, скриншот в PR-описании не обязателен);
    зачистка серий по AGENTS.
11. **AC11 (гигиена)**: `dotnet build`/`dotnet test` панели зелёные
    (TreatWarningsAsErrors); `npm run typecheck`/`build` зелёные; arch-правки
    фазы 0 тем же PR; roadmap-тег снят мерж-коммитом.
