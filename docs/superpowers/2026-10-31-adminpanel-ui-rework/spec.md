# Спека: переделка UI AdminPanel по целевой модели 06-ui-analysis (u01–u19)

> Работа в worktree `feat-adminpanel-ui-rework`. Канон панели — `arch/adminpanel/`
> (01-архитектура, 02-etcd-контракт, 03-панели). Целевая модель и список задач —
> [`docs/adminpanel/06-ui-analysis.md`](../../adminpanel/06-ui-analysis.md) §4 (модель),
> §6 (u01–u19). Режим задачи: вопросы пользователю запрещены, все развилки закрыты
> в разделе «Принятые решения»; гейт изучения spec предодобрен.

## 1. Цель и фон

Переделать интерфейс AdminPanel под целевую модель `docs/adminpanel/06-ui-analysis.md`:
навигация в 5 групп, единый шаблон доменной страницы, слияние HA в pg-домен,
общие компоненты действий, серверная агрегация дашборда, лента событий без
хранения истории, закрытие пробелов наблюдаемости (стоп бэкапов, приёмники WAL,
эвакуации, work-журналы kafka/valkey/s3, координация воркеров), эффективный
конфиг воркера (read-only), отмена живых заявок ротаций kafka/valkey, поля
`garbage_min_age_sec` в policy, группировка алертов по Remedy-движителю,
синхронизация контракта arch↔код.

Ключевое требование пользователя: **«нужно чтоб мог вводить параметры где
можно»** — все поддерживаемые мутации доступны как формы/кнопки в UI у своей
сущности. Результат — основа, по которой пользователь будет давать оценки и
задачи на доработку: делаем максимально по документу (u01–u19 + то, что §4.3
прямо включает в управление).

Объём: **u01–u19** (u20 — секция etcd под etcdworker — ВНЕ объёма, документ
помечает «после roadmap t32»). Мутации — только существующие в API воркеров +
прямо предписанные документом (u17 — отмена ротаций; restore-заявка из §4.3 —
см. решение D18). Новых мутаций «от себя» нет.

## 2. Принципы

1. **Arch-first**: каждое изменение контракта панели (новые читаемые префиксы,
   новые эндпоинты/DTO, каталог алертов) сначала фиксируется в
   `arch/adminpanel/02-etcd-contract.md` / `03-panels.md` (и, для контрактов API
   воркеров, в `arch/14/16/21/24`), затем код — тем же коммитом. Полный список
   правок arch — раздел 8.
2. **Сервер — источник истины валидации**, фронт дублирует для UX из ЕДИНОГО
   модуля границ (сейчас константы повторены в 4+ модалах и DTO).
3. **Панель в etcd не пишет** (кроме `/workers/api_tls/*`); все новые мутации —
   прокси в HTTP API воркеров по живым lease-ключам дискавери.
4. **API не ходит в etcd на запрос** — только чтение снапшотов из памяти;
   исключение — уже существующий паттерн on-demand прокси к API воркера
   (мутации, healthz-поллер; u15 добавляет read-only on-demand конфиг тем же
   паттерном).
5. **Обновление — только polling** (TanStack Query `refetchInterval`,
   2/5/15/off); WebSocket/SSE не появляются.
6. **UI не обещает историю**: журналы воркеров перезаписываются; событийный слой
   (u09) живёт только в открытой вкладке, без persistence.
7. **Штатная тишина — серым, не красным**: NOT_INITIALIZED/TO_REMOVE не алертятся
   (движок уже подавляет) и в UI красятся нейтрально.
8. Аутентификация/роли — без изменений: cookie-сессия, один админ
   (`arch/adminpanel/01` §4), логика не трогается.

## 3. Целевая структура UI (u01, u03, u05)

### 3.1. Дерево навигации (5 групп)

Навигация из плоского списка 11 пунктов (текущий `AppLayout.NAV_ITEMS`)
перегруппировывается в 5 групп (06 §4.2). Существующие URL-маршруты СОХРАНЯЮТСЯ
без изменений (перегруппировка — только меню; редиректы не нужны). Новые
маршруты: `clusters/:cluster/shards/:shard` (страница шарда, u03) и `/events`
(лента, u09).

```
ОБЗОР
  Дашборд                    /                          (существ.)

ДОМЕНЫ
  PostgreSQL                 /clusters                  (существ.)
    Кластер                  /clusters/:c               (существ.; вкладки по §3.2)
    Шард                     /clusters/:c/shards/:x     (НОВЫЙ, u03)
  Kafka                      /kafka, /kafka/:c          (существ.)
  Valkey                     /valkey, /valkey/:c        (существ.)
  S3-хранилища               /s3, /s3/:c                (существ.)

ДАННЫЕ И ЗАЩИТА
  Бэкапы                     /backups-storage           (существ.; + :c/:x детали)
  Надёжность                 /reliability               (существ.)

НАБЛЮДЕНИЕ
  Алерты                     /alerts                    (существ.)
  События                    /events                    (НОВЫЙ, u09)
  Репликация                 /ha                        (существ.; глобальный срез
                                                        HA-скопов, u03)

ИНФРАСТРУКТУРА
  Воркеры                    /workers                   (существ.)
  etcd-контур                /etcd                      (существ.)
```

Реализация: `AppLayout` — группы-заголовки (Mantine `NavLink` с `label` группы
и `children` либо `Box`-секции с `Text`-заголовком; выбор за исполнителем в
рамках Mantine 9 без новых зависимостей). Подписи навигации — русские
(решение D2): «Дашборд», «PostgreSQL», «Kafka», «Valkey», «S3-хранилища»,
«Бэкапы», «Надёжность», «Алерты», «События», «Репликация», «Воркеры»,
«etcd-контур». Имена собственных систем (PostgreSQL/Kafka/Valkey/S3/etcd) не
переводятся. Счётчики алертов у пункта «Алерты» сохраняются.

### 3.2. Единый шаблон доменной страницы (u01)

Общий layout-компонент (условное имя `DomainPageLayout`) для страниц деталей
кластера всех 4 доменов:

1. **Шапка-статус**: имя, state-бейджи (ACTIVE/NOT_INITIALIZED/TO_REMOVE —
   серым, не красным), режимы (pg: strict; kafka: RF/minISR; valkey:
   maxmemory/policy; s3: etcdBackup), endpoints, заявки в полёте (бейджи
   «ротация…», «reassignment…», «в очереди: N заявок»), кнопки действий уровня
   кластера (удалить, ротации — из общих компонентов u02).
2. **Вкладки сущностей** — состав по доменам:
   - **pg-кластер** `/clusters/:c`: `Шарды | Бакеты | Переезды | Бэкапы (срез) |
     Секреты | Журнал` (по дереву 06 §4.2):
     - `Шарды` — как сейчас (dsn, replicas, master+lease, ноды, заявка ресурсов,
       «Добавить шард», «Убрать шард»); строка шарда — ссылка на страницу шарда;
     - `Бакеты` — как сейчас (грид id×owner×state, заявки, перенос/откат/
       финализация); вкладка скрыта при `sharded=false` (вычисляемое поле —
       грабля 06);
     - `Переезды` — как сейчас (не-ACTIVE, очередь заявок, журнал воркера
       переносится во вкладку «Журнал», здесь — очередь + статусы);
     - `Бэкапы (срез)` — НОВАЯ вкладка: RPO/WAL/полные этого кластера + policy
       (данные существующего `GET /api/backups/storage`, клиентский фильтр по
       кластеру; серверных изменений не требуется) + ссылки на детали шарда
       бэкапов и на `/reliability` (u04);
     - `Секреты` — НОВАЯ вкладка: заявка ротации per-cluster секретов (кнопка
       «Сменить app-пароль» переносится из шапки сюда, решение D5), состав
       ротируемых ролей (app/bucket_admin/bucket_mover — текст, без значений),
       предупреждение о разрыве подключений;
     - `Журнал` — НОВАЯ вкладка: work-журнал `/pgworker/work/<C>` (op/phase/
       updated/last_error/fail-серия), heals (переносится сюда), эвакуации
       (u12, новый блок). Блок «Стендовая топология» остаётся на странице
       кластера как сейчас (вне дерева §4.2, статус-кво).
   - **kafka-кластер** `/kafka/:c`: `Брокеры | Топики | Группы | Журнал` —
     существующие вкладки + `Журнал` (work-журнал `/kafkaworker/work/<C>`, u13:
     op/phase/instance/updated/last_error; исходы заявок `ticketOutcome` уже в
     DTO).
   - **valkey-кластер** `/valkey/:c`: `Нода | Журнал` — существующая вкладка
     Нода + `Журнал` (work `/valkeyworker/work/<C>`, u13).
   - **s3-кластер** `/s3/:c`: `Ноды | Репликация | Связность и синхронность |
     Журнал` — существующие + `Журнал` (work `/s3worker/work/<C>`, u13).
3. **Журнал воркера** — нижний блок шаблона (единый компонент; на pg-странице
   дублируется во вкладке «Журнал» вместе с heals/эвакуациями, в остальных
   доменах — вкладка «Журнал» и есть этот блок).

Все существующие формы/кнопки на вкладках сохраняют поведение (эндпоинты, guards,
тексты предупреждений — по `arch/adminpanel/03` §3.x–§12.3).

### 3.3. Слияние HA в pg-домен (u03)

- **Страница шарда** `/clusters/:c/shards/:x` — НОВАЯ: детали нод шарда
  (существующие данные `ShardDto`), заявка ресурсов, runtime SQL (при включённых
  пробах) и **HA-блок**: leader, optime, members (роль/состояние/лаг/timeline),
  raw config (свернуто), кнопка «Пересоздать ноду» (soft/hard — перенос
  `recreateNode` с `HaScopeDetailsPage` сюда), заявленные ресурсы scope.
  Данные — существующие `GET /api/clusters/{c}` (шард) + `GET /api/ha/{scope}`
  (scope = `<c>-<x>`); новых эндпоинтов не нужно.
- **`/ha` («Репликация»)** — глобальный срез всех HA-скопов (список как
  сейчас: scope, лидер, члены, лаг, unmatched); строка matched-скопа — ссылка
  на страницу шарда.
- **`/ha/:scope`** — сохраняется ТОЛЬКО для unmatched-скопов (нет пары
  кластер×шард → странице шарда не адресуемы). Для matched-скопа маршрут делает
  клиентский редирект (`Navigate replace`) на `/clusters/:c/shards/:x`.
  `HaScopeDetailsPage` остаётся в коде как страница unmatched-деталей + редирект.

### 3.4. Общие компоненты действий (u02)

Вместо 4 копий по каталогам доменов (сейчас: `DeleteClusterButton`,
`DeleteKafkaClusterButton`, `DeleteValkeyClusterButton`,
`DeleteS3ClusterButton`; `RotateClusterSecretsButton`,
`RotatePasswordButton`×2, `RotateAdminPasswordButton`, `RotateCaButton`×2,
`RotateSecretsButton`; `EditBrokerResourcesModal`, `EditNodeResourcesModal`×2)
— общие компоненты в `frontend/src/components/actions/`:

| Компонент | Заменяет | Параметризация |
|---|---|---|
| `DeleteEntityButton` | 4 Delete-кнопки | заголовок/подтверждение, `canMutate`, мутация (endpooint) |
| `RotateSecretButton` | 6 Rotate-кнопок/модалей | роль (app/admin/ca/secrets), текст предупреждения, «живая заявка» (409-текст), опциональная кнопка отмены (u17) |
| `ResourcesForm` (модал) | 3 Resources-модала | границы (общие константы), подпись способа применения (rolling/пересоздание) |
| `ConfirmModal` | confirm-модалы с вводом | заголовок, текст, ожидаемая строка подтверждения |

Единый источник границ валидации — новый модуль
`frontend/src/utils/validation.ts`: имя кластера `^[a-z][a-z0-9_]{0,62}$`,
cpu 0.01..64 (шаг), mem/disk 1..65536 GiB, replicas 1..26, buckets 1..8192,
shards 1..128, kafka: brokers 1..9, RF 1..9, minISR 1..RF, partitions 1..1000,
retentionMs 1..2147483647; valkey: maxmemoryBytes ≥ 1, 8 значений policy,
инвариант R3 `maxmemory < memGi`; s3: инстансы 0..9, placement-селекторы.
Значения — дословно из `arch/adminpanel/02` §9.3/§10.3/§11.3/§12.3. Все модали
форм импортируют константы оттуда (дублирование чисел в компонентах
удаляется). Сервер остаётся источником истины.

### 3.5. Двусторонние связи (u04)

- **Алерт → целевая сущность**: в `AlertsPage` (и в ленте дашборда) —
  переход по kind/target. Маппинг kind-префикса → маршрут (единая таблица в
  SPA): `kafka-*` → `/kafka/:c` (target = имя кластера/брокера/топика);
  `valkey-*` → `/valkey/:c`; `s3-*` → `/s3/:c`; pg-кластерные (`shard-*`,
  `move-*`, `bucket-*`, `cluster-*`, `provision-stuck`, `backup-*` с
  кластерным target) → `/clusters/:c`; `backup-*` per-shard → детали шарда
  бэкапов `/backups-storage/:c/:x`; probe-алерты → `/ha` или `/clusters/:c`;
  `etcd-*`/`snapshot-stale`/`worker-*` → `/etcd`/`/workers`. Точный разбор
  target — по фактическому формату `id=kind:target` каждого движка (исполнитель
  сверяет по коду правил; таблица покрывает все kind из каталогов 03 §4/§7.4/
  §8.4/§12.4, непокрытые — без ссылки).
- **Кластер → его бэкапы/RPO**: на вкладке «Бэкапы (срез)» (pg) — ссылки на
  `/backups-storage` и `/reliability`; из строки кластера на `/reliability` —
  обратная ссылка на кластер (уже есть формат RPO-таблицы).
- **Бакет → его переезд**: клик по не-ACTIVE бакету (вкладка Бакеты) —
  переход/скролл к вкладке «Переезды» с подсветкой строки.
- **RPO-строка → детали шарда**: клик по id полного — переход к
  `/backups-storage/:c/:x` (уже есть, сохранить).

### 3.6. Дашборд (u05–u08)

- **u05**: карточка Valkey на дашборде (порт `KafkaCard`/`S3Card`: из
  `OverviewDto.valkey` — clustersTotal/clustersCritical; null до первого тика
  valkey-refresher'а). `WORKER_TITLES` в `WorkersPage` дополняется
  `valkeyworker: 'ValkeyWorker (кеш)'` (решение D2 — русские подписи; фактическая
  строка — по согласованному стилю карточек: `ValkeyWorker (Valkey)`).
- **u06 — серверная агрегация Overview**: `OverviewDto` расширяется полями
  (сервер считает в `OverviewQueryHandler` из снапшотов; клиентские доборы
  `/api/ha`, `/api/backups/storage`, `/api/reliability` с дашборда УДАЛЯЮТСЯ):

```text
OverviewDto: ...существующие...
 + ha { scopesTotal, withoutLeader, unmatched }          // из EtcdSnapshot.HaScopes
 + backups { configured, health { apiOk, liveOk, clusterOk } | null,
             storageState (OK|WARN|CRIT|null), usedBytes, quotaBytes,
             etcdSnapshots { enabled, state } | null }    // из S3StorageInfo + BackupsInfo
 + reliability { worstRpoPotentialSec | null, fullOnlyCount, offCount,
                 lastFailover { durationSec, atUnix } | null } // из ReliabilityCalculator
```

  Логика агрегации — перенос текущей TS-логики `OverviewPage`
  (`ReliabilityCard`, `HaCard`, `BackupsCard`) в C# (`OverviewMapper`);
  юнит-тесты на маппер обязательны (те же входы → те же числа).
- **u07 — блок «Активные операции»** на дашборде (серверное поле):

```text
 + operations[{ domain (pg|kafka|valkey|s3), kind, cluster, entity | null,
                detail, updatedUnix | null }]
```

  Источники (все из уже читаемых снапшотов + новые из u12/u13):
  - pg: активные переезды (бакеты SYNCING/FROZEN/ABORTING — как сейчас
    `activeMoves`), очередь заявок `pendingMoves`, живая ротация
    `/pgworker/rotations/<C>`, restore-ключи (`state=RUNNING/REJOINING`) и
    drill (`RUNNING`) из `/pgworker/backups/`, op из work-журнала при
    переходных фазах;
  - kafka: живые ротации (app/admin/ca), rebalance-заявка, прогресс
    reassignment/regens, lifecycle-заявки топиков (create/delete);
  - valkey: живые ротации (пароль app/admin, CA);
  - s3: живая заявка ротации секретов.
  Существующая таблица «Active moves» заменяется этим блоком (переезды —
  подмножество операций домена pg).
- **u08 — свежесть per-домен**: `OverviewDto` получает
  `freshness { etcd { builtAtUtc }, pg { builtAtUtc }, kafka { builtAtUtc },
  valkey { builtAtUtc }, s3 { builtAtUtc } }` (null — снапшот ещё не тикал;
  источники — `EtcdSnapshot.BuiltAtUtc`, `KafkaSnapshot.BuiltAtUtc`,
  `ValkeySnapshot.BuiltAtUtc`, `S3Snapshot.BuiltAtUtc`, etcd —
  `EtcdStatus.lastRefreshUtc`). UI: в шапке layout'а вместо одного `StaleBadge`
  — компактная группа из 5 мини-бейджей (etcd/pg/kafka/valkey/s3: возраст
  данных; старше `3×RefreshInterval` — жёлтый «stale», по механике
  `SnapshotStaleRule`); на дашборде возраст дублируется в карточках доменов.
  Общий stale-бейдж (по pg-снапшоту) удаляется.

### 3.7. Лента событий `/events` (u09)

Чисто клиентский событийный слой поверх polling-ответов (без серверных
изменений и без хранения):

- Реализация: модуль `frontend/src/events/` — подписка на изменения
  TanStack Query-кэша (queryCache-подписка или обёртка в queryFn) для ключей
  overview/clusters/ha/kafka/valkey/s3/backups; сравнение prev→next снимков
  (снятых целиком) → эмит событий в in-memory store (массив, cap 500,
  новые сверху). Живёт с открытой вкладкой; перезагрузка = пустая лента.
- **Событие** = { tsUnix, domain, entityKind (cluster|shard|node|broker|
  bucket|topic|ticket|heal|evacuation|rotation|instance), entity, type
  (state-change|appeared|gone|outcome), from?, to?, detail? }. Что считаем:
  - переходы state кластеров/нод/шардов/брокеров/бакетов/инстансов s3
    (вкл. исчезновение сущности = удалена);
  - заявки (moves/ротации/rebalance/lifecycle/desired): появление = «поставлена»,
    исчезновение = «исполнена или отвергнута» (detail по work.last_error, если
    появился/обновился); `ticket_outcomes` kafka/valkey — исход напрямую;
  - heals: новые записи (по ts);
  - эвакуации (после u12): появление/изменение/исчезновение;
  - freshness-события НЕ эмитим (шум).
- UI: таблица-лента с фильтром по домену и по entityKind; событие кликабельно
  (deep-link на сущность по таблице u04).

## 4. Пробелы наблюдаемости (u10–u14) — arch-first чтение новых префиксов

### u10 — глобальный стоп бэкапов
- Чтение: ключ `/pgworker/backups-disabled` (точечный range в pg-тик; ключ НЕ
  входит в существующий префикс `/pgworker/backups/` — сравнение байтов:
  `-` < `/`; документ 06 §3.4 «префикс читается» — фактически не читается,
  расхождение зафиксировано). Модель: `EtcdSnapshot.BackupsDisabled
  { UpdatedUnix, UpdatedBy? } | null` (значение — канон arch/19; толерантный
  парсер: битый JSON → parseError-запись).
- UI: жёлтый баннер «Бэкапы остановлены (глобальный стоп)» на дашборде и на
  грани `/backups-storage`.
- Алерт: `backups-disabled` (warning; Remedy `OperatorRunbook` — «запуск —
  снять ключ /pgworker/backups-disabled или включить Backups:Enabled»),
  в `AlertEngine` + каталог 03 §4.

### u11 — статусы приёмников WAL per-node
- Чтение: ключи `/pgworker/backups/<C>/<X>/agent/<N>` — уже внутри читаемого
  range `/pgworker/backups/`; парсер `BackupsParser` расширяется моделью
  `WalAgentInfo { Cluster, Shard, Node, State, Error?, UpdatedUnix, ... }`
  (формат — канон arch/19 §4: `starting|waiting_password|waiting_slot|
  streaming|paused|failed_permanent`, `tli_jump`; толерантный парс).
- UI: в деталях шарда бэкапов (`/backups-storage/:c/:x`) — блок «Приёмники
  WAL»: таблица нода×state×ошибка×штамп; state-бейджи (streaming — зелёный,
  waiting_* — жёлтый, failed_permanent — красный).
- DTO: `BackupShardStorageDto` + `agents[]`.

### u12 — эвакуации
- Чтение: range `/pgworker/evacuations/` (новый запрос в pg-тик, образец
  существующих параллельных range). Модель `EvacuationInfo { Cluster, Shard,
  Plan..., UpdatedUnix }` (формат — канон arch/14; толерантный парсер,
  битый JSON → parseError).
- UI: блок «Эвакуации» во вкладке «Журнал» pg-кластера (план, карантин,
  возврат); событие в ленте u09. DTO: `ClusterDto` + `evacuations[]`.

### u13 — work-журналы kafka/valkey/s3
- Чтение: ranges `/kafkaworker/work/`, `/valkeyworker/work/`,
  `/s3worker/work/` в соответствующих доменных тиках (симметрично pg
  `WorkJournalParser`; формат — канон arch/16/21/24 §3.3-аналоги).
- Модели: `KafkaSnapshot.WorkJournals[]`, `ValkeySnapshot.WorkJournals[]`,
  `S3Snapshot.WorkJournals[]` (record по образцу `WorkJournalInfo` pg).
- UI: вкладка «Журнал» доменных страниц (§3.2) — блок «Что делает воркер»:
  op/phase/instance/updated/last_error/fail-серия. DTO: `KafkaClusterDto`/
  `ValkeyClusterDto`/`S3ClusterDto` + `work`.

### u14 — координация воркеров
- Чтение: ranges `/pgworker/{leader,claims/,instances/}`,
  `/kafkaworker/{leader,claims/,instances/}`, `/valkeyworker/{...}`,
  `/s3worker/{...}` — в соответствующих тиках. Модели:
  `WorkerCoordination { LeaderInstance | null, Claims[] { Cluster,
  Instance, SinceUnix? }, Instances[] { InstanceId, SinceUnix? } }`
  per домен (ключи существуют у всех воркеров: состав префиксов координации
  подтверждён arch/02 §11.1 для valkey «(leader, claims, work, portalloc,
  locks, instances)», pg — arch/14 §3.3; толерантный парсер: отсутствие ключей
  — пустая координация, не ошибка).
- UI (`/workers`): в карточке воркера — строка «Лидер: `<instance>`» и таблица
  claims «кто держит кластер» (кластер → инстанс); read-only, без действий.
- DTO: `WorkerViewDto` + `coordination`.

## 5. Параметры (u15–u17) + restore-заявка

### u15 — эффективный конфиг воркера (read-only)
- **Новый GET-эндпоинт API каждого воркера**: `GET /api/config` (mTLS-зона
  `/api`, read-only) возвращает эффективные параметры установки: pg —
  `Thresholds`, `Loops`, `Moves`, `Parallelism`, `Backups.Enabled`,
  `Snapshots` (Export: enabled/interval), `Pgtune`-профиль (входы: memory/
  connections/тип); kafka/valkey/s3 — свои `Thresholds`/`Loops`/
  `Parallelism` + флаги подсистем каждого (состав — фактические POCO
  `Options.cs` соответствующего воркера; значения — только небезопасные
  НЕ-секретные поля: секреты/env-креды НЕ отдаются никогда).
- Панельный прокси: `GET /api/workers/{worker}/effective-config` — on-demand
  запрос к живому инстансу (паттерн мутаций/healthz-поллера; без снапшота,
  без etcd). Живых нет → 503.
- UI (`/workers`): раскрывающийся блок «Эффективный конфиг» в карточке
  воркера: группы параметров с пометкой «env, применяется рестартом воркера»;
  read-only, форм ввода НЕТ (мутации — вне объёма, 06 §2.3 п.1–5 — отдельные
  решения владельца).
- Воркеры: pg/kafka/valkey/s3 — правки `src/{PgWorker,KafkaWorker,ValkeyWorker,
  S3Worker}.App` → docker-E2E PgWorker в мерж-гейте (критерии, §10).

### u16 — `garbage_min_age_sec` в форме policy
- Тело `PUT /api/clusters/{c}/backups/policy` (панель) и
  `UpdateBackupsPolicyRequestDto` дополняются полем `garbageMinAgeSec`
  (целое, секунды; границы валидации — сервер воркера, канон arch/19 §4;
  панель шлёт ПОЛНОЕ тело, включая текущее значение из policy-ключа/дефолта —
  канон замещения целиком 02 §9.12).
- UI: поле «Минимальный возраст мусора (сек)» в единой форме policy
  (существующий модал «Пул бэкапов» перерастает в форму «Policy бэкапов
  кластера»: retention + full_max_age_sec + verify + drill + garbage_min_age_sec
  + секция s3 — всё в одном модале, у своей сущности: кнопка на
  `/backups-storage` per-кластер и на вкладке «Бэкапы (срез)» pg-кластера).

### u17 — отмена живых заявок ротаций kafka/valkey
- Новые панельные эндпоинты (прокси в API воркера):
  - `DELETE /api/kafka/clusters/{c}/app-password/rotate` — снятие живой
    заявки `/kafkaworker/rotations/<C>`;
  - `DELETE /api/kafka/clusters/{c}/admin-password/rotate` —
    `/kafkaworker/admin_rotations/<C>`;
  - `DELETE /api/kafka/clusters/{c}/ca/rotate` — `/kafkaworker/ca_rotations/<C>`;
  - `DELETE /api/valkey/clusters/{c}/password/rotate` —
    `/valkeyworker/rotations/<C>` (живая заявка роли app|admin — ключ один);
  - `DELETE /api/valkey/clusters/{c}/ca/rotate` —
    `/valkeyworker/ca_rotations/<C>`.
- Семантика (канон для arch/02 §10/§11 и API воркеров): удаление ключа заявки
  воркером; **не останавливает начатую ротацию** — гвард: воркер удаляет ключ
  только если ротация ещё не начата (не взята в работу фазами A/B/C или
  P/D/R/C); начатая → 409 «ротация уже исполняется». Заявки нет → 404
  (идемпотентность повтора отсутствует осознанно, образец §9.7.5). Коды:
  204 / 404 / 409 / 503.
- UI: у бейджа «ротация…» в шапке доменной страницы kafka/valkey — кнопка
  «Отменить» (confirm: «начатая ротация доедет; отменяется только не начатая
  заявка»); 409/404 — текстом ProblemDetails.
- Воркеры Kafka/Valkey меняются → E2E в мерж-гейте (§10).

### Restore-заявка (из 06 §4.3 «Управление: Бэкапы»)
- Панельный прокси существующего эндпоинта воркера (02 §9-семейство):
  `POST /api/clusters/{c}/shards/{shard}/restore` — тело
  `{ confirm: "<shard>", targetTime?: RFC3339 }` (гварды и протокол —
  arch/19 §3.5/§4, воркер уже реализован: `RestoreShardHandler`,
  202/400/404/409/503).
- UI: кнопка «Восстановить из бэкапа» в деталях шарда бэкапов
  (`/backups-storage/:c/:x`) — модал: поле `targetTime` (опционально,
  RFC3339) + подтверждение вводом имени шарда (по образцу delete-сироты);
  предупреждение «операция разрушающая и долгая; шард восстанавливается из
  бэкапа».
- Обоснование включения — решение D18.

## 6. Алертинг и контракт (u18–u19)

### u18 — группировка алертов
- `AlertsPage`: группировка по Remedy-движителю (секции `WorkerAuto` /
  `OperatorApi` / `OperatorRunbook` — поле `remedy` уже в `AlertDto`) и
  фильтр по домену (клиентский, по kind-префиксу: `kafka-*`, `valkey-*`,
  `s3-*`, остальное — pg/etcd/бэкапы/воркеры = «pg и инфраструктура»).
- Вью «Что требует меня»: переключатель/фильтр «Требуют оператора» =
  remedy ∈ {OperatorApi, OperatorRunbook} — выносится также на дашборд
  (секция рядом с лентой алертов: список только операторских алертов).
- Серверных изменений нет (`GET /api/alerts` уже объединяет движки; severity/
  kind-фильтры сохраняются).

### u19 — синхронизация контракта с кодом
- **Маршрут ротации pg**: arch/03 §1/§1.6/§2 фиксирует
  `POST /api/clusters/{cluster}/app-password/rotate`; фактический код
  (панель `OperationsModule`, воркер `ApiModule`) — `POST /api/clusters/
  {cluster}/secrets/rotate` (ротация трёх ролей app/bucket_admin/bucket_mover,
  DTO `ClusterSecretsRotatedDto`). **Arch правится под код**: `app-password/
  rotate` → `secrets/rotate` (заголовок/§1.6/§2/§3 шапка кластера; ключ
  `/pgworker/rotations/<C>` — без изменений). Код не трогаем.
- **Каталог алертов**: `AlertEngine` pg — фактически 46 правил-файлов
  (перечень `src/AdminPanel.Core/Alerting/Rules/`), arch/03 §4 описывает
  меньше; docs/adminpanel/03 — «25 правил». Arch/03 §4 и docs/03
  актуализируются полным фактическим перечнем kind (сверка по коду правил:
  добавить, помимо описанных, `wal-chain-broken`/`wal-stream-lag`/
  `wal-stream-stopped`/`restore-failed`/`backup-chain-broken`/
  `backup-deleting-stuck`/`backup-drill-failed`/`backup-drill-stale`/
  `backup-orphan`/`backup-verify-failed`/`backup-wal-tli-gap`/
  `backup-storage-quota`/`backup-full-stale`/`worker-api-unreachable`/
  `worker-unhealthy`/`provision-stuck` и др. — точный список исполнитель
  снимает с кода; критерий — каждый `kind` кода присутствует в каталоге arch).
- Правки — тем же коммитом, что связанные UI-изменения (правило 06 §6).

## 7. Серверные изменения API панели (сводно)

| Изменение | Задача | Файл-якорь |
|---|---|---|
| `OverviewDto` + `ha`/`backups`/`reliability`/`operations`/`freshness` | u06–u08 | `src/AdminPanel.Api/Inspection/OverviewQuery.cs` |
| `BackupShardStorageDto` + `agents[]` | u11 | `BackupStorageQuery.cs` |
| `ClusterDto` + `evacuations[]` | u12 | `ClusterDetailsQuery.cs` |
| `KafkaClusterDto`/`ValkeyClusterDto`/`S3ClusterDto` + `work` | u13 | `KafkaQuery.cs`/`ValkeyQuery.cs`/`SQuery.cs` |
| `WorkerViewDto` + `coordination` | u14 | `WorkersModule.cs`/`WorkersQuery` |
| `GET /api/workers/{worker}/effective-config` (прокси) | u15 | `WorkersModule.cs` |
| policy-тело + `garbageMinAgeSec` | u16 | `BackupsCommands.cs` |
| `DELETE .../app-password/rotate`, `.../admin-password/rotate`, `.../ca/rotate` (kafka); `DELETE .../password/rotate`, `.../ca/rotate` (valkey) | u17 | `KafkaOperationsModule.cs`/`ValkeyOperationsModule.cs` |
| `POST /api/clusters/{c}/shards/{shard}/restore` (прокси) | §4.3 | `OperationsModule.cs` |
| Алерт `backups-disabled` | u10 | `AlertEngine` + правило |
| Снапшот: `BackupsDisabled`, `Evacuations`, `WalAgents`, kafka/valkey/s3 `WorkJournals`, `Coordination` per домен | u10–u14 | `SnapshotRefresher`/`SnapshotBuilder`, доменные refresher'ы + парсеры |

Новые эндпоинты API воркеров: `GET /api/config` × 4 (u15), DELETE-отмена
ротаций kafka/valkey (u17) — правки `src/PgWorker.App`, `src/KafkaWorker.App`,
`src/ValkeyWorker.App`, `src/S3Worker.App`.

Инварианты сохраняются: refresher — единственный писатель снапшота; транспортный
провал KV-чтения роняет тик (неполный снапшот хуже прежнего); неизвестные ключи —
`unknownKeys`, битый JSON — parseError-запись; mTLS-дискавери по lease-ключам;
`requested_by` — username сессии (аудит) на всех новых мутациях, создающих
заявку. **Исключение — DELETE-отмены**: отмена живых заявок ротаций (u17:
kafka app/admin/CA ×3, valkey пароль/CA ×2) и существующая отмена rebalance
(образец `CancelKafkaRebalanceCommandHandler`) НЕ создают аудит-записи
`requested_by` — DELETE идёт без тела, заявка не создаётся, воркер при del
ничего в etcd не пишет; аудит самой отмены — host-лог воркера.

## 8. Правки arch/ и docs (тем же коммитом, что код)

**`arch/adminpanel/02-etcd-contract.md`**:
- шапка: состав мутаций (+отмена ротаций kafka/valkey, +restore-прокси);
- §2.3.1 таблица: +`/pgworker/backups-disabled`, +`/pgworker/evacuations/
  <C>/<X>`, +`/pgworker/backups/<C>/<X>/agent/<N>`, +`/pgworker/leader` +
  `/pgworker/claims/` + `/pgworker/instances`;
- §2.3.2/§2.3.3/§2.3.4: читаемые ключи +`work/`, `leader`, `claims/`,
  `instances` соответствующего воркера;
- §3 модель снапшота: новые записи (`WalAgentInfo`, `EvacuationInfo`,
  `BackupsDisabled`, `WorkerCoordination`, work-журналы доменов);
- §9.12: тело policy +`garbage_min_age_sec`; +новый §9.13 restore-прокси;
- §10.2: +мутации 18–20 (отмена app/admin/CA-ротаций) — без `requested_by`
  (del без тела, аудит — host-лог воркера); §10.1: чтение work;
- §11.1/§11.2: чтение work/leader/claims/instances; +мутации 7–8 (отмена
  ротаций пароля/CA) — без `requested_by` (del без тела, аудит — host-лог
  воркера);
- §12.1/§12.2: чтение work/leader/claims/instances s3worker;
- +эффективный конфиг: правка описания дискавери/прокси (§2.3.x) и ссылка на
  контракт `GET /api/config` воркеров.

**`arch/adminpanel/03-panels.md`**:
- §1: таблица эндпоинтов (+`DELETE` ротаций, +restore, +
  `GET /api/workers/{worker}/effective-config`); §1.6: маршрут
  `secrets/rotate` под фактический код (u19);
- §2: DTO (`OverviewDto` новые поля, `agents`, `evacuations`, `work`,
  `coordination`, policy-поле);
- §3: дерево панелей — 5 групп навигации, вкладки доменных страниц по §3.2
  этой спеки, страница шарда, `/events`, «Репликация» (u01/u03);
- §3.7 Воркеры: +координация, +эффективный конфиг (read-only);
- §3.8: форма policy +`garbage_min_age_sec`, единый модал policy;
- §4/§7.4/§8.4/§12.4: каталог алертов — актуализация полного перечня (u19) +
  `backups-disabled`;
- §7.1/§8.1: +DELETE-эндпоинты отмены ротаций.

**`arch/14-pgworker.md`, `arch/16-kafkaworker.md`, `arch/21-valkeyworker.md`,
`arch/24-s3worker.md`** (§1.1 — контракты API): +`GET /api/config`
(эффективный конфиг, read-only, без секретов); kafka/16 и valkey/21 —
+DELETE-отмена ротаций (гварды «не начата», 204/404/409/503).

**`docs/adminpanel/06-ui-analysis.md`** (чек-лист самого документа):
- дерево §4.2 и таблица §5 — под фактическую структуру после переделки;
- §3.4 «не наблюдаемо из панели» — снять пункты, закрытые u10–u14;
- §6 — удалить пункты u01–u19 тем же мерж-коммитом (правила roadmap; u20
  остаётся с пометкой «после t32»).

**`docs/adminpanel/03-probes-alerts.md`**: «25 правил» → фактический перечень
(u19). **`docs/adminpanel/04-frontend.md`**: структура слоёв SPA не меняется
(новые модули `components/actions/`, `events/` — дописать в «Слои» одной
строкой каждый). **`docs/adminpanel/INDEX.md`**: без изменений (документы те
же).

## 9. Что НЕ делаем (границы)

- u20 (секция etcd-контура под etcdworker) — вне объёма (после roadmap t32).
- Метрики Prometheus/графики, WebSocket/SSE-пуш — не появляются.
- История событий с хранением (персистентность, воспроизведение после
  перезагрузки) — нет; лента живёт в открытой вкладке.
- Мутации env-параметров воркеров (Thresholds/Loops/PGTune/список §2.3
  06-документа) — НЕТ: u15 read-only; вынос каждого пункта §2.3 — отдельные
  arch-first решения.
- Новые мутации сверх перечисленных (правка стоящих заявок переездов, abort
  начатых ротаций, автоперебалансировка, патрон-управление) — нет.
- Аутентификация/роли/аудит — без изменений.
- Изменение алерт-движков (кроме нового kind `backups-disabled` и каталога):
  правила/пороги не трогаем.
- Формула раскладки бакетов, TTL, порт-тройки, тайминги etcd — не
  параметризуются.
- Backend-рефакторинги вне зоны задачи; budgets/таймауты существующие не
  меняются.

## 10. Критерии приёмки

Сборка/статика (хост разработчика):
1. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
   — 0 warnings (TreatWarningsAsErrors), 0 errors.
2. `cd frontend && npm ci && npm run typecheck && npm run build` — зелёные;
   бандл ложится в `src/AdminPanel.Api/wwwroot`.
3. Юниты панели (без docker):
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build`
   — Passed. Обязательные новые тесты: `OverviewMapper` (ha/backups/
   reliability/operations/freshness — те же входы, что давала TS-агрегация),
   парсеры `backups-disabled`/`agent/<N>`/`evacuations`/`{leader,claims,
   instances}`/work-kafka/valkey/s3 (реальные фрагменты etcd-значений по
   канону 02 §8, вкл. вырожденные), правило `backups-disabled`,
   DTO-мапперы новых полей, валидация policy-тела.
4. Юниты воркеров (правки u15/u17): соответствующие
   `src/tests/{KafkaWorker,ValkeyWorker}.UnitTests` — зелёные.
5. Мерж-гейт (трогаем `src/PgWorker.App`/`KafkaWorker.App`/`ValkeyWorker.App`/
   `S3Worker.App`): docker-E2E PgWorker на свежем Release по канону AGENTS
   (двумя отдельными командами: build, затем `dotnet test src/tests/
   PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_
   AddEmptyShard` после `dotnet run --project src/tests/PgWorker.E2ePrepare`);
   для KafkaWorker/ValkeyWorker — их E2E-серии по тому же канону. Зачистка
   контейнеров/сетей после каждой серии.
6. Dev-стенд: `dev-stand/adminpanel/checks/00-up.sh` поднимается, e2e-чеки
   панели зелёные; панель работает в докере (:5050, cookie admin/admin).

Функциональные проверки пользователя («могу ввести параметры X через форму Y»;
проверяется на dev-стенде вручную по этому списку):
7. Навигация: 5 групп; все существующие URL открываются; `/ha/:scope`
   matched-скопа редиректит на страницу шарда; unmatched — страница деталей.
8. PG: создать кластер / добавить шард / убрать шард / перенести бакеты /
   откатить / финализировать / отменить переезд / снять заявку / переключить
   strict / сменить app-пароль (вкладка «Секреты») — формы на месте, работают.
9. Страница шарда: HA-блок (leader/members/optime/raw config) + «Пересоздать
   ноду» (soft/hard) доступны на `/clusters/:c/shards/:x`.
10. Kafka: все 17 мутаций доступны формами; у живой заявки ротации
    (app/admin/CA) — кнопка «Отменить» (u17); отмена не начатой — 204,
    начатой — 409 текстом.
11. Valkey: 6 мутаций + отмена ротаций пароля/CA (u17).
12. S3: 10 мутаций формами; вкладка «Журнал» показывает work s3worker.
13. Бэкапы: форма policy кластера принимает `garbage_min_age_sec` (u16);
    кнопка «Восстановить из бэкапа» в деталях шарда бэкапов ставит
    restore-заявку (подтверждение вводом имени шарда); сироты — hold/unhold/
    delete как раньше.
14. Дашборд: карточка Valkey; блок «Активные операции» (переезды/ротации/
    reassignment/drill/restore — по доменам); серверные поля HA/бэкапов/
    надёжности (network-вкладка дашборда делает ОДИН запрос `/api/overview`);
    5 бейджей свежести в шапке.
15. `/events`: при выполнении переезда/ротации в ленте появляются события
    (появление заявки, переходы состояний бакета, исход); после перезагрузки
    страницы лента пуста (истории нет).
16. Наблюдаемость: при ключе `/pgworker/backups-disabled` — баннер + алерт;
    приёмники WAL видны в деталях шарда бэкапов; эвакуации — во вкладке
    «Журнал» кластера; `/workers` показывает лидера и «кто держит кластер»;
    «Эффективный конфиг» раскрывается (пометки «применяется рестартом»).
17. Алерты: группировка по движителю, фильтр домена, вью «что требует меня»;
    клик по алерту ведёт на сущность (выборочная проверка маппинга kind→route
    по всем доменам).
18. Arch/docs: правки раздела 8 в том же коммите; `06-ui-analysis.md` §4.2/§5/
    §3.4/§6 обновлены; маршрут `secrets/rotate` в arch соответствует коду;
    каталог алертов полон (сверка по kind кода).

## 11. Принятые решения (все развилки закрыты; вопросы запрещены режимом)

- **D1. Сохранение URL**. Существующие маршруты не меняются (перегруппировка
  только меню): у стенда/закладок ничего не ломается, редиректы не нужны.
  Новые: `clusters/:c/shards/:x`, `/events`. Основание: 06 §5 «перегруппировка
  без новых страниц» — не меняет адресов.
- **D2. Язык интерфейса — русский** (§4.4 06: «единообразие»). Основание:
  контент уже русский по канону (Hint/Remedy — «строка, русский», arch/03
  §4.1), модалы в основном русские; нейминг доменов/technical kind/state —
  английские идентификаторы без перевода (моноширинно). Переводятся подписи
  навигации, заголовки, кнопки, пустые состояния.
- **D3. `/ha` остаётся глобальным срезом «Репликация»** (не удаляется):
  06 §4.2 прямо включает «Репликация /ha — срез всех HA-скопов (без лидера,
  лаги) — вход в шард». Детали matched-скопа — редирект на страницу шарда;
  unmatched-скопы (чужой service в общем etcd — норма, arch/02 §7) остаются
  адресуемыми на `/ha/:scope`.
- **D4. Ротации pg — вкладка «Секреты»** (06 §4.2: «Секреты — ротации
  per-cluster (сейчас в шапке)»): кнопка «Сменить app-пароль» переносится из
  шапки кластера во вкладку; шапка остаётся без ротаций. Strict-переключатель
  остаётся в шапке (это режим кластера, не секреты).
- **D5. Heals — во вкладку «Журнал»** (06 §4.2: журнал = work + heals +
  эвакуации); отдельная вкладка Heals исчезает. «Стендовая топология»
  остаётся блоком на странице (в дереве §4.2 её нет — сохраняем статус-кво,
  не выбрасываем функцию).
- **D6. u06 — агрегация ровно той же логикой, что в TS сейчас** (active-only
  worst RPO, «самый недавний закрытый failover» и т.д. — перенос 1:1 в
  `OverviewMapper`): поведение дашборда не меняется, меняется исполнитель.
  Клиентские доборы `/api/ha`, `/api/backups/storage`, `/api/reliability` с
  дашборда удаляются; сами эндпоинты остаются (страницы-потребители есть).
- **D7. Свежесть = BuiltAtUtc per снапшот** (не «каждый refresher отдельно»):
  4 домена имеют по одному снапшоту с `BuiltAtUtc`, etcd — `lastRefreshUtc`;
  этого достаточно (грабля «свежесть ≠ одно число» закрывается пятью
  независимыми полями). Порог stale — тот же мультипликатор 3×RefreshInterval
  (`SnapshotStaleRule.Multiplier`).
- **D8. u09 — события из query-кэша целиком**: диффы считаются по полным
  ответам опрашиваемых списков (overview/clusters/ha/kafka/valkey/s3), а не
  по отдельным сущностям — проще и покрывает все переходы; кап 500 событий;
  без persistence (06: «живёт с открытой вкладкой»).
- **D9. u10 — расхождение 06 §3.4 п.5 с кодом**: ключ
  `/pgworker/backups-disabled` НЕ входит в существующий range
  `/pgworker/backups/` (байтовое сравнение `-` < `/`), т.е. фактически панель
  его не читает; в spec закладываем явное точечное чтение. Документ
  корректируется при правке §3.4 (раздел 8).
- **D10. u11 — agent-ключи уже в читаемом range** `/pgworker/backups/`
  (`/pgworker/backups/<C>/<X>/agent/<N>` — под префиксом), нужен только
  парсер и модель; нового range-запроса нет.
- **D11. Координация (u14) — для всех 4 воркеров** симметрично (панель
  единая; состав ключей координации у воркеров одинаков — leader/claims/
  instances подтверждены arch/02 §11.1 (valkey перечислены явно), arch/14
  §3.3 (pg), у kafka/s3 — те же семейства, парсер толерантен к отсутствию).
- **D12. u15 — on-demand прокси, без снапшота**: эффективный конфиг не
  кладётся в снапшот (редко нужен, объём безразличен для снапшота, но паттерн
  on-demand уже есть — мутации/healthz; не плодим второй механизм). Секреты
  (креды, PEM) в ответе `GET /api/config` отсутствуют всегда.
- **D13. u17 — гвард «не начата» на воркере** (409 на начатую): остановить
  начатую ротацию безопасно нельзя (окна двойного доверия/rolling уже идут) —
  семантика «отмена только не начатой заявки», симметрия §9.7.5 (удаление
  заявки ≠ остановка исполнения). Воркер — единственный писатель своих
  префиксов: del выполняет воркер, панель только проксирует.
- **D14. u16 — единый модал «Policy бэкапов кластера»** (retention+full_max_
  age+verify+drill+garbage_min_age_sec+s3 одним телом) вместо двухразрывного
  UX (сейчас policy-поля размазаны): канон «замещение целиком» 02 §9.12 этого
  требует технически; кнопка остаётся у кластера в `/backups-storage` и
  дублируется на вкладке «Бэкапы (срез)».
- **D15. u18 — домен алерта по kind-префиксу** (kafka-/valkey-/s3- → свой
  домен; прочее → «pg и инфраструктура»): kind-префиксы уже namespaces по
  движкам, отдельных полей домена в AlertDto не добавляем.
- **D16. u19 — arch под код, не код под arch** (по условию задачи):
  `secrets/rotate` — фактический маршрут в коде панели и воркера, семантика
  шире app-пароля (3 роли); переименование эндпоинта задело бы работающий
  контракт без нужды.
- **D17. Валидационные константы — из arch в один TS-модуль**: границы уже
  канонизированы в 02 §9.3/§10.3/§11.3/§12.3; дублирование в модалях
  заменяется импортом; источник истины остаётся сервер (ProblemDetails по
  полям), TS-модуль — UX-зеркало.
- **D18. Restore-заявка включена в объём**: 06 §4.3 (целевая модель, колонка
  «Управление» секции Бэкапы) прямо содержит «restore-заявка»; мутация
  существует в API воркера (не выдумывается); требование пользователя «мог
  вводить параметры где можно» — restore имеет параметры (targetTime,
  подтверждение). Панель получает зеркальный прокси — arch-first (раздел 8).
- **D19. `GET /api/config` — единое имя на 4 воркеров** (разный состав тела):
  без версионирования (контракт панели и воркеров развёртывается одним
  репозиторием/артефактом — 03 §9).
- **D20. E2E-гейт задачи**: правки кода воркеров (u15/u17) автоматически
  включают docker-E2E серию PgWorker (мминимум `Scale_AddEmptyShard`) и
  серии Kafka/Valkey по канону AGENTS — включено в критерии приёмки, а не
  «по желанию».

## 12. Риски и грабли (из 06 и кода; исполнитель учитывает)

- Пять refresher'ов: свежесть per-домен не складывается в одно число — UI
  показывает 5 независимых бейджей (u08).
- `sharded` — вычисляемое поле (1×1 = нешардированный): вкладка «Бакеты»
  скрывается по нему (сохранить поведение).
- NOT_INITIALIZED/TO_REMOVE — штатная тишина: серый цвет, без алерт-шума
  (движок уже подавляет — не «чинить»).
- Журналы перезаписываются: вкладки «Журнал» показывают последнюю фазу;
  история — только события u09 из диффов.
- Транспортный провал любого нового KV-чтения роняет тик домена (инвариант
  02 §4) — новые ranges добавляются в существующий набор того же тика.
- Новые парсеры — толерантные: битый JSON → parseError-запись, ключ не
  трогаем (домен воркера); неизвестные ключи — `unknownKeys`.
- TS7/css-импорт, `.npmrc`, wwwroot вне git — грабли docs/04-frontend
  действуют.
- Порядок работ: arch-правки → код воркеров (`GET /api/config`, DELETE
  ротаций) → панель (снапшот/DTO/эндпоинты) → SPA (навигация/шаблон/
  компоненты/страницы/события) → тесты/чеки → E2E.
