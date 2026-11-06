# Spec: перенос DR-бэкапа etcd в узлы контура — компонент EtcdNode

Дата: 2026-11-06. Ветка: `feat-etcdnode-backup`.
Канон (обновлён этой задачей, arch-first): `arch/etcdnode/` — новый
компонент; `arch/etcdworker/` — функция DR-слепков демонтирована.

## 1. Цель

1. Функцию бэкапа etcd (DR-слепки + выгрузка в S3 + обслуживание контура)
   ВЫНЕСТИ из EtcdWorker в САМИ УЗЛЫ etcd-кластера: новый компонент
   **EtcdNode** — агент внутри каждого managed-узла контура. Слепок снимает
   и выгружает узел; EtcdWorker бэкапить перестаёт (функция демонтируется
   полностью, дублей не остаётся).
2. Триггер внеочередного слепка учитывает ТОЛЬКО важные изменения:
   временные метки работающих сервисов (пульсы Patroni-DCS `/service/*`,
   мастер-ключи HA `/clusters/<C>/shards/<X>/master` — перезаписываются
   каждые 1–2 с и сейчас непрерывно растят revision) НЕ триггерят бэкап.
   Обоснование (решение пользователя): если из бэкапа пришлось
   подниматься — эти метки не актуальны, сервисы перепишут их первыми
   тактами; восстанавливаться можно и без них.

Дополнительный эффект: бэкап живёт, пока жив контур, — не зависит от
доступности инстансов EtcdWorker (сегодня «обе инстанции лежат» = выгрузка
молчит).

## 2. Принципы

- **Arch-first**: канон уже обновлён (этой же задачей) —
  `arch/etcdnode/{README,00-etcdnode-contract,01-agent,02-snapshots,
  03-restore,processes/c-snapshot-export}.md`; `arch/etcdworker/04-snapshots.md`
  и `arch/etcdworker/processes/c-snapshot-export.md` удалены; ссылки во
  всех затронутых документах переключены. Код зеркалит канон; при
  расхождении правится СНАЧАЛА канон.
- **Перенос механики 1:1**: конвейер выгрузки (резолв цели → put .db → put
  .meta.json → ретенция пар → статус-ключ с семантикой покрытия → CatchUp)
  переезжает из `EtcdWorker.*` в новые проекты без изменения форматов
  объектов S3 (`backups/06-s3-layout.md` не меняется) и статуса.
- **Единственный писатель домена**: `/etcdnode/` пишут только агенты;
  `/etcdworker/` остаётся доменом воркера (ключ `/etcdworker/snapshots`
  упразднён).
- **Идемпотентность и takeover** — канонические: lease-клэймы TTL 15 с,
  повтор тика перепроверяет факт, любое состояние переживает рестарт.
- Надёжность: максимально автономное решение (не требует вмешательства);
  multi-host-инварианты соблюдены (том слепков узла — кэш, не гарантия).

## 3. Структура / компоненты

### 3.1. Новые проекты

- `src/EtcdNode.App` — агент: `Program.cs` (хост, fail-fast валидация env),
  `Options.cs` (привязка `ETCDNODE_SNAPSHOTS__*`, `ETCDNODE_S3_CA_PEM`),
  `Loops/AgentLoop.cs` (единственный BackgroundService: keepalive ключей →
  лидерство → тик C0–C5). Метрики — Meter `EtcdNode`, словарь
  `worker_*` (канон `arch/metrics/02-dictionary.md` §2; портов нет —
  метрики в процесс, наблюдаемость через ключи и логи).
- `src/EtcdNode.Core` — перенос из EtcdWorker с переименованием
  namespace: `SnapshotExporter` (из
  `EtcdWorker.Provisioning/SnapshotExporter.cs`; статус-ключ →
  `/etcdnode/snapshots`; `instance` в мете — имя узла), `EtcdBackupTargetResolver`,
  `EtcdSnapshotS3`+`IEtcdSnapshotS3`+`EtcdS3Object`, `BackupCaPem`
  (файл-ридер PEM на хосте ВОРКЕРА — содержимое уходит в env узла),
  `S3CaHttpClientFactory`, `EtcdExportRetention`,
  `EtcdSnapshotMeta`+`EtcdSnapshotMetaJson`, `EtcdSnapshotStatus`+
  `EtcdSnapshotStatusJson`; НОВОЕ: `SnapshotSchedule` (решение снятия с
  фильтром важности — §3.4) и `PulseKeys` (канонический список пульсов).
- Оба проекта в `src/PgWorker.slnx`. Используют `Shared.Etcd`
  (`EtcdGateway`, `Maintenance.SnapshotJob`: СНЯТИЕ — SnapshotJob с ровно
  одним endpoint `http://localhost:2379` (слепок только своего узла);
  ОБСЛУЖИВАНИЕ (compact+defrag) — отдельный вызов SnapshotJob с полным
  списком endpoints тика: localhost первым + члены из member list — канон
  `arch/etcdnode/02-snapshots.md` §5, решение №9) и
  `Shared.Etcd.Coordination.ClaimStore` (keyPrefix `/etcdnode`,
  endpoints — ТОЛЬКО `http://localhost:2379` (координация localhost-only,
  `arch/etcdnode/01-agent.md` §2: агент и etcd умирают вместе, отказ
  своего etcd = lease гаснет по TTL ≤ 15 с, лидерство уходит живому узлу),
  instanceId = имя узла `etcdw-node-<k>` из env, `advertiseApiUrl: null` —
  api-ключ не ставится; instanceof-ключ `/etcdnode/instances/<k>` и
  leader-клэйм `/etcdnode/leader` получаются штатно). Доработка ClaimStore
  ограничена одной точкой (источник — канон
  `arch/etcdnode/00-etcdnode-contract.md` §1): необязательный параметр
  конструктора `instanceId` (ключ живости несёт имя узла, дефолт GUID —
  существующие call-сайты не меняются). F-релиз лидерства (ветка F автомата
  C) НЕ вводится: при мёртвом localhost клэйм недоступен для записи и
  гаснет по TTL сам (`arch/etcdnode/processes/c-snapshot-export.md` F).

### 3.2. Образ узла

- `docker/etcdnode/Dockerfile` (+ `supervisord.conf`, `entrypoint.sh`):
  база — `quay.io/coreos/etcd:v3.5.21` (через локальный registry
  `192.168.0.1:5000/...`), `apt-get install supervisor` (прецедент
  `docker/node/Dockerfile`); self-contained publish `src/EtcdNode.App` НА
  ХОСТЕ → `COPY` артефактов (канон: в контейнер — только publish-вывод);
  `CMD` — entrypoint → supervisord; программы: `etcd` (команда
  `etcd $ETCD_ARGS`), `etcdnode` (агент), обе `autorestart=true`,
  `stopasgroup/killasgroup` для etcd-ребёнка не нужны (etcd — не ребёнок
  агента, оба дети supervisord).
- Тег `pgworker/etcdnode:dev`; в registry НЕ класть (правило локальных
  образов). Сборка — двумя шагами (publish → docker build), вывод с
  `[PHASE]`-меткой при вызове из утилит подготовки.

### 3.3. Демонтаж из EtcdWorker

- УДАЛИТЬ: `EtcdWorker.App/Loops/SnapshotLoop.cs`;
  `EtcdWorker.Provisioning/SnapshotExporter.cs`;
  `EtcdWorker.Provisioning/EtcdBackupTargetResolver.cs`;
  `EtcdWorker.Core/{EtcdSnapshotS3,EtcdS3Object,SnapshotSchedule,BackupCaPem,S3CaHttpClientFactory}.cs`;
  `EtcdWorker.Etcd/{EtcdExportRetention,EtcdSnapshotMeta,EtcdSnapshotStatus}.cs`
  (перенесены в `EtcdNode.*`).
- ПРАВИТЬ:
  - `Options.cs`: секция `Snapshots` → `NodeSnapshot` (те же поля, БЕЗ
    `Dir` — том теперь в узле; `S3.CaBundlePath` остаётся — источник PEM);
    `Docker.Images{Etcd}` → `Images{EtcdNode="pgworker/etcdnode:dev"}`.
  - `Program.cs`: валидация `NodeSnapshot` (те же пороги, что были у
    `Snapshots`); сборка DI без экспортёра/цикла слепков; CA-PEM
    (`BackupCaPem`) читается для передачи в env узлов.
  - `EtcdNodeSpecBuilder.Build`: флаги etcd — те же, но возвращаются как
    env `ETCD_ARGS` (одна строка, shell-слова через пробел; флаги без
    shell-метасимволов — экранирование не требуется, имена/URL состоят из
    `[a-zA-Z0-9:/.,-]`); Cmd контейнера НЕ передаётся (CMD образа —
    supervisord); добавить env агента
    `ETCDNODE_NODE_NAME=etcdw-node-<k>` (имя узла — ключ живости
    `instanceId` и `.meta.json.instance`) и
    `ETCDNODE_SNAPSHOTS__{INTERVAL_MIN,MIN_INTERVAL_SEC,RETRY_INTERVAL_SEC,TIMEOUT_SEC,RETENTION_FILES,RETENTION_OBJECTS,MAINTENANCE_INTERVAL_MIN}`
    (значения из `NodeSnapshot`) и `ETCDNODE_S3_CA_PEM` (содержимое,
    опционально). Канал доставки `NodeSnapshot`→env — расширение record
    `EtcdProvisioningOptions` (Core-тип, единственный канал из Program в
    MembershipProcess/NodeSupervisor; App-тип `EtcdWorkerOptions` в
    Core-процессах недоступен), семантика сохранена: значения — из секции
    `EtcdWorker:NodeSnapshot`; сигнатура Build:
    `(int k, EtcdProvisioningOptions options, int clientHostPort,
    string? initialCluster = null)`.
  - `EtcdNodeDriver` (Plain+Swarm): второй volume
    `etcdw-node-<k>-snapshots` → `/snapshots` (EnsureVolume при создании,
    DeleteVolume при removeVolume, листинг сирот включает суффикс
    `-snapshots` рядом с `-data`); label `etcdworker=member` без изменений.
  - `HealthChecks/`+`HealthState`: секция `snapshot-export` и метки
    `MarkSnapshot*` — удалить; `EtcdWorkerLoopsVitality` — цикл snapshot
    убрать.
  - `Api/Operations/EffectiveConfig.cs`: группа `snapshots` →
    `nodeSnapshot`, `images{etcd}` → `images{etcdNode}` (см.
    `arch/etcdworker/05-api.md` §2).
  - `deploy/docker-compose.yml` воркера: монтирование тома `/snapshots`
    (если есть) убрать.
- СБОРКА: `TreatWarningsAsErrors=true` — после удаления не должно остаться
  ни одной ссылки на перенесённые типы.

### 3.4. Логика фильтра важности изменений (ядро новой семантики)

`SnapshotSchedule.ShouldTake` (EtcdNode.Core), входы: `nowUnix`,
`lastTakeUnix` (локальный том своего узла), `lastUploadedUnix` (статус),
`currentRevision`, `baselineRevision` (revision эталонной карты, `null`
если карты нет), `diffImportant` (bool — есть ли изменения вне пульсов;
вычислен по range-диффу), `intervalMin`, `minIntervalSec`:

1. Плановый: `lastTakeUnix` нет или ≥ `intervalMin` → true.
2. Внеочередной: важное изменение (`baselineRevision == null` — карта
   отсутствует → важно; иначе `diffImportant`) И выгрузка старше
   `minIntervalSec` (никогда не выгружали → true).

`diffImportant`: полный `Range("/")` ОДНИМ запросом
(`IEtcdGateway.RangeAsync` — без limit/курсора; контроль-плейн установки
компактен, канон `arch/etcdnode/02-snapshots.md` §3) → map
`key → mod_revision`; сравнение с эталонной картой (в памяти лидера,
сохраняется после каждой УСПЕШНОЙ выгрузки — тот же range-ответ, что
использован для диффа): появился ключ вне PulseKeys, исчез ключ вне
PulseKeys, изменился `mod_revision` ключа вне PulseKeys → important.
`PulseKeys` (канон `arch/etcdnode/02-snapshots.md` §3.1): префикс
`/service/`; паттерн `/clusters/*/shards/*/master` (разбор сегментов
пути: 5 сегментов, последний `master`). Всё прочее — важное. revision НЕ
вырос → дифф не выполняется (дешёвый гейт тика).

Смена лидера/рестарт агента: карты нет → первый тик внеочередной
(снимает сразу — RPO-консервативно).

### 3.5. Панель

- `AdminPanel.Etcd/SnapshotRefresher.cs`: константа `EtcdSnapshots` →
  `/etcdnode/snapshots`; `SnapshotBuilder.cs`, `Parsing/EtcdSnapshotsParser.cs`
  — комментарии/канон-ссылки. Читатель точечный (один ключ) — префиксных
  списков не касается. Модель `EtcdSnapshotExportInfo`, алерты
  `etcd-snapshot-export-failed`/`-stale` и UI — без изменений (формат тот
  же).

### 3.6. Тесты и подготовка статики

- ЮНИТЫ (новый `src/tests/EtcdNode.UnitTests`): `SnapshotSchedule`
  (плановый/внеочередной/тишина/нет карты), дифф-фильтр `PulseKeys`
  (пульс-перезапись ≠ триггер; смена значения мастер-ключа = триггер;
  новый ключ = триггер; исчезновение = триггер), ретенция пар, статус
  OK/FAILED (перенос существующих юнитов из `EtcdWorker.UnitTests` для
  перенесённых типов + новые по фильтру). Комментарии тестов — AAA.
- ИНТЕГРАЦИИ (новый `src/tests/EtcdNode.IntegrationTests`): перенос
  механики `EtcdWorker.IntegrationTests/Snapshots/SnapshotExportTests.cs`
  (экспортёр/ретенция/CatchUp/статус на живом etcd + ownS3-нода цели,
  посев `/s3/clusters/` с `etcd_backup=true` — окружение по паттерну
  `EtcdWorkerEnvironment`, но контур поднимается ИЗ ОБРАЗА
  `pgworker/etcdnode:dev`); кейс «пульс не триггерит / важное
  триггерит» на живом контуре (пишем `/service/x/leader` в цикле —
  слепка нет; пишем `/clusters/shop/shards/s1/routing` — слепок снят);
  кейс лидерства (два агента в контуре — статус пишет один).
  Динамические порты (`assignRandomHostPort: true`), `BrokerBootSec` ≤
  100 с, полный teardown + ассерт чистоты (канон
  `arch/testing/02-e2e-isolation.md`).
- `EtcdWorker.IntegrationTests`: фикстура проверяет наличие образа
  `pgworker/etcdnode:dev` и fail-fast с подсказкой «статика не
  подготовлена» (membership-тесты создают managed-узлы из этого образа);
  Snapshots-подкаталог удаляется (перенесён).
- ПОДГОТОВКА: `src/tests/PgWorker.E2ePrepare` расширяется сборкой образа
  `pgworker/etcdnode:dev` (publish `src/EtcdNode.App` self-contained →
  docker build `docker/etcdnode/Dockerfile`) + тем же штампом готовности
  (прецедент pgworker-* образов; утилита вызывается `run-all-tests.sh`
  до всех серий — правка скрипта не нужна).
- E2E PgWorker не затронуты (не поднимают managed-узлы EtcdWorker).

## 4. Фазы

Фаза A — код агента: проекты EtcdNode.App/Core (перенос механики 1:1 +
фильтр §3.4 + ClaimStore-интеграция), юниты, Dockerfile-набор.
Фаза B — демонтаж воркера: §3.3 (spec-builder → ETCD_ARGS+env, второй
volume, конфиг NodeSnapshot, healthz, api/config), правки панели §3.5.
Фаза C — тестовая статика и интеграции: E2ePrepare-сборка образа,
EtcdNode.IntegrationTests, адаптация фикстуры EtcdWorker.
Фаза D — прогон: `scripts/run-all-tests.sh` (полный, одной командой);
зачистка серий по канону между фазами локальной отладки.

Каждая фаза заканчивается `dotnet build src/PgWorker.slnx -c Release`
без warning-ошибок (`TreatWarningsAsErrors`).

## 5. Ограничения

- P/Invoke запрещён; только управляемый .NET API.
- Бюджеты/таймауты существующие НЕ меняются: `IntervalMin=360`,
  `MinIntervalSec=300`, `RetryIntervalSec=300`, `TimeoutSec=30`,
  `RetentionFiles=10`, `RetentionObjects=28`,
  `MaintenanceIntervalMin=60`, lease TTL 15 с — значения прежние.
- Формат объектов S3 и статус-ключа НЕ меняются (кроме пути
  статус-ключа); `.meta.json.instance` теперь имя узла.
- External-контуры (стенд `as-etcd-*`, seed) слепков не имеют; воркер не
  возвращает fallback-механику.
- Образ узла собирается локально, в registry не кладётся; исходники в
  образ не копируются (publish-артефакты only).
- Observability агента: без новых публикуемых портов (наружу узла —
  только client-порт 2379).

## 6. Принятые решения (закрытые развилки)

| # | Развилка | Решение | Источник |
|---|---|---|---|
| 1 | Форма EtcdNode: один контейнер (кастомный образ etcd+агент под supervisord) vs пара контейнеров (sidecar) | ОДИН контейнер, кастомный образ `pgworker/etcdnode`; прецедент `pgworker-node`; обновление агента = пересоздание узла (repair/замена строит spec из текущего конфига) | пользователь |
| 2 | Слепки external-контуров (стенд, seed) | НЕ снимаются: DR — только managed-контур; стенд — дев-контур; из воркера функция демонтирована полностью (без fallback) | пользователь |
| 3 | Механика фильтра пульсов: канон-список пульсирующих ключей vs сравнение значений vs whitelist | Канон-список пульсов (`/service/`, `/clusters/*/shards/*/master`) + дифф полной карты `key→mod_revision` с эталоном выгрузки; нет эталона — важно (снимаем). Список предсказуем, пульс с меняющимся значением (таймстамп) не даёт ложный триггер, новый неизвестный ключ автоматически защищён — самое надёжное | пользователь |
| 4 | Куда писать статус DR-выгрузки | Новый домен `/etcdnode/snapshots` (писатель EtcdNode); `/etcdworker/snapshots` упразднён; панель переключена на новый ключ, формат/алерты без изменений | пользователь |
| 5 | Кто из агентов исполняет работу | Lease-лидер `/etcdnode/leader` TTL 15 c (ClaimStore, keyPrefix `/etcdnode`); слепок лидер снимает СО СВОЕГО `localhost:2379`; свой etcd недоступен → тик работу не исполняет, статус НЕ пишется, лидерство НЕ релизится — lease гаснет по TTL ≤ 15 с, работу и статус пишет новый лидер (экспортёр несёт полный список endpoints тика) | канон координации + решение №1 (один контейнер: агент и etcd умирают вместе) |
| 6 | Список endpoints агента | Не конфигурируется: `localhost:2379` + client-грани членов из member list (имена сети) на каждом тике; снятие — только localhost | канон адресации контура |
| 7 | Том слепков агента | Отдельный volume `etcdw-node-<k>-snapshots` (`/snapshots`), переживает пересоздание контейнера, сносится вместе с узлом; кэш доводки per-узел | канон «кэш без durability» |
| 8 | Доставка конфигурации агента | env контейнера узла из `EtcdWorker:NodeSnapshot` (spec-builder); CA-бандл — СОДЕРЖИМОЕ PEM в env `ETCDNODE_S3_CA_PEM` (воркер читает файл на своём хосте через BackupCaPem); смена значений подхватывается пересозданием узла (repair/замена из текущего конфига) | однозначно из механики spec-builder |
| 9 | Обслуживание контура (compact+defrag) | Уезжает в EtcdNode (тот же лидерный цикл, по членам из member list) — единый автомат C | из формулировки задачи (функция целиком) |
| 10 | Наблюдаемость агента | Ключи домена (живость instances, статус) + docker-логи; портов /healthz и /metrics агент не публикует | минимизация поверхности узла |
| 11 | Подготовка тестового образа | Расширение `PgWorker.E2ePrepare` (вызывается run-all-tests ДО серий) — без правки скрипта | прецедент подготовки статики |
| 12 | Ключ живости агентов | `/etcdnode/instances/<k>` (instanceId = имя узла) через ClaimStore; api-ключ не ставится; доработка ClaimStore — ТОЛЬКО необязательный параметр конструктора `instanceId` (источник — канон arch/etcdnode/00-etcdnode-contract.md §1); F-релиз лидерства НЕ вводится — координация localhost-only, клэйм гаснет по TTL | переиспользование ClaimStore, доработка минимальна |
| 13 | SnapshotJob | Переиспользуется из `Shared.Etcd`; агент передаёт единственный endpoint `http://localhost:2379` (без failover-списка — снимает только свой узел) | из №5 |

## 7. Критерии приёмки

1. Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c
   Release --nologo` — успех, 0 warnings (warnings-as-errors).
2. Демонтаж подтверждён: в `src/EtcdWorker.*` нет SnapshotLoop/
   SnapshotExporter/резолвера/S3-клиента слепков; воркер не пишет
   `/etcdworker/snapshots`; healthz без секции snapshot-export.
3. Агент: на живом контуре из образа `pgworker/etcdnode:dev` агент
   становится лидером, снимает слепок с localhost, выгружает пару
   db+meta в bucket `etcd-backups`, пишет `/etcdnode/snapshots` OK;
   CatchUp закрывает FAILED-транзиент; ретенция пар соблюдается.
4. Фильтр: непрерывная перезапись `/service/x/leader` и
   `/clusters/c/shards/s/master` НЕ порождает новых слепков сверх
   планового интервала (интеграционный кейс); появление/изменение
   прочего ключа — внеочередной слепок ≥ MinIntervalSec после предыдущей
   выгрузки.
5. Managed-узлы воркера создаются из образа агента: membership-тесты
   зелёные, узлы стартуют (supervisord: etcd + агент), флаги etcd те же
   (сверка `ETCD_ARGS`), оба volume создаются и сноятся с узлом.
6. Панель: карточка/алерты читают `/etcdnode/snapshots`; статус FAILED
   зажигает `etcd-snapshot-export-failed` (юнит-проверка парсера).
7. Полный прогон `scripts/run-all-tests.sh` зелёный (включая новые
   EtcdNode.UnitTests/EtcdNode.IntegrationTests и адаптированные
   EtcdWorker.*); между сериями локальной отладки — зачистка по канону.
8. arch/ согласован: ни одной ссылки на удалённые
   `etcdworker/04-snapshots.md`/`c-snapshot-export.md`; новые документы
   `arch/etcdnode/` соответствуют реализации.

## 8. Границы задачи (не делается)

- Восстановление из слепка не автоматизируется (runbook
  `arch/etcdnode/03-restore.md` — операторский).
- Стенд/seed на новый образ не переводятся.
- Механика фильтра не конфигурируется (канон-список пульсов — в коде и
  каноне; изменение списка — отдельная задача правкой канона).
- Метрики/health-грань агента не вводятся.
