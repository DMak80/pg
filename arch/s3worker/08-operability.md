# S3Worker: эксплуатация

Уровень 3. Функция «эксплуатация»: классификация тика и владение клэймом,
надёжность, наблюдаемость, конфигурация и риски. Процессы —
[A](processes/a-provisioning.md)…[H](processes/h-credentials-rotator.md);
перечисления состояний — [processes/enums.md](processes/enums.md); общие
принципы — [synchronization/README.md](../synchronization/README.md).

## 1. Тик и порядок веток

**Состояния инстанса** (`nodes/<I>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт) → `RUNNING` (health-грань
отвечает); `UNREACHABLE` (молчит дольше `NodeDeadSec`); `REMOVING`
(демонтаж); `TO_REMOVE` (маркер панели, one-way). Семантика каждого
значения — [processes/enums.md](processes/enums.md).

Тик ReconcileLoop — работает КАЖДЫЙ тик, без гейтов по дельте (self-healing:
provision/надзор/зонд ждут не-etcd события — health-грань инстанса,
docker-факты). etcd в тике не читается: контроль-плейн берётся из слепка
опросного кэша `/s3/` (S10
[synchronization](../synchronization/README.md), реестр E10 — чтение уходит в
фоновый опрос, единая точка failover S7; заявки ротации кредов и
по-командного зонда, клэймы и portalloc-клэйм процессы читают из etcd
напрямую при исполнении). Применения etcd-значений процессами (ensure
секретов/PKI, RMW, снятие state) — только по факту изменения значения
(S10). Журналы `/s3worker/work/` воркер пишет сам — в кэш домена они не
входят. Смерть инстанса гасит его клэймы-lease ≤ 15 с — takeover очередным
тиком.

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning
([A](processes/a-provisioning.md)); `TO_REMOVE` → Deprovisioning
([B](processes/b-deprovisioning.md)); битый/отсутствующий `config` → пропуск
с логом; иначе Active-ветка: **живая заявка ротации — эксклюзивная ветка**
([H](processes/h-credentials-rotator.md): надзор/D/E в этом тике не идут —
дисциплина «одно пересоздание за тик»); без заявки — надзор
([C](processes/c-node-supervisor.md)) → add-node
([D](processes/d-add-node.md)) → remove-node
([E](processes/e-remove-node.md)) → доводка состава
([G](processes/g-mesh-governance.md)) → зонд
([F](processes/f-sync-probe.md)). Ошибка G прерывает ветку до зонда (заявка
зонда живёт до исполнения); все прочие стадии — независимы.

Все операции — только под живым клэймом `<C>`; владение держится на всём
протяжении мутации: потеря lease прерывает исполняющийся тик
(cancellation-гвард), критические шаги — после живой сверки клэйма
(S5 [synchronization](../synchronization/README.md)); journal-before-manipulations
(`/s3worker/work/<C>`).

## 2. Клэймы, идемпотентность, takeover

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть?
  health отвечает? bucket существует? правило mesh стоит? `state` уже снят?);
  отказ между фазами — ретрай тиком по journal-фазе.
- **Takeover** ≤ TTL 15 с: клэймы/journal/states/portalloc — в etcd; смерть
  инстанса гасит lease — другой инстанс продолжает с записанной фазы; двойной
  контроллер невозможен (операции — только под живым клэймом).
- **Атомарность etcd**: переходы — txn с compare (`mod_revision` config,
  `version==0` клэймы/заявки); RMW `endpoints` — по value прочитанного
  значения; инвариант — любые записи в `/s3/` делает только держатель
  клэйма `<C>` ([s3-clusters/08-coordination.md](../s3-clusters/08-coordination.md)).
- **Порядок записей**: `endpoints` снимается/ставится в завершающем окне
  процессов; `state` инстанса меняется до docker-мутаций (PROVISIONING,
  REMOVING) — незавершённый тик виден следующему по журналу и состояниям.

## 3. Надёжность

- **Отказ docker-хоста**: инстансы на нём `UNREACHABLE`, размещение не
  меняется (`placed_on` фиксирован — перепривязка на месте не допускается,
  [s3-clusters/02-keys.md](../s3-clusters/02-keys.md)), новые инстансы — только на резолвируемые
  хосты.
- **Что теряется при отключении машины X**: volume инстанса — локальное
  хранение без гарантий durability, доступность его доли теряется до
  возврата машины; переживание отказа — с точностью до лага доставки
  (RPO = лаг репликации,
  `../../../OwnS3/docs/owns3-replication-requirements.md` R2) ТОЛЬКО для связей,
  настроенных в `links` (инстанс без входящих связей теряет долю
  безвозвратно, R1 §6); восстановление полноты — backfill из живых
  инстансов (R4 там же, reconcile-механика ownS3); гарантии строятся от
  реплицированных источников (etcd + mesh), не от локальных volume.
- **Отказ etcd**: контроль-плейн заморожен; живые инстансы S3 от него не
  зависят (клиенты пишут по последним `endpoints`).
- **Снапшоты P12**: контроль-плейн `/s3/` + `/s3worker/` снимается
  лидером периодически (`Loops:SnapshotIntervalMin`, `Snapshots:Dir`,
  retention) и процессами в точках изменений «до/после» — provisioning
  (P0/P5), deprovisioning (X0/X3), add-node и remove-node (старт/финал).
- **Ретраи**: короткие сетевые — Polly jitter; ожидание подъёма инстанса —
  транзиент-толерантный цикл с бюджетом `NodeBootSec`, пробы — per-попыточный
  таймаут + пульс живости.
- **Watchdog зависших циклов** — общий каркас `LoopWatchdog` (Shared.Core,
  [pgworker/08-operability.md](../pgworker/08-operability.md) §3): инварианты живости/поллинга
  обязательны и для циклов S3Worker; долгие фазы Active-ветки (последовательные
  пробы молчащих инстансов) отмечаются прогресс-отметками, чтобы живой тик не
  принимался за зависание; «пульс конечного внешнего вызова» (S3-операции
  зонда) — тот же канон.

## 4. Наблюдаемость

`/healthz` — честный health по канону воркеров: `etcd-reachable`,
`docker-hosts` (per-host ping), `loops-alive` (reconcile/keepalive/snapshot),
`claims`. Healthz отдаёт последнее состояние цикла, а не первый сбой; сетевое
исключение пробы — `Degraded` с данными секций, а не исключение чека.

Etcd-клиент воркера — SocketsHttpHandler с периодическим пере-резолвом DNS
и IPv4-first-резолвом (паттерн
[kafkaworker/08-operability.md](../kafkaworker/08-operability.md) §5).
**Единая правда для панели**: опрос `/healthz` живых инстансов по URL из
`/s3worker/api/<id>` ([07-api.md](07-api.md) §4).

Prometheus-метрики — единый каркас [metrics/README.md](../metrics/README.md)
(`/metrics` на порту API): циклы/клэймы/фазы процессов/операции воркера
(`s3worker_*` в Meter-именах), счётчики API по эндпоинтам и фактическим
кодам. Телеметрия самих нод — метрики ownS3
([owns3/05-access-config.md](../../../OwnS3/arch/owns3/05-access-config.md) §5);
scrape-механика — вне канона. Диагностика фаз —
`/s3worker/work/<C>` и `nodes/<I>/state`; операции доводки G — счётчики
`mesh_*` (join/удаления/deny-перезаписи: accepted/conflict/cleaned);
операции зонда F — счётчики `sync_probe_*` (прогоны по исходам, линки
ok/offline, пары synced/lagging/diverged).

## 5. Конфигурация (appsettings + env)

```
S3Worker:Etcd { Endpoints[] }                      # единый etcd-контур (../synchronization/README.md S1)
S3Worker:Bucket                                    # имя per-install bucket (../backups/07-retention.md;
                                                   #   создаётся оркестратором на каждой ноде)
S3Worker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                  PortRange{From=18000,To=18999}, Images{OwnS3="owns3:dev"},
                  Tls, Ssh, ContourNetwork, AdvertisedHost }
S3Worker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000,
                 SnapshotIntervalMin=360,
                 Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15, StopDelaySec=1 } }
                 # watchdog: порог сноса = Multiplier × CheckIntervalSec;
                 # ScanIntervalSec/KeepaliveSec < порога — fail-fast старта
S3Worker:OwnS3 { ReplicationReconcileIntervalSec=0 }   # период reconcile mesh-репликации инстансов
                                                   #   (env OWNS3_REPLICATION_RECONCILE_INTERVAL_SECONDS,
                                                   #   owns3/07 §9; 0 — не ставится, дефолт OwnS3 3 ч)
S3Worker:Thresholds { NodeBootSec=100, NodeDeadSec=90, HealthIntervalSec=15,
                      SyncIntervalSec=60, SyncMaxPages=20 }
                 # SyncIntervalSec — гейт периода зонда F по checked_unix (06 §1);
                 # SyncMaxPages — бюджет страниц сверки на сторону пары (06 §3);
                 # HealthIntervalSec — гейт сбора usage и health-проб надзора
S3Worker:Parallelism { MaxClusters=4 }
S3Worker:Snapshots { Dir="/snapshots", RetentionFiles=10, MaintenanceIntervalMin=60 }
S3Worker:Api { AdvertiseUrl, EnableSeedEndpoint=false, Tls{...} }   # HTTPS-грань,
                                                   # mTLS обязателен только на /api (07 §2)
# env-секреты S3W_*: S3W_API_TLS_{CERT,KEY,CLIENT_CA}, S3W_DOCKER_TLS_{CA,CERT,KEY},
#   S3W_DOCKER_SSH_KEY[_PATH], S3W_ETCD_BACKUP_{ACCESSKEY,SECRETKEY} (04 §1;
#   полупара — fail-fast старта, пара целиком опциональна — обязательна
#   к моменту создания etcd-backup кластера, ../s3worker/04-security.md)
```

Fail-fast валидации старта — по образцу pg (неполные секретные пары,
отрицательные пороги); `EnableSeedEndpoint` — при необходимости стендового
сида, default `false` (паттерн [pgworker/07-api.md](../pgworker/07-api.md) §5).

## 6. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Инстанс без входящих связей в `links` (частичный граф) или единственный инстанс: гибель его диска — потеря доли данных без реплики | рекомендация N≥2/3 и full-связность гео-групп (операционная, [s3-clusters/02-keys.md](../s3-clusters/02-keys.md)); панель — индикация состава связей; риск осознан декларативностью, не запретом |
| R2 | Снос кластера/вывод инстанса уничтожает данные бэкапов его инстансов | осознанное действие оператора: панель — confirm ([02](02-cluster-lifecycle.md) §3, [03](03-node-scaling.md) §2); вывод последнего инстанса запрещён гвардой E (кластер без инстансов — только созданием) |
| R3 | Собственный зонд — единственный честный источник синхронности | зонд — процесс F, форматы [s3-clusters/03-observation-statuses.md](../s3-clusters/03-observation-statuses.md); метрики доставки ownS3 (R2 `../../../OwnS3/docs/owns3-replication-requirements.md`) — наблюдаемость механики, не источник истины о данных |
| R4 | Отсутствие per-cluster сети — межнодовые линки зависят от advertised-адресов и открытых межхостовых портов | ограничение фиксируется явно ([01](01-placement.md) §5); раснос по площадкам — явные селекторы оператора |
| R5 | Смена per-install advertised-адресов (`AdvertisedHost`) — target-записи соседей держат прежние endpoint'ы | join идемпотентно обновляет endpoint (owns3/07 §2); лечение — переприсвоение пары в `links` (снос + повторный join) или пересоздание связей; плановая смена адресов — вне штатных операций |
| R6 | Ротация ключей репликации/CA не автоматизирована (сертификаты бессрочные) | отзыв компрометации — deny-list ([05](05-mesh-governance.md), мгновенный); плановая ротация — roadmap (owns3/07 §12); enterprise-CRL — вне |
| R7 | DR-окно ротации спец-ключа бэкапа etcd: спец-ключ живёт в env воркера и (копией) в secrets-ключе etcd; слепок etcd, снятый до ротации, несёт старое значение — восстановление из него даёт воркерам устаревший ключ до первой свежей записи | фиксируется риск-строкой; оперативное лечение — пере-подстановка значения через заявку ротации (зона выгрузки — [etcdnode/02-snapshots.md](../etcdnode/02-snapshots.md), восстановление — [etcdnode/03-restore.md](../etcdnode/03-restore.md)); механик DR нет |
