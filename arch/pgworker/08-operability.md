# PgWorker: эксплуатация

Уровень 3. Функция «эксплуатация»: классификация тика и владение клэймом,
надёжность, наблюдаемость, конфигурация и риски. Процессы —
[A](processes/a-provisioning.md)…[K](processes/k-move-repair.md); перечисления
состояний — [processes/enums.md](processes/enums.md); общие принципы —
[17-synchronization-principles.md](../17-synchronization-principles.md).

## 1. Тик и порядок веток

Тик ReconcileLoop — работает КАЖДЫЙ тик, без гейтов по дельте (self-healing:
provisioning в waiting-фазах, restore, надзор ждут не-etcd события —
готовность Patroni, docker-состояние; гейт по дельте заморозил бы
недоведённую работу навсегда). etcd в тике не читается: контроль-плейн
берётся из опросного кэша наблюдаемых префиксов (`/clusters/`,
`/service/`, `/pgworker/backups/`, `/pgworker/claims/` — S10
[17-synchronization-principles.md](../17-synchronization-principles.md),
реестр E10). Смерть инстанса гасит его клэймы-lease ≤ 15 с — takeover
очередным тиком.

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning
([A](processes/a-provisioning.md)); `TO_REMOVE` → Deprovisioning
([B](processes/b-deprovisioning.md)); иначе Active-ветка: надзор
([C](processes/c-node-supervisor.md)) и ensure-инвариант Active
([02](02-cluster-lifecycle.md) §6); после надзора — scale-проход remove
([H](processes/h-remove-shard.md)), затем add ([G](processes/g-add-shard.md)),
по одному шарду за тик; ротация секретов
([I](processes/i-secret-rotator.md)) — после scale-прохода (короткая
секундная операция, не ждёт длинных переездов); adoption
([J](processes/j-adoption.md)) и репарация брошенных статусов
([K](processes/k-move-repair.md)) — в Active-ветке; переезды
([F](processes/f-move.md)) — старейшая заявка кластера; эвакуация
([D](processes/d-bucket-evacuator.md)) — по порогу `ShardDeadSec`. Точный
порядок веток — в коде цикла.

## 2. Клэймы, идемпотентность, takeover

- Все операции — только под живым клэймом `<C>`; владение держится на всём
  протяжении мутации: потеря lease прерывает исполняющийся тик
  (cancellation-гвард), критические мутации длинных процессов — только
  после живой сверки клэйма (S5
  [17-synchronization-principles.md](../17-synchronization-principles.md));
  journal-before-manipulations, журнал несёт фазы `lease-lost`/
  `claim-lost/<фаза>`.
- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть? БД
  есть? схема есть? routing уже переведён?) — повтор после сбоя безопасен;
  именование объектов детерминировано (`pgw-<C>-<X>-<n>`).
- **Takeover**: состояние процессов — в etcd (journal + фазы в
  `nodes/<n>/state`, `dsn`, portalloc); смерть инстанса гасит lease-клэймы
  ≤15 с, следующий инстанс продолжает с записанной фазы. Двойной
  контроллер невозможен: операции над кластером — только под живым
  lease-клэймом.
- **Атомарность etcd**: flip-подобные переходы — txn с compare (routing,
  config.mod_revision, `version==0` для клэймов); «нет ключа = ACTIVE» —
  инвариант (status-ключи снимаются пакетами после создания схем).
- **Бэкофф provisioning**: подряд идущие фейлы наращивают `fail_count` в
  `/pgworker/work/<C>`, задержка ретрая —
  `min(ProvisionRetryBaseSec·2^(n−1), ProvisionRetryMaxSec)` (5 с → 60 с);
  до `retry_not_before_unix` тик процесса — skip без записи (самолечение не
  блокируется: серия сбрасывается успехом; фазы прогресса серию переносят).
  Исчерпание бюджета Patroni сбрасывает трекер бюджета: следующая попытка
  получает новый бюджет (иначе каждый тик после первого таймаута фейлил
  мгновенно).
- **Гонка «панель пишет TO_REMOVE посреди provisioning»**: перед фазой
  процесс перечитывает config — смена state безопасно прекращает
  provisioning (контейнеры подчистит deprovisioning).

## 3. Надёжность

- **Отказ etcd**: контрол-плейн заморожен (P9): разрушительных операций
  без свежего клэйма нет; живые ноды от него не зависят.
- **Отказ docker-хоста**: размещение не меняется (portalloc фиксирован);
  недоступный хост → нода `UNREACHABLE` → сценарии надзора; новые ноды —
  только на живые хосты.
- **Ретраи**: короткие сетевые/SQL — Polly jitter-политики; долгие
  ожидания (Patroni-подъём, догоняние) — транзиент-толерантные циклы с
  бюджетом; ошибка тика → `journal.last_error` + продолжение со следующего
  тика.
- **Снапшоты P12 (локальные)**: регулярные (лидер, раз в 6 ч) + в точках
  изменений (provisioning/deprovisioning/эвакуация — до и после, переезды —
  `move-<bucket>-start`/`flip-<bucket>-<to>`, ротация — старт/финал);
  ретенция `RetentionFiles` в локальном томе. Restore — внешний рецепт
  (`restore-cluster.sh`), PgWorker только снимает. Выгрузки в S3 НЕТ:
  DR-слепки контура и их обслуживание (compact/defrag) — зона EtcdWorker
  ([etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md)); PgWorker
  не владеет S3-целями слепков и статусом выгрузки.
- **Watchdog зависших циклов**: внутренний компонент `LoopWatchdog`
  (`Shared.Core`, `BackgroundService`) следит за **активностью** всех
  фоновых циклов (reconcile/keepalive/snapshot/orphan-sweep) по отметкам
  `HealthState`; активность = тик ИЛИ прогресс-отметка долгой фазы
  (`ILoopProgress`). Порог сноса = `Loops:Watchdog:Multiplier` ×
  `Loops:Watchdog:CheckIntervalSec` (дефолт 2 × 15 = 30 с, единый для всех
  циклов; «правило 2 по 15 — суммарно через 30 с срабатывает»).
  healthz-порог (`LoopStaleness`) в формулу НЕ входит: watchdog знает
  только собственные опции, формулы healthz (`LoopStaleness`) потребляет
  только healthz. `WatchdogOptions` без изменений
  (Enabled/Multiplier/CheckIntervalSec/StopDelaySec). Механика сноса
  прежняя: проверка раз в `CheckIntervalSec`, снос цикла при возрасте
  активности сверх порога (одного наблюдения достаточно — порог сам
  защищает от ложных срабатываний, гистерезис не вводится); отметка `null`
  («цикл ещё не проявлял активности») не firing, пока возраст watchdog с
  его запуска меньше 2×порога цикла (стартовый grace первого проявления).
  Снос: журнал (critical) + метрика `worker_watchdog_restarts_total{loop}` +
  graceful `StopApplication` (тот же путь, что `POST /api/restart`):
  контейнер поднимает docker-политика, lease гаснут ≤15 с, клэймы мигрируют
  второму инстансу (takeover). Компонент без сетевых вызовов и без etcd; за
  собой не следит.
  **Инвариант живости**: каждый цикл обязан обновлять активность (тик,
  отметка поллинга, пульс сна) строго чаще порога сноса (30 с при
  дефолтах); тики быстрых циклов — `ScanIntervalSec`/`KeepaliveSec` <
  порога (fail-fast старта воркера при нарушении — общий
  `WatchdogConfigGuard` в Program.cs ТРЁХ воркеров (PgWorker/Kafka/
  Valkey): порог фиксирован опциями watchdog и, в отличие от прежней
  healthz-зависимой формулы, не растёт вместе с интервалами циклов); любой
  сон длиннее порога (интервал лидера snapshot, интервал sweeper'а,
  retry-сон выгрузки снапшотов) — «пульсирующий сон» чанками короче окна
  проверки, каждый чанк — отметка активности БЕЗ тика (healthz loops-alive
  читает только тики — семантика healthz/Degraded-окна не меняется).
  **Инвариант поллинга долгих фаз**: ни один одиночный блокирующий вызов
  внутри фазы reconcile (docker create/start контейнеров нод/джобов,
  SQL/HTTP-пробы, etcd-операции) не имеет права молчать дольше порога сноса
  — медленный daemon делает одиночный вызов длиннее любого наперёд
  заданного числа, разметка «отметка после вызова» оставляет слепую зону
  между отметками. Долгая операция = цикл идемпотентных поллинг-итераций с
  индивидуальным таймаутом короче окна проверки (идентифицирующий
  инспект/повторный ensure), каждая итерация — `ILoopProgress.Mark` ПО ФАКТУ
  итерации + лог-строка elapsed («фаза <X>: уже <N> с»). Останов контейнера
  — тот же поллинг: `docker stop` (грейс `t=N` у упирающегося контейнера
  дольше пер-попыточного таймаута) — цикл идемпотентных итераций (повтор
  `POST /stop` и/или GET-инспект `State.Running`) с индивидуальным
  таймаутом ≤7.5 с до факта `exited`/`304`/`404` в бюджете `t + одна
  итерация`, суммарно короче порога сноса (30 с); бюджет исчерпан без
  exited-факта — отказ. Для spilo-нод (PG/Patroni) остановка — `docker
  kill` (SIGKILL: грейса нет, exited-факт немедленный — поллинг не нужен;
  жёсткая смерть ноды — штатный канал); поллинг-стоп — для всех прочих
  контейнеров (брокеры, инстансы, джобы, агенты). Поллинг не итерируется
  (висящий вызов) — отметок нет — watchdog сносит процесс (зависание
  ловится не позднее порога + окно проверки). Живая фаза любой
  длительности неуязвима: пока итерации идут, возраст активности не
  превышает порога. Отметка «в обмен на ничто» запрещена: Mark ставится
  только по факту исполнившейся итерации, не по расписанию — маскировка
  зависания тиками/отметками без работы исключена.
  **Пульс конечного внешнего вызова** (класс вызовов, не разбиваемых
  поллингом, — S3-операции бэкапов,
  [backups/06-s3-layout.md](../backups/06-s3-layout.md)): одиночный вызов внешней системы,
  чей идемпотентный повтор бессмыслен/опасен внутри фазы (list/get/put/
  delete S3), не поллингуется — вместо этого: (а) клиенту задаётся
  КОНЕЧНЫЙ пер-попыточный таймаут короче окна проверки (BackupS3:
  `AmazonS3Config.Timeout` = полокна, 10 с при дефолтах — симметрия
  таймаутов etcd/patroni/SQL), (б) на время вызова цикл держит пульс
  активности по расписанию (`S3Pulse`: Timer с периодом полокна, живёт
  ровно вокруг одного вызова). Отметка-по-расписанию здесь НЕ маскирует
  зависание: вызов гарантированно завершается пер-попыточным таймаутом →
  transient-фейл тика → повтор ПРОЦЕССОМ (следующий тик reconcile / проход
  sweeper'а / retry выгрузки — `Retry.BaseSec`/`RetryIntervalSec`,
  [backups/05-etcd-contract.md](../backups/05-etcd-contract.md)), молчаливое продление запрещено.
  Границы паттерна: слепое окно = ОДНА логическая S3-операция (пульс
  угасает с завершением вызова); пер-попыточный таймаут ОБЯЗАТЕЛЕН —
  «пульс вокруг потенциально вечного вызова» запрещён (это и отличает его
  от поллинга долгих фаз: нет идемпотентных итераций внутри, есть
  конечность вызова снаружи); зависание процесса живого цикла всё равно
  ловится watchdog'ом (вызов умрёт таймаутом, отметки тиков/фаз
  прекратятся). Граница: «завис весь процесс» (healthz не отвечает) — зона
  docker HEALTHCHECK, watchdog закрывает только «цикл завис, процесс жив».
  Анти-луп рестартов не вводится: сдерживание — docker restart-backoff,
  видимость — метрика/журнал.

## 4. Наблюдаемость

- **Health checks** (`/healthz`): `etcd-reachable` (все endpoints),
  `docker-hosts` (per-host ping), `loops-alive` (последний тик каждого
  цикла), `claims` (сколько держим), `snapshot-freshness` (возраст
  последнего снапшота), `watchdog` (armed/stale — состояние компонента).
- **Логи**: ключевые события — claim/takeover кластера, фазы процессов
  (с journal-фазой), rebuild ноды, эвакуация (полный план), сверка
  мастер-ключа с коррекцией.
- **Diag-ключи etcd** несут наблюдаемость для панели/оператора:
  `/pgworker/work/<C>` (живая фаза), `nodes/<n>/state`, journal эвакуаций.
- Prometheus-метрики — единый каркас [18-metrics.md](../18-metrics.md):
  `/metrics` на том же порту `:8080` (словарь §2.2 — циклы/клэймы/фазы/
  операции/снапшоты/рестарты watchdog), репликация PG — scrape Patroni
  `:8008` напрямую.

## 5. Конфигурация (appsettings + env-оверрайды)

```
PgWorker:Etcd:Endpoints[] # http://host:2379
 # список всех узлов HA-контура — 04 §8
PgWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}],
 SwarmManager, PortRange{From,To}, Images{Node},
 Tls {CaPem|CaPath, ClientCertPem|Path, ClientKeyPem|Path}, # 01-placement §4
 Ssh {KeyPem|KeyPath, RemoteDaemonHost=127.0.0.1,
 RemoteDaemonPort=2376, FingerprintSha256?}, # 01-placement §4
 RestTls {CaPem|CaPath, CaKeyPem|CaKeyPath} } # REST-TLS :8008:
 # per-install CA + ключ выпуска серверных
 # сертов REST-эндпоинтов нод; env-имена
 # PGW_REST_TLS_*; оба обязательны — fail-fast
 # старта (симметрия запрета Pgtune:DbType=desktop);
 # WAF-фикстуры — тестовый CA
PgWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, SnapshotIntervalMin=360,
 ErrorDelayMs=2000,
 Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15, StopDelaySec=1 } }
 # watchdog зависших циклов: порог сноса = Multiplier ×
 # CheckIntervalSec («2 по 15» = 30 с, единый для всех циклов);
 # порог healthz loops-alive в формуле НЕ участвует (watchdog
 # знает только свои опции); Enabled=false — компонент не
 # регистрируется; ScanIntervalSec/KeepaliveSec < порога
 # сноса — fail-fast старта (тики быстрых циклов обязаны
 # обновлять активность чаще порога)
PgWorker:Thresholds { NodeDeadSec=90, ShardDeadSec=300, PatroniBootSec=600,
 CutoverTimeoutSec=90, ConnFailBudgetSec=120,
 ProvisionRetryBaseSec=5, ProvisionRetryMaxSec=60 }
 # бэкофф ретраев provision: задержка
 # min(Base·2^(fail_count−1), Max) — 5,10,20,40,60,60…;
 # PatroniBootSec — он же бюджет отсутствия лидера для
 # лечения HA-scope при доказанной утрате данных (Д3)
PgWorker:Moves { PollIntervalSec=2, FreezeWaitSec=5, FreezeLockTimeoutSec=5,
 FreezeLockTries=3, AbortMinAgeSec=120, FailoverSlots=true,
 AdvertisedPublisherHost=null } # host издателя, как виден из
 # контейнеров приёмников (single-docker-host стенды:
 # host.docker.internal; прод — null, адреса dsn достижимы);
 # применяется только для канонических pgw-приёмников
PgWorker:Moves { RepairStaleSec=600, RepairFrozenSec=120 } # репарация брошенных
 # статусов: 600 = StaleMove панели (ремонт синхронизирован с
 # алертом), 120 = AbortMinAgeSec (FROZEN режет запись — чиним
 # быстрее, живой cutover межтиков невозможен: непрерывный блок
 # одного тика)
PgWorker:Parallelism { MaxClusters=4 }
PgWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
                # Локальные слепки P12: регулярные + точки изменений,
                # ретенция файлов в томе. Выгрузка в S3 и обслуживание
                # контура (compact/defrag) — EtcdWorker
                # (etcdworker/04-snapshots.md): секций Export и MaintenanceIntervalMin
                # у PgWorker нет.
PgWorker:AppParams { Default="sslmode=require" }  # per-node ключ
                  # shards/<X>/nodes/<n>/app_params (P2.5'/A5/C; P17)
PgWorker:Pgtune { DbVersion=18, DbType="oltp", HdType="ssd", DbSize="mid_ram",
 Connections=60, ExcludeParams=["io_method","io_workers"] }
 # входы PGTune ([01-placement.md](01-placement.md) §2): DbVersion 10..18;
 # DbType web|oltp|dw|mixed — desktop ЗАПРЕЩЁН (wal_level=minimal
 # несовместим с P3) — fail-fast валидация старта; Connections 20..999999
 # = вход connectionNum (P15: doorman = Connections − 5);
 # ExcludeParams — имена PGTune-параметров, не применяемые при сборке
 # YAML (io_method=io_uring требует --with-liburing — не проверено для
 # образа; после проверки оператор убирает из exclude). Память/CPU —
 # НЕ конфигурация: etcd-заявки /service/<scope>/request_{cpu,mem} на
 # ноду — ОБЯЗАТЕЛЬНЫ; заявки нет — фейл фазы, дефолтов нет.
 # Переопределение параметров — ТОЛЬКО через эти опции:
 # etcd-канала переопределения нет, параметры не фиксируются
PgWorker:Api { AdvertiseUrl, EnableSeedEndpoint=false,
 Tls {ServerCertPem|Path, ServerKeyPem|Path, ClientCaPem|Path,
 AllowInsecureHttp=false} } # HTTPS-грань, mTLS обязателен только на /api;
 # URL API (достижимый панелью) в /pgworker/api/<id> — https://;
 # демо-сид-эндпоинт за флагом; AllowInsecureHttp — только WAF-тесты
# секреты — env PGW_* ([04-security.md](04-security.md) §2): per-install пароли +
# PGW_API_TLS_* / PGW_DOCKER_TLS_* / PGW_DOCKER_SSH_*; PGW_REST_TLS_* —
# REST-TLS нод; PGW_API_KEY исключён (mTLS вместо X-Api-Key)
```

Пулер pg_doorman — ОБЯЗАТЕЛЬНАЯ часть образа ноды (клиентский вход `:6432`,
master-ключ `host:<doorman>` полноценный): узлов без пулера нет, клиентский
трафик — только через пулер; бинарник — пин: обязательный build-arg
`DOORMAN_IMAGE` — мульти-арх контейнер-образ проекта pg_doorman (fail-fast
без него); целевое решение — x64 (amd64-сборка), arm64 — платформа
разработки ([pg-clusters/03-prerequisites.md](../pg-clusters/03-prerequisites.md) §1, R1).

## 6. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ `pgworker-node` зависит от бинарника pg_doorman (молодой проект, P16) | версия — пин: `DOORMAN_IMAGE` обязателен при сборке (fail-fast без него) — мульти-арх контейнер-образ проекта pg_doorman: целевая платформа x64 (amd64 — целевое решение), arm64 — платформа разработки ([pg-clusters/03-prerequisites.md](../pg-clusters/03-prerequisites.md) §1); бинарники GitHub-релиза существуют только под amd64 и отдельным путём поставки не служат |
| R2 | Swarm `publish mode=host` + уникальные порты — нестандартный путь; коллизии при ручных контейнерах | portalloc проверяет фактическую занятость перед созданием; коллизия → сдвиг порта + перегенерация DSN |
| R3 | Spilo callback (master-lease) — отдельный процесс; отказ = протухший master-ключ | P11-сверка MasterKeyReconciler восстанавливает ключ по Patroni REST — двойной контур |
| R4 | Эвакуация «пустыми схемами» теряет записи умершего с момента смерти | осознанное аварийное поведение; фиксируется в journal; слияние после возврата — только ручной runbook, автоматизация отвергнута (воркер не знает семантику схем `bucket_%`, конфликт двух живых ветвей записей неразрешим автоматически; данные до момента смерти доступны на дисках карантина и в бэкапах — [backups/README.md](../backups/README.md)) |
| R5 | Ноды одного шарда на одном хосте (hosts < replicas) — пониженная отказоустойчивость | требование «если топология позволяет»; факт отражается в плане и виден оператору |
| R6 | Гонка «панель пишет TO_REMOVE посреди provisioning» | перечитывание config перед фазой; смена state → перепланирование, контейнеры подчистит deprovisioning |
| R7 | Restore etcd из снапшота откатывает journal/клэймы | клэймы — lease (не восстанавливаются с живым lease); journal может откатиться — процессы идемпотентны, повторная фаза безопасна |
| R8 | Усыновление: master-ключ внешнего формата `node:port` vs reconciler `host:doorman` — война писателей | MasterKeyReconciler пропускает усыновлённые шарды; master-ключ пишет внешний HA-контур, резолв мастера понимает оба формата ([06-bucket-moves.md](06-bucket-moves.md) §2) |
| R9 | Rebuild усыновлённой ноды поднял бы дубль (второй Patroni на scope, конфликт портов) | self-healing для `object`-нод отключён: `UNREACHABLE` + journal, разбор оператором |
| R10 | Репарация прибивает живой переезд, запущенный вне воркера (писатель статуса не тикает updated_unix в copy-wait) | скриптовый путь исключён из живой системы — мутации только заявками; статус без тика = брошенный по построению; порог RepairStaleSec = панельному stale; заявка репарации помечена `requested_by=pgworker-repair` |
| R11 | Чистка HA-scope (Д3) при живых данных = потеря кластера | трёхуровневая проба данных (Present/Absent/Unknown — `GET /api/node/pgdata` приёмнику ноды, канал [25-component-communications.md](../25-component-communications.md) №22; Unknown — транспортный отказ): чистка ТОЛЬКО при Absent у ВСЕХ нод scope; Unknown и хоть одна Present — не лечить (Present → журнал-фейл «разбор оператора»); одна чистка на scope за бюджет (новый бюджет после чистки); journal phase=reset-scope — оператор видит каждую чистку |
| R12 | Перепланирование портов (Д1/Д2) меняет dsn — клиенты держат старые адреса | dsn-ключ — единственная точка входа (клиенты перечитывают etcd, паттерн app_password-ротации); репарация только при расхождении с фактом (стабильный кластер не трогается); journal repaired-dsn — событие видно |
| R13 | SAN серверного серта API не покрывает фактический advertise-хост → строгие клиенты (curl, Prometheus server_name) получают TLS-отказ | канон SAN-набора пакета: `pgworker`, `localhost`, `host.docker.internal`, `127.0.0.1`; генерация — `deploy/tls/gen.sh` с полным SAN; панель валидирует цепочку (CustomRootTrust) — hostname-check не ломает её даже при расхождении, строгие клиенты защищены каноном SAN |
| R14 | SSH-туннель без pinned host-key — MITM на первичном подключении (TOFU) | `PgWorker:Docker:Ssh:FingerprintSha256` — pin строго; не задан → accept + warning-лог при старте хоста (диагностируемое ослабление); канон прода — pin задан |
| R15 | Plaintext `tcp://:2375` к Engine API остаётся технически возможным (dev/тесты) | канон [01-placement.md](01-placement.md) §3.1: прод — только 2376+mTLS или ssh; plaintext → warning-лог воркера на каждом старте хоста; 2375 в firewall-матрице [pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §2 отсутствует (default deny) |
| R16 | Компрометация `ca.key` = выпуск серверных сертов REST/API от имени установки | ключ в TLS-томе поставки (ro), права 600, в etcd/registry не попадает; на стенде панель/as-prometheus монтируют `deploy/tls` целиком (bind ro — домашний контур, принято; точечное монтирование — опция поставки); потеря deploy-хоста = потеря `ca.key` — обязательный бэкап пакета `deploy/tls` (runbook), серты живых нод не инвалидируются |
