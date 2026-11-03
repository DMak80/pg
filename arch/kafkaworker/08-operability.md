# KafkaWorker: эксплуатация

Уровень 3. Функция «эксплуатация»: классификация тика и владение клэймом,
политика ожиданий и экспирации, надёжность, наблюдаемость, конфигурация и
риски. Процессы — [A](processes/a-provisioning.md)…[M](processes/m-security-migrator.md);
перечисления состояний — [processes/enums.md](processes/enums.md); общие
принципы — [17-synchronization-principles.md](../17-synchronization-principles.md).

## 1. Тик и порядок веток

**Состояния ноды** (`brokers/<b>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт контейнер) → `RUNNING` (брокер в
кластере); `UNREACHABLE` (молчит дольше `NodeDeadSec`); `REMOVING` (в
процессе демонтажа); `TO_REMOVE` (маркер панели, one-way). Семантика каждого
значения — [processes/enums.md](processes/enums.md).

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning
([A](processes/a-provisioning.md)); `TO_REMOVE` → Deprovisioning
([B](processes/b-deprovisioning.md)); иначе Active-ветка: **премиграционный
кластер (нет `ca_pem`/`admin_password` или брокеры без SASL_SSL-env) →
SecurityMigrator ([M](processes/m-security-migrator.md), до всего Active)**;
надзор ([C](processes/c-node-supervisor.md)) → converger
([E](processes/e-config-converger.md), вкл. ACL) → reassignment
([I](processes/i-partition-reassigner.md), тик `ReassignIntervalSec` — drain
TO_REMOVE-брокеров с репликами и заявка ребалансировки) → scale-проход remove
([G](processes/g-remove-broker.md)) → add ([F](processes/f-add-broker.md)) →
ротация паролей ([H](processes/h-password-rotator.md), по одному за тик —
app или admin) → ротация CA ([K](processes/k-ca-rotator.md)) → регенерация
([J](processes/j-node-regenerator.md), одно пересоздание за тик) → TopicSync
([D](processes/d-topic-sync.md), тик `TopicSyncIntervalSec`).

Reassignment стоит перед remove — к моменту G дренируемый брокер уже пуст.

## 2. Клэймы, идемпотентность, takeover

- Все операции — только под живым клэймом `<C>`; владение держится на всём
  протяжении мутации: потеря lease прерывает исполняющийся тик
  (cancellation-гвард), критические шаги — после живой сверки клэйма
  ([17](../17-synchronization-principles.md) S5); journal-before-manipulations.
- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть? брокер в
  кластере? desired уже применён? reassignment-батч завершён?); именование
  детерминировано (`kfw-<C>-<b>[-data]`, порты в portalloc); повторная подача
  того же reassignment-assignment безопасна.
- **Takeover**: состояние в etcd (journal, states, portalloc, endpoints);
  смерть инстанса гасит lease ≤ 15 с, следующий продолжает с journal-фазы.
  Двойной контроллер невозможен: операции — только под живым клэймом.
- **Атомарность etcd**: переходы — txn с compare (`mod_revision` config,
  `version==0` клэймы, RMW topics/endpoints).
- Kafka-шаги Active (E–J, D) пропускаются на время backoff недоступного
  кластера (15→60→300 с, сброс при успехе) — тик успех; docker-надзор и
  provisioning не гейтятся (self-healing не зависит от kafka).

## 3. Ожидания, экспирация, восстановление адресов

**Возрастная самозачистка заявок**: заявка (ротации пароля app/admin,
CA-ротации, ребалансировки) старше `RotationTicketTimeoutSec` (возраст — по
`requested_unix` payload) снимается самим воркером в точке waiting-исхода
ТОЛЬКО при строгом «не начато» — тройной гвард: заявка жива ∧ окно ротации
не открыто (staging `ca_next_*` отсутствует) ∧ журнал соответствующего
процесса не в незавершённой мутационной фазе (H: фаза роли
`phase-a`/`rotated-commit`/`phase-c`; K: `rotate-ca` вне `{done, waiting-*}`;
ребалансировка: прогресс-ключ balance не жив). Любой из предикатов ложен —
обычный waiting БЕЗ экспирации (слепой кластер посреди A/R — передержка:
waiting-точки достижимы и при живом staging/журнале, waiting сам по себе
«не начато» НЕ доказывает). Действие: journal-фаза `expired` (терминальная,
с reason) → ОДНА txn `[del заявки][put /kafkaworker/ticket_outcomes/<C>]`
(формат — [kafka-clusters/04-coordination.md](../kafka-clusters/04-coordination.md)) → счёт
`worker_operation_total{result=expired}`. НАЧАТАЯ операция (staging жив,
мутационная фаза журнала, батчи balance подаются) таймаутом не снимается —
только панельный алерт стагнации
([adminpanel/03-panels.md](../adminpanel/03-panels.md) §7.4). Снятие не теряет
заявку навсегда: оператор повторяет её после устранения причины (исход виден
панели).

**Слепота и backoff**: слепая проба DescribeCluster — бюджет молчания не
стартует, пересоздания по молчанию запрещены; окно backoff (15→60→300 с,
сброс при успехе; писатели — проба надзора и коллектор метрик) гейтит
kafka-пробу и kafka-шаги Active-ветки; в etcd backoff ничего не пишет.

Восстановление адресов безадресных брокеров (лестница E9) — в процессе
[C. NodeSupervisor](processes/c-node-supervisor.md).

## 4. Надёжность

- **AdminClient-кэш**: фабрика — sharable-кэш per (bootstrap, user, password);
  Create — «получить клиент ключа», DisposeAsync адаптера — no-op (владение у
  кэша); пины librdkafka `reconnect/retry.backoff.ms ≥ 1000` (+ rdkafka-лог на
  Debug); Failed нативной операции → unhealthy-инвалидация → пересоздание
  (замена немедленна, запись удаляется из кэша мгновенно); протокольные
  операции без нативного клиента кэш не инвалидируют. Dispose заменяемого — в
  фоне ПОСЛЕ завершения его висящих операций: дрейн по счётчику активных
  операций адаптера, бюджет requestTimeout + хвост; при исчерпании бюджета
  дрейн не убивает операцию — warning и продолжение ожидания; принудительный
  dispose остаётся только у остановки host'а. Вход новой операции и решение о
  dispose атомарны под одним замком: операция либо встаёт на счётчик до
  dispose — дрейн ждёт её, либо получает транзиентный отказ «клиент в
  dispose» и не стартует на уничтожаемом хендле; dispose под начатой
  операцией исключён по построению — нативный ERR__DESTROYED; раздача новых
  клиентов дрейном не блокируется; неактивные ключи вытесняются (dispose
  вытесняемого — тот же дрейн); остановка — детерминированный Dispose (кэш =
  IDisposable DI-синглтон; тот же дрейн с бюджетом по каждому клиенту —
  вечного ожидания нет).
- **Снапшоты P12**: лидер регулярно (раз в 6 ч) + **«до/после» в точках
  изменений** — provisioning (K0/K6), deprovisioning (X0/X3), ротация
  (старт/финал). Add/remove брокеров и регенерация (J) — без снапшотов
  (точки изменений — только три перечисленных). Retention `/snapshots`.
- **Ретраи**: короткие сетевые — Polly jitter; ожидание подъёма брокера —
  транзиент-толерантный цикл с бюджетом `BrokerBootSec`.
- **Отказ etcd**: контрол-плейн заморожен; живые брокеры от него не зависят.
  **Отказ docker-хоста**: размещение фиксировано (portalloc); нода
  UNREACHABLE → сценарии надзора.
- **Самозачистка зависших заявок** — §3; дедлоки заявок исключены порядком
  ветки: H доигрывает до K (приоритет), M не гейтится не-начатыми заявками.
- **Переживание данных**: бэкапов Kafka нет и не заводится — данные Kafka
  невосстановимы из бэкапов, RPO держится RF×minISR: долговечность
  обеспечивает репликация, а не копирование (RF/minISR — per-cluster
  конфиги, [02](02-cluster-lifecycle.md) §5; служебные топики — формулы
  [01](01-placement.md) §1; отставание реплик видно метрикой USR, §5).
  Подсистема бэкапов [19-backups.md](../19-backups.md) покрывает только
  PG-шарды.
- **Watchdog зависших циклов**: внутренний компонент `LoopWatchdog`
  (`Shared.Core`, `BackgroundService`) следит за активностью (тик или
  прогресс-отметка; глубоких долгих фаз у KafkaWorker нет — ожидания
  расползаются по тикам; сон snapshot-лидера длиннее окна проверки —
  пульсирующим сном) циклов (reconcile/keepalive/snapshot) по отметкам
  `HealthState`; возраст активности сверх порога
  `Loops:Watchdog:Multiplier` × `Loops:Watchdog:CheckIntervalSec` («2 по 15» =
  30 с; порог healthz loops-alive в формуле НЕ участвует — watchdog знает
  только свои опции, `WatchdogOptions` без изменений; сон snapshot-лидера
  длиннее порога — пульсирующим сном; интервалы `ScanIntervalSec`/
  `KeepaliveSec` < порога сноса — fail-fast старта, общий
  `WatchdogConfigGuard` в Program.cs трёх воркеров) → журнал (critical) +
  метрика `worker_watchdog_restarts_total{loop}` + graceful `StopApplication` —
  контейнер поднимает docker-политика, lease гаснут ≤15 с, takeover вторым
  инстансом. Механика (инварианты живости/поллинга, границы) — канон
  [14-pgworker.md](../14-pgworker.md) §6; формулы порогов healthz
  loops-alive — общий хелпер `LoopStaleness` Shared.Core (healthz watchdog
  больше не потребляет).

## 5. Наблюдаемость

Health `/healthz`: `etcd-reachable` (все endpoints), `docker-hosts` (per-host
ping), `loops-alive` (последний тик каждого цикла), `claims` (сколько
держим), `snapshot-freshness`, `watchdog` (armed/stale — состояние
компонента). Канон честного health:

- **healthz = последнее состояние цикла, а не первый сбой**: успешный тик
  гасит `StatusError` прошлого тика — transient-сбой (пересоздание
  etcd-контейнера и т.п.) ≠ вечный 503 до рестарта воркера;
- **чек всегда отдаёт структуру**: активные пробы (etcd/docker-hosts)
  оборачиваются catch-all — сетевое исключение пробы становится
  `Result.Failed` → `Degraded` с данными секций, а не исключением чека;
- **etcd-клиент — SocketsHttpHandler** с `PooledConnectionLifetime`
  (периодический пере-резолв DNS; лечит застарелый пул коннектов после
  пересоздания etcd-контейнера) и последовательным IPv4-first-резолвом в
  `ConnectCallback` — параллельные A/AAAA-запросы .NET против Docker embedded
  DNS (127.0.0.11) флейпят «Name or service not known».

**Единая правда для панели**: панель опрашивает `/healthz` живых инстансов по
URL из `/kafkaworker/api/<id>` (порт паттерна PgWorker,
[adminpanel/02](../adminpanel/02-etcd-contract.md) §2.3.2) — docker-health и
панель видят одно и то же здоровье, degraded/unhealthy виден алертом
`worker-unhealthy` ≤ 2 тиков поллера и гаснет после восстановления.

Логи: claim/takeover, фазы процессов (journal-фаза), rebuild ноды,
converge-изменения. Diag-ключи: `/kafkaworker/work/<C>`,
`brokers/<b>/state`. Prometheus-метрики — единый каркас
[18-metrics.md](../18-metrics.md): `/metrics` на том же порту `:8080` (словарь
§2.2 воркер-паттерн + §2.3 коллектор лагов/USR).

## 6. Конфигурация (appsettings + env-оверрайды)

```
KafkaWorker:Etcd { Endpoints[] } # список всех узлов HA-контура — 04 §8 (env KFW_ETCD_ENDPOINT_0..2)
KafkaWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
 PortRange{From=16000,To=16999}, Images{Node="apache/kafka:4.0.0"} }
KafkaWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000,
 TopicSyncIntervalSec=15, ReassignIntervalSec=15,
 ReassignBatchPartitions=10,
 Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15, StopDelaySec=1 } }
 # watchdog зависших циклов: порог сноса = Multiplier ×
 # CheckIntervalSec («2 по 15» = 30 с); порог healthz в
 # формуле НЕ участвует; Enabled=false — компонент не
 # регистрируется; ScanIntervalSec/KeepaliveSec < порога
 # сноса — fail-fast старта
KafkaWorker:Thresholds { BrokerBootSec=600, NodeDeadSec=90, ReassignSubmitSec=60,
 ReassignRetrySubmitSec=120, RotationTicketTimeoutSec=3600 }
 # ReassignSubmitSec: бюджет reassign-подачи батча
 # (connect+TLS+SASL+AlterReassignments round-trip, 06 §1);
 # RotationTicketTimeoutSec: возраст не-начатой
 # заявки (ротации app/admin/CA, ребалансировка), после
 # которого воркер снимает её в waiting-точке (§3)
KafkaWorker:Parallelism { MaxClusters=4 }
KafkaWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
KafkaWorker:AdvertisedClientHost=null # null → адрес docker-хоста ноды (placement)
KafkaWorker:Api { AdvertiseUrl (https://…), EnableSeedEndpoint=false,
 Tls { ServerCertPem|ServerCertPath, ServerKeyPem|ServerKeyPath,
 ClientCaPem|ClientCaPath, AllowInsecureHttp=false } }
 # 07-api.md §2–§3: HTTPS-грань, mTLS обязателен только на /api —
 # серверный серт + доверие клиентским (per-install API-CA);
 # URL https:// в /kafkaworker/api/<id>
# env-секреты (07-api.md §3, 04-security.md §3): KFW_API_TLS_CERT / KFW_API_TLS_KEY /
# KFW_API_TLS_CLIENT_CA (PEM) или KFW_API_TLS_{CERT,KEY,CLIENT_CA}_PATH
# (файлы volume kfw-api-tls); KFW_API_KEY удалён
```

`AdvertisedClientHost=null` допустим только когда имя docker-хоста резолвимо
клиентами само по себе; для локальных стендов — `host.docker.internal`
(правило [01-placement.md](01-placement.md) §1).

## 7. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ `apache/kafka` сторонний (CVE/брейкинг-чейнджи 4.x) | версия пин (4.0.0, `Images:Node`); обновление — правкой настройки |
| R2 | Порт-коллизии 16000–16999 с ручными контейнерами | portalloc проверяет фактическую занятость перед созданием; коллизия → сдвиг порта |
| R3 | SASL-JAAS env регенерируется при каждом пересоздании контейнера (идемпотентность env) | env детерминирован от (заявка, portalloc, креды); окно ротации держит оба креда |
| R4 | RF=1: потеря тома = потеря данных | warning-журнал (документированное поведение); RF>1 — self-healing репликацией |
| R5 | Собственный протокольный миниклиент (06 §1): расхождение с фактическими схемами протокола брокера (эволюция api) | версии схем фиксированы и пин образа `apache/kafka:4.0.0` (R1); поддержка api проверяется ApiVersions-fail-fast до подачи; поведение против реального брокера — docker-интеграционная серия reassignment |
| R6 | Долгий drain на больших объёмах данных (минуты–часы) | батчи партиций + прогресс-ключ (UI видит остаток); bandwidth-throttle — roadmap |
| R7 | Уменьшение mem/cpu в мутации №15 ниже рабочего набора брокера → OOM-килл / деградация (валидация §10.3 границы не отсекает) | ответственность оператора (UI-предупреждение в модалке); OOM-рестарт подхватит надзор C (том жив — данные не теряются) |
| R8 | SAN сертификата ноды не покрывает фактический advertised-хост (смена `AdvertisedClientHost` после подъёма) → TLS-отказ клиентов на CLIENT | SAN строится по тому же advertised-правилу [01](01-placement.md) §1 при каждой пересборке env; смена host-настройки требует пересоздания брокеров (NodeRegenerator-событие или пере-ensure: снапшот-детект env vs декларация — M-механика); ротация CA ([K](processes/k-ca-rotator.md)) пересобирает серты с тем же SAN-правилом |
| R9 | Окно миграции M: клиенты без TLS-поддержки получают отказ после M2 (breaking change дискавери) | заявляется релизом (панель обновляется тем же релизом; приложения — библиотекой дискавери); окно ~1–3 мин, тома живы, повтор тика доводит |
| R10 | Приватный ключ CA (`ca_key`) в etcd — компрометация etcd = подделка сертов кластера | etcd — уже хранилище per-cluster-секретов (паролей); зона доверия контроль-плейна; ротация CA — процесс [K](processes/k-ca-rotator.md): компрометированный OLD-ключ уничтожается перезаписью в фазе C, серты нод перевыпускаются; доступ к etcd только из закрытой сети установки |
