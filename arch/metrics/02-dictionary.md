# Телеметрия: словарь метрик

Уровень 3. Канон имён, типов и лейблов метрик. Родитель — [README.md](README.md);
соседние документы — [01-framework.md](01-framework.md),
[03-exposure.md](03-exposure.md), [04-collectors.md](04-collectors.md),
[05-storage.md](05-storage.md), [06-testing.md](06-testing.md),
[07-risks.md](07-risks.md).

Имена — финальные после OTel-экспорта (точки → подчёркивания, суффиксы
единиц/`_total` по стандарту экспортёра; фактические имена фиксирует
интеграционный тест [06-testing.md](06-testing.md)). Лейблы конечны
(никаких free-form строк в лейблах, кроме `cluster` — доменное имя кластера).

## 1. Общие (все сервисы, из OTel-рантайма)

| Имя | Тип | Источник |
|---|---|---|
| `dotnet_*` (gc, threadpool, allocations, process) | разные | OTel `System.Runtime`-метры |
| `http_server_request_duration_seconds` | histogram | OTel ASP.NET-метр (панель; у воркеров — при наличии HTTP-грани). Факт пинов 1.16.0-beta.1 (фиксирует тест `Shared.Metrics.UnitTests`): на минимальном slim-хосте гистограмма не эмитится; фактически присутствуют `http_server_active_requests`, `kestrel_*`, `aspnetcore_memory_pool_*` — на реальных сервисах сверяют интеграционные тесты |

## 2. Воркер-паттерн (`Shared.Metrics` WorkerMetrics; воркеры PgWorker/KafkaWorker/ValkeyWorker/S3Worker)

Имена инструментов глобальны (имя Meter в метрику не входит) — все воркеры
пишут в одни серии; различение сервисов/инстансов — лейблы `job`/`instance`,
которые назначает Prometheus по scrape-джобе ([05-storage.md](05-storage.md) §2). Факт экспортёра
1.16.0-beta.1 (фиксирует интеграционный тест
[06-testing.md](06-testing.md)): к каждой серии добавляется
системный лейбл `otel_scope_name` = имя Meter (свой у каждого воркера) —
PromQL-запросы словаря от него не зависят, но факт зафиксирован для инспекции.

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `worker_loop_ticks_total` | counter | `loop`, `ok` | тики циклов (Reconcile/Keepalive/Snapshot/OrphanSweep); `ok` ∈ {true,false} |
| `worker_loop_last_success_timestamp_seconds` | gauge | `loop` | unix-время последнего успешного тика (алерт «цикл умер» WorkerLoopStalled — только быстрые циклы `{loop=~"reconcile\|keepalive"}`, порог 60 с: reconcile/keepalive тикают каждые 5 с; snapshot-цикл тикает раз в SnapshotIntervalMin (6 ч) и под порог не попадает — его живость закрывает SnapshotStale по `worker_snapshot_age_seconds`; цикл `orphan-sweep` (BackupOrphanSweeperLoop, тик лидера раз в `Backups:Supervisor:IntervalSec`, дефолт 600 с; `ok` = успех прохода `SweepAsync`) — живость закрывает отдельное правило **OrphanSweepStalled** (`time() - worker_loop_last_success_timestamp_seconds{loop="orphan-sweep"} > 1800`, порог 3× дефолтного интервала; серия эмитится только лидером при `Backups:Enabled=true` — выключенная подсистема не алертится, смерть процесса ловит ServiceDown); цикл `wal-sweeper` (WalSweeperLoop, тик лидера раз в 60 с при `Backups:Enabled=true`, `ok` = успех прохода `WalSweeperProcess.SweepAsync`; чистка мусора `wal/` — [backups/processes/d-wal-sweeper.md](../backups/processes/d-wal-sweeper.md)) — живость закрывает правило **WalSweepStalled** (`time() - worker_loop_last_success_timestamp_seconds{loop="wal-sweeper"} > 180`; серия эмитится только лидером при `Backups:Enabled=true`)) |
| `worker_wal_sweep_shard_last_success_timestamp_seconds` | gauge | cluster, shard | unix последнего успешного per-shard прохода wal-sweeper ([backups/processes/d-wal-sweeper.md](../backups/processes/d-wal-sweeper.md); обновляется по факту прохода, серия исчезает с шардом); алерт **WalSweepShardStale** — time() − значение > 180 (3× минутного интервала), только при живой серии |
| `worker_loop_duration_seconds` | gauge | `loop` | длительность последнего тика |
| `worker_claims_held` | gauge | — | сколько кластеров держим под клэймом |
| `worker_process_phase_duration_seconds` | gauge | `cluster`, `process`, `phase` | сколько секунд кластер в текущей фазе процесса (source: марк-методы фаз; смена фазы/завершение процесса сбрасывает серию) |
| `worker_operation_total` | counter | `operation`, `result` | завершённые операции (provision/deprovision/rotate/move/rollback/finalize/abort…; подавленные ops — supervise/evacuate/backup-orphan-sweeper — не считаются, см. ниже), `result` ∈ {ok,error,expired} (`expired` — заявка снята возрастным таймаутом воркером) |
| `worker_snapshot_age_seconds` | gauge | — | возраст последнего снапшота P12 |
| `worker_watchdog_restarts_total` | counter | `loop` | инициированные watchdog-остановки цикла по отсутствию активности (тик или прогресс-отметка) (внутренний watchdog воркера: [pgworker/08-operability.md](../pgworker/08-operability.md) §3, [kafkaworker/08-operability.md](../kafkaworker/08-operability.md) §4, [valkeyworker/06-operability.md](../valkeyworker/06-operability.md) §3); источник — марк-метод `WatchdogRestart` |

`process` — фактическое `op` журнала работы (канон = факт, фиксируется
интеграционным тестом): у PgWorker — `provision`, `deprovision`, `adopt`,
`add-shard`, `remove-shard`, `rotate-app-password`, `move`, `rollback`,
`finalize`, `repair`, `abort`; у KafkaWorker — `provision`, `deprovision`, `add-broker`,
`remove-broker`, `reassign`, `rotate`, `regen`, `topicsync`;
у ValkeyWorker — `provision`, `deprovision`, `rotate`, `supervise` (подавляемый);
у S3Worker — `provision`, `deprovision`, `add-node`, `remove-node`,
`rotate-secrets`, `supervise` (подавляемый). `phase` —
фаза машины состояний (journal-фаза). Завершение фазовой серии —
терминальные фазы журнала по фактическому словарю: `done`, `failed`,
`crashed`, `rejected`, `cancelled`, `expired` (возрастное снятие
не-начатой заявки; сброс серии + счёт `worker_operation_total`;
`skipped` — промежуточная фаза усыновления, серию НЕ закрывает).
Стационарные ops без терминальной фазы — `supervise` (пишется и через
`WriteSupervisionAsync`, мимо фазового события) и `evacuate` (только
`waiting-*`/`blocked-moving`), а также `backup-orphan-sweeper`
(журнал домена сирот — свой ключ `/pgworker/backups/orphan-journal`
([backups/07-retention.md](../backups/07-retention.md)), в work-фазовые метрики не входит) — фазовых серий НЕ
эмитят, вечные серии не копятся: живость `supervise`/`evacuate` закрывают клэйм-циклы
(`loop="reconcile"` → алерт WorkerLoopStalled), живость `backup-orphan-sweeper` —
цикл `orphan-sweep` (LoopTick по итогам SweepAsync → алерт OrphanSweepStalled).
Подавляемый также `healing-portalloc` — вспомогательная точечная реконструкция
portalloc внутри клэйм-тика надзора ([valkeyworker/processes/c-node-supervisor.md](../valkeyworker/processes/c-node-supervisor.md) /
[s3worker/08-operability.md](../s3worker/08-operability.md), общий
EtcdPortAllocHealer): фазовые серии не нужны, зависание закрывается watchdog'ом
того же reconcile-цикла.

## 3. Kafka-домен (коллектор KafkaWorker, [04-collectors.md](04-collectors.md) §1)

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `kafka_consumer_lag` | gauge | `cluster`, `group`, `topic` | суммарный consumer-lag (end-offsets − committed), по группе и топику |
| `kafka_under_replicated_partitions` | gauge | `cluster`, `topic` | число партиций топика с USR>0 (ISR ⊂ assignment) |
| `kafka_collector_last_success_timestamp_seconds` | gauge | — | unix-время последнего успешного сбора коллектора (самонаблюдение) |

## 4. Панель (AdminPanel)

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `panel_refresher_last_success_timestamp_seconds` | gauge | — | успешный тик etcd-refresher (свежесть снапшота панели) |
| `http_server_request_duration_seconds` | histogram | OTel | см. §1 |

## 5. PG-репликация — scrape Patroni напрямую

Метрики репликации НЕ дублирует воркер: Prometheus скрейпит `:8008/metrics`
Patroni-нод ([pg-clusters/08-operations.md](../pg-clusters/08-operations.md) §7). Таргеты реальных нод — file_sd из portalloc
([05-storage.md](05-storage.md) §2 `patroni-nodes`, §5.4).

Словарь реальных Patroni-нод — нативные серии REST `/metrics` (spilo,
Patroni 4.x); канон-минимум, на который пишутся дашборд/алерты:
`patroni_primary`, `patroni_replica`, `patroni_sync_standby`,
`patroni_postgres_timeline`, `patroni_xlog_replayed_timestamp`,
`patroni_version`, `patroni_postgres_running` (лейблы `scope`, `name` —
сам Patroni; роли — отдельными сериями: primary/replica/sync_standby/
standby_leader/quorum_standby). Словарь зафиксирован фактом
docker-E2E ([06-testing.md](06-testing.md), M3): полный фактический набор шире — 23 серии
(`patroni_dcs_last_seen`, `patroni_xlog_received/replayed_location`,
`patroni_postgres_streaming/state/server_version`, `patroni_pending_restart`,
`patroni_is_paused`, `patroni_postmaster_start_time`, …); расширяют его
только фактом того же прогона.
Скрейп реальных нод — `scheme: https` + `tls_config.ca_file`
(REST `:8008` — TLS, [pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §4).
Семантика per-node: запись с `"alias"` → сетевой таргет `<alias>:8008`
(контейнерный порт Patroni REST;
штатная ветка единой сети контура —
[pgworker/01-placement.md](../pgworker/01-placement.md) §5),
верифицируется полностью (имя = SAN серта); запись без
`"alias"` (легаси-переходный контур, усыновлённая нода) → advertised
`host:patroni` по host-публикации — деградационная ветка: не только
достижимость, но и TLS-верификация имени по advertised-хосту не
гарантируется (SAN его не несёт) — таргет уходит в down; в поставке таких
таргетов нет. Фильтр `patroni <= 0` (усыплены) действует в обеих ветках.

## 6. Valkey-домен (коллектор ValkeyWorker, [04-collectors.md](04-collectors.md) §2)

Имена — финальные Prometheus-формата; в коде — dot-нотация Meter. Лейблы конечны:
`cluster` (доменное имя), `node` (`node<k>`), `role` ∈ {master,slave} (только у
`valkey_role`); `job`/`instance` назначает Prometheus по scrape-джобе
([05-storage.md](05-storage.md) §2).

| Имя | Тип | Лейблы | Источник INFO | Смысл |
|---|---|---|---|---|
| `valkey_memory_used_bytes` | gauge | cluster, node | Memory.used_memory | потребление памяти нодой, байты |
| `valkey_memory_max_bytes` | gauge | cluster, node | Memory.maxmemory | предел памяти ноды (maxmemory), байты |
| `valkey_connected_clients` | gauge | cluster, node | Clients.connected_clients | подключённые клиенты |
| `valkey_blocked_clients` | gauge | cluster, node | Clients.blocked_clients | заблокированные клиенты (BLPOP и т.п.) |
| `valkey_evicted_keys` | gauge | cluster, node | Stats.evicted_keys | кумулятив выселенных ключей (сброс при рестарте — gauge) |
| `valkey_expired_keys` | gauge | cluster, node | Stats.expired_keys | кумулятив истёкших ключей |
| `valkey_keyspace_hits` | gauge | cluster, node | Stats.keyspace_hits | кумулятив попаданий (hit-rate = hits/(hits+misses)) |
| `valkey_keyspace_misses` | gauge | cluster, node | Stats.keyspace_misses | кумулятив промахов |
| `valkey_instantaneous_ops_per_sec` | gauge | cluster, node | Stats.instantaneous_ops_per_sec | операции/сек (готовая скорость, без rate()) |
| `valkey_total_connections_received` | gauge | cluster, node | Stats.total_connections_received | кумулятив принятых соединений |
| `valkey_rejected_connections` | gauge | cluster, node | Stats.rejected_connections | кумулятив отклонённых соединений (maxclients) |
| `valkey_total_commands_processed` | gauge | cluster, node | Stats.total_commands_processed | кумулятив обработанных команд |
| `valkey_role` | gauge | cluster, node, role | Replication.role | 1 в серии с `role="master\|slave"` (enum-паттерн) |
| `valkey_connected_slaves` | gauge | cluster, node | Replication.connected_slaves | число реплик (v1 standalone — всегда 0; заготовка под будущие топологии) |
| `valkey_collector_last_success_timestamp_seconds` | gauge | — | самонаблюдение | unix-время последнего успешного тика коллектора |

Семантика gauge-кумулятивов: persistence off и пересоздания контейнера сбрасывают
счётчики Valkey в 0 — counter-тип ломал бы `increase()`/`rate()`; динамика в PromQL —
`rate()`/`increase()` по gauge-сериям (каноническое решение доменного словаря).
Поле INFO отсутствует/нечислово в ответе ноды → конкретная серия не эмитится
(консервативно, без нулей-фантомов); сам сбор кластера при этом успешен.

## 7. Бэкап-домен и RTO-длительности PgWorker

Серии питают тики процессов бэкапов и надзора
([backups/README.md](../backups/README.md),
[pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md)).

Отдельного коллектора НЕТ: серии пишутся тиками существующих процессов бэкапов
под клэймом `<C>` (observer-делегаты, паттерн [04-collectors.md](04-collectors.md) §1/§2) — возраст полных —
BackupProcess (планировщик уже вычисляет обе величины,
[backups/01-full-backup.md](../backups/01-full-backup.md) §6), WAL-серии —
контрольный проход WalStreamProcess (`Wal:VerifyIntervalSec`,
[backups/02-wal-archive.md](../backups/02-wal-archive.md) §3),
counters — точки фиксации терминальных исходов verify/restore/drill. RTO-длительности
пишутся тиком надзора (NodeSupervisor) из фактов work-ключа
([pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md));
skipped-пулы свипера сирот — тиком глобального лидера свипера
(BackupOrphanSweeperLoop, [backups/07-retention.md](../backups/07-retention.md), вне клэймов). Набор серий
кластера перезатирается каждым тиком (паттерн `UpdateCluster`
[04-collectors.md](04-collectors.md) §2): ушедшие
шарды/кластеры стейт не копят; null-факт (нет валидного полного, зонд не удался,
wal-ключ STOPPED/WAL_CHAIN_ERROR/удалён) — серия исчезает до появления факта. Панель этих
серий не читает (живёт на etcd-снапшоте) — словарь для Prometheus/Grafana/алертов;
панельные правила бэкапов (etcd-истина) остаются, Prometheus-группа — независимый
от панели канал наблюдения.

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `pgworker_backup_full_age_seconds` | gauge | cluster, shard | возраст последнего ВАЛИДНОГО COMPLETED-полного (валидный = verify ≠ FAILED — семантика планировщика [backups/01-full-backup.md](../backups/01-full-backup.md) §6 и панельного `backup-full-stale`); нет валидного — серия не эмитится |
| `pgworker_backup_full_max_age_seconds` | gauge | cluster, shard | порог `full_max_age_sec` эффективной политики кластера (policy-ключ ?? дефолт конфига) — per-cluster порог алерта `age > max_age` без хардкода |
| `pgworker_backup_wal_lag_segments` | gauge | cluster, shard | отставание WAL-потока шарда, сегментов ([backups/02-wal-archive.md](../backups/02-wal-archive.md) §3) |
| `pgworker_backup_wal_last_uploaded_age_seconds` | gauge | cluster, shard | возраст последней доставки закрытого сегмента в S3 (тишина загрузок) |
| `pgworker_backup_verify_total` | counter | cluster, shard, result | исходы verify полных, `result` ∈ {ok,failed,transient} |
| `pgworker_backup_restore_total` | counter | cluster, shard, result | терминальные исходы restore-заявок, `result` ∈ {ok,failed} |
| `pgworker_backup_drill_total` | counter | cluster, shard, result | терминальные исходы дрилов восстановимости, `result` ∈ {ok,failed} |
| `pgworker_backup_orphan_skipped_pools_total` | counter | `pool` | проход свипера сирот исключил пул из union (пул недоступен/без кредов, листинг не удался — [backups/07-retention.md](../backups/07-retention.md) §3): реестр сирот неполный до следующего прохода (интервал 600 с); источник — марк-метод `BackupOrphanSkippedPool` |
| `pgworker_ha_failover_duration_seconds` | gauge | cluster, shard | длительность последнего ЗАВЕРШЁННОГО failover (смена лидера после недоступности; resolved − detected, [pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md)); открытого события/факта нет — серия не эмитится |
| `pgworker_ha_rebuild_duration_seconds` | gauge | cluster, shard | длительность последнего ЗАВЕРШЁННОГО rebuild ноды; факта нет — серия не эмитится |

Prometheus-алерты группы `backups` (rules.yml; severity — зеркало панельных
правил): `BackupFullStale` (critical; `age > max_age` — порог per-cluster из
серии), `BackupFullMissing` (critical; `max_age unless age` — валидного полного
нет вовсе), `BackupWalLagHigh` (warning; порог = дефолт `Wal:LagMaxSegments`,
конфиг per-install — в etcd per-cluster политики WAL нет), `BackupWalStale`
(warning; порог = дефолт `Wal:StaleSec`), `BackupVerifyFailed`,
`BackupRestoreFailed`, `BackupDrillFailed` (critical; `increase(total{result=
"failed"}[15m]) > 0`).

## 8. Приёмник WAL бэкапов (экспонирует сам приёмник)

Приёмник WAL — процесс внутри контейнера PG-ноды
([pgworker/01-placement.md](../pgworker/01-placement.md) §1) — несёт
СОБСТВЕННУЮ телеметрию своей работы (в отличие от §7 — серий воркера):
экспозиция — Kestrel `:8080` ВНУТРИ контейнера ноды, `/metrics`
(`Shared.Metrics`, Meter `PgWorker.PgNode`; порт наружу хоста НЕ
публикуется). Тот же порт `:8080` несёт HTTP API операций бэкапов
([pgnode/02-api.md](../pgnode/02-api.md), канал
[communications/02-channel-registry.md](../communications/02-channel-registry.md) №22);
новых серий приёмника операции не вводят — наблюдаемость операций
бэкапов: etcd-статусы воркера
([backups/05-etcd-contract.md](../backups/05-etcd-contract.md)) и журнал воркера.
Скрейп — Prometheus по `alias:8080` через
`Docker:ScrapeNetwork` (та же сеть/та же file_sd-запись, что patroni-нод
§5 — порт другой), джоба `pgnode-nodes` — http (закрытая сеть
скрейпа контура; прецедент — джоба `adminpanel`). Скрейп-сети нет —
телеметрия приёмника недоступна снаружи (локальная диагностика curl'ом
внутри контейнера), дашборд показывает пусто; состояние доставки при этом
полностью наблюдаемо сериями §7 и etcd-статусами
([backups/05-etcd-contract.md](../backups/05-etcd-contract.md)). Лейблы
конечны: `cluster`, `shard`, `node`, `role` (только у `walreceiver_role`).

| Имя | Тип | Лейблы | Смысл |
|---|---|---|---|
| `walreceiver_role` | gauge | cluster, shard, node, role | роль источника приёмника (REST-проба своей ноды, [backups/02-wal-archive.md](../backups/02-wal-archive.md) §1), `role` ∈ {master,sync,none} — 1 в активной серии |
| `walreceiver_streaming` | gauge | cluster, shard, node | 1 = replication-стрим активен (приём/доставка), 0 = ожидание (роль/условия/стоп-семантика) |
| `walreceiver_segments_delivered_total` | counter | cluster, shard, node | доставлено закрытых сегментов в S3 (сброс при рестарте процесса — counter от старта) |
| `walreceiver_bytes_delivered_total` | counter | cluster, shard, node | доставлено байт сегментов/`.history` |
| `walreceiver_backlog_bytes` | gauge | cluster, shard, node | размер WAL-хвоста к отправке: позиция записи сервера (walEnd потока) − последний подтверждённый хвост |
| `walreceiver_put_retries_total` | counter | cluster, shard, node | повторные попытки put (перебор целей пула) |
| `walreceiver_put_errors_total` | counter | cluster, shard, node | неудачные попытки put (вкл. исчерпание списка пула) |
| `walreceiver_reconnects_total` | counter | cluster, shard, node | переподключения к источнику (обрывы/смены роли) |
| `walreceiver_last_success_timestamp_seconds` | gauge | cluster, shard, node | unix-время последнего успешного put (знак живости доставки из первых рук) |

Отдельных Prometheus-алертов на серии приёмника НЕ заводится: здоровье
доставки закрывают алерты группы `backups` по сериям воркера §7
(BackupWalLagHigh/BackupWalStale — истина по объектам S3), серии приёмника —
дашборд и разбор инцидентов (who/where: какая нода стримит, где копится
хвост). Дашборд — `dashboards/backups.json` (панель per-node приёмников).
