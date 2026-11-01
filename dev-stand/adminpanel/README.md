# dev-stand — локальный docker-стенд AdminPanel

Канон — `../../arch/adminpanel/04-local-stand.md`; спецификация —
`docs/adminpanel/superpowers/2026-08-23-t10-dev-stand/spec.md`.

## Быстрый старт

```bash
# всё — одним скриптом; стенд = ЯДРО + ПАНЕЛЬ на пустом контуре, ОДИН контур
# ОДНА сеть adminpanel-stand-net (AGENTS.md): панель (докер, localhost:5050,
# admin/admin), kafkaworker/valkeyworker, PgWorker/S3Worker (deploy/,
# contour-override), прометей/графана — и ВСЕ на одном etcd-кластере
# as-etcd-1/2/3 (источник правды, 3 узла — зеркало прода, arch/04 §8).
# Ресурсы (PG-кластеры, S3-пулы, valkey/kafka демо) НЕ создаются — их создаёт
# оператор вручную через панель; сиды чеков — checks/05-seed.sh вручную.
cd dev-stand/adminpanel && checks/00-up.sh
# или: docker compose up -d   # стенд части: etcd-кластер+панель (без воркеров);
#                             # сиды — отдельно через checks/05-seed.sh

open http://localhost:5050
```

Порт панели/логин переопределяются: `ADMINPANEL_URL` (чеки), env `AdminPanel__Auth__*`
сервиса `adminpanel`. Панель живёт в стендовой сети: etcd/kafka — по
compose-DNS, ноды кластеров (созданных вручную через панель) — по advertised
portalloc-адресам (`host.docker.internal:15xxx/:17xxx/:18xxx`, extra_hosts) —
HostMap не нужен.

## Профили

| Профиль | Состав | Для чего |
|---|---|---|
| quick (по умолчанию) | etcd + панель | цикл бэкенд-разработки: сиды — через `checks/05-seed.sh` (поднимает воркеров); Patroni/SQL-пробы закономерно падают (нод нет) |
| full | + as-minio (переходный архив, :9000/:9001) — поднимается вручную, в подъём `00-up.sh` НЕ входит | live-пробы, failover, e2e |
| kafka | + kafkaworker | живой воркер: управление кафкой; входит в `00-up.sh` всегда; e2e — чек `55-kafka-e2e.sh` (волна C) |
| metrics | + prometheus (:9090), grafana (:3000, admin/admin), alertmanager (:9093) | мониторинг полной системы: дашборды, алерты §3.7; входит в 00-up.sh (arch/18 §5) |

## Мониторинг (профиль metrics)

- Сервисы: `prometheus` (:9090), `grafana` (:3000, admin/admin),
  `alertmanager` (:9093) — конфиги в `metrics/` (scrape-джобы, 8
  алерт-рулов §3.7, provisioning Grafana, JSON-дашборды workers/kafka/pg).
- Env-переменные (дефолты в `.env.example`): `METRICS_PROMETHEUS_PORT`,
  `METRICS_GRAFANA_PORT`, `METRICS_ALERTMANAGER_PORT` (коллизии хост-портов),
  `METRICS_ALERT_WEBHOOK_URL` (URL webhook-ресивера Alertmanager; пусто —
  алерты только в UI Prometheus/Alertmanager, Д4).
- Scrape: таргеты — имена стендовой сети `adminpanel-stand-net`
  (`pgworker:8080`, `kafkaworker:8080`, `adminpanel:8080`; patroni-ноды —
  по alias portalloc-скрейпа, отдельной сети мониторинга на стенде нет).
- Проверка: `checks/65-metrics.sh` — /metrics трёх сервисов, все scrape-джобы
  up, серии словаря arch/18 §2 в TSDB, rules, дашборды Grafana, живость
  Alertmanager и симуляция алерта ServiceDown (stop/start kafkaworker). Чек
  запускается и после серии чеков — сам поднимает остановленного kafkaworker'а.
- Канон мониторинга — `../../arch/18-metrics.md` §5.

## Хранилище бэкапов (arch/19 + arch/24)

Хранилище бэкапов стенда — S3-пул (кластер `/s3/`), который провижинит
S3Worker по заявке оператора через панель (контейнеры `s3w-<C>-<I>`,
bucket создаётся провижинингом пула; `00-up.sh` ничего не наливает).
Грань панели «Хранилище бэкапов» до создания пула — configured с
health-отказом — ожидаемое состояние пустого контура. Панель берёт endpoint
и креды пула из снапшота S3-домена per-cluster (read-only ключ `panel`
secrets-ключа `/s3/clusters/<C>/secrets`); из env остаётся только bucket
(`AdminPanel__Backups__S3__Bucket` сервиса `adminpanel`). Проверка грани —
`checks/45-backups-storage.sh` (после создания пула вручную через панель;
root-креды налива mc берёт из secrets-ключа etcd). Standalone-`owns3`
deploy-проекта — ручной подъём при необходимости (проверка — чек `46-owns3.sh`
после ручного подъёма).

Кластеры создаются вручную через панель (API воркеров); стенд задаёт
`PGW_CONTOUR_NETWORK` = `adminpanel-stand-net` (режим сети контура движков
pg/vwk/s3w — ноды в одной сети со всем контуром).

Профиль `full`: `as-minio` (S3 API `:9000`, консоль `:9001`, креды
`minioadmin`/`minioadmin`) — переходный read-only-архив старых цепочек:
новых объектов не получает, снос — по критерию готовности
(`../OwnS3/docs/owns3-migration.md`).

**HA-etcd (t09)**: контур всегда 3 узла (`as-etcd-1/2/3`, порты
2379/2381/2383, env `ETCD{1,2,3}_HOST_PORT` при коллизиях);
отказоустойчивость проверяет `checks/43-etcd-ha.sh` (stop узла → кворум
пишет, master-ключ жив → start → 3/3).

## Сиды через API воркеров

Прямая запись etcdctl'ом упразднена (spec etcd-via-worker-api §3.5).
Pg-демо-контур сидом НЕ наливается — кластер создаётся вручную через панель
(API воркера); kafka/valkey-домены наливаются `05-seed.sh`
(`POST /api/seed/demo` соответствующего воркера, за флагом
`EnableSeedEndpoint`). Идемпотентны: повторный вызов — no-op.

- `checks/05-seed.sh [kafka|valkey|all]` (default `all`) — поднимает нужного
  воркера (kafkaworker — `--profile kafka`, хост-порт 8082; valkeyworker —
  `--profile valkey`), ждёт `/healthz`, зовёт `POST /api/seed/demo`, в
  kafka-режиме дожидается живого lease-ключа `/kafkaworker/api/<id>`. Жизнью
  воркера ПОСЛЕ наливки НЕ управляет — решает потребитель сида.
- `05-seed.sh` — НЕ часть штатного подъёма: его зовут чеки `50`/`51` и
  ручные прогоны.
- Совместимость kafka-сида с живым воркером: у сида нет контейнеров брокеров →
  пробы воркера слепые (arch/16 §5 C: слепая проба = бездействие) → сидовые
  заявки (lifecycle/rotate/rebalance) не исполняются, данные сида стабильны.
  End-state полного прогона — «kafkaworker остановлен после сида»: чек 50
  сам останавливает его финальным шагом; изолированный `05-seed.sh kafka`
  оставляет воркера поднятым (безопасно — см. выше).

## Kafka-чеки

- `checks/50-kafka-api.sh` — API kafka-домена на сиде, налитом ЧЕРЕЗ API
  живого воркера: `05-seed.sh kafka` поднимает kafkaworker и ждёт живой ключ
  `/kafkaworker/api/<id>` (без него мутации панели — 503); все мутации шагов
  1–13 идут панель → прокси (`WorkerApiGateway`) → API живого воркера; финал —
  `docker compose stop kafkaworker` + kafka-грань алерта
  `worker-api-unreachable`.
- `checks/55-kafka-e2e.sh` (волна C) — полный цикл с живым воркером, с чистого
  состояния (15 подшагов: создание → автосинк → desired → негатив → группа+лаг →
  missing-ветка → lifecycle создание/удаление топиков из панели (t01) → обе
  отмены заявок → демонтаж broker-only → ребалансировка → TO_REMOVE). Чек сам
  разбирает стенд, чистит kfw-объекты, собирает образ kafkaworker и поднимает
  `--profile kafka`; панельные мутации физически идут через прокси в API
  живого воркера. Порт 2379 хоста не должен быть занят внешним etcd.

## Valkey-чеки

- `checks/52-valkey-contour.sh` — ручная приёмка контурного режима сетей
  ValkeyWorker (arch/21 §2): создание valkey-кластера (уникальное имя) через
  панель → wait Active/нода RUNNING → контейнер `vwk-<C>-node1` в сети
  `adminpanel-stand-net`, endpoints — advertised `host:port` (portalloc) →
  удаление через панель → чистота (ни контейнера, ни ключей
  `/valkey/clusters/<C>/`).

## HostMap

Панель в докере резолвит стендовые адреса сетью compose — HostMap для
adminpanel-стенда не нужен. `appsettings.Development.json` несёт HostMap для
хостовых сценариев против PgWorker-стенда (portalloc 15000-18999, kafka
160xx). Ключи записываются как `host__port` (двойное подчёркивание вместо
двоеточия): конфиг-провайдеры .NET режут ключи секций по `:`, словарь с
`host:port`-ключами биндится пустым; `HostMapResolver` принимает оба формата.

## E2E (полный прогон; с чистого состояния)

```bash
checks/90-down.sh -v        # если стенд уже поднимался
checks/00-up.sh && checks/10-smoke-api.sh && checks/15-cluster-create.sh \
  && checks/20-alerts.sh && checks/30-failover.sh && checks/40-live-probes.sh \
  && checks/50-kafka-api.sh
# полный прогон 10–60 — ПОСЛЕ ручного создания ресурсов через панель
# (демо-кластер / S3-пул) и/или сидов checks/05-seed.sh; e2e 55-й —
# отдельно с чистого состояния (разбирает стенд)
```

Порядок важен: 30-й делает Patroni-failover shard1 (kill мастера → промоут
реплики → rejoin надзором движка) — 40-й гоняется по фактической топологии
(мастер по API). Повторный прогон — только с чистого состояния
(`90-down.sh -v`).

Quick-режим: `checks/90-down.sh -v && checks/05-seed.sh` → зелёные
`10-smoke-api.sh` и `20-alerts.sh` (quick-ветка); 30/40 требуют full.
После full-прогонов переход в quick — только с `-v` (lease-ключи
протухли, идемпотентный сид их не восстановит).

## Отладка

- контейнеры: `docker compose ps`, логи `docker compose logs <сервис>`;
  канонические ноды demo — контейнеры движка `pgw-demo-*` (`docker ps | grep pgw-`);
- etcd: `docker compose exec etcd1 etcdctl --endpoints=http://localhost:2379 get / --prefix --keys-only`;
- живые ключи API воркеров (lease TTL 15 c): `etcdctl get /pgworker/api/ --prefix`
  и `/kafkaworker/api/` — ключ есть = инстанс жив и URL валиден;
- Patroni нод: portalloc-порты из `/pgworker/portalloc/demo` ( REST `:18xxx`);
- панель (докер): логи `docker compose logs adminpanel`, API —
  `curl -b jar $BASE/api/overview`.

## TLS API воркеров (pgworker + kafkaworker)

HTTP API воркеров — HTTPS-грань ЕДИНОЙ per-install API-PKI
`kfw-install-ca` (arch/14 §1.1 / arch/16 §1.1): mTLS (клиентский серт)
обязателен только на `/api`; `/healthz` и `/metrics` — открытая грань
без клиентского серта (arch/18 §3/§5.2). Ключевая аутентификация
X-Api-Key удалена у обоих воркеров.

- **Генерация пакета**: `bash deploy/tls/gen.sh` — создаёт в `deploy/tls/`
  `ca.pem`, серверные `server.crt/key` (kafkaworker) и `pgserver.crt/key`
  (pgworker; SAN: pgworker, localhost, host.docker.internal, 127.0.0.1),
  клиентские `panel.crt/key`, `seed.crt/key` (каталог gitignored). `00-up.sh` делает это
  идемпотентно (только если `ca.pem` нет).
- **pgworker**: volume `pgw-api-tls:/tls:ro` (наполняет `00-up.sh`) + env
  `PGW_API_TLS_{CERT,KEY,CLIENT_CA}_PATH` (deploy/docker-compose.yml).
- **kafkaworker**: bind `../../deploy/tls:/tls:ro` + env `KFW_API_TLS_*_PATH`
  (compose); панель: `../../deploy/tls:/tls-workers:ro` + env
  `ADMINPANEL__WORKERS__WORKERTLS__*_PATH` (единый клиентский серт panel
  на оба API и доверие ca.pem).
- **Prometheus**: mount `../../deploy/tls:/tls:ro` + `tls_config`
  (канон arch/18 §5.2 — только `ca_file`, без клиентского серта).
- **Чеки**: прямые пробы `/healthz` и `/metrics` — `https` + `--cacert`
  без клиентского серта; `/api`-вызовы чеков — клиентский серт `seed.crt`;
  остальные — через панель, чей `IWorkerApiGateway` сам подписывает запросы
  клиентским сертом (mTLS-проксирование `panel.crt`).
