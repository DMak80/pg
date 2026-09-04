# dev-stand — локальный docker-стенд AdminPanel

Канон — `../../arch/adminpanel/04-local-stand.md`; спецификация —
`docs/adminpanel/superpowers/2026-08-23-t10-dev-stand/spec.md`.

## Быстрый старт

```bash
# всё — одним скриптом; стенд = локальный запуск ПОЛНОЙ системы (AGENTS.md):
# панель (докер, localhost:5050, admin/admin), PG-шарды+эмуляторы, kafkaworker,
# PgWorker (deploy/) — и ВСЕ на одном etcd (as-etcd, источник правды, контур один)
cd dev-stand/adminpanel && checks/00-up.sh
# или: docker compose up -d   # стенд части: etcd+панель (без PG/kafka/PgWorker);
#                             # сиды — отдельно через checks/05-seed.sh

open http://localhost:5050
```

Порт панели/логин переопределяются: `ADMINPANEL_URL` (чеки), env `AdminPanel__Auth__*`
сервиса `adminpanel`. Панель живёт в сети стенда и резолвит ноды напрямую
(`s1a:5432`, patroni `:8008`) — HostMap для adminpanel-стенда не нужен.

## Профили

| Профиль | Состав | Для чего |
|---|---|---|
| quick (по умолчанию) | etcd + панель | цикл бэкенд-разработки: сиды — через `checks/05-seed.sh` (поднимает воркеров); Patroni/SQL-пробы закономерно падают (нод нет) |
| full | + s1a/s1b, s2a/s2b, hc1a/hc1b, hc2a/hc2b | live-пробы, failover, e2e |
| kafka | + kafkaworker | живой воркер: управление кафкой; входит в полный подъём `00-up.sh` (full+kafka) всегда; e2e — чек `55-kafka-e2e.sh` (волна C) |
| metrics | + prometheus (:9090), grafana (:3000, admin/admin), alertmanager (:9093) | мониторинг полной системы: дашборды, алерты §3.7; входит в 00-up.sh (arch/18 §5) |

## Мониторинг (профиль metrics)

- Сервисы: `prometheus` (:9090), `grafana` (:3000, admin/admin),
  `alertmanager` (:9093) — конфиги в `metrics/` (scrape-джобы, 8
  алерт-рулов §3.7, provisioning Grafana, JSON-дашборды workers/kafka/pg).
- Env-переменные (дефолты в `.env.example`): `METRICS_PROMETHEUS_PORT`,
  `METRICS_GRAFANA_PORT`, `METRICS_ALERTMANAGER_PORT` (коллизии хост-портов),
  `METRICS_ALERT_WEBHOOK_URL` (URL webhook-ресивера Alertmanager; пусто —
  алерты только в UI Prometheus/Alertmanager, Д4).
- Scrape: pgworker — `host.docker.internal:8080` (deploy-проект), kafkaworker/
  adminpanel — DNS сети стенда (`kafkaworker:8080`, `adminpanel:8080`),
  patroni — `hc1a..hc2b:8008` (эмуляторы отдают `/metrics`, arch/18 §2.5).
- Проверка: `checks/65-metrics.sh` — /metrics трёх сервисов, все scrape-джобы
  up, серии словаря arch/18 §2 в TSDB, rules, дашборды Grafana, живость
  Alertmanager и симуляция алерта ServiceDown (stop/start kafkaworker). Чек
  запускается и после серии чеков — сам поднимает остановленного kafkaworker'а.
- Канон мониторинга — `../../arch/18-metrics.md` §5.

## Сиды через API воркеров

Прямая запись etcdctl'ом упразднена (spec etcd-via-worker-api §3.5): демо-сид
pg-контура наливается `POST /api/seed/demo` PgWorker, kafka-домена — тем же
эндпоинтом KafkaWorker (обa за флагом `EnableSeedEndpoint`, в стеночных compose —
`true`). Идемпотентны: повторный вызов — no-op `{"seeded":false}`.

- `checks/05-seed.sh [pg|kafka|all]` (default `all`) — поднимает нужного
  воркера (pgworker — `deploy/`, kafkaworker — `--profile kafka`, хост-порт
  8082), ждёт `/healthz`, зовёт `POST /api/seed/demo`, в kafka-режиме
  дожидается живого lease-ключа `/kafkaworker/api/<id>`. Жизнью воркера ПОСЛЕ
  наливки НЕ управляет — решает потребитель сида.
- `00-up.sh` зовёт `05-seed.sh pg` после подъёма PgWorker (kafka-сид в полный
  подъём не входит — e2e 55-го идёт на чистом `/kafka/`).
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
# сиды — внутри: 00-up наливает pg-сид (05-seed.sh pg), 50-й — kafka-сид
# (05-seed.sh kafka, живой API, финальный stop воркера); e2e 55-й — отдельно
# с чистого состояния (разбирает стенд)
```

Порядок важен: 30-й делает failover s1 (мастером остаётся s1b, s1a
rejoin'ится репликой) — 40-й рассчитан на эту топологию. Повторный
прогон — только с чистого состояния (`90-down.sh -v`).

Quick-режим: `checks/90-down.sh -v && checks/05-seed.sh` → зелёные
`10-smoke-api.sh` и `20-alerts.sh` (quick-ветка); 30/40 требуют full.
После full-прогонов переход в quick — только с `-v` (lease-ключи
протухли, идемпотентный сид их не восстановит).

## Отладка

- контейнеры: `docker compose ps`, логи `docker compose logs <сервис>`;
  ноды — по имени сервиса (`s1a`…), контейнеры — `as-*` (не конфликтуют
  со стендом pg (этот монорепозиторий));
- etcd: `docker compose exec etcd etcdctl --endpoints=http://localhost:2379 get / --prefix --keys-only`;
- живые ключи API воркеров (lease TTL 15 c): `etcdctl get /pgworker/api/ --prefix`
  и `/kafkaworker/api/` — ключ есть = инстанс жив и URL валиден;
- эмуляторы: `curl 127.0.0.1:8011/cluster | jq .` (8011/8012/8021/8022);
- панель (докер): логи `docker compose logs adminpanel`, API —
  `curl -b jar $BASE/api/overview`.
