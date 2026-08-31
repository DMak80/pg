# dev-stand — локальный docker-стенд AdminPanel

Канон — `../../arch/adminpanel/04-local-stand.md`; спецификация —
`docs/adminpanel/superpowers/2026-08-23-t10-dev-stand/spec.md`.

## Быстрый старт

```bash
# всё — одним скриптом; стенд = локальный запуск ПОЛНОЙ системы (AGENTS.md):
# панель (докер, localhost:5050, admin/admin), PG-шарды+эмуляторы, kafkaworker,
# PgWorker (deploy/) — и ВСЕ на одном etcd (as-etcd, источник правды, контур один)
cd dev-stand/adminpanel && checks/00-up.sh
# или: docker compose up -d            # стенд части: etcd+сид+панель (без PG/kafka/PgWorker)

open http://localhost:5050
```

Порт панели/логин переопределяются: `ADMINPANEL_URL` (чеки), env `AdminPanel__Auth__*`
сервиса `adminpanel`. Панель живёт в сети стенда и резолвит ноды напрямую
(`s1a:5432`, patroni `:8008`) — HostMap для adminpanel-стенда не нужен.

## Профили

| Профиль | Состав | Для чего |
|---|---|---|
| quick (по умолчанию) | etcd + seed | цикл бэкенд-разработки: API/алерты на сиде; Patroni/SQL-пробы закономерно падают (нод нет) |
| full | + s1a/s1b, s2a/s2b, hc1a/hc1b, hc2a/hc2b | live-пробы, failover, e2e |
| seed | + kafka-seed (разовый) | kafka-домен: 2 кластера `/kafka/` для чека `50-kafka-api.sh` (API на статике) |
| kafka | + kafkaworker | живой воркер: управление кафкой; входит в полный подъём `00-up.sh` (full+kafka) всегда; e2e — чек `55-kafka-e2e.sh` (волна C) |

⚠️ **kafka-профили не смешивать**: `--profile seed` и `--profile kafka`
одновременно не поднимать. Сид выглядит для живого воркера как заявки
(`pending` → provisioning, `events`-RUNNING без контейнеров →
supervisor-пересоздания, заявка ротации → journal-fail). Чек `50-kafka-api.sh`
сам останавливает поднятого `00-up.sh` воркера перед наливкой сида. Сид
поднимается разово: `docker compose --profile seed run --rm kafka-seed`
(идемпотентен); e2e-гейты (`55-kafka-e2e.sh`) идут на чистом `/kafka/`
(контроль: `etcdctl get /kafka/ --prefix --keys-only` пусто до старта).

## Kafka-чеки

- `checks/50-kafka-api.sh` — API kafka-домена на сиде: сам активирует
  `--profile seed` первым шагом; воркер в чеке не запускается, контейнеров
  не поднимает. Панель — хост-процессом как всегда.
- `checks/55-kafka-e2e.sh` (волна C) — полный цикл с живым воркером, с чистого
  состояния (14 подшагов: создание → автосинк → desired → негатив → группа+лаг →
  missing-ветка → lifecycle создание/удаление топиков из панели (t01) → обе
  отмены заявок → демонтаж broker-only → TO_REMOVE). Чек сам разбирает стенд,
  чистит kfw-объекты, собирает образ kafkaworker и поднимает `--profile kafka`;
  панель — хост-процессом (fresh-сборка с кодом волны C). Порт 2379 хоста не
  должен быть занят внешним etcd (на время прогона его можно остановить).

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
  && checks/20-alerts.sh && checks/30-failover.sh && checks/40-live-probes.sh
# панель уже в докере (шаг 8 подъёма); kafka: 50-й поднимает сид сам (профиль
# seed); e2e 55-й — отдельно с чистого состояния
```

Порядок важен: 30-й делает failover s1 (мастером остаётся s1b, s1a
rejoin'ится репликой) — 40-й рассчитан на эту топологию. Повторный
прогон — только с чистого состояния (`90-down.sh -v`).

Quick-режим: `checks/90-down.sh -v && docker compose up -d` → зелёные
`10-smoke-api.sh` и `20-alerts.sh` (quick-ветка); 30/40 требуют full.
После full-прогонов переход в quick — только с `-v` (lease-ключи
протухли, идемпотентный сид их не восстановит).

## Отладка

- контейнеры: `docker compose ps`, логи `docker compose logs <сервис>`;
  ноды — по имени сервиса (`s1a`…), контейнеры — `as-*` (не конфликтуют
  со стендом pg (этот монорепозиторий));
- etcd: `docker compose exec etcd etcdctl --endpoints=http://localhost:2379 get / --prefix --keys-only`;
- эмуляторы: `curl 127.0.0.1:8011/cluster | jq .` (8011/8012/8021/8022);
- панель (докер): логи `docker compose logs adminpanel`, API —
  `curl -b jar $BASE/api/overview`.
