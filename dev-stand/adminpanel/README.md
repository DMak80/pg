# dev-stand — локальный docker-стенд AdminPanel

Канон — `../../arch/adminpanel/04-local-stand.md`; спецификация —
`docs/adminpanel/superpowers/2026-08-23-t10-dev-stand/spec.md`.

## Быстрый старт

```bash
# терминал 1 — панель (localhost:5050, admin/admin)
dotnet run --project src/AdminPanel.Api

# терминал 2 — стенд
cd dev-stand/adminpanel && checks/00-up.sh        # full: etcd+seed+2 PG-шарда+эмуляторы
# или: docker compose up -d            # quick: только etcd+сид (без PG/проб)

open http://localhost:5050
```

Порт панели/логин переопределяются: `ADMINPANEL_URL`, `AdminPanel:Auth`.

## Профили

| Профиль | Состав | Для чего |
|---|---|---|
| quick (по умолчанию) | etcd + seed | цикл бэкенд-разработки: API/алерты на сиде; Patroni/SQL-пробы закономерно падают (нод нет) |
| full | + s1a/s1b, s2a/s2b, hc1a/hc1b, hc2a/hc2b | live-пробы, failover, e2e |
| seed | + kafka-seed (разовый) | kafka-домен: 2 кластера `/kafka/` для чека `50-kafka-api.sh` (API на статике) |
| kafka | + kafkaworker | живой воркер: e2e полного цикла kafka (чек `55-kafka-e2e.sh`, волна C) |

⚠️ **kafka-профили не смешивать**: `--profile seed` и `--profile kafka`
одновременно не поднимать. Сид выглядит для живого воркера как заявки
(`pending` → provisioning, `events`-RUNNING без контейнеров →
supervisor-пересоздания, заявка ротации → journal-fail). Сид поднимается
разово: `docker compose --profile seed run --rm kafka-seed` (идемпотентен);
e2e-гейты (`55-kafka-e2e.sh`) идут на чистом `/kafka/` (контроль:
`etcdctl get /kafka/ --prefix --keys-only` пусто до старта).

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

`appsettings.Development.json` мапит стендовые адреса на хост-порты
(5433–5436 → PG-ноды, 8011/8012/8021/8022 → эмуляторы). Ключи записываются
как `host__port` (двойное подчёркивание вместо двоеточия): конфиг-провайдеры
.NET режут ключи секций по `:`, словарь с `host:port`-ключами биндится
пустым; `HostMapResolver` принимает оба формата.

## E2E (полный прогон; с чистого состояния)

```bash
checks/90-down.sh -v        # если стенд уже поднимался
# панель: dotnet run --project src/AdminPanel.Api (отдельный терминал)
checks/00-up.sh && checks/10-smoke-api.sh && checks/15-cluster-create.sh \
  && checks/20-alerts.sh && checks/30-failover.sh && checks/40-live-probes.sh
# kafka: 50-й поднимает сид сам (профиль seed); e2e 55-й — отдельно с чистого состояния
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
- панель: логи запуска `/tmp/adminpanel.log` (если через nohup), API —
  `curl -b jar $BASE/api/overview`.
