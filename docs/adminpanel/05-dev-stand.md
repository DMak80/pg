# 05 — Dev-стенд и e2e

> Назад: [docs/README.md](README.md) · Подсистема: `dev-stand/` (docker compose,
> проект `adminpanel-stand`). Канон: [arch/04](../arch/04-local-stand.md);
> быстрый старт — `dev-stand/README.md`.

Кратко: quick-профиль (по умолчанию) — etcd + идемпотентный сид контроль-плейна;
full — + 4 PG-ноды (2 шарда: мастер+реплика) и 4 patroni-эмулятора `hc*`
(master-lease TTL 5 c в etcd). Панель всегда на хосте (`dotnet run`, :5000);
compose-адреса проб маппятся `HostMap` на хост-порты 5433–5436/8011–8022.

## Состав

- `docker-compose.yml`: `etcd` (2379), `seed` (alpine + etcdctl из distroless-образа:
  официальный etcd без shell), `s1a/s1b/s2a/s2b` (5432→5433–5436, физреплики,
  self-healing мастеров), `hc1a…hc2b` (8008→8011–8022, python-эмуляторы Patroni:
  `/cluster`, `/primary`, `/replica`; пишут `master`/`leader`/`optime`/`members`/
  `/cluster/nodes/*` c lease, пока жива PG ноды).
- `seed.sh`: значения = `EtcdSeed` интеграционных тестов (= unit-фикстуры
  `EtcdFixtures/*.json`), времена статусов динамические от `now`.
- `checks/`: `00-up.sh` (full-up + wait-healthy + БД demo + 13 схем + sync-names),
  `10-smoke-api.sh`, `20-alerts.sh`, `30-failover.sh`, `40-live-probes.sh`,
  `90-down.sh [-v]`.

## E2E-прогон (порядок важен)

```bash
# терминал 1: панель
dotnet run --project src/AdminPanel.Api
# терминал 2:
cd dev-stand
checks/90-down.sh -v                      # чистое состояние (обязательно)
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh
checks/90-down.sh -v                      # разбор
```

30-й делает failover s1 (мастер s1b, s1a rejoin-ится репликой) — 40-й рассчитан на
эту топологию. Quick-режим: `90-down.sh -v && docker compose up -d` → зелёные
10/20 (quick-ветка); 30/40 требуют full.

## Чек-лист «изменить стенд»

1. Данные сида: `seed.sh` + `EtcdSeed` + `EtcdFixtures/*.json` — синхронно
   (расхождение = зелёный стенд и красные тесты, и наоборот).
2. Новая аномалия для UI/алертов: статус-ключ в сидe + ожидание в 20-alerts;
   динамические времена — от `now`, чтобы аномалии были «живыми».
3. Топология/порты: только через arch/04 §1 (порты зафиксированы контрактом) +
   `HostMap` в `appsettings.Development.json`.
4. Повторный прогон — всегда с `90-down.sh -v`.

## Грабли

- **Повторный прогон без `-v` флакает**: lease-ключи (master/members/nodes) после
  остановки эмуляторов протухают, идемпотентный сид их не восстанавливает; full →
  quick — тоже только с `-v` (t10).
- **SyncRep-ловушка после promote**: коммиты висят без реплики — 30-й чек снимает
  `synchronous_standby_names` сразу после promote и возвращает после rejoin (урок
  `../pg`).
- **Контейнеры `as-*`** (container_name): не конфликтуют со стендом `../pg`, который
  порты на хост не публикует; имена сервисов (`etcd`, `s1a`…) — канон, на них DSN и
  скрипты.
- **Официальный etcd-образ distroless** (нет shell) — seed-образ это `alpine:3.20` +
  скопированный `etcdctl` 3.5.21.
- **Тики против TTL**: панель 3 c / пробы 15 c / lease 5 c — все ожидания чеков
  ретраи с запасом (≤15 c API, ≤40 c пробы); lease-гашение ассертится в etcd до
  проверки панели.
