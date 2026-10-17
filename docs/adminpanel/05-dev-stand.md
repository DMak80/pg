# 05 — Dev-стенд и e2e

> Назад: [INDEX.md](INDEX.md) · Подсистема: `dev-stand/adminpanel/` (docker compose,
> проект `adminpanel-stand`). Канон стенда: [arch/adminpanel/04](../../arch/adminpanel/04-local-stand.md);
> быстрый старт и состав сервисов — `dev-stand/adminpanel/README.md`.

Стенд — ОДИН контур в одной сети `adminpanel-stand-net`: etcd (3 узла), панель,
канонический PG-demo-кластер (создаёт `00-up.sh` через `POST /api/clusters` —
движок провижинит Spilo/Patroni-ноды `pgw-demo-*` в той же сети), kafkaworker и
valkeyworker, PgWorker + ownS3 (deploy-контур подключается external;
`owns3:9000` внутри контура, `:9010` — хост-публикация наружу), мониторинг.
Профили compose: quick (etcd + панель), full (+ as-minio — переходный
read-only-архив), kafka/valkey/metrics. Эмуляторов Patroni, усыновлённых
as-нод и HostMap-словаря на стенде нет.

## E2E-прогон (порядок важен)

```bash
cd dev-stand/adminpanel
checks/90-down.sh -v                      # чистое состояние (обязательно)
checks/00-up.sh && checks/10-smoke-api.sh && checks/20-alerts.sh \
  && checks/30-failover.sh && checks/40-live-probes.sh && checks/60-move-ops.sh \
  && checks/65-metrics.sh
checks/90-down.sh -v                      # разбор
```

30-й меняет топологию demo (Patroni-failover: kill мастера по leader-ключу →
промоушен реплики → rejoin надзором движка) — 40-й рассчитан на неё.
Панель — сервис compose `adminpanel` (:5050); КАЖДЫЙ запуск `00-up.sh`
поднимает НОВОЕ окружение: предварительная зачистка контура встроена
первым шагом (вызов `90-down.sh -v`, движковые pgw-*/s3w-* — тоже) —
отдельный `90-down.sh` перед подъёмом не нужен.

## Грабли

- **`00-up.sh` падает на deploy-up: «required variable OWNS3_MESH_PEER1_* is
  missing»** — docker compose интерполирует ВЕСЬ файл `deploy/docker-compose.yml`,
  включая профилированные `owns3-m1/m2`, даже когда профиль `owns3-mesh` не
  стартует: `:?`-обязательные переменные peer-ключей меша роняют весь `up`.
  Лечение: дописать в локальный `deploy/.env` (в git не tracked) четыре
  значения `OWNS3_MESH_PEER1/2_{ACCESSKEY,SECRETKEY}` (dev-значения в стиле
  compose-комментария, например `os3m-peer-1` / `os3m-peer-1-secret`).

Подробности профилей, чеков и сетевой модели — канон
[arch/adminpanel/04](../../arch/adminpanel/04-local-stand.md).
