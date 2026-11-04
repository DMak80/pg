# Runbook: вторые инстансы воркеров

Уровень 3. Эксплуатация двух инстансов каждого воркера: порты, обслуживание,
takeover. Родитель — [README.md](README.md); координация инстансов —
[pgworker/08-operability.md](../pgworker/08-operability.md) §2,
[kafkaworker/08-operability.md](../kafkaworker/08-operability.md) §2,
[valkeyworker/06-operability.md](../valkeyworker/06-operability.md) §2,
[s3worker/08-operability.md](../s3worker/08-operability.md) §2,
[etcdworker/06-operability.md](../etcdworker/06-operability.md) §2.

Деплой и dev-стенд поднимают по **два** инстанса каждого воркера
(PgWorker/KafkaWorker/ValkeyWorker) всегда — SPOF надзора устранён дефолтом,
не опцией. Смерть одного контейнера: lease-ключи (`/pgworker/api/*`,
`/instances/*`, `/claims/*`) гаснут ≤15 с, клэймы мигрируют второму инстансу —
надзор (provisioning/rebuild/ротации/бэкапы) не прерывается; панель и
Prometheus видят оба инстанса.

- **Порты** (прод-ряд, `deploy/.env`): первые — 8080 (pgw) / 8081 (kfw) /
  8082 (vwk); вторые — `PGW_API_HOST_PORT2=8083` / `KFW_API_HOST_PORT2=8084` /
  `VWK_API_HOST_PORT2=8085`. AdvertiseUrl вторых следует за их портом
  автоматически. На стенде kfw-2/vwk-2 публикуются только compose-DNS
  (`kafkaworker-2:8080`, `valkeyworker-2:8080`) — хост-порт не занимают.
- **Обслуживание одного инстанса**: `cd deploy && docker compose stop
  pgworker-2` (стенд: `docker stop as-kafkaworker-2`) — второй продолжает
  надзор; после работ `docker compose start pgworker-2` — оба ключа
  восстанавливаются ≤15 с. Останавливать ОБА — только для обслуживания etcd:
  надзор замирает, датаплейн (PG/Kafka/Valkey) живёт сам.
- **Ничего не дублировать**: тома снапшотов (`pgw-snapshots`/`kfw-snapshots`/
  `vw-snapshots`) и TLS-тома — ОБЩИЕ на пару инстансов (лидерство снапшотов
  мигрирует между ними); секреты per-install наследуются вторым инстансом
  идентично. Уникальность инстанса — случайный InstanceId, env не задаётся.
- **Проверка**: `dev-stand/adminpanel/checks/35-worker-second-instance.sh` —
  kill→takeover всех трёх воркеров с восстановлением; docker-E2E —
  `dotnet test src/tests/PgWorker.E2eTests … --filter
  FullyQualifiedName~SecondInstance`.
