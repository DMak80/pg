# PgNode: размещение и жизненный цикл процесса

Уровень 3. Где и как живёт агент: контейнер ноды, supervisord, порты, сеть,
env без секретов, tmpfs, ресурсы. Секреты — [08-operability.md](08-operability.md)
§1; доставка — [pgworker/04-security.md](../pgworker/04-security.md) §3;
модель ноды — [pgworker/01-placement.md](../pgworker/01-placement.md).

## 1. Процесс в контейнере ноды

PgNode — программа `pgnode` supervisord'а образа `pgworker-node`
(self-contained .NET-бинарь; отдельного контейнера/образа нет). Supervisord
автостартует ТОЛЬКО pgnode: программы `spilo` и `pg_doorman` —
`autostart=false`; их стартует агент после доставки секретов (без них
процессы не жизнеспособны). Процесс живёт под supervisord'ом с autorestart;
permanent-условия приёмника (несовместимый `wal_segment_size`, разрыв LSN
протокола, невалидный env) — состояние статус-ключа `failed_permanent`, а
НЕ exit-код: воркер не может перезапустить процесс чужого контейнера,
перезапуск permanent-причины не лечит — лечение за воркером/оператором.
Отсутствие слота/кредов/policy — ожидание (retry-цикл), НЕ permanent.

Внутри контейнера всё общается через localhost — работает и в plain, и в
swarm (у swarm нет host-network и «подов»; sidecar'ы отдельными сервисами
громоздки).

## 2. Порты и сеть

| Порт | Назначение | Публикация |
|---|---|---|
| `:8080` | `/metrics` (Prometheus через ScrapeNetwork) + HTTP API операций PgWorker (`/api/backup/full*`, `/api/ops/*`, `/api/node/*`) | наружу хоста НЕ публикуется (docker и swarm) |
| `:8081` | mTLS-сервер загрузочного агента (доставка секретов/применение материала; только для PgWorker) | наружу хоста НЕ публикуется |

Адресация API — `alias:8080` по имени ноды в общей сети воркера и нод
(инвариант поставки: контейнеры воркера и нод ВСЕГДА в одной сети — контур
или per-cluster сеть развёртывания,
[pgworker/01-placement.md](../pgworker/01-placement.md) §5.1). Недостижимость
alias — transient тика воркера с journal-фактом адреса, НЕ краш тика.
`:8081` — ВНУТРИ docker-сети кластера; верификация: воркер проверяет
серверный серт агента по per-install REST-CA (SAN имени ноды); агент
проверяет клиентский серт воркера по публичному CA из env контейнера +
EKU ClientAuth + CN=pgworker. Граница канала — ПОРТ: маршруты
`/api/bootstrap*` обслуживаются ТОЛЬКО листенером `:8081`; по plaintext
`:8080` — `404`.

## 3. Env контейнера (без секретов)

Env ноды секретов НЕ несёт: `docker inspect` не раскрывает ничего
конфиденциального. Только неконфиденциальное:

- идентичность: `PGW_BACKUP_CLUSTER/SHARD/NODE` (пишет EnsureNode),
  `SCOPE`, `PGW_NODE_*`, `PGW_MASTER_KEY`, `PGW_DOORMAN_PORT`,
  `USE_DATA_DIR_FOR_WAL`;
- etcd: `PGW_ETCD`, `ETCD3_HOSTS` (формат `host:port` БЕЗ scheme);
- S3: `PGW_BACKUP_S3_BUCKET` (per-install bucket), опц.
  `PGW_BACKUP_S3_CA_PEM`, `PGW_BACKUP_STAGING_DIR`,
  `PGW_WAL_PARTIAL_INTERVAL_SEC` (доставка `Wal:PartialIntervalSec`),
  `PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC` (доставка
  `Wal:PartialCompactIntervalSec`);
- публичные CA: `SSL_RESTAPI_CA` (REST-слой), CA верификации клиентов
  mTLS-канала агента;
- шаблон пулера: `DOORMAN_CONFIG_TEMPLATE` (пароль — плейсхолдер
  `${PGW_SU_PASSWORD}`).

Всё «куда бэкапиться» (роль источника, цели S3, креды, слот) агент
резолвит сам из etcd живым резолвом на каждом цикле подключения —
[03-wal-receiver.md](03-wal-receiver.md) §4. Нода, созданная до включения
приёмника в образ (без env `PGW_BACKUP_*`), приёмника не несёт:
WAL-архивация с неё невозможна, обновление образа — пересозданием
(rebuild/recreate, данные в volume сохраняются).

## 4. tmpfs и данные

Секретные файлы — ТОЛЬКО tmpfs `/run/pgw` (в памяти контейнера, 0600
postgres): кэша секретов на диске НЕТ (writable-слой/тома секретов не
несут). Серты материализуются в `/run/certs/*` (спило-константы
`get_placeholders`); процессы Spilo получают секреты env'ом ПРОЦЕССА:
обёртка spilo-supervised.sh перед launch.sh экспортирует
`SPILO_CONFIGURATION`, `PGPASSWORD_SUPERUSER`, `PGPASSWORD_STANDBY`,
`SSL_RESTAPI_{CERTIFICATE,PRIVATE_KEY}` из tmpfs-файлов агента —
docker-env контейнера при этом чист. Права доставляемых файлов обеспечивает
поставщик (tmpfs 0600 postgres): root-владелец файла — PermissionError
Patroni на reload.

Локальные каталоги на data-volume ноды: staging полного/verify —
`Staging:Dir` (дефолт `/home/postgres/pgdata/backup-staging`), scratch
дрилла — `/home/postgres/pgdata/backup-drill/<id>/pgdata`. Staging-ТОМА
приёмника НЕТ: локальный буфер сегментов — кэш без гарантий durability;
всё неподтверждённое перезаказывается у источника (RPO-инвариант
[03-wal-receiver.md](03-wal-receiver.md) §3), терять нечего.

## 5. Ресурсы

Отдельных контейнеров/лимитов для операций бэкапов НЕТ — verify/restore/
drill исполняет PgNode внутри контейнера ноды и делит его ресурсы с нодой
(стрим/полный — и так делили). Память приёмника: буфер сегмента
`wal_segment_size` — обычно 16 МиБ + рабочие множества фоновых операций;
скачанные наборы лежат на data-volume ноды: staging полного/verify (ENOSPC —
реактивный FAILED), scratch дрилла. Размеры — операционный риск
[backups/09-operability.md](../backups/09-operability.md) §5.
