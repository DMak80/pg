# Бэкапы: эксплуатация

Уровень 3. Функция «эксплуатация»: конфигурация `PgWorker:Backups`,
ресурсный профиль операций, бюджеты зависших операций, источник/HA и риски.
Процессы — [processes/](processes/); ключи —
[05-etcd-contract.md](05-etcd-contract.md); секреты —
[08-secrets.md](08-secrets.md).

## 1. Конфигурация

Секция `PgWorker:Backups`:

```
Enabled=false
S3 { Bucket, CaBundlePath="" } # путь ca.pem per-install CA — доверие
                               # https-endpoint (на все цели); Endpoint/Region/
                               # AccessKey/SecretKey/PathStyle/AdvertisedEndpoint
                               # УШЛИ — цели даёт пул per-cluster, креды пулов —
                               # per-cluster secrets в etcd
Policy { Retention { Days=1, Weeks=1, Months=1 }, FullMaxAgeSec=86400,
         VerifyOnCreate=true, VerifyIntervalSec=604800 }
         # период перепроверки оставшихся полных — verify.interval_sec
         # policy-ключа кластера перекрывает
Staging { Dir=/home/postgres/pgdata/backup-staging }
         # каталог staging полного в data-volume ноды;
         # Staging:QuotaBytes упразднён — ENOSPC реактивный
Job { FullTimeoutSec=21600, VerifyTimeoutSec=21600, RestoreTimeoutSec=86400 }
         # бюджеты зависших операций; Agent { Cpu, Mem } и Job:Image УШЛИ —
         # контейнеров/джобов бэкапов нет, операции живут в приёмниках нод
Retry { BaseSec=300, MaxSec=3600 } # бэкофф переснятия FAILED
Wal { PartialIntervalSec=3, VerifyIntervalSec=30, LagMaxSegments=1024,
      StaleSec=300 }
         # PartialIntervalSec — период .partial-доотправки префикса живого
         # сегмента, доставляется в контейнер ноды env
         # PGW_WAL_PARTIAL_INTERVAL_SEC — как PGW_BACKUP_STAGING_DIR;
         # период list/контроля цепочки, порог отставания в сегментах,
         # порог тишины загрузок; конфигурации приёмника отдельной нет —
         # приёмник внутри образа ноды управляется etcd-контрактом
Retention { IntervalSec=600, KeepFailed=20 }
         # период ретенционного прохода per-shard и число хранимых
         # FAILED-записей на шард
Quota { Bytes=0, WarnPercent=80, CritPercent=90 }
         # квота bucket установки для ключа/алертов хранилища;
         # Bytes=0 — квота не задана, пишется только used_bytes
Restore { RecoveryTimeoutSec=1800 }
         # бюджет локального наката WAL restore/drill-операции — фаза
         # recovering; исчерпание → FAILED «target не достигнут»
Drill { IntervalDays=1, TimeoutSec=21600 }
         # дефолт периода дрилов шардов кластеров без policy-поля
         # drill.interval_days, 0 — глобальное выключение новых запусков
         # при доводке активных; бюджет активного дрилла от started_unix
         # → FAILED drill-timeout; бюджет наката WAL внутри
         # drill-операции — общий Restore:RecoveryTimeoutSec
Supervisor { IntervalSec=600, OrphanTtlSec=604800 }
         # период сверок per-cluster и глобального лидер-прохода;
         # TTL сирот, 0 — только алерт
```

Валидация старта: `Enabled=true` → непустой `S3:Bucket` (env
`PGW_BACKUP_S3_BUCKET`) — fail-fast (проверок наличия кредов пулов в etcd на
старте НЕ вводится: креды появляются/исчезают динамически, резолв — живой);
отрицательные таймауты/TTL супервизора — fail-fast; отрицательные
`Drill:IntervalDays`/`Drill:TimeoutSec` — fail-fast (`0` интервала валиден —
глобальное выключение новых запусков).

## 2. Ресурсный профиль операций

Отдельных контейнеров/лимитов для операций бэкапов НЕТ — verify/restore/drill
исполняет процесс PgNode внутри контейнера ноды и делит его ресурсы с нодой
(стрим/полный — и так делили). Память приёмника: буфер сегмента
`wal_segment_size` — обычно 16 МиБ + рабочие множества фоновых операций;
скачанные наборы лежат на data-volume ноды: staging полного/verify — каталог
`Staging:Dir` (ENOSPC — реактивный FAILED), scratch дрилла —
`backup-drill/<id>/`; размеры — операционный риск §5.

## 3. Бюджеты зависших операций

Возрастные бюджеты (полный/verify/restore/drill) и порядок cancel →
FAILED/переснятие — [E. BackupSupervisor](processes/e-supervisor.md) §4;
бюджет наката WAL внутри restore/drill — `Restore:RecoveryTimeoutSec` из
команды.

## 4. Источник и HA

**Реплика-источник**: штатно полный бэкап не трогает мастер; fallback на
мастер — журнал-факт (`role=master`) + I/O одной операции (не потоковой).
Смерть источника посреди бэкапа → `pg_basebackup` теряет соединение,
операция завершается FAILED (приёмник чистит staging), переснятие (бэкофф
[01-full-backup.md](01-full-backup.md) §6).

**Слот на источнике**: WAL копится, пока приёмник не подтвердил доставку;
потолок — `max_slot_wal_keep_size` (уже в каноне нод,
[pgworker/01-placement.md](../pgworker/01-placement.md) §2 P3/P4): смерть
обоих приёмников → WAL в пределах лимита → по исчерпании слот инвалидируется
PG → цепочка рвётся → алерт панели + переснятие полного. Диск источника
переполнить нельзя. Слоты per-instance: инвалидация одного не касается
второго. **Patroni/HA не затрагивается**: бэкапы — сторонние клиенты нод;
изменений в конфиги нод/HA-контура подсистема не вносит.

## 5. Риски

| Риск | Закрытие |
|---|---|
| ownS3 — ещё один сервис стенда (порты/ресурсы/зачистка) | фиксированные порты только стенда (9010, healthz — `/healthz`); стенд: адресация по имени стендовой сети (`owns3:9000`), хост-порт `:9010` — наружу; переходный as-minio до критерия `../OwnS3/docs/owns3-migration.md`; testcontainers — с динамическими портами |
| Сетевой режим контура ломает прод-поведение | режим строго опционален (пусто = прод-канон); юнит-таблицы режимов; docker-E2E PgWorker в мерж-гейте |
| Компрометация S3-кредов per-install = доступ ко ВСЕМ бэкапам установки | env-секреты вне git/etcd; отдельный bucket/креды per-install; ротация — перегенерация пакета (как TLS) |
| Слот при смерти приёмников копит WAL на источнике | потолок `max_slot_wal_keep_size` уже в каноне нод — диск не переполнится; инвалидация слота → авто-recreate слота воркером: потеря одного источника закрывается живым (цепочка продолжается вторым, потерянный возвращается от хвоста S3), потеря всех → WAL_CHAIN_ERROR + пересъём полного; панельный `slot-wal-lost` — страховка (ремеди WorkerAuto) |
| Поведение физического слота реплики при её promote (двойная архивация) | promote сохраняет слоты; приёмник после promote возобновляет от хвоста S3 уже новой TLI (стартовая позиция явная, restart_lsn для старта не используется) — проверяется docker-E2E |
| 58P01 после фейловера при хвосте S3 ниже точки форка текущего таймлайна (стрим не стартует, цикл ретраев) | досыпка старого таймлайна / прыжок-fallback — tli-процедура; зазор fallback-прыжка — поле `gap` wal-ключа + форс-пересъём полного + алерт `backup-wal-tli-gap` |

## 6. Наблюдаемость

Статусы — ключи `/pgworker/backups/*` ([05-etcd-contract.md](05-etcd-contract.md));
метрики приёмника — Prometheus `:8080` внутри контейнера ноды
([metrics/02-dictionary.md](../metrics/02-dictionary.md) §8); живость циклов воркера — серия
`worker_loop_last_success_timestamp_seconds{loop=…}`; алерты панели —
`backup-full-stale`, `backup-verify-failed`, `backup-storage-quota`,
`backup-pool-unreachable`, `backup-pool-missing`, `backup-deleting-stuck`,
`backup-wal-tli-gap`, `slot-wal-lost`, `OrphanSweepStalled` (расчёт — над
снапшотом, [07-retention.md](07-retention.md) §6).

## 7. Потеря WAL-слота (`wal_status='lost'`)

Что происходит: при длительном неподтверждении приёма (S3 недоступен — приёмник
в backpressure) WAL под слотом `pgw_bkp_<C>_<X>` копится на источнике до потолка
`max_slot_wal_keep_size`; затем PG срезает WAL и слот переходит в
`wal_status='lost'` — стрим через него невозможен (нужные сегменты удалены).

Что делает воркер (тик контроля WAL, зонд на КАЖДОМ источнике — мастер и
sync-standby):

- lost ОДНОГО источника при живом втором — слот потерянного пересоздаётся
  (drop + create immediate+reserved) автоматически, журнальная фаза
  `slot-recreate/<X>` (op `backup-wal` в `/pgworker/work/<C>`); потерянный агент
  возвращается сам от хвоста S3 (вечный ретрай приёмника), живой продолжает
  доставку — WAL_CHAIN_ERROR не ставится, цепочка не рвётся;
- lost/исчезновение слотов ВСЕХ источников при живой цепочке — wal-ключ WAL_CHAIN_ERROR,
  агенты останавливаются, воркер переснимает полный бэкап; архивация
  восстанавливается от `wal_start` нового полного.

Границы (RPO): неподтверждённый хвост WAL теряется в момент среза PG — свойство
аварии до любой автоматики; автоматика ускоряет RTO восстановления архивации.

Ручная чистка слота не требуется. Если алерт `slot-wal-lost` висит после
автолечения (слот не lost, но алерт жив) — воркер не лечит: смотрите журнал
`backup-wal` (`/pgworker/work/<C>`) и healthz воркера.
