# Бэкапы: секреты и доступ

Уровень 3. Функция «секреты»: per-install bucket, креды пулов, CA,
per-cluster роль `backup_exec` и pg_hba-гвард. Общие правила секретов
PgWorker — [pgworker/04-security.md](../pgworker/04-security.md); ключи —
[05-etcd-contract.md](05-etcd-contract.md).

## 1. Per-install и per-cluster

- **Per-install, env воркера** (не etcd, не git): (а) **bucket per-install** —
  env `PGW_BACKUP_S3_BUCKET` (конфиг-биндинг `PgWorker:Backups:S3:Bucket`,
  группа транспортных секретов
  [pgworker/04-security.md](../pgworker/04-security.md) §2; единый на всех
  нодах всех пулов установки); (б) **креды пулов — per-cluster secrets в
  etcd**: `/s3/clusters/<C>/secrets`
  ([s3-clusters/06-secrets.md](../s3-clusters/06-secrets.md) §6; ключ
  `name=backups`, read-write) — воркер читает их резолвером пула живым
  резолвом на каждом тике (отсутствие/битость secrets — transient-отказ
  резолва с маркером `s3-pool-exhausted` у процессов, ретраи тиком, НЕ краш
  тика); единого
  `PGW_BACKUP_S3_ENDPOINT/REGION/ACCESS_KEY/SECRET_KEY/ADVERTISED_ENDPOINT`
  больше нет — цели даёт пул per-cluster; (в) **etcd-слепки — зона
  EtcdWorker** ([etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md)):
  цель — фиксированный bucket `etcd-backups` на нодах etcd-backup кластера
  `/s3/clusters/` (признак `etcd_backup=true`; bucket создаёт S3Worker;
  креды — ключ `backups` его secrets-ключа), источник правды о DR-слепках не
  живёт в восстанавливаемом etcd — резолв живой на каждом проходе. У
  PgWorker env-пары слепков НЕТ.

Передача агенту: env контейнера ноды несёт только per-install bucket
(`PGW_BACKUP_S3_BUCKET`), путь staging полного (`PGW_BACKUP_STAGING_DIR`) и
опциональный CA (ниже). Приёмник WAL целей/кредов в env НЕ получает —
резолвит сам из etcd живым резолвом (policy.s3 → пул → secrets; пароль
`backup_exec` из `/clusters/<C>/backup_password`) — тем же резолвом
пользуется операция полного (команда `POST /api/backup/full` несёт только
`id`). Tool-операции (verify/restore/drill) — полный S3-контекст read-списка
в POST-команде запуска от воркера (endpoints/креды/bucket/префикс; секреты
ТОЛЬКО в POST — не в GET-статусах, не в логах). Bucket в env ноды
отсутствует (пустой `S3:Bucket` — инконсистентность инсталляции, валидация
старта воркера её исключает) ⇒ приёмник стоит в `waiting_policy` с
диагностикой в `error` — старт-отказа живого процесса нет: приёмник
восстановится сам при появлении env (рестарт ноды/EnsureNode).

Стенд/контур-режим: адресация S3 всех потребителей контура — по имени сети
(`http://owns3:9000` на стенде); хост-публикация хранилища — только для
внешних потребителей (чеки с хоста, mc-миграция, оператор). Прод — endpoints
нод пула (`endpoints`-ключ + portalloc). Точка расширения: статические
внешние цели (`external`, вне `/s3/`) в env воркера — roadmap
(`s3-external-targets`).

## 2. Доверие CA при https-endpoint

TLS-режим ownS3: per-install CA (`ca.pem` TLS-пакета `deploy/tls/`).
Воркер: env `PGW_BACKUP_S3_CA_BUNDLE_PATH` → конфиг
`PgWorker:Backups:S3:CaBundlePath`; при заданном пути SDK-клиент воркера
(BackupS3, снапшеты etcd) доверяет CA **программно — фабрика HttpClient с
валидацией корня цепочки сервера по отпечатку CA** (env `AWS_CA_BUNDLE`
линейкой AWSSDK.Core 3.7.x не читается — эмпирический факт) — единый
per-install пакет на ВСЕ цели (ноды всех пулов и слепочная пара; per-пул CA
не вводится); путь не задан (http-endpoint) — фабрика не ставится,
поведение прежнее; нечитаемый/пустой файл CA — отказ с диагностикой.
Приёмник (контейнер на любом docker-хосте — файл хоста воркера в нём
недоступен): CA передаётся значением PEM в env ноды
(`PGW_BACKUP_S3_CA_PEM` — его ставит EnsureNode из `CaBundlePath` воркера);
приёмник материализует PEM во временный файл и передаёт его тем же
`CaBundlePath` (та же фабрика доверия) — и стриму, и операции полного.
Ops-операции (verify/restore/drill): PEM в POST-команде (`ca_pem`), приёмник
материализует тем же порядком. http-endpoint — переменные не заданы,
поведение прежнее.

## 3. Per-cluster роль `backup_exec`

LOGIN + REPLICATION; пароль per-cluster `/clusters/<C>/backup_password` — по
образцу прочих per-cluster секретов (ensure put-if-absent, 32 симв
`[A-Za-z0-9]`). Ensure роли в PG (G2, каждый тик) — CREATE-if-absent И
выравнивание пароля ALTER'ом ПРИ РАСХОЖДЕНИИ: проверка — пересчёт verifier'а
из etcd-значения против сохранённого rolpassword (форматы md5 —
`md5(пароль+роль)`; SCRAM-SHA-256 — PBKDF2 по сохранённой соли → StoredKey;
нераспознанный формат = расхождение). Безусловный ALTER каждым тиком
отвергнут: pg_authid — строка каталога, переписывалась бы каждый тик
(WAL-шум, реплицируется). pg_authid — не принадлежит воркеру, роль может
быть создана/перезаписана кем-то ещё с чужим значением (второй писатель);
CREATE-only гвард такое расхождение молчал вечно — новые подключения
приёмника падают 28P01 при живой сессии стрима (факт стенда). Расхождение
pg_authid↔etcd наблюдаемо тик за тиком отпечатками `[t07-diag]`
(etcd-значение и rolpassword, без секретов). Ротация: в общий тикет ротации
(ETCD-FIRST, app/bucket_admin/bucket_mover/rest + dsn) backup-креды НЕ
входят — ротация `backup_password` — отдельная задача t06-rotate-backup-creds
([pgworker/roadmap.md](../roadmap/pgworker.md)), тем же каноном ETCD-FIRST
после t05; до неё пароль статичен с ensure (выравнивание ALTER'ом — не
ротация: идемпотентная конвергенция к неизменному etcd-значению). Отдельная
от bucket_mover (иная зона доверия/ротации).

## 4. pg_hba-гвард

Дефолтный Spilo-pg_hba разрешает replication-соединения только роли
`standby` — физический WAL-стриминг `pg_basebackup` под `backup_exec`
отсекается. Строка `hostssl replication backup_exec all scram-sha-256`
обеспечивается ДВУМЯ каналами, владелец строки — приёмник в своём
контейнере:

**Самообеспечение приёмника** (первый канал): цикл подключения приёмника
перед каждой попыткой сессии/инвентарного прохода выполняет ту же операцию
обеспечения локально — идемпотентно (строка на месте — один file-read, без
SIGHUP). Причина: воркерный тик не связан со стартом контейнера ноды —
свежеподнятая/пересозданная нода может начать стрим раньше первого тика, и
её replication-подключения отсекаются («28000 no pg_hba.conf entry for
replication», факт стенда); канал замыкает окно до периода ретрая цикла
приёмника.

**Гвард G2 воркера** (каждый тик) — сверка/подстраховка: командует приёмнику
КАЖДОЙ ноды шарда `POST /api/node/hba-ensure`
([pgnode/07-node-ops.md](../pgnode/07-node-ops.md)): приёмник добавляет
строку в ИСТОЧНИК конфига Patroni `/run/postgres.yml` (секция
`postgresql.pg_hba`) и будит Patroni SIGHUP'ом — тот сам перегенерирует
`pg_hba.conf` при каждом старте postgres уже с этой строкой (правка самого
`pg_hba.conf` бессмысленна: файл перегенерируется Patroni). Ожидание
применения — поллинг `$PGDATA/pg_hba.conf` (нода без PG_VERSION ещё
бутстрапится — ждать нечего, строка применится при первом старте). Отказ
операции — transient: шард skip, следующий тик дообеспечит (запуск на
непатченной ноде карался бы FAILED-циклом с бэкоффом).
