# Прототип сверки BASE_BACKUP с pg_basebackup (шаг 5 плана t01)

Рабочий артефакт фазы 1 (spec §4, §7 «побайтовая сверка зафиксирована»):
собственный стриминговый клиент `BASE_BACKUP` против живого PG, выходной tar
сверен с набором `pg_basebackup -Ft` по перечню файлов, размерам и sha256.

## 1. Окружение и инструмент

- Источник — контейнер `postgres:18-alpine` (тот же major, что Spilo-18
  контура), TLS включён (self-signed серт), `password_encryption=scram-sha-256`,
  роль `backup_exec` (`LOGIN REPLICATION`), подключение с хоста по
  опубликованному динамическому порту.
- Временный инструмент: `/Users/demakaev/ZCodeProject/.t01proto/` (вне
  репозитория, НЕ коммитится; ProjectReference на `PgWorker.PgNode` +
  `PgWorker.Backups`): `PgWireReplicationClient` → `BaseBackupCommand` →
  `TarStreamReader`/`TarStreamWriter` → единый `/Users/demakaev/ZCodeProject/.t01proto/own.tar`.
  Каталог инструмента вне `/tmp`: MSBuild не нормализует относительные пути
  ProjectReference через symlink-корни macOS (`/tmp`, `/var`), сборка падает
  на MSB3202.
- Эталон: `docker exec -e PGPASSWORD=... t01proto-pg pg_basebackup -h 127.0.0.1
  -p 5432 -U backup_exec -D /tmp/ref -Ft -X stream --checkpoint=fast
  --manifest-checksums=SHA256` → `base.tar`, `pg_wal.tar`, `backup_manifest`.
- Сверка: собственный tar распакован в `own/`; `base.tar` — в `ref/`;
  `pg_wal.tar` — в `ref/pg_wal/` (в эталоне сегменты WAL — отдельный архив с
  голыми именами).

## 2. Расхождения с планом, найденные прототипом (правки внесены в шаги 1–4 и канон)

1. **Синтаксис команды (PG15+).** `WAL 'STREAM'` — ошибка сервера
   `42601 wal requires a Boolean value`; `TARGET 'CLIENT'` — ошибка
   `0A000 unrecognized target: "CLIENT"` (регистр значим); `MANIFEST_CHECKSUMS`
   без `MANIFEST` — ошибка `42601 manifest checksums require a backup manifest`.
   Рабочая команда: `BASE_BACKUP ( LABEL 'pgw-full-<id>', CHECKPOINT 'fast',
   MANIFEST 'yes', MANIFEST_CHECKSUMS 'SHA256', TARGET 'client', WAL true,
   WAIT false )`.
2. **SCRAM client-first.** Полное сообщение — GS2-заголовок `n,,` + bare
   `n=,r=<nonce>`; без заголовка сервер отвечает `08P01 malformed SCRAM message`.
   `authMessage` строится по bare-форме (RFC 5802) — исправлено в
   `PgWireReplicationClient`.
3. **Форма ответа BASE_BACKUP (реальный протокол PG18, документирован в
   protocol-replication):**
   - первый result set — стартовая позиция WAL (2 поля `recptr`,`tli`);
   - второй result set — tablespace list (3 поля `spcoid`,`spclocation`,`size`),
     один row для основного каталога (пустые значения);
   - каждый result set завершается своим `CommandComplete` (`SELECT`) —
     конец команды только после COPY OUT-потока;
   - далее ОДИН `CopyOutResponse`; внутри CopyData — собственный фрейминг:
     `n` (имя архива + каталог), `m` (манифест), `d` (данные), `p` (прогресс);
   - при `WAL true` сегменты WAL дописываются ВНУТРЬ `base.tar` (записи
     `pg_wal/…`), отдельного `pg_wal.tar` сервер НЕ шлёт (его делает клиент
     `pg_basebackup`, разрезая поток);
   - манифест приходит отдельной секцией `m`/`d` (не внутри `base.tar`);
   - после потока — финальный result set (конечная позиция WAL), затем
     `ReadyForQuery`.
4. **Манифест не перечисляет** `backup_manifest` (сам себя) и файлы
   WAL-сегментов `pg_wal/<segment>` (они покрыты `WAL-Ranges`); записи
   `pg_wal/archive_status/<segment>.done` в манифесте ЕСТЬ. Правило
   потокового verify (шаг 14): отсутствие в манифесте допустимо только для
   `backup_manifest` и `pg_wal/<segment>`.

## 3. Команды сверки и результаты

```
# перечень файлов
(cd own && find . -type f | sort) > own-files.txt
(cd ref && find . -type f | sort) > ref-files.txt
diff own-files.txt ref-files.txt
```
Итог: `own` 971 файл, `ref` 970. Расхождения перечня — ровно ожидаемые:
```
3d2
< ./backup_manifest                                   # у нас — запись единого tar (эталон: отдельный файл)
967,968c966
< ./pg_wal/00000001000000000000000F                   # разные прогоны: у нас сегмент 0F …
< ./pg_wal/archive_status/00000001000000000000000F.done
---
> ./pg_wal/000000010000000000000011                   # … у эталона 11
971a970
> ./tablespace_map                                    # эталон передаёт TABLESPACE_MAP; нам файл не нужен
```

```
# размеры и sha256 каждого общего файла
comm -12 own-files.txt ref-files.txt > common-files.txt   # 968 файлов
```
| показатель | значение |
|---|---|
| общих файлов | 968 |
| sha256 совпал | **966** |
| sha256 разошёлся | 2 |

Расхождения — только run-specific поля (не расхождение формата):

| файл | own | ref | причина |
|---|---|---|---|
| `backup_label` | 214 Б | 227 Б | START/CHECKPOINT LSN, START TIME, LABEL — свои у каждого прогона |
| `global/pg_control` | 8192 Б | 8192 Б | checkpoint LSN/время своего прогона (каждый бэкап делает свой fast-checkpoint) |

Содержимое `backup_label` (структура идентична, значения — свои):
```
# own
START WAL LOCATION: 0/F000028 (file 00000001000000000000000F)
CHECKPOINT LOCATION: 0/F000080
BACKUP METHOD: streamed
BACKUP FROM: primary
START TIME: 2026-11-04 22:27:00 UTC
LABEL: pgw-full-proto
START TIMELINE: 1
# ref
START WAL LOCATION: 0/11000028 (file 000000010000000000000011)
CHECKPOINT LOCATION: 0/11000080
BACKUP METHOD: streamed
BACKUP FROM: primary
START TIME: 2026-11-04 22:27:43 UTC
LABEL: pg_basebackup base backup
START TIMELINE: 1
```

`pg_controldata` обоих `pg_control` (diff):
```
< Latest checkpoint location:           0/F000080
< Latest checkpoint's REDO location:    0/F000028
< Latest checkpoint's REDO WAL file:    00000001000000000000000F
---
> Latest checkpoint location:           0/11000080
> Latest checkpoint's REDO location:    0/11000028
> Latest checkpoint's REDO WAL file:    000000010000000000000011
```
(остальные поля, включая System identifier/state/checksum version, совпали).

Сверка `backup_manifest` (оба — манифесты сервера):
```
own keys: PostgreSQL-Backup-Manifest-Version, System-Identifier, Files, WAL-Ranges, Manifest-Checksum
версия 2, Checksum-Algorithm = SHA256, файлов: own 969 / ref 969
only-own: pg_wal/archive_status/00000001000000000000000F.done
only-ref: tablespace_map
checksum/size mismatches: backup_label, global/pg_control
own WAL-Ranges: [{'Timeline': 1, 'Start-LSN': '0/F000028', 'End-LSN': '0/F000120'}]
ref WAL-Ranges: [{'Timeline': 1, 'Start-LSN': '0/11000028', 'End-LSN': '0/11000120'}]
```

## 4. Вывод

Собственный tar эквивалентен набору `pg_basebackup -Ft` по перечню файлов,
размерам и sha256: **966 из 968 общих файлов побайтово совпали**, оставшиеся
два (`backup_label`, `global/pg_control`) различаются только run-specific
полями (LSN/время/метка), что не является расхождением формата. Манифест,
`backup_label` и `pg_control` структурно идентичны эталону; расхождения
метаданных заголовков (`mtime`/`uid`/`gid`) наш writer пишет нулями — на
потребителей (restore/verify) не влияет, чинить не требуется.

Фаза 1 подтверждена; правки шагов 1–4 (синтаксис команды, GS2-заголовок
SCRAM, автомат result set'ов и фрейминга архивов) внесены, канон
`arch/pgnode/04-full-backup.md` §2 обновлён arch-first.
