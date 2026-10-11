# Runbook: перевод подсистемы бэкапов MinIO → ownS3

Операционная процедура перевода подсистемы бэкапов (джобы полных, WAL-агенты,
verify/restore/drill, экспорт etcd-снапшотов) и панели «Хранилище бэкапов»
с `as-minio` на ownS3. Каноны: [arch/19-backups.md](../arch/19-backups.md)
(S3-агностичность подсистемы), [arch/owns3/05-access-config.md](../arch/owns3/05-access-config.md)
(ключи/health). Тон — по [docs/runbook.md](runbook.md).

## 1. Сводка механики

Путь миграции — **`mc mirror` всего bucket `pgworker-backups`** (включая
служебный префикс `etcd/` — экспорт etcd-снапшотов) тремя проходами с гейтом
сверки листингов. Истина подсистемы — list S3 (arch/19 §3/§4): etcd-статусы
endpoint-агностичны, после mirror с совпавшим листингом подсистема продолжает
работу с того же места (свежесть полных, хвост WAL-цепочек, история
etcd-ключей). Двойная запись НЕ вводится (arch/19 §7 — один комплект
`PGW_BACKUP_S3_*`); «гонка хранилищ» = параллельная жизнь сервисов: до
переключения воркер пишет в MinIO, ownS3 живёт рядом; после — только в ownS3,
`as-minio` остаётся read-only-архивом (новых объектов не получает, ретенцией
воркера не чистится).

ETag/mtime объектов после mirror меняются (пересоздание PUT'ом) — подсистема
от них не зависит (verify — SHA256 из `backup_manifest`, листинги — по
ключам/размерам).

## 2. Предпосылки

- ownS3 поднят (deploy-compose, healthz `curl :9010/healthz` — 200) и
  приёмочен клиентами (AWS SDK .NET, `mc`).
- В `deploy/.env` заполнены статические ключи ownS3 (канон arch/owns3/05 §2):
  `OWNS3_ACCESS_KEYS__0__*` — writer (`read-write`), значения СОВПАДАЮТ с
  `PGW_BACKUP_S3_ACCESS_KEY/SECRET_KEY`; `OWNS3_ACCESS_KEYS__1__*` —
  panel-reader (`read-only`). Сервису `owns3` их прокидывает
  deploy-compose; после правки — рестарт ownS3.
- Bucket `pgworker-backups` существует в ownS3 (создаёт `00-up.sh`
  идемпотентно; вручную: `mc mb --ignore-existing` root-кредами — CreateBucket
  только admin).
- Стендовый env панели уже на ownS3 (compose) — панель read-only, переводу
  не мешает.
- Стенд — один контур одна сеть: endpoint воркера/панели —
  `http://owns3:9000` (имя стендовой сети); миграция выполняется СНАРУЖИ
  через хост-публикации `:9000`/`:9010`.

## 3. Команды

Общие обозначения: `SRC` — MinIO (`http://host.docker.internal:9000`,
root-креды `minioadmin/minioadmin` стенда), `DST` — ownS3
(`http://host.docker.internal:${OWNS3_HOST_PORT:-9010}`, root-креды из
`deploy/.env`). mc — ephemeral-контейнер (образ в локальном registry):

```bash
MC() { docker run --rm --add-host host.docker.internal:host-gateway \
  --entrypoint /bin/sh minio/mc:RELEASE.2025-08-13T08-35-41Z -c "$1"; }
```

### 3.1. Mirror, проход 1 (базовый)

По живой подсистеме (воркер продолжает писать в MinIO):

```bash
MC "mc alias set src http://host.docker.internal:9000 minioadmin minioadmin >/dev/null \
  && mc alias set dst http://host.docker.internal:9010 <OWNS3_ROOT_USER> <OWNS3_ROOT_PASSWORD> >/dev/null \
  && mc mirror --overwrite src/pgworker-backups dst/pgworker-backups"
```

`--overwrite` идемпотентен к повторным проходам. Промежуточная сверка (п. 3.5)
даст естественный дрейф живого источника (WAL пишется в MinIO) — это норма.

### 3.2. Mirror, проход 2 (дельта до переключения)

Повтор команды 3.1 до дельты, близкой к нулю (быстрый проход по отсутствующим
объектам; «дельта» — вывод п. 3.5 пуст или только растущие WAL-хвосты).

### 3.3. Переключение

1. Правка `deploy/.env`:
   `PGW_BACKUP_S3_ENDPOINT=http://owns3:9000` (имя стендовой сети;
   advertised НЕ задаётся — равен endpoint),
   `PGW_BACKUP_S3_ACCESS_KEY=pgw-backups-writer`,
   `PGW_BACKUP_S3_SECRET_KEY=<writer-secret>` (= `OWNS3_ACCESS_KEYS__0__*`).
2. Рестарт подсистемы: `00-up.sh` (пересоздаст pgworker-инстанты и owns3)
   либо точечно: `cd deploy && docker compose --env-file .env up -d
   --force-recreate pgworker pgworker-2 owns3`.
3. Панель: `docker compose up -d --force-recreate adminpanel` (стендовый
   compose; её env уже `http://owns3:9000`).

Окно «проход 2 → рестарт воркера» НЕ требует остановки подсистемы: недостающие
WAL-сегменты окна агент досылает из удерживаемого слотом WAL после
переключения (инвариант подтверждения-по-доставке, arch/19 §3); полный бэкап
в полёте на момент рестарта уходит в FAILED (takeover RUNNING-статуса без
контейнера) и переснимается штатно новым id.

### 3.4. Mirror, проход 3 (хвостовая дельта после переключения)

Финальный повтор 3.1 — доносит объекты, записанные в MinIO в окне между
проходом 2 и рестартом воркера. Безопасен: PUT отсутствующих идемпотентен,
ownS3 уже активен; новые ownS3-объекты не трогает (их в источнике нет —
MinIO после переключения не пишет).

### 3.5. Гейт полноты переноса (обязателен до признания завершённым)

```bash
MC "mc alias set src http://host.docker.internal:9000 minioadmin minioadmin >/dev/null \
  && mc ls --recursive --json src/pgworker-backups" | jq -r '"\(.key) \(.size)"' | sort > /tmp/src-ls.txt
MC "mc alias set dst http://host.docker.internal:9010 <OWNS3_ROOT_USER> <OWNS3_ROOT_PASSWORD> >/dev/null \
  && mc ls --recursive --json dst/pgworker-backups" | jq -r '"\(.key) \(.size)"' | sort > /tmp/dst-ls.txt
comm -23 /tmp/src-ls.txt /tmp/dst-ls.txt > /tmp/migration-delta.txt
wc -l < /tmp/migration-delta.txt
```

Критерий: `/tmp/migration-delta.txt` пуст — каждый объект MinIO присутствует
в ownS3 с тем же ключом и размером. ownS3 — надмножество: новые объекты,
записанные ownS3-стороной после переключения, корректны и в гейте не
учитываются. Для окончательной побайтовой уверенности — штатная периодическая
verify-перепроверка оставшихся полных (`verify.interval_sec`, arch/19 §4)
и дрилл восстановимости (п. 4.2).

## 4. Критерий готовности к отказу от MinIO

Отказ от MinIO (снос `as-minio` из full-профиля, удаление тома
`as-minio-data`, чистка env/чеков от переходных ссылок) — отдельным
изменением, допустим когда ОДНОВРЕМЕННО (наблюдение — панель/etcd):

1. **Цепочки всех Active-шардов валидны в ownS3**: у каждого шарда ≥1
   COMPLETED-полный с `verify.state=OK` и непрерывной WAL-цепочкой от его
   `wal_start_segment` (wal-ключи ACTIVE; BROKEN нет). Панель: грань
   «Хранилище бэкапов» → детали шардов; etcd:
   `/pgworker/backups/<C>/<X>/…`.
2. **DR-путь из ownS3 доказан**: ≥1 дрилл восстановимости (reliability)
   завершился SUCCEEDED на бэкапе из ownS3 ПОСЛЕ переключения.
3. **Полный цикл без S3-деградаций**: прошло ≥ `full_max_age_sec` (дефолт —
   сутки) после переключения без алертов `backup-s3-unreachable`,
   `backup-verify-failed`, `backup-full-stale` и без CRIT/WARN ключа
   `/pgworker/backups/storage`.
4. **Источник исчерпан**: хвостовая дельта (п. 3.5 повторно) пуста — MinIO
   новых объектов не получает.

До выполнения критерия `as-minio` — источник ручного DR (`mc` с кредами
стенда). Контроль размера архива до сноса: `MC "mc alias set src … && mc du
src/pgworker-backups"` — тихий рост невозможен (воркер bucket не видит), но
факт занятости проверяется явно.

## 5. Откат (ownS3 деградирует в переходный период)

Env-флип назад: `PGW_BACKUP_S3_ENDPOINT=http://minio:9000` (compose-DNS
as-minio стендовой сети), креды `minioadmin/minioadmin` → рестарт (п. 3.3.2). Дыра MinIO за окно работы
с ownS3 самозаживляется штатно: WAL-агенты стартуют от хвоста MinIO и досылают
дыру из удерживаемого слотом WAL (в пределах `max_slot_wal_keep_size` —
долгие окна недопустимы, это аварийный путь); полные переснимет планировщик
(нет свежего валидного COMPLETED). Обратный mirror не нужен.

## 6. Порядок сноса as-minio (отдельное изменение)

Только после выполнения §4: удалить сервис `minio` и том `as-minio-data` из
`dev-stand/adminpanel/docker-compose.yml`, убрать bucket-сид as-minio и
health-гвард из `00-up.sh`, чек 45 уже против ownS3, упоминания переходного
периода — из README/env-шаблона. Осознанная потеря старых данных тома, если
не сохранены иначе.

## 7. Тестовый прогон процедуры (стенд без живых цепочек в MinIO)

Когда as-minio пуст (свежий стенд), процедура доказывается тестовым прогоном:

1. **Bucket-сид в as-minio**: `MC "mc alias set src http://localhost:9000
   minioadmin minioadmin >/dev/null && mc mb --ignore-existing
   src/pgworker-backups"` (as-minio — сервис стендового компоуза; bucket
   сидируется только здесь — 00-up.sh его больше не создаёт).
2. **Временный env-флип на MinIO**: `PGW_BACKUP_S3_ENDPOINT=http://minio:9000`,
   креды `minioadmin/minioadmin` → рестарт `pgworker pgworker-2` (контур-
   override) → дождаться ≥1 COMPLETED-полного на шард + wal-объекты в
   as-minio (наблюдение — панель/etcd; полные снимает планировщик).
3. **Процедура §3 целиком**: mirror×3 (снаружи `:9000`→`:9010`) → гейт §3.5 →
   переключение на `http://owns3:9000` → рестарт.
4. **Чистка тестовых цепочек** (возврат стенда к рабочему состоянию):
   - вернуть `deploy/.env` (`http://owns3:9000`, writer-ключ) — если не
     возвращено шагом 3;
   - удалить перенесённые тестовые цепочки из ownS3 и as-minio:
     `mc rm --recursive --force <alias>/pgworker-backups/demo/` в ОБОИХ
     хранилищах (только тестовые префиксы, созданные прогоном);
   - etcd-статусы тестовых полных/wal-ключи: подсистема сойдётся сама
     (супервизор/ретенция уберут осиротевшие записи; форсировать руками
     не нужно).
   Прогон на стенде с ЖИВЫМИ цепочками — шаги 1–3 без чистки (данные
   остаются рабочими).
