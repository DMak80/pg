# 05. ownS3: учётки, конфиг и эксплуатация

Модель доступа (root + статические access keys), конфигурация/env,
health и метрики, правила запуска. Операции — глава
[02-operations.md](02-operations.md); формат ошибок (включая
`AccessDenied`) — глава [03-protocol.md](03-protocol.md); том и
служебный контур `.owns3.sys` — глава [04-storage.md](04-storage.md).

## 1. Root-учётка

- Env `OWNS3_ROOT_USER` / `OWNS3_ROOT_PASSWORD` — оба **обязательны**.
- Минимальные длины: **user ≥ 3, password ≥ 8** (критерий — референс
  MinIO: те же пороги для root-пары).
- Отсутствие переменной или короткое значение — **отказ старта**
  (fail-fast: диагностика в лог + ненулевой exit; сервис не поднимается
  с дефолтным/слабым секретом).
- Root — полный доступ: права роли `admin` (матрица ниже) над всеми
  бакетами; аутентифицируется как обычный access key SigV4
  (клиентам не отличим от статического ключа admin).

## 2. Статические access keys

- **Источник — конфигурация**: секция `OwnS3:AccessKeys` appsettings /
  env `OWNS3_ACCESS_KEYS__*` — массив записей
  `{accessKey, secretKey, policy}` (критерий: простота — единый
  env-механизм с воркерами монорепо; полный IAM — вне начального
  этапа, глава 01).
- `policy` — `read-only` / `read-write` / `admin` (матрица ниже).
- **Материализация в `.owns3.sys/config/`**: при первом старте и при
  изменении конфигурации — файл ключей тома (список ключей с правами;
  используется для сверки подписи без повторного чтения env).
- Secret в файле локального тома **допускается** — осознанно, с
  границей: том локальный (multi-host durability правил не даёт;
  глава 01 «Durability»), утечка тома = утечка ключей. Ротация —
  переконфигурация env + рестарт (отозванный между рестартами ключ
  продолжает работать до рестарта — ограничение модели).
- Пример env-записи: `OWNS3_ACCESS_KEYS__0__ACCESSKEY=backups-writer`,
  `OWNS3_ACCESS_KEYS__0__SECRETKEY=…`,
  `OWNS3_ACCESS_KEYS__0__POLICY=read-write` (индексная нумерация
  массива в env — конвенция .NET).

## 3. Матрица прав

| Операция | read-only | read-write | admin |
|---|---|---|---|
| `GetObject`, `HeadObject` | ✓ | ✓ | ✓ |
| `ListObjects` (v1), `ListObjectsV2`, `ListObjectVersions` | ✓ | ✓ | ✓ |
| `HeadBucket`, `ListBuckets`, `GetBucketLocation` | ✓ | ✓ | ✓ |
| `ListMultipartUploads`, `ListParts` | ✓ (только свои загрузки) | ✓ (все загрузки) | ✓ (все загрузки) |
| `PutObject` | — | ✓ | ✓ |
| `DeleteObject`, `DeleteObjects` | — | ✓ | ✓ |
| `CopyObject`, `GetObjectAttributes` | — | ✓ | ✓ |
| `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload` | — | ✓ | ✓ |
| `CreateBucket`, `DeleteBucket` | — | — | ✓ |

- Root — права `admin`.
- «Свои» загрузки — созданные тем же access key: владелец фиксируется
  в `uploads.json` при CreateMultipartUpload (глава 04). Для read-only
  чужая загрузка неотличима от несуществующей: `ListParts` по чужому
  uploadId → **404 `NoSuchUpload`** (фильтр видимости — право на саму
  операцию у read-only есть); `ListMultipartUploads` фильтрует список
  по владельцу.
- **Отказ в правах — `AccessDenied` (403)** в формате ошибок главы 03.
- Граница матрицы: справочник бакетов (`HeadBucket`/`ListBuckets`/
  `GetBucketLocation`) доступен всем ролям — нужен целевым клиентам
  (инициализация SDK вызывает локацию/список); мутации бакетов —
  только admin.

## 4. Конфиг и env

Префикс `OWNS3_*` — единая конвенция имён; каждой переменной отвечает
секция `OwnS3:*` appsettings.

| Env | Секция appsettings | Назначение | Дефолт |
|---|---|---|---|
| `OWNS3_SERVER_PORT` | `OwnS3:Server:Port` | порт HTTP-грани Kestrel | **9000** (S3-конвенция) |
| `OWNS3_DATA_DIR` | `OwnS3:DataDir` | путь(и) тома данных: **1 путь — single-режим**, **список путей через запятую (≥ 2) — erasure-режим** (глава [06-erasure.md](06-erasure.md) §1; порядок путей = порядок дисков) | **`/data`** (докер-конвенция) |
| `OWNS3_EC_SET_SIZE` | `OwnS3:Ec:SetSize` | размер erasure-сета (k+m), 4–16 | авто: наибольший делитель числа дисков из {4, 8, 16}; нет делителя — отказ старта (глава 06 §1) |
| `OWNS3_EC_PARITY` | `OwnS3:Ec:Parity` | parity m для новых объектов, 1 ≤ m ≤ setSize/2 | **2** |
| `OWNS3_ROOT_USER` | `OwnS3:Root:User` | root-учётка | нет — обязательно |
| `OWNS3_ROOT_PASSWORD` | `OwnS3:Root:Password` | root-пароль | нет — обязательно |
| `OWNS3_ACCESS_KEYS__*` | `OwnS3:AccessKeys` | статические ключи (п. 2) | пусто = только root |
| `OWNS3_HOST_ID` | `OwnS3:HostId` | идентификатор инстанса (`HostId` ошибок, глава 03) | имя контейнера |

## 5. Health и метрики

- **`/healthz` — liveness**: том смонтирован и доступен на запись +
  `volume.json` валиден (глава 04) → **200**; иначе **503**. Эндпоинт
  не требует подписи (метрика доступности, не данных).
- **Prometheus-метрики** — по канону [18-metrics.md](../18-metrics.md)
  (каркас `Shared.Metrics`), минимальный состав:

| Метрика | Тип | Смысл |
|---|---|---|
| `ownS3_requests_total{operation,code}` | counter | запросы по операциям и кодам ответов |
| `ownS3_request_duration_seconds{operation}` | histogram | длительность обработки по операциям |
| `ownS3_disk_used_bytes` | gauge | занятость тома данных |
| `ownS3_disk_total_bytes` | gauge | размер тома данных |

Метрики erasure-режима (глава [06-erasure.md](06-erasure.md) §7/§9; в
single-режиме не экспортируются):

| Метрика | Тип | Смысл |
|---|---|---|
| `ownS3_ec_drive_online{set,drive}` | gauge | 1 — диск сета онлайн |
| `ownS3_ec_set_readable{set}` | gauge | 1 — онлайн-дисков сета ≥ k |
| `ownS3_ec_set_writable{set}` | gauge | 1 — онлайн-дисков сета ≥ write-кворума |
| `ownS3_heal_passes_total` | counter | завершённые heal-проходы |
| `ownS3_heal_objects_total` | counter | объекты, восстановленные heal |

Расширенный состав (по частям, очередям чисток) — задачами
эксплуатации, не каноном этой главы.

## 6. Запуск в докере

- **Образ**: сборка .NET на хосте (`dotnet publish`) + runtime-слой с
  `COPY` publish-вывода (правило E2E-образов; прод-Dockerfile поставки —
  отдельный процесс). Dockerfile — `docker/OwnS3.Dockerfile` (базовый
  runtime-образ aspnet из существующих `images.txt`, новых внешних
  образов нет; `ARG OWNS3_PUBLISH_DIR` — путь вывода publish по образцу
  `docker/Metrics.SdGenerator.Dockerfile`): CLI-сборка — узким контекстом
  каталога артефактов publish, compose-сборка — контекстом корня
  репозитория с переданным ARG. Теги: `owns3:dev` — стенд/локальный
  контур, `owns3:e2e` — E2E-тесты; локально собираемые образы в registry
  `192.168.0.1:5000` **не кладутся**.
- **Стенд/локальный контур — сервис `owns3` в `deploy/docker-compose.yml`**
  (по образцу воркеров): env-секреты из `deploy/.env` (шаблон — секция
  `OWNS3_*` в `deploy/.env.example`: обязательная root-пара п. 1,
  опциональные статические ключи п. 2), публикация
  `${OWNS3_HOST_PORT:-9010}:9000` (порты 9000/9001 хоста заняты
  `as-minio` полного стенда — ownS3 продолжает портовой ряд), том
  `owns3-data` → `OWNS3_DATA_DIR` (`/data`) — единственный
  persistent-volume (глава 01). Подъём — `dev-stand/adminpanel/checks/
  00-up.sh` (publish на хосте → compose build), smoke-проверки —
  `checks/46-owns3.sh` (`/healthz`, `/metrics`, операции `mc` против
  хост-публикации). Стендовая сетевая модель — весь стенд ОДНА сеть
  (стендовый compose — владелец): в ней живут etcd-контур, панель, воркеры
  deploy-проекта, ноды demo-кластера, wal-агенты, ephemeral-джобы бэкапов,
  ownS3 и мониторинг; контейнеров ownS3 в двух сетях нет. Адресация S3 всех
  потребителей контура — по имени сети: `http://owns3:9000` — единый
  endpoint воркера/агентов/джобов/панели (deploy-сервис подключается к
  стендовой сети external с alias `owns3`); хост-публикация
  `${OWNS3_HOST_PORT:-9010}` — только наружу (чеки с хоста, mc-миграция,
  оператор). Число инстансов ownS3 на установке — вопрос эксплуатации (не
  канон этой модели); стенд — один инстанс. Подсистема бэкапов пишет в
  ownS3: комплект `PGW_BACKUP_S3_*` → `http://owns3:9000` (ключи — §2:
  writer `read-write`; значения совпадают с `OWNS3_ACCESS_KEYS__0__*` —
  один комплект S3-секретов воркера, arch/19 §7), bucket `pgworker-backups`
  сеет `00-up.sh` (`mc mb --ignore-existing` root-кредами — CreateBucket
  только `admin`, матрица §3). Панель читает bucket read-only (ключ
  `read-only`, §2). `as-minio` полного стенда — переходный read-only-контур
  старых цепочек (новых объектов не получает, ретенцией воркера не чистится);
  критерий готовности к отказу и порядок сноса — `docs/owns3-migration.md`.
  Грань «Хранилище бэкапов» панели проверяет `checks/45-backups-storage.sh`
  против ownS3.
- **E2E реальными клиентами** — серия
  `src/tests/OwnS3.IntegrationTests/E2e` (AWS SDK .NET и `mc` против
  собственного контейнера `owns3:e2e`): гейт `PGW_TEST_DOCKER=1`,
  автосборка Release на каждом прогоне (`PGW_TEST_E2E_NOBUILD=1` —
  бисект/отладка конкретного бинаря), изоляция и зачистка контуров —
  каноны `docs/e2e-isolation.md` / `docs/e2e-launch.md`. Клиентская
  настройка flexible checksums (глава 03 §3) проверяется приёмкой в
  обоих режимах: `WHEN_REQUIRED` (основной) и `WHEN_SUPPORTED`
  (чанковая подпись с трейлерами).
- ownS3 всегда запускается в докере (никогда хост-процессом — глава 01).

## 7. Админ-грань (erasure-режим)

Наблюдение и управление состоянием EC-тома — вне 21 операции S3, отдельные
эндпоинты ownS3 тем же конвейером подписи (SigV4; права — `policy=admin`
или root, прочим **403** `AccessDenied`; матрица п. 3 на них не
распространяется — грань служебная):

- **`GET /admin/storage`** — статус: режим, топология (сеты/диски: путь,
  diskId, online), деградация сетов (readable/writable), состояние heal
  (идёт/последний проход/восстановлено объектов и шардов). Формат — JSON.
- **`POST /admin/heal`** — запуск внепланового heal-прохода (глава
  [06-erasure.md](06-erasure.md) §9): **202 Accepted**; идущий проход
  продолжается (параллельный не создаётся).
- В single-режиме оба эндпоинта отвечают статусом single-тома без сетов
  (грань доступна всегда — единый интерфейс наблюдения).

## 8. Стенд-сервис erasure-режима

- **Профиль-сервис `owns3-ec`** в `deploy/docker-compose.yml` (образ тот же
  `owns3:dev`): минимальный erasure-контур — 4 тома `owns3-ec-data-1..4`
  (`OWNS3_DATA_DIR=/data1,/data2,/data3,/data4`,
  `OWNS3_EC_SET_SIZE=4`, `OWNS3_EC_PARITY=1`), порт
  `${OWNS3_EC_HOST_PORT:-9011}:9000`; поднимается профилем compose
  (`--profile owns3-ec`), в базовый подъём стенда не входит (single-сервис
  `owns3` — п. 6). Секреты — та же секция `OWNS3_*` `deploy/.env`.
- До перевода подсистемы бэкапов на ownS3 (`t41`) EC-сервис живёт в
  стенде параллельно с single-сервисом; smoke-проверки —
  `dev-stand/adminpanel/checks/47-owns3-ec.sh` (`/healthz`,
  `/admin/storage`, `mc`-операции против хост-публикации).
