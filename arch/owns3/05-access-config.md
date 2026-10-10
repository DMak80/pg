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
| `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload` | — | ✓ | ✓ |
| `CreateBucket`, `DeleteBucket` | — | — | ✓ |

- Root — права `admin`.
- «Свои» загрузки — созданные тем же access key: владелец фиксируется
  в `uploads.json` при CreateMultipartUpload (глава 04).
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
| `OWNS3_DATA_DIR` | `OwnS3:DataDir` | путь тома данных | **`/data`** (докер-конвенция) |
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

Расширенный состав (по частям, очередям чисток) — задачами
эксплуатации, не каноном t35.

## 6. Запуск в докере

- **Образ**: сборка .NET на хосте (`dotnet publish`) + runtime-слой с
  `COPY` publish-вывода (правило E2E-образов; прод-Dockerfile поставки —
  отдельный процесс, не канон t35). Базовый runtime-образ — из
  существующих `images.txt` (новых внешних образов нет); локально
  собираемый образ в registry `192.168.0.1:5000` **не кладётся**.
- **Том данных**: bind/volume на `OWNS3_DATA_DIR` — единственный
  persistent-volume (глава 01).
- **Env-секреты**: через env-файл по образцу deploy-воркеров
  (`deploy/.env.example`); env-шаблон ownS3 создаётся в t39 — канон
  фиксирует состав: root-пара + статические ключи (п. 1–2).
- ownS3 всегда запускается в докере (никогда хост-процессом — глава
  01); детальная модель dev-стенд-сервиса — t39.
