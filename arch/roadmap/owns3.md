# Roadmap: ownS3 (собственный S3-сервис: API и xl-хранение)

Собственное S3-совместимое объектное хранилище на C#/.NET 10 (проекты
`src/OwnS3.*` монорепо). Начальный этап — **API-грань и дисковое
хранение**; критерий правильности — **стандарт S3 API**: поведение,
которое ожидают стандартные S3-клиенты (AWS SDK .NET подсистемы
бэкапов, `mc`). **MinIO — пример реализации, а не образец для
копирования**: полная копия MinIO не делается — API реализуется по
стандарту S3; исходники MinIO ([../minio](../../../minio), Go) —
точечный референс «как этот вопрос решает работающая реализация», без
переноса его механики целиком (раскладка хранения на диске —
единственное, что берётся у MinIO сознательно, см. границы этапа).

ownS3 — действующее S3-хранилище подсистемы бэкапов (стенд — адресация
по имени сети контура; as-minio стенда — переходный read-only-архив до
критерия сноса, `docs/owns3-migration.md`). Трек [s3.md](s3.md)
живёт параллельно на переходный период.

Границы начального этапа:

- **21 операция**: объекты PutObject/GetObject/HeadObject/DeleteObject/
  DeleteObjects/CopyObject/GetObjectAttributes; листинги
  ListObjects(v1)/ListObjectsV2/ListObjectVersions; бакеты
  CreateBucket/DeleteBucket/HeadBucket/ListBuckets/GetBucketLocation;
  multipart CreateMultipartUpload/UploadPart/
  CompleteMultipartUpload/AbortMultipartUpload/ListParts/
  ListMultipartUploads. Сабресурсы объекта (ACL/tagging/lock/…),
  конфигурации бакета (versioning/lifecycle/replication/…) и S3-Express
  — вне набора: бакеты без versioning, ListObjectVersions отвечает
  текущими версиями как unversioned.
- **Хранение — xl-раскладка MinIO**: режимы single-drive и erasure-сеты
  (RS-коды, кворумы, деградация, heal — канон
  [`arch/owns3/06-erasure.md`](../owns3/06-erasure.md)); бакеты —
  каталоги первого уровня, объект — каталог с `xl.meta` +
  `<dataDir-uuid>/part.N`, служебный контур `.owns3.sys` (формат тома,
  multipart, tmp/.trash). Бакетная репликация (правила per-bucket) — не
  делается; межсерверная mesh-репликация —
  канон [`arch/owns3/07-replication.md`](../owns3/07-replication.md).
- Протокол: подпись SigV4 (включая presigned и чанковую потоковую
  STREAMING-AWS4-HMAC-SHA256-PAYLOAD), XML-схемы ответов, канонический
  формат ошибок, семантика ETag (MD5 простого PUT / составной
  multipart), conditional-запросы, Range.

Карта точечного референса (../minio): HTTP-роутинг — `cmd/api-router.go`,
`cmd/routers.go`; хендлеры — `cmd/object-handlers.go`,
`cmd/bucket-handlers.go`, `cmd/bucket-listobjects-handlers.go`,
`cmd/object-multipart-handlers.go`; подпись — `cmd/auth-handler.go`,
`cmd/signature-v4*.go`, `cmd/streaming-signature-v4.go`; диск —
`cmd/xl-storage.go`, `cmd/xl-storage-format-v2.go` (формат xl.meta),
`cmd/erasure-object.go` (запись через tmp, атомарный rename-коммит),
`cmd/format-erasure.go` (xl-single); multipart —
`cmd/erasure-multipart.go`; конфиг/IAM — `cmd/config.go`,
`cmd/iam-object-store.go`.

Порядок: канон → протокол сервиса → хранение/бакеты/объекты →
multipart → приёмка клиентами → erasure coding.

## Задачи

- **`t37-owns3-replication-ping`** — активная проба репликационных
  линков ownS3 (канон
  [../owns3/07-replication.md](../owns3/07-replication.md)):
  лёгкий запрос `/replication/ping` replication-грани (§6) — тот же
  SigV4-конвейер, только replication-политика ключа; приёмник ничего
  не пишет и не применяет (дедуп/fetch-очереди/курсоры не
  затрагиваются), на источнике — тем же клиентом/кредами/маршрутом,
  что notifier, но мимо outbox. Админ-триггер
  `POST /admin/replication/probe` (рядом со status/reconcile, §2):
  нода синхронно пингует всех (или одного) соседей — короткий
  per-target таймаут — и возвращает per-target результат
  `{name, reachable, rttMs?}`. Мотивация: `online` статуса — пассивный
  кэш исходов notifier (без трафика не обновляется); клиентские
  PUT-пробы связность ownS3 не детектируют (путь записи без синхронных
  взаимодействий с соседями — PUT всегда 0 с, §1); health-проба
  недостаточна (изолированная от всех целей нода отвечает health).
  Семантика: ping-ок = «канал жив» — отдельно от доставки (лаг/очередь
  статуса) и сверки данных (sync-report §10). Existing-регресс: без
  соседей грань `/replication/*` отвечает 403/404 (§6), probe —
  осмысленный ответ «соседей нет». Потребитель — зонд t33 трека s3
  ([s3.md](s3.md)). Arch-first правки: arch/owns3/07 §6 (грань ping),
  §2/§10-рядом (админ-триггер probe, наблюдаемость).
