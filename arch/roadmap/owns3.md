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
(t31–t34, MinIO-кластера) живёт параллельно на переходный период.

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
  multipart, tmp/.trash). Межсайтовая и бакетная репликация — не
  делаем (позже отдельными задачами трека).
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

- **`t35-owns3-replication`** — межсерверная mesh-репликация ownS3 по
  требованиям [`docs/owns3-replication-requirements.md`](../../docs/owns3-replication-requirements.md)
  (контекст изысканий/антипримеров —
  [`docs/s3-replication.md`](../../docs/s3-replication.md)): arch-first
  глава `arch/owns3/07-replication.md` — подтверждение PUT после
  локальной фиксации без ожидания целей (R1: серия 10 PUT ≤1 с при
  любых отказах целей), журнальная outbox-доставка per-target с
  бэкоффом, переживающая рестарт (R2), автоматическое срастание и
  backfill без рестартов процесса (R4), LWW-разрешение конкурирующих
  ключей с журналированием (R4), честные метрики лага/очереди +
  админ-сверка листингов (R5), аутентификация/адреса целей и
  изменение состава mesh без даунтайма S3-грани (R6). Приёмка —
  матрица 2–5 серверов из требований (stop/раскол/серии/срастание,
  методика docs/s3-replication.md). Синхронизация между серверами —
  вся здесь: клиентские задачи пула от неё не зависят.
