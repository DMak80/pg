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

ownS3 — целевое S3-хранилище подсистемы бэкапов: после приёмки t39
оно вытесняет MinIO (клиенты переключаются env-endpoint'ом, без
изменений кода клиентов); миграция данных MinIO→ownS3 — задача
`t41-owns3-minio-migration`. Трек [s3.md](s3.md) (t31–t34,
MinIO-кластера) живёт параллельно на переходный период.

Границы начального этапа:

- **22 операции**: объекты PutObject/GetObject/HeadObject/DeleteObject/
  DeleteObjects/CopyObject/GetObjectAttributes; листинги
  ListObjects(v1)/ListObjectsV2/ListObjectVersions; бакеты
  CreateBucket/DeleteBucket/HeadBucket/ListBuckets/GetBucketLocation;
  multipart CreateMultipartUpload/UploadPart/UploadPartCopy/
  CompleteMultipartUpload/AbortMultipartUpload/ListParts/
  ListMultipartUploads. Сабресурсы объекта (ACL/tagging/lock/…),
  конфигурации бакета (versioning/lifecycle/replication/…) и S3-Express
  — вне набора: бакеты без versioning, ListObjectVersions отвечает
  текущими версиями как unversioned.
- **Хранение — single-drive xl-раскладка MinIO без erasure coding**:
  бакеты — каталоги первого уровня, объект — каталог с `xl.meta` +
  `<dataDir-uuid>/part.N`, служебный контур `.owns3.sys` (формат тома,
  multipart, tmp/.trash). Межсайтовая и бакетная репликация, erasure
  sets — не делаем (позже отдельными задачами трека).
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

- **`t35-owns3-canon`** — канон ownS3 (arch-first,
  [../22-owns3.md](../22-owns3.md) — документ заведён, наполнение
  задачей): контракт 22 операций; протокол (SigV4/presigned/чанковая
  подпись, XML, ошибки, ETag, conditional/Range; path-style против
  virtual-host style); модель xl-хранения single-drive — раскладка и
  имена каталогов/файлов как в MinIO, формат `xl.meta` собственный по
  образцу (побайтовая совместимость msgp-формата MinIO не требуется);
  служебный контур `.owns3.sys` (format-тома, multipart, tmp/.trash);
  учётки/секреты и конфиг (образец MinIO: root из env + IAM в
  хранилище); состав проектов `src/OwnS3.*`; запуск в докере (образ —
  правила локального registry). Критерий каждого решения — стандарт
  S3 API (совместимость стандартных клиентов), ../minio — точечный
  референс-пример, полная копия не делается.
- **`t36-owns3-protocol`** ← `t35-owns3-canon` — каркас сервиса и
  протоколная обвязка: HTTP-грань, роутинг 22 операций (скелеты
  хендлеров), проверка подписи SigV4 + presigned + чанковая потоковая
  (фрейминг aws-chunked), XML-сериализация, формат S3-ошибок,
  учётки/доступ; юниты на тест-векторах подписи. Референс:
  `cmd/api-router.go`, `cmd/auth-handler.go`, `cmd/signature-v4*.go`,
  `cmd/streaming-signature-v4.go`.
- **`t37-owns3-storage-objects`** ← `t36-owns3-protocol` — xl-хранение
  и основные операции: раскладка бакетов/объектов/`part.N`, запись
  через `.owns3.sys/tmp` с атомарным rename-коммитом, `xl.meta`
  (версионирование формата), корзина tmp/.trash; бакеты
  Create/Delete/Head/List/GetBucketLocation; объекты
  Put/Get/Head/Delete/DeleteObjects/Copy/GetObjectAttributes
  (conditional, Range, ETag); листинги v1/V2 (лексикографический
  порядок, continuation-токены, лимит) и ListObjectVersions.
  Референс: `cmd/xl-storage.go`, `cmd/xl-storage-format-v2.go`,
  `cmd/erasure-object.go`, `cmd/bucket-handlers.go`,
  `cmd/object-handlers.go`, `cmd/bucket-listobjects-handlers.go`.
- **`t38-owns3-multipart`** ← `t37-owns3-storage-objects` —
  multipart-цикл: CreateMultipartUpload/UploadPart/UploadPartCopy/
  CompleteMultipartUpload/AbortMultipartUpload/ListParts/
  ListMultipartUploads; раскладка
  `.owns3.sys/multipart/<sha256(bucket/object)>/<uploadID>/`
  (журнал активных uploadID — uploads.json), сборка Complete
  rename'ами частей, составной ETag, чистка брошенных загрузок по
  возрасту. Референс: `cmd/erasure-multipart.go`,
  `cmd/object-multipart-handlers.go`.
- **`t39-owns3-e2e`** ← `t38-owns3-multipart` — приёмочная грань:
  интеграционные/E2E тесты реальными клиентами (AWS SDK .NET — простые
  PUT/GET, multipart, conditional, Range; `mc`), dev-стенд сервис в
  докере (образ — по правилам E2E-образов (сборка .NET на хосте,
  publish-COPY; локально собираемый образ в registry не кладётся),
  env-секреты), health/метрики;
  изоляция и зачистка контуров — по канонам E2E проекта.
- **`t40-owns3-erasure-coding`** ← `t39-owns3-e2e` — хранение с
  кодами Рида—Соломона по образцу MinIO (в начальной версии
  t35–t39 не реализуется): multi-drive тома и erasure-сеты,
  распределение объектов по сетам (у MinIO — SIPMOD+PARITY),
  кодирование/декодирование шардов data+parity, кворумы записи/чтения,
  работа в деградации (потеря дисков до parity), bitrot-контроль
  шардов, расширение `xl.meta` EC-полями (EcAlgo/EcM/EcN/EcBSize/
  EcIndex/EcDist), формат multi-drive тома, heal недостающих/
  повреждённых шардов; конфигурация числа дисков и parity — каноном
  задачи. Референс: `cmd/erasure-coding.go`, `cmd/erasure-encode.go`,
  `cmd/erasure-decode.go`, `cmd/erasure-sets.go`,
  `cmd/erasure-object.go`, `cmd/format-erasure.go`.
- **`t41-owns3-minio-migration`** ← `t39-owns3-e2e` — перевод
  подсистемы бэкапов с MinIO на ownS3: выбор пути (перезаливка цепочек
  бэкапов / `mc mirror` / новый цикл ретенции), гонка двух хранилищ
  на переход, критерий готовности к отказу от MinIO; состав
  фиксируется спекой t41.
- **`t42-owns3-tls`** ← `t39-owns3-e2e` — TLS-опция S3-грани:
  per-install сертификат, режим HTTP-only для стенда, доверие CA у
  клиентов (SDK бэкапов, `mc`).
