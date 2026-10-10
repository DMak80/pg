# 02. ownS3: контракт операций

Полный контракт 22 операций начального этапа (состав и границы — глава
[01-overview.md](01-overview.md)). Формат ошибок, подпись, XML-namespace
и кодирование — глава [03-protocol.md](03-protocol.md); поведение диска
(атомарность, раскладка) — глава [04-storage.md](04-storage.md);
права ролей на операции — глава
[05-access-config.md](05-access-config.md).

**Единый шаблон контракта** (каждая операция ниже описана ровно этими
пунктами):

1. **Назначение** (1–2 предложения) и **референс-хендлер** MinIO —
   файл из `/Users/demakaev/ZCodeProject/minio/cmd/` (точечный
   референс, см. карту во введении
   [../roadmap/owns3.md](../roadmap/owns3.md)).
2. **HTTP**: метод, path-style URI (`/{bucket}`, `/{bucket}/{key+}`),
   обязательные/опциональные query-параметры.
3. **Заголовки запроса**: специфичные для операции.
4. **Тело запроса**: есть/нет; XML-схема при наличии.
5. **Успех**: код, заголовки ответа, тело/XML-схема.
6. **Ошибки**: коды S3 и условия (формат — глава 03).
7. **Семантика**: атомарность/идемпотентность, ETag-правило,
   conditional-поведение, видимость в листингах.

Соглашения: операции озаглавлены `### <Operation>`; общие семантики
(ETag, conditional, Range, лимиты, вне-наборные грани) вынесены в
раздел 1 и в контрактах не дублируются — контракты на него ссылаются.
Референс-файлы указаны маркдаун-ссылками на
[../minio](../../../minio).

## 1. Общие семантики

### ETag-правила

| Случай | ETag |
|---|---|
| простой `PutObject` | hex-MD5 содержимого (32 hex-символа, в кавычках) |
| multipart (`CompleteMultipartUpload`) | составной `<N>-<md5(concat part-ETag'ов)>`: N — число частей, конкатенация hex-строк ETag частей без разделителей, от неё md5 |
| ETag части (`UploadPart`/`UploadPartCopy`) | hex-MD5 тела части |
| `CopyObject` | семантика по стандарту S3: заголовок ETag возвращается, в `CopyObjectResult` — ETag нового объекта (hex-MD5 содержимого) |

ETag — всегда сильный (без `-`-суффикса слабости); сравнение в
`If-Match`/`If-None-Match` — точное (с кавычками клиента
нормализуется).

### Conditional-запросы

Заголовки `If-Match`, `If-None-Match`, `If-Modified-Since`,
`If-Unmodified-Since` — к целевому объекту (у copy-операций —
отдельная семья `x-amz-copy-source-if-*` к источнику, см. контракты
копирования). Правила по стандарту S3:

| Комбинация | Приоритет | Исход |
|---|---|---|
| `If-Match` присутствует | оценивается первым, остальные по нему не меняют исход | совпал → запрос продолжается; не совпал → **412** `PreconditionFailed` |
| `If-Match` отсутствует, `If-None-Match` присутствует | `If-None-Match` важнее `If-Modified-Since` | совпал → **304** `Not Modified` (GET/HEAD, без тела) / **412** (остальные методы); не совпал → запрос продолжается |
| только `If-Modified-Since` | — (GET/HEAD) | объект не менялся с даты → **304**; менялся → продолжается |
| только `If-Unmodified-Since` | — | объект менялся после даты → **412**; не менялся → продолжается |

- `If-Modified-Since` применяется только к GET/HEAD (у остальных
  методов игнорируется); не оценивается вовсе, если задан
  `If-None-Match`.
- Даты — HTTP-формат (`RFC 7231 IMF-fixdate`); невалидная дата
  игнорируется (заголовок считается отсутствующим).
- Ответ 304 — без тела, с заголовками `ETag`/`Last-Modified`.

### Range-запросы

`Range: bytes=a-b` к `GetObject`/`HeadObject`:

- **Одиночный диапазон** `bytes=a-b` (включительно), открытый
  `bytes=a-`, суффиксный `bytes=-N` — поддерживаются; ответ **206
  Partial Content** с `Content-Range: bytes a-b/<size>` и телом
  диапазона. Выход `a` за размер объекта → **416** `InvalidRange`.
- **Множественный диапазон** (`bytes=0-1,3-4`) — **не
  поддерживается**: заголовок не соответствует одиночному диапазону и
  трактуется как отсутствующий — ответ **200 полным объектом**
  (критерий — поведение Amazon S3; референс —
  [cmd/httprange.go](../../../minio/cmd/httprange.go): парсер
  принимает только один диапазон, прочие ошибки парсинга игнорируются
  как в Amazon S3).
- Невалидная спецификация (`a > b`, `bytes=-0`) → **416**
  `InvalidRange`.
- `End` за размером объекта обрезается до последнего байта (206).
- Суффикс длиннее объекта — весь объект (206, `Content-Range:
  bytes 0-<size-1>/<size>`).
- **`If-Range`**: поддерживается только форма сильного ETag — ETag
  совпал → применяется `Range` (206); не совпал → **200 полным
  объектом**. Форма HTTP-даты не поддерживается: присутствие даты
  трактуется как несовпадение (200 полным объектом; критерий —
  простота, клиенты бэкапов дату не используют).
- Пустой объект (size 0): любой `Range` → **416** (кроме
  отсутствующего — 200 с `Content-Length: 0`).

### Лимиты размеров

| Что | Лимит | Превышение |
|---|---|---|
| объект в одном `PutObject` | **5 ГБ** | **400** `EntityTooLarge` — большие объекты загружаются multipart (критерий — стандарт S3; отступление от референса MinIO: там предел одного PUT — 5 ТиБ) |
| одна часть `UploadPart`/`UploadPartCopy` | **5 ГБ** | **400** `EntityTooLarge` |
| ключи в одном `DeleteObjects` | **1000** | **400** `MalformedXML` (пустой список — тоже) |
| части в одной загрузке | 10000 (`partNumber` 1–10000) | см. контракты multipart |
| все части, кроме последней, на `Complete` | ≥ 5 МиБ | **400** `InvalidPart` (стандарт S3) |

### Вне-наборные грани — единый отказ

Обращение к граням вне набора 22 операций завершается единым
канонизированным отказом (критерий — стандарт S3; поведение и текст —
по референсу MinIO):

- **Сабресурсы объекта**: `?acl`, `?tagging`, `?retention`,
  `?legal-hold`, `?torrent`, `?restore`, `?attributes`-конфигурации
  вне контракта `GetObjectAttributes`.
- **Конфигурации бакета**: `?versioning`, `?lifecycle`, `?replication`,
  `?encryption`, `?policy`, `?cors`, `?website`, `?notification`,
  `?accelerate`, `?object-lock`, `?logging`, `?metrics`, `?inventory`,
  `?intelligent-tiering`, `?ownershipControls`, `?publicAccessBlock`,
  `?requestPayment`.
- **Отдельные подсистемы**: S3-Express, notification, select
  (`?select&select-type`).

Отказ: **501** `NotImplemented`, `Message` = «A header you provided
implies functionality that is not implemented» (формат ошибки — глава
03). Вне-наборные query-параметры, не являющиеся сабресурсами
(например `x-id`), игнорируются.

### Идемпотентность DeleteObject

`DeleteObject` несуществующего ключа — **успех** (204), без записи
каких-либо меток (versioning отсутствует). `DeleteBucket`
несуществующего бакета — наоборот, **404** `NoSuchBucket` (см.
контракт): бакет — административная сущность, «молчаливое» удаление
скрыло бы опечатку в имени.

## 2. Объекты

### PutObject

Загрузка объекта одним запросом. Референс:
[cmd/object-handlers.go](../../../minio/cmd/object-handlers.go).

- **HTTP**: `PUT /{bucket}/{key+}`; query-параметры отсутствуют.
- **Заголовки**: `Content-Length` (обязателен; при чанковой подписи —
  `x-amz-decoded-content-length`, глава 03); `Content-Type`
  (сохраняется в метаданных; дефолт
  `application/octet-stream`); `Content-MD5` — опционален, при наличии
  проверяется: несовпадение с телом → **400** `BadDigest`;
  `x-amz-meta-*` — произвольные user-metadata (ключи без префикса в
  ответах); `x-amz-storage-class` — **принимается и игнорируется**
  (влияние на хранение отсутствует); `x-amz-content-sha256` — по
  главе 03 (подпись).
- **Тело**: байты объекта; либо aws-chunked-фрейминг чанковой подписи
  (глава 03).
- **Успех**: **200**; заголовок `ETag` (hex-MD5); тело отсутствует.
- **Ошибки**: `NoSuchBucket` (404); `EntityTooLarge` (> 5 ГБ);
  `BadDigest`; `InvalidBucketName`/лимиты ключа — глава 03;
  подпись/доступ — глава 03/05.
- **Семантика**: объект атомарно появляется в Get/Head/List после
  коммита rename (глава 04); повторная загрузка того же ключа
  перезаписывает объект целиком (новый `dataDir`, старый — в `.trash/`);
  ETag — по общим правилам; в листингах виден сразу после коммита.

### GetObject

Чтение объекта. Референс:
[cmd/object-handlers.go](../../../minio/cmd/object-handlers.go).

- **HTTP**: `GET /{bucket}/{key+}`; query-параметры: `response-*`
  (`response-content-type`, `response-content-disposition`,
  `response-cache-control`, `response-content-encoding`,
  `response-content-language`, `response-expires`) — переопределение
  соответствующих заголовков ответа; применяются к **любому
  подписанному GET** — заголовочная подпись и presigned (критерий —
  стандарт S3); прочие query-параметры отсутствуют.
- **Заголовки**: conditional-заголовки и `Range`/`If-Range` — по общим
  семантикам (раздел 1).
- **Тело**: отсутствует.
- **Успех**: **200** (полный объект) / **206** (Range); заголовки:
  `ETag`, `Last-Modified`, `Content-Type` (сохранённый или
  `response-*`), `Content-Length`, `Accept-Ranges: bytes`,
  `x-amz-meta-*`; при `response-expires` — `Expires`.
- **Ошибки**: `NoSuchKey` (404); `NoSuchBucket` (404); **304**
  Not Modified / **412** PreconditionFailed (раздел 1); **416**
  InvalidRange.
- **Семантика**: чтение только закоммиченного объекта; при чтении
  сверяется контрольная сумма данных (глава 04): несовпадение → 500
  `InternalError` с диагностикой в лог; conditional/Range — по общим
  семантикам.

### HeadObject

Метаданные объекта без тела. Референс:
[cmd/object-handlers.go](../../../minio/cmd/object-handlers.go).

- **HTTP**: `HEAD /{bucket}/{key+}`; query-параметры — как у
  `GetObject` (`response-*`).
- **Заголовки**: conditional и Range — по разделу 1.
- **Тело**: отсутствует (в обе стороны).
- **Успех**: **200/206**; те же заголовки, что у `GetObject`
  (`ETag`, `Last-Modified`, `Content-Type`, `Content-Length`,
  `Accept-Ranges`, `x-amz-meta-*`) — **без тела** (различие
  Get/Head — только тело).
- **Ошибки**: коды как у `GetObject`, но всегда **без тела** (формат
  ошибок главы 03 для HEAD — только статус/заголовки); 304/412/416 —
  как в разделе 1.
- **Семантика**: дешёвая проверка существования/метаданных;
  conditional/Range — по разделу 1.

### DeleteObject

Удаление одного объекта. Референс:
[cmd/object-handlers.go](../../../minio/cmd/object-handlers.go).

- **HTTP**: `DELETE /{bucket}/{key+}`; query-параметры отсутствуют.
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **204** No Content; тело отсутствует.
- **Ошибки**: `NoSuchBucket` (404). Отсутствующий ключ — не ошибка
  (идемпотентность, раздел 1).
- **Семантика**: каталог объекта переименовывается в `.trash/`
  (глава 04); объект исчезает из Get/Head/List сразу после rename;
  повторное удаление — тот же 204.

### DeleteObjects

Пакетное удаление (multi-delete). Референс:
[cmd/bucket-handlers.go](../../../minio/cmd/bucket-handlers.go).

- **HTTP**: `POST /{bucket}?delete`; query-параметр `delete` —
  обязательный дискриминатор (пустое значение).
- **Заголовки**: `Content-MD5` — опционален (проверяется при наличии);
  `Content-Type: application/xml`.
- **Тело**: XML `Delete` — схема главы 03: элементы `Object/{Key,
  VersionId?}` (до 1000 штук; `VersionId` не поддерживается —
  versioning отсутствует, присутствие элемента игнорируется) и
  `Quiet` (bool; `true` — успешные удаления не возвращаются в
  ответе).
- **Успех**: **200**; тело `DeleteResult` — `Deleted/{Key}` на каждый
  удалённый (кроме Quiet-режима) и `Error/{Key, Code, Message}` на
  каждый неудалённый ключ.
- **Ошибки**: `MalformedXML` (некорректный XML, пустой список или
  больше 1000 ключей); `NoSuchBucket` (404).
- **Семантика**: каждый ключ обрабатывается независимо — ошибка одного
  не влияет на остальные (результат по-ключевой в `DeleteResult`);
  атомарность — по-ключевая (каждый ключ — rename главы 04), весь
  пакет не атомарен.

### CopyObject

Серверная копия объекта (только внутри ownS3). Референс:
[cmd/object-handlers.go](../../../minio/cmd/object-handlers.go).

- **HTTP**: `PUT /{bucket}/{key+}` при наличии заголовка
  `x-amz-copy-source` (дискриминатор от `PutObject`); query-параметры
  отсутствуют.
- **Заголовки**: `x-amz-copy-source: /<src-bucket>/<src-key>` —
  обязателен (URL-кодированный; только внутренний источник — внешний
  S3 не копируется); `x-amz-metadata-directive`: `COPY` (дефолт) /
  `REPLACE` — перенос user-metadata и Content-Type источника либо
  замена заголовками запроса; `x-amz-storage-class` — принимается и
  игнорируется; `x-amz-meta-*`/`Content-Type` — новые метаданные при
  `REPLACE`; copy-условия `x-amz-copy-source-if-match`,
  `x-amz-copy-source-if-none-match`,
  `x-amz-copy-source-if-modified-since`,
  `x-amz-copy-source-if-unmodified-since` — к источнику, приоритеты и
  исходы — по таблице раздела 1 (провал → 412 `PreconditionFailed`).
- **Тело**: отсутствует.
- **Успех**: **200**; тело `CopyObjectResult` — `ETag`, `LastModified`
  нового объекта (XML-схема — глава 03).
- **Ошибки**: `NoSuchBucket` (404 — приёмник или источник);
  `NoSuchKey` (404 — источник); `InvalidArgument` (400 — невалидное
  значение `x-amz-metadata-directive`); `PreconditionFailed` (412 —
  copy-условия); `EntityTooLarge` (источник > 5 ГБ — большие объекты
  копируются через `UploadPartCopy`).
- **Семантика**: новый объект закоммичен атомарно (глава 04); ETag —
  hex-MD5 содержимого (содержимое неизменно — совпадает с ETag
  источника); источник не меняется; метаданные — по директиве; в
  листингах новый ключ виден после коммита.

### GetObjectAttributes

Атрибуты объекта без содержимого. Референс:
[cmd/object-handlers.go](../../../minio/cmd/object-handlers.go).

- **HTTP**: `GET /{bucket}/{key+}?attributes` — query-параметр
  `attributes` (пустое значение) — **обязательный дискриминатор**
  операции (его отправляют стандартные клиенты, включая AWS SDK .NET);
  `partNumber`-варианта (`?attributes&partNumber=N` — атрибуты
  отдельной части) **нет**.
- **Заголовки**: `x-amz-object-attributes` — обязательный, список
  запрашиваемых атрибутов через запятую; поддерживаются: `ETag`,
  `ObjectSize`, `StorageClass`, `ObjectParts` (минимальный набор,
  достаточный целевым клиентам; прочие значения, включая `Checksum`,
  — вне-наборные грани, раздел 1); `x-amz-max-parts` и
  `x-amz-part-number-marker` — пагинация `ObjectParts` (дефолт
  max-parts 1000); conditional-заголовки — по разделу 1.
- **Тело**: отсутствует.
- **Успех**: **200**; заголовки `Last-Modified`, `ETag`; тело
  `GetObjectAttributesOutput` (схема — глава 03): только запрошенные
  элементы `ETag` / `ObjectSize` / `StorageClass` / `ObjectParts`
  (`PartsCount`, `PartNumberMarker`, `NextPartNumberMarker`, `MaxParts`,
  `IsTruncated`, `Part/{PartNumber, Size}`). **`ResponseExpires`
  (`response-expires`) операция не возвращает** — в отличие от
  `GetObject`; query-параметры `response-*` не применяются.
- **Ошибки**: `NoSuchKey`/`NoSuchBucket` (404); `InvalidArgument`
  (400 — отсутствует/пуст `x-amz-object-attributes`); 304/412 — по
  разделу 1.
- **Семантика**: объект не читается и не меняется; conditional — по
  разделу 1; `ObjectParts` отражает загруженные части multipart-объекта
  (для простого объекта — одна часть).

## 3. Листинги

Общая семантика листингов (все три операции):

- **`prefix`**: ключи, начинающиеся с префикса; отсутствующий/пустой —
  весь бакет. Ключевой параметр целевых клиентов: листинг цепочек
  бэкапов идёт по префиксу (`backups/<cluster>/…`).
- **Порядок**: лексикографический по UTF-8 байтам ключа.
- **`max-keys`**: дефолт 1000, максимум 1000; `0` — пустой ответ с
  `IsTruncated=false`; значение больше 1000 обрезается до 1000.
- **`IsTruncated`** и продолжение: v1 — `NextMarker` (последний ключ,
  возвращается при заданном `delimiter`; без delimiter клиент
  использует последний `Contents/Key`); V2 — `NextContinuationToken`
  (передаётся как `continuation-token`).
- **`delimiter`**: ключи после префикса, содержащие delimiter,
  сворачиваются в `CommonPrefixes` (строка от префикса до следующего
  delimiter включительно); ключи без delimiter в остатке — обычные
  `Contents`. `CommonPrefixes` считаются в `max-keys`.
- **`encoding-type=url`** — единственное значение: ключи и префиксы в
  ответе URL-кодированы; без параметра — без кодирования; иное
  значение → 400 `InvalidArgument`.
- `Owner` в `Contents` — только по явному запросу (V2 `fetch-owner`);
  значение — заполнитель (владельческой модели нет).

### ListObjects

Листинг v1. Референс:
[cmd/bucket-listobjects-handlers.go](../../../minio/cmd/bucket-listobjects-handlers.go).

- **HTTP**: `GET /{bucket}`; query: `prefix`, `delimiter`, `marker`,
  `max-keys`, `encoding-type` (все опциональны).
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **200**; `ListBucketResult` (глава 03): `Name`,
  `Prefix`, `Marker`, `MaxKeys`, `IsTruncated`, `NextMarker?`,
  `Delimiter?`, `Contents*` (`Key`, `LastModified`, `ETag`, `Size`,
  `StorageClass`, `Owner?`), `CommonPrefixes*`.
- **Ошибки**: `NoSuchBucket` (404); `InvalidArgument`
  (`encoding-type`).
- **Семантика**: `marker` — продолжение строго после ключа;
  согласованность с коммитами — глава 04 (раздел «Согласованность»).

### ListObjectsV2

Листинг v2. Референс:
[cmd/bucket-listobjects-handlers.go](../../../minio/cmd/bucket-listobjects-handlers.go).

- **HTTP**: `GET /{bucket}?list-type=2`; query: `list-type=2` —
  обязательный дискриминатор; `prefix`, `delimiter`, `start-after`,
  `continuation-token`, `max-keys`, `encoding-type`, `fetch-owner`
  (опциональны).
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **200**; `ListBucketResult`: `Name`, `Prefix`,
  `StartAfter?`, `ContinuationToken?`, `NextContinuationToken?`,
  `KeyCount` (число `Contents` + `CommonPrefixes` в ответе), `MaxKeys`,
  `IsTruncated`, `Delimiter?`, `EncodingType?`, `Contents*`
  (`Owner` — только при `fetch-owner=true`, значение — заполнитель),
  `CommonPrefixes*`.
- **Ошибки**: `NoSuchBucket` (404); `InvalidArgument`.
- **Семантика**: `start-after` — листинг строго после ключа (без
  токена, лексикографически); `continuation-token` —
  возобновление с того же места (токен — непрозрачная строка ownS3);
  `start-after` игнорируется при заданном `continuation-token`.

### ListObjectVersions

Листинг версий — unversioned-семантика. Референс:
[cmd/bucket-listobjects-handlers.go](../../../minio/cmd/bucket-listobjects-handlers.go).

- **HTTP**: `GET /{bucket}?versions`; query: `versions` — обязательный
  дискриминатор; `prefix`, `delimiter`, `key-marker`, `max-keys`,
  `encoding-type` (опциональны).
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **200**; `ListVersionsResult` — **unversioned-вид**
  (точная схема): `Name`, `Prefix`, `KeyMarker`, `VersionIdMarker`
  (возвращаются пустыми), `MaxKeys`, `IsTruncated`, `NextKeyMarker?`,
  `Delimiter?`, `Version*` (`Key`, `LastModified`, `ETag`, `Size`,
  `StorageClass`, `Owner?`) — **без элементов `VersionId` и `IsLatest`
  и без delete markers** (версий не существует: каждый ключ — одна
  текущая запись), `CommonPrefixes*`.
- **Ошибки**: `NoSuchBucket` (404); `InvalidArgument`.
- **Семантика**: эквивалент листинга текущих объектов в
  versions-обёртке (стандартные клиенты, читающие versions-ответ,
  получают каждый ключ ровно один раз); `key-marker` — продолжение
  строго после ключа; `version-id-marker` не поддерживается —
  игнорируется (версий нет).

## 4. Бакеты

### CreateBucket

Создание бакета. Референс:
[cmd/bucket-handlers.go](../../../minio/cmd/bucket-handlers.go).

- **HTTP**: `PUT /{bucket}`; query-параметры отсутствуют.
- **Заголовки**: `x-amz-bucket-region` — принимается и игнорируется.
- **Тело**: опциональное `CreateBucketConfiguration/LocationConstraint`
  — принимается и игнорируется (регион фиксирован, см.
  `GetBucketLocation`).
- **Успех**: **200**; заголовок `Location: /{bucket}`; тело
  отсутствует.
- **Ошибки**: **409** `BucketAlreadyOwnedByYou` — бакет уже существует
  (владельческой модели нет: любой повтор create одного имени —
  «already owned by you»; критерий — референс MinIO: локальный
  повторный create маппится в `BucketAlreadyOwnedByYou`);
  `InvalidBucketName` (400 — правила имени, глава 03).
- **Семантика**: бакет — каталог первого уровня тома (глава 04);
  создание атомарно (mkdir); служебные метаданные бакета —
  `.owns3.sys/buckets/<bucket>/`.

### DeleteBucket

Удаление пустого бакета. Референс:
[cmd/bucket-handlers.go](../../../minio/cmd/bucket-handlers.go).

- **HTTP**: `DELETE /{bucket}`; query-параметры отсутствуют.
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **204** No Content.
- **Ошибки**: **409** `BucketNotEmpty` — бакет содержит объекты
  (включая незавершённые multipart-загрузки этого бакета);
  **404** `NoSuchBucket` — несуществующий бакет (идемпотентности
  отсутствия нет — раздел 1).
- **Семантика**: удаляется только пустой бакет; каталог и служебные
  метаданные `.owns3.sys/buckets/<bucket>/` переименовываются в
  `.trash/` (глава 04).

### HeadBucket

Проверка бакета. Референс:
[cmd/bucket-handlers.go](../../../minio/cmd/bucket-handlers.go).

- **HTTP**: `HEAD /{bucket}`; query-параметры отсутствуют.
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует (в обе стороны).
- **Успех**: **200**; заголовок `x-amz-bucket-region: us-east-1`;
  тела нет.
- **Ошибки**: **404** `NoSuchBucket` — без тела (HEAD).
- **Семантика**: существование/доступность бакета без тела ответа.

### ListBuckets

Список бакетов. Референс:
[cmd/bucket-handlers.go](../../../minio/cmd/bucket-handlers.go).

- **HTTP**: `GET /`; query-параметры отсутствуют.
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **200**; `ListAllMyBucketsResult`: `Owner` —
  заполнитель (`ID`, `DisplayName` — фиксированные значения ownS3;
  владельческой модели нет), `Buckets/Bucket/{Name, CreationDate}` —
  по алфавиту имён.
- **Ошибки**: `AccessDenied` (403 — права, глава 05).
- **Семантика**: все бакеты тома; `CreationDate` — время создания
  каталога бакета.

### GetBucketLocation

Регион бакета. Референс:
[cmd/bucket-handlers.go](../../../minio/cmd/bucket-handlers.go).

- **HTTP**: `GET /{bucket}?location`; query-параметр `location` —
  обязательный дискриминатор.
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **200**; `LocationConstraint` — **пустой элемент**
  (по стандарту пустое значение = `us-east-1`; референс MinIO
  возвращает пустой элемент для дефолтного региона). Регион —
  заполнитель из credential scope подписи (глава 03), без
  гео-семантики; фиксированное значение `us-east-1`.
- **Ошибки**: `NoSuchBucket` (404).
- **Семантика**: справочная операция (клиенты SDK вызывают её при
  инициализации); данных о реальном размещении не несёт.

## 5. Multipart

### CreateMultipartUpload

Инициация multipart-загрузки. Референс:
[cmd/object-multipart-handlers.go](../../../minio/cmd/object-multipart-handlers.go).

- **HTTP**: `POST /{bucket}/{key+}?uploads`; query-параметр `uploads`
  — обязательный дискриминатор.
- **Заголовки**: `Content-Type`, `x-amz-meta-*` — сохраняются в
  метаданные будущего объекта; `x-amz-storage-class` — принимается и
  игнорируется.
- **Тело**: отсутствует.
- **Успех**: **200**; `InitiateMultipartUploadResult`: `Bucket`, `Key`,
  `UploadId`. **UploadId — UUID v4** (критерий — простота,
  глобальная уникальность без реестра).
- **Ошибки**: `NoSuchBucket` (404).
- **Семантика**: создаётся каталог загрузки и запись в `uploads.json`
  (глава 04) с uploadId, временем инициации и access key создателя
  (матрица видимости — глава 05); загрузка не видна в листингах
  объектов (только в `ListMultipartUploads`); объект появляется после
  `CompleteMultipartUpload`.

### UploadPart

Загрузка одной части. Референс:
[cmd/object-multipart-handlers.go](../../../minio/cmd/object-multipart-handlers.go).

- **HTTP**: `PUT /{bucket}/{key+}?partNumber=N&uploadId=X`; query:
  `partNumber` и `uploadId` — обязательны.
- **Заголовки**: `Content-Length` (обязателен; чанковая подпись —
  `x-amz-decoded-content-length`, глава 03); `Content-MD5`
  (опционален, при наличии проверяется → `BadDigest`).
- **Тело**: байты части (или aws-chunked-фрейминг, глава 03).
- **Успех**: **200**; заголовок `ETag` — hex-MD5 тела части; тело
  отсутствует.
- **Ошибки**: **400** `InvalidArgument` — `partNumber` отсутствует,
  не число или вне 1–10000 (критерий — стандарт S3; отступление от
  референса MinIO: там `InvalidPart`/`InvalidMaxParts`); **404**
  `NoSuchUpload` — `uploadId` отсутствует/не существует (в т.ч.
  после Complete/Abort); `NoSuchBucket` (404); `EntityTooLarge`
  (часть > 5 ГБ); `BadDigest`.
- **Семантика**: часть пишется в каталог загрузки (глава 04);
  повторная загрузка того же `partNumber` в живую загрузку
  перезаписывает часть; порядок загрузки произвольный; ETag части
  клиент обязан сохранить для Complete.

### UploadPartCopy

Копирование части из существующего объекта. Референс:
[cmd/object-multipart-handlers.go](../../../minio/cmd/object-multipart-handlers.go).

- **HTTP**: `PUT /{bucket}/{key+}?partNumber=N&uploadId=X` при
  наличии `x-amz-copy-source` (дискриминатор от `UploadPart`); query:
  `partNumber`, `uploadId` — обязательны.
- **Заголовки**: `x-amz-copy-source: /<src-bucket>/<src-key>` —
  обязателен; `x-amz-copy-source-range: bytes=a-b` — опционален (без
  него — весь объект источника); copy-условия
  `x-amz-copy-source-if-*` — как у `CopyObject` (раздел 2).
- **Тело**: отсутствует.
- **Успех**: **200**; `CopyPartResult`: `ETag`, `LastModified`.
- **Ошибки**: `NoSuchUpload` (404); `NoSuchBucket`/`NoSuchKey` (404 —
  источник); `InvalidArgument` (`partNumber` вне 1–10000;
  `x-amz-copy-source-range` — синтаксически невалидный **или
  выходящий за размер источника** — обе причины в один код;
  критерий — референс MinIO: `cmd/copy-part-range.go` отвечает
  `InvalidArgument` без разделения причин);
  `EntityTooLarge` (диапазон/источник > 5 ГБ); `PreconditionFailed`.
  `InvalidRange` (416) у copy-операций не используется — код
  закреплён только за `Range`-заголовком Get/Head (глава 03).
- **Семантика**: часть — байты диапазона источника; ETag — hex-MD5
  этих байтов; источник не меняется; остальное — как у `UploadPart`.

### CompleteMultipartUpload

Сборка объекта из частей. Референс:
[cmd/object-multipart-handlers.go](../../../minio/cmd/object-multipart-handlers.go).

- **HTTP**: `POST /{bucket}/{key+}?uploadId=X`; query: `uploadId` —
  обязателен.
- **Заголовки**: специфичных нет.
- **Тело**: XML-манифест `CompleteMultipartUpload/Part/{PartNumber,
  ETag}` (схема — глава 03): **минимум 1 часть, максимум 10000**;
  `PartNumber` — в **строго возрастающем** порядке (нарушение → 400
  `InvalidPartOrder`); пустой манифест → 400 `MalformedXML`.
- **Успех**: **200**; `CompleteMultipartUploadResult`: `Location`
  (`http://<host>/<bucket>/<key>`), `Bucket`, `Key`, `ETag` —
  составной (`<N>-<md5(concat part-ETag'ов)>`, раздел 1).
- **Ошибки**: `NoSuchUpload` (404 — uploadId не существует или уже
  завершён/прерван); **400** `InvalidPart` — несовпадение ETag или
  номера части с фактически загруженными, несуществующая часть, или
  часть меньше 5 МиБ не последняя; **400** `InvalidPartOrder` —
  нарушение порядка; `MalformedXML`; `NoSuchBucket`.
- **Семантика**: части собираются rename'ами в целевой объект
  (глава 04) — коммит-поинт и точка видимости: атомарная замена
  `xl.meta` целевого объекта последним rename'ом, объект появляется в
  Get/Head/List этим коммитом; запись удаляется из `uploads.json`
  сразу после коммита (глава 04). Повторный Complete **успешно
  завершённой** загрузки → `NoSuchUpload`; **прерванный** Complete
  (сбой до коммит-поинта — загрузка ещё в `uploads.json`) — повторная
  сборка идемпотентна (глава 04).

Примечание к лимитам: отдельного суммарного лимита размера объекта
на Complete нет — максимум уже обеспечен частными лимитами
(10000 частей × 5 ГБ, раздел «Лимиты размеров»).

### AbortMultipartUpload

Прерывание загрузки. Референс:
[cmd/object-multipart-handlers.go](../../../minio/cmd/object-multipart-handlers.go).

- **HTTP**: `DELETE /{bucket}/{key+}?uploadId=X`; query: `uploadId` —
  обязателен.
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **204** No Content — при живущей загрузке.
- **Ошибки**: **404** `NoSuchUpload` — uploadId не существует
  (в т.ч. **повторный Abort после успешного**: первый — 204, второй —
  `NoSuchUpload`; критерий — стандарт S3; отступление от референса
  MinIO — там повторный Abort молча возвращает 204).
- **Семантика**: каталог загрузки переименовывается в `.trash/`,
  запись удаляется из `uploads.json` (глава 04); загруженные части
  перестают занимать место после чистки `.trash/` (глава 04).

### ListParts

Список частей загрузки. Референс:
[cmd/object-multipart-handlers.go](../../../minio/cmd/object-multipart-handlers.go).

- **HTTP**: `GET /{bucket}/{key+}?uploadId=X`; query: `uploadId` —
  обязателен; `max-parts` (дефолт 1000, максимум 1000),
  `part-number-marker` (продолжение строго после номера).
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **200**; `ListPartsResult`: `Bucket`, `Key`, `UploadId`,
  `PartNumberMarker`, `NextPartNumberMarker?`, `MaxParts`,
  `IsTruncated`, `Part*` (`PartNumber`, `LastModified`, `ETag`,
  `Size`), `Initiator`/`Owner` — заполнители, `StorageClass`
  (`STANDARD`).
- **Ошибки**: `NoSuchUpload` (404); `NoSuchBucket` (404).
- **Семантика**: части в порядке возрастания номеров; видимость
  загрузки — по матрице прав (глава 05: read-write/admin — все
  загрузки, read-only — только свои).

### ListMultipartUploads

Список активных загрузок бакета. Референс:
[cmd/bucket-handlers.go](../../../minio/cmd/bucket-handlers.go).

- **HTTP**: `GET /{bucket}?uploads`; query-параметр `uploads` —
  обязательный дискриминатор; `prefix`, `delimiter`, `key-marker`,
  `upload-id-marker`, `max-uploads` (дефолт 1000, максимум 1000),
  `encoding-type` (опциональны).
- **Заголовки**: специфичных нет.
- **Тело**: отсутствует.
- **Успех**: **200**; `ListMultipartUploadsResult`: `Bucket`,
  `KeyMarker`, `UploadIdMarker`, `NextKeyMarker?`,
  `NextUploadIdMarker?`, `MaxUploads`, `IsTruncated`, `Prefix?`,
  `Delimiter?`, `Upload*` (`Key`, `UploadId`, `Initiated`,
  `StorageClass`, `Owner?`, `Initiator?` — заполнители),
  `CommonPrefixes?` (по `delimiter`).
- **Ошибки**: `NoSuchBucket` (404); `InvalidArgument`
  (`encoding-type`).
- **Семантика**: фильтры применяются к загрузкам (по ключу объекта);
  порядок — лексикографический по ключу, затем по времени инициации;
  видимость — глава 05 (read-only — только свои загрузки,
  read-write/admin — все).
