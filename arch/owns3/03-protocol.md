# 03. ownS3: протокол

Протокольный слой ownS3: подпись SigV4 (заголовочная, presigned,
чанковая потоковая), XML-схемы, формат ошибок, транспорт. Контракты
операций — глава [02-operations.md](02-operations.md); модель доступа
(учётки, роли) — глава [05-access-config.md](05-access-config.md).
Реализует `src/OwnS3.Protocol` (состав — глава
[01-overview.md](01-overview.md)).

Референс при сверке семантик подписи:
[cmd/auth-handler.go](../../../minio/cmd/auth-handler.go),
[cmd/signature-v4.go](../../../minio/cmd/signature-v4.go),
[cmd/signature-v4-parser.go](../../../minio/cmd/signature-v4-parser.go),
[cmd/signature-v4-utils.go](../../../minio/cmd/signature-v4-utils.go),
[cmd/streaming-signature-v4.go](../../../minio/cmd/streaming-signature-v4.go).

## 1. SigV4: заголовочная подпись

Режим по умолчанию: `Authorization`-заголовок вида
`AWS4-HMAC-SHA256 Credential=<ak>/<date>/<region>/s3/aws4_request,
SignedHeaders=..., Signature=...`.

**Canonical request** (строки `\n`-разделённые):

1. HTTP-метод (верхний регистр).
2. Canonical URI: path-style путь `/{bucket}/{key+}` — **каждый сегмент
   URI-кодирован** (RFC 3986 unreserved + сохранение `/` как
   разделителя сегментов; незакодированные спецсимволы пути не
   допускаются — кодирование сегментное).
3. Canonical query string: все query-параметры (включая
   дискриминаторы операций и `response-*`), отсортированные по ключу
   (потом по значению), `key=value` с URI-кодированием; компоненты
   сортировки — до кодирования.
4. Canonical headers: имена в нижнем регистре, отсортированы по имени,
   `name:value\n` (значение — с усечёнными по краям пробелами); `host`
   обязателен;
   `x-amz-content-sha256` входит в подписанный список, когда клиент
   его подписал.
5. `x-amz-content-sha256` — значение заголовка (или
   `UNSIGNED-PAYLOAD`-семантика для presigned).

**String-to-sign**: `AWS4-HMAC-SHA256\n<x-amz-date>\n<scope>\n<hex-sha256(canonical
request)>`, где scope = `<date>/<region>/s3/aws4_request`.
**Регион — заполнитель**: берётся из credential scope запроса без
гео-семантики (собственного региона нет, `GetBucketLocation` — глава
02); канонизированное значение — `us-east-1` (рекомендуется клиентам;
любое значение scope принимается и проверяется на согласованность
внутри запроса).

**Signing key**: цепочка HMAC-SHA256: `AWS4<secret>` → date → region →
`s3` → `aws4_request`; подпись = hex-HMAC(signing key, string-to-sign).

**Обязательные заголовки**: `Host` (или явный `host` в
SignedHeaders), `x-amz-date` (формат `yyyyMMdd'T'HHmmss'Z'`, UTC;
допустима подстановка `Date` при отсутствии `x-amz-date` — референс
допускает, фиксируем поддержку обоих).

**Допустимый clock skew ±15 минут** от времени сервера: нарушение →
**403** `RequestTimeTooSkewed` (критерий — стандарт SigV4; референс:
`globalMaxSkewTime = 15 * time.Minute`).

**Значения `x-amz-content-sha256`**:

| Значение | Семантика |
|---|---|
| `<hex-sha256 тела>` | тело сверяется с хэшем: несовпадение → 400 `InvalidRequest` (XAmzContentSHA256Mismatch-семантика) |
| `UNSIGNED-PAYLOAD` | тело не хэшируется (подпись покрывает остальной запрос) |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | чанковая подпись (раздел 3) |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` | чанковая с подписанными трейлерами (раздел 3) |
| прочие (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`, `StreamBody-*`, …) | **не поддерживаются** → 400 `InvalidRequest`, Message «Unsupported x-amz-content-sha256 value» (сверка со стандартом: значение вне перечня режимов SigV4 для S3) |

**Исходы проверки подписи**: несовпадение подписи → **403**
`SignatureDoesNotMatch`; некорректный `Authorization`-заголовок
(структура/ scope) → 400 `AuthorizationHeaderMalformed`; отсутствие
подписи и прав на анонимный доступ → **403** `AccessDenied`
(public-доступа нет — все запросы аутентифицируются, глава 05).

## 2. Presigned URL

Подпись в query-параметрах (без `Authorization`-заголовка):

| Параметр | Назначение |
|---|---|
| `X-Amz-Algorithm=AWS4-HMAC-SHA256` | версия алгоритма (обязателен) |
| `X-Amz-Credential` | `<ak>/<date>/<region>/s3/aws4_request` |
| `X-Amz-Date` | момент подписи (`yyyyMMdd'T'HHmmss'Z'`) |
| `X-Amz-Expires` | срок действия, секунды |
| `X-Amz-SignedHeaders` | подписанные заголовки (`host` обязателен) |
| `X-Amz-Signature` | подпись |

- **Тело не подписывается**: payload-строка canonical request —
  `UNSIGNED-PAYLOAD` (в query отдельного sha256-параметра нет).
- **Максимум `X-Amz-Expires` — 604800 секунд (7 дней)**; превышение
  или отрицательное/невалидное значение → **400**
  `AuthorizationQueryParametersError` (критерий — стандарт S3;
  референс: `signature-v4-parser.go`, порог 604800).
- **Просроченный presigned** (прошло больше `X-Amz-Expires` с
  `X-Amz-Date`) → **403** `AccessDenied`.
- Clock skew ±15 минут применяется и к `X-Amz-Date`.
- **Разрешённые операции через presigned** — ровно потребности
  клиентов бэкапов и `mc` (критерий — стандарт S3 допускает per-API
  ограничение): `GetObject`, `PutObject`, `DeleteObject`, `HeadObject`,
  `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`.
  Обращение прочих операций через presigned → **400**
  `AuthorizationQueryParametersError`.
- Query-параметры `response-*` операции `GetObject` (глава 02)
  подписываются как часть canonical query.

## 3. Чанковая потоковая подпись

Потоковая загрузка тела без предварительного хэша всего тела. Два
поддерживаемых режима: базовый (`x-amz-content-sha256:
STREAMING-AWS4-HMAC-SHA256-PAYLOAD`) и с подписанными трейлерами
(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`).

**Обязательные заголовки**: `x-amz-content-sha256` (значение режима),
`Content-Encoding: aws-chunked`,
`x-amz-decoded-content-length` — реальная длина тела (без фрейминга);
`Content-Length` при этом — длина закодированного потока (включая
заголовки чанков). Несовпадение суммы данных чанков с
`x-amz-decoded-content-length` → 400 `InvalidRequest`.

**Фрейминг** (референс —
[cmd/streaming-signature-v4.go](../../../minio/cmd/streaming-signature-v4.go)):

```
<hex-размер-чанка>;chunk-signature=<подпись>\r\n<данные>\r\n
...
0;chunk-signature=<подпись>\r\n                     ← финальный чанк, без данных
[только TRAILER-режим:]
x-amz-checksum-<alg>:<base64>\r\n                   ← трейлеры из x-amz-trailer
x-amz-trailer-signature:<подпись>\r\n
```

**Цепочка подписей**: seed = подпись исходного запроса (заголовочная
подпись считается первой, с payload-строкой = значению режима);
string-to-sign чанка N:

```
AWS4-HMAC-SHA256-PAYLOAD\n<date>\n<scope>\n<подпись чанка N-1>\n<sha256("")>\n<sha256(чанк N)>
```

каждый чанк сверяется при чтении; **разрыв цепочки или несовпадение
подписи чанка → 403 `SignatureDoesNotMatch`** — отказ происходит в
момент чтения соответствующего чанка (тело до этого чанка уже получено;
запрос прерывается, объект не закоммичен). Финальный `0`-чанк
подписывается той же цепочкой.

**Трейлеры** (`x-amz-trailer` — список имён через запятую; режим
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`):

- Поддерживаемый минимальный набор (референс + потребности AWS SDK
  .NET): `x-amz-checksum-crc32`, `x-amz-checksum-crc32c`,
  `x-amz-checksum-sha1`, `x-amz-checksum-sha256` — значения base64,
  **сверяются с декодированным телом**: несовпадение → **400**
  `BadDigest`.
- Прочие имена трейлеров → **400** `InvalidRequest` (Message
  «Unsupported trailer header»; критерий — простота: набор ровно под
  клиентов).
- Подпись трейлера: алгоритм `AWS4-HMAC-SHA256-TRAILER` — string-to-sign
  по конкатенации trailer-строк (в порядке `x-amz-trailer`),
  завершается заголовком `x-amz-trailer-signature:<подпись>`;
  несовпадение → 403 `SignatureDoesNotMatch`.

**Неподдерживаемые значения `x-amz-content-sha256`** — раздел 1:
`STREAMING-UNSIGNED-PAYLOAD-TRAILER` (неподписанное тело) и прочие
вне двух режимов → 400 `InvalidRequest`.

**Требование к клиентам** (следствие отказа от
`STREAMING-UNSIGNED-PAYLOAD-TRAILER`): свежие AWS SDK .NET с
включёнными по умолчанию flexible checksums шлюют `PutObject` именно
в этом режиме — клиент обязан отключить вычисление checksum по
умолчанию (RequestChecksumCalculation = WHEN_REQUIRED) либо
использовать подписанный trailer-режим
(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`, поддерживается).
Иначе PUT завершится 400 `InvalidRequest` — это требуется учесть
при приёмке t39 (настройка клиентов бэкапов и `mc`).

## 4. XML-схемы

Namespace всех XML-ответов:
`xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`. `Content-Type:
application/xml` — у всех XML-ответов (charset UTF-8). Кодирование
спецсимволов — XML-сущности (`&` → `&amp;`, `<` → `&lt;`, `>` →
`&gt;`, атрибуты — `&quot;`/`&apos;`), недопустимые кодпоинты —
числовыми ссылками. Схемы-образцы (полные контракты — глава 02):

```xml
<!-- листинги v1 / V2: ListBucketResult -->
<ListBucketResult>
  <Name>…</Name><Prefix>…</Prefix><Marker>…</Marker>          <!-- v1 -->
  <!-- V2: StartAfter, ContinuationToken, NextContinuationToken, KeyCount -->
  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>…</Key><LastModified>…</LastModified>
    <ETag>"…"</ETag><Size>…</Size><StorageClass>STANDARD</StorageClass>
    <Owner>…</Owner></Contents>                                <!-- Owner опционален -->
  <CommonPrefixes><Prefix>…</Prefix></CommonPrefixes>
</ListBucketResult>

<!-- versions (unversioned-вид): ListVersionsResult -->
<ListVersionsResult>
  <Name>…</Name><Prefix>…</Prefix><KeyMarker/><VersionIdMarker/>
  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <Version><Key>…</Key><LastModified>…</LastModified>
    <ETag>"…"</ETag><Size>…</Size><StorageClass>STANDARD</StorageClass></Version>
  <!-- без VersionId/IsLatest и без DeleteMarker — глава 02 -->
</ListVersionsResult>

<!-- multi-delete: Delete (запрос) / DeleteResult (ответ) -->
<Delete><Object><Key>…</Key></Object><Quiet>true</Quiet></Delete>
<DeleteResult><Deleted><Key>…</Key></Deleted>
  <Error><Key>…</Key><Code>…</Code><Message>…</Message></Error></DeleteResult>

<!-- multipart -->
<InitiateMultipartUploadResult><Bucket>…</Bucket><Key>…</Key>
  <UploadId>…</UploadId></InitiateMultipartUploadResult>
<CompleteMultipartUpload>                                                <!-- запрос -->
  <Part><PartNumber>1</PartNumber><ETag>"…"</ETag></Part></CompleteMultipartUpload>
<CompleteMultipartUploadResult><Location>…</Location><Bucket>…</Bucket>
  <Key>…</Key><ETag>"N-md5"</ETag></CompleteMultipartUploadResult>
<ListPartsResult><Bucket>…</Bucket><Key>…</Key><UploadId>…</UploadId>
  <PartNumberMarker>0</PartNumberMarker><MaxParts>1000</MaxParts>
  <IsTruncated>false</IsTruncated>
  <Part><PartNumber>1</PartNumber><LastModified>…</LastModified>
    <ETag>"…"</ETag><Size>…</Size></Part>
  <Initiator>…</Initiator><Owner>…</Owner><StorageClass>STANDARD</StorageClass>
</ListPartsResult>
<ListMultipartUploadsResult><Bucket>…</Bucket><KeyMarker/><UploadIdMarker/>
  <MaxUploads>1000</MaxUploads><IsTruncated>false</IsTruncated>
  <Upload><Key>…</Key><UploadId>…</UploadId><Initiated>…</Initiated>
    <StorageClass>STANDARD</StorageClass><Owner>…</Owner></Upload>
  <CommonPrefixes><Prefix>…</Prefix></CommonPrefixes></ListMultipartUploadsResult>

<!-- бакеты -->
<ListAllMyBucketsResult><Owner><ID>…</ID><DisplayName>…</DisplayName></Owner>
  <Buckets><Bucket><Name>…</Name><CreationDate>…</CreationDate></Bucket>
  </Buckets></ListAllMyBucketsResult>
<LocationConstraint xmlns="…"/><!-- пустой элемент = us-east-1 -->

<!-- копирование и атрибуты -->
<CopyObjectResult><LastModified>…</LastModified><ETag>"…"</ETag></CopyObjectResult>
<CopyPartResult><LastModified>…</LastModified><ETag>"…"</ETag></CopyPartResult>
<GetObjectAttributesOutput><ETag>"…"</ETag><ObjectSize>…</ObjectSize>
  <StorageClass>STANDARD</StorageClass>
  <ObjectParts><PartsCount>1</PartsCount><PartNumberMarker>0</PartNumberMarker>
    <MaxParts>1000</MaxParts><IsTruncated>false</IsTruncated>
    <Part><PartNumber>1</PartNumber><Size>…</Size></Part></ObjectParts>
</GetObjectAttributesOutput>
```

**Запросные XML** (`Delete`, `CompleteMultipartUpload`-манифест,
`CreateBucketConfiguration`): обязательная валидация против схемы —
некорректный/нераспарсиваемый XML → **400** `MalformedXML`
(дополнительные условия — лимиты главы 02).

## 5. Формат ошибок

Канонический XML-формат ошибки (все операции, кроме HEAD — HEAD несёт
только статус):

```xml
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Resource>/bucket/key</Resource>
  <RequestId>3fa85f64-5717-4562-b3fc-2c963f66afa6</RequestId>
  <HostId>owns3-1</HostId>
</Error>
```

- `Resource` — path-style путь запроса.
- `RequestId` — **UUID на каждый запрос**; возвращается и заголовком
  `x-amz-request-id`, пишется в лог запроса (глава 05).
- `HostId` — **статичный идентификатор инстанса** (env `OWNS3_HOST_ID`
  или имя контейнера; глава 05) — одинаков во всех ответах инстанса.

**Маппинг внутренних сбоев на S3-коды** (единая точка маппинга; условия
— глава 02 и разделы выше):

| Код | HTTP | Условие |
|---|---|---|
| `NoSuchBucket` | 404 | бакет не существует (все бакетные/объектные операции) |
| `NoSuchKey` | 404 | ключ не существует (Get/Head/Copy-источник/атрибуты) |
| `BucketAlreadyExists` | 409 | **не эмитируется ownS3** — справочный алиас стандарта; CreateBucket существующего отвечает `BucketAlreadyOwnedByYou` (владельческой модели нет) |
| `BucketAlreadyOwnedByYou` | 409 | CreateBucket: бакет уже существует |
| `BucketNotEmpty` | 409 | DeleteBucket непустого бакета |
| `InvalidRange` | 416 | `Range`-заголовок GetObject/HeadObject вне размера объекта; `bytes=-0`; `a > b` (глава 02) — у copy-операций не используется |
| `PreconditionFailed` | 412 | провал conditional (глава 02, раздел 1) |
| `NotModified` | 304 | 304-исход conditional GET/HEAD (тела нет) |
| `EntityTooLarge` | 400 | объект/часть > 5 ГБ (глава 02, лимиты) |
| `InvalidPart` | 400 | Complete: несовпадение ETag/номера, отсутствующая или < 5 МиБ не последняя часть |
| `InvalidPartOrder` | 400 | Complete: нестрого возрастающий порядок частей |
| `MalformedXML` | 400 | некорректный запросный XML; пустой или > 1000 ключей Delete |
| `AuthorizationHeaderMalformed` | 400 | некорректная структура Authorization/scope |
| `AuthorizationQueryParametersError` | 400 | некорректные presigned-параметры; Expires > 604800; presigned вне разрешённых операций |
| `SignatureDoesNotMatch` | 403 | несовпадение подписи (заголовочной, чанка, трейлера) |
| `AccessDenied` | 403 | нет прав (глава 05); анонимный запрос; просроченный presigned |
| `RequestTimeTooSkewed` | 403 | x-amz-date вне ±15 минут |
| `BadDigest` | 400 | несовпадение Content-MD5; несовпадение trailer-checksum |
| `NoSuchUpload` | 404 | uploadId не существует / повторные Complete/Abort |
| `InvalidArgument` | 400 | невалидные аргументы: partNumber вне 1–10000, encoding-type ≠ url, metadata-directive, пустой x-amz-object-attributes, `x-amz-copy-source-range` — невалидный или вне размера источника (обе причины в один код, как в референсе) |
| `InvalidBucketName` | 400 | имя бакета нарушает правила (раздел 6) |
| `InvalidRequest` | 400 | неподдерживаемый `x-amz-content-sha256`; несовпадение sha256 тела; unsupported trailer header; несвязанный `x-amz-decoded-content-length` |
| `NotImplemented` | 501 | вне-наборные грани (глава 02, раздел 1) |
| `InternalError` | 500 | внутренние сбои; повреждение данных при чтении (checksum главы 04) |

## 6. Транспорт и стиль

- **Path-style only**: бакет всегда в пути (`http://host:9000/{bucket}/{key}`).
  Host-header обрабатывается как обычный host (без извлечения бакета из
  домена) — и в роутинге, и в подписи (canonical URI — путь целиком).
  **Virtual-host style против ownS3 не работает**: клиент обязан
  настроить path-style (AWS SDK .NET — `ForcePathStyle = true`, `mc` —
  path-style по умолчанию). Запрос с virtual-host-закодированным бакетом
  (бакет в первом сегменте домена) маршрутизируется как path-style —
  бакет из host не извлекается (критерий — решения главы 01).
- **HTTP/1.1 — основной транспорт**. HTTP/2 при Kestrel
  (h2c cleartext / за терминирующим прокси) **принимается**: семантика
  всех операций и подписи неизменна (подпись не зависит от версии
  транспорта); явной поддержки ALPN-согласования канон не требует.
- **Лимиты имён**:
  - ключ объекта: **≤ 1024 байт в UTF-8** (незакодированный;
    превышение → 400 `InvalidArgument`); непустой; слэши — часть ключа.
  - имя бакета: **3–63 символа**, строчные `a-z`, `0-9`, `-`; начало и
    конец — буква или цифра; нарушение → 400 `InvalidBucketName`
    (критерий — стандарт S3).
- **`Expect: 100-continue`** — поддерживается Kestrel'ом штатно
  (фиксация: дополнительной обработки ownS3 не требует; клиенты SDK
  отправляют его на крупных PUT).
- Кодирование пути: percent-кодирование сегментов сохраняется как
  отправлено клиентом (canonical URI повторяет кодирование запроса).
