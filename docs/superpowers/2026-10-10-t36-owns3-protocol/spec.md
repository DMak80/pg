# t36-owns3-protocol — каркас сервиса и протокольная обвязка

- **Roadmap**: [`arch/roadmap/owns3.md`](../../../arch/roadmap/owns3.md), пункт
  `t36-owns3-protocol` (трек ownS3: канон t35 → **протокол t36** → хранение
  t37 → multipart t38 → приёмка t39 → erasure coding t40).
- **Формулировка**: «каркас сервиса и протокольная обвязка: HTTP-грань,
  роутинг 22 операций (скелеты хендлеров), проверка подписи SigV4 +
  presigned + чанковая потоковая (фрейминг aws-chunked), XML-сериализация,
  формат S3-ошибок, учётки/доступ; юниты на тест-векторах подписи.
  Референс: `cmd/api-router.go`, `cmd/auth-handler.go`,
  `cmd/signature-v4*.go`, `cmd/streaming-signature-v4.go`».
- **Канон**: [`arch/22-owns3.md`](../../../arch/22-owns3.md) + главы
  [`arch/owns3/01…05`](../../../arch/owns3/01-overview.md). Задача
  реализует главу 03 (протокол) целиком, главу 05 (учётки/доступ/конфиг) —
  без дисковой части, каркас — по главе 01.

**Решения пользователя (2026-10-10):**

1. **Скелеты хендлеров — полный протокольный контур + заглушка объектного
   слоя**: каждый из 22 хендлеров доводит запрос до последнего шага
   (роутинг → подпись → права → парсинг/валидация аргументов → оборачивание
   тела), финальный шаг — вызов контракта объектного слоя с заглушкой,
   отвечающей 500 `InternalError`. t37/t38 подставляют реальные реализации
   без изменения протокольного конвейера.
2. **Проект `src/OwnS3.Storage` создаётся уже в t36**: контракт объектного
   слоя (интерфейс + доменные типы по главе 02) + заглушка. Соответствует
   канону главы 01 (Storage — отдельный проект без HTTP).
3. **Тестовая стратегия — юниты + in-memory HTTP**: `OwnS3.UnitTests`
   (тест-векторы SigV4, XML, ошибки) + `OwnS3.IntegrationTests` на
   `WebApplicationFactory` (in-memory Kestrel без докера; паттерн
   `PgWorker.IntegrationTests/Api`) — сквозные проверки
   роутинга+подписи+прав+ошибок по всем 22 операциям с заглушкой.
4. **Метрики и health — в t36**: каркас `Shared.Metrics` с операционными
   серями `ownS3_requests_total{operation,code}` /
   `ownS3_request_duration_seconds{operation}` (operation-метка возникает в
   роутере t36) и `/healthz` → 200 (без валидации `volume.json` — том
   появляется в t37). Дисковые метрики `ownS3_disk_*` и томная часть health
   — t37; докер-стенд и E2E реальными клиентами — t39.

## 1. Контекст и цель

### 1.1. Контекст

- **Канон готов** (t35 слит): входная точка `arch/22-owns3.md`, главы
  `arch/owns3/01-overview` (состав трёх проектов `OwnS3.Protocol` /
  `OwnS3.Storage` / `OwnS3.App`), `02-operations` (полные контракты 22
  операций), `03-protocol` (SigV4 трёх режимов, XML-схемы, формат ошибок,
  транспорт), `04-storage` (xl-хранение — t37), `05-access-config`
  (учётки, матрица прав, env-таблица, health/метрики).
- **Кода ownS3 нет**: t36 — первая задача-реализация, создаёт все проекты
  `src/OwnS3.*` с нуля.
- **Референс** — копия исходников MinIO (Go) по пути
  `/Users/demakaev/ZCodeProject/minio` (от канона — `../../../minio`):
  точечный референс, не образец. Карта для t36: роутинг —
  `cmd/api-router.go` (таблица маршрутов method+path+query-дискриминаторов,
  порядок специфичное→общее, rejected-APIs), аутентификация —
  `cmd/auth-handler.go` (типы auth по заголовкам, clock skew,
  конвейер authenticate→authorize), подпись — `cmd/signature-v4.go`
  (canonical request / string-to-sign / signing key), парсинг —
  `cmd/signature-v4-parser.go`, канонизация —
  `cmd/signature-v4-utils.go` (`signV4TrimAll`, `extractSignedHeaders`,
  `getContentSha256Cksum`), потоковая — `cmd/streaming-signature-v4.go`
  (`s3ChunkedReader`: фрейминг, цепочка подписей, трейлеры, `maxChunkSize`
  16 МиБ).
- **Монорепо-паттерны**: .NET 10, `TreatWarningsAsErrors=true`, CPM
  (`Directory.Packages.props`), решение `src/PgWorker.slnx` (общее для всех
  сервисов); тестовый хост — `WebApplicationFactory` (прецеденты
  `PgWorker.IntegrationTests/Api`, `KafkaWorker.IntegrationTests/Api`);
  метрики — `Shared.Metrics` (`AddAppMetrics`/`MapAppMetrics`, канон
  [`arch/18-metrics.md`](../../../arch/18-metrics.md): имя Meter = имя
  системы, dot-нотация инструментов, лейблы конечны).
- **Целевые клиенты** (приёмка — t39): AWS SDK .NET подсистемы бэкапов
  (`ForcePathStyle`, статические ключи) и `mc`; оба работают path-style и
  подписывают SigV4.

### 1.2. Цель

Создать протокольный каркас ownS3 — всё, что окружает дисковое хранение:

1. **`src/OwnS3.Protocol`** — полная реализация главы 03: SigV4 всех трёх
   режимов (заголовочная, presigned, чанковая потоковая с aws-chunked и
   трейлерами), XML-схемы запросов/ответов, канонический формат S3-ошибок
   с полным каталогом кодов, валидация имён бакета/ключа, path-стиль.
   Чистые типы без ASP.NET и без диска.
2. **`src/OwnS3.Storage`** — контракт объектного слоя: интерфейс 22
   операций в доменных терминах главы 02 + доменные типы результатов +
   заглушка `NotWiredObjectStore` (решение пользователя 2). Реализация
   xl-хранения — t37.
3. **`src/OwnS3.App`** — хост Kestrel: конвейер обработки (RequestId →
   роутинг → аутентификация SigV4 → авторизация по матрице прав → хендлер),
   роутер 22 операций с дискриминаторами, 22 хендлера полного протокольного
   контура (решение пользователя 1), учётки (root + статические ключи из
   env) и матрица прав главы 05, конфиг/env с fail-fast, метрики и
   `/healthz` (решение пользователя 4).
4. **Тесты**: `src/tests/OwnS3.UnitTests` (тест-векторы подписи, XML,
   ошибки, валидация) и `src/tests/OwnS3.IntegrationTests`
   (WebApplicationFactory, сквозной протокол по HTTP — решение
   пользователя 3).
5. **Arch-правки**: закрыть обнаруженные при проектировании пробелы канона
   главы 03 (§3.1) — arch-first, до кода.

### 1.3. НЕ-цели (границы)

- **Без дискового хранения**: никакой раскладки тома, `xl.meta`,
  `.owns3.sys`, `volume.json` — t37. `OWNS3_DATA_DIR` читается в конфиг, но
  не используется; материализация ключей в `.owns3.sys/config/` — t37
  (учётки t36 живут в памяти из env).
- **Без multipart-механики**: раскладка загрузок, `uploads.json`, составной
  ETag — t38; протокольные контракты Create/UploadPart/Complete/Abort/
  ListParts/ListMultipartUploads (роутинг, парсинг, валидация, права) —
  t36.
- **Без докера, образов, dev-станда, E2E реальными клиентами** — t39;
  ownS3 в t36 тестируется только in-memory тестовым хостом. Локально
  собираемый образ не создаётся, в registry ничего не кладётся.
- **Без TLS** (HTTP-only, глава 03), **без virtual-host style**, **без
  вне-наборных граней** (единый отказ 501 — по канону).
- **Не трогаем** `arch/19-backups.md`, код PgWorker/KafkaWorker/AdminPanel;
  изменения монорепо — только добавление проектов OwnS3, строки в
  `src/PgWorker.slnx` и `Directory.Packages.props`.
- **Не реализуем conditional/Range/ETag-семантику данных** (чтение
  объекта, сравнение, 304/412/206): это семантика объектного слоя — t37. В
  t36 заголовки парсятся и валидацию формата проходят (если канон её
  фиксирует), оценка — с данными в t37.

## 2. Принципы

1. **arch-first**: канон — источник истины; пробелы канона, обнаруженные
   при проектировании, закрываются правкой `arch/owns3/03-protocol.md`
   (список — §3.1) в первой фазе, до кода. Код зеркалит канон; расхождение
   проекта с `arch/` — блокирующее замечание ревью.
2. **Критерий правильности — стандарт S3 API**: поведение, ожидаемое
   стандартными S3-клиентами (AWS SDK .NET, `mc`). Отступления от
   референса MinIO фиксируются явно (как в каноне) с критерием.
3. **MinIO — точечный референс**: сверка семантик подписи/роутинга с
   файлами-референсами; перенос механики целиком не делается (маппинг
   ошибок, IAM, трейсинг, CORS, throttling MinIO не берутся).
4. **Границы проектов — по канону главы 01**: `Protocol` — чистые типы
   (без ASP.NET, без диска); `Storage` — без HTTP; `App` — от обоих;
   `Shared.*` — по надобности. `Protocol` и `Storage` не зависят друг от
   друга.
5. **Надёжность**: сравнение подписей — constant-time; отказ старта на
   невалидной конфигурации (fail-fast: root-пара обязательна, длины
   user ≥ 3 / password ≥ 8 — глава 05); сверка целостности тела (sha256 /
   Content-MD5 / чанковые подписи / trailer-checksum) происходит при
   чтении тела — до передачи данных дальше; никаких «молчаливых» веток:
   каждый исход проверки подписи имеет канонный S3-код.
6. **Скелет = полный протокольный контур** (решение пользователя 1):
   хендлер t36 отличается от хендлера t37 ровно одним последним шагом —
   реализация `IObjectStore` вместо заглушки. Протокольный конвейер
   (подпись/права/парсинг/валидация/формат ответа) в t36 финален и не
   переписывается в t37.
7. **Язык и стиль**: документация и комментарии — по-русски,
   идентификаторы — на английском; тесты — по нотации AAA (комментарии
   Arrange/Act/Assert); стиль — как у соседних сервисов монорепо
   (ValkeyWorker.App, KafkaWorker.App).

## 3. Структура и компоненты

### 3.1. Arch-правки: пробелы канона, закрываемые t36

Первой фазой вносятся в [`arch/owns3/03-protocol.md`](../../../arch/owns3/03-protocol.md)
(источник истины — правки контракта до кода; все — заполнение пробелов,
не меняющее существующих решений):

| # | Пробел | Решение (внести в канон) | Критерий |
|---|---|---|---|
| 1 | Поведение при несуществующем accessKey (в `Authorization`/`X-Amz-Credential`) не зафиксировано | Новый код `InvalidAccessKeyId` (403) в таблицу маппинга §5: «accessKey не существует» | референс: `ErrInvalidAccessKeyID` 403; стандарт S3 |
| 2 | Отсутствие/невалидный формат `x-amz-date` (и `Date`) — код ошибки не назван | → 400 `AuthorizationHeaderMalformed` (Message «Missing/Invalid x-amz-date header») — без заведения отдельного кода | простота; референс отвечает 400 (`ErrMissingDateHeader`/`ErrMalformedDate`) |
| 3 | Невалидный синтаксис aws-chunked-фрейма и лимит размера чанка не зафиксированы | → 400 `InvalidRequest` (Message «Malformed chunked encoding»); лимит одного чанка — **16 МиБ** (превышение — тот же исход) | референс: `errMalformedEncoding`/`errChunkTooBig` → 400, `maxChunkSize = 16 << 20` |
| 4 | Чанковая подпись (`STREAMING-*`) на не-PUT методах не оговорена | Чанковые режимы применяются только к PUT с телом (`PutObject`, `UploadPart`); значение `STREAMING-*` на прочих методах → 400 `InvalidRequest` | референс: `isRequestSignStreamingV4` = значение заголовка ∧ `MethodPut`; стандарт SigV4 |
| 5 | Поведение OPTIONS-запроса не зафиксировано | → пустой ответ 200 без CORS-заголовков, до аутентификации (CORS-модели нет) | референс: ранний `return` в `errorResponseHandler` |
| 6 | Запрос, не матчатщийся ни на одну из 22 операций (метод+path+query) и не содержащий вне-наборных сабресурсов | → 400 `InvalidArgument` (Message «Unsupported request»); известный путь с неподдерживаемым методом — тот же исход | референс: `ErrUnknownAPIRequest` 400; канонный каталог кодов ownS3 расширяется минимально — отдельный код не заводится (простота) |
| 7 | §1 п.4 фиксирует только усечение значений canonical headers по краям; схлопывание внутренних последовательностей пробелов (Trimall стандарта SigV4) не зафиксировано | Дополнить §1 п.4: значение заголовка — усечение по краям **и** схлопывание внутренних последовательностей пробелов в один пробел | стандарт SigV4 (Trimall); референс: `signV4TrimAll` (`signature-v4-utils.go`: `strings.Fields` → join одним пробелом) |
| 8 | Исход при алгоритме в `Authorization`, отличном от `AWS4-HMAC-SHA256` (вкл. SigV2-заголовок `AWS …`), не зафиксирован | → 400 **`InvalidRequest`**, Message «The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.» (дополнить §1, «Исходы проверки подписи») | референс: `ErrSignatureVersionNotSupported` → Code `InvalidRequest`, 400 (`api-errors.go`); документированное поведение Amazon S3 для SigV2; семантика «механизм не поддерживается» ≠ «малформированный заголовок» — отдельный код не заводится |
| 9 | §1 п.3 («`key=value` с URI-кодированием») не фиксирует правило для пробела и `+` — типовая ловушка: «плюс как пробел» — семантика form-декодирования query при разборе, а не канонизации | Дополнить §1 п.3: percent-кодирование RFC 3986 — **пробел → `%20`, литеральный `+` → `%2B`**; «плюс как пробел» применяется только при разборе query-параметров (form-декодирование), в канонизации не участвует | стандарт SigV4; референс: `getCanonicalRequest` (`signature-v4.go`): `Form.Encode()` даёт пробел как `+` (form-кодирование), `ReplaceAll("+", "%20")` заменяет его на `%20`; литеральный `+` на входе уже `%2B` и не затрагивается |
| 10 | Строка §2 «Clock skew ±15 минут применяется и к `X-Amz-Date`» противоречит самому §2 (максимум `X-Amz-Expires` 604800; «просроченный → AccessDenied» — значит непросроченный обязан приниматься) и референсу: буквальное прочтение ограничивает фактическую жизнь presigned 15 минутами | Заменить формулировку §2: skew для presigned применяется **только к будущему** — `X-Amz-Date > now + 15 мин` → 403 `RequestTimeTooSkewed`; для прошедших дат skew-отказов нет — URL валиден всё время окна: просрочка определяется строго как `now − X-Amz-Date > X-Amz-Expires` → 403 `AccessDenied`; уточнить условие `RequestTimeTooSkewed` в таблице §5 (заголовочная подпись: ±15 мин; presigned: только будущее) | референс: Abs-skew (`auth-handler.go`) — только заголовочно-подписанные типы; для presigned (`signature-v4.go`, `doesPresignedSignatureMatch`) — «дата из будущего за skew» + просрочка `now − date > Expires`; отступление: «будущее за skew» у референса — Code `AccessDenied` («Request is not valid yet»), ownS3 нормализует в `RequestTimeTooSkewed` — единый код каталога «время вне допуска», статус 403 совпадает |

Правки вносятся в соответствующие разделы главы 03 (§1 — п. 1–2 и 7–9,
§2 — п. 10, §3 — п. 3–4, §6 — п. 5–6; таблица §5 — строка
`InvalidAccessKeyId` и уточнение условия `RequestTimeTooSkewed`).
Прочие главы не затрагиваются.

### 3.2. `src/OwnS3.Protocol` — чистые типы протокола

Без ASP.NET, без диска; единственная внешняя зависимость — пакет
`System.IO.Hashing` (CRC32/CRC32C для trailer-checksum). Внутреннее
устройство:

**Транспортно-независимая модель запроса.** `S3RequestModel` — POCO:
HTTP-метод, сырой путь (как прислал клиент, без декодирования), сырой
query, коллекция заголовков (регистронезависимая, с доступом к сырым
ключам для canonical headers), host, доступ к потоку тела. `App`
строит модель из `HttpRequest`; тесты — напрямую. Вся логика `Protocol`
работает только с моделью (тестируемость без хоста).

**SigV4 — заголовочный режим** (глава 03 §1):

- `AuthorizationHeaderParser` — парсинг
  `AWS4-HMAC-SHA256 Credential=<ak>/<date>/<region>/s3/aws4_request,
  SignedHeaders=..., Signature=...`: структура (3 поля после алгоритма),
  scope (service обязан быть `s3`, терминал `aws4_request`, дата
  `yyyyMMdd`), accessKey (может содержать `/`); ошибки структуры/scope →
  `AuthorizationHeaderMalformed`; алгоритм, отличный от
  `AWS4-HMAC-SHA256` (вкл. SigV2-заголовок `AWS …`), → 400
  `InvalidRequest` с Message «The authorization mechanism you have
  provided is not supported. Please use AWS4-HMAC-SHA256.» (arch-правка
  §3.1 п. 8).
  Референс `signature-v4-parser.go`.
- `CanonicalRequestBuilder`:
  - **canonical URI** — path-style путь; канонизация по образцу
    `s3utils.EncodePath` референса: существующее percent-кодирование
    (`%XX`) сохраняется как отправлено, незакодированные символы вне RFC
    3986 unreserved кодируются, `/` — разделитель сегментов (глава 03 §1,
    §6);
  - **canonical query** — все параметры (включая дискриминаторы и
    `response-*`), сортировка по ключу, затем по значению (сравнение до
    кодирования), percent-кодирование RFC 3986: **пробел → `%20`,
    литеральный `+` → `%2B`** («плюс как пробел» — только семантика
    form-декодирования при разборе query-параметров, в канонизации не
    участвует — arch-правка §3.1 п. 9);
  - **canonical headers** — имена в нижнем регистре, сортировка по имени,
    значения: усечение пробелов по краям + схлопывание внутренних
    последовательностей пробелов в один (Trimall стандарта SigV4 —
    arch-правка §3.1 п. 7; референс `signV4TrimAll`), многозначные
    заголовки — join через
    запятую; `host` обязан быть в SignedHeaders (иначе —
    `SignatureDoesNotMatch`-семантика отказа: невалидный список
    подписанных заголовков);
  - **payload-строка** — значение `x-amz-content-sha256` по режимам
    (таблица главы 03 §1): hex-sha256 / `UNSIGNED-PAYLOAD` /
    `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` /
    `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`; прочие значения → 400
    `InvalidRequest` «Unsupported x-amz-content-sha256 value».
- `StringToSign` (`AWS4-HMAC-SHA256\n<x-amz-date>\n<scope>\n<hex-sha256(canonical)>`),
  `SigningKey` (HMAC-цепочка `AWS4<secret>` → date → region → `s3` →
  `aws4_request`), `SignatureCalculator` — hex-HMAC.
- `SigV4HeaderVerifier` — сборка конвейера: parse → canonical → string to
  sign → signature → **constant-time сравнение** (`CryptographicOperations.FixedTimeEqual`);
  исходы: `SignatureDoesNotMatch` (403), `AuthorizationHeaderMalformed`
  (400), `RequestTimeTooSkewed` (403, skew ±15 мин; `x-amz-date`, при
  отсутствии — `Date`), `InvalidAccessKeyId` (403, arch-правка §3.1 п. 1),
  `InvalidRequest` (400, неподдерживаемый алгоритм Authorization —
  arch-правка §3.1 п. 8).
  Регион scope — заполнитель: любое значение принимается (согласованность
  внутри запроса: scope из `Credential` используется и в signing key, и в
  string-to-sign).

**SigV4 — presigned** (глава 03 §2): `PresignedRequestVerifier` —
`X-Amz-Algorithm/-Credential/-Date/-Expires/-SignedHeaders/-Signature`;
все параметры обязательны (отсутствие → `AuthorizationQueryParametersError`
400); `Expires` ∈ [0, 604800] (нарушение → тот же 400); **skew — только
на будущее**: `X-Amz-Date > now + 15 мин` → 403 `RequestTimeTooSkewed`
(arch-правка §3.1 п. 10); просрочка — строгое `now − X-Amz-Date >
X-Amz-Expires` → 403 `AccessDenied`; непросроченный presigned
принимается всё время окна независимо от возраста (никакого Abs-skew
для прошедших дат); payload-строка
canonical request — `UNSIGNED-PAYLOAD`; canonical query — все параметры
запроса кроме `X-Amz-Signature`. Список разрешённых операций (ровно 10 по
канону: GetObject, PutObject, DeleteObject, HeadObject,
CreateMultipartUpload, UploadPart, UploadPartCopy,
CompleteMultipartUpload, AbortMultipartUpload, ListParts) — константа
`Protocol` (`S3PresignedOperations`); операция вне списка → 400
`AuthorizationQueryParametersError`. Имя операции передаётся верификатору
вызывающей стороной (роутер `App` определил операцию раньше).

**SigV4 — чанковая потоковая** (глава 03 §3): `AwsChunkedReader` — обёртка
`Stream` над телом запроса:

- seed-подпись — заголовочная подпись запроса с payload-строкой = значению
  режима (вычисляется верификатором заголовочного режима; результат — seed
  для цепочки);
- фрейминг `<hex-size>;chunk-signature=<sig>\r\n<data>\r\n`, финальный
  `0;chunk-signature=<sig>\r\n` без данных;
- цепочка: string-to-sign чанка N =
  `AWS4-HMAC-SHA256-PAYLOAD\n<date>\n<scope>\n<подпись N-1>\n<sha256("")>\n<sha256(чанк N)>`;
  несовпадение подписи чанка → 403 `SignatureDoesNotMatch` — отказ в
  момент чтения чанка (референс `streaming-signature-v4.go`);
- сверка суммы данных чанков с `x-amz-decoded-content-length` → 400
  `InvalidRequest`;
- синтаксис фрейма и лимит чанка 16 МиБ → 400 `InvalidRequest` «Malformed
  chunked encoding» (arch-правка §3.1 п. 3);
- трейлер-режим (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`): список
  `x-amz-trailer` (поддерживаются `x-amz-checksum-crc32/-crc32c/-sha1/
  -sha256`; прочие имена → 400 `InvalidRequest` «Unsupported trailer
  header»); после финального чанка — строки трейлеров в порядке
  `x-amz-trailer`, подпись `AWS4-HMAC-SHA256-TRAILER` по конкатенации
  trailer-строк, завершается `x-amz-trailer-signature:<sig>`
  (несовпадение → 403 `SignatureDoesNotMatch`); значения checksum — base64,
  сверяются с декодированным телом (несовпадение → 400 `BadDigest`;
  CRC32/CRC32C — `System.IO.Hashing`, SHA-1/SHA-256 — BCL);
- применимость — только PUT с телом (arch-правка §3.1 п. 4).

**XML-схемы** (глава 03 §4): `S3Xml`-типы для `System.Xml.Serialization`
(XmlSerializer; namespace `http://s3.amazonaws.com/doc/2006-03-01/`,
UTF-8):

- запросные: `Delete` (Object/Key + Quiet; до 1000 ключей — лимит
  проверяет хендлер, парсер — форму), `CompleteMultipartUpload`
  (Part/PartNumber+ETag), `CreateBucketConfiguration` (принимается и
  игнорируется);
- ответные: `ListBucketResult` (v1 и V2 — один тип с опциональными
  полями), `ListVersionsResult` (unversioned-вид), `DeleteResult`,
  `InitiateMultipartUploadResult`, `CompleteMultipartUploadResult`,
  `ListPartsResult`, `ListMultipartUploadsResult`,
  `ListAllMyBucketsResult`, `LocationConstraint` (пустой элемент),
  `CopyObjectResult`, `CopyPartResult`, `GetObjectAttributesOutput`;
- сериализация: XML-сущности для спецсимволов, недопустимые кодпоинты —
  числовыми ссылками; `Content-Type: application/xml`;
- десериализация запросных XML: нераспарсиваемый/не соответствующий схеме
  XML → 400 `MalformedXML` (дополнительные условия — лимиты главы 02,
  проверяются хендлером).

**Формат ошибок** (глава 03 §5): `S3Error` (Code, Message, Resource,
RequestId, HostId) + `S3ErrorCode` — полный каталог канонной таблицы
(каждый код: строка Code, канонический Message, HTTP-статус) + коды
arch-правок §3.1 (`InvalidAccessKeyId`); `S3ErrorXmlWriter` —
канонический XML. `Resource` — path-style путь запроса; `RequestId` —
UUID на запрос (генерирует `App`, `Protocol` только форматирует);
`HostId` — из конфига.

**Транспортные утилиты**: `S3PathParser` — разбор path-style
(`/{bucket}`, `/{bucket}/{key+}`, `/`), декодирование сегментов ключа;
`BucketNameValidator` (3–63 символа, `[a-z0-9-]`, начало/конец — буква
или цифра → иначе `InvalidBucketName`); `ObjectKeyValidator` (непустой,
≤ 1024 байт UTF-8 → иначе `InvalidArgument`); матрица «имя операции →
минимальная роль» — не здесь (это доступ, глава 05 — `App`).

### 3.3. `src/OwnS3.Storage` — контракт объектного слоя + заглушка

Без HTTP; **ссылок на `OwnS3.Protocol` нет** — глава 01 фиксирует
независимость `Protocol` и `Storage` в обе стороны. Контракт оперирует
собственными доменными типами и собственным перечислением кодов исходов
(`ObjectStoreErrorCode`; значения-имена совпадают с S3-кодами каталога
главы 03 — `NoSuchBucket`, `NoSuchKey`, …); `App` маппит исходы Storage
в `S3Error` из Protocol по каталогу. Это сохраняет независимость
проектов канона и тестируемость Storage без протокола (t37).

Состав:

- **`IObjectStore`** — контракт всех 22 операций в доменных терминах
  главы 02, сгруппирован по семействам (сигнатуры — ориентировочные,
  финализируются планом с учётом типов результатов; изменения в t37
  допустимы без изменения протокольного слоя `App`):
  - бакеты: `CreateBucketAsync`, `DeleteBucketAsync` (исходы
    `BucketAlreadyOwnedByYou`/`BucketNotEmpty`/`NoSuchBucket`),
    `BucketExistsAsync`, `ListBucketsAsync`;
  - объекты: `PutObjectAsync(bucket, key, body: Stream, metadata)` →
    `PutResult(ETag)`; `GetObjectAsync` → `ObjectContent` (метаданные +
    стрим); `HeadObjectAsync` → метаданные; `DeleteObjectAsync`;
    `DeleteObjectsAsync(keys, quiet)` → по-ключевые исходы;
    `CopyObjectAsync`; `GetObjectAttributesAsync`;
  - листинги: `ListObjectsAsync(ListQuery)` — один метод с параметрами
    (prefix/delimiter/marker/continuation/max-keys/encoding-type/вариант
    v1|V2|versions) → `ListPage` (Contents + CommonPrefixes +
    continuation-данные по варианту);
  - multipart: `CreateMultipartUploadAsync` → uploadId;
    `UploadPartAsync`; `UploadPartCopyAsync`; `CompleteMultipartUploadAsync`;
    `AbortMultipartUploadAsync`; `ListPartsAsync`; `ListMultipartUploadsAsync`.
- **Доменные типы**: метаданные объекта (ETag, LastModified, ContentType,
  user-metadata, размер), записи листингов, части загрузок и пр. — по
  контрактам главы 02.
- **Ошибки контракта**: `ObjectStoreException(ObjectStoreErrorCode, Message)`
  — доменные исходы (не найдено/конфликт/…); `App` переводит в S3-ошибку
  по каталогу.
- **`NotWiredObjectStore`** — заглушка t36 (решение пользователя 1):
  - методы **с телом** (`PutObject`, `UploadPart`) — сначала дочитывают
    (`drain`) переданный поток до конца: это форсирует полную сверку
    подписи и хэшей тела конвейером `App` (sha256-режим, Content-MD5,
    чанковая подпись, trailer-checksum) — протокольные проверки остаются
    наблюдаемыми в тестах t36; после drain — бросают
    `ObjectStoreUnavailableException`;
  - методы **без тела** — сразу бросают `ObjectStoreUnavailableException`;
  - `App` маппит `ObjectStoreUnavailableException` → 500 `InternalError`.
- DI: `IObjectStore` → `NotWiredObjectStore` (регистрация в `Program`);
  t37 подставляет реализацию xl-хранения на том же контракте.

### 3.4. `src/OwnS3.App` — хост и конвейер

**`Program.cs`** (паттерн ValkeyWorker.App: top-level, `public partial
class Program` для WAF):

- `OwnS3Options` из секции `OwnS3` / env `OWNS3_*` (таблица главы 05 §4):
  `Server:Port` (дефолт 9000), `DataDir` (дефолт `/data`; в t36 читается,
  не используется), `Root:User/Password`, `AccessKeys[]`
  (`{AccessKey, SecretKey, Policy}`), `HostId`.
- **Fail-fast** (глава 05 §1): `ValidateOnStart` — `Root:User` ≥ 3 и
  `Root:Password` ≥ 8, оба непусты; нарушение — отказ старта (лог +
  ненулевой exit).
- Kestrel: `ListenAnyIP(port)`, `Protocols = Http1AndHttp2` (h2c
  принимается — глава 03 §6), **лимит тела отключён**
  (`MaxRequestBodySize = null`): лимит 5 ГБ — уровень хендлера (глава 02),
  не транспорта;
- DI: `TimeProvider.System`, `AccessKeyRegistry`, `IObjectStore` →
  `NotWiredObjectStore`, хендлеры, метрики `AddAppMetrics("ownS3", …)`
  (канон arch/18: имя Meter = имя системы — `ownS3` строчными, чтобы
  финальные имена серий совпали с каноном ownS3 §5).
- Маппинги вне S3-конвейера: `/healthz` (глава 05 §5: в t36 — 200 без
  проверки тома; томная валидация `volume.json` — t37) и `/metrics`
  (`MapAppMetrics`).

**Конвейер S3** (собственный middleware-конвейер; встроенные
UseAuthentication/Authorization ASP.NET не используются — SigV4 не
совместим с ними семантически). Порядок (каждый шаг — канонизированные
исходы):

1. **RequestId** — UUID v4 на каждый запрос; заголовок `x-amz-request-id`
   в каждом ответе (успех и ошибка); лог-скоуп `RequestId` (глава 03 §5).
2. **OPTIONS** → пустой 200 без CORS-заголовков (arch-правка §3.1 п. 5).
3. **`S3Router`** — определение операции по таблице маршрутов (матчинг
   специфичное→общее, референс `api-router.go`):
   - разбор path-style: `/` (ListBuckets) / `/{bucket}` / `/{bucket}/{key+}`
     (сырой путь — для подписи, декодированные сегменты — для домена);
   - дискриминаторы: query (`uploads`, `uploadId`, `partNumber`,
     `delete`, `location`, `list-type=2`, `versions`, `attributes`) и
     заголовок `x-amz-copy-source` (PUT с ним → CopyObject/UploadPartCopy);
   - порядок (от специфичного к общему): GetObjectAttributes
     (`?attributes`) раньше GetObject; ListObjectsV2 (`list-type=2`)
     раньше ListObjects v1; Copy-семейство (по заголовку
     `x-amz-copy-source`) раньше PutObject/UploadPart;
     ListMultipartUploads (`?uploads`) и ListObjectVersions (`?versions`)
     раньше ListObjects v1 на том же методе; полная таблица с точным
     порядком — в плане (скелет — §3.4.1);
   - **вне-наборные сабресурсы** (перечень главы 02 §1: `acl`, `tagging`,
     `retention`, `legal-hold`, `torrent`, `restore`, `versioning`,
     `lifecycle`, `replication`, `encryption`, `policy`, `cors`,
     `website`, `notification`, `accelerate`, `object-lock`, `logging`,
     `metrics`, `inventory`, `intelligent-tiering`, `ownershipControls`,
     `publicAccessBlock`, `requestPayment`, `select`) → 501
     `NotImplemented` «A header you provided implies functionality that
     is not implemented» — **до аутентификации** (по референсу:
     rejected-APIs не проходят auth);
   - вне-наборные query-параметры-не-сабресурсы (`x-id` и т.п.) —
     игнорируются (глава 02 §1);
   - отсутствие матча → 400 `InvalidArgument` «Unsupported request»
     (arch-правка §3.1 п. 6);
   - operation-метка (`PutObject`, …; для вне-наборных — имя сабресурса;
     `unknown` для не-матча) — в контекст запроса (метрики/лог).
4. **Аутентификация** — `S3Authenticator` (тип: заголовочная подпись /
   presigned / аноним):
   - анонимный (нет `Authorization` и presigned-параметров) → 403
     `AccessDenied` (public-доступа нет, глава 03 §1);
   - заголовочная: `SigV4HeaderVerifier` (Protocol); при
     `x-amz-content-sha256` = чанковый режим — заголовочная подпись
     считается с payload-строкой режима (seed), тело оборачивается в
     `AwsChunkedReader`;
   - presigned: `PresignedRequestVerifier` (Protocol) с именем операции
     из роутера;
   - результат — `AuthenticatedIdentity` (accessKey, policy-роль).
5. **Авторизация** — `AccessPolicy`-матрица главы 05 §3: роль →
   минимальные права на операцию (`OperationAccessMatrix`: read-only /
   read-write / admin × 22 операции); отказ → 403 `AccessDenied`.
   «Свои» загрузки для read-only (ListParts/ListMultipartUploads) —
   фильтрация по создателю появляется с данными загрузок (t38); в t36
   read-only допускается к этим операциям без фильтра (загрузок нет,
   заглушка отвечает `InternalError` после проверок).
6. **Хендлер операции** (§3.4.1) — парсинг/валидация аргументов по
   контракту главы 02 → оборачивание тела (при hex-sha256 —
   `HashingStream` с пост-сверкой по завершении чтения; при Content-MD5 —
   одновременный подсчёт MD5) → `IObjectStore` → сборка ответа.
7. **Единый обработчик ошибок**: `ObjectStoreException` → S3-код;
   `ObjectStoreUnavailableException` → 500 `InternalError`;
   неперехваченное исключение → 500 `InternalError` (+ диагностика в
   лог, Message без внутренних деталей); HEAD — только статус/заголовки,
   без тела (глава 03 §5).
8. **Метрики** (решение пользователя 4; канон arch/18 + глава 05 §5):
   после ответа — `ownS3.requests` counter (лейблы `operation`, `code` =
   HTTP-код) и `ownS3.request.duration` histogram (seconds, лейбл
   `operation`) → финальные имена `ownS3_requests_total{operation,code}` /
   `ownS3_request_duration_seconds{operation}`; лейбл `operation` —
   конечное множество (22 операции + имена вне-наборных сабресурсов +
   `unknown`). `ownS3_disk_*` — t37. Структурный лог запроса:
   requestId, operation, bucket, key, метод, код ответа, длительность.

#### 3.4.1. Хендлеры 22 операций — протокольный контур t36

Единый каркас `OperationHandlerBase` (RequestId/лог/метрики — в
конвейере; хендлер: парсинг → валидация → storage → ответ). Что именно
работает в t36 (и остаётся неизменным в t37+):

- **Дискриминация и парсинг по контракту главы 02**: query-параметры,
  заголовки (Content-Type, Content-MD5, `x-amz-meta-*`, `x-amz-storage-class`
  — принимается и игнорируется, `x-amz-copy-source`, metadata-directive,
  conditional-заголовки, `x-amz-object-attributes`, `x-amz-max-parts`,
  `x-amz-part-number-marker`), XML-тела (`Delete`,
  `CompleteMultipartUpload`, `CreateBucketConfiguration`).
- **Протокольная валидация**: имена бакета/ключа (глава 03 §6);
  `partNumber` 1–10000 (иначе 400 `InvalidArgument` — глава 02); лимиты
  DeleteObjects (пустой/больше 1000 ключей → `MalformedXML`); порядок
  частей Complete (строго возрастающий → `InvalidPartOrder`; пустой
  манифест → `MalformedXML`); `encoding-type` (только `url`, иначе
  `InvalidArgument`); `max-keys`/`max-parts`/`max-uploads` (дефолт 1000,
  максимум 1000); `x-amz-metadata-directive` (COPY/REPLACE, иначе
  `InvalidArgument`); `x-amz-object-attributes` (непустой/отсутствующий
  → `InvalidArgument`; поддерживаемые значения — ETag/ObjectSize/
  StorageClass/ObjectParts; прочие значения (вкл. Checksum) — вне-наборные
  грани → 501, глава 02); EntityTooLarge 5 ГБ
  (уровень хендлера: по Content-Length / `x-amz-decoded-content-length`).
- **Сверка целостности тела** (при наличии тела): sha256-режим (hex) —
  при дочитывании тела; Content-MD5 — при дочитывании (`BadDigest`);
  чанковая подпись — по чанкам при чтении; trailer-checksum — по
  завершении. Тело читает `NotWiredObjectStore` (drain) — см. §3.3.
- **Финальный шаг** — `IObjectStore` (заглушка → 500 `InternalError`),
  кроме:
  - **`GetBucketLocation`** — полностью протокольная операция (регион —
    константа `us-east-1`, пустой `LocationConstraint`): в t36 работает
    целиком (успех 200) без объектного слоя; дискриминатор `?location`,
    права — по матрице (все роли).
- Ответы-заголовки t36: `x-amz-request-id` (конвейер), `ETag`/тела —
  только с данными (t37).

Таблица маршрутов (скелет; полная таблица с точным порядком — в плане;
источник — глава 02, порядок — референс `api-router.go`):

| Операция | Метод+path | Дискриминатор |
|---|---|---|
| ListBuckets | GET `/` | — |
| CreateBucket / DeleteBucket / HeadBucket | PUT/DELETE/HEAD `/{bucket}` | — |
| GetBucketLocation | GET `/{bucket}` | `?location` |
| ListObjectsV2 / ListObjects (v1) / ListObjectVersions / ListMultipartUploads / DeleteObjects | POST/GET `/{bucket}` | `?list-type=2` / — / `?versions` / `?uploads` / `?delete` |
| PutObject / CopyObject | PUT `/{bucket}/{key+}` | `x-amz-copy-source` |
| GetObjectAttributes / GetObject / HeadObject / DeleteObject | GET/HEAD/DELETE `/{bucket}/{key+}` | `?attributes` / — |
| CreateMultipartUpload / CompleteMultipartUpload | POST `/{bucket}/{key+}` | `?uploads` / `?uploadId` |
| UploadPart / UploadPartCopy | PUT `/{bucket}/{key+}` | `?partNumber&uploadId` (+`x-amz-copy-source`) |
| AbortMultipartUpload / ListParts | DELETE/GET `/{bucket}/{key+}` | `?uploadId` |

### 3.5. Тестовые проекты

**`src/tests/OwnS3.UnitTests`** (xunit.v3 + FluentAssertions; без хоста):

- **Тест-векторы SigV4** (канон roadmap: «юниты на тест-векторах
  подписи»): фикстуры «запрос → ожидаемая подпись» с захардкоженными
  ожиданиями. Источники векторов: (а) официальные примеры AWS SigV4
  (AWS General Reference, sigv4-signed-request-examples) — эталон внешнего
  происхождения; (б) векторы, построенные тестовым signer'ом —
  независимой клиентской реализацией подписи в тестовом проекте (не
  переиспользует код `Protocol`), ожидания зафиксированы литералами.
  Покрытие: заголовочная подпись (позитив; битая подпись; битый scope;
  skew > 15 мин; несуществующий accessKey; отсутствующий/невалидный
  x-amz-date; неподдерживаемый алгоритм Authorization, вкл. SigV2 → 400
  `InvalidRequest`; UNSIGNED-PAYLOAD; hex-sha256), канонизация
  (спецсимволы пути, пробел → `%20` и литеральный `+` → `%2B` в query,
  многозначные заголовки, Trimall —
  краевые и внутренние пробельные последовательности, регистр имён),
  presigned (валидный; **использование через N > 15 мин внутри окна
  `X-Amz-Expires` → Ok** — Abs-skew для прошедших дат отсутствует;
  `X-Amz-Date` в будущем дальше +15 мин → 403 `RequestTimeTooSkewed`;
  просроченный → 403 `AccessDenied`; Expires > 604800; отсутствующие
  параметры; операция вне разрешённых 10).
- **AwsChunkedReader**: корректный мультичанковый поток + финальный
  0-чанк; битая подпись чанка (в момент чтения); битый фрейм; чанк > 16
  МиБ; несовпадение `x-amz-decoded-content-length`; trailer-режим
  (валидные crc32/crc32c/sha1/sha256; `BadDigest` на несовпадение;
  неподдерживаемое имя трейлера; битая trailer-подпись).
- **XML**: сериализация всех схем главы 03 §4 — сравнение с эталонными
  XML-строками (вкл. спецсимволы/кодпоинты); десериализация запросных
  XML; `MalformedXML`.
- **Ошибки**: каталог кодов (код ↔ статус ↔ Message); `S3ErrorXmlWriter`.
- **Валидация имён**: бакеты (валидные/невалидные), ключи (пустой, > 1024
  байт).
- **Матрица прав**: параметризованные проверки (роль × операция).

**`src/tests/OwnS3.IntegrationTests`** (WebApplicationFactory, без
докера — решение пользователя 3):

- `OwnS3AppFactory : WebApplicationFactory<Program>` — конфигурация
  in-memory (root-пара, статические ключи трёх ролей), уникальные
  настройки на класс/сборку; никаких портов и контейнеров.
- **Тестовый SigV4-клиент** (генератор подписи — тот же независимый
  signer, что в юнитах): заголовочная подпись, presigned-URL, сборка
  aws-chunked-тела (вкл. трейлеры) — полный контроль над сырым путём и
  заголовками (`HttpClient` + `HttpRequestMessage`).
- Сценарии (AAA-комментарии):
  - **роутинг**: каждая из 22 операций матчится корректно по
    дискриминаторам (проверка по поведению: код ответа заглушки 500
    `InternalError` с правильным Resource/операцией в метриках —
    сигнатура «операция определена»); вне-наборные сабресурсы → 501
    (все методы); не-матч → 400 `InvalidArgument`; OPTIONS → 200 пустой;
  - **аутентификация**: валидная подпись проходит до заглушки (500);
    битая → 403 `SignatureDoesNotMatch`; анонимный → 403 `AccessDenied`;
    skew заголовочной подписи (±15 мин) → 403 `RequestTimeTooSkewed`;
    несуществующий ключ → 403
    `InvalidAccessKeyId`; presigned — все исходы §3.2 (вкл. «через
    N > 15 мин внутри окна → Ok»);
  - **права**: матрица трёх ролей по репрезентативному набору операций
    (read-only: GET ok, PUT → 403; read-write: PUT ok-до-заглушки,
    CreateBucket → 403; admin: CreateBucket ok-до-заглушки);
  - **формат ошибок**: XML-структура `Error` (Code/Message/Resource/
    RequestId/HostId), `RequestId` — валидный UUID и совпадает с
    заголовком `x-amz-request-id`; HEAD-ошибки — без тела;
  - **тела**: PUT с hex-sha256 — несовпадение хэша → 400
    `InvalidRequest` (сверка состоялась — drain заглушки); Content-MD5
    несовпадение → 400 `BadDigest`; чанковый PUT: валидное тело → 500
    (заглушка после полной сверки цепочки), битая подпись чанка → 403,
    битый трейлер-checksum → 400 `BadDigest`;
  - **GetBucketLocation** → 200 с пустым `LocationConstraint` (полный
    успех);
  - **метрики**: после запросов `/metrics` содержит
    `ownS3_requests_total{operation="…",code="…"}` и
    `ownS3_request_duration_seconds` (финальные имена — паттерн
    `Shared.Metrics.UnitTests`);
  - **healthz** → 200.

### 3.6. Решение и инфраструктура

- `src/PgWorker.slnx`: новая solution-папка `/owns3/` — `OwnS3.Protocol`,
  `OwnS3.Storage`, `OwnS3.App`; `/tests/` — `OwnS3.UnitTests`,
  `OwnS3.IntegrationTests`.
- `Directory.Packages.props`: + `System.IO.Hashing` (CRC32/CRC32C;
  актуальная стабильная версия под net10.0). Прочих новых пакетов нет:
  XmlSerializer/HMAC/SHA — BCL; тестовые и WAF-пакеты уже версионированы.
- `OwnS3.App`: `appsettings.json` (секция `OwnS3` с дефолтами порта и
  DataDir; секреты — только env, в файле их нет).
- Имена по канону: `OwnS3.*` (dot-net конвенция проектов), env `OWNS3_*`,
  служебный контур (t37) — `.owns3.sys`.

## 4. Фазы реализации

Каждая фаза — коммит feature-ветки. Юнит-тесты пишутся вместе с
компонентом своей фазы (TDD на фазе кода — по скиллу разработки);
интеграционные — фазой 8.

1. **Arch-правки** (§3.1): десять дополнений в
   `arch/owns3/03-protocol.md` (arch-first — до любого кода; п. 10 —
   исправление противоречивой формулировки §2 о skew presigned, вкл.
   уточнение условия `RequestTimeTooSkewed` в таблице §5).
2. **Каркас решений**: 5 csproj (Protocol/Storage/App/UnitTests/
   IntegrationTests), строка в `PgWorker.slnx`, `System.IO.Hashing` в
   `Directory.Packages.props`, `OwnS3Options` + `Program`-минимум
   (fail-fast root-пары, Kestrel, `/healthz`), appsettings.json. Сборка
   решения зелёная.
3. **Protocol: заголовочная SigV4** — модель запроса, парсер
   Authorization, canonical builder, string-to-sign/signing key,
   верификатор; юниты на тест-векторах (§3.5).
4. **Protocol: presigned + чанковая** — `PresignedRequestVerifier`
   (константа разрешённых операций), `AwsChunkedReader` (оба режима,
   трейлеры, лимиты); юниты (§3.5).
5. **Protocol: XML + ошибки + имена** — `S3Xml`-типы, каталог
   `S3ErrorCode` (вкл. `InvalidAccessKeyId`), `S3ErrorXmlWriter`,
   валидаторы имён, `S3PathParser`; юниты (§3.5).
6. **Storage** — `ObjectStoreErrorCode`, доменные типы, `IObjectStore`,
   `NotWiredObjectStore` (drain-семантика); юниты заглушки (drain
   вызывает сверку; все методы → `ObjectStoreUnavailableException`).
7. **App: конвейер** — RequestId/лог, `S3Router` (таблица 22 операций,
   вне-наборные 501, не-матч 400, OPTIONS), `S3Authenticator`,
   `AccessKeyRegistry` + `OperationAccessMatrix`, обработчик ошибок,
   метрики + `/healthz` в конвейере.
8. **App: 22 хендлера** — каркас + парсинг/валидация каждого контракта
   (глава 02), `GetBucketLocation` — полный; стыковка с
   `NotWiredObjectStore`.
9. **Интеграционные тесты** — `OwnS3AppFactory`, тестовый signer-клиент,
   сценарии §3.5 (роутинг/аутентификация/права/ошибки/тела/метрики/
   healthz).
10. **Финальный прогон**: сборка решения + все тесты OwnS3 (юниты →
    интеграция) зелёные; `git status` — только ожидаемые файлы; roadmap
    не правится здесь (пункт t36 снимается мерж-коммитом в `main` по
    правилу трека).

Фазы 3–5 последовательны (канонические слои), 6 независима от 4–5 (может
идти параллельно по содержанию, коммит — после 5 для линейности), 7–8
после 3–6, 9 после 8.

## 5. Ограничения и риски

| Риск | Митигация |
|---|---|
| Тест-векторы подписи самосогласованы (signer и verifier ошибутся одинаково) | Два независимых источника векторов: эталонные примеры AWS-документации (внешнее происхождение) + независимый signer в тестах, не переиспользующий код Protocol; финальная страховка — реальные клиенты (AWS SDK, `mc`) в t39 |
| Контракт `IObjectStore` проектируется до реализации хранения — сигнатуры уточнятся в t37 | Контракт — эволюционируемая граница (уточнение сигнатур в t37 допустимо); неизменяемое — протокольный конвейер App и независимость Storage от Protocol/App |
| Заглушка drain-читает тело целиком: гигантский PUT к t36-сервису держит соединение | Сервис t36 не имеет хранения и не разворачивается в продуктив; лимит 5 ГБ (EntityTooLarge) проверяется хендлером по Content-Length до drain; полный поток-контроль — t37 |
| Чанковая подпись — сложнейшая часть протокола (цепочка, трейлеры, сверка по ходу) | Побайтовые юниты ридера (фрейминг/цепочка/трейлеры/лимиты) по образцу референса `streaming-signature-v4.go`; trailer-режим — отдельные сценарии |
| Kestrel/ASP.NET-поведение отличается от ожиданий подписи (декодирование пути, Expect: 100-continue, лимиты тела) | Конвейер работает с сырыми `Request.Path`/`Request.QueryString` (канонизация — Protocol); `MaxRequestBodySize = null`; 100-continue — штатный Kestrel (глава 03 §6); поведение фиксируется интеграционными тестами |
| Метрики/health в t36 без тома дают ложное ощущение готовности эксплуатации | Спека фиксирует границы: томная часть health и `ownS3_disk_*` — t37; докер/E2E/приёмка — t39 |
| Общий `PgWorker.slnx` затронут — риск поломки соседних сборок | Только добавление узлов; фаза 2 и фаза 10 — полная сборка решения; тесты соседних сервисов не запускаются по отдельному требованию (их код не меняется), E2E-гейт PgWorker не применим (код воркеров не тронут) |
| Пробелы канона, не покрытые §3.1, всплывут при реализации | Правило arch-first: каждый новый пробел закрывается правкой канона в своей фазе (малой), до реализации поведения в коде |

## 6. Критерии приёмки

1. **Arch-правки внесены**: все десять дополнений §3.1 — в
   `arch/owns3/03-protocol.md` (вкл. `InvalidAccessKeyId` в таблицу §5,
   Trimall-семантику значений заголовков в §1 п.4, исход для
   неподдерживаемого алгоритма Authorization в §1, правило
   percent-кодирования query в §1 п.3 и presigned-skew-семантику в §2
   «только будущее + строгая граница просрочки» с уточнением условия
   `RequestTimeTooSkewed` в таблице §5); изменений прочих глав
   нет; правки — первой фазой, до кода.
2. **`src/OwnS3.Protocol`**: реализует главу 03 целиком — SigV4
   заголовочная (canonical request/string-to-sign/signing key,
   constant-time), presigned (10 операций, 604800, skew только на
   будущее + строгая граница просрочки), чанковая
   (фрейминг, цепочка, трейлеры, 16 МиБ, decoded-length), XML-схемы всех
   ответов/запросов, полный каталог ошибок главы 03 §5 + `InvalidAccessKeyId`,
   валидация имён бакета/ключа, path-style; без ASP.NET-зависимостей.
3. **`src/OwnS3.Storage`**: `IObjectStore`-контракт 22 операций +
   доменные типы + собственные коды исходов (без ссылки на Protocol);
   `NotWiredObjectStore` с drain-семантикой тел.
4. **`src/OwnS3.App`**: Kestrel-хост с fail-fast root-пары (≥3/≥8),
   конвейер (RequestId → OPTIONS → роутер → аутентификация →
   авторизация → хендлер → обработчик ошибок → метрики), роутер 22
   операций с дискриминаторами и порядком (вне-наборные сабресурсы → 501
   до аутентификации; не-матч → 400), 22 хендлера полного протокольного
   контура, `GetBucketLocation` — полностью рабочий, `/healthz` → 200,
   `/metrics` с `ownS3_requests_total{operation,code}` и
   `ownS3_request_duration_seconds{operation}`.
5. **Матрица прав** (глава 05 §3) соблюдается: аноним → AccessDenied;
   read-only/read-write/admin — по таблице; отказ — 403 в формате главы
   03.
6. **Юниты**: тест-векторы подписи (два независимых источника), чанковый
   ридер, XML, ошибки, имена, матрица прав — зелёные; AAA-комментарии;
   без хардкодов портов и докера.
7. **Интеграционные тесты** (WebApplicationFactory, без докера): роутинг
   всех 22 + вне-наборные 501 + не-матч 400 + OPTIONS; аутентификация
   (все исходы §3.2); права трёх ролей; формат ошибок (RequestId/HostId/
   Resource, HEAD без тела); сверки тел (sha256/MD5/чанковая/трейлеры —
   отказы при порче, полный drain при валидных); GetBucketLocation 200;
   метрики; healthz — зелёные.
8. **Инфраструктура**: `PgWorker.slnx` — папка `/owns3/` + тесты;
   `Directory.Packages.props` — только `System.IO.Hashing`; сборка всего
   решения зелёная (`TreatWarningsAsErrors`); новых docker-образов,
   registry-загрузок, правок `deploy/**`/`dev-stand/**` нет.
9. **Границы изменений**: `git status` — только `arch/owns3/03-protocol.md`
   (фаза 1), `src/OwnS3.*`, `src/tests/OwnS3.*`, `src/PgWorker.slnx`,
   `src/Directory.Packages.props`, `docs/superpowers/**` (спека/план);
   PgWorker/KafkaWorker/AdminPanel/Shared — не тронуты.
10. **Стиль**: русский текст/комментарии, английские идентификаторы;
    структура кода — по паттернам соседних сервисов; никаких
    «TBD»/плейсхолдеров.
