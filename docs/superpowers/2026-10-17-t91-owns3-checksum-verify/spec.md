# t91-owns3-checksum-verify — спецификация

Серверная верификация заголовочных контрольных сумм `x-amz-checksum-*`
(полный набор crc32/crc32c/sha1/sha256, несовпадение → `400 BadDigest`) на
PutObject в ownS3 и кластер-health эндпоинт `/s3/health/cluster` (200/503)
для ClusterOk-пробы панели. Обе части — одной задачей (решение пользователя).

- Первоисточник задачи: `arch/roadmap/owns3.md`, пункт `t91-owns3-checksum-verify`.
- Канон ownS3: `arch/owns3/` (протокол — 03, контракты операций — 02,
  конфиг/health — 05, erasure — 06).
- Контракт панели (ClusterOk): `arch/adminpanel/02-etcd-contract.md` §2.5.
- Точечный референс MinIO: `/Users/demakaev/ZCodeProject/minio`
  (`cmd/object-handlers.go`, `internal/hash/checksum.go`,
  `cmd/healthcheck-handler.go`, `cmd/api-errors.go`).

## 1. Цель

1. **Заголовочная checksum-верификация PutObject — полный набор**. Сейчас
   заголовки `x-amz-checksum-*` (base64 контрольной суммы тела, flexible
   checksums AWS SDK) ownS3 молча игнорирует: серверная гарантия целостности
   put'ов клиентов отсутствует — при `UNSIGNED-PAYLOAD` тело не сверяется
   вовсе. Цель: при наличии РОВНО ОДНОГО заголовка из набора
   `x-amz-checksum-{crc32,crc32c,sha1,sha256}` (набор симметричен трейлерам
   главы 03 §3) сервер сверяет его с фактически прочитанным телом;
   несовпадение → `400 BadDigest` (стандарт S3), объект не коммитится.
2. **Кластер-health `/s3/health/cluster` (200/503)**. Панель — контрагент
   уже ждёт эндпоинт: `ClusterOk — GET /s3/health/cluster, 200 → true,
   503 → false, нет ответа/404/403 → null` (`arch/adminpanel/02` §2.5).
   Контракт ownS3-стороны не определял эндпоинт — задача закрывает дыру:
   канон + реализация + JSON-тело с drives-полями (толерантный парс панели
   уже написан против имён `healthyDrives`/`offlineDrives`/`healingDrives`/
   `totalDrives`, camelCase).

## 2. Принципы

- **Arch-first**: контракт обновляется в `arch/owns3/` ДО кода. Все три
  правки канона внесены этой спекой (фаза 0 исполнена, разделы 3.1 и 5.1).
- **Полный набор алгоритмов** (решение пользователя): crc32/crc32c/sha1/
  sha256 — симметрия заголовочного и трейлерного путей верификации; один и
  тот же клиент получает одинаковую гарантию целостности в обоих режимах
  подписи.
- **Стандарт S3 как критерий**: коды/статусы/сообщения — по стандарту S3 и
  референсу MinIO (точечные ссылки в каноне), формулировки переиспользуют
  существующие тексты ownS3 (BadDigest-текст трейлерной сверки).
- **Нулевая цена для существующих клиентов**: заголовок опционален; его
  отсутствие не меняет ни один существующий исход (регресс-бюджет = 0);
  клиенты канона (`RequestChecksumCalculation = WHEN_REQUIRED`) заголовки
  не шлют.
- **Ровно один хэш на запрос**: контракт допускает не более одного
  checksum-заголовка — при наличии вычисляется ровно выбранный алгоритм,
  «четыре хэша на каждый put» не возникает (оценка — раздел 3.2).
- **Сверка в существующем конвейере**: чтение тела уже обёрнуто
  `HashingBodyStream`; верификация добавляется в ту же точку EOF-сверки,
  второго прохода по телу и промежуточных буферов нет.
- **Multi-host/durability**: изменения не создают локального состояния;
  гарантии строятся на сверке до коммит-поинта Storage (объект с
  непрошедшей сверкой не появляется в хранилище и не реплицируется).

## 3. Структура/компоненты (решение, часть 1 — checksum)

### 3.1. Контракт (arch — внесено)

| Файл | Правка |
|---|---|
| `arch/owns3/02-operations.md` | Контракт `PutObject`, раздел «Заголовки»: описан `x-amz-checksum-<alg>` (`alg` ∈ crc32/crc32c/sha1/sha256 — симметрия трейлерам главы 03 §3): опционален, не более одного на запрос; base64 контрольной суммы тела (crc32/crc32c — 4 байта, sha1 — 20, sha256 — 32); несовпадение → 400 `BadDigest`; форматно-невалидный (не base64 / неверная длина) → 400 `InvalidArgument`; более одного заголовка `x-amz-checksum-*` → 400 `InvalidArgument` («Invalid checksum provided.»); checksum алгоритма вне набора четырёх сам не проверяется, но считается заголовком семейства в правиле «не более одного»; `x-amz-sdk-checksum-algorithm` — игнорируется; чанковые режимы — заголовочные `x-amz-checksum-*` игнорируются (авторитет — трейлер); ETag — по общим правилам (hex-MD5), от алгоритма checksum не зависит |
| `arch/owns3/03-protocol.md` | §5, таблица маппинга: `BadDigest` — добавлено «несовпадение заголовочного `x-amz-checksum-*` (PutObject)»; `InvalidArgument` — добавлены «форматно-невалидный заголовок `x-amz-checksum-*` (PutObject)» и «более одного заголовка `x-amz-checksum-*`» |
| `arch/owns3/05-access-config.md` | §5: эндпоинт `/s3/health/cluster` (см. раздел 5.1) |

Семантика по режимам подписи (следствие контракта, для плана):

| Режим `x-amz-content-sha256` | Поведение |
|---|---|
| `<hex-sha256>` | сверка подписи (InvalidRequest, существующая) + сверка checksum-заголовка (BadDigest, новая) — независимые проверки |
| `UNSIGNED-PAYLOAD` | checksum-заголовок — единственная серверная сверка тела |
| presigned PutObject | заголовок не входит в подпись; при наличии проверяется так же |
| `STREAMING-AWS4-HMAC-SHA-PAYLOAD(-TRAILER)` | заголовочные `x-amz-checksum-*` игнорируются; верификация — трейлером (существующая, глава 03 §3) |

Тексты сообщений (фиксация для кода и тестов):

- несовпадение: `BadDigest` / «The checksum you specified did not match the
  calculated checksum.» — тот же текст, что у трейлерной сверки
  (`AwsChunkedReader.Finish`);
- формат: `InvalidArgument` / «Value for x-amz-checksum-<algorithm> header
  is invalid.» (фактическое имя алгоритма: crc32/crc32c/sha1/sha256);
- мульти: `InvalidArgument` / «Invalid checksum provided.» (текст
  `ErrInvalidChecksum` референса, `api-errors.go`).

### 3.2. Код (часть 1)

| Компонент | Изменение |
|---|---|
| `src/OwnS3.App/Handlers/OperationValidation.cs` | Парсер `ParseChecksumHeader(IHeaderDictionary)` → `ChecksumExpectation?`; `record ChecksumExpectation(ChecksumAlgorithm Algorithm, string ExpectedBase64)`, `enum ChecksumAlgorithm { Crc32, Crc32C, Sha1, Sha256 }`. Правила: заголовки семейства `x-amz-checksum-*`: ни одного → `null`; известный ровно один (из четырёх) → валидация `Convert.TryFromBase64String` + длина по алгоритму (crc32/crc32c — 4 байта, sha1 — 20, sha256 — 32) → значение, иначе `S3ProtocolException(InvalidArgument, "Value for x-amz-checksum-<algorithm> header is invalid.")`; более одного (любых, включая неизвестные алгоритмы) → `S3ProtocolException(InvalidArgument, "Invalid checksum provided.")`; единственный неизвестный алгоритм → `null` (игнор). `x-amz-sdk-checksum-algorithm` не читается. Чистая функция — юнит-тестируема |
| `src/OwnS3.App/Handlers/HashingBodyStream.cs` | Дополнительный конструкторный аргумент `ChecksumExpectation? checksum`. Вычисление ровно выбранного алгоритма в существующих точках `AppendData`: `Crc32` — `System.IO.Hashing` (little-endian, конверсия big-endian — как у трейлерного пути), `Crc32C` — `OwnS3.Protocol.Auth.Crc32C` (собственная реализация репо: в System.IO.Hashing 10.x тип удалён апстримом; big-endian; тот же тип, что в трейлерном пути `AwsChunkedReader`), `Sha1` — `IncrementalHash(SHA1)`, `Sha256` — переиспользование уже существующего `_sha256` (второй инстанс не создаётся). В `VerifyHashes()` — сверка в порядке: sha256-hex (InvalidRequest, существующая) → checksum выбранного алгоритма (BadDigest, новый текст — см. 3.1) → MD5 (BadDigest, существующая). Байтовая конвенция CRC — та же, что у трейлерной сверки `AwsChunkedReader` (вычисление и hex-конверсию вынести в общий helper Protocol↔App либо повторить — по усмотрению плана, поведение обязано совпадать) |
| `src/OwnS3.App/Handlers/ObjectHandlers.cs` (`PutObjectHandler`) | После `UploadMetadata`, при НЕ-чанковом режиме (`context.TryGetChunkedContext` false): `var checksum = OperationValidation.ParseChecksumHeader(context.Http.Headers);` и передача в конструктор `HashingBodyStream`. Чанковый режим — вызов парсера не выполняется (трейлер — авторитет) |
| Прочие хендлеры | Без изменений: `UploadPart`, `CopyObject` и др. checksum-заголовки не читают (молчаливый игнор — прежнее поведение) |
| Пакеты | `System.IO.Hashing` уже в решении (используется `OwnS3.Protocol` — `AwsChunkedReader`); новых пакетов нет — `Crc32C` берётся из `OwnS3.Protocol` (ссылка `OwnS3.App` → `OwnS3.Protocol` уже есть), версия централизуется из `Directory.Packages.props` без изменений |

**ETag-семантика**: не меняется — простой PUT всегда hex-MD5 (глава 02 §1),
multipart — составной; от алгоритма checksum ETag не зависит. Checksum не
пишется в метаданные объекта и не возвращается в ответе (вне набора, §6).

**Производительность** (обоснование):

- Запросы без checksum-заголовков — путь кода прежний, накладных ноль
  (парсер возвращает null до создания хэшей).
- Запрос с заголовком — ровно ОДИН дополнительный инкрементальный хэш
  поверх потока (второго прохода по телу нет): CRC32 (`System.IO.Hashing`)
  — аппаратно-ускоренный (ARM CRC-расширения / x86 SSE4.2); CRC32C —
  managed-реализация репо (`OwnS3.Protocol.Auth.Crc32C`); SHA1/SHA256 —
  аппаратно-ускоренные (ARMv8 crypto / SHA-NI, ~0,5–3 ГБ/с); при
  `checksum=sha256` переиспользуется уже существующий `_sha256`.
- Существующий конвейер и так всегда считает SHA-256 тела (hex-режим
  подписи) и MD5 при `Content-MD5` — новая сверка стоит в одном ряду,
  узкое место put — диск/сеть. Итог: единицы процентов CPU при активно
  используемом checksum, ноль — без него.
- Память: инкрементальные хэши без буферизации — константная надбавка
  (десятки байт состояния на запрос).

**Ограничение drain-семантики**: сохраняется t36 — отказ Storage до чтения
тела не скрывает сверку (тело дочитывается, BadDigest перекрывает исход
Storage); формат/мульти-отказ происходит до чтения тела (сразу 400).

**Риск CRC-конвенции** (на план): трейлерная сверка `AwsChunkedReader`
кодирует crc32 через `Reverse` (тип `System.IO.Hashing.Crc32` — little-endian
→ big-endian), crc32c — собственным типом `OwnS3.Protocol.Auth.Crc32C`
(big-endian, прямой порядок). Заголовочная сверка обязана давать исходы,
идентичные реальным клиентам (AWS SDK `ChecksumCRC32`/`ChecksumCRC32C`) —
корректность закрывается юнит-тест-векторами (эталонные base64) и E2E
SDK-кейсами всех четырёх алгоритмов. Если E2E вскроет расхождение
трейлерного пути с эталоном SDK — это дефект существующей трейлерной
сверки: минимальная синхронная правка обоих путей одной задачей (единый
helper), с юнит-вектором; не откладывается.

### 3.3. Тесты (часть 1)

- **Юнит** (`src/tests/OwnS3.UnitTests/HandlersValidationTests.cs`): парсер —
  для каждого из четырёх алгоритмов: валидный base64 корректной длины;
  не-base64 → InvalidArgument; base64 неверной длины (4/4/20/32 нарушены)
  → InvalidArgument; `sha256 + crc32` → InvalidArgument (мульти);
  `sha256 + crc64nvme` (неизвестный) → InvalidArgument (семейство);
  единственный `crc64nvme` → null; пустой набор → null;
  `x-amz-sdk-checksum-algorithm` на исход не влияет. Сверка —
  юнит-векторы `HashingBodyStream` на эталонных суммах каждого алгоритма
  (включая CRC-конвенцию big-endian) от фиксированных входов.
- **Интеграционные WAF** (`src/tests/OwnS3.IntegrationTests/Api/
  BodyIntegrityScenarios.cs`, база + single/EC-наследники — параметризация
  t40 сохраняется), параметризация по алгоритму [Theory]: (а) корректный
  checksum при `UNSIGNED-PAYLOAD` → запрос доходит до Storage (NoSuchBucket —
  существующий шаблон); (б) порча checksum → 400 BadDigest; (в)
  форматно-невалидный → 400 InvalidArgument. Непараметризованные:
  (г) hex-sha256 подписи + корректный и битый sha256-checksum — обе сверки
  независимо; (д) два checksum-заголовка → 400 InvalidArgument; (е) пустое
  тело + корректная сумма пустоты → сверка проходит; (ж) чанковый
  trailer-режим с заголовочным checksum — поведение трейлерное, без
  изменений; (з) `UploadPart` с checksum-заголовком — игнор, поведение
  прежнее (регресс).
- **E2E реальными клиентами** (`src/tests/OwnS3.IntegrationTests/E2e/
  AwsSdkSimpleScenarios.cs`, гейт `PGW_TEST_DOCKER=1`, свежий Release):
  AWS SDK `PutObjectRequest.ChecksumSHA256` и `ChecksumCRC32` (обязательно),
  `ChecksumCRC32C` и `ChecksumSHA1` (желательно — все четыре): валидный →
  200 и объект читается; битый → исключение клиента с HTTP 400 (код
  BadDigest в теле ошибки). Маркер-кейс мерж-гейта — один из новых
  AwsSdk-сценариев (фильтр выбирается планом по образцу существующих);
  существующая приёмка flexible checksums (`WHEN_REQUIRED`/`WHEN_SUPPORTED`
  -trailer) — регресс без изменений.

## 4. Фазы

| Фаза | Содержимое | Статус/исполнитель |
|---|---|---|
| 0. Канон | Правки `arch/owns3/02-operations.md` (контракт PutObject — полный набор), `arch/owns3/03-protocol.md` (таблица §5), `arch/owns3/05-access-config.md` (§5 — кластер-health) | Исполнено этой спекой (arch-first); правки в рабочем дереве |
| 1. Парсер+поток | `OperationValidation.ParseChecksumHeader` + `ChecksumExpectation`, аргумент `HashingBodyStream` (вычисление выбранного алгоритма, общий CRC-helper), вызов в `PutObjectHandler` | Фаза кода |
| 2. Юниты | `HandlersValidationTests` — кейсы парсера; векторы сверки | Фаза кода (TDD: тесты до/вместе с фазой 1) |
| 3. Интеграция WAF | `BodyIntegrityScenarios` — кейсы (а)–(з) в базе, single+EC | Фаза кода |
| 4. E2E checksum | AwsSdk-сценарии всех четырёх алгоритмов (валидный/битый) | Фаза кода |
| 5. Health | `S3Middleware` шаг 0 + `Program.MapGet` + `AdminHandlers.HandleClusterHealthAsync` + тесты (раздел 5.2) | Фаза кода |
| 6. Гейт-прогоны | Полный E2eFixture на свежем Release (трогается `OwnS3.App` — обязателен); полный прогон юнитов/интеграции OwnS3 | Мерж-гейт |

## 5. Кластер-health `/s3/health/cluster` (в составе задачи)

### 5.1. Контракт (arch — внесено)

`arch/owns3/05-access-config.md` §5, пункт перед `/healthz`:

- Готовность хранилища обслуживать запись: single-режим — условия
  `/healthz`; EC-режим — все erasure-сеты writable (онлайн-дисков сета ≥
  write-кворума, глава 06); выполнение → **200**, невыполнение → **503**
  (критерий — референс `ClusterCheckHandler`: health = write-quorum всех
  сетов).
- Без подписи (публичная проба наблюдения, прецедент `/healthz`;
  потребитель — ClusterOk-проба панели, `arch/adminpanel/02` §2.5); метод
  GET.
- Тело — JSON: `healthyDrives`, `offlineDrives`, `totalDrives` (long;
  single — 1/0/1 по итогам touch-пробы, при отказе 0/1/1; EC — по всем
  дискам всех сетов); `healingDrives` не отдаётся (heal-проход ownS3 не
  per-drive — статус heal в `/admin/storage`).
- Префикс `/s3/health/*` — служебные пути до роутинга 21 операции (запрос
  без подписи не падает в path-style матче бакета `s3`; объекты с ключами
  `health/*` в бакете `s3` через эту грань недоступны — прецедент
  `/minio/*` референса). `/s3/health/live` не реализуется: панель
  использует фолбэк `GET /healthz` (контракт панели §2.5).

### 5.2. Код и тесты

| Компонент | Изменение |
|---|---|
| `src/OwnS3.App/Pipeline/S3Middleware.cs` | Шаг 0: пропустить `path == "/s3/health/cluster"` в `next` (до роутинга, рядом с `/healthz`) |
| `src/OwnS3.App/Program.cs` | `app.MapGet("/s3/health/cluster", ...)` — вычисление статуса и JSON-тела |
| `src/OwnS3.App/Handlers/AdminHandlers.cs` | Хендлер-метод `HandleClusterHealthAsync(HttpContext)`: single — `drive.CheckHealth()`; EC — `ErasureStatusProvider.GetStatus()`: все `Sets[*].Writable`; счётчики drives по `Sets[*].Drives[*].Online`. Источники те же, что у `/admin/storage` (глава 05 §7) |

Тесты:

- WAF single (`HealthzVolumeScenarios`-образец): 200 + JSON-поля на
  валидном томе; 503 + JSON при сломанном томе (изоляция кейса tmp-каталогом
  с восстановлением — как существующий 503-кейс healthz); анонимный запрос —
  не 403 (изоляция шага 0).
- WAF EC (`OwnS3EcAppFactory`, образец `ErasureDegradationScenarios`): 200 и
  `totalDrives`/`healthyDrives` на здоровом пуле; офлайн-диска достаточно
  для write-деградации сета → 503, `offlineDrives` > 0.
- E2E (`E2e/HealthMetricsScenarios.cs`): против контейнера — 200 + парсинг
  JSON-полей.
- Прогон чека стенда `dev-stand/adminpanel/checks/46-owns3.sh` не ломается
  (новый эндпоинт чеками не занят; дополнение чека пробой cluster —
  опционально, на усмотрение плана).

## 6. Ограничения (вне набора задачи)

- `UploadPart`/`UploadPartCopy`/`CopyObject`: checksum-заголовки не
  проверяются (PutObject — по формулировке roadmap); при потребности —
  отдельный пункт roadmap.
- Заголовочные checksum алгоритмов вне набора четырёх (например
  `x-amz-checksum-crc64nvme`) не проверяются (участвуют только в правиле
  «не более одного» семейства).
- Эхо-заголовок `x-amz-checksum-*` в ответе PutObject, `x-amz-checksum-type`,
  персист checksum в `xl.meta`, GetObject-режим `x-amz-checksum-mode:
  ENABLED` — вне набора (стандарт допускает per-API состав).
- `/s3/health/live` не реализляется (фолбэк панели на `/healthz` — уже
  контрагент §2.5).
- Метрики: новых серий нет — отказы видны в существующих
  `ownS3_requests_total{operation,code}` (400-е PutObject).
- Новых env/конфигурации, новых внешних docker-образов, изменений xl-формата
  и контрактов Storage — нет.

## 7. Критерии приёмки

Checksum-верификация:

1. PutObject с корректным checksum каждого из четырёх алгоритмов (включая
   `UNSIGNED-PAYLOAD` и пустое тело с суммой пустоты) → 200, объект
   закоммичен и читается GetObject.
2. PutObject с битым значением (каждый алгоритм) → 400 `BadDigest` (XML-код
   BadDigest, Message «The checksum you specified did not match the
   calculated checksum.»), объект НЕ закоммичен (GetObject → NoSuchKey),
   событие репликации не отправлялось.
3. Невалидный base64/неверная длина (каждый алгоритм) → 400
   `InvalidArgument`; более одного `x-amz-checksum-*` → 400
   `InvalidArgument` («Invalid checksum provided.»); отказ до чтения тела.
4. Единственный checksum неизвестного алгоритма, `x-amz-sdk-checksum-
   algorithm` — поведение запроса прежнее (регресс).
5. Чанковые режимы (базовый и trailer) — поведение неизменно; заголовочные
   checksum в них игнорируются.
6. ETag ответа — hex-MD5, не зависит от алгоритма checksum.
7. `UploadPart` и прочие операции — поведение неизменно (регресс).
8. Заголовочная и трейлерная сверки одного алгоритма дают идентичные
   исходы на одних данных (юнит-векторы + E2E SDK).

Кластер-health:

9. `GET /s3/health/cluster` без подписи: single — 200/503 по состоянию тома;
   EC — 200 при всех writable-сетах, 503 при write-деградации любого сета.
10. Тело 200/503 — JSON с `healthyDrives`/`offlineDrives`/`totalDrives`
    (camelCase, long); `healingDrives` отсутствует; панельная проба §2.5
    даёт ClusterOk=true/false соответственно.
11. Путь перехватывается до роутинга 21 операции (шаг 0): анонимный запрос
    не получает 403 path-style конвейера.

Общие:

12. Все новые кейсы исполняются в single- и EC-параметризациях WAF; полный
    прогон E2eFixture на свежем Release зелёный (мерж-гейт); существующие
    тесты OwnS3 (юниты/интеграция/E2E) — без изменений исходов.

## 8. Решения пользователя (открытых вопросов нет)

1. **Состав задачи**: обе части одной задачей — checksum-верификация И
   `/s3/health/cluster` (вариант A; roadmap-перенос новым тегом не
   требуется — тег `t91` снимается мерж-коммитом целиком).
2. **Набор алгоритмов заголовочной верификации**: полный —
   crc32/crc32c/sha1/sha256 (симметрия с трейлерным набором главы 03 §3);
   форматы и семантика — разделы 3.1–3.3.
