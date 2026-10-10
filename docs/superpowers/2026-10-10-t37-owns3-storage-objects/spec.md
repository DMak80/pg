# Spec t37-owns3-storage-objects: xl-хранение и основные операции ownS3

Дата: 2026-10-10. Постановка — `arch/roadmap/owns3.md` (тег
`t37-owns3-storage-objects`). Каноны: `arch/owns3/01…05`; задача
наполняет реализацией контракт `IObjectStore`
(`src/OwnS3.Storage`), созданный в t36 вместе с протокольным контуром
`src/OwnS3.App` / `src/OwnS3.Protocol`.

## 1. Цель

Собственный объектный слой ownS3: single-drive xl-хранение по канону
`arch/owns3/04-storage.md` и реализация 15 операций (7 объектных +
3 листинга + 5 бакетных) из 22 контракта `arch/owns3/02-operations.md`.
После t37 сервис ownS3 реально хранит, читает и листит данные;
multipart-операции (7 шт.) остаются заглушечными до t38.

Не-цели (границы): multipart (t38), erasure coding (t40),
docker-E2E-контур и dev-стенд-сервис (t39), TLS (t42), изменения
протокольного слоя SigV4/XML/ошибок (готовы в t36), IAM (готово).

## 2. Принципы

1. **Arch-first**: канон 04 — истина раскладки и атомарности. Пробелы
   канона закрываются правкой канона ДО кода (см. раздел 3 — уже
   внесены при написании спеки).
2. **Единый коммит-поинт**: видимость объекта для читателей меняется
   только атомарной заменой `xl.meta` (rename). Частично записанное
   не видно никогда; сбой оставляет мусор только в `tmp/`, `.trash/`
   и orphan-dataDir — всё чистится стартом/фоном.
3. **Надёжность без per-object локов**: конкурентные PUT одного ключа —
   last-writer-wins (каждый коммит атомарен, `xl.meta` всегда указывает
   ровно на один валидный dataDir; проигравшие dataDir — orphan,
   чистятся). Гонки PUT/Delete допускают исход «выжил последний» —
   данные никогда не повреждаются. Словари локов (как у MinIO
   `distLock`) не заводятся: один процесс, один том, домашний контур.
4. **Тестируемость без хоста**: `OwnS3.Storage` не зависит от
   ASP.NET; юниты работают с temp-каталогом тома; интеграционные
   тесты — in-memory WAF с `OwnS3:DataDir` → temp.
5. **Production ready**: fsync-дисциплина записи, fail-fast старта на
   невалидном томе, диагностика в лог; enterprise-защиты не нужны
   (домашний контур).
6. Язык: документация и комментарии — русские; идентификаторы —
   английские.

## 3. Arch-first правки канона (внесены в worktree этой спекой)

Пробелы канона 04, без закрытия которых реализация неоднозначна или
неверна. Правки уже применены к `arch/owns3/04-storage.md` (и одна к
`02-operations.md`):

1. **Спецслучаи кодирования пути** (04 §1): сегменты `.`/`..` →
   `%2E`/`%2E%2E` (недопустимые компоненты ФС-пути); пустой сегмент
   (`a//b`) → одиночный `%` (литеральный `%` кодируется `%25` —
   коллизии нет); результат, оканчивающийся на `__XLDIR__`,
   экранируется (`_` → `%5F`); порядок декодирования — суффикс-проверка
   маркера на сыром сегменте, затем percent-декод.
2. **Метаданные бакета** (04 §1): `.owns3.sys/buckets/<bucket>/bucket.json`
   (`createdAt`, unix-мс) — источник `CreationDate` листинга
   ListBuckets (btime ФС ненадёжен; правка согласована в
   02 §4 ListBuckets).
3. **Имена целей в `.trash/`** (04 §1): `<guid>` — повторные удаления
   одного ключа не конфликтуют.
4. **Delete при вложенных ключах** (04 §4 п.5, критично): `xl.meta` и
   dataDir переименовываются в `.trash/` ПО ОТДЕЛЬНОСТИ — rename
   каталога объекта целиком удалил бы чужие подобъекты (`a` и `a/b`
   делят один каталог); опустевший каталог объекта удаляется,
   каталоги-префиксы с подобъектами остаются (пустые каталоги листингу
   не видны).
5. **Новый ключ при существующем каталоге-префиксе** (04 §4 п.3):
   пишется схемой перезаписи — rename каталога целиком поверх
   непустого невозможен (POSIX).

Сверка checksum при чтении каноном НЕ меняется: сверка SHA-256
остаётся «при каждом чтении данных» по канону 04 §2 буквально — при
любом GET (200 и 206) объект хэшируется целиком, Range определяет
только отдаваемые байты (детали — §4.4).

## 4. Структура и компоненты

### 4.1. Новое в `src/OwnS3.Storage`

| Компонент | Ответственность |
|---|---|
| `XlPathEncoder` | ключ ↔ кодированный путь (правила канона 04 §1 вкл. спецслучаи п.3.1); лимит сегмента 255 байт → `InvalidArgument` «Key too long after encoding» |
| `XlMetaFile` | чтение/запись `xl.meta`: побайтовая сериализация (§4.2), порядок записи канона 04 §2 (`xl.meta.bkp` → tmp-файл → rename), фолбэк чтения `xl.meta.bkp` при повреждении основного (лог warning) |
| `XlVolume` | жизненный цикл тома: создание/валидация `volume.json` (fail-fast), пути `.owns3.sys/{tmp,multipart,.trash,buckets,config}`, чистки старт/фон (пороги канона 04 §6), `TimeProvider` |
| `XlObjectStore` | реализация `IObjectStore`: 15 операций t37; multipart-методы — заглушечное поведение (`ObjectStoreUnavailableException`, `UploadPart` — drain тела, семантика NotWired t36) |
| `ListWalker` | обход bucket-дерева в лексикографическом порядке UTF-8 байт по декодированным ключам: отсечение по prefix/marker, свёртка delimiter → CommonPrefixes, лимит max-keys |
| `ConditionalEvaluator` | таблица conditional-исходов канона 02 §1 (If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since, кавычки-нормализация ETag) |
| `RangeResolver` | резолв одиночного Range канона 02 §1: `a-b`, `a-`, `-N`; обрезка end, суффикс длиннее объекта, пустой объект, `If-Range` (сильный ETag) |
| `BucketMetaStore` | `.owns3.sys/buckets/<bucket>/bucket.json` (создание/чтение `createdAt`) |

`ObjectStoreErrorCode`/`ObjectStoreException` — без изменений (все
исходы t37 уже в перечне: `NoSuchBucket`, `NoSuchKey`,
`BucketAlreadyOwnedByYou`, `BucketNotEmpty`, `InvalidRange`,
`PreconditionFailed`, `NotModified`, `EntityTooLarge`).

### 4.2. Формат `xl.meta` (детализация канона 04 §2)

Собственный бинарный формат, little-endian, без msgp:

```
Header:  magic "OWS3" (4 байта ASCII) | uint8 formatVersion = 1
Запись-версия (unversioned — ровно одна):
  uint16 recordVersion = 1
  versionId: 16 байт UUID v4
  int64  size                     // размер объекта, байты
  uint64 modTime                  // unix-миллисекунды UTC
  string etag                     // uint16-длина + UTF-8 (hex-MD5 или «N-md5» после t38)
  string contentType              // uint16-длина + UTF-8
  uint16 userMetadataCount | пары string key, string value   // x-amz-meta-* без префикса
  uint16 headersCount        | пары string name, string value // прочие сохранённые заголовки
  string contentSha256             // 64 hex-символа — SHA-256 данных dataDir
```

- `string` = `uint16 byteLength + bytes(UTF-8)`; отдельного лимита
  user-metadata не вводим (канон 02 лимита не задаёт; uint16-длины
  сериализации покрывают фактические размеры заголовков клиентов).
- Чтение: magic/версия формата ≠ ожидаемой, битая структура → попытка
  `xl.meta.bkp`; обе битые → `InternalError` (500, диагностика в лог —
  каталог ошибки канона 03 §5).
- Неизвестная `recordVersion` → `InternalError` (расширяемость — t40).

### 4.3. Жизненный цикл записи (детализация канона 04 §4)

`PutObject(bucket, key, body, contentLength, metadata)`:

1. staging `.owns3.sys/tmp/<guid>/<dataDir-uuid>/part.1`: поток
   копируется в `part.1` с одновременным вычислением MD5 (ETag) и
   SHA-256 (checksum записи); счётчик байт сверен с `contentLength`
   (расхождение → `InvalidRequest`-семантика протокола — тело уже
   проверено конвейером App; остаточный случай → `InternalError`).
2. В staging пишется `xl.meta` (новый versionId, modTime = now);
   fsync файлов; fsync каталога staging (Unix; best-effort —
   платформы без fsync-каталога: warning в лог, не отказ).
3. Коммит:
   - целевой каталог объекта не существует → rename staging целиком
     в `<bucket>/<кодированный путь>` (создав недостающие
     каталоги-префиксы до rename);
   - существует (перезапись или живой каталог-префикс) → rename
     `<dataDir-uuid>/` из staging в каталог объекта → атомарная
     замена `xl.meta` (bkp → tmp → rename) → старый dataDir (если был)
     → `.trash/<guid>`.
4. `finally`: staging в `tmp/` удаляется при любом отказе (не ждёт
   чисток).

`DeleteObject`: rename `xl.meta` и `<dataDir-uuid>/` в `.trash/<guid>`
по отдельности; опустевший каталог объекта удаляется; отсутствие
объекта — успех (идемпотентность канона 02 §1).

`CopyObject`: новый `<dataDir-uuid>/part.1` — **жёсткая ссылка** на
`part.1` источника (`link(2)`-образец;
.NET — `File.CreateHardLink` / P-Invoke `link`), fallback при ошибке
link — побайтовое копирование (warning в лог); метаданные нового
`xl.meta` — по директиве COPY/REPLACE, etag/sha256 наследуются из
записи источника (содержимое неизменно), versionId/modTime новые;
коммит — как перезапись.

### 4.4. Чтение: conditional, Range, checksum

`GetObject`/`HeadObject` (`ObjectReadOptions`):

1. Чтение `xl.meta` (с фолбэком bkp). Отсутствие → `NoSuchKey`.
2. `ConditionalEvaluator`: провал `If-Match`/`If-Unmodified-Since` →
   бросок `PreconditionFailed` (412); попадание `If-None-Match` →
   исход Not-Modified (см. контрактные правки §5: флаг, не бросок);
   `If-Modified-Since` — только GET/HEAD и только без
   `If-None-Match` (приоритеты таблицы канона 02 §1; для copy-семей
   оценка идёт к источнику в `CopyObjectAsync`).
3. `If-Range` (сильный ETag): совпал → применять Range; не совпал →
   полный объект (200). Дата-форма — трактуется как несовпадение.
4. `RangeResolver`: невалидный/вне размера/пустой объект → бросок
   `InvalidRange` (416); валидный — `(start, end, total)` → 206.
5. Возврат: сверка SHA-256 выполняется при **каждом чтении данных**
   (канон 04 §2): при любом GET — полном
   (200) и Range (206) — объект читается и хэшируется **целиком**;
   Range определяет только возвращаемые клиенту байты, хэш для сверки
   — всегда от всего объекта. Порядок: полный проход с вычислением
   SHA-256 **до передачи любых байтов клиенту** (несовпадение → 500
   `InternalError` с диагностикой в лог, клиенту данные не
   отдаются — канон 04 §2); при успехе — отдача (полная — копированием
   при повторном чтении, диапазонная — `FileStream` со `Seek`).
   HEAD данных не читает — сверка не выполняется.
6. Conditional-сверки (`If-Match`/`If-None-Match`/`If-Range`) —
   сравнение с ETag **всего объекта** (значение из `xl.meta`,
   hex-MD5 содержимого): Range-запрос не меняет хэш для сверки.

### 4.5. Листинги

`ListWalker` по каталогам бакета, порядок — лексикографический по
UTF-8 байтам декодированного ключа (сортировка имён детей уровня — по
декодированным сегментам; кодированные имена не сортируются:
percent-кодирование искажает порядок). Правила:

- объект = каталог с `xl.meta`; подкаталог без `xl.meta` — префикс;
  ключ `a` и вложенный `a/b` сосуществуют;
- `prefix` → спуск только в поддерево префикса;
- `marker`/`start-after`/`key-marker` → продолжение строго после
  ключа: поддеревья с максимальным ключом ≤ marker пропускаются;
- `continuation-token` (V2) — непрозрачный `base64url(lastKey)`
  (декод → маркер «строго после»; повторная выдача страницы
  детерминирована — деревья читаются с диска);
- `delimiter` → ключи, содержащие delimiter после префикса,
  сворачиваются в `CommonPrefixes` (дедупликация), считаются в
  `max-keys`; ключ-маркер каталога (`__XLDIR__`) раскрывается в ключ с
  завершающим `/`;
- `max-keys`: 0 → пустая страница `IsTruncated=false`; > 1000
  обрезается (App уже обрезает; Storage принимает значение как есть);
- `NextMarker` (v1) — последний ключ страницы, возвращается при
  усечении с заданным `delimiter` (как стандарт: без delimiter клиент
  продолжает по последнему `Contents/Key`);
- `KeyCount` (V2) = Contents + CommonPrefixes;
- `ListObjectVersions` — тот же обход в variants-обёртке (без полей
  версий).

### 4.6. Бакеты

- `CreateBucket`: `Directory.CreateDirectory(<bucket>)` +
  `bucket.json` (`createdAt`); существование → `BucketAlreadyOwnedByYou`.
- `DeleteBucket`: бакета нет → `NoSuchBucket`; непуст (в поддереве
  есть `xl.meta` или живые multipart-загрузки — последних в t37 нет
  физически, проверка места под t38) → `BucketNotEmpty`; иначе rename
  каталога и `.owns3.sys/buckets/<bucket>/` в `.trash/<guid>`.
- `HeadBucket`/`BucketExists`: каталог существует и не служебный.
- `ListBuckets`: каталоги первого уровня кроме `.owns3.sys`, по
  алфавиту, `CreationDate` из `bucket.json`.
- `GetBucketLocation` — уже в App (пустой `LocationConstraint`),
  Storage не участвует.

### 4.7. Чистки и фон

`XlVolume` (пороги канона 04 §6, источники времени — `TimeProvider`):

- **старт** (`InitializeAsync`): валидация `volume.json` (отсутствие/
  чужой magic/несовместимая версия/чужой режим → исключение старта —
  fail-fast, Program не поднимает Kestrel); `tmp/*` — безусловное
  удаление; `.trash/*` старше 1 ч — удаление; orphan-dataDir (каталоги
  `<uuid>` в каталогах объектов, не упомянутые текущим `xl.meta`)
  старше 1 ч (mtime) → `.trash/<guid>`.
- **фон**: `IHostedService` в OwnS3.App, период **15 минут**, вызывает
  `XlVolume.RunCleanupAsync` (те же `.trash`/orphan-пороги; multipart
  — t38). Таймаут прохода — мягкий: чистка не блокирует ответов
  (фоновый поток), лог `[CLEANUP]` с длительностью.
- Каталожные метрики диска (канон 05 §5): `ownS3_disk_used_bytes` /
  `ownS3_disk_total_bytes` (gauge) обновляются тем же фоновым сервисом
  (`DriveInfo` по корню тома).

## 5. Контрактные правки кода (доменный слой, не arch)

1. `ObjectReadOptions` + поле `IfRange` (string?) — парсинг в
   `ReadOptions` App уже рядом (Range/conditional там парсятся);
   хендлеры передают.
2. `HeadObjectAsync(bucket, key, ct)` →
   `HeadObjectAsync(bucket, key, ObjectReadOptions?, ct)`: контракт
   канона 02 даёт HeadObject conditional+Range (206 без тела).
3. `GetObjectAsync`/`HeadObjectAsync` возвращают исход Not-Modified
   флагом, не броском: 304 обязан идти без тела, но с заголовками
   `ETag`/`Last-Modified` (канон 02 §1) — бросок `NotModified` не
   несёт метаданных. Вариант контракта:
   `ObjectContent(ObjectMetadata Metadata, Stream Body, bool NotModified, AppliedByteRange? Range)`,
   `AppliedByteRange(long Start, long End, long Total)`.
4. `NotWiredObjectStore` и `NotWiredObjectStoreTests` удаляются:
   заглушечное поведение multipart-методов переезжает в `XlObjectStore`
   (остальные методы реальны). `ObjectStoreUnavailableException`
   остаётся (App маппит в 500 — до t38).

## 6. Правки `src/OwnS3.App`

1. `Program`: DI `XlObjectStore` (вместо `NotWiredObjectStore`);
   инициализация тома до старта Kestrel (fail-fast: невалидный том →
   диагностика + ненулевой exit); регистрация фонового сервиса чисток.
2. `/healthz`: том доступен на запись + `volume.json` валиден → 200,
   иначе 503 (канон 05 §5; проба записи — touch служебного файла с
   очисткой, не частая).
3. `GetObjectHandler`: 206 + `Content-Range: bytes a-b/total` при
   применённом Range; 304 без тела с `ETag`/`Last-Modified`;
   `HeadObjectHandler`: те же исходы без тела.
4. `ReadOptions` (ObjectHandlers): извлечение `If-Range`, передача в
   `ObjectReadOptions`.
5. Интеграционная фабрика `OwnS3AppFactory`: конфигурация
   `OwnS3:DataDir` → temp-каталог теста (иначе fail-fast старта тома
   уронит хост); сценарии объектного контура — свой экземпляр фабрики
   с собственным temp-том и полным удалением в teardown (каноны
   изоляции).

## 7. Фазы реализации (для plan)

0. **Arch-фаза**: контрольная сверка спеки с каноном 04 после правок
   §3 (checksum-семантику канон уже фиксирует — «при каждом чтении
   данных», правка не требуется).
1. **Ядро Storage**: `XlPathEncoder`, `XlMetaFile`, `XlVolume`
   (init/валидация/чистки) + юниты (roundtrip кодирования вкл.
   спецслучаи/коллизии; сериализация; bkp-фолбэк; чистки на
   `FakeTimeProvider`-подобной подмене — используется `TimeProvider`
   с тестовой реализацией).
2. **Бакеты**: `XlObjectStore` бакетные методы + `BucketMetaStore` +
   юниты (CRUD, `BucketAlreadyOwnedByYou`/`BucketNotEmpty`/`NoSuchBucket`,
   CreationDate).
3. **Объекты**: Put/Get/Head/Delete/DeleteObjects + контракные правки
   §5 + conditional/Range/checksum (полная сверка SHA-256 при каждом
   GET — 200 и 206) + юниты (коммит-атомарность,
   прерванный PUT невидим, перезапись оставляет orphan → чистится,
   все ветки conditional/Range/If-Range, checksum-порча → 500 до
   отдачи байтов (вкл. Range-чтение),
   параллельные PUT одного ключа).
4. **Copy + Attributes**: CopyObject (хардлинк + fallback),
   GetObjectAttributes
   (ObjectParts — синтетически 1 часть) + юниты (COPY/REPLACE,
   ETag-наследование, source-conditional, копия переживает удаление
   источника, fallback-путь).
5. **Листинги**: `ListWalker` + v1/V2/Versions + юниты (порядок UTF-8
   байт, delimiter/CommonPrefixes, marker/continuation/start-after,
   max-keys=0, вложенные ключи `a`+`a/b`, ключи-каталоги `dir/`,
   спецсимволы/unicode).
6. **App-стыковка**: Program/healthz/чистки/метрики диска, хендлеры
   206/304, фабрика с temp-том, переработка `RoutingScenarios`
   (15 операций → реальные исходы: 404 `NoSuchBucket` на несозданном
   бакете и т.п.; multipart — остаются 500) + интеграционные сценарии
   полного HTTP-цикла (§8).
7. **Мерж-гейт**: `dotnet build` Release (0 warnings,
   `TreatWarningsAsErrors`), `dotnet test` юниты+интеграция OwnS3
   зелёные; docker-контур не поднимается (E2E — t39).

## 8. Тестовая стратегия

Юниты (`OwnS3.UnitTests`, temp-том `Path.GetTempPath`+guid, teardown —
удаление): перечислены по фазам §7; обязательные классы кейсов —
`XlPathEncoderTests`, `XlMetaFileTests`, `XlVolumeTests`,
`XlObjectStoreBucketTests`, `XlObjectStoreObjectTests`,
`XlObjectStoreListTests`. Тесты чисток — детерминированное время
(тестовый `TimeProvider`), без реальных sleep (правило ≤30 с).

Интеграционные (`OwnS3.IntegrationTests`): сценарный класс
собственного хоста+тома (полный teardown): подписанный HTTP-цикл
Put→Head→Get→List(v1/V2/Versions)→Copy→Attributes→Delete→DeleteObjects;
206/416/304/412 по HTTP; ошибки `NoSuchBucket`/`NoSuchKey` в XML;
пересоздание объектов; healthz при живом томе. Существующие
сценарии t36 (auth/подпись/ошибки формата) не регрессируют
(BodyIntegrity-сценарии: сверки хэшей конвейера происходят ДО коммита
— статусы не меняются).

AAA-нотация в комментариях тестов; без хардкода портов (in-memory
хост); без docker (t39).

## 9. Ограничения и риски

- **Не трогаем**: `OwnS3.Protocol` (крайний случай — только если
  юнит-сверка выявит ошибку t36; отдельным пунктом с обоснованием),
  роутинг/подпись/матрицу прав.
- **Один процесс**: конкурентный доступ к тому извне (второй инстанс
  ownS3 на том же каталоге) не поддерживается — `volume.json` не
  блокируется (домашний контур; риск задокументирован).
- **fsync каталогов**: на платформах без поддержки — best-effort
  (warning); прод-контур Linux-докер поддерживает.
- **Производительность листинга**: без индекса; обход с prefix/marker
  отсечениями. Бакеты бэкапов (десятки тысяч ключей) — приемлемо;
  индекс — вне этапа.
- **Цена полной сверки checksum**: каждый GET — два прохода по файлу
  (SHA-256 целиком, затем отдача); постраничное Range-чтение большого
  объекта — O(n²) IO. Осознанная плата максимальной целостности
  бэкапов; изменение решения — отдельной задачей с arch-правкой.
- **ListBuckets `DriveInfo`**: сетевые/виртуальные тома могут давать
  неточные gauge-значения — метрика эксплуатационная.

## 10. Критерии приёмки

1. 15 операций работают через подписанный HTTP (интеграционные
   сценарии §8 зелёные): Put/Get/Head/Delete/DeleteObjects/Copy/
   GetObjectAttributes; ListObjects(v1)/ListObjectsV2/
   ListObjectVersions; Create/Delete/Head/ListBuckets
   (+GetBucketLocation уже работал).
2. Семантики канона 02 в тестах: ETag = hex-MD5; conditional-таблица
   (412/304/приоритеты; сверка — с ETag всего объекта); Range
   (206/416/If-Range/суффикс/обрезка); сверка SHA-256 при каждом GET
   (200 и 206) от всего объекта, порча данных → 500 до передачи
   байтов, Range не влияет на хэш сверки;
   идемпотентность DeleteObject; DeleteObjects per-key; лимиты
   (5 ГБ, 1000 ключей — App-валидация остаётся).
3. Раскладка диска соответствует канону 04 (тест-ассерты путей:
   `<bucket>/<кодированный путь>/xl.meta`, `<dataDir-uuid>/part.1`,
   `.owns3.sys/{tmp,multipart,.trash,buckets,config}`, `volume.json`).
4. Атомарность: после успешного ответа PUT объект виден Get/Head/List;
   PUT с исключением в середине тела не оставляет видимого объекта и
   удаляет staging.
5. Вложенные ключи: `a` и `a/b` сосуществуют; удаление `a` не трогает
   `a/b`; ключ `dir/` (маркер каталога) — полноценный ключ.
6. Чистки: старт очищает `tmp` безусловно; `.trash`/orphan удаляются
   по порогу 1 ч (детерминированные тесты времени); фоновый сервис
   периодичен (15 мин), метрики диска экспортируются.
7. `/healthz` 200 на валидном томе, 503 на недоступном (тест
   подменой/удалением).
8. `NotWiredObjectStore` удалён; multipart-методы `XlObjectStore`
   отвечают 500 `InternalError` (интеграционный кейс сохранён).
9. Сборка Release чистая (`TreatWarningsAsErrors`), все тесты
   `OwnS3.UnitTests` + `OwnS3.IntegrationTests` зелёные.
10. Multipart-операции не реализованы (t38) — границы не размыты.

## 11. Решения по открытым вопросам

**Q1. Сверка SHA-256 при Range-чтении — решена пользователем.**
«Хэш всего объекта»: вариант «по букве канона» — при каждом чтении
GET (200 и 206) объект читается и хэшируется целиком, Range влияет
только на возвращаемые байты; conditional-сверки (If-Match/
If-None-Match/If-Range) идут с ETag всего объекта. Дословно:
«все верно — нам нужно получить хэш от всего объекта, а не от
ренджа, сверять то с хэшем всего объекта надо». Правка канона не
требуется (канон 04 §2 действует буквально). Цена (два прохода по
файлу на GET, O(n²) Range-пагинации) зафиксирована в §9 как
осознанная.

**Q2. Механика CopyObject — решена пользователем: хардлинк + fallback
на побайтовое копирование.** Новый объект ссылается на те же байты
через жёсткую ссылку `part.1` (по образцу `link(2)`):
копирование мгновенно независимо от размера; удаление источника
безопасно (inode жив, пока есть ссылки); orphan-чистка и `.trash` не
ломают копию. При ошибке link (нестандартные ФС) — fallback на
побайтовое копирование (лог warning).

## 12. Ссылки

- Каноны: `arch/owns3/01-overview.md`, `02-operations.md`,
  `03-protocol.md`, `04-storage.md` (с правками §3), `05-access-config.md`.
- Roadmap: `arch/roadmap/owns3.md` (t37, границы; карта референса).
- Референс MinIO (точечный): `cmd/xl-storage.go`,
  `cmd/xl-storage-format-v2.go`, `cmd/erasure-object.go`,
  `cmd/bucket-handlers.go`, `cmd/object-handlers.go`,
  `cmd/bucket-listobjects-handlers.go`.
- Код t36: `src/OwnS3.Storage/IObjectStore.cs`,
  `NotWiredObjectStore.cs`, `src/OwnS3.App/Handlers/*`,
  `src/OwnS3.App/Program.cs`.
