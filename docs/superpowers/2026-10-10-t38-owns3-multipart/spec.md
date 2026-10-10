# Spec t38-owns3-multipart: multipart-загрузка ownS3

Дата: 2026-10-10. Постановка — `arch/roadmap/owns3.md` (тег
`t38-owns3-multipart`). Каноны: `arch/owns3/01…05`; задача наполняет
дисковой механикой multipart-методы `XlObjectStore`
(`src/OwnS3.Storage`), протокольный контур которых (роутинг, подпись,
presigned, матрица прав, хендлеры, маппинг ошибок) создан в t36/t37.
Референс (точечный): `cmd/erasure-multipart.go`,
`cmd/object-multipart-handlers.go`.

## 1. Цель

Полный multipart-цикл S3 Multipart Upload API — 7 операций канона
02 §5 (CreateMultipartUpload, UploadPart, UploadPartCopy,
CompleteMultipartUpload, AbortMultipartUpload, ListParts,
ListMultipartUploads) на диске по канону 04 §5: раскладка
`.owns3.sys/multipart/<sha256(bucket/object)>/<uploadID>/`, журналы
`uploads.json`/`parts.json`, сборка Complete rename'ами частей,
составной ETag, чистка брошенных загрузок по возрасту (24 ч).

Объект, собранный из частей, — полноценный объект хранилища: чтение
(Get/Head с conditional/Range/If-Range и checksum-сверкой), копирование
(CopyObject/UploadPartCopy), атрибуты (GetObjectAttributes — реальные
части), листинги и удаление работают с ним как с простым PUT.

После t38 заглушечное поведение multipart-методов (500 InternalError)
исчезает; сервис принимает большие объекты частями (путь подсистемы
бэкапов для объектов > 5 ГБ).

Не-цели (границы): приёмка реальными клиентами AWS SDK/`mc` и
dev-стенд-сервис (t39), erasure coding (t40), миграция MinIO (t41),
TLS (t42), изменения протокольного слоя SigV4/XML/ошибок (готово в
t36), IAM (готово), per-part/checksum-расширения протокола S3
(additional checksums `x-amz-checksum-*` на UploadPart — вне набора).

## 2. Принципы

1. **Arch-first**: канон 04 §5 — истина раскладки и атомарности
   multipart. Пробелы канона закрыты правками ДО кода (раздел 3 —
   уже внесены при написании спеки).
2. **Единый коммит-поинт** (продолжение принципа t37): видимость
   объекта для читателей меняется только атомарной заменой `xl.meta`
   на Complete. Загруженные части и журналы загрузки клиентам не
   видны; сбой до коммит-поинта оставляет загрузку живой и мусор
   только в `tmp/`, `.trash/` и orphan-dataDir — всё чистится
   стартом/фоном.
3. **Идемпотентность и восстановимость**: прерванный Complete (сбой до
   коммит-поинта — загрузка ещё в `uploads.json`) повторяется
   идемпотентно (тот же манифест → тот же результат) — доиспользование
   перенесённых частей через маркер попытки; дозагрузка недостающих
   частей клиентом разрешена. Повторный Complete после успешного и
   повторный Abort после успешного → `NoSuchUpload` (канон 02 §5).
4. **Надёжность без per-upload/per-part локов**: конкурентные
   UploadPart одного `partNumber` — last-writer-wins (файл части и
   журнал обновляются согласованной парой rename'ов под `_commitLock`
   процесса — тем же единственным локом, что и коммиты t37);
   конкурентные Complete/Abort одной загрузки сериализуются им же.
5. **Тестируемость без хоста**: `OwnS3.Storage` без ASP.NET; юниты на
   temp-томе; тестовые хуки точек сбоя — по образцу `HardLinkProbe`
   t37.
6. **Production ready**: fsync-дисциплина t37 (файлы —
   `Flush(flushToDisk: true)`, каталоги — best-effort с warning),
   диагностика в лог; enterprise-защиты не нужны (домашний контур).
7. Язык: документация и комментарии — русские; идентификаторы —
   английские.

## 3. Arch-first правки канона (внесены в worktree этой спекой)

Пробелы канонов, без закрытия которых реализация неоднозначна. Правки
применены к `arch/owns3/`:

1. **Журнал частей `parts.json`** (04 §5): записи
   `{partNumber, etag (hex-MD5 без кавычек), size, modTime}` в
   каталоге `<uploadID>/` — источник ListParts и сверки манифеста
   Complete; атомарное обновление вместе с заменой файла части.
   Решение пользователя (Q1): журнал, а не метафайлы на часть и не
   вычисление MD5 на лету.
2. **Поля `bucket`/`key` в записях `uploads.json`** (04 §5): sha256
   необратим, а ListMultipartUploads листит загрузки бакета с
   фильтрами по ключу — записи дополняются исходными `bucket`/`key`.
3. **Маркер попытки `attempt.json`** (04 §5): `dataDir` текущей сборки
   Complete в каталоге загрузки — механизм идемпотентности повторной
   сборки (доиспользование перенесённых частей).
4. **Чужая загрузка для read-only — `NoSuchUpload`** (05 §3):
   ListParts по чужому uploadId → 404, неотличимо от несуществующей
   (фильтр видимости, не отказ прав). Решение пользователя (Q2).
5. **ETag CopyObject наследуется от источника** (02 §1, §2): для
   multipart-источника — составной `N-md5` (содержимое неизменно;
   референс MinIO переносит etag при RenameData). Решение
   пользователя (Q3); устраняет внутреннее противоречие прежней
   формулировки («hex-MD5 содержимого» vs «совпадает с ETag
   источника») для multipart-источников.

## 4. Структура и компоненты

### 4.1. Раскладка multipart-контура на диске

```
.owns3.sys/multipart/
└── <sha256hex>/                        ← SHA-256 UTF-8 байтов строки "<bucket>/<key>",
│                                          hex, lowercase; каталог на активный ключ
    ├── uploads.json                    ← журнал активных загрузок ключа
    └── <uploadID>/                     ← каталог загрузки (uploadID — UUID v4, формат "N")
        ├── xl.meta                     ← метаданные будущего объекта (формат XlMetaFile t37)
        ├── parts.json                  ← индекс загруженных частей
        ├── attempt.json                ← маркер попытки Complete (dataDir) — только на время сборки
        └── part.N                      ← байты частей (N = partNumber, 1–10000)
```

- `uploads.json`: JSON-массив записей
  `{uploadId, bucket, key, initiatedMs (unix-мс), accessKey}`.
  Обновление — tmp + атомарный rename (по образцу `xl.meta` t37);
  файл остаётся пустым массивом после завершения всех загрузок ключа.
- `parts.json`: JSON-массив записей
  `{partNumber, etag (hex-MD5 без кавычек), size, modTimeMs}` по
  возрастанию partNumber. Обновление — tmp + rename.
- `xl.meta` загрузки — запись формата `XlMetaFile` (recordVersion 1):
  `VersionId` = uploadId, `Size` = 0, `ETag` = `""`,
  `ContentSha256` = `""` (заполняются на Complete), `ModTime` = время
  инициации, `ContentType`/`UserMetadata` — метаданные будущего
  объекта из CreateMultipartUpload.
- Части пишутся через `part.N.tmp` (fsync) → rename поверх `part.N`;
  чтение частей Storage-ом — по `FileShare.Read`.
- Журналы и `xl.meta` загрузки не несут данных: потеря целостности
  журнала (`JsonException`) — warning в лог + загрузка недоступна
  (исход `NoSuchUpload`); `parts.json` битый при живой записи в
  `uploads.json` — загрузка недоступна для Complete/ListParts
  (`NoSuchUpload`), каталог чистится по возрасту.

### 4.2. Операции (дисковая механика)

**CreateMultipartUpload**: `EnsureBucket` (NoSuchBucket) → uploadId =
`Guid.NewGuid()` → создание `<sha256>/` и `<uploadID>/`, запись
`xl.meta` загрузки → добавление записи в `uploads.json` → возврат
uploadId.

**UploadPart**: `EnsureBucket` → разрешение загрузки (запись в
`uploads.json` ∧ каталог существует; иначе `NoSuchUpload`) → тело
копируется в `part.N.tmp` (MD5-инкремент + счётчик; фактическая длина ≠
`contentLength` → `XlIntegrityException`/500 — остаточный случай,
тело уже проверено конвейером), fsync → под `_commitLock`: rename
`part.N.tmp` → `part.N` + upsert записи в `parts.json` (etag hex, size,
modTime = now). Тело пишется БЕЗ лока (долго), rename+journal — только
под локом: конкурентные перезагрузки одного partNumber линеаризуются,
файл и журнал не расходятся. Возврат `PutResult` (ETag в кавычках —
P8 t37).

**UploadPartCopy**: разрешение загрузки приёмника (`NoSuchUpload`) →
чтение `xl.meta` источника (`NoSuchKey`) → conditional источника
(`ifModifiedSinceApplies: false`; любой провал → 412 — как
CopyObjectAsync t37) → резолв диапазона: `null` → весь объект;
`start > end`, `start >= size` или `end >= size` → `InvalidArgument`
(обе причины — один код, канон 02 UploadPartCopy); размер диапазона >
5 ГБ → `EntityTooLarge` → чтение среза источника (маппинг offset на
части, §4.3) → запись части (механика UploadPart). ETag части =
hex-MD5 байтов диапазона; источник не меняется.

**CompleteMultipartUpload** (под `_commitLock`, шаги 4–9):

1. Разрешение загрузки → `NoSuchUpload`.
2. Сверка манифеста с `parts.json`: каждая пара манифеста
   `(partNumber, ETag)` — кавычки нормализуются (клиент мог прислать
   без кавычек), hex в lowercase; несуществующий номер, несовпадение
   ETag → `InvalidPart`. Проверка размеров: каждая часть манифеста,
   кроме последней, ≥ 5 МиБ → иначе `InvalidPart` (канон 02).
3. Попытка сборки: `attempt.json` отсутствует → новый dataDir-uuid
   (он же будущий `versionId` записи), `attempt.json` = `{dataDir}`;
   присутствует → переиспользовать dataDir из него (пересоздать при
   утрате). dataDir-каталог создаётся в целевом каталоге объекта
   (создавая каталоги-префиксы).
4. Перенос частей: для каждого номера манифеста —
   `dataDir/part.N` отсутствует → rename из каталога загрузки
   (отсутствие файла → `InvalidPart`); уже перенесённое — не трогать
   (идемпотентность повтора).
5. Составной ETag: `<N>-<md5(concat hex-ETag'ов манифеста)>` —
   конкатенация hex-строк без кавычек и разделителей, от неё MD5
   (канон 02 §1).
6. SHA-256 записи: **полный последовательный проход** по
   `dataDir/part.N` в порядке номеров манифеста — SHA-256 конкатенации
   не собирается из хэшей частей, чтение единственный способ (осознанная
   цена; см. §9).
7. `xl.meta` объекта: `ContentType`/`UserMetadata` — из `xl.meta`
   загрузки; `Size` = сумма размеров частей манифеста; `ETag` —
   составной; `VersionId` = dataDir-uuid попытки; `ModTime` = now.
8. **КОММИТ**: атомарная замена `xl.meta` целевого объекта
   (bkp → tmp → rename — `XlMetaFile.Write`, схема перезаписи канона
   04 §4 п.4; для нового ключа — та же атомарная установка). С этого
   мгновения объект виден Get/Head/List.
9. После коммита: старый dataDir целевого объекта (если был) →
   `.trash/<guid>`; удаление записи из `uploads.json`; каталог
   загрузки целиком (остатки частей не из манифеста, журналы,
   `xl.meta` загрузки, `attempt.json`) → `.trash/<guid>`.
   Возврат `CompleteResult` (составной ETag в кавычках).

Сбой на шагах 3–7 — загрузка жива (`uploads.json` не тронут),
частично собранный dataDir — orphan (чистится порогом 1 ч по mtime,
механизм t37); повторный Complete доиспользует перенос (шаг 4).

**AbortMultipartUpload**: под `_commitLock`: разрешение загрузки →
`NoSuchUpload` (повторный Abort после успешного — тот же исход, канон
02) → удаление записи из `uploads.json` → каталог загрузки →
`.trash/<guid>`. Живой attempt.json уходит вместе с каталогом;
недособранный orphan-dataDir чистится возрастом.

**ListParts**: разрешение загрузки С УЧЁТОМ видимости (раздел 4.5) →
части из `parts.json` по возрастанию номеров; `part-number-marker` —
строго после; `max-parts` (null = 1000); `NextPartNumberMarker` при
усечении. `LastModified` части — modTime записи.

**ListMultipartUploads**: обход всех `<sha256>/` каталогов
`multipart/`-контура, чтение `uploads.json`, фильтр по `bucket`;
далее — семантика канона 02 §5: фильтры `prefix`/`delimiter` (свёртка
в `CommonPrefixes`, считаются в `max-uploads`), `key-marker` +
`upload-id-marker` (продолжение строго после пары), сортировка —
лексикографическая по UTF-8 байтам ключа, затем по времени инициации
(внутри одного ключа — по initiatedMs; uploadId-каталоги одного ключа
выдаются в порядке инициации).

### 4.3. Чтение многочастевых объектов

`t37` хардкодит `part.1` (чтение тела, checksum, Range). t38 вводит
многочастевое тело как первый класс:

- **Часть данных**: `dataDir` закоммиченного объекта содержит
  `part.1…part.N` (порядок номеров = порядок байтов). Простой PUT —
  ровно `part.1` (частный случай).
- **Составной поток**: `MultipartBodyStream` — последовательность
  `FileStream` по `part.N` (открывается лениво по мере чтения,
  `FileShare.Read`); `Length` = `xl.meta.Size`.
- **Checksum-сверка** (канон 04 §2 — при каждом чтении данных):
  SHA-256 по конкатенации всех `part.N` в порядке номеров, ДО передачи
  любых байтов клиенту (200 и 206 — как t37); несовпадение → 500
  `InternalError`, байты не отдаются.
- **Range**: маппинг `(start, end)` на границы частей (offset →
  partIndex + смещение); срез читается с `Seek` в части, переходя по
  частям. Правила резолва (`RangeResolver` t37) не меняются —
  применяются к `Size` всего объекта.
- **Conditional/If-Range**: без изменений — сверка с ETag (составным)
  и ModTime записи.
- **GetObjectAttributes**: `ObjectParts` — реальные части: перечисление
  `part.N` в dataDir (номер из имени, размер из файла), по возрастанию;
  `PartsCount`, пагинация `part-number-marker`/`max-parts`,
  `IsTruncated`. Синтетическая «одна часть» остаётся только для
  простых PUT-объектов.
- **CopyObject multipart-источника** (≤ 5 ГБ, лимит уже в t37): dataDir
  копии — пофайловый перенос `part.N` источника (хардлинк-точка t37 —
  на net10.0 fallback: побайтовое копирование каждой части);
  `ETag`/`ContentSha256`/`Size` наследуются из записи источника
  (etag — составной, arch-правка §3.5); versionId/modTime новые.
- **UploadPartCopy** источника-любого: срез диапазона по границам
  частей (§4.2).

### 4.4. Чистки (XlVolume, старт + фон)

Пороги канона 04 §5–6; время — `TimeProvider` (тесты — подмена):

- **Брошенные загрузки — 24 ч от initiation-времени** (запись
  `uploads.json`; журнал недоступен/битый → mtime каталога загрузки
  как прокси): каталог загрузки → `.trash/<guid>`, запись → из
  `uploads.json`. Выполняется на старте и каждым фоновым проходом
  (VolumeCleanupService, период 15 мин t37 — без изменений).
- Опустевший `<sha256>/` (пустой `uploads.json` и нет каталогов
  загрузок) — удалить.
- `.trash/` и orphan-dataDir — существующие чистки t37 не меняются
  (orphan ловит недособранные Complete-попытки).
- `tmp/` multipart не использует (части пишутся tmp-файлом прямо в
  каталоге загрузки) — безусловная чистка старта безопасна.

### 4.5. Видимость загрузок и права

- Владелец загрузки фиксируется в `uploads.json` при Create (accessKey
  инициатора — контракная правка §5.1).
- `ListParts`/`ListMultipartUploads`: read-only — только свои загрузки
  (чужая → `NoSuchUpload` / фильтр списка); read-write/admin — все.
- Остальные multipart-операции (Create/UploadPart/UploadPartCopy/
  Complete/Abort) — read-write/admin (матрица готова, App).
- `DeleteBucket`: бакет с живыми загрузками → `BucketNotEmpty` —
  проверка активных записей `uploads.json` с `bucket` = удаляемому
  (дополнение к существующей проверке `xl.meta` в поддереве; пометка
  «t38» в коде t37).

### 4.6. Правки `src/OwnS3.App`

1. Передача инициатора в Create и owner-фильтра в ListParts/
   ListMultipartUploads (§5.1): хендлеры берут `context.Request
   .Identity.AccessKey` и policy-признак «все загрузки».
2. `UploadPartCopyHandler`: `LastModified` ответа — из
   `PutResult.LastModified` (modTime части), не `UtcNow` (аналог
   CopyObjectHandler t37).
3. Заглушечные следы удаляются: `DrainAsync`/`ThrowUnavailable` из
   `XlObjectStore.Multipart.cs`; комментарий `NotWired`-семантики.
4. `ObjectStoreUnavailableException` остаётся в коде (маппинг 500) —
   источников больше нет.

## 5. Контрактные правки домена (IObjectStore, не arch)

1. `CreateMultipartUploadAsync(bucket, key, metadata, ct)` →
   `CreateMultipartUploadAsync(bucket, key, metadata, initiatorAccessKey,
   ct)` — владелец для `uploads.json` (канон 05).
2. `ListPartsAsync(bucket, key, uploadId, maxParts, partNumberMarker, ct)` →
   + `UploadVisibility visibility` (параметр: `AllUploads` для
   read-write/admin, `OwnedBy(accessKey)` для read-only): чужая →
   `NoSuchUpload`.
3. `ListMultipartUploadsAsync(bucket, query, ct)` → `UploadsQuery`
   + поле `UploadVisibility Visibility` (фильтр списка).
4. `UploadVisibility` — небольшой доменный тип (record/класс) в
   `DomainTypes.cs`: `AllUploads` | `OwnedBy(string AccessKey)`.

## 6. Фазы реализации (для plan)

0. **Arch-фаза**: контрольная сверка спеки с канонами 02/04/05 после
   правок §3.
1. **Контуры и журналы**: sha256-пути, `uploads.json`/`parts.json`/
   `attempt.json` (чтение/запись/атомарность), `xl.meta` загрузки,
   чистка 24 ч в `XlVolume` + юниты (roundtrip журналов, раскладка
   путей, чистка на подменённом `TimeProvider`, битые журналы).
2. **Цикл загрузки**: Create/UploadPart/Abort/ListParts/ListMultipart
   Uploads (вкл. видимость) + юниты (раскладка, NoSuchUpload-исходы,
   перезагрузка части last-writer-wins, пагинации/фильтры, read-only
   фильтры, DeleteBucket при живой загрузке).
3. **Complete**: сверка манифеста, попытка/идемпотентность (тест-хук
   сбоя между переносом и коммитом — по образцу `HardLinkProbe`),
   составной ETag, коммит, зачистка + юниты (InvalidPart/размер 5 МиБ/
   идемпотентный повтор/повтор после успеха → NoSuchUpload/остатки
   частей не из манифеста → .trash).
4. **Многочастевое чтение**: `MultipartBodyStream`, checksum по
   конкатенации, Range-маппинг, GetObjectAttributes реальные части,
   CopyObject multipart-источника + юниты (тело по частям, 206 через
   границы частей, порча части → 500 до байтов, атрибуты, копия
   наследует составной ETag).
5. **UploadPartCopy**: диапазон/лимиты/conditional + юниты (весь
   объект/диапазон/InvalidArgument/EntityTooLarge/412/multipart-
   источник).
6. **App-стыковка**: контрактные правки §5, хендлеры (инициатор,
   LastModified), переделка `RoutingScenarios` (multipart-кейсы →
   реальные исходы) и `AccessScenarios` («Reader_MultipartListings_
   PassWithoutOwnerFilter» → реальный фильтр) + интеграционные
   HTTP-сценарии §7.
7. **Мерж-гейт**: `dotnet build` Release (0 warnings,
   `TreatWarningsAsErrors`), `dotnet test` юниты+интеграция OwnS3
   зелёные; docker-контур не поднимается (E2E — t39).

## 7. Тестовая стратегия

Юниты (`OwnS3.UnitTests/Storage`, temp-том + teardown): классы кейсов
`XlObjectStoreMultipartTests` (циклы операций, идемпотентность,
сбоеустойчивость через хук), `MultipartCleanupTests` (24 ч/битые
журналы/пустые sha256-каталоги). Детерминированное время — тестовый
`TimeProvider`; без реальных sleep (правило ≤ 30 с).

Интеграционные (`OwnS3.IntegrationTests/Api`, in-memory хост с temp-томом
— фабрика t37):

- полный цикл: Create → UploadPart×3 (вкл. перезагрузка одного номера)
  → ListParts (пагинация) → Complete → Get (тело, ETag составной) →
  Head → List → Delete;
- Range 206 с границей на стыке частей; conditional по составному
  ETag; GetObjectAttributes части multipart-объекта;
- UploadPartCopy (весь объект, диапазон) → Complete;
- CopyObject multipart-источника → наследованный составной ETag;
- Abort → NoSuchUpload на последующие операции; повторный Complete
  после успеха → NoSuchUpload; повторный Abort → NoSuchUpload;
- ошибки: InvalidPart (чужой ETag, дыра в номерах, часть < 5 МиБ не
  последняя), InvalidPartOrder (App), EntityTooLarge части,
  NoSuchUpload на несуществующем uploadId;
- права: read-only — Create → 403; reader видит свои загрузки (фильтр
  списков), чужие — NoSuchUpload; writer/admin — все;
- DeleteBucket при живой загрузке → 409 BucketNotEmpty, после Abort →
  204;
- HTTP-статусы/XML-схемы ответов (InitiateMultipartUploadResult,
  CompleteMultipartUploadResult, ListPartsResult,
  ListMultipartUploadsResult вкл. CommonPrefixes/маркеры).

Существующие сценарии t36/t37 не регрессируют: `BodyIntegrityScenarios`
(сверки конвейера UploadPart — чанковая подпись, sha256, Content-MD5),
`ErrorFormatScenarios`, `AuthScenarios` (presigned multipart-набор).

AAA-нотация в комментариях тестов; без хардкода портов; без docker.

## 8. Ограничения и риски

- **Не трогаем**: `OwnS3.Protocol` (крайний случай — только если
  юнит-сверка выявит ошибку t36; отдельным пунктом с обоснованием),
  роутинг/подпись/матрицу прав (готовы), формат `xl.meta` записи
  (recordVersion 1 достаточно — части читаются из dataDir).
- **Цена SHA-256 на Complete**: полный проход по собранному dataDir
  (максимум 10000 × 5 ГБ теоретически; практические объекты бэкапов —
  десятки–сотни ГБ: Complete читает их с диска один раз, ~минуты на
  сотни ГБ). Неизбежно при канонном «SHA-256 от содержимого dataDir»
  (хэш конкатенации не собирается из хэшей частей); повтор — только с
  arch-правкой семантики checksum.
- **Цена checksum-чтения multipart-объекта**: каждый GET читает все
  части для сверки (продолжение решения t37 Q1 на многочастевые
  объекты; Range-пагинация большого объекта — O(n²) IO по частям).
- **Один процесс**: конкурентный второй инстанс на том же томе не
  поддерживается (ограничение t37); `_commitLock` не защищает от
  внешнего процесса.
- **24 ч чистка vs долгие загрузки**: клиент, грузящий одну загрузку
  дольше 24 ч от Create, потеряет её (критерий канона: загрузки живут
  минуты; перешедшие порог считаются брошенными).
- **ListMultipartUploads без индекса**: полный обход sha256-каталогов
  (их число = число ключей с активными загрузками — мало по
  построению чисткой 24 ч).

## 9. Критерии приёмки

1. 7 multipart-операций работают через подписанный HTTP
   (интеграционные сценарии §7 зелёные); заглушечные 500 исчезли.
2. Раскладка диска соответствует канону 04 §5 (тест-ассерты путей:
   `.owns3.sys/multipart/<sha256>/<uploadID>/{xl.meta, parts.json,
   part.N}`, `uploads.json` в `<sha256>/`, `attempt.json` на время
   сборки).
3. Составной ETag: `<N>-<md5(concat hex-ETag'ов)>` — на Complete,
   в ответе, в `xl.meta`, в листингах; ETag части = hex-MD5 тела;
   conditional-сверки с составным ETag работают.
4. Коммит-поинт: объект появляется в Get/Head/List только заменой
   `xl.meta` на Complete; прерванный Complete (сбой до коммита) не
   создаёт видимого объекта, загрузка остаётся живой, повтор
   идемпотентен (тот же манифест → тот же результат, переиспользование
   перенесённых частей), дозагрузка частей после сбоя работает.
5. Повторный Complete после успеха и повторный Abort — `NoSuchUpload`
   (404); идемпотентность DeleteObject не регрессирует.
6. Чтение multipart-объекта: тело = конкатенация частей; Range 206
   (вкл. границы частей), checksum-сверка при каждом GET (порча части
   → 500 до байтов); GetObjectAttributes — реальные части с
   пагинацией.
7. CopyObject multipart-источника наследует составной ETag;
   UploadPartCopy: весь объект/диапазон, InvalidArgument на невалидный
   и выходящий за размер диапазон, EntityTooLarge > 5 ГБ,
   copy-условия 412.
8. Чистки: брошенные загрузки удаляются по порогу 24 ч от
   initiation-времени (детерминированные тесты времени); битые журналы
   не валят процесс (warning, недоступность загрузки); опустевшие
   sha256-каталоги удаляются.
9. Права: read-only — 403 на Create/UploadPart/Complete/Abort; свои
   загрузки в листингах, чужие — фильтр/`NoSuchUpload`;
   read-write/admin — все.
10. DeleteBucket: живые загрузки бакета → 409 BucketNotEmpty.
11. Сборка Release чистая (`TreatWarningsAsErrors`), все тесты
    `OwnS3.UnitTests` + `OwnS3.IntegrationTests` зелёные; сценарии
    t36/t37 не регрессируют.
12. E2E реальными клиентами не выполняется (граница t39).

## 10. Решения по открытым вопросам

**Q1. Индекс частей живой загрузки — решено пользователем: parts.json.**
Журнал в каталоге загрузки (partNumber → etag/size/modTime, атомарный
tmp+rename) — вместо метафайлов на часть и вместо вычисления MD5 на
лету (последнее сделало бы ListParts/Complete полным проходом по
байтам). Arch-правка канона 04 §5 внесена (§3.1).

**Q2. Чужая загрузка для read-only в ListParts — решено пользователем:
404 NoSuchUpload.** Чужая загрузка неотличима от несуществующей
(фильтр видимости; право на операцию есть). Arch-правка канона 05 §3
внесена (§3.4).

**Q3. ETag CopyObject multipart-источника — решено пользователем:
наследовать от источника.** Составной `N-md5` переносится копией
(содержимое неизменно; референс MinIO); пересчёт hex-MD5 полным
проходом отвергнут. Arch-правка канона 02 §1/§2 внесена (§3.5).

## 11. Ссылки

- Каноны: `arch/owns3/01-overview.md`, `02-operations.md` (§5
  multipart; §1 ETag — с правкой), `03-protocol.md`, `04-storage.md`
  (§5 multipart — с правками), `05-access-config.md` (§3 — с правкой).
- Roadmap: `arch/roadmap/owns3.md` (t38; t39 — приёмочная грань).
- Референс MinIO (точечный): `cmd/erasure-multipart.go`,
  `cmd/object-multipart-handlers.go`.
- Спека/план t37 (наследуемые решения): `docs/superpowers/
  2026-10-10-t37-owns3-storage-objects/spec.md` — коммит-схема §4.3,
  `_commitLock` §2.3, `XlMetaFile` §4.2, тест-хук `HardLinkProbe`,
  drain-семантика.
- Код: `src/OwnS3.Storage/XlObjectStore.Multipart.cs` (заглушки),
  `IObjectStore.cs`, `Domain/DomainTypes.cs`, `XlVolume.cs`,
  `XlObjectStore.Objects.cs`, `src/OwnS3.App/Handlers/
  MultipartHandlers.cs`, `Pipeline/S3Middleware.cs`,
  `Pipeline/VolumeCleanupService.cs`.
