# Spec: t01-basebackup-streaming-client

Стриминговый клиент `BASE_BACKUP`: физический полный бэкап ОДНИМ S3-объектом
(tar-поток напрямую в S3 multipart), без `pg_basebackup`, без второй копии
PGDATA на диске; потоковые restore и verify.

Дорожная карта — [`arch/roadmap/backup.md`](../../../arch/roadmap/backup.md)
(требования 1–9, критерии, этапы, риски). Канон обновлён ПЕРВЫМ (arch-first):
[`arch/backups/01-full-backup.md`](../../../arch/backups/01-full-backup.md),
[`arch/backups/06-s3-layout.md`](../../../arch/backups/06-s3-layout.md),
[`arch/pgnode/04-full-backup.md`](../../../arch/pgnode/04-full-backup.md),
процессы verify/restore/retention и зависимые (перечень — §8). Спека
описывает отражение канона в код; при расхождении канон первичен.

## 1. Цель и мотивация

Сегодня полный бэкап — plain-`pg_basebackup` в staging-каталог ноды с
последующей пофайловой заливкой 1:1 в `full/<id>/`. Факты (из задачи): один
полный E2E-набора — `files=1304` при 73 МБ; для базы 100 ГБ это десятки
тысяч S3-объектов с per-object put/листингами, а на диске ноды одновременно
живут PGDATA + staging-копия (2× объёма, для 100 ГБ — 200 ГБ).
`pg_basebackup` не имеет режима stdout (`-D`/`-Ft`/`--target` — всегда
файл/каталог). Требование владельца: база 100 ГБ не должна занимать 200 ГБ
на диске и не должна превращаться в десятки тысяч S3-объектов, при
сохранении PITR/restore/verify.

Целевое состояние (канон зафиксирован): полный бэкап — собственный
стриминговый клиент `BASE_BACKUP` (протокол физической репликации,
`TARGET=CLIENT`, tar-поток с WAL внутри), перепаковывающийся на лету в
ЕДИНЫЙ несжатый tar и льющийся напрямую в S3 multipart одним объектом
`full/<id>.tar`. Restore и verify читают объект потоково — без архива на
диске и (для verify) без распаковки.

## 2. Принципы

- **Один объект на полный** — `full/<id>.tar`; пик диска операции ≈ 0 сверх
  PGDATA ноды; пик памяти — буфер частей multipart (≤5×16 МиБ), НЕ зависит
  от размера базы (требование 2).
- **Канон arch/ — источник истины**: спека его не пересказывает, а отражает
  в код; противоречие = баг спеки.
- **Семантика прежняя**: команды API, фазы `basebackup|uploading` /
  `downloading|verifying` / `downloading|recovering`, стоп-гейты, 404-vanished,
  «резюмов нет — FAILED → переснятие новым id», гварды (последний валидный,
  чужая цепочка, verify-gate ретенции) — всё сохраняется (требования 4–6).
- **Пишущий тот же процесс**: всё в приёмнике `pgnode`; отдельных
  контейнеров/процессов нет; P/Invoke запрещён — только управляемый .NET.

### Non-goals

- Сжатие данных (gzip/zstd) НЕ вводится — Range-доступ к записям tar важнее
  экономии места.
- Инкрементальные/дифференциальные полные — вне задачи.
- Второй tablespace, слоты для BASE_BACKUP, сжатие манифеста, шифрование
  объекта — вне задачи (fail-fast/не требуется).
- Прежний пофайловый формат `full/<id>/` НЕ поддерживается: система не
  развёрнута, читателей старого формата в коде не остаётся (требование 1).

### Anti-goals (провал даже при «работает»)

- Вторая копия PGDATA на диске ноды где-либо в пайплайне (staging, temp-tar,
  кэш) — прямой провал цели.
- Обход гвардов идемпотентности WAL (перезапись чужой цепочки при дубле).
- Тихая порча набора при несовместимости протокола (требование 8:
  fail-fast с диагностикой).

## 3. Структура: компоненты и размещение в коде

Новых проектов НЕ заводим. Компоненты:

### 3.1 Клиент BASE_BACKUP — `src/PgWorker.PgNode/BaseBackup/`

- **`PgWireReplicationClient`** — низкоуровневый клиент wire-протокола PG v3:
  TCP `127.0.0.1:5432` → `SSLRequest` → TLS (`SslStream`, доверие серта —
  как стрим: Trust Server Certificate) → `StartupMessage`
  (`replication=database`, user `backup_exec`, пароль в памяти/DSN, не argv)
  → SASL SCRAM-SHA-256 / cleartext-password (иное — fail-fast
  `unsupported auth: <метод>`). Сообщения: простая запись/чтение фреймов
  (тип+длина+payload), simple-query.
- **`BaseBackupCommand`** — отправка `BASE_BACKUP (LABEL 'pgw-full-<id>',
  CHECKPOINT 'fast', MANIFEST 'yes', MANIFEST_CHECKSUMS 'SHA256', TARGET
  'client', WAL true, WAIT false)` и **автомат разбора ответа**: DataRow'ы заголовка
  (tablespace list; >1 tablespace → fail-fast `multiple tablespaces not
  supported`) → COPY OUT tar-потоки (основной → при `WAL STREAM` финальный
  поток `pg_wal`) → DataRow'ы итога → CommandComplete. Автомат устойчив к
  вариациям разделителей result-set'ов (не привязан к ним: границы —
  CopyOutResponse/CopyDone/RowDescription). Ошибки сервера (ErrorResponse)
  — наружу с SQLState и текстом.
- Тестовый шов: интерфейс потока сообщений + фейк-последовательности
  (юниты «разбор BASE_BACKUP»).

### 3.2 Tar-перепаковщик — `src/PgWorker.Backups/Tar/` (namespace `PgWorker.Backups.Tar`)

Размещение в PgWorker.Backups (не в PgWorker.PgNode): `TarStreamReader`
нужен И приёмнику (полный/verify/restore — PgWorker.PgNode), И воркеру
(`FullTarSet.ReadBackupLabelAsync` — §3.7); ссылка проектов
PgWorker.PgNode → PgWorker.Backups существует, обратной нет (циклические
ссылки запрещены) — общий код живёт в PgWorker.Backups. Прочие компоненты
BaseBackup (клиент протокола, команда, multipart-поток, парсер манифеста —
§3.1/§3.3/§3.5) остаются в `src/PgWorker.PgNode/BaseBackup/`: потребителей
в воркере у них нет.

- **`TarStreamWriter`** — пишет единый выходной tar в `Stream`-цель:
  заголовки ustar, GNU longname (`L`) для имён >100 байт, PAX (`x`) для
  размеров >8 ГиБ; нулевая добивка в конце. Считает сквозной sha256 и
  общий размер; колбэк-перехват записи `backup_label` (парс
  `WalStartSegment` — переиспользуем `BackupLabel.WalStartSegment`) и
  фиксация карты записей `pg_wal/*` (имя → offset/size) для дубля.
- **`TarStreamReader`** — потоковое чтение tar из `Stream`: ustar+prefix,
  GNU longname, PAX; валидация checksum заголовков; типы `0`/`5`/`2`
  (файл/каталог/symlink); прочие typeflag — ошибка. Используется verify
  (без записи на диск) и restore/дриллом (запись файлов в PGDATA/scratch).
- Входные потоки BASE_BACKUP подаются перепаковщику: записи основного
  потока — как есть; записи WAL-потока — с префиксом `pg_wal/` (в pg_wal
  tar имена без префикса).

### 3.3 Multipart-обёртка — расширение `src/PgWorker.Backups/BackupS3.cs`

- **`IMultipartUploadSession`** (`CreateMultipartUploadAsync`/
  `UploadPartAsync`/`CompleteAsync`/`AbortAsync` в фабрике `IBackupS3`):
  части 16 МиБ, `Content-MD5` каждой части (md5), Complete со списком
  partNumber→ETag; endpoint ФИКСИРОВАН на сессию (обёртка не перебирает
  ноды — перебор делает операция рестартом попытки, см. §6.4).
  `ListMultipartUploadsAsync(prefix)` + `AbortMultipartUploadAsync` — для
  супервизора (гигиена висячих сессий). Внутренние ретраи SDK на цели
  выключены (уже так — `MaxErrorRetry=0`), per-попыточный таймаут —
  существующий `WalReceiverCore.FileAttemptTimeout` (300 с).
- **`GetObjectStreamAsync`** (стрим тела) и **Range-GET** (`GetObjectRangeAsync`)
  — для потокового verify/restore и дубля WAL; **`HeadObjectAsync`**
  возвращает `Result<(bool Exists, long SizeBytes)>` — размер из того же
  `GetObjectMetadataAsync` (HEAD и так выполняется для фиксации endpoint'а
  чтения — размер достаётся бесплатно; суффиксный Range `bytes=-N` OwnS3
  поддерживает, но не используется: полный размер нужен ДО расчёта хвоста
  манифеста, а парсинг Content-Range уступает прямому HEAD). Реализация на
  существующем AWSSDK (`GetObjectRequest` c `ByteRange`); OwnS3 поддерживает
  Range (206 + Content-Range) и multipart (части 1–10000, ≤5 ГиБ — проверено
  по коду `../OwnS3/src/OwnS3.App/Handlers`).

### 3.4 Полный бэкап — перепись `src/PgWorker.PgNode/Operations/FullBackupOperation.cs`

Пайплайн (фазы канона `basebackup` → `uploading`):
1. резолв кредов/пула, REST-проба роли → write-список (фиксация на старте) —
   как сейчас;
2. попытка на первом endpoint write-списка: `CreateMultipartUpload` →
   `PgWireReplicationClient` → BASE_BACKUP → `TarStreamWriter` перепаковывает
   потоки в multipart-писатель (части нарезаются по мере накопления 16 МиБ;
   до 4 `UploadPartAsync` в полёте; буфер ≤5×16 МиБ; backpressure чтения —
   естественный: не читаем из сокета быстрее, чем улетают части) →
   `CompleteMultipartUpload` (сверка числа частей и ETag);
3. фаза `uploading`: IDENTIFY_SYSTEM (существующий `NpgsqlXLogSource` —
   короткое соединение) → дублирование закрытых сегментов набора в `wal/`:
   Range-GET байтов сегмента из СВОЕГО tar-объекта (по карте смещений) →
   `PutWalSegmentAsync`-гвард (строчные имена, сверка ETag/md5, чужая
   цепочка → FAILED `foreign WAL chain`) — до 8 параллельно; последний
   живой сегмент набора (позиция ≥ активного сегмента источника) не
   дублируется;
4. снимок `succeeded`: `wal_start_segment`, `size_bytes` (размер объекта),
   `sha256`, прогресс-поля финализируются.
Отказ endpoint'а посреди попытки: `AbortAsync` (best-effort) → попытка на
следующем endpoint списка (полный рестарт чтения). Исчерпание списка →
FAILED `s3-pool-exhausted`. Отмена (cancel API / linked-CTS бюджета):
закрыть сокет BASE_BACKUP, `AbortAsync`, `failed` `cancelled`. Прогресс в
журнал ноды (`Prog`) каждые N частей + в снимок (`bytes_done`, `parts_done`).

### 3.5 Потоковый verify — перепись `src/PgWorker.PgNode/Operations/VerifyOperation.cs`

- Фаза `downloading`: фиксация endpoint'а (HEAD до первого найденного —
  возвращает и размер объекта) → Range-чтение хвоста объекта `[max(0,
  size−tail), size−1]` (tail = 8 МиБ; manifest не найден → удвоение,
  ≤4 попыток; объект меньше tail → диапазон от 0) → `TarStreamReader` по
  хвосту (валидация checksum) → `backup_manifest` (парс: перечень файлов,
  размеры, алгоритм sha256, хэши).
- Фаза `verifying`: единственный проход `GetObjectStreamAsync`: для каждой
  tar-записи sha256 на лету; сверка с манифестом: файл существует, размер
  совпал, хэш совпал; лишние файлы/отсутствующие → FAILED (permanent).
  Сквозной sha256 объекта — против etcd-поля `sha256` полного (если задано;
  у орфанов может отсутствовать — сверка пропускается).
- `pg_verifybackup`, staging-каталог, скачивание — удаляются. Транспортный
  сбой → failed/downloading (transient); отмена → cancelled — семантика
  прежняя.

### 3.6 Потоковые restore/drill — правка `RestoreOperation.cs`

Шаг «скачивание файлов 1:1» заменяется: фиксация endpoint'а (HEAD) →
`GetObjectStreamAsync` → `TarStreamReader` → запись файлов в PGDATA/scratch
(каталоги/права из заголовков; symlinks — `File.CreateSymbolicLink`;
uid/gid — уже корректны, процесс под postgres). Всё остальное (auto.conf,
recovery.signal, повторное удаление standby.signal, pg_ctl, promote,
fetch-wal, чистки) — БЕЗ изменений. Drill — тот же код в scratch.

### 3.7 Воркер — правки `src/PgWorker.Backups/`

- `BackupStatusJson`/`OperationSnapshot`-маппинг: новые поля `sha256`,
  `progress{bytes,parts}` (терминальные состояния — без progress);
  `BackupProcess` отражает их в etcd.
- `BackupVerifyProcess`: упразднить проверку полноты WAL через
  `ListUnionAsync("full/{id}/pg_wal/")` (канон §7: полнота набора —
  манифестом в потоковом verify; непрерывность дубля — общий контроль
  цепочки `wal/`); TLI-history-разбор этой проверки удаляется вместе с ней.
- `RestoreProcess`/`RestoreDrillProcess`: валидация наличия полного — HEAD
  объекта `full/<id>.tar` вместо `DownloadTextAsync backup_manifest`
  (существование = Complete прошёл); `ListFullsAsync` — объекты
  `full/<id>.tar` (последний компонент, `.tar`-суффикс).
- `RetentionProcess`: удаление полного — `DeleteObject` одного ключа
  (вместо list+batch-delete префикса).
- `SupervisorPlanner`/`BackupSupervisorProcess`: мусор — объекты
  `full/<id>.tar` без etcd-ключа (гейты прежние: свежий ключ + возраст);
  NEW: `ListMultipartUploads` по префиксу шарда → `Abort` сессий старше
  `garbage_min_age_sec`.
- `OrphanRegistry`: критерий `has_valid_full` — объект `full/<id>.tar`.

### 3.8 Тесты

- Юниты (`src/tests/PgWorker.UnitTests`, новые файлы): `TarStreamWriter`/
  `TarStreamReader` (roundtrip, longname, PAX >8 ГиБ-заголовок, symlink,
  добивка, checksum-валидация, отбраковка битого заголовка);
  multipart-обёртка (фейк `IBackupS3`: части/ETag, Abort при отказе,
  рестарт попытки, исчерпание списка → маркер); потоковый
  manifest-verifier (корректный/битый файл/лишний/отсутствующий/транспортный
  сбой → фазы); разбор BASE_BACKUP (фейк-сообщения: заголовок→tar→wal→итог;
  >1 tablespace; ErrorResponse; unsupported auth).
- E2E (`src/tests/PgWorker.E2eTests`): см. §7.

## 4. Фазы реализации (этапы задачи → порядок плана)

1. **Прототип клиента** (этап 1): `PgWireReplicationClient`+
   `BaseBackupCommand`+`TarStreamReader`; консольный прогон против живого PG
   стенда → tar в файл → побайтовая сверка НАБОРА с `pg_basebackup -Ft`
   (распаковка обоих: идентичность перечня файлов, размеров, sha256;
   байтовая сверка содержимого каждого файла). Внутри фазы допускается
   временный debug-инструмент; в финальный код он не входит.
2. **Multipart + потоковая загрузка** (этап 2): `TarStreamWriter`,
   multipart-обёртка, новая `FullBackupOperation`; юниты; E2E полного
   одним объектом (первый зелёный сценарий).
3. **Потоковый restore** (этап 3): `TarStreamReader` в `RestoreOperation`,
   правка drill; E2E restore latest.
4. **Потоковый verify** (этап 4): `VerifyOperation`; E2E verify OK.
5. **Ретенция/супервизор/панель** (этап 5): воркер-правки §3.7; E2E
   retention удаляет объект.
6. **E2E + нагрузочный профиль** (этап 6): полный цикл E2E (PITR через
   TLI), «большой набор» — отсутствие второй копии на диске; docker-E2E
   PgWorker на свежем Release в мерж-гейте (AGENTS.md).

Каждая фаза: юниты → интеграция → E2E; зачистка контейнеров/сетей после
каждой серии (AGENTS.md).

## 5. Ограничения

- .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`;
  централизованные версии пакетов; НОВЫХ пакетов не добавляем (wire-клиент,
  tar, sha256/md5, SASL — штатный BCL; tar своими силами ~500 строк).
- P/Invoke запрещён. Секреты: пароль `backup_exec` — только в памяти
  (DSN), не argv/логи; TLS к S3 — как сейчас (требование 9).
- Существующие бюджеты НЕ меняются: `Job:FullTimeoutSec=21600`,
  `FileAttemptTimeout=300 с`, breaker 45 с, `Retry.BaseSec/MaxSec`,
  `Restore:RecoveryTimeoutSec` — значения из канона (AGENTS.md: бюджеты без
  согласования не менять).
- Лимиты OwnS3: части 1–10000, часть ≤5 ГиБ → при 16 МиБ потолок объекта
  160 ГиБ (цель 100 ГБ с запасом); PG стенда — Spilo-18 (PG18: BASE_BACKUP
  TARGET=CLIENT есть с PG15).
- E2E: динамические порта, полная самоочистка, `[PHASE]`-метки, никаких
  скипов (AGENTS.md).

## 6. Принятые решения (закрытые развилки)

Все решения выведены из требований roadmap-задачи, канона `arch/` и правил
AGENTS; пользователь дал карт-бланш («всё сам, не спрашивать»).

1. **Формат: единый несжатый tar-объект `full/<id>.tar`, маркер формата —
   суффикс `.tar` ключа.** Требование 1 («один объект… или эквивалент одним
   ключом… маркер в имени/ключе»). Без сжатия: Range-доступ к записям
   (дубль WAL, verify-хвост) требует смещаемости; сжатие сломало бы её и
   добавило CPU. Sidecar-объект с манифестом НЕ вводится: второй объект на
   полный противоречит минимизации, а все потребители манифеста читают его
   Range-хвостом или вовсе заменены критерием существования объекта.
2. **Собственный wire-клиент протокола вместо Npgsql для BASE_BACKUP.**
   Npgsql 10.0.3 не экспонирует BASE_BACKUP (публичный API репликации:
   StartReplication/IdentifySystem/TimelineHistory; COPY OUT произвольной
   репликационной команды недоступен; риск зафиксирован в задаче). Приватные
   хаки (рефлексия `ReadSingleRow`) годятся для single-row команд, но не для
   длинного COPY-потока. Свой клиент на TcpClient+SslStream — управляемый
   код, полный контроль потока и отмены.
3. **Аутентификация: SCRAM-SHA-256 + cleartext; иное — fail-fast.**
   Spilo-18 `password_encryption=scram-sha-256` (дефолт PG14+; строка
   pg_hba-гварда — `scram-sha-256`, канон `08-secrets` §4). Требование 8:
   несовместимость — fail-fast с диагностикой. MD5/SCRAM-PLUS не
   реализуем (в контуре не встречаются; поддержка расширяла бы
   непроверяемый код).
4. **Multipart: части 16 МиБ, сессия на ОДНОМ endpoint, отказ → Abort +
   полный рестарт попытки на следующей ноде write-списка; исчерпание →
   FAILED `s3-pool-exhausted`.** Требование 3 (перебор посреди сессии
   недопустим; retry) + канон «резюмов нет»: источник (BASE_BACKUP-стрим)
   не перематывается — retry и есть рестарт чтения. Число попыток =
   размеру write-списка. 16 МиБ — верхняя граница диапазона требования
   (8–16): 10000 частей × 16 МиБ = 160 ГиБ > 100 ГБ цели (при 8 МиБ потолок
   80 ГиБ < цели — недопустимо).
5. **Целостность COMPLETED: Content-MD5 каждой части + Complete с сверкой
   перечня partNumber→ETag + сквозной sha256 tar-потока → etcd-поле
   `sha256`.** Требование 3 («Complete + проверки целостности (Content-MD5
   частей/ETag, sha256)»). sha256 при заливке считается бесплатно на лету;
   сверяется потоковым verify — так битый транспорт ловится дважды (на
   части и на проходе).
6. **Дубль WAL — Range-GET из только что залитого tar-объекта (не из
   потока).** Требование 6 («из tar-потока, либо явно зафиксированный
   отказ»). Чтение из объекта: без второй копии байтов на диске/памяти
   (карта смещений копеечная), параллелится (8), гарантированно читает те
   же байты, что залогированы (объект уже Complete). Буферизация сегментов
   из потока потребовала бы держать до N×16 МиБ и переусложнила пайплайн.
7. **Verify: двухфазный (Range-хвост → манифест; проход → сверка), без
   распаковки и без `pg_verifybackup`.** Требование 5 (потоковый
   верификатор по манифесту без распаковки). Фазы канона `downloading|
   verifying` сохранены с новой семантикой (downloading = добывание
   манифеста, verifying = сверка) — transient/permanent-грань прежняя
   (транспорт vs данные), enum не меняется. Хвост 8 МиБ покрывает
   манифесты ~50K файлов; удвоение ≤4 попыток — для больших наборов.
8. **Проверка полноты WAL-цепочки `full/<id>/pg_wal/` упразднена.** Канон
   §7 обновлён: полнота набора (вкл. pg_wal) доказывается манифестом в
   потоковом verify (сервер сам прислал все сегменты набора — листинг S3
   ничего не добавлял), непрерывность дубля в `wal/` — общий контроль
   цепочки [02-wal-archive §3], который уже существует и от формата полного
   не зависит. Меньше кода, меньше S3-листингов.
9. **Restore/drill: однопроходная потоковая распаковка, endpoint чтения
   фиксирован на операцию (HEAD до первого найденного).** Требование 4
   (без файла архива на диске) + зеркальная семантика write-сессии
   (переключение ноды посреди потока недопустимо — частичный объект;
   транспортный отказ = transient-отказ операции, повтор с нуля — прежняя
   семантика «скачивание transient»).
10. **Критерий завершённости/валидности полного = существование объекта
    `full/<id>.tar`.** Объект появляется в листинге только после Complete
    multipart — «частичный upload» невозможен by construction; DROP старого
    критерия `backup_manifest`-объекта (DR-выбор, runbook, сироты,
    drill-валидация). Прежний «полный без манифеста = недокачан» исчезает
    как класс ошибок.
11. **Фазы приёмника: `basebackup` = соединение+чтение+перепаковка+заливка
    частей+Complete; `uploading` = дубль WAL+итог.** Канонная пара фаз и
    переход RUNNING→UPLOADING сохранены (воркер/панель не меняют автомат);
    UPLOADING теперь короче, но осмысленна (дубль — часть операции до
    COMPLETED, отказ дубля = FAILED — как раньше).
12. **Прогресс: `bytes_done`/`parts_done` в GET-снимке + отражение воркером
    в etcd `progress{bytes,parts}`; `[PHASE]`-метки в E2E; журнал ноды —
    `Prog`-строки каждые 64 МиБ.** Требование 7. Поля опциональны — парсеры
    панели толерантны (канон §5 backwards-compat).
13. **Слот для BASE_BACKUP не используется.** Удержание WAL на время
    бэкапа — инвариант ядра (`pg_backup_start` удерживает сегменты от
    start LSN до конца бэкапа; иначе не работал бы `-X fetch`). Слот
    стримящего приёмника продолжает держать хвост независимо. Новый слот
    = новая сущность без пользы.
14. **Более одного основного tablespace → fail-fast `multiple tablespaces
    not supported`.** Контур Spilo — один tablespace; поддержка многих
    размножала бы непроверяемые пути (порядок потоков, префиксы).
    Требование 8 (fail-fast, не тихая порча).
15. **tar-поддержка: ustar + GNU longname + PAX; типы `0`/`5`/`2`; прочее —
    ошибка.** PG-сервер шлёт ustar с longname; файлы >8 ГиБ требуют PAX
    size (база 100 ГБ может содержать такие). Hardlinks PGDATA не содержит
    — fail-fast вместо эмуляции.
16. **Точка фиксации `wal_start_segment` — из `backup_label` tar-потока,
    публикация с фазы UPLOADING (как в контракте).** Значение доступно с
    начала потока, но публикация в etcd остаётся на старом месте — контракт
    и парсеры не меняются без нужды.
17. **`Staging:Dir`/`PGW_BACKUP_STAGING_DIR` упразднены** (полный/verify на
    диск не пишут); scratch дрилла `/home/postgres/pgdata/backup-drill`
    остаётся (дрилл распаковывает в scratch — ENOSPC реактивный, как в
    каноне). Конфиг-секция `Staging` удаляется (09-operability обновлён).
18. **Никаких изменений в панель AdminPanel.** Полные/verify/алерты читаются
    из etcd-статусов (поля совместимы, новые — опциональны); листинги
    «Хранилища» работают с объектами как раньше. `used_bytes` считает
    tar-объекты автоматически.
19. **Прежний код pg_basebackup/pg_verifybackup/staging-пути из операций
    удаляется** (не мёртвым грузом): система не развёрнута, старого формата
    читателей нет (требование 1). `IProcessRunner` остаётся (pg_dump,
    pg_ctl, pg_controldata).
20. **Версия протокола: только простой wire v3 + replication-режим; TARGET
    `SERVER`/инкрементальные (`INCREMENTAL`) не поддерживаются — fail-fast.**
    Требование 8: несовместимость диагностируется, набор не создаётся.
21. **Таймауты чтения BASE_BACKUP-потока: per-read таймаута НЕТ; внешний
    бюджет — `Job:FullTimeoutSec` супервизора (21600 с) через cancel.**
    Поток легально может стоять (сервер удерживает соединение до появления
    данных/checkpoint'а); искусственный read-таймаут рвал бы валидные
    долгие съёмы. TCP-keepalive на сокете включаем (детект смерти
    соединения). Ровно так живёт и текущий `pg_basebackup` child. Бюджет
    без изменений (AGENTS.md: бюджеты не менять).
22. **Распаковка restore/drill: каталоги создаются и из записей typeflag
    `5`, и лениво по пути файла (`Directory.CreateDirectory` —
    идемпотентен); права из заголовка применяются `chmod`-эквивалентом
    после записи файла.** tar PG содержит каталоговые записи, но
    идемпотентное дублирование создания исключает порядок-зависимость.
23. **Размер объекта для Range-хвоста манифеста — из HEAD:
    `HeadObjectAsync` возвращает `Result<(bool Exists, long SizeBytes)>`
    (размер — `GetObjectMetadataAsync.ContentLength`).** HEAD выполняется
    в verify/restore и так — для фиксации endpoint'а чтения (решение 9);
    размер достаётся тем же вызовом, без отдельного запроса и без парсинга
    `Content-Range` суффиксного Range (который OwnS3 поддерживает, но
    который дал бы только размер, всё равно требуя первого запроса).
    Пул-реализация (§3.7 `PoolBackupS3`): перебор read-целей — первая
    цель, где объект существует, даёт и размер.

## 7. Критерии приёмки (из roadmap-задачи)

- **E2E (docker, свежий Release)**: полный бэкап снят одним объектом
  `full/<id>.tar` (list: ровно один объект префикса) → verify OK → restore
  `latest` COMPLETED → restore `target_time` (PITR через TLI-переход)
  COMPLETED → retention удаляет объект (list пуст, ключ удалён). Профиль
  «большой набор» (генерация ≥3 ГиБ данных): во время полного бэкапа
  свободное место data-volume ноды падает меньше, чем на 10% от размера
  набора (вторая копия отсутствует; staging-копия съела бы ~100%), пик
  объектов S3 на полный = 1.
- **Юниты**: tar-writer/reader (roundtrip, longname/PAX, symlink, битый
  checksum, добивка); multipart-обёртка (части/ETag/Abort/retry/
  исчерпание); потоковый manifest-verifier (ok/битый/лишний/отсутствующий/
  транспорт); разбор BASE_BACKUP (последовательности сообщений, tablespace
  fail-fast, ErrorResponse, auth-методы).
- **Канон обновлён первым** — выполнено (см. §8); код ревьюится на
  соответствие канону (расхождение = блокирующее).
- **Прототип (этап 1)**: побайтовая сверка содержимого набора с
  `pg_basebackup -Ft` на живом PG — зафиксирована в отчёте фазы
  (временный инструмент в поставку не входит).

## 8. Обновлённые arch/-документы (сделано до спеки)

- `arch/backups/01-full-backup.md` — инструмент, пайплайн, WAL набора,
  §7 verify (одна потоковая проверка).
- `arch/backups/06-s3-layout.md` — layout `full/<id>.tar`, семантика чтения
  потока с фиксацией endpoint, дублирование WAL.
- `arch/pgnode/04-full-backup.md` — клиент, перепаковка, multipart, дубль,
  отмена (переписан).
- `arch/pgnode/05-verify.md`, `arch/pgnode/processes/c-verify.md` —
  потоковый верификатор.
- `arch/pgnode/06-restore-drill.md`, `processes/d-restore.md`,
  `processes/e-drill.md`, `processes/g-wal-delta-assembly.md` — потоковая
  распаковка.
- `arch/pgnode/processes/b-full-backup.md` — фазы/исходы операции.
- `arch/pgnode/01-placement.md` (staging/ресурсы), `02-api.md` (прогресс),
  `README.md`.
- `arch/backups/05-etcd-contract.md` (поля `sha256`/`progress`, критерий
  сирот), `processes/c-retention.md` (удаление объекта),
  `processes/e-supervisor.md` (мусор-объекты, multipart-гигиена),
  `processes/a-full-scheduler.md`, `processes/enums.md`,
  `processes/f-restore.md`, `processes/g-drill.md`, `07-retention.md`,
  `08-secrets.md`, `09-operability.md`, `10-runbook-restore.md`, `README.md`,
  `arch/00-components.md`.

## 9. Что осталось исполнителю плана (без выбора)

Всё выше — обязательные решения; план раскладывает §3–§4 по шагам с
TDD. Единственные степени свободы — внутренние имена приватных членов и
порядок вспомогательных юнит-тестов внутри фазы. Размещение компонентов
(включая Tar-код в `src/PgWorker.Backups/Tar/`, §3.2) и все имена публичных
компонентов §3 зафиксированы и выбору исполнителя не принадлежат.
