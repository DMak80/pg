# t01-basebackup-streaming-client — план реализации

> **Для исполнителя:** план исполняется шаг за шагом (checkbox `- [ ]`).
> Шаг выполняет СИЛЬНАЯ модель целиком; часть работы выделена в ПОДПУНКТЫ —
> самодостаточные задания для СЛАБОЙ модели (внутри шага, в конце). Всё, что
> не выделено в подпункты, делает сильная модель. Все решения уже приняты
> (spec §6) — точек выбора при реализации нет.

**Цель:** полный бэкап шарда — собственный стриминговый клиент `BASE_BACKUP`,
перепаковывающий tar-поток в ЕДИНЫЙ объект `full/<id>.tar` (S3 multipart,
без pg_basebackup и без второй копии PGDATA на диске); потоковые restore и
verify по тому же объекту.

**Архитектура:** приёмник `pgnode` (PgWorker.PgNode) читает wire-протокол PG
v3 сам (TcpClient+SslStream, SCRAM-SHA-256), перепаковывает входные tar-потоки
BASE_BACKUP в один выходной tar (ustar+GNU longname+PAX) и льёт его частями
по 16 МиБ в OwnS3 (сессия на одном endpoint); verify читает манифест
Range-хвостом и сверяет sha256 одним проходом; restore распаковывает поток в
PGDATA. Воркер меняет только точечные места (валидация наличия объекта,
удаление одного объекта, мусор-гейты, новые поля статуса).

**Стек:** .NET 10/C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`;
новых NuGet-пакетов НЕ добавлять (BCL: `System.Security.Cryptography`,
`System.Net.Security`, `System.Text.Json`; AWSSDK.S3 уже подключён).
P/Invoke запрещён.

**Spec:** `docs/superpowers/2026-11-05-t01-basebackup-streaming-client/spec.md`
(обязателен к прочтению; канон arch/ — первичен при расхождении).

## Глобальные ограничения (из spec §5)

- Бюджеты НЕ менять: `Job:FullTimeoutSec=21600`, `WalReceiverCore.FileAttemptTimeout=300 c`,
  breaker 45 c, `Retry.BaseSec=300/MaxSec=3600`, `Restore:RecoveryTimeoutSec`.
- Части multipart: 16 МиБ; лимиты OwnS3: части 1–10000 (потолок объекта
  160 ГиБ), часть ≤5 ГиБ; превышение 10000 частей — FAILED с диагностикой.
- Пароль `backup_exec` — только в памяти (строка соединения), не в argv/логах.
- TLS к источнику PG: доверие серта (Trust Server Certificate — как у
  стрима); TLS к S3 — как сейчас (CA-bundle через `S3ClientOptions.CaBundlePath`).
- Комментарии/документация — русский; идентификаторы — английские.
- Тесты — AAA-комментарии. Сборка и тесты — команды из §«Проверка» каждого
  шага, с env-префиксом `DOTNET_CLI_UI_LANGUAGE=en`; docker-E2E — сборка и
  тесты ДВУМЯ отдельными командами; перед docker-E2E — статическая
  подготовка (команда в шаге 11).
- Коммит после каждого шага; стиль сообщений как в ветке (`t01: ...`).
- Все юнит-команды выполнять из корня worktree:
  `/Users/demakaev/ZCodeProject/worktrees/feat-t01-basebackup-streaming-client`.

## Review Focus (режимы, не покрытые сценариями шагов — каждый закрыт тестом в шаге-владельце)

1. **Битый/обрезанный tar** (checksum заголовка не сходится / EOF в данных) —
   `TarStreamReader` обязан бросать `TarFormatException`, а не молчать (тест
   в шаге 1).
2. **Имя >100 байт (GNU longname) и размер >8 ГиБ (PAX)** — roundtrip
   writer→reader обязан сохранять имя и размер (тесты в шагах 1–2).
3. **Обрыв соединения посреди BASE_BACKUP** (EOF из сокета) — операция
   завершается `failed` с однострочной причиной, multipart-сессия получает
   Abort (тест в шаге 8).
4. **ErrorResponse сервера на BASE_BACKUP** (SQLState, например версия) —
   `BaseBackupCommand` бросает исключение с SqlState и текстом, набор не
   создаётся (тест в шаге 4).
5. **Объект меньше запрошенного хвоста манифеста** (малый набор < 8 МиБ) —
   verify запрашивает Range `[max(0, size−tail), size−1]`, не выходит за
   границы (тест в шаге 14).

---

## Сводка интерфейсов (вводится шагами 1–7; потребители — шаги 8+)

```csharp
// src/PgWorker.Backups/Tar/TarEntry.cs (шаг 1; размещение в PgWorker.Backups
// — см. примечание в конце плана: tar-код нужен обоим проектам, ссылка
// PgNode→Backups уже существует, обратной нет)
public sealed record TarEntry(string Name, char TypeFlag, long Size, int Mode, string LinkName);
public static class TarTypeFlags
{
    public const char File = '0';
    public const char LegacyFile = '\0';
    public const char Dir = '5';
    public const char Symlink = '2';
    public const char GnuLongName = 'L';
    public const char PaxHeader = 'x';
}

// TarStreamReader(Stream source): шаг 1
public sealed class TarStreamReader
{
    public TarStreamReader(Stream source);
    public Task<TarEntry?> ReadHeaderAsync(CancellationToken ct); // null = конец (нулевой заголовок)
    public Stream OpenData();          // чтение Size байт; Dispose дочитывает padding до 512
    public Task<byte[]> ReadAllDataAsync(CancellationToken ct);   // мелкие записи целиком
}
public sealed class TarFormatException(string message) : Exception(message);

// TarStreamWriter(Stream output): шаг 2
public sealed class TarStreamWriter
{
    public TarStreamWriter(Stream output);
    public long Position { get; }      // байт записано в выход (для карты смещений дубля)
    public long TotalBytes { get; }    // синоним Position
    public Task WriteEntryAsync(TarEntry entry, Stream data, CancellationToken ct);
    public Task WriteEntryAsync(TarEntry entry, byte[] data, CancellationToken ct);
    public Task FinishAsync(CancellationToken ct); // 1024 нулевых байта (2 записи)
    public byte[] Sha256Hash();        // после FinishAsync: сквозной sha256 выходного tar
}

// src/PgWorker.PgNode/BaseBackup/PgMessage.cs (шаг 3)
public sealed record PgMessage(byte Type, byte[] Payload);

// src/PgWorker.PgNode/BaseBackup/PgWireReplicationClient.cs (шаг 3)
public interface IBaseBackupSource : IAsyncDisposable
{
    Task SendAsync(string command, CancellationToken ct);
    Task<PgMessage?> ReadAsync(CancellationToken ct);   // null = EOF
}
public sealed class PgWireReplicationClient : IBaseBackupSource
{
    public PgWireReplicationClient(string host, int port, string user, string password);
    public Task ConnectAsync(CancellationToken ct);
    public Task SendAsync(string command, CancellationToken ct);      // simple-query 'Q'
    public Task<PgMessage?> ReadAsync(CancellationToken ct);
}

// src/PgWorker.PgNode/BaseBackup/BaseBackupCommand.cs (шаг 4)
public sealed class BaseBackupCommand(IBaseBackupSource source)
{
    // tarSink(copyStreamIndex, stream): 0 = основной tablespace, 1 = pg_wal (WAL STREAM)
    public Task RunAsync(string backupId, Func<int, Stream, CancellationToken, Task> tarSink,
        CancellationToken ct);
}
public sealed class BaseBackupErrorException(string sqlState, string message)
    : Exception($"[{sqlState}] {message}");

// src/PgWorker.Backups/MultipartS3.cs (шаг 6)
public interface IMultipartUploadSession
{
    string UploadId { get; }
    Task<Result<string>> UploadPartAsync(int partNumber, byte[] data, CancellationToken ct); // Result = ETag
    Task<Result<string>> CompleteAsync(IReadOnlyDictionary<int, string> partETags, CancellationToken ct);
    Task<Result> AbortAsync(CancellationToken ct);
}
public sealed record MultipartUploadInfo(string Key, string UploadId, DateTimeOffset Initiated);
public interface IMultipartS3
{
    Task<Result<IMultipartUploadSession>> CreateMultipartUploadAsync(string key, CancellationToken ct);
    Task<Result<IReadOnlyList<MultipartUploadInfo>>> ListMultipartUploadsAsync(string prefix, CancellationToken ct);
    Task<Result> AbortMultipartUploadAsync(string key, string uploadId, CancellationToken ct);
}
public interface IStreamObjectS3
{
    Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(string cluster, string shard, string key, CancellationToken ct);
    Task<Result<byte[]>> GetObjectRangeAsync(string cluster, string shard, string key, long start, long end, CancellationToken ct); // тело диапазона [start, end]; 404 → Failed(": not found"); полный размер объекта не нужен — он из HeadObjectAsync (spec §6.23)
    Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(string cluster, string shard, string key, CancellationToken ct); // NotFound → Success((false, 0)); размер — из GetObjectMetadataAsync.ContentLength (spec §6.23)
}
public interface IDeleteObjectS3
{
    Task<Result> DeleteObjectAsync(string cluster, string shard, string key, CancellationToken ct);
}

// src/PgWorker.PgNode/BaseBackup/MultipartPartBufferStream.cs (шаг 7)
public sealed class MultipartPartBufferStream : Stream   // цель TarStreamWriter
{
    public MultipartPartBufferStream(IMultipartUploadSession session, int partSizeBytes = 16 * 1024 * 1024);
    public long BytesDone { get; }   // прочитано байт (прогресс)
    public long PartsDone { get; }   // залито частей (прогресс)
    public Task<Result<string>> CompleteAsync(CancellationToken ct); // flush хвоста + Complete (ETag объекта)
    public Task AbortAsync();        // best-effort abort (ошибки глотаются)
}

// src/PgWorker.PgNode/BaseBackup/BackupManifest.cs (шаг 14)
public sealed record ManifestFile(string Path, long Size, string Sha256Hex);
public static class BackupManifest
{
    public static IReadOnlyDictionary<string, ManifestFile> ParseFiles(string manifestJson); // throw JsonException при битом
}

// src/PgWorker.Backups/FullTarSet.cs (шаг 18)
public static class FullTarSet
{
    public static async Task<Result<string?>> ReadBackupLabelAsync(
        IStreamObjectS3 s3, string cluster, string shard, string backupId, CancellationToken ct);
}
```

`Result`/`Result<T>` — `Shared.Core` (уже используется во всём коде).
`key` во всех новых методах — относительный (`"full/<id>.tar"`), кроме
`IMultipartS3` (полный ключ `<C>/<X>/full/<id>.tar` — симметрия `PutFileAsync`).

---

### Шаг 1. TarStreamReader + TarEntry (TDD)

**Вход:** ветка чистая; spec §3.2 (reader), §6.15 (ustar+longname+PAX, типы 0/5/2).

**Действие:** создать `src/PgWorker.Backups/Tar/TarEntry.cs`
(`TarEntry`, `TarTypeFlags`, `TarFormatException`) и
`src/PgWorker.Backups/Tar/TarStreamReader.cs` (namespace `PgWorker.Backups.Tar`
— placement-решение см. в конце плана). Reader: заголовок
512 байт; поля ustar: name[0:100], mode[100:108], size[124:136] (octal,
может быть пробел-оканчивающимся), typeflag[156], linkname[157:257],
magic[257:263] (`ustar`), prefix[345:500]. Порядок разрешения имени:
PAX-запись (`x`) с полем `path=` переопределяет имя СЛЕДУЮЩЕЙ записи (и
`size=` — её размер); GNU longname (`L`) — имя следующей записи из данных;
иначе `prefix + "/" + name`, иначе `name`. Checksum: сумма байт заголовка с
полем checksum[148:156]=пробелы сравнивается с octal значением поля —
несовпадение → `TarFormatException`. typeflag не из
{`0`,`\0`,`5`,`2`,`L`,`x`} → `TarFormatException` (hardlink и прочее —
ошибка, spec §6.15). Нулевой заголовок (все нули) → `ReadHeaderAsync`
возвращает null. `OpenData()` — Stream поверх Size байт; Dispose дочитывает
padding до кратности 512. Все поля — `Nullable`-чисто, `Encoding.ASCII`/
`TrimEnd('\0',' ')`.

**Выход:** reader читает эталонные tar (в т.ч. произведённые `pg_basebackup
-Ft` и GNU tar) с валидацией.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~TarStreamTests" --nologo
```
Ожидание: PASS (тесты: короткое имя-файл; имя с prefix; longname>100;
PAX-запись path/size; каталог typeflag 5; symlink 2 + linkname; нулевой
конец → null; битый checksum → TarFormatException; чужой typeflag ('1')
→ TarFormatException; обрыв данных (EOF раньше Size) →
TarFormatException при дочитывании; padding пропускается).

**Связь со spec:** §3.2 TarStreamReader; §6.15; Review Focus 1–2.

**Подпункты:**

1.1. Создай файл `src/PgWorker.Backups/Tar/TarEntry.cs` с точным
содержимым (идентификаторы английские, комментарий русский — приведи его как
здесь):

```csharp
namespace PgWorker.Backups.Tar;

/// <summary>Запись tar-архива (ustar + GNU longname + PAX): заголовочные
/// поля, значимые для перепаковщика/верификатора. Size — размер данных
/// записи; для PAX/longname-служебных записей читатель применяет их к
/// СЛЕДУЮЩЕЙ записи и наружу не отдаёт.</summary>
public sealed record TarEntry(string Name, char TypeFlag, long Size, int Mode, string LinkName);

/// <summary>Поддерживаемые typeflag tar-записей (spec t01 §6.15): файл,
/// каталог, symlink, служебные GNU longname и PAX. Прочие — ошибка формата
/// (hardlink'ов в PGDATA нет — fail-fast вместо эмуляции).</summary>
public static class TarTypeFlags
{
    public const char File = '0';
    public const char LegacyFile = '\0';
    public const char Dir = '5';
    public const char Symlink = '2';
    public const char GnuLongName = 'L';
    public const char PaxHeader = 'x';
}

/// <summary>Нарушение формата tar (битый checksum, чужой typeflag, обрыв
/// данных): поток не является валидным tar — читать дальше нельзя.</summary>
public sealed class TarFormatException(string message) : Exception(message);
```

1.2. Создай файл `src/PgWorker.Backups/Tar/TarStreamReader.cs`:
класс `public sealed class TarStreamReader` с конструктором
`public TarStreamReader(Stream source)` и методами по «Сводке интерфейсов».
Логика:
- `private static int ParseOctal(ReadOnlySpan<char> s)` — октальная строка
  tar (пробелы/NUL на краях отбрасываются; пустая → 0).
- `ReadHeaderAsync`: читать ровно 512 байт (`ReadExactlyAsync`; IOException
  на обрыве — допустимо наружу); все нули → null; валидация checksum
  (поле checksum — 6 октальных цифр + NUL+пробел; при подсчёте байты
  [148..156) считаются пробелами); `TarEntry` с разрешением имени
  (longname/PAX: прочитать данные записи через `ReadAllDataAsync`, применить
  к следующей итерации — цикл внутри одного вызова `ReadHeaderAsync`
  возвращает ЗАПИСЬ-ПОТРЕБИТЕЛЯ, служебные наружу не видны; PAX-поля —
  строки `<len> <key>=<value>\n`, парсить `path=` и `size=`).
- `OpenData()`: внутренний класс `EntryStream : Stream` — читает из source
  не более Size байт; `Dispose` дочитывает `padding = (512 - Size % 512) % 512`.
- `ReadAllDataAsync`: `var data = new byte[entry.Size]` после чтения
  заголовка; вычитать, пропустить padding (короткий путь без OpenData).
Комментарии — русские, кратко.

1.3. Создай тест `src/tests/PgWorker.UnitTests/Backups/TarStreamTests.cs`
(xunit; `Xunit` приходит в тест-файлы из `<Using Include="Xunit"/>` в
`src/tests/PgWorker.UnitTests/PgWorker.UnitTests.csproj`, `FluentAssertions`
и `PgWorker.Backups` — из `GlobalUsings.cs`; стиль — соседний
`BackupLabelTests.cs`): билдер тестового tar прямо в
тесте (массив байт: 512-заголовок + данные + padding): заголовок собирай
вспомогательным приватным методом `static byte[] Header(string name, char
type, long size, string magic = "ustar\0", string prefix = "", string
linkname = "")` с корректным checksum (заполни поле пробелами, посчитай
сумму байт, впиши 6 октальных цифр + NUL + пробел). Тест-кейсы — все из
«Проверки», каждый с AAA-комментарием (`// Arrange`, `// Act`, `// Assert`).
Имя класса тестов `TarStreamTests`, файл в namespace
`PgWorker.UnitTests.Backups`.

---

### Шаг 2. TarStreamWriter (TDD)

**Вход:** шаг 1 слит.

**Действие:** создать
`src/PgWorker.Backups/Tar/TarStreamWriter.cs` (namespace `PgWorker.Backups.Tar`).
Заголовок ustar
(magic `ustar\0`, version `00`); имя ≤100 → name-поле; 100<длина≤255 →
prefix/name-разбиение; длиннее — GNU longname-запись (`L`, имя в данных)
перед основной (основная name — урезанное до 99 + '/'). Size > 8 ГиБ−1 →
PAX-запись (`x`, поле `size=` с длиной-префиксом) перед основной, в основной
size-поле 0. Все записи: 512-байт заголовок + данные + padding до 512.
`Position` — счётчик записанных байт (для карты смещений записей pg_wal).
`FinishAsync` — 1024 нулевых байта. `Sha256Hash()` —
`IncrementalSHA256`/`SHA256.HashData` нельзя (поток) → держать
`System.Security.Cryptography.IncrementalHash`
(`HashAlgorithmName.SHA256`, `AppendData` на каждый записанный буфер);
`Sha256Hash()` возвращает `GetHashAndReset()` — вызывать строго после
`FinishAsync`.

**Выход:** writer производит tar, читаемый шагом 1 (roundtrip) и GNU tar.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~TarWriterTests|FullyQualifiedName~TarStreamTests" --nologo
```
Ожидание: PASS (TarWriterTests: roundtrip короткое имя; longname>100
восстанавливается reader'ом; PAX size >8 ГиБ — Size читается верно без
выделения 8 ГиБ (данные-заглушка не пишем: проверка на записи-КАТАЛОГЕ с
чужим size недопустима — вместо этого юнит на HeaderSizeOverride: пишет
PAX-запись и основную с size-полем 0; reader возвращает size из PAX;
полную >8ГиБ запись гонять не нужно); FinishAsync = 2 нулевых блока;
Position = фактическая длина MemoryStream; Sha256Hash = SHA256 всего
потока (сверка с `SHA256.HashData(ms.ToArray())`).

**Связь со spec:** §3.2 TarStreamWriter; §6.15; Review Focus 2.

**Подпункты:**

2.1. Создай `src/PgWorker.Backups/Tar/TarStreamWriter.cs`:
`public sealed class TarStreamWriter` по «Сводке интерфейсов» (ctor принимает
`Stream output`; namespace `PgWorker.Backups.Tar`). Заголовок собирай `private static byte[] BuildHeader(
string name, char type, long size, int mode, string linkName)` — тот же
layоut, что в тесте шага 1 (поля: name[100], mode[8] octal, uid/gid[16]=0,
size[12] octal (0 при PAX-переопределении), mtime[12]=0, checksum[8],
typeflag[1], linkname[100], magic "ustar\0", version "00", uname/gname[64]=0,
devmajor/devminor[16]=0, prefix[155]). Порядок `WriteEntryAsync(TarEntry,
Stream data, ct)`: при необходимости служебная запись (longname/PAX) →
основной заголовок → копия данных кусками 64 КиБ (и в `output`, и в
`IncrementalHash`, и в `Position`) → padding нулями. `mode` — octal из
`entry.Mode`. Комментарии русские.

2.2. Допиши тесты: новый файл
`src/tests/PgWorker.UnitTests/Backups/TarWriterTests.cs` (namespace
`PgWorker.UnitTests.Backups`): кейсы из «Проверки», AAA-комментарии.

---

### Шаг 3. PgWireReplicationClient (wire-протокол + SASL) (TDD)

**Вход:** шаги 1–2 слиты.

**Действие:** создать `src/PgWorker.PgNode/BaseBackup/PgMessage.cs` и
`src/PgWorker.PgNode/BaseBackup/PgWireReplicationClient.cs`. Протокол:
фрейм = 1 байт тип + 4 байта длина (включая себя, network order) + payload.
`ConnectAsync`: `TcpClient` (socket `SocketTcpKeepalive` —
`Socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true)`),
отправить SSLRequest (int32 8, int32 80877103) → прочитать ответ 'S' →
обернуть `SslStream(NetworkStream, leaveInnerStream:false,
validateTarget: (_,_,_) => true)` + `AuthenticateAsClientAsync(host)` →
StartupMessage: payload int32 196608 (3.0) + нуль-терминированные пары
ключ/значение `user`, `database=postgres`, `replication=database` → цикл
аутентификации по сообщениям 'R': код 10/11/12 (SASL) — механизм
`SCRAM-SHA-256` (выбрать из списка механизмов; `SCRAM-SHA-256-PLUS`/другие →
`ApplicationException("unsupported auth: SCRAM mechanism <name>")`); код 3 —
cleartext ('p' + пароль); коды 5 (md5) и прочие → `ApplicationException(
"unsupported auth: <код>")`. SCRAM: client-first `n,,n=,r=<hex-nonce(18
случайных байт)>`; server-first `r=...,s=<base64 salt>,i=<iter>`; salted =
`Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.Normalize(NormalizationForm.
FormKC, password), salt, i, HashAlgorithmName.SHA256, 32)`; clientKey =
HMACSHA256(salted, "Client Key"); storedKey = SHA256(clientKey);
clientFinalWithoutProof = `c=biws,r=<r>`; authMessage = clientFirstBare +
"," + serverFirst + "," + clientFinalWithoutProof (clientFirstBare = без
"n,,"); proof = clientKey XOR HMACSHA256(storedKey, authMessage); отправить
'p' с `clientFinalWithoutProof + ",p=" + base64(proof)`; server-final `v=`
— сверить с base64(HMACSHA256(serverKey = HMACSHA256(salted, "Server
Key"), authMessage)) — несовпадение → исключение. После аутентификации
читать до 'Z' (ReadyForQuery), игнорируя 'S'/'K'/'N'. `SendAsync` — 'Q' +
command + NUL. `ReadAsync` — один фрейм или null при EOF (чистое чтение 0
байт). ВСЕ поля кодов сообщений — константы приватные.

**Выход:** клиент подключается к Spilo-18 под backup_exec и обменивается
simple-query.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~PgWireTests" --nologo
```
Ожидание: PASS (юниты без сети: фрейм-кодек roundtrip по фейк-парам
Stream (MemoryStream пара); SCRAM: эталонный вектор — прогнать алгоритм на
фиксированных nonce/salt/iter/password и сверить proof/serverSignature с
предвычисленными в тесте через тот же BCL — тест фиксирует порядок
склейки authMessage и XOR; unsupported auth: код 5 → исключение с текстом
`unsupported auth: 5`; cleartext: фейк-сервер шлёт R(3) → клиент отвечает
'p' с паролем → 'Z' → Connect завершён).

**Связь со spec:** §3.1 PgWireReplicationClient; §6.2–6.3, §6.21 (keepalive,
без per-read таймаута — таймаут на ConnectAsync не ставить вовсе, бюджет —
воркер-супервиз).

**Подпункты:**

3.1. Создай `src/PgWorker.PgNode/BaseBackup/PgMessage.cs`:
```csharp
namespace PgWorker.PgNode.BaseBackup;

/// <summary>Одно сообщение wire-протокола PG v3: тип-байт + payload
/// (без заголовка длины — кодек пишет/читает его сам).</summary>
public sealed record PgMessage(byte Type, byte[] Payload);
```

3.2. Создай `src/PgWorker.PgNode/BaseBackup/PgWireReplicationClient.cs` по
«Действие» и «Сводке интерфейсов»: `public sealed class PgWireReplicationClient
: IBaseBackupSource`; интерфейс `IBaseBackupSource` — в ЭТОМ ЖЕ файле над
классом (запись как в сводке). Кодек фреймов — приватные
`static async Task WriteFrameAsync(Stream s, byte type, ReadOnlyMemory<byte>
payload, CancellationToken ct)` и `static async Task<PgMessage?>
ReadFrameAsync(Stream s, CancellationToken ct)` (длина из заголовка
проверяется ≥4 и ≤64 МиБ — защита от мусора). SCRAM — вложенный приватный
статический класс `ScramSha256` c методом `static (string ClientFirst,
Func<string, (string ClientFinal, Func<string, byte[]> VerifyServer)>)
Begin(string username, string password)` ИЛИ эквивалентно простой
последовательной реализацией в `HandleAuthLoopAsync` — на твой выбор по
читаемости, тестам важен результат. Комментарии русские.

3.3. Создай `src/tests/PgWorker.UnitTests/PgNode/PgWireTests.cs`: фрейм-кодек
(пара MemoryStream; write→read roundtrip; мусорная длина 3 → исключение);
SCRAM-вектор (собери в тесте ожидания теми же BCL-вызовами из
фиксированных строк — фиксирует порядок склейки и XOR, а не «повторение
кода» — ожидания вычисли в тесте руками по шагам RFC 5802 с ЖЁСТКО
прописанными промежуточными строками client-first/server-first);
cleartext-цикл (фейк-«сервер»: заранее записанная в MemoryStream
последовательность байт R(3) и затем чтение ответа клиента; вторая пара —
'Z'); unsupported 5 → исключение. AAA.

---

### Шаг 4. BaseBackupCommand — автомат разбора (TDD)

**Вход:** шаг 3 слит.

**Действие:** создать `src/PgWorker.PgNode/BaseBackup/BaseBackupCommand.cs`.
`RunAsync(backupId, tarSink, ct)`: `SendAsync($"BASE_BACKUP ( LABEL
'pgw-full-{backupId}', CHECKPOINT 'fast', WAIT false, MANIFEST_CHECKSUMS
'SHA256', TARGET 'CLIENT', WAL 'STREAM' )")` → цикл сообщений: DataRow('D')
фазы заголовка — считать строки (tablespace list); CopyOutResponse('H') →
счётчик потоков ++ (1-й — base, 2-й — pg_wal; 3-й и далее → `ApplicationException(
"multiple tablespaces not supported")`; также если DataRow-строк заголовка
было >1 ДО первого 'H' → то же исключение) → вызвать `tarSink(index, new
CopyOutDataStream(this, ct))`, где `CopyOutDataStream : Stream` читает
подряд CopyData('d')-payload'ы текущего потока до CopyDone('c') (чтение
за CopyDone → EOF потока; CopyFail('f') → исключение с текстом payload).
После потоков: DataRow'ы итога пропускаются, CommandComplete('C') →
возврат; ReadyForQuery('Z') — опционально прочитать если есть (не ждать
его). ErrorResponse('E') → разобрать поля (байт-код: 'C' sqlstate, 'M'
message) → `BaseBackupErrorException(sqlState, message)`. NoticeResponse
('N')/ParameterStatus('S')/RowDescription('T') — пропуск. Любое иное
сообщение → `ApplicationException($"unexpected message '{(char)type}' in
BASE_BACKUP response")`.

**Выход:** автомат читает эталонные последовательности живого PG
(валидируется шагом 5 на стенде).

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BaseBackupCommandTests" --nologo
```
Ожидание: PASS (фейк `IBaseBackupSource` с запрограммированной
последовательностью: T,D,H,d("abc"),d("def"),c,H,d("wal"),c,D,C → tarSink
вызван дважды, 1-й поток читается как "abcdef", 2-й "wal"; >1 DataRow
заголовка → multiple tablespaces; 3-й 'H' → то же; ErrorResponse E
(C=XX000,M=boom) → BaseBackupErrorException с "[XX000] boom"; CopyFail →
исключение; неожидаемый тип 'I' → исключение).

**Связь со spec:** §3.1 BaseBackupCommand; §6.14 (tablespace fail-fast),
§6.20 (TARGET SERVER/INCREMENTAL не поддерживаются — ErrorResponse сервера
покрывает: сервер ответит ошибкой → исключение).

**Подпункты:**

4.1. Создай `src/PgWorker.PgNode/BaseBackup/BaseBackupCommand.cs` по
«Действие» и «Сводке интерфейсов»: `public sealed class
BaseBackupCommand(IBaseBackupSource source)`; вложенный приватный
`sealed class CopyOutDataStream : Stream` (ReadAsync тянет следующий 'd',
позиция внутри payload; Dispose ничего не делает — владение потоком
сообщений у команды). `BaseBackupErrorException` — отдельный record-class
в этом файле. Комментарии русские.

4.2. Создай `src/tests/PgWorker.UnitTests/PgNode/BaseBackupCommandTests.cs`:
фейк `IBaseBackupSource` (очередь `Queue<PgMessage?>` + запись отправленных
команд) — кейсы из «Проверки»; ассерт отправленной команды содержит
`TARGET 'CLIENT'` и `WAL 'STREAM'` и `LABEL 'pgw-full-<id>'`. AAA.

---

### Шаг 5. Прототип: сверка с pg_basebackup на живом PG (сильная модель, без подпунктов)

**Вход:** шаги 1–4 зелены; dev-стенд доступен
(`dev-stand/adminpanel/checks/00-up.sh`; etcd + PG-шард поднят).

**Действие (сильная модель, всё вручную):**
1. Напиши временный инструмент ВНЕ репозитория (`/tmp/t01proto/`): маленькая
   консольная программа, подключающая `PgWorker.PgNode.csproj` через
   `dotnet run` (ProjectReference) — `PgWireReplicationClient` →
   `BaseBackupCommand` → `TarStreamWriter` в файл `/tmp/t01proto/own.tar`.
   Пароль backup_exec возьми из etcd стенда (`/clusters/<C>/backup_password`).
2. Сними эталон: `docker exec <нода> pg_basebackup -h 127.0.0.1 -p 5432 -U
   backup_exec -D /tmp/ref -Ft -X stream --checkpoint=fast
   --manifest-checksums=SHA256` (PGPASSWORD в env); забери base.tar+
   pg_wal.tar.
3. Сравни: распакуй оба (GNU tar), сравни ПЕРЕЧЕНЬ файлов (`diff <(cd a &&
   find . -type f | sort) ...`), размеры и sha256 каждого файла; отдельно
   сверь содержимое `backup_label` и `backup_manifest`.
4. Отчёт о сверке (полные команды/вывод) — в файл
   `docs/superpowers/2026-11-05-t01-basebackup-streaming-client/proto-report.md`
   (это рабочий артефакт фазы, не «отчёт-сам по себе»: требование критериев
   приёмки spec §7 «побайтовая сверка зафиксирована»). Расхождения
    PG-полей mtime/uid/gid в заголовках НЕ чинить (наш writer пишет
   нули — они не влияют на consumer'ов; зафиксировать в отчёте).
5. Инструмент `/tmp/t01proto/` НЕ коммитить.

**Выход:** прототип подтверждён: наш tar эквивалентен набору pg_basebackup
-Ft по перечню/размерам/sha256 всех файлов.

**Проверка:** отчёт содержит таблицу «файл/размер/sha256 — совпало» без
расхождений по содержимому; при расхождении — остановиться и разобраться
(правки в шаги 1–4), не продолжать фазу 2.

**Связь со spec:** §4 фаза 1, §7 (критерий прототипа).

---

### Шаг 6. Multipart/Range/Head/Stream в BackupS3 (без юнитов на сеть — фейков нет, покрытие шагом 7+интеграция)

**Вход:** шаги 1–4 слиты; фаза 1 (прототип) подтверждена.

**Действие:** создать `src/PgWorker.Backups/MultipartS3.cs` (интерфейсы
`IMultipartUploadSession`, `MultipartUploadInfo`, `IMultipartS3`,
`IStreamObjectS3`, `IDeleteObjectS3` — точный код из «Сводки интерфейсов»,
русские doc-комментарии) и расширить `src/PgWorker.Backups/BackupS3.cs`:
класс `BackupS3` получает реализацию `IMultipartS3, IStreamObjectS3,
IDeleteObjectS3`:
- `CreateMultipartUploadAsync(key)` → `_client.InitiateMultipartUploadAsync`
  (uploadId); `UploadPartAsync` → `UploadPartRequest { PartNumber, InputStream
  = new MemoryStream(data), MD5Digest = Convert.ToBase64String(MD5.HashData(
  data)) }` → Result(ETag ответа, `Trim('"')`); `CompleteAsync` →
  `CompleteMultipartUploadRequest { PartETags = partETags.Select(kv => new
  PartETag(kv.Key, kv.Value)).ToList() }` → Result(ETag объекта); сверить
  `response.PartETags.Count == partETags.Count` — не сошлось → Failed.
  `AbortAsync` → `AbortMultipartUploadRequest`. Сессия — вложенный класс
  `MultipartSession(string uploadId) : IMultipartUploadSession`.
- `ListMultipartUploadsAsync(prefix)` → `ListMultipartUploadsRequest {
  Prefix }` → `MultipartUploadInfo(u.Key, u.UploadId, u.Initiated)`.
- `AbortMultipartUploadAsync(key, uploadId)` → request.
- `GetObjectStreamAsync` → `GetObjectAsync` → `(response.ResponseStream,
  response.ContentLength)` (стрим диспозить вызывающий).
- `GetObjectRangeAsync` → `GetObjectRequest { ByteRange = new ByteRange(
  $"{start}-{end}") }` → `Result<byte[]>` — тело диапазона `[start, end]`
  (вычитать `ResponseStream` целиком); 404 →
  Failed(": not found") (симметрия существующих GET). Полный размер объекта
  из ответа НЕ берётся (при 206 `ContentLength` — размер диапазона, не
  объекта; полный размер даёт `HeadObjectAsync`, spec §6.23).
- `HeadObjectAsync` → `GetObjectMetadataAsync`; NotFound → `Success((false,
  0L))`; иначе `Success((true, response.ContentLength))` — размер нужен
  verify ДО первого Range-запроса хвоста (spec §6.23; HEAD и так выполняется
  для фиксации endpoint'а — размер бесплатно, без суффиксного Range).
- `DeleteObjectAsync(cluster, shard, key)` → `_client.DeleteObjectAsync`.
Все — try/catch в Result по образцу существующих методов файла.
`PoolBackupS3` НЕ реализует новые интерфейсы (multipart/стрим — только
единичный `BackupS3`; для HEAD/Range/делита пул добавляет собственные
методы в шаге 16/18).

**Выход:** низкоуровневые S3-операции доступны; компиляция чистая.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug --nologo > /tmp/t01-build.log 2>&1; echo "exit=$?"
```
Ожидание: `exit=0`, в логе 0 warnings (TreatWarningsAsErrors).

**Связь со spec:** §3.3; §6.4 (сессия на одном endpoint — обёртка не
перебирает), §6.5 (Content-MD5/ETag).

**Подпункты:**

6.1. Создай `src/PgWorker.Backups/MultipartS3.cs` с интерфейсами ТОЧНО из
«Сводки интерфейсов» (namespace `PgWorker.Backups`; doc-комментарии
русские: назначение каждого — multipart-заливка одного объекта / потоковое
чтение / Range-чтение / HEAD / одиночный delete).

6.2. В `src/PgWorker.Backups/BackupS3.cs` добавь реализацию: класс
`BackupS3` объявляет `: IBackupS3, IWalSegmentGuard, IMultipartS3,
IStreamObjectS3, IDeleteObjectS3, IAsyncDisposable`; методы по «Действие»
(образец try/catch — соседние методы файла; AWSSDK-типы:
`InitiateMultipartUploadRequest/Response`, `UploadPartRequest/Response`,
`CompleteMultipartUploadRequest/Response`, `AbortMultipartUploadRequest`,
`ListMultipartUploadsRequest/Response` (`MultipartUploads` — поле
`Uploads`), `ByteRange`). Вложенный `private sealed class MultipartSession`.
Комментарии русские, кратко.

---

### Шаг 7. MultipartPartBufferStream (TDD)

**Вход:** шаг 6 слит.

**Действие:** создать
`src/PgWorker.PgNode/BaseBackup/MultipartPartBufferStream.cs`: `Stream` поверх
`IMultipartUploadSession`. Внутри: кольцевой буфер части (`byte[
partSizeBytes]`, заполнение `WriteAsync`), при заполнении — часть уходит в
очередь `UploadPartAsync(nextPartNumber++, buffer.Copy)` с ограничением
4 в полёте (`SemaphoreSlim(4)`; очередные части — копия буфера — кладутся в
`Channel<Task<Result<string>>>`/`List<Task>` — простейшее: `List<Task<
Result<string>>>`, await всех в CompleteAsync); промежуточные части —
ровно 16 МиБ (каждая выше S3-минимума 5 МиБ), часть МЕНЬШЕ 5 МиБ
допускается только финальная (остаток буфера, выгружаемый CompleteAsync);
ошибка любой части — запоминается
первой (`Interlocked.CompareExchange`) и все последующие Write/Complete
возвращают её. `BytesDone` — всего записано байт; `PartsDone` — успешно
завершённые `UploadPartAsync` (awaited). `CompleteAsync(ct)`: дождаться всех
частей → flush хвоста (если >0) → `session.CompleteAsync(partETags)` →
вернуть его Result. `AbortAsync()` — best-effort `session.AbortAsync`
(ошибки глотать). Write после ошибки части — кидать сохранённое исключение
как `ApplicationException`. Писать можно ТОЛЬКО последовательно (Stream
без seek; `CanWrite=true`, остальное false).

**Выход:** поток-цель для TarStreamWriter с нарезкой/параллельной заливкой.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~MultipartPartBufferStreamTests" --nologo
```
Ожидание: PASS (фейк-сессия: 40 МиБ через Write → 2 полных части + хвост;
partNumber 1..3; Content-MD5 не проверяем — фейк; отказ 2-й части →
CompleteAsync Failed с сообщением фейка, AbortAsync вызван вручную и
поглотил отказ; BytesDone=40 МиБ+хвост; PartsDone=2; часть <5 МиБ только
последняя — размер хвоста < 16 МиБ).

**Связь со spec:** §3.4 п.2 (части 16 МиБ, ≤4 в полёте, буфер ≤5×16 МиБ),
§6.4–6.5.

**Подпункты:**

7.1. Создай `src/PgWorker.PgNode/BaseBackup/MultipartPartBufferStream.cs`
по «Действие»/«Сводке интерфейсов». Комментарии русские.

7.2. Создай `src/tests/PgWorker.UnitTests/PgNode/
MultipartPartBufferStreamTests.cs`: фейк `IMultipartUploadSession` (словарь
partNumber→data, управляемые отказы). Кейсы из «Проверки», AAA.

---

### Шаг 8. Новая FullBackupOperation (перепись, TDD)

**Вход:** шаги 2–7 слиты; существующий
`src/PgWorker.PgNode/Operations/FullBackupOperation.cs` (pg_basebackup-
версия) прочитан.

**Действие (сильная модель):** переписать `FullBackupOperation` ЦЕЛИКОМОМ
(новый ctor — см. ниже; старый пайплайн staging/pg_basebackup/PGPASSWORD/
DirectorySize/UploadParallelism удалить). Пайплайн (`RunAsync`):
1. Резолв `WalReceiverTargets` (как сейчас: waiting → честный failed),
   bucket-гейт, REST-проба роли → `writeTargets` (фиксация), CA-материализация
   (как сейчас, оставить код).
2. Цикл попыток `foreach endpoint in writeTargets`:
   - `var client = _targetFactory is { } f ? f(endpoint) : new BackupS3(new
     S3ClientOptions(endpoint, bucket, pool.AccessKey, pool.SecretKey,
     CaBundlePath: caBundlePath, MaxErrorRetry: 0), WalReceiverCore.
     FileAttemptTimeout);` — привести к `IMultipartS3`+`IStreamObjectS3`+
     `IBackupS3` (приватный helper `CreateClient(endpoint)`).
   - `CreateMultipartUploadAsync($"{Cluster}/{Shard}/full/{_id}.tar")` →
     отказ → continue (следующий endpoint).
   - `var parts = new MultipartPartBufferStream(session); var tar = new
     TarStreamWriter(parts);` → снимок `running/basebackup`.
   - `var source = _wireFactory is { } wf ? wf("127.0.0.1", targets.Password!)
     : new PgWireReplicationClient("127.0.0.1", 5432, "backup_exec",
     targets.Password!);` → `ConnectAsync` → `new BaseBackupCommand(source).
     RunAsync(_id, CopyStream, token)`; `CopyStream(index, stream, ct)`:
     `var entryReader = new TarStreamReader(stream); while (await
     entryReader.ReadHeaderAsync(ct) is { } e: { var name = index == 0 ?
     e.Name : "pg_wal/" + e.Name; if (index == 0 && e.Name == "backup_label")
     { var label = await reader.ReadAllDataAsync(ct); _walStart =
     BackupLabel.WalStartSegment(Encoding.ASCII.GetString(label)); }
     if (name.startsWith("pg_wal/")) _walEntries.Add((name, tar.Position +
     512, e.Size)); // смещение ДАННЫХ записи
     await tar.WriteEntryAsync(e with { Name = name }, reader.OpenData(),
     ct); }` (для typeflag '5' данных нет — писать заголовок с
     `Stream.Null`/пустым; TarStreamWriter.WriteEntryAsync каталогового
     размера 0 — без данных).
   - После `RunAsync`: `await tar.FinishAsync(ct)` → `parts.CompleteAsync`
     → ETag; снять `tar.Sha256Hash()`, `_sizeBytes = tar.TotalBytes`.
   - Успех → `break`; исключение (транспорт/протокол) → `await parts.
     AbortAsync()` → continue (следующий endpoint). Отдельно ДО общего
     catch: `catch (OperationCanceledException) when (token.
     IsCancellationRequested)` — ПЕРЕД выходом из цикла best-effort
     `await parts.AbortAsync()` (spec §3.4: отмена гасит и
     BASE_BACKUP-соединение, и multipart-сессию — иначе висячая сессия
     живёт до гигиены супервизора), прерывает цикл попыток БЕЗ следующего
     endpoint (отмена — не отказ endpoint'а; деградация отмены в
     s3-pool-exhausted недопустима), наружу — в общий `catch
     (OperationCanceledException)` → `Fail("cancelled")` (п. «Отмена»).
     Превышение лимита
     частей (фактически: `parts.PartsDone` не ограничить заранее — лимит
     ловится отказом сервера; ДОБАВИТЬ проверку в CopyStream: `tar.
     TotalBytes > 10000L * 16MiB` → ApplicationException "backup exceeds
     160 GiB multipart limit").
3. Исчерпание writeTargets → `Fail("s3-pool-exhausted: <первая ошибка>")`.
4. Снимок `uploading/uploading` (+WalStartSegment); IDENTIFY_SYSTEM через
   `_sourceFactory ?? NpgsqlXLogSource` (код взять из СТАРОЙ версии файла —
   строки с `identifyOptions`/`identifySource`; подключение — новый
   `NpgsqlXLogSource(new WalReceiverOptions("127.0.0.1", 5432,
   "backup_exec", targets.Password!, "postgres", targets.Slot!, Cluster,
   Shard, writeTargets, pool.ReadAll, bucket, pool.AccessKey,
   pool.SecretKey))`); отказ → Fail (как раньше).
5. Дубль: `SemaphoreSlim(8)`, для каждой `_walEntries` записи: позиция
   сегмента (`WalFileName.TryParse(FetchWalCommand.NormalizeSegment(name))`;
   null → skip — не-сегментные файлы pg_wal/archive_status и т.п.)
   < активной позиции источника → `GetObjectRangeAsync(cluster, shard,
   "full/<id>.tar", offset, offset + size - 1)` (клиент ПОПЫТКИ — объект
   только что залит на этот endpoint) → put дубля через гвард:
   `client is IWalSegmentGuard guard ? guard.PutWalSegmentAsync(
   $"{Cluster}/{Shard}/wal/{normalized}", data, null, token) : Fail(...)` —
   `PutWalSegmentAsync` живёт в `IWalSegmentGuard`, НЕ в `IBackupS3`
   (образец — старый `FullBackupOperation.cs` ~строка 300); каст
   `(IBackupS3)` не компилируется; `WalArchiveConflict`/отказ → Fail(строка).
   НЕ ниже активного → `Prog("wal duplicate skipped: ...")` (как старый).
6. Итог `succeeded`: снимок `(_id, succeeded, uploading, WalStartSegment:
   _walStart, SizeBytes: _sizeBytes, Sha256: hex, BytesDone: parts.
   BytesDone, PartsDone: parts.PartsDone)`.
Прогресс в фазе basebackup: в CopyStream после каждой записи инкрементально
обновлять `BytesDone` в снимке (через `_registry.Update` — дешёвая замена
ссылки; частота — каждая запись файла) и `Prog($"upload progress: bytes=
{tar.TotalBytes} parts={parts.PartsDone}")` каждые 64 МиБ (`TotalBytes /
(64<<20)` изменился). `CancelAsync` — `_cts.Cancel()` (сокет закроется
OperationCanceledException; finally попытки: Abort уже в обработчиках).
Отмена → `Fail("cancelled")` (как старый catch OperationCanceledException).
finally: dispose source/clients (как старый), CA-файл удалить. Staging-
чисток НЕТ.

Новый ctor (точный):
```csharp
public sealed class FullBackupOperation(
    string id,
    PgNodeIdentity identity,
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    IS3PoolStore pools,
    Func<CancellationToken, Task<WalReceiverRole>> roleProbe,
    OperationRegistry registry,
    Func<string, IBackupS3>? targetFactory = null,          // тестовый шов (endpoint → клиент)
    Func<WalReceiverOptions, IXLogReplicationSource>? sourceFactory = null, // IDENTIFY_SYSTEM шов
    Func<string, string, IBaseBackupSource>? wireFactory = null)  // (host, password) → BASE_BACKUP-источник
    : IReceiverOperation
```

**Выход:** операция снимает полный одним объектом `full/<id>.tar` + дубль
WAL; фазы/снимки канона.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~FullBackupOperationTests" --nologo
```
Ожидание: PASS — тесты ПЕРЕПИСАТЬ под новый ctor: фейк-источник BASE_BACKUP
(`wireFactory` возвращает фейк `IBaseBackupSource`, который выдаёт
запрограммированную последовательность сообщений с двумя COPY-потоками:
первый — записи backup_label+файл+pg_wal/каталог, второй — сегмент
000000010000000000000001) + фейк `IBackupS3`/`IMultipartS3` (существующий
`FakeTargetS3` — private-класс в
`src/tests/PgWorker.UnitTests/Backups/PoolBackupS3Tests.cs` — новых
интерфейсов не реализует и наследоваться нельзя; сделать локальный в
тесте класс `FakeMultipartS3`
реализующий `IBackupS3 + IMultipartS3 + IStreamObjectS3`, собирающий байты
частей в объект и отвечающий на Range из них; для PutWalSegmentAsync —
запись ключей). Кейсы: фазы running/basebackup → uploading → succeeded;
ключ объекта один `full/<id>.tar`; дубль `wal/<строчное имя>` для сегмента
ниже активного (фейк IDENTIFY_SYSTEM через `sourceFactory`); пропуск живого
сегмента; отказ части → попытка на 2-м endpoint (фейк: 1-й endpoint кидает
исключение в UploadPart, 2-й работает) → succeeded; исчерпание 2
endpoints → failed `s3-pool-exhausted`; cancel → failed `cancelled`;
EOF-фейк источника (RST: ReadAsync возвращает null посреди потока) →
попытка 2 → exhausted-failed; >160 ГиБ-гейт — имитировать нельзя без 160
ГиБ — тест на границу НЕ писать (логика — одна строка сравнения).

**Связь со spec:** §3.4 (весь), §6.4 (попытки), §6.6 (дубль Range), §6.11
(фазы), §6.13 (IDENTIFY_SYSTEM), Review Focus 3.

**Подпункты:**

8.1. В `src/PgWorker.PgNode/Operations/FullBackupOperation.cs` ЗАМЕНИ
старый класс новым по «Действие»: удалить все упоминания pg_basebackup/
staging/PGPASSWORD/IProcessRunner/DirectorySize; using: `PgWorker.PgNode.
BaseBackup`, `PgWorker.Backups.Tar`, `System.Text`. Новый ctor —
из «Действие». Тело — по «Действие» п.1–6, комментарии русские (образец
стиля — старый файл). Прогресс-обновление снимка — после каждой tar-записи.

8.2. Перепиши `src/tests/PgWorker.UnitTests/PgNode/
FullBackupOperationTests.cs` под новый ctor: фейк `IBaseBackupSource`
(конструктор последовательности `PgMessage[]`; можно переиспользовать
билдер из `BaseBackupCommandTests`, перенеся его helper в общий внутренний
класс внутри файла тестов) и `FakeMultipartS3` (как в «Проверке»; для
`PutWalSegmentAsync` — запись ключа+data; для `GetObjectRangeAsync` — байты
из собранного объекта; для `CreateMultipartUploadAsync` — сессия в памяти).
Кейсы из «Проверки», AAA. Существующие кейсы старого файла (пароль-в-env,
staging-чистка) УДАЛИТЬ (механики больше нет).

8.3. Компиляция:
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug --nologo > /tmp/t01-s8.log 2>&1; echo "exit=$?"
```
Ожидание `exit=0`. Ошибки компиляции из-за старого вызова фабрик в
`Program.cs`/`FullBackupOperationFactory.cs` — НЕ чинить в этом подпункте
(шаг 9-10); временно: если сборка падает из-за фабрики, правку фабрики
выполнить сейчас минимально (убрать параметры `stagingDir`/`runner` из
`FullBackupOperationFactory` и его вызова в `Program.cs` строки ~127–135,
новый аргумент `wireFactory: null`).

---

### Шаг 9. Прогресс/sha256 — сквозной контракт приёмник→воркер→etcd (TDD)

**Вход:** шаг 8 слит (снимок операции несёт BytesDone/PartsDone/Sha256 —
добавить в `OperationSnapshot` прямо здесь, см. ниже).

**Действие (сильная модель):** пронести новые поля по цепочке:
1. `src/PgWorker.PgNode/Operations/OperationSnapshot.cs`: добавить
   `long? BytesDone = null, long? PartsDone = null, string? Sha256 = null`
   (в конец record).
2. `src/PgWorker.PgNode/Operations/ApiContracts.cs` `FullStatusResponse`:
   добавить `[property: JsonPropertyName("bytes_done")] long? BytesDone =
   null`, `parts_done`, `sha256` (в конец).
3. Маппинг GET-ответа приёмника (`src/PgWorker.PgNode/Program.cs` строит
   `FullStatusResponse` из `OperationSnapshot` в трёх местах: строки ~425,
   ~431 и helper `ActiveFullStatus` ~452) — добавить три поля во все места
   (терминальные снимки — BytesDone/PartsDone/Sha256 из snapshot при
   наличии).
4. `src/PgWorker.Backups/WalReceiverApiClient.cs` `ReceiverStatus`: добавить
   `bytes_done`/`parts_done`/`sha256` (в конец, nullable).
5. `src/PgWorker.Etcd/Parsing/BackupsModel.cs` `FullBackupState`: добавить
   `string? Sha256 = null, long? ProgressBytes = null, long? ProgressParts =
   null` (в конец record, после ChainLife).
6. `src/PgWorker.Etcd/Parsing/BackupsParser.cs` `TryParseFull`: после
   `ReadChainLife(root)` — `Sha256: ReadString(root, "sha256"), ProgressBytes:
   ReadProgressLong(root, "bytes"), ProgressParts: ReadProgressLong(root,
   "parts")`; приватный helper `ReadProgressLong(JsonElement root, string
   field)` — читает объект `progress`, из него long-поле; отсутствие
   объекта/поля → null.
7. `src/PgWorker.Backups/Job/BackupStatusJson.cs`: после `size_bytes`
   писать `sha256` (если задан); `progress` = `{"bytes":B,"parts":P}` если
   ProgressBytes/ProgressParts заданы.
8. `src/PgWorker.Backups/Process/BackupProcess.cs` `SuperviseActiveAsync`:
   - ветка `running`: если `snapshot.BytesDone != active.ProgressBytes ||
     snapshot.PartsDone != active.ProgressParts` → `active with {
     ProgressBytes = snapshot.BytesDone, ProgressParts = snapshot.PartsDone }`
     → put (каждый тик при живом прогрессе — штатная частота тика);
   - ветка `succeeded`: `outcome` добавить `Sha256 = snapshot.Sha256,
     ProgressBytes = null, ProgressParts = null`;
   - ветки `uploading`/`failed`/timeout/vanished: прогресс обнулять
     (`ProgressBytes = null, ProgressParts = null`) в `with`.
9. `BackupProcess` пишет ключ — новых полей в планировании не трогать.

**Выход:** прогресс/sha256 наблюдаемы в etcd; терминальные состояния без
progress (канон 05-etcd-contract).

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupStatusJsonTests|FullyQualifiedName~BackupsParser|FullyQualifiedName~BackupProcessTests|FullyQualifiedName~WalReceiverApiClientTests" --nologo
```
Ожидание: PASS (дополнить тесты: `BackupStatusJsonTests` — сериализация
sha256/progress и их отсутствие; парсер — roundtrip `progress{bytes,parts}`/
sha256; `BackupProcessTests` — по образцу существующих кейсов: running с
изменённым bytes_done → put с progress; succeeded → sha256 записан,
progress отсутствует).

**Связь со spec:** §3.7 (маппинг полей), §6.12 (прогресс), §6.5 (sha256 в
etcd).

**Подпункты:**

9.1. Внеси правки 1–7 (механические поля/JSON) по «Действие» в файлы:
`OperationSnapshot.cs`, `ApiContracts.cs`, `Program.cs` (маппинг GET —
найти `FullStatusResponse(` в `src/PgWorker.PgNode/Program.cs`),
`WalReceiverApiClient.cs`, `BackupsModel.cs`, `BackupsParser.cs` (+
helper `ReadProgressLong`), `BackupStatusJson.cs`. Комментарии русские
(одно-двухстрочные, по образцу соседних полей).

9.2. Внеси правку 8 в `BackupProcess.cs` (ветки SuperviseActiveAsync по
«Действие»).

9.3. Допиши тесты в `src/tests/PgWorker.UnitTests/Backups/
BackupStatusJsonTests.cs` (кейсы sha256/progress есть/нет) и создай
`BackupsParserFullProgressTests.cs` (roundtrip: JSON с sha256+progress →
FullBackupState; без них → null; битый progress-объект → поля null, запись
не отбрасывается). AAA.

---

### Шаг 10. Упразднение staging: конфиг/env воркера

**Вход:** шаг 9 слит.

**Действие (сильная модель):** удалить остатки staging-механики, КРОМЕ
приёмниковой части (`NodePaths.Staging`/`NodeOperationFactory.stagingRoot`
использует СТАРАЯ `VerifyOperation` — их удаляет шаг 14 вместе с переписью
verify; здесь не трогать):
1. `src/PgWorker.Backups/Options.cs`: удалить параметр `string StagingDir =
   "/home/postgres/pgdata/backup-staging",` (строка ~24) и его упоминания в
   конфиг-биндинге (`Program` воркера `PgWorker.App` — найти
   `Staging:Dir`/`StagingDir` по grep; секция конфига уходит).
2. `src/PgWorker.Docker/Drivers/ClusterDriver.cs` строки ~580–584: удалить
   блок `if (!string.IsNullOrWhiteSpace(backupEnv.StagingDir)) env[
   "PGW_BACKUP_STAGING_DIR"] = backupEnv.StagingDir;` и поле `StagingDir`
   из `NodeBackupEnv` (найти record по grep `NodeBackupEnv`; заполнение
   поля в воркере — тоже по grep `StagingDir`).
3. `src/PgWorker.PgNode/Program.cs`: удалить чтение `PGW_BACKUP_STAGING_DIR`
   (строки ~37, ~127) и параметр `stagingDir` из DI-регистрации
   `FullBackupOperationFactory` (полный staging больше не нужен);
   `NodeOperationFactory`-вызов НЕ трогать (его очередь — шаг 14);
   комментарий про env — удалить.
4. `FullBackupOperationFactory.cs` — уже без staging (шаг 8.3); сверить
   финально (параметры: identity, etcd, etcdEndpoints, pools, roleProbe,
   registry).
5. Grep-контроль: `PGW_BACKUP_STAGING|StagingDir` — 0 ссылок в src вне
   тестов; `NodePaths.Staging` остаётся ДО шага 14.

**Выход:** staging-конфиг/env воркера удалены; сборка чистая.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug --nologo > /tmp/t01-s10.log 2>&1; echo "exit=$?"; grep -rn "PGW_BACKUP_STAGING\|StagingDir" src --include='*.cs' | grep -v tests | wc -l
```
Ожидание: `exit=0`, вывод `0`.

**Связь со spec:** §6.17 (упразднение Staging), §6.19 (удаление старых
механик).

**Подпункты:**

10.1. Выполни правки 1–4 по «Действие» в перечисленных файлах (удаления
строк/полей). После каждого файла — не собирать; сборка — в конце
подпункта командой из «Проверка» (запусти её).

---

### Шаг 11. E2E фазы B: полный одним объектом (docker)

**Вход:** шаги 8–10 слиты; юниты зелены; docker-хост свободен.

**Действие (сильная модель):**
1. Grep `full/` по `src/tests/PgWorker.E2eTests/E2E/*.cs` — все прямые
   обращения к пофайловому layout (list `full/<id>/...`, ожидания
   `backup_manifest`-объекта, подсчёты файлов) заменить: объект один
   `full/<id>.tar`; листинг `full/`-префикса возвращает один ключ.
   Сценарии (все — компиляция сейчас; фактические прогоны по фазам):
   `E2eBackupJobsScenarios`, `E2eBackupFailsOnBadS3Scenarios`,
   `E2eBackupPoolScenarios` + его наследники `PoolNotSelectedScenarios`/
   `PoolMasterReplicaSplitScenarios`/`PoolSingleNodeScenarios`/
   `PoolFailoverExhaustionScenarios`/`PoolRoleSwitchScenarios`/
   `PoolReadAnyNodeScenarios` (прогон — п.2 этого шага);
   `E2eBackupVerifyScenarios` (прогон — шаг 19; verify-механика — шаг 14);
   `E2eRestoreScenarios`/`E2eRestoreNewClusterScenarios` (прогон — шаг 13);
   `E2eDrillScenarios`/`E2eRetentionScenarios`/`E2eSupervisorOrphan*`
   (прогон — шаг 19).
2. Прогон серий фазы B: полный одним объектом + отказные (единственное
   E2E-покрытие Abort/retry-семантики multipart — требование 3 roadmap,
   anti-goal spec): `E2eBackupJobsScenarios`,
   `E2eBackupFailsOnBadS3Scenarios` и все Pool-серии (классы-наследники
   `E2eBackupPoolScenarios` — самостоятельные FQN, фильтр по базовому
   имени их НЕ матчит).
3. После серии — зачистка docker-остатков (`docker ps -a | grep pgw`,
   `docker network prune -f`) по правилам AGENTS.md.

**Выход:** E2E: полный бэкап = один объект `full/<id>.tar`; COMPLETED в
etcd; отказные сценарии (пул/недоступный S3) живы.

**Проверка (три отдельные команды, по порядку; перед тестами —
статподготовка; каждая фаза >60с — `[PHASE]`-лог теста уже ведёт фикстура):**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t01-s11-build.log 2>&1; echo "exit=$?"
```
Ожидание `exit=0`. Затем:
```
DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/t01-s11-prepare.log 2>&1; echo "exit=$?"
```
Ожидание `exit=0`, в логе штамп готовности образов. Затем:
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~E2eBackupJobsScenarios|FullyQualifiedName~E2eBackupFailsOnBadS3Scenarios|FullyQualifiedName~PoolNotSelectedScenarios|FullyQualifiedName~PoolMasterReplicaSplitScenarios|FullyQualifiedName~PoolSingleNodeScenarios|FullyQualifiedName~PoolFailoverExhaustionScenarios|FullyQualifiedName~PoolRoleSwitchScenarios|FullyQualifiedName~PoolReadAnyNodeScenarios" > /tmp/t01-s11-e2e.log 2>&1; echo "exit=$?"
```
Ожидание `exit=0`, в `/tmp/t01-s11-e2e.log` только Passed (фильтр покрывает
все 8 классов серий фазы B по фактическим именам — см. «Действие» п.2).
Длинные логи —
разбор из файла, не перезапускать упавшие без анализа логов (AGENTS.md).

**Связь со spec:** §4 фаза 2 (E2E полного одним объектом), §7 (критерий
«list: ровно один объект»).

**Подпункты:**

11.1. Механические замены ассертов в перечисленных E2E-файлах по «Действие»
п.1 (сильная модель указала строки — подпункт получает конкретный список
после её grep'а; здесь — правило замены: `full/<id>/<rel>`-ожидания → один
ключ `full/<id>.tar`; счётчики файлов → счётчик объектов = 1; явные проверки
объекта `backup_manifest` → проверка существования `full/<id>.tar`).
Если конкретных строк меньше 20 — сильная модель делает сама, подпункт не
выделяется.

---

### Шаг 12. RestoreOperation: потоковая распаковка (TDD)

**Вход:** шаги 1, 6, 10 слиты.

**Действие (сильная модель):** в
`src/PgWorker.PgNode/Operations/RestoreOperation.cs` заменить блок фазы
downloading (union-лист + цикл скачивания, строки ~122–169) на:
1. Фиксация endpoint'а: `IBackupS3? client = null; IStreamObjectS3?
   streamClient = null; foreach (var ep in _request.S3.Endpoints) { var c =
   CreateClient(ep); if (await ((IStreamObjectS3)c).HeadObjectAsync(cluster,
   shard, $"full/{_request.BackupId}.tar", token) is { IsSuccess: true,
   Value.Exists: true }) { client = c; streamClient = (IStreamObjectS3)c;
   break; } }`
   — не найден ни на одном → `Fail(phase, $"full/{_request.BackupId}.tar:
   object not found")` (transient-семантика сохранена: failed/downloading).
   `CreateClient(ep)` — приватный helper `new BackupS3(new S3ClientOptions(
   ep, _request.S3.Bucket, _request.S3.AccessKey, _request.S3.SecretKey,
   CaBundlePath: caBundlePath, MaxErrorRetry: 0), WalReceiverCore.
   FileAttemptTimeout)` (шов `_targetFactory` — как раньше: фабрика от
   endpoint).
2. `var (stream, totalSize) = await streamClient.GetObjectStreamAsync(cluster,
   shard, "full/<id>.tar", token)` — отказ → Fail(downloading).
3. Распаковка: `var reader = new TarStreamReader(stream); Directory.
   CreateDirectory(pgdata); while (await reader.ReadHeaderAsync(token) is
   { } e) { var dest = Path.Combine(pgdata, e.Name.Replace('/',
   Path.DirectorySeparatorChar)); switch (e.TypeFlag) { case TarTypeFlags.
   Dir: Directory.CreateDirectory(dest); await reader.SkipDataAsync(token);
   break; case
   TarTypeFlags.Symlink: (существует → File.Delete/Directory.Delete)
   File.CreateSymbolicLink(dest, e.LinkName); await reader.SkipDataAsync(
   token); break;
   default (File/LegacyFile): Directory.CreateDirectory(Path.GetDirectoryName(
   dest)); await using (var fs = new FileStream(dest, FileMode.Create,
   FileAccess.Write, FileShare.None)) await reader.OpenData().CopyToAsync(fs,
   64KiБ, token); if (!OperatingSystem.IsWindows() &&
   Path.GetFullPath(dest).StartsWith(pgdata)) File.SetUnixFileMode(dest,
   (UnixFileMode)(e.Mode & 0x1FF)); break; } }` — `SkipDataAsync` — НОВЫЙ
   метод `TarStreamReader` (подпункт 12.1; для Dir/Symlink Size=0 —
   no-op). Прогресс-лог `[restore]
   extracting ... every 512 МиБ` по счётчику байт. Отказ/битый tar →
   Fail(downloading, однострочно).
4. Проверка `backup_label` после распаковки — оставить (файл теперь из tar).
5. Убрать: `ListPrefixUnionAsync`-вызов, цикл `foreach (var obj in
   listed.Value)`, `[restore] union-list ...`/`downloaded N file(s)` —
   заменить на `[restore] tar stream <size> bytes`/`[restore] extracted
   <bytes> bytes` (хронометраж сохранить). CA-материализация/пул-клиент —
   пул `PoolBackupS3` больше НЕ строить в операции (endpoint фиксирован);
   dispose клиента в finally.
6. Восстановительная механика (auto.conf/recovery.signal/pg_ctl/...) — БЕЗ
   изменений. `_targetFactory`-шов сохранён (фейк на endpoint).

**Выход:** restore/drill распаковывают tar-объект потоково; фазы/семантика
прежние; drill — тот же код (ScratchRoot); юнит-кейсы операции переведены
на tar-объект.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~RestoreOperationTests|FullyQualifiedName~TarStreamTests|FullyQualifiedName~TarWriterTests" --nologo > /tmp/t01-s12.log 2>&1; echo "exit=$?"
```
Ожидание `exit=0`, только Passed: существующий
`src/tests/PgWorker.UnitTests/PgNode/RestoreOperationTests.cs` переписан
(подпункт 12.3) и зелён вместе с tar-тестами. Поведенческое E2E-покрытие —
шаг 13.

**Связь со spec:** §3.6; §6.9 (endpoint фиксирован, транспортный отказ =
transient); §6.22 (каталоги лениво + права); §6.15 (typeflag).

**Подпункты:**

12.1. Добавь метод `public async Task SkipDataAsync(CancellationToken ct)`
в `src/PgWorker.Backups/Tar/TarStreamReader.cs`: дочитать `Size`
байт данных + padding (кусками 64 КиБ в буфер-мусорку); вызвать
запрещено после `OpenData()` той же записи — кидать
`InvalidOperationException`.

12.2. Замени блок downloading в
`src/PgWorker.PgNode/Operations/RestoreOperation.cs` по «Действие» п.1–5
(сильная модель уже выделила строки ~122–169; using добавить
`PgWorker.Backups.Tar`). Комментарии русские.

12.3. ПОЛНОСТЬЮ перепиши `src/tests/PgWorker.UnitTests/PgNode/
RestoreOperationTests.cs` под новую механику скачивания (шаг ломает
существующий файл: его `FakeS3 : IBackupS3` не реализует
`IStreamObjectS3` — каст в операции упал бы `InvalidCastException` во ВСЕХ
кейсах): замени `FakeS3` на локальный `FakeTarS3 : IBackupS3,
IStreamObjectS3` — хранит один tar-объект `full/<backupId>.tar` (байты
собирает через `TarStreamWriter` из списка тестовых записей:
`backup_label` с валидной строкой `START WAL LOCATION: ... (file
000000010000000000000002)` + пара файлов/каталогов `pg_wal/…`);
`HeadObjectAsync` → `Success((true, размер))`; `GetObjectStreamAsync` →
`MemoryStream` объекта; `GetObjectRangeAsync` → срез массива; прочие
методы `IBackupS3` — `throw new NotSupportedException()`. Кейсы
union-листинга/пофайловой загрузки (если были) — удалить; вместо них:
`Downloading_ExtractsTarFiles_IntoPgdata` (после операции в целевом
каталоге есть файлы набора); `TarObjectNotFound_AllEndpointsMiss_Failed
Downloading` (Head=false везде → failed/фаза downloading, error содержит
`not found`); `CorruptTar_FailedDownloading` (объект-мусор без валидных
заголовков → failed/downloading). Существующие кейсы НЕ-скачивательной
механики (`HappyPath_IncidentEditsAndCallOrder_ThenSucceeded`,
`AutoConfOverrides_ContainFullSet_TargetTimeOnlyWhenSet`,
`LatestTarget_NoRecoveryTargetTimeLine`,
`PollRetries_PromoteWindow_BeforeOk`,
`CancelDuringRecoveryPoll_StateCancelled_NotFailed` и остальные) —
СОХРАНИТЬ как есть (FakeRunner-моки auto.conf/pg_ctl не зависят от
механики скачивания), они должны пройти на новом фейке без правок тел —
правится только источник набора. AAA-комментарии сохранить.

---

### Шаг 13. E2E фазы C: restore latest + DR (docker)

**Вход:** шаг 12 слит; зачистка после шага 11 выполнена.

**Действие (сильная модель):** обновить при необходимости ассерты
`E2eRestoreScenarios` (restore latest восстанавливается из `full/<id>.tar`)
и `E2eRestoreNewClusterScenarios` (DR-путь: восстановление в новый кластер
из префикса погибшего — выбор полных теперь по объектам `.tar`; если
сценарий правится в шаге 11 — только прогон). Прогон обеих серий;
зачистка.

**Выход:** restore latest/DR зелые на новом формате.

**Проверка (те же три команды, фильтр `FullyQualifiedName~
E2eRestoreScenarios|FullyQualifiedName~E2eRestoreNewClusterScenarios`;
логи `/tmp/t01-s13-*.log`).** Ожидание: exit=0, Passed.

**Связь со spec:** §4 фаза 3; §7 (restore latest COMPLETED).

---

### Шаг 14. Потоковый verify: BackupManifest + VerifyOperation (TDD)

**Вход:** шаги 1–2, 6 слиты; `pg_verifybackup`-версия `VerifyOperation`
прочитана.

**Действие (сильная модель):**
1. Создать `src/PgWorker.PgNode/BaseBackup/BackupManifest.cs`:
   `ParseFiles(string json)` — `JsonDocument`: корень `Files` — массив
   объектов с `Path`(string), `Size`(long), `Checksum`(string hex-sha256);
   алгоритм `Checksum-Algorithm` ≠ SHA256 — исключение (мы задаём SHA256
   при съёме). Вернуть словарь Path→ManifestFile.
2. Переписать `src/PgWorker.PgNode/Operations/VerifyOperation.cs`:
   - Фаза `downloading`: фиксация endpoint (HEAD-цикл как в шаге 12 —
     `HeadObjectAsync` возвращает и размер: `{ IsSuccess: true,
     Value.Exists: true }` → `long size = Value.SizeBytes`) → хвост:
     `var tail = 8L << 20; for (int attempt = 1; attempt <= 4; attempt++)
     { var start = Math.Max(0, size - tail); var range = await
     streamClient.GetObjectRangeAsync(cluster, shard, key, start, size - 1,
     token); if (!range.IsSuccess) { Fail(downloading, ...); return; }
     if (TryFindManifest(range.Value, start, out var manifestJson))
     break; tail <<= 1; }` — не найден за 4 попытки → Fail(downloading,
     "backup_manifest not found in tail") (transient-семантика:
     транспортный сбой — тоже failed/downloading; различие permanent здесь
     не нужно: отсутствие манифеста у Complete-объекта — битые данные, но
     фазу оставляем downloading/transient — воркер ретраит; вечный цикл
     исключён VerifyTimeoutSec-бюджетом супервизора — семантика прежняя
     «сбой скачивания transient»). Размер объекта — ТОЛЬКО из HEAD
     (спец §6.23: `GetObjectRangeAsync` возвращает байты диапазона,
     полный размер ему не нужен).
   - `TryFindManifest(byte[] data, long rangeStart, out string?)`: сканирование
     буфера от `firstHeader = (rangeStart % 512 == 0 ? rangeStart :
     rangeStart + (512 - rangeStart % 512))` (позиция в объекте; смещение
     в буфере = позиция − rangeStart) с шагом 512: заголовок с валидным
     checksum (переиспользовать разбор шага 1: приватный доступ —
     сделать в `TarStreamReader` internal-метод `static TarEntry?
     TryParseHeaderAt(byte[] buf, int offset)`) и
     именем `backup_manifest` → данные записи в буфере (полностью) → ASCII
     string.
   - Фаза `verifying`: `GetObjectStreamAsync` → `TarStreamReader` →
     для каждой записи: `sha256` по `OpenData()` кусками (IncrementalHash
     на запись); имя/size/hex сверить с манифестом: отсутствует в
     манифесте → Fail(verifying, $"unexpected file {name}"); есть, но
     size/hex не сошлись → Fail(verifying, $"checksum mismatch {name}").
     Собрать множество проверенных; в конце: все записи манифеста покрыты
     (отсутствующий в tar → Fail(verifying, $"missing file {path}")).
     Сквозной sha256: IncrementalHash по всему потоку — сверить с
     `_request.Sha256` если задано (НОВОЕ поле `StartVerifyRequest`:
     `[property: JsonPropertyName("sha256")] string? Sha256 = null`;
     воркер передаёт из etcd-статуса — правка `BackupVerifyProcess`
     шага 15).
   - Служебные записи tar (заголовки longname/PAX) в сверке не участвуют
     (reader их не отдаёт). `pg_verifybackup`/staging/ProcessRunner —
     удалить из операции (ctor: `(StartVerifyRequest request,
     OperationRegistry registry, Func<string, IBackupS3>?
     targetFactory = null)`).
3. `BackupVerifyProcess` (шаг 15) передаёт `sha256` — здесь только принять
   поле в `ApiContracts.StartVerifyRequest`/`WalReceiverApiClient.
   StartVerifyRequest` (одноимённые record'ы двух проектов — добавить
   поле в ОБА).
4. Зачистка staging приёмника (перенесена сюда из фазы B — до переписи
   verify его использовала старая операция): в
   `src/PgWorker.PgNode/Operations/NodePaths.cs` удалить константу
   `Staging` и упоминание staging в doc-комментарии (Pgdata/DrillRoot
   остаются); в `src/PgWorker.PgNode/Operations/NodeOperationFactory.cs`
   убрать параметр `string stagingRoot` и его проброс (новый
   `VerifyOperation`-ctor его не принимает); в `src/PgWorker.PgNode/
   Program.cs` убрать аргумент stagingRoot при создании
   `NodeOperationFactory` (grep `NodeOperationFactory`); grep-контроль
   `NodePaths.Staging` — 0 ссылок.

**Выход:** verify-операция потоковая, двухфазная, без распаковки.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupManifestTests|FullyQualifiedName~VerifyOperationTests" --nologo
```
Ожидание: PASS (`BackupManifestTests` — новый файл: ParseFiles эталонного
JSON-фрагмента манифеста PG (Fixture-строка с 2 файлами), битый JSON →
исключение, Checksum-Algorithm CRC32 → исключение;
`VerifyOperationTests` — СУЩЕСТВУЮЩИЙ файл
(`src/tests/PgWorker.UnitTests/PgNode/VerifyOperationTests.cs`, кейсы
pg_verifybackup/staging с `FakeRunner` и ctor `new VerifyOperation(request,
runner, registry, staging, ...)`) — ПОЛНОСТЬЮ ЗАМЕНЯЕТСЯ (старые кейсы
удаляются): фейк endpoint-клиента `IBackupS3+IStreamObjectS3` с собранным
tar (writer
шага 2 с манифестом в конце): OK-набор → succeeded/verifying; битый байт
файла → failed "checksum mismatch"; лишний файл → failed "unexpected";
отсутствующий → failed "missing"; объект < 8 МиБ → Range запрошен от 0
(ассерт фейка: минимальный start; размер фейк отдаёт из HeadObjectAsync);
транспортный отказ GetObjectRange →
failed/downloading; сквозной sha256 не сошёлся → failed "object sha256
mismatch").

**Связь со spec:** §3.5; §6.7 (двухфазность, хвост/удвоение ≤4); §6.23
(размер из HEAD); Review Focus 5.

**Подпункты (порядок исполнения — по номерам):**

14.1. Создай `src/PgWorker.PgNode/BaseBackup/BackupManifest.cs` по
«Действие» п.1 (namespace `PgWorker.PgNode.BaseBackup`; `ManifestFile`/
`ParseFiles` из сводки; System.Text.Json; doc-комментарии русские).

14.2. Добавь в `src/PgWorker.Backups/Tar/TarStreamReader.cs`:
`internal static TarEntry? TryParseHeaderAt(byte[] buffer, int offset)` —
разбор 512-байт заголовка по смещению с проверкой checksum; null если
checksum не сошёлся или буфер короче. Рефакторинг: основной разбор
`ReadHeaderAsync` вызывает этот метод (одна реализация).

14.3. Перепиши `src/PgWorker.PgNode/Operations/VerifyOperation.cs` по
«Действие» п.2 (новый ctor `(StartVerifyRequest request, OperationRegistry
registry, Func<string, IBackupS3>? targetFactory = null)`; фазы; helpers
`TryFindManifest`/`VerifyPass`; using `PgWorker.Backups.Tar`).
Комментарии русские.

14.4. Добавь `string? Sha256 = null` (`[JsonPropertyName("sha256")]`) в
ОБА record'а `StartVerifyRequest`: `src/PgWorker.PgNode/Operations/
ApiContracts.cs` и `src/PgWorker.Backups/WalReceiverApiClient.cs`.

14.5. Создай файл `src/tests/PgWorker.UnitTests/PgNode/
BackupManifestTests.cs` и ПОЛНОСТЬЮ замени существующий
`src/tests/PgWorker.UnitTests/PgNode/VerifyOperationTests.cs` (старые
кейсы pg_verifybackup/staging/FakeRunner-целиком удалить — новая операция
не запускает процессов и не пишет на диск) по «Проверке»: фейк-клиент
`IBackupS3 + IStreamObjectS3` хранит tar-объект (собери `TarStreamWriter`-ом
в MemoryStream: файлы + `backup_manifest` ПОСЛЕДНЕЙ записью перед
FinishAsync), `HeadObjectAsync` → `Success((true, размер))`; AAA.

14.6. Выполни зачистку «Действие» п.4: в `NodePaths.cs` удали константу
`Staging` (doc-комментарий перепиши без staging); в
`NodeOperationFactory.cs` убери параметр `stagingRoot` (вызов
`CreateVerify` — `new VerifyOperation(request, registry)`); в
`src/PgWorker.PgNode/Program.cs` убери аргумент stagingRoot у создания
`NodeOperationFactory` (найди по grep `NodeOperationFactory`). Затем
запусти команду «Проверки» шага — юниты + компиляция после ВСЕХ
подпунктов.

---

### Шаг 15. BackupVerifyProcess: убрать проверку полноты + передать sha256

**Вход:** шаг 14 слит; `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`
прочитан.

**Действие (сильная модель):**
1. В `TickShardAsync` удалить блок «(3) Цепочка: list wal/ + list набора +
строгие TLI-переходы» ЦЕЛИКОМ: `walList`/`setList`/`start`/`setSegments`/
`endSegment`/`historyContents`/`WalChain.CheckRange` и проверки «no
wal_start_segment — set is not archiveable» + «invalid wal_start_segment»
+ «set contains no pg_wal WAL segments» (строки ~140–230: от `var walList
= ...` до закрытия `check`-блока). Проверка `no wal_start_segment`
(строки ~146–153) — удалить тоже: полнота набора доказывается манифестом
в verify-операции (канон §7). Комментарий перед (4) обновить: «Полнота
набора (вкл. pg_wal) — манифестом в потоковой verify-оперции; дубль wal/ —
общий контроль цепочки (02-wal-archive §3)».
2. `StartVerifyRequest`-вызовы в этом же файле — их ДВА: старт операции
   (~строка 256) и relaunch после GET 404/vanished (~строка 334); добавить
   `Sha256: candidate.Sha256` (поле из шага 9) в ОБА вызова — поле
   опционально, пропуск relaunch-места компилятор не заметит, а
   перезапущенная verify молча потеряет sha256-сверку сквозного хэша.
3. Проверить неиспользуемые using (`WalChain`, `WalHistory` и т.п. —
   компилятор подскажет: TreatWarningsAsErrors).

**Выход:** воркер запускает только потоковую verify-операцию; листингов
`full/<id>/pg_wal/` нет.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug --nologo > /tmp/t01-s15.log 2>&1; echo "exit=$?"
```
Ожидание `exit=0` (удаление кода; поведение — E2E шага 19; юнит-файла у
BackupVerifyProcess нет — grep подтвердил).

**Связь со spec:** §3.7 (упразднение проверки полноты); §6.8.

**Подпункты:**

15.1. Удали блоки по «Действие» п.1–2 в
`src/PgWorker.Backups/Process/BackupVerifyProcess.cs` (сильная модель
пометила диапазон строк ~140–230; после удаления скомпилируй командой из
«Проверка» и добейся exit=0: неиспользуемые using/поля — удалить по
подсказкам компилятора).

---

### Шаг 16. Ретенция: удаление одного объекта; ListFulls по .tar (TDD)

**Вход:** шаг 6 слит; `RetentionProcess`/`BackupS3.ListFullsAsync` прочитаны.

**Действие (сильная модель):**
1. `BackupS3.ListFullsAsync`: заменить Delimiter/CommonPrefixes-разбор на
   обычный list по префиксу `{cluster}/{shard}/full/` БЕЗ delimiter:
   ключ вида `.../full/<id>.tar` → id = имя файла без `.tar`; прочие ключи
   (внутренние, гипотетические) — игнор. Та же пагинация.
2. `RetentionProcess.FinishDeletionAsync` (строки ~212–241): заменить
   list→batch-delete→recheck на: `var deleted = await s3.DeleteObjectAsync(
   cluster, shard, $"full/{deleting.Id}.tar", ct)`; отказ → исключение
   (остаётся DELETING, идемпотентно). Но `s3` здесь — `IBackupS3`
   (PoolBackupS3!) — добавить `IDeleteObjectS3`-реализацию в
   `PoolBackupS3`: write-перебор целей (образец существующего write-метода
   пула — см. `PutFileAsync`-обёртки пула), исчерпание → Failed с маркером
   `s3-pool-exhausted` (образец маркера — `S3PoolMarkers`).
3. Правка вызова в `FinishDeletionAsync`: cast не нужен — параметр
   метода сменить на принимаемый интерфейс (`IBackupS3` остаётся для
   list-частей метода; передавать отдельным аргументом
   `IDeleteObjectS3 deleter` или привести в месте вызова — по образцу
   файла).

**Выход:** ретенция удаляет полный одним DeleteObject; DR-листинг полных —
по `.tar`-объектам.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~RetentionPlannerTests|FullyQualifiedName~PoolBackupS3Tests" --nologo
```
Ожидание: PASS + дописать кейс в `PoolBackupS3Tests` (фейк-цели: DeleteObject
write-перебор: 1-я цель отказала → 2-я удалила → успех; все отказали →
маркер `s3-pool-exhausted`); `ListFullsAsync`-кейс (если фейк-тесты листинга
есть в `PoolBackupS3Tests`/`TestPoolFactory` — обновить под `.tar`-ключи;
нет — добавить через фейк-цель с list-ответом).

**Связь со spec:** §3.7 (RetentionProcess), §6.10 (существование объекта =
завершённость).

**Подпункты:**

16.1. В `src/PgWorker.Backups/BackupS3.cs` перепиши `ListFullsAsync` по
«Действие» п.1 (пагинация та же; имя файла оканчивается `.tar` → id без
суффикса; SortedSet сохранить).

16.2. В `src/PgWorker.Backups/PoolBackupS3.cs` добавь реализацию
`IDeleteObjectS3` (класс объявляет интерфейс; метод — write-перебор целей,
у каждой цели вызывается `(target as IDeleteObjectS3)?.DeleteObjectAsync(...)`;
цель без интерфейса — пропуск с диагностикой в маркер; исчерпание →
`Result.Failed` с маркером исчерпания — сверить константу по файлу
`src/PgWorker.Backups/S3PoolMarkers.cs`; образец write-перебора —
существующие write-методы этого файла).

16.3. В `src/PgWorker.Backups/Retention/RetentionProcess.cs` замени
`FinishDeletionAsync` по «Действие» п.3 (list/recheck-блок удалить;
DeleteObjectAsync; ошибки — ApplicationException как раньше).

16.4. Допиши тесты по «Проверке» в
`src/tests/PgWorker.UnitTests/Backups/PoolBackupS3Tests.cs`: локальный
фейк-цель `FakeDeleteTargetS3 : IBackupS3, IDeleteObjectS3` (тело методов
IBackupS3 — `throw new NotSupportedException()`, кроме используемых листов
если нужны; DeleteObjectAsync — управляемый отказ/успех), передаётся через
`targetFactory`-параметр конструктора `PoolBackupS3` (паттерн существующих
тестов файла). Кейсы: 1-я цель отказала → 2-я удалила → успех; все
отказали → маркер `s3-pool-exhausted` в ошибке. ListFullsAsync-кейс: если
в `PoolBackupS3Tests` (фейк-цель `FakeTargetS3` с list-делегатами) есть
листинг-кейсы — обновить ожидания под `.tar`-ключи; таких нет — не
добавлять (покрывается E2E). AAA.

---

### Шаг 17. Супервизор: мусор-объекты + multipart-гигиена + OrphanRegistry (TDD)

**Вход:** шаг 6, 16 слиты; `BackupSupervisorProcess.SweepShardAsync`,
`SupervisorPlanner`, `OrphanRegistry.GroupPrefixes` прочитаны.

**Действие (сильная модель):**
1. `SweepShardAsync` (строки ~118–180): `ListFullsAsync` уже даёт id `.tar`-
   объектов; гейт 2 «самый старый объект префикса» → возраст КОНКРЕТНОГО
   объекта: заменить list-префикса на `HeadObjectAsync`-подобное получение
   LastModified — проще: листинг `ListPrefixAsync($"{cluster}/{shard}/full/")`
   (уже есть в IBackupS3) фильтром `.tar` → LastModified нужного ключа.
   Удаление: `DeleteObjectAsync(cluster, shard, $"full/{id}.tar")`
   (PoolBackupS3-обёртка из шага 16). recheck-подтверждение: повторный
   list — ключ исчез.
2. NEW-гигиена: в конец `SweepShardAsync`: перебор endpoints =
   ОБЪЕДИНЕНИЕ `WriteMaster ∪ WriteReplica` пула (сессии multipart
   создаёт полный на write-списке, зафиксированном ПО РОЛИ на старте
   операции — супервизор роль снятия не знает, потому обходятся ОБА
   списка; дедуп по endpoint); контекст пула уже есть в вызывающем коде —
   `BackupS3Factory`-резолв (`BackupPoolContext.WriteMaster`/`WriteReplica`;
   сверить по вызывающему коду `BackupSupervisorProcess.TickAsync`):
   для каждого endpoint строить единичный `BackupS3` (helper как в шаге
   8/12) → `ListMultipartUploadsAsync($"{cluster}/{shard}/")` → для каждой
   сессии старше `garbageMinAgeSec` (Initiated) → `AbortMultipartUploadAsync(
   key, uploadId)`; журнал-фаза `aborted-mpu/<shard>/<uploadId-prefix>`;
   транспортный отказ endpoint — log warning, продолжить.
3. `OrphanRegistry.GroupPrefixes`: критерий валидного полного: `parts.
   Length == 4 && parts[2] == "full" && parts[3].EndsWith(".tar",
   StringComparison.Ordinal)` → `fulls.Add(prefix)`.
4. Комментарии «unowned-префикс» → «unowned-объект» в обоих файлах.

**Выход:** мусор-гейты под один объект; висячие multipart-сессии чистятся;
сироты считаются по `.tar`.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~OrphanRegistryTests|FullyQualifiedName~SupervisorPlannerTests" --nologo
```
Ожидание: PASS (обновить `OrphanRegistryTests`: ключ `<C>/<X>/full/<id>.tar`
→ has_valid_full=true; старый 5-сегментный `backup_manifest`-ключ → false
(критерий сменился)); `SupervisorPlannerTests` — сверить: `SelectUnownedFulls`
работает с id без изменений (id-строки те же) — тесты должны остаться
зелеными без правок или с минимальной правкой fixtures.

**Связь со spec:** §3.7 (супервизор); §6.10; канон e-supervisor §1.

**Подпункты:**

17.1. В `src/PgWorker.Backups/Supervisor/OrphanRegistry.cs` замени критерий
в `GroupPrefixes` по «Действие» п.3 (+doc-комментарий: «объект
full/<id>.tar — критерий DR-выбора»).

17.2. В `src/PgWorker.Backups/Supervisor/BackupSupervisorProcess.cs`
перепиши `SweepShardAsync` по «Действие» п.1–2 (+п.4 комментарии). Новые
using — по компилятору.

17.3. Обнови `src/tests/PgWorker.UnitTests/Backups/OrphanRegistryTests.cs`
по «Проверке» (AAA; старые кейсы с 5-сегментным manifest-ключом —
инвертировать ожидание: false).

---

### Шаг 18. Restore/Drill: DR-валидация по объекту + backup_label из tar (TDD)

**Вход:** шаги 6, 14 слиты; `RestoreProcess` (строки ~275–300) и
`RestoreDrillProcess` (строки ~135–155) прочитаны.

**Действие (сильная модель):**
1. Создать `src/PgWorker.Backups/FullTarSet.cs`: `ReadBackupLabelAsync(
   IStreamObjectS3 s3, cluster, shard, backupId, ct)` → `GetObjectRangeAsync(
   cluster, shard, $"full/{backupId}.tar", 0, 64 * 1024 - 1, ct)` → отказ →
   Result<string?>.Failed(ошибка); успех → `TarStreamReader` над
   MemoryStream(data) → первая запись: имя `backup_label` → `ReadAllData` →
   ASCII-строка; запись не та/нет → Success(null). Doc: «backup_label —
   первая запись tar-объекта полного; 64 КиБ покрывают заголовок+содержимое
   с запасом».
2. `PoolBackupS3`: добавить реализацию `IStreamObjectS3` (класс объявляет
   интерфейс): `HeadObjectAsync` — read-перебор целей; первая цель, где
   объект существует, даёт `Success((true, size))`; все NotFound →
   `Success((false, 0))` (spec §6.23); `GetObjectRangeAsync` — read-перебор
   до найденного (NotFound → следующая цель; образец: `GetObjectToFileAsync`
   пула); `GetObjectStreamAsync` — НЕ реализуем в пуле (операции приёмника
   строят единичный клиент): метод явного исключения
   `NotSupportedException("stream is endpoint-pinned; use single-target
   client")` (интерфейс требует тело).
3. `RestoreProcess` валидация (строки ~283–290): `DownloadTextAsync(...
   backup_manifest)` → `HeadObjectAsync(srcC, srcX, $"full/{backupId}.tar")`
   (`s3` здесь PoolBackupS3 → привести к `IStreamObjectS3`); `false` →
   FailPermanent `"full <id>: object full/<id>.tar not found"`; текст
   комментария блока обновить (существование = Complete прошёл, канон).
4. `RestoreProcess` backup_label (строки ~292–300): `DownloadTextAsync(...
   backup_label)` → `FullTarSet.ReadBackupLabelAsync(...)` (тот же
   pool-клиент как `IStreamObjectS3`).
5. `RestoreDrillProcess` — те же две замены (строки ~139–152).

**Выход:** DR-валидация и чтение wal_start работают по tar-объекту.

**Проверка:**
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~FullTarSetTests|FullyQualifiedName~PoolBackupS3Tests" --nologo
```
Ожидание: PASS (`FullTarSetTests` — НОВЫЙ: фейк `IStreamObjectS3` с
tar-объектом (writer шага 2, первая запись backup_label с эталонной строкой
`START WAL LOCATION: 0/2000028 (file 000000010000000000000002)`): возвращает
`000000010000000000000002`; первая запись не backup_label → Success(null);
Range [0,65535] запрошен (ассерт фейка); транспортный отказ → Failed).

**Связь со spec:** §3.7 (Restore/Drill), §6.10; Review Focus 5 (малый
объект — Range от 0).

**Подпункты:**

18.1. Создай `src/PgWorker.Backups/FullTarSet.cs` по «Действие» п.1
(статический класс; using `PgWorker.Backups.Tar` — `TarStreamReader` уже в
этом проекте с шага 1; doc-комментарии русские).

18.2. В `src/PgWorker.Backups/PoolBackupS3.cs` добавь реализацию
`IStreamObjectS3` по «Действие» п.2 (перебор по образцу существующих
read-методов файла; `_targetFactory`/`GetTarget`-паттерн файла).

18.3. Внеси правки 3–5 в `src/PgWorker.Backups/Process/RestoreProcess.cs` и
`src/PgWorker.Backups/Process/RestoreDrillProcess.cs` (замены
`DownloadTextAsync`-вызовов по «Действие»; тексты ошибок из «Действие»).

18.4. Создай `src/tests/PgWorker.UnitTests/Backups/FullTarSetTests.cs` по
«Проверке» (AAA; tar собери `TarStreamWriter`-ом).

> Примечание к 18.1: размещение Tar-кода в PgWorker.Backups закреплено
> spec §3.2 (обоснование — недопустимость циклической ссылки проектов:
> PgWorker.PgNode → PgWorker.Backups уже существует, обратной нет).

---

### Шаг 19. Финал: полный E2E-цикл + «большой набор» + мерж-гейт (docker)

**Вход:** шаги 11–18 слиты; все юниты зелены.

**Действие (сильная модель):**
1. Полный прогон серий (по порядку, с зачисткой между; фильтры — по
   фактическим именам классов, Pool-наследники перечислены явно — фильтр
   по базовому имени их не матчит):
   `E2eBackupJobsScenarios` → `E2eBackupFailsOnBadS3Scenarios` →
   `E2eBackupVerifyScenarios` → `E2eRestoreScenarios` →
   `E2eRestoreNewClusterScenarios` → `E2eDrillScenarios` →
   `E2eRetentionScenarios` → `E2eSupervisorOrphanRegistryScenarios` →
   `E2eSupervisorOrphanDrHoldScenarios` → `E2eTliSwitchScenarios`
   (PITR-цепочка) → Pool-серии: `PoolNotSelectedScenarios`,
   `PoolMasterReplicaSplitScenarios`, `PoolSingleNodeScenarios`,
   `PoolFailoverExhaustionScenarios`, `PoolRoleSwitchScenarios`,
   `PoolReadAnyNodeScenarios` (отказные серии повторно после правок шагов
   16–18 — Abort/retry-семантика multipart, требование 3 roadmap).
   Каждая серия — свой прогон с фильтром `FullyQualifiedName~<Класс>`,
   логи `/tmp/t01-s19-<серия>.log`.
2. Профиль «большой набор»: НОВЫЙ сценарий `E2eBackupLargeSetScenarios`
   (файл `src/tests/PgWorker.E2eTests/E2E/E2eBackupLargeSetScenarios.cs`,
   по образцу `E2eBackupJobsScenarios`): кластер из фикстуры; засев ~3 ГиБ
   (pgbench init -s 200 ИЛИ COPY-генерация таблиц; образец засева —
   существующие сценарии с данными); замер свободного места data-volume
   ноды-источника ДО и ВО ВРЕМЯ полного (docker exec df внутри контейнера
   ноды по пути /home/postgres/pgdata; поллинг в цикле ожидания COMPLETED);
   ассерты: свободное место упало < 10% от размера набора; list `full/` —
   ровно 1 объект; verify OK; etcd COMPLETED с sha256/size_bytes.
3. Мерж-гейт (AGENTS.md): сборка Release ОТДЕЛЬНО → E2ePrepare → минимум
   `--filter FullyQualifiedName~E2eBackupJobsScenarios` на свежем бинаре.
4. Зачистка docker-остатков после ВСЕХ серий (`docker ps -a`/`network
   prune -f` по правилам).
5. Финальный юнит-прогон всего проекта:
```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --nologo > /tmp/t01-s19-unit.log 2>&1; echo "exit=$?"
```

**Выход:** все критерии spec §7 закрыты (E2E-цикл одним объектом → verify →
restore latest + PITR → retention; большой набор без второй копии;
мерж-гейт).

**Проверка:** `/tmp/t01-s19-*.log` — только Passed в каждой серии; финальный
юнит-прогон exit=0.

**Связь со spec:** §4 фаза 6; §7 (все критерии приёмки).

**Подпункты:**

19.1. Создай файл `src/tests/PgWorker.E2eTests/E2E/
E2eBackupLargeSetScenarios.cs` — ТОЛЬКО каркас по скелету соседнего
`E2eBackupJobsScenarios.cs`: using'и, объявление класса с тем же
namespace, приватные поля фикстуры (копия соседнего файла), конструктор
фикстуры, пустые `[Fact]`-методы-заглушки `FullBackup_NoSecondCopyOnDisk`
и `FullBackup_SingleObjectInList` (тела бросают
`NotImplementedException`), teardown-метод по образцу соседа. Тест-ТЕЛА
(засев ~3 ГиБ, df-поллинг, ассерты) пишет сильная модель в самом шаге 19,
заменяя заглушки.

---

## Порядок исполнения и зависимости

1→2→3→4→5 (фаза A) → 6→7→8→9→10→11 (фаза B) → 12→13 (фаза C) → 14→15 (фаза
D) → 16→17→18 (фаза E) → 19 (фаза F). Шаг 5 требует живого стенда; если
стенд недоступен — остановиться (не продолжать фазу B без прототипа).

## Placement-решение (зафиксировано spec §3.2)

Tar-код (`TarEntry.cs`, `TarStreamReader.cs`, `TarStreamWriter.cs`) живёт в
`src/PgWorker.Backups/Tar/`, namespace `PgWorker.Backups.Tar` (шаги 1–2
создают файлы сразу там; spec §3.2 закрепляет это размещение явно).
Причина: `TarStreamReader` нужен И приёмнику
(FullBackup/Verify/Restore — PgWorker.PgNode), И воркеру
(`FullTarSet.ReadBackupLabelAsync` — PgWorker.Backups); ссылка проектов
PgWorker.PgNode → PgWorker.Backups существует, обратной нет (циклические
ссылки запрещены) — общий код кладётся в PgWorker.Backups. Имена классов и
API соответствуют сводке интерфейсов.
Остальные файлы BaseBackup (`PgMessage.cs`, `PgWireReplicationClient.cs`,
`BaseBackupCommand.cs`, `MultipartPartBufferStream.cs`,
`BackupManifest.cs`) — в `src/PgWorker.PgNode/BaseBackup/`: потребителей
в воркере у них нет. Прототип шага 5 подключает ОБА проекта
(`PgWorker.PgNode.csproj` даёт BaseBackup-команды; `PgWorker.Backups.csproj`
— Tar) через ProjectReference.
