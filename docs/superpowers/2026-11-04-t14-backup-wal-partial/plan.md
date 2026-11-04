# t14-backup-wal-partial — план реализации

> **Для agentic-исполнителя:** план исполняется субагентом coder пошагово (задачи 0–15 по порядку; внутри задачи — шаги, внутри шагов — подпункты). Чекбоксы `- [ ]` — для трекинга. Все решения уже приняты (spec §7, развилки 1–34); свободных точек выбора НЕТ — любое отклонение фиксируй текстом в ответе, не решай молча.

**Цель:** реализовать `.partial`-доотправку RPO-хвоста WAL, дубль `pg_wal` только закрытыми сегментами, читатели `wal/` по максимальному валидному префиксу, фоновый wal-sweeper и retention-гигиену `.partial` — устранив гонку «срез дубля против стрим-финала» (3 красных E2E) без ослабления RPO-гарантий.

**Архитектура:** приёмник WalReceiver выкладывает валидный префикс живого сегмента объектом `wal/<seg>.<endLsn>.partial` (только длиннее лежащей, без гварда; финал поглощает); дубль полного фильтруется позицией активного сегмента источника (IDENTIFY_SYSTEM); контроль цепи/fetch-wal считают хвост = финал ∨ лучший `.partial`; глобальный лидер чистит мусор `wal/` per-shard циклом 60 с; ретенция чистит `.partial` тем же проходом. Канон — `arch/19-backups.md` (уже с правками t14).

**Стек:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit (AAA-комментарии), AWSSDK (существующие клиенты). P/Invoke запрещён.

**Spec:** `docs/superpowers/2026-11-04-t14-backup-wal-partial/spec.md` (исполнитель читает spec целиком перед задачей 0; решения развилок — spec §7).

## Global Constraints

- Комментарии в коде — русские; идентификаторы — английские; тесты — AAA.
- Сборка без ворнингов (`TreatWarningsAsErrors=true`).
- Таймауты/бюджеты существующих операций не менять: fetch-wal `CallDeadline=45 с`, `TransportAttempts=3`, `PutAttemptTimeout=15 с`, breaker 45 с — как есть.
- Новых etcd-ключей нет; панель не трогаем; стоп-семантика/гейты/слоты/планировщик полных/дрилл/RestoreProcess — не менять.
- Централизованное версионирование пакетов (CPM) — новых пакетов НЕТ.
- Коммит после каждой задачи (feature-ветка worktree, коммитить свободно); arch-правки (задача 0) — отдельным коммитом ДО кода.
- Юнит-запуски: `DOTNET_CLI_UI_LANGUAGE=en dotnet test <проект> --filter "<фильтр>" --nologo` из корня worktree; лог длинного прогона — в `/tmp/`-файл, не в чат.
- E2E (задача 15): ВСЕГДА две отдельные команды (build, затем test `--no-build`), перед тестами — `dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`; между сериями — зачистка остаточных контейнеров/сетей (`docker ps -a | grep pgw-`, `docker network prune -f` при осиротевших сетях); упавший E2E НЕ перезапускать без разбора логов (артефакты `/tmp/pgw-e2e-artifacts-*`, `$TMPDIR`).

## Review Focus (где spec молчит — ждём именно этого поведения; тесты — в задачах-владельцах)

1. `WalPrefixScanner` на «нулевом» буфере после long-заголовка → префикс 0, не падение (задача 1, тест `Empty_AfterLongHeader_Zero`).
2. `TailOf` при `.partial` сегмента НИЖЕ финала другой позиции (tli-ветвление) → финал выше выигрывает; `.partial` ниже cutoff-подобных позиций не «удлиняет» хвост назад (задача 3).
3. Памп при закрытии сегмента ровно между list и put: put `.partial` после финала безвреден — sweeper/поглощение уберут; тест не требуется, поведение — не падать (задача 6, код пропускает put только по EndLsn, конфликт невозможен).
4. Дубль при наборе из ОДНОГО сегмента (wal_start == последний сегмент, он же живой) → в `wal/` не попадает ничего; verify-union закрывает диапазон из набора (задачи 7, 3).
5. `SelectPartialForDeletion` при равных EndLsn двух версий → удаляется ровно одна (любая из равных — детерминизм не важен, важен размер остатка 1) (задача 4).

---

### Задача 0 (Ф0): arch-правки

**Вход:** worktree чистый (git status), spec прочитан.
**Действие:** три точечные правки канона (arch-first; текст — ниже, подпункты 0.1–0.3). Больше в arch/ НЕ менять.
**Выход:** arch/18 §2.2 + arch/19 §3/§9 отражают реализуемое.
**Проверка:** `git diff --stat` — ровно 2 файла; `git diff` — внесены только тексты подпунктов.
**Spec:** §3 (arch-правки 1–3).

- [ ] Шаг: внести правки 0.1–0.3 и закоммитить `docs(arch): t14 — Wal:PartialIntervalSec=3 (§9), порог .partial 4 МиБ (§3), wal-sweeper-метрики/алерты (18 §2.2)`.

Подпункты:

**0.1. `arch/19-backups.md`, §3** — в абзаце «Агент» найти фразу «доотправляется инкрементально — каждые `Wal:PartialIntervalSec` (дефолт 3 с) и/или по порогу байт — объектом» и заменить «и/или по порогу байт» на «и/или по порогу байт (4 МиБ — константа приёмника, не конфиг)». Больше в этом абзаце ничего не менять.

**0.2. `arch/19-backups.md`, §9** — найти `` Wal { VerifyIntervalSec=30, `` и дописать в секцию Wal первым параметром `` PartialIntervalSec=3, `` (период `.partial`-доотправки приёмником; доставляется в контейнер ноды env `PGW_WAL_PARTIAL_INTERVAL_SEC` — как `PGW_BACKUP_STAGING_DIR`). В поясняющую скобку после секции Wal (текст «период list/контроля цепочки, порог отставания в сегментах, порог тишины загрузок;…») дописать в начало: «`PartialIntervalSec` — период `.partial`-доотправки префикса живого сегмента (§3);».

**0.3. `arch/18-metrics.md`, §2.2** — в строке таблицы `worker_loop_last_success_timestamp_seconds` (та, что с OrphanSweepStalled) после текста про цикл `orphan-sweep` дописать: «; цикл `wal-sweeper` (WalSweeperLoop, тик лидера раз в 60 с при `Backups:Enabled=true`, `ok` = успех прохода `WalSweeperProcess.SweepAsync`; чистка мусора `wal/` — arch/19 §4) — живость закрывает правило **WalSweepStalled** (`time() - worker_loop_last_success_timestamp_seconds{loop="wal-sweeper"} > 180`; серия эмитится только лидером при `Backups:Enabled=true`)». Сразу после строки `worker_loop_last_success_timestamp_seconds` добавить новую строку таблицы:
`| worker_wal_sweep_shard_last_success_timestamp_seconds | gauge | cluster, shard | unix последнего успешного per-shard прохода wal-sweeper (arch/19 §4; обновляется по факту прохода, серия исчезает с шардом); алерт **WalSweepShardStale** — time() − значение > 180 (3× минутного интервала), только при живой серии |`

---

### Задача 1 (Ф1): модель ключа `.partial` и сканер валидного префикса

**Вход:** задача 0 закоммичена.
**Действие:** два новых файла модели + два новых тест-файла (TDD: тесты можно писать одновременно — сигнатуры зафиксированы).
**Выход:** `WalPartialName.Key/TryParse/SegmentPrefixOf`, `WalPrefixScanner.ValidPrefixLength` — чистые функции, покрытые юнитами.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalPartialName|FullyQualifiedName~WalPrefixScanner" --nologo` — зелёные.
**Spec:** §4.1, развилки 1–2.
**Interfaces (для последующих задач):**
- `WalPartialName.Key(string segmentName, ulong endLsn) -> string` — `"<segmentName>.<endLsn:x16>.partial"`.
- `WalPartialName.TryParse(string name) -> (WalFileName Segment, ulong EndLsn)?`.
- `WalPartialName.SegmentPrefixOf(string segmentName) -> string` — `segmentName + "."`.
- `WalPrefixScanner.ValidPrefixLength(byte[] buffer, int count) -> int`; константы `LongHeaderBytes=40`, `ShortHeaderBytes=24`, `MinRecordBytes=24`, `PageBytes=8192`.

- [ ] Шаг: подпункты 1.1–1.4 (код — целиком в подпунктах), затем прогон фильтра выше и коммит `feat(backups): WalPartialName + WalPrefixScanner (t14)`.

Подпункты:

**1.1. Создать `src/PgWorker.Backups/Model/WalPartialName.cs`** (рядом с WalFileName.cs; using `System.Globalization`):

```csharp
using System.Globalization;

namespace PgWorker.Backups;

/// <summary>Ключ .partial-версии незакрытого сегмента (arch/19 §3):
/// wal/&lt;segment&gt;.&lt;endLsn&gt;.partial — segment 24 hex строчных, endLsn РОВНО
/// 16 hex строчных (LSN конца валидного префикса): фиксированная ширина даёт
/// лексикографический порядок суффиксов = числовому — лучший выбирается
/// без числового парсинга. Финалы и .history — не парсятся (null).</summary>
public static class WalPartialName
{
    public const string Suffix = ".partial";

    /// <summary>Ключ объекта по имени сегмента (24 hex) и LSN конца префикса.</summary>
    public static string Key(string segmentName, ulong endLsn)
        => $"{segmentName}.{endLsn:x16}{Suffix}";

    /// <summary>Разбор имени объекта: (сегмент, EndLsn) либо null (не .partial).</summary>
    public static (WalFileName Segment, ulong EndLsn)? TryParse(string name)
    {
        // <24hex>.<16hex>.partial — ровно 49 символов.
        if (name.Length != 24 + 1 + 16 + Suffix.Length
            || name[24] != '.'
            || !name.EndsWith(Suffix, StringComparison.Ordinal))
            return null;
        if (WalFileName.TryParse(name[..24]) is not { } segment)
            return null;
        return ulong.TryParse(name.AsSpan(25, 16), NumberStyles.HexNumber,
                   CultureInfo.InvariantCulture, out var endLsn)
            ? (segment, endLsn)
            : null;
    }

    /// <summary>Узкий S3-префикс листинга всех версий сегмента: "<segmentName>.".</summary>
    public static string SegmentPrefixOf(string segmentName)
        => segmentName + ".";
}
```

**1.2. Создать `src/PgWorker.Backups/Model/WalPrefixScanner.cs`:**

```csharp
namespace PgWorker.Backups;

/// <summary>Валидный префикс незакрытого WAL-сегмента (arch/19 §3): байты до
/// конца последней ПОЛНОЙ записи, без нулей-добивки. Страничная механика PG:
/// страница 8 КиБ; первая страница сегмента — long-заголовок 40 байт, прочие —
/// short 24; запись НЕ пересекает границу страницы (не влезающая пишется целиком
/// на следующей, остаток текущей добит нулями); заголовок записи: xl_tot_len
/// LE u32 @4, минимум 24 байта. Любая аномалия — стоп на конце последней
/// полной записи (консервативно: короткий префикс теряет лишь часть RPO-хвоста,
/// финал восстановит полный). Чистая функция.</summary>
public static class WalPrefixScanner
{
    public const int PageBytes = 8192;       // XLOG_BLCKSZ
    public const int LongHeaderBytes = 40;   // XLogLongPageHeaderData — первая страница сегмента
    public const int ShortHeaderBytes = 24;  // XLogPageHeaderData — прочие страницы
    public const int MinRecordBytes = 24;    // SizeOf(XLogRecord)

    /// <summary>Длина валидного префикса, байт; 0 — ни одной полной записи
    /// (буфер короче long-заголовка / только заголовок / нули).</summary>
    public static int ValidPrefixLength(byte[] buffer, int count)
    {
        if (count < LongHeaderBytes)
            return 0;

        var prefix = 0; // конец последней полной записи
        var pos = 0;
        var firstPage = true;
        while (pos < count)
        {
            if (pos % PageBytes == 0)
            {
                // Заголовок страницы: long — только на первой странице сегмента.
                pos += firstPage ? LongHeaderBytes : ShortHeaderBytes;
                firstPage = false;
                if (pos > count)
                    return prefix; // заголовок не дописан — записей на странице нет
            }

            var pageEnd = (pos / PageBytes + 1) * PageBytes;
            var remainingInPage = pageEnd - pos;
            if (remainingInPage < MinRecordBytes)
                return prefix; // хвост страницы — нули-добивка
            if (count - pos < MinRecordBytes)
                return prefix; // заголовок записи ещё не принят потоком

            var totalLen = (uint)(buffer[pos + 4] | buffer[pos + 5] << 8
                | buffer[pos + 6] << 16 | buffer[pos + 7] << 24);
            if (totalLen < MinRecordBytes || totalLen > (uint)remainingInPage)
                return prefix; // нули/мусор/запись не влезает в страницу
            if (pos + totalLen > count)
                return prefix; // запись пишется — принимаем только полные

            pos += (int)totalLen;
            prefix = pos;
        }

        return prefix;
    }
}
```

**1.3. Создать `src/tests/PgWorker.UnitTests/Backups/WalPartialNameTests.cs`** (namespace `PgWorker.UnitTests.Backups`, xUnit + FluentAssertions, AAA):

Тест-кейсы (каждый — `[Fact]`, тело AAA):
- `Key_CanonicalLowercaseHex`: `WalPartialName.Key("000000010000000000000002", 0x1000000UL)` → `"000000010000000000000002.0000000001000000.partial"`.
- `TryParse_Valid_ReturnsSegmentAndLsn`: парс ключа выше → Segment.Name == `"000000010000000000000002"`, EndLsn == `0x1000000UL`.
- `TryParse_Rejects`: `null` для: финала `"000000010000000000000002"`, `.history` `"00000002.history"`, короткого суффикса `"000000010000000000000002.0100.partial"`, суффикса не-hex `"000000010000000000000002.zzzzzzzzzzzzzzzz.partial"`, мусора `"abc"`.
- `SegmentPrefixOf_EndsWithDot`: `WalPartialName.SegmentPrefixOf("000000010000000000000002")` → `"000000010000000000000002."`.
- `Lexicographic_MaxSuffix_IsMaxLsn`: из трёх ключей `Key(seg, 0x10)`, `Key(seg, 0x9)`, `Key(seg, 0xFF)` максимум `OrderBy(n => n, StringComparer.Ordinal)` — ключ с `0xFF` (фиксированная ширина = числовой порядок).

**1.4. Создать `src/tests/PgWorker.UnitTests/Backups/WalPrefixScannerTests.cs`** — с локальным билдером страниц:

```csharp
// Локальный билдер (в классе теста): страница 8192; long-заголовок первой
// страницы (tli LE u32 @4, pageaddr LE u64 @8 = 0 — сканеру достаточно полей);
// записи: [зарезерв 4 байта нулей][totalLen LE u32 @4][полезные байты до totalLen].
private static (byte[] Buffer, int Count) Page(params int[] recordLengths)
{
    var buffer = new byte[8192];
    // long-заголовок: tli=1 @4 (ненулевой — валиден), pageaddr @8 = 0
    buffer[4] = 1;
    var pos = WalPrefixScanner.LongHeaderBytes;
    foreach (var len in recordLengths)
    {
        var l = (uint)len;
        buffer[pos + 4] = (byte)l;
        buffer[pos + 5] = (byte)(l >> 8);
        buffer[pos + 6] = (byte)(l >> 16);
        buffer[pos + 7] = (byte)(l >> 24);
        buffer[pos + 8] = 0xAB; // полезный байт записи (отличен от нуля)
        pos += len;
    }
    return (buffer, pos);
}
```

Тест-кейсы (AAA):
- `EmptyBuffer_Zero`: `ValidPrefixLength(new byte[10], 10)` → 0.
- `OnlyLongHeader_Zero`: буфер 8192, count = 40 (один long-заголовок) → 0.
- `Empty_AfterLongHeader_Zero`: count = 40, остальное нули → 0 (Review Focus 1).
- `SingleRecord_PrefixToEnd`: `Page(100)`, count = 40+100 → 140.
- `TwoRecords_PrefixToSecondEnd`: `Page(100, 50)` → 190.
- `TrailingZeros_Cut`: `Page(100)`, но count = 8192 (хвост страницы нули) → 140.
- `UnfinishedRecord_CutToPrevious`: `Page(100, 50)`, count = 40+100+10 (вторая запись начата, не принята) → 140.
- `ZeroTotLen_Cut`: буфер: long header + запись 100 (count=140), затем байты нулей до 200 с totalLen=0 @140 → count=200 → префикс 140.
- `RecordNotFittingPage_MovedToNext`: long header + 30 байт нулей (добивка: остаток страницы 8152-… не хватает) — точнее: первая запись длиной 8130 (> остатка страницы 8152−0=8152? нет) —构造: запись длиной `remainingInPage + 1` (не влезает) на позиции 40 → префикс 0; затем на следующей странице short-заголовок 24 + запись 40 → префикс = 8192+24+40: собрать буфер 2 страниц вручную (модификация билдера с offset). Упрощённо: два ассерта — «запись длиннее остатка страницы → префикс до неё» и «запись на второй странице после short-заголовка читается».
  (решение Фазы 6: `xl_tot_len` — ПЕРВОЕ поле `XLogRecord` (смещение 0), а не @4: смещение 4 — `xl_xid`. План/спека давали @4 — чтение xid как длины обрезало `.partial`-префикс посреди следующей записи (E2E-факт: CRC-ошибка восстановления и «recovery ended before target»; подтверждено `pg_waldump` на реальном объекте). Сканер читает длину от 0 и выравнивает следующую запись по 8 байт (MAXALIGN, как PG). Добавлен регресс-тест `RecordLengthFromOffsetZero_NotXid`; билдеры тестов пишут длину от 0 и выравнивают записи.)

---

### Задача 2 (Ф1): SegmentAssembler.PartialSnapshot + потокобезопасность

**Вход:** задача 1 в дереве.
**Действие:** `src/PgWorker.PgNode/SegmentAssembler.cs`: общий `lock` для Append/Close/PartialSnapshot + новый метод PartialSnapshot. Тесты — в существующий `src/tests/PgWorker.UnitTests/Backups/WalReceiverSegmentAssemblerTests.cs`.
**Выход:** снимок префикса доступен из другого потока (памп), инварианты сборщика сохранены.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalReceiverSegmentAssembler" --nologo` — зелёные (вкл. прежние).
**Spec:** §4.1, развилки 10, 22, 26.
**Interfaces:** `SegmentAssembler.PartialSnapshot() -> (string Name, byte[] Prefix, ulong EndLsn)?`.

- [ ] Шаг: подпункты 2.1–2.2, прогон фильтра, коммит `feat(pgnode): SegmentAssembler.PartialSnapshot — валидный префикс живого сегмента (t14)`.

Подпункты:

**2.1. `SegmentAssembler.cs`** — три правки:
(a) добавить поле `private readonly object _sync = new();` рядом с `_buffer`;
(b) обернуть ТЕЛО метода `Append` (от `_serverWalEnd = chunk.WalEnd;` до `return closed;`) в `lock (_sync) { ... }` и ТЕЛО `Close()` — тоже в `lock (_sync) { ... }` (lock реентерабелен: Close зовётся из Append);
(c) добавить метод (после `Close`):

```csharp
/// <summary>Снимок валидного префикса незакрытого сегмента (arch/19 §3):
/// имя — математика Close (TLI из long page header буфера с валидацией
/// xlp_pageaddr, иначе наследование _currentTli), КОПИЯ префикса (буфер —
/// исключительное владение hot-path пула, отдаётся копия — WalPrefixScanner
/// режет нули-добивку и незавершённые записи), EndLsn = _segmentStart + длина
/// префикса. Нет незакрытого сегмента/заголовка, префикс пуст — null.
/// Потокобезопасен относительно Append (общий _sync — памп-поток читает
/// снимок параллельно стриму).</summary>
public (string Name, byte[] Prefix, ulong EndLsn)? PartialSnapshot()
{
    lock (_sync)
    {
        if (_buffer is null || _bufferCount < WalPrefixScanner.LongHeaderBytes)
            return null;
        var prefixLen = WalPrefixScanner.ValidPrefixLength(_buffer, _bufferCount);
        if (prefixLen <= 0)
            return null;

        uint tli;
        if (LeU64(_buffer, 8) == _segmentStart)
            tli = LeU32(_buffer, 4);
        else if (_currentTli is { } inherited)
            tli = inherited;
        else
            return null; // заголовок невалиден и наследовать нечего (§3, развилка 22)

        var segId = (uint)(_segmentStart / (ulong)WalFileName.SegmentBytes);
        var name = new WalFileName(
            tli, segId / WalFileName.SegsPerLog, segId % WalFileName.SegsPerLog).Name;
        var prefix = new byte[prefixLen];
        Buffer.BlockCopy(_buffer, 0, prefix, 0, prefixLen);
        return (name, prefix, _segmentStart + (ulong)prefixLen);
    }
}
```

**2.2. Тесты** — добавить в `WalReceiverSegmentAssemblerTests.cs` (файл уже имеет хелпер `SegmentBytesWithHeader(uint tli, ulong boundary, int length)`):

- `PartialSnapshot_NoSegment_ReturnsNull`: `new SegmentAssembler(0)` (ничего не append'или) → `PartialSnapshot()` null.
- `PartialSnapshot_WithoutValidRecord_ReturnsNull`: собрать буфер с long-заголовком (tli=1, boundary=0) длиной 100 байт одним чанком `new XLogChunk(0, 100, data)` → Append → `PartialSnapshot()` null (ни одной полной записи: totalLen-байты нулевые).
- `PartialSnapshot_ReturnsValidPrefix`: те же 100 байт, но со валидной записью: data[44..48] = LE u32 длины 60 (запись начинается на 40, totalLen @44), count=100 → EndLsn == `0 + 100` нет: prefix = 40+60 = 100 → `(Name: "000000010000000000000000", Prefix.Length == 100, EndLsn == 100UL)`.
  Байты длины вручную: `data[44] = 60;` (60 < 256).
- `PartialSnapshot_DoesNotDisturbClose`: после снимка дослать чанк до полного сегмента (16 МиБ от границы 0) → Append вернул 1 ClosedSegment с Name `"000000010000000000000000"` (прежняя механика не задета).

---

### Задача 3 (Ф1): WalChain.TailOf + текст ошибки CheckRange

**Вход:** задачи 0–2 в дереве.
**Действие:** `src/PgWorker.Backups/Model/WalChain.cs`: record `WalTailFact` + `TailOf` + правка текста ошибки Walk. Тесты — в `src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs`.
**Выход:** хвост потока (финал ∨ лучший `.partial`) вычислим чистой функцией.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalChain" --nologo` — зелёные.
**Spec:** §4.1, развилки 14; §4.4 (текст ошибки).
**Interfaces:** `WalChain.TailOf(WalFileName? chainLastFinal, IEnumerable<string> objectNames) -> WalTailFact?`; `record WalTailFact(string SegmentName, string ObjectName)`.

- [ ] Шаг: подпункты 3.1–3.3, прогон, коммит `feat(backups): WalChain.TailOf — хвост потока финал/лучший .partial (t14)`.

Подпункты:

**3.1. `WalChain.cs`** — добавить (после `RatchetedStart`, до `CheckWithRestart`):

```csharp
/// <summary>Хвост потока (arch/19 §3 «Истина прогресса»).</summary>
public sealed record WalTailFact(string SegmentName, string ObjectName);

/// <summary>Хвост потока — максимальный валидный префикс: финальный объект
/// цепочки (chainLastFinal — результат Check/CheckWithRestart) ЛИБО лучший
/// .partial (версии схлопываются по сегменту, учитывается длиннейшая по
/// EndLsn — WalPartialName). Сравнение по (Tli, log·256+seg); при равной
/// позиции финал авторитетен. Чистая функция; null — объектов нет.</summary>
public static WalTailFact? TailOf(WalFileName? chainLastFinal, IEnumerable<string> objectNames)
{
    // Лучшие .partial по сегментам (дедуп версий: длиннейшая EndLsn).
    var bestPartialBySegment = new Dictionary<WalFileName, string>();
    var bestEndLsn = new Dictionary<WalFileName, ulong>();
    foreach (var name in objectNames)
        if (WalPartialName.TryParse(name) is { } p
            && (!bestEndLsn.TryGetValue(p.Segment, out var cur) || p.EndLsn > cur))
        {
            bestEndLsn[p.Segment] = p.EndLsn;
            bestPartialBySegment[p.Segment] = name;
        }

    var candidates = new List<(WalFileName Segment, string ObjectName, bool IsFinal)>();
    if (chainLastFinal is { } final)
        candidates.Add((final, final.Name, IsFinal: true));
    foreach (var (segment, objectName) in bestPartialBySegment)
        candidates.Add((segment, objectName, IsFinal: false));
    if (candidates.Count == 0)
        return null;

    // Максимум по (Tli, позиция); равная позиция — финал авторитетен.
    return candidates
        .OrderByDescending(c => ((long)c.Segment.Tli,
            (long)c.Segment.Log * WalFileName.SegsPerLog + c.Segment.Seg))
        .ThenByDescending(c => c.IsFinal)
        .Select(c => new WalTailFact(c.Segment.Name, c.ObjectName))
        .First();
}
```

**3.2. `WalChain.cs`, метод `Walk`** — заменить строку `$"segment {reached.Name} from the backup set is missing in wal/ (expected after "` на `$"segment {reached.Name} from the backup set is missing in wal/ or the backup set (expected after "`.

**3.3. Тесты** в `WalChainTests.cs` (AAA; имена сегментов собирайте `new WalFileName(1, 0, 5).Name` и т.п.):
- `TailOf_NoObjects_ReturnsNull`: `TailOf(null, [])` → null.
- `TailOf_FinalOnly`: chainLastFinal = seg(1,0,5) → SegmentName == имя сегмента, ObjectName == оно же.
- `TailOf_PartialAboveFinal_Wins`: финал seg(1,0,5) + имена `Key(seg(1,0,6).Name, 0x100)` и `Key(seg(1,0,6).Name, 0x200)` (через `WalPartialName.Key`) → SegmentName == имя seg(1,0,6), ObjectName == ключ с 0x200 (длиннейшая версия).
- `TailOf_EqualPosition_FinalWins`: финал seg(1,0,6) + `.partial` сегмента seg(1,0,6) → ObjectName == имя финала (Review Focus 2, часть 1).
- `TailOf_PartialBelowFinalPosition_Ignored`: финал seg(1,0,7) + `.partial` сегмента seg(1,0,3) → SegmentName == имя seg(1,0,7).
- `TailOf_HigherTliPartial_Wins`: финал seg(1,0,7) + `.partial` сегмента seg(2,0,1) → SegmentName == имя seg(2,0,1) (tli выше — хвост нового таймлайна).
- `CheckRange_EndOnlyInSet_Closes` (Review Focus 4): `CheckRange(seg(1,0,2), seg(1,0,4), имена [финалы 2,3] ∪ [имя сегмента 4 из набора], null)` → IsContinuous true (сегмент 4 не в wal/, но в объединении).

---

### Задача 4 (Ф1): RetentionPlanner.SelectPartialForDeletion

**Вход:** задача 3 в дереве.
**Действие:** функция в `src/PgWorker.Backups/Retention/RetentionPlanner.cs` (partial-класс; добавить в тот же файл), тесты в `RetentionPlannerTests.cs`.
**Выход:** чистая функция мусора `.partial`.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~RetentionPlanner" --nologo` — зелёные.
**Spec:** §4.1, §4.7; развилки 17, 19.
**Interfaces:** `RetentionPlanner.SelectPartialForDeletion(IReadOnlyList<string> objectNames, WalFileName? cutoff) -> IReadOnlyList<string>`.

- [ ] Шаг: подпункты 4.1–4.2, прогон, коммит `feat(backups): SelectPartialForDeletion — гигиена .partial (t14)`.

Подпункты:

**4.1. Функция** (добавить в `RetentionPlanner`, после `SelectWalForDeletion`):

```csharp
/// <summary>Мусорные .partial-объекты (arch/19 §4 п.5, чистая функция):
/// (а) поглощённые финалом — лежит wal/&lt;seg&gt; ⇒ все версии мусор;
/// (б) устаревшие версии живого сегмента — остаётся длиннейшая по EndLsn
/// (равные — остаётся одна, любая); (в) .partial сегмента ниже cutoff
/// (cutoff null — ветка не выполняется). Финалы/.history не трогает.</summary>
public static IReadOnlyList<string> SelectPartialForDeletion(
    IReadOnlyList<string> objectNames, WalFileName? cutoff)
{
    var finals = new HashSet<WalFileName>();
    var partials = new Dictionary<WalFileName, List<(string Name, ulong EndLsn)>>();
    foreach (var name in objectNames)
    {
        if (WalFileName.TryParse(name) is { } final)
            finals.Add(final);
        else if (WalPartialName.TryParse(name) is { } p)
        {
            if (!partials.TryGetValue(p.Segment, out var list))
                partials[p.Segment] = list = [];
            list.Add((name, p.EndLsn));
        }
    }

    var doomed = new List<string>();
    foreach (var (segment, versions) in partials)
    {
        if (finals.Contains(segment))
        {
            doomed.AddRange(versions.Select(v => v.Name)); // (а) поглощено финалом
            continue;
        }
        if (cutoff is { } limit
            && (segment.Tli < limit.Tli
                || (segment.Tli == limit.Tli
                    && (long)segment.Log * WalFileName.SegsPerLog + segment.Seg
                       < (long)limit.Log * WalFileName.SegsPerLog + limit.Seg)))
        {
            doomed.AddRange(versions.Select(v => v.Name)); // (в) ниже cutoff
            continue;
        }

        var keep = versions.MaxBy(v => v.EndLsn).Name; // (б) живой сегмент
        doomed.AddRange(versions.Where(v => v.Name != keep).Select(v => v.Name));
    }

    return doomed;
}
```

**4.2. Тесты** в `RetentionPlannerTests.cs` (AAA; ключи — `WalPartialName.Key(new WalFileName(1, 0, n).Name, lsn)`):
- `AbsorbedByFinal_AllVersionsDoomed`: финал seg(1,0,5) + две версии его `.partial` → обе в результате.
- `LiveSegment_OnlyLongestKept`: три версии сегмента seg(1,0,6) (без финала), cutoff null → в результате ровно две короткие, длиннейшей нет.
- `BelowCutoff_AllVersionsDoomed`: версии сегмента seg(1,0,3), cutoff seg(1,0,5) → обе; сегмент РАВНЫЙ cutoff → НЕ doomятся.
- `CutoffNull_KeepsLiveVersions`: версии живого сегмента при cutoff null → только устаревшие (ветка (в) молчит).
- `EqualEndLsn_KeepsExactlyOne` (Review Focus 5): две версии с равным EndLsn → в результате ровно одна из них.
  (решение Фазы 6: при фиксированной ширине EndLsn равный числовой LSN у двух имён достижим только неканоническим регистром hex-суффикса — канон пишется lower-case; тест использует пару `Key(seg, 0x3FF)` + `{seg}.{0x3FF:X16}.partial` и проверяет «ровно одна версия удалена, остаток 1», как задаёт Review Focus. Канонические одинаковые ключи физически один объект — удаление не требуется.)
- `FinalsAndHistory_Untouched`: финал seg(1,0,2) + `"00000002.history"` в списке → в результате их нет.

---

### Задача 5 (Ф2): конфиг и env-доставка PartialIntervalSec

**Вход:** задачи 0–4 в дереве.
**Действие:** опции воркера + record приёмника + env-цепочка Program→NodeBackupEnv→ClusterDriver→WalReceiverLoop.
**Выход:** `PgWorker:Backups:Wal:PartialIntervalSec` (дефолт 3, fail-fast <1) доставляется в приёмник env-ом; приёмник клэмпит ≥1.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~BackupsOptions|FullyQualifiedName~PgNodeIdentity" --nologo` + `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` (без ворнингов).
**Spec:** §4.2, §4.9; развилки 4, 25.

- [ ] Шаг: подпункты 5.1–5.5, прогоны, коммит `feat(backups): конфиг/env Wal:PartialIntervalSec=3 (t14)`.

Подпункты:

**5.1. `src/PgWorker.PgNode/WalReceiverOptions.cs`** — добавить в конец record последний параметр `int PartialIntervalSec = 3` (с комментарием: «Период .partial-доотправки префикса живого сегмента, с (arch/19 §3/§9); env PGW_WAL_PARTIAL_INTERVAL_SEC ноды, дефолт 3; клэмп ≥1 — приёмник не падает на мусорном env»). Опциональный хвостовой параметр — существующие вызовы конструктора не ломаются.

**5.2. `src/PgWorker.App/Options.cs`**:
(a) в `BackupsWalOptions` (строка ~627) первым свойством добавить `public int PartialIntervalSec { get; set; } = 3;` (doc-комментарий: период `.partial`-доотправки приёмником, arch/19 §3/§9);
(b) в `BackupsOptions.ToRuntime()` после `WalStaleSec: Wal.StaleSec,` добавить `WalPartialIntervalSec: Wal.PartialIntervalSec,`;
(c) в `BackupsRuntimeOptions` (файл `src/PgWorker.Backups/Options.cs`) после `int WalStaleSec = 300,` добавить `int WalPartialIntervalSec = 3,` (комментарий: период `.partial`-доотправки, arch/19 §3/§9; доставляется env ноды);
(d) в `BackupsOptions.IsValid()` добавить условие `&& Wal.PartialIntervalSec >= 1`.

**5.3. `src/PgWorker.Core/Templates/NodeConfigBuilders.cs`** — record `NodeBackupEnv` (строка ~61) расширить хвостовым параметром: `public sealed record NodeBackupEnv(string? Bucket, string? CaPem, string? StagingDir = null, int? PartialIntervalSec = null);` (комментарий: период `.partial`-доотправки приёмника, §9; null/≤1 — env не ставится, приёмник берёт дефолт 3).

**5.4. `src/PgWorker.App/Program.cs`** — в конструкции `var nodeBackupEnv = new NodeBackupEnv(` (строка ~357) добавить последний аргумент `PartialIntervalSec: opts.Backups.Wal.PartialIntervalSec >= 1 ? opts.Backups.Wal.PartialIntervalSec : null`.

**5.5. `src/PgWorker.Docker/Drivers/ClusterDriver.cs`** — в блоке `if (nodeBackupEnv is { } backupEnv)` (строки ~570-585), после `PGW_BACKUP_STAGING_DIR`-ветки добавить:

```csharp
// Период .partial-доотправки приёмника (arch/19 §3/§9): env по образцу
// staging; null/≤1 → env не ставится — приёмник берёт дефолт 3 с.
if (backupEnv.PartialIntervalSec is > 0)
    env["PGW_WAL_PARTIAL_INTERVAL_SEC"] = backupEnv.PartialIntervalSec.Value.ToString();
```

**5.6. `src/PgWorker.PgNode/WalReceiverLoop.cs`** — в `ExecuteAsync`, при построении `options` (блок 4.4, `var options = new WalReceiverOptions(...)`), последним аргументом передать `PartialIntervalSec: ReadPartialIntervalSec()`. Добавить приватный статический метод:

```csharp
// env PGW_WAL_PARTIAL_INTERVAL_SEC (arch/19 §9): мусор/отсутствие → дефолт 3;
// клэмп ≥1 — приёмник живёт на любом env (пересоздание ноды не требуется).
private static int ReadPartialIntervalSec()
{
    var raw = Environment.GetEnvironmentVariable("PGW_WAL_PARTIAL_INTERVAL_SEC");
    return int.TryParse(raw, out var sec) && sec >= 1 ? sec : 3;
}
```
(Метод читает env напрямую — цикл строит options сам; инъекция для тестов не нужна: значение только проходит транзитом в WalReceiverCore, где клэмпится `Math.Max(1, ...)`.)

**5.7. Тест** — в `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` добавить `[Fact]` `Wal_PartialIntervalSec_DefaultAndValidation`: дефолт `new BackupsOptions().Wal.PartialIntervalSec` == 3; `IsValid()` false при `Wal.PartialIntervalSec = 0`; true при 3 (AAA).

---

### Задача 6 (Ф2): partial-памп приёмника + поглощение финалом

**Вход:** задачи 1–2, 5 в дереве.
**Действие:** `src/PgWorker.PgNode/WalReceiverCore.cs`: SignalGate, памп-задача в RunSessionAsync, `PutWithRetryAsync(useGuard)`, поглощение при финале. Тесты — `WalReceiverCoreTests.cs` (расширить FakeS3).
**Выход:** приёмник выкладывает `.partial` (только длиннее лежащей) и поглощает версии финалом; стрим/подтверждение не блокируются.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalReceiverCore" --nologo` — зелёные (вкл. ВСЕ прежние: инвариант подтверждения, backpressure, tli).
**Spec:** §4.2; развилки 5–9, 23, 28–30.
**Interfaces:** `WalReceiverCore` — без новых публичных сигнатур, кроме internal `PartialThresholdBytes` и констант; `PutWithRetryAsync(..., bool useGuard = true)` (private).

- [ ] Шаг (сильная модель): интеграция пампа в RunSessionAsync — вставки 6.2–6.4 по указанным якорям; отладка таймингов тестов при необходимости (без изменения прод-значений).
- [ ] Шаг: прогон фильтра WalReceiverCore + всего юнит-проекта, коммит `feat(pgnode): .partial-доотправка приёмника + поглощение финалом (t14)`.

Подпункты:

**6.1. `WalReceiverCore.cs`** — добавить internal-константу и SignalGate (внутри класса WalReceiverCore, рядом с `PutAttemptTimeout`):

```csharp
// Порог мгновенного сигнала пампу (arch/19 §3: «и/или по порогу байт» —
// 4 МиБ, 1/4 сегмента; константа приёмника, не конфиг): при бурной записи
// выкладка не ждёт тика PartialIntervalSec.
internal static readonly ulong PartialThresholdBytes = 4L * 1024 * 1024;

/// <summary>Сбрасываемый одноразовый сигнал (пампу): Pulse будит ожидающего,
/// следующий WaitAsync ждёт нового сигнала; ожидание отменяемо.</summary>
private sealed class SignalGate
{
    private TaskCompletionSource _tcs = New();
    private static TaskCompletionSource New()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);
    public void Pulse()
        => Interlocked.Exchange(ref _tcs, New()).TrySetResult();
    public Task WaitAsync(CancellationToken ct)
        => _tcs.Task.WaitAsync(ct);
}
```
(`using System.Threading;` — уже есть неявно через ImplicitUsings; `Interlocked` — System.Threading.)

**6.2. `WalReceiverCore.cs`, `PutWithRetryAsync`** — добавить последний параметр `bool useGuard = true` и заменить выбор вызова:

```csharp
// Гвард чужой цепочки (arch/19 §3) — ТОЛЬКО на финальных объектах wal/<segment>:
// .partial кладётся обычным put-ом (правило «длиннее выигрывает» исключает
// конкурирующие байты — конфликт невозможен по построению).
put = useGuard && s3 is IWalSegmentGuard guard
    ? await guard.PutWalSegmentAsync(key, data, sha256, attemptCts.Token)
    : await s3.PutObjectAsync(key, data, sha256, attemptCts.Token);
```
(прежний код выбирал guard-путь при `s3 is IWalSegmentGuard guard`; существующие вызовы без параметра сохраняют поведение).

**6.3. `RunSessionAsync`** — памп. Якорь: после `var assembler = new SegmentAssembler(startAt); uint prevTli = 0;` и ДО `try { await foreach ...`. Вставить:

```csharp
// .partial-памп (arch/19 §3): фоновая задача итерации — выкладка валидного
// префикса незакрытого сегмента (только длиннее лежащей); подтверждение слота
// НЕ двигает (инвариант RPO — только финалы). Триггеры: таймер
// PartialIntervalSec ИЛИ сигнал порога из цикла приёма (4 МиБ).
var lastPartialEndLsn = 0UL; // Volatile-гейт прироста (вкл. версии соседа)
var partialGate = new SignalGate();
using var partialCts = CancellationTokenSource.CreateLinkedTokenSource(sct);
var partialPumpToken = partialCts.Token;
var partialPump = Task.Run(async () =>
{
    var interval = TimeSpan.FromSeconds(Math.Max(1, o.PartialIntervalSec));
    while (!partialPumpToken.IsCancellationRequested)
    {
        try
        {
            var delay = Task.Delay(interval, partialPumpToken);
            var signal = partialGate.WaitAsync(partialPumpToken);
            await Task.WhenAny(delay, signal);
        }
        catch (OperationCanceledException)
        {
            break; // итерация/сессия закрываются
        }

        var snap = assembler.PartialSnapshot();
        if (snap is not { } shot)
            continue; // незакрытого сегмента/полной записи нет (§3, развилка 22)
        if (shot.EndLsn <= Volatile.Read(ref lastPartialEndLsn))
            continue; // нулевой прирост — версии не пере-clay'ются (§3)

        // Сверка «длиннее лежащей»: list узкого префикса (первая доступная
        // read-нода); отказ — skip тика (стрим не рвём, хвост доедет следующим
        // тиком/финалом).
        var listed = await s3.ListAsync(
            o.Cluster, o.Shard, $"wal/{WalPartialName.SegmentPrefixOf(shot.Name)}",
            ct: partialPumpToken);
        if (!listed.IsSuccess)
        {
            Diagnostic(events, $"partial list failed: {listed.Error!.Message.Split('\n')[0]}");
            continue;
        }
        ulong lying = 0;
        foreach (var obj in listed.Value)
            if (WalPartialName.TryParse(obj.Name) is { } v && v.EndLsn > lying)
                lying = v.EndLsn;
        if (lying >= shot.EndLsn)
        {
            // Сосед двойной архивации длиннее — ждём прироста за его версию.
            Volatile.Write(ref lastPartialEndLsn, lying);
            continue;
        }

        var partialKey = $"{o.Cluster}/{o.Shard}/wal/{WalPartialName.Key(shot.Name, shot.EndLsn)}";
        var partialSha = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(shot.Prefix));
        var putPartial = await PutWithRetryAsync(
            s3, events, o, partialKey, shot.Prefix, partialSha, retry, partialPumpToken,
            exhaustion, useGuard: false);
        if (!putPartial.IsSuccess)
            return; // permanent (конфликт невозможен; прочее — вечный ретрай не завершается)
        Volatile.Write(ref lastPartialEndLsn, shot.EndLsn);
        events.PutSuccess?.Invoke();
    }
}, CancellationToken.None);
```

Затем: (a) в приёмном цикле, сразу после `events.StreamProgress?.Invoke(assembler.ServerWalEnd);` вставить сигнал порога:

```csharp
// Порог-сигнал пампу (4 МиБ от последней выкладки; LSN-прирост ≈ байты —
// скан записей в hot-path не нужен, выкладка идёт по валидному префиксу).
if (assembler.LastAppendedLsn - Volatile.Read(ref lastPartialEndLsn)
    >= PartialThresholdBytes)
    partialGate.Pulse();
```

(b) обернуть существующий `try { await foreach ... } catch (Exception e) ...` блока приёма в дополнительный внешний `try { ... } finally { await partialCts.CancelAsync(); try { await partialPump; } catch { /* отменён */ } }` — памп живёт ровно одну итерацию assembler'а.

**6.4. Поглощение финалом** — в цикле `foreach (var segment in closedSegments)`, после блока подтверждения слота (шаг 7: `else { ... confirm failed ... }`), ПЕРЕД `finally { SegmentBufferPool.Return(...) }` — на том же уровне, вставить:

```csharp
// Поглощение версий финалом (arch/19 §3): best-effort list узкого
// префикса + batch-delete; отказ — diagnostic (докладывает sweeper).
var absorbed = await s3.ListAsync(
    o.Cluster, o.Shard, $"wal/{WalPartialName.SegmentPrefixOf(segment.Name)}",
    ct: sct);
if (absorbed.IsSuccess && absorbed.Value.Count > 0)
{
    var absorbedKeys = absorbed.Value
        .Select(w => $"{o.Cluster}/{o.Shard}/wal/{w.Name}").ToList();
    var absorbDelete = await s3.DeleteKeysAsync(absorbedKeys, sct);
    if (!absorbDelete.IsSuccess)
        Diagnostic(events,
            $"partial absorb delete failed: {absorbDelete.Error!.Message.Split('\n')[0]}");
}
```

**6.5. FakeS3 в `WalReceiverCoreTests.cs`** — заменить броски двух методов на рабочие реализации и добавить поле:

```csharp
public readonly HashSet<string> FailListPrefixes = []; // устойчивый отказ list по префиксу
public readonly List<string> DeletedKeys = [];

public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
{
    foreach (var key in keys)
        Objects.Remove(key);
    DeletedKeys.AddRange(keys);
    return Task.FromResult(Result.Success());
}

public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
    string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
{
    var fullPrefix = $"{cluster}/{shard}/{prefix}";
    if (FailListPrefixes.Any(p => fullPrefix.StartsWith(p, StringComparison.Ordinal)))
        return Task.FromResult(Result<IReadOnlyList<WalObject>>.Failed(
            new ApplicationException($"S3 list {fullPrefix}: test transport failure")));
    var objects = Objects.Keys
        .Where(k => k.StartsWith(fullPrefix, StringComparison.Ordinal))
        .Select(k => new WalObject(k[(k.LastIndexOf('/') + 1)..], DateTimeOffset.UnixEpoch))
        .ToList();
    return Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(objects));
}
```

**6.6. Тесты пампа** в `WalReceiverCoreTests.cs` (по образцу существующих: источник-фейк с чанками, Options(); в Options() добавить последний аргумент `PartialIntervalSec: 1` НЕНУЖНО — использовать опциональный таймаут-инъекцию: тестам передавать `retryDelay` короткий; для пампа период прокидывать через options.PartialIntervalSec=1 — конструктор WalReceiverOptions позиционный, последний параметр). Тест-кейсы (AAA):
- `PartialPrefix_Uploaded_LongerThanLying`: стрим шлёт ЧАСТИЧНЫЙ сегмент (чанк < 16 МиБ с long-заголовком + валидной записью, как в задаче 2), затем пауза (источник не завершает) → ждать `FakeS3.Objects` содержит ключ `wal/<seg>.<lsn:x16>.partial` (WaitFor-поллинг ≤5 c); затем ещё чанк с бОльшим префиксом → появляется версия с бОльшим EndLsn.
- `PartialPrefix_ShorterThanLying_Skipped`: предварительно положить в FakeS3.Objects версию с EndLsn выше будущего снимка → после стрима в Objects НЕ появляется ключей с меньшим EndLsn (только лежащая).
- `PartialPrefix_ListFailure_DoesNotBreakStream`: `FailListPrefixes` = префикс листинга → сессия продолжает приём/закрытие сегмента (финал put успешен, памп skip'ает).
- `SlotConfirmation_NotMovedByPartial`: журнал фейка `ConfirmedLsn` — только EndLsn закрытых сегментов (после частичной выкладки подтверждения с той же LSN не происходит).
- `FinalAbsorbsPartialVersions`: после закрытия сегмента (полные 16 МиБ) в FakeS3 его `.partial`-ключи удалены (`DeletedKeys` содержит их, `Objects` — нет).
(Если тайминги нестабильны — уменьшать только ТЕСТОВЫЕ ожидания, не прод-константы.)
(решение Фазы 6: тест-фейк `FakeS3` в `WalReceiverLoopTests.cs` тоже получил рабочие `ListAsync`/`DeleteKeysAsync` — поглощение финалом (6.4) вызывает их в живом контуре цикла; заглушки с `NotSupportedException` роняли сессию после каждого закрытого сегмента. Прод-код не менялся: контракт `IBackupS3` возвращает `Result`, `PoolBackupS3` исключений не бросает.)

---

### Задача 7 (Ф3): дубль `pg_wal` — только закрытые сегменты

**Вход:** задачи 1, 5 в дереве.
**Действие:** `src/PgWorker.PgNode/Operations/FullBackupOperation.cs`: шов sourceFactory + IDENTIFY_SYSTEM + фильтр; тесты в `src/tests/PgWorker.UnitTests/PgNode/FullBackupOperationTests.cs`.
**Выход:** дубль кладёт только сегменты строго ниже активного сегмента источника; отказ identify → FAILED.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~FullBackupOperation" --nologo` — зелёные (HappyPath остаётся зелёным — fake-identity закрывает оба сегмента набора).
**Spec:** §4.3; развилки 11–12, 27.
**Interfaces:** конструктор `FullBackupOperation(..., Func<string, IBackupS3>? targetFactory = null, Func<WalReceiverOptions, IXLogReplicationSource>? sourceFactory = null)`.

- [ ] Шаг: подпункты 7.1–7.3, прогон, коммит `feat(pgnode): дубль pg_wal — только сегменты, закрытые в источнике (t14)`.

Подпункты:

**7.1. `FullBackupOperation.cs`**:
(a) поле `private readonly Func<WalReceiverOptions, IXLogReplicationSource>? _sourceFactory;` + последний параметр конструктора `Func<WalReceiverOptions, IXLogReplicationSource>? sourceFactory = null` с присваиванием (комментарий: тестовый шов IDENTIFY_SYSTEM — образец `_sourceFactory` WalReceiverLoop; прод — null → NpgsqlXLogSource).
(b) шаг 6 (цикл дубля `foreach (var file in Directory.EnumerateFiles(Path.Combine(fullDir, "pg_wal")))`) — ПЕРЕД циклом вставить:

```csharp
// Закрытость сегментов — ПО ИСТОЧНИКУ НА МОМЕНТ ЗАЛИВКИ (arch/19 §2):
// дублируются только сегменты набора СТРОГО ниже текущего активного
// wal-файла источника (IDENTIFY_SYSTEM); последний живой сегмент набора
// остаётся только в full/<id>/pg_wal/ — его финал доставит стрим при
// закрытии, срез гонки «дубль против финала» исключён. Отказ identify —
// операция FAILED (резюмов нет — переснятие новым id, §2).
var identifyOptions = new WalReceiverOptions(
    "127.0.0.1", 5432, "backup_exec", targets.Password!, "postgres",
    targets.Slot!, _identity.Cluster, _identity.Shard,
    writeTargets, pool.ReadAll, bucket, pool.AccessKey, pool.SecretKey);
var identifySource = _sourceFactory is { } factory
    ? factory(identifyOptions)
    : new NpgsqlXLogSource(identifyOptions);
await using (identifySource)
{
    var opened = await identifySource.OpenAsync(token);
    if (!opened.IsSuccess)
    {
        Fail($"IDENTIFY_SYSTEM open failed: {opened.Error!.Message.Split('\n')[0]}");
        return;
    }
    var identified = await identifySource.IdentifySystemAsync(token);
    if (!identified.IsSuccess)
    {
        Fail($"IDENTIFY_SYSTEM failed: {identified.Error!.Message.Split('\n')[0]}");
        return;
    }
    var activeSegId = identified.Value.XLogPos / (ulong)WalFileName.SegmentBytes;
    foreach (var file in Directory.EnumerateFiles(Path.Combine(fullDir, "pg_wal")))
    {
        var name = Path.GetFileName(file);
        if (name.EndsWith(".partial", StringComparison.Ordinal)
            || name.EndsWith(".history", StringComparison.Ordinal))
            continue;
        var normalized = FetchWalCommand.NormalizeSegment(name);
        if (WalFileName.TryParse(normalized) is not { } setSegment)
            continue;
        // Позиция сегмента набора строго ниже активного сегмента источника.
        var setPos = (long)setSegment.Log * WalFileName.SegsPerLog + setSegment.Seg;
        if (setPos >= (long)activeSegId)
        {
            Prog($"wal duplicate skipped: {normalized} is not closed in the source yet");
            continue;
        }
        var key = $"{_identity.Cluster}/{_identity.Shard}/wal/{normalized}";
        var put = s3 is IWalSegmentGuard guard
            ? await guard.PutWalSegmentFileAsync(key, file, token)
            : await s3.PutFileAsync(key, file, token);
        if (!put.IsSuccess)
        {
            Fail(put.Error!.Message.Split('\n')[0]);
            return;
        }
    }
}
```
(старый цикл дубля удалить — он заменён этим; PutWalSegmentFileAsync/нормализация/гвард — прежние).

**7.2. Тестовый фейк источника** — в `FullBackupOperationTests.cs` добавить минимальный фейк:

```csharp
/// <summary>Фейк replication-источника для IDENTIFY_SYSTEM дубля (arch/19 §2):
/// только Open+Identify; Stream/Start не вызываются операцией дубля.</summary>
private sealed class FakeIdentifySource : IXLogReplicationSource
{
    public Result Open = Result.Success();
    public Result<ServerIdentity> Identify = Result<ServerIdentity>.Success(new(1, 0xACUL << 24));
    public Task<Result> OpenAsync(CancellationToken ct) => Task.FromResult(Open);
    public Task<Result> StartReplicationAsync(string slot, ulong startLsn, uint? timeline, CancellationToken ct)
        => throw new NotSupportedException();
    public Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct)
        => Task.FromResult(Identify);
    public IAsyncEnumerable<XLogChunk> Stream { get; } = AsyncEnumerable.Empty<XLogChunk>();
    public Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct)
        => throw new NotSupportedException();
    public Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct)
        => throw new NotSupportedException();
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
```
(`using System.Linq.Async`? — НЕ добавлять пакет: `AsyncEnumerable.Empty` живёт в System.Linq.Async, которого может не быть. Вместо свойства-автосвойства сделать метод-заглушку: `public IAsyncEnumerable<XLogChunk> Stream => throw new NotSupportedException();` — операция дубля Stream не зовёт.)

**7.3. Тесты** в `FullBackupOperationTests.cs` (AAA; хелпер `Create` расширить необязательным параметром `FakeIdentifySource? identify = null`, прокинув `sourceFactory: _ => identify ?? new FakeIdentifySource()`; дефолтный `Identify = new(1, 0xACUL << 24)` — активный сегмент 0xAC=172 > позиций набора 2 и 0xAB=171 — HappyPath/WalDuplicates остаются зелёными):
- `WalDuplicate_LiveSegmentInSource_Skipped` (главный): `identify.Identify = Result<ServerIdentity>.Success(new ServerIdentity(1, 0xABUL << 24))` (активный сегмент 0xAB) → snap succeeded; `PutKeys` содержит `wal/000000010000000000000002` (закрыт: 2 < 171) и НЕ содержит `wal/0000000100000000000000ab` (живой: 171 ≥ 171); `PutKeys` содержит `full/<id>/pg_wal/0000000100000000000000AB` (набор 1:1 не тронут).
- `IdentifyFailed_FailedWithOneLineError`: `identify.Identify = Result<ServerIdentity>.Failed(new ApplicationException("identify boom"))` → snap failed, Error содержит "IDENTIFY_SYSTEM".
- `IdentifyOpenFailed_Failed`: `identify.Open = Result.Failed(...)` → snap failed.

---

### Задача 8 (Ф3): verify-полконтроль — union wal/ + набора

**Вход:** задача 3 в дереве.
**Действие:** одна правка вызова в `src/PgWorker.Backups/Process/BackupVerifyProcess.cs` (строка ~215).
**Выход:** end-сегмент набора валидируется из `full/<id>/pg_wal/` (юнит-семантика — тест 3.3 `CheckRange_EndOnlyInSet_Closes`).
**Проверка:** сборка Release; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalChain|FullyQualifiedName~BackupVerify" --nologo`.
**Spec:** §4.4; развилка 13.

- [ ] Шаг: подпункт 8.1, сборка, коммит `fix(backups): verify-цепь — union имён wal/ и pg_wal набора (t14)`.

Подпункт:

**8.1.** Заменить вызов:

```csharp
var check = WalChain.CheckRange(
    start.Value, endSegment, walList.Value.Select(o => o.Name), historyContents);
```

на:

```csharp
// UNION имён wal/ и pg_wal набора (arch/19 §5 п.2: «list wal/ + list
// full/<id>/pg_wal/»): последний сегмент набора может быть живым в источнике
// на момент заливки и НЕ дублироваться в wal/ (§2 t14) — диапазон до точки
// бэкапа закрывается объектами набора; сегменты выше end игнорирует Walk.
var check = WalChain.CheckRange(
    start.Value, endSegment,
    walList.Value.Select(o => o.Name)
        .Concat(setList.Value.Select(o => o.Name)),
    historyContents);
```

---

### Задача 9 (Ф4): контроль цепи — хвост TailOf

**Вход:** задача 3 в дереве.
**Действие:** одна правка блока фактов в `src/PgWorker.Backups/WalStreamProcess.cs` (`ControlDueAsync`, строки ~628-640).
**Выход:** `last_uploaded_*`/lag/«тишина» — от максимального валидного префикса; формат etcd-ключа не меняется.
**Проверка:** сборка Release; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalChain|FullyQualifiedName~WalStream|FullyQualifiedName~WalStatusWriter" --nologo`.
**Spec:** §4.5; развилка 14.

- [ ] Шаг: подпункт 9.1, сборка, коммит `feat(backups): контроль цепи — хвост потока финал/лучший .partial (t14)`.

Подпункт:

**9.1.** В `ControlDueAsync` заменить блок (комментарий «Факты прогресса — только из наблюдений...» и 4 строки вычислений) на:

```csharp
// Факты прогресса — только из наблюдений: хвост потока (arch/19 §3) —
// финальный объект цепочки ИЛИ лучший .partial (версии схлопнуты по
// сегменту WalChain.TailOf); цепочка не наблюдалась — прошлый ключ,
// НИКОГДА не now() (ревью Ф4-2 №2). last_uploaded_segment — имя сегмента
// БЕЗ суффикса (формат etcd-ключа не меняется, парсеры панели толерантны).
var tail = WalChain.TailOf(chain.LastSegment, objects.Select(o => o.Name));
var lastUploadedName = tail?.SegmentName ?? wal?.LastUploadedSegment;
var observedLastUnix = tail is { } tailFact
    ? objects.Where(o => o.Name == tailFact.ObjectName).Select(o => o.LastModified)
        .Cast<DateTimeOffset?>().FirstOrDefault()?.ToUnixTimeSeconds()
    : null;
var lastUploadedUnix = observedLastUnix ?? wal?.LastUploadedUnix;
```
(строки ниже — `if (lastUploadedName is null || lastUploadedUnix is null)`, lag-зонд и запись — НЕ менять: `WalFileName.TryParse(lastUploadedName)` парсит имя сегмента без суффикса).

---

### Задача 10 (Ф4): fetch-wal — лучший `.partial`

**Вход:** задачи 1, 8 в дереве.
**Действие:** `src/PgWorker.PgNode/Operations/FetchWalCommand.cs`: чистая `BestPartialKey` + перебор листингов; тесты — в `src/tests/PgWorker.UnitTests/PgNode/RestoreOperationTests.cs` (рядом с существующими FetchWal-тестами, строки ~329+); правка фейка `FakeFetchS3` (метод `ListAsync` — подпункт 10.3): новое тело `FetchOnceAsync` после NotFound-финалов зовёт `ListAsync`, а фейк его бросает — существующие кейсы `FetchOnce_*` обязаны остаться зелёными на месте.
**Выход:** restore_command докатывается до конца лучшего `.partial`; транспорт/дедлайн прежние.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~FetchWal|FullyQualifiedName~FetchOnce" --nologo` — зелёные (вкл. существующие `FetchOnce_NotFoundEverywhere_IsEndOfWal` / `FetchOnce_TransportFailure_IsDistinguishable`).
**Spec:** §4.6; развилка 16.

- [ ] Шаг: подпункты 10.1–10.3, прогон, коммит `feat(pgnode): fetch-wal — лучший .partial как хвост потока (t14)`.

Подпункты:

**10.1. `FetchWalCommand.cs`** — заменить тело `FetchOnceAsync` (сигнатура прежняя) и добавить два метода:

```csharp
/// <summary>Чистая функция для тестов: лучший (длиннейший по EndLsn)
/// .partial-ключ из списка имён; null — версий нет.</summary>
internal static string? BestPartialKey(IEnumerable<string> names)
{
    string? best = null;
    ulong bestLsn = 0;
    foreach (var name in names)
        if (PgWorker.Backups.WalPartialName.TryParse(name) is { } v
            && v.EndLsn >= bestLsn)
        {
            bestLsn = v.EndLsn;
            best = name;
        }
    return best;
}

/// <summary>Лучший .partial по ВСЕМ нодам пула (лаг mesh допускается —
/// arch/19 §5): перебор листингов узкого префикса; транспортный отказ ноды
/// пропускает ноду. Возврат: (ключ, есть ли хоть один ответивший, первая
/// транспортная ошибка) — никто не ответил ⇒ транспорт, не «конец WAL».</summary>
private static async Task<(string? Key, bool AnyResponded, string? FirstError)> ListBestPartialAsync(
    IReadOnlyList<IBackupS3> clients, string cluster, string shard,
    string normalizedSegment, CancellationToken ct)
{
    string? best = null;
    var anyResponded = false;
    string? firstError = null;
    foreach (var client in clients)
    {
        var listed = await client.ListAsync(
            cluster, shard, $"wal/{normalizedSegment}.", ct: ct);
        if (!listed.IsSuccess)
        {
            firstError ??= listed.Error!.Message.Split('\n')[0];
            continue;
        }
        anyResponded = true;
        if (BestPartialKey(listed.Value.Select(o => o.Name)) is not { } candidate)
            continue;
        if (best is null
            || PgWorker.Backups.WalPartialName.TryParse(candidate)!.EndLsn
               > PgWorker.Backups.WalPartialName.TryParse(best)!.EndLsn)
            best = candidate;
    }
    return (best, anyResponded, firstError);
}

// Новое тело FetchOnceAsync: финал как раньше, затем .partial.
internal static async Task<(int ExitCode, string? TransportError)> FetchOnceAsync(
    IReadOnlyList<IBackupS3> clients, string cluster, string shard,
    string segment, string target, CancellationToken ct)
{
    string? transportError = null;
    var normalized = NormalizeSegment(segment);
    foreach (var client in clients)
    {
        var got = await client.GetObjectToFileAsync(
            cluster, shard, $"wal/{normalized}", target, ct);
        if (got.IsSuccess)
            return (0, null);
        if (!IsNotFound(got))
            transportError ??= got.Error!.Message.Split('\n')[0];
    }

    // Финала нет нигде — лучший .partial (хвост потока, arch/19 §3.5):
    // скачанный префикс валиден по построению — postgres остановится в
    // конце записей; ничего нет нигде ⇒ конец WAL (exit 1).
    var (bestKey, anyResponded, listError) = await ListBestPartialAsync(
        clients, cluster, shard, normalized, ct);
    if (!anyResponded)
        return (1, listError ?? "wal partial listing failed on all nodes");
    if (bestKey is { } partial)
    {
        foreach (var client in clients)
        {
            var got = await client.GetObjectToFileAsync(
                cluster, shard, $"wal/{partial}", target, ct);
            if (got.IsSuccess)
                return (0, null);
            if (!IsNotFound(got))
                transportError ??= got.Error!.Message.Split('\n')[0];
        }
    }
    return (1, transportError);
}
```

**10.2. Тесты** в `RestoreOperationTests.cs` (AAA):
- `FetchWal_BestPartialKey_LongestWins`: имена трёх версий (EndLsn 0x10/0xFF/0x9) → `BestPartialKey` — ключ с 0xFF.
- `FetchWal_BestPartialKey_EmptyNull`: пустой список/только финал → null.
- `FetchWal_FetchOnce_FallsBackToPartial`: два фейк-клиента `IBackupS3` (локальный мини-фейк: GetObjectToFileAsync возвращает NotFound `"<key>: not found"` для финала, успех для `.partial`-ключа; ListAsync отдаёт одну версию) → `(0, null)` и файл записан (фейк пишет в target) — build фейка по образцу `IsNotFound`-текста ошибки `": not found"`.
- `FetchWal_FetchOnce_NothingAnywhere_EndOfWal`: оба клиента NotFound на финал, ListAsync пуст → `(1, null)`.

**10.3. Фейк `FakeFetchS3` в том же файле** (`RestoreOperationTests.cs`, строки ~404-420): метод `ListAsync` после правки 10.1 вызывается НОВЫМ телом `FetchOnceAsync` на пути «финала нет нигде» — заменить бросок `NotSupportedException` на пустой успешный листинг:

```csharp
public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
    string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    // версий .partial нет — конец WAL (fetch-wal t14: ListAsync позволяется
    // после NotFound-финалов; пустой список = «лучшей версии нет»)
    => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));
```

(существующие кейсы `FetchOnce_NotFoundEverywhere_IsEndOfWal` / `FetchOnce_TransportFailure_IsDistinguishable` остаются зелёными: первый получает `(1, null)` через пустой ListAsync — конец WAL; транспортная семантика второго не затронута — ListAsync транспортом не падает).
(решения Фазы 6: (1) в `ListBestPartialAsync` вместо `TryParse(...)!.EndLsn` — `TryParse(...)!.Value.EndLsn`: у `Nullable<ValueTuple>` оператор `!` тип не разворачивает, план-код не компилировался; (2) отдельный тест `FetchWal_FetchOnce_NothingAnywhere_EndOfWal` не заводился — его контракт (оба клиента NotFound на финал, пустой ListAsync → `(1, null)`) уже покрыт существующим `FetchOnce_NotFoundEverywhere_IsEndOfWal` на том же пути; дубль не добавлялся.)

---

### Задача 11 (Ф5): ретенция — `.partial` тем же проходом

**Вход:** задача 4 в дереве.
**Действие:** правка шага (4) `TickShardAsync` в `src/PgWorker.Backups/Retention/RetentionProcess.cs` (строки ~145-170): list `wal/` ВЫНОСИТСЯ за пределы cutoff-условия — `SelectPartialForDeletion(names, cutoff)` вызывается ВСЕГДА (при cutoff=null чистит поглощённые/устаревшие версии), `SelectWalForDeletion` — только при cutoff (гвард verify: нет OK-полного ⇒ финалы не режем); оба списка — в ОДИН batch-delete. Юнит-кейсы композиции — в `src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs`.
**Выход:** ретенционный batch-delete включает мусор `.partial` при ЛЮБОМ cutoff (в т.ч. null); юнит фиксирует контракт «финалы ниже cutoff + `.partial`-гигиена одним списком» (критерий приёмки spec §8.4).
**Проверка:** сборка; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~Retention" --nologo`.
**Spec:** §4.7; развилка 17 (cutoff=null ⇒ только поглощённые/версии — финалы НЕ режутся).

- [ ] Шаг: подпункты 11.1–11.2, сборка, коммит `feat(backups): ретенция чистит .partial этим же проходом (t14)`.

Подпункты:

**11.1.** В `TickShardAsync` заменить блок шага (4) ЦЕЛИКОМ — от строки `if (RetentionPlanner.LatestVerifiedWalStart(remaining) is { } cutoff)` до закрывающей `}` его тела (включая вложенные `listed`/`doomed`/`keys`/`deleted`/journal) — на (переменная `remaining` уже вычислена выше по методу, НЕ переопределять; переменная `policy` тоже выше):

```csharp
// (4) Чистка WAL (arch/19 §4 п.5): cutoff = wal_start новейшего
// (по started_unix) verify-OK COMPLETED-полного из оставляемых — ОДНА
// точка с chain_start контроля (§3). ГВАРД verify: прунинг ФИНАЛОВ без
// успешного verify нового полного запрещён (OK-полного нет ⇒ финалы не
// режем); .partial-гигиена (поглощённые финалом, устаревшие версии) —
// ВСЕГДА, тем же list'ом и тем же batch-delete («этого же прохода», §4 п.5:
// при cutoff=null list выполняется, чистятся только .partial).
var listed = await S3Pulse.CallAsync(
    progress, token => s3.ListPrefixAsync($"{cluster}/{shard}/wal/", ct: token), ct);
if (!listed.IsSuccess)
    throw new ApplicationException($"list wal: {listed.Error!.Message}");
var names = listed.Value.Select(o => o.Key.Split('/')[^1]).ToList();
var doomed = (RetentionPlanner.LatestVerifiedWalStart(remaining) is { } cutoff
        ? RetentionPlanner.SelectWalForDeletion(names, cutoff) : [])
    .Concat(RetentionPlanner.SelectPartialForDeletion(names, cutoff))
    .ToList();
if (doomed.Count > 0)
{
    var keys = doomed.Select(n => $"{cluster}/{shard}/wal/{n}").ToList();
    var deleted = await S3Pulse.CallAsync(
        progress, token => s3.DeleteKeysAsync(keys, token), ct);
    if (!deleted.IsSuccess)
        throw new ApplicationException($"delete wal: {deleted.Error!.Message}");
    await journal.WritePhaseAsync(cluster, Op, $"wal-trimmed/{shard}/{doomed.Count}",
        claims.InstanceId, null, ct);
}
```
(прежний код блока удаляется полностью — включая старый комментарий шага (4); нумерация комментария «(5) Гигиена FAILED» ниже по методу НЕ меняется.)
(решение Фазы 6: cutoff вычисляется один раз в локальную `var cutoff = LatestVerifiedWalStart(remaining)` с отдельным `cutoff is { } cut` в тернарнике — pattern-переменная из условия в `.Concat(...)` вне ветки не определена (CS0165), план-код в этом виде не компилировался; семантика шага не менялась.)

**11.2. Юнит-кейсы** — добавить в `RetentionPlannerTests.cs` (фиксируют композицию шага 11.1 — контракт объединения списков; сборка в RetentionProcess — механическая). Общий Arrange-хелпер кейсов (в классе теста):

```csharp
// Листинг wal/ одного прохода ретенции: финалы seg1/seg2 (ниже cutoff seg3),
// финал cutoff-сегмента seg3, поглощённые .partial seg3 (финал лежит) и
// версии живого seg4 (длиннейшая остаётся).
private static readonly WalFileName Seg1 = new(1, 0, 1);
private static readonly WalFileName Seg2 = new(1, 0, 2);
private static readonly WalFileName Seg3 = new(1, 0, 3);
private static readonly WalFileName Seg4 = new(1, 0, 4);

private static List<string> RetentionNames() =>
[
    Seg1.Name, Seg2.Name, Seg3.Name,
    WalPartialName.Key(Seg3.Name, 0x100),
    WalPartialName.Key(Seg3.Name, 0x200), // поглощённые финалом seg3
    WalPartialName.Key(Seg4.Name, 0x100),
    WalPartialName.Key(Seg4.Name, 0x300), // живой сегмент: остаётся 0x300
];

// Композиция шага (4) RetentionProcess (план 11.1): финалы ниже cutoff
// (только при cutoff) + .partial-гигиена (всегда) — один список doomed.
private static IReadOnlyList<string> RetentionDoomed(List<string> names, WalFileName? cutoff)
    => (cutoff is { } limit
            ? RetentionPlanner.SelectWalForDeletion(names, limit) : [])
        .Concat(RetentionPlanner.SelectPartialForDeletion(names, cutoff))
        .ToList();
```

- `RetentionPass_PartialsAndFinalsDoomedTogether` (AAA): `RetentionDoomed(RetentionNames(), Seg3)` → содержит `Seg1.Name`, `Seg2.Name` (финалы ниже cutoff), обе версии `.partial` seg3 (поглощённые) и ровно одну версию seg4 (`…0100…`, короткую); НЕ содержит `Seg3.Name` (сам cutoff жив).
- `RetentionPass_CutoffNull_OnlyPartialHygiene` (AAA): `RetentionDoomed(RetentionNames(), null)` → результат эквивалентен `SelectPartialForDeletion(RetentionNames(), null)`: финалы (`Seg1/2/3.Name`) НЕ doomятся, поглощённые версии seg3 — да, seg4 — только короткая (гвард verify: без OK-полного финалы не режем — развилка 17).

---

### Задача 12 (Ф5): метрики wal-sweeper

**Вход:** задача 0 в дереве.
**Действие:** `src/Shared.Metrics/Worker/WorkerMetricsInstrumentation.cs`: gauge + 2 метода + DebugSnapshot-поле; тест в `src/tests/Shared.Metrics.UnitTests/WorkerMetricsInstrumentationTests.cs`.
**Выход:** per-shard серия свежести эмитится и снимается.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Metrics.UnitTests --nologo`.
**Spec:** §4.8 (метрики); развилки 20, 31, 33.

- [ ] Шаг: подпункты 12.1–12.3, прогон, коммит `feat(metrics): per-shard свежесть wal-sweeper (t14)`.

Подпункты:

**12.1.** В стейт-секции (рядом с `_walLag`) добавить `private readonly Dictionary<(string Cluster, string Shard), long> _walSweepShardUnix = [];`; в конструкторе рядом с gauge `pgworker_backup_wal_lag_segments` добавить:

```csharp
meter.CreateObservableGauge(
    "worker.wal_sweep.shard_last_success_timestamp_seconds",
    () => Measure(() => _walSweepShardUnix.Select(kv =>
        new Measurement<long>(kv.Value,
            new KeyValuePair<string, object?>("cluster", kv.Key.Cluster),
            new KeyValuePair<string, object?>("shard", kv.Key.Shard)))),
    unit: "s", description: "Unix timestamp of the last successful wal-sweeper pass per shard (arch/19 §4)");
```

**12.2.** Методы (рядом с `BackupWalLag`, тот же lock/try-паттерн; «пассивный наблюдатель»):

```csharp
// Gauge worker.wal_sweep.shard_last_success_timestamp_seconds{cluster,shard}:
// свежесть per-shard прохода wal-sweeper (arch/19 §4); unix=null — серия
// исчезает (образец BackupWalLag).
public void WalSweepShardUnix(string cluster, string shard, long? unix)

// Чистка серий шардов вне живых (иначе WalSweepShardStale ложно горит на
// удалённом шарде): проход вызывает по факту полного перебора.
public void WalSweepShardRetain(IReadOnlySet<(string Cluster, string Shard)> live)
```
(тела: под `_lock` — set/remove; Retain: удалить все ключи словаря, не входящие в `live`, материализуя список ключей до удаления).

**12.3.** DebugSnapshot-тип (private record ~строка 691) дополнить полем `IReadOnlyDictionary<(string Cluster, string Shard), long> WalSweepShardUnix` и включить в построение снапшота. Тест `WalSweepShard_UnixAndRetain` (AAA): set 2 шардов → snapshot содержит оба; `WalSweepShardRetain` с одним живым → остался один; `WalSweepShardUnix(cluster, shard, null)` → серия удалена.

---

### Задача 13 (Ф5): WalSweeperProcess

**Вход:** задачи 4, 12 в дереве.
**Действие:** новый `src/PgWorker.Backups/Supervisor/WalSweeperProcess.cs` + тест `src/tests/PgWorker.UnitTests/Backups/WalSweeperProcessTests.cs`.
**Выход:** проход глобального лидера чистит мусор `wal/` per-shard (один batch-delete на шард), метрики, независимость шардов.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalSweeper" --nologo`.
**Spec:** §4.8; развилки 18–19, 32.
**Interfaces:** `WalSweeperProcess(IEtcdGateway, string[], BackupS3Factory, ClaimStore, WorkJournal, Func<BackupsRuntimeOptions?>, TimeProvider, ILogger?, WorkerMetricsInstrumentation?)`; `Task<Result> SweepAsync(CancellationToken)`; `internal Task<Result> SweepShardsAsync(IReadOnlyList<ClusterSnapshot>, IReadOnlyDictionary<string, ClusterBackups?>, long nowUnix, CancellationToken)`.

- [ ] Шаг (сильная модель): реализация 13.1 — код каркаса в подпункте; сверка компиляции using-ов по образцу `BackupOrphanSweeper.cs` + `RetentionProcess.cs`.
- [ ] Шаг: тесты 13.2, прогон, коммит `feat(backups): WalSweeperProcess — фоновая чистка wal/ per-shard (t14)`.

Подпункты:

**13.1. Создать `src/PgWorker.Backups/Supervisor/WalSweeperProcess.cs`** (using-набор: `Microsoft.Extensions.Logging`, `PgWorker.Core`, `PgWorker.Core.Model`, `PgWorker.Etcd.Parsing`, `PgWorker.Provisioning.Processes`, `Shared.Core`, `Shared.Etcd.Client`, `Shared.Metrics.Worker`; полный код):

```csharp
namespace PgWorker.Backups.Supervisor;

/// <summary>Фоновая чистка мусора префикса wal/ (arch/19 §4 «wal-sweeper»):
/// ТОЛЬКО глобальный лидер /pgworker/leader; per-shard независимо (отказ/пауза
/// шарда не останавливает остальные); листинг — соло, первая доступная нода
/// (инвентарная семантика §5, повторные проходы досматривают лаг mesh).
/// Удаляет: финальные сегменты ниже cutoff (cutoff = wal_start новейшего
/// verify-OK COMPLETED из оставляемых GFS-отбором — «по факту ретенционного
/// отбора», повтор RetentionPlanner; нет OK-полного ⇒ срез ниже cutoff не
/// выполняется) и .partial-гигиену (SelectPartialForDeletion: поглощённые
/// финалом, устаревшие версии, ниже cutoff). Список на удаление — ОДИН
/// batch-delete на шард (идемпотентно). Backups:Enabled=false ⇒ no-op.
/// Наблюдаемость: worker_loop_*{loop="wal-sweeper"} (цикл) + per-shard серия
/// WalSweepShardUnix (arch/18 §2.2).</summary>
public sealed class WalSweeperProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    BackupS3Factory s3Factory,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    TimeProvider time,
    ILogger? logger = null,
    WorkerMetricsInstrumentation? metrics = null)
{
    private const string Op = "backup-wal-sweeper";

    public async Task<Result> SweepAsync(CancellationToken ct)
    {
        // Гварды: только глобальный лидер; выключено — no-op (как ретенция).
        if (!claims.IsLeader)
            return Result.Success();
        if (runtime() is not { } options)
            return Result.Success();

        var range = await RangeAsync("/clusters/", ct);
        if (!range.IsSuccess)
            return Result.Failed(range.Error!);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        if (!parsed.IsSuccess)
            return Result.Failed(parsed.Error!);

        var backupsByCluster = new Dictionary<string, ClusterBackups?>();
        foreach (var snap in parsed.Value)
            backupsByCluster[snap.Config.Cluster] = await ReadBackupsAsync(snap.Config.Cluster, ct);

        return await SweepShardsAsync(parsed.Value, backupsByCluster,
            time.GetUtcNow().ToUnixTimeSeconds(), ct);
    }

    /// <summary>Ядро прохода (юнит-точка входа): снапшоты кластеров + их
    /// бэкап-статусы; per-shard cutoff/листинг/batch-delete; завершение —
    /// снятие per-shard серий исчезнувших шардов.</summary>
    internal async Task<Result> SweepShardsAsync(
        IReadOnlyList<ClusterSnapshot> snaps,
        IReadOnlyDictionary<string, ClusterBackups?> backupsByCluster,
        long nowUnix, CancellationToken ct)
    {
        var options = runtime();
        if (options is null)
            return Result.Success();

        var live = new HashSet<(string Cluster, string Shard)>();
        foreach (var snap in snaps)
        {
            if (snap.Config.State == ClusterState.ToRemove)
                continue;
            var poolResult = await s3Factory.ResolveAsync(snap.Config.Cluster, ct);
            if (!poolResult.IsSuccess || poolResult.Value is null)
            {
                // Пул не выбран/Failed — шард skip (ретенция молчит симметрично).
                logger?.LogWarning("{Op} {Cluster}: pool not resolved — shards skipped",
                    Op, snap.Config.Cluster);
                continue;
            }
            var backups = backupsByCluster.GetValueOrDefault(snap.Config.Cluster);
            var policy = backups?.Policy ?? new BackupPolicy(
                options.PolicyRetentionDays, options.PolicyRetentionWeeks,
                options.PolicyRetentionMonths, options.FullMaxAgeSec, options.VerifyOnCreate);

            foreach (var shard in snap.Shards)
            {
                if (shard.ToRemove)
                    continue;
                live.Add((snap.Config.Cluster, shard.Name));
                try
                {
                    await SweepShardAsync(snap.Config.Cluster, shard.Name,
                        backups?.Shards.GetValueOrDefault(shard.Name), policy, options,
                        poolResult.Value, nowUnix, ct);
                    metrics?.WalSweepShardUnix(snap.Config.Cluster, shard.Name, nowUnix);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // Ошибка шарда не валит остальные (образец WalStreamProcess).
                    logger?.LogError(ex, "{Op} {Cluster}/{Shard}: {Message}",
                        Op, snap.Config.Cluster, shard.Name, ex.Message);
                    await journal.WritePhaseAsync(snap.Config.Cluster, Op,
                        $"shard-error/{shard.Name}", claims.InstanceId, ex.Message, ct);
                }
            }
        }

        metrics?.WalSweepShardRetain(live);
        return Result.Success();
    }

    // Один шард: list wal/ → cutoff (GFS-отбор оставляемых ретенции) →
    // doomed = финалы ниже cutoff + .partial-гигиена → ОДИН batch-delete.
    private async Task SweepShardAsync(
        string cluster, string shard, ShardBackups? shardBackups, BackupPolicy policy,
        BackupsRuntimeOptions options, BackupPoolContext pool, long nowUnix, CancellationToken ct)
    {
        var s3 = s3Factory.Create(pool);
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list wal: {listed.Error!.Message}");
        var names = listed.Value.Select(o => o.Name).ToList();

        // Cutoff = wal_start новейшего verify-OK COMPLETED из ОСТАВЛЯЕМЫХ
        // (SelectKeep — тот же отбор, что у ретенции; кандидат Delete[0] не
        // якорит — arch/19 §4 п.5). Нет OK-полного ⇒ cutoff null.
        WalFileName? cutoff = null;
        if (shardBackups?.Full is { Count: > 0 } fulls)
        {
            var selection = RetentionPlanner.SelectKeep(fulls, policy, nowUnix);
            var remaining = fulls.Where(f =>
                f.State == FullBackupStatus.Completed
                && !(selection.Delete.Count > 0 && f.Id == selection.Delete[0])).ToList();
            cutoff = RetentionPlanner.LatestVerifiedWalStart(remaining);
        }

        var doomed = (cutoff is { } limit
                ? RetentionPlanner.SelectWalForDeletion(names, limit) : [])
            .Concat(RetentionPlanner.SelectPartialForDeletion(names, cutoff))
            .ToList();
        if (doomed.Count == 0)
            return;
        var keys = doomed.Select(n => $"{cluster}/{shard}/wal/{n}").ToList();
        var deleted = await s3.DeleteKeysAsync(keys, ct);
        if (!deleted.IsSuccess)
            throw new ApplicationException($"delete wal garbage: {deleted.Error!.Message}");
        await journal.WritePhaseAsync(cluster, Op, $"wal-swept/{shard}/{doomed.Count}",
            claims.InstanceId, null, ct);
    }

    // Снапшот бэкапов кластера: Range /pgworker/backups/<C>/ → BackupsParser;
    // отказ — null (transient: проход без cutoff-ветки, только .partial-гигиена).
    private async Task<ClusterBackups?> ReadBackupsAsync(string cluster, CancellationToken ct)
    {
        var range = await RangeAsync($"/pgworker/backups/{cluster}/", ct);
        if (!range.IsSuccess)
            return null;
        var parsed = BackupsParser.Parse(range.Value, out _);
        return parsed.IsSuccess
            ? parsed.Value.FirstOrDefault(b => b.Cluster == cluster)
            : null;
    }

    // Failover-Range (образец BackupOrphanSweeper.RangeClustersAsync).
    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var range = await etcd.RangeAsync(endpoint, prefix, ct);
            if (range.IsSuccess)
                return range;
            last = range;
        }
        return Result<IReadOnlyList<Kv>>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints not configured"));
    }
}
```
ПРИМЕЧАНИЕ СИЛЬНОЙ МОДЕЛИ (шаг): `FullBackupStatus` — `using PgWorker.Backups.Job;`; `EtcdUnreachableException` — `Shared.Etcd.Client`; `ClusterSnapshotParser`/`ClusterState`/`Kv` — сверить по `BackupOrphanSweeper.cs` (те же using) и добавить недостающие. Если `BackupPolicy` имеет другой набор позиционных параметров — сверить с вызовом в `RetentionProcess.cs:83` и использовать его.

**13.2. Тесты** — создать `src/tests/PgWorker.UnitTests/Backups/WalSweeperProcessTests.cs`:
фикстуры: `Fakes.FakeEtcd` (PgWorker.UnitTests.Provisioning; для SweepShardsAsync не нужен), `new WorkJournal("/pgworker", fakeEtcd, ["http://etcd:2379"])`, ClaimStore — не-лидер проверяется отдельным тестом публичного SweepAsync (по образцу BackupOrphanSweeperLoopTests: `new ClaimStore("/pgworker", endpoints, etcd, TimeProvider.System)`, `claims.IsLeader == false` → SweepAsync → Success без вызовов S3). Для SweepShardsAsync — фабрика: скопировать `TestPoolFactory.Single()` в локальный хелпер с заменой `targetFactory: _ => fakeS3` (фейк из 6.5-паттерна: словарь объектов, ListWalAsync/ListAsync/DeleteKeysAsync рабочие); `runtime: () => new BackupsRuntimeOptions(Enabled: true)`; снапшот кластера — по образцу `src/tests/PgWorker.IntegrationTests/Backups/BackupSupervisorProcessTests.cs:197` (`new ClusterSnapshot(new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active), [shards], ...)` — арность сверить по месту). ShardBackups с full-статусами — construct по образцу парсера (etcd-ключ `full/<id>` JSON) ЛИБО проще: положить в FakeEtcd ключи `/pgworker/backups/c1/backups`... — СВЕРИТЬ как ShardBackups строится тестами `BackupProcessTests` (если прямое конструирование record невозможно — через FakeEtcd-ключи `full/<id>` в JSON-формате `BackupStatusJson`); сильная модель выбирает рабочий путь по существующим тестам и НЕ меняет прод-код ради теста.
Тест-кейсы (AAA):
- `NotLeader_NoOp`: claims не-лидер → SweepAsync Success; фейк S3 не получил list/delete.
- `Disabled_NoOp`: runtime() == null → SweepShardsAsync Success без вызовов.
- `AbsorbedAndStalePartials_DeletedOneBatch`: объекты: финал seg5, `.partial`×2 seg5 (поглощённые), `.partial`×3 seg6 (остаётся длиннейшая), финалы seg1..2 (без cutoff — нет verify-OK полных: backups пуст) → DeletedKeys ровно один batch-вызов с 4 ключами (2+2), финалы НЕ удалены.
- `BelowCutoffFinalsDeleted`: ShardBackups с COMPLETED+verify OK полным (wal_start seg3) + финалы seg1,seg2,seg3,`.partial` seg2 → удалены seg1, seg2 (финал) и `.partial` seg2; seg3 жив.
- `ShardError_DoesNotBreakOthers`: targetFactory-фейк с программируемым отказом list по одному шарду (префикс кластера A) → проход Success; шард B удалён, A — journal sharding-error (journal-фазы читаются через WorkJournal → FakeEtcd ключи `/pgworker/journal/...` — сверить по существующим тестам журнала; если чтение фаз из фейка громоздко — ассерт только «B удалён, исключение не покинуло SweepShardsAsync»).
  (решения Фазы 6: (1) тестовая фабрика пула собирается с `breakerWindow: TimeSpan.Zero` — иначе отказ листинга шарда A открывает breaker ноды пула и шард B получает breaker-отказ вместо прохода (проверялся бы breaker, а не изоляция ошибки шарда); (2) journal агрегирует ОДНУ фазу на кластер (последняя запись побеждает) — факт `shard-error/s1` перекрывается успехом `wal-swept/s2/1`, поэтому ассерт — по плану-фолбэку: проход Success + B вычищен, а также journal-фаза здорового шарда.)

---

### Задача 14 (Ф5): WalSweeperLoop + врезка в Program.cs

**Вход:** задачи 12, 13 в дереве.
**Действие:** новый `src/PgWorker.App/Loops/WalSweeperLoop.cs`; врезка DI в `src/PgWorker.App/Program.cs` (по образцу BackupOrphanSweeperLoop, строки ~816-838); тест в `src/tests/PgWorker.UnitTests/App/` (по образцу BackupOrphanSweeperLoopTests).
**Выход:** фоновый цикл глобального лидера (60 с) с метриками `loop="wal-sweeper"`.
**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --filter "FullyQualifiedName~WalSweeper" --nologo`; сборка Release.
**Spec:** §4.8; развилки 18, 34.

- [ ] Шаг (сильная модель): врезка DI в Program.cs — зависимые сервисы те же, что у BackupOrphanSweeperLoop (IEtcdGateway, BackupS3Factory, ClaimStore, WorkJournal, runtime-функтор BackupsRuntimeOptions, TimeProvider, WorkerMetricsInstrumentation); сверить фактические DI-выражения соседних строк и повторить.
- [ ] Шаг: подпункты 14.1–14.2, прогон, коммит `feat(app): WalSweeperLoop — цикл глобального лидера 60 с (t14)`.

Подпункты:

**14.1. Создать `src/PgWorker.App/Loops/WalSweeperLoop.cs`** (using-набор — как у BackupOrphanSweeperLoop; internal sealed class : BackgroundService):

```csharp
/// <summary>Лидерный фоновый цикл чистки wal/ (arch/19 §4 «wal-sweeper»):
/// глобальный лидер /pgworker/leader выполняет WalSweeperProcess.SweepAsync
/// раз в 60 с (канон: per-shard оценка .partial-гигиены НЕ РЕЖЕ раза в
/// минуту); не-лидер пере-захватывает лидерство и ждёт Loops:ScanIntervalSec.
/// Backups:Enabled=false — серию LoopTick не пишем (живость выключенной
/// подсистемы не алертится — паттерн orphan-sweep, arch/18 §2.2). Потеря
/// leader-lease прерывает проход (гвард arch/17 S5). НЕ входит в
/// LoopsVitality/healthz: живость — только метрики LoopTick/LoopDuration.</summary>
internal sealed class WalSweeperLoop(
    IOptionsMonitor<PgWorkerOptions> options,
    ClaimStore claims,
    WalSweeperProcess sweeper,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics,
    ILogger<WalSweeperLoop> logger) : BackgroundService
{
    // Канон arch/19 §4: оценка per-shard не реже раза в минуту (константа,
    // internal — инъекция тестам).
    internal static readonly TimeSpan SweepInterval = TimeSpan.FromSeconds(60);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation(
                            "leadership acquired (wal-sweeper): {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader && options.CurrentValue.Backups.Enabled)
                {
                    // Cancellation-гвард лидерства (arch/17 S5).
                    using var leaderLease = CancellationTokenSource.CreateLinkedTokenSource(
                        stoppingToken, claims.LeaderToken);
                    var tick = leaderLease.Token;
                    var startedAt = Stopwatch.GetTimestamp();
                    var result = await sweeper.SweepAsync(tick);
                    metrics.LoopDuration("wal-sweeper", Stopwatch.GetElapsedTime(startedAt).TotalSeconds);
                    metrics.LoopTick("wal-sweeper", ok: result.IsSuccess);
                    if (!result.IsSuccess)
                        logger.LogWarning("wal-sweeper: {Error}", result.Error?.Message);
                    await Task.Delay(SweepInterval, tick);
                }
                else
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                        stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // штатная остановка
            }
            catch (OperationCanceledException)
            {
                // Потеря leader-lease — пауза скана, пере-захват следующим тиком.
                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                        stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "wal-sweeper loop: {Message}", ex.Message);
                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec),
                        stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }
}
```
(`using System.Diagnostics;` для Stopwatch.)

**14.2. Тест** — создать `src/tests/PgWorker.UnitTests/App/WalSweeperLoopTests.cs` (по образцу BackupOrphanSweeperLoopTests.Create: FakeEtcd, FixedOptionsMonitor с `Backups = new BackupsOptions { Enabled = true }`, ClaimStore, WalSweeperProcess с `UnitPoolFactory.Disabled()`-фабрикой и `() => new BackupsRuntimeOptions(Enabled: true)`, metrics на своём Meter; НО лидерство ClaimStore в юните не захватится без etcd-lease — потому тест цикла строит так: `Enabled=false` → цикл тихо тикает (LoopTick НЕ пишется), выполняется без исключений N секунд и отменяется — ассерт «не упал/не написал серию»; полный лидерный путь покрыт E2E). Один кейс `[Fact] WalSweeperLoop_DisabledQuiet_RunsWithoutLoopSeries` (AAA): StartAsync → 2 с → отмена → DebugSnapshot().LastSuccess не содержит "wal-sweeper", процесс жив (никакого исключения наружу).
(решение Фазы 6: `DebugSnapshot()` — internal и виден только `Shared.Metrics.UnitTests` (InternalsVisibleTo), из `PgWorker.UnitTests` недоступен; ассерт сделан строже — через `MeterListener` на экспортируемом инструменте `worker.loop.last_success_timestamp_seconds`: серия `loop="wal-sweeper"` в экспорте ОТСУТСТВУЕТ, цикл жив после двух сканов.)

---

### Задача 15 (Ф6): E2E-гейт — 3 целевых теста + полная серия

**Вход:** задачи 0–14 закоммичены; юниты зелёные.
**Действие (только СИЛЬНАЯ модель; docker-хост):**
1. Полный юнит-прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests --nologo > /tmp/t14-unit-full.log 2>&1` и `... src/tests/Shared.Metrics.UnitTests ...` — оба зелёные (внеобластные фейлы — по канону AGENTS.md в `arch/roadmap/bugs.md`, НЕ фиксить здесь).
2. Статподготовка ОДНИМ процессом: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release > /tmp/t14-e2e-prepare.log 2>&1`.
3. Сборка (отдельная команда): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t14-build.log 2>&1` — без ворнингов/ошибок.
4. Целевые тесты (каждый — отдельной командой, env-префикс при команде, `--no-build`):
   - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Restore_TargetTime_PitrRollback > /tmp/t14-e2e-pitr.log 2>&1`
   - `... --filter FullyQualifiedName~Drill_Failed_OnCorruptedWal > /tmp/t14-e2e-drill.log 2>&1`
   - `... --filter FullyQualifiedName~Restore_Latest_RebuildsDestroyedShard > /tmp/t14-e2e-latest.log 2>&1`
   Правила прогона: после старта — в течение 15 с убедиться по логу, что тест СТАРТОВАЛ; каждые 2 минуты проверять отсутствие зависания; тест > 5 мин — онлайн-анализ логов (docker-логи контейнеров, артефакты `/tmp/pgw-e2e-artifacts-*` и `$TMPDIR`); упавший тест НЕ перезапускать без разбора логов.
5. Зачистка между сериями: `docker ps -a --format '{{.Names}}' | grep pgw-` (пусто), `docker network ls | grep -c kfw-net` (0 при нуле контейнеров; иначе `docker network prune -f`); затем полная серия: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build > /tmp/t14-e2e-full.log 2>&1`.
6. Разбор итогов по логам-файлам; внеобластные фейлы — пункт в `arch/roadmap/bugs.md` (файлы логов скопировать в `arch/roadmap/bugs/<тег>/`, папка вне git-индекса по правилам трека).
7. Наблюдаемость wal-sweeper (spec §8.5, дёшево по артефактам ПРОГОННОГО контура, без перезапуска): снять метрики контейнера PgWorker контура (порт/путь — по факту контура E2E; если наружу не экспонирован — `docker exec` в контейнер воркера и запрос metrics-endpoint изнутри, образец снятия взять у контура/Metrics.SdGenerator) и проверить: (а) серия `worker_loop_last_success_timestamp_seconds{loop="wal-sweeper"}` присутствует и её возраст ≤ ~2×60 с; (б) `worker_wal_sweep_shard_last_success_timestamp_seconds{cluster=...}` присутствует для шардов контура и свежая. Метрики недостижимы — зафиксировать в отчёте прогона факт проверки по journal-фазам `wal-swept/...` (если удаления были) и ЯВНО отметить непроверенное (не блокирует гейт, но попадает в отчёт).
**Выход:** 3 целевых теста зелёные; полная серия без регрессий; факт проверки серий wal-sweeper зафиксирован.
**Проверка:** финальные строки логов: Passed; `git status` — только ожидаемые файлы.
**Spec:** §8 (критерии 1, 2, 5, 6).
**Развилка 15 из spec соблюдена**: resume приёмника не менялся — если в E2E видно иное поведение, разбирать по логам, код не править вслепую.

**Решения Фазы 6 по итогам первого E2E-прогона (разбор логов, до повторного прогона):**
1. **Дубль набора: усечённый последний файл.** E2E-лог: `archive file "000000010000000000000003" has wrong size: 987 instead of 16777216` (drill) — дубль положил в `wal/` усечённый концом бэкапа файл набора: к моменту дублирования источник уже ушёл дальше по имени, поэтому фильтр «строго ниже активного» пропустил его. Дополнено: дублируется только файл набора ровно `wal_segment_size` байт (arch/19 §2 — канон исправлен arch-first, коммит 330b71a9); тест-фейк pg_basebackup пишет закрытые сегменты полного размера + усечённый 0004 (ассерт «не дублируется»).
2. **fetch-wal: `.partial` нельзя класть «как есть».** E2E-лог: `archive file "000000010000000000000010" has wrong size: 1023 instead of 16777216` — postgres отвергает архивный файл restore_command размером ≠ `wal_segment_size`; spec §4.6 «кладётся как есть» на практике не работает. Валидный префикс добивается нулями до полного сегмента (`FetchWalCommand.TryWritePaddedSegment`): нули за концом последней записи — штатный конец WAL (arch/19 §3.5 — канон исправлен arch-first). Контракт/RPO не меняются (тот же лучший `.partial`, те же байты префикса).
4. **Запись WAL пересекает границу страницы.** E2E-факт: сканер обрывал префикс на первой записи, не влезающей в остаток страницы (`tot > remainingInPage`) — план/спека ошибочно считали, что такая запись целиком переносится на следующую страницу. В PG запись, чей заголовок помещается, НАЧИНАЕТСЯ в текущей странице и продолжается на следующей ПОСЛЕ short-заголовка (`xlp_rem_len`); страничный обход переписан (`Advance`: перенос байтов записи через страницы + MAXALIGN-добивка), тест `RecordSpanningPage_Continuation` на реальной раскладке. Канон не менялся (механика страниц в arch/19 не описана; инвариант «до конца последней полной записи» сохранён).
3. **Дубль: «хвост набора как .partial» отвергнут (A/B на базе).** Проверка на dde41879 vs t14 для `Restore_NewCluster_FromSourcePrefix`: база зелёная, t14 красная (restore не доходит до consistency point). Причина: последний файл `pg_wal/` набора (бэкап со standby) содержит разреженные чанки/дыры — эвристики «валидный префикс» (ни размер файла, ни prev-link/CRC-обход) не дают надёжной границы; PG сам корректно читает этот файл как финал (base), а restore_command-версия обрезалась неверно. Решение: правило канона §2 восстановлено — дублируются только сегменты набора СТРОГО ниже активного wal-файла источника (идентификационный фильтр t14, задача 7, остаётся — именно он убирает гонку «срез живого сегмента против финала»); хвост набора финалом не дублируется, его байты — в `full/<id>/pg_wal/`. Остаточный риск «источник ушёл дальше между концом бэкапа и дублем → срез усечённого хвоста финалом» — предсуществующий (базовое поведение), не регрессия t14; в E2E не воспроизводится (pg_basebackup добивает последний файл до 16 МиБ).
4. **Запись WAL пересекает границу страницы.** E2E-факт: сканер обрывал префикс на первой записи, не влезающей в остаток страницы (`tot > remainingInPage`) — план/спека ошибочно считали, что такая запись целиком переносится на следующую страницу. В PG запись, чей заголовок помещается, НАЧИНАЕТСЯ в текущей странице и продолжается на следующей ПОСЛЕ short-заголовка (`xlp_rem_len`); страничный обход переписан (`Advance`: перенос байтов записи через страницы + MAXALIGN-добивка), тест `RecordSpanningPage_Continuation` на реальной раскладке. Канон не менялся (механика страниц в arch/19 не описана; инвариант «до конца последней полной записи» сохранён).
3. **Дубль: WAL-хвост набора должен быть доступен restore_command.** A/B-проверка (база dde41879 vs t14) на `Restore_NewCluster_FromSourcePrefix`: база зелёная, t14 красная. Причина: последний файл `pg_wal/` набора (у бэкапа со standby) несёт реальные записи до точки консистентности бэкапа (`minRecoveryPoint` внутри файла), а правило «только сегменты строго ниже активного» его не дублирует — restore не доходит до consistency point. Решение по канону (arch/19 §2, arch-first): хвост набора (файл максимальной позиции) финалом не дублируется НИКОГДА, а его валидный префикс (`WalPrefixScanner`) публикуется `.partial`-версией — restore дочитывает им хвост, ключ финала остаётся стриму (гонка «срез против финала» не возвращается). E2E-факт провала/успеха: `arch/roadmap/bugs/` (до фикса) и повторный прогон после.

- [ ] Шаг: пункты 1–4, разбор, фикс отчёта прогона текстом в ответе (не файлом).
- [ ] Шаг: пункт 5 (зачистка → полная серия), разбор; финальный коммит при правках (если правок нет — коммит не нужен).
- [ ] Шаг: пункт 7 — проверка серий `loop="wal-sweeper"` и per-shard свежести по метрикам/логам прогона; результат — строкой в отчёте (spec §8.5).

---

## Самопроверка плана (исполнена автором)

- Покрытие spec: §3 → задача 0; §4.1 → 1–4; §4.2 → 5–6; §4.3 → 7; §4.4 → 8; §4.5 → 9; §4.6 → 10; §4.7 → 11; §4.8 → 12–14; §4.9 → 5; §4.10 → без правок (развилка 21); §5-фазы → порядок задач; §8-критерии → 15 (+юниты в каждой задаче).
- Типы/имена сверены с кодом: WalReceiverOptions (13 полей + новое), BackupsRuntimeOptions/ToRuntime, NodeBackupEnv, ClusterDriver env-блок, BackupVerifyProcess:215, WalStreamProcess:628-640, FetchWalCommand.FetchOnceAsync, RetentionProcess:154-170, WorkerMetricsInstrumentation (Measure/lock/DebugSnapshot), BackupOrphanSweeper/Loop (паттерны 13–14).
- Развилки spec §7 (1–34) распределены по задачам; новых развилок у исполнителя нет — каждое имя/значение/место задано.
- Пропорция: план длиннее спеки из-за самодостаточных подпунктов для слабой модели (требование dev-flow приоритетнее пропорции skill).
