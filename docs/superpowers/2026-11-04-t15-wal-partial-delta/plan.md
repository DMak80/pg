# t15-wal-partial-delta — план реализации (двухуровневый)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** инкрементальная доставка RPO-хвоста WAL дельта-кусками (база-компак + дельты прироста) с номером жизни цепочки из etcd-счётчика `wal-life`.

**Architecture:** приёмник пишет куски `wal/<seg>.partial_<start>_<end>_<life>_<role>_<md5>` (номер жизни — из конфигурации сессии, etcd-счётчик, единственный писатель PgWorker); fetch-wal собирает сегмент из кусков (фильтр по номеру → порядок кандидатов → побайтовая сверка → режим выживания → скан-валидация с CRC); контроль/сборка/sweeper фильтруют листинг по текущему номеру; воркер инкрементирует счётчик только при разрыве цепочки (restore/пересоздание/гвард); панель алертит смену жизни.

**Tech Stack:** .NET 10, C# (`Nullable=enable`, `TreatWarningsAsErrors=true`), AWSSDK S3, xunit + FluentAssertions, docker-E2E (MinIO, patroni).

**Spec:** `docs/superpowers/2026-11-04-t15-wal-partial-delta/spec.md` (далее «spec»); канон — `arch/backups/*`, `arch/pgnode/*` (правки t15 уже в дереве). План аргументируется от spec: исполнитель читает spec + этот план.

Роли: **кодер** — сильная модель (субагент coder по умолчанию; при executor="main" — main-агент); **подпункты** — слабой модели (coder-flash), каждый самодостаточен.

---

## Global Constraints (из spec §6; действуют для КАЖДОЙ задачи)

- Бюджеты НЕ менять: fetch-wal `CallDeadline=45 с`, попытки 3×2 с, breaker 45 с, `PutAttemptTimeout=15 с`, Job-бюджеты, `BrokerBootSec ≤ 100 с`. `DeltaDownloadParallelism=8` — НОВАЯ константа, не таймаут.
- P/Invoke ЗАПРЕЩЁН (CRC-32C — управляемая табличная реализация).
- Комментарии в коде — русские; тесты — AAA (`// Arrange`, `// Act`, `// Assert`); идентификаторы — английские.
- `TreatWarningsAsErrors=true` — сборка без ворнингов.
- Никаких скипов в тестах; docker-тесты — динамические порты, полный teardown (канон AGENTS.md).
- Сборка и тесты — ОТДЕЛЬНЫМИ командами: сначала `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`, затем `DOTNET_CLI_UI_LANGUAGE=en dotnet test <project> -c Release --no-build --filter …`.
- RPO-инвариант: подтверждение слота — только после успешного put ФИНАЛА; куски никогда не двигают `restart_lsn`.
- Каждая задача заканчивается коммитом в feature-ветке (коммитить свободно; в main не мержить).

## Review Focus (не покрытые явными тестами классы отказов; тест-строки добавлены в задачи-владельцы)

1. **Порча `xl_crc` в середине записи при склейке кусков** → скан-валидация усекает префикс до последней CRC-валидной записи (Task 2).
2. **Кусок с `ChainLife` = текущему, но мусорным md5 в имени (порча при хранении)** → отброс после скачивания + stderr-факт, сборка продолжается (Task 9).
3. **`end−start ≠ Size` из листинга** → битый кусок отброшен до скачивания (Task 9).
4. **Безномерная t14-версия рядом с кусками текущего номера** → не учитывается нигде (TailOf/сборка/sweeper) (Tasks 4/5/9).
5. **bump `wal-life` при живой сессии приёмника (гонка)** → куски старого номера становятся чужими: контроль не видит, sweeper выметает; RPO не страдает (гарантии — финалы) (Task 10, юнит идемпотентности).

---

### Task 0: коммит arch-правок и базовая линия

**Вход:** worktree `feat-t15-wal-partial-delta`; в дереве незакоммиченные arch-правки t15 (19 файлов + новый `arch/pgnode/processes/g-wal-delta-assembly.md`) и `docs/superpowers/2026-11-04-t15-wal-partial-delta/` (spec, план). Код ещё не тронут.

**Действие (кодер):**
- [ ] Убедиться: `git status` показывает ТОЛЬКО arch-правки из spec §3 и `docs/superpowers/2026-11-04-t15-wal-partial-delta/`; ничего лишнего.
- [ ] Коммит: `git add arch/ docs/superpowers/2026-11-04-t15-wal-partial-delta/ && git commit -m "arch(t15): дельта-куски RPO-хвоста + номер жизни цепочки (wal-life) — канон до кода; spec/plan"`.

**Выход:** arch-first зафиксирован коммитом; чистая база для кода.

**Проверка:** `git log --oneline -1` — коммит на месте; `git status` чист.

**Связь со spec:** §2 arch-first, §3 (весь), Ф0.

---

### Task 1: WalDeltaName + IsPartial (модель имени куска)

**Вход:** Task 0 закоммичен.
**Файлы:**
- Create: `src/PgWorker.Backups/Model/WalDeltaName.cs`
- Modify: `src/PgWorker.Backups/Model/WalFileName.cs` (метод `IsPartial`, строки 31–33)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalDeltaNameTests.cs` (новый)

**Interfaces (produces):**
```csharp
public static class WalDeltaName
{
    public sealed record Piece(WalFileName Segment, ulong Start, ulong End, uint ChainLife, bool IsMaster, string Md5Hex);
    public static string Key(WalFileName segment, ulong start, ulong end, uint chainLife, bool isMaster, string md5Hex);
    // start >= end → ArgumentException (spec №29: вырожденный кусок запрещён конструированием)
    public static Piece? TryParse(string name);
    // start >= end в имени → null (битый/вырожденный)
    public static string SegmentPrefixOf(string segmentName); // segmentName + "."
}
```

- [ ] **Шаг 1 — тесты (файл целиком даёт подпункт 1.1), прогнать — падают (нет класса).**
- [ ] **Шаг 2 — реализация (подпункт 1.2), прогнать — зелёные.**
- [ ] **Шаг 3 — IsPartial: `Contains` вместо `EndsWith` (подпункт 1.3); прогон всего `WalFileNameTests` + `WalChainTests` + `WalPartialNameTests` (регресс: `IsPartial` теперь true и для `<seg>.partial_…` — это ожидаемо, финалы/`.history` не затронуты).**
- [ ] **Шаг 4 — коммит** `feat(t15): WalDeltaName — имя дельта-куска с номером жизни; IsPartial Contains`.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` → 0 ошибок/ворнингов; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build --filter "FullyQualifiedName~WalDeltaNameTests|FullyQualifiedName~WalFileNameTests|FullyQualifiedName~WalChainTests|FullyQualifiedName~WalPartialNameTests"` → все Passed.

**Выход:** имя куска парсится/строится везде единообразно; вырожденный диапазон запрещён конструированием (spec №29).
**Связь со spec:** §4.1 (WalDeltaName, IsPartial), решения №1/№25/№27/№29.

**Подпункты (слабой модели):**

**1.1. Создай файл** `src/tests/PgWorker.UnitTests/Backups/WalDeltaNameTests.cs` с содержимым:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Имя дельта-куска (arch/pgnode/03 §2, spec §4.1): фиксированные ширины,
// hex строчные, лексикографическая сортировка = порядок по start;
// вырожденный диапазон (start >= end) запрещён конструированием (spec №29).
public class WalDeltaNameTests
{
    private const string Seg = "000000010000000000000001";

    [Fact]
    public void Key_CanonicalFormat()
    {
        // Arrange / Act
        var key = WalDeltaName.Key(
            WalFileName.TryParse(Seg)!.Value, 0x0, 0x1234ABCDEF567890UL, 2, true, "0123456789abcdef0123456789abcdef");

        // Assert
        key.Should().Be(
            $"{Seg}.partial_0000000000000000_1234abcdef567890_00000002_m_0123456789abcdef0123456789abcdef");
    }

    [Fact]
    public void Key_ReplicaRole_IsR()
    {
        // Arrange / Act
        var key = WalDeltaName.Key(
            WalFileName.TryParse(Seg)!.Value, 1, 2, 0, false, "a".PadRight(32, '0'));

        // Assert
        key.Should().EndWith("_00000000_r_" + "a".PadRight(32, '0'));
    }

    [Fact]
    public void Key_StartNotLessThanEnd_Throws()
    {
        // Arrange
        var seg = WalFileName.TryParse(Seg)!.Value;

        // Act
        var act = () => WalDeltaName.Key(seg, 5, 5, 1, true, "0".PadRight(32, '0'));

        // Assert — вырожденный/перевёрнутый диапазон запрещён (spec №29)
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void TryParse_ValidName_AllFields()
    {
        // Arrange
        var name = $"{Seg}.partial_0000000000000000_1234abcdef567890_0000000a_r_0123456789abcdef0123456789abcdef";

        // Act
        var piece = WalDeltaName.TryParse(name);

        // Assert
        piece.Should().NotBeNull();
        piece!.Segment.Name.Should().Be(Seg);
        piece.Start.Should().Be(0);
        piece.End.Should().Be(0x1234ABCDEF567890UL);
        piece.ChainLife.Should().Be(0xA);
        piece.IsMaster.Should().BeFalse();
        piece.Md5Hex.Should().Be("0123456789abcdef0123456789abcdef");
    }

    [Fact]
    public void TryParse_StartNotLessThanEnd_ReturnsNull()
    {
        // Arrange — start == end: вырожденный кусок
        var name = $"{Seg}.partial_0000000000000005_0000000000000005_00000002_m_0123456789abcdef0123456789abcdef";

        // Act / Assert
        WalDeltaName.TryParse(name).Should().BeNull();
    }

    [Theory]
    [InlineData("")]                                   // пусто
    [InlineData("000000010000000000000001")]           // финал
    [InlineData("000000010000000000000001.0000000000000005.partial")] // t14-версия — НЕ кусок
    [InlineData("000000010000000000000001.partial_0_1_2_m_md5")]      // нефиксированные ширины
    [InlineData("000000010000000000000001.partial_0000000000000000_1234abcdef567890_00000002_x_0123456789abcdef0123456789abcdef")] // роль не m/r
    [InlineData("000000010000000000000001.partial_0000000000000000_1234abcdef567890_00000002_m_0123456789abcdef0123456789abcdeg")] // не hex md5
    public void TryParse_Invalid_ReturnsNull(string name)
    {
        // Arrange / Act / Assert
        WalDeltaName.TryParse(name).Should().BeNull();
    }

    [Fact]
    public void SegmentPrefixOf_IsNarrowListingPrefix()
    {
        // Arrange / Act / Assert
        WalDeltaName.SegmentPrefixOf(Seg).Should().Be(Seg + ".");
    }

    [Fact]
    public void Key_SortsByStart_Lexicographically()
    {
        // Arrange
        var seg = WalFileName.TryParse(Seg)!.Value;
        var md5 = "0".PadRight(32, '0');
        var a = WalDeltaName.Key(seg, 0, 1, 1, true, md5);
        var b = WalDeltaName.Key(seg, 1, 2, 1, true, md5);
        var legacy = $"{Seg}.ffffffffffffffff.partial"; // t14-версия

        // Act / Assert — суффикс куска ПОСЛЕ hex-суффиксов legacy ('p' > любой hex)
        string.CompareOrdinal(a, b).Should().BeNegative();
        string.CompareOrdinal(legacy, a).Should().BeNegative();
    }
}
```

**1.2. Создай файл** `src/PgWorker.Backups/Model/WalDeltaName.cs` с содержимым:

```csharp
using System.Globalization;

namespace PgWorker.Backups;

/// <summary>Имя дельта-куска незакрытого сегмента (arch/pgnode/03 §2, spec §4.1):
/// wal/&lt;segment&gt;.partial_&lt;start:x16&gt;_&lt;end:x16&gt;_&lt;life:x8&gt;_&lt;role&gt;_&lt;md5&gt; —
/// start/end — абсолютные LSN (полузакрытый [start,end), размер = end−start),
/// life — номер жизни цепочки (u32, 8 hex), роль источника m|r на момент
/// выкладки, md5 байтов куска (32 hex). Вырожденный диапазон start &gt;= end
/// запрещён конструированием (Key бросает, TryParse → null — spec №29).
/// Все ширины фиксированы — сортировка имён сегмента = порядок по start;
/// t14-версии wal/&lt;segment&gt;.&lt;endLsn&gt;.partial этим парсером не разбираются
/// (TryParse null — безномерный legacy переходного окна). Чистые функции.</summary>
public static class WalDeltaName
{
    public const string PrefixLiteral = ".partial_";

    /// <summary>Кусок: сегмент, границы [Start,End), номер жизни, роль, md5-hex.</summary>
    public sealed record Piece(
        WalFileName Segment, ulong Start, ulong End, uint ChainLife, bool IsMaster, string Md5Hex);

    /// <summary>Ключ объекта: сегмент + суффикс с фиксированными ширинами;
    /// start &gt;= end — ArgumentException (пустые/перевёрнутые куски не существуют).</summary>
    public static string Key(
        WalFileName segment, ulong start, ulong end, uint chainLife, bool isMaster, string md5Hex)
    {
        if (start >= end)
            throw new ArgumentException("delta piece must have start < end (half-open range)");
        return $"{segment.Name}{PrefixLiteral}{start:x16}_{end:x16}_{chainLife:x8}_{(isMaster ? "m" : "r")}_{md5Hex}";
    }

    /// <summary>Разбор имени объекта: кусок либо null (финал/t14-версия/мусор/
    /// вырожденный диапазон).</summary>
    public static Piece? TryParse(string name)
    {
        // <24hex>.partial_<16hex>_<16hex>_<8hex>_<m|r>_<32hex>
        // = 24 + 9 + 16 + 1 + 16 + 1 + 8 + 1 + 1 + 1 + 32 = 110 символов.
        const int Total = 24 + 9 + 16 + 1 + 16 + 1 + 8 + 1 + 1 + 1 + 32;
        if (name.Length != Total)
            return null;
        if (!name.AsSpan(24, PrefixLiteral.Length).SequenceEqual(PrefixLiteral))
            return null;
        var span = name.AsSpan();
        if (span[24 + 9 + 16] != '_' || span[24 + 9 + 16 + 1 + 16] != '_'
            || span[24 + 9 + 16 + 1 + 16 + 1 + 8] != '_' || span[24 + 9 + 16 + 1 + 16 + 1 + 8 + 1 + 1] != '_')
            return null;
        var inv = CultureInfo.InvariantCulture;
        if (!ulong.TryParse(span.Slice(24 + 9, 16), NumberStyles.HexNumber, inv, out var start)
            || !ulong.TryParse(span.Slice(24 + 9 + 1 + 16, 16), NumberStyles.HexNumber, inv, out var end)
            || !uint.TryParse(span.Slice(24 + 9 + 1 + 16 + 1, 8), NumberStyles.HexNumber, inv, out var life))
            return null;
        if (start >= end)
            return null; // вырожденный диапазон — битый кусок
        var role = span[24 + 9 + 1 + 16 + 1 + 8 + 1];
        if (role is not ('m' or 'r'))
            return null;
        var md5 = span.Slice(24 + 9 + 1 + 16 + 1 + 8 + 1 + 1 + 1, 32);
        foreach (var c in md5)
            if (!Uri.IsHexDigit(c) || char.IsUpper(c))
                return null;
        if (WalFileName.TryParse(name[..24]) is not { } segment)
            return null;
        return new Piece(segment, start, end, life, role == 'm', new string(md5));
    }

    /// <summary>Узкий S3-префикс листинга всего кусочного семейства сегмента
    /// (куски и t14-версии): "&lt;segmentName&gt;.".</summary>
    public static string SegmentPrefixOf(string segmentName)
        => segmentName + ".";
}
```

**1.3. В файле** `src/PgWorker.Backups/Model/WalFileName.cs` замени метод:

```csharp
    /// <summary>Незакрытый сегмент (pg_receivewal дописывает) — не грузится агентом.</summary>
    public static bool IsPartial(string name)
        => name.EndsWith(".partial", StringComparison.Ordinal);
```

на:

```csharp
    /// <summary>Частичное семейство имён — незакрытый сегмент: t14-версии
    /// wal/&lt;seg&gt;.&lt;endLsn&gt;.partial И дельта-куски wal/&lt;seg&gt;.partial_… —
    /// в цепь финалов не входят ни при каком парсинге (spec §4.1).</summary>
    public static bool IsPartial(string name)
        => name.Contains(".partial", StringComparison.Ordinal);
```

---

### Task 2: CRC-32C + ValidatedPrefixLength (сканер с CRC)

**Вход:** Task 1 закоммичен.
**Файлы:**
- Create: `src/PgWorker.Backups/Model/Crc32C.cs`
- Modify: `src/PgWorker.Backups/Model/WalPrefixScanner.cs` (добавить `ValidatedPrefixLength` рядом с `ValidPrefixLength`)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalPrefixScannerCrcTests.cs` (новый)

**Interfaces (produces):**
```csharp
// Crc32C: инкрементальный CRC-32C (Castagnoli, reflected 0x82F63B78, init/xorout 0xFFFFFFFF)
internal static class Crc32C
{
    internal static uint Init();                       // 0xFFFFFFFF
    internal static uint Update(uint crc, byte[] b, int offset, int count); // без отражения байтов
    internal static uint Finish(uint crc);             // crc ^ 0xFFFFFFFF
    internal static uint HashData(byte[] data);        // эталон одним вызовом
}
// WalPrefixScanner:
public static int ValidatedPrefixLength(byte[] buffer, int count);
```

Семантика `ValidatedPrefixLength` (spec §4.1, решение №18): тот же проход, что `ValidPrefixLength`, но каждая ПОЛНОСТЬЮ принятая запись дополнительно проверяется по `xl_crc` (LE u32 по смещению `pos+20` от начала записи). CRC считается по ЛОГИЧЕСКОМУ образу записи: байты `[pos..pos+20)` (заголовок до `xl_crc`) плюс байты от `pos+24` до физического конца байтов записи (`recordEnd` из механики `Advance`), при этом из образа ИСКЛЮЧАЮТСЯ вставленные short-заголовки страниц (24 б на каждой границе 8192) и MAXALIGN-добивка записи. Несовпадение CRC → стоп на конце последней CRC-валидной записи (тот же `prefix`).

- [ ] **Шаг 1 — `Crc32C` (подпункт 2.1; таблица 256×u32 по reflected-полиному). Юнит-эталон: `HashData("123456789" ASCII) == 0xE3069283`.**
- [ ] **Шаг 2 — тест-файл (подпункт 2.2: helper-строитель записей с корректным CRC + кейсы). Прогнать — падают (нет метода).**
- [ ] **Шаг 3 — реализация `ValidatedPrefixLength` (кодер): проход переиспользует механику страниц; CRC считай инкрементально двумя сегментами ([pos..pos+20) и с pos+24 до recordEnd), постранично минус short-заголовки; `Advance` уже даёт `recordEnd`/`next`. Прогнать — зелёные.**
- [ ] **Шаг 4 — регресс: весь `WalPrefixScannerTests` зелёный (ValidPrefixLength не менялся). Коммит** `feat(t15): сканер WAL — CRC-валидация записей (ValidatedPrefixLength, CRC-32C)`.

**Проверка:** build 0 ворнингов; `--filter "FullyQualifiedName~WalPrefixScanner"` → Passed.
**Выход:** сборка читателя может доверять CRC каждой записи.
**Связь со spec:** §4.1 (WalPrefixScanner), решение №18; Review Focus №1.

**Подпункты (слабой модели):**

**2.1. Создай файл** `src/PgWorker.Backups/Model/Crc32C.cs` с содержимым:

```csharp
namespace PgWorker.Backups;

/// <summary>CRC-32C (Castagnoli) — сверка xl_crc записей WAL (spec §4.1, решение
/// №18): reflected-полином 0x82F63B78, init 0xFFFFFFFF, xorout 0xFFFFFFFF, БЕЗ
/// отражения байтов (зеркальный алгоритм); управляемая табличная реализация —
/// P/Invoke запрещён, пакет System.IO.Hashing не подключается. Инкрементальный:
/// Init → Update(куски) → Finish.</summary>
internal static class Crc32C
{
    private static readonly uint[] Table = BuildTable();

    internal static uint Init() => 0xFFFFFFFF;

    internal static uint Update(uint crc, byte[] b, int offset, int count)
    {
        for (var i = offset; i < offset + count; i++)
            crc = Table[(crc ^ b[i]) & 0xFF] ^ (crc >> 8);
        return crc;
    }

    internal static uint Finish(uint crc) => crc ^ 0xFFFFFFFF;

    internal static uint HashData(byte[] data) => Finish(Update(Init(), data, 0, data.Length));

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var crc = i;
            for (var k = 0; k < 8; k++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ 0x82F63B78 : crc >> 1;
            table[i] = crc;
        }
        return table;
    }
}
```

**2.2. Создай файл** `src/tests/PgWorker.UnitTests/Backups/WalPrefixScannerCrcTests.cs` с содержимым:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// ValidatedPrefixLength: границы записей + CRC xl_crc каждой записи (spec §4.1,
// решение №18). Фикстуры строятся валидными по CRC, затем портятся точечно.
public class WalPrefixScannerCrcTests
{
    private const int Page = WalPrefixScanner.PageBytes;

    /// <summary>Сегмент с long-заголовком (tli=1, pageaddr=0) и записями с
    /// корректным xl_crc: запись recordLen байт от позиции 40; тело после
    /// 4-байтового xl_crc заполнено паттерном; xl_crc = CRC-32C образа
    /// ([pos..pos+20) + [pos+24..recordEnd)).</summary>
    private static byte[] SegmentWithRecords(params int[] recordLengths)
    {
        var records = new List<(int Pos, int Len)>();
        var data = new byte[Page * 2];
        data[4] = 1; // tli=1 LE
        var pos = WalPrefixScanner.LongHeaderBytes;
        var rng = 1;
        foreach (var len in recordLengths)
        {
            if (pos % Page == 0)
                pos += WalPrefixScanner.ShortHeaderBytes;
            var end = pos + len;
            if (end > data.Length)
                Array.Resize(ref data, end + Page);
            records.Add((pos, len));
            // тело записи: ненулевой паттерн
            for (var i = pos + 24; i < end; i++)
                data[i] = (byte)(0x40 | (rng = rng * 5 + 1) % 0x3F);
            WriteLen(data, pos, len);
            WriteCrc(data, records[^1]);
            pos = (end + 7) & ~7; // MAXALIGN следующей записи
        }
        return data;
    }

    private static void WriteLen(byte[] data, int pos, int len)
    {
        data[pos] = (byte)len;
        data[pos + 1] = (byte)(len >> 8);
        data[pos + 2] = (byte)(len >> 16);
        data[pos + 3] = (byte)(len >> 24);
    }

    // xl_crc = CRC-32C образа записи: [pos..pos+20) + [pos+24..pos+len).
    private static void WriteCrc(byte[] data, (int Pos, int Len) r)
    {
        var crc = Crc32C.Init();
        crc = Crc32C.Update(crc, data, r.Pos, 20);
        crc = Crc32C.Update(crc, data, r.Pos + 24, r.Len - 24);
        var value = Crc32C.Finish(crc);
        data[r.Pos + 20] = (byte)value;
        data[r.Pos + 21] = (byte)(value >> 8);
        data[r.Pos + 22] = (byte)(value >> 16);
        data[r.Pos + 23] = (byte)(value >> 24);
    }

    [Fact]
    public void ValidRecords_FullPrefix()
    {
        // Arrange
        var data = SegmentWithRecords(64, 100);

        // Act
        var len = WalPrefixScanner.ValidatedPrefixLength(data, data.Length);

        // Assert — обе записи валидны: конец БАЙТОВ второй записи (первая [40..104), вторая [104..204))
        len.Should().Be(204);
    }

    [Fact]
    public void CorruptedBodyCrc_TruncatesToPreviousRecord()
    {
        // Arrange — портим тело ВТОРОЙ записи [104..204) (CRC перестанет сходиться)
        var data = SegmentWithRecords(64, 100);
        data[104 + 30] ^= 0xFF;

        // Act
        var len = WalPrefixScanner.ValidatedPrefixLength(data, data.Length);

        // Assert — валидна только первая запись: конец её байтов
        len.Should().Be(104);
    }

    [Fact]
    public void CorruptedCrcField_TruncatesToPreviousRecord()
    {
        // Arrange — портим сам xl_crc первой записи
        var data = SegmentWithRecords(64, 100);
        data[40 + 20] ^= 0xFF;

        // Act / Assert — ни одной CRC-валидной записи
        WalPrefixScanner.ValidatedPrefixLength(data, data.Length).Should().Be(0);
    }

    [Fact]
    public void RecordWithoutCrcField_StillValidByBoundaries_InValidPrefix()
    {
        // Arrange — ValidPrefixLength (границы) не проверяет CRC: обе записи целиком
        var data = SegmentWithRecords(64, 100);
        data[40 + 20] ^= 0xFF;

        // Act / Assert — старый сканер принимает обе (границы целы)
        WalPrefixScanner.ValidPrefixLength(data, data.Length).Should().Be(204);
    }

    [Fact]
    public void Crc32C_ReferenceVector()
    {
        // Arrange / Act / Assert — эталон RFC 3720: "123456789" → 0xE3069283
        Crc32C.HashData("123456789"u8.ToArray()).Should().Be(0xE3069283);
    }
}
```

Если тест `ValidRecords_FullPrefix` упадёт на границе MAXALIGN — проверь: префикс = конец БАЙТОВ записи (`recordEnd`), НЕ выровненная позиция следующей (образец `ValidPrefixLength`: `prefix = (int)recordEnd`).

**Примечание (для кодера, шаг 3):** запись, пересекающая границу страницы (short-заголовок внутри тела), покрывается ДОПОЛНИТЕЛЬНЫМ кейсом, который пишет кодер: `RecordSpanningPageBoundary_CrcSkipsPageHeader` — запись длиной, заводящей `Advance` через границу 8192; CRC-образ минует short-заголовок страницы.

---

### Task 3: WalLifeStateJson + WalLifeReason + BackupNames.WalLifeKey

**Вход:** Task 2 закоммичен.
**Файлы:**
- Create: `src/PgWorker.Backups/Job/WalLifeStateJson.cs`
- Modify: `src/PgWorker.Backups/BackupNames.cs` (добавить `WalLifeKey`)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalLifeStateJsonTests.cs` (новый)

**Interfaces (produces):**
```csharp
public enum WalLifeReason { ShardRebuilt, Restore, ForeignChain }
public sealed record WalLifeState(uint ChainLife, long? BumpedUnix, WalLifeReason? Reason);
public static class WalLifeStateJson
{
    public static string Serialize(WalLifeState state);      // {"chain_life":N,"bumped_unix":T,"reason":"…"} — snake_case
    public static WalLifeState? TryParse(string? json);      // null — битый
}
// BackupNames:
public static string WalLifeKey(string cluster, string shard); // "/pgworker/backups/<C>/<X>/wal-life"
```
Строки reason: `shard-rebuilt|restore|foreign-chain` (spec §4.1, канон 05 §1). Ключи JSON — РУЧНЫЕ snake_case (паттерн `BackupStatusJson`/`RestoreStatusJson`: Web-политика даёт camelCase, контракт etcd — snake_case; панельный парсер Task 17 и воркерский reader обязаны читать одинаковые ключи).

- [ ] **Шаг 1 — тест (подпункт 3.1), падает. Шаг 2 — реализация (подпункт 3.2), зелёный. Шаг 3 — коммит** `feat(t15): WalLifeStateJson + BackupNames.WalLifeKey — счётчик жизни цепочки`.

**Проверка:** build; `--filter "FullyQualifiedName~WalLifeStateJsonTests|FullyQualifiedName~BackupNamesTests"` → Passed.
**Выход:** формат ключа wal-life зафиксирован roundtrip'ом (snake_case, консистентен с панелью).
**Связь со spec:** §4.1 (WalLifeStateJson), решение №31.

**Подпункты (слабой модели):**

**3.1. Создай файл** `src/tests/PgWorker.UnitTests/Backups/WalLifeStateJsonTests.cs`:

```csharp
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Сериализация значения ключа wal-life (канон backups/05 §1, spec §4.1):
// roundtrip writer/reader, канонические snake_case-поля и reason-строки.
public class WalLifeStateJsonTests
{
    [Fact]
    public void Serialize_CanonicalSnakeCaseFields()
    {
        // Arrange / Act
        var json = WalLifeStateJson.Serialize(new(3, 1757500000, WalLifeReason.Restore));

        // Assert
        json.Should().Contain("\"chain_life\":3")
            .And.Contain("\"bumped_unix\":1757500000")
            .And.Contain("\"reason\":\"restore\"");
        json.Should().NotContain("chainLife").And.NotContain("bumpedUnix"); // camelCase запрещён
    }

    [Theory]
    [InlineData(WalLifeReason.ShardRebuilt, "shard-rebuilt")]
    [InlineData(WalLifeReason.Restore, "restore")]
    [InlineData(WalLifeReason.ForeignChain, "foreign-chain")]
    public void Roundtrip_AllReasons(WalLifeReason reason, string wire)
    {
        // Arrange
        var json = WalLifeStateJson.Serialize(new(7, 42, reason));

        // Act
        var parsed = WalLifeStateJson.TryParse(json);

        // Assert
        parsed.Should().Be(new WalLifeState(7, 42, reason));
        json.Should().Contain($"\"reason\":\"{wire}\"");
    }

    [Fact]
    public void TryParse_BrokenJson_ReturnsNull()
    {
        // Arrange / Act / Assert
        WalLifeStateJson.TryParse("{oops").Should().BeNull();
        WalLifeStateJson.TryParse(null).Should().BeNull();
    }

    [Fact]
    public void WalLifeKey_CanonicalPath()
    {
        // Arrange / Act / Assert
        BackupNames.WalLifeKey("c1", "shard1")
            .Should().Be("/pgworker/backups/c1/shard1/wal-life");
    }
}
```

**3.2. Создай файл** `src/PgWorker.Backups/Job/WalLifeStateJson.cs` с содержимым (snake_case-ключи — ВРУЧНЫЕ: Dictionary для записи, JsonDocument для чтения; Web-политика Newtonsoft-стиля camelCase НЕ применяется к ключам):

```csharp
using System.Text.Json;

namespace PgWorker.Backups.Job;

// Значение ключа /pgworker/backups/<C>/<X>/wal-life (arch/backups/05 §1):
// {"chain_life":N,"bumped_unix":T,"reason":"shard-rebuilt|restore|foreign-chain"}.
// Отсутствие ключа = жизнь 0 (запись не создаётся). Пишет ТОЛЬКО воркер.
// Ключи JSON — snake_case вручную (паттерн BackupStatusJson/RestoreStatusJson:
// JsonSerializerDefaults.Web даёт camelCase имён свойств, контракт etcd —
// snake_case; панельный и воркерский парсеры читают одни и те же ключи).
public enum WalLifeReason { ShardRebuilt, Restore, ForeignChain }

public sealed record WalLifeState(uint ChainLife, long? BumpedUnix, WalLifeReason? Reason);

public static class WalLifeStateJson
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static string Serialize(WalLifeState state)
    {
        var o = new Dictionary<string, object?>
        {
            ["chain_life"] = state.ChainLife,
        };
        if (state.BumpedUnix is { } bumped)
            o["bumped_unix"] = bumped;
        if (ReasonName(state.Reason) is { } reason)
            o["reason"] = reason;
        return JsonSerializer.Serialize(o, Json);
    }

    public static WalLifeState? TryParse(string? json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json ?? string.Empty);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("chain_life", out var lifeEl)
                || !lifeEl.TryGetInt64(out var life)
                || life is <= 0 or > uint.MaxValue)
                return null;
            long? bumpedUnix = root.TryGetProperty("bumped_unix", out var bEl)
                && bEl.ValueKind == JsonValueKind.Number
                && bEl.TryGetInt64(out var bumped)
                ? bumped
                : null;
            WalLifeReason? reason = root.TryGetProperty("reason", out var rEl)
                && rEl.ValueKind == JsonValueKind.String
                ? ReasonOf(rEl.GetString())
                : null;
            return new WalLifeState((uint)life, bumpedUnix, reason);
        }
        catch
        {
            return null; // битый JSON — толерантный читатель
        }
    }

    private static string? ReasonName(WalLifeReason? reason) => reason switch
    {
        WalLifeReason.ShardRebuilt => "shard-rebuilt",
        WalLifeReason.Restore => "restore",
        WalLifeReason.ForeignChain => "foreign-chain",
        _ => null,
    };

    private static WalLifeReason? ReasonOf(string? name) => name switch
    {
        "shard-rebuilt" => WalLifeReason.ShardRebuilt,
        "restore" => WalLifeReason.Restore,
        "foreign-chain" => WalLifeReason.ForeignChain,
        _ => null,
    };
}
```

**3.3. В файле** `src/PgWorker.Backups/BackupNames.cs` после метода `RestoreKey` добавь:

```csharp
    /// <summary>Ключ счётчика жизни цепочки WAL шарда (arch/backups/05 §1):
    /// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal-life; отсутствие ключа = жизнь 0;
    /// пишет ТОЛЬКО воркер (единственный писатель счётчика).</summary>
    public static string WalLifeKey(string cluster, string shard)
        => $"/pgworker/backups/{cluster}/{shard}/wal-life";
```

---

### Task 4: TailOf с фильтром номера жизни

**Вход:** Task 3 закоммичен.
**Файлы:**
- Modify: `src/PgWorker.Backups/Model/WalChain.cs` — `TailOf` (строки 38–66)
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` — единственный вызов `TailOf` (строка 633)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalChainTests.cs` (дополнить)

**Interfaces (produces):**
```csharp
public static WalTailFact? TailOf(
    WalFileName? chainLastFinal, IEnumerable<string> objectNames, uint chainLife);
```
Семантика (spec §4.1, решения №19/№36/№39):
- candidates = финал `chainLastFinal` + по сегментам: покрытие кусков ТЕКУЩЕЙ жизни (`WalDeltaName.Piece.ChainLife == chainLife`, покрывающее правило: coverage = max(end) баз (start==0), затем куски `start ≤ coverage < end` расширяют до max(end), итеративно до фиксации) ИЛИ, если у сегмента НЕТ ни одного куска `chainLife`, — лучшая t14-версия (`WalPartialName`, длиннейшая EndLsn; прежнее поведение);
- куски чужих `ChainLife` не рассматриваются вовсе;
- сравнение сегментов и авторитет финала — прежние.

- [ ] **Шаг 1 (кодер) — тесты в `WalChainTests`** (остальной файл не трогать):
  - `TailOf_DeltasOfCurrentLife_ExtendCoverage`: финала нет; `wal/segA.partial_0_X_life1_…` + `wal/segA.partial_X_Y_life1_…` → хвост `segA`/объект-«база или последняя дельта» — факт: `SegmentName == segA`; проверяется только сегмент (имя объекта не детерминировано при пересечениях — не ассертить).
  - `TailOf_ForeignLifePieces_Ignored`: те же куски с `life2` при `chainLife: 1` → null; при `chainLife: 2` → segA.
  - `TailOf_LegacyVersion_UsedUntilCurrentLifePieceAppears`: t14-версия segB + кусок `life1` у segB → при `chainLife:1` хвост по КУСКУ (не версии); у segC — только t14-версия → хвост по версии.
  - `TailOf_GapInCoverage_StopsAtCoverage`: куски `life1` с дырой (`[0..A)`,`[B..C)`, A<B) → покрытие = A.
  - `TailOf_FinalAuthoritative`: финал segA + куски segA `life1` длиннее → выбрать финал (IsFinal авторитетен при равной позиции — прежнее правило).
- [ ] **Шаг 2 — реализация `TailOf` (кодер): расширить сигнатуру, покрытие кусков — жадно по отсортированным по start; вызвать `WalDeltaName.TryParse`/`WalPartialName.TryParse`.**
- [ ] **Шаг 3 — обновить вызов в `WalStreamProcess.cs:633`:** `WalChain.TailOf(chain.LastSegment, objects.Select(o => o.Name), chainLife: 0)` — ВРЕМЕННЫЙ литерал 0 (чтение счётчика подводит Task 11; безопасно: кусков с номером ≠ 0 приёмник пока не пишет, legacy-версии учитываются прежним порядком — правило №39). Комментарий в коде: `// t15: chainLife — текущий номер wal-life; подводится чтением счётчика (Task 11)`; литерал заменяется в Task 11.**
- [ ] **Шаг 4 — прогон WalChainTests + WalReceiverTailTests + WalSweeperProcessTests (затронутые TailOf потребители), коммит** `feat(t15): TailOf — покрытие кусков текущей жизни цепочки (фильтр чужих, legacy-окно)`.

**Проверка:** build; `--filter "FullyQualifiedName~WalChainTests|FullyQualifiedName~WalReceiverTailTests|FullyQualifiedName~WalSweeperProcessTests"` → Passed.
**Выход:** контроль готов считать хвост по кускам (после подвода номера).
**Связь со spec:** §4.1 (TailOf), §4.5, решения №19/№36/№39.

---

### Task 5: SelectPartialForDeletion с веткой чужих номеров

**Вход:** Task 4 закоммичен.
**Файлы:**
- Modify: `src/PgWorker.Backups/Retention/RetentionPlanner.cs` — `SelectPartialForDeletion` (строки 140–180)
- Modify: вызовы `src/PgWorker.Backups/Retention/RetentionProcess.cs:162` и `src/PgWorker.Backups/Supervisor/WalSweeperProcess.cs:149`
- Test: `src/tests/PgWorker.UnitTests/Backups/RetentionPlannerTests.cs` (дополнить)

**Interfaces (produces):**
```csharp
public static IReadOnlyList<string> SelectPartialForDeletion(
    IReadOnlyList<string> objectNames, WalFileName? cutoff, uint? chainLife);
```
Семантика (spec §4.1, решение №20): прежние ветки (а) поглощено финалом — ВСЁ кусочное семейство (WalPartialName-версии + WalDeltaName-куски); (б) кусок ниже лучшей базы сегмента (база = компак start==0 максимального end среди кусков chainLife; legacy-версии ниже базы — мусор); (в) устаревшие компаки — остаётся длиннейшая; (г) семейство ниже cutoff; НОВАЯ (д): при `chainLife != null` — куски `ChainLife != chainLife` И безномерные t14-версии сегмента, у которого есть кусок `chainLife`, — мусор НЕЗАВИСИМО от cutoff. `chainLife == null` — ветка (д) не выполняется (гвард restore: чистку чужих отключили). Дельты выше базы — НЕ мусор.

- [ ] **Шаг 1 (кодер) — тесты** (`RetentionPlannerTests`): (д1) кусок `life2` при `chainLife:1` — в Delete даже выше cutoff; (д2) t14-версия сегмента с куском `life1` — в Delete; t14-версия сегмента БЕЗ кусков текущего номера — живёт (лучшая версия — остаётся одна); (д3) `chainLife: null` — чужих не трогает; (б/в) компаки: остаётся длиннейшая, дельты ниже базы — мусор, выше — живут; (а) финал поглощает семейство вкл. куски чужих жизней.
- [ ] **Шаг 2 — реализация (кодер). Шаг 3 — оба вызова передают `chainLife: null` ВРЕМЕННО (гвард/чтение подводит Task 15) — комментарий `// t15: вход — текущий номер wal-life (Task 15); null = ветка чужих отключена`. Шаг 4 — прогон RetentionPlannerTests + WalSweeperProcessTests, коммит** `feat(t15): кусочная гигиена sweeper — чужие номера жизни сразу, без cutoff`.

**Проверка:** build; `--filter "FullyQualifiedName~RetentionPlannerTests|FullyQualifiedName~WalSweeperProcessTests"` → Passed.
**Связь со spec:** §4.1 (SelectPartialForDeletion), решение №20; Review Focus №4.

---

### Task 6: S3-методы PutObjectMd5Async / GetObjectBytesAsync + пагинация кусочных объёмов

**Вход:** Task 5 закоммичен.
**Файлы:**
- Modify: `src/PgWorker.Backups/BackupS3.cs` — интерфейс `IBackupS3` + `BackupS3`
- Modify: `src/PgWorker.Backups/PoolBackupS3.cs` — проброс новых методов
- Modify: тестовые фейки `IBackupS3` (по месту обнаружения компилятором; юнит-фейки приёмника в `WalReceiverCoreTests`)
- Test: `src/tests/PgWorker.IntegrationTests/Backups/BackupS3Tests.cs` (дополнить — пагинация кусочных объёмов)

**Interfaces (produces):**
```csharp
Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default);
Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(string cluster, string shard, string key, CancellationToken ct = default);
```
Реализация `PutObjectMd5Async`: как `PutObjectAsync`, но `request.MD5Digest = Convert.ToBase64String(Convert.FromHexString(md5Hex))` (base64 БАЙТОВ хэша). `GetObjectBytesAsync`: `GetObjectResponse` → `ResponseBytes`(`MemoryStream`).ToArray() + `ETag` (кавычки MinIO снять: `etag.Trim('"')`).

- [ ] **Шаг 1 (кодер) — интерфейс + реализации + проброс PoolBackupS3 + починка всех фейков (компилятор укажет). Транспортный маршрут put — тот же, что у PutObjectAsync (без изменений таймаутов).**
- [ ] **Шаг 2 — интеграционный юнит пагинации кусочных объёмов** (в `BackupS3Tests`, паттерн соседних тестов — per-Fact OwnS3 + SeedClient): посеять 5 кусочных имён `c2/shard1/wal/000000010000000000000001.partial_…` (разной длины, валидный формат необязателен — листингу важны имена) → `s3.ListWalAsync("c2","shard1", maxKeysPerTest: 2)` → все 5 возвращены (пагинация list-v2 поверх кусочного семейства; spec решение №21, §8.9).
- [ ] **Шаг 3 — build; юнит-проект целиком зелёный (фейки починены); интеграционный тест прогнать по канону IntegrationTests (docker-контур OwnS3, полный teardown). Коммит** `feat(t15): IBackupS3 — put с Content-MD5, GET байтов с ETag; пагинация кусочных объёмов`.

**Проверка:** build 0 ворнингов; `dotnet test src/tests/PgWorker.UnitTests -c Release --no-build` → Passed; `dotnet test src/tests/PgWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~BackupS3Tests"` → Passed.
**Связь со spec:** §4.4, решения №21/№22.

---

### Task 7: приёмник — опции, env, номер жизни в конфигурации сессии

**Вход:** Task 6 закоммичен.
**Файлы:**
- Modify: `src/PgWorker.PgNode/WalReceiverOptions.cs` — добавить поля
- Modify: `src/PgWorker.PgNode/WalReceiverLoop.cs` — `ReadPartialCompactIntervalSec()` (после `ReadPartialIntervalSec`, строка ~758) + проброс в `new WalReceiverOptions(...)` (строка ~333)
- Modify: `src/PgWorker.PgNode/WalReceiverTargets.cs` — чтение wal-life + `ChainLife` в результате
- Test: `src/tests/PgWorker.UnitTests/Backups/WalReceiverLoopTests.cs`, `WalReceiverTailTests.cs` (targets) — дополнить

**Interfaces (produces):**
```csharp
// WalReceiverOptions (в конец, после PartialIntervalSec):
int PartialCompactIntervalSec = 3600, uint ChainLife = 0, bool IsMasterSource = false
// WalReceiverTargetsResult:
string? Password, BackupPoolContext? Pool, string? Slot, string? Error, uint ChainLife = 0
// WalReceiverLoop:
internal static int ReadPartialCompactIntervalSec() // env PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC; мусор/<60 → 3600 (клэмп ≥60)
```
`WalReceiverTargets.ResolveAsync`: после успешного резолва пароля/пула — чтение `BackupNames.WalLifeKey(cluster, shard)` (failover-перебор endpoints по образцу `ReadPasswordAsync`; `WalLifeStateJson.TryParse` → ChainLife; ключа нет/битый → 0); транспортный отказ etcd → `WalReceiverTargetsResult.PolicyMissing($"wal-life read failed: {…}")` — transient-ожидание (тот же класс, что policy; spec §4.2).
`WalReceiverLoop`: в `new WalReceiverOptions(...)` добавить `PartialCompactIntervalSec: ReadPartialCompactIntervalSec(), ChainLife: targets.ChainLife, IsMasterSource: currentRole == WalReceiverRole.Master` (currentRole — локальная переменная цикла на момент построения options). `ReadPartialIntervalSec` перевести в `internal static` (так же новая функция).

- [ ] **Шаг 1 (кодер) — тест env-парсера** в `WalReceiverLoopTests`: `"120"` → 120; `""`/"abc"/"59" → 3600; `"3600"` → 3600 (`InternalsVisibleTo` PgWorker.UnitTests уже даёт доступ). **Шаг 2 — тест targets** в `WalReceiverTailTests` (или рядом с существующими тестами targets): фейк gateway с ключом wal-life `{"chain_life":5,…}` → `ChainLife == 5`; без ключа → 0; транспортный отказ → `WaitingPolicy` true. **Шаг 3 — реализация. Шаг 4 — прогон PgNode-подмножества юнитов + коммит** `feat(t15): приёмник — номер жизни и компакция в конфигурации сессии (wal-life из etcd)`.

**Проверка:** build; `--filter "FullyQualifiedName~WalReceiverLoopTests|FullyQualifiedName~WalReceiverTailTests|FullyQualifiedName~WalReceiverCoreTests"` → Passed.
**Связь со spec:** §4.2 (опции/targets), §4.7, решения №3/№26.

---

### Task 8: приёмник — дельта-памп, компакция, CoverageOf

**Вход:** Task 7 закоммичен (Options несёт ChainLife/IsMasterSource/PartialCompactIntervalSec; S3-методы из Task 6).
**Файлы:**
- Modify: `src/PgWorker.Backups/Model/WalDeltaName.cs` — добавить `CoverageOf`
- Modify: `src/PgWorker.PgNode/WalReceiverCore.cs` — переписать partial-памп (строки ~395–468) + `PutDeltaWithRetryAsync`
- Test: `src/tests/PgWorker.UnitTests/Backups/WalReceiverCoreTests.cs` (дополнить), `WalDeltaNameTests.cs` (CoverageOf)

**Interfaces (produces):**
```csharp
// WalDeltaName:
public static ulong CoverageOf(IEnumerable<string> names, uint chainLife);
// WalReceiverCore (private):
private static async Task<Result> PutDeltaWithRetryAsync(
    IBackupS3 s3, WalSessionEvents events, WalReceiverOptions o,
    string key, byte[] data, string md5Hex, TimeSpan retry, CancellationToken ct,
    PoolExhaustionFlag exhaustion)
```
`CoverageOf` (чистая): из имён кусков с `ChainLife == chainLife` — жадное покрытие от 0: отсортировать по `Start`; coverage=0; повторять до фиксации: кусок с `Start ≤ coverage < End` → coverage = End; 0 — если нет стыкующихся с 0 (базы нет). Финалы и t14-версии не парсируются `WalDeltaName.TryParse` — игнорируются по построению.

Памп (spec §4.2, канон pgnode/03 §2; полная замена тела `partialPump = Task.Run(...)`):
- локальное состояние пампа: `string? pumpedSegment = null; ulong coverage = 0; var deltasSinceCompact = 0; var compactAt = DateTimeOffset.MaxValue;`
- на каждом пробуждении (таймер `PartialIntervalSec` / сигнал 4 МиБ — прежние): `var snap = assembler.PartialSnapshot(); if (snap is not { } shot) continue;`
- `if (shot.Name != pumpedSegment) { pumpedSegment = shot.Name; coverage = 0; deltasSinceCompact = 0; compactAt = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(Math.Max(60, o.PartialCompactIntervalSec)); }` — новый сегмент сессии: первая выкладка будет базой;
- `if (shot.EndLsn <= coverage) continue;` (нулевой прирост);
- компакция (проверяется в начале пробуждения, ДО выкладки дельты): `if (DateTimeOffset.UtcNow >= compactAt && deltasSinceCompact > 0) { var snap2 = assembler.PartialSnapshot(); if (snap2 is { } s2 && s2.EndLsn > coverage) { /* база [0..s2.EndLsn) тем же put-путём (start=0, тот же номер жизни); успех: coverage = s2.EndLsn; Volatile.Write(ref lastPartialEndLsn, s2.EndLsn); deltasSinceCompact = 0; */ } compactAt = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(Math.Max(60, o.PartialCompactIntervalSec)); }` — при нулевом приросте (`EndLsn <= coverage`) база НЕ перезаливается, таймер сдвигается (spec решение №10; пустых перезаливов нет);
- list — ПРЕФИКС БЕЗ ТОЧКИ `wal/{shot.Name}` (как t14-памп, строка 431): в листинг попадают И финал `wal/<seg>` (имя без суффикса), И всё кусочное семейство `wal/<seg>.…` (префикс строковый). Отказ list → diagnostic + continue (стрим не рвём). Лежащий финал (`obj.Name == shot.Name`) → continue навсегда (поглощение/чистка удалят) — проверка работает именно из-за префикса без точки (префикс `<seg>.` финал НЕ возвращает — не использовать);
- пересчёт: `var lying = WalDeltaName.CoverageOf(listed.Value.Select(x => x.Name), o.ChainLife); if (lying >= shot.EndLsn) { Volatile.Write(ref lastPartialEndLsn, lying); continue; } coverage = Math.Max(coverage, lying);` (кус-ты чужих номеров и безномерные legacy CoverageOf игнорирует по построению; финал в листинге CoverageOf не мешает);
- выкладка: байты = срез снимка `shot.Prefix` от `coverage - StartOfSegment` до `shot.EndLsn - StartOfSegment` (StartOfSegment = `((ulong)seg.Log * WalFileName.SegsPerLog + seg.Seg) * (ulong)WalFileName.SegmentBytes`; при `coverage == 0` — весь префикс от 0); `md5Hex = Convert.ToHexStringLower(MD5.HashData(bytes))`; ключ `WalDeltaName.Key(seg, coverage, shot.EndLsn, o.ChainLife, o.IsMasterSource, md5Hex)`;
- put — `PutDeltaWithRetryAsync` (копия петли `PutWithRetryAsync`: тот же `callBudget`/`attemptCts`/`events.PutError`/`PutRetry`/`PoolExhaustedFlag`/`WalArchiveConflict.Is`-возврат, но вызов `s3.PutObjectMd5Async(key, data, md5Hex, token)` — гварда НЕТ);
- успех: `if (coverage > 0) deltasSinceCompact++; coverage = shot.EndLsn; Volatile.Write(ref lastPartialEndLsn, shot.EndLsn); events.PutSuccess?.Invoke();`
- подтверждение слота/финалы/поглощение — БЕЗ ИЗМЕНЕНИЙ (поглощение финалом уже удаляет всё семейство префикса `"<seg>."`, вкл. куски).

- [ ] **Шаг 1 (кодер) — юниты `CoverageOf`** (в `WalDeltaNameTests`): пусто → 0; только дельта без базы → 0; база+примыкающие → end последней; дыра → до дыры; куски чужого life игнор; финал в списке имён — игнор. **Шаг 2 — юниты пампа** (в `WalReceiverCoreTests`, фейки-паттерны файла): (1) первый тик незакрытого сегмента → объект `…partial_0000000000000000_<end>_<life:x8>_m_<md5>` (база), байты = весь префикс; (2) второй прирост → дельта от конца первой; (3) прироста нет → нового объекта нет; (4) лежащий кусок соседа длиннее (list отдаёт финал+кусочное семейство с куском end больше) → приёмник не кладёт; (5) `EndLsn ≤ coverage` → skip; (6) лежащий ФИНАЛ сегмента → куски не выкладываются (list-префикс без точки возвращает финал); (7) компакция: `PartialCompactIntervalSec` мал (например 1) + после первой дельты прошёл интервал → база `[0..новый конец)` положена, счётчик дельт сброшен; тик без прироста после компакции — нового объекта нет; (8) сессия с `ChainLife: 2` поверх лежащих кусков `life1` → база кладётся с `00000002`; (9) подтверждение слота (`SetConfirmed`) вызывается ТОЛЬКО на закрытии сегмента (существующий инвариант — не сломать); (10) S3-отказы put при активных дельтах (фейк возвращает Failed на put) → памп ретраит/молчит, СТРИМ НЕ РВЁТСЯ (backpressure), `SetConfirmed` не вызывается — подтверждение только по финалу (spec §8.4).
- [ ] **Шаг 3 — реализация. Шаг 4 — весь `WalReceiverCoreTests` + `WalReceiverLoopTests` зелёные; коммит** `feat(t15): дельта-памп приёмника — база+дельты с номером жизни, компакция, Content-MD5`.

**Проверка:** build; `--filter "FullyQualifiedName~WalReceiverCoreTests|FullyQualifiedName~WalDeltaNameTests"` → Passed.
**Выход:** приёмник инкрементален; RPO-инвариант не тронут (backpressure-юнит).
**Связь со spec:** §4.2 (весь), §8.2/8.4, решения №7/№8/№9/№10/№11/№28/№29; Review Focus №5.

---

### Task 9: fetch-wal — сборка сегмента из кусков

**Вход:** Task 8 закоммичен.
**Файлы:**
- Modify: `src/PgWorker.PgNode/Operations/FetchWalCommand.cs` — `FetchWalContext` (+`uint? ChainLife`), `ParseEnv` (+`PGW_WAL_CHAIN_LIFE`), чистые функции сборки, `FetchOnceAsync` (финальная ветка), `LocalSetCoversPartial`
- Test: `src/tests/PgWorker.UnitTests/PgNode/FetchWalCommandTests.cs` (новый; `InternalsVisibleTo` уже есть)

**Interfaces (produces, все `internal static`, чистые):**
```csharp
internal static readonly int DeltaDownloadParallelism = 8;
internal sealed record AssemblyCandidate(string Name, ulong Start, ulong End, bool IsMaster, string? Md5Hex);
// Md5Hex null — безномерная t14-версия (сверка по ETag); legacy Start = начало сегмента, End = EndLsn имени.
internal static IReadOnlyList<AssemblyCandidate> FilterDeltaInventory(
    IReadOnlyList<(string Name, long Size)> objects, uint? chainLife, WalFileName segment);
internal static IReadOnlyList<AssemblyCandidate> OrderCandidates(IReadOnlyList<AssemblyCandidate> c);
internal sealed record AssemblyResult(ulong Coverage, bool Conflict, ulong? HoleStart);
internal static AssemblyResult GreedyAssemble(
    IReadOnlyList<AssemblyCandidate> candidates, IReadOnlyDictionary<string, byte[]> bytes);
internal static AssemblyResult SurvivalAssemble(
    IReadOnlyList<AssemblyCandidate> candidates, IReadOnlyDictionary<string, byte[]> bytes,
    IReadOnlyDictionary<string, string?> etags);
```
Семантика (spec §4.3, канон g-процесса):
- `FilterDeltaInventory`: парс `WalDeltaName` — при `chainLife != null` отброс `ChainLife != chainLife`; парс `WalPartialName` — безномерная legacy: отброс, если у сегмента есть кусок `chainLife` (при `chainLife == null` — все кандидаты); сверка `End − Start == Size` (для legacy: `End − segmentStart == Size`) — несовпадение/неразбор/`Start >= End` → отброс; `Start >= segmentStart`, `End <= конец сегмента`.
- `OrderCandidates`: `IsMaster desc → Start asc`.
- `GreedyAssemble`: coverage=0; кандидат применяется, если `End > coverage && Start <= coverage` и байты `[Start..coverage)` совпали с буфером → coverage=End; `End <= coverage` — молча пропустить; `Start > coverage` — HoleStart=min; несовпадение байтов → Conflict=true (сборка останавливается, результат — coverage до конфликта).
- `SurvivalAssemble`: пересборка с нуля, кандидаты по возрастанию md5 (legacy — по ETag-строке, ETag null — в конце); каждый применяемый кусок после стыка проверяется `WalPrefixScanner.ValidatedPrefixLength` затронутой области `[max(Start, prevCoverage)..coverage)` — несовпадение байтов пересечения ИЛИ валидный префикс затронутой области короче добавленных куском байт (записи битые) → кусок отбрасывается без ретрая; итог — coverage первой полностью валидной сборки.
- `FetchOnceAsync` финальная ветка: листинг union по всем клиентам узкого префикса `wal/<seg>.` (дедуп по имени, `anyResponded`-семантика прежняя; финал в листинге уже исключён предыдущим шагом — он качался первым) → `FilterDeltaInventory` → пусто → exit 1 → скачивание параллельно (`SemaphoreSlim(DeltaDownloadParallelism)`, `GetObjectBytesAsync` перебором клиентов до найденного; сверка: md5 байтов != `Md5Hex` имени → stderr-факт `fetch-wal: delta {name}: md5 mismatch`, отброс; безномерная legacy — ETag одиночного put = md5 содержимого: `Convert.ToHexStringLower(MD5.HashData(bytes))` != `etag.Trim('"')` → тот же отброс с фактом) → `GreedyAssemble`; конфликт → stderr-факт + `SurvivalAssemble` → дыра (HoleStart при наличии кусков выше) → stderr-факт `fetch-wal: delta hole at …` → результат `[0..coverage)` → финальная скан-валидация `ValidatedPrefixLength` (усечение + факт) → `TryWritePaddedSegment` → exit 0. `LocalSetCoversPartial(segment, coverage)` — сравнение последнего ненулевого байта копии с `coverage` (абс. LSN; прежняя арифметика `segmentStart`).
- `ParseEnv`: `uint.TryParse(env("PGW_WAL_CHAIN_LIFE"))` — мусор/отсутствие → null (DR-режим).

- [ ] **Шаг 1 (кодер) — тесты чистых функций** (`FetchWalCommandTests`): union-фильтрация (чужой life невидим; `null` — все кандидаты); size-мд5-отбросы; порядок m/r→start; жадная сборка: склейка/пересечение-union/конфликт/дыра; выживание: md5 asc + CRC-отброс битого куска; `ParseEnv` номер/мусор. Фейк `IBackupS3` для `FetchOnceAsync`-интеграционных кейсов: база+дельта склеились → файл записан (паттерн фейков `WalReceiverCoreTests`). **Шаг 2 — реализация. Шаг 3 — прогон; коммит** `feat(t15): fetch-wal — сборка сегмента из дельта-кусков (фильтр жизни, выживание, дыры, CRC)`.

**Проверка:** build; `--filter "FullyQualifiedName~FetchWalCommandTests"` → Passed.
**Связь со spec:** §4.3 (весь), решения №14/№15/№16/№17/№23/№34/№35/№39; Review Focus №2/№3/№4.

---

### Task 10: WalLifeStore (чтение/инкремент счётчика)

**Вход:** Task 3 закоммичен (использует WalLifeStateJson/WalLifeKey). Фейк `FakeEtcdGateway` (src/tests/PgWorker.UnitTests/Api/FakeEtcdGateway.cs) поддерживает только Version-компарер — расширяется подпунктом 10.1.
**Файлы:**
- Create: `src/PgWorker.Backups/WalLifeStore.cs`
- Modify: `src/tests/PgWorker.UnitTests/Api/FakeEtcdGateway.cs` (подпункт 10.1)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalLifeStoreTests.cs` (новый)

**Interfaces (produces):**
```csharp
public sealed class WalLifeStore(IEtcdGateway etcd, string[] endpoints)
{
    public Task<Result<(WalLifeState State, ulong ModRevision)?> ReadAsync(string cluster, string shard, CancellationToken ct);
    // null — ключа нет (жизнь 0); битый JSON трактуется как жизнь 0.
    public async Task<Result<uint>> ReadValueAsync(string cluster, string shard, CancellationToken ct); // → State?.ChainLife ?? 0
    public Task<Result<uint>> BumpAsync(string cluster, string shard, WalLifeReason reason, long? startedUnix, CancellationToken ct);
    public static bool IsForeignChainFact(WalReceiverAgentFacts fact); // State=="failed_permanent" && Error содержит "foreign WAL chain"
}
```
`BumpAsync` семантика (spec §4.6, решения №31/№32 — реализация идемпотентности):
1. прочитать ключ (failover-перебор; отказ → Failed);
2. гейты «уже сделано»: `ForeignChain` — текущая `Reason == ForeignChain` → Success(текущий Life) без записи; `Restore`/`ShardRebuilt` — `startedUnix != null && Reason == reason && BumpedUnix >= startedUnix` → Success(Life);
3. txn: ключа нет → `TxnRequest.Of([TxnCompare.NotExists(key)], [put значения {"chain_life":1,"bumped_unix":now,"reason":…}])`; есть → `[TxnCompare.ModRevisionEqual(key, modRevision)]` + put `chain_life+1`;
4. `!Succeeded` (гонка с другим bump'ом) — ОДНА повторная итерация (перечитать → гейты → txn); вторая неудача → Failed (вызвалющий ретраит тиком — условия причин персистентны).
`IsForeignChainFact`: `fact.State == "failed_permanent" && fact.Error?.Contains("foreign WAL chain", StringComparison.Ordinal) == true`.

- [ ] **Шаг 0 — расширение FakeEtcdGateway (подпункт 10.1: реальные ModRevision + TxnTarget.ModRevision в txn-компарере); прогнать существующие тесты фейка (Api-хендлеров) — не сломались.**
- [ ] **Шаг 1 (кодер) — юниты на обновлённом фейке**: (1) ключа нет → `ReadAsync` null, `ReadValueAsync` 0; (2) bump Restore с `startedUnix=100` при значении `{"chain_life":2,"bumped_unix":50,"reason":"shard-rebuilt"}` → пишет 3/restore/now, `ReadValueAsync` 3; (3) повторный тот же bump (`startedUnix=100`, теперь bumped_unix=now ≥ 100, reason=restore) → без записи, Success(3); (4) ForeignChain-гейт: reason=foreign-chain в ключе → повторный ForeignChain-bump без записи; после restore-bump — ForeignChain-bump инкрементирует; (5) конкурентный txn: между Read и Bump сторонний put меняет ключ → ModRevisionEqual не сходится → вторая итерация перечитывает и инкрементирует от свежего значения; (6) `IsForeignChainFact` — позитив/негативы (failed_permanent с другой ошибкой; streaming). **Шаг 2 — реализация. Шаг 3 — коммит** `feat(t15): WalLifeStore — счётчик жизни цепочки (txn-идемпотентный инкремент, reason-гейт)`.

**Проверка:** build; `--filter "FullyQualifiedName~WalLifeStoreTests"` → Passed; Api-тесты фейка не сломаны (`--filter "FullyQualifiedName~Api"` → Passed).
**Связь со spec:** §4.6, решения №31/№32/№37.

**Подпункты (слабой модели):**

**10.1. В файле** `src/tests/PgWorker.UnitTests/Api/FakeEtcdGateway.cs` выполни ТРИ замены (остальное не трогай; публичный `Store` сохраняет тип `Dictionary<string,string>` — его используют другие тесты напрямую):

Замени метод `RangeAsync` на:

```csharp
    public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<Kv>>.Success(
            Store.Where(p => p.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(p => new Kv(p.Key, p.Value, ModRevOf(p.Key))).ToList()));
```

Замени метод `GetAsync` на:

```csharp
    public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
        => Task.FromResult(Result<Kv?>.Success(
            Store.TryGetValue(key, out var v) ? new Kv(key, v, ModRevOf(key)) : null));
```

Замени методы `PutAsync` и `TxnAsync` и добавь приватные члены сразу ПОСЛЕ строки `public Dictionary<string, string> Store { get; } = [];`:

```csharp
    public Dictionary<string, string> Store { get; } = [];

    // Мод-ревизии ключей (инкремент на каждый put; 0 — ключа нет): для
    // txn-компарера TxnTarget.ModRevision (как реальный etcd).
    private readonly System.Collections.Generic.Dictionary<string, ulong> _modRev = [];
    private ulong _revision;
```

```csharp
    public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
    {
        Store[key] = value;
        TouchModRev(key);
        return Task.FromResult(Result.Success());
    }
```

```csharp
    public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
    {
        // compare: Version (ключа нет/есть) и ModRevision (равенство с реальной
        // ревизией ключа; ключа нет — ревизия 0); сошёлся — применяем success-ветку.
        var succeeded = req.Compare.All(c => c.Target switch
        {
            TxnTarget.Version
                => (!Store.ContainsKey(c.Key) && c.Num == 0) || (Store.ContainsKey(c.Key) && c.Num != 0),
            TxnTarget.ModRevision => ModRevOf(c.Key) == (ulong)c.Num,
            _ => false,
        });
        if (succeeded)
            foreach (var op in req.Success)
                ApplyTxnOp(op);
        return Task.FromResult(Result<TxnResult>.Success(new TxnResult(succeeded)));
    }
```

И в `ApplyTxnOp` вызов put замени на `Store[put.Key] = put.Value; TouchModRev(put.Key);`, а в конец класса добавь два приватных метода:

```csharp
    private ulong ModRevOf(string key)
        => _modRev.TryGetValue(key, out var rev) ? rev : 0;

    private void TouchModRev(string key)
        => _modRev[key] = ++_revision;
```

Поведение для существующих тестов (Version-компарер, чтения) не меняется: ревизия 0 у отсутствующего ключа, ненулевая у существующего.

---

### Task 11: контроль воркера — номер в TailOf + bump по гварду

**Вход:** Tasks 4 и 10 закоммичены.
**Файлы:**
- Modify: `src/PgWorker.Backups/WalStreamProcess.cs` — `ControlDueAsync`
- Test: детект-факт — юнит в `WalLifeStoreTests` (Task 10); интеграционная проверка смены жизни — E2E Task 18 (п.4).

**Действие (кодер):**
- [ ] В начале `ControlDueAsync` (после `_lastVerifyUnix[key] = now;`): `var walLife = new WalLifeStore(etcd, endpoints);` (поля ctor класса — те же `etcd`/`endpoints`, что у `new WalReceiverStatusReader(etcd, endpoints)` строка 373);
- [ ] foreign-chain-bump — ДО чтения значения (свежий номер сразу виден TailOf): при `agentFacts?.Any(WalLifeStore.IsForeignChainFact) == true` вызвать `await walLife.BumpAsync(cluster, shard, WalLifeReason.ForeignChain, startedUnix: null, ct)`; неудача bump НЕ роняет тик — записать однострочный факт отказа в журнал по паттерну соседних веток `ControlDueAsync` (строка вида `backup-wal {cluster}/{shard}: wal-life bump failed: <первая строка ошибки>`): факт failed_permanent персистентен (пока приёмник заморожен), следующий контроль догонит bump;
- [ ] чтение: `var chainLifeRead = await walLife.ReadValueAsync(cluster, shard, ct); if (!chainLifeRead.IsSuccess) throw new ApplicationException($"read wal-life {cluster}/{shard}: {chainLifeRead.Error!.Message}");` — transient-фейл тика (тот же класс, что отказ S3-list выше по методу: контроль повторит тиком; НЕ молчаливый 0 — при неверном номере TailOf потерял бы живой хвост); далее `var chainLife = chainLifeRead.Value;`
- [ ] заменить ВРЕМЕННЫЙ вызов Task 4: `WalChain.TailOf(chain.LastSegment, objects.Select(o => o.Name), chainLife)`; удалить временный комментарий.
- [ ] Прогон `WalChainTests` + build; коммит `feat(t15): контроль цепи — хвост по кускам текущей жизни; bump wal-life по гварду foreign-chain`.

**Проверка:** build; `--filter "FullyQualifiedName~WalStream|FullyQualifiedName~WalLifeStoreTests|FullyQualifiedName~WalChainTests"` → Passed.
**Связь со spec:** §4.5, решения №19/№32(в).

---

### Task 12: restore — chain_life заявки, bump при demolished, env fetch-wal

**Вход:** Tasks 9, 10 закоммичены.
**Файлы:**
- Modify: `src/PgWorker.Etcd/Parsing/BackupsModel.cs` — `RestoreOperationState` + `uint? ChainLife = null` (после `SourcePool`), `FullBackupState` + `uint? ChainLife = null` (последним позиционным параметром — существующие 10-аргументные вызовы не ломаются)
- Modify: `src/PgWorker.Etcd/Parsing/BackupsParser.cs` — `TryParseRestore`/`TryParseFull` читают `chain_life` (толерантно: отсутствует → null)
- Modify: `src/PgWorker.Backups/Restore/RestoreStatusJson.cs` — сериализация `chain_life` — snake_case `"chain_life"`, ручной ключ в Dictionary (паттерн соседних полей `"source_pool"`/`"backup_id"`; camelCase-ключ сломал бы roundtrip с BackupsParser)
- Modify: `src/PgWorker.Backups/Process/RestoreProcess.cs` — подстановка `ChainLife` в шаге валидации (п.7 `running`), bump в `DemolishAsync`
- Modify: `src/PgWorker.PgNode/Operations/ApiContracts.cs` — `StartRestoreRequest` + `[property: JsonPropertyName("chain_life")] uint? ChainLife = null`
- Modify: `src/PgWorker.PgNode/Operations/RestoreOperation.cs` — env `PGW_WAL_CHAIN_LIFE` в `PgCtl` (строки ~499–507)
- Test: `src/tests/PgWorker.UnitTests/Backups/RestoreStatusJsonTests.cs` (дополнить), `WalLifeStoreTests` (bump-кейс restore — уже есть)

**Действие (кодер):**
- [ ] `RestoreProcess`, валидация (после резолва `backupId`, перед п.7): найти full-статус выбранного в `mine?.Shards[srcX].Full` (по `f.Id == backupId`); `var chainLife = full?.ChainLife;` — DR-orphan (статуса нет) остаётся null. В `running` добавить `ChainLife = chainLife`.
- [ ] `DemolishAsync` — bump ДО записи фазы (строка ~476, ПЕРЕД `PutStatusAsync(... op with { Phase = "demolished" })`): `var life = await new WalLifeStore(etcd, endpoints).BumpAsync(cluster, shard.Name, WalLifeReason.Restore, startedUnix: op.StartedUnix, ct); if (!life.IsSuccess) return Result<ProcessOutcome>.Failed(life.Error!);` — порядок обязателен: bump-до-фазы делает повторный тик безопасным в ОБЕ стороны (put фазы упал → повтор: гейт времени `bumped_unix >= op.StartedUnix` пропускает уже сделанный bump, put доезжает; bump упал → повторный тик входит в DemolishAsync снова — Phase ещё не записана); после записи фазы повторные тики в DemolishAsync не заходят (switch по Phase).
- [ ] `RestoreOperation.PgCtl`: после `PGW_S3_*` — `if (_request.ChainLife is { } lifeValue) psi.EnvironmentVariables["PGW_WAL_CHAIN_LIFE"] = lifeValue.ToString();` (наследуется postgres'ом → fetch-wal).
- [ ] Тесты `RestoreStatusJsonTests`: roundtrip с/без `chain_life` (поле опционально — старые ключи читаются). Проверка компиляции `RestoreOperationTests` (поле с default — вызовы не ломаются).
- [ ] Коммит `feat(t15): restore — заявка несёт chain_life выбранного полного; bump wal-life при demolished; env fetch-wal`.

**Проверка:** build; `--filter "FullyQualifiedName~RestoreStatusJsonTests|FullyQualifiedName~RestoreOperationTests|FullyQualifiedName~WalLifeStoreTests"` → Passed.
**Связь со spec:** §4.6 (RestoreProcess), §4.7, решения №32(б)/№33/№34/№35.

---

### Task 13: полный бэкап + drill — фиксация/передача chain_life

**Вход:** Task 12 закоммичен (FullBackupState.ChainLife уже добавлен).
**Файлы:**
- Modify: `src/PgWorker.Backups/Job/BackupStatusJson.cs` — `Serialize` пишет `"chain_life"` — snake_case ручной ключ в Dictionary (паттерн соседних `"wal_start_segment"`/`"size_bytes"`), по факту (null не сериализуем)
- Modify: `src/PgWorker.Backups/Process/BackupProcess.cs:343` — перед конструированием `planned` прочитать счётчик: `var lifeRead = await new WalLifeStore(etcd, endpoints).ReadValueAsync(cluster, shard.Name, ct);` при `!lifeRead.IsSuccess` — transient-возврат шага по образцу соседних etcd/S3-отказов метода (тик повторит; НЕ дефолт 0 — неверный номер в манифесте отрезал бы restore-заявке её хвост), иначе `new FullBackupState(...)` + `ChainLife: lifeRead.Value`
- Modify: `src/PgWorker.Backups/Process/RestoreDrillProcess.cs` — оба `new StartRestoreRequest(...)` (строки ~201, ~332) + `ChainLife: <chain_life выбранного полного из ShardBackups.Full по BackupId>` (нет статуса — null)
- Test: `src/tests/PgWorker.UnitTests/Backups/BackupStatusJsonTests.cs` (дополнить)

- [ ] **Шаг 1 (кодер) — тест roundtrip `BackupStatusJson`** (chain_life пишется при наличии, отсутствует при null; парсер `BackupsParser.TryParseFull` читает). **Шаг 2 — правки. Шаг 3 — прогон `BackupStatusJsonTests` + `BackupProcessTests` + `DrillPlannerTests`; коммит** `feat(t15): полный бэкап хранит chain_life; drill передаёт номер fetch-wal`.

**Проверка:** build; `--filter "FullyQualifiedName~BackupStatusJsonTests|FullyQualifiedName~BackupProcessTests|FullyQualifiedName~DrillPlannerTests"` → Passed.
**Связь со spec:** §4.6 (планировщик), §4.7 (drill), решения №33.

---

### Task 14: add-shard — bump пересоздания

**Вход:** Task 10 закоммичен.
**Файлы:**
- Modify: `src/PgWorker.Provisioning/Processes/AddShardProcess.cs` — A1 (после guard'ов, до A2), приватное поле-память прогонов
- Test: юниты идемпотентности BumpAsync — `WalLifeStoreTests` (Task 10); компилируемая связка — build; сквозная проверка — E2E Task 18 (п.4, через restore-путь пересоздания).

**Действие (кодер):**
- [ ] Поле: `private readonly ConcurrentDictionary<string, long> _runStartedUnix = new();` — cluster/shard → UTC-момент первого тика прогона.
- [ ] В A1, после успешных guard'ов, ДО A2 (ensure секрета): `var runKey = $"{cluster}/{shardName}"; var startedUnix = _runStartedUnix.GetOrAdd(runKey, _ => DateTimeOffset.UtcNow.ToUnixTimeSeconds()); var existing = await new WalLifeStore(etcd, endpoints).ReadAsync(cluster, shardName, ct);` — `existing.IsSuccess && existing.Value is not null` (ключ wal-life существует — пересоздание поверх непустого бэкап-контура) → `var bumped = await new WalLifeStore(etcd, endpoints).BumpAsync(cluster, shardName, WalLifeReason.ShardRebuilt, startedUnix, ct);` отказ чтения/bump → transient-возврат тика (journal last_error, повтор следующим тиком). Ключа нет → инкремента нет (первое создание, жизнь 0). Память `_runStartedUnix` чистится при завершении прогона (фаза done/aborted — там же, где чистятся `_patroniWaitSince`).
- [ ] Takeover-окно (bump прошёл, тик упал до nodes→PROVISIONING, новый инстанс): возможен лишний номер (пустая жизнь) — БЕЗВРЕДНО и принято (spec §7 №37: номер монотонен, жизней с пропуском не существует); комментарий в коде.
- [ ] Build; коммит `feat(t15): add-shard — инкремент wal-life при пересоздании шарда (счётчик существовал)`.

**Проверка:** build 0 ворнингов; `--filter "FullyQualifiedName~AddShard"` → Passed (существующие).
**Связь со spec:** §4.6 (AddShardProcess), решения №32(а)/№31 (remove-shard счётчик не удаляет).

---

### Task 15: sweeper/retention — вход номера, гвард restore

**Вход:** Tasks 5, 10 закоммичены.
**Файлы:**
- Modify: `src/PgWorker.Backups/Supervisor/WalSweeperProcess.cs` (~строка 149) и `src/PgWorker.Backups/Retention/RetentionProcess.cs` (~строка 162)
- Test: `src/tests/PgWorker.UnitTests/Backups/WalSweeperProcessTests.cs`, `App/WalSweeperLoopTests.cs` (дополнить)

**Действие (кодер):** в обоих местах заменить временный `chainLife: null` на вычисление: `uint? chainLife = null; if (!await HasActiveRestoreAsync(cluster, shard, ct)) { var life = await new WalLifeStore(etcd, endpoints).ReadValueAsync(cluster, shard, ct); if (life.IsSuccess) chainLife = life.Value; }` — helper `HasActiveRestoreAsync` по образцу `WalSweeperProcess.ReadBackupsAsync` (строка ~163): range `/pgworker/backups/<C>/<X>/restore/` через `etcd.RangeAsync` → `BackupsParser.Parse(range.Value, out _)` (ПУБЛИЧНЫЙ вход парсера; приватные TryParseFull/TryParseRestore снаружи недоступны) → `parsed?.Shards.TryGetValue(shard, out var sb) == true && sb.Restores.Any(r => r.State is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining)`. Активная заявка → chainLife null (ветка чужих выключена — куски жизни заявки читает fetch-wal наката; spec решение №20). Отказ чтения → chainLife null (консервативно).

- [ ] **Шаг 1 — юниты** (`WalSweeperProcessTests`): активный restore-ключ → чужие куски НЕ в batch-delete; нет заявки → в delete; чтение счётчика участвует. **Шаг 2 — реализация. Шаг 3 — коммит** `feat(t15): sweeper/ретенция — чистка чужих номеров жизни; гвард активного restore`.

**Проверка:** build; `--filter "FullyQualifiedName~WalSweeper"` → Passed.
**Связь со spec:** §4.1(д)/§4.6, решение №20; Review Focus №4.

---

### Task 16: конфиг/env-доставка компакции + fail-fast

**Вход:** Task 7 закоммичен.
**Файлы:**
- Modify: `src/PgWorker.Backups/Options.cs` — `BackupsRuntimeOptions` + `int WalPartialCompactIntervalSec = 3600` (после `WalPartialIntervalSec`)
- Modify: `src/PgWorker.App/Options.cs` — `BackupsWalOptions` + `public int PartialCompactIntervalSec { get; set; } = 3600;` (после `PartialIntervalSec`); `ToRuntime` (строка ~455) + `WalPartialCompactIntervalSec: Wal.PartialCompactIntervalSec`; fail-fast — условие в `BackupsOptions.IsValid()` (строка ~480, рядом с `Wal.PartialIntervalSec >= 1`): добавить `&& Wal.PartialCompactIntervalSec >= 60` с комментарием `// период компакции дельт приёмника (t15, arch/backups/09 §1): чаще минуты — разрушает инкрементальность`
- Modify: `src/PgWorker.Core/Templates/NodeConfigBuilders.cs` — `NodeBackupEnv` + `int? PartialCompactIntervalSec = null` (после `PartialIntervalSec`)
- Modify: `src/PgWorker.App/Program.cs` (~строка 357–362) — проброс `PartialCompactIntervalSec: opts.Backups.Wal.PartialCompactIntervalSec >= 60 ? opts.Backups.Wal.PartialCompactIntervalSec : null`
- Modify: `src/PgWorker.Docker/Drivers/ClusterDriver.cs` (~строка 587) — по образцу `PGW_WAL_PARTIAL_INTERVAL_SEC`: `if (backupEnv.PartialCompactIntervalSec is >= 60) env["PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC"] = backupEnv.PartialCompactIntervalSec.Value.ToString();`
- Test: `src/tests/PgWorker.UnitTests/Docker/ClusterDriverTests.cs` (дополнить: env ставится/не ставится).

- [ ] **Шаги 1–3 (кодер):** тесты → правки → прогон `ClusterDriverTests`. **Коммит** `feat(t15): конфиг PartialCompactIntervalSec — биндинг, fail-fast <60, env-доставка ноды`.

**Проверка:** build; `--filter "FullyQualifiedName~ClusterDriverTests"` → Passed.
**Связь со spec:** §4.7, решение №26.

---

### Task 17: панель — wal-life в снапшот + алерт backup-wal-life-changed

**Вход:** Tasks 3, 12 закоммичены (формат ключа и chain_life полных).
**Файлы:**
- Modify: `src/AdminPanel.Core/BackupsInfo.cs` — `public sealed record WalLifeInfo(string Cluster, string Shard, long ChainLife, long? BumpedUnix, string? Reason);` + в `ClusterBackupsInfo` (файл `BackupInfo.cs`) поле `IReadOnlyDictionary<string, WalLifeInfo>? WalLife` (per-shard; null — ключей нет)
- Modify: `src/AdminPanel.Etcd/Parsing/BackupsParser.cs` — разбор `…/<X>/wal-life` (segments==6 && segments[5]=="wal-life"): поля `chain_life`/`bumped_unix`/`reason` (snake_case — те же ключи, что пишет воркер Task 3); битый JSON → parseError-запись, ключ не падает (толерантно, образец drill-ветки)
- Create: `src/AdminPanel.Core/Alerting/Rules/BackupWalLifeChangedRule.cs` (по образцу `BackupWalTliGapRule`/`BackupDeletingStuckRule`)
- Modify: `AlertsOptions` (в `AdminPanel.Core/Alerting/`) — `Backups` + `public int WalLifeChangedSec { get; set; } = 3600;` (дефолт, каталожный)
- Test: `src/tests/AdminPanel.UnitTests/` — новый `BackupWalLifeChangedRuleTests` + парсер-кейс в существующих тестах парсера

**Правило (точно):** kind `backup-wal-life-changed`; per-shard живого Active-кластера; условие: `WalLife.BumpedUnix` существует и `nowUnix - BumpedUnix < WalLifeChangedSec` (>0 из конфига, иначе дефолт 3600); severity: `Reason == "foreign-chain"` → Warning, иначе (`restore`/`shard-rebuilt`/null) → Info; id `$"backup-wal-life-changed:{cluster}/{shard}"`; target `$"{cluster}/{shard}"`; message: `WAL chain life changed for shard {shard} of cluster {cluster}: life {ChainLife} (reason: {Reason ?? "unknown"}) — {foreign-chain ? "foreign WAL chain detected, receiver frozen, operator review required" : "regular restore/rebuild"}`; Remedy: foreign-chain → OperatorRunbook («разбор по runbook restore: в префиксе чужая цепочка»), иначе WorkerAuto.

**Лента /events — отдельной правки НЕ требует** (фиксируется явно): ключ `wal-life` попадает в снапшот (новое поле `WalLife` в `ClusterBackupsInfo`); лента — клиентские диффы prev→next опрашиваемых моделей (arch/adminpanel/02 §5, arch/adminpanel/03 §3 «События»), смена `chain_life` N→N+1 отображается state-change'ом автоматически. Оговорка записана в spec §8.10.

- [ ] **Шаги (кодер):** тесты правила (info/warning по reason; гаснет по возрасту — BumpedUnix старше порога → нет алерта; без ключа → нет; NOT_INITIALIZED-кластер → нет) + парсер (валидный/битый JSON) → реализация → прогон `AdminPanel.UnitTests`. **Коммит** `feat(t15): панель — ключ wal-life в снапшоте, алерт backup-wal-life-changed (info/warning)`.

**Проверка:** build; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build` → Passed.
**Связь со spec:** §4.8, решение №38, канон adminpanel/03 §4; E2E алерт не проверяет — осознанное решение (spec §5 Ф5/§8.10: панель в E2E-контуре PgWorker не поднимается).

---

### Task 18: E2E — три новых сценария + расширение rebuild

**Вход:** Tasks 8–15 закоммичены; юниты зелёные.
**Файлы:**
- Create: `src/tests/PgWorker.E2eTests/E2e/E2eWalDeltaScenarios.cs` (новый класс `E2eWalDeltaScenarios(E2eRestoreScenariosFx Ctx) : IClassFixture<E2eRestoreScenariosFx>` — переиспользует фикстуру restore-сценариев: контур кластера+воркера с бэкапами; образцы: `E2eRestoreScenarios.Restore_Latest_RebuildsDestroyedShard`, `E2eDrillScenarios.Drill_Failed_OnCorruptedWal` (host-клиент `BackupS3`, `PutRestorePlannedAsync`, `WaitRestoreAsync`))
- Modify: `src/tests/PgWorker.E2eTests/E2e/E2eRestoreScenarios.cs` — расширить `Restore_Latest_RebuildsDestroyedShard` ассертами жизни.

**Сценарии (кодер; канон E2E-изоляции AGENTS.md обязателен):**

1. `Restore_Latest_WalDeltaHoleCoversPrefix`: поднять кластер+воркер; дождаться полного и живого WAL-хвоста; ВЫКЛЮЧИТЬ приёмники не нужно — тест сам выкладывает СИНТЕТИЧЕСКИЙ кусочный набор поверх живого префикса: выбрать целевой сегмент `<seg>` восстановительной цепочки; host-клиентом `BackupS3.PutObjectAsync` положить в `<C>/<X>/wal/`: базу `WalDeltaName.Key(seg, 0, A, life, true, md5(байты[0..A)))` и дельту `Key(seg, B, C, life, …)` — дыра `[A..B)` (байты — реальный префикс из скачанного `wal/<seg>` или `full/<id>/pg_wal/<seg>`; номер `life` — прочитать ключ wal-life etcd фикстуры, при отсутствии 0; md5 — `MD5.HashData`). Удалить финал `wal/<seg>` и легитимные куски/версии префикса сегмента (чтобы сборка шла из выложенного набора). Запустить restore latest; ассерты: restore зелёный; строки, закоммиченные до LSN A, восстановлены; строки после дыры — нет; факт `fetch-wal: delta hole` в логах операции/`restored_to_lsn ≤ A`.
2. `Restore_Latest_WalDeltaConflictSurvival`: тот же контур; выложить куски с КОНФЛИКТНЫМ пересечением (дельта-дубль диапазона с испорченными байтами копии: пересечение `[S..E)` совпадает у базы, хвост различается; второй кусок с МЕНЬШИМ md5 — валидные байты); restore latest зелёный; собранный префикс консистентен (режим выживания выбрал валидный кусок; данные до конца валидного покрытия на месте).
3. `Restore_Latest_WalDeltaSourceFailover`: живой поток с записью; patroni switchover МЕЖДУ выкладками дельт (образец switchover-механики `E2eWalStreamPromoteScenarios`); дождаться кусков обеих ролей (`_m_`/`_r_`) одного номера жизни; restore latest доезжает до конца хвоста; ассерт: ключ wal-life НЕ инкрементирован (failover жизнь не меняет; читать etcd до/после).
4. Расширение `Restore_Latest_RebuildsDestroyedShard`: после COMPLETED — читать ключ wal-life: `chain_life ≥ 1`, `reason == "restore"`; листинг `wal/`: новые куски (если хвост снова живой) несут номер `chain_life`; непосредственно после restore куски прежней жизни отсутствуют (sweeper/поглощение; допускается окно — ассерт с retry-поллингом ≤ 60 с, образец `E2eFixture.WaitForAsync`). Алерт `backup-wal-life-changed` E2E НЕ проверяет — юнит-покрытие Task 17 (осознанное решение, spec §5 Ф5/§8.10: панель в E2E-контуре PgWorker не поднимается).

- [x] **Шаг 1 (кодер) — сценарии + ассерты; таймауты сценариев ≤ 3 мин (CancelAfter по образцу); никаких скипов; teardown фикстуры полный (класс уже обеспечивает).**
- [x] **Шаг 2 — статическая подготовка и прогон НОВЫХ сценариев** (две отдельные команды, затем тесты `--no-build`):
  - `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
  - `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
  - `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~E2eWalDeltaScenarios|FullyQualifiedName~Restore_Latest_RebuildsDestroyedShard"`
- [x] **Шаг 3 — разбор упавших ПО ЛОГАМ (docker-логи/артефакты `/tmp/pgw-e2e-artifacts-*`, host-логи воркера; перезапуск упавших — только после анализа, канон AGENTS.md). Зачистка контейнеров/сетей после серии (`docker network prune -f` при осиротевших `pgw-*-net`). Коммит** `test(t15): E2E — дыра/конфликт/failover дельт; смена жизни в rebuild-scenario`.

**Проверка:** все новые сценарии Passed; повторный прогон стабилен.
**Связь со spec:** §5 Ф5, §8.5–8.7, решение №30 (алерт — юнит Task 17, осознанно).

---

### Task 19: финальный гейт

**Вход:** Tasks 0–18 закоммичены.
**Действие (кодер):**
- [ ] Полная сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo` — 0 ворнингов/ошибок.
- [ ] Все юниты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build` и `dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build` — зелёные (внеобластный фейл — НЕ чинить: задача в `arch/roadmap/bugs.md` по канону AGENTS.md, работа продолжается).
- [ ] Docker-E2EPgWorker гейт (трогает воркеров — обязателен, канон AGENTS.md): Е2ePrepare уже готов; фильтр-маркер `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~Scale_AddEmptyShard"`; затем ПОЛНАЯ серия E2eFixture (изменены provisioning (g-add-shard), wal-процессы). Между сериями — зачистка контейнеров/сетей/томов по канону.
- [ ] Самопроверка канона: grep по `src/`: `SystemId`/`IdentifySystem` НЕ появились в новых t15-местах (приёмник/кусочные имена); `PGW_WAL_CHAIN_LIFE`/`wal-life`/`chain_life` — на местах по спеке; `DeltaDownloadParallelism` = 8; таймауты не менялись (`CallDeadline`, `PutAttemptTimeout`, `TransportAttempts` — git diff чисел нет).
- [ ] Финальный коммит (правки по итогам гейта) `chore(t15): финальный гейт — серия зелёная`.
- [ ] Пометка на мерж: МЕРЖ-КОММИТ в main обязан удалить пункт t15-wal-partial-delta из `arch/roadmap/backup.md` тем же коммитом и проверить `grep -rn "t15" arch/roadmap/` (зависимостей `← t15` нет) — исполняется при мерже, НЕ сейчас.

**Проверка:** сборка/тесты/E2E зелёные; `git log --oneline` — цепочка задач.
**Выход:** ветка готова к ревью и мержу.
**Связь со spec:** §8 (все критерии), §6 (мерж-гейт).

---

## Порядок исполнения и зависимости

```
T0 → T1 → T2 → T3 → T4 → T5 → T6 → T7 → T8 → T9 → T10 → T11 → T12 → T13 → T14/T15/T16/T17 (параллельны после T12: T14←T10; T15←T5,T10; T16←T7; T17←T3,T12) → T18 → T19
```
Обязательные входы: T8 требует T6+T7; T9 требует T8 (CoverageOf в T8, ValidatedPrefixLength в T2); T11 требует T4+T10; T12 требует T9+T10; T15 требует T5+T10; T18 требует T8–T15.

## Соответствие задач требованиям spec (для ревью)

| Spec | Задачи |
|---|---|
| §4.1 модель (WalDeltaName/IsPartial/Scanner/TailOf/Retention/WalLifeStateJson) | T1, T2, T3, T4, T5 |
| §4.2 приёмник (опции/targets/памп/компакция/поглощение) | T7, T8 |
| §4.3 сборка fetch-wal | T9 |
| §4.4 S3-методы (+пагинация кусочных объёмов №21) | T6 |
| §4.5 контроль (фильтр, гвард-bump) | T4 (вызов), T10, T11 |
| §4.6 счётчик/restore/полный/add-shard | T10, T12, T13, T14 |
| §4.7 конфиг/env-доставка | T7 (env приёмника), T12 (env fetch-wal), T16 |
| §4.8 панель | T17 |
| §5 Ф5 E2E | T18, T19 (алерт панели — юнит T17, осознанное решение spec §5/§8.10) |
| §8 критерии | распределены по проверкам задач; 8.4 — T8 юнит №10; 8.9 — T4/T6; 8.10 — T17; 8.11–8.12 — T19 |
