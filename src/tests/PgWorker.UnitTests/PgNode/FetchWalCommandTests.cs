using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.S3;
using PgWorker.PgNode.Operations;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.PgNode;

// fetch-wal: сборка сегмента из дельта-кусков (t15, spec §4.3, процесс
// g-wal-delta-assembly): фильтр номера жизни/размера/диапазона, порядок
// кандидатов, жадная сборка с побайтовой сверкой пересечений, режим выживания
// (md5 asc + скан-валидация CRC), дыра-фолбэк, выдача покрытия.
public class FetchWalCommandTests
{
    private const int Page = WalPrefixScanner.PageBytes;
    private const long SegmentBytes = WalFileName.SegmentBytes;
    private const string SegmentName = "000000010000000000000005";

    private static WalFileName Segment => WalFileName.TryParse(SegmentName)!.Value;
    private static ulong SegStart => WalDeltaName.SegmentStartOf(Segment);

    // Валидный префикс сегмента: long-заголовок (tli=1) + записи заданных длин
    // с корректным xl_crc; возвращает байты [начало сегмента..конец последней).
    private static byte[] BuildPrefix(params int[] recordLengths)
    {
        var data = new byte[Page * 2];
        data[4] = 1; // xlp_tli = 1
        var pos = WalPrefixScanner.LongHeaderBytes;
        for (var i = 0; i < recordLengths.Length; i++)
        {
            if (pos % Page == 0)
                pos += WalPrefixScanner.ShortHeaderBytes;
            var len = recordLengths[i];
            if (pos + len > data.Length)
                Array.Resize(ref data, pos + len + Page);
            WriteRecord(data, pos, len, seed: i * 37);
            pos = (pos + len + 7) & ~7;
        }
        return data[..pos];
    }

    private static void WriteRecord(byte[] data, int pos, int len, int seed)
    {
        WriteLe32(data, pos, (uint)len);
        for (var i = 24; i < len; i++)
            data[pos + i] = (byte)(0x40 + (seed + i) % 0x3F);
        // CRC в порядке postgres: сначала ТЕЛО записи, затем заголовок до xl_crc
        var crc = Crc32C.Init();
        crc = Crc32C.Update(crc, data, pos + 24, len - 24);
        crc = Crc32C.Update(crc, data, pos, 20);
        WriteLe32(data, pos + 20, Crc32C.Finish(crc));
    }

    private static void WriteLe32(byte[] data, int offset, uint value)
    {
        for (var i = 0; i < 4; i++)
            data[offset + i] = (byte)(value >> (8 * i));
    }

    private static string Md5Hex(byte[] data) => Convert.ToHexStringLower(
        System.Security.Cryptography.MD5.HashData(data));

    // Кусок с байтами из префикса и корректным md5 в имени.
    private static (string Name, byte[] Data) Piece(
        byte[] prefix, int from, int to, uint chainLife = 1, bool isMaster = true)
    {
        var data = prefix[from..to];
        var name = WalDeltaName.Key(
            Segment, SegStart + (ulong)from, SegStart + (ulong)to, chainLife, isMaster, Md5Hex(data));
        return (name, data);
    }

    private static string LegacyName(ulong endOffset, byte[] data)
        => WalPartialName.Key(SegmentName, SegStart + endOffset);

    // ---- фильтры инвентаря ----

    // AAA: куски чужого номера жизни невидимы при заданном; без номера (DR) — все.
    [Fact]
    public void Filter_ForeignLife_Invisible()
    {
        // Arrange — куски жизней 1 и 2
        var prefix = BuildPrefix(64);
        var (life1, _) = Piece(prefix, 0, 104, chainLife: 1);
        var (life2, _) = Piece(prefix, 0, 104, chainLife: 2);
        (string, long)[] inventory = [(life1, 104), (life2, 104)];

        // Act
        var current = FetchWalCommand.FilterDeltaInventory(inventory, chainLife: 1, Segment);
        var dr = FetchWalCommand.FilterDeltaInventory(inventory, chainLife: null, Segment);

        // Assert
        current.Should().ContainSingle().Which.Name.Should().Be(life1);
        dr.Should().HaveCount(2);
    }

    // AAA: end−start ≠ размер объекта из листинга — кусок отброшен.
    [Fact]
    public void Filter_SizeMismatch_Dropped()
    {
        // Arrange — размер в листинге «не тот»
        var prefix = BuildPrefix(64);
        var (name, _) = Piece(prefix, 0, 104);
        (string, long)[] inventory = [(name, 103)];

        // Act
        var filtered = FetchWalCommand.FilterDeltaInventory(inventory, chainLife: 1, Segment);

        // Assert
        filtered.Should().BeEmpty();
    }

    // AAA (решение №39): t14-версия учитывается только пока у сегмента нет
    // кусков текущего номера; появился кусок — версия невидима.
    [Fact]
    public void Filter_LegacyWindow()
    {
        // Arrange — версия + кусок жизни 1
        var prefix = BuildPrefix(64);
        var (piece, _) = Piece(prefix, 0, 104);
        var legacy = LegacyName(0x100, prefix);
        (string, long)[] withPiece = [(piece, 104), (legacy, 0x100)];
        (string, long)[] onlyLegacy = [(legacy, 0x100)];

        // Act
        var current = FetchWalCommand.FilterDeltaInventory(withPiece, chainLife: 1, Segment);
        var legacyOnly = FetchWalCommand.FilterDeltaInventory(onlyLegacy, chainLife: 1, Segment);

        // Assert — кусок жизни 1 виден, версия при нём — нет; без кусков версия живёт
        current.Should().ContainSingle().Which.Name.Should().Be(piece);
        legacyOnly.Should().ContainSingle().Which.Name.Should().Be(legacy);
        legacyOnly[0].Start.Should().Be(SegStart);
        legacyOnly[0].Md5Hex.Should().BeNull();
    }

    // AAA: порядок кандидатов — m над r, затем ранний start.
    [Fact]
    public void Order_MasterFirst_ThenStart()
    {
        // Arrange
        var prefix = BuildPrefix(64, 64);
        var (replicaEarly, _) = Piece(prefix, 0, 104, isMaster: false);
        var (masterLate, _) = Piece(prefix, 104, 168, isMaster: true);
        var (masterEarly, _) = Piece(prefix, 0, 104, isMaster: true);

        // Act
        var ordered = FetchWalCommand.OrderCandidates(
            FetchWalCommand.FilterDeltaInventory(
                [(replicaEarly, 104L), (masterLate, 64L), (masterEarly, 104L)], 1, Segment));

        // Assert — сначала master (по start), затем replica
        ordered.Select(c => c.Name).Should().Equal(masterEarly, masterLate, replicaEarly);
    }

    // ---- сборка покрытия ----

    [Fact]
    public void Greedy_Join_CoverageToEnd()
    {
        // Arrange — база [0..104) + дельта [104..168)
        var prefix = BuildPrefix(64, 64);
        var (baseKey, baseData) = Piece(prefix, 0, 104);
        var (deltaKey, deltaData) = Piece(prefix, 104, 168);

        // Act
        var (result, buffer) = FetchWalCommand.GreedyAssembleCore(
            FetchWalCommand.OrderCandidates(
                FetchWalCommand.FilterDeltaInventory(
                    [(baseKey, (long)baseData.Length), (deltaKey, (long)deltaData.Length)], 1, Segment)),
            new Dictionary<string, byte[]> { [baseKey] = baseData, [deltaKey] = deltaData });

        // Assert — покрытие до конца дельты, байты = исходный префикс
        result.Conflict.Should().BeFalse();
        result.Coverage.Should().Be(SegStart + 168);
        buffer.Should().Equal(prefix);
    }

    [Fact]
    public void Greedy_OverlapUnion_IdenticalBytes()
    {
        // Arrange — база [0..104) и кусок [56..168) с совпадающими байтами
        var prefix = BuildPrefix(64, 64);
        var (baseKey, baseData) = Piece(prefix, 0, 104);
        var (overlapKey, overlapData) = Piece(prefix, 56, 168);

        // Act
        var result = FetchWalCommand.GreedyAssemble(
            [Candidate(baseKey, 0, 104, baseData), Candidate(overlapKey, 56, 168, overlapData)],
            new Dictionary<string, byte[]> { [baseKey] = baseData, [overlapKey] = overlapData });

        // Assert — union: покрытие по бóльшему end
        result.Conflict.Should().BeFalse();
        result.Coverage.Should().Be(SegStart + 168);
    }

    [Fact]
    public void Greedy_Conflict_StopsAtConflict()
    {
        // Arrange — кусок пересекает базу РАЗНЫМИ байтами
        var prefix = BuildPrefix(64, 64);
        var (baseKey, baseData) = Piece(prefix, 0, 104);
        var corrupted = (byte[])prefix[56..168].Clone();
        corrupted[10] ^= 0xFF; // пересечение с базой испорчено
        var conflictKey = WalDeltaName.Key(
            Segment, SegStart + 56, SegStart + 168, 1, true, Md5Hex(corrupted));

        // Act
        var result = FetchWalCommand.GreedyAssemble(
            [Candidate(baseKey, 0, 104, baseData), Candidate(conflictKey, 56, 168, corrupted)],
            new Dictionary<string, byte[]> { [baseKey] = baseData, [conflictKey] = corrupted });

        // Assert — конфликт, покрытие = база
        result.Conflict.Should().BeTrue();
        result.Coverage.Should().Be(SegStart + 104);
    }

    [Fact]
    public void Greedy_Hole_StopsAtCoverage()
    {
        // Arrange — база [0..104), кусок за дырой [200..304)
        var prefix = BuildPrefix(64, 64, 64);
        var (baseKey, baseData) = Piece(prefix, 0, 104);
        var (afterHole, afterData) = Piece(prefix, 168, 232);

        // Act
        var result = FetchWalCommand.GreedyAssemble(
            [Candidate(baseKey, 0, 104, baseData), Candidate(afterHole, 168, 232, afterData)],
            new Dictionary<string, byte[]> { [baseKey] = baseData, [afterHole] = afterData });

        // Assert — покрытие до дыры, граница дыры зафиксирована
        result.Coverage.Should().Be(SegStart + 104);
        result.HoleStart.Should().Be(SegStart + 168);
    }

    // AAA (spec §4.3 п.7): режим выживания применяет куски по возрастанию md5;
    // испорченный кусок (CRC записей бит) отбрасывается, побеждает валидный.
    [Fact]
    public void Survival_Md5Asc_CorruptedPieceDropped()
    {
        // Arrange — два куска [0..168) с разными байтами: «порченный» (md5 '0'…)
        // и валидный (md5 'f'…); пересечение [56..168) различается
        var prefix = BuildPrefix(64, 64);
        var corrupted = (byte[])prefix.Clone();
        corrupted[130] ^= 0xFF; // порча тела второй записи: CRC перестанет сходиться
        var baseData = prefix[..104];
        var badTail = corrupted[104..168];
        var goodTail = prefix[104..168];
        // Ключи порядка (md5-строки возрастанием): база → порченный → валидный.
        var baseKey = WalDeltaName.Key(Segment, SegStart, SegStart + 104, 1, true,
            "0".PadLeft(32, '0'));
        var corruptedKey = WalDeltaName.Key(Segment, SegStart + 104, SegStart + 168, 1, false,
            "1".PadLeft(32, '1'));
        var goodTailKey = WalDeltaName.Key(Segment, SegStart + 104, SegStart + 168, 1, false,
            "f".PadLeft(32, 'f'));

        // Act — выживание по md5 asc: «порченный» применяется вторым
        var (result, buffer) = FetchWalCommand.SurvivalAssembleCore(
            [new FetchWalCommand.AssemblyCandidate(baseKey, SegStart, SegStart + 104, true, "0"),
             new FetchWalCommand.AssemblyCandidate(corruptedKey, SegStart + 104, SegStart + 168, false, "1"),
             new FetchWalCommand.AssemblyCandidate(goodTailKey, SegStart + 104, SegStart + 168, false, "f")],
            new Dictionary<string, byte[]> { [baseKey] = baseData, [corruptedKey] = badTail,
                [goodTailKey] = goodTail },
            new Dictionary<string, string?>());

        // Assert — порченный отброшен сканером, покрытие собрано валидным куском
        result.Coverage.Should().Be(SegStart + 168);
        buffer.Should().Equal(prefix);
    }

    private static FetchWalCommand.AssemblyCandidate Candidate(
        string name, ulong startOffset, ulong endOffset, byte[] data)
        => new(name, SegStart + startOffset, SegStart + endOffset, true,
            Md5Hex(data));

    // ---- env ----

    // AAA: PGW_WAL_CHAIN_LIFE — число; мусор/отсутствие — null (DR-режим).
    [Theory]
    [InlineData("5", 5u)]
    [InlineData("0", 0u)]
    [InlineData("abc", null)]
    [InlineData("", null)]
    public void ParseEnv_ChainLife(string raw, uint? expected)
    {
        // Arrange
        var env = new Dictionary<string, string?>
        {
            ["PGW_S3_ENDPOINTS"] = "http://s3a:9000",
            ["PGW_S3_BUCKET"] = "backups",
            ["PGW_S3_PREFIX"] = "c1/shard1",
            ["PGW_S3_ACCESS_KEY"] = "ak",
            ["PGW_S3_SECRET_KEY"] = "sk",
            ["PGW_WAL_CHAIN_LIFE"] = raw,
        };

        // Act
        var context = FetchWalCommand.ParseEnv(key => env.GetValueOrDefault(key));

        // Assert
        context.Should().NotBeNull();
        context!.ChainLife.Should().Be(expected);
    }

    // ---- FetchOnceAsync: выдача покрытия ----

    // Мини-фейк IBackupS3: объекты по имени в wal/, размеры из листинга, GET байтов.
    private sealed class FakeS3 : IBackupS3
    {
        public readonly Dictionary<string, byte[]> Objects = new(StringComparer.Ordinal);
        public bool FailsList { get; set; }

        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        {
            if (FailsList)
                return Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Failed(
                    new ApplicationException("S3 list: test transport failure")));
            // prefix — полный ключ-префикс "<C>/<X>/wal/<seg>."; объекты — по имени:
            // полный ключ = корень каталога ("<C>/<X>/wal/") + имя объекта.
            var root = prefix[..(prefix.LastIndexOf('/') + 1)];
            var namePrefix = prefix[root.Length..];
            var infos = Objects
                .Where(kv => kv.Key.StartsWith(namePrefix, StringComparison.Ordinal))
                .Select(kv => new S3ObjectInfo(root + kv.Key, kv.Value.Length, DateTimeOffset.UnixEpoch))
                .ToList();
            return Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success(infos));
        }

        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
        {
            var name = key[(key.LastIndexOf('/') + 1)..];
            return Task.FromResult(Objects.TryGetValue(name, out var data)
                ? Result<(byte[] Data, string? ETag)>.Success((data, Md5Hex(data)))
                : Result<(byte[] Data, string? ETag)>.Failed(
                    new ApplicationException($"S3 get {cluster}/{shard}/{key}: not found")));
        }

        public Task<Result> GetObjectToFileAsync(
            string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
        {
            var name = key[(key.LastIndexOf('/') + 1)..];
            return Task.FromResult(Objects.TryGetValue(name, out var data)
                ? WriteFile(destFilePath, data)
                : Result.Failed(new ApplicationException(
                    $"S3 get-to-file {cluster}/{shard}/{key}: not found")));
        }

        private static Result WriteFile(string path, byte[] data)
        {
            File.WriteAllBytes(path, data);
            return Result.Success();
        }

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => throw new NotSupportedException();
    }

    // AAA: база+дельта склеиваются, файл записан добитым до wal_segment_size.
    [Fact]
    public async Task FetchOnce_BasePlusDelta_GluedAndPadded()
    {
        // Arrange — ни финала, ни версий: только база [0..104) и дельта [104..168)
        var prefix = BuildPrefix(64, 64);
        var (baseKey, baseData) = Piece(prefix, 0, 104);
        var (deltaKey, deltaData) = Piece(prefix, 104, 168);
        var s3 = new FakeS3();
        s3.Objects[baseKey] = baseData;
        s3.Objects[deltaKey] = deltaData;
        var target = Path.Combine(Path.GetTempPath(), $"fetchwal-{Guid.NewGuid():N}.seg");

        try
        {
            // Act
            var (exitCode, transportError) = await FetchWalCommand.FetchOnceAsync(
                [s3], "c1", "shard1", SegmentName, target, chainLife: 1, CancellationToken.None);

            // Assert — файл полного размера, первые байты = собранный префикс
            exitCode.Should().Be(0);
            transportError.Should().BeNull();
            var written = await File.ReadAllBytesAsync(target, TestContext.Current.CancellationToken);
            written.Length.Should().Be((int)SegmentBytes);
            written[..168].Should().Equal(prefix);
            written.Skip(168).Should().OnlyContain(b => b == 0);
        }
        finally
        {
            File.Delete(target);
        }
    }

    // AAA (решение №39): безномерная t14-версия — кандидат-база переходного
    // окна (кусков текущего номера нет): её префикс выдаётся postgres'у.
    [Fact]
    public async Task FetchOnce_LegacyVersion_UsedAsBase()
    {
        // Arrange — валидный префикс как t14-версия; финала нет
        var prefix = BuildPrefix(64);
        var legacy = LegacyName(0x68, prefix);
        var s3 = new FakeS3();
        s3.Objects[legacy] = prefix;
        var target = Path.Combine(Path.GetTempPath(), $"fetchwal-{Guid.NewGuid():N}.seg");

        try
        {
            // Act
            var (exitCode, transportError) = await FetchWalCommand.FetchOnceAsync(
                [s3], "c1", "shard1", SegmentName, target, chainLife: 1, CancellationToken.None);

            // Assert — версия собрана как база и выдана добитой нулями
            exitCode.Should().Be(0);
            transportError.Should().BeNull();
            var written = await File.ReadAllBytesAsync(target, TestContext.Current.CancellationToken);
            written.Length.Should().Be((int)SegmentBytes);
            written[..104].Should().Equal(prefix);
        }
        finally
        {
            File.Delete(target);
        }
    }

    // AAA: финала и кусков нет — конец WAL (exit 1, транспортной ошибки нет).
    [Fact]
    public async Task FetchOnce_NothingAnywhere_EndOfWal()
    {
        // Arrange — пустой префикс
        var s3 = new FakeS3();
        var target = Path.Combine(Path.GetTempPath(), $"fetchwal-{Guid.NewGuid():N}.seg");

        // Act
        var (exitCode, transportError) = await FetchWalCommand.FetchOnceAsync(
            [s3], "c1", "shard1", SegmentName, target, chainLife: 1, CancellationToken.None);

        // Assert
        exitCode.Should().Be(1);
        transportError.Should().BeNull();
        File.Exists(target).Should().BeFalse();
    }
}
