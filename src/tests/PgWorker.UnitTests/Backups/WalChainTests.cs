using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Gap-детектор непрерывности WAL-цепочки (arch/19 §3): последовательность Next()
// без пропусков; TLI-переход валиден при history + первом сегменте нового TLI
// ∈ {последний старого, Next(последний)}; .partial игнорируется.
public class WalChainTests
{
    private static WalFileName Seg(uint tli, uint log, uint seg) => new(tli, log, seg);

    [Fact]
    public void SolidChain_IsContinuous()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002", "000000010000000000000003",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.GapError.Should().BeNull();
        result.LastSegment!.Value.Name.Should().Be("000000010000000000000003");
    }

    [Fact]
    public void HoleInsideChain_GivesGapWithBounds()
    {
        // Arrange — нет 000000010000000000000002
        var objects = new[] { "000000010000000000000001", "000000010000000000000003" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000002")
            .And.Contain("000000010000000000000003");
    }

    [Fact]
    public void FirstObjectAboveStart_GivesHoleFromStart()
    {
        // Arrange — цепочка начинается с chainStart, а первый объект позже
        var objects = new[] { "000000010000000000000005" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000001");
    }

    [Fact]
    public void SegmentsBeforeStart_AreIgnored()
    {
        // Arrange — хвост до chain_start (ретенция/сдвиг старта полным бэкапом)
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000005", "000000010000000000000006",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 5), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void PartialSegments_AreIgnored()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002.partial",
            "000000010000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void TliSwitch_WithHistory_AndLastSegmentRepeat_IsValid()
    {
        // Arrange — failover в середине сегмента: PG перезаписывает сегмент с нового TLI
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002",
            "00000002.history",
            "000000020000000000000002", // == последнему старого TLI
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Tli.Should().Be(2);
    }

    [Fact]
    public void TliSwitch_WithHistory_AndNextSegment_IsValid()
    {
        // Arrange — переключение на границе сегмента: первый новый = Next(последнего)
        var objects = new[]
        {
            "000000010000000000000001",
            "00000002.history",
            "000000020000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void TliSwitch_WithoutHistory_IsGap()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "000000020000000000000002",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("00000002.history");
    }

    [Fact]
    public void TliSwitch_JumpingOverSwitchPoint_IsGap()
    {
        // Arrange — первый сегмент нового TLI ≠ последнему/next (skip позиции)
        var objects = new[]
        {
            "000000010000000000000001", "00000002.history", "000000020000000000000005",
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
    }

    // ── CheckRange (arch/19 §3): непрерывность [start..end] ──

    // AAA: сплошной диапазон [1..3] с end=3 — непрерывен, LastSegment == end
    [Fact]
    public void CheckRange_SolidRange_IsContinuous()
    {
        // Arrange
        var objects = new[] { "000000010000000000000001", "000000010000000000000002", "000000010000000000000003" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 3), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Name.Should().Be("000000010000000000000003");
    }

    // AAA: сегменты ВЫШЕ end (после точки бэкапа) не мешают — диапазон закрыт
    [Fact]
    public void CheckRange_SegmentsAboveEnd_AreIgnored()
    {
        // Arrange — end=2, но wal/ содержит ещё 3 и 4 (поток продолжился)
        var objects = new[]
        {
            "000000010000000000000001", "000000010000000000000002",
            "000000010000000000000003", "000000010000000000000004",
        };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 2), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Name.Should().Be("000000010000000000000002");
    }

    // AAA: дыра внутри [start..end] — разрыв с границами (AC3)
    [Fact]
    public void CheckRange_HoleBeforeEnd_GapWithBounds()
    {
        // Arrange — нет 2, диапазон [1..3]
        var objects = new[] { "000000010000000000000001", "000000010000000000000003" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 3), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000002")
            .And.Contain("000000010000000000000003");
    }

    // AAA: сегмент end есть в наборе (pg_wal), но объекта wal/<end> нет — дыра
    // дублирования (AC3)
    [Fact]
    public void CheckRange_NoEndObjectInWal_Hole()
    {
        // Arrange — wal/ обрывается на 2, end (из листинга набора) = 3
        var objects = new[] { "000000010000000000000001", "000000010000000000000002" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 3), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000003");
    }

    // AAA: пустой wal/ при заданном end — дыра от старта
    [Fact]
    public void CheckRange_EmptyWal_HoleFromStart()
    {
        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(1, 0, 2), []);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000010000000000000001");
    }

    // ── CheckRange: TLI-переходы ──

    // AAA: fallback — без historyContents работает эвристика (history-объект
    // в листинге + первый сегмент нового TLI ∈ {последний старого, Next(last)})
    [Fact]
    public void CheckRange_SwitchWithoutContent_Heuristic()
    {
        // Arrange — TLI 1: seg 1; TLI 2: seg 2 (== Next(1)); history-объект есть
        var objects = new[]
        {
            "000000010000000000000001", "000000020000000000000002", "00000002.history",
        };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    // AAA: строгий переход — switchWALLSN внутри границ (сегмент позиции == первому
    // сегменту нового TLI) → непрерывен (AC4)
    [Fact]
    public void CheckRange_StrictSwitch_PointWithinBounds_Ok()
    {
        // Arrange — первый сегмент TLI 2 = 2 (позиция 0/2000000); история: parent=1, lsn в сегменте 2
        var objects = new[] { "000000010000000000000001", "000000020000000000000002", "00000002.history" };
        var history = new Dictionary<uint, string> { [2] = "1\t0/2000000\n" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects, history);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    // AAA: строгий переход — точка вне границ (switchLSN указывает на сегмент 5,
    // а первый сегмент нового TLI — 2) → FAILED с границами (AC4)
    [Fact]
    public void CheckRange_StrictSwitch_PointOutsideBounds_Hole()
    {
        // Arrange
        var objects = new[] { "000000010000000000000001", "000000020000000000000002", "00000002.history" };
        var history = new Dictionary<uint, string> { [2] = "1\t0/5000000\n" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects, history);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("00000002.history").And.Contain("0/5000000");
    }

    // AAA: строгий переход — parent не совпадает (history говорит parent=5) → дыра
    [Fact]
    public void CheckRange_StrictSwitch_ForeignParent_Hole()
    {
        // Arrange
        var objects = new[] { "000000010000000000000001", "000000020000000000000002", "00000002.history" };
        var history = new Dictionary<uint, string> { [2] = "5\t0/2000000\n" };

        // Act
        var result = WalChain.CheckRange(Seg(1, 0, 1), Seg(2, 0, 2), objects, history);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("parent");
    }

    [Fact]
    public void History_WithoutSwitch_HasNoEffect()
    {
        // Arrange — лишний history (файл от старого failover) — не ошибка
        var objects = new[] { "00000002.history", "000000010000000000000001", "000000010000000000000002" };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
    }

    [Fact]
    public void HoleAfterSwitchToNewTli_IsCaught()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000001", "00000002.history", "000000020000000000000002",
            "000000020000000000000004", // пропущен 3-й
        };

        // Act
        var result = WalChain.Check(Seg(1, 0, 1), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().Contain("000000020000000000000003");
    }

    // ── CheckWithRestart (AC4): promote восстановленного шарда легитимно
    // обрезает старый TLI в архиве — дыра на TLI-границе не деградация ──

    // AAA: restore-профиль — старый поток tl1 оборван на 0A, новый tl3 начат
    // с 0D (промежуточных 0B/0C не существует): перезапуск от первого сегмента
    // нового TLI делает цепь целой
    [Fact]
    public void CheckWithRestart_OldTliTruncation_ChainContinuous()
    {
        // Arrange — chain_start от старого полного (tl1/09)
        var objects = new[]
        {
            "000000010000000000000009", "00000001000000000000000a",
            "00000003.history", "00000003000000000000000d",
        };

        // Act
        var result = WalChain.CheckWithRestart(Seg(1, 0, 9), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Name.Should().Be("00000003000000000000000d");
    }

    // AAA: настоящая дыра внутри одного TLI перезапуском не маскируется —
    // другой истории нет
    [Fact]
    public void CheckWithRestart_HoleInsideTli_Degradation()
    {
        // Arrange — сегменты tl1: 09, 0B (0A пропущен)
        var objects = new[] { "000000010000000000000009", "00000001000000000000000b" };

        // Act
        var result = WalChain.CheckWithRestart(Seg(1, 0, 9), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
        result.GapError.Should().NotBeNullOrEmpty();
    }

    // AAA: непрерывная цепь без TLI-переходов ведёт себя как обычный Check
    [Fact]
    public void CheckWithRestart_ContinuousChain_NoChanges()
    {
        // Arrange
        var objects = new[]
        {
            "000000010000000000000009", "00000001000000000000000a",
            "00000001000000000000000b",
        };

        // Act
        var result = WalChain.CheckWithRestart(Seg(1, 0, 9), objects);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Name.Should().Be("00000001000000000000000b");
    }

    // AAA: дыра в самом новом TLI — деградация (обрезка оправдывает только
    // границу таймлайнов, не пропуск внутри нового потока)
    [Fact]
    public void CheckWithRestart_HoleInNewTli_Degradation()
    {
        // Arrange — tl3: 0D, 0F (0E пропущен уже в новом потоке)
        var objects = new[]
        {
            "000000010000000000000009", "00000003.history",
            "00000003000000000000000d", "00000003000000000000000f",
        };

        // Act
        var result = WalChain.CheckWithRestart(Seg(1, 0, 9), objects);

        // Assert
        result.IsContinuous.Should().BeFalse();
    }

    // ---: RatchetedStart — одиночный кандидат (новейший verify-OK полный) ----

    // AAA : кандидат ≥ записанной границы — контроль от кандидата
    // (новый OK-полный закрывает разрыв/поднимает точку).
    [Fact]
    public void RatchetedStart_CandidateAboveBounds_Accepted()
    {
        // Arrange — записанная граница ..05; кандидат (новейший OK-полный) ..09
        var recorded = WalFileName.TryParse("000000010000000000000005");
        var candidate = WalFileName.TryParse("000000010000000000000009");

        // Act
        var start = WalChain.RatchetedStart(recorded, candidate);

        // Assert — контроль от ..09
        start!.Value.Name.Should().Be("000000010000000000000009");
    }

    // AAA : ratchet — кандидат НИЖЕ границы не понижает точку: verify-
    // перепроверка старшего OK→FAILED откатила кандидата — контроль остаётся
    // на прежней границе (полные ниже границы разрыва игнорируются).
    [Fact]
    public void RatchetedStart_CandidateBelowBounds_DoesNotLower()
    {
        // Arrange — записанная граница ..05; кандидат ..03
        var recorded = WalFileName.TryParse("000000010000000000000005");
        var candidate = WalFileName.TryParse("000000010000000000000003");

        // Act
        var start = WalChain.RatchetedStart(recorded, candidate);

        // Assert — точка не понижена
        start.Should().Be(recorded);
    }

    // AAA : записи нет — контроль от кандидата (первый OK-полный задаёт
    // стартовую точку; полных нет — остаётся min-объект потока у вызова).
    [Fact]
    public void RatchetedStart_NoRecord_Candidate()
    {
        // Arrange — записи нет; кандидат ..03
        var candidate = WalFileName.TryParse("000000010000000000000003");

        // Act
        var start = WalChain.RatchetedStart(null, candidate);

        // Assert — старт от кандидата
        start!.Value.Name.Should().Be("000000010000000000000003");
    }

    // AAA : кандидата нет (verify-OK-полных нет) — записанная точка
    // держится; нет и записи — null (контролю остаётся min-объект).
    [Fact]
    public void RatchetedStart_NoCandidate_Recorded()
    {
        // Arrange — записанная ..05; кандидата нет
        var recorded = WalFileName.TryParse("000000010000000000000005");

        // Act
        var onlyRecorded = WalChain.RatchetedStart(recorded, null);
        var neither = WalChain.RatchetedStart(null, null);

        // Assert — записанная точка / null
        onlyRecorded.Should().Be(recorded);
        neither.Should().BeNull();
    }

    // ---: TailOf — хвост потока финал/лучший .partial (arch/19 §3) ----

    // AAA (хвост потока): объектов нет — хвоста нет.
    [Fact]
    public void TailOf_NoObjects_ReturnsNull()
    {
        // Act
        var tail = WalChain.TailOf(null, [], chainLife: 0);

        // Assert
        tail.Should().BeNull();
    }

    // AAA: только финал цепочки — хвост = он сам.
    [Fact]
    public void TailOf_FinalOnly()
    {
        // Arrange
        var final = Seg(1, 0, 5);

        // Act
        var tail = WalChain.TailOf(final, [final.Name], chainLife: 0);

        // Assert
        tail.Should().NotBeNull();
        tail!.SegmentName.Should().Be(final.Name);
        tail.ObjectName.Should().Be(final.Name);
    }

    // AAA: .partial сегмента ВЫШЕ финала — хвост новее; из версий — длиннейшая.
    [Fact]
    public void TailOf_PartialAboveFinal_Wins()
    {
        // Arrange — финал ..05; две версии ..06
        var final = Seg(1, 0, 5);
        var next = Seg(1, 0, 6);
        var shortVersion = WalPartialName.Key(next.Name, 0x100);
        var longVersion = WalPartialName.Key(next.Name, 0x200);

        // Act
        var tail = WalChain.TailOf(final, [final.Name, shortVersion, longVersion], chainLife: 0);

        // Assert
        tail!.SegmentName.Should().Be(next.Name);
        tail.ObjectName.Should().Be(longVersion);
    }

    // AAA: равная позиция — финал авторитетен (Review Focus 2, часть 1).
    [Fact]
    public void TailOf_EqualPosition_FinalWins()
    {
        // Arrange
        var final = Seg(1, 0, 6);
        var partial = WalPartialName.Key(final.Name, 0x400);

        // Act
        var tail = WalChain.TailOf(final, [final.Name, partial], chainLife: 0);

        // Assert
        tail!.ObjectName.Should().Be(final.Name);
    }

    // AAA: .partial ниже финальной позиции не «удлиняет» хвост назад (Review Focus 2).
    [Fact]
    public void TailOf_PartialBelowFinalPosition_Ignored()
    {
        // Arrange
        var final = Seg(1, 0, 7);
        var below = Seg(1, 0, 3);

        // Act
        var tail = WalChain.TailOf(final, [final.Name, WalPartialName.Key(below.Name, 0x100)], chainLife: 0);

        // Assert
        tail!.SegmentName.Should().Be(final.Name);
    }

    // AAA: .partial нового таймлайна — хвост выше (TLI первичен в сравнении).
    [Fact]
    public void TailOf_HigherTliPartial_Wins()
    {
        // Arrange
        var final = Seg(1, 0, 7);
        var newTli = Seg(2, 0, 1);

        // Act
        var tail = WalChain.TailOf(final, [final.Name, WalPartialName.Key(newTli.Name, 0x100)], chainLife: 0);

        // Assert
        tail!.SegmentName.Should().Be(newTli.Name);
    }

    // ---: TailOf — покрытие дельта-кусков ТЕКУЩЕГО номера жизни (t15) ----

    private static string Md5(char c) => new(c, 32);

    // Абсолютный LSN начала сегмента (координаты кусков — абсолютные LSN).
    private static ulong Start(WalFileName segment) => WalDeltaName.SegmentStartOf(segment);

    // AAA: база [0..X) + примыкающая дельта [X..Y) — хвост по сегменту покрытия.
    [Fact]
    public void TailOf_DeltasOfCurrentLife_ExtendCoverage()
    {
        // Arrange
        var segA = Seg(1, 0, 8);
        var baseKey = WalDeltaName.Key(segA, Start(segA), Start(segA) + 0x100, 1, true, Md5('a'));
        var deltaKey = WalDeltaName.Key(segA, Start(segA) + 0x100, Start(segA) + 0x200, 1, false, Md5('b'));

        // Act
        var tail = WalChain.TailOf(null, [baseKey, deltaKey], chainLife: 1);

        // Assert — сегмент покрыт кусками жизни 1 (имя объекта не детерминировано)
        tail.Should().NotBeNull();
        tail!.SegmentName.Should().Be(segA.Name);
    }

    // AAA: куски ЧУЖОГО номера жизни не рассматриваются вовсе.
    [Fact]
    public void TailOf_ForeignLifePieces_Ignored()
    {
        // Arrange — куски жизни 2
        var segA = Seg(1, 0, 8);
        string[] pieces =
        [
            WalDeltaName.Key(segA, Start(segA), Start(segA) + 0x100, 2, true, Md5('a')),
            WalDeltaName.Key(segA, Start(segA) + 0x100, Start(segA) + 0x200, 2, false, Md5('b')),
        ];

        // Act
        var foreign = WalChain.TailOf(null, pieces, chainLife: 1);
        var own = WalChain.TailOf(null, pieces, chainLife: 2);

        // Assert
        foreign.Should().BeNull();
        own!.SegmentName.Should().Be(segA.Name);
    }

    // AAA (решение №39): безномерная t14-версия учитывается, пока у сегмента нет
    // кусков текущего номера; появился кусок — версия сегмента игнорируется.
    [Fact]
    public void TailOf_LegacyVersion_UsedUntilCurrentLifePieceAppears()
    {
        // Arrange — segB: версия + кусок жизни 1; segC: только версия
        var segB = Seg(1, 0, 9);
        var segC = Seg(1, 0, 10);
        var legacyB = WalPartialName.Key(segB.Name, 0x180);
        var pieceB = WalDeltaName.Key(segB, Start(segB), Start(segB) + 0x100, 1, true, Md5('c'));
        var legacyC = WalPartialName.Key(segC.Name, 0x50);

        // Act
        var withPiece = WalChain.TailOf(null, [legacyB, pieceB], chainLife: 1);
        var onlyLegacy = WalChain.TailOf(null, [legacyC], chainLife: 1);

        // Assert — по куску (не версии) / по лучшей версии
        withPiece!.ObjectName.Should().Be(pieceB);
        onlyLegacy!.ObjectName.Should().Be(legacyC);
    }

    // AAA: дыра в покрытии кусков — покрытие останавливается на её границе.
    [Fact]
    public void TailOf_GapInCoverage_StopsAtCoverage()
    {
        // Arrange — [0..A) и [B..C), A < B
        var segA = Seg(1, 0, 8);
        var baseKey = WalDeltaName.Key(segA, Start(segA), Start(segA) + 0x100, 1, true, Md5('a'));
        var afterGap = WalDeltaName.Key(segA, Start(segA) + 0x200, Start(segA) + 0x300, 1, false, Md5('b'));

        // Act
        var tail = WalChain.TailOf(null, [baseKey, afterGap], chainLife: 1);

        // Assert — покрытие = база (A), кусок за дырой не примыкает
        tail!.SegmentName.Should().Be(segA.Name);
        tail.ObjectName.Should().Be(baseKey);
    }

    // AAA: при равной позиции финал авторитетен даже при более длинных кусках.
    [Fact]
    public void TailOf_FinalAuthoritative()
    {
        // Arrange — финал segA + куски segA жизни 1 длиннее
        var final = Seg(1, 0, 11);
        string[] objects =
        [
            final.Name,
            WalDeltaName.Key(final, Start(final), Start(final) + 0x1000, 1, true, Md5('a')),
            WalDeltaName.Key(final, Start(final) + 0x1000, Start(final) + 0x2000, 1, false, Md5('b')),
        ];

        // Act
        var tail = WalChain.TailOf(final, objects, chainLife: 1);

        // Assert
        tail!.SegmentName.Should().Be(final.Name);
        tail.ObjectName.Should().Be(final.Name);
    }

    // AAA (Review Focus 4): end-сегмент есть только в объединённом списке
    // (wal/ + набор) — диапазон закрывается.
    [Fact]
    public void CheckRange_EndOnlyInSet_Closes()
    {
        // Arrange — финалы ..02/..03 в wal/ + ..04 из набора full/<id>/pg_wal/
        var start = Seg(1, 0, 2);
        var end = Seg(1, 0, 4);
        string[] union = [Seg(1, 0, 2).Name, Seg(1, 0, 3).Name, end.Name];

        // Act
        var result = WalChain.CheckRange(start, end, union);

        // Assert
        result.IsContinuous.Should().BeTrue();
        result.LastSegment!.Value.Name.Should().Be(end.Name);
    }
}
