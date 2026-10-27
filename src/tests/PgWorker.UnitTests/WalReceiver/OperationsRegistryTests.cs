using FluentAssertions;
using PgWorker.WalReceiver.Operations;
using Xunit;

namespace PgWorker.UnitTests.WalReceiver;

// Реестр одной активной операции приёмника (spec t26 §3.2): TryBegin — 409-
// гвард второй активной (running/uploading), терминальный снимок перезаписывается;
// Update/Clear — атомарная видимость Current (параллельные публикации без гонок).
// AAA-комментарии.
public class OperationsRegistryTests
{
    private const string Id = "20260101000000Z";

    private static OperationSnapshot Snap(string state, string phase = "basebackup")
        => new(Id, state, phase);

    [Fact]
    public void TryBegin_OnEmptyRegistry_Accepts()
    {
        // Arrange: пустой реестр.
        var registry = new OperationRegistry();

        // Act: первый TryBegin.
        var accepted = registry.TryBegin(Snap("running"));

        // Assert: снимок принят и виден.
        accepted.Should().BeTrue();
        registry.Current.Should().NotBeNull();
        registry.Current!.State.Should().Be("running");
    }

    [Fact]
    public void TryBegin_WhileRunning_Rejected()
    {
        // Arrange: активная running-операция.
        var registry = new OperationRegistry();
        registry.TryBegin(Snap("running"));

        // Act: вторая попытка старта.
        var accepted = registry.TryBegin(Snap("running"));

        // Assert: 409-гвард — отклонено, исходный снимок не затёрт.
        accepted.Should().BeFalse();
        registry.Current!.State.Should().Be("running");
    }

    [Fact]
    public void TryBegin_WhileUploading_Rejected()
    {
        // Arrange: активная uploading-операция (полный).
        var registry = new OperationRegistry();
        registry.TryBegin(Snap("uploading", "uploading"));

        // Act: вторая попытка старта.
        var accepted = registry.TryBegin(Snap("running"));

        // Assert: uploading — тоже активная; отклонено.
        accepted.Should().BeFalse();
    }

    [Fact]
    public void TryBegin_AfterTerminal_Overwrites()
    {
        // Arrange: терминальный снимок в реестре.
        var registry = new OperationRegistry();
        registry.TryBegin(Snap("succeeded"));

        // Act: новый старт поверх терминального.
        var accepted = registry.TryBegin(Snap("running"));

        // Assert: принят (следующий POST — новая операция).
        accepted.Should().BeTrue();
        registry.Current!.State.Should().Be("running");
    }

    [Fact]
    public void Update_PublishesNewSnapshot_Atomic()
    {
        // Arrange: принятая операция.
        var registry = new OperationRegistry();
        registry.TryBegin(Snap("running"));

        // Act: публикация новой фазы.
        registry.Update(new OperationSnapshot(
            Id, "uploading", "uploading", WalStartSegment: "000000010000000000000002"));

        // Assert: виден свежий снимок целиком (атомарная замена ссылки).
        registry.Current!.State.Should().Be("uploading");
        registry.Current.WalStartSegment.Should().Be("000000010000000000000002");
    }

    [Fact]
    public async Task ParallelUpdates_DoNotRace()
    {
        // Arrange: реестр + параллельные публикации из многих задач.
        var registry = new OperationRegistry();
        registry.TryBegin(Snap("running"));
        var snapshots = Enumerable.Range(0, 1000)
            .Select(i => new OperationSnapshot(Id, "running", $"phase-{i}"))
            .ToArray();

        // Act: все задачи публикуют без синхронизации.
        await Task.WhenAll(snapshots.Select(s =>
            Task.Run(() => registry.Update(s))));

        // Assert: Current — один из опубликованных снимков, ссылка целая
        // (без разрыва/испорченного объекта).
        registry.Current.Should().BeOneOf(snapshots);
    }
}
