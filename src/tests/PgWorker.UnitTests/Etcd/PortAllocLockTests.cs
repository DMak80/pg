using FluentAssertions;
using PgWorker.Core;
using PgWorker.Etcd.Coordination;
using PgWorker.UnitTests.Provisioning;
using Xunit;

namespace PgWorker.UnitTests.Etcd;

// PortAllocLock (t90, arch/14 §2.4/§3.3): глобальный portalloc-клэйм —
// взаимоисключение секции довыделения портов между кластерами/инстансами.
public class PortAllocLockTests
{
    private const string Ep = "http://etcd:2379";

    // AAA: первый захват проходит и пишет ключ с instance держателя;
    // второй (другой инстанс) получает false — не ошибка, не перезаписывает.
    [Fact]
    public async Task TryAcquire_SecondInstance_GetsFalse()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var first = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-2");

        // Act
        var firstAcquired = await first.TryAcquireAsync(CancellationToken.None);
        var secondAcquired = await second.TryAcquireAsync(CancellationToken.None);

        // Assert
        firstAcquired.IsSuccess.Should().BeTrue();
        firstAcquired.Value.Should().BeTrue();
        secondAcquired.IsSuccess.Should().BeTrue();
        secondAcquired.Value.Should().BeFalse();
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-1");
    }

    // AAA: release (del + revoke) освобождает — повторный захват другим инстансом проходит;
    // повторный ReleaseAsync — no-op.
    [Fact]
    public async Task Release_AllowsTakeover_AndIsIdempotent()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var first = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-2");
        (await first.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        await first.ReleaseAsync();
        await first.ReleaseAsync(); // повтор — no-op
        var reclaimed = await second.TryAcquireAsync(CancellationToken.None);

        // Assert
        reclaimed.Value.Should().BeTrue();
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-2");
    }

    // AAA (ревью-блокер t90): повторный TryAcquire тем же объектом при живом
    // захвате — false, НЕ true: клэйм-объект DI-синглтон, параллельные тики
    // разных кластеров одного инстанса обязаны взаимоисключаться (reentrant-true
    // пускал обе секции concurrently — гонка t90 воспроизводилась в дефолтной
    // конфигурации). «Занят» — не ошибка: waiting-portalloc-lock, следующий тик.
    [Fact]
    public async Task TryAcquire_AlreadyHeldBySameObject_ReturnsFalse()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var locks = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        (await locks.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();

        // Act
        var again = await locks.TryAcquireAsync(CancellationToken.None);

        // Assert
        again.IsSuccess.Should().BeTrue();
        again.Value.Should().BeFalse(); // держит параллельный тик этого же инстанса
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-1"); // ключ держателя не тронут
    }

    // AAA (регрессия ревью-блокера t90): два TryAcquireAsync на ОДНОМ объекте
    // (один инстанс, параллельные тики двух кластеров) — первый true, второй
    // false; после ReleaseAsync первого — захват снова проходит (следующий тик).
    [Fact]
    public async Task SameObject_SecondTickBlockedUntilRelease()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var portLock = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");

        // Act
        var first = await portLock.TryAcquireAsync(CancellationToken.None);
        var second = await portLock.TryAcquireAsync(CancellationToken.None);
        await portLock.ReleaseAsync();
        var reclaimed = await portLock.TryAcquireAsync(CancellationToken.None);
        await portLock.ReleaseAsync();

        // Assert
        first.Value.Should().BeTrue();
        second.Value.Should().BeFalse();
        reclaimed.Value.Should().BeTrue();
    }

    // AAA: лок перехвачен (lease истёк, ключ перезаписан чужим value) —
    // release НЕ удаляет чужой ключ (del под compare ValueEqual).
    [Fact]
    public async Task Release_AfterTakeover_DoesNotDeleteForeignKey()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd();
        var mine = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");
        (await mine.TryAcquireAsync(CancellationToken.None)).Value.Should().BeTrue();
        // имитация истечения TTL и перехвата: ключ перезаписан чужим value
        etcd.Seed(PortAllocLock.Key, """{"instance":"inst-2","since_unix":1}""");

        // Act
        await mine.ReleaseAsync();

        // Assert: чужой ключ жив — del под ValueEqual(наш value) не сошёлся
        etcd.Store[PortAllocLock.Key].Value.Should().Contain("inst-2");
    }

    // AAA: сбой etcd на txn → Result.Failed (процесс пойдёт в обычный бэкофф, не InProgress-тихо).
    [Fact]
    public async Task TryAcquire_EtcdTxnFailure_ReturnsFailed()
    {
        // Arrange
        var etcd = new Fakes.FakeEtcd
        {
            TxnFault = _ => Result<PgWorker.Etcd.Client.TxnResult>.Failed(
                new ApplicationException("etcd: connection refused")),
        };
        var locks = new PortAllocLock([Ep], etcd, TimeProvider.System, "inst-1");

        // Act
        var acquired = await locks.TryAcquireAsync(CancellationToken.None);

        // Assert
        acquired.IsSuccess.Should().BeFalse();
    }
}
