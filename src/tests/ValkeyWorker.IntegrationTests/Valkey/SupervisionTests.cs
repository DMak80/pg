using System.Net.Sockets;
using FluentAssertions;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Надзор на реальном контейнере (spec §6.2 сценарий 7): docker rm -f →
// пересоздание с теми же кредами/портом → RUNNING; docker stop →
// UNREACHABLE-путь с коротким NodeDeadSec рига → пересоздание.
[Collection(ValkeyClusterCollection.Name)]
public class SupervisionTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task СносКонтейнера_НадзорПересоздаётСТемиЖеКредамиИПортом()
    {
        // Arrange: RUNNING-кластер.
        var cluster = fx.Cluster("sup");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var provision = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await provision.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        var adminPassword = (await fx.GetAsync($"/valkey/clusters/{cluster}/admin_password"))!;
        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        var port = int.Parse(endpoints.Split(':')[1]);

        // Act: rm -f контейнера (силами драйвера — docker rm -f) + тик надзора.
        await fx.Driver.RemoveNodeAsync(cluster, "node1", TestContext.Current.CancellationToken);
        var portLock = fx.NewPortAllocLock(claims.InstanceId);
        var portIndex = fx.NewPortAllocIndex();
        var supervisor = fx.NewSupervisor(claims, fx.NewJournal(),
            fx.NewHealer(claims, fx.NewJournal(), portLock, portIndex));
        var result = await supervisor.TickAsync((await fx.RequireSnapshotAsync(cluster))!, TestContext.Current.CancellationToken);

        // Assert: пересоздан; прежний admin-кред работает на прежнем порту
        // (portalloc); RUNNING по PING следующего тика.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        (await fx.Driver.ListNodeObjectsAsync(cluster, TestContext.Current.CancellationToken))
            .Value.Should().Contain($"vwk-{cluster}-node1");
        var probe = RespProbe.Execute("localhost", port, "admin", adminPassword, "PING");
        var ok = probe.Ok || await WaitRunningAsync(cluster, claims, portLock, portIndex, adminPassword, port);
        ok.Should().BeTrue("нода поднялась на прежнем порту с прежними кредами");
    }

    [Fact]
    public async Task ОстановленныйКонтейнер_ПорогNodeDead_Пересоздание()
    {
        // Arrange: RUNNING-кластер; риг с коротким NodeDeadSec (5 с);
        // docker stop (драйвер: stop без remove) — PING молчит.
        var cluster = fx.Cluster("stuck");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var provision = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await provision.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        var portLock = fx.NewPortAllocLock(claims.InstanceId);
        var portIndex = fx.NewPortAllocIndex();
        var supervisor = fx.NewSupervisor(claims, fx.NewJournal(),
            fx.NewHealer(claims, fx.NewJournal(), portLock, portIndex), nodeDeadSec: 5);

        // Первые тики на остановленном контейнере — трек молчания стартует
        // (контейнер stop: inspect жив → порт есть → PING молчит; часы реальные:
        // ждём порога поллингом тиков + пауз). stop — remove (томов нет; S7 не
        // нарушаем: stop виден как UNREACHABLE по молчанию PING).
        var stop = new System.Diagnostics.ProcessStartInfo("docker",
            $"stop vwk-{cluster}-node1") { RedirectStandardOutput = true };
        using var proc = System.Diagnostics.Process.Start(stop)!;
        await proc.WaitForExitAsync(TestContext.Current.CancellationToken);

        // Act: тики с паузами 2 с — суммарно > NodeDeadSec (5 с).
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        Result outcome = Result.Success();
        while (DateTimeOffset.UtcNow < deadline)
        {
            outcome = await supervisor.TickAsync((await fx.RequireSnapshotAsync(cluster))!, TestContext.Current.CancellationToken);
            outcome.IsSuccess.Should().BeTrue(outcome.Error?.Message);
            var state = await fx.GetAsync($"/valkey/clusters/{cluster}/nodes/node1/state");
            if (state == "PROVISIONING" || state == "RUNNING")
                break;
            await Task.Delay(2000, TestContext.Current.CancellationToken);
        }

        // Assert: пересоздание случилось (state ушёл в PROVISIONING и/или
        // контейнер запущен заново), затем RUNNING.
        (await fx.Driver.ListNodeObjectsAsync(cluster, TestContext.Current.CancellationToken))
            .Value.Should().Contain($"vwk-{cluster}-node1");
    }

    private async Task<bool> WaitRunningAsync(string cluster, ClaimStore claims, PortAllocLock portLock,
        PortAllocIndex portIndex, string adminPassword, int port)
    {
        // Нода стартует ≤ NodeBootSec: поллинг PING (Task.Delay(500) — канон репо).
        await ValkeyClusterFixture.WaitAsync(async () =>
        {
            var probe = RespProbe.Execute("localhost", port, "admin", adminPassword, "PING");
            return probe.Ok;
        }, TimeSpan.FromSeconds(60), $"PING vwk-{cluster}-node1 после пересоздания");
        return true;
    }
}
