using FluentAssertions;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Provisioning.Processes;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Интеграция ротации CA (t07): заявка → CaRotator доводит окно двойного
// доверия на живом docker-valkey; после коммита ca_pem/ca_key = NEW,
// staging удалён, заявка снята, PING по NEW отвечает, OLD доверия нет.
// Контур — реальный etcd + реальный valkey-контейнер (см. примечание задачи).
[Collection(ValkeyClusterCollection.Name)]
public class CaRotationTests(ValkeyClusterFixture fx)
{
    private CaRotator NewCaRotator(ClaimStore claims)
        => new(fx.Gateway, [fx.Endpoint], fx.Driver, claims, fx.NewJournal(),
            fx.NewTlsProvisioner(),
            new ValkeyConnection(TimeSpan.FromSeconds(2)), fx.Options);

    // Канонический TLS-кластер (образец TlsMigrationTests.PlainCluster_MigratesToTls):
    // посев + plain-контейнер + миграция T тиками до NotNeeded. Возврат: порт,
    // admin-пароль, OLD ca_pem. Клэйм кладётся в claims вызывающего.
    private async Task<(int Port, string AdminPassword, string OldCaPem)> SeedTlsClusterAsync(
        string cluster, ClaimStore claims)
    {
        var ct = TestContext.Current.CancellationToken;
        await fx.SeedClusterAsync(cluster);
        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();

        // Креды для args старого канона; CA не пишем (эмуляция премиграционного).
        var ensured = await fx.NewSecretEnsurer().EnsureAsync(cluster, ct);
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        await fx.DelAsync($"/valkey/clusters/{cluster}/ca_pem");
        await fx.DelAsync($"/valkey/clusters/{cluster}/ca_key");

        var port = fx.NextPort();
        await fx.PutAsync($"/valkeyworker/portalloc/{cluster}",
            "{\"node1\":{\"host\":\"local\",\"client\":" + port + "}}");
        var plainArgs = NodeArgsBuilder.Build(
            536870912, "allkeys-lru", ensured.Value.AdminPassword, ensured.Value.AppPassword)
            .TakeWhile((arg, i) => arg != "--tls-port").ToArray();
        (await fx.Driver.EnsureNodeAsync(new(
            cluster, "node1", ValkeyClusterFixture.DockerHost, port, fx.Options.NodeImage, plainArgs,
            1m, 1024L * 1024 * 1024), ct)).IsSuccess.Should().BeTrue();
        await fx.PutAsync($"/valkey/clusters/{cluster}/endpoints", $"localhost:{port}");
        await fx.PutAsync($"/valkey/clusters/{cluster}/nodes/node1/state", "RUNNING");

        // Миграция T доигрывает тиками до NotNeeded (канон TLS-кластер).
        var migrator = new TlsMigrator(
            fx.Gateway, [fx.Endpoint], fx.Driver, claims, fx.NewJournal(),
            fx.NewSecretEnsurer(), fx.NewTlsProvisioner(),
            new ValkeyConnection(TimeSpan.FromSeconds(2)), fx.Options);
        while (true)
        {
            var tick = await migrator.RunAsync(await fx.RequireSnapshotAsync(cluster), ct);
            tick.IsSuccess.Should().BeTrue(tick.Error?.Message);
            if (tick.Value == TlsMigrator.MigrationOutcome.NotNeeded)
                break;
            await Task.Delay(1000, ct);
        }

        var oldCaPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;
        oldCaPem.Should().Contain("BEGIN CERTIFICATE");
        return (port, ensured.Value.AdminPassword, oldCaPem);
    }

    // Заявка ротации (формат §9.8, без role) + тик-цикл CaRotator до del заявки.
    private async Task<string> RotateToCompletionAsync(string cluster, ClaimStore claims)
    {
        await fx.PutAsync($"/valkeyworker/ca_rotations/{cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"it"}""");
        var rotator = NewCaRotator(claims);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200); // ≤ BrokerBootSec-класс бюджета
        while (DateTimeOffset.UtcNow < deadline)
        {
            var tick = await rotator.RunAsync(await fx.RequireSnapshotAsync(cluster),
                TestContext.Current.CancellationToken);
            tick.IsSuccess.Should().BeTrue($"тик ротации не должен падать: {tick.Error?.Message}");
            if (await fx.GetAsync($"/valkeyworker/ca_rotations/{cluster}") is null)
                return (await fx.GetAsync($"/valkeyworker/work/{cluster}"))!;
            await Task.Delay(1000, TestContext.Current.CancellationToken);
        }

        return $"/!\\ бюджет исчерпан: journal={await fx.GetAsync($"/valkeyworker/work/{cluster}")}";
    }

    [Fact]
    public async Task CaRotate_FullWindow_CommitsNewCa()
    {
        // Arrange: канонический TLS-кластер, клэйм наш; baseline — PING по OLD.
        var cluster = fx.Cluster("carot");
        var ct = TestContext.Current.CancellationToken;
        var claims = fx.NewClaimStore();
        var (port, adminPw, oldCaPem) = await SeedTlsClusterAsync(cluster, claims);

        RespProbe.ExecuteTls("localhost", port, "admin", adminPw, oldCaPem, "PING").Ok
            .Should().BeTrue("стартовое доверие OLD");

        // Act: заявка + тик-цикл ротатора до del заявки.
        var journal = await RotateToCompletionAsync(cluster, claims);
        journal.Should().Contain("done", $"ротация исполнена за бюджет; journal={journal}");

        // Assert 1: ca_pem = NEW (не bundle), ca_key = NEW, staging нет, заявки нет.
        var newCaPem = (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem"))!;
        newCaPem.Should().NotBe(oldCaPem).And.NotContain(oldCaPem, "bundle свёрнут");
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_key")).Should().NotBe(oldCaPem);
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_next_key")).Should().BeNull();
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_next_pem")).Should().BeNull();
        (await fx.GetAsync($"/valkeyworker/ca_rotations/{cluster}")).Should().BeNull();

        // Assert 2: PING по NEW-CA отвечает; OLD-CA отклоняется (ключ уничтожен).
        RespProbe.ExecuteTls("localhost", port, "admin", adminPw, newCaPem, "PING").Ok
            .Should().BeTrue("доверие NEW после коммита");
        var oldRejected = false;
        try
        {
            oldRejected = !RespProbe.ExecuteTls("localhost", port, "admin", adminPw, oldCaPem, "PING").Ok;
        }
        catch (Exception)
        {
            oldRejected = true; // TLS-отказ на хендшейке: серт подписан NEW
        }
        oldRejected.Should().BeTrue("серверный серт подписан NEW — OLD больше не якорь");
    }

    [Fact]
    public async Task CrashBetweenDAndR_ReplaysToCommit()
    {
        // Arrange: окно открыто руками ДО R (staging + bundle в ca_pem — «краш
        // сразу после D»); контейнер снесён — фаза R обязана «лечить мёртвую
        // ноду»: RemoveNode 404=ок, EnsureNode пересоздаёт, коммит доходит.
        var cluster = fx.Cluster("carotd");
        var ct = TestContext.Current.CancellationToken;
        var claims = fx.NewClaimStore();
        var (port, adminPw, oldCaPem) = await SeedTlsClusterAsync(cluster, claims);

        var (nextPem, nextKey) = ValkeyPki.GenerateCa(cluster + "-new");
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_next_pem", nextPem);
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_next_key", nextKey);
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_pem", oldCaPem + "\n" + nextPem);
        (await fx.Driver.RemoveNodeAsync(cluster, "node1", ct)).IsSuccess.Should().BeTrue();

        // Act: заявка + тик-цикл (R пересоздаёт ноду, C коммитит).
        var journal = await RotateToCompletionAsync(cluster, claims);
        journal.Should().Contain("done", $"доигрывание после краша D/R: {journal}");

        // Assert: коммит от ЧУЖОЙ staging; контейнер снова жив; PING по NEW.
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem")).Should().Be(nextPem);
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_next_key")).Should().BeNull();
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_next_pem")).Should().BeNull();
        (await fx.GetAsync($"/valkeyworker/ca_rotations/{cluster}")).Should().BeNull();
        RespProbe.ExecuteTls("localhost", port, "admin", adminPw, nextPem, "PING").Ok
            .Should().BeTrue("пересозданная нода доверяет NEW");
    }

    [Fact]
    public async Task CrashBetweenRAndC_FactDetectSkipsRecreate()
    {
        // Arrange: серт NEW УЖЕ в volume («краш после R до C»): факт-детект
        // обязан пропустить пересоздание (Id контейнера неизменен) и коммитить.
        var cluster = fx.Cluster("carotr");
        var ct = TestContext.Current.CancellationToken;
        var claims = fx.NewClaimStore();
        var (port, adminPw, oldCaPem) = await SeedTlsClusterAsync(cluster, claims);

        var (nextPem, nextKey) = ValkeyPki.GenerateCa(cluster + "-new");
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_next_pem", nextPem);
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_next_key", nextKey);
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_pem", oldCaPem + "\n" + nextPem);
        var ensured = await fx.NewTlsProvisioner().EnsureNodeTlsAsync(
            cluster, "node1", ValkeyClusterFixture.DockerHost, ValkeyClusterFixture.AdvertisedClientHost,
            nextPem, nextKey, ct);
        ensured.IsSuccess.Should().BeTrue(ensured.Error?.Message);
        var idBefore = ContainerId($"vwk-{cluster}-node1");
        idBefore.Should().NotBeEmpty();

        // Act: заявка + тик-цикл (факт-детект → сразу C).
        var journal = await RotateToCompletionAsync(cluster, claims);
        journal.Should().Contain("done", $"доигрывание после краша R/C: {journal}");

        // Assert: пересоздания НЕ было; коммит от NEW. Живой valkey-процесс
        // держит серт, загруженный при старте (OLD): перезапись volume сертом
        // NEW вступает при следующем пересоздании (надзор/следующий тик R) —
        // потому PING отвечает по OLD-якорю, а не NEW (окно доверия живо).
        ContainerId($"vwk-{cluster}-node1").Should().Be(idBefore, "факт-детект: контейнер не тронут");
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_pem")).Should().Be(nextPem);
        (await fx.GetAsync($"/valkey/clusters/{cluster}/ca_next_key")).Should().BeNull();
        RespProbe.ExecuteTls("localhost", port, "admin", adminPw, oldCaPem, "PING").Ok
            .Should().BeTrue("живой процесс продолжает OLD-серт до пересоздания");
    }

    [Fact]
    public async Task Deprovision_WithLiveTicket_CleansAll()
    {
        // Arrange: канонический TLS-кластер + живая заявка ca_rotations + staging.
        var cluster = fx.Cluster("carotx");
        var ct = TestContext.Current.CancellationToken;
        var claims = fx.NewClaimStore();
        await SeedTlsClusterAsync(cluster, claims);
        await fx.PutAsync($"/valkeyworker/ca_rotations/{cluster}",
            """{"requested_unix":1756500000,"requested_by":"it"}""");
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_next_key", "stg");
        await fx.PutAsync($"/valkey/clusters/{cluster}/ca_next_pem", "stg");

        // Act: TO_REMOVE + тик демонтажа X0–X3.
        await fx.PutAsync($"/valkey/clusters/{cluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000,"state":"TO_REMOVE"}""");
        var deprovision = fx.NewDeprovisioning(claims, fx.NewJournal());
        var result = await deprovision.TickAsync(await fx.RequireSnapshotAsync(cluster), ct);
        result.IsSuccess.Should().BeTrue(result.Error?.Message);

        // Assert: ни заявки, ни staging, ни клэйма — X2 снёс координацию.
        (await fx.GetAsync($"/valkeyworker/ca_rotations/{cluster}")).Should().BeNull();
        var prefix = await fx.Gateway.RangeAsync(fx.Endpoint, $"/valkey/clusters/{cluster}/", ct);
        prefix.Value.Should().BeEmpty("префиксный del домена забирает staging ca_next_*");
        (await fx.GetAsync($"/valkeyworker/claims/{cluster}")).Should().BeNull();
    }

    // docker inspect -f {{.Id}} (пусто — объекта нет).
    private static string ContainerId(string name)
    {
        var psi = new System.Diagnostics.ProcessStartInfo(
            "docker", $"inspect --format {{{{.Id}}}} {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var proc = System.Diagnostics.Process.Start(psi)!;
        var id = proc.StandardOutput.ReadToEnd().Trim();
        proc.WaitForExit(5000);
        return proc.ExitCode == 0 ? id : string.Empty;
    }
}
