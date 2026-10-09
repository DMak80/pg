using PgWorker.Core.Templates;
using PgWorker.IntegrationTests.E2e;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Rolling-ротация rest_password на живом контейнере (t22, arch/14 §5 I):
// заявка → первый тик ротатора фиксирует NEW в rest_pending → тики надзора
// катят пересоздания (hash env != hash(pending)) → txn коммитит четвёрку и
// сбрасывает pending; окно без PATCH конвергенции DCS; идемпотентность
// повторной заявки; no-op тика без заявки. SQL — стаб рига. Гейт
// PGW_TEST_DOCKER=1; окружение per-guid с ассертом чистоты (e2e-isolation).
[Collection(NonE2eCollection.Name)]
public class RestRotationTests(Etcd.EtcdFixture fixture)
{
    [Fact]
    public async Task Rotation_RollingWindow_CommitsAndIdempotent()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var c = await RestTlsDockerRig.BuildAsync(fixture);

        // Arrange: конвергенция контура до TLS — обе ноды несут hash(OLD)
        // (shard1b поднимается EnsureDeclared-шагом первого тика).
        var oldPassword = await ConvergeAsync(c);
        oldPassword.Should().NotBeNullOrEmpty("ensure шага уже положил ключ");
        var oldId = await ContainerIdAsync(c.ContainerA);

        // (а) заявка → первый тик ротатора: rest_pending зафиксирован,
        // NEW-пара сгенерирована (не OLD), заявка ещё жива.
        await SubmitTicketAsync(c, ct);
        var first = await c.Rotator.TickAsync(await RestTlsDockerRig.SnapshotAsync(c), ct);
        first.IsSuccess.Should().BeTrue(first.Error?.ToString());
        var pending = (await c.Journal.ReadAsync(c.Cluster, ct)).Value!.RestPending;
        pending.Should().NotBeNullOrEmpty("rotate-rest-start фиксирует пару в полёте");
        pending.Should().NotBe(oldPassword, "NEW-пара отлична от текущего ключа");
        (await TicketAsync(c, ct)).Should().NotBeNull("коммита ещё не было — заявка жива");

        // Act (б)+(г): цикл тиков до txn-коммита; каждый промежуточный тик
        // ПРОДОЛЖАЕТ проход той же парой (сбой посреди окна не регенерирует).
        var committed = await RunUntilCommitAsync(c, pending, ct);

        // Assert (б): txn-коммит — ключ==NEW, заявка снята, pending сброшен;
        // контейнер пересоздан с hash(NEW), volume/маркер на месте.
        committed.Should().BeTrue("rolling обязан дойти до txn за бюджет тиков");
        var key = (await c.Gateway.GetAsync(c.Endpoint, $"/clusters/{c.Cluster}/rest_password", ct)).Value;
        key!.Value.Should().Be(pending, "txn закоммитил pending-пару");
        (await TicketAsync(c, ct)).Should().BeNull("заявка удалена txn'ом");
        (await c.Journal.ReadAsync(c.Cluster, ct)).Value!.RestPending
            .Should().BeNull("фаза done сбросила окно");
        (await ContainerIdAsync(c.ContainerA)).Should().NotBe(oldId, "нода пересоздана rolling'ом");
        var env = await c.Driver.InspectNodeEnvAsync(c.Cluster, "shard1", "shard1a", ct);
        env.Value[RestRotation.EnvPasswordHash]
            .Should().Be(RestRotation.PasswordHash(pending), "env несёт hash(NEW)");
        var marker = await E2eFixture.RunDockerAsync(
            ["exec", c.ContainerA, "test", "-f", "/home/postgres/pgdata/MARKER"], ct);
        marker.Should().BeEmpty("volume пережил rolling (exec код 0)");

        // (в) идемпотентность: повторная заявка — НОВЫЙ проход с новой парой.
        await SubmitTicketAsync(c, ct);
        (await c.Rotator.TickAsync(await RestTlsDockerRig.SnapshotAsync(c), ct))
            .IsSuccess.Should().BeTrue();
        var pending2 = (await c.Journal.ReadAsync(c.Cluster, ct)).Value!.RestPending;
        pending2.Should().NotBeNullOrEmpty();
        pending2.Should().NotBe(pending, "повторный проход — свежая пара");
        (await RunUntilCommitAsync(c, pending2!, ct)).Should().BeTrue("второй проход коммитится");
        var key2 = (await c.Gateway.GetAsync(c.Endpoint, $"/clusters/{c.Cluster}/rest_password", ct)).Value;
        key2!.Value.Should().Be(pending2, "ключ — второй NEW");

        // (д) no-op тика без заявки: ключ/контейнер не меняются, окна нет.
        var idAfter = await ContainerIdAsync(c.ContainerA);
        var noop = await c.Rotator.TickAsync(await RestTlsDockerRig.SnapshotAsync(c), ct);
        noop.IsSuccess.Should().BeTrue(noop.Error?.ToString());
        (await c.Gateway.GetAsync(c.Endpoint, $"/clusters/{c.Cluster}/rest_password", ct)).Value!.Value
            .Should().Be(pending2, "без заявки ключ не трогается");
        (await c.Journal.ReadAsync(c.Cluster, ct)).Value!.RestPending
            .Should().BeNull("окна нет");
        (await ContainerIdAsync(c.ContainerA)).Should().Be(idAfter, "пересозданий нет");
    }

    // Цикл тиков ротатор↔надзор до коммита; пока окно открыто — pending
    // неизменен (гвард (г)) и фаз патча конвергенции DCS нет (гвард (е)).
    private static async Task<bool> RunUntilCommitAsync(
        RestTlsDockerRig.Contour c, string expectedPending, CancellationToken ct)
    {
        for (var i = 0; i < 10; i++)
        {
            var rot = await c.Rotator.TickAsync(await RestTlsDockerRig.SnapshotAsync(c), ct);
            rot.IsSuccess.Should().BeTrue(rot.Error?.ToString());
            if (await TicketAsync(c, ct) is null)
                return true; // txn закрыл проход — pending уже сброшен фазой done

            var work = (await c.Journal.ReadAsync(c.Cluster, ct)).Value;
            work!.RestPending.Should().Be(expectedPending, "тик продолжает проход той же парой");

            var sup = await c.Supervisor.TickAsync(await RestTlsDockerRig.SnapshotAsync(c), null, ct);
            sup.IsSuccess.Should().BeTrue(sup.Error?.ToString());

            // Гвард окна (е): PATCH /config конвергенции DCS подавлен — фазы
            // патча в журнале нет (окно — только фаза skip либо фазы rolling).
            var after = (await c.Journal.ReadAsync(c.Cluster, ct)).Value;
            if (after!.RestPending is not null)
                after.Phase.Should().NotBe("dcs-converge", "окно подавляет PATCH конвергенции");
        }

        return false;
    }

    private static async Task<string?> ConvergeAsync(RestTlsDockerRig.Contour c)
    {
        var ct = TestContext.Current.CancellationToken;
        string? old = null;
        for (var i = 0; i < 6; i++)
        {
            var snap = await RestTlsDockerRig.SnapshotAsync(c);
            var tick = await c.Supervisor.TickAsync(snap, null, ct);
            tick.IsSuccess.Should().BeTrue(tick.Error?.ToString());
            old = (await c.Gateway.GetAsync(c.Endpoint, $"/clusters/{c.Cluster}/rest_password", ct))
                .Value?.Value;

            var envA = await c.Driver.InspectNodeEnvAsync(c.Cluster, "shard1", "shard1a", ct);
            var envB = await c.Driver.InspectNodeEnvAsync(c.Cluster, "shard1", "shard1b", ct);
            if (envA.Value.ContainsKey(RestRotation.EnvCert)
                && envB.Value.ContainsKey(RestRotation.EnvCert))
                return old!;
        }

        return old; // не сошлось за бюджет — ассерты падают по значению
    }

    private static async Task SubmitTicketAsync(RestTlsDockerRig.Contour c, CancellationToken ct)
        => await c.Gateway.PutAsync(c.Endpoint, $"/pgworker/rotations/{c.Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}}}""",
            null, ct);

    private static async Task<Shared.Etcd.Client.Kv?> TicketAsync(
        RestTlsDockerRig.Contour c, CancellationToken ct)
        => (await c.Gateway.GetAsync(c.Endpoint, $"/pgworker/rotations/{c.Cluster}", ct)).Value;

    private static async Task<string> ContainerIdAsync(string name)
        => (await E2eFixture.RunDockerAsync(["inspect", "-f", "{{.Id}}", name],
            TestContext.Current.CancellationToken)).Trim();
}
