using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Интеграция экспирации valkey-заявок (t10, spec §6 п.11-аналог): заявка на
// невалидируемом кластере (endpoints занулены, стейта rotation нет) снимается
// первым тиком с исходом expired; повторная заявка ставится; восстановленный
// кластер + полная ротация перезаписывают исход done (AC13-тройка expired →
// done). Порог 3600 с в риге фикстуры по умолчанию — заявки с requested_unix
// = now - 3700 срабатывают первым тиком, без реального ожидания.
[Collection(ValkeyClusterCollection.Name)]
public class RotationTimeoutTests(ValkeyClusterFixture fx)
{
    [Fact]
    public async Task PasswordTicketOnUnraisedCluster_ExpiredAndRetriable()
    {
        // Arrange: поднятый кластер; endpoints затёрт (пустое значение —
        // парсер даёт null); заявка старше порога; стейта work/<C>/rotation нет
        // (тройной гвард §3.1: ротация не начата).
        var cluster = fx.Cluster("rotto");
        await fx.SeedClusterAsync(cluster);
        var claims = fx.NewClaimStore();
        await claims.TryClaimClusterAsync(cluster, TestContext.Current.CancellationToken);
        var provision = fx.NewProvisioning(claims, fx.NewJournal(),
            fx.NewPortAllocLock(claims.InstanceId), fx.NewPortAllocIndex(), fx.NewSecretEnsurer());
        (await provision.TickAsync(await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        var endpoints = (await fx.GetAsync($"/valkey/clusters/{cluster}/endpoints"))!;
        await fx.PutAsync($"/valkey/clusters/{cluster}/endpoints", "");
        var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await fx.PutAsync($"/valkeyworker/rotations/{cluster}",
            $$"""{"role":"app","requested_unix":{{nowUnix - 3700}},"requested_by":"it"}""");

        // Act: тик ротатора — waiting-cluster-ветка (state is null) снимает
        // заявку возрастным таймаутом.
        var rotator = fx.NewRotator(claims, fx.NewJournal());
        var result = await rotator.TickAsync(
            await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken);

        // Assert: Success (тик кластера не фейлится — AC7); заявка удалена;
        // исход expired; повторная заявка ставится (409-ситуация невозможна).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        (await fx.GetAsync($"/valkeyworker/rotations/{cluster}")).Should().BeNull(
            "не-начатая заявка старше порога снята");
        (await fx.GetAsync($"/valkeyworker/ticket_outcomes/{cluster}"))!
            .Should().Contain("\"kind\":\"password-app\"")
            .And.Contain("\"outcome\":\"expired\"")
            .And.Contain("\"reason\":\"waiting-cluster\"");
        await fx.PutAsync($"/valkeyworker/rotations/{cluster}",
            $$"""{"role":"app","requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"it"}""");
        (await fx.GetAsync($"/valkeyworker/rotations/{cluster}")).Should().NotBeNull(
            "повторная заявка ставится после снятия");

        // Восстановленный кластер: полная ротация доигрывается и перезаписывает
        // исход done (AC13: expired → done, алерт панели погашен).
        await fx.PutAsync($"/valkey/clusters/{cluster}/endpoints", endpoints);
        var done = await rotator.TickAsync(
            await fx.RequireSnapshotAsync(cluster), TestContext.Current.CancellationToken);
        done.IsSuccess.Should().BeTrue(done.Error?.Message);
        (await fx.GetAsync($"/valkeyworker/rotations/{cluster}")).Should().BeNull(
            "ротация доиграна тиком (окно двух паролей E1–E3)");
        (await fx.GetAsync($"/valkeyworker/ticket_outcomes/{cluster}"))!
            .Should().Contain("\"outcome\":\"done\"", "успешная ротация гасит expired перезаписью");
    }
}
