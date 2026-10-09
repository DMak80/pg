using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using PgWorker.App;
using PgWorker.IntegrationTests.Api;
using Xunit;

namespace PgWorker.IntegrationTests.Hosting;

// Валидация старта воркера по REST-TLS (t22, arch/14 §4 гр.3): WAF-хост с
// тестовым CA поднимается (env PGW_REST_TLS_* фикстуры), битый PEM —
// fail-fast с diagnose. Коллекция NonE2e — последовательность с Api-серией
// (общий глобальный env процесса).
[Collection(NonE2eCollection.Name)]
public class RestTlsStartupTests(PgApiFixture fixture)
{
    [Fact]
    public void WafHost_WithTestCa_StartsAndResolvesRestTlsMaterial()
    {
        // Arrange: фикстура поставила валидный тестовый CA в env процесса.
        // Act: хост построен (клиент фабрики коллекции) — материал REST-TLS
        // резолвится и несёт распарсенный CA.
        using var client = fixture.Factory.CreateClient();
        var material = fixture.Factory.Services.GetRequiredService<RestTlsMaterial>();

        // Assert: PEM совпадают с фикстурным CA, серт распарсен.
        material.CaPem.Should().Be(PgApiFixture.RestTestCa.CaPem);
        material.CaKeyPem.Should().Be(PgApiFixture.RestTestCa.CaKeyPem);
        material.Ca.Should().NotBeNull();
    }

    [Fact]
    public void WafHost_BrokenRestTlsCa_FailFastWithDiagnose()
    {
        // Arrange: битый PEM в PGW_REST_TLS_CA (валидный ключ фикстуры);
        // отдельная фабрика на общем etcd коллекции — хост строится ПОД битым env.
        Environment.SetEnvironmentVariable("PGW_REST_TLS_CA", "not a pem");
        PgWorkerApiFactory broken = null!;
        try
        {
            broken = new PgWorkerApiFactory(fixture.Etcd);

            // Act: разбор материала на старте — битый PEM → fail-fast.
            using var client = broken.CreateClient();
            var act = () => broken.Services.GetRequiredService<RestTlsMaterial>();

            // Assert: diagnose называет PGW_REST_TLS_* (HTTP-режим не существует).
            act.Should().Throw<Exception>()
                .Which.Message.Should().Contain("PGW_REST_TLS");
        }
        finally
        {
            // Teardown: восстановить валидный CA фикстуры для соседних тестов.
            Environment.SetEnvironmentVariable("PGW_REST_TLS_CA", PgApiFixture.RestTestCa.CaPem);
            if (broken is not null)
                _ = broken.DisposeAsync();
        }
    }
}
