using FluentAssertions;
using PgWorker.Docker.Drivers;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Канон-суперсет движка против живого docker (t07 §7.2, шаг 4.8a плана):
// create по отсутствующему образу → движок выполняет pull-фолбэк (POST
// /images/create) и повтор create. Негативная форма: pull несуществующего
// образа из локального registry завершается ошибкой — ассерт различает СТАДИЮ
// по пути в DockerHttpException (голый 404 create дал бы /containers/create).
[Collection(NonE2eCollection.Name)]
public class EngineSupersetTests
{
    // Внешний образ — только из локального registry (канон образов, runbook):
    // голый alpine:3.20 резолвится в docker.io и роняет тест таймаутом при
    // недоступном хабе.
    private const string AlpineImage = "192.168.0.1:5000/alpine:3.20";

    // Локальный registry — канон внешних образов (runbook); уникальный
    // гарантированно отсутствующий репозиторий задачи.
    private const string MissingImage = "192.168.0.1:5000/pgw-t07-no-such-image:missing";

    private static readonly DockerEngineFactory Factory = new();

    private static readonly string Suffix = Guid.NewGuid().ToString("N")[..6];

    [Fact]
    public async Task CreateContainer_MissingImage_PullsAndRetries()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var engine = Factory.Create("unix:///var/run/docker.sock", hostAlias: "local");
        var name = "pgw-t07-superset-" + Suffix;
        var spec = new ContainerSpec(MissingImage, [], name);
        try
        {
            // Act
            var created = await engine.CreateContainerAsync(spec, name, ct);

            // Assert: итог Failed (pull несуществующего образа неуспешен), но
            // упал именно pull-фолбэк (/images/create), а не голый 404 create.
            created.IsSuccess.Should().BeFalse("the image does not exist — create cannot pass");
            created.Error!.Message.Should().Contain("/images/create",
                "the engine must have performed the pull fallback (§7.2 canon-superset)");
            created.Error.Message.Should().NotContain("/containers/create",
                "a bare 404 create would mean the fallback is missing");
        }
        finally
        {
            // Полный teardown при любом исходе (404 = успех).
            await engine.RemoveContainerAsync(name, force: true, ct);
        }
    }

    // Позитивный контроль путей: create по существующему образу проходит
    // (гарантирует, что предыдущий кейс красен именно из-за отсутствия образа).
    // Отдельный pull НЕ зовём: локальный registry за basic-auth, движок тянет
    // анонимно (без X-Registry-Auth) — «pull access denied»; по канону ранбука
    // образы предзеркалируются на хост ДО тестов (dev-stand/images/pull-images.sh),
    // поэтому образ уже локален.
    [Fact]
    public async Task CreateContainer_ExistingImage_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var engine = Factory.Create("unix:///var/run/docker.sock", hostAlias: "local");
        var name = "pgw-t07-superset-ok-" + Suffix;
        var spec = new ContainerSpec(AlpineImage, [], name, Cmd: ["true"], ResetEntrypoint: true);
        try
        {
            // Act / Assert
            var created = await engine.CreateContainerAsync(spec, name, ct);
            created.IsSuccess.Should().BeTrue(created.Error?.ToString());
        }
        finally
        {
            await engine.RemoveContainerAsync(name, force: true, ct);
        }
    }
}
