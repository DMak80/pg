using FluentAssertions;
using PgWorker.Docker.Drivers;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Канон-суперсет движка против живого docker (t07 §7.2, шаг 4.8a плана):
// create по отсутствующему образу → движок выполняет pull-фолбэк (POST
// /images/create) и повтор create. Негативная форма: pull несуществующего
// образа из локального registry завершается ошибкой — ассерт различает СТАДИЮ
// по пути в DockerHttpException (голый 404 create дал бы /containers/create).
public class EngineSupersetTests
{
    private const string AlpineImage = "alpine:3.20";

    // Локальный registry — канон внешних образов (runbook); уникальный
    // гарантированно отсутствующий репозиторий задачи.
    private const string MissingImage = "192.168.0.1:5000/pgw-t07-no-such-image:missing";

    private static readonly DockerEngineFactory Factory = new();

    private static readonly string Suffix = Guid.NewGuid().ToString("N")[..6];

    [Fact]
    public async Task CreateContainer_MissingImage_PullsAndRetries()
    {
        DockerTrait.SkipIfUnavailable();
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
            created.IsSuccess.Should().BeFalse("образ не существует — create не может пройти");
            created.Error!.Message.Should().Contain("/images/create",
                "движок обязан был выполнить pull-фолбэк (канон-суперсет §7.2)");
            created.Error.Message.Should().NotContain("/containers/create",
                "голый 404 create означал бы отсутствие фолбэка");
        }
        finally
        {
            // Полный teardown при любом исходе (404 = успех).
            await engine.RemoveContainerAsync(name, force: true, ct);
        }
    }

    // Позитивный контроль путей: после pull реального образа create проходит
    // (гарантирует, что предыдущий кейс красен именно из-за отсутствия образа).
    [Fact]
    public async Task CreateContainer_ExistingImage_Succeeds()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var engine = Factory.Create("unix:///var/run/docker.sock", hostAlias: "local");
        var name = "pgw-t07-superset-ok-" + Suffix;
        var spec = new ContainerSpec(AlpineImage, [], name, Cmd: ["true"], ResetEntrypoint: true);
        try
        {
            await using var puller = Factory.Create("unix:///var/run/docker.sock", hostAlias: "local");
            await ((DockerEngine)puller).PullImageAsync(AlpineImage, ct);

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
