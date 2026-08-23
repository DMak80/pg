using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Exec-механика драйвера на живом docker (t01 задача 18, ревью №8; spec §10):
// ExecNodeAsync резолвит контейнер ноды pgw-<C>-<X>-<n>, возвращает stdout,
// ненулевой exit — Result.Failed с кодом, отсутствие контейнера — отказ.
// Гейт: PGW_TEST_DOCKER=1 (DockerTrait), трейт DockerAvailable — для фильтрации.
[Trait("DockerAvailable", "true")]
public class ExecDriverTests
{
    private const string AlpineImage = "alpine:3.20";

    // Имя из плана: ExecNodeAsync("execit","shard1","n1", …) резолвит его же.
    private const string ContainerName = "pgw-execit-shard1-n1";

    private static readonly DockerEngineFactory Factory = new();

    private static IDockerEngine NewEngine() => Factory.Create("unix:///var/run/docker.sock", hostAlias: "local");

    private static PlainClusterDriver NewDriver()
        => new([new HostEndpoint("local", "unix:///var/run/docker.sock")], Factory, enableDoorman: false, AlpineImage);

    // Pull образа один раз на прогон (только при включённой серии).
    private static readonly Lazy<Task> ImageReady = new(async () =>
    {
        await using var engine = (DockerEngine)NewEngine();
        await engine.PullImageAsync(AlpineImage, CancellationToken.None); // ошибка — исключение в Lazy
    }, LazyThreadSafetyMode.ExecutionAndPublication);

    private static async Task CleanupAsync()
    {
        try
        {
            await using var engine = NewEngine();
            await engine.RemoveContainerAsync(ContainerName, force: true, CancellationToken.None);
        }
        catch
        {
            // best-effort уборка перед тестом
        }
    }

    // Живой alpine sleep-контейнер с именем ноды (без volume/портов — не нужен).
    private static async Task CreateRunningAsync()
    {
        await using var engine = NewEngine();
        var spec = new ContainerSpec(
            AlpineImage, new Dictionary<string, string>(), "", "", [],
            "n1", null, null, null, ["sleep", "30"]);
        (await engine.CreateContainerAsync(spec, ContainerName, CancellationToken.None))
            .IsSuccess.Should().BeTrue("контейнер exec-теста должен создаться");
        (await engine.StartContainerAsync(ContainerName, CancellationToken.None))
            .IsSuccess.Should().BeTrue("контейнер exec-теста должен стартовать");
    }

    // AAA: exec в running-контейнер ноды возвращает demultiplexed stdout
    [Fact]
    public async Task ExecNode_RunningContainer_EchoHello()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange — живой pgw-execit-shard1-n1 (sleep 30)
        await CleanupAsync();
        await CreateRunningAsync();
        var driver = NewDriver();

        // Act
        var exec = await driver.ExecNodeAsync("execit", "shard1", "n1", ["echo", "hello"], CancellationToken.None);

        // Assert — stdout команды без мультиплекс-заголовков (echo добавляет \n)
        exec.IsSuccess.Should().BeTrue(exec.Error?.ToString());
        exec.Value.Should().Be("hello\n");

        await CleanupAsync();
    }

    // AAA: ненулевой exit — Result.Failed с кодом в сообщении (pg_dump-транспорт
    // обязан отличать сбой команды от успеха)
    [Fact]
    public async Task ExecNode_NonZeroExit_FailsWithCode()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange — живой контейнер; команда с exit 3
        await CleanupAsync();
        await CreateRunningAsync();
        var driver = NewDriver();

        // Act
        var exec = await driver.ExecNodeAsync("execit", "shard1", "n1", ["sh", "-c", "exit 3"], CancellationToken.None);

        // Assert
        exec.IsSuccess.Should().BeFalse("ненулевой exit — не успех");
        exec.Error!.Message.Should().Contain("3", "код выхода виден оператору");

        await CleanupAsync();
    }

    // AAA: контейнера нет (rm -f) — Result.Failed «контейнер не найден»
    [Fact]
    public async Task ExecNode_NoContainer_Fails()
    {
        DockerTrait.SkipIfUnavailable();
        await ImageReady.Value;

        // Arrange — контейнер точно удалён
        await CleanupAsync();
        var driver = NewDriver();

        // Act
        var exec = await driver.ExecNodeAsync("execit", "shard1", "n1", ["echo", "x"], CancellationToken.None);

        // Assert
        exec.IsSuccess.Should().BeFalse("exec без контейнера невозможен");
        exec.Error!.Message.Should().Contain("не найден");
    }
}
