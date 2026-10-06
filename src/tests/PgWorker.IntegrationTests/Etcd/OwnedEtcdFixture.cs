using System.Diagnostics;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// Собственный etcd-контур сценарного класса (целевая архитектура E2E,
// docs/e2e-isolation.md §1–§3): единица изоляции — тест-класс, окружение —
// полностью своё. СВОЯ docker-сеть с guid-именем + СВОЙ etcd-контейнер
// (динамический хост-порт): никто вне класса не может писать в этот etcd,
// а чужие записи в общий etcd коллекции классу не видны — обе стороны
// независимости («своя сеть, свой etcd, на который никто другой не может
// повлиять»). Подключение класса — IClassFixture (экземпляр на класс).
// Teardown при ЛЮБОМ исходе: stop/rm контейнера, rm сети, ассерт чистоты
// окружения (опознание по guid-именам; чужие объекты не включаются).
// Ключи etcd умирают вместе с контейнером — отдельная чистка ключей не нужна
// (docs/e2e-isolation.md §3.2). Готовность — POST-ретрай /v3/maintenance/status
// (встроенные HTTP-wait шлют GET, а /v3/* требует POST), бюджет ≤ 30 c.
public sealed class OwnedEtcdFixture : IAsyncLifetime
{
    // ЕДИНЫЙ runId окружения (канон per-runId, t24: «все сети частные на один
    // тест класс»): один guid опознаёт и сеть, и контейнер — own-only чистка
    // и ассерт чистоты по этому идентификатору (docs/e2e-isolation.md §2–3).
    // Прежние имена сохранены свойствами-выражениями: конструктор (:43/:45)
    // не меняется; ассерт DisposeAsync переформулирован по _runId (ниже).
    private readonly string _runId = Guid.NewGuid().ToString("N");
    private string _netName => $"pgw-it-net-{_runId}";
    private string _containerName => $"pgw-it-etcd-{_runId}";

    private readonly INetwork _network;
    private readonly IContainer _container;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    public string Endpoint { get; private set; } = "";

    // Готов и до InitializeAsync: на «мёртвом» endpoint Gateway доступен
    // негативным кейсам (инвариант как у коллекционной фикстуры).
    public EtcdGateway Gateway { get; }

    public OwnedEtcdFixture()
    {
        _network = new NetworkBuilder().WithName(_netName).Build();
        _container = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
            .WithName(_containerName)
            .WithCommand(
                "etcd",
                "--name=test",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379")
            .WithNetwork(_network)
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        Gateway = new EtcdGateway(_http);
    }

    public async ValueTask InitializeAsync()
    {
        // Retry старта (docs/e2e-isolation.md §4): свежесозданную пустую сеть
        // может снести чужой глобальный prune до старта контейнера — окружение
        // пересоздаётся целиком; ретрай только на распознанную гонку.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await _network.CreateAsync(TestContext.Current.CancellationToken);
                await _container.StartAsync(TestContext.Current.CancellationToken);
                break;
            }
            catch (Exception e) when (attempt < 3
                && e.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
                && e.Message.Contains("not found", StringComparison.OrdinalIgnoreCase))
            {
                await TeardownAsync(TestContext.Current.CancellationToken);
            }
        }

        Endpoint = $"http://localhost:{_container.GetMappedPublicPort(2379)}";
        await WaitReadyAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await TeardownAsync(CancellationToken.None);

        // АССЕРТ ЧИСТОТЫ окружения (docs/e2e-isolation.md §3.6): не осталось
        // ни контейнера, ни сети СВОЕГО окружения — иначе тесты класса краснеют
        // сразу, а не когда кончатся подсети. Фильтр по единому runId покрывает
        // оба объекта; чужие не включаются.
        var leftContainers = await RunDockerAsync(
            $"ps -a --filter name={_runId} --format {{{{.Names}}}}");
        leftContainers.Should().BeEmpty("teardown окружения неполный: остался контейнер");
        var leftNetworks = await RunDockerAsync(
            $"network ls --filter name={_runId} --format {{{{.Names}}}}");
        leftNetworks.Should().BeEmpty("teardown окружения неполный: осталась сеть");
    }

    // Stop/rm контейнера и rm сети — идемпотентно при любом исходе
    // (guid-имена: чужие прогоны не заденем; добьёт ryuk/ассерт).
    private async Task TeardownAsync(CancellationToken ct)
    {
        try
        {
            await _container.DisposeAsync();
        }
        catch
        {
            // контейнер уже удалён/не стартовал — не ошибка teardown
        }

        try
        {
            await _network.DeleteAsync(ct);
        }
        catch
        {
            // сеть уже нет (чужой prune) — не ошибка teardown
        }

        try
        {
            await _network.DisposeAsync();
        }
        catch
        {
            // ресурс testcontainers уже освобождён
        }
    }

    // Готовность etcd: POST /v3/maintenance/status (GET встроенных wait'ов
    // etcd-гейтвею не подходит), 30 попыток × 1 c — бюджет ≤ 30 c.
    private async Task WaitReadyAsync(CancellationToken ct)
    {
        using var probeClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var probe = await probeClient.PostAsync(
                    Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"),
                    ct);
                if (probe.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается — ждём следующую попытку
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException($"etcd в {Endpoint} не поднялся за 30 c");
    }

    // docker CLI (опознание СВОИХ артефактов в ассерте чистоты).
    private static async Task<List<string>> RunDockerAsync(string arguments)
    {
        var psi = new ProcessStartInfo("docker", arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        using var process = Process.Start(psi)!;
        var stdout = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return [.. stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];
    }
}
