using System.Diagnostics;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Shared.Etcd.Client;
using PgWorker.IntegrationTests.E2e;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

/// <summary>
/// СОБСТВЕННОЕ etcd-окружение одного Fact (e2e-isolation §1/§3, docs/e2e-isolation.md):
/// guid-имя pgw-ee-{guid}, динамический хост-порт, свой ключевой контур — тест
/// НЕ делит etcd ни с соседними классами, ни с чужими прогонами (инцидент t02:
/// джобы/ключи соседних сценариев в общем etcd). Ключи умирают вместе с
/// контейнером — отдельная чистка не нужна. Teardown в DisposeAsync при ЛЮБОМ
/// исходе (фикстура в await using тела Fact) + АССЕРТ ЧИСТОТЫ: своего
/// контейнера не осталось (опознание — по guid прогона, чужие pgw-* не трогаем).
/// </summary>
public sealed class OwnEtcd : IAsyncDisposable
{
    private const string Image = "quay.io/coreos/etcd:v3.5.21";

    private readonly IContainer _container;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    private OwnEtcd(string slug, string runId, IContainer container)
    {
        Slug = slug;
        RunId = runId;
        _container = container;
        Gateway = new EtcdGateway(_http);
    }

    public string Slug { get; }

    /// <summary>Идентификатор прогона (полный guid) — единственное «своё» имя:
    /// контейнер pgw-ee-{guid}; никаких константных имён окружений.</summary>
    public string RunId { get; }

    /// <summary>Имя контейнера (pgw-ee-{guid}): публичное для docker cp/exec
    /// etcdctl в restore-verify сценариях (t08).</summary>
    public string ContainerName => $"pgw-ee-{RunId}";

    public EtcdGateway Gateway { get; }

    /// <summary>etcd endpoint: published порт на хосте (динамический, зонд docker).</summary>
    public string Endpoint { get; private set; } = "";

    /// <summary>Подъём своего etcd: pgw-ee-{guid}, порт assignRandomHostPort
    /// (AGENTS.md: никаких литералов), advertise 127.0.0.1 — клиент только хост-процесс.</summary>
    public static async Task<OwnEtcd> StartAsync(string slug, CancellationToken ct = default)
    {
        var runId = Guid.NewGuid().ToString("N");
        var container = new ContainerBuilder(Image)
            .WithName($"pgw-ee-{runId}")
            .WithCommand(
                "etcd", "--name=e2e", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        await container.StartAsync(ct);
        var fx = new OwnEtcd(slug, runId, container)
        {
            Endpoint = $"http://localhost:{container.GetMappedPublicPort(2379)}",
        };
        await fx.WaitReadyAsync(ct);
        return fx;
    }

    // Готовность POST-ретраем с конечным бюджетом (30 c): встроенные HTTP-wait
    // testcontainers шлют GET, а /v3/* принимает только POST (паттерн EtcdFixture).
    // Транзиентные транспортные сбои (t29 §4.7): клиентский таймаут пробы (3 c)
    // при холодном/медленном старте контейнера — «ещё не готов» (как
    // StartHostOnPortAsync-readiness); отмена теста продолжает всплывать.
    private async Task WaitReadyAsync(CancellationToken ct)
    {
        using var probe = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        var attempts = 0;
        var lastLatencyMs = 0.0;
        for (var i = 0; i < 30; i++)
        {
            attempts++;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                using var response = await probe.PostAsync(
                    Endpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"),
                    ct);
                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается — повтор пробы (не сон: блокирующий ретрай)
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Клиентский таймаут пробы (3 c) при холодном/медленном старте
                // контейнера — «ещё не готов»; бюджет 30×1 c неизменен (t29 §4.7).
            }

            lastLatencyMs = sw.Elapsed.TotalMilliseconds;
            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException(
            $"etcd {ContainerName} не поднялся за 30 c (попыток {attempts}, латентность последней пробы {lastLatencyMs:0} мс)");
    }

    /// <summary>Teardown при любом исходе: стоп/rm СВОЕГО контейнера (ключи умирают
    /// вместе с ним) → АССЕРТ ЧИСТОТЫ: pgw-ee-{guid} в docker ps -a отсутствует;
    /// «своё» опознаём по guid, чужие pgw-* не трогаем и в ассерт не включаем.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _container.DisposeAsync();
        }
        finally
        {
            _http.Dispose();
        }

        var left = await E2eFixture.RunProcessAsync(
            "docker",
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={ContainerName}"],
            CancellationToken.None);
        left.Should().BeEmpty(
            $"{Slug}: teardown окружения неполный — остался контейнер {ContainerName}");
    }
}
