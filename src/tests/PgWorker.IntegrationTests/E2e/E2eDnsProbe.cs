using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>Синтетический DNS-зонд (t24, spec §5.2): контейнер-резолвер в
/// сети окружения; env-контракт — dns_probe.py. Лог — CSV в docker logs;
/// имя pgw-dns-{runId} содержит runId — телеметрия CollectDiagnosticsAsync и
/// teardown подбирают его по OwnName без специальных крючков. Wait-стратегия
/// не нужна: зонд ничего не отдаёт наружу, старт python мгновенен.</summary>
internal static class E2eDnsProbe
{
    internal const string Image = "python:3.12-alpine";

    // Путь от корня репозитория (скрипт общий для E2E и генератора нагрузки).
    internal const string ScriptRelativePath = "src/tests/PgWorker.IntegrationTests/E2e/dns_probe.py";

    internal static IContainer Build(
        string runId, INetwork net, IReadOnlyList<string> targets, string mode, string intervalSec)
    {
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var script = File.ReadAllBytes(Path.Combine(root, ScriptRelativePath));
        return new ContainerBuilder(Image)
            .WithName($"pgw-dns-{runId}")
            .WithNetwork(net)
            .WithNetworkAliases("e2e-dns-probe")
            .WithEnvironment("DNS_PROBE_TARGETS", string.Join(",", targets))
            .WithEnvironment("DNS_PROBE_MODE", mode)
            .WithEnvironment("DNS_PROBE_INTERVAL", intervalSec)
            .WithResourceMapping(script, "/dns_probe.py")
            .WithCommand("python3", "-u", "/dns_probe.py")
            .Build();
    }
}
