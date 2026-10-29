namespace PgWorker.IntegrationTests.E2e;

/// <summary>Запущенный SUT-воркер — КОНТЕЙНЕР в сети контура (Вариант A,
/// docs/e2e-isolation.md §3). Имя класса сохранено (HostInstance) — сценарии
/// не переписываются на новый тип. Dispose идемпотентен: сценарий dispose-ит
/// воркер сам, teardown окружения может попытаться ещё раз (страховка).</summary>
public sealed class HostInstance : IAsyncDisposable
{
    private readonly E2eEnvironment _env;
    private bool _disposed;

    public HostInstance(E2eEnvironment env, string name, string alias, string containerName)
    {
        _env = env;
        Name = name;
        Alias = alias;
        ContainerName = containerName;
    }

    public string Name { get; }

    /// <summary>DNS-алиас воркера в сети контура (mTLS SAN, адрес API).</summary>
    public string Alias { get; }

    public string ContainerName { get; }

    /// <summary>mTLS-адрес HTTP API воркера (внутрисетевой, раннер в той же сети).</summary>
    public string ApiUrl => $"https://{Alias}:8443";

    /// <summary>Каталог etcd-снапшотов воркера (Snapshots__Dir в контейнере
    /// /snapshots — bind на каталог артефактов хоста; в раннере виден как
    /// /snaps/&lt;name&gt;): сценарии читают snapshot-*.db напрямую.</summary>
    public string SnapshotsDir => $"/snaps/{Name}";

    /// <summary>Жив ли воркер (контейнер running) — семантика «процесс жив»
    /// старого хост-инстанса (смерть контроллера, cert-сценарии).</summary>
    public async Task<bool> IsRunningAsync(CancellationToken ct = default)
    {
        var ps = await E2eFixture.RunDockerAsync(
        [
            "ps", "--filter", $"name=^{ContainerName}$", "--format", "{{.State}}",
        ], ct);
        return ps.Contains("running", StringComparison.Ordinal);
    }

    /// <summary>Хвост логов воркера (docker logs) — семантика host.log старого
    /// хост-инстанса (диагностика сценариев «что произошло» на живом воркере).</summary>
    public async Task<string> ReadLogTailAsync(int lines = 100, CancellationToken ct = default)
        => await E2eFixture.RunDockerLogsAsync(["--tail", lines.ToString(), ContainerName], ct);

    /// <summary>Состояние контейнера воркера (docker inspect): Status и ExitCode
    /// (graceful self-stop сценария — exit 0; null — контейнер жив).</summary>
    public async Task<(string Status, int? ExitCode)> InspectStateAsync(CancellationToken ct = default)
    {
        var raw = await E2eFixture.RunDockerAsync(
        [
            "inspect", "--format", "{{.State.Status}}:{{.State.ExitCode}}", ContainerName,
        ], ct);
        var parts = raw.Split(':', 2);
        return parts.Length == 2 && int.TryParse(parts[1], out var code)
            ? (parts[0], code)
            : (raw, null);
    }

    /// <summary>Мгновенный kill (смерть контроллера, AC3) — lease истекают ≤15 с.</summary>
    public void Kill()
    {
        try
        {
            E2eFixture.RunDockerAsync(["kill", ContainerName]).GetAwaiter().GetResult();
        }
        catch (ApplicationException)
        {
            // уже завершён/удалён
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        // Журнал воркера — в телеметрию ДО удаления: тест держит воркер в
        // await using и удаляет раньше CollectDiagnostics teardown'а факта —
        // после rm логи контейнера недоступны (прогон 2026-10-16:
        // container-pgw-ew-*.log отсутствовал в артефактах при живых прочих).
        try
        {
            var logs = await E2eFixture.RunDockerLogsAsync(
                ["--timestamps", ContainerName], CancellationToken.None);
            await System.IO.File.WriteAllTextAsync(
                System.IO.Path.Combine(_env.ArtifactsDir, $"container-{ContainerName}.log"),
                logs);
        }
        catch
        {
            // телеметрия лучшими усилиями — не мешает teardown
        }
        try
        {
            await E2eFixture.RunDockerAsync(["rm", "-f", ContainerName]);
        }
        catch (ApplicationException e) when (e.Message.Contains("No such container", StringComparison.OrdinalIgnoreCase))
        {
            // уже удалён (teardown окружения)
        }
        catch (ApplicationException e) when (e.Message.Contains("already in progress", StringComparison.OrdinalIgnoreCase))
        {
            // удаляется демоном — исход тот же
        }
    }
}
