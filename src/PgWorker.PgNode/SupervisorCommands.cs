using System.Diagnostics;
using PgWorker.PgNode.Operations;

namespace PgWorker.PgNode;

/// <summary>Управление сервисами ноды через supervisorctl (arch/14 §2.1/§4;
/// прецедент RestoreOperation): start блокирует до startsecs и рапортует
/// провал старта ненулевым кодом — попытка старта = один вызов; «already
/// started» — успех (идемпотентность). restart безопасен на остановленной
/// программе.</summary>
public interface ISupervisorCommands
{
    Task StartAsync(string program, CancellationToken ct);
    Task RestartAsync(string program, CancellationToken ct);
}

public sealed class SupervisorCommands(IProcessRunner runner) : ISupervisorCommands
{
    public async Task StartAsync(string program, CancellationToken ct)
        => await RunAsync("start", program, ct);

    public async Task RestartAsync(string program, CancellationToken ct)
        => await RunAsync("restart", program, ct);

    private async Task RunAsync(string verb, string program, CancellationToken ct)
    {
        var psi = new ProcessStartInfo { FileName = "supervisorctl" };
        psi.ArgumentList.Add(verb);
        psi.ArgumentList.Add(program);
        var (exit, output, err) = await runner.RunAsync(psi, ct);
        if (exit != 0
            && !(verb == "start"
                 && (output + err).Contains("already started", StringComparison.Ordinal)))
            throw new ApplicationException(
                $"supervisorctl {verb} {program}: exit {exit}: " +
                ((output.Length > 0 ? output : err).Split('\n')[0]));
    }
}
