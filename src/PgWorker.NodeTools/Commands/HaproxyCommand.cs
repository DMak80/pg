using PgWorker.NodeTools;

namespace PgWorker.NodeTools.Commands;

// CLI-доступ к HAProxy runtime API (стендовые проверки): вывод — как у сокета.
public static class HaproxyCommand
{
    public static Task<int> RunAsync(IReadOnlyList<string> args)
    {
        var cli = new CommandLine(args);
        var socket = cli.Value("socket") ?? "/run/haproxy/admin.sock";
        // Unix-сокет HAProxy — только под /run (CWE-22, реальное расположение
        // /run/haproxy/admin.sock).
        EnvironmentGuard.EnsureFileUnderRoot(socket, "/run", "socket path");
        var command = cli.Value("command")
            ?? throw new ArgumentException("--command is required");
        Console.Write(HaproxyRuntime.Call(command, socket));
        return Task.FromResult(0);
    }
}
