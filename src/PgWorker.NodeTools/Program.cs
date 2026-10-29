using PgWorker.NodeTools.Commands;

// Точка входа CLI. Контракт — позиционная команда + именованные параметры;
// никакой неявной конфигурации из env у команд нет (env-файл — только
// явно переданный параметр master-lease, необходимый после env-фильтра runit).
if (args.Length == 0)
{
    Console.Error.WriteLine("usage: pgwnodetool <master-lease|rolecheck|hasync|haproxy> [parameters]");
    return 2;
}

try
{
    return await (args[0] switch
    {
        "master-lease" => MasterLeaseCommand.RunAsync(args[1..]),
        "rolecheck" => RoleCheckCommand.RunAsync(args[1..]),
        "hasync" => HasyncCommand.RunAsync(args[1..]),
        "haproxy" => HaproxyCommand.RunAsync(args[1..]),
        _ => Task.FromResult(Usage(args[0])),
    });
}
catch (OperationCanceledException)
{
    return 0;
}
catch (Exception e)
{
    Console.Error.WriteLine($"pgwnodetool: {e.GetType().Name}: {e.Message}");
    return 1;
}

static int Usage(string command)
{
    Console.Error.WriteLine($"unknown command: {command}");
    return 2;
}
