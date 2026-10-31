using Npgsql;

namespace PgWorker.PgNode;

/// <summary>Пробы готовности фазы (б) initializing (spec §3.6-3б): SELECT 1
/// со стороны ноды. PG — 127.0.0.1:5432 под postgres/SuPassword (доставлен);
/// doorman — клиентский вход :6432 (sslmode=require) под app-ролью с
/// app_password per-cluster etcd (проба заодно валидирует цепочку auth_query).
/// Бюджет — Connection Timeout=5 (кратный интервалу опроса, spec §8.17);
/// ожидание готовности — БЕЗ бюджета (живой процесс может инициализироваться
/// долго).</summary>
public static class ComponentProbes
{
    public const int TimeoutSec = 5;

    public static string PgConnectionString(string suPassword)
        => $"Host=127.0.0.1;Port=5432;Username=postgres;Password={suPassword};" +
           $"Database=postgres;Timeout={TimeoutSec}";

    public static string DoormanConnectionString(string appUser, string appPassword, string dbName)
        => $"Host=127.0.0.1;Port=6432;Username={appUser};Password={appPassword};" +
           $"Database={dbName};SslMode=Require;Timeout={TimeoutSec}";

    public static async Task<bool> ProbeAsync(string connectionString, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand("SELECT 1", connection);
            var result = await command.ExecuteScalarAsync(ct);
            return result is 1;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return false; // ещё инициализируется/недоступен — следующая проба
        }
    }
}
