using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using PgWorker.Core;
using PgWorker.Core.Retry;
using PgWorker.Core.Templates;

namespace PgWorker.Provisioning.Sql;

/// <summary>
/// SQL-слой инициализации шарда (задача 18; эталоны: init-cluster.sh шаг 5,
/// гранты — §4 доки 11). Все тексты идемпотентны: роли/БД — guard через
/// каталоги (pg_database/pg_roles, паттерн «SELECT команды WHERE NOT EXISTS»),
/// схемы — IF NOT EXISTS. Подключение — к master-ноде шарда (user=postgres,
/// пароль из InstallSecrets Д7); пароли живут только в DSN-строке в памяти.
/// </summary>
public sealed partial class DatabaseProvisioner
{
    private const int RetryCount = 3;

    private static readonly TimeSpan FirstRetryDelay = TimeSpan.FromSeconds(1);

    // Шаблон идентификатора БД (valid_dbname из init-cluster.sh): защита от
    // SQL-инъекций через имя кластера, заявленное панелью.
    [GeneratedRegex("^[a-z_][a-z0-9_]*$", RegexOptions.CultureInvariant)]
    private static partial Regex IdentifierRegex();

    // Guard + команда создания БД: SELECT возвращает САМ текст CREATE DATABASE,
    // если БД нет (паттерн из PG-доков без psql-\gexec: скаляр → исполнить).
    public static string BuildCreateDatabaseSql(string dbname)
    {
        ValidateIdentifier(dbname);
        return $"SELECT 'CREATE DATABASE \"{dbname}\"' " +
               $"WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{dbname}')";
    }

    // Роли бакетного слоя (§4 доки 11): app (write-доступ клиентов), bucket_admin
    // (DSN-точка входа), bucket_mover (REPLICATION — подписки переездов P2/P3).
    // Пароли per-install (Д7) — из InstallSecrets; идемпотентно через pg_roles.
    public static string BuildRolesSql(InstallSecrets s)
    {
        var sb = new StringBuilder();
        sb.AppendLine(Role("app", s.AppPassword, replication: false));
        sb.Append(Role("bucket_admin", s.BucketAdminPassword, replication: false));
        sb.Append(Role("bucket_mover", s.MoverPassword, replication: true));
        return sb.ToString();
    }

    // Схемы бакетов шарда + гранты (шаг 5 init-cluster.sh; «гранты — при
    // создании схем, пустых таблиц нет» — GRANT ON ALL безвреден и на пустой
    // схеме, применяется идемпотентно повторными тиками).
    public static string BuildSchemasSql(string dbname, IEnumerable<int> bucketIds)
    {
        ValidateIdentifier(dbname);
        var sb = new StringBuilder();
        sb.AppendLine($"-- схемы бакетов БД {dbname} (идемпотентно; §4 доки 11)");
        foreach (var id in bucketIds.OrderBy(i => i))
        {
            if (id < 0)
                throw new ArgumentException($"идентификатор бакета не может быть отрицательным: {id}");

            sb.AppendLine($"CREATE SCHEMA IF NOT EXISTS bucket_{id};");
            sb.AppendLine($"GRANT USAGE ON SCHEMA bucket_{id} TO \"app\", \"bucket_admin\", \"bucket_mover\";");
            sb.AppendLine($"GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_{id} TO \"app\", \"bucket_admin\";");
            sb.AppendLine($"GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_{id} TO \"app\", \"bucket_admin\";");
            sb.AppendLine($"GRANT SELECT ON ALL TABLES IN SCHEMA bucket_{id} TO \"bucket_mover\";");
        }

        return sb.ToString();
    }

    // Исполнить батч (ExecuteNonQuery) с транзиент-ретраем Npgsql (Polly).
    public async Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
    {
        var pipeline = RetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryAsync(token);
            }, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"SQL не выполнен [{Redact(dsn)}]: {e.Message}", e));
        }
    }

    // Скалярный запрос (guard-проверки pg_database/pg_roles, SQL-пробы надзора).
    public async Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        var pipeline = RetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            var value = await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                return await cmd.ExecuteScalarAsync(token);
            }, ct);
            return Result<object?>.Success(value is DBNull ? null : value);
        }
        catch (Exception e)
        {
            return Result<object?>.Failed(new ApplicationException($"SQL-скаляр не выполнен [{Redact(dsn)}]: {e.Message}", e));
        }
    }

    // Идемпотентное создание БД: guard-SELECT генерирует команду CREATE,
    // если БД нет — исполняем её тем же подключением к поддерживающей БД.
    public async Task<Result> EnsureDatabaseAsync(string adminDsn, string dbname, CancellationToken ct)
    {
        var probe = await ExecuteScalarAsync(adminDsn, BuildCreateDatabaseSql(dbname), ct);
        if (!probe.IsSuccess)
            return probe;

        return probe.Value is string create
            ? await ExecuteAsync(adminDsn, create, ct)
            : Result.Success(); // БД уже есть — идемпотентность
    }

    // DSN master-ноды шарда для админ-операций: user=postgres, пароль Д7.
    public static string BuildAdminDsn(string host, int pgPort, string dbname, InstallSecrets secrets)
        => $"host={host} port={pgPort} dbname={dbname} user=postgres password={Escape(secrets.SuPassword)}";

    private static string Role(string name, string password, bool replication)
    {
        var attr = replication ? " REPLICATION" : string.Empty;
        return $"SELECT 'CREATE ROLE \"{name}\" LOGIN{attr} PASSWORD '{Escape(password)}' " +
               $"WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{name}');\n";
    }

    private static void ValidateIdentifier(string name)
    {
        if (!IdentifierRegex().IsMatch(name))
            throw new ArgumentException($"недопустимый идентификатор SQL: '{name}' (шаблон ^[a-z_][a-z0-9_]*)");
    }

    // Экранирование литерала: одинарная кавычка удваяется (SQL-инъекции паролей).
    private static string Escape(string value) => value.Replace("'", "''");

    // Пароль не должен попадать в тексты ошибок/логов (P12/P17).
    private static string Redact(string dsn)
        => Regex.Replace(dsn, "password=[^ ]*", "password=***");
}
