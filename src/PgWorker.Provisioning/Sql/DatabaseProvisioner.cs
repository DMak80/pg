using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using PgWorker.Core;
using PgWorker.Core.Retry;
using PgWorker.Core.Templates;

namespace PgWorker.Provisioning.Sql;

/// <summary>
/// SQL-исполнитель процессов (мокабельная грань DatabaseProvisioner): все
/// операции идемпотентны на стороне SQL-текстов; DSN собирает процесс.
/// </summary>
public interface ISqlExecutor
{
    Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct);

    Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct);

    Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct);
}

/// <summary>
/// SQL-слой инициализации шарда (задача 18; эталоны: init-cluster.sh шаг 5,
/// гранты — §4 доки 11). Все тексты идемпотентны: роли/БД — guard через
/// каталоги (pg_database/pg_roles, паттерн «SELECT команды WHERE NOT EXISTS»),
/// схемы — IF NOT EXISTS. Подключение — к master-ноде шарда (user=postgres,
/// пароль из InstallSecrets Д7); пароли живут только в DSN-строке в памяти.
/// </summary>
public sealed partial class DatabaseProvisioner : ISqlExecutor
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

    // Guard-SELECT'ы ролей бакетного слоя (§4 доки 11): app (write-доступ
    // клиентов), bucket_admin (DSN-точка входа), bucket_mover (REPLICATION —
    // подписки переездов P2/P3). Паттерн \gexec: скаляр ВОЗВРАЩАЕТ текст
    // CREATE ROLE, если её нет — исполнитель запускает его отдельной командой
    // (Npgsql ExecuteNonQuery батчем gexec-SELECT не исполняет).
    // bucket_admin: per-cluster credentials (user+password из config кластера).
    public static IReadOnlyList<string> BuildRoleGuardsSql(InstallSecrets s,
        string? bucketAdminUser = null, string? bucketAdminPassword = null)
        => [Role("app", s.AppPassword),
            Role(bucketAdminUser ?? "bucket_admin", bucketAdminPassword ?? s.BucketAdminPassword),
            Role("bucket_mover", s.MoverPassword, replication: true)];

    // SQL-команды после guard-SELECT (исполняются через ExecuteAsync, не scalar).
    // pg_monitor: SQL-проба панели читает pg_stat_replication/pg_replication_slots
    // под bucket_admin — без pg_monitor PG маскирует state/sync_state NULL (arch/02 §6.2).
    public static IReadOnlyList<string> BuildRoleExecSql(string? bucketAdminUser = null)
        => [PgMonitorGrant(bucketAdminUser ?? "bucket_admin")];

    private static string PgMonitorGrant(string bucketAdminUser)
    {
        ValidateIdentifier(bucketAdminUser);
        // pg_monitor: GRANT идемпотентен (повторная выдача — no-op, не ошибка).
        return $"GRANT pg_monitor TO \"{bucketAdminUser}\";\n";
    }

    private static string Role(string name, string password, bool replication = false)
    {
        var attr = replication ? " REPLICATION" : string.Empty;
        // Пароль лежит внутри внешнего строкового литерала; хвост ''pw''' =
        // '' (кавычка внутри литерала) + закрывающая кавычка литерала.
        return $"SELECT 'CREATE ROLE \"{name}\" LOGIN{attr} PASSWORD ''{Escape(Escape(password))}''' " +
               $"WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{Escape(name)}');\n";
    }

    // Схемы бакетов шарда + гранты (шаг 5 init-cluster.sh; «гранты — при
    // создании схем, пустых таблиц нет» — GRANT ON ALL безвреден и на пустой
    // схеме, применяется идемпотентно повторными тиками).
    public static string BuildSchemasSql(string dbname, IEnumerable<int> bucketIds,
        string bucketAdminUser = "bucket_admin")
    {
        ValidateIdentifier(dbname);
        ValidateIdentifier(bucketAdminUser);
        var sb = new StringBuilder();
        sb.AppendLine($"-- схемы бакетов БД {dbname} (идемпотентно; §4 доки 11)");
        foreach (var id in bucketIds.OrderBy(i => i))
        {
            if (id < 0)
                throw new ArgumentException($"идентификатор бакета не может быть отрицательным: {id}");

            sb.AppendLine($"CREATE SCHEMA IF NOT EXISTS bucket_{id};");
            sb.AppendLine($"GRANT USAGE ON SCHEMA bucket_{id} TO \"app\", \"{bucketAdminUser}\", \"bucket_mover\";");
            sb.AppendLine($"GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA bucket_{id} TO \"app\", \"{bucketAdminUser}\";");
            sb.AppendLine($"GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA bucket_{id} TO \"app\", \"{bucketAdminUser}\";");
            sb.AppendLine($"GRANT SELECT ON ALL TABLES IN SCHEMA bucket_{id} TO \"bucket_mover\";");
        }

        // pg_monitor выдан в BuildRoleExecSql (при создании ролей — работает
        // и на AddShard, где BuildSchemasSql не вызывается).

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
    // Внутренний DSN Npgsql: разделитель ';' (libpq-пробелы Npgsql не парсит).
    // dsn-ключ etcd (P2.5) — остаётся libpq-форматом для панели/клиентов.
    public static string BuildAdminDsn(string host, int pgPort, string dbname, InstallSecrets secrets)
        => $"Host={host};Port={pgPort};Database={dbname};Username=postgres;Password={Escape(secrets.SuPassword)};SSL Mode=Require;Trust Server Certificate=true";

    private static void ValidateIdentifier(string name)
    {
        if (!IdentifierRegex().IsMatch(name))
            throw new ArgumentException($"недопустимый идентификатор SQL: '{name}' (шаблон ^[a-z_][a-z0-9_]*)");
    }

    // Экранирование литерала: одинарная кавычка удваяется (SQL-инъекции паролей).
    private static string Escape(string value) => value.Replace("'", "''");

    // Пароль не должен попадать в тексты ошибок/логов (P12/P17). Значение
    // ограничено: «;» (Npgsql-DSN, BuildAdminDsn пишет «Password=» с большой
    // буквы) либо пробел/конец строки (libpq dsn-ключи панели/клиентов);
    // quoted-вариант '…' — libpq-пароль с пробелами. Регистр не важен.
    [GeneratedRegex("password=(?:'[^']*'|[^; ]*)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex PasswordRegex();

    // internal для unit-тестов редакции (rework №2).
    internal static string Redact(string dsn) => PasswordRegex().Replace(dsn, "password=***");
}
