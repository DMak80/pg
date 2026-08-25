using System.Data;
using System.Globalization;
using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;
using Npgsql;

namespace AdminPanel.Probes;

// Результат SQL-пробы одного шарда: runtime + статус попытки (spec §4.7).
public sealed record SqlShardResult(ShardRuntime Runtime, ProbeResult Result);

// Проба шарда: 5 запросов каталога arch/03 §5 одним подключением (read-only).
public interface ISqlProbe
{
    Task<SqlShardResult> ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct);
}

// Реализация: строка строится из разобранных полей ShardInfo (DSN уже разобран
// DsnParser t03 — повторный парсинг не нужен, spec §3.6); любой отказ — ошибка
// целиком на шард (§3.7). Хосты — эндпоинт-синтаксис Npgsql host:port после HostMap.
[InjectAsSingleton(typeof(ISqlProbe))]
public sealed class SqlProbe(IOptions<ProbesOptions> options, TimeProvider time) : ISqlProbe
{
    // Часовой таймаут фолбэка не нужен:<= 0 → 3 c, как ModuleExtensions "patroni" (spec §4.4).
    private static int TimeoutSeconds(ProbesOptions value)
        => (int)Math.Ceiling(value.TimeoutSeconds <= 0 ? 3 : value.TimeoutSeconds);

    // Строка подключения пробы — публичный static: чистая часть для unit-тестов (spec §10.5).
    // Npgsql 10: TargetSessionAttributes — string с libpq-значением "read-write"
    // (enum-тип удалён в 10-й версии Npgsql) и допускается ТОЛЬКО при мульти-хосте:
    // с одиночным хостом Npgsql бросает NotSupportedException, а фильтровать некого —
    // ключ не ставится. default_transaction_read_only несовместим с read-write-фильтром
    // (PG отвергает сервер с default_transaction_read_only=on как не-writable), поэтому
    // «двойная защита от записи» (arch/02 §6.2) включается сессионным SET после выбора
    // мастера — см. ReadOnlyGuardSql и ProbeAsync.
    public static NpgsqlConnectionStringBuilder BuildConnectionString(ShardInfo shard, ProbesOptions options)
    {
        var defaultPort = shard.Port ?? 5432;
        var ports = shard.DsnPorts is { Count: > 0 } list ? list : [];
        // Порт per-host (libpq port=h1p,h2p); без порта в списке — Port/5432.
        var hosts = shard.DsnHosts.Select((host, i) => HostMapResolver.Resolve(
                options.HostMap, host, i < ports.Count && ports[i] is { } p ? p : defaultPort))
            .ToList();
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = string.Join(",", hosts),
            ApplicationName = "adminpanel",
            Timeout = TimeoutSeconds(options),
            CommandTimeout = TimeoutSeconds(options), // statement_timeout (arch/02 §6.2)
            // Prefer: узлы PgWorker (Spilo) пускают внешние хосты только hostssl
            // (pg_hba «no encryption» reject); trust-стенд без SSL — фолбэк.
            SslMode = SslMode.Prefer,
        };
        if (hosts.Count > 1)
            builder.TargetSessionAttributes = "read-write"; // multi-host ведёт на мастер
        if (shard.DbName is not null)
            builder.Database = shard.DbName;
        if (shard.User is not null)
            builder.Username = shard.User;
        // Per-cluster password: приоритет — пароль из DSN (per-cluster), фолбэк на
        // глобальный из ProbesOptions (для старых кластеров без password в DSN).
        builder.Password = shard.Password ?? options.Password;
        return builder;
    }

    // Тексты каталога — arch/03 §5 дословно (инвариант документа; семантика неизменна).
    private const string ReplicationSql = """
        select application_name, client_addr, state, sync_state, pg_wal_lsn_diff(
                 pg_current_wal_lsn(), replay_lsn) as lag_bytes
        from pg_stat_replication
        """;

    private const string SlotsSql = """
        select slot_name, slot_type, active, wal_status, safe_wal_size, confirmed_flush_lsn,
               pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes
        from pg_replication_slots
        """;

    private const string SubscriptionsSql = """
        select subname, received_lsn, latest_end_lsn, latest_end_time
        from pg_stat_subscription
        """;

    private const string SchemasSql = """
        select nspname from pg_namespace where nspname like 'bucket\_%' escape '\'
        """;

    private const string RecoverySql = "select pg_is_in_recovery()";

    // Двойная защита от записи (arch/02 §6.2): сессионный SET после выбора мастера —
    // в строке подключения несовместим с read-write-фильтром (см. BuildConnectionString).
    private const string ReadOnlyGuardSql = "set default_transaction_read_only = on";

    public async Task<SqlShardResult> ProbeAsync(ClusterInfo cluster, ShardInfo shard, CancellationToken ct)
    {
        var at = time.GetUtcNow();
        var target = $"{cluster.Name}/{shard.Name}";
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await using var connection = new NpgsqlConnection(
                BuildConnectionString(shard, options.Value).ConnectionString);
            await connection.OpenAsync(ct);
            await using (var guard = new NpgsqlCommand(ReadOnlyGuardSql, connection))
                await guard.ExecuteNonQueryAsync(ct);

            var inRecovery = await ScalarBoolAsync(connection, RecoverySql, ct);
            var standbies = await StandbiesAsync(connection, ct);
            var slots = await SlotsAsync(connection, ct);
            var subscriptions = await SubscriptionsAsync(connection, ct);
            var schemas = await SchemasAsync(connection, ct);

            var runtime = new ShardRuntime(shard.Name, slots, standbies, subscriptions, schemas, inRecovery, null);
            return new SqlShardResult(
                runtime,
                new ProbeResult(
                    target, "sql", true,
                    System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds, null, at));
        }
        catch (Exception e)
        {
            // Отказ пробы — целиком на шард: runtime с Error, списки пустые (spec §3.7);
            // etcd-данные шарда не роняются (arch/02 §6).
            return new SqlShardResult(
                new ShardRuntime(shard.Name, [], [], [], [], null, e.Message),
                new ProbeResult(
                    target, "sql", false,
                    System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds, e.Message, at));
        }
    }

    // numeric (pg_wal_lsn_diff/safe_wal_size) читается decimal → long: разности LSN
    // целочисленны и < 2^53 (spec §3.21); inet — значением + ToString.
    private static async Task<bool?> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct) is bool value ? value : null;
    }

    private static async Task<IReadOnlyList<StandbyInfo>> StandbiesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ReplicationSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<StandbyInfo>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new StandbyInfo(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetValue(1)?.ToString(),
                reader.GetString(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : (long)reader.GetDecimal(4)));
        }

        return result;
    }

    private static async Task<IReadOnlyList<ReplicationSlotInfo>> SlotsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(SlotsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<ReplicationSlotInfo>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new ReplicationSlotInfo(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : (long)reader.GetDecimal(4),
                reader.IsDBNull(6) ? null : (long)reader.GetDecimal(6)));
        }

        return result;
    }

    private static async Task<IReadOnlyList<SubscriptionInfo>> SubscriptionsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(SubscriptionsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<SubscriptionInfo>();
        while (await reader.ReadAsync(ct))
        {
            result.Add(new SubscriptionInfo(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetValue(1)?.ToString(),
                reader.IsDBNull(2) ? null : reader.GetValue(2)?.ToString(),
                reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3)));
        }

        return result;
    }

    private static async Task<IReadOnlyList<string>> SchemasAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(SchemasSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<string>();
        while (await reader.ReadAsync(ct))
            result.Add(reader.GetString(0));
        return result;
    }
}
