using PgWorker.Backups;

namespace PgWorker.Moves;

/// <summary>
/// DDL-перенос бакета (t01 задача 10, решение Д3): pg_dump --schema-only
/// командой приёмнику ноды-источника (канал arch/25 №22 — приёмник снимает
/// дамп локально в контейнере ноды, arch/14 §Move M1), применение батчем на
/// приёмнике, гранты app-роли, сверка инвентаря P5 (relkind×relname
/// источник/приёмник равны).
/// </summary>
public sealed class MoveDdl(IWalReceiverApiClient receiver, IMoveSqlExecutor sql)
{
    /// <summary>Dump схемы бакета из ноды-мастера источника: приёмник ноды
    /// запускает pg_dump локально (socket-коннект, без секретов); имя бакета
    /// валидируется до подстановки (SQL/shell-инъекции) и на стороне
    /// приёмника — дубль контракта PgDumpOperation.</summary>
    public Task<NodeReply<PgDumpResponse>> DumpAsync(
        string cluster, string shard, string node, string dbname, string bucket,
        CancellationToken ct)
    {
        if (!MoveNames.ValidateIdentifier(bucket))
            throw new ArgumentException($"invalid bucket name: '{bucket}' (pattern ^[a-z][a-z0-9_]*)");

        return receiver.PgDumpAsync(
            WalReceiverAddress.ForNode(cluster, shard, node),
            new StartPgDumpRequest(bucket, dbname), ct);
    }

    // Применение DDL на приёмнике: батч Npgsql (ON_ERROR_STOP-эквивалент —
    // исключение батча → transient-отказ тика, повтор безопасен по P5-сверке).
    // psql-метакоманды (PG17.2+/18: пары \restrict/\unrestrict против инъекций
    // имён при restore) вырезаются — серверный протокол их не понимает.
    public Task<Result> ApplyAsync(string dsn, string ddl, CancellationToken ct)
        => sql.ExecuteAsync(dsn, StripPsqlMeta(ddl), ct);

    internal static string StripPsqlMeta(string ddl)
    {
        // Строки, начинающиеся с '\' — psql-метакоманды (\restrict, \unrestrict…);
        // внутри SQL-строк/комментариев '\' в начале строки не встречается
        // (pg_dump цитирует и комментирует по своим правилам).
        var lines = ddl.Split('\n');
        var kept = lines.Where(l => !l.StartsWith('\\'));
        return string.Join('\n', kept);
    }

    // Гранты app-роли на приёмнике: USAGE + DML + sequences (grant_app_role).
    public Task<Result> GrantAppOnSchemaAsync(string dsn, string bucket, CancellationToken ct)
        => sql.ExecuteAsync(dsn, MoveSql.GrantAppOnSchema(bucket, MoveNames.AppRole), ct);

    // Сверка инвентаря P5: построчные списки relkind|relname источника и
    // приёмника равны (мораторий DDL соблюдён, копия полна).
    public async Task<Result<bool>> InventoryMatchesAsync(
        string srcDsn, string dstDsn, string bucket, CancellationToken ct)
    {
        var inventory = MoveSql.SchemaInventory(bucket);
        var src = await sql.ListAsync(srcDsn, inventory, ct);
        if (!src.IsSuccess)
            return Result<bool>.Failed(src.Error!);
        var dst = await sql.ListAsync(dstDsn, inventory, ct);
        if (!dst.IsSuccess)
            return Result<bool>.Failed(dst.Error!);

        return Result<bool>.Success(src.Value.SequenceEqual(dst.Value));
    }
}
