using PgWorker.Core;
using PgWorker.Docker.Drivers;

namespace PgWorker.Moves;

/// <summary>
/// DDL-перенос бакета (t01 задача 10, решение Д3): pg_dump --schema-only через
/// docker exec в мастер-контейнере источника (Spilo: утилиты под postgres;
/// exec от root → su postgres -c), применение батчем на приёмнике, гранты
/// app-роли, сверка инвентаря P5 (relkind×relname источник/приёмник равны).
/// </summary>
public sealed class MoveDdl(IClusterDriver driver, IMoveSqlExecutor sql)
{
    // Dump схемы бакета из мастер-контейнера узла-источника: флаги 1:1 со
    // скриптом шага 1 (--schema-only --no-owner --no-privileges). Имя бакета
    // валидируется до подстановки в shell-команду (SQL/shell-инъекции).
    // containerOverride (adopt-repair spec §3.3): у усыновлённой ноды pg_dump
    // идёт в её фактический docker-контейнер (postgres-образ несёт утилиты),
    // а не в каноническое pgw-имя; null — обычное поведение.
    public Task<Result<string>> DumpAsync(
        string cluster, string shard, string node, string dbname, string bucket,
        CancellationToken ct, string? containerOverride = null)
    {
        if (!MoveNames.ValidateIdentifier(bucket))
            throw new ArgumentException($"недопустимое имя бакета: '{bucket}' (шаблон ^[a-z][a-z0-9_]*)");

        var cmd = new[]
        {
            "su", "postgres", "-c",
            $"pg_dump --schema-only --no-owner --no-privileges --schema={bucket} {dbname}"
        };

        return containerOverride is { Length: > 0 }
            ? driver.ExecContainerAsync(containerOverride, cmd, ct)
            : driver.ExecNodeAsync(cluster, shard, node, cmd, ct);
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
