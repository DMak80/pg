using PgWorker.Etcd.S3;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.App.Api.Operations;

/// <summary>Тело секции s3 policy (t36, arch/14 §1.1): пул + два списка имён
/// инстансов; null-поля валидируются по правилам «задана секция целиком».</summary>
public sealed record BackupsPolicyS3Body(
    [property: System.Text.Json.Serialization.JsonPropertyName("pool")] string? Pool,
    [property: System.Text.Json.Serialization.JsonPropertyName("write_master")] System.Collections.Generic.IReadOnlyList<string>? WriteMaster,
    [property: System.Text.Json.Serialization.JsonPropertyName("write_replica")] System.Collections.Generic.IReadOnlyList<string>? WriteReplica);

/// <summary>
/// Валидация секции s3 policy (t36, arch/14 §1.1): ЖИВОЕ чтение /s3/ (не кэш
/// снапшота). Ошибки — ValidationError-поля для 400-тела. Правила:
/// pool существует и не TO_REMOVE; имена списков — инстансы пула
/// (nodes/&lt;I&gt;/placement существует — заявка; portalloc-записи может НЕ быть —
/// инстанс заявлен, не провижинен → policy ВАЛИДНА, адрес появится);
/// без дублей внутри списка; списки непустые; pool обязателен при заданных
/// списках. Отсутствие секции s3 целиком — удаление из policy (валидации нет).
/// </summary>
public sealed class BackupsPolicyS3Validator(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<System.Collections.Generic.IReadOnlyList<ValidationError>>> ValidateAsync(
        BackupsPolicyS3Body? s3, CancellationToken ct)
    {
        var errors = new List<ValidationError>();
        if (s3 is null)
            return Result<System.Collections.Generic.IReadOnlyList<ValidationError>>.Success(errors); // секции нет — выключение

        var poolName = s3.Pool?.Trim();
        if (string.IsNullOrEmpty(poolName))
        {
            errors.Add(new ValidationError("s3.pool", "обязателен при заданной секции s3"));
            return Result<System.Collections.Generic.IReadOnlyList<ValidationError>>.Success(errors);
        }

        var read = await new S3PoolStore(gateway, endpoints).ReadAsync(poolName, ct);
        if (!read.IsSuccess)
            return Result<System.Collections.Generic.IReadOnlyList<ValidationError>>.Failed(read.Error!); // 503
        if (read.Value is null)
        {
            errors.Add(new ValidationError("s3.pool", $"пул {poolName} не найден в /s3/clusters/"));
            return Result<System.Collections.Generic.IReadOnlyList<ValidationError>>.Success(errors);
        }
        if (read.Value.ToRemove)
            errors.Add(new ValidationError("s3.pool", $"пул {poolName} в состоянии TO_REMOVE"));

        ValidateList("s3.write_master", "write_master", s3.WriteMaster, read.Value, errors);
        ValidateList("s3.write_replica", "write_replica", s3.WriteReplica, read.Value, errors);
        return Result<System.Collections.Generic.IReadOnlyList<ValidationError>>.Success(errors);
    }

    // Один список: непустота, отсутствие дублей, имена — инстансы пула
    // (Nodes собираются из ключей заявки nodes/<I>/state|placement — S3PoolStore).
    private static void ValidateList(
        string field, string label, System.Collections.Generic.IReadOnlyList<string>? names,
        S3Pool pool, List<ValidationError> errors)
    {
        if (names is null || names.Count == 0)
        {
            errors.Add(new ValidationError(field, $"список пуст (пусто = отсутствие секции s3 целиком)"));
            return;
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in names)
        {
            if (!seen.Add(name))
            {
                errors.Add(new ValidationError(field, $"дубликат {name}"));
                continue;
            }
            if (pool.Nodes.All(n => n.Name != name))
                errors.Add(new ValidationError(field, $"нода {name} не в пуле {pool.Name}"));
        }
    }
}
