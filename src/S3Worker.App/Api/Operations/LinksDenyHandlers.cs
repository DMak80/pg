using S3Worker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.App.Api.Operations;

// Замена графа репликации целиком (arch/24 §1.1, spec §3.3): кластер существует
// (404) — единственная гварда состояния (запись links в NOT_INITIALIZED кластер
// штатна — P4 применит граф при provisioning); валидация имён по фактическому
// составу → put ключа links каноническим JSON (замена целиком) → 202.
public sealed class PutLinksHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<bool>> HandleAsync(string cluster, PutLinksRequest request, CancellationToken ct)
    {
        var config = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, cluster, ct);
        if (!config.IsSuccess)
            return Result<bool>.Failed(config.Error!);
        if (config.Value is null)
            return Result<bool>.Failed(new S3ClusterNotFoundException(cluster));

        var names = await S3ApiHelpers.ListNodeNamesAsync(gateway, endpoints, cluster, ct);
        if (!names.IsSuccess)
            return Result<bool>.Failed(names.Error!);

        // Валидация — чистая функция links × фактические имена (юнит-тестируемая).
        var errors = S3LinksValidator.Validate(request.Links ?? [], names.Value);
        if (errors.Count > 0)
            return Result<bool>.Failed(new S3InvalidLinksException(errors));

        var canonical = S3LinksValidator.CanonicalJson(request.Links!);
        var put = await S3ApiHelpers.CallAsync<bool>(endpoints, async endpoint =>
        {
            var write = await gateway.PutAsync(endpoint, S3ProcessCommon.LinksKey(cluster), canonical, null, ct);
            return write.IsSuccess ? Result<bool>.Success(true) : Result<bool>.Failed(write.Error!);
        });
        if (!put.IsSuccess)
            return Result<bool>.Failed(put.Error!);
        return Result<bool>.Success(true); // 202
    }
}

// POST /api/s3/clusters/{c}/nodes/{n}/deny — отзыв ключа: имя в denylist кластера
// (RMW по mod_revision; уже в списке → 204 идемпотентно; добавили → 202).
public sealed class DenyHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<bool>> HandleAsync(string cluster, string node, CancellationToken ct)
    {
        var names = await S3ApiHelpers.ListNodeNamesAsync(gateway, endpoints, cluster, ct);
        if (!names.IsSuccess)
            return Result<bool>.Failed(names.Error!);
        if (!names.Value.Contains(node, StringComparer.Ordinal))
            return Result<bool>.Failed(new S3NodeNotFoundException(cluster, node));

        var current = await S3ApiHelpers.CallAsync(endpoints, endpoint =>
            gateway.GetAsync(endpoint, S3ProcessCommon.DenyListKey(cluster), ct));
        if (!current.IsSuccess)
            return Result<bool>.Failed(current.Error!);

        var list = ParseDenyList(current.Value?.Value);
        if (list.Contains(node, StringComparer.Ordinal))
            return Result<bool>.Success(false); // already — 204

        // RMW: txn по mod_revision; проигрыш — повтор следующим запросом/тиком.
        list.Add(node);
        var key = S3ProcessCommon.DenyListKey(cluster);
        var compare = current.Value is { } kv
            ? TxnCompare.ModRevisionEqual(key, (long)kv.ModRevision)
            : TxnCompare.NotExists(key);
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([compare], [new TxnOp.Put(key, S3LinksValidator.DenyListJson(list), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<bool>.Failed(new S3ConcurrentWriteException(key));
        return Result<bool>.Success(true); // 202
    }

    internal static List<string> ParseDenyList(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            return doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Array
                ? doc.RootElement.EnumerateArray()
                    .Where(e => e.ValueKind == System.Text.Json.JsonValueKind.String)
                    .Select(e => e.GetString()!).ToList()
                : [];
        }
        catch (System.Text.Json.JsonException)
        {
            return []; // битое значение — перезапись с нуля (доведение восстановит)
        }
    }
}

// POST /api/s3/clusters/{c}/nodes/{n}/allow — снятие отзыва: имени нет → 204
// идемпотентно; удалили → 202 (RMW по mod_revision).
public sealed class AllowHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<bool>> HandleAsync(string cluster, string node, CancellationToken ct)
    {
        var names = await S3ApiHelpers.ListNodeNamesAsync(gateway, endpoints, cluster, ct);
        if (!names.IsSuccess)
            return Result<bool>.Failed(names.Error!);
        if (!names.Value.Contains(node, StringComparer.Ordinal))
            return Result<bool>.Failed(new S3NodeNotFoundException(cluster, node));

        var current = await S3ApiHelpers.CallAsync(endpoints, endpoint =>
            gateway.GetAsync(endpoint, S3ProcessCommon.DenyListKey(cluster), ct));
        if (!current.IsSuccess)
            return Result<bool>.Failed(current.Error!);

        var list = DenyHandler.ParseDenyList(current.Value?.Value);
        if (!list.Remove(node))
            return Result<bool>.Success(false); // already — 204

        var key = S3ProcessCommon.DenyListKey(cluster);
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)current.Value!.ModRevision)],
                [new TxnOp.Put(key, S3LinksValidator.DenyListJson(list), null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<bool>.Failed(new S3ConcurrentWriteException(key));
        return Result<bool>.Success(true); // 202
    }
}

// Валидатор графа links и канонические JSON-значения деклараций (чистые функции).
public static class S3LinksValidator
{
    // Имена существуют в фактическом составе, a≠b, пары уникальны с точностью
    // до зеркальности ({a,b} ≡ {b,a} — одна связь; зеркальный дубль = 400).
    public static List<ValidationError> Validate(IReadOnlyList<S3LinkDto> links, IReadOnlyList<string> names)
    {
        var errors = new List<ValidationError>();
        var existing = names.ToHashSet(StringComparer.Ordinal);
        var seen = new HashSet<(string, string)>(OrdinalTupleComparer.Instance);
        for (var i = 0; i < links.Count; i++)
        {
            var (a, b) = (links[i].A, links[i].B);
            if (a is null || !S3Limits.InstanceNamePattern().IsMatch(a) || !existing.Contains(a))
                errors.Add(new($"links[{i}].a", $"имя «{a}» не существует в кластере"));
            if (b is null || !S3Limits.InstanceNamePattern().IsMatch(b) || !existing.Contains(b))
                errors.Add(new($"links[{i}].b", $"имя «{b}» не существует в кластере"));
            if (a is not null && string.Equals(a, b, StringComparison.Ordinal))
                errors.Add(new($"links[{i}]", "a и b совпадают — связь вырождена"));
            if (a is not null && b is not null && !string.Equals(a, b, StringComparison.Ordinal))
            {
                // Каноническая пара (Ordinal-минимальный первым) — зеркальный дубль ловится тем же множеством.
                var canonicalPair = string.CompareOrdinal(a, b) < 0 ? (a, b) : (b, a);
                if (!seen.Add(canonicalPair))
                    errors.Add(new($"links[{i}]", $"дубль пары {{a,b}} (зеркальность ≡ одна связь)"));
            }
        }

        return errors;
    }

    // Канонические JSON — одна точка с процессами (S3ProcessCommon, arch/23 §2.1).
    public static string CanonicalJson(IReadOnlyList<S3LinkDto> links)
        => S3ProcessCommon.LinksJson(links.Select(l => (l.A!, l.B!)).ToList());

    public static string DenyListJson(IReadOnlyList<string> names)
        => S3ProcessCommon.DenyListJson(names);

    // Компаратор канонических пар (a,b) без реверса.
    private sealed class OrdinalTupleComparer : IEqualityComparer<(string A, string B)>
    {
        public static readonly OrdinalTupleComparer Instance = new();
        public bool Equals((string A, string B) x, (string A, string B) y)
            => string.Equals(x.A, y.A, StringComparison.Ordinal) && string.Equals(x.B, y.B, StringComparison.Ordinal);
        public int GetHashCode((string A, string B) pair) => HashCode.Combine(pair.A, pair.B);
    }
}
