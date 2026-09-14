using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using Shared.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Приём per-cluster политики бэкапов (t06, arch/19 §4): тело канона
// {"retention":{"days":..,"weeks":..,"months":..},"full_max_age_sec":..,
// "verify":{"on_create":..}}; отсутствующие retention-поля → дефолты, тело
// ЦЕЛИКОМ замещает политику (put полного значения). Применение — следующим
// тиком планировщика t02/ретенции (etcd-снапшот, нотификации нет).
public sealed partial class BackupsPolicyHandler(IEtcdGateway gateway, string[] endpoints)
{
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    private sealed record PolicyBody(
        [property: JsonPropertyName("retention")] RetentionBody? Retention,
        [property: JsonPropertyName("full_max_age_sec")] long? FullMaxAgeSec,
        [property: JsonPropertyName("verify")] VerifyBody? Verify);

    private sealed record RetentionBody(
        [property: JsonPropertyName("days")] int? Days,
        [property: JsonPropertyName("weeks")] int? Weeks,
        [property: JsonPropertyName("months")] int? Months);

    private sealed record VerifyBody(
        [property: JsonPropertyName("on_create")] bool? OnCreate);

    public async Task<Result<string>> HandleAsync(string cluster, string rawBody, CancellationToken ct)
    {
        // 1) Каноническое имя кластера.
        if (!ClusterPattern().IsMatch(cluster))
            return Result<string>.Failed(new ClusterNotFoundException(cluster));

        // 2) Гвард кластера: /clusters/<C>/config отсутствует → 404 (spec §3.5).
        var config = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/clusters/{cluster}/config", ct));
        if (!config.IsSuccess)
            return Result<string>.Failed(new EtcdWriteUnavailableException());
        if (config.Value.All(kv => kv.Key != $"/clusters/{cluster}/config"))
            return Result<string>.Failed(new ClusterNotFoundException(cluster));

        // 3) Разбор тела: мусорный JSON → 400 (валидация); отсутствующие поля → дефолты.
        PolicyBody? body;
        try
        {
            body = JsonSerializer.Deserialize<PolicyBody>(rawBody,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = false });
        }
        catch (JsonException)
        {
            return Result<string>.Failed(new BackupsPolicyValidationException(
                [new ValidationError("body", "тело запроса — JSON вида {\"retention\":{...},\"full_max_age_sec\":..,\"verify\":{\"on_create\":..}}")]));
        }

        if (body is null)
            return Result<string>.Failed(new BackupsPolicyValidationException(
                [new ValidationError("body", "тело запроса обязательно")]));

        // 4) Валидация диапазонов (400 с перечнем): days [1..365], weeks [0..52],
        //    months [0..120], full_max_age_sec >= 600, on_create — bool из JSON-схемы.
        var errors = new List<ValidationError>();
        var days = body.Retention?.Days ?? 7;
        var weeks = body.Retention?.Weeks ?? 4;
        var months = body.Retention?.Months ?? 6;
        var maxAge = body.FullMaxAgeSec ?? 86400;
        var onCreate = body.Verify?.OnCreate ?? true;
        if (days is < 1 or > 365)
            errors.Add(new("retention.days", "дневная гранула — целое в [1..365]"));
        if (weeks is < 0 or > 52)
            errors.Add(new("retention.weeks", "недельная гранула — целое в [0..52]"));
        if (months is < 0 or > 120)
            errors.Add(new("retention.months", "месячная гранула — целое в [0..120]"));
        if (maxAge < 600)
            errors.Add(new("full_max_age_sec", "минимум 600 c"));
        if (errors.Count > 0)
            return Result<string>.Failed(new BackupsPolicyValidationException(errors));

        // 5) Put полного значения формата канона (тело замещает политику целиком).
        var payload = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["retention"] = new Dictionary<string, object> { ["days"] = days, ["weeks"] = weeks, ["months"] = months },
            ["full_max_age_sec"] = maxAge,
            ["verify"] = new Dictionary<string, object> { ["on_create"] = onCreate },
        });
        var put = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.PutAsync(endpoint, $"/pgworker/backups/{cluster}/policy", payload, null, ct));
        if (!put.IsSuccess)
            return Result<string>.Failed(new EtcdWriteUnavailableException());

        return Result<string>.Success(payload);
    }
}
