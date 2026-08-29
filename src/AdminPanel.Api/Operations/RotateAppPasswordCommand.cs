using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Заявка ротации app-пароля (арх-канон arch/02 §9.8, spec §4.5): панель ставит
// /pgworker/rotations/<C> txn-клэймом [version==0]+[put]; выполняет PgWorker
// (AppPasswordRotator): ALTER ROLE на всех шардах + атомарная замена app_password.
// Панель сама в SQL нод не ходит и app_password не пишет/не читает.
public sealed record RotateAppPasswordCommand(string Cluster, string RequestedBy)
    : ICommand<AppPasswordRotatedDto>;

public sealed record AppPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

// Живая заявка уже стоит: панель не перезаписывает (отмена — runbook/etcdctl).
public sealed class RotationAlreadyRequestedException(string cluster)
    : Exception($"ротация app-пароля {cluster} уже запрошена — дождитесь исполнения (ключ /pgworker/rotations/{cluster})");

[InjectAsScoped]
public sealed partial class RotateAppPasswordCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<RotateAppPasswordCommand, AppPasswordRotatedDto>
{
    // Канон тела заявки PgWorker: snake_case (образец TicketBody MoveBucketsCommand).
    private static readonly JsonSerializerOptions TicketJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private sealed record TicketBody(
        [property: JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: JsonPropertyName("requested_by")] string RequestedBy);

    // Имя кластера панели: ^[a-z][a-z0-9_]{0,62}$ (02 §9.3).
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    public async ValueTask<Result<AppPasswordRotatedDto>> Handle(
        RotateAppPasswordCommand command, CancellationToken ct)
    {
        var cluster = command.Cluster;

        // 1) Каноническое имя.
        if (!ClusterPattern().IsMatch(cluster))
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotFoundException(cluster));

        // 2) Активный endpoint.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<AppPasswordRotatedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Config напрямую (снапшот отстаёт до тика): нет → 404; state не Active → 409.
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(config.Error!);
        if (config.Value is null)
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            state = ReadState(config.Value);
        }
        catch (JsonException)
        {
            return Result<AppPasswordRotatedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<AppPasswordRotatedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Живая заявка → 409 (после исполнения PgWorker ключ исчезает — POST валиден).
        var key = $"/pgworker/rotations/{cluster}";
        var ticket = await ReadKeyAsync(endpoint, key, ct);
        if (!ticket.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(ticket.Error!);
        if (ticket.Value is not null)
            return Result<AppPasswordRotatedDto>.Failed(new RotationAlreadyRequestedException(cluster));

        // 5) Клэйм-txn: compare version==0 + put (образец §9.7 п.5; API панели —
        // TxnAsync(endpoint, compares, puts)). Проигрыш → 409.
        var requestedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var payload = JsonSerializer.Serialize(
            new TicketBody(requestedUnix, command.RequestedBy), TicketJson);
        var txn = await gateway.TxnAsync(
            endpoint, [new TxnCompare(key, 0)], [new KvPut(key, payload)], ct);
        if (!txn.IsSuccess)
            return Result<AppPasswordRotatedDto>.Failed(
                new EtcdWriteUnavailableException()); // транспортный сбой — 503
        if (!txn.Value.Succeeded)
            return Result<AppPasswordRotatedDto>.Failed(new RotationAlreadyRequestedException(cluster));

        return Result<AppPasswordRotatedDto>.Success(
            new AppPasswordRotatedDto(cluster, requestedUnix, command.RequestedBy));
    }

    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }
}
