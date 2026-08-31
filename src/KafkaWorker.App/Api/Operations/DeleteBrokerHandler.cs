using System.Text.RegularExpressions;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.App.Api.Operations;

// Удаление брокера через API воркера (arch/02 §10.2-5): маркер TO_REMOVE
// (one-way, идемпотентен). Серверные пред-проверки по прямым чтениям etcd:
// не controller (role-ключ), не последний; live-проверку размещения реплик
// панель НЕ делает (в etcd фактических реплик нет) — guard «на брокере есть
// партиции» авторитетно исполняет воркер (drain процессом I arch/16 §5).
// Порт панельного RemoveKafkaBrokerCommandHandler.
public sealed partial class DeleteBrokerHandler(IEtcdGateway gateway, string[] endpoints)
{
    // Имя брокера каноническое (иначе 404 — воркер такие не создаёт).
    [GeneratedRegex("^broker[1-9]$")]
    private static partial Regex BrokerPattern();

    public async Task<Result> HandleAsync(string cluster, string broker, CancellationToken ct)
    {
        if (!BrokerPattern().IsMatch(broker))
            return Result.Failed(new KafkaBrokerNotFoundException(cluster, broker));

        var config = await KafkaApiHelpers.ReadConfigAsync(gateway, endpoints, cluster, ct);
        if (config.Error is not null)
            return Result.Failed(config.Error);
        if (config.Value is null)
            return Result.Failed(new KafkaClusterNotFoundException(cluster));
        if (config.Value.State is not null)
            return Result.Failed(new KafkaClusterNotActiveException(cluster, config.Value.State));

        // Guard'ы по свежим данным префикса (роль/число живых брокеров).
        var role = await KafkaApiHelpers.ReadKeyAsync(
            gateway, endpoints, KafkaApiHelpers.BrokerKey(cluster, broker, "role"), ct);
        if (!role.IsSuccess)
            return Result.Failed(role.Error!);
        if (role.Value == "controller")
            return Result.Failed(new KafkaBrokerIsControllerException(cluster, broker));

        var brokers = await KafkaApiHelpers.ReadBrokerNamesAsync(gateway, endpoints, cluster, ct);
        if (!brokers.IsSuccess)
            return Result.Failed(brokers.Error!);
        if (!brokers.Value.Contains(ParseBrokerId(broker)))
            return Result.Failed(new KafkaBrokerNotFoundException(cluster, broker));
        if (brokers.Value.Count <= 1)
            return Result.Failed(new KafkaLastBrokerException(cluster));

        // Маркер one-way; идемпотентен (повтор PUT того же значения безвреден).
        var marked = await EtcdFailover.CallAsync(endpoints, endpoint => gateway.PutAsync(
            endpoint, KafkaApiHelpers.BrokerKey(cluster, broker, "state"), "TO_REMOVE", null, ct));
        return marked.IsSuccess ? Result.Success() : Result.Failed(marked.Error!);
    }

    private static int ParseBrokerId(string broker)
        => int.TryParse(broker["broker".Length..], out var id) ? id : 0;
}
