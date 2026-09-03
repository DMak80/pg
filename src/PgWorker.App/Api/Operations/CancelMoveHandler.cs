using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Отмена стоящей заявки: DELETE /api/clusters/{cluster}/moves/{bucket}
// (t07, arch/02 §9.7.5). Удаление НЕ останавливает взятую в работу заявку —
// процесс ведёт фазы по статус-ключу и доедет до конца; остановка начатого —
// только abort. State кластера не проверяется (TO_REMOVE: заявки чистит D2 —
// ручная отмена безвредна). Идемпотентностью не обладает (повтор → 404).
public sealed partial class CancelMoveHandler(IEtcdGateway gateway, string[] endpoints)
{
    // Канонический leaf заявки: bucket_<int> без ведущих нулей.
    [GeneratedRegex("^bucket_(0|[1-9][0-9]*)$")]
    private static partial Regex BucketLeafPattern();

    public async Task<Result> HandleAsync(string cluster, string bucket, CancellationToken ct)
    {
        // 1) Имена канонические — иначе 404 (§9.7.5).
        if (!CreateClusterLimits.NamePattern().IsMatch(cluster))
            return Result.Failed(new ClusterNotFoundException(cluster));
        if (!BucketLeafPattern().IsMatch(bucket))
            return Result.Failed(new MoveTicketNotFoundException(cluster, bucket));

        // 2) Чтение ключа напрямую одним get: нет → 404 «заявки нет».
        var key = $"/pgworker/moves/{cluster}/{bucket}";
        var existing = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.GetAsync(endpoint, key, ct));
        if (!existing.IsSuccess)
            return Result.Failed(existing.Error!);
        if (existing.Value is null)
            return Result.Failed(new MoveTicketNotFoundException(cluster, bucket));

        // 3) del ключа → успех (204 ставит маршрут).
        var deleted = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.DeleteAsync(endpoint, key, prefix: false, ct));
        return deleted.IsSuccess ? Result.Success() : Result.Failed(deleted.Error!);
    }
}
