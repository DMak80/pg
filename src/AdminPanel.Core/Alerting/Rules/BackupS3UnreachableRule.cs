using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-s3-unreachable (warning, t08, каталог 03 §4): грань «Хранилище
// бэкапов» настроена (Endpoint задан), но инвентарь-тик MinIO падал ≥ 2
// подряд — панель не видит содержимое bucket бэкапов. Инвентарь устаревает
// (UpdatedAtUnix не растёт), etcd-часть грани (статусы/квота/сироты)
// продолжает работать. Снимается первым успешным тиком (failures → 0).
// Порог — константа, не настройка (spec §3.7; новых настроек нет).
[InjectAsSingleton(typeof(IAlertRule))]
public sealed class BackupS3UnreachableRule : IAlertRule
{
    public const string KindName = "backup-s3-unreachable";

    // Каталог 03 §4: >= 2 тиков (образец etcd-unreachable).
    public const int Threshold = 2;

    public string Kind => KindName;

    public IEnumerable<Alert> Evaluate(EtcdSnapshot snapshot, AlertContext context)
    {
        var minio = snapshot.MinioStorage;
        if (minio is not { Configured: true } || minio.ConsecutiveFailures < Threshold)
            yield break; // грань выключена или порог не достигнут — молчание

        yield return new Alert(
            $"{KindName}:{minio.Endpoint}/{minio.Bucket}",
            AlertSeverity.Warning,
            KindName,
            $"{minio.Endpoint}/{minio.Bucket}",
            $"панель не может листить bucket бэкапов {minio.Bucket}: {minio.ConsecutiveFailures} подряд неудачных инвентарь-тиков ({minio.LastError})",
            new Dictionary<string, string>
            {
                ["consecutiveFailures"] = minio.ConsecutiveFailures.ToString(),
                ["bucket"] = minio.Bucket,
            },
            SinceUnix: null, // проставляет AlertEngine по стабильному id
            Hint: "панель не может листить bucket бэкапов: endpoint/креды/сеть — бэкапы воркера под угрозой, смотрите логи PgWorker и MinIO; live-инвентарь грани устаревает, etcd-статусы бэкапов продолжают обновляться",
            Remedy: AlertRemedy.OperatorRunbook,
            RemedyText: "проверьте доступность MinIO (endpoint AdminPanel:Backups:S3:Endpoint, креды, docker-сеть) и логи PgWorker; после восстановления алерт гаснет первым успешным тиком");
    }
}
