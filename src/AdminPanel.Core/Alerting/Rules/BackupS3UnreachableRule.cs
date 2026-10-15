using AdminPanel.Core.Alerting;
using Shared.Core.DI;

namespace AdminPanel.Core.Alerting.Rules;

// backup-s3-unreachable (warning, t08, каталог 03 §4): грань «Хранилище
// бэкапов» настроена (Endpoint задан), но S3-инвентарь-тик падал ≥ 2
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
        var s3 = snapshot.S3Storage;
        if (s3 is not { Configured: true } || s3.ConsecutiveFailures < Threshold)
            yield break; // грань выключена или порог не достигнут — молчание

        // t36: цель в заголовке — первая из списка (клиент перебирает с неё).
        var target = $"{s3.Endpoints.FirstOrDefault() ?? ""}/{s3.Bucket}";
        yield return new Alert(
            $"{KindName}:{target}",
            AlertSeverity.Warning,
            KindName,
            target,
            $"панель не может листить bucket бэкапов {s3.Bucket}: {s3.ConsecutiveFailures} подряд неудачных инвентарь-тиков ({s3.LastError})",
            new Dictionary<string, string>
            {
                ["consecutiveFailures"] = s3.ConsecutiveFailures.ToString(),
                ["bucket"] = s3.Bucket,
            },
            SinceUnix: null, // проставляет AlertEngine по стабильному id
            Hint: "панель не может листить bucket бэкапов: endpoints/креды/сеть — бэкапы воркера под угрозой, смотрите логи PgWorker и MinIO; live-инвентарь грани устаревает, etcd-статусы бэкапов продолжают обновляться",
            Remedy: AlertRemedy.OperatorRunbook,
            RemedyText: "проверьте доступность нод пула (AdminPanel:Backups:S3:Endpoints, креды, docker-сеть) и логи PgWorker; после восстановления алерт гаснет первым успешным тиком");
    }
}
