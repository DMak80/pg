using AdminPanel.Infrastructure;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace AdminPanel.Probes.Kafka;

// Адаптер Confluent.Kafka для kafka-пробы: SASL/PLAIN + SASL_PLAINTEXT (arch/15 §5);
// DescribeCluster с RequestTimeout на вызов (прецедент KafkaAdminClient воркера);
// исключения → Result.Failed (проба не роняет панель). Единственное место
// Probes-сборки с Confluent-типами.
public sealed class ConfluentKafkaProbeClient : IKafkaProbeClient
{
    public async Task<Result<KafkaProbeView>> DescribeClusterAsync(
        string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrap,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = user,
                SaslPassword = password,
            };
            using var admin = new AdminClientBuilder(config).Build();
            var cluster = await admin.DescribeClusterAsync(
                new DescribeClusterOptions { RequestTimeout = timeout });
            return Result<KafkaProbeView>.Success(new KafkaProbeView(
                cluster.Nodes.Select(n => new KafkaProbeBroker(n.Id, n.Host)).ToList(),
                cluster.Controller?.Id));
        }
        catch (Exception e)
        {
            return Result<KafkaProbeView>.Failed(new InvalidOperationException(
                $"DescribeCluster ({bootstrap}): {e.Message}", e));
        }
    }
}
