using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes;

// Модуль live-проб (t06): attribute-DI + именованный HttpClient "patroni" с таймаутом
// из настроек — паттерн и порядок регистраций Etcd ModuleExtensions (t03).
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddProbes(this IServiceCollection services)
    {
        services.AutoRegistration(Assembly);

        // Порядок важен: AddHttpClient после AutoRegistration — typed-фабрика перекрывает
        // дескриптор автоскана, и PatroniRestProbe получал HttpClient из фабрики (t03 §4).
        services
           .AddHttpClient<PatroniRestProbe>(PatroniRestProbe.HttpClientName)
           .ConfigureHttpClient((sp, client) =>
            {
                var seconds = sp.GetRequiredService<IOptions<ProbesOptions>>().Value.TimeoutSeconds;
                if (seconds <= 0)
                {
                    sp.GetRequiredService<ILogger<PatroniRestProbe>>()
                       .LogWarning("AdminPanel:Probes:TimeoutSeconds <= 0 — использую 3 c");
                    seconds = 3;
                }

                client.Timeout = TimeSpan.FromSeconds(seconds);
            });

        // Kafka-проба (план B6): отдельный тик DescribeCluster, состояние — свой стор
        // (в снапшот вносит KafkaSnapshotRefresher); адаптер Confluent — единственный.
        services.AddSingleton<AdminPanel.Etcd.IKafkaSecretsStore, AdminPanel.Etcd.KafkaSecretsStore>();
        services.AddSingleton<Kafka.IKafkaProbeStore, Kafka.KafkaProbeStore>();
        services.AddSingleton<Kafka.IKafkaProbeClient, Kafka.ConfluentKafkaProbeClient>();
        services.AddSingleton<Kafka.KafkaProbeLoop>();
        services.AddHostedService(sp => sp.GetRequiredService<Kafka.KafkaProbeLoop>());

        return services;
    }
}
