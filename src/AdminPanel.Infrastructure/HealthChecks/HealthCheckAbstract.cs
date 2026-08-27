using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AdminPanel.Infrastructure.HealthChecks;

public class HealthCheckAbstract<T>(T service) : IHealthCheck
    where T : IHealthCheckService
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        if (!service.StatusError.IsSuccess)
        {
            return Task.FromResult(
                HealthCheckResult.Unhealthy($"{GetName()} service has error", service.StatusError.Error));
        }

        if (!service.Inited)
        {
            return Task.FromResult(HealthCheckResult.Degraded($"{GetName()} service is starting"));
        }

        if (service.Working)
        {
            return Task.FromResult(HealthCheckResult.Healthy($"{GetName()} service has started"));
        }

        return Task.FromResult(HealthCheckResult.Unhealthy($"{GetName()} service is stopped"));
    }

    private string GetName()
        => typeof(T).Name;
}