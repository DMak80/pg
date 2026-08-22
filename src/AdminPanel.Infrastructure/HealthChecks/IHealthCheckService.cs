namespace AdminPanel.Infrastructure.HealthChecks;

public interface IHealthCheckService
{
    bool Inited { get; }

    bool Working { get; }

    Result StatusError { get; }
}