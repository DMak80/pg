using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using PgWorker.WalReceiver;
using Shared.Metrics;

namespace PgWorker.WalReceiver;

// Точка входа приёмника WAL (t25, arch/19 §3): WebApplication — база образа
// требует aspnet-рантайм; Kestrel 0.0.0.0:8080 несёт ТОЛЬКО /metrics
// (Shared.Metrics.AddAppMetrics/MapAppMetrics, Meter PgWorker.WalReceiver,
// словарь arch/18 §2.8); стрим — hosted-сервис WalReceiverLoop. Идентичность —
// env контейнера ноды (WalReceiverIdentity): невалидный набор — старт-отказ
// (лог в stdout контейнера + exit 1; supervisord exitcodes=0 не перезапускает).
// Явный internal-класс (не top-level statements): сгенерированный top-level
// Program публичен в глобальном namespace и конфликтует (CS0433) с
// PgWorker.App.Program в сборках, ссылающихся на оба проекта (тесты).
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.WebHost.UseUrls("http://0.0.0.0:8080");
        builder.Services.AddAppMetrics(WalReceiverMetrics.MeterName, builder.Configuration.GetSection("Metrics"));
        builder.Services.AddHostedService<WalReceiverLoop>();

        var app = builder.Build();
        app.MapAppMetrics();
        await app.RunAsync();
    }
}
