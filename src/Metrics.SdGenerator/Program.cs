using System.Diagnostics.Metrics;
using Metrics.SdGenerator;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Options;
using Shared.Etcd.Client;
using Shared.Metrics;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<SdGeneratorOptions>(builder.Configuration.GetSection("SdGenerator"));
// Плоский инстанс опций — hosted service/цикл принимают SdGeneratorOptions напрямую.
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<SdGeneratorOptions>>().Value);
// Самонаблюдение (arch/18 §5.2 job sd-generator): AddAppMetrics при Enabled=true
// регистрирует OTel-провайдер и DI-Meter с именем SdGenerator; инструментарий
// берёт этот Meter из DI (канон KafkaWorker.App). Enabled=false —
// провайдера/Meter в DI нет, SdGeneratorMetrics создаст свой (пишем «в никуда»).
builder.Services.AddAppMetrics(SdGeneratorMetrics.MeterName,
    builder.Configuration.GetSection("SdGenerator:Metrics"));
builder.Services.AddHttpClient("etcd");
builder.Services.AddSingleton<IEtcdGateway>(sp =>
    new EtcdGateway(sp.GetRequiredService<IHttpClientFactory>().CreateClient("etcd")));

// file_sd-контур: writer → метрики → цикл → hosted service.
builder.Services.AddSingleton(sp =>
    new SdFileWriter(sp.GetRequiredService<IOptions<SdGeneratorOptions>>().Value.OutputPath));
builder.Services.AddSingleton(sp => new SdGeneratorMetrics(
    sp.GetService<Meter>() ?? new Meter(SdGeneratorMetrics.MeterName), TimeProvider.System));
builder.Services.AddSingleton(sp =>
{
    var options = sp.GetRequiredService<IOptions<SdGeneratorOptions>>().Value;
    return new SdGeneratorLoop(
        sp.GetRequiredService<IEtcdGateway>(),
        options.Etcd.Endpoints,
        sp.GetRequiredService<SdFileWriter>(),
        sp.GetRequiredService<SdGeneratorMetrics>(),
        sp.GetRequiredService<ILogger<SdGeneratorLoop>>());
});
builder.Services.AddHostedService<SdGeneratorHostedService>();

var app = builder.Build();
app.MapAppMetrics();
app.Run();
