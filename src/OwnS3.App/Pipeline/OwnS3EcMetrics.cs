using System.Diagnostics.Metrics;
using OwnS3.Storage.Erasure;

namespace OwnS3.App.Pipeline;

// Метрики EC-режима (канон 05 §5; конвенция OwnS3Metrics: точки только в
// инструменте Meter, серии экспорта через подчёркивания):
// ownS3_ec_drive_online{set,drive}, ownS3_ec_set_readable{set},
// ownS3_ec_set_writable{set} — observable-gauge от ErasureStatusProvider;
// ownS3_heal_passes_total, ownS3_heal_objects_total — counters heal-сервиса.
public sealed class OwnS3EcMetrics
{
    public OwnS3EcMetrics(Meter meter, ErasureStatusProvider status, ErasureHealService heal)
    {
        meter.CreateObservableGauge<long>(
            "ownS3.ec.drive.online",
            () => status.GetStatus().Sets.SelectMany(s => s.Drives.Select(d => new Measurement<long>(
                d.Online ? 1 : 0,
                new KeyValuePair<string, object?>("set", s.Index.ToString()),
                new KeyValuePair<string, object?>("drive", d.Position.ToString())))).ToArray(),
            description: "Онлайн-флаг диска сета (1/0)");
        meter.CreateObservableGauge<long>(
            "ownS3.ec.set.readable",
            () => status.GetStatus().Sets.Select(s => new Measurement<long>(
                s.Readable ? 1 : 0, new KeyValuePair<string, object?>("set", s.Index.ToString()))).ToArray(),
            description: "Read-кворум сета доступен (1/0)");
        meter.CreateObservableGauge<long>(
            "ownS3.ec.set.writable",
            () => status.GetStatus().Sets.Select(s => new Measurement<long>(
                s.Writable ? 1 : 0, new KeyValuePair<string, object?>("set", s.Index.ToString()))).ToArray(),
            description: "Write-кворум сета доступен (1/0)");
        HealPasses = meter.CreateCounter<long>("ownS3.heal.passes", unit: "{pass}",
            description: "Heal-проходы EC");
        HealObjects = meter.CreateCounter<long>("ownS3.heal.objects", unit: "{object}",
            description: "Объекты, вылеченные heal");

        // heal-сервис публикует счётчики событием завершения прохода
        heal.PassCompleted += objectsHealed =>
        {
            HealPasses.Add(1);
            if (objectsHealed > 0)
                HealObjects.Add(objectsHealed);
        };
    }

    internal Counter<long> HealPasses { get; }

    internal Counter<long> HealObjects { get; }
}
