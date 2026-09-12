namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Runtime-склейка секции PgWorker:Pgtune (spec.md §4.2): Provisioning не
/// зависит от PgWorker.App — App конвертирует PgtuneOptions.ToRuntime() и
/// провайдит record в DI (паттерн MovesRuntimeOptions). Строки домена
/// (DbType/HdType/DbSize) передаются как есть — маппинг в enum ядра PgTune
/// выполняет фабрика входов (PgtuneInputsFactory). ExcludeParams — имена
/// PGTune-параметров, вырезаемые при сборке YAML (SpiloEnvBuilder), не в ядре:
/// Calculate всегда даёт полный вывод. Память/CPU — НЕ конфигурация и НЕ
/// дефолты: их единственный источник — ОБЯЗАТЕЛЬНЫЕ etcd-заявки
/// /service/&lt;scope&gt;/request_{cpu,mem} на ноду (arch/14 §2.1 п.4).
/// </summary>
public sealed record PgtuneSettings(
    int DbVersion,
    string DbType,
    string HdType,
    string DbSize,
    int Connections,
    IReadOnlySet<string> ExcludeParams);
