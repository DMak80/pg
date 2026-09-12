namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Runtime-склейка секции PgWorker:Pgtune (spec.md §4.2): Provisioning не
/// зависит от PgWorker.App — App конвертирует PgtuneOptions.ToRuntime() и
/// провайдит record в DI (паттерн MovesRuntimeOptions). Строки домена
/// (DbType/HdType/DbSize) передаются как есть — маппинг в enum ядра PgTune
/// выполняет фабрика входов (PgtuneInputsFactory). ExcludeParams — имена
/// PGTune-параметров, вырезаемые при сборке YAML (SpiloEnvBuilder), не в ядре:
/// Calculate всегда даёт полный вывод.
/// </summary>
public sealed record PgtuneSettings(
    int DbVersion,
    string DbType,
    string HdType,
    string DbSize,
    int Connections,
    long DefaultTotalMemoryBytes,
    IReadOnlySet<string> ExcludeParams);
