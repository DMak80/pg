using System.Diagnostics;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Обеспечение pg_hba-строки backup_exec (arch/19 §7, канал arch/25
/// №22): приёмник в СВОЁМ контейнере добавляет идемпотентную строку в
/// ИСТОЧНИК конфига Patroni /run/postgres.yml (секция postgresql.pg_hba —
/// единственная «  pg_hba:» в файле) и будит Patroni SIGHUP'ом — тот сам
/// перегенерирует pg_hba.conf при каждом старте postgres уже с этой строкой
/// (правка pg_hba.conf напрямую бессмысленна: файл перегенерируется).
/// Ожидание применения — поллинг pg_hba.conf; нода без PG_VERSION ещё
/// бутстрапится — применится при первом старте, ждать нечего. Владелец
/// /run/postgres.yml после configure_spilo — postgres (adjust_owner):
/// запись возможна процессом приёмника (supervisord user=postgres).</summary>
public sealed class HbaEnsureOperation(IProcessRunner runner)
{
    /// <summary>Идемпотентная hba-строка физического стриминга pg_basebackup
    /// под ролью backup_exec (arch/19 §7).</summary>
    public const string HbaLine = "hostssl replication backup_exec all scram-sha-256";

    /// <summary>Маркер поиска строки в файлах (подстрока — как grep -q
    /// прежнего скрипта; уникальна).</summary>
    public const string Marker = "replication backup_exec";

    private const string PatroniConfigFile = "/run/postgres.yml";

    /// <summary>Бюджет ожидания перегенерации pg_hba.conf после SIGHUP:
    /// транспорт и файловые операции укладывают полный вызов в таймаут
    /// клиента воркера (10 с на вызов, Program.cs App).</summary>
    internal static readonly TimeSpan ApplyWait = TimeSpan.FromSeconds(8);

    public async Task<HbaEnsureResponse> RunAsync(CancellationToken ct)
    {
        var srcText = await File.ReadAllTextAsync(PatroniConfigFile, ct);
        var srcHas = srcText.Contains(Marker, StringComparison.Ordinal);

        var hbaPath = Path.Combine(NodePaths.Pgdata, "pg_hba.conf");
        // Нода без PG_VERSION ещё бутстрапится: применится при первом старте.
        var bootstrap = !File.Exists(Path.Combine(NodePaths.Pgdata, "PG_VERSION"));
        var hbaHas = File.Exists(hbaPath)
            && (await File.ReadAllTextAsync(hbaPath, ct)).Contains(Marker, StringComparison.Ordinal);
        if (srcHas && (hbaHas || bootstrap))
            return new HbaEnsureResponse(Applied: false, Bootstrap: bootstrap);

        if (!srcHas)
        {
            srcText = InsertIntoPgHbaSection(srcText);
            // Перезапись существующего файла сохраняет владельца/права
            // (postgres:root 644 после configure_spilo) — sed -i-эквивалент.
            await File.WriteAllTextAsync(PatroniConfigFile, srcText, ct);
        }

        // SIGHUP Patroni — безусловно после возможной вставки (семантика
        // прежнего скрипта); exit-код pkill не значим. Паттерн «patro[n]i» —
        // regex-группа против само-матча по cmdline (arch/19 §7).
        var pkill = new ProcessStartInfo { FileName = "pkill" };
        pkill.ArgumentList.Add("-HUP");
        pkill.ArgumentList.Add("-f");
        pkill.ArgumentList.Add("local/bin/patro[n]i");
        await runner.RunAsync(pkill, ct);

        if (bootstrap)
            return new HbaEnsureResponse(Applied: true, Bootstrap: true);

        var deadline = DateTime.UtcNow + ApplyWait;
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            if (File.Exists(hbaPath)
                && (await File.ReadAllTextAsync(hbaPath, ct)).Contains(Marker, StringComparison.Ordinal))
                return new HbaEnsureResponse(Applied: true, Bootstrap: false);
        }

        throw new ApplicationException(
            $"hba line did not appear in {hbaPath} within {ApplyWait.TotalSeconds:0}s after SIGHUP");
    }

    // Вставка строки списка после секции «  pg_hba:» (уровень секции
    // postgresql конфига Patroni). Секции нет — конфигурация ноды не
    // Spilo-шаблонная: отказ (transient для тика воркера).
    internal static string InsertIntoPgHbaSection(string text)
    {
        var lines = text.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].TrimEnd() != "  pg_hba:")
                continue;
            return string.Join('\n',
                lines[..(i + 1)].Append($"  - {HbaLine}").Concat(lines[(i + 1)..]));
        }
        throw new ApplicationException($"{PatroniConfigFile}: pg_hba section not found");
    }
}
