using System.Diagnostics;
using System.Text.RegularExpressions;

namespace PgWorker.WalReceiver.Operations;

/// <summary>pg_dump схемы-бакета (arch/14 §Move M1, канал arch/25 №22):
/// приёмник ноды-источника запускает pg_dump ЛОКАЛЬНО в своём контейнере
/// (ProcessRunner, arch/19 §3.4) с socket-коннектом к своей базе — секретов
/// нет (локальный pg_hba Spilo — trust, юзер приёмника postgres). Имя схемы
/// валидируется до подстановки в argv (SQL/shell-инъекции).</summary>
public sealed partial class PgDumpOperation(IProcessRunner runner)
{
    // Канонический шаблон имени схемы-бакета (тот же контракт, что на воркере).
    [GeneratedRegex("^[a-z][a-z0-9_]*$", RegexOptions.CultureInvariant)]
    private static partial Regex BucketNameRegex();

    // Socket-каталог Spilo (postgres:postgres 2775 — доступен юзеру приёмника);
    // роль postgres — локальный trust, суперпользователь кластера.
    private const string SocketDir = "/var/run/postgresql";

    public async Task<PgDumpResponse> RunAsync(string schema, string dbname, CancellationToken ct)
    {
        if (!BucketNameRegex().IsMatch(schema))
            throw new ApplicationException($"invalid bucket name: '{schema}' (pattern ^[a-z][a-z0-9_]*)");

        // Флаги 1:1 с прежним exec-вариантом (--schema-only --no-owner
        // --no-privileges); HOME=/tmp — нет ожиданий на домашний каталог.
        var psi = new ProcessStartInfo { FileName = "pg_dump" };
        psi.ArgumentList.Add("--schema-only");
        psi.ArgumentList.Add("--no-owner");
        psi.ArgumentList.Add("--no-privileges");
        psi.ArgumentList.Add($"--schema={schema}");
        psi.ArgumentList.Add("-h");
        psi.ArgumentList.Add(SocketDir);
        psi.ArgumentList.Add("-U");
        psi.ArgumentList.Add("postgres");
        psi.ArgumentList.Add(dbname);
        psi.EnvironmentVariables["HOME"] = "/tmp";

        var (exitCode, stdout, stderr) = await runner.RunAsync(psi, ct);
        if (exitCode != 0)
            throw new ApplicationException(
                $"pg_dump {schema}: exit {exitCode}: {FirstLine(stderr)}");
        return new PgDumpResponse(stdout);
    }

    private static string FirstLine(string text)
    {
        var nl = text.IndexOf('\n');
        var line = nl < 0 ? text : text[..nl];
        return line.Length > 0 ? line : text.Trim();
    }
}
