using System.Globalization;

namespace AdminPanel.Etcd.Parsing;

// Разобранный libpq keyword-DSN: хосты (multi-host), порты (по порту на хост —
// PgWorker пишет port=15000,15001), dbname, user (spec §6.4).
public sealed record DsnInfo(
    IReadOnlyList<string> Hosts,
    int? Port,
    IReadOnlyList<int?> Ports,
    string? DbName,
    string? User,
    string? Password = null);

// Парсер libpq keyword-строки: токены key=value по пробелам; нераспознанное игнорируется
// (DSN пишут init-скрипты ../pg; quoting-синтаксис libpq в системе не используется).
public static class DsnParser
{
    public static DsnInfo Parse(string dsn)
    {
        var hosts = new List<string>();
        var ports = new List<int?>();
        int? port = null;
        string? dbName = null;
        string? user = null;
        string? password = null;

        foreach (var token in dsn.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = token.IndexOf('=');
            if (eq <= 0)
                continue;

            var key = token[..eq];
            var value = token[(eq + 1)..];
            switch (key)
            {
                case "host":
                    hosts.AddRange(value.Split(',', StringSplitOptions.RemoveEmptyEntries));
                    break;
                case "port":
                    // libpq допускает список портов (по одному на хост); Port — первый,
                    // Ports выровнен с Hosts для построения эндпоинтов пробы.
                    ports = value.Split(',', StringSplitOptions.RemoveEmptyEntries)
                        .Select(ParsePort)
                        .ToList();
                    port = ports.FirstOrDefault();
                    break;
                case "dbname":
                    dbName = value;
                    break;
                case "user":
                    user = value;
                    break;
                case "password":
                    password = value;
                    break;
            }
        }

        return new DsnInfo(hosts, port, ports, dbName, user, password);
    }

    private static int? ParsePort(string raw)
        => int.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
}
