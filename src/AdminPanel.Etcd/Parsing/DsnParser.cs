namespace AdminPanel.Etcd.Parsing;

// Разобранный libpq keyword-DSN: хосты (multi-host), порт, dbname, user (spec §6.4).
public sealed record DsnInfo(
    IReadOnlyList<string> Hosts,
    int? Port,
    string? DbName,
    string? User);

// Парсер libpq keyword-строки: токены key=value по пробелам; нераспознанное игнорируется
// (DSN пишут init-скрипты ../pg; quoting-синтаксис libpq в системе не используется).
public static class DsnParser
{
    public static DsnInfo Parse(string dsn)
    {
        var hosts = new List<string>();
        int? port = null;
        string? dbName = null;
        string? user = null;

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
                case "port" when int.TryParse(value, out var parsed):
                    port = parsed;
                    break;
                case "dbname":
                    dbName = value;
                    break;
                case "user":
                    user = value;
                    break;
            }
        }

        return new DsnInfo(hosts, port, dbName, user);
    }
}
