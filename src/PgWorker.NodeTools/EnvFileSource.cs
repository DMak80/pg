namespace PgWorker.NodeTools;

// Patroni-callback запускается runit'ом с обрезанным env. Env-файл — явно
// переданный параметр; существующие env имеют приоритет (обратная совместимость).
public sealed class EnvFileSource : IDisposable
{
    public EnvFileSource(string path)
    {
        try
        {
            foreach (var line in File.ReadLines(path))
            {
                var name = line.Split('=', 2);
                if (name.Length == 2 && !string.IsNullOrWhiteSpace(name[0]))
                    Environment.SetEnvironmentVariable(name[0], name[1]);
            }
        }
        catch (FileNotFoundException)
        {
            // ручной запуск без entrypoint — остаёмся на текущем env.
        }
    }

    public void Dispose()
    {
    }
}
