using System.Net;
using System.Net.Sockets;

namespace S3Worker.IntegrationTests.S3;

// Рантайм-выбор свободного окна хост-портов публикации S3-нод (порт
// FreePortWindow valkey; AGENTS.md «порты docker-контейнеров — динамические»):
// окно зондится TcpListener'ом на старте фикстуры — никаких литералов портов
// и никаких пересечений с dev-стендом (pg 15xxx / kfw 16xxx / valkey 17xxx /
// owns3+s3 стенды и valkey-тесты 21–31xxx: наша зона — 31–41xxx).
internal static class FreePortWindow
{
    // Поиск выше зоны valkey-тестов (21–31xxx).
    private const int SearchFrom = 31000;

    private const int SearchTo = 41000;

    // Окно с запасом: сценарии держат несколько кластеров по 1–2 ноды.
    internal const int Size = 64;

    // Шаг с запасом: соседние кандидаты не перекрываются.
    private const int Step = 128;

    // Порт-зоны стенда и valkey-тестов: не пересекаем.
    private const int BusyZoneFrom = 15000;

    private const int BusyZoneTo = 31000;

    private static readonly object Gate = new();

    private static readonly int[] Candidates = BuildCandidates();

    private static int _cursor;

    private static int[] BuildCandidates()
    {
        var list = new List<int>();
        for (var start = SearchFrom; start + Size <= SearchTo; start += Step)
        {
            if (start < BusyZoneTo && start + Size > BusyZoneFrom)
                continue; // кандидат пересекает занятую зону — дальше
            list.Add(start);
        }

        return [.. list];
    }

    public static (int From, int To) Find()
    {
        lock (Gate)
        {
            for (var attempt = 0; attempt < Candidates.Length; attempt++)
            {
                var start = Candidates[_cursor];
                _cursor = (_cursor + 1) % Candidates.Length;
                if (IsWindowFree(start))
                    return (start, start + Size);
            }
        }

        throw new InvalidOperationException("FreePortWindow: свободного окна не нашлось");
    }

    private static bool IsWindowFree(int start)
    {
        for (var port = start; port < start + Size; port++)
        {
            if (!IsPortFree(port))
                return false;
        }

        return true;
    }

    private static bool IsPortFree(int port)
    {
        try
        {
            var listener = new TcpListener(IPAddress.Any, port);
            listener.Start();
            listener.Stop();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }
}
