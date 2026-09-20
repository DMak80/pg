using System.Net;
using System.Net.Sockets;

namespace ValkeyWorker.IntegrationTests.Valkey;

// Рантайм-выбор свободного окна хост-портов для публикации valkey-нод (копия
// kfw; AGENTS.md «порты docker-контейнеров в тестах — динамические»): окно
// зондится TcpListener'ом на старте фикстуры — никаких литералов вида 17000 и
// никакой зависимости от поднятого dev-стенда (стендовая зона valkey-копии
// 15000–18000: 15xxx pg / 16xxx kfw / 17xxx valkey-стенды) и от параллельных
// прогонов тестов.
internal static class FreePortWindow
{
    // Поиск выше порт-зоны dev-стенда: 21000+.
    private const int SearchFrom = 21000;

    private const int SearchTo = 31000;

    // Окно с запасом: сценарии держат несколько кластеров по 1 ноде.
    internal const int Size = 64;

    // Шаг с запасом: соседние кандидаты не перекрываются.
    private const int Step = 128;

    // Порт-зона dev-стенда (вкл. стендовые valkey-копии): не пересекаем.
    private const int StandZoneFrom = 15000;

    private const int StandZoneTo = 18000;

    private static readonly object Gate = new();

    private static readonly int[] Candidates = BuildCandidates();

    private static int _cursor;

    private static int[] BuildCandidates()
    {
        var list = new List<int>();
        for (var start = SearchFrom; start + Size <= SearchTo; start += Step)
        {
            if (start < StandZoneTo && start + Size > StandZoneFrom)
                continue; // кандидат пересекает зону стенда — дальше
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

    private static int _singleCursor;

    // Выдача ОДНОГО свободного host-порта (ручные кейсы с контейнерами —
    // TlsMigrationTests): статический курсор процесса + зонд bind'ом. Порт не
    // пересекается ни с окнами других фикстур/коллекций параллельного прогона,
    // ни с портами, уже розданными V1-PortAllocIndex живым кластерам серии —
    // docker-proxy занятого порта держит LISTEN, зонд его видит (голый курсор
    // «From+2+n» в полной серии выдавал занятый порт → docker 500 «port is
    // already allocated»).
    internal static int NextFreePort()
    {
        lock (Gate)
        {
            for (var attempt = 0; attempt < SearchTo - SearchFrom; attempt++)
            {
                var port = SearchFrom + _singleCursor;
                _singleCursor = (_singleCursor + 1) % (SearchTo - SearchFrom);
                if (port >= StandZoneFrom && port < StandZoneTo)
                    continue; // зона dev-стенда — не выдаём
                if (IsPortFree(port))
                    return port;
            }
        }

        throw new InvalidOperationException("FreePortWindow: свободного порта не нашлось");
    }

    // Зонд одиночного порта: TcpListener bind — порт занят любым живым
    // биндингом (docker-proxy контейнера, чужой процесс).
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

    // Зонд окна: достаточно свободности первого порта (соседние с шагом).
    private static bool IsWindowFree(int start)
    {
        try
        {
            var listener = new TcpListener(IPAddress.Any, start);
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
