using System.Net;
using System.Net.Sockets;

namespace KafkaWorker.IntegrationTests.Kafka;

// Рантайм-выбор свободного окна хост-портов для публикации kafka-брокеров
// (фикс хрупкости интеграционных прогонов; AGENTS.md «порты docker-контейнеров
// в тестах — динамические»): окно зондится TcpListener'ом на старте фикстуры —
// никаких литералов вида 16000 и никакой зависимости от поднятого dev-станда
// (стенд живёт в зоне 15000–17000: kfw-брокеры 16xxx + portalloc PgWorker) и от
// параллельных прогонов тестов.
internal static class FreePortWindow
{
    // Поиск выше порт-зоны dev-станда: 21000+ (16xxx занят стендовыми
    // kafka-брокерами, 15xxx — portalloc PgWorker-нод стенда).
    private const int SearchFrom = 21000;

    private const int SearchTo = 31000;

    // Окно с запасом: коллекция kafka-e2e держит несколько кластеров по 1–4
    // брокера одновременно (itlifecycle*, re2, it1 — пик ~10 контейнеров).
    internal const int Size = 64;

    // Шаг с запасом: соседние кандидаты не перекрываются.
    private const int Step = 128;

    // Порт-зона dev-станда (вкл. kfw-брокеры и portalloc PgWorker): не пересекаем
    // даже если SearchFrom когда-нибудь опустят ниже.
    private const int StandZoneFrom = 15000;

    private const int StandZoneTo = 17000;

    // Шлюз выдачи окон: Find() может зваться параллельно (фикстуры кластеров +
    // MtlsApiTests), а зонд «bind→release» порт не резервирует — без шлюза два
    // потребителя получали одно окно (гонка TOCTOU: docker-publish упирался в
    // Kestrel того же порта). Кандидаты считаются один раз, курсор выдаёт их
    // без повторов в рамках процесса.
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
                // Курсор: окна выдаются непересекающимися; зонд ниже остаётся
                // защитой от внешних занятий (стенд, другие процессы) и окон,
                // освободившихся после обхода всего диапазона.
                var index = (_cursor + attempt) % Candidates.Length;
                var start = Candidates[index];
                if (!IsFree(start))
                    continue;

                _cursor = (index + 1) % Candidates.Length;
                return (start, start + Size - 1);
            }
        }

        throw new InvalidOperationException(
            $"нет свободного окна из {Size} хост-портов в диапазоне {SearchFrom}–{SearchTo}");
    }

    // Зонд: все порты окна должны биндиться одновременно (listen на всех
    // интерфейсах — ловит и wildcard-занятые); занят хоть один — окно не подходит.
    private static bool IsFree(int start)
    {
        var listeners = new List<TcpListener>();
        try
        {
            for (var port = start; port < start + Size; port++)
            {
                var listener = new TcpListener(IPAddress.Any, port);
                listener.Start(); // порт занят → SocketException → следующее окно
                listeners.Add(listener);
            }

            return true;
        }
        catch (SocketException)
        {
            return false;
        }
        finally
        {
            foreach (var listener in listeners)
                listener.Stop();
        }
    }
}
