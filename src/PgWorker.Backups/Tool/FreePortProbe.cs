using System.Net;
using System.Net.Sockets;
using Shared.Core;

namespace PgWorker.Backups.Tool;

/// <summary>Зонд свободного host-порта (правило AGENTS.md «динамические
/// порты»): TcpListener на IPAddress.Any:0 — порт выделен ядром, слушатель
/// немедленно освобождается. Гонка «порт заняли до create» — create падает,
/// вызывающий трактует transient и повторяет с новым портом.</summary>
public static class FreePortProbe
{
    public static Result<int> Allocate()
    {
        var listener = new TcpListener(IPAddress.Any, 0);
        try
        {
            listener.Start();
            return Result<int>.Success(((IPEndPoint)listener.LocalEndpoint).Port);
        }
        catch (Exception e)
        {
            return Result<int>.Failed(new ApplicationException($"free port probe failed: {e.Message}", e));
        }
        finally
        {
            listener.Stop();
        }
    }
}
