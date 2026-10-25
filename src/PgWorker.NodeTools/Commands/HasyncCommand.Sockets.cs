using System.Net.Sockets;
using System.Text;

namespace PgWorker.NodeTools.Commands;

// Runtime-команды HAProxy через Unix-сокет. Таймаут — 5 c (штатный потолок).
public static class HaproxyRuntime
{
    public static string Call(string command, string socketPath)
    {
        using var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.IP);
        socket.ReceiveTimeout = 5_000;
        socket.SendTimeout = 5_000;
        socket.Connect(new UnixDomainSocketEndPoint(socketPath));
        var bytes = Encoding.UTF8.GetBytes(command + "\n");
        socket.Send(bytes);

        var buffer = new byte[65_536];
        try
        {
            var read = socket.Receive(buffer);
            return Encoding.UTF8.GetString(buffer, 0, read).TrimEnd('\n');
        }
        catch (SocketException e) when (e.SocketErrorCode == SocketError.TimedOut)
        {
            return string.Empty;
        }
    }
}
