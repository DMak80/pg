using System.Buffers.Binary;
using System.Text;
using PgWorker.PgNode.BaseBackup;

namespace PgWorker.UnitTests.PgNode;

// Автомат разбора BASE_BACKUP (spec t01 §3.1): стартовый/финальный result
// set, tablespace list, COPY OUT-поток с внутренним фреймингом архивов
// ('n'/'m'/'d'/'p'); отказные ветки — server error, copy fail, чужие
// сообщения, дополнительные tablespace.
public class BaseBackupCommandTests
{
    // AAA: base.tar, pg_wal.tar и манифест разбираются в три sink'а,
    // CommandComplete завершает операцию.
    [Fact]
    public async Task ReadsArchivesAndManifest_ThenCompletes()
    {
        // Arrange
        var fake = new FakeSource(
            RowDescription(2), Message('D'), Message('C'),   // стартовая позиция WAL
            RowDescription(3), Message('D'), Message('C'),   // tablespace list (1 строка)
            Message('H'),                                    // COPY OUT
            CopyData(ArchiveStart("base.tar")),
            CopyData(Data("abc")), CopyData(Data("def")),
            CopyData(ArchiveStart("pg_wal.tar")),
            CopyData(Data("wal")),
            CopyData(Manifest()),
            CopyData(Data("{\"Files\":[]}")),
            Message('c'),                                    // CopyDone
            RowDescription(2), Message('D'), Message('C'));  // финальная позиция WAL
        var command = new BaseBackupCommand(fake);
        var sinks = new List<string>();

        // Act
        await command.RunAsync("id1", async (index, stream, ct) =>
        {
            using var buffer = new MemoryStream();
            await stream.CopyToAsync(buffer, ct);
            sinks.Add($"{index}:{Encoding.ASCII.GetString(buffer.ToArray())}");
        }, CancellationToken.None);

        // Assert
        sinks.Should().Equal("0:abcdef", "1:wal", "2:{\"Files\":[]}");
        fake.Sent.Should().ContainSingle()
            .Which.Should().Contain("LABEL 'pgw-full-id1'")
            .And.Contain("TARGET 'client'")
            .And.Contain("MANIFEST 'yes'")
            .And.Contain("WAL true");
    }

    // AAA: более одной строки tablespace list — fail-fast multiple tablespaces.
    [Fact]
    public async Task MultipleTablespaceRows_Throws()
    {
        // Arrange
        var fake = new FakeSource(
            RowDescription(2), Message('D'), Message('C'),
            RowDescription(3), Message('D'), Message('D'), Message('C'));
        var command = new BaseBackupCommand(fake);

        // Act
        var act = () => command.RunAsync("id1", (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Be("multiple tablespaces not supported");
    }

    // AAA: архив дополнительного tablespace (не base.tar/pg_wal.tar) — fail-fast.
    [Fact]
    public async Task ExtraTablespaceArchive_Throws()
    {
        // Arrange
        var fake = new FakeSource(
            RowDescription(2), Message('D'), Message('C'),
            RowDescription(3), Message('D'), Message('C'),
            Message('H'),
            CopyData(ArchiveStart("16384.tar")));
        var command = new BaseBackupCommand(fake);

        // Act
        var act = () => command.RunAsync("id1", (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Be("multiple tablespaces not supported");
    }

    // AAA: второй COPY OUT-поток — не наш формат (fail-fast).
    [Fact]
    public async Task SecondCopyOut_Throws()
    {
        // Arrange
        var fake = new FakeSource(
            RowDescription(2), Message('D'), Message('C'),
            RowDescription(3), Message('D'), Message('C'),
            Message('H'), Message('c'),
            Message('H'), Message('c'));
        var command = new BaseBackupCommand(fake);

        // Act
        var act = () => command.RunAsync("id1", (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Be("multiple tablespaces not supported");
    }

    // AAA: ErrorResponse сервера — BaseBackupErrorException с SQLState и текстом.
    [Fact]
    public async Task ErrorResponse_ThrowsWithSqlState()
    {
        // Arrange
        var fake = new FakeSource(Message('E', ErrorPayload("XX000", "boom")));
        var command = new BaseBackupCommand(fake);

        // Act
        var act = () => command.RunAsync("id1", (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<BaseBackupErrorException>();
        thrown.Which.Message.Should().Be("[XX000] boom");
        thrown.Which.SqlState.Should().Be("XX000");
    }

    // AAA: CopyFail внутри потока — исключение с текстом причины.
    [Fact]
    public async Task CopyFail_Throws()
    {
        // Arrange
        var fake = new FakeSource(
            RowDescription(2), Message('D'), Message('C'),
            RowDescription(3), Message('D'), Message('C'),
            Message('H'),
            CopyData(ArchiveStart("base.tar")),
            CopyData(Data("abc")),
            Message('f', "disk full"));
        var command = new BaseBackupCommand(fake);

        // Act
        var act = () => command.RunAsync("id1", async (_, stream, ct) =>
        {
            using var buffer = new MemoryStream();
            await stream.CopyToAsync(buffer, ct);
        }, CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Contain("disk full");
    }

    // AAA: чужой тип сообщения в ответе BASE_BACKUP — fail-fast.
    [Fact]
    public async Task UnexpectedMessage_Throws()
    {
        // Arrange
        var fake = new FakeSource(Message('I'));
        var command = new BaseBackupCommand(fake);

        // Act
        var act = () => command.RunAsync("id1", (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Contain("unexpected message 'I'");
    }

    private static PgMessage RowDescription(int fields)
    {
        var payload = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(payload, (short)fields);
        return new PgMessage((byte)'T', payload);
    }

    private static PgMessage Message(char type, string payload = "")
        => new((byte)type, Encoding.ASCII.GetBytes(payload));

    private static PgMessage Message(char type, byte[] payload)
        => new((byte)type, payload);

    private static PgMessage CopyData(byte[] payload) => new((byte)'d', payload);

    // 'n' + имя архива + каталог (пустая строка — основной data dir).
    private static byte[] ArchiveStart(string name)
    {
        var payload = new byte[1 + name.Length + 1 + 1];
        payload[0] = (byte)'n';
        Encoding.ASCII.GetBytes(name).CopyTo(payload, 1);
        return payload;
    }

    private static byte[] Manifest() => [(byte)'m'];

    private static byte[] Data(string text)
    {
        var bytes = Encoding.ASCII.GetBytes(text);
        var payload = new byte[1 + bytes.Length];
        payload[0] = (byte)'d';
        bytes.CopyTo(payload, 1);
        return payload;
    }

    private static byte[] ErrorPayload(string sqlState, string message)
    {
        using var buffer = new MemoryStream();
        buffer.WriteByte((byte)'C');
        WriteCString(buffer, sqlState);
        buffer.WriteByte((byte)'M');
        WriteCString(buffer, message);
        buffer.WriteByte(0);
        return buffer.ToArray();
    }

    private static void WriteCString(Stream target, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        target.Write(bytes);
        target.WriteByte(0);
    }

    // Фейк-источник: запрограммированная последовательность сообщений + запись команд.
    private sealed class FakeSource(params PgMessage[] messages) : IBaseBackupSource
    {
        private readonly Queue<PgMessage?> _queue = new(messages);

        public List<string> Sent { get; } = [];

        public Task SendAsync(string command, CancellationToken ct)
        {
            Sent.Add(command);
            return Task.CompletedTask;
        }

        public Task<PgMessage?> ReadAsync(CancellationToken ct)
            => Task.FromResult(_queue.Count > 0 ? _queue.Dequeue() : null);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
