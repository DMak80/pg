using System.Text;
using PgWorker.PgNode.BaseBackup;

namespace PgWorker.UnitTests.PgNode;

// Автомат разбора BASE_BACKUP (spec t01 §3.1): заголовок tablespace list,
// COPY OUT потоки (основной + pg_wal), итог; отказные ветки — server error,
// copy fail, чужие сообщения, несколько tablespace.
public class BaseBackupCommandTests
{
    // AAA: основной поток + pg_wal читаются раздельно, CommandComplete завершает.
    [Fact]
    public async Task ReadsTwoCopyStreams_ThenCompletes()
    {
        // Arrange
        var fake = new FakeSource(
            Message('T'), Message('D'), Message('H'),
            Message('d', "abc"), Message('d', "def"), Message('c'),
            Message('H'), Message('d', "wal"), Message('c'),
            Message('D'), Message('C'));
        var command = new BaseBackupCommand(fake);
        var streams = new List<string>();

        // Act
        await command.RunAsync("id1", async (index, stream, ct) =>
        {
            using var buffer = new MemoryStream();
            await stream.CopyToAsync(buffer, ct);
            streams.Add($"{index}:{Encoding.ASCII.GetString(buffer.ToArray())}");
        }, CancellationToken.None);

        // Assert
        streams.Should().Equal("0:abcdef", "1:wal");
        fake.Sent.Should().ContainSingle()
            .Which.Should().Contain("LABEL 'pgw-full-id1'")
            .And.Contain("TARGET 'CLIENT'")
            .And.Contain("WAL 'STREAM'");
    }

    // AAA: более одного DataRow заголовка (второй tablespace) — fail-fast.
    [Fact]
    public async Task MultipleHeaderRows_Throws()
    {
        // Arrange
        var fake = new FakeSource(Message('T'), Message('D'), Message('D'), Message('H'));
        var command = new BaseBackupCommand(fake);

        // Act
        var act = () => command.RunAsync("id1", (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        var thrown = await act.Should().ThrowAsync<ApplicationException>();
        thrown.Which.Message.Should().Be("multiple tablespaces not supported");
    }

    // AAA: третий COPY OUT поток — fail-fast multiple tablespaces.
    [Fact]
    public async Task ThirdCopyStream_Throws()
    {
        // Arrange
        var fake = new FakeSource(
            Message('D'), Message('H'), Message('c'),
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
        var fake = new FakeSource(Message('D'), Message('H'), Message('d', "abc"), Message('f', "disk full"));
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

    private static PgMessage Message(char type, string payload = "")
        => new((byte)type, Encoding.ASCII.GetBytes(payload));

    private static PgMessage Message(char type, byte[] payload)
        => new((byte)type, payload);

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
