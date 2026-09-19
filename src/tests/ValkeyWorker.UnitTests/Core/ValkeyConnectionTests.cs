using System.Text;
using FluentAssertions;
using ValkeyWorker.Core.Valkey;
using Xunit;

namespace ValkeyWorker.UnitTests.Core;

// RESP-миниклиент (spec §4.3): фрейминг запросов (AUTH-массив), разбор кадров
// сервера (простые/ошибки/целые/bulk/массивы, куски), команды AUTH/PING/CONFIG/
// ACL, таймаут — на эфемерной TCP-заглушке (фактический порт — литералов нет).
public class ValkeyConnectionTests
{
    private static readonly TimeSpan FastTimeout = TimeSpan.FromMilliseconds(700);

    private static ValkeyEndpoint Ep(int port) => new("127.0.0.1", port, "admin", "AdminPassword0123456789ab");

    private static string AssertAuthFrame(byte[] received)
    {
        // Фрейминг: AUTH идёт первым (до команды) — REP-массив.
        var text = Encoding.UTF8.GetString(received);
        text.Should().StartWith("*3\r\n$4\r\nAUTH\r\n");
        return text;
    }

    [Fact]
    public async Task Ping_УспехКадрПлюсПонг()
    {
        // Arrange: сервер отвечает +OK на AUTH и +PONG на PING.
        await using var stub = RespStub.Start("+OK\r\n", "+PONG\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.PingAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

        // Assert: PONG — успех.
        result.IsSuccess.Should().BeTrue();
        var text = AssertAuthFrame(stub.Received);
        text.Should().Contain("$4\r\nPING\r\n");
    }

    [Fact]
    public async Task Auth_ОтказWrongPass_FailedБезКоманды()
    {
        // Arrange: сервер отвергает AUTH.
        await using var stub = RespStub.Start("-WRONGPASS invalid username-password pair\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.PingAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

        // Assert: Failed с сообщением ошибки (не исключение, S7).
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("WRONGPASS");
    }

    [Fact]
    public async Task ConfigGet_МассивОтвета_Словарь()
    {
        // Arrange: RESP2 CONFIG GET — плоский массив пар [key, value].
        await using var stub = RespStub.Start("+OK\r\n", "*2\r\n$9\r\nmaxmemory\r\n$9\r\n536870912\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.ConfigGetAsync(Ep(stub.Port), "maxmemory", TestContext.Current.CancellationToken);

        // Assert: словарь {"maxmemory":"536870912"}.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().ContainKey("maxmemory").WhoseValue.Should().Be("536870912");
    }

    [Fact]
    public async Task ConfigSet_ПлюсОк_Успех()
    {
        // Arrange
        await using var stub = RespStub.Start("+OK\r\n", "+OK\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.ConfigSetAsync(Ep(stub.Port), "maxmemory", "536870912", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var text = Encoding.UTF8.GetString(stub.Received);
        text.Should().Contain("*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\nmaxmemory\r\n$9\r\n536870912\r\n");
    }

    [Fact]
    public async Task AclList_МассивСтрок()
    {
        // Arrange: два правила ACL.
        await using var stub = RespStub.Start("+OK\r\n", "*2\r\n$31\r\nuser default on nopass ~* +@all\r\n$28\r\nuser admin on #hash ~* +@all\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.AclListAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

        // Assert: массив строк правил.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().HaveCount(2);
        result.Value[0].Should().Contain("user default");
        result.Value[1].Should().Contain("user admin");
    }

    [Fact]
    public async Task AclSetUser_ПлюсОк_ФреймАргументов()
    {
        // Arrange: ротация E1 — ACL SETUSER app >NEW ~* +@read +@write.
        await using var stub = RespStub.Start("+OK\r\n", "+OK\r\n");
        var conn = new ValkeyConnection(FastTimeout);
        var args = new[] { "app", ">NewPassword0123456789abcdefgh", "~*", "+@read", "+@write" };

        // Act
        var result = await conn.AclSetUserAsync(Ep(stub.Port), args, TestContext.Current.CancellationToken);

        // Assert: +OK; фрейм — RESP-массив AUTH + ACL SETUSER с аргументами.
        result.IsSuccess.Should().BeTrue();
        var text = Encoding.UTF8.GetString(stub.Received);
        text.Should().Contain("*7\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$3\r\napp\r\n");
        text.Should().Contain("$2\r\n~*\r\n");
    }

    [Fact]
    public async Task Chunked_КадрВДваЗахода_КлиентДочитывает()
    {
        // Arrange: сервер пишет ответ частями (80 мс пауза внутри кадра).
        await using var stub = RespStub.StartChunked("+OK\r\n", "+PONG\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act: AUTH (+OK) и PING (+PONG, chunked) в одном соединении.
        var result = await conn.PingAsync(new ValkeyEndpoint("127.0.0.1", stub.Port, "admin", "pwd"), TestContext.Current.CancellationToken);

        // Assert: дочитал недописанный кадр.
        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task NullBulk_МинусОдин_РазбираетсяБезИсключения()
    {
        // Arrange: ответ массивом с $-1 (bulk null) — ACL LIST пустого? Здесь:
        // CONFIG GET отвечает [$5\r\nhello\r\n]-стилем + $-1 элемент.
        await using var stub = RespStub.Start("+OK\r\n", "*2\r\n$5\r\nhello\r\n$-1\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.ConfigGetAsync(Ep(stub.Port), "x", TestContext.Current.CancellationToken);

        // Assert: null-элемент не валит разбор (пустое значение).
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().ContainKey("hello");
    }

    [Fact]
    public async Task Таймаут_ЗаглушкаМолчит_Failed()
    {
        // Arrange: сервер принимает соединение, но не отвечает.
        await using var stub = RespStub.Silent();
        var conn = new ValkeyConnection(TimeSpan.FromMilliseconds(300));

        // Act
        var result = await conn.PingAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

        // Assert: Failed (бюджет исчерпан — это проба, не ожидание).
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task СоединениеОтвергнуто_FailedБезИсключения()
    {
        // Arrange: порт, где никто не слушает (зонд: слушаем 0 → закрываем).
        var probe = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        probe.Start();
        var port = ((System.Net.IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.PingAsync(Ep(port), TestContext.Current.CancellationToken);

        // Assert: сетевой отказ → Failed (S7), не исключение.
        result.IsSuccess.Should().BeFalse();
    }

    // ── Юнит парсера кадров (чистый, без TCP): все типы кадров ──

    private static object? ParseOne(string frame)
    {
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(frame));
        return ValkeyConnection.Resp.ReadReplyAsync(stream, TestContext.Current.CancellationToken).GetAwaiter().GetResult();
    }

    [Fact]
    public void Парсер_SimpleString()
    {
        // Arrange/Act/Assert: +PONG → строка PONG.
        ParseOne("+PONG\r\n").Should().Be("PONG");
    }

    [Fact]
    public void Парсер_Error_ТипRespError()
    {
        // Arrange/Act
        var parsed = ParseOne("-ERR unknown command\r\n");

        // Assert: ошибка — отдельный тип (клиент переведёт в Failed).
        parsed.Should().BeOfType<ValkeyConnection.RespError>()
            .Which.Message.Should().Contain("unknown command");
    }

    [Fact]
    public void Парсер_ЦелоеЧисло()
    {
        // Arrange/Act/Assert: :1 → long 1.
        ParseOne(":1\r\n").Should().Be(1L);
    }

    [Fact]
    public void Парсер_BulkString()
    {
        // Arrange/Act/Assert: $5 hello → строка hello.
        ParseOne("$5\r\nhello\r\n").Should().Be("hello");
    }

    [Fact]
    public void Парсер_BulkNull()
    {
        // Arrange/Act/Assert: $-1 → null.
        ParseOne("$-1\r\n").Should().BeNull();
    }

    [Fact]
    public void Парсер_Массив()
    {
        // Arrange/Act
        var parsed = ParseOne("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        // Assert: список из двух строк.
        var list = parsed.Should().BeOfType<List<object?>>().Subject;
        list.Should().Equal("foo", "bar");
    }

    // ── INFO all: парсер + команда (t05, arch/18 §4.2) ──

    // AAA: заголовки секций «# Name», пустые строки и \r\n — парсер даёт плоский словарь.
    [Fact]
    public void ParseInfo_СекцииПустыеСтроки_ПлоскийСловарь()
    {
        // Arrange: типовой фрагмент INFO all с тремя секциями.
        var bulk = "# Server\r\nredis_version:9.1.2\r\nredis_mode:standalone\r\n\r\n" +
                   "# Memory\r\nused_memory:1048576\r\nmaxmemory:536870912\r\n\r\n" +
                   "# Keyspace\r\n";

        // Act
        var info = ValkeyConnection.ParseInfo(bulk);

        // Assert: k:v-пары собраны; заголовки/пустые строки не попали.
        info["redis_version"].Should().Be("9.1.2");
        info["used_memory"].Should().Be("1048576");
        info["maxmemory"].Should().Be("536870912");
        info.Should().HaveCount(4);
        info.Should().NotContainKey("# Server");
    }

    // AAA: нечисловые значения хранятся строками (db0 из Keyspace) — выбор чисел за коллектором.
    [Fact]
    public void ParseInfo_НечисловыеЗначения_ХранятсяСтроками()
    {
        // Arrange
        var bulk = "# Keyspace\r\ndb0:keys=3,expires=0,avg_ttl=0\r\nrole:master\r\n";

        // Act
        var info = ValkeyConnection.ParseInfo(bulk);

        // Assert
        info["db0"].Should().Be("keys=3,expires=0,avg_ttl=0");
        info["role"].Should().Be("master");
    }

    // AAA: битый ввод — строки без «ключа», «:значение», пустой bulk — мусор пропускается,
    // пустой ввод → пустой словарь (не исключение).
    [Fact]
    public void ParseInfo_МусорныеСтроки_Пропущены()
    {
        // Arrange
        var bulk = "без-разделителя\r\n:значение-без-ключа\r\nused_memory:1\r\n";

        // Act
        var info = ValkeyConnection.ParseInfo(bulk);

        // Assert: валидная пара сохранена, мусор пропущен.
        info.Should().HaveCount(1);
        info["used_memory"].Should().Be("1");
        ValkeyConnection.ParseInfo("").Should().BeEmpty();
    }

    // AAA: INFO all по проводу — bulk-кадр; фрейминг команды «INFO all» после AUTH.
    [Fact]
    public async Task InfoAll_BulkОтвет_СловарьПолей()
    {
        // Arrange: полный INFO-ответ (bulk), включающий Replication.
        var body = "# Memory\r\nused_memory:1048576\r\nmaxmemory:536870912\r\n" +
                   "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n";
        var bulk = $"${Encoding.UTF8.GetByteCount(body)}\r\n{body}\r\n";
        await using var stub = RespStub.Start("+OK\r\n", bulk);
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.InfoAllAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

        // Assert: словарь полей; фрейминг — RESP-массив INFO all.
        result.IsSuccess.Should().BeTrue();
        result.Value!["used_memory"].Should().Be("1048576");
        result.Value!["role"].Should().Be("master");
        var text = AssertAuthFrame(stub.Received);
        text.Should().Contain("$4\r\nINFO\r\n$3\r\nall\r\n");
    }

    // AAA: ошибка сервера на INFO → Result.Failed (S7: проба — не исключение).
    [Fact]
    public async Task InfoAll_ОшибкаСервера_Failed()
    {
        // Arrange
        await using var stub = RespStub.Start("+OK\r\n", "-ERR unknown command\r\n");
        var conn = new ValkeyConnection(FastTimeout);

        // Act
        var result = await conn.InfoAllAsync(Ep(stub.Port), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("ERR");
    }
}
