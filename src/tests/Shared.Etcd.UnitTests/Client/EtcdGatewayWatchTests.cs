using System.Net;
using System.Text;
using System.Text.Json;

namespace Shared.Etcd.UnitTests;

// Watch-стрим gateway (/v3/watch): запрос create_request (base64 + start_revision),
// потоковое чтение фреймов grpc-gateway, события по одному колбэку, исходы
// Compacted/Closed/Cancelled.
public class EtcdGatewayWatchTests
{
    // Управляемый транспорт: перехватывает запросы и отвечает заготовленным ответом.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public readonly List<(string Url, string Body)> Requests = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.RequestUri!.ToString(), await request.Content!.ReadAsStringAsync(ct)));
            return responder(request);
        }
    }

    // Ответ-поток: тело отдаётся стримом (WatchAsync читает его пофреймово).
    private static HttpResponseMessage Stream(string body) => new()
    {
        StatusCode = HttpStatusCode.OK,
        Content = new StreamContent(new MemoryStream(Encoding.UTF8.GetBytes(body))),
    };

    // Поток без данных: чтение висит до отмены (имитация открытой подписки).
    private sealed class BlockingStream : Stream
    {
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct)
        {
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
            }

            return 0;
        }
    }

    private static string B64(string value) => Convert.ToBase64String(Encoding.UTF8.GetBytes(value));

    [Fact]
    public async Task Watch_Request_HasBase64KeyRangeEndAndStartRevision()
    {
        // Arrange — поток без событий (сервер сразу закрыл соединение)
        var handler = new FakeHandler(_ => Stream("{\"result\":{\"created\":true}}\n"));
        var gateway = new EtcdGateway(new HttpClient(handler));

        // Act
        var result = await gateway.WatchAsync(
            "http://etcd:2379", "/p/", 42, _ => Task.CompletedTask, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Reason.Should().Be(EtcdWatchStopReason.Closed);
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/watch");
        var body = JsonDocument.Parse(request.Body).RootElement;
        var create = body.GetProperty("create_request");
        create.GetProperty("key").GetString().Should().Be("L3Av"); // base64("/p/")
        create.GetProperty("range_end").GetString().Should().Be("L3Aw"); // base64("/p0")
        create.GetProperty("start_revision").GetInt64().Should().Be(42);
    }

    [Fact]
    public async Task Watch_Events_StreamedPerFrame_DeliveredPerEvent()
    {
        // Arrange — два фрейма; в первом два события (PUT + DELETE), во втором один;
        // mod_revision — decimal-строка (protojson int64)
        var body =
            "{\"result\":{\"events\":[" +
            "{\"type\":\"PUT\",\"kv\":{\"key\":\"" + B64("/p/a") + "\",\"value\":\"" + B64("v1") + "\",\"mod_revision\":\"5\"}}," +
            "{\"type\":\"DELETE\",\"kv\":{\"key\":\"" + B64("/p/b") + "\",\"mod_revision\":\"6\"}}]}}\n" +
            "{\"result\":{\"events\":[{\"type\":\"PUT\",\"kv\":{\"key\":\"" + B64("/p/c") + "\",\"value\":\"" + B64("v2") + "\",\"mod_revision\":\"7\"}}]}}\n";
        var handler = new FakeHandler(_ => Stream(body));
        var gateway = new EtcdGateway(new HttpClient(handler));
        var events = new List<EtcdWatchEvent>();
        Task OnEvent(EtcdWatchEvent e)
        {
            events.Add(e);
            return Task.CompletedTask;
        }

        // Act
        var result = await gateway.WatchAsync("http://etcd:2379", "/p/", 1, OnEvent, CancellationToken.None);

        // Assert — каждое событие отдельным колбэком; у DELETE value = null
        result.IsSuccess.Should().BeTrue();
        result.Value.Reason.Should().Be(EtcdWatchStopReason.Closed);
        events.Should().HaveCount(3);
        events[0].Key.Should().Be("/p/a");
        events[0].Value.Should().Be("v1");
        events[0].ModRevision.Should().Be(5);
        events[0].Type.Should().Be(EtcdWatchEventType.Put);
        events[1].Key.Should().Be("/p/b");
        events[1].Value.Should().BeNull();
        events[1].Type.Should().Be(EtcdWatchEventType.Delete);
        events[2].Key.Should().Be("/p/c");
        events[2].Value.Should().Be("v2");
        events[2].ModRevision.Should().Be(7);
    }

    [Fact]
    public async Task Watch_SkipServiceFramesWithoutKv()
    {
        // Arrange — служебные фреймы (created/progress_notify) без events/kv — не события
        var body =
            "{\"result\":{\"created\":true}}\n" +
            "{\"result\":{}}\n" +
            "{\"result\":{\"events\":[{\"type\":\"PUT\",\"kv\":{\"key\":\"" + B64("/p/a") + "\",\"value\":\"" + B64("v") + "\",\"mod_revision\":\"3\"}}]}}\n";
        var handler = new FakeHandler(_ => Stream(body));
        var gateway = new EtcdGateway(new HttpClient(handler));
        var events = new List<EtcdWatchEvent>();
        Task OnEvent(EtcdWatchEvent e)
        {
            events.Add(e);
            return Task.CompletedTask;
        }

        // Act
        var result = await gateway.WatchAsync("http://etcd:2379", "/p/", 1, OnEvent, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        events.Should().ContainSingle();
        events[0].Key.Should().Be("/p/a");
    }

    [Fact]
    public async Task Watch_Compacted_ReturnsCompactedStopWithRevision()
    {
        // Arrange — canceled=true + compact_revision: история недоступна
        var handler = new FakeHandler(_ => Stream(
            "{\"result\":{\"compact_revision\":\"7\",\"canceled\":true,\"cancel_reason\":\"mvcc: required revision has been compacted\"}}\n"));
        var gateway = new EtcdGateway(new HttpClient(handler));

        // Act
        var result = await gateway.WatchAsync(
            "http://etcd:2379", "/p/", 2, _ => Task.CompletedTask, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Reason.Should().Be(EtcdWatchStopReason.Compacted);
        result.Value.CompactRevision.Should().Be(7);
    }

    [Fact]
    public async Task Watch_HttpError_ReturnsFailed()
    {
        // Arrange — 500 от etcd
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = new StringContent("boom"),
        });
        var gateway = new EtcdGateway(new HttpClient(handler));

        // Act
        var result = await gateway.WatchAsync(
            "http://etcd:2379", "/p/", 1, _ => Task.CompletedTask, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<EtcdHttpException>();
    }

    [Fact]
    public async Task Watch_Cancelled_ReturnsCancelledStop()
    {
        // Arrange — подписка, которая не отвечает до отмены (долгоживущий поток)
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(new BlockingStream()),
        });
        var gateway = new EtcdGateway(new HttpClient(handler));
        using var cts = new CancellationTokenSource();

        // Act
        var watch = gateway.WatchAsync("http://etcd:2379", "/p/", 1, _ => Task.CompletedTask, cts.Token);
        cts.Cancel();
        var result = await watch;

        // Assert — отмена потребителя: штатный исход, не ошибка
        result.IsSuccess.Should().BeTrue();
        result.Value.Reason.Should().Be(EtcdWatchStopReason.Cancelled);
    }
}
