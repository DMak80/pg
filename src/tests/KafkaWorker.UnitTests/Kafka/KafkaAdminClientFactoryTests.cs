using System.Diagnostics;
using Confluent.Kafka;
using KafkaWorker.Core;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.UnitTests.Provisioning;

namespace KafkaWorker.UnitTests.Kafka;

// Кэш AdminClient'ов (t05, spec §3.1): sharable-фабрика per
// (bootstrap,user,password) — «клиент на тик» давал churn rd_kafka-инстансов
// и 100% ядра на лежащем кластере (инцидент as-kafkaworker 2026-09-04).
// Адаптер строит нативный клиент лениво (первая операция) — юниты кэша
// работают без сети.
public class KafkaAdminClientFactoryTests
{
    // AAA: reuse по ключу — тот же адаптер, счётчик не растёт.
    [Fact]
    public void Create_SameKey_ReturnsSameClient()
    {
        // Arrange: фабрика без натива (клиент ленивый).
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));

        // Act: два Create по одному ключу.
        var first = factory.Create("h:9092", "app", "pw", caPem: null);
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: тот же адаптер, клиент создан один.
        second.Should().BeSameAs(first);
        factory.CreatedClients.Should().Be(1);
    }

    // AAA: смена endpoints/кредов = другой ключ = другой клиент.
    [Fact]
    public void Create_DifferentCredentials_DifferentClient()
    {
        // Arrange: фабрика.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));

        // Act: три разных ключа.
        var a = factory.Create("h:9092", "app", "pw1", caPem: null);
        var b = factory.Create("h:9092", "app", "pw2", caPem: null);
        var c = factory.Create("h2:9092", "app", "pw1", caPem: null);

        // Assert: каждый ключ — свой клиент.
        b.Should().NotBeSameAs(a);
        c.Should().NotBeSameAs(a);
        factory.CreatedClients.Should().Be(3);
    }

    // AAA: пины librdkafka в конфиге (reconnect-шторм дефолтных 100 мс)
    // + t03-канон соединения: SASL_SSL, per-cluster CA — только при caPem.
    // t07: доверие ФАЙЛОМ ssl.ca.location (inline ssl.ca.pem у librdkafka
    // читает один серт — bundle окна ротации CA требует файла).
    [Fact]
    public void BaseAdminConfig_PinsLibrdkafkaBackoffs()
    {
        // Act: конфиг фабрики (без CA и с CA).
        var config = KafkaAdminClientFactory.BaseAdminConfig("h:9092", "app", "pw", caPem: null);
        var withCa = KafkaAdminClientFactory.BaseAdminConfig("h:9092", "app", "pw", "-----BEGIN CERTIFICATE-----");

        // Assert: bootstrap + пины backoff ≥1000 мс; SASL_SSL всегда;
        // ssl.ca.location ставится ровно при заданном caPem (файловый кеш).
        config.BootstrapServers.Should().Be("h:9092");
        config.RetryBackoffMs.Should().Be(1000);
        config.ReconnectBackoffMs.Should().Be(1000);
        config.ReconnectBackoffMaxMs.Should().Be(10000);
        config.SecurityProtocol.Should().Be(SecurityProtocol.SaslSsl);
        config.Get("ssl.ca.location").Should().BeNull();
        var location = withCa.Get("ssl.ca.location");
        location.Should().NotBeNull().And.StartWith(Path.GetTempPath());
        File.Exists(location).Should().BeTrue("файл truststore создаётся при построении конфига");
        File.ReadAllText(location!).Should().Contain("-----BEGIN CERTIFICATE-----");
    }

    // AAA: caPem — часть ключа кэша (t03×t05): клиенты кластеров с разным
    // per-cluster CA не шарятся (разный TLS-контекст нативного клиента).
    [Fact]
    public void Create_DifferentCaPem_DifferentClient()
    {
        // Arrange: фабрика.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));

        // Act: один bootstrap/креды, разные CA.
        var a = factory.Create("h:9092", "app", "pw", caPem: null);
        var b = factory.Create("h:9092", "app", "pw", caPem: "-----BEGIN CERTIFICATE-----");

        // Assert: другой CA — другой клиент.
        b.Should().NotBeSameAs(a);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: вытеснение по неактивности (FixedTimeProvider — сдвиг на 11 мин).
    [Fact]
    public void Create_EvictsIdleEntries()
    {
        // Arrange: фабрика на управляемых часах.
        var clock = new FixedTimeProvider();
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3), clock: clock);
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: время дальше окна неактивности + повторный Create.
        clock.Utc += KafkaAdminClientFactory.IdleEvictAfter + TimeSpan.FromMinutes(1);
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: запись вытеснена — построен новый клиент.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: активный ключ не вытесняется (LastUsed обновляется на Create).
    [Fact]
    public void Create_ActiveKey_NotEvicted()
    {
        // Arrange: фабрика на управляемых часах.
        var clock = new FixedTimeProvider();
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3), clock: clock);
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: два промежуточных тика внутри окна неактивности.
        clock.Utc += TimeSpan.FromMinutes(5);
        factory.Create("h:9092", "app", "pw", caPem: null);
        clock.Utc += TimeSpan.FromMinutes(5);
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: ключ жив — клиент тот же, пересозданий нет.
        second.Should().BeSameAs(first);
        factory.CreatedClients.Should().Be(1);
    }

    // AAA: Failed операции помечает запись Unhealthy — следующий Create
    // отдаёт свежий клиент (internal NotifyFailed — тот же путь, что зовёт
    // RunAsync при исключении; сети/натива нет — клиент ленивый).
    [Fact]
    public void Create_AfterFailure_CreatesFreshClient()
    {
        // Arrange: клиент в кэше.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: помечаем операцию Failed (путь RunAsync при исключении).
        ((KafkaAdminClient)first).NotifyFailed();
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: unhealthy-запись заменена свежим клиентом.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }

    // AAA: отмена host'а не инвалидирует (IsHostCancellation — условие
    // пометки в RunAsync): OCE при отменённом токене — да; OCE без отмены
    // (чужой cancellation) и обычные исключения — нет.
    [Theory]
    [InlineData(true, true)]   // OCE + ct.IsCancellationRequested → не фейл-пометка
    [InlineData(false, false)] // OCE без отмены → фейл-пометка
    public void IsHostCancellation_Classifies(bool cancelled, bool expected)
    {
        // Arrange: токен в состоянии отмены/без.
        using var cts = new CancellationTokenSource();
        if (cancelled)
            cts.Cancel();

        // Act + Assert: классификация отмены host'а.
        KafkaAdminClient.IsHostCancellation(new OperationCanceledException(), cts.Token)
            .Should().Be(expected);
        KafkaAdminClient.IsHostCancellation(new ApplicationException("down"), cts.Token)
            .Should().BeFalse();
    }

    // AAA: Dispose (shutdown) детерминированно вычищает кэш — повторный
    // Create строит новый клиент (CreatedClients +1), старый Disposeится.
    [Fact]
    public void Dispose_ThenCreate_BuildsFreshClient()
    {
        // Arrange: клиент в кэше.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = factory.Create("h:9092", "app", "pw", caPem: null);

        // Act: shutdown фабрики + повторный Create.
        factory.Dispose();
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: кэш пуст — построен свежий клиент.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
    }

    // ===== Дрейн-dispose заменяемого (arch/16 §6): dispose под висящей
    // операцией запрещён (ERR__DESTROYED) — тесты через шов
    // RunUnderOperationAsync (нативный клиент не строится). =====

    // AAA: замена Unhealthy при висящей операции НЕ диспозит клиент — dispose
    // ждёт завершения операции (дрейн).
    [Fact]
    public async Task UnhealthyReplace_DoesNotDisposeUnderActiveOperation()
    {
        // Arrange: клиент в кэше; операция висит (TCS управляет завершением).
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = (KafkaAdminClient)factory.Create("h:9092", "app", "pw", caPem: null);
        var gate = new TaskCompletionSource<Result>();
        var operation = first.RunUnderOperationAsync(_ => gate.Task, TestContext.Current.CancellationToken);
        first.ActiveOperations.Should().Be(1, "операция на счётчике");

        // Act: пометка Failed + замена (фоновый дрейн обязан ждать нуля).
        first.NotifyFailed();
        var second = factory.Create("h:9092", "app", "pw", caPem: null);

        // Assert: замена мгновенна (новый клиент), dispose НЕ исполнился —
        // операция первого всё ещё висит.
        second.Should().NotBeSameAs(first);
        factory.CreatedClients.Should().Be(2);
        first.DisposeNativeCount.Should().Be(0, "dispose под висящей операцией запрещён");

        // Cleanup: отпустить операцию — фоновый дрейн завершится сам.
        gate.TrySetResult(Result.Success());
        await operation;
    }

    // AAA: дрейн исполняет dispose по завершении последней операции
    // (continuation), не дожидаясь бюджета.
    [Fact]
    public async Task DisposeWhenIdle_WaitsForLastOperation_ThenDisposes()
    {
        // Arrange: клиент (вне фабричной замены — наблюдаем детерминированно);
        // дрейн запущен при висящей операции.
        var client = new KafkaAdminClient("h:9092", "app", "pw", caPem: null, TimeSpan.FromSeconds(3));
        var gate = new TaskCompletionSource<Result>();
        var operation = client.RunUnderOperationAsync(_ => gate.Task, TestContext.Current.CancellationToken);
        var drain = client.DisposeWhenIdleAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await Task.Delay(200, TestContext.Current.CancellationToken);
        drain.IsCompleted.Should().BeFalse("дрейн ждёт висящую операцию");

        // Act: операция завершается.
        gate.SetResult(Result.Success());
        await operation;
        await drain;

        // Assert: dispose исполнен по нулю операций — без исчерпания бюджета.
        client.DisposeNativeCount.Should().Be(1);
        client.ActiveOperations.Should().Be(0);
    }

    // AAA: бюджет дрейна — по исчерпании принудительный dispose (не ждать
    // вечно), операция при этом остаётся при своём исходе.
    [Fact]
    public async Task DisposeWhenIdle_BudgetExhausted_ForcesDispose()
    {
        // Arrange: операция висит дольше бюджета.
        var client = new KafkaAdminClient("h:9092", "app", "pw", caPem: null, TimeSpan.FromSeconds(3));
        var gate = new TaskCompletionSource<Result>();
        var operation = client.RunUnderOperationAsync(_ => gate.Task, TestContext.Current.CancellationToken);

        // Act: дрейн с коротким бюджетом.
        var drain = client.DisposeWhenIdleAsync(TimeSpan.FromMilliseconds(300), TestContext.Current.CancellationToken);
        await drain;

        // Assert: принудительный dispose по бюджету; операция не сорвана
        // исключением — завершится своим исходом.
        client.DisposeNativeCount.Should().Be(1, "бюджет исчерпан — принудительный dispose");

        // Cleanup.
        gate.TrySetResult(Result.Success());
        var done = await operation;
        done.IsSuccess.Should().BeTrue("операция завершилась своим исходом, не ERR__DESTROYED");
    }

    // AAA: Create не блокируется дрейном заменяемого (раздача новых клиентов
    // жива, пока старый дрейнит).
    [Fact]
    public async Task Create_NotBlockedByDrainOfReplaced()
    {
        // Arrange: заменяемый клиент с висящей операцией (дрейн ждёт).
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = (KafkaAdminClient)factory.Create("h:9092", "app", "pw", caPem: null);
        var gate = new TaskCompletionSource<Result>();
        var operation = first.RunUnderOperationAsync(_ => gate.Task, TestContext.Current.CancellationToken);
        first.NotifyFailed();
        factory.Create("h:9092", "app", "pw", caPem: null); // замена запустила фоновый дрейн

        // Act: новые Create по ДРУГОМУ ключу и по ключу заменённого — замер.
        var sw = Stopwatch.StartNew();
        var fresh = factory.Create("h2:9092", "app", "pw", caPem: null);
        var again = factory.Create("h2:9092", "app", "pw", caPem: null);
        sw.Stop();

        // Assert: раздача мгновенна (не ждёт дрейна), reuse нового ключа жив.
        sw.ElapsedMilliseconds.Should().BeLessThan(3000, "Create не блокируется дрейном");
        again.Should().BeSameAs(fresh);

        // Cleanup.
        gate.TrySetResult(Result.Success());
        await operation;
    }

    // AAA: Dispose фабрики при висящей операции дожидается её завершения
    // (дрейн с позитивным исходом — без принуждения) и детерминированно
    // возвращается.
    [Fact]
    public async Task FactoryDispose_DrainsActiveOperation_Deterministically()
    {
        // Arrange: клиент с операцией, освобождающейся через 200 мс.
        var factory = new KafkaAdminClientFactory(TimeSpan.FromSeconds(3));
        var first = (KafkaAdminClient)factory.Create("h:9092", "app", "pw", caPem: null);
        var gate = new TaskCompletionSource<Result>();
        var operation = first.RunUnderOperationAsync(
            async _ =>
            {
                await Task.Delay(200);
                return await gate.Task;
            }, TestContext.Current.CancellationToken);

        // Act: остановка фабрики (внутренний seam — короткий бюджет теста).
        var disposeTask = Task.Run(
            () => factory.Dispose(TimeSpan.FromSeconds(5)), TestContext.Current.CancellationToken);
        gate.TrySetResult(Result.Success());
        var finished = await Task.WhenAny(
            disposeTask, Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)) == disposeTask;

        // Assert: Dispose вернулся (не висит) — дождался операции, без
        // принуждения бюджет не потребовался.
        finished.Should().BeTrue("Dispose детерминирован: дрейн с бюджетом, не вечное ожидание");
        first.DisposeNativeCount.Should().Be(1, "клиент задиспожен по завершении операции");
        await operation;
    }

    // ===== Гэп дрейна: операция в окне «ноль счётчика → dispose» =====

    // AAA: после dispose вход операций закрыт — новый вызов получает
    // управляемый транзиентный отказ (НЕ unhealthy-фейл клиента) и не
    // уходит на уничтоженный нативный хендл.
    [Fact]
    public async Task DisposedClient_NewOperation_RefusedWithTransientError()
    {
        // Arrange: клиент без операций — дрейн завершён, dispose исполнен.
        var client = new KafkaAdminClient("h:9092", "app", "pw", caPem: null, TimeSpan.FromSeconds(3));
        await client.DisposeWhenIdleAsync(TimeSpan.Zero, TestContext.Current.CancellationToken);
        client.DisposeNativeCount.Should().Be(1, "операций нет — dispose немедленно");

        // Act: операция на уже диспознутом клиенте.
        var refused = await client.RunUnderOperationAsync(
            _ => Task.FromResult(Result.Success()), TestContext.Current.CancellationToken);

        // Assert: транзиентный отказ без старта — счётчик не тронут.
        refused.IsSuccess.Should().BeFalse("вход операций закрыт флагом dispose");
        refused.Error!.Message.Should().Contain("клиент в dispose",
            "отказ управляемый (повтор даст свежий клиент), не сбой клиента");
        client.ActiveOperations.Should().Be(0, "операция не вставала на счётчик");
    }

    // AAA: операция, входящая в гэп «дрейн дождался нуля → dispose», либо
    // не стартует (транзиентный отказ), либо спокойно доигрывает (дрейн в
    // цикле дождётся её нуля) — исход ERR__DESTROYED под операцией исключён,
    // клиент в итоге диспозится ровно один раз.
    [Fact]
    public async Task DisposeWhenIdle_OperationEnteringGap_NotDestroyedAndClientDisposed()
    {
        // Arrange: висящая операция + запущенный дрейн (ждёт нуля).
        var client = new KafkaAdminClient("h:9092", "app", "pw", caPem: null, TimeSpan.FromSeconds(30));
        var gate = new TaskCompletionSource<Result>();
        var operation = client.RunUnderOperationAsync(_ => gate.Task, TestContext.Current.CancellationToken);
        var drain = client.DisposeWhenIdleAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        // Act: отпускаем операцию — дрейн проснётся по нулю; в его гэп
        // (continuation в очереди пула) стартуем «опоздавшую» операцию.
        gate.TrySetResult(Result.Success());
        var late = await client.RunUnderOperationAsync(
            _ => Task.FromResult(Result.Success()), TestContext.Current.CancellationToken);
        await operation;
        await drain;

        // Assert: опоздавшая операция НЕ уничтожена — либо успех (встала на
        // счётчик до dispose, дрейн дождался), либо транзиентный отказ; клиент
        // после гэпа всё же диспозится, счётчик чист.
        (late.IsSuccess || late.Error!.Message.Contains("клиент в dispose")).Should().BeTrue(
            $"исход — успех или управляемый транзиент, не ERR__DESTROYED; фактически: " +
            $"{(late.IsSuccess ? "success" : late.Error!.Message)}");
        client.DisposeNativeCount.Should().Be(1, "клиент после гэпа всё же диспозится");
        client.ActiveOperations.Should().Be(0, "висящих операций не осталось");
    }
}
