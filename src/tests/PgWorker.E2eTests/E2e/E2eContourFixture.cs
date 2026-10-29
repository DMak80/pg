namespace PgWorker.IntegrationTests.E2e;

/// <summary>База класс-фикстур PgWorker E2E (docs/e2e-isolation.md §1): слот
/// E2eParallelismGuard ДО первого docker-объекта, затем полный контур класса
/// (сеть/etcd/ownS3/CA/окно портов — E2eEnvironment.StartAsync); teardown
/// при любом исходе в DisposeAsync. Единица окружения — тестовый класс.</summary>
public abstract class E2eContourFixture : IAsyncLifetime
{
    protected abstract string Slug { get; }
    protected virtual bool WithS3Node => false;
    protected virtual bool HaEtcd => false;

    public E2eEnvironment Fx { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        E2eParallelismGuard.Acquire(Slug, ct);
        try
        {
            Fx = await E2eEnvironment.StartAsync(Slug, WithS3Node, HaEtcd, ct);
        }
        catch
        {
            E2eParallelismGuard.Release(Slug);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Слот Guard'а освобождается ПРИ ЛЮБОМ исходе teardown'а (канон §3.9):
        // исключение DisposeAsync окружения (ассерт чистоты и пр.) не должно
        // утекать слот — иначе серия исчерпывает Guard и навечно висит в
        // Acquire последующих классов. Исход teardown'а — наружу.
        if (Fx is null)
            return; // провал подъёма: слот Release'нут в catch InitializeAsync —
                    // второй Release здесь переполняет Gate (SemaphoreFullException)
        try
        {
            await Fx.DisposeAsync();
        }
        finally
        {
            E2eParallelismGuard.Release(Slug);
        }
    }

    /// <summary>Пометка упавшего сценария (stop-режим teardown'а).</summary>
    public void MarkFailed() => Fx?.MarkFailed();
}
