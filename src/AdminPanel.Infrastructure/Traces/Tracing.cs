using System.Diagnostics;

namespace AdminPanel.Infrastructure.Traces;

public static class Tracing
{
    private static ActivitySource ActivitySource = null!;

    public static void Init(string traceSourceName)
    {
        ActivitySource = new ActivitySource(traceSourceName);
    }

    public static T Activity<T>(string name, ActivityKind kind, Func<T> func)
    {
        using var activity = ActivitySource.StartActivity(name, kind);
        activity?.Start();
        return func();
    }

    public static async ValueTask<T> ActivityVT<T>(string name, ActivityKind kind, Func<ValueTask<T>> func)
    {
        using var activity = ActivitySource.StartActivity(name, kind);
        activity?.Start();
        return await func();
    }

    public static async ValueTask ActivityVT(string name, ActivityKind kind, Func<ValueTask> func)
    {
        using var activity = ActivitySource.StartActivity(name, kind);
        activity?.Start();
        await func();
    }

    public static async Task<T> ActivityT<T>(string name, ActivityKind kind, Func<Task<T>> func)
    {
        using var activity = ActivitySource.StartActivity(name, kind);
        activity?.Start();
        return await func();
    }

    public static async Task ActivityT(string name, ActivityKind kind, Func<Task> func)
    {
        using var activity = ActivitySource.StartActivity(name, kind);
        activity?.Start();
        await func();
    }

    public static void Activity(string name, ActivityKind kind, Action action)
    {
        using var activity = ActivitySource.StartActivity(name, kind);
        activity?.Start();
        action();
    }
}