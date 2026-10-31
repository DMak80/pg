using System.Text.RegularExpressions;

namespace AdminPanel.Api.Operations;

// Канонические имена сущностей для path-интерполяции прокси (arch/02 §9.3:
// те же паттерны, что у воркеров — ClusterPattern/ShardPattern). Прокси
// интерполирует route-параметры в URL воркера — неканоническое имя (%2F,
// %3F, %23 в path) до интерполяции отсекается 404 ProblemDetails (тот же
// код, что отвечает воркер на неканоническое имя — RestoreShardHandler,
// §10.3): endpoint-confusion в пределах хоста невозможна.
public static partial class CanonicalNames
{
    [GeneratedRegex("^[a-z][a-z0-9_]{0,62}$")]
    private static partial Regex ClusterPattern();

    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex ShardPattern();

    public static bool IsCanonicalCluster(string name) => ClusterPattern().IsMatch(name);

    public static bool IsCanonicalShard(string name) => ShardPattern().IsMatch(name);

    // 404 ProblemDetails (тело — канон ASP.NET): зеркалит ответ воркера на
    // неканоническое имя — UI-контракт (ProblemDetails-текст) не меняется.
    public static WorkerProblemDetails NotFoundProblem(string name)
        => new(404, System.Text.Json.JsonSerializer.Serialize(new
        {
            title = "Not found",
            status = 404,
            detail = $"canonical name required (^[a-z][a-z0-9_]{{0,62}}$): {name}",
        }));
}
