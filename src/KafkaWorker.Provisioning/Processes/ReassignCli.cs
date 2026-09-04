using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Сборка CLI-вызова kafka-reassign-partitions (arch/16 §2.4; spec t02 §6):
/// файлы плана и SASL-конфига передаются в контейнер брокера однострочной
/// sh -c 'printf …' обёрткой (без новых docker-объектов, без host-портов —
/// bootstrap через INTERNAL-listener сети kfw-net-<C>). Содержимое данных не
/// содержит апострофов (топики ^[a-zA-Z0-9._-]+$, креды [A-Za-z0-9]) —
/// printf-обёртка безопасна; CLI JVM ограничен KAFKA_HEAP_OPTS=-Xmx256m.
/// </summary>
public static class ReassignCli
{
    // Внутренний порт брокера: INTERNAL-listener (docker-DNS alias сети kfw-net-<C>).
    private const int InternalPort = 9092;

    // Имена файлов внутри контейнера брокера (одноразовые, префикс kfw-).
    private const string PropertiesPath = "/tmp/kfw-cmd.properties";
    private const string AssignmentPath = "/tmp/kfw-reassign.json";
    private const string CaPath = "/tmp/kfw-ca.pem";

    /// <summary>bootstrap INTERNAL-listener живых брокеров: "broker1:9092,broker2:9092".</summary>
    public static string Bootstrap(IReadOnlyList<string> brokerNames)
        => string.Join(",", brokerNames.Select(n => $"{n}:{InternalPort}"));

    /// <summary>reassignment.json: {"version":1,"partitions":[{"topic","partition","replicas","log_dirs":["any"…]}]}.</summary>
    public static string BuildAssignmentJson(IReadOnlyList<ReassignMove> moves)
    {
        var payload = new ReassignmentJson(
            1,
            moves
                .OrderBy(m => m.Topic, StringComparer.Ordinal)
                .ThenBy(m => m.Partition)
                .Select(m => new ReassignmentPart(
                    m.Topic,
                    m.Partition,
                    m.Replicas.ToList(),
                    m.Replicas.Select(_ => "any").ToList()))
                .ToList());
        return JsonSerializer.Serialize(payload);
    }

    /// <summary>SASL_SSL/PLAIN properties для --command-config (креды admin из etcd,
    /// доверие per-cluster CA PEM-файлом, arch/16 §2.4).</summary>
    public static string BuildAdminProperties(string user, string password, string caPem)
        => "security.protocol=SASL_SSL\n"
            + "sasl.mechanism=PLAIN\n"
            + """sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required """
            + $"""username="{user}" password="{password}";""" + "\n"
            + "ssl.truststore.type=PEM\n"
            + $"ssl.truststore.location={CaPath}";

    /// <summary>
    /// sh -c команда: printf файлов (префикс kfw-) + CLI c
    /// KAFKA_HEAP_OPTS=-Xmx256m. Одна строка, без литеральных переносов:
    /// переносы строк properties-файла кодируются форматом printf '%s\n'
    /// (эскейп формата), JSON пишется одним %s (spec §6). Первым блоком —
    /// PEM-файл per-cluster CA (caPem.Split('\n') → printf по строкам);
    /// base64-алфавит CA не содержит ' и \ — printf-обёртка безопасна.
    /// </summary>
    public static IReadOnlyList<string> BuildExecCommand(
        IReadOnlyList<ReassignMove> moves, string bootstrap, string user, string password, string caPem)
    {
        // Файл CA: каждая строка PEM — аргумент printf '%s\n...' (литеральных
        // переносов в exec-строке нет).
        var caLines = caPem.Split('\n');
        var caFormat = string.Join("", caLines.Select(_ => "%s\\n"));
        var caArgs = string.Join(" ", caLines.Select(l => $"'{l}'"));

        // printf '%s\n%s\n' — каждый property на своей строке файла;
        // литеральных переносов в exec-строке нет.
        var lines = BuildAdminProperties(user, password, caPem).Split('\n');
        var format = string.Join("", lines.Select(_ => "%s\\n"));
        var args = string.Join(" ", lines.Select(l => $"'{l}'"));
        var json = BuildAssignmentJson(moves);
        var line =
            $"printf '{caFormat}' {caArgs} > {CaPath} && "
            + $"printf '{format}' {args} > {PropertiesPath} && "
            + $"printf %s '{json}' > {AssignmentPath} && "
            + $"KAFKA_HEAP_OPTS=-Xmx256m /opt/kafka/bin/kafka-reassign-partitions.sh"
            + $" --bootstrap-server {bootstrap} --command-config {PropertiesPath}"
            + $" --execute --reassignment-json-file {AssignmentPath}";
        return ["sh", "-c", line];
    }

    // Канонический формат reassignment.json (KIP-455).
    private sealed record ReassignmentJson(
        [property: JsonPropertyName("version")] int Version,
        [property: JsonPropertyName("partitions")] List<ReassignmentPart> Partitions);

    private sealed record ReassignmentPart(
        [property: JsonPropertyName("topic")] string Topic,
        [property: JsonPropertyName("partition")] int Partition,
        [property: JsonPropertyName("replicas")] List<int> Replicas,
        [property: JsonPropertyName("log_dirs")] List<string> LogDirs);
}
