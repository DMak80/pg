using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KafkaWorker.IntegrationTests.Kafka;

// Интеграция возрастных таймаутов ротационных заявок (t10, spec §6 п.10–12):
// дедлок H↔K разрывается приоритетом (обе заявки доиграны БЕЗ экспирации),
// вечный waiting-cluster снимается воркером с исходом expired, заявка
// rebalances при вечном drain-кандидате снимается, идущий balance не снимается.
// Все заявки с requested_unix в прошлом (now - 3700) — порог 3600 срабатывает
// первым тиком, без реального ожидания.
//
// Примечание к AC12 (отклонение от буквы плана, механика Kafka): план
// предполагал слепоту DescribeTopics от `docker stop broker1` при 2 брокерах,
// но bootstrap-список содержит оба брокера — клиент идёт на живой broker2,
// describe успешен, и тик исполнил бы заявку вместо экспирации. Сценарий
// реализован по букве spec §6 п.12: вечный drain-кандидат (broker1
// state=TO_REMOVE; drain заблокирован minISR-guard'ом навсегда) → точка
// waiting-drain; идущий balance — снижением config RF (прецедент
// ReassignmentTests), подача батчей реальна.
// Per-class коллекция (t24/AGENTS.base §13; spec §6.3.2): свой инстанс
// KafkaClusterFixture — свой etcd-контур, свои кластеры/сети (RunTag),
// своё окно портов и AdminFactory; классы параллельны (потолок —
// xunit.runner.json), факты класса последовательны.
[CollectionDefinition(Name)]
public sealed class RotationTimeoutCollection : ICollectionFixture<KafkaClusterFixture>
{
    public const string Name = "kfw-kafka-rotto";
}

[Collection(RotationTimeoutCollection.Name)]
public class RotationTimeoutTests(KafkaClusterFixture fixture, ITestOutputHelper output)
{
    // Риг процессов кластера (как ReassignmentTests): процессы напрямую,
    // тики цикла — тестом.
    private sealed record Rig(
        ClaimStore Claims,
        WorkJournal Journal,
        ProvisioningProcess Provision,
        DeprovisioningProcess Deprovision,
        PartitionReassignerProcess Reassigner);

    // ===== Механика стабильности AC12 (spec 2026-10-17-kfw-rotation-timeout-flake §5) =====

    // Риг AC12: батч подач 50 партиций (§5.4) — сходимость 100 партиций это
    // 2 подачи + финальный тик вместо 10+1: подача — сетевой round-trip, не
    // JVM-старт, экономия подач — меньше время и вероятность транзиента.
    // Единая константа для рига и расчёта
    // backstop финала (§5.3) — риг и расчёт не разъезжаются.
    private const int ReassignBatchPartitions = 50;

    private async Task<Rig> NewRigAsync(string cluster, int brokers)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.SeedClusterAsync(cluster, brokers);
        var claims = new ClaimStore("/kafkaworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal("/kafkaworker", fixture.Gateway, [fixture.Endpoint]);
        return new Rig(
            claims,
            journal,
            new ProvisioningProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                new PortAllocLock("/kafkaworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System, claims.InstanceId),
                new PortAllocIndex(fixture.Gateway, [fixture.Endpoint], NullLogger<PortAllocIndex>.Instance),
                new ClusterSecretEnsurer(fixture.Gateway, [fixture.Endpoint]),
                fixture.AdminFactory, new ClusterConfigConverger(fixture.AdminFactory),
                fixture.Options, fixture.Certificates, snapshot: null),
            new DeprovisioningProcess(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal, snapshot: null),
            new PartitionReassignerProcess(
                fixture.Gateway, [fixture.Endpoint], claims, journal,
                fixture.AdminFactory,
                new ReassignOptions(IntervalSec: 0, BatchPartitions: ReassignBatchPartitions, SubmitSec: 60, RetrySubmitSec: 120),
                TimeProvider.System));
    }

    // Provisioning-цикл до готовности (config без state).
    private async Task UpAsync(Rig rig, string cluster, int budgetSec)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(budgetSec);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null)
                return;

            var tick = await rig.Provision.RunAsync(snap, ct);
            tick.IsSuccess.Should().BeTrue(
                $"provisioning tick must not fail (waiting-brokers is a success): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        throw new TimeoutException($"cluster {cluster} did not come up within {budgetSec} s");
    }

    // Финал теста: демонтаж кластера (контейнеры kfw-* не оставляем коллегам).
    private async Task TeardownAsync(Rig rig, string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var raw = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
        if (raw is not null)
            await fixture.Gateway.PutAsync(fixture.Endpoint, $"/kafka/clusters/{cluster}/config",
                raw.Replace("}", ",\"state\":\"TO_REMOVE\"}", StringComparison.Ordinal), lease: null, ct);
        var dying = await fixture.SnapshotAsync(cluster);
        if (dying is not null)
            await rig.Deprovision.RunAsync(cluster, dying.Brokers.Select(b => b.Name).ToList(), ct);
        await rig.Claims.DisposeAsync();
    }

    private async Task PutTicketAsync(string key, long requestedUnix)
        => await fixture.Gateway.PutAsync(fixture.Endpoint, key,
            $$"""{"requested_unix":{{requestedUnix}},"requested_by":"it"}""", lease: null,
            TestContext.Current.CancellationToken);

    // Лимиты стагнации (spec §5.1 — подряд неудачные тики; §5.3 — подряд
    // успешные с неизменным remaining) и размер кольцевого буфера диагностики.
    private const int FailedTickLimit = 3;
    private const int StagnantTickLimit = 3;
    private const int DiagLimit = 32;

    // Периоды опроса и подачи (spec §5.6; фазы H/K AC10 — вычисляемый
    // backstop + PollMs-опрос + TickInterval-подача): завершённая работа
    // обнаруживается мгновенно — опрос состояния дешёв (etcd-GET,
    // миллисекунды), тик с подачей дороже (сетевой round-trip AlterReassignments,
    // не JVM-старт); интервал тика страхует от спама дешёвых waiting-тиков
    // и ретраев хелпера.
    // Поллинг AC11 и UpAsync (3000 мс) — вне области правки.
    private const int PollMs = 250;

    private const int TickIntervalMs = 1000;

    // Полный диагностический контекст кластера для сообщений падения
    // (spec §4.3): прогресс-ключ (вкл. last_error), журнал work/<C>, исход
    // ticket_outcomes/<C> — всё, что нужно для разбора флейка по выводу
    // прогона без повторных запусков. Ошибки etcd не всплывают (fixture
    // возвращает null) — сборщик не может замаскировать причину падения.
    private async Task<string> DiagContextAsync(string cluster)
    {
        var progress = await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}");
        var journal = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
        var outcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
        return $"progress={progress ?? "<none>"}; journal={journal ?? "<none>"}; outcome={outcome ?? "<none>"}";
    }

    // Полный диагностический контекст фаз H/K AC10: журнал work/<C> + хвост
    // journalTexts, счётчик тиков по journal-фазам, живость заявок и исход,
    // наличие ключей снапшота (различает «слепой снапшот» от «слепого
    // преф-чека»), прямая describe-проба тестом (фактический текст ошибки
    // брокера/клиента при явном RequestTimeout 10 с) и CreatedClients фабрики
    // (маркер Unhealthy-цикла пересозданий). Сборщик не бросает: ошибки
    // чтений и пробы попадают в текст, не маскируя причину падения.
    private async Task<string> RotationDiagAsync(
        string cluster, IReadOnlyList<string> journalTexts, IReadOnlyDictionary<string, int> phaseTicks)
    {
        var work = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
        var rotTicket = await fixture.GetAsync($"/kafkaworker/rotations/{cluster}");
        var caTicket = await fixture.GetAsync($"/kafkaworker/ca_rotations/{cluster}");
        var outcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
        var endpoints = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
        var appPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password");
        var adminPassword = await fixture.GetAsync($"/kafka/clusters/{cluster}/admin_password");
        var caPem = await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem");

        string probe;
        if (endpoints is null)
        {
            probe = "skip: endpoints key is null (blind snapshot branch)";
        }
        else
        {
            try
            {
                using var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build();
                // Confluent.Kafka 2.15: DescribeClusterAsync — только options
                // (без ct), отмена не нужна: проба ограничена RequestTimeout.
                var view = await admin.DescribeClusterAsync(
                    new DescribeClusterOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
                probe = $"ok: brokers={view.Nodes.Count}";
            }
            catch (Exception e)
            {
                probe = $"fail: {e.Message}";
            }
        }

        var createdClients = ((KafkaAdminClientFactory)fixture.AdminFactory).CreatedClients;
        var phases = string.Join(",", phaseTicks.Select(kv => $"{kv.Key}={kv.Value}"));
        return $"work={work ?? "<none>"}; rotations={rotTicket ?? "<none>"}; "
            + $"ca_rotations={caTicket ?? "<none>"}; outcome={outcome ?? "<none>"}; "
            + $"snapshot: endpoints={endpoints is not null} app_password={appPassword is not null} "
            + $"admin_password={adminPassword is not null} ca_pem={caPem is not null}; "
            + $"describe probe={probe}; createdClients={createdClients}; "
            + $"phase ticks: {phases}; journal tail: {string.Join(" | ", journalTexts.TakeLast(6))}";
    }

    // Транзиент-толерантный тик (spec §5.1): одиночный Result.Failed — не
    // провал теста: процесс идемпотентен, следующий тик переподаст батч
    // (arch/16 §5 I); подряд FailedTickLimit неудачных — исключение с
    // ПОЛНЫМ контекстом (DiagContextAsync + хвост ошибок тиков). Backstop
    // на хелпер не заводится: тик завершается сам (≤30 с etcd-транзиент,
    // ≤SubmitSec=60 подачи), три подряд провала — быстрое красное. Возвращает
    // длительность успешной попытки — скользящий замер темпа для backstop
    // финала (spec §5.3).
    private async Task<TimeSpan> TickAsync(
        string label, string cluster, Func<Task<Result>> tick, Queue<string> diag,
        Func<Task<string>>? extraDiag = null)
    {
        for (var attempt = 1; ; attempt++)
        {
            var started = DateTimeOffset.UtcNow;
            var result = await tick();
            if (result.IsSuccess)
                return DateTimeOffset.UtcNow - started;

            EnqueueDiag(diag, $"{label}: tick #{attempt} failed — {result.Error!.Message}");
            if (attempt >= FailedTickLimit)
            {
                var context = await DiagContextAsync(cluster);
                var extra = extraDiag is null ? "" : "; " + await extraDiag();
                throw new Xunit.Sdk.XunitException(
                    $"{label}: stagnation — {FailedTickLimit} consecutive failed ticks; {context}{extra}; "
                    + $"tick tail: {string.Join(" | ", diag)}");
            }

            await Task.Delay(TickIntervalMs, TestContext.Current.CancellationToken);
        }
    }

    // Кольцевой буфер диагностики: последние DiagLimit строк.
    private static void EnqueueDiag(Queue<string> diag, string line)
    {
        diag.Enqueue(line);
        while (diag.Count > DiagLimit)
            diag.Dequeue();
    }

    // partitions_remaining из прогресс-ключа balance (null — ключа нет или
    // поля нет: ключ удаляется только финалом вместе с заявкой).
    private async Task<int?> ReadRemainingAsync(string cluster)
    {
        var progress = await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}");
        if (progress is null)
            return null;
        using var doc = JsonDocument.Parse(progress);
        return doc.RootElement.TryGetProperty("partitions_remaining", out var rem)
            ? rem.GetInt32()
            : null;
    }

    // Опрос состояния Act2 (spec §5.2/§5.6): различает два валидных исхода —
    // живой путь (прогресс mode=balance; сэмпл remaining собирается) и
    // вырожденный (H5 «мгновенная сходимость»: заявка и прогресс отсутствуют,
    // исход done/rebalance с finished_unix ≥ requested заявки Act2).
    private async Task<(bool LiveBalance, bool DegenerateDone)> ReadAct2StateAsync(
        string cluster, long requestedUnix, List<int> remaining)
    {
        var progress = await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}");
        if (progress is not null
            && progress.Contains("\"mode\":\"balance\"", StringComparison.Ordinal))
        {
            using var doc = JsonDocument.Parse(progress);
            if (doc.RootElement.TryGetProperty("partitions_remaining", out var rem))
                remaining.Add(rem.GetInt32());
            return (true, false);
        }

        if (progress is null
            && await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}") is null
            && await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}") is { } outcome
            && outcome.Contains("\"kind\":\"rebalance\"", StringComparison.Ordinal)
            && outcome.Contains("\"outcome\":\"done\"", StringComparison.Ordinal)
            && TryReadFinishedUnix(outcome, out var finished)
            && finished >= requestedUnix)
            return (false, true);

        return (false, false);
    }

    // finished_unix из JSON исхода ticket_outcomes (false — поля нет/битый JSON).
    private static bool TryReadFinishedUnix(string outcomeJson, out long finishedUnix)
    {
        try
        {
            using var doc = JsonDocument.Parse(outcomeJson);
            if (doc.RootElement.TryGetProperty("finished_unix", out var fin)
                && fin.ValueKind == JsonValueKind.Number
                && fin.TryGetInt64(out finishedUnix))
                return true;
        }
        catch (JsonException)
        {
            // битый JSON исхода — вырожденным путём не считается
        }

        finishedUnix = 0;
        return false;
    }

    // op/phase journal-записи work/<C> (null — битый JSON или полей нет):
    // ключ счётчика тиков по journal-фазам фаз H/K.
    private static (string Op, string Phase)? ParseJournalOpPhase(string workJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(workJson);
            if (doc.RootElement.TryGetProperty("op", out var op) && op.ValueKind == JsonValueKind.String
                && doc.RootElement.TryGetProperty("phase", out var phase) && phase.ValueKind == JsonValueKind.String)
                return (op.GetString()!, phase.GetString()!);
        }
        catch (JsonException)
        {
            // битый JSON журнала — в счётчик фаз не попадает
        }

        return null;
    }

    // Признак «фаза H доиграна»: journal op=rotate + phase=done, ЛИБО заявка
    // удалена фазой B + исход password-app/done (исход пишется до journal
    // done). Проверка «не expired» — на каждом чтении журнала.
    private async Task<bool> HPhaseDoneAsync(string cluster, string? workNow)
    {
        if (workNow is not null)
        {
            workNow.Should().NotContain(
                "expired", "started rotations are never expired (triple guard)");
            if (workNow.Contains("\"op\":\"rotate\"", StringComparison.Ordinal)
                && workNow.Contains("\"phase\":\"done\"", StringComparison.Ordinal))
                return true;
        }

        if (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}") is null)
        {
            var outcomeH = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            if (outcomeH is not null
                && outcomeH.Contains("\"kind\":\"password-app\"", StringComparison.Ordinal)
                && outcomeH.Contains("\"outcome\":\"done\"", StringComparison.Ordinal))
                return true; // исход H done записан (до journal done)
        }

        return false;
    }

    // Признак «фаза K доиграна»: journal op=rotate-ca + phase=done;
    // «не expired» — на каждом чтении журнала.
    private static bool KPhaseDone(string? workNow)
    {
        if (workNow is null)
            return false;
        workNow.Should().NotContain("expired", "K window completes without expiration");
        return workNow.Contains("\"op\":\"rotate-ca\"", StringComparison.Ordinal)
            && workNow.Contains("\"phase\":\"done\"", StringComparison.Ordinal);
    }

    // Вычисляемый backstop финала (spec §5.3): объём в батчах — из
    // ФАКТИЧЕСКОГО pending первого успешного тика финального цикла, то есть
    // из remaining, который этот тик записал в прогресс-ключ (100 — объём
    // нового плана RF=2), а НЕ из remaining старого ключа до первого тика
    // (~50 — занизил бы backstop вдвое): batches = ceil(remainingFirst /
    // BatchPartitions); backstop = batches × фактическая средняя длительность
    // успешного тика (скользящий замер) × запас 1.5 + 60 с; нижняя граница
    // 300 с, верхний кап 1800 с. До первого тика — пол 300 с (нет ни замера
    // темпа, ни объёма нового плана); backstop — предохранитель от вечного
    // цикла, реальное падение при поломке даёт стагнация-детект раньше него.
    private static int ConvergeBackstopSec(IReadOnlyList<double> tickSeconds, int? remainingFirst)
    {
        if (tickSeconds.Count == 0 || remainingFirst is null)
            return 300;
        var batches = Math.Max(1, (int)Math.Ceiling(remainingFirst.Value / (double)ReassignBatchPartitions));
        var avgTickSec = tickSeconds.Average();
        return Math.Clamp((int)(batches * avgTickSec * 1.5 + 60), 300, 1800);
    }

    // Вычисляемый backstop фаз H/K AC10: пол = 2 rolling-фазы ×
    // BrokerBootSec(100) + гейт-хвост 60 с = 260 с (литеральные 240/300 с
    // ниже механики самого теста — удалены); при наличии замеров темпа
    // (длительность успешных тиков) — batches(2) × средний тик × запас 1.5
    // + 60 с, кап 1800 с (по образцу ConvergeBackstopSec). Счётчик стагнации
    // по тикам для H/K НЕ вводится: передержка гейта waiting-cluster
    // канонична и не ограничена по сроку — красит только этот backstop.
    private const int RotationBackstopFloorSec = 260;
    private const int RotationBackstopCapSec = 1800;

    private static int RotationBackstopSec(IReadOnlyList<double> tickSeconds)
    {
        if (tickSeconds.Count == 0)
            return RotationBackstopFloorSec;
        var avgTickSec = tickSeconds.Average();
        return Math.Clamp(
            (int)(2 * avgTickSec * 1.5 + 60), RotationBackstopFloorSec, RotationBackstopCapSec);
    }

    // Красный финала AC12 с полной диагностикой (spec §4.3, §5.3): причина,
    // сэмплы partitions_remaining, прогресс-ключ (вкл. last_error), журнал
    // work/<C>, исход ticket_outcomes, хвост тиков — следующий флейк (если
    // случится) разбирается по выводу прогона без повторов.
    private async Task FailConvergeAsync(string cluster, Queue<string> diag, IReadOnlyList<int> remainingSamples, string cause)
    {
        var context = await DiagContextAsync(cluster);
        throw new Xunit.Sdk.XunitException(
            $"{cause}; remaining samples: {string.Join(",", remainingSamples)}; {context}; "
            + $"ticks: {string.Join(" | ", diag)}");
    }

    // ===== AC10 (spec §6 п.10): дедлок H↔K разорван приоритетом =====

    [Fact]
    public async Task BothTicketsPasswordAndCa_PlayThroughWithoutExpiry()
    {
        var cluster = fixture.Cluster("rotto");
        var ct = TestContext.Current.CancellationToken;
        var rig = await NewRigAsync(cluster, brokers: 1);
        try
        {
            // Arrange: канонический кластер (1 брокер) + ОБЕ заявки одновременно
            // (обе свежие — waiting-фазы чужих процессов не экспирируют их).
            await UpAsync(rig, cluster, budgetSec: 200);
            var oldAppPassword = (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password"))!;
            var oldCaPem = (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"))!;
            var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await PutTicketAsync($"/kafkaworker/rotations/{cluster}", nowUnix);
            await PutTicketAsync($"/kafkaworker/ca_rotations/{cluster}", nowUnix);

            var claims = rig.Claims;
            var journal = rig.Journal;
            var passwordRotator = new PasswordRotator(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                fixture.AdminFactory, fixture.Options, fixture.Certificates, snapshot: null);
            var caRotator = new CaRotator(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                fixture.AdminFactory, fixture.Options, fixture.Certificates, snapshot: null);

            // Act: последовательные фазы — H доигрывает ПЕРВЫМ (при живой
            // ca-заявке: старый H гейтился ca-заявкой вечно — исполнение H и
            // есть доказательство разрыва дедлока, spec §2.4 «H доигрывает свою,
            // затем K исполняет свою»), затем K. Порядок тиков H→K
            // последовательный (не перемежающийся): журнал work/<C> — ОДИН ключ
            // на оба процесса, waiting-writes K затирают маркер хвоста H
            // (rotated-commit) — известная гонка общего журнала, вне буквы t10.
            var passwordSeenDone = false;
            var journalTexts = new List<string>();
            var phaseTicks = new Dictionary<string, int>(); // "op:phase" → сколько тиков вернули фазу
            var diag = new Queue<string>();
            var hTickSeconds = new List<double>();

            // Фаза H: тики — не чаще TickIntervalMs, опрос journal/заявки/исхода —
            // немедленно после тика и каждые PollMs; тики транзиент-толерантны
            // (TickAsync, 3 подряд Failed → красный с диагностикой); красный —
            // по вычисляемому backstop (RotationBackstopSec) с полным
            // диагностическим контекстом, не по литеральным часам. Признак
            // «H доиграна» — HPhaseDoneAsync; порядок тиков H→K —
            // последовательный (не перемежающийся).
            var hStarted = DateTimeOffset.UtcNow;
            var hNextTickAt = DateTimeOffset.UtcNow; // первый тик — немедленно
            while (!passwordSeenDone)
            {
                if (DateTimeOffset.UtcNow >= hNextTickAt)
                {
                    var took = await TickAsync("phase-h", cluster,
                        async () => await passwordRotator.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag,
                        extraDiag: () => RotationDiagAsync(cluster, journalTexts, phaseTicks));
                    hTickSeconds.Add(took.TotalSeconds);
                    hNextTickAt = DateTimeOffset.UtcNow.AddMilliseconds(TickIntervalMs);

                    var workAfterTick = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                    if (workAfterTick is not null)
                    {
                        journalTexts.Add(workAfterTick);
                        if (ParseJournalOpPhase(workAfterTick) is { } opPhase)
                            phaseTicks[$"{opPhase.Op}:{opPhase.Phase}"] =
                                phaseTicks.GetValueOrDefault($"{opPhase.Op}:{opPhase.Phase}") + 1;
                    }

                    passwordSeenDone = await HPhaseDoneAsync(cluster, workAfterTick);
                    if (passwordSeenDone)
                        break;
                }

                var hBackstopSec = RotationBackstopSec(hTickSeconds);
                if (DateTimeOffset.UtcNow - hStarted >= TimeSpan.FromSeconds(hBackstopSec))
                    throw new Xunit.Sdk.XunitException(
                        $"phase H: backstop {hBackstopSec} s exhausted without the rotation completing "
                        + $"(live ca ticket — deadlock broken by priority; the budget follows the rotation "
                        + $"mechanics, not literal clocks); "
                        + $"{await RotationDiagAsync(cluster, journalTexts, phaseTicks)}; "
                        + $"ticks: {string.Join(" | ", diag)}");

                await Task.Delay(PollMs, ct);
                var workPeek = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                if (workPeek is not null)
                    journalTexts.Add(workPeek);
                passwordSeenDone = await HPhaseDoneAsync(cluster, workPeek);
            }

            // Фаза K: та же механика, что H (PollMs-опрос, TickInterval-подача,
            // транзиент-толерантные тики, вычисляемый backstop с полной
            // диагностикой); тики H→K последовательны — общий журнал work/<C>.
            var caDone = false;
            var kTickSeconds = new List<double>();
            var kStarted = DateTimeOffset.UtcNow;
            var kNextTickAt = DateTimeOffset.UtcNow; // первый тик — немедленно
            while (!caDone)
            {
                if (DateTimeOffset.UtcNow >= kNextTickAt)
                {
                    var took = await TickAsync("phase-k", cluster,
                        async () => await caRotator.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag,
                        extraDiag: () => RotationDiagAsync(cluster, journalTexts, phaseTicks));
                    kTickSeconds.Add(took.TotalSeconds);
                    kNextTickAt = DateTimeOffset.UtcNow.AddMilliseconds(TickIntervalMs);

                    var workAfterTick = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                    if (workAfterTick is not null)
                    {
                        journalTexts.Add(workAfterTick);
                        if (ParseJournalOpPhase(workAfterTick) is { } opPhase)
                            phaseTicks[$"{opPhase.Op}:{opPhase.Phase}"] =
                                phaseTicks.GetValueOrDefault($"{opPhase.Op}:{opPhase.Phase}") + 1;
                    }

                    caDone = KPhaseDone(workAfterTick);
                    if (caDone)
                        break;
                }

                var kBackstopSec = RotationBackstopSec(kTickSeconds);
                if (DateTimeOffset.UtcNow - kStarted >= TimeSpan.FromSeconds(kBackstopSec))
                    throw new Xunit.Sdk.XunitException(
                        $"phase K: backstop {kBackstopSec} s exhausted without the CA rotation completing; "
                        + $"{await RotationDiagAsync(cluster, journalTexts, phaseTicks)}; "
                        + $"ticks: {string.Join(" | ", diag)}");

                await Task.Delay(PollMs, ct);
                var workPeek = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                if (workPeek is not null)
                    journalTexts.Add(workPeek);
                caDone = KPhaseDone(workPeek);
            }

            // Assert: обе заявки удалены; журнал — rotate-ca done (финал K);
            // НИ одна фаза не была expired; исход последний — done; пароль и
            // CA сменены.
            (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}")).Should().BeNull(
                "password-rotation ticket completed by H (deadlock broken by priority)");
            (await fixture.GetAsync($"/kafkaworker/ca_rotations/{cluster}")).Should().BeNull(
                "ca ticket completed by K after H");
            var workFinal = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
            workFinal.Should().Contain("rotate-ca").And.Contain("done");
            journalTexts.Should().NotContain(t => t.Contains("expired"));
            var outcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            outcome.Should().NotBeNull("K finalization writes the done outcome");
            outcome!.Should().Contain("\"kind\":\"ca\"").And.Contain("\"outcome\":\"done\"");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password"))!
                .Should().NotBe(oldAppPassword, "phase A replaced app_password");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"))!
                .Should().NotBe(oldCaPem, "phase C switched the canonical key to NEW CA");
        }
        finally
        {
            await TeardownAsync(rig, cluster);
        }
    }

    // ===== AC11 (spec §6 п.11): вечный waiting-cluster → expired + повторная заявка =====

    [Fact]
    public async Task PasswordTicketOnUnvalidatedCluster_ExpiredAndRetriable()
    {
        var cluster = fixture.Cluster("rotexp");
        var ct = TestContext.Current.CancellationToken;
        var rig = await NewRigAsync(cluster, brokers: 1);
        try
        {
            // Arrange: кластер поднят, затем endpoints занулены (слепой кластер:
            // снапшот без endpoints; staging отсутствует — дооконная ветка H);
            // заявка старше порога.
            await UpAsync(rig, cluster, budgetSec: 200);
            await fixture.PutAsync("/kafka/clusters/" + cluster + "/endpoints", "");
            var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await PutTicketAsync($"/kafkaworker/rotations/{cluster}", nowUnix - 3700);

            var rotator = new PasswordRotator(
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, rig.Claims, rig.Journal,
                fixture.AdminFactory, fixture.Options, fixture.Certificates, snapshot: null);

            // Act: один-два тика ротатора.
            for (var i = 0; i < 2; i++)
            {
                var tick = await rotator.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);
                tick.IsSuccess.Should().BeTrue($"tick must not fail: {tick.Error?.Message}");
                if (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}") is null)
                    break;
            }

            // Assert: заявка снята; исход expired waiting-cluster; журнал
            // expired; повторная заявка ставится (ключа нет — 409 невозможен).
            (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}")).Should().BeNull(
                "unstarted ticket older than the threshold is removed in waiting-cluster");
            var outcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            outcome.Should().NotBeNull();
            outcome!.Should().Contain("\"kind\":\"password-app\"")
                .And.Contain("\"outcome\":\"expired\"")
                .And.Contain("\"reason\":\"waiting-cluster\"");
            (await fixture.GetAsync($"/kafkaworker/work/{cluster}"))!
                .Should().Contain("expired", "the expired journal phase is terminal");
            await PutTicketAsync($"/kafkaworker/rotations/{cluster}",
                DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}")).Should().NotBeNull(
                "a new ticket can be placed after removal");

            // Teardown-гигиена: снять тестовую заявку (демонтаж ниже чистит всё).
            await fixture.DelAsync($"/kafkaworker/rotations/{cluster}");
        }
        finally
        {
            await TeardownAsync(rig, cluster);
        }
    }

    // ===== AC12 (spec §6 п.12): drain-кандидат → expired; идущий balance не снимается =====

    [Fact]
    public async Task RebalanceTicket_DrainCandidateExpired_LiveBalanceKept()
    {
        var cluster = fixture.Cluster("rebrto");
        var ct = TestContext.Current.CancellationToken;
        var rig = await NewRigAsync(cluster, brokers: 2);
        try
        {
            await UpAsync(rig, cluster, budgetSec: 200);
            var nowUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            // Юзер-топик (100 партиций RF=2): у internal-топиков RF-цель
            // min(3, цели) и снижение config RF их не двигает; 100 партиций
            // дают план ровно на 2 батча по 50. Окно идущей balance держится
            // живым прогресс-ключом mode=balance (гвард экспирации), а не
            // темпом сходимости; крупные батчи — экономия подач (подача —
            // сетевой round-trip), меньше тайминговая поверхность
            // (spec 2026-10-17-...-flake §5.4; прецедент ReassignmentTests).
            var topic = $"rebrto-{fixture.RunTag}";
            using (var admin = (await fixture.DiscoveryAdminBuilderAsync(cluster, "admin")).Build())
                await admin.CreateTopicsAsync(
                    [new TopicSpecification { Name = topic, NumPartitions = 100, ReplicationFactor = 2 }],
                    new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });

            // --- Act/Assert 1: вечный drain-кандидат + старая заявка → expired.
            // broker1 state=TO_REMOVE: drain-кандидат есть, но план блокирован
            // minISR=2 при единственной цели — drain вечен; тик попадает в
            // waiting-drain-точку и снимает заявку.
            await fixture.PutAsync($"/kafka/clusters/{cluster}/brokers/broker1/state", "TO_REMOVE");
            await PutTicketAsync($"/kafkaworker/rebalances/{cluster}", nowUnix - 3700);
            // Кольцевой буфер диагностики кейса (spec §4.3): ошибки тиков
            // хелпера; полный хвост печатается в сообщениях падения.
            var diag = new Queue<string>();
            // Expired-тик детерминирован (вечный drain + заявка старше порога);
            // сетевой/etcd-транзиент одиночного тика покрывается хелпером (§5.5).
            await TickAsync("act1-expired", cluster,
                async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);

            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().BeNull(
                "rebalance ticket removed at the waiting-drain point (never started)");
            var expiredOutcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            expiredOutcome.Should().NotBeNull();
            expiredOutcome!.Should().Contain("\"kind\":\"rebalance\"")
                .And.Contain("\"outcome\":\"expired\"")
                .And.Contain("\"reason\":\"waiting-drain\"");            (await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}")).Should().BeNull(
                "progress key NOT created — the operation never started");
            (await fixture.GetAsync($"/kafkaworker/work/{cluster}"))!
                .Should().Contain("expired");

            // Возврат broker1 в RUNNING (drain-кандидат больше не гейтит).
            await fixture.PutAsync($"/kafka/clusters/{cluster}/brokers/broker1/state", "RUNNING");

            // --- Act/Assert 2: идущая balance не снимается.
            // Снижение config RF 2→1 — реальный план с pending: тик подаёт
            // батчи и создаёт прогресс mode=balance.
            var configRaw = (await fixture.GetAsync($"/kafka/clusters/{cluster}/config"))!;
            await fixture.PutAsync($"/kafka/clusters/{cluster}/config",
                configRaw.Replace("\"replication_factor\":2", "\"replication_factor\":1", StringComparison.Ordinal));
            // Запомненный requested заявки Act2 — различительный предикат
            // вырожденного исхода (finished_unix ≥ requested, §5.2).
            var requestedUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await PutTicketAsync($"/kafkaworker/rebalances/{cluster}", requestedUnix);

            // Act2 (spec §5.2, рев.2): ветвление по факту — ОДИН из двух
            // валидных исходов, различение по состоянию etcd-ключей ПОСЛЕ
            // каждого тика (немедленно, §5.6):
            //  - живой путь: прогресс mode=balance прочитан (гварды §2 п.2
            //    опираются на живость ключа; обновление фиксируют гвард-тики);
            //    при видимости юзер-топика первый тик подачи оставляет
            //    pending ≥ 50 — финальная ветка недостижима до применения
            //    всего плана (§5.4);
            //  - вырожденный путь (H5): заявка и прогресс отсутствуют, исход
            //    done/rebalance с finished_unix ≥ requested — «мгновенная
            //    сходимость» (первый тик увидел пустой pending и дошёл до
            //    финальной ветки БЕЗ подачи). Валидный исход §2 п.3.
            // Транзиент-толерантные тики; стагнация — 3 подряд неудачных
            // (хелпер). Тик с подачей ≤ SubmitSec (60 с) — сетевой round-trip;
            // три подряд неудачных ≤ 240 с; backstop 600 с — предохранитель
            // с запасом. Опрос — сразу после каждого тика и каждые PollMs
            // между тиками; подача — не чаще TickIntervalMs (§5.6:
            // завершённая работа не ждёт пустых пауз).
            var remaining = new List<int>();
            var balanceDeadline = DateTimeOffset.UtcNow.AddSeconds(600);
            var nextTickAt = DateTimeOffset.UtcNow; // первый тик — немедленно
            var liveBalance = false;
            var degenerateDone = false;
            while (!liveBalance && !degenerateDone)
            {
                if (DateTimeOffset.UtcNow >= nextTickAt)
                {
                    await TickAsync("act2-submit", cluster,
                        async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);
                    nextTickAt = DateTimeOffset.UtcNow.AddMilliseconds(TickIntervalMs);
                    (liveBalance, degenerateDone) = await ReadAct2StateAsync(cluster, requestedUnix, remaining);
                    if (liveBalance || degenerateDone)
                        break;
                }

                if (DateTimeOffset.UtcNow >= balanceDeadline)
                {
                    var context = await DiagContextAsync(cluster);
                    throw new Xunit.Sdk.XunitException(
                        "act2: neither a live balance (mode=balance) nor a degenerate done within 600 s; ticket="
                        + (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}") ?? "<none>")
                        + "; " + context + "; ticks: " + string.Join(" | ", diag));
                }

                await Task.Delay(PollMs, ct);
                (liveBalance, degenerateDone) = await ReadAct2StateAsync(cluster, requestedUnix, remaining);
            }

            if (degenerateDone)
            {
                // Вырожденный путь (H5) — валидный исход §2 п.3: гварды §2 п.2
                // вырождены (balance не открывалась — снимать нечего), финальный
                // цикл §5.3 не исполняется (заявки нет; RF юзер-топиков без
                // заявки воркер не конвергит — arch/16 §5 E); демонтаж —
                // teardown-гигиеной. Различительная валидация подготовки:
                // вырожденный исход обязан быть гонкой метаданных (H5-i), НЕ
                // дефектом подготовки (H5-ii) — снижение RF применилось.
                output.WriteLine("degenerate path: convergence overtook the first sample (H5)");
                var configNow = await fixture.GetAsync($"/kafka/clusters/{cluster}/config");
                configNow.Should().NotBeNull("the cluster config key exists");
                configNow!.Should().Contain("\"replication_factor\":1",
                    "degenerate path: the RF replacement did not apply — the outcome is caused by a "
                    + "preparation defect, not a metadata race (spec §5.2, H5-ii)");
                (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().BeNull(
                    "degenerate path: ticket removed (balance finalization on convergence, §2 p.3)");
                (await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}")).Should().BeNull(
                    "degenerate path: progress key not created (§2 p.3)");
                var degenerateOutcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
                degenerateOutcome.Should().NotBeNull("degenerate path: outcome written");
                degenerateOutcome!.Should().Contain("\"kind\":\"rebalance\"")
                    .And.Contain("\"outcome\":\"done\"");
                TryReadFinishedUnix(degenerateOutcome, out var finished).Should().BeTrue(
                    "degenerate path: finished_unix present in the outcome");
                finished.Should().BeGreaterThanOrEqualTo(requestedUnix,
                    "degenerate path: finalization completed the Act2 ticket (finished ≥ requested)");
                return; // teardown — в finally
            }

            output.WriteLine("live path: balance opened (progress mode=balance)");

            var outcomeAfterBalance = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            remaining.Should().NotBeEmpty(
                "batches submitted — progress mode=balance created; remaining samples: "
                + string.Join(",", remaining)
                + "; journal=" + await fixture.GetAsync($"/kafkaworker/work/{cluster}")
                + "; outcome=" + outcomeAfterBalance);
            var progressLive = await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}");
            progressLive.Should().NotBeNull(
                "balance is live — the window is held by a live progress key, not by convergence pace; "
                + "journal=" + await fixture.GetAsync($"/kafkaworker/work/{cluster}"))
                .And.Contain("\"balance\"");

            // Старая заявка при идущей balance: тик НЕ снимает (гвард — прогресс
            // balance жив; в balance-ветке экспирационных точек нет вообще).
            await PutTicketAsync($"/kafkaworker/rebalances/{cluster}", nowUnix - 3700);
            await TickAsync("guard2-old-ticket", cluster,
                async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);
            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().NotBeNull(
                "a running rebalance is never removed (triple guard)");

            // Гвард в waiting-точке при живом balance: слепая проба (endpoints
            // занулены) + старая заявка → НЕ expired, waiting-cluster.
            var endpointsRaw = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
            await fixture.PutAsync("/kafka/clusters/" + cluster + "/endpoints", "");
            await TickAsync("guard3-blind-endpoints", cluster,
                async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);
            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().NotBeNull(
                "the previous mode=balance guard also holds at the endpoints point");
            var outcomeLive = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            outcomeLive.Should().Be(expiredOutcome,
                "the previous mode=balance guard forbids expiration — the Act1 outcome is not overwritten; "
                + "journal=" + await fixture.GetAsync($"/kafkaworker/work/{cluster}"));
            await fixture.PutAsync($"/kafka/clusters/{cluster}/endpoints", endpointsRaw!);

            // --- Финал: возврат RF=2 → balance доигрывается → исход done.
            // Падение — по стагнации, не по часам (spec §5.3). Механика воркера,
            // на которую опирается детект: прогресс-ключ пишется тиком как
            // pending.Count НОВОГО плана (факт vs план этого тика) — при
            // возврате RF 1→2 первый тик финального цикла переписывает
            // remaining со старых ~50 на 100: РОСТ remaining — штатная
            // перепланировка (объём нового плана), а не стагнация. Идеальный
            // прогон счётчик не зажигает вовсе: 50 → 100 (перепланировка) →
            // 50 (прогресс) → удаление (выход). Дедуп-тик (RetrySubmitSec=120,
            // батч не применился за межтик) или затяжка ISR дают равенство —
            // на ложное красное нужно 3 подряд таких тика.
            var restoredRaw = (await fixture.GetAsync($"/kafka/clusters/{cluster}/config"))!;
            await fixture.PutAsync($"/kafka/clusters/{cluster}/config",
                restoredRaw.Replace("\"replication_factor\":1", "\"replication_factor\":2", StringComparison.Ordinal));

            // База детекта — remaining текущего прогресс-ключа на старте цикла.
            var bestRemaining = await ReadRemainingAsync(cluster) ?? 100;
            var remainingSamples = new List<int> { bestRemaining };
            int? remainingFirst = null; // объём нового плана: remaining, записанный первым тиком финала
            var convergeStarted = DateTimeOffset.UtcNow;
            var tickSeconds = new List<double>();
            var stagnantTicks = 0;
            while (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}") is not null)
            {
                var backstopSec = ConvergeBackstopSec(tickSeconds, remainingFirst);
                if (DateTimeOffset.UtcNow - convergeStarted >= TimeSpan.FromSeconds(backstopSec))
                    await FailConvergeAsync(cluster, diag, remainingSamples,
                        $"final: backstop {backstopSec} s exhausted with the ticket still live "
                        + $"(remainingFirst={(remainingFirst?.ToString() ?? "—")}, ticks={tickSeconds.Count}, "
                        + $"avg tick={(tickSeconds.Count > 0 ? tickSeconds.Average().ToString("F1") : "—")} s) — no convergence");

                var tick = await TickAsync("converge", cluster,
                    async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);
                tickSeconds.Add(tick.TotalSeconds);

                var remainingNow = await ReadRemainingAsync(cluster);
                if (remainingNow is null)
                    continue; // прогресс-ключ ушёл вместе с финалом — выход по while

                remainingSamples.Add(remainingNow.Value);
                remainingFirst ??= remainingNow.Value; // первый тик финала определил объём нового плана
                if (remainingNow.Value != bestRemaining)
                {
                    // Убывание — прогресс (батчи применяются), рост —
                    // перепланировка (новый объём работы): движение в обе
                    // стороны сбрасывает счётчик, база следует за фактом.
                    bestRemaining = remainingNow.Value;
                    stagnantTicks = 0;
                }
                else if (++stagnantTicks >= StagnantTickLimit)
                {
                    await FailConvergeAsync(cluster, diag, remainingSamples,
                        $"final: stagnation — {stagnantTicks} consecutive successful ticks with unchanged "
                        + $"partitions_remaining={bestRemaining} (RetrySubmitSec dedup or ISR lag)");
                }

                // Между тиками — интервал подачи (§5.6); опрос remaining —
                // немедленно после каждого тика (ReadRemainingAsync выше).
                await Task.Delay(TickIntervalMs, ct);
            }

            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().BeNull(
                "ticket removed on convergence");
            (await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}")).Should().BeNull(
                "progress removed on convergence");
            (await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}"))!
                .Should().Contain("\"outcome\":\"done\"", "balance finalization writes the done outcome");
        }
        finally
        {
            await TeardownAsync(rig, cluster);
        }
    }
}
