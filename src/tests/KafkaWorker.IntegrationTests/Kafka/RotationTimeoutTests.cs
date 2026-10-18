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
[Collection(KafkaCollection.Name)]
public class RotationTimeoutTests(KafkaClusterFixture fixture)
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
    // 2 подачи + финальный тик вместо 10+1: меньше exec-JVM-подач — меньше
    // время и вероятность транзиента. Единая константа для рига и расчёта
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
                fixture.Gateway, [fixture.Endpoint], fixture.Driver, claims, journal,
                fixture.AdminFactory,
                new ReassignOptions(IntervalSec: 0, BatchPartitions: ReassignBatchPartitions, ExecSec: 180, RetrySubmitSec: 120),
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
                $"тик provisioning не должен падать (waiting-brokers — успех): {tick.Error?.Message}");
            await Task.Delay(3000, ct);
        }

        throw new TimeoutException($"кластер {cluster} не поднялся за {budgetSec} с");
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
        return $"progress={progress ?? "<нет>"}; journal={journal ?? "<нет>"}; outcome={outcome ?? "<нет>"}";
    }

    // Транзиент-толерантный тик (spec §5.1): одиночный Result.Failed — не
    // провал теста: процесс идемпотентен, следующий тик переподаст батч
    // (arch/16 §5 I); подряд FailedTickLimit неудачных — исключение с
    // ПОЛНЫМ контекстом (DiagContextAsync + хвост ошибок тиков). Backstop
    // на хелпер не заводится: тик завершается сам (≤30 с etcd-транзиент,
    // ≤ExecSec=180 exec), три подряд провала — быстрое красное. Возвращает
    // длительность успешной попытки — скользящий замер темпа для backstop
    // финала (spec §5.3).
    private async Task<TimeSpan> TickAsync(string label, string cluster, Func<Task<Result>> tick, Queue<string> diag)
    {
        for (var attempt = 1; ; attempt++)
        {
            var started = DateTimeOffset.UtcNow;
            var result = await tick();
            if (result.IsSuccess)
                return DateTimeOffset.UtcNow - started;

            EnqueueDiag(diag, $"{label}: тик #{attempt} провален — {result.Error!.Message}");
            if (attempt >= FailedTickLimit)
            {
                var context = await DiagContextAsync(cluster);
                throw new Xunit.Sdk.XunitException(
                    $"{label}: стагнация — {FailedTickLimit} подряд неудачных тика; {context}; "
                    + $"хвост тиков: {string.Join(" | ", diag)}");
            }

            await Task.Delay(3000, TestContext.Current.CancellationToken);
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

    // Красный финала AC12 с полной диагностикой (spec §4.3, §5.3): причина,
    // сэмплы partitions_remaining, прогресс-ключ (вкл. last_error), журнал
    // work/<C>, исход ticket_outcomes, хвост тиков — следующий флейк (если
    // случится) разбирается по выводу прогона без повторов.
    private async Task FailConvergeAsync(string cluster, Queue<string> diag, IReadOnlyList<int> remainingSamples, string cause)
    {
        var context = await DiagContextAsync(cluster);
        throw new Xunit.Sdk.XunitException(
            $"{cause}; сэмплы remaining: {string.Join(",", remainingSamples)}; {context}; "
            + $"тики: {string.Join(" | ", diag)}");
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

            // Фаза H (бюджет 240 c): тики ротатора до journal done.
            var hDeadline = DateTimeOffset.UtcNow.AddSeconds(240);
            while (DateTimeOffset.UtcNow < hDeadline && !passwordSeenDone)
            {
                var snap = (await fixture.SnapshotAsync(cluster))!;
                var hTick = await passwordRotator.RunAsync(snap, ct);
                hTick.IsSuccess.Should().BeTrue($"тик H не должен падать: {hTick.Error?.Message}");

                var workNow = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                if (workNow is not null)
                {
                    journalTexts.Add(workNow);
                    workNow.Should().NotContain(
                        "expired", "начатые ротации не экспирируются (тройной гвард)");
                    if (workNow.Contains("\"op\":\"rotate\"", StringComparison.Ordinal)
                        && workNow.Contains("\"phase\":\"done\"", StringComparison.Ordinal))
                        passwordSeenDone = true;
                }

                // Заявка удаляется фазой B — финал (C-хвост) доигрывается тиками.
                if (!passwordSeenDone
                    && await fixture.GetAsync($"/kafkaworker/rotations/{cluster}") is null)
                {
                    var outcomeH = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
                    if (outcomeH is not null
                        && outcomeH.Contains("\"kind\":\"password-app\"", StringComparison.Ordinal)
                        && outcomeH.Contains("\"outcome\":\"done\"", StringComparison.Ordinal))
                        passwordSeenDone = true; // исход H done записан (до journal done)
                }

                await Task.Delay(3000, ct);
            }

            passwordSeenDone.Should().BeTrue(
                "H обязан был завершиться в бюджете фазы (при ЖИВОЙ ca-заявке — дедлок разорван); "
                + "журнал (последние фазы): " + string.Join(" | ", journalTexts.TakeLast(6)));

            // Фаза K (бюджет 300 c): тики ca-ротатора до финала K4.
            var kDeadline = DateTimeOffset.UtcNow.AddSeconds(300);
            var caDone = false;
            while (DateTimeOffset.UtcNow < kDeadline && !caDone)
            {
                var kTick = await caRotator.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);
                kTick.IsSuccess.Should().BeTrue($"тик K не должен падать: {kTick.Error?.Message}");

                var workNow = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
                if (workNow is not null)
                {
                    journalTexts.Add(workNow);
                    workNow.Should().NotContain("expired", "окно K доигрывается без экспирации");
                    if (workNow.Contains("\"op\":\"rotate-ca\"", StringComparison.Ordinal)
                        && workNow.Contains("\"phase\":\"done\"", StringComparison.Ordinal))
                        caDone = true;
                }

                await Task.Delay(3000, ct);
            }

            // Assert: обе заявки удалены; журнал — rotate-ca done (финал K);
            // НИ одна фаза не была expired; исход последний — done; пароль и
            // CA сменены.
            (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}")).Should().BeNull(
                "заявка пароль-ротации доиграна H (дедлок разорван приоритетом)");
            (await fixture.GetAsync($"/kafkaworker/ca_rotations/{cluster}")).Should().BeNull(
                "ca-заявка доиграна K после H");
            caDone.Should().BeTrue(
                "K обязан был завершиться в бюджете фазы; журнал: "
                + string.Join(" | ", journalTexts.TakeLast(6)));
            var workFinal = await fixture.GetAsync($"/kafkaworker/work/{cluster}");
            workFinal.Should().Contain("rotate-ca").And.Contain("done");
            journalTexts.Should().NotContain(t => t.Contains("expired"));
            var outcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            outcome.Should().NotBeNull("финал K пишет исход done");
            outcome!.Should().Contain("\"kind\":\"ca\"").And.Contain("\"outcome\":\"done\"");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/app_password"))!
                .Should().NotBe(oldAppPassword, "фаза A сменила app_password");
            (await fixture.GetAsync($"/kafka/clusters/{cluster}/ca_pem"))!
                .Should().NotBe(oldCaPem, "фаза C переключила канон на NEW CA");
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
                tick.IsSuccess.Should().BeTrue($"тик не должен падать: {tick.Error?.Message}");
                if (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}") is null)
                    break;
            }

            // Assert: заявка снята; исход expired waiting-cluster; журнал
            // expired; повторная заявка ставится (ключа нет — 409 невозможен).
            (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}")).Should().BeNull(
                "не-начатая заявка старше порога снята в waiting-cluster");
            var outcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            outcome.Should().NotBeNull();
            outcome!.Should().Contain("\"kind\":\"password-app\"")
                .And.Contain("\"outcome\":\"expired\"")
                .And.Contain("\"reason\":\"waiting-cluster\"");
            (await fixture.GetAsync($"/kafkaworker/work/{cluster}"))!
                .Should().Contain("expired", "journal-фаза expired терминальна");
            await PutTicketAsync($"/kafkaworker/rotations/{cluster}",
                DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            (await fixture.GetAsync($"/kafkaworker/rotations/{cluster}")).Should().NotBeNull(
                "повторная заявка ставится после снятия");

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
            // темпом сходимости; крупные батчи — меньше exec-JVM-подач,
            // меньше тайминговая поверхность (spec 2026-10-17-...-flake §5.4;
            // прецедент ReassignmentTests).
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
            // etcd/exec-транзиент одиночного тика покрывается хелпером (§5.5).
            await TickAsync("act1-expired", cluster,
                async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);

            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().BeNull(
                "заявка ребалансировки снята в waiting-drain-точке (не начата)");
            var expiredOutcome = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            expiredOutcome.Should().NotBeNull();
            expiredOutcome!.Should().Contain("\"kind\":\"rebalance\"")
                .And.Contain("\"outcome\":\"expired\"")
                .And.Contain("\"reason\":\"waiting-drain\"");            (await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}")).Should().BeNull(
                "прогресс-ключ НЕ создан — операция не начиналась");
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
            await PutTicketAsync($"/kafkaworker/rebalances/{cluster}",
                DateTimeOffset.UtcNow.ToUnixTimeSeconds());

            // Act2 (spec §5.2): один сэмпл живого прогресс-ключа mode=balance —
            // гварды экспирации опираются на живость прогресс-ключа, а не на
            // его «обновляемость»; повторную подачу делает гвард-тик 2 ниже.
            // Транзиент-толерантные тики; стагнация — 3 подряд неудачных
            // (хелпер). Backstop 600 с — предохранитель: 3 × ExecSec =
            // 3 × 180 = 540 с на три подряд exec-провала на пределе таймаута
            // + 60 с хвоста на etcd/describe; фактическое завершение —
            // 1–2 тика. H4 («сошлось между сэмплами») структурно исключён:
            // выход по первому сэмплу, а первый же тик подачи оставляет
            // pending ≥ 50 (BatchPartitions=50 из 100 партиций) — финальная
            // ветка тика (удаление заявки) недостижима до применения всего
            // плана.
            var remaining = new List<int>();
            var balanceDeadline = DateTimeOffset.UtcNow.AddSeconds(600);
            while (true)
            {
                await TickAsync("act2-submit", cluster,
                    async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);
                var progress = await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}");
                if (progress is not null
                    && progress.Contains("\"mode\":\"balance\"", StringComparison.Ordinal))
                {
                    using var doc = JsonDocument.Parse(progress);
                    if (doc.RootElement.TryGetProperty("partitions_remaining", out var rem))
                        remaining.Add(rem.GetInt32());
                    break;
                }

                if (DateTimeOffset.UtcNow >= balanceDeadline)
                {
                    var context = await DiagContextAsync(cluster);
                    throw new Xunit.Sdk.XunitException(
                        "act2: прогресс mode=balance не прочитан за 600 с; заявка="
                        + (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}") ?? "<нет>")
                        + "; " + context + "; тики: " + string.Join(" | ", diag));
                }

                await Task.Delay(3000, ct);
            }

            var outcomeAfterBalance = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            remaining.Should().NotBeEmpty(
                "батчи поданы — прогресс mode=balance создан; сэмплы remaining: "
                + string.Join(",", remaining)
                + "; journal=" + await fixture.GetAsync($"/kafkaworker/work/{cluster}")
                + "; outcome=" + outcomeAfterBalance);
            var progressLive = await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}");
            progressLive.Should().NotBeNull(
                "balance жив — окно держится живым прогресс-ключом, а не темпом сходимости; "
                + "journal=" + await fixture.GetAsync($"/kafkaworker/work/{cluster}"))
                .And.Contain("\"balance\"");

            // Старая заявка при идущей balance: тик НЕ снимает (гвард — прогресс
            // balance жив; в balance-ветке экспирационных точек нет вообще).
            await PutTicketAsync($"/kafkaworker/rebalances/{cluster}", nowUnix - 3700);
            await TickAsync("guard2-old-ticket", cluster,
                async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);
            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().NotBeNull(
                "идущая ребалансировка не снимается никогда (тройной гвард)");

            // Гвард в waiting-точке при живом balance: слепая проба (endpoints
            // занулены) + старая заявка → НЕ expired, waiting-cluster.
            var endpointsRaw = await fixture.GetAsync($"/kafka/clusters/{cluster}/endpoints");
            await fixture.PutAsync("/kafka/clusters/" + cluster + "/endpoints", "");
            await TickAsync("guard3-blind-endpoints", cluster,
                async () => await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct), diag);
            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().NotBeNull(
                "гвард previous mode=balance работает и в endpoints-точке");
            var outcomeLive = await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}");
            outcomeLive.Should().Be(expiredOutcome,
                "гвард previous mode=balance запрещает экспирацию — исход Act1 не перезаписан; "
                + "journal=" + await fixture.GetAsync($"/kafkaworker/work/{cluster}"));
            await fixture.PutAsync($"/kafka/clusters/{cluster}/endpoints", endpointsRaw!);

            // --- Финал: возврат RF=2 → balance доигрывается → исход done.
            var restoredRaw = (await fixture.GetAsync($"/kafka/clusters/{cluster}/config"))!;
            await fixture.PutAsync($"/kafka/clusters/{cluster}/config",
                restoredRaw.Replace("\"replication_factor\":1", "\"replication_factor\":2", StringComparison.Ordinal));
            var convergeDeadline = DateTimeOffset.UtcNow.AddSeconds(300);
            while (DateTimeOffset.UtcNow < convergeDeadline)
            {
                var tick = await rig.Reassigner.RunAsync((await fixture.SnapshotAsync(cluster))!, ct);
                tick.IsSuccess.Should().BeTrue($"тик сходимости не должен падать: {tick.Error?.Message}");
                if (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}") is null)
                    break;
                await Task.Delay(3000, ct);
            }

            (await fixture.GetAsync($"/kafkaworker/rebalances/{cluster}")).Should().BeNull(
                "заявка снята по сходимости");
            (await fixture.GetAsync($"/kafkaworker/reassignments/{cluster}")).Should().BeNull(
                "прогресс удалён по сходимости");
            (await fixture.GetAsync($"/kafkaworker/ticket_outcomes/{cluster}"))!
                .Should().Contain("\"outcome\":\"done\"", "финал balance пишет исход done");
        }
        finally
        {
            await TeardownAsync(rig, cluster);
        }
    }
}
