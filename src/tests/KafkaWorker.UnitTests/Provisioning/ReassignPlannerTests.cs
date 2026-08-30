using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

/// <summary>
/// Юнит-тесты чистых функций планов reassignment (spec t02 §3.3/§3.4):
/// drain (замещение/снижение RF/minISR-guard), balance (RF-цель, лидер,
/// детерминизм), Pending/DrainComplete/HasUnderReplicated.
/// </summary>
public sealed class ReassignPlannerTests
{
    // Фабрика топика-факта: реплики по партициям, опционально ISR по партициям.
    private static KafkaTopicView TopicView(string name, int[][] replicas, int[][]? isr = null)
        => new(name, replicas.Length,
            replicas.Select(p => (IReadOnlyList<int>)[.. p]).ToList(),
            isr?.Select(p => (IReadOnlyList<int>)[.. p]).ToList());

    [Fact]
    public void PlanDrain_замещает_реплику_когда_целей_достаточно()
    {
        // Arrange: топик t: p0 [1,2,4], p1 [2,4,1]; drain broker4, цели 1..3.
        var topics = new[] { TopicView("t", [[1, 2, 4], [2, 4, 1]]) };

        // Act
        var plan = ReassignPlanner.PlanDrain(topics, drainBrokerId: 4, targetBrokerIds: [1, 2, 3],
            minIsrByTopic: new Dictionary<string, int>());

        // Assert: замещение с хвоста — лидеры (первая реплика) сохранены,
        // цель 3 добирается в хвост; порядок старых реплик сохранён.
        Assert.True(plan.IsSuccess);
        var moves = plan.Value.OrderBy(m => m.Partition).ToList();
        Assert.Equal(2, moves.Count);
        Assert.Equal([1, 2, 3], moves[0].Replicas);
        Assert.Equal([2, 1, 3], moves[1].Replicas);
    }

    [Fact]
    public void PlanDrain_снижение_RF_при_нехватке_целей()
    {
        // Arrange: p0 [1,2,4] RF=3, drain 4, цели только 1 и 2.
        var topics = new[] { TopicView("t", [[1, 2, 4]]) };

        // Act
        var plan = ReassignPlanner.PlanDrain(topics, drainBrokerId: 4, targetBrokerIds: [1, 2],
            minIsrByTopic: new Dictionary<string, int>());

        // Assert: добор невозможен — min(len(old), цели)=2, RF 3→2.
        Assert.True(plan.IsSuccess);
        var move = Assert.Single(plan.Value);
        Assert.Equal([1, 2], move.Replicas);
    }

    [Fact]
    public void PlanDrain_отказ_когда_минISR_недостижим()
    {
        // Arrange: p0 [1,2,3], drain 3, единственная цель 1; minISR=2.
        var topics = new[] { TopicView("t", [[1, 2, 3]]) };

        // Act
        var plan = ReassignPlanner.PlanDrain(topics, drainBrokerId: 3, targetBrokerIds: [1],
            minIsrByTopic: new Dictionary<string, int> { ["t"] = 2 });

        // Assert: план смог бы дать только 1 реплику < minISR — отказ с
        // человекочитаемой причиной (spec §5.2 D3).
        Assert.False(plan.IsSuccess);
        Assert.Contains("min.insync.replicas", plan.Error!.Message);
        Assert.Contains("t", plan.Error.Message);
    }

    [Fact]
    public void PlanDrain_партиции_без_drain_не_в_плане()
    {
        // Arrange: p0 с drain-репликой, p2 [1,2] — без drain.
        var topics = new[] { TopicView("t", [[1, 2, 4], [1, 2], [1, 2]]) };

        // Act
        var plan = ReassignPlanner.PlanDrain(topics, drainBrokerId: 4, targetBrokerIds: [1, 2, 3],
            minIsrByTopic: new Dictionary<string, int>());

        // Assert: move только для p0 — партиции без drain не двигаются.
        Assert.True(plan.IsSuccess);
        var move = Assert.Single(plan.Value);
        Assert.Equal(0, move.Partition);
    }

    [Fact]
    public void PlanBalance_восстанавливает_RF_и_сохраняет_лидера()
    {
        // Arrange: факт RF=2, configRf=3, три живых брокера.
        var topics = new[] { TopicView("t", [[1, 2], [2, 1]]) };

        // Act
        var plan = ReassignPlanner.PlanBalance(topics, targetBrokerIds: [1, 2, 3], configRf: 3);

        // Assert: множество реплик = все три цели, первая реплика (лидер)
        // факта сохранена, добор least-loaded в хвост.
        var moves = plan.OrderBy(m => m.Partition).ToList();
        Assert.Equal(2, moves.Count);
        Assert.Equal(1, moves[0].Replicas[0]);
        Assert.Equal([1, 2, 3], moves[0].Replicas.OrderBy(r => r));
        Assert.Equal(2, moves[1].Replicas[0]);
        Assert.Equal([2, 1, 3], moves[1].Replicas);
    }

    [Fact]
    public void PlanBalance_детерминизм()
    {
        // Arrange: один вход — два прогона.
        var topics = new[] { TopicView("a", [[1, 2], [2, 1]]), TopicView("b", [[3, 1], [1, 3]]) };

        // Act
        var first = ReassignPlanner.PlanBalance(topics, [1, 2, 3], configRf: 3);
        var second = ReassignPlanner.PlanBalance(topics, [1, 2, 3], configRf: 3);

        // Assert: последовательность move и реплик идентична (стабильность
        // между тиками — осцилляций нет, spec §3.4).
        Assert.Equal(first.Count, second.Count);
        for (var i = 0; i < first.Count; i++)
        {
            Assert.Equal(first[i].Topic, second[i].Topic);
            Assert.Equal(first[i].Partition, second[i].Partition);
            Assert.Equal(first[i].Replicas, second[i].Replicas);
        }
    }

    [Fact]
    public void PlanBalance_internal_формулы()
    {
        // Arrange: __consumer_offsets p0 [1,2]; configRf юзер-топиков = 3
        // (на internal не влияет — их RF-цель min(3, B)).
        var topics = new[] { TopicView("__consumer_offsets", [[1, 2]]) };

        // Act
        var plan = ReassignPlanner.PlanBalance(topics, targetBrokerIds: [1, 2, 3], configRf: 3);
        var pending = ReassignPlanner.Pending(topics, plan);

        // Assert: internal получил 3 реплики (min(3,B)); факт [1,2] != план
        // → партиция в Pending.
        var move = Assert.Single(plan);
        Assert.Equal(3, move.Replicas.Count);
        var pend = Assert.Single(pending);
        Assert.Equal(0, pend.Partition);
    }

    [Fact]
    public void DrainComplete_и_HasUnderReplicated()
    {
        // Arrange: drain 3 вне реплик; одна партиция с USR (Isr < Replicas);
        // второй топик без ISR-данных (null).
        var ok = new[] { TopicView("t", [[1, 2]]) };
        var usr = new[] { TopicView("t", [[1, 2]], [[1]]) };
        var noIsr = new[] { TopicView("t", [[1, 2]], null) };

        // Act / Assert: drain-брокер вне всех реплик → завершён.
        Assert.True(ReassignPlanner.DrainComplete(ok, drainBrokerId: 3));
        Assert.False(ReassignPlanner.DrainComplete(usr, drainBrokerId: 1));

        // USR есть (Isr [1] < Replicas [1,2]) → true; null = данных нет → false.
        Assert.True(ReassignPlanner.HasUnderReplicated(usr));
        Assert.False(ReassignPlanner.HasUnderReplicated(noIsr));
    }

    [Fact]
    public void Pending_сортировка_по_топику_и_партиции()
    {
        // Arrange: планы для партиций в обратном лексикографическом порядке.
        var topics = new[] { TopicView("b", [[1, 2], [1, 2]]), TopicView("a", [[1, 2]]) };
        var plan = new List<ReassignMove>
        {
            new("b", 1, [1, 2, 3]),
            new("b", 0, [1, 2, 3]),
            new("a", 0, [1, 2, 3]),
        };

        // Act
        var pending = ReassignPlanner.Pending(topics, plan);

        // Assert: кандидаты батча отсортированы (Topic, Partition).
        Assert.Equal([("a", 0), ("b", 0), ("b", 1)],
            pending.Select(m => (m.Topic, m.Partition)).ToList());
    }
}
