using EtcdWorker.IntegrationTests.Docker;

namespace EtcdWorker.IntegrationTests.Etcd;

// Фиксация фактического поведения member-API etcd 3.5 на живом контуре
// (spec §8-риск 2). Инвариант детекта — peer-URL-паттерн управляемого узла,
// имя члена etcd выводит из hostname peer-URL (появляется после старта узла).
// Сверенные факты: unstarted-learner — без имени; в кластере допустим ОДИН
// unstarted-learner (второй → "too many learner members"); повторный add
// того же peer-URL сразу после remove → "member not found" (квирк etcd) —
// потому каждый кейс использует СВОЙ номер узла; member/remove принимает ID
// числом. Состав — глобальное состояние: коллекция без параллелизма, кейс
// чистит своего члена.
[Collection(EtcdWorkerCollection.Name)]
public sealed class MemberApiBehaviorTests(EtcdWorkerEnvironment env)
{
    private EtcdWorkerEnvironment Env { get; } = env;

    // AAA: learner, добавленный по peer-URL, появляется в member list
    // unstarted — без имени и с isLearner=true
    [Fact]
    public async Task MemberAdd_Learner_AppearsUnstartedWithEmptyName()
    {
        ulong memberId = 0;
        try
        {
            // Act
            var added = await Env.Gateway.MemberAddAsync(
                Env.Endpoint, ["http://etcdw-node-1:2380"], isLearner: true,
                TestContext.Current.CancellationToken);

            // Assert
            added.IsSuccess.Should().BeTrue(added.Error?.Message);
            memberId = added.Value.Id;
            added.Value.IsLearner.Should().BeTrue();
            var list = await Env.Gateway.MemberListAsync(Env.Endpoint, TestContext.Current.CancellationToken);
            list.IsSuccess.Should().BeTrue();
            var member = list.Value.Should().ContainSingle(m => m.PeerUrls.Contains("http://etcdw-node-1:2380")).Subject;
            member.Name.Should().BeNullOrEmpty("имя члена etcd выводит из hostname peer-URL после старта узла");
            member.IsLearner.Should().BeTrue();
        }
        finally
        {
            if (memberId != 0)
                await Env.Gateway.MemberRemoveAsync(Env.Endpoint, memberId, TestContext.Current.CancellationToken);
        }
    }

    // AAA: unstarted-learner выводится из состава без ошибок
    [Fact]
    public async Task MemberRemove_Unstarted_Succeeds()
    {

        // Arrange
        var added = await Env.Gateway.MemberAddAsync(
            Env.Endpoint, ["http://etcdw-node-2:2380"], isLearner: true,
            TestContext.Current.CancellationToken);
        added.IsSuccess.Should().BeTrue(added.Error?.Message);

        // Act
        var removed = await Env.Gateway.MemberRemoveAsync(
            Env.Endpoint, added.Value.Id, TestContext.Current.CancellationToken);

        // Assert
        removed.IsSuccess.Should().BeTrue();
        var list = await Env.Gateway.MemberListAsync(Env.Endpoint, TestContext.Current.CancellationToken);
        list.Value.Should().NotContain(m => m.PeerUrls.Contains("http://etcdw-node-2:2380"));
    }

    // AAA: promote до старта узла — etcd отвечает ошибкой (not started),
    // клиент получает Result.Failed, исключение наружу не летит
    [Fact]
    public async Task MemberPromote_UnstartedLearner_Fails()
    {

        // Arrange
        var added = await Env.Gateway.MemberAddAsync(
            Env.Endpoint, ["http://etcdw-node-3:2380"], isLearner: true,
            TestContext.Current.CancellationToken);
        added.IsSuccess.Should().BeTrue(added.Error?.Message);

        try
        {
            // Act
            var promoted = await Env.Gateway.MemberPromoteAsync(
                Env.Endpoint, added.Value.Id, TestContext.Current.CancellationToken);

            // Assert
            promoted.IsSuccess.Should().BeFalse("promote unstarted learner отклоняется etcd до синхронизации");
        }
        finally
        {
            await Env.Gateway.MemberRemoveAsync(Env.Endpoint, added.Value.Id, TestContext.Current.CancellationToken);
        }
    }

    // AAA: peer-URL управляемого члена содержит паттерн /etcdw-node-<k>: —
    // инвариант детекта managed-состава (arch/etcdworker/01-placement.md)
    [Fact]
    public async Task MemberList_ManagedPatternParse()
    {
        ulong memberId = 0;
        try
        {
            // Arrange
            var added = await Env.Gateway.MemberAddAsync(
                Env.Endpoint, ["http://etcdw-node-4:2380"], isLearner: true,
                TestContext.Current.CancellationToken);
            added.IsSuccess.Should().BeTrue(added.Error?.Message);
            memberId = added.Value.Id;

            // Act
            var list = await Env.Gateway.MemberListAsync(Env.Endpoint, TestContext.Current.CancellationToken);

            // Assert
            list.IsSuccess.Should().BeTrue();
            list.Value.Should().Contain(m =>
                m.PeerUrls.Any(u => u.Contains("/etcdw-node-4:", StringComparison.Ordinal)));
        }
        finally
        {
            if (memberId != 0)
                await Env.Gateway.MemberRemoveAsync(Env.Endpoint, memberId, TestContext.Current.CancellationToken);
        }
    }
}
