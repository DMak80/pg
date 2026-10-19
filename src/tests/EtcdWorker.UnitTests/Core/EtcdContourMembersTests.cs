using EtcdWorker.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.UnitTests.Core;

// Разбор member list контура (EtcdContourMembers): managed — peer-URL содержит
// паттерн "/etcdw-node-<k>:", прочие — external; EtcdMember собираем прямо в тесте.
public class EtcdContourMembersTests
{
    // Член etcd с заданными peer-URL (имя не дискриминант — оставляем null).
    private static EtcdMember Member(params string[] peerUrls) =>
        new(Id: 1, Name: null, PeerUrls: peerUrls, ClientUrls: [], IsLearner: false);

    private static EtcdMember Member(bool isLearner, params string[] peerUrls) =>
        new(Id: 1, Name: null, PeerUrls: peerUrls, ClientUrls: [], IsLearner: isLearner);

    [Fact]
    public void Managed_PeerUrlWithPattern_ParsesK()
    {
        // Arrange — член с паттерном узла в peer-URL
        var m = Member("http://etcdw-node-3:2380");

        // Act
        var (managed, external) = EtcdContourMembers.Build([m]);

        // Assert — распознан managed с K=3, исходный член сохранён, external пуст
        managed.Should().ContainSingle().Which.K.Should().Be(3);
        managed.Single().Member.Should().Be(m);
        external.Should().BeEmpty();
        EtcdContourMembers.ParseK(m).Should().Be(3);
    }

    [Fact]
    public void PeerUrlWithoutPattern_IsExternal()
    {
        // Arrange — член без паттерна в peer-URL
        var m = Member("http://seed:2380");

        // Act
        var (managed, external) = EtcdContourMembers.Build([m]);

        // Assert — уходит в external, K не распознан
        managed.Should().BeEmpty();
        external.Should().ContainSingle().Which.Should().Be(m);
        EtcdContourMembers.ParseK(m).Should().BeNull();
    }

    [Fact]
    public void NonNumericK_PeerUrl_IsExternal()
    {
        // Arrange — паттерн есть, но номер нечисловой
        var m = Member("http://etcdw-node-abc:2380");

        // Act
        var (managed, external) = EtcdContourMembers.Build([m]);

        // Assert — external: edge-детект совпадает с гвардом API,
        // т.к. это один и тот же EtcdContourMembers
        managed.Should().BeEmpty();
        external.Should().ContainSingle().Which.Should().Be(m);
        EtcdContourMembers.ParseK(m).Should().BeNull();
    }

    [Fact]
    public void MultiplePeerUrls_DecisionOnFirstMatchingUrl()
    {
        // Arrange — первый URL без паттерна, второй с паттерном → managed
        var validAfterSeed = Member(false, "http://seed:2380", "http://etcdw-node-5:2380");

        // Arrange — первый совпавший URL с нечисловым K, второй валидный → external
        var brokenFirst = Member(false, "http://etcdw-node-abc:2380", "http://etcdw-node-5:2380");

        // Act
        var (managedOk, externalOk) = EtcdContourMembers.Build([validAfterSeed]);
        var (managedBad, externalBad) = EtcdContourMembers.Build([brokenFirst]);

        // Assert — решение принимает первый URL с паттерном, дальше не смотрим
        managedOk.Should().ContainSingle().Which.K.Should().Be(5);
        externalOk.Should().BeEmpty();
        managedBad.Should().BeEmpty();
        externalBad.Should().ContainSingle().Which.Should().Be(brokenFirst);
        EtcdContourMembers.ParseK(brokenFirst).Should().BeNull();
    }

    [Fact]
    public void IsLearner_PassesThroughBuild()
    {
        // Arrange — managed- и external-члены с IsLearner=true
        var managedLearner = Member(true, "http://etcdw-node-2:2380");
        var externalLearner = Member(true, "http://seed:2380");

        // Act
        var (managed, external) = EtcdContourMembers.Build([managedLearner, externalLearner]);

        // Assert — флаг IsLearner доезжает сквозь разбор без изменений
        managed.Should().ContainSingle().Which.Member.IsLearner.Should().BeTrue();
        external.Should().ContainSingle().Which.IsLearner.Should().BeTrue();
    }
}
