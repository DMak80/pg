using FluentAssertions;
using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

// ACL-план роли app (t03 Ф3, arch/16 §2.3/E): канонические 7 binding'ов;
// diff — создать недостающие, удалить лишние у app, чужих принципалов не трогать.
public class AclPlanTests
{
    [Fact]
    public void Target_CanonicalSevenBindingsForAppRole()
    {
        // Arrange / Act: канонический план роли app (16 §2.3).
        var target = AclPlan.Target();

        // Assert: 7 ACL, все LITERAL/Allow, принципал User:app, wildcard-ресурс.
        target.Should().HaveCount(7);
        target.Should().OnlyContain(b =>
            b.Principal == "User:app" && b.Permission == KafkaAclPermission.Allow
            && b.PatternType == KafkaAclPatternType.Literal && b.ResourceName == "*");
        target.Where(b => b.ResourceType == KafkaAclResourceType.Topic)
            .Should().OnlyContain(b =>
                b.Operation == KafkaAclOperation.Read || b.Operation == KafkaAclOperation.Write
                || b.Operation == KafkaAclOperation.Describe);
        target.Where(b => b.ResourceType == KafkaAclResourceType.Group)
            .Should().OnlyContain(b =>
                b.Operation == KafkaAclOperation.Read || b.Operation == KafkaAclOperation.Describe);
        target.Where(b => b.ResourceType == KafkaAclResourceType.TransactionalId)
            .Should().OnlyContain(b =>
                b.Operation == KafkaAclOperation.Write || b.Operation == KafkaAclOperation.Describe);
    }

    [Fact]
    public void Diff_MissingCreated_SuperfluousDeleted_ForeignUntouched()
    {
        // Arrange: факт = половина цели + лишний ACL app (Create на TOPIC *) +
        // ACL чужого принципала User:someone.
        var current = new List<KafkaAclBinding>
        {
            new(KafkaAclResourceType.Topic, "*", KafkaAclPatternType.Literal, "User:app",
                KafkaAclOperation.Read, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Topic, "orders", KafkaAclPatternType.Literal, "User:app",
                KafkaAclOperation.Create, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Group, "*", KafkaAclPatternType.Literal, "User:someone",
                KafkaAclOperation.Read, KafkaAclPermission.Allow),
        };

        // Act: дифф.
        var (create, delete) = AclPlan.Diff(current);

        // Assert: создать 6 недостающих, удалить 1 лишний у app, чужого не трогать.
        create.Should().HaveCount(6);
        delete.Should().ContainSingle(b => b.Operation == KafkaAclOperation.Create);
        delete.Should().NotContain(b => b.Principal == "User:someone");
    }

    [Fact]
    public void Diff_Converged_NoChanges()
    {
        // Arrange: факт == цель.
        // Act / Assert: пустой дифф (идемпотентность).
        var (create, delete) = AclPlan.Diff([.. AclPlan.Target()]);
        create.Should().BeEmpty();
        delete.Should().BeEmpty();
    }
}
