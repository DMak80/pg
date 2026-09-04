using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// ACL-план роли app (t03, arch/16 §2.3/E): канонический минимум прав
/// deny-by-default кластера — TOPIC * {READ, WRITE, DESCRIBE}, GROUP * {READ,
/// DESCRIBE}, TRANSACTIONAL_ID * {WRITE, DESCRIBE}; все LITERAL/Allow. Diff —
/// чистая функция: создать недостающие, удалить лишние у User:app; ACL чужих
/// принципалов (admin/inter — super.users, сервисные) converge не трогает.
/// </summary>
public static class AclPlan
{
    public const string AppPrincipal = "User:app";

    // Канонический набор роли app: wildcard-ресурсы, LITERAL, Allow.
    public static IReadOnlySet<KafkaAclBinding> Target()
    {
        HashSet<KafkaAclBinding> target = new()
        {
            new(KafkaAclResourceType.Topic, "*", KafkaAclPatternType.Literal,
                AppPrincipal, KafkaAclOperation.Read, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Topic, "*", KafkaAclPatternType.Literal,
                AppPrincipal, KafkaAclOperation.Write, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Topic, "*", KafkaAclPatternType.Literal,
                AppPrincipal, KafkaAclOperation.Describe, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Group, "*", KafkaAclPatternType.Literal,
                AppPrincipal, KafkaAclOperation.Read, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Group, "*", KafkaAclPatternType.Literal,
                AppPrincipal, KafkaAclOperation.Describe, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.TransactionalId, "*", KafkaAclPatternType.Literal,
                AppPrincipal, KafkaAclOperation.Write, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.TransactionalId, "*", KafkaAclPatternType.Literal,
                AppPrincipal, KafkaAclOperation.Describe, KafkaAclPermission.Allow),
        };
        return target;
    }

    // Цель минус факт → создать; лишние ACL принципала app → удалить.
    public static (IReadOnlyList<KafkaAclBinding> Create, IReadOnlyList<KafkaAclBinding> Delete) Diff(
        IReadOnlyList<KafkaAclBinding> current)
    {
        var target = Target();
        var create = target.Except(current).ToList();
        var delete = current
            .Where(b => b.Principal == AppPrincipal && !target.Contains(b))
            .ToList();
        return (create, delete);
    }
}
