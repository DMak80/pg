namespace AdminPanel.Core;

/// <summary>Домен EtcdWorker (adminpanel/02 §2.3.5): декларация состава +
/// managed-узлы /etcd/nodes/*.</summary>
public sealed record EtcdDomainInfo(
    int? DeclaredNodes,
    long? CreatedUnix,
    IReadOnlyList<ManagedNodeView> Nodes);

/// <summary>Managed-узел: номер, состояние, размещение.</summary>
public sealed record ManagedNodeView(int K, string? State, string? PlacedOn);
