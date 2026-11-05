namespace EtcdWorker.Core;

/// <summary>Значения /etcd/nodes/&lt;k&gt;/state (arch/etcdworker/00-etcd-contract.md) — семантика
/// kafka/valkey/s3.</summary>
public static class ManagedNodeStates
{
    public const string NotInitialized = "NOT_INITIALIZED";
    public const string Provisioning = "PROVISIONING";
    public const string Running = "RUNNING";
    public const string Unreachable = "UNREACHABLE";
    public const string Removing = "REMOVING";
}
