using Xunit;

// xUnit v3: disable parallelization for integration tests — e2e tests share
// etcd/docker state and must run sequentially.
[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly, DisableTestParallelization = true)]
