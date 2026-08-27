using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Probes;

// Хранилище состояния проб: volatile-замена ссылки — зеркалит SnapshotStore (spec §4.9).
[InjectAsSingleton(typeof(IProbeStateStore))]
public sealed class ProbeResultsStore : IProbeStateStore
{
    private volatile ProbeState? _current;

    public ProbeState? Current => _current;

    public void Replace(ProbeState state) => _current = state;
}
