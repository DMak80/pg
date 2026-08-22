using AdminPanel.Core;
using AdminPanel.Etcd;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Хранилище снапшота: атомарная замена ссылки, nullable-Current (spec §10.8).
public class SnapshotStoreTests
{
    private static EtcdSnapshot NewSnapshot()
        => new(
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new EtcdStatus(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0),
            [], [], [], [], [], [], 0);

    [Fact]
    public void Current_NullBeforeFirstReplace()
    {
        // Arrange
        var store = new SnapshotStore();

        // Act
        var current = store.Current;

        // Assert
        current.Should().BeNull();
    }

    [Fact]
    public void Replace_SetsCurrentAtomically()
    {
        // Arrange
        var store = new SnapshotStore();
        var snapshot = NewSnapshot();

        // Act
        store.Replace(snapshot);

        // Assert
        store.Current.Should().BeSameAs(snapshot);
    }
}
