using AdminPanel.Core.S3;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер s3-домена: канон arch/23 §2.1/§3 (приёмочные) + толерантность §2.
public sealed class S3ParserTests
{
    // Arrange: канонические фикстуры §2.1/§3. Act: ParseClusters.
    // Assert: поля дословно; pki — молча (unknownKeys не растит).
    [Fact]
    public void ParseClusters_Canonical_BuildsModel()
    {
        var result = S3Parser.ParseClusters(EtcdFixtures.LoadKv("S3/clusters-canonical.json"));

        var main = Assert.Single(result.Clusters, c => c.Name == "backupsmain");
        Assert.Equal(S3ClusterState.Active, main.State);
        Assert.Equal("owns3", main.Platform);
        Assert.Equal(3, main.NodesDeclared);
        Assert.Equal(1760156800L, main.CreatedUnix);
        Assert.Equal("h1:18001,h2:18002,h3:18003", main.Endpoints);
        Assert.Equal(2, main.Links.Count);
        Assert.Equal(("east1", "east2"), (main.Links[0].A, main.Links[0].B));
        Assert.Equal(["west1"], main.DenyList);
        Assert.Equal("OK", main.Connectivity!.State);
        Assert.Equal(1760156900L, main.Connectivity.CheckedUnix);
        Assert.Equal("ok", main.Connectivity.Links["node1"]["node2"]);
        Assert.Equal("SYNCED", main.Sync!.State);
        Assert.Equal(0, main.Sync.Pairs[0].Missing);

        // secrets-ключ: root_user + access_keys (имена/политики).
        Assert.Equal("s3w-backupsmain-root", main.Secrets!.RootUser);
        Assert.Equal(2, main.Secrets.AccessKeys.Count);
        Assert.Equal(("backups", "read-write"),
            (main.Secrets.AccessKeys[0].Name, main.Secrets.AccessKeys[0].Policy));
        Assert.Equal(("panel", "read-only"),
            (main.Secrets.AccessKeys[1].Name, main.Secrets.AccessKeys[1].Policy));

        var east1 = Assert.Single(main.NodesList, n => n.Name == "east1");
        Assert.Equal("RUNNING", east1.State);
        Assert.Equal("h1", east1.PlacedOn);
        Assert.Equal("h1", east1.Placement!.DockerHost);
        Assert.Equal(2m, east1.Resources!.Cpu);
        Assert.Equal(4, east1.Resources.MemGi);
        Assert.Equal(200, east1.Resources.DiskGi);
        Assert.Equal(107374182400L, east1.Usage!.UsedBytes);
        Assert.Equal(214748364800L, east1.Usage.TotalBytes);

        // swarm-селектор и комбинированный (конъюнкция атрибутов).
        Assert.Equal("dc1", Assert.Single(main.NodesList, n => n.Name == "east2").Placement!.SwarmLabels!["geo"]);
        var swarmy = Assert.Single(result.Clusters, c => c.Name == "swarmy");
        var s1 = Assert.Single(swarmy.NodesList, n => n.Name == "s1");
        Assert.Equal("h2", s1.Placement!.DockerHost);
        Assert.Equal(2, s1.Placement.SwarmLabels!.Count);

        // заявка NOT_INITIALIZED (0 инстансов — декларация без развёртывания) и TO_REMOVE.
        var claim = Assert.Single(result.Clusters, c => c.Name == "newclaim");
        Assert.Equal(S3ClusterState.NotInitialized, claim.State);
        Assert.Equal(0, claim.NodesDeclared);
        Assert.True(claim.EtcdBackup);                    // config etcd_backup:true → EtcdBackup
        Assert.Empty(claim.NodesList);
        Assert.Equal(S3ClusterState.ToRemove, Assert.Single(result.Clusters, c => c.Name == "dying").State);

        // pki-ключи пропущены МОЛЧА: unknownKeys считает только future_feature.
        Assert.Equal(1, result.UnknownKeyCount);
        Assert.Empty(result.Errors);
    }

    // Arrange: вырожденные. Act: ParseClusters. Assert: parseError-записи,
    // незнакомое state → Active, пустой endpoints → null, битые ресурсы → null-поля.
    [Fact]
    public void ParseClusters_Tolerance_TableRows()
    {
        var result = S3Parser.ParseClusters(EtcdFixtures.LoadKv("S3/clusters-tolerance.json"));

        Assert.Contains(result.Errors, e => e.Key == "/s3/clusters/bad/config");
        Assert.Contains(result.Errors, e => e.Key == "/s3/clusters/bad/links");
        var odd = Assert.Single(result.Clusters, c => c.Name == "odd");
        Assert.Equal(S3ClusterState.Active, odd.State);          // STRANGE_NEW → Active
        Assert.Null(odd.Endpoints);                              // пробельный → null
        var n1 = Assert.Single(odd.NodesList, n => n.Name == "n1");
        Assert.Null(n1.Resources);                               // cpu не число → весь resources битый (parseError)
        Assert.Null(n1.Usage);                                   // нет total_bytes → битый usage
        Assert.Equal("DOWN", odd.Connectivity!.State);
        Assert.Equal("all nodes are silent", odd.Connectivity.ProbeError);
        Assert.Equal("DIVERGED", odd.Sync!.State);
        Assert.Equal(1024L, odd.Sync.Pairs[0].LagBytes);
        Assert.Equal("diff page", odd.Sync.LastError);
    }

    // Arrange: rotations-фикстура. Act: ParseRotations. Assert: тикет жив,
    // битый JSON / нет requested_unix → parseError.
    [Fact]
    public void ParseRotations_CanonicalAndTolerance()
    {
        var result = S3Parser.ParseRotations(EtcdFixtures.LoadKv("S3/rotations.json"));

        var ticket = Assert.Single(result.Tickets);
        Assert.Equal("backupsmain", ticket.Cluster);
        Assert.Equal(1760157000L, ticket.RequestedUnix);
        Assert.Equal("admin", ticket.RequestedBy);
        Assert.Equal(2, result.Errors.Count);
    }
}
