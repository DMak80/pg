using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using OwnS3.Protocol.PeerAuth;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// PeerAuth-E2E mesh (t39, spec фаза 5): docker-сценарии поверх
/// OwnS3MeshE2eEnvironment (изоляция/teardown/телеметрия/ассерт чистоты —
/// окружение, docs/e2e-isolation.md + docs/e2e-launch.md). Ключи/CA генерируются
/// в коде теста (MeshPeerAuthMaterial → PeerCertificateAuthority) и поставляются
/// узлам env-ЗНАЧЕНИЯМИ. Состав key-записей строится join'ом оркестратора
/// (POST /admin/replication/join, root SigV4) либо ручным PUT targets (env-seed
/// key-записей не существует — глава 07 §2). Проверяется живая mesh-механика:
/// доставка уведомления+фетч на peer-подписи, отзыв/возврат, курсоры,
/// сосуществование legacy/key, работа без trust-anchor.
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class PeerAuthScenarios
{
    // ── 1. mesh 3 ноды на key-записях (spec фаза 5, сценарий 1) ──

    [Fact]
    public async Task KeyMesh_JoinFullMesh_ReplicatesToAll()
    {
        DockerTrait.SkipIfUnavailable();
        // Arrange — материал (CA + серты 3 узлов) генерируется в коде теста;
        // identity+trust+advertised у всех (env через окружение)
        var auth = MeshPeerAuthMaterial.Create(3, "os3m-keymesh-ca");
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("keymesh", 3, async env =>
        {
            var ct = TestContext.Current.CancellationToken;

            // Act — join пар (0,1)(0,2)(1,2) оркестратором (по одному вызову на пару, §4.4)
            foreach (var (a, b) in new[] { (0, 1), (0, 2), (1, 2) })
            {
                using var join = await AdminJoinAsync(env, a, env.NodeInternalUrl(b), ct);
                // Assert — двустороннее соседство подтверждено (200)
                join.StatusCode.Should().Be(HttpStatusCode.OK, $"join пары ({a},{b})");
            }

            // Assert — у каждой ноды 2 соседа auth=key с серт-именами (статус §10)
            for (var i = 0; i < 3; i++)
            {
                var neighbors = await StatusNeighborsAsync(env, i, ct);
                neighbors.EnumerateArray().Should().HaveCount(2);
                neighbors.EnumerateArray().Should().OnlyContain(
                    n => n.GetProperty("auth").GetString() == "key");
                neighbors.EnumerateArray().Select(n => n.GetProperty("name").GetString())
                    .Should().BeEquivalentTo(
                        Enumerable.Range(0, 3).Where(j => j != i).Select(auth.CertNameOf));
            }

            // Assert — доставка на peer-подписи: PUT на A → объект на B и C
            var bucket = $"km-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, ct);
            var body = "keymesh-body"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k1", body, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k1", ct) is { } onB
                        && onB.SequenceEqual(body)
                    && await MeshClients.GetObjectOrNullAsync(env, 2, bucket, "k1", ct) is { } onC
                        && onC.SequenceEqual(body),
                "PUT узла-0 доезжает до узлов 1 и 2 (peer-подпись)");

            // Assert — идемпотентность повторного join той же пары: 200, записей не прибавилось
            using var repeat = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct);
            repeat.StatusCode.Should().Be(HttpStatusCode.OK);
            (await StatusNeighborsAsync(env, 0, ct)).EnumerateArray().Should().HaveCount(2);
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken, peerAuth: auth);
    }

    // ── 2. отзыв revoke=true на всех (сценарий 2) ──

    [Fact]
    public async Task RevokeOnAll_DenyBlocksJoin_UndenyAllows()
    {
        DockerTrait.SkipIfUnavailable();
        var auth = MeshPeerAuthMaterial.Create(2, "os3m-revoke-ca");
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("revoke", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            var bucket = $"rv-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, ct);

            // Arrange — mesh пары (join 0→1), данные k1 доставлены
            using (var join = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
                join.StatusCode.Should().Be(HttpStatusCode.OK);
            var k1 = "revoke-k1"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k1", k1, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k1", ct) is { } got
                        && got.SequenceEqual(k1),
                "k1 до узла-1 до отзыва");

            // Act — отзыв НА ОБОИХ (delete ?revoke=true — отлучение оркестрацией, §4.5)
            using (var del0 = await AdminDeleteTargetAsync(env, 0, auth.CertNameOf(1), revoke: true, ct))
                del0.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using (var del1 = await AdminDeleteTargetAsync(env, 1, auth.CertNameOf(0), revoke: true, ct))
                del1.StatusCode.Should().Be(HttpStatusCode.Accepted);
            // данные, записанные в период отзыва (соседей нет — остаются локальными)
            var k2 = "revoke-k2"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k2", k2, ct);

            // Assert — повторный join → 403 (deny-list по имени на грани узла-1, §4.4)
            using (var denied = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
                denied.StatusCode.Should().Be(HttpStatusCode.Forbidden);
            // deny-list обоих узлов видит записи (имя + публичный ключ)
            (await AdminDenyListAsync(env, 0, ct)).EnumerateArray().Should().ContainSingle(e =>
                e.GetProperty("name").GetString() == auth.CertNameOf(1)
                && e.GetProperty("publicKeyPem").GetString() == auth.Nodes[1].Cert.PublicKeyPem);
            (await AdminDenyListAsync(env, 1, ct)).EnumerateArray().Should().ContainSingle(e =>
                e.GetProperty("name").GetString() == auth.CertNameOf(0));

            // Act — снятие отзыва на обоих (DELETE denylist/{name} → 202, §4.5)
            using (var undeny0 = await AdminDenyRemoveAsync(env, 0, auth.CertNameOf(1), ct))
                undeny0.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using (var undeny1 = await AdminDenyRemoveAsync(env, 1, auth.CertNameOf(0), ct))
                undeny1.StatusCode.Should().Be(HttpStatusCode.Accepted);
            // join снова проходит (200)
            using (var rejoined = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
                rejoined.StatusCode.Should().Be(HttpStatusCode.OK);
            // reconcile обоим — «данные доезжают reconcile» (spec §5 сц. 2)
            using (var rec0 = await MeshClients.AdminReconcileAsync(env, 0, ct))
                rec0.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using (var rec1 = await MeshClients.AdminReconcileAsync(env, 1, ct))
                rec1.StatusCode.Should().Be(HttpStatusCode.Accepted);

            // Assert — k2 периода отзыва доехал, объекты сравнялись
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k2", ct) is { } got
                        && got.SequenceEqual(k2),
                "k2 периода отзыва доезжает после снятия отзыва");
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken, peerAuth: auth);
    }

    // ── 3. базовое удаление без revoke (сценарий 3) ──

    [Fact]
    public async Task DeleteWithoutRevoke_JoinBack_CursorContinues()
    {
        DockerTrait.SkipIfUnavailable();
        var auth = MeshPeerAuthMaterial.Create(2, "os3m-cursor-ca");
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("cursor", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            var bucket = $"cu-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, ct);

            // Arrange — mesh пары, k1 доставлен; счётчик applied источника зафиксирован
            using (var join = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
                join.StatusCode.Should().Be(HttpStatusCode.OK);
            var k1 = "cursor-k1"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k1", k1, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k1", ct) is { } got
                        && got.SequenceEqual(k1),
                "k1 до узла-1");
            var appliedBefore = await AppliedCountAsync(env, 1, auth.CertNameOf(0), ct);

            // Act — базовое удаление БЕЗ revoke (202; курсор сохраняется, §4.5)
            // и возврат join'ом
            using (var del = await AdminDeleteTargetAsync(env, 0, auth.CertNameOf(1), revoke: false, ct))
                del.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using (var rejoined = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
                rejoined.StatusCode.Should().Be(HttpStatusCode.OK);
            var k2 = "cursor-k2"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k2", k2, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k2", ct) is { } got
                        && got.SequenceEqual(k2),
                "k2 до узла-1 после возврата (курсор продолжился)");

            // Assert — применено ровно ОДНО новое событие: повторного применения k1
            // нет (дедуп (источник, seqNo), курсор продолжился с места — §4.5)
            (await AppliedCountAsync(env, 1, auth.CertNameOf(0), ct))
                .Should().Be(appliedBefore + 1, "курсор продолжился — k1 повторно не применялся");
            (await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k1", ct) ?? [])
                .Should().Equal(k1, "k1 не тронут повторным применением");
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken, peerAuth: auth);
    }

    // ── 4. сосуществование legacy/key (сценарий 4) ──

    [Fact]
    public async Task MixedMesh_LegacyPairAndKeyPair_BothDeliver()
    {
        DockerTrait.SkipIfUnavailable();
        // 3 ноды: A (0) — legacy-узел БЕЗ identity; B (1) и C (2) — key-узлы
        // (identity+CA). Legacy-пара A↔B — pair-креды per-пара (зарегистрированы
        // env ОБЕИХ сторон окружением, паттерн t35), состав — hot-PUT admin-грани
        // (глава 07 §2, прецедент A5/A9; env-seed считает runId только окружение).
        var auth = MeshPeerAuthMaterial.Create(3, "os3m-mixed-ca");
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("mixed", 3, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            var bucket = $"mx-{env.RunTag}";

            // Arrange — legacy-записи A↔B (4-польный legacy-формат PUT) и key-пара
            // B↔C join'ом оркестратора
            using (var putA = await MeshClients.AdminPutTargetsAsync(env, 0,
                       [(env.NodeName(1), env.NodeInternalUrl(1))], ct))
                putA.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using (var putB = await MeshClients.AdminPutTargetsAsync(env, 1,
                       [(env.NodeName(0), env.NodeInternalUrl(0))], ct))
                putB.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using (var join = await AdminJoinAsync(env, 1, env.NodeInternalUrl(2), ct))
                join.StatusCode.Should().Be(HttpStatusCode.OK);

            // Assert — состав B смешанный: legacy-запись A + key-запись C (статус §10)
            var neighborsB = await StatusNeighborsAsync(env, 1, ct);
            neighborsB.EnumerateArray().Should().HaveCount(2);
            neighborsB.EnumerateArray()
                .First(n => n.GetProperty("name").GetString() == env.NodeName(0))
                .GetProperty("auth").GetString().Should().Be("legacy");
            neighborsB.EnumerateArray()
                .First(n => n.GetProperty("name").GetString() == auth.CertNameOf(2))
                .GetProperty("auth").GetString().Should().Be("key");

            // Нога (1) — A→B (SigV4): PUT на A → объект на B
            await MeshClients.CreateBucketAsync(env, 0, bucket, ct);
            var onA = "mixed-a"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "obj-a", onA, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "obj-a", ct) is { } got
                        && got.SequenceEqual(onA),
                "нога A→B (SigV4): obj-a на B");
            // каскад B→C довозит бакет+объект на C (нужно до записи obj-c)
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 2, bucket, "obj-a", ct) is { } got
                        && got.SequenceEqual(onA),
                "каскад B→C (peer): obj-a на C");

            // Ноги (2)+(3) — PUT на B → И на A (SigV4), И на C (peer из СМЕШАННОГО
            // списка targets: B шлёт legacy-ногу A и key-ногу C одновременно)
            var onB = "mixed-b"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 1, bucket, "obj-b", onB, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 0, bucket, "obj-b", ct) is { } got
                        && got.SequenceEqual(onB),
                "нога B→A (SigV4): obj-b на A");
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 2, bucket, "obj-b", ct) is { } got
                        && got.SequenceEqual(onB),
                "нога B→C (peer): obj-b на C");

            // Нога (4) — C→B (peer): PUT на C → объект на B
            var onC = "mixed-c"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 2, bucket, "obj-c", onC, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "obj-c", ct) is { } got
                        && got.SequenceEqual(onC),
                "нога C→B (peer): obj-c на B");
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken,
            peerAuth: auth with { IdentityOf = i => i != 0, TrustOf = i => i != 0 });
    }

    // ── 5. без trust-anchor (сценарий 5) ──

    [Fact]
    public async Task NoTrustAnchor_AdminJoin400_EdgeJoin403_ManualKeyTargetsWork()
    {
        DockerTrait.SkipIfUnavailable();
        // 2 ноды с identity БЕЗ trust-anchor env (ручной режим состава — spec §5 сц. 5)
        var auth = MeshPeerAuthMaterial.Create(2, "os3m-notrust-ca");
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("notrust", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            var bucket = $"nt-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, ct);

            // Assert — admin join на A → 400 с пояснением (нет trust-anchor:
            // сертификат ответа соседа не проверить, spec §4.4 п.1)
            using (var adminJoin = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
            {
                adminJoin.StatusCode.Should().Be(HttpStatusCode.BadRequest);
                (await adminJoin.Content.ReadAsStringAsync(ct)).Should().Contain("error");
            }

            // Assert — прямой POST /replication/join peer-подписью на грань B → 403
            // (trust-anchor не задан у соседа — до-join аутентификация §4.4 п.3)
            using (var key0 = ECDsa.Create())
            {
                key0.ImportFromPem(auth.Nodes[0].PrivatePem);
                using var edgeJoin = await SendEdgeJoinAsync(env, 1, key0, auth.CertNameOf(0),
                    Convert.ToBase64String(Encoding.UTF8.GetBytes(auth.Nodes[0].CertJson)),
                    env.NodeInternalUrl(0), ct);
                edgeJoin.StatusCode.Should().Be(HttpStatusCode.Forbidden);
            }

            // Act — ручной состав: PUT targets с publicKey-записями на обоих (без CA)
            using (var put0 = await AdminPutKeyTargetsAsync(env, 0,
                       [(auth.CertNameOf(1), env.NodeInternalUrl(1), auth.Nodes[1].Cert.PublicKeyPem)], ct))
                put0.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using (var put1 = await AdminPutKeyTargetsAsync(env, 1,
                       [(auth.CertNameOf(0), env.NodeInternalUrl(0), auth.Nodes[0].Cert.PublicKeyPem)], ct))
                put1.StatusCode.Should().Be(HttpStatusCode.Accepted);

            // Assert — доставка работает без CA (исходящая peer-подпись identity,
            // входящая верификация ключом записи — §4.3)
            var body = "notrust-body"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k", body, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k", ct) is { } got
                        && got.SequenceEqual(body),
                "доставка на key-записях без CA");
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken,
            peerAuth: auth with { TrustOf = _ => false });
    }

    // ── 6. полная перезапись deny-list (PUT /replication/denylist, глава 07 §2) ──

    [Fact]
    public async Task DenyReplaceFull_BlocksJoinThenEmptyListAllows()
    {
        DockerTrait.SkipIfUnavailable();
        // 2 ноды с identity+CA; вызывающий перезаписи — сертификат узла-0
        // (оркестратор, не обязан числиться соседом узла-1). Очистку списка
        // выполняет узел-1 СВОИМ сертификатом: после первой перезаписи узел-0
        // отозван на узле-1 — по §4.2 п.5 отозванный не переписывает список
        // (иначе 403); узел-1 не отозван у самого себя, серт того же CA.
        var auth = MeshPeerAuthMaterial.Create(2, "os3m-denyfull-ca");
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("denyfull", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            using var orchestratorKey = ECDsa.Create();
            orchestratorKey.ImportFromPem(auth.Nodes[0].PrivatePem);
            var orchestratorCert = Convert.ToBase64String(Encoding.UTF8.GetBytes(auth.Nodes[0].CertJson));
            using var node1Key = ECDsa.Create();
            node1Key.ImportFromPem(auth.Nodes[1].PrivatePem);
            var node1Cert = Convert.ToBase64String(Encoding.UTF8.GetBytes(auth.Nodes[1].CertJson));

            // Act — перезапись на узле-1 списка, содержащего узел-0
            using var put = await SendEdgeDenyReplaceAsync(env, 1, orchestratorKey,
                auth.CertNameOf(0), orchestratorCert,
                [(auth.CertNameOf(0), auth.Nodes[0].Cert.PublicKeyPem)], ct);

            // Assert — 202; admin-грань узла-1 видит запись (имя + ключ)
            put.StatusCode.Should().Be(HttpStatusCode.Accepted);
            (await AdminDenyListAsync(env, 1, ct)).EnumerateArray().Should().ContainSingle(e =>
                e.GetProperty("name").GetString() == auth.CertNameOf(0)
                && e.GetProperty("publicKeyPem").GetString() == auth.Nodes[0].Cert.PublicKeyPem
                && e.GetProperty("revokedAtUnixMs").GetInt64() > 0);

            // Assert — join узла-0 → узла-1 отказан (deny-list решает будущие join)
            using (var denied = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
                denied.StatusCode.Should().Be(HttpStatusCode.Forbidden);

            // Act — перезапись пустым списком (снятие всех отзывов одним вызовом);
            // вызывающий — узел-1 (узел-0 отозван и получил бы 403)
            using var clear = await SendEdgeDenyReplaceAsync(env, 1, node1Key,
                auth.CertNameOf(1), node1Cert, [], ct);

            // Assert — 202; список пуст; join снова проходит
            clear.StatusCode.Should().Be(HttpStatusCode.Accepted);
            (await AdminDenyListAsync(env, 1, ct)).GetArrayLength().Should().Be(0);
            using (var rejoined = await AdminJoinAsync(env, 0, env.NodeInternalUrl(1), ct))
                rejoined.StatusCode.Should().Be(HttpStatusCode.OK);
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken, peerAuth: auth);
    }

    [Fact]
    public async Task DenyReplace_DoesNotBreakActiveLinks()
    {
        DockerTrait.SkipIfUnavailable();
        // 3 ноды: join (0,1) и (0,2); перезапись на узле-0 списка без узла-1
        // (и с узлом-2) — действующие линки продолжают доставлять
        var auth = MeshPeerAuthMaterial.Create(3, "os3m-denyactive-ca");
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("denyactive", 3, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            var bucket = $"da-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, ct);

            // Arrange — активная mesh 0→1, 0→2; k1 доставлен обоим
            foreach (var b in new[] { 1, 2 })
            {
                using var join = await AdminJoinAsync(env, 0, env.NodeInternalUrl(b), ct);
                join.StatusCode.Should().Be(HttpStatusCode.OK, $"join пары (0,{b})");
            }
            var k1 = "denyactive-k1"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k1", k1, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k1", ct) is { } on1
                        && on1.SequenceEqual(k1)
                        && await MeshClients.GetObjectOrNullAsync(env, 2, bucket, "k1", ct) is { } on2
                        && on2.SequenceEqual(k1),
                "k1 до узлов 1 и 2 до перезаписи");

            // Act — перезапись deny-list узла-0: список БЕЗ узла-1 (линк с ним —
            // действующий) и С узлом-2 (даже отозванный в списке — линк жив)
            using var orchestratorKey = ECDsa.Create();
            orchestratorKey.ImportFromPem(auth.Nodes[0].PrivatePem);
            using var put = await SendEdgeDenyReplaceAsync(env, 0, orchestratorKey,
                auth.CertNameOf(0), Convert.ToBase64String(Encoding.UTF8.GetBytes(auth.Nodes[0].CertJson)),
                [(auth.CertNameOf(2), auth.Nodes[2].Cert.PublicKeyPem)], ct);

            // Assert — 202; доставка продолжает работать по обоим линкам (критерий 7)
            put.StatusCode.Should().Be(HttpStatusCode.Accepted);
            var k2 = "denyactive-k2"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "k2", k2, ct);
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k2", ct) is { } on1
                        && on1.SequenceEqual(k2),
                "k2 до узла-1 после перезаписи (список без него — линк не тронут)");
            await MeshClients.PollUntilAsync(async () =>
                    await MeshClients.GetObjectOrNullAsync(env, 2, bucket, "k2", ct) is { } on2
                        && on2.SequenceEqual(k2),
                "k2 до узла-2 после перезаписи (узел в списке — действующий линк всё равно жив)");

            // Assert — состав targets узла-0 не тронут перезаписью (оба соседа на месте)
            (await StatusNeighborsAsync(env, 0, ct)).EnumerateArray().Should().HaveCount(2);
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken, peerAuth: auth);
    }

    // ── хелперы admin-грани (root SigV4) и peer-грани ──

    /// <summary>POST /admin/replication/join инициатору (тело {"endpoint"} — §4.4 п.1).</summary>
    private static Task<HttpResponseMessage> AdminJoinAsync(OwnS3MeshE2eEnvironment env, int initiator,
        string neighborEndpoint, CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new { endpoint = neighborEndpoint });
        return MeshClients.AdminClient(env, initiator).SendAsync("POST", "/admin/replication/join", "", body, ct);
    }

    /// <summary>DELETE /admin/replication/targets/{name}[?revoke=true] (§4.5).</summary>
    private static Task<HttpResponseMessage> AdminDeleteTargetAsync(OwnS3MeshE2eEnvironment env, int node,
        string name, bool revoke, CancellationToken ct) =>
        MeshClients.AdminClient(env, node).SendAsync("DELETE", $"/admin/replication/targets/{name}",
            revoke ? "revoke=true" : "", null, ct);

    /// <summary>GET /admin/replication/denylist → массив entries (§4.5).</summary>
    private static async Task<JsonElement> AdminDenyListAsync(OwnS3MeshE2eEnvironment env, int node,
        CancellationToken ct)
    {
        using var response = await MeshClients.AdminClient(env, node).SendAsync(
            "GET", "/admin/replication/denylist", "", null, ct);
        response.EnsureSuccessStatusCode();
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct)).RootElement
            .GetProperty("entries");
    }

    /// <summary>DELETE /admin/replication/denylist/{name} — снятие отзыва (§4.5).</summary>
    private static Task<HttpResponseMessage> AdminDenyRemoveAsync(OwnS3MeshE2eEnvironment env, int node,
        string name, CancellationToken ct) =>
        MeshClients.AdminClient(env, node).SendAsync("DELETE", $"/admin/replication/denylist/{name}",
            "", null, ct);

    /// <summary>GET /admin/replication/status → массив neighbors (глава 07 §10).</summary>
    private static async Task<JsonElement> StatusNeighborsAsync(OwnS3MeshE2eEnvironment env, int node,
        CancellationToken ct)
    {
        var status = await MeshClients.AdminGetStatusAsync(env, node, ct);
        return JsonDocument.Parse(status).RootElement.GetProperty("neighbors");
    }

    /// <summary>Счётчик applied соседа-источника из статуса узла (исходы применения
    /// из этого источника, глава 07 §10) — базис ассерта «курсор продолжился».</summary>
    private static async Task<long> AppliedCountAsync(OwnS3MeshE2eEnvironment env, int node,
        string sourceName, CancellationToken ct)
    {
        var neighbors = await StatusNeighborsAsync(env, node, ct);
        return neighbors.EnumerateArray()
            .First(n => n.GetProperty("name").GetString() == sourceName)
            .GetProperty("counters").GetProperty("applied").GetInt64();
    }

    /// <summary>PUT /admin/replication/targets с key-записями (§4.5: PUT работает
    /// для обоих видов; env-seed key-записей не существует — только admin-грань).</summary>
    private static Task<HttpResponseMessage> AdminPutKeyTargetsAsync(OwnS3MeshE2eEnvironment env,
        int node, IReadOnlyList<(string Name, string Endpoint, string PublicKeyPem)> targets,
        CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new
        {
            targets = targets.Select(t => new
            {
                name = t.Name, endpoint = t.Endpoint, auth = "key", publicKeyPem = t.PublicKeyPem,
            }).ToList(),
        });
        return MeshClients.AdminClient(env, node).SendAsync("PUT", "/admin/replication/targets", "", body, ct);
    }

    /// <summary>Прямой POST /replication/join на replication-грань узла с хоста
    /// (peer-подпись §4.2 вручную — по образцу SendPeerOnAsync Api-сценариев;
    /// host — authority хост-URL, дата — настоящее время контейнера).</summary>
    private static async Task<HttpResponseMessage> SendEdgeJoinAsync(OwnS3MeshE2eEnvironment env,
        int node, ECDsa signingKey, string credential, string certificateBase64,
        string advertisedEndpoint, CancellationToken ct)
    {
        // Arrange — тело и заголовки схемы OWNS3-PEER (§4.2)
        var body = JsonSerializer.Serialize(new { advertisedEndpoint });
        var payloadHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(body)))
            .ToLowerInvariant();
        var at = DateTimeOffset.UtcNow;
        var peerDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var host = new Uri(env.NodeHostUrl(node)).Authority;
        var (authorization, _, _) = PeerRequestSigner.Sign(signingKey, credential, "POST",
            "/replication/join", "",
            new List<(string, string)>
            {
                ("host", host),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash),
            }, payloadHash, at);

        // Act — запрос на replication-грань
        using var request = new HttpRequestMessage(HttpMethod.Post,
            $"{env.NodeHostUrl(node)}/replication/join");
        request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body));
        request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate", certificateBase64);
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        return await http.SendAsync(request, ct);
    }

    /// <summary>Прямой PUT /replication/denylist на replication-грань узла с хоста
    /// (peer-подпись §4.2 вручную — по образцу SendEdgeJoinAsync; вызывающий —
    /// сертификат из материала mesh, подписанный CA, не обязан числиться соседом;
    /// дата — настоящее время контейнера).</summary>
    private static async Task<HttpResponseMessage> SendEdgeDenyReplaceAsync(OwnS3MeshE2eEnvironment env,
        int node, ECDsa signingKey, string credential, string certificateBase64,
        IReadOnlyList<(string Name, string PublicKeyPem)> entries, CancellationToken ct)
    {
        // Arrange — тело и заголовки схемы OWNS3-PEER (§4.2)
        var body = JsonSerializer.Serialize(new
        {
            entries = entries.Select(e => new { name = e.Name, publicKeyPem = e.PublicKeyPem }).ToList(),
        });
        var payloadHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(body)))
            .ToLowerInvariant();
        var at = DateTimeOffset.UtcNow;
        var peerDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var host = new Uri(env.NodeHostUrl(node)).Authority;
        var (authorization, _, _) = PeerRequestSigner.Sign(signingKey, credential, "PUT",
            "/replication/denylist", "",
            new List<(string, string)>
            {
                ("host", host),
                ("x-owns3-peer-date", peerDate),
                ("x-owns3-peer-content-sha256", payloadHash),
            }, payloadHash, at);

        // Act — запрос на replication-грань
        using var request = new HttpRequestMessage(HttpMethod.Put,
            $"{env.NodeHostUrl(node)}/replication/denylist");
        request.Content = new ByteArrayContent(Encoding.UTF8.GetBytes(body));
        request.Headers.TryAddWithoutValidation("x-owns3-peer-date", peerDate);
        request.Headers.TryAddWithoutValidation("x-owns3-peer-content-sha256", payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        request.Headers.TryAddWithoutValidation("X-OwnS3-Peer-Certificate", certificateBase64);
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        return await http.SendAsync(request, ct);
    }
}
