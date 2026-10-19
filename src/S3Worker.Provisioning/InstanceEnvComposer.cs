using S3Worker.Core.OwnS3;
using OwnS3.Protocol.PeerAuth;

namespace S3Worker.Provisioning;

/// <summary>
/// Единая точка сборки env инстанса ownS3 (arch/24 §4, spec §3.6): клиентская
/// грань кластера — per-cluster secrets (OWNS3_ROOT_*/OWNS3_ACCESS_KEYS__*)
/// + репликационный материал
/// PKI (значения, не пути — прецедент owns3/07 §11). Чистый класс без etcd —
/// потребители (P2/D/надзор/ротатор) приносят готовый материал из PkiStore.
/// Состав mesh env'ом НЕ передаётся (env-seed ownS3 — legacy-only, owns3/07 §2).
/// </summary>
public sealed class InstanceEnvComposer
{
    // advertisedEndpoint строит вызывающий по правилу endpoints (§3.5 граница G):
    // http://{AdvertisedHost ?? addr.Host}:{addr.S3Port}.
    public IReadOnlyDictionary<string, string> Compose(
        OwnS3NodeSecrets secrets,
        IssuedPkiMaterial nodePki,
        PeerTrustAnchor ca,
        string advertisedEndpoint)
    {
        var env = new Dictionary<string, string>(secrets.ToContainerEnv())
        {
            ["OWNS3_DATA_DIR"] = "/data",
            ["OWNS3_REPLICATION_CERT"] = nodePki.CertificateJson,
            ["OWNS3_REPLICATION_PRIVATE_KEY"] = nodePki.PrivateKeyPem,
            ["OWNS3_REPLICATION_TRUST_CA"] = ca.ToJson(),
            ["OWNS3_REPLICATION_ADVERTISED_ENDPOINT"] = advertisedEndpoint,
        };
        return env;
    }
}
