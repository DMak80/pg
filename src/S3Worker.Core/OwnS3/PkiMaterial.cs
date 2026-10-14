using OwnS3.Protocol.PeerAuth;

namespace S3Worker.Core.OwnS3;

/// <summary>Материал CA кластера (arch/23 §6, spec §3.2): trust-anchor
/// (name/publicKeyPem) + приватный ключ CA (выпуск сертификатов — только воркеру).
/// Модель — в Core: потребители и PkiStore (Provisioning), и клиент ноды
/// (IOwnS3NodeClient — до-join подпись оркестратором).</summary>
public sealed record ClusterCaMaterial(PeerTrustAnchor Ca, string CaPrivateKeyPem);

/// <summary>Выданный сертификат пира: распарсенный + исходный JSON (заголовок
/// X-OwnS3-Peer-Certificate / env OWNS3_REPLICATION_CERT) + приватный ключ PEM.</summary>
public sealed record IssuedPkiMaterial(PeerCertificate Certificate, string CertificateJson, string PrivateKeyPem);
