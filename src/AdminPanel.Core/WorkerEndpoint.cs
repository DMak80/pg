namespace AdminPanel.Core;

/// <summary>
/// Живой инстанс API воркера: lease-ключ /pgworker/api/&lt;id&gt; (или
/// /kafkaworker/api/&lt;id&gt;, arch/02 §2.3.1/§2.3.2). Ключ есть — инстанс жив
/// и Url валиден; гаснет с lease (≤15 c) вместе с instances/&lt;id&gt;.
/// CertThumbprint — sha256 серта на грани инстанса; null — старая версия
/// не пишет (статус unknown).
/// </summary>
public sealed record WorkerEndpoint(string InstanceId, string Url, long SinceUnix, string? CertThumbprint = null);
