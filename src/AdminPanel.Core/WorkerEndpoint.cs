namespace AdminPanel.Core;

/// <summary>
/// Живой инстанс API воркера: lease-ключ /pgworker/api/&lt;id&gt; (или
/// /kafkaworker/api/&lt;id&gt;, arch/02 §2.3.1/§2.3.2). Ключ есть — инстанс жив
/// и Url валиден; гаснет с lease (≤15 c) вместе с instances/&lt;id&gt;.
/// </summary>
public sealed record WorkerEndpoint(string InstanceId, string Url, long SinceUnix);
