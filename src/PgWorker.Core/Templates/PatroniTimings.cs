namespace PgWorker.Core.Templates;

/// <summary>
/// Канонические тайминги Patroni (arch/14 §2.1/§5 C, t09) — единый источник
/// для bootstrap.dcs SPILO_CONFIGURATION и для конвергенции динамического
/// DCS-конфига в надзоре. Полы Patroni 4.x: loop_wait≥1, retry_timeout≥3,
/// ttl≥20 и правило loop_wait + 2*retry_timeout ≤ ttl — заниженное значение
/// Patroni молча поднимает до пола и записывает обратно в DCS (t09:
/// заявленный ttl=5 превращался в 20 без ведома воркера). Нода, начавшая
/// работу на чужом/дефолтном/мусорном конфиге, приводится к канону
/// конвергенцией (PATCH /config) — «старое с плохими параметрами» не живёт
/// параллельно канону. Построение патча конвергенции — DcsConfigConvergence
/// (t11: тайминги + postgresql.parameters).
/// </summary>
public static class PatroniTimings
{
    public const int Ttl = 20;
    public const int LoopWait = 1;
    public const int RetryTimeout = 3;
    public const bool SynchronousMode = true;
}
