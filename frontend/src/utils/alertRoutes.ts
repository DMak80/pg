// Единая таблица маршрутов алертов (u04, D15): kind-префикс + target-формат
// (`id = kind:target` движков, arch/03 §4/§7.4/§8.4/§12.4) → маршрут SPA.
// Непокрытые kind → null (без ссылки). Потребители: /alerts, лента дашборда,
// /events (клик по событию). Снято с фактических target-форматов кода правил:
// pg — <C>/<X>[/<узел>] (бакеты <C>/bucket_<N>, слоты <C>/<X>/<slot>,
// restore <C>/<X>/<id>, etcd-alarm <memberId>:<type>, endpoints <url>),
// kafka/valkey/s3 — <C> и <C>/<объект>; worker-* — <worker>[/<instance>].

export function alertRoute(kind: string, target: string): string | null {
  // ——— домены kafka/valkey/s3: target = <C> или <C>/<объект> → кластер. ———
  if (kind.startsWith('kafka-')) return kafkaRoute(target);
  if (kind.startsWith('valkey-')) return workerlessDomainRoute('/valkey', target);
  if (kind.startsWith('s3-')) return workerlessDomainRoute('/s3', target);

  // ——— pg/инфраструктура (AlertEngine + доменные worker-алерты). ———
  const cluster = target.split('/')[0] ?? '';

  if (kind === 'etcd-unreachable' || kind === 'etcd-no-quorum') return '/etcd';
  if (kind === 'etcd-alarm') return '/etcd';
  if (kind === 'etcd-endpoint-down') return '/etcd';
  if (kind === 'snapshot-stale') return '/etcd';
  if (kind === 'etcd-snapshot-export-failed' || kind === 'etcd-snapshot-export-stale')
    return '/etcd';

  if (kind === 'worker-api-unreachable' || kind === 'worker-unhealthy') return '/workers';
  if (kind === 'backups-disabled') return '/backups-storage';

  if (kind === 'probe-failed') {
    // target: sql:<C>/<X> | patroni-scope:<scope> | patroni:<scope>/<member>
    if (target.startsWith('sql:')) return `/clusters/${target.slice(4).split('/')[0] ?? ''}`;
    return '/ha'; // patroni-scope / patroni → срез репликации
  }

  // pg-кластерные: cluster-incomplete/cluster-not-initialized/cluster-…
  // (target = <C>); shard-*/replica-*/slot-*/sync-standby-missing/wal-*
  // (target = <C>/<X>[/<узел>]); move-*/bucket-* (target = <C>/bucket_<N>);
  // backup-* per-shard (target = <C>/<X>[/<id|slot>]) → детали шарда бэкапов;
  // key-malformed (target = ключ etcd) → кластер из первого сегмента.
  const shard = target.split('/')[1] ?? '';

  if (kind === 'backup-full-stale' || kind === 'backup-verify-failed'
    || kind === 'backup-chain-broken' || kind === 'backup-wal-tli-gap'
    || kind === 'backup-drill-failed' || kind === 'backup-drill-stale'
    || kind === 'backup-deleting-stuck' || kind === 'restore-failed') {
    // target = <C>/<X>[/<id>] — per-shard.
    return cluster !== '' && shard !== '' && !shard.startsWith('bucket_')
      ? `/backups-storage/${cluster}/${shard}`
      : `/clusters/${cluster}`;
  }

  if (kind === 'backup-orphan' || kind === 'backup-pool-missing'
    || kind === 'backup-pool-unreachable' || kind === 'backup-storage-quota'
    || kind === 'backup-s3-unreachable') {
    // target = <C> (pool-missing/unreachable/quota) или <C>/<X> (orphan).
    return '/backups-storage';
  }

  if (kind === 'provision-stuck' || kind === 'inventory-mismatch') {
    return `/clusters/${cluster}`;
  }

  if (kind === 'key-malformed') {
    // target — СЫРОЙ etcd-ключ (с ведущим '/'): первый сегмент пуст → ссылки
    // нет (deep-link на /clusters/'<' было бы 404; key-malformed — про ключ,
    // а не про сущность панели).
    return cluster !== '' ? `/clusters/${cluster}` : null;
  }

  if (kind.startsWith('cluster-') || kind.startsWith('shard-')
    || kind.startsWith('move-') || kind.startsWith('bucket-')
    || kind.startsWith('replica-') || kind.startsWith('slot-')
    || kind.startsWith('sync-standby-missing') || kind.startsWith('wal-')
    || kind.startsWith('ha-')) {
    return `/clusters/${cluster}`;
  }

  return null; // непокрытые — без ссылки
}

// kafka: target = <C>[/<брокер|топик|группа>] → страница кластера.
function kafkaRoute(target: string): string | null {
  const cluster = target.split('/')[0] ?? '';
  return cluster !== '' ? `/kafka/${cluster}` : null;
}

// valkey/s3: target = <C>[/<нода>] → страница кластера.
function workerlessDomainRoute(prefix: string, target: string): string | null {
  const cluster = target.split('/')[0] ?? '';
  return cluster !== '' ? `${prefix}/${cluster}` : null;
}
