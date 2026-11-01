// Тесты таблицы маршрутов алертов (u04/D15): kind→route по всем доменам —
// kafka/valkey/s3, pg-кластерные, backup-* per-shard и кластерные, probe,
// etcd/worker, backups-disabled, key-malformed (#30h), непокрытые → null.
import { describe, expect, it } from 'vitest';
import { alertRoute } from './alertRoutes';

describe('alertRoutes', () => {
  // ——— домены kafka/valkey/s3 ———
  it.each([
    ['kafka-rotation-pending', 'kc', '/kafka/kc'],
    ['kafka-topic-missing', 'kc/orders', '/kafka/kc'],
    ['kafka-broker-unreachable', 'kc/broker2', '/kafka/kc'],
    ['valkey-rotation-pending', 'vc', '/valkey/vc'],
    ['valkey-node-not-running', 'vc/n1', '/valkey/vc'],
    ['s3-rotation-pending', 'sc', '/s3/sc'],
    ['s3-connectivity-down', 'sc/n1', '/s3/sc'],
  ])('%s %s → %s', (kind, target, expected) => {
    // Arrange/Act/Assert — доменный префикс + target <C>[/<объект>]
    expect(alertRoute(kind, target)).toBe(expected);
  });

  // ——— pg-кластерные ———
  it.each([
    ['cluster-incomplete', 'demo', '/clusters/demo'],
    ['shard-no-master', 'demo/s1', '/clusters/demo'],
    ['move-frozen-long', 'demo/bucket_3', '/clusters/demo'],
    ['bucket-lost', 'demo/bucket_3', '/clusters/demo'],
    ['provision-stuck', 'demo', '/clusters/demo'],
    ['replica-lag-high', 'demo/s1', '/clusters/demo'],
    ['slot-wal-lost', 'demo/s1/slot_a', '/clusters/demo'],
    ['ha-member-not-streaming', 'demo/s1', '/clusters/demo'],
    ['wal-chain-broken', 'demo/s1', '/clusters/demo'],
  ])('%s %s → %s', (kind, target, expected) => {
    // Arrange/Act/Assert — pg-кластерные kind'ы → страница кластера
    expect(alertRoute(kind, target)).toBe(expected);
  });

  // ——— backup-* per-shard → детали шарда бэкапов ———
  it.each([
    ['backup-full-stale', 'demo/s1', '/backups-storage/demo/s1'],
    ['backup-verify-failed', 'demo/s1/b1', '/backups-storage/demo/s1'],
    ['backup-chain-broken', 'demo/s1', '/backups-storage/demo/s1'],
    ['restore-failed', 'demo/s1/r1', '/backups-storage/demo/s1'],
  ])('%s %s → %s', (kind, target, expected) => {
    // Arrange/Act/Assert — target <C>/<X>[/<id>] → /backups-storage/:c/:x
    expect(alertRoute(kind, target)).toBe(expected);
  });

  // ——— backup-* кластерные/глобальные → /backups-storage ———
  it.each([
    ['backup-orphan', 'demo/s1'],
    ['backup-pool-missing', 'demo'],
    ['backup-storage-quota', 'global'],
    ['backups-disabled', 'global'],
  ])('%s %s → /backups-storage', (kind, target) => {
    // Arrange/Act/Assert
    expect(alertRoute(kind, target)).toBe('/backups-storage');
  });

  // ——— probe/etcd/worker/key-malformed ———
  it('probe sql-target → кластер; patroni-target → /ha', () => {
    // Arrange/Act/Assert — разбор префиксов probe-failed
    expect(alertRoute('probe-failed', 'sql:demo/s1')).toBe('/clusters/demo');
    expect(alertRoute('probe-failed', 'patroni-scope:demo-s1')).toBe('/ha');
  });

  it.each([
    ['etcd-unreachable'],
    ['etcd-no-quorum'],
    ['etcd-alarm'],
    ['etcd-endpoint-down'],
    ['snapshot-stale'],
    ['etcd-snapshot-export-failed'],
  ])('%s → /etcd', (kind) => {
    // Arrange/Act/Assert
    expect(alertRoute(kind, 'whatever')).toBe('/etcd');
  });

  it.each([
    ['worker-api-unreachable', 'kafkaworker/i-1'],
    ['worker-unhealthy', 'pgworker/i-2'],
  ])('%s %s → /workers', (kind, target) => {
    // Arrange/Act/Assert
    expect(alertRoute(kind, target)).toBe('/workers');
  });

  it('key-malformed: etcd-ключ с ведущим / → без ссылки (#30h)', () => {
    // Arrange/Act/Assert — сырой etcd-ключ НЕ превращается в /clusters/''
    expect(alertRoute('key-malformed', '/pgworker/work/demo')).toBeNull();
    expect(alertRoute('key-malformed', '/pgworker/backups-disabled')).toBeNull();
  });

  // ——— непокрытые ———
  it('непокрытый kind → null (без ссылки)', () => {
    // Arrange/Act/Assert
    expect(alertRoute('unknown-kind', 'demo')).toBeNull();
  });
});
