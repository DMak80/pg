// Табличные тесты диффов ленты событий (u09): prev/next-пары на каждый
// переход. AAA — на каждый кейс Arrange (prev/next) → Act (diff*) → Assert
// (события в store). Store — общий синглтон: каждый кейс читает свой срез
// событий через снапшот длины до/после (использование капа/дедупа — в
// eventsStore.test.ts).
import { afterEach, describe, expect, it } from 'vitest';
import {
  diffBackupsStorage, diffClusterDetails, diffClusters, diffEtcdDomain, diffHa,
  diffKafka, diffOverview, diffS3, diffValkey,
} from '../events/eventsDiff';
import { clearEvents, getEvents } from '../events/eventsStore';
import type {
  BackupStorageDto, ClusterDto, ClusterSummaryDto, EtcdDomainDto, HaScopeSummaryDto,
  KafkaClusterDto, OverviewDto, S3ClusterDto, ValkeyClusterDto,
} from '../api/dto';

// События с момента mark в ХРОНОЛОГИЧЕСКОМ порядке (store кладёт новые сверху).
function eventsSince(mark: number) {
  return getEvents().slice(0, getEvents().length - mark).reverse();
}

afterEach(() => clearEvents());

// ——— pg: список кластеров ———

describe('diffClusters', () => {
  const summary = (name: string, opts?: { notInitialized?: boolean; toRemove?: boolean }): ClusterSummaryDto =>
    ({
      name, dbName: null, bucketsCount: 1, incomplete: false,
      notInitialized: opts?.notInitialized ?? false, toRemove: opts?.toRemove ?? false,
      shardsTotal: 1, shardsWithMaster: 1, activeMoves: 0,
      sharded: true, synchronousModeStrict: false,
    });

  it('появление/исчезновение/state-change кластера', () => {
    // Arrange — prev: demo ACTIVE; next: demo TO_REMOVE + new + без old
    const prev = [summary('demo'), summary('old')];
    const next = [summary('demo', { toRemove: true }), summary('new')];
    const mark = getEvents().length;

    // Act
    diffClusters(prev, next);

    // Assert — три события: state-change demo, appeared new, gone old
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.entityKind}:${e.entity}:${e.type}`)).toEqual([
      'cluster:demo:state-change',
      'cluster:new:appeared',
      'cluster:old:gone',
    ]);
    expect(ev[0].from).toBe('ACTIVE');
    expect(ev[0].to).toBe('TO_REMOVE');
  });

  it('первый тик (prev нет) — все диффы молчат', () => {
    // Arrange/Act/Assert — prev undefined → без событий (u09/D8)
    const mark = getEvents().length;
    diffClusters(undefined, [summary('demo')]);
    expect(eventsSince(mark)).toEqual([]);
  });
});

// ——— pg: детали кластера ———

describe('diffClusterDetails', () => {
  const details = (overrides: Partial<ClusterDto>): ClusterDto =>
    ({
      name: 'demo', dbname: 'app', bucketsCount: 2, createdUnix: 1,
      incomplete: false, state: 'ACTIVE', sharded: true, synchronousModeStrict: false,
      shards: [], buckets: [], pendingMoves: [], heals: [], standNodes: [],
      work: null, ...overrides,
    }) as ClusterDto;

  it('переход state бакета ACTIVE→SYNCING с detail переезда', () => {
    // Arrange
    const prev = details({
      buckets: [{ id: 3, owner: 's1', state: 'ACTIVE', move: null, ageSec: null }],
    });
    const next = details({
      buckets: [{
        id: 3, owner: 's2', state: 'SYNCING',
        move: { owner: 's1', target: 's2', startedUnix: 1, updatedUnix: 2, phase: 'x', lastError: null },
        ageSec: 5,
      }],
    });
    const mark = getEvents().length;

    // Act
    diffClusterDetails(prev, next);

    // Assert — state-change с from→to и detail owner → target
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'pg', entityKind: 'bucket', entity: 'demo/bucket_3',
      type: 'state-change', from: 'ACTIVE', to: 'SYNCING',
      detail: 's1 → s2',
    });
  });

  it('повторная заявка на тот же бакет (другой op) — не нема (#11)', () => {
    // Arrange — заявка move поставлена и снята, затем поставлена rollback
    const base = details({
      pendingMoves: [
        { bucketId: 3, bucket: 'bucket_3', op: 'move', to: 's2', requestedUnix: 1, requestedBy: 'ops' },
      ],
    });
    const empty = details({ pendingMoves: [] });
    const rollback = details({
      pendingMoves: [
        { bucketId: 3, bucket: 'bucket_3', op: 'rollback', to: null, requestedUnix: 2, requestedBy: 'ops' },
      ],
    });
    const mark = getEvents().length;

    // Act — снятие move, постановка rollback
    diffClusterDetails(base, empty);
    diffClusterDetails(empty, rollback);

    // Assert — 3 события: gone (move), appeared (rollback); по одному на смену
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.type}:${e.detail}`)).toEqual([
      'gone:заявка move исполнена или отвергнута',
      'appeared:заявка rollback поставлена',
    ]);
  });
});

// ——— pg: HA ———

describe('diffHa', () => {
  const scope = (name: string, leader: string | null): HaScopeSummaryDto =>
    ({ scope: name, leaderName: leader, matched: true, cluster: 'demo', shard: 's1' }) as HaScopeSummaryDto;

  it('смена лидера — entity <C>/<X>, не scope <C>-<X> (#22)', () => {
    // Arrange — скоп demo-s1: pgw-a → pgw-b
    const prev = [scope('demo-s1', 'pgw-a')];
    const next = [scope('demo-s1', 'pgw-b')];
    const mark = getEvents().length;

    // Act
    diffHa(prev, next);

    // Assert — entity demo/s1 (deep-link на кластер, не /clusters/demo-s1)
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0].entity).toBe('demo/s1');
    expect(ev[0]).toMatchObject({ type: 'state-change', from: 'pgw-a', to: 'pgw-b' });
  });

  it('no leader → лидер появился', () => {
    // Arrange
    const mark = getEvents().length;
    diffHa([scope('demo-s1', null)], [scope('demo-s1', 'pgw-a')]);

    // Assert
    const ev = eventsSince(mark);
    expect(ev[0]).toMatchObject({ from: 'no leader', to: 'pgw-a' });
  });
});

// ——— kafka ———

describe('diffKafka', () => {
  const kafka = (overrides: Partial<KafkaClusterDto>): KafkaClusterDto =>
    ({
      name: 'kc', state: 'ACTIVE', brokers: 3, replicationFactor: 3, minIsr: 2,
      brokersList: [], topics: [], groups: [],
      rotation: null, caRotation: null, rebalance: null,
      ticketOutcome: null, ...overrides,
    }) as KafkaClusterDto;

  it('цепочка исходов app→admin: событие на каждую смену содержимого (#11a)', () => {
    // Arrange — исход done по app сменился исходом по admin
    const prev = kafka({
      ticketOutcome: { kind: 'password-app', outcome: 'done', reason: null, requestedUnix: 1, finishedUnix: 5 },
    });
    const next = kafka({
      ticketOutcome: { kind: 'password-admin', outcome: 'done', reason: null, requestedUnix: 2, finishedUnix: 6 },
    });
    const mark = getEvents().length;

    // Act
    diffKafka(prev, next);

    // Assert — outcome эмитится по смене kind, хотя outcome был не-null
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({ type: 'outcome', to: 'done', entity: 'kc' });
    expect(ev[0].detail).toContain('password-admin');
  });

  it('появление rebalance-заявки и её снятие с last_error из work (#11b)', () => {
    // Arrange — work.lastError прокинут в detail исчезновения
    const withTicket = kafka({ rebalance: { requestedUnix: 1, requestedBy: 'ops' } } as Partial<KafkaClusterDto>);
    const gone = kafka({
      rebalance: null,
      work: { op: 'rebalance', phase: 'done', updatedUnix: 2, lastError: 'broker down' },
    });
    const mark = getEvents().length;

    // Act
    diffKafka(withTicket, gone);

    // Assert — gone с last_error
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.type}:${e.detail}`)).toEqual([
      'gone:заявка rebalance исполнена или отвергнута (last_error: broker down)',
    ]);
  });

  it('переписанная заявка (то же значение presence) не эмитит', () => {
    // Arrange/Act/Assert — rebalance жив до и после: без событий
    const mark = getEvents().length;
    diffKafka(kafka({ rebalance: { requestedUnix: 1, requestedBy: 'ops' } } as Partial<KafkaClusterDto>),
      kafka({ rebalance: { requestedUnix: 9, requestedBy: 'ops' } } as Partial<KafkaClusterDto>));
    expect(eventsSince(mark)).toEqual([]);
  });
});

// ——— valkey ———

describe('diffValkey', () => {
  const valkey = (overrides: Partial<ValkeyClusterDto>): ValkeyClusterDto =>
    ({
      name: 'vc', state: 'ACTIVE', maxmemoryBytes: 1, maxmemoryPolicy: 'noeviction',
      nodesList: [], rotation: null, caRotation: null, ticketOutcome: null,
      ...overrides,
    }) as ValkeyClusterDto;

  it('исход expired→done по смене содержимого (#11a)', () => {
    // Arrange
    const prev = valkey({
      ticketOutcome: { kind: 'ca', outcome: 'expired', reason: null, requestedUnix: 1, finishedUnix: 5 },
    });
    const next = valkey({
      ticketOutcome: { kind: 'ca', outcome: 'done', reason: null, requestedUnix: 2, finishedUnix: 6 },
    });
    const mark = getEvents().length;

    // Act
    diffValkey(prev, next);

    // Assert — смена expired→done не молчит
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({ type: 'outcome', to: 'done' });
  });
});

// ——— s3 ———

describe('diffS3', () => {
  const s3 = (overrides: Record<string, unknown>): S3ClusterDto =>
    ({
      name: 'sc', state: 'ACTIVE', nodesList: [], rotation: null, ...overrides,
    }) as unknown as S3ClusterDto;

  it('state-переход кластера + исчезновение ноды', () => {
    // Arrange
    const prev = s3({ nodesList: [{ name: 'n1', state: 'RUNNING' }] });
    const next = s3({ state: 'TO_REMOVE', nodesList: [] });
    const mark = getEvents().length;

    // Act
    diffS3(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.entityKind}:${e.type}`)).toEqual([
      'cluster:state-change',
      'node:gone',
    ]);
  });
});

// ——— backups-storage ———

describe('diffBackupsStorage', () => {
  const storage = (shards: { cluster: string; shard: string; restoreState?: string | null; drillState?: string | null }[],
    orphans: { prefix: string; registryState?: string | null }[]): BackupStorageDto =>
    ({
      configured: true, notConfiguredReason: null, endpoints: [], bucket: 'b',
      health: null, buckets: [], etcd: null, etcdSnapshots: null,
      liveUsedBytes: 0,
      clusters: shards.map((s) => ({
        cluster: s.cluster, sizeBytes: 0, policy: null,
        shards: [{
          cluster: s.cluster, shard: s.shard, sizeBytes: 0, fullsCount: 0,
          walSegmentCount: 0, hasS3Only: false, orphan: false,
          restoreState: s.restoreState ?? null, drillState: s.drillState ?? null,
        }],
      })),
      foreignPrefixes: [],
      orphans: orphans.map((o) => ({
        prefix: o.prefix, kind: 'shard', sizeBytes: 0, inWorkerRegistry: true,
        registryState: o.registryState ?? null, firstSeenUnix: 1, ttlLeftSec: null,
      })),
      inventoryUpdatedUnix: 1, inventoryError: null, backupsDisabled: null,
    }) as unknown as BackupStorageDto;

  it('restore: поставлена → завершилась (outcome, #30j)', () => {
    // Arrange
    const prev = storage([{ cluster: 'demo', shard: 's1', restoreState: 'RUNNING' }], []);
    const next = storage([{ cluster: 'demo', shard: 's1' }], []);
    const mark = getEvents().length;

    // Act
    diffBackupsStorage(prev, next);

    // Assert — исход как у drill (outcome), не безликое gone
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({ entityKind: 'restore', type: 'outcome', to: 'done' });
    expect(ev[0].detail).toContain('RUNNING');
  });

  it('drill RUNNING→SUCCEEDED — outcome', () => {
    // Arrange
    const prev = storage([{ cluster: 'demo', shard: 's1', drillState: 'RUNNING' }], []);
    const next = storage([{ cluster: 'demo', shard: 's1', drillState: 'SUCCEEDED' }], []);
    const mark = getEvents().length;

    // Act/Assert
    diffBackupsStorage(prev, next);
    const ev = eventsSince(mark);
    expect(ev[0]).toMatchObject({ entityKind: 'drill', type: 'outcome', to: 'SUCCEEDED' });
  });

  it('сирота: появление, null→OBSERVED (#30k), удаление', () => {
    // Arrange — префикс вне реестра (null) → в реестре OBSERVED → удалена
    const prev = storage([], [{ prefix: 'orphan/x', registryState: null }]);
    const next = storage([], [{ prefix: 'orphan/x', registryState: 'OBSERVED' }]);
    const empty = storage([], []);
    const mark = getEvents().length;

    // Act
    diffBackupsStorage(prev, next);
    diffBackupsStorage(next, empty);

    // Assert — state-change from '—' + gone
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.type}:${e.from ?? ''}→${e.to ?? ''}`)).toEqual([
      'state-change:—→OBSERVED',
      'gone:→',
    ]);
  });

  it('первый тик (prev нет) — молчание', () => {
    // Arrange/Act/Assert
    const mark = getEvents().length;
    diffBackupsStorage(undefined, storage([], []));
    expect(eventsSince(mark)).toEqual([]);
  });

  it('служебный ключ пулов (S3PoolDto[]) под тем же префиксом — без TypeError и без событий (#28-регресс)', () => {
    // Arrange — ответ ['backups-storage','s3-pools'] — МАССИВ пулов, не DTO
    // сводки (нет clusters): shape-guard обязан молча пропустить оба тика,
    // не роняя TypeError в счётчик ошибок ленты.
    const pools = [{ pool: 'p1', writeMaster: [], writeReplica: [] }] as unknown as BackupStorageDto;
    const mark = getEvents().length;

    // Act — оба направления (массив→массив, массив→валидный DTO)
    diffBackupsStorage(pools, pools);
    diffBackupsStorage(pools, storage([], []));
    diffBackupsStorage(undefined, storage([], []));

    // Assert — ни событий, ни исключения (бросок упал бы тест)
    expect(eventsSince(mark)).toEqual([]);
  });

  it('позитивный кейс после гварда жив: restore-факт по-прежнему диффится', () => {
    // Arrange
    const prev = storage([{ cluster: 'demo', shard: 's1', restoreState: null }], []);
    const next = storage([{ cluster: 'demo', shard: 's1', restoreState: 'RUNNING' }], []);
    const mark = getEvents().length;

    // Act
    diffBackupsStorage(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({ entityKind: 'restore', type: 'appeared' });
  });
});

// ——— overview: глобальный стоп ———

describe('diffOverview', () => {
  const overview = (backupsDisabled: { updatedUnix: number; updatedBy: string | null } | null): OverviewDto =>
    ({
      alertsCritical: 0, alertsWarning: 0,
      etcd: { reachable: true, endpointsOk: 3, endpointsTotal: 3 },
      clusters: [], activeMoves: [], kafka: null, valkey: null, s3: null,
      snapshotAgeMs: 0, stale: false, backupsDisabled,
    }) as unknown as OverviewDto;

  it('появление стопа и снятие — appeared/gone', () => {
    // Arrange
    const mark = getEvents().length;
    const off = overview(null);
    const on = overview({ updatedUnix: 1761900000, updatedBy: 'ops' });

    // Act
    diffOverview(off, on);
    diffOverview(on, off);

    // Assert
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.entityKind}:${e.entity}:${e.type}`)).toEqual([
      'ticket:backups-disabled:appeared',
      'ticket:backups-disabled:gone',
    ]);
  });

  it('первый тик — молчание (ложного appeared при стоявшем стопе нет)', () => {
    // Arrange/Act/Assert
    const mark = getEvents().length;
    diffOverview(undefined, overview({ updatedUnix: 1761900000, updatedBy: 'ops' }));
    expect(eventsSince(mark)).toEqual([]);
  });
});

// ——— etcd: домен EtcdWorker ———

describe('diffEtcdDomain', () => {
  const domain = (overrides?: Partial<EtcdDomainDto>): EtcdDomainDto =>
    ({
      declaredNodes: 2,
      createdUnix: 1,
      nodes: [
        { k: 1, state: 'PROVISIONING', placedOn: 'local' },
        { k: 2, state: 'RUNNING', placedOn: null },
      ],
      snapshots: null,
      ...overrides,
    }) as EtcdDomainDto;

  it('state-change узла PROVISIONING→RUNNING', () => {
    // Arrange
    const prev = domain();
    const next = domain({
      nodes: [
        { k: 1, state: 'RUNNING', placedOn: 'local' },
        { k: 2, state: 'RUNNING', placedOn: null },
      ],
    });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'node', entity: 'etcdw-node-1',
      type: 'state-change', from: 'PROVISIONING', to: 'RUNNING',
    });
  });

  it('appeared/gone управляемого узла', () => {
    // Arrange — узел 3 появился, узел 2 исчез
    const prev = domain();
    const next = domain({
      nodes: [
        { k: 1, state: 'PROVISIONING', placedOn: 'local' },
        { k: 3, state: 'NOT_INITIALIZED', placedOn: null },
      ],
    });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev.map((e) => `${e.entityKind}:${e.entity}:${e.type}`)).toEqual([
      'node:etcdw-node-3:appeared',
      'node:etcdw-node-2:gone',
    ]);
  });

  it('статус DR-слепков OK→FAILED', () => {
    // Arrange
    const prev = domain({ snapshots: { state: 'OK', lastUploadedUnix: null, lastObject: null, lastSha256: null, sizeBytes: null, intervalMin: null, error: null } });
    const next = domain({ snapshots: { state: 'FAILED', lastUploadedUnix: null, lastObject: null, lastSha256: null, sizeBytes: null, intervalMin: null, error: 'boom' } });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'snapshot', entity: 'etcd',
      type: 'state-change', from: 'OK', to: 'FAILED',
    });
  });

  it('декларация 0→2 (kind config)', () => {
    // Arrange
    const prev = domain({ declaredNodes: 0 });
    const next = domain({ declaredNodes: 2 });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'config', entity: 'etcd',
      type: 'state-change', from: '0', to: '2',
    });
  });

  it('первый тик (prev нет) и без изменений — молчит', () => {
    // Arrange/Act/Assert — prev undefined → без событий
    let mark = getEvents().length;
    diffEtcdDomain(undefined, domain());
    expect(eventsSince(mark)).toEqual([]);
    // повторный тик без изменений → без событий
    mark = getEvents().length;
    diffEtcdDomain(domain(), domain());
    expect(eventsSince(mark)).toEqual([]);
  });

  it('появление ключа слепков: snapshots null→OK (from «—»)', () => {
    // Arrange — в prev ключа /etcdworker/snapshots нет
    const prev = domain({ snapshots: null });
    const next = domain({ snapshots: { state: 'OK', lastUploadedUnix: null, lastObject: null, lastSha256: null, sizeBytes: null, intervalMin: null, error: null } });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert — null отображается «—» (arch/03 §3)
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'snapshot', entity: 'etcd',
      type: 'state-change', from: '—', to: 'OK',
    });
  });

  it('декларация null→3 (только наблюдение → поддерживать 3)', () => {
    // Arrange — в prev ключа /etcd/config нет
    const prev = domain({ declaredNodes: null });
    const next = domain({ declaredNodes: 3 });
    const mark = getEvents().length;

    // Act
    diffEtcdDomain(prev, next);

    // Assert
    const ev = eventsSince(mark);
    expect(ev).toHaveLength(1);
    expect(ev[0]).toMatchObject({
      domain: 'etcd', entityKind: 'config', entity: 'etcd',
      type: 'state-change', from: '—', to: '3',
    });
  });
});
