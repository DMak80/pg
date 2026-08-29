// Типы DTO REST API (arch/03 §2; фактические поля — C#-DTO t04–t06).
// Nullable-поля C# → '| null'; unix-время → number | null; DateTimeOffset → string.

// Строковый канон статусов бакета (arch/02 §2.1).
export type BucketStateName = 'ACTIVE' | 'SYNCING' | 'FROZEN' | 'ABORTING' | 'NOT_INITIALIZED';

// Канон состояния кластера (arch/03 §2): отсутствие записи о state = ACTIVE.
export type ClusterStateName = 'ACTIVE' | 'NOT_INITIALIZED' | 'TO_REMOVE';

// Канон состояния шарда (t06, arch/03 §2): отсутствие ключа = ACTIVE.
export type ShardStateName = 'ACTIVE' | 'TO_REMOVE';

// POST /api/clusters — тело и ответ (arch/03 §1.1).
// sharded: фронт передаёт всегда; buckets/shards — только при sharded=true
// (для нешардированной не запрашиваются вовсе, сервер нормализует в 1/1).
export interface CreateClusterRequestDto {
  name: string;
  sharded: boolean;
  buckets?: number;
  shards?: number;
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

export interface ClusterCreatedDto {
  name: string;
  dbName: string;
  sharded: boolean;
  bucketsCount: number;
  shardsTotal: number;
  replicas: number;
  requestCpu: string;
  requestMem: string;
  requestDisk: string;
  state: ClusterStateName;
}

// POST /api/clusters/{cluster}/shards — тело и ответ (t06, arch/03 §1.3).
export interface AddShardRequestDto {
  replicas: number;
  requestCpu: number;
  requestMem: number;
  requestDisk: number;
}

export interface ShardAddedDto {
  cluster: string;
  name: string;
  replicas: number;
  requestCpu: string;
  requestMem: string;
  requestDisk: string;
  state: ClusterStateName;
}

// POST /api/clusters/{cluster}/moves — тело и ответ (arch/03 §1.5, 02 §9.7).
export interface MoveBucketsRequestDto {
  from: string;
  to: string;
  buckets: number[];
}

export interface MovesQueuedDto {
  cluster: string;
  from: string;
  to: string;
  queued: number[];
  skipped: number[];
}

// Строка очереди заявок кластера (arch/03 §2): /pgworker/moves/<C>/<bucket>.
export interface MoveTicketDto {
  bucketId: number | null;
  bucket: string;
  op: string;
  to: string | null;
  requestedUnix: number;
  requestedBy: string | null;
}

// Строковый канон severity алертов (arch/03 §1).
export type AlertSeverityName = 'critical' | 'warning' | 'info';

// GET /api/auth/me
export interface SessionDto {
  username: string;
}

// GET /api/overview
export interface OverviewDto {
  alertsCritical: number;
  alertsWarning: number;
  etcd: OverviewEtcdDto;
  clusters: OverviewClusterDto[];
  activeMoves: OverviewMoveDto[];
  snapshotAgeMs: number;
  stale: boolean;
}

export interface OverviewEtcdDto {
  reachable: boolean;
  endpointsOk: number;
  endpointsTotal: number;
}

export interface OverviewClusterDto {
  name: string;
  shards: number;
  buckets: number;
  activeMoves: number;
  masterlessShards: number;
  notInitialized: boolean;
}

export interface OverviewMoveDto {
  cluster: string;
  bucket: number;
  state: BucketStateName;
  owner: string | null;
  target: string | null;
  updatedUnix: number | null;
}

// GET /api/etcd/status
export interface EtcdStatusDto {
  endpoints: EtcdEndpointDto[];
  members: EtcdMemberDto[];
  alarms: EtcdAlarmDto[];
  quorumSuspected: boolean;
  lastRefreshUtc: string;
}

export interface EtcdEndpointDto {
  url: string;
  reachable: boolean;
  latencyMs: number | null;
  version: string | null;
  dbSizeBytes: number | null;
  leaderMemberId: string | null;
  raftTerm: number | null;
  errors: string[];
  active: boolean;
}

export interface EtcdMemberDto {
  id: string;
  name: string | null;
  peerUrls: string[];
  clientUrls: string[];
  isLeader: boolean;
}

export interface EtcdAlarmDto {
  memberId: string;
  type: string;
}

// GET /api/clusters — сводный список.
export interface ClusterSummaryDto {
  name: string;
  dbName: string | null;
  bucketsCount: number;
  incomplete: boolean;
  notInitialized: boolean;
  // config.state=TO_REMOVE (arch/02 §9.4): пометка «к удалению» в списке.
  toRemove: boolean;
  shardsTotal: number;
  shardsWithMaster: number;
  activeMoves: number;
  // Вычисляется сервером (arch/03 §2), как в деталях: false ⟺ 1 бакет и ≤1
  // шард — список рисует прочерк в «Бакеты»/«Шарды».
  sharded: boolean;
}

// GET /api/clusters/{cluster} — детали.
export interface ClusterDto {
  name: string;
  dbName: string | null;
  bucketsCount: number;
  createdUnix: number | null;
  incomplete: boolean;
  state: ClusterStateName;
  // Вычисляется сервером (arch/03 §2): false ⟺ 1 бакет и ≤1 шард —
  // нешардированная БД; скрывает вкладку «Бакеты» на странице деталей.
  sharded: boolean;
  shards: ShardDto[];
  buckets: BucketDto[];
  pendingMoves: MoveTicketDto[]; // очередь заявок переездов (arch/02 §2.3.1)
  heals: HealDto[];
  standNodes: StandNodeDto[];
}

export interface ShardDto {
  name: string;
  // Маркер демонтажа shards/<X>/state (t06, arch/02 §9.6): отсутствие = ACTIVE.
  state: ShardStateName;
  dsn: string;
  hosts: string[];
  replicasDeclared: number | null;
  masterAddress: string | null;
  masterLeaseAlive: boolean;
  nodes: NodeDto[];
  requests: NodeRequestsDto | null;
  runtime: ShardRuntimeDto | null;
}

// Плановая нода шарда (arch/02 §9.1).
export interface NodeDto {
  name: string;
  state: string | null;
}

// Заявка ресурсов на ноду scope /service/<C>-<X>/request_* (arch/02 §9.1).
export interface NodeRequestsDto {
  cpu: string;
  mem: string;
  disk: string;
}

export interface ShardRuntimeDto {
  standbiesSync: number | null;
  slotsLagMaxBytes: number | null;
  walStatusLost: string[];
  subscriptions: string[];
  bucketSchemas: string[];
  error: string | null;
}

export interface BucketDto {
  id: number;
  owner: string | null;
  state: BucketStateName;
  move: MoveDto | null;
  ageSec: number | null;
}

export interface MoveDto {
  owner: string | null;
  target: string | null;
  startedUnix: number | null;
  updatedUnix: number | null;
  phase: string | null;
  lastError: string | null;
}

export interface HealDto {
  bucket: string;
  was: string | null;
  now: string | null;
  reason: string | null;
  tsUnix: number | null;
}

// Стендовая топология в деталях кластера: глобальный реестр снапшота, обычно пуст (t08 spec §8).
export interface StandNodeDto {
  name: string;
  address: string | null;
}

// GET /api/ha — сводный список.
export interface HaScopeSummaryDto {
  scope: string;
  cluster: string | null;
  shard: string | null;
  matched: boolean;
  leaderName: string | null;
  membersTotal: number;
  membersHealthy: number;
  lagMaxBytes: number | null;
}

// GET /api/ha/{scope} — детали.
export interface HaScopeDto {
  scope: string;
  cluster: string | null;
  shard: string | null;
  matched: boolean;
  leaderName: string | null;
  optimeLeader: number | null;
  requests: NodeRequestsDto | null;
  members: HaMemberDto[];
  rawConfig: string | null;
}

export interface HaMemberDto {
  name: string;
  host: string;
  port: number | null;
  role: string | null;
  state: string | null;
  timeline: number | null;
  lagBytes: number | null;
  probeAtUtc: string | null;
  probeError: string | null;
  nodeState: string | null;
}

// POST /api/ha/{scope}/nodes/{node}/recreate — ответ.
export interface NodeRecreatedDto {
  scope: string;
  node: string;
  state: string;
  mode: RecreateMode;
}

// Режим пересоздания: soft — живой лидер сначала переезжает switchover'ом;
// hard — снос сразу, failover делает Patroni.
export type RecreateMode = 'soft' | 'hard';

// GET /api/alerts
export interface AlertDto {
  id: string;
  severity: AlertSeverityName;
  kind: string;
  target: string;
  message: string;
  details: Record<string, string> | null;
  sinceUnix: number | null;
}

// POST /api/clusters/{cluster}/app-password/rotate — заявка ротации app-пароля
// (arch/03 §1.6, протокол arch/02 §9.8): панель пароль не знает — только факт заявки.
export interface AppPasswordRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}
