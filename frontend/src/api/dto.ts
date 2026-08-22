// Типы DTO REST API (arch/03 §2; фактические поля — C#-DTO t04–t06).
// Nullable-поля C# → '| null'; unix-время → number | null; DateTimeOffset → string.

// Строковый канон статусов бакета (arch/02 §2.1).
export type BucketStateName = 'ACTIVE' | 'SYNCING' | 'FROZEN' | 'ABORTING';

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
  shardsTotal: number;
  shardsWithMaster: number;
  activeMoves: number;
}

// GET /api/clusters/{cluster} — детали.
export interface ClusterDto {
  name: string;
  dbName: string | null;
  bucketsCount: number;
  createdUnix: number | null;
  incomplete: boolean;
  shards: ShardDto[];
  buckets: BucketDto[];
  heals: HealDto[];
}

export interface ShardDto {
  name: string;
  dsn: string;
  hosts: string[];
  replicasDeclared: number | null;
  masterAddress: string | null;
  masterLeaseAlive: boolean;
  runtime: ShardRuntimeDto | null;
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
}

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
