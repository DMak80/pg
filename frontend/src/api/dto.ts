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

// POST /api/clusters/{cluster}/moves/rollback — тело и ответ (t07, arch/03 §1.7).
export interface RollbackBucketsRequestDto {
  buckets: number[];
}

export interface RollbackQueuedDto {
  cluster: string;
  queued: number[];
  skipped: number[];
}

// POST /api/clusters/{cluster}/moves/finalize — тело и ответ (arch/03 §1.8).
export interface FinalizeBucketRequestDto {
  bucket: number;
  oldShard: string;
}

export interface BucketFinalizeQueuedDto {
  cluster: string;
  bucket: number;
  oldShard: string;
}

// POST /api/clusters/{cluster}/moves/abort — тело и ответ (arch/03 §1.9);
// force?: true — только когда включён (false не шлём).
export interface AbortBucketRequestDto {
  bucket: number;
  force?: boolean;
}

export interface BucketAbortQueuedDto {
  cluster: string;
  bucket: number;
  force: boolean;
}

// Журнал последнего процесса воркера кластера /pgworker/work/<C>
// (arch/03 §2): результат исполненной/отвергнутой заявки переездов.
export interface ClusterWorkDto {
  op: string;
  phase: string;
  updatedUnix: number;
  lastError: string | null;
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
  // Сводка kafka-домена (arch/03 §7.1); null до первого тика kafka-refresher'а.
  kafka: OverviewKafkaDto | null;
  // Сводка valkey-домена (arch/03 §8.1); null до первого тика valkey-refresher'а.
  valkey: OverviewValkeyDto | null;
  snapshotAgeMs: number;
  stale: boolean;
}

export interface OverviewKafkaDto {
  clustersTotal: number;
  clustersCritical: number;
}

export interface OverviewValkeyDto {
  clustersTotal: number;
  clustersCritical: number;
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
  // Журнал последнего процесса воркера (t07, arch/03 §2); null — журнала нет.
  work?: ClusterWorkDto | null;
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
export type AlertRemedyName = 'worker-auto' | 'operator-api' | 'operator-runbook';

export interface AlertDto {
  id: string;
  severity: AlertSeverityName;
  kind: string;
  target: string;
  message: string;
  details: Record<string, string> | null;
  sinceUnix: number | null;
  // Объяснение и движитель (arch/03 §4.1, task etcd-via-worker-api): null —
  // обратно совместимые старые алерты без полей.
  hint: string | null;
  remedy: AlertRemedyName | null;
  remedyText: string | null;
}

// POST /api/clusters/{cluster}/secrets/rotate — заявка ротации per-cluster
// секретов (arch/03 §1.6, протокол arch/02 §9.8, t02): панель креды не знает —
// только факт заявки.
export interface ClusterSecretsRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}

// ===== Kafka-домен (arch/03 §7.2; C#-DTO B5/B6) =====

// Канон состояния kafka-кластера: config.state (arch/15 §2); отсутствие = ACTIVE.
export type KafkaClusterStateName = 'ACTIVE' | 'NOT_INITIALIZED' | 'TO_REMOVE';

// GET /api/kafka/clusters — сводный список.
export interface KafkaClusterSummaryDto {
  name: string;
  state: KafkaClusterStateName;
  brokersTotal: number;
  brokersRunning: number;
  topicsCount: number;
  endpoints: string | null;
  rotationPending: boolean;
  rebalancePending: boolean;
}

// GET /api/kafka/clusters/{cluster} — детали.
export interface KafkaClusterDto {
  name: string;
  state: KafkaClusterStateName;
  brokers: number;
  replicationFactor: number;
  minInSyncReplicas: number;
  defaultPartitions: number;
  defaultRetentionMs: number;
  createdUnix: number | null;
  endpoints: string | null;
  brokersList: KafkaBrokerDto[];
  topics: KafkaTopicDto[];
  rotation: KafkaRotationTicketDto | null;
  // Ребалансировка партиций (t02): null = заявки/операции нет.
  rebalance: KafkaRebalanceTicketDto | null;
  reassignment: KafkaReassignmentDto | null;
  // Live-прогресс rolling-регенерации брокеров (t06): null = операции нет.
  regen: KafkaRegenDto | null;
  // Live-группы из пробы (волна C): null — проба молчит о кластере.
  groups: KafkaGroupDto[] | null;
  probeOk: boolean | null;
  probeError: string | null;
}

export interface KafkaBrokerDto {
  name: string;
  state: string | null;
  role: string | null;
  cpu: number | null;
  memGi: number | null;
  diskGi: number | null;
  live: boolean | null;
  brokerId: number | null;
}

export interface KafkaTopicDto {
  name: string;
  partitions: number;
  replicationFactor: number | null;
  retentionMs: number | null;
  minInSyncReplicas: number | null;
  desired: KafkaTopicDesiredDto | null;
  missing: boolean;
  syncedUnix: number | null;
  // Live-USR из пробы (волна C): null — проба молчит.
  underReplicatedPartitions: number | null;
  // Живая lifecycle-заявка (t01): create без факт-ключа — «виртуальная» строка
  // (факт-поля null/0, параметры — здесь).
  lifecycle: KafkaTopicLifecycleDto | null;
}

// Live-группа консьюмеров (вкладка Группы, волна C).
export interface KafkaGroupDto {
  group: string;
  state: string | null;
  members: number;
  totalLag: number;
}

export interface KafkaTopicDesiredDto {
  partitions: number | null;
  retentionMs: number | null;
  minInSyncReplicas: number | null;
  requestedUnix: number | null;
  requestedBy: string | null;
}

// Lifecycle-заявка топика topics/<T>/desired.{create,delete} (t01, arch/15 §3.1).
export interface KafkaTopicLifecycleDto {
  op: 'create' | 'delete';
  partitions: number | null;
  replicationFactor: number | null;
  retentionMs: number | null;
  minInSyncReplicas: number | null;
  requestedUnix: number;
  requestedBy: string | null;
}

// POST /api/kafka/clusters/{cluster}/topics — создание топика (arch/02 §10.2-9).
export interface CreateTopicRequestDto {
  name: string;
  partitions?: number;
  replicationFactor?: number;
  retentionMs?: number;
  minInSyncReplicas?: number;
}

export interface KafkaTopicCreatedDto {
  cluster: string;
  topic: string;
  partitions: number;
  replicationFactor: number;
}

export interface KafkaRotationTicketDto {
  requestedUnix: number;
  requestedBy: string | null;
}

// Заявка ребалансировки (t02, arch/03 §7.2); null = заявки нет.
export interface KafkaRebalanceTicketDto {
  requestedUnix: number;
  requestedBy: string | null;
}

// Прогресс reassignment (t02, arch/03 §7.2); null = операции нет.
export interface KafkaReassignmentDto {
  mode: 'drain' | 'balance';
  drainBroker: string | null;
  partitionsTotal: number;
  partitionsRemaining: number;
  updatedUnix: number;
}

// Прогресс rolling-регенерации брокеров (t06, arch/15 §4); null = операции нет.
export interface KafkaRegenDto {
  brokersTotal: number;
  brokersRemaining: number;
  currentBroker: string | null;
  updatedUnix: number;
}

// PUT /api/kafka/clusters/{c}/brokers/{b}/resources — мутация №15 (t06):
// эффективные ресурсы после применения (cpu/memGi/diskGi — канон-строки).
export interface KafkaBrokerResourcesUpdatedDto {
  cluster: string;
  broker: string;
  cpu: string;
  memGi: string;
  diskGi: string;
}

// POST /api/kafka/clusters — тело и ответ (arch/02 §10.3).
export interface CreateKafkaClusterRequestDto {
  name: string;
  brokers?: number;
  replicationFactor?: number;
  minInSyncReplicas?: number;
  defaultPartitions?: number;
  defaultRetentionMs?: number;
  cpu?: number;
  memGi?: number;
  diskGi?: number;
}

export interface KafkaClusterCreatedDto {
  name: string;
  state: string;
  brokers: number;
  replicationFactor: number;
  minInSyncReplicas: number;
  defaultPartitions: number;
  defaultRetentionMs: number;
  cpu: string;
  memGi: string;
  diskGi: string;
}

// PUT /api/kafka/clusters/{cluster}/config — тело и ответ.
export interface KafkaConfigUpdateRequestDto {
  replicationFactor?: number;
  minInSyncReplicas?: number;
  defaultPartitions?: number;
  defaultRetentionMs?: number;
}

export interface KafkaConfigUpdatedDto {
  cluster: string;
  replicationFactor: number;
  minInSyncReplicas: number;
  defaultPartitions: number;
  defaultRetentionMs: number;
}

// POST /api/kafka/clusters/{cluster}/brokers — тело и ответ.
export interface AddKafkaBrokerRequestDto {
  cpu?: number;
  memGi?: number;
  diskGi?: number;
}

export interface KafkaBrokerAddedDto {
  cluster: string;
  name: string;
  cpu: string;
  memGi: string;
  diskGi: string;
  state: string;
}

// PUT /api/kafka/clusters/{cluster}/topics/{topic} — конфиг-заявка топика
// (arch/02 §10.2-7): хотя бы одно поле; partitions — только увеличение.
export interface TopicDesiredRequestDto {
  partitions?: number;
  retentionMs?: number;
  minInSyncReplicas?: number;
}

export interface TopicDesiredDto {
  cluster: string;
  topic: string;
  partitions: number | null;
  retentionMs: number | null;
  minInSyncReplicas: number | null;
}

// POST /api/kafka/clusters/{cluster}/app-password/rotate — ответ.
export interface KafkaPasswordRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}

// POST /api/kafka/clusters/{cluster}/admin-password/rotate — ответ (мутация №16, t03).
export interface KafkaAdminPasswordRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}

// POST /api/kafka/clusters/{cluster}/ca/rotate — ответ (мутация №17, t07).
export interface KafkaCaRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}

// POST /api/kafka/clusters/{cluster}/rebalance — ответ (t02).
export interface KafkaRebalanceRequestedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}

// ===== Valkey-домен (arch/03 §8.2; зеркало C#-DTO ValkeyQuery.cs/ValkeyCommands.cs) =====

// Канон состояния valkey-кластера: config.state (arch/20 §2); отсутствие = ACTIVE.
export type ValkeyClusterStateName = 'ACTIVE' | 'NOT_INITIALIZED' | 'TO_REMOVE';

// GET /api/valkey/clusters — сводный список (arch/03 §8.2).
export interface ValkeyClusterSummaryDto {
  name: string;
  state: ValkeyClusterStateName;
  nodesTotal: number;
  nodesRunning: number;
  endpoints: string | null;
  rotationPending: boolean;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
}

// GET /api/valkey/clusters/{cluster} — детали (arch/03 §8.2).
export interface ValkeyClusterDto {
  name: string;
  state: ValkeyClusterStateName;
  nodesTotal: number;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
  createdUnix: number | null;
  endpoints: string | null;
  nodesList: ValkeyNodeDto[];
  rotation: ValkeyRotationDto | null;
  // Живая заявка ротации CA /valkeyworker/ca_rotations/<C> (t07; бейдж UI).
  caRotation: ValkeyCaRotationDto | null;
}

export interface ValkeyNodeDto {
  name: string;
  state: string | null;
  cpu: number | null;
  memGi: number | null;
  diskGi: number | null;
  // Из PING-пробы (только факт живости); null — проба молчит/кредов нет.
  live: boolean | null;
  probeError: string | null;
}

export interface ValkeyRotationDto {
  role: string;
  requestedUnix: number;
  requestedBy: string | null;
}

// POST /api/valkey/clusters — тело и ответ (arch/03 §8.2/§8.3.1).
export interface CreateValkeyClusterRequestDto {
  name: string;
  maxmemoryBytes?: number;
  maxmemoryPolicy?: string;
  resources?: { cpu?: number; memGi?: number; diskGi?: number };
}

export interface ValkeyClusterCreatedDto {
  name: string;
  state: string;
  nodes: number;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
  cpu: string;
  memGi: string;
  diskGi: string;
}

// PUT config / PUT resources / POST rotate — тела и ответы.
export interface ValkeyConfigUpdateRequestDto {
  maxmemoryBytes?: number;
  maxmemoryPolicy?: string;
}
export interface ValkeyConfigUpdatedDto {
  cluster: string;
  maxmemoryBytes: number;
  maxmemoryPolicy: string;
}
export interface ValkeyResourcesRequestDto {
  cpu?: number;
  memGi?: number;
  diskGi?: number;
}
export interface ValkeyResourcesUpdatedDto {
  cluster: string;
  node: string;
  cpu: string;
  memGi: string;
  diskGi: string;
}
export interface ValkeyRotateRequestDto {
  role: 'app' | 'admin';
}
export interface ValkeyPasswordRotatedDto {
  cluster: string;
  role: string;
  requestedUnix: number;
  requestedBy: string;
}

// Ответ 202 POST /api/valkey/clusters/{c}/ca/rotate (t07).
export interface ValkeyCaRotatedDto {
  cluster: string;
  requestedUnix: number;
  requestedBy: string;
}

// Живая заявка ротации CA из деталей кластера (t07; JSON-поле caRotation).
export interface ValkeyCaRotationDto {
  requestedUnix: number;
  requestedBy: string | null;
}

// ===== Грань «Хранилище бэкапов» (t08, arch/03 §2; зеркало C#-DTO BackupStorageQuery.cs) =====

// Статус сверки одного полного (C# BackupFullReconcileStatus).
export type BackupReconcileStatusName = 'Ok' | 'S3Only' | 'EtcdOnly' | 'InProgress' | 'Deleting';

// GET /api/backups/storage — сводка (без S3-ключей никогда — AC8).
export interface MinioHealthDto {
  apiOk: boolean;
  apiError: string | null;
  liveOk: boolean;
  clusterOk: boolean | null;
  healthyDrives: number | null;
  offlineDrives: number | null;
  healingDrives: number | null;
  totalDrives: number | null;
}

export interface BackupStorageEtcdDto {
  usedBytes: number;
  quotaBytes: number | null;
  usedPercent: number | null;
  state: string; // OK | WARN | CRIT (вердикт воркера)
  updatedUnix: number;
}

export interface BackupShardSummaryDto {
  cluster: string;
  shard: string;
  sizeBytes: number;
  fullsCount: number;
  walSegmentCount: number;
  hasS3Only: boolean; // есть объекты без ключа
  orphan: boolean; // префикс без владельца
}

export interface BackupClusterStorageDto {
  cluster: string;
  sizeBytes: number;
  shards: BackupShardSummaryDto[];
}

export interface BackupOrphanDto {
  prefix: string;
  kind: string; // shard | cluster
  sizeBytes: number;
  inWorkerRegistry: boolean;
  registryState: string | null;
  firstSeenUnix: number | null;
  ttlLeftSec: number | null;
}

export interface BackupStorageDto {
  configured: boolean;
  notConfiguredReason: string | null;
  endpoint: string | null;
  bucket: string | null;
  health: MinioHealthDto | null;
  buckets: string[];
  etcd: BackupStorageEtcdDto | null;
  liveUsedBytes: number | null;
  clusters: BackupClusterStorageDto[];
  foreignPrefixes: string[];
  orphans: BackupOrphanDto[];
  inventoryUpdatedUnix: number;
  inventoryError: string | null;
}

// GET /api/backups/storage/{cluster}/{shard} — детали шарда.
export interface BackupFullDto {
  id: string;
  sizeBytes: number | null;
  objectCount: number | null;
  lastModifiedUnix: number | null;
  etcdState: string | null;
  verifyState: string | null;
  etcdSizeBytes: number | null;
  reconcile: BackupReconcileStatusName;
}

export interface BackupWalDto {
  etcdState: string | null;
  etcdLastSegment: string | null;
  etcdLastUnix: number | null;
  s3SegmentCount: number;
  s3HistoryCount: number;
  s3SizeBytes: number;
  s3LastModifiedUnix: number;
  s3LastObject: string | null;
}

export interface BackupRestoreBadgeDto {
  state: string;
  phase: string | null;
  error: string | null;
}

export interface BackupShardStorageDto {
  cluster: string;
  shard: string;
  fulls: BackupFullDto[];
  wal: BackupWalDto | null;
  activeRestore: BackupRestoreBadgeDto | null;
  reconcileNote: string | null;
}

// GET /api/backups/objects — on-demand постраничный list-v2.
export interface BackupObjectDto {
  key: string;
  sizeBytes: number;
  lastModifiedUnix: number;
}

export interface BackupObjectsPageDto {
  items: BackupObjectDto[];
  nextContinuationToken: string | null;
}

// ===== Грань «Воркеры» (arch/adminpanel/03 §1/§3.7): сводные DTO и мутации серта =====

export type WorkerApplyStatus = 'applied' | 'pending restart' | 'unmanaged' | 'unknown';

export interface WorkerInstanceDto {
  instance: string;
  url: string;
  sinceUnix: number;
  health?: string | null;
  certThumbprint?: string | null;
  applyStatus: WorkerApplyStatus;
}

export interface WorkerCertDto {
  thumbprint: string;
  subject: string;
  issuer: string;
  san: string[];
  notBeforeUnix: number;
  notAfterUnix: number;
  updatedUnix: number;
  updatedBy?: string | null;
}

export interface WorkerViewDto {
  worker: string;
  instances: WorkerInstanceDto[];
  targetCert?: WorkerCertDto | null;
}

export interface WorkersViewDto {
  workers: WorkerViewDto[];
}

export interface WorkerApiCertDto {
  worker: string;
  thumbprint: string;
  updatedUnix: number;
  updatedBy: string;
  restartRequired: boolean;
  warning?: string | null;
}

export interface RestartInstanceResultDto {
  instance: string;
  accepted: boolean;
  error?: string | null;
}

export interface WorkerRestartDto {
  results: RestartInstanceResultDto[];
}

// Тело PUT серта: snake_case — контракт 03 §3.7 (не camelCase).
export interface UploadWorkerCertRequestDto {
  cert_pem: string;
  key_pem: string;
}
