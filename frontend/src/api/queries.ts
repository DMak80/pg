// Query-ключи и fetch-функции всех эндпоинтов (arch/03 §1); t08/t09 используют без правок слоя api.
import { apiFetch } from './client';
import type {
  AddKafkaBrokerRequestDto,
  AddShardRequestDto,
  CreateKafkaClusterRequestDto,
  AlertDto,
  AppPasswordRotatedDto,
  ClusterCreatedDto,
  ClusterDto,
  ClusterSummaryDto,
  CreateClusterRequestDto,
  EtcdStatusDto,
  HaScopeDto,
  KafkaBrokerAddedDto,
  KafkaClusterCreatedDto,
  KafkaClusterDto,
  KafkaClusterSummaryDto,
  KafkaConfigUpdatedDto,
  KafkaConfigUpdateRequestDto,
  KafkaPasswordRotatedDto,
  KafkaRebalanceRequestedDto,
  TopicDesiredDto,
  TopicDesiredRequestDto,
  HaScopeSummaryDto,
  MoveBucketsRequestDto,
  MovesQueuedDto,
  NodeRecreatedDto,
  OverviewDto,
  RecreateMode,
  SessionDto,
  ShardAddedDto,
} from './dto';

export const queryKeys = {
  session: ['session'] as const,
  overview: ['overview'] as const,
  etcdStatus: ['etcd-status'] as const,
  clusters: ['clusters'] as const,
  cluster: (name: string) => ['clusters', name] as const,
  haScopes: ['ha-scopes'] as const,
  haScope: (scope: string) => ['ha-scopes', scope] as const,
  alerts: (severity?: string, kind?: string) => ['alerts', { severity, kind }] as const,
};

export function fetchSession(): Promise<SessionDto> {
  return apiFetch<SessionDto>('/api/auth/me');
}

export function fetchOverview(): Promise<OverviewDto> {
  return apiFetch<OverviewDto>('/api/overview');
}

export function fetchEtcdStatus(): Promise<EtcdStatusDto> {
  return apiFetch<EtcdStatusDto>('/api/etcd/status');
}

export function fetchClusters(): Promise<ClusterSummaryDto[]> {
  return apiFetch<ClusterSummaryDto[]>('/api/clusters');
}

export function fetchClusterDetails(
  name: string,
  owner?: string,
  state?: string,
): Promise<ClusterDto> {
  const params = new URLSearchParams();
  if (owner !== undefined) params.set('owner', owner);
  if (state !== undefined) params.set('state', state);
  const query = params.size > 0 ? `?${params.toString()}` : '';
  return apiFetch<ClusterDto>(`/api/clusters/${encodeURIComponent(name)}${query}`);
}

export function fetchHaScopes(): Promise<HaScopeSummaryDto[]> {
  return apiFetch<HaScopeSummaryDto[]>('/api/ha');
}

export function fetchHaScope(scope: string): Promise<HaScopeDto> {
  return apiFetch<HaScopeDto>(`/api/ha/${encodeURIComponent(scope)}`);
}

export function fetchAlerts(severity?: string, kind?: string): Promise<AlertDto[]> {
  const params = new URLSearchParams();
  if (severity !== undefined) params.set('severity', severity);
  if (kind !== undefined) params.set('kind', kind);
  const query = params.size > 0 ? `?${params.toString()}` : '';
  return apiFetch<AlertDto[]>(`/api/alerts${query}`);
}

export function loginRequest(username: string, password: string): Promise<void> {
  return apiFetch<void>('/api/auth/login', { method: 'POST', body: { username, password } });
}

// POST /api/clusters — первая мутация панели (spec t12 §3.8).
export function createCluster(request: CreateClusterRequestDto): Promise<ClusterCreatedDto> {
  return apiFetch<ClusterCreatedDto>('/api/clusters', { method: 'POST', body: request });
}

// DELETE /api/clusters/{name} — перевод кластера в TO_REMOVE (arch/02 §9.4);
// 204 без тела; ключи etcd не удаляются (очистка — внешний оркестратор).
export function deleteCluster(name: string): Promise<void> {
  return apiFetch<void>(`/api/clusters/${encodeURIComponent(name)}`, { method: 'DELETE' });
}

// POST /api/clusters/{cluster}/shards — третья мутация панели (t06, 02 §9.5):
// шард стартует пустым; имя генерирует сервер (shard<max+1>).
export function addShard(cluster: string, request: AddShardRequestDto): Promise<ShardAddedDto> {
  return apiFetch<ShardAddedDto>(`/api/clusters/${encodeURIComponent(cluster)}/shards`,
    { method: 'POST', body: request });
}

// DELETE /api/clusters/{cluster}/shards/{shard} — маркер демонтажа TO_REMOVE
// (t06, 02 §9.6); 204 без тела; демонтаж выполняет PgWorker.
export function removeShard(cluster: string, shard: string): Promise<void> {
  return apiFetch<void>(
    `/api/clusters/${encodeURIComponent(cluster)}/shards/${encodeURIComponent(shard)}`,
    { method: 'DELETE' });
}

// POST /api/clusters/{cluster}/moves — пятая мутация панели (arch/02 §9.7):
// заявки в очередь /pgworker/moves/; выполнение — PgWorker (последовательно).
export function moveBuckets(cluster: string, request: MoveBucketsRequestDto): Promise<MovesQueuedDto> {
  return apiFetch<MovesQueuedDto>(`/api/clusters/${encodeURIComponent(cluster)}/moves`,
    { method: 'POST', body: request });
}

export function logoutRequest(): Promise<void> {
  return apiFetch<void>('/api/auth/logout', { method: 'POST' });
}

// POST /api/ha/{scope}/nodes/{node}/recreate — маркер TO_RECREATE с режимом
// soft|hard (sixth mutation); NodeSupervisor PgWorker выполнит rebuild ноды.
// apiFetch сериализует body сам — передаём объект, не строку.
export function recreateNode(scope: string, node: string, mode: RecreateMode): Promise<NodeRecreatedDto> {
  return apiFetch<NodeRecreatedDto>(
    `/api/ha/${encodeURIComponent(scope)}/nodes/${encodeURIComponent(node)}/recreate`,
    { method: 'POST', body: { mode } });
}

// POST /api/clusters/{cluster}/app-password/rotate — заявка ротации app-пароля
// (arch/02 §9.8): ставит /pgworker/rotations/<C>; выполняет PgWorker (AppPasswordRotator).
export function rotateAppPassword(cluster: string): Promise<AppPasswordRotatedDto> {
  return apiFetch<AppPasswordRotatedDto>(
    `/api/clusters/${encodeURIComponent(cluster)}/app-password/rotate`,
    { method: 'POST' });
}

// ===== Kafka-домен (arch/03 §7.1) =====

export const kafkaQueryKeys = {
  clusters: ['kafka-clusters'] as const,
  cluster: (name: string) => ['kafka-clusters', name] as const,
};

export function fetchKafkaClusters(): Promise<KafkaClusterSummaryDto[]> {
  return apiFetch<KafkaClusterSummaryDto[]>('/api/kafka/clusters');
}

export function fetchKafkaClusterDetails(name: string): Promise<KafkaClusterDto> {
  return apiFetch<KafkaClusterDto>(`/api/kafka/clusters/${encodeURIComponent(name)}`);
}

// POST /api/kafka/clusters — создание kafka-кластера (arch/02 §10.2-1).
export function createKafkaCluster(
  request: CreateKafkaClusterRequestDto,
): Promise<KafkaClusterCreatedDto> {
  return apiFetch<KafkaClusterCreatedDto>('/api/kafka/clusters', { method: 'POST', body: request });
}

// DELETE /api/kafka/clusters/{cluster} — перевод в TO_REMOVE (arch/02 §10.2-2).
export function deleteKafkaCluster(cluster: string): Promise<void> {
  return apiFetch<void>(`/api/kafka/clusters/${encodeURIComponent(cluster)}`, { method: 'DELETE' });
}

// PUT /api/kafka/clusters/{cluster}/config — default-конфиги (arch/02 §10.2-3).
export function updateKafkaConfig(
  cluster: string,
  request: KafkaConfigUpdateRequestDto,
): Promise<KafkaConfigUpdatedDto> {
  return apiFetch<KafkaConfigUpdatedDto>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/config`,
    { method: 'PUT', body: request });
}

// POST /api/kafka/clusters/{cluster}/brokers — добавление брокера (arch/02 §10.2-4);
// имя генерирует сервер (broker<max+1>).
export function addKafkaBroker(
  cluster: string,
  request: AddKafkaBrokerRequestDto,
): Promise<KafkaBrokerAddedDto> {
  return apiFetch<KafkaBrokerAddedDto>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/brokers`,
    { method: 'POST', body: request });
}

// DELETE /api/kafka/clusters/{cluster}/brokers/{broker} — маркер TO_REMOVE
// (arch/02 §10.2-5); демонтаж выполняет KafkaWorker.
export function removeKafkaBroker(cluster: string, broker: string): Promise<void> {
  return apiFetch<void>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/brokers/${encodeURIComponent(broker)}`,
    { method: 'DELETE' });
}

// POST /api/kafka/clusters/{cluster}/app-password/rotate — заявка ротации
// (arch/02 §10.2-8): rolling-перезапуск брокеров; выполняет KafkaWorker.
export function rotateKafkaPassword(cluster: string): Promise<KafkaPasswordRotatedDto> {
  return apiFetch<KafkaPasswordRotatedDto>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/app-password/rotate`,
    { method: 'POST' });
}

// POST /api/kafka/clusters/{cluster}/rebalance — заявка ребалансировки
// партиций (t02, arch/02 §10.2-9): перенос реплик выполняет KafkaWorker.
export function requestKafkaRebalance(cluster: string): Promise<KafkaRebalanceRequestedDto> {
  return apiFetch<KafkaRebalanceRequestedDto>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/rebalance`,
    { method: 'POST' });
}

// DELETE /api/kafka/clusters/{cluster}/rebalance — отмена ребалансировки
// (t02, arch/02 §10.2-10): новые батчи не подаются, поданные Kafka доиграет.
export async function cancelKafkaRebalance(cluster: string): Promise<void> {
  await apiFetch<void>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/rebalance`,
    { method: 'DELETE' });
}

// PUT /api/kafka/clusters/{cluster}/topics/{topic} — конфиг-заявка топика
// (arch/02 §10.2-7): применяет автосинк воркера (конфиги → partitions↑).
export function upsertTopicDesired(
  cluster: string,
  topic: string,
  request: TopicDesiredRequestDto,
): Promise<TopicDesiredDto> {
  return apiFetch<TopicDesiredDto>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/topics/${encodeURIComponent(topic)}`,
    { method: 'PUT', body: request });
}

// DELETE /api/kafka/clusters/{cluster}/topics/{topic}/desired — отмена заявки
// (arch/02 §10.2-8): для missing-топиков следующий автосинк удалит ключ.
export function cancelTopicDesired(cluster: string, topic: string): Promise<void> {
  return apiFetch<void>(
    `/api/kafka/clusters/${encodeURIComponent(cluster)}/topics/${encodeURIComponent(topic)}/desired`,
    { method: 'DELETE' });
}
