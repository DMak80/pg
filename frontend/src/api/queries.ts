// Query-ключи и fetch-функции всех эндпоинтов (arch/03 §1); t08/t09 используют без правок слоя api.
import { apiFetch } from './client';
import type {
  AddShardRequestDto,
  AlertDto,
  ClusterCreatedDto,
  ClusterDto,
  ClusterSummaryDto,
  CreateClusterRequestDto,
  EtcdStatusDto,
  HaScopeDto,
  HaScopeSummaryDto,
  OverviewDto,
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

export function logoutRequest(): Promise<void> {
  return apiFetch<void>('/api/auth/logout', { method: 'POST' });
}
