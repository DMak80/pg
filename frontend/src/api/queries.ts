// Query-ключи и fetch-функции всех эндпоинтов (arch/03 §1); t08/t09 используют без правок слоя api.
import { apiFetch } from './client';
import type {
  AlertDto,
  ClusterDto,
  ClusterSummaryDto,
  EtcdStatusDto,
  HaScopeDto,
  HaScopeSummaryDto,
  OverviewDto,
  SessionDto,
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

export function logoutRequest(): Promise<void> {
  return apiFetch<void>('/api/auth/logout', { method: 'POST' });
}
