// Бейджи свежести 5 доменов в шапке (u08, D7): возраст данных etcd/pg/kafka/
// valkey/s3 из OverviewDto.freshness; старше 3×RefreshInterval — жёлтый
// «stale» (механика SnapshotStaleRule.Multiplier); null-домен — серый «—».
// Общий stale-бейдж по pg-снапшоту удалён (u08).
import { useQuery } from '@tanstack/react-query';
import { Badge, Group, Tooltip } from '@mantine/core';
import { fetchOverview, queryKeys } from '../api/queries';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatAge } from '../utils/format';

const DOMAINS = ['etcd', 'pg', 'kafka', 'valkey', 's3'] as const;

export function FreshnessBadges() {
  const intervalMs = usePollingIntervalMs();
  const { data, isError } = useQuery({
    queryKey: queryKeys.overview,
    queryFn: fetchOverview,
    refetchInterval: intervalMs,
  });

  if (isError) return <Badge color="red" variant="light">нет данных</Badge>;
  if (data === undefined || data.freshness === null || data.freshness === undefined) return null;

  // Порог stale — 3×интервал (SnapshotStaleRule.Multiplier); off — без порога.
  const staleThresholdSec = typeof intervalMs === 'number' ? (intervalMs / 1000) * 3 : null;
  const nowSec = Date.now() / 1000;

  return (
    <Group gap={4} wrap="nowrap">
      {DOMAINS.map((domain) => {
        const item = data.freshness?.[domain] ?? null;
        if (item === null || item === undefined)
          return (
            <Badge key={domain} color="gray" variant="light" size="xs">{domain}: —</Badge>
          );
        const ageSec = Math.max(0, nowSec - item.builtAtUnix);
        const stale = staleThresholdSec !== null && ageSec > staleThresholdSec;
        return (
          <Tooltip key={domain} label={`builtAt: ${new Date(item.builtAtUnix * 1000).toLocaleString()}`}>
            <Badge color={stale ? 'yellow' : 'gray'} variant="light" size="xs">
              {domain}: {formatAge(ageSec * 1000)}
            </Badge>
          </Tooltip>
        );
      })}
    </Group>
  );
}
