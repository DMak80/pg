// Stale-бейдж в шапке: возраст данных / stale / нет данных (spec §3.12, arch/03 §3).
import { useQuery } from '@tanstack/react-query';
import { Badge } from '@mantine/core';
import { fetchOverview, queryKeys } from '../api/queries';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatAge } from '../utils/format';

export function StaleBadge() {
  // Опрос overview текущим polling-интервалом — демонстрация переключателя в t07.
  const { data, isError } = useQuery({
    queryKey: queryKeys.overview,
    queryFn: fetchOverview,
    refetchInterval: usePollingIntervalMs(),
  });

  if (isError) return <Badge color="red" variant="light">нет данных</Badge>;
  if (data === undefined) return null;
  if (data.stale)
    return <Badge color="yellow" variant="light">stale: {formatAge(data.snapshotAgeMs)}</Badge>;
  return <Badge color="gray" variant="light">данные: {formatAge(data.snapshotAgeMs)}</Badge>;
}
