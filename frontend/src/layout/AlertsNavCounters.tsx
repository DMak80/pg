// Счётчики critical/warning у пункта «Алерты» в навигации (t09 spec §4.2).
// Показываются только при N > 0; pending/ошибка/пусто — секция пуста (навигация не мигает).
import { useQuery } from '@tanstack/react-query';
import { Badge, Group } from '@mantine/core';
import { fetchAlerts, queryKeys } from '../api/queries';
import { usePollingIntervalMs } from '../polling/PollingContext';

export function AlertsNavCounters() {
  // Тот же ключ, что у Overview-ленты и Alerts-страницы — один запрос на тик (t09 spec §3).
  const { data } = useQuery({
    queryKey: queryKeys.alerts(),
    queryFn: () => fetchAlerts(),
    refetchInterval: usePollingIntervalMs(),
  });

  if (data === undefined) return null;
  const critical = data.filter((a) => a.severity === 'critical').length;
  const warning = data.filter((a) => a.severity === 'warning').length;
  if (critical === 0 && warning === 0) return null;
  return (
    <Group gap={4} wrap="nowrap">
      {critical > 0 ? <Badge color="red" variant="light" size="xs">{critical}</Badge> : null}
      {warning > 0 ? <Badge color="yellow" variant="light" size="xs">{warning}</Badge> : null}
    </Group>
  );
}
