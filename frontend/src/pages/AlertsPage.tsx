// Панель «Алерты»: таблица всех алертов с severity-цветами и клиентским
// фильтром severity (t09 spec §4.10–4.11). Один запрос всех алертов —
// ключ дедуплицируется с Overview-лентой и навигационными счётчиками.
import { useQuery } from '@tanstack/react-query';
import { Group, SegmentedControl, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { useState } from 'react';
import type { AlertDto, AlertSeverityName } from '../api/dto';
import { fetchAlerts, queryKeys } from '../api/queries';
import { AlertRemedyBadge } from '../components/AlertRemedyBadge';
import { AlertSeverityBadge } from '../components/AlertSeverityBadge';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnix, formatUnixAge } from '../utils/format';

// Значения фильтра: «все» либо конкретный severity (t09 spec §4.10).
type SeverityFilter = 'all' | AlertSeverityName;

// Ранг severity для сортировки: critical раньше warning раньше info (t09 spec §4.10).
const SEVERITY_RANK: Record<AlertSeverityName, number> = {
  critical: 0,
  warning: 1,
  info: 2,
};

// Сортировка: severity-ранг, внутри — новые сверху, sinceUnix null — в конец.
function sortAlertRows(a: AlertDto, b: AlertDto): number {
  if (SEVERITY_RANK[a.severity] !== SEVERITY_RANK[b.severity])
    return SEVERITY_RANK[a.severity] - SEVERITY_RANK[b.severity];
  return (b.sinceUnix ?? -1) - (a.sinceUnix ?? -1);
}

export function AlertsPage() {
  const intervalMs = usePollingIntervalMs();
  const [filter, setFilter] = useState<SeverityFilter>('all');
  const alerts = useQuery({
    queryKey: queryKeys.alerts(),
    queryFn: () => fetchAlerts(),
    refetchInterval: intervalMs,
  });

  if (alerts.data === undefined)
    return alerts.isError ? (
      <ErrorSection error={alerts.error} onRetry={() => void alerts.refetch()} />
    ) : (
      <LoadingSection />
    );

  const all = [...alerts.data].sort(sortAlertRows);
  const rows = filter === 'all' ? all : all.filter((a) => a.severity === filter);
  return (
    <Stack gap="md">
      <Title order={2}>Алерты</Title>
      <Group justify="space-between">
        <SegmentedControl
          value={filter}
          onChange={(value) => setFilter(value as SeverityFilter)}
          data={[
            { value: 'all', label: 'все' },
            { value: 'critical', label: 'critical' },
            { value: 'warning', label: 'warning' },
            { value: 'info', label: 'info' },
          ]}
        />
        <Text size="sm" c="dimmed">{rows.length} из {all.length}</Text>
      </Group>
      {rows.length === 0 ? (
        filter === 'all' ? (
          <Text c="teal" size="sm">Алертов нет</Text>
        ) : (
          <Text c="dimmed" size="sm">Нет алертов этого уровня</Text>
        )
      ) : (
        <Table.ScrollContainer minWidth={800}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Severity</Table.Th>
                <Table.Th>Kind</Table.Th>
                <Table.Th>Target</Table.Th>
                <Table.Th>Сообщение</Table.Th>
                <Table.Th>Присутствует с</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {rows.map((a) => (
                <AlertRow key={a.id} alert={a} />
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Stack>
  );
}

// Строка алерта: severity-бейдж + бейдж движителя, kind с details в Tooltip,
// под сообщением — пояснение Hint, since-возраст (t09 spec §4.11; arch/03 §4.1).
function AlertRow({ alert }: { alert: AlertDto }) {
  const details = alert.details === null ? [] : Object.entries(alert.details);
  return (
    <Table.Tr>
      <Table.Td>
        <Stack gap={4} align="flex-start">
          <AlertSeverityBadge severity={alert.severity} />
          {alert.remedy !== null && <AlertRemedyBadge remedy={alert.remedy} />}
        </Stack>
      </Table.Td>
      <Table.Td>
        {details.length > 0 ? (
          <Tooltip multiline label={details.map(([k, v]) => `${k}: ${v}`).join('\n')}>
            <Text ff="monospace" size="sm">{alert.kind}</Text>
          </Tooltip>
        ) : (
          <Text ff="monospace" size="sm">{alert.kind}</Text>
        )}
      </Table.Td>
      <Table.Td><Text ff="monospace" size="sm" c="dimmed">{alert.target}</Text></Table.Td>
      <Table.Td>
        <Stack gap={2}>
          <Text size="sm">{alert.message}</Text>
          {alert.hint !== null && (
            <Text size="xs" c="dimmed">
              {alert.hint}
              {alert.remedyText !== null ? ` → ${alert.remedyText}` : null}
            </Text>
          )}
        </Stack>
      </Table.Td>
      <Table.Td>
        <Tooltip label={formatUnix(alert.sinceUnix)}>
          <span>
            <Text size="sm" c="dimmed">
              {alert.sinceUnix === null ? '—' : `с ${formatUnixAge(alert.sinceUnix)}`}
            </Text>
          </span>
        </Tooltip>
      </Table.Td>
    </Table.Tr>
  );
}
