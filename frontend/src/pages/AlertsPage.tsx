// Панель «Алерты» (u18/D15): группировка секциями по Remedy-движителю
// («Автоматика воркера» WorkerAuto / «Через API» OperatorApi / «Runbook»
// OperatorRunbook; null-remedy → «Прочее»), фильтр домена — клиентский по
// kind-префиксу (kafka-*/valkey-*/s3-*/остальное «pg и инфраструктура»),
// переключатель «Требуют оператора», severity-фильтр сохранён; строки
// кликабельны — deep-link alertRoute(kind, target) (u04).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Group, SegmentedControl, Select, Stack, Switch, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link } from 'react-router';
import { useState } from 'react';
import type { AlertDto, AlertSeverityName } from '../api/dto';
import { fetchAlerts, queryKeys } from '../api/queries';
import { AlertRemedyBadge } from '../components/AlertRemedyBadge';
import { AlertSeverityBadge } from '../components/AlertSeverityBadge';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { alertRoute } from '../utils/alertRoutes';
import { formatUnix, formatUnixAge } from '../utils/format';

// Значения фильтра: «все» либо конкретный severity (t09 spec §4.10).
type SeverityFilter = 'all' | AlertSeverityName;

// Фильтр домена по kind-префиксу (D15): kafka-/valkey-/s3-; остальное —
// «pg и инфраструктура» (etcd/бэкапы/воркеры — то же прочее).
type DomainFilter = 'all' | 'kafka' | 'valkey' | 's3' | 'pg';

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

// Секции по движителю (u18) — порядок фиксированный.
const REMEDY_SECTIONS: { key: string; title: string }[] = [
  { key: 'worker-auto', title: 'Автоматика воркера' },
  { key: 'operator-api', title: 'Через API' },
  { key: 'operator-runbook', title: 'Runbook' },
  { key: 'none', title: 'Прочее' },
];

export function AlertsPage() {
  const intervalMs = usePollingIntervalMs();
  const [filter, setFilter] = useState<SeverityFilter>('all');
  const [domain, setDomain] = useState<DomainFilter>('all');
  const [operatorOnly, setOperatorOnly] = useState(false);
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

  // Фильтры: severity + домен по kind-префиксу + «требуют оператора».
  const domainOf = (a: AlertDto): DomainFilter =>
    a.kind.startsWith('kafka-') ? 'kafka'
      : a.kind.startsWith('valkey-') ? 'valkey'
        : a.kind.startsWith('s3-') ? 's3'
          : 'pg';
  const all = alerts.data
    .filter((a) => filter === 'all' || a.severity === filter)
    .filter((a) => domain === 'all' || domainOf(a) === domain)
    .filter((a) => !operatorOnly || a.remedy === 'operator-api' || a.remedy === 'operator-runbook')
    .sort(sortAlertRows);

  // Группировка секциями по Remedy (пустые секции не рисуем).
  const sections = REMEDY_SECTIONS
    .map(({ key, title }) => ({
      key,
      title,
      rows: all.filter((a) =>
        key === 'none' ? a.remedy === null : a.remedy === key),
    }))
    .filter((s) => s.rows.length > 0);

  return (
    <Stack gap="md">
      <Title order={2}>Алерты</Title>
      <Group justify="space-between" align="flex-end">
        <Group gap="sm" align="flex-end">
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
          {/* u18/D15: фильтр домена — по kind-префиксу */}
          <Select
            w={230}
            label="Домен"
            data={[
              { value: 'all', label: 'Все' },
              { value: 'pg', label: 'pg и инфраструктура' },
              { value: 'kafka', label: 'kafka' },
              { value: 'valkey', label: 'valkey' },
              { value: 's3', label: 's3' },
            ]}
            value={domain}
            onChange={(v) => setDomain((v ?? 'all') as DomainFilter)}
          />
          <Switch
            label="Требуют оператора"
            description="remedy: через API или runbook"
            checked={operatorOnly}
            onChange={(e) => setOperatorOnly(e.currentTarget.checked)}
          />
        </Group>
        <Text size="sm" c="dimmed">{all.length} алертов</Text>
      </Group>

      {all.length === 0 ? (
        <Text c="dimmed" size="sm">Алертов по фильтру нет</Text>
      ) : (
        sections.map((section) => (
          <Stack key={section.key} gap="xs">
            <Text fw={600} size="sm" c="dimmed" tt="uppercase">{section.title}</Text>
            <Table.ScrollContainer minWidth={800}>
              <Table highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Severity</Table.Th>
                    <Table.Th>Kind</Table.Th>
                    <Table.Th>Target</Table.Th>
                    <Table.Th>Message</Table.Th>
                    <Table.Th>Present since</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {section.rows.map((a) => (
                    <AlertRow key={a.id} alert={a} />
                  ))}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          </Stack>
        ))
      )}
    </Stack>
  );
}

// Строка алерта: severity-бейдж + бейдж движителя, kind с details в Tooltip,
// под сообщением — пояснение Hint, since-возраст; строка кликабельна —
// deep-link alertRoute(kind, target) (u04).
function AlertRow({ alert }: { alert: AlertDto }) {
  const details = alert.details === null ? [] : Object.entries(alert.details);
  const route = alertRoute(alert.kind, alert.target);
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
      <Table.Td>
        {route !== null ? (
          <Anchor component={Link} to={route} size="sm" ff="monospace">{alert.target}</Anchor>
        ) : (
          <Text ff="monospace" size="sm" c="dimmed">{alert.target}</Text>
        )}
      </Table.Td>
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
              {alert.sinceUnix === null ? '—' : `for ${formatUnixAge(alert.sinceUnix)}`}
            </Text>
          </span>
        </Tooltip>
      </Table.Td>
    </Table.Tr>
  );
}
