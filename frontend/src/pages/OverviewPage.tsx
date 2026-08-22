// Панель Обзор: карточки etcd/кластеров/алертов (HA — t09), активные переезды,
// лента алертов critical/warning (t08 spec §4.3–4.5).
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Anchor,
  Badge,
  Card,
  Group,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import { Link } from 'react-router';
import type { OverviewDto } from '../api/dto';
import { fetchAlerts, fetchOverview, queryKeys } from '../api/queries';
import { BucketStateBadge } from '../components/BucketStateBadge';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnix, formatUnixAge } from '../utils/format';

// Сортировка ленты: critical раньше warning, внутри — новые сверху (t08 spec §4.4).
function sortAlertRows(a: { severity: string; sinceUnix: number | null }, b: { severity: string; sinceUnix: number | null }): number {
  const rankA = a.severity === 'critical' ? 0 : 1;
  const rankB = b.severity === 'critical' ? 0 : 1;
  if (rankA !== rankB) return rankA - rankB;
  return (b.sinceUnix ?? 0) - (a.sinceUnix ?? 0);
}

export function OverviewPage() {
  const intervalMs = usePollingIntervalMs();
  // Тот же ключ, что у StaleBadge — TanStack дедуплицирует опрос (t08 spec §3).
  const overview = useQuery({
    queryKey: queryKeys.overview,
    queryFn: fetchOverview,
    refetchInterval: intervalMs,
  });
  const alerts = useQuery({
    queryKey: queryKeys.alerts(),
    queryFn: () => fetchAlerts(),
    refetchInterval: intervalMs,
  });

  if (overview.data === undefined)
    return overview.isError ? (
      <ErrorSection error={overview.error} onRetry={() => void overview.refetch()} />
    ) : (
      <LoadingSection />
    );

  const data = overview.data;
  return (
    <Stack gap="md">
      <Title order={2}>Обзор</Title>
      <SimpleGrid cols={{ base: 1, sm: 2, lg: 4 }}>
        <EtcdCard data={data} />
        <ClustersCard data={data} />
        <AlertsCard data={data} />
        <Card withBorder padding="md" radius="md">
          <Text fw={600} mb="xs">HA</Text>
          <Text c="dimmed" size="sm">Сводка HA будет реализована в t09</Text>
        </Card>
      </SimpleGrid>
      <MovesSection data={data} />
      <AlertsFeedSection
        isPending={alerts.isPending}
        isError={alerts.isError}
        onRetry={() => void alerts.refetch()}
        rows={(alerts.data ?? []).filter((a) => a.severity !== 'info').sort(sortAlertRows)}
      />
    </Stack>
  );
}

// Карточка etcd: доступность и endpoints ok/total; alarms — в ленте и на панели etcd (t08 spec §4.3).
function EtcdCard({ data }: { data: OverviewDto }) {
  const etcd = data.etcd;
  return (
    <Card
      withBorder
      padding="md"
      radius="md"
      style={{ borderColor: etcd.reachable ? undefined : 'var(--mantine-color-red-6)' }}
    >
      <Group justify="space-between" mb="xs">
        <Text fw={600}>etcd</Text>
        <Badge color={etcd.reachable ? 'teal' : 'red'} variant="light">
          {etcd.reachable ? 'доступен' : 'недоступен'}
        </Badge>
      </Group>
      <Text size="sm" c="dimmed">endpoints: {etcd.endpointsOk}/{etcd.endpointsTotal}</Text>
      <Anchor component={Link} to="/etcd" size="sm" mt="xs" display="inline-block">Детали →</Anchor>
    </Card>
  );
}

// Карточка кластеров: строка на кластер, счётчики (t08 spec §4.3).
function ClustersCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Кластеры</Text>
      {data.clusters.length === 0 ? (
        <Text c="dimmed" size="sm">Кластеры не найдены</Text>
      ) : (
        <Stack gap={4}>
          {data.clusters.map((c) => (
            <Group key={c.name} justify="space-between" gap="xs" wrap="nowrap">
              <Anchor component={Link} to={`/clusters/${c.name}`} size="sm" truncate="end">{c.name}</Anchor>
              <Group gap={5} wrap="nowrap">
                <Text size="sm" c="dimmed">шарды {c.shards}</Text>
                <Text size="sm" c="dimmed">бакеты {c.buckets}</Text>
                {c.activeMoves > 0 ? (
                  <Badge color="yellow" variant="light">переезды: {c.activeMoves}</Badge>
                ) : null}
                {c.masterlessShards > 0 ? (
                  <Badge color="red" variant="light">без мастера: {c.masterlessShards}</Badge>
                ) : null}
              </Group>
            </Group>
          ))}
        </Stack>
      )}
    </Card>
  );
}

// Карточка алертов: счётчики severity, нули — приглушённо (t08 spec §4.3).
function AlertsCard({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Алерты</Text>
      <Group gap="md">
        <Badge
          color={data.alertsCritical > 0 ? 'red' : 'gray'}
          variant="light"
          size="lg"
        >
          critical: {data.alertsCritical}
        </Badge>
        <Badge
          color={data.alertsWarning > 0 ? 'yellow' : 'gray'}
          variant="light"
          size="lg"
        >
          warning: {data.alertsWarning}
        </Badge>
      </Group>
      <Anchor component={Link} to="/alerts" size="sm" mt="xs" display="inline-block">Все алерты →</Anchor>
    </Card>
  );
}

// Секция активных переездов: таблица не-ACTIVE бакетов всех кластеров (t08 spec §4.5).
function MovesSection({ data }: { data: OverviewDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Активные переезды</Text>
      {data.activeMoves.length === 0 ? (
        <Text c="dimmed" size="sm">Активных переездов нет</Text>
      ) : (
        <Table.ScrollContainer minWidth={700}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Кластер</Table.Th>
                <Table.Th>Бакет</Table.Th>
                <Table.Th>Состояние</Table.Th>
                <Table.Th>Маршрут</Table.Th>
                <Table.Th>Обновлён</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {data.activeMoves.map((m) => (
                <Table.Tr key={`${m.cluster}-${m.bucket}`}>
                  <Table.Td>
                    <Anchor component={Link} to={`/clusters/${m.cluster}`} size="sm">{m.cluster}</Anchor>
                  </Table.Td>
                  <Table.Td>{m.bucket}</Table.Td>
                  <Table.Td><BucketStateBadge state={m.state} /></Table.Td>
                  <Table.Td>{m.owner ?? '—'} → {m.target ?? '—'}</Table.Td>
                  <Table.Td>
                    <Tooltip label={formatUnix(m.updatedUnix)}>
                      <span>{formatUnixAge(m.updatedUnix)}</span>
                    </Tooltip>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}

// Лента алертов: critical/warning, все (без топ-N); info отфильтрован (t08 spec §4.4).
function AlertsFeedSection({ isPending, isError, onRetry, rows }: {
  isPending: boolean;
  isError: boolean;
  onRetry: () => void;
  rows: { id: string; severity: string; kind: string; target: string; message: string; sinceUnix: number | null }[];
}) {
  let content;
  if (isPending) content = <Text c="dimmed" size="sm">Загрузка алертов…</Text>;
  else if (isError)
    content = (
      <Stack gap="xs" align="flex-start">
        <Alert color="red">Нет данных об алертах</Alert>
        <Anchor size="sm" onClick={onRetry}>Повторить</Anchor>
      </Stack>
    );
  else if (rows.length === 0) content = <Text c="teal" size="sm">Критических алертов нет</Text>;
  else
    content = (
      <Stack gap={4}>
        {rows.map((a) => (
          <Group key={a.id} gap="sm" wrap="nowrap" align="flex-start">
            <Badge color={a.severity === 'critical' ? 'red' : 'yellow'} variant="light">{a.severity}</Badge>
            <Text size="sm" ff="monospace">{a.kind}</Text>
            <Text size="sm" ff="monospace" c="dimmed">{a.target}</Text>
            <Text size="sm" style={{ flex: 1 }}>{a.message}</Text>
            <Tooltip label={formatUnix(a.sinceUnix)}>
              <span>
                <Text size="sm" c="dimmed">{a.sinceUnix === null ? '—' : `с ${formatUnixAge(a.sinceUnix)}`}</Text>
              </span>
            </Tooltip>
          </Group>
        ))}
      </Stack>
    );
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Лента алертов</Text>
        <Anchor component={Link} to="/alerts" size="sm">Все алерты →</Anchor>
      </Group>
      {content}
    </Card>
  );
}
