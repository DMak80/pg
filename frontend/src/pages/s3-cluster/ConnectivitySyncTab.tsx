// Вкладка «Связность и синхронность» (arch/03 §12.3): connectivity-матрица
// ok/offline + state + checked_unix-штамп + probe_error; sync-таблица пар
// (missing/extra/lag) + state + last_error. Источник — зонд S3Worker.
import { Badge, Group, Stack, Table, Text, Title } from '@mantine/core';
import { formatUnix } from '../../utils/format';
import type { S3ClusterDto } from '../../api/dto';

function ProbeBadge({ state }: { state: string | null }) {
  if (state === null) return <Text c="dimmed">—</Text>;
  const color = state === 'OK' || state === 'SYNCED'
    ? 'green'
    : state === 'DEGRADED' || state === 'LAGGING'
      ? 'yellow'
      : 'red';
  return <Badge color={color} variant="light">{state}</Badge>;
}

export function ConnectivitySyncTab({ cluster }: { cluster: S3ClusterDto }) {
  const connectivity = cluster.connectivity;
  const sync = cluster.sync;

  return (
    <Stack gap="md">
      <Text size="xs" c="dimmed">
        Источник — зонд S3Worker (ключи /s3/clusters/{cluster.name}/{'{'}connectivity,sync{'}'})
      </Text>

      {/* Связность: state + штамп + probe_error + матрица ok/offline. */}
      <Stack gap="xs">
        <Group gap="sm">
          <Title order={4}>Связность</Title>
          <ProbeBadge state={connectivity?.state ?? null} />
          <Text size="sm" c="dimmed">
            проверено: {formatUnix(connectivity?.checkedUnix ?? null)}
          </Text>
        </Group>
        {connectivity?.probeError ? (
          <Text size="sm" c="red">probe_error: {connectivity.probeError}</Text>
        ) : null}
        {connectivity !== null && Object.keys(connectivity.links).length > 0 ? (
          <Table.ScrollContainer minWidth={420}>
            <Table withTableBorder verticalSpacing="xs">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>нода → сосед</Table.Th>
                  <Table.Th>Статус</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {Object.entries(connectivity.links).flatMap(([node, cells]) =>
                  Object.entries(cells).map(([peer, status]) => (
                    <Table.Tr key={`${node}-${peer}`}>
                      <Table.Td>{node} → {peer}</Table.Td>
                      <Table.Td>
                        <Badge color={status === 'ok' ? 'green' : 'red'} variant="light">
                          {status}
                        </Badge>
                      </Table.Td>
                    </Table.Tr>
                  )))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        ) : (
          <Text c="dimmed">линков не настроено</Text>
        )}
      </Stack>

      {/* Синхронность: state + штамп + last_error + таблица пар. */}
      <Stack gap="xs">
        <Group gap="sm">
          <Title order={4}>Синхронность</Title>
          <ProbeBadge state={sync?.state ?? null} />
          <Text size="sm" c="dimmed">
            проверено: {formatUnix(sync?.checkedUnix ?? null)}
          </Text>
        </Group>
        {sync?.lastError ? (
          <Text size="sm" c="red">last_error: {sync.lastError}</Text>
        ) : null}
        {sync !== null && sync.pairs.length > 0 ? (
          <Table.ScrollContainer minWidth={620}>
            <Table withTableBorder verticalSpacing="xs">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>from</Table.Th>
                  <Table.Th>to</Table.Th>
                  <Table.Th>missing</Table.Th>
                  <Table.Th>extra</Table.Th>
                  <Table.Th>lag_bytes</Table.Th>
                  <Table.Th>lag_objects</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {sync.pairs.map((p) => (
                  <Table.Tr key={`${p.from}-${p.to}`}>
                    <Table.Td>{p.from}</Table.Td>
                    <Table.Td>{p.to}</Table.Td>
                    <Table.Td c={p.missing > 0 ? 'yellow' : undefined}>{p.missing}</Table.Td>
                    <Table.Td c={p.extra > 0 ? 'yellow' : undefined}>{p.extra}</Table.Td>
                    <Table.Td>{p.lagBytes ?? '—'}</Table.Td>
                    <Table.Td>{p.lagObjects ?? '—'}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        ) : (
          <Text c="dimmed">пар сверки нет (0 RUNNING-нод или зонд ещё не гонялся)</Text>
        )}
      </Stack>
    </Stack>
  );
}
