// Страница шарда /clusters/:cluster/shards/:shard (u03, arch/03 §3): детали
// нод шарда (ShardDto), заявка ресурсов, runtime SQL (при включённых пробах) +
// HA-блок scope <c>-<x>: leader/optime/members/raw config свёрнуто + кнопка
// «Пересоздать ноду» soft/hard (перенос с HaScopeDetailsPage) + заявленные
// ресурсы scope. HA-скоп не найден — блок скрыт с пометкой.
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Button, Card, Group, SimpleGrid, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link, useParams } from 'react-router';
import type { HaMemberDto, ShardRuntimeDto, ShardDto } from '../../api/dto';
import { fetchClusterDetails, fetchHaScope, queryKeys } from '../../api/queries';
import { ErrorSection, LoadingSection } from '../../components/LoadState';
import { usePollingIntervalMs } from '../../polling/PollingContext';
import { formatBytes, formatIso } from '../../utils/format';
import { RecreateNodeButton } from '../HaScopeDetailsPage';

// Карта цветов состояний Patroni (порт HaScopeDetailsPage): строка как есть.
const STATE_COLORS: Record<string, string> = {
  running: 'teal',
  streaming: 'teal',
  stopped: 'red',
  starting: 'yellow',
  'creating replica': 'yellow',
  restart: 'yellow',
  'crash reinit': 'yellow',
  waiting: 'yellow',
};

// Карта ролей Patroni (порт HaScopeDetailsPage).
const ROLE_META: Record<string, { color: string; label: string }> = {
  master: { color: 'violet', label: 'master' },
  replica: { color: 'blue', label: 'replica' },
  sync_standby: { color: 'teal', label: 'sync-standby' },
};

export function ShardDetailsPage() {
  const { cluster = '', shard = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const details = useQuery({
    queryKey: queryKeys.cluster(cluster),
    queryFn: () => fetchClusterDetails(cluster),
    refetchInterval: intervalMs,
  });
  // HA-данные скопа `<c>-<x>`; 404/ошибка → HA-блок скрыт с пометкой (u03).
  const ha = useQuery({
    queryKey: queryKeys.shardHa(cluster, shard),
    queryFn: () => fetchHaScope(`${cluster}-${shard}`),
    refetchInterval: intervalMs,
    retry: false,
  });

  if (details.data === undefined)
    return details.isError ? (
      <ErrorSection
        error={details.error}
        onRetry={() => void details.refetch()}
        notFound={
          <Stack gap="xs">
            <Text>Кластер не найден</Text>
            <Anchor component={Link} to={`/clusters/${cluster}`} size="sm">← Кластер</Anchor>
          </Stack>
        }
      />
    ) : (
      <LoadingSection />
    );

  const shardDto: ShardDto | null =
    details.data.shards.find((s) => s.name === shard) ?? null;
  const haData = ha.data ?? null;
  const haAvailable = haData !== null && haData.matched;
  const runtime = shardDto?.runtime ?? null;

  return (
    <Stack gap="md">
      <div>
        <Anchor component={Link} to={`/clusters/${cluster}`} size="sm">← Кластер {cluster}</Anchor>
        <Group gap="sm" mt={4}>
          <Title order={2} ff="monospace">{shard}</Title>
          {shardDto?.state === 'TO_REMOVE' ? (
            <Badge color="gray" variant="light">TO_REMOVE</Badge>
          ) : null}
        </Group>
      </div>

      {/* Детали шарда (ShardDto): dsn/replicas/master + ноды + заявка ресурсов */}
      <Card withBorder padding="md" radius="md">
        <Text fw={600} mb="xs">Детали шарда</Text>
        {shardDto === null ? (
          <Text c="dimmed">Шард не найден в декларации кластера.</Text>
        ) : (
          <Stack gap="xs">
            <SimpleGrid cols={{ base: 2, md: 4 }}>
              <Field label="DSN">
                <Text size="sm" ff="monospace">{shardDto.dsn}</Text>
              </Field>
              <Field label="Replicas (declared)">{String(shardDto.replicasDeclared ?? '—')}</Field>
              <Field label="Master">
                <Group gap={4}>
                  <Text size="sm" ff="monospace">{shardDto.masterAddress ?? '—'}</Text>
                  {shardDto.masterLeaseAlive ? (
                    <Badge color="green" variant="light">lease alive</Badge>
                  ) : (
                    <Tooltip label="lease-ключ мастера протух или отсутствует (P11)">
                      <Badge color="red" variant="light">lease dead</Badge>
                    </Tooltip>
                  )}
                </Group>
              </Field>
              <Field label="State">
                <Badge color={shardDto.state === 'ACTIVE' ? 'teal' : 'gray'} variant="light">
                  {shardDto.state}
                </Badge>
              </Field>
            </SimpleGrid>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Нода</Table.Th>
                  <Table.Th>State</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {shardDto.nodes.map((n) => (
                  <Table.Tr key={n.name}>
                    <Table.Td>
                      <Text size="sm" ff="monospace">{n.name}</Text>
                    </Table.Td>
                    <Table.Td>
                      <Badge color={n.state === 'RUNNING' ? 'teal' : 'gray'} variant="light">
                        {n.state ?? '—'}
                      </Badge>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
            {shardDto.requests !== null ? (
              <Group gap="xs">
                <Text size="sm" c="dimmed">Заявка ресурсов на ноду:</Text>
                <Badge variant="light" color="gray">{shardDto.requests.cpu} CPU</Badge>
                <Badge variant="light" color="gray">{shardDto.requests.mem}</Badge>
                <Badge variant="light" color="gray">{shardDto.requests.disk}</Badge>
              </Group>
            ) : null}
            {/* Runtime SQL (spec: только при включённых пробах) */}
            {runtime !== null ? <RuntimeBlock runtime={runtime} /> : null}
          </Stack>
        )}
      </Card>

      {/* HA-блок (u03): leader/optime/members/raw config/пересоздание/заявки scope */}
      <Card withBorder padding="md" radius="md">
        <Group justify="space-between" mb="xs">
          <Text fw={600}>HA-scope <Text span ff="monospace">{cluster}-{shard}</Text></Text>
          {haAvailable && haData !== null ? (
            <RecreateNodeButton scope={haData.scope} members={haData.members} />
          ) : null}
        </Group>
        {ha.isError ? (
          <Stack gap="xs">
            <Text c="red" size="sm">
              HA-данные не получены (ошибка запроса, не «скопа нет»): {ha.error instanceof Error ? ha.error.message : String(ha.error)}
            </Text>
            <Button size="xs" variant="light" onClick={() => void ha.refetch()}>Повторить</Button>
          </Stack>
        ) : !haAvailable ? (
          <Text c="dimmed" size="sm">HA-scope не найден (скоп не matched либо пробы/тик ещё не пришли).</Text>
        ) : (
          <Stack gap="xs">
            <Group gap="lg">
              <Text size="sm" c="dimmed">
                Leader:{' '}
                {haData.leaderName === null ? (
                  <Badge color="red" variant="light">no leader</Badge>
                ) : (
                  <Text ff="monospace" size="sm" span>{haData.leaderName}</Text>
                )}
              </Text>
              <Text size="sm" c="dimmed">
                optime leader: <Text ff="monospace" size="sm" span>{haData.optimeLeader ?? '—'}</Text>
              </Text>
            </Group>
            <MembersTable members={haData.members} leaderName={haData.leaderName} />
            {haData.requests !== null ? (
              <Group gap="xs">
                <Text size="sm" c="dimmed">Заявленные ресурсы нод scope:</Text>
                <Badge variant="light" color="gray">{haData.requests.cpu} CPU</Badge>
                <Badge variant="light" color="gray">{haData.requests.mem}</Badge>
                <Badge variant="light" color="gray">{haData.requests.disk}</Badge>
              </Group>
            ) : null}
          </Stack>
        )}
      </Card>
    </Stack>
  );
}

// Таблица members с probe-статусом (порт HaScopeDetailsPage).
function MembersTable({ members, leaderName }: { members: HaMemberDto[]; leaderName: string | null }) {
  return (
    <Table highlightOnHover>
      <Table.Thead>
        <Table.Tr>
          <Table.Th>Member</Table.Th>
          <Table.Th>Роль</Table.Th>
          <Table.Th>Состояние</Table.Th>
          <Table.Th>Timeline</Table.Th>
          <Table.Th>Лаг</Table.Th>
          <Table.Th>Probe</Table.Th>
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {members.map((m) => {
          const isLeader = m.name === leaderName;
          const role = ROLE_META[m.role ?? ''] ?? { color: 'gray', label: m.role ?? '—' };
          return (
            <Table.Tr key={m.name}>
              <Table.Td>
                <Group gap={4}>
                  <Text size="sm" ff="monospace">{m.name}</Text>
                  {isLeader ? <Badge color="violet" variant="light">leader</Badge> : null}
                </Group>
              </Table.Td>
              <Table.Td>
                <Tooltip label={m.role ?? ''}>
                  <Badge color={role.color} variant="light">{role.label}</Badge>
                </Tooltip>
              </Table.Td>
              <Table.Td>
                <Badge color={STATE_COLORS[m.state ?? ''] ?? 'gray'} variant="light">
                  {m.state ?? '—'}
                </Badge>
              </Table.Td>
              <Table.Td>{m.timeline ?? '—'}</Table.Td>
              <Table.Td>{formatBytes(m.lagBytes)}</Table.Td>
              <Table.Td>
                {m.probeError !== null ? (
                  <Tooltip label={m.probeError}>
                    <Text size="sm" c="orange" span>probe error</Text>
                  </Tooltip>
                ) : m.probeAtUtc !== null ? (
                  <Text size="sm" c="dimmed">{formatIso(m.probeAtUtc)}</Text>
                ) : (
                  <Text size="sm" c="dimmed">—</Text>
                )}
              </Table.Td>
            </Table.Tr>
          );
        })}
      </Table.Tbody>
    </Table>
  );
}

// Runtime SQL-проба (при включённых пробах): sync-standby/слоты/подписки.
function RuntimeBlock({ runtime }: { runtime: ShardRuntimeDto }) {
  return (
    <Stack gap={4}>
      <Text size="xs" c="dimmed" tt="uppercase">Runtime SQL</Text>
      <Group gap="lg">
        <Text size="sm">sync-standby: {runtime.standbiesSync ?? '—'}</Text>
        <Text size="sm">
          slots lag max: {runtime.slotsLagMaxBytes === null ? '—' : formatBytes(runtime.slotsLagMaxBytes)}
        </Text>
        {runtime.walStatusLost.length > 0 ? (
          <Text size="sm" c="red">wal lost: {runtime.walStatusLost.join(', ')}</Text>
        ) : null}
      </Group>
      {runtime.error !== null ? <Text size="sm" c="red">{runtime.error}</Text> : null}
    </Stack>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <Stack gap={2}>
      <Text size="xs" c="dimmed" tt="uppercase">{label}</Text>
      {typeof children === 'string' ? <Text>{children}</Text> : children}
    </Stack>
  );
}
