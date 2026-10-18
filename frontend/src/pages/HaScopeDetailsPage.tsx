// Детали HA-скопа: шапка (лидер, optime, кластер/шард), таблица членов с
// probe-статусом, raw config свёрнуто (t09 spec §4.4–4.9).
// Кнопка «Пересоздать» — operator-triggered rebuild ноды (TO_RECREATE):
// мягко (switchover для лидера) или грубо (снос сразу, failover — Patroni).
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Accordion, Alert, Anchor, Badge, Button, Group, Modal, Radio, Select, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link, useParams } from 'react-router';
import { useState } from 'react';
import type { HaMemberDto, RecreateMode } from '../api/dto';
import { fetchHaScope, queryKeys, recreateNode } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatIso, formatIsoAge } from '../utils/format';
import { ApiError } from '../api/client';

// Карта ролей Patroni: русские подписи известных, канон — в Tooltip (t09 spec §4.6);
// master — violet, рифма с Badge «лидер» etcd-панели (t08 EtcdPage).
const ROLE_META: Record<string, { color: string; label: string }> = {
  master: { color: 'violet', label: 'master' },
  replica: { color: 'blue', label: 'replica' },
  sync_standby: { color: 'teal', label: 'sync-standby' },
};

// Карта цветов состояний Patroni: строка как есть, без перевода — внешний
// идентификатор (t09 spec §4.7); неизвестные — серые (не ошибка).
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

export function HaScopeDetailsPage() {
  const { scope = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.haScope(scope),
    queryFn: () => fetchHaScope(scope),
    refetchInterval: intervalMs,
  });

  // 404 (скоп исчез между тиками) → notFound-контент; 503/сеть → ErrorSection (t08 §4.15).
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection
        error={query.error}
        onRetry={() => void query.refetch()}
        notFound={
          <Stack gap="xs">
            <Text>Scope not found</Text>
            <Anchor component={Link} to="/ha" size="sm">← HA</Anchor>
          </Stack>
        }
      />
    ) : (
      <LoadingSection />
    );

  const data = query.data;
  return (
    <Stack gap="md">
      <div>
        <Anchor component={Link} to="/ha" size="sm">← HA</Anchor>
        <Group gap="sm" mt={4}>
          <Title order={2} ff="monospace">{data.scope}</Title>
          {data.matched ? null : (
            <Tooltip label="scope is not mapped to a cluster (arch/02 §7)">
              <Badge color="yellow" variant="light">unmatched</Badge>
            </Tooltip>
          )}
        </Group>
        <Group gap="sm" mt={4}>
          <Text c="dimmed" size="sm">
            Cluster/shard:{' '}
            {data.cluster === null ? '—' : (
              <Anchor component={Link} to={`/clusters/${data.cluster}`} size="sm">
                {data.cluster}/{data.shard ?? '—'}
              </Anchor>
            )}
          </Text>
          <Text c="dimmed" size="sm">
            Leader:{' '}
            {data.leaderName === null ? (
              <Badge color={data.matched ? 'red' : 'gray'} variant="light">no leader</Badge>
            ) : (
              <Text ff="monospace" size="sm" span>{data.leaderName}</Text>
            )}
          </Text>
          <Text c="dimmed" size="sm">
            leader optime:{' '}
            <Text ff="monospace" size="sm" span>{data.optimeLeader ?? '—'}</Text>
          </Text>
        </Group>
      </div>
      <Group justify="space-between">
        <div />
        {data.matched ? <RecreateNodeButton scope={data.scope} members={data.members} /> : null}
      </Group>
      <MembersTable members={data.members} leaderName={data.leaderName} />
      {data.requests === null ? null : (
        <Group gap="sm">
          <Text c="dimmed" size="sm">Declared node resources:</Text>
          <Badge variant="light" color="gray">{data.requests.cpu} CPU</Badge>
          <Badge variant="light" color="gray">{data.requests.mem}</Badge>
          <Badge variant="light" color="gray">{data.requests.disk}</Badge>
        </Group>
      )}
      {data.rawConfig === null ? null : (
        <Accordion>
          <Accordion.Item value="raw-config">
            <Accordion.Control>Raw config</Accordion.Control>
            <Accordion.Panel>
              <Text ff="monospace" size="sm" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {data.rawConfig}
              </Text>
            </Accordion.Panel>
          </Accordion.Item>
        </Accordion>
      )}
    </Stack>
  );
}

// Кнопка «Пересоздать»: открывает модалку с выбором ноды.
// Недоступна если: 1 нода (последняя), или все остальные уже в REBUILDING/TO_RECREATE.
function RecreateNodeButton({ scope, members }: { scope: string; members: HaMemberDto[] }) {
  const [opened, setOpened] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [mode, setMode] = useState<RecreateMode>('soft');
  const [error, setError] = useState<string | null>(null);
  const queryClient = useQueryClient();

  const recreatingStates = new Set(['REBUILDING', 'TO_RECREATE']);
  // Доступные для пересоздания: ноды, у которых хотя бы одна ДРУГАЯ нода не в процессе пересоздания.
  const canRecreate = (member: HaMemberDto) => {
    const others = members.filter((m) => m.name !== member.name);
    if (others.length === 0) return false;
    return !others.every((m) => m.nodeState !== null && recreatingStates.has(m.nodeState));
  };

  const options = members.map((m) => ({
    value: m.name,
    label: m.name + (m.nodeState && recreatingStates.has(m.nodeState) ? ' (already recreating)' : ''),
    disabled: !canRecreate(m),
  }));

  const mutation = useMutation({
    mutationFn: (node: string) => recreateNode(scope, node, mode),
    onSuccess: async () => {
      setOpened(false);
      setSelected(null);
      setError(null);
      await queryClient.invalidateQueries({ queryKey: queryKeys.haScope(scope) });
    },
    onError: (e: unknown) => {
      setError(e instanceof ApiError ? e.message : 'Failed to queue the recreate marker');
    },
  });

  // Каждое открытие — безопасный дефолт «мягко».
  const openModal = () => {
    setMode('soft');
    setError(null);
    setOpened(true);
  };

  return (
    <>
      <Button size="xs" variant="light" color="orange" onClick={openModal}>
        Recreate node
      </Button>
      <Modal opened={opened} onClose={() => { setOpened(false); setError(null); }} title="Recreate node" centered>
        <Stack gap="sm">
          <Text size="sm" c="dimmed">
            The node will be deleted (container and volume) and recreated with the same address. Patroni will run pg_basebackup from a live replica.
          </Text>
          <Select
            label="Node"
            placeholder="Select a node"
            data={options}
            value={selected}
            onChange={setSelected}
            searchable={false}
          />
          <Radio.Group
            label="Mode (matters for the leader)"
            value={mode}
            onChange={(v) => setMode(v as RecreateMode)}
          >
            <Stack gap="xs" mt={4}>
              <Radio value="soft" label="Soft — switchover first: leadership moves without a write pause, then the node is recreated" />
              <Radio value="hard" label="Hard — tear down immediately: Patroni will run failover itself (short write pause)" />
            </Stack>
          </Radio.Group>
          {error ? <Alert color="red" variant="light">{error}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => { setOpened(false); setError(null); }}>Cancel</Button>
            <Button
              color="orange"
              loading={mutation.isPending}
              disabled={selected === null}
              onClick={() => selected && mutation.mutate(selected)}
            >
              Recreate
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}

// Таблица членов: имя (+метка лидера), адрес, роль, состояние, timeline, лаг,
// probe-статус (t09 spec §4.5).
function MembersTable({ members, leaderName }: { members: HaMemberDto[]; leaderName: string | null }) {
  if (members.length === 0) return <Text c="dimmed">No members found</Text>;
  return (
    <Table.ScrollContainer minWidth={900}>
      <Table highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Name</Table.Th>
            <Table.Th>Address</Table.Th>
            <Table.Th>Role</Table.Th>
            <Table.Th>State</Table.Th>
            <Table.Th>Timeline</Table.Th>
            <Table.Th>Lag</Table.Th>
            <Table.Th>Probe</Table.Th>
            <Table.Th>Node state</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {members.map((m) => (
            <MemberRow key={m.name} member={m} isLeader={m.name === leaderName} />
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}

function MemberRow({ member, isLeader }: { member: HaMemberDto; isLeader: boolean }) {
  return (
    <Table.Tr>
      <Table.Td>
        <Group gap="xs" wrap="nowrap">
          <Text ff="monospace" size="sm">{member.name}</Text>
          {isLeader ? <Badge color="violet" variant="light">leader</Badge> : null}
        </Group>
      </Table.Td>
      <Table.Td>
        <Text ff="monospace" size="sm">
          {member.port === null ? member.host : `${member.host}:${member.port}`}
        </Text>
      </Table.Td>
      <Table.Td><RoleBadge role={member.role} /></Table.Td>
      <Table.Td><StateBadge state={member.state} /></Table.Td>
      <Table.Td>{member.timeline ?? '—'}</Table.Td>
      <Table.Td>{formatBytes(member.lagBytes)}</Table.Td>
      <Table.Td><ProbeCell member={member} /></Table.Td>
      <Table.Td><NodeStateCell nodeState={member.nodeState} /></Table.Td>
    </Table.Tr>
  );
}

// Роль: известные — Badge с русской подписью, канон в Tooltip; прочие — серым (t09 spec §4.6).
function RoleBadge({ role }: { role: string | null }) {
  if (role === null) return <Text c="dimmed">—</Text>;
  const meta = ROLE_META[role];
  return (
    <Tooltip label={role}>
      <Badge color={meta?.color ?? 'gray'} variant="light">{meta?.label ?? role}</Badge>
    </Tooltip>
  );
}

// Состояние: строка Patroni как есть, цвет по карте (t09 spec §4.7).
function StateBadge({ state }: { state: string | null }) {
  if (state === null) return <Text c="dimmed">—</Text>;
  return <Badge color={STATE_COLORS[state] ?? 'gray'} variant="light">{state}</Badge>;
}

// Probe-статус: ошибка / возраст с абсолютом в Tooltip / «—» — проб не было (t09 spec §4.8).
function ProbeCell({ member }: { member: HaMemberDto }) {
  if (member.probeError !== null)
    return (
      <Group gap="xs" wrap="nowrap">
        <Tooltip label={member.probeError} multiline>
          <Badge color="red" variant="light">error</Badge>
        </Tooltip>
        {member.probeAtUtc === null ? null : (
          <Text size="sm" c="dimmed">{formatIsoAge(member.probeAtUtc)}</Text>
        )}
      </Group>
    );
  if (member.probeAtUtc !== null)
    return (
      <Tooltip label={formatIso(member.probeAtUtc)}>
        <span><Text size="sm" c="dimmed">{formatIsoAge(member.probeAtUtc)}</Text></span>
      </Tooltip>
    );
  return <Text c="dimmed">—</Text>;
}

// Node state из etcd (/clusters/<C>/shards/<X>/nodes/<n>/state).
// REBUILDING/TO_RECREATE — оранжевым (в процессе пересоздания), RUNNING — зелёным.
function NodeStateCell({ nodeState }: { nodeState: string | null }) {
  if (nodeState === null) return <Text c="dimmed">—</Text>;
  const colors: Record<string, string> = {
    RUNNING: 'teal',
    PROVISIONING: 'yellow',
    REBUILDING: 'orange',
    TO_RECREATE: 'orange',
    UNREACHABLE: 'red',
    QUARANTINED: 'red',
    REMOVING: 'red',
    NOT_INITIALIZED: 'gray',
  };
  return <Badge color={colors[nodeState] ?? 'gray'} variant="light">{nodeState}</Badge>;
}
