// Детали HA-скопа: шапка (лидер, optime, кластер/шард), таблица членов с
// probe-статусом, raw config свёрнуто (t09 spec §4.4–4.9).
import { useQuery } from '@tanstack/react-query';
import { Accordion, Anchor, Badge, Group, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link, useParams } from 'react-router';
import type { HaMemberDto } from '../api/dto';
import { fetchHaScope, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatIso, formatIsoAge } from '../utils/format';

// Карта ролей Patroni: русские подписи известных, канон — в Tooltip (t09 spec §4.6);
// master — violet, рифма с Badge «лидер» etcd-панели (t08 EtcdPage).
const ROLE_META: Record<string, { color: string; label: string }> = {
  master: { color: 'violet', label: 'мастер' },
  replica: { color: 'blue', label: 'реплика' },
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
            <Text>Скоп не найден</Text>
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
            <Tooltip label="scope не сопоставлен кластеру (arch/02 §7)">
              <Badge color="yellow" variant="light">unmatched</Badge>
            </Tooltip>
          )}
        </Group>
        <Group gap="sm" mt={4}>
          <Text c="dimmed" size="sm">
            Кластер/шард:{' '}
            {data.cluster === null ? '—' : (
              <Anchor component={Link} to={`/clusters/${data.cluster}`} size="sm">
                {data.cluster}/{data.shard ?? '—'}
              </Anchor>
            )}
          </Text>
          <Text c="dimmed" size="sm">
            Лидер:{' '}
            {data.leaderName === null ? (
              <Badge color={data.matched ? 'red' : 'gray'} variant="light">нет лидера</Badge>
            ) : (
              <Text ff="monospace" size="sm" span>{data.leaderName}</Text>
            )}
          </Text>
          <Text c="dimmed" size="sm">
            optime лидера:{' '}
            <Text ff="monospace" size="sm" span>{data.optimeLeader ?? '—'}</Text>
          </Text>
        </Group>
      </div>
      <MembersTable members={data.members} leaderName={data.leaderName} />
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

// Таблица членов: имя (+метка лидера), адрес, роль, состояние, timeline, лаг,
// probe-статус (t09 spec §4.5).
function MembersTable({ members, leaderName }: { members: HaMemberDto[]; leaderName: string | null }) {
  if (members.length === 0) return <Text c="dimmed">Члены не найдены</Text>;
  return (
    <Table.ScrollContainer minWidth={900}>
      <Table highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Имя</Table.Th>
            <Table.Th>Адрес</Table.Th>
            <Table.Th>Роль</Table.Th>
            <Table.Th>Состояние</Table.Th>
            <Table.Th>Timeline</Table.Th>
            <Table.Th>Лаг</Table.Th>
            <Table.Th>Проба</Table.Th>
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
          {isLeader ? <Badge color="violet" variant="light">лидер</Badge> : null}
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
          <Badge color="red" variant="light">ошибка</Badge>
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
