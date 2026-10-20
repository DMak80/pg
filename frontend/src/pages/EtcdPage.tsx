// Панель etcd: endpoints (+метка «активный»), блок «Состав» контура
// (декларация EtcdWorker, managed-узлы, форма «Поддерживать N узлов» — 02 §9.13),
// members/лидер (+пометки managed/external), карточка DR-слепков, alarms,
// lastRefresh, баннер подозрения кворума (t08 spec §5).
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Badge,
  Button,
  Card,
  Group,
  Modal,
  NumberInput,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import { useState } from 'react';
import type { EtcdDomainDto, EtcdEndpointDto, EtcdStatusDto } from '../api/dto';
import { fetchEtcdDomain, fetchEtcdStatus, putEtcdConfig, queryKeys } from '../api/queries';
import { ApiError } from '../api/client';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatIso, formatUnix, formatUnixAge } from '../utils/format';

// Managed-члены контура — префикс имени etcdw (arch/25 §2; arch/03 §2).
function isManagedMember(name: string | null): boolean {
  return name !== null && name.startsWith('etcdw');
}

export function EtcdPage() {
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.etcdStatus,
    queryFn: fetchEtcdStatus,
    refetchInterval: intervalMs,
  });
  const domainQuery = useQuery({
    queryKey: queryKeys.etcdDomain,
    queryFn: fetchEtcdDomain,
    refetchInterval: intervalMs,
  });

  // Паттерн состояний (t08 spec §4.15): нет данных + ошибка → ErrorSection;
  // нет данных без ошибки → загрузка; ошибка ПРИ данных (polling-сбой) — тихо,
  // показываем предыдущие данные (StaleBadge в шапке уже сигнализирует).
  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const data = query.data;
  const domain = domainQuery.data;
  return (
    <Stack gap="md">
      <Title order={2}>etcd</Title>
      {data.quorumSuspected ? (
        <Alert color="red" title="Quorum loss suspected">
          Signs of a missing raft leader — check the cluster members status
        </Alert>
      ) : null}
      <EndpointsCard data={data} />
      <ContourCard domain={domain} membersTotal={data.members.length} />
      <MembersCard data={data} />
      <DrSnapshotsCard domain={domain} />
      <AlarmsCard data={data} />
      <Text c="dimmed" size="sm">Updated: {formatIso(data.lastRefreshUtc)}</Text>
    </Stack>
  );
}

// Таблица endpoints: доступность, латентность, версия, raft term, размер БД, ошибки (t08 spec §5).
function EndpointsCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Endpoints</Text>
      <Table.ScrollContainer minWidth={900}>
        <Table highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>URL</Table.Th>
              <Table.Th>Status</Table.Th>
              <Table.Th>Latency</Table.Th>
              <Table.Th>Version</Table.Th>
              <Table.Th>raft term</Table.Th>
              <Table.Th>DB size</Table.Th>
              <Table.Th>Errors</Table.Th>
              <Table.Th>Role</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {data.endpoints.map((e) => <EndpointRow key={e.url} endpoint={e} />)}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Card>
  );
}

function EndpointRow({ endpoint }: { endpoint: EtcdEndpointDto }) {
  return (
    <Table.Tr>
      <Table.Td ff="monospace">{endpoint.url}</Table.Td>
      <Table.Td>
        <Badge color={endpoint.reachable ? 'teal' : 'red'} variant="light">
          {endpoint.reachable ? 'ok' : 'unreachable'}
        </Badge>
      </Table.Td>
      <Table.Td>{endpoint.latencyMs === null ? '—' : `${endpoint.latencyMs.toFixed(1)} ms`}</Table.Td>
      <Table.Td>{endpoint.version ?? '—'}</Table.Td>
      <Table.Td>{endpoint.raftTerm === null ? '—' : endpoint.raftTerm}</Table.Td>
      <Table.Td>{formatBytes(endpoint.dbSizeBytes)}</Table.Td>
      <Table.Td>
        {endpoint.errors.length === 0 ? '—' : (
          <Tooltip multiline label={endpoint.errors.join('\n')}>
            <Badge color="red" variant="light">{endpoint.errors.length}</Badge>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>
        {endpoint.active ? <Badge color="blue" variant="light">active</Badge> : null}
      </Table.Td>
    </Table.Tr>
  );
}

// Блок «Состав» контура (arch/03 §2): заявлено N управляемых узлов или
// «только наблюдение»; managed-узлы /etcd/nodes/<k>/; форма «Поддерживать N
// узлов» — мутация через API воркера (PUT /api/etcd/config, 02 §9.13).
function ContourCard({ domain, membersTotal }: { domain: EtcdDomainDto | undefined; membersTotal: number }) {
  const [opened, setOpened] = useState(false);
  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Contour membership</Text>
        <Button size="xs" variant="light" onClick={() => setOpened(true)}>Maintain N nodes</Button>
      </Group>
      {domain === undefined ? (
        <Text c="dimmed" size="sm">Loading…</Text>
      ) : (
        <Stack gap="xs">
          <Group gap="xs">
            <Text size="sm">Declared:</Text>
            {domain.declaredNodes === null ? (
              <Badge color="gray" variant="light">observation only (no /etcd/config)</Badge>
            ) : (
              <Badge color="blue" variant="light">{domain.declaredNodes} managed nodes</Badge>
            )}
            <Text size="sm" c="dimmed">actual members: {membersTotal}</Text>
          </Group>
          {domain.nodes.length === 0 ? (
            <Text c="dimmed" size="sm">No managed nodes</Text>
          ) : (
            <Table.ScrollContainer minWidth={600}>
              <Table highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>K</Table.Th>
                    <Table.Th>State</Table.Th>
                    <Table.Th>Placed on</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {domain.nodes.map((n) => (
                    <Table.Tr key={n.k}>
                      <Table.Td ff="monospace">etcdw-node-{n.k}</Table.Td>
                      <Table.Td><NodeStateBadge state={n.state} /></Table.Td>
                      <Table.Td>{n.placedOn ?? '—'}</Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          )}
          <Text size="xs" c="dimmed">
            Доводка состава асинхронна: EtcdWorker добавляет (learner → promote) / выводит
            узлы по одному шагу за тик — прогресс виден по state выше.
          </Text>
        </Stack>
      )}
      <MaintainNodesModal opened={opened} onClose={() => setOpened(false)} domain={domain} />
    </Card>
  );
}

// Бейдж состояния managed-узла (NOT_INITIALIZED|PROVISIONING|RUNNING|UNREACHABLE|REMOVING).
function NodeStateBadge({ state }: { state: string | null }) {
  const color = state === 'RUNNING'
    ? 'teal'
    : state === 'PROVISIONING'
      ? 'yellow'
      : state === 'UNREACHABLE'
        ? 'red'
        : state === 'REMOVING'
          ? 'orange'
          : 'gray';
  return <Badge color={color} variant="light">{state ?? '—'}</Badge>;
}

// Модал «Поддерживать N узлов» (02 §9.13): NumberInput 0..7, двойной клик
// защищён (isPending); текст ошибки (400/409/503) — из ProblemDetails как есть.
function MaintainNodesModal({
  opened,
  onClose,
  domain,
}: {
  opened: boolean;
  onClose: () => void;
  domain: EtcdDomainDto | undefined;
}) {
  const queryClient = useQueryClient();
  const [nodes, setNodes] = useState<number>(0);
  const [lastOpenedDomain, setLastOpenedDomain] = useState<EtcdDomainDto | undefined>(undefined);
  // При каждом открытии — стартовое значение из текущей декларации.
  if (opened && lastOpenedDomain !== domain) {
    setLastOpenedDomain(domain);
    setNodes(domain?.declaredNodes ?? 0);
  }
  const mutation = useMutation({
    mutationFn: () => putEtcdConfig(nodes),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: queryKeys.etcdDomain });
    },
  });
  const invalid = !Number.isInteger(nodes) || nodes < 0 || nodes > 7;
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title="Maintain N nodes" centered>
      <Stack gap="sm">
        <Text size="sm">
          Декларация замещается целиком: EtcdWorker доведёт состав контура до{' '}
          <b>{Number.isInteger(nodes) ? nodes : '?'}</b> managed-узлов (кворум-гвард:
          итоговое число членов ≥ 3).
        </Text>
        <NumberInput
          value={nodes}
          onChange={(v) => setNodes(typeof v === 'number' ? v : 0)}
          min={0}
          max={7}
          step={1}
          label="Managed nodes (0..7)"
        />
        {invalid ? (
          <Text c="red" size="sm">nodes must be an integer in 0..7</Text>
        ) : null}
        {serverError !== null ? (
          <Alert color="red" title={`HTTP ${serverError.status}`}>
            {serverError.detail ?? serverError.title ?? serverError.message}
          </Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button
            disabled={invalid}
            loading={mutation.isPending}
            onClick={() => void mutation.mutate()}
          >
            Apply
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}

// Члены кластера etcd: id, имена, URL; лидер — меткой; managed/external —
// по префиксу имени etcdw (arch/25 §2, arch/03 §2).
function MembersCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Cluster members</Text>
      {data.members.length === 0 ? (
        <Text c="dimmed" size="sm">No member data</Text>
      ) : (
        <Table.ScrollContainer minWidth={800}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>ID</Table.Th>
                <Table.Th>Name</Table.Th>
                <Table.Th>Peer URLs</Table.Th>
                <Table.Th>Client URLs</Table.Th>
                <Table.Th>Role</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {data.members.map((m) => (
                <Table.Tr key={m.id}>
                  <Table.Td ff="monospace">{m.id}</Table.Td>
                  <Table.Td>{m.name ?? '—'}</Table.Td>
                  <Table.Td ff="monospace">{m.peerUrls.length === 0 ? '—' : m.peerUrls.join(', ')}</Table.Td>
                  <Table.Td ff="monospace">{m.clientUrls.length === 0 ? '—' : m.clientUrls.join(', ')}</Table.Td>
                  <Table.Td>
                    <Group gap={4}>
                      {m.isLeader ? <Badge color="violet" variant="light">leader</Badge> : null}
                      {m.name !== null ? (
                        <Badge color={isManagedMember(m.name) ? 'blue' : 'gray'} variant="light">
                          {isManagedMember(m.name) ? 'managed' : 'external'}
                        </Badge>
                      ) : null}
                    </Group>
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

// Карточка «DR-слепки» (arch/03 §2): статус выгрузки EtcdWorker
// (/etcdworker/snapshots): состояние, возраст, sha-префикс, размер, ошибка.
function DrSnapshotsCard({ domain }: { domain: EtcdDomainDto | undefined }) {
  const snap = domain?.snapshots ?? null;
  return (
    <Card withBorder padding="md" radius="md" w={420}>
      <Group justify="space-between" mb="xs">
        <Text fw={600}>DR snapshots</Text>
        {snap === null ? (
          <Badge color="gray" variant="light">no key</Badge>
        ) : (
          <Badge color={snap.state === 'OK' ? 'green' : 'red'} variant="light">{snap.state ?? '?'}</Badge>
        )}
      </Group>
      {snap === null ? (
        <Text c="dimmed" size="sm">
          No /etcdworker/snapshots key — the worker has not uploaded a snapshot yet
          (check the etcd-backup S3 cluster and EtcdWorker logs)
        </Text>
      ) : (
        <Stack gap="xs">
          <Text size="sm">
            Last upload:{' '}
            {snap.lastUploadedUnix === null
              ? '—'
              : `${formatUnix(snap.lastUploadedUnix)} (${formatUnixAge(snap.lastUploadedUnix)} ago)`}
          </Text>
          <Text size="sm" c="dimmed" ff="monospace">
            sha256: {snap.lastSha256 === null ? '—' : snap.lastSha256.slice(0, 12)}
          </Text>
          <Text size="sm" c="dimmed">
            size: {snap.sizeBytes === null ? '—' : formatBytes(snap.sizeBytes)}
          </Text>
          {snap.error !== null ? (
            <Text size="sm" c="red">{snap.error}</Text>
          ) : null}
        </Stack>
      )}
    </Card>
  );
}

// Alarms: member → тип; пусто — зелёная строка (t08 spec §5).
function AlarmsCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Alarms</Text>
      {data.alarms.length === 0 ? (
        <Text c="teal" size="sm">No active alarms</Text>
      ) : (
        <Table highlightOnHover w="50%">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Member ID</Table.Th>
              <Table.Th>Type</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {data.alarms.map((a) => (
              <Table.Tr key={`${a.memberId}-${a.type}`}>
                <Table.Td ff="monospace">{a.memberId}</Table.Td>
                <Table.Td><Badge color="red" variant="light">{a.type}</Badge></Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Card>
  );
}
