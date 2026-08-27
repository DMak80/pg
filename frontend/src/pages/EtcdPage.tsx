// Панель etcd: endpoints (+метка «активный»), members/лидер, alarms, lastRefresh,
// баннер подозрения кворума (t08 spec §5).
import { useQuery } from '@tanstack/react-query';
import {
  Alert,
  Badge,
  Card,
  Stack,
  Table,
  Text,
  Title,
  Tooltip,
} from '@mantine/core';
import type { EtcdEndpointDto, EtcdStatusDto } from '../api/dto';
import { fetchEtcdStatus, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes, formatIso } from '../utils/format';

export function EtcdPage() {
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: queryKeys.etcdStatus,
    queryFn: fetchEtcdStatus,
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
  return (
    <Stack gap="md">
      <Title order={2}>etcd</Title>
      {data.quorumSuspected ? (
        <Alert color="red" title="Подозрение на отсутствие кворума">
          Признаки отсутствия raft-лидера — проверьте статус членов кластера
        </Alert>
      ) : null}
      <EndpointsCard data={data} />
      <MembersCard data={data} />
      <AlarmsCard data={data} />
      <Text c="dimmed" size="sm">Обновлено: {formatIso(data.lastRefreshUtc)}</Text>
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
              <Table.Th>Статус</Table.Th>
              <Table.Th>Задержка</Table.Th>
              <Table.Th>Версия</Table.Th>
              <Table.Th>raft term</Table.Th>
              <Table.Th>Размер БД</Table.Th>
              <Table.Th>Ошибки</Table.Th>
              <Table.Th>Роль</Table.Th>
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
          {endpoint.reachable ? 'ok' : 'недоступен'}
        </Badge>
      </Table.Td>
      <Table.Td>{endpoint.latencyMs === null ? '—' : `${endpoint.latencyMs.toFixed(1)} мс`}</Table.Td>
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
        {endpoint.active ? <Badge color="blue" variant="light">активный</Badge> : null}
      </Table.Td>
    </Table.Tr>
  );
}

// Члены кластера etcd: id, имена, URL; лидер — меткой (t08 spec §5).
function MembersCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Члены кластера</Text>
      {data.members.length === 0 ? (
        <Text c="dimmed" size="sm">Нет данных о членах</Text>
      ) : (
        <Table.ScrollContainer minWidth={800}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>ID</Table.Th>
                <Table.Th>Имя</Table.Th>
                <Table.Th>Peer URLs</Table.Th>
                <Table.Th>Client URLs</Table.Th>
                <Table.Th>Роль</Table.Th>
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
                    {m.isLeader ? <Badge color="violet" variant="light">лидер</Badge> : null}
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

// Alarms: member → тип; пусто — зелёная строка (t08 spec §5).
function AlarmsCard({ data }: { data: EtcdStatusDto }) {
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Alarms</Text>
      {data.alarms.length === 0 ? (
        <Text c="teal" size="sm">Активных alarm'ов нет</Text>
      ) : (
        <Table highlightOnHover w="50%">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Member ID</Table.Th>
              <Table.Th>Тип</Table.Th>
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
