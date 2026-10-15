// Панель S3: сводный список s3-кластеров снапшота (arch/03 §12.3) + создание.
// Порт ValkeyClustersPage: бейджи связности/синхронности/ротации вместо maxmemory.
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Button, Card, Group, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link } from 'react-router';
import { useState } from 'react';
import type { S3ClusterSummaryDto } from '../api/dto';
import { fetchS3Clusters, s3QueryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { CreateS3ClusterModal } from './s3-cluster/CreateS3ClusterModal';

// State-бейдж канона arch/23 §2 (ACTIVE/NOT_INITIALIZED/TO_REMOVE).
function StateBadge({ state }: { state: S3ClusterSummaryDto['state'] }) {
  if (state === 'ACTIVE')
    return <Badge color="green" variant="light">ACTIVE</Badge>;
  if (state === 'NOT_INITIALIZED')
    return (
      <Tooltip label="кластер заявлен, инстансы не подняты">
        <Badge color="gray" variant="light">не инициализирован</Badge>
      </Tooltip>
    );
  return (
    <Tooltip label="TO_REMOVE: воркер демонтирует инстансы и ключи">
      <Badge color="red" variant="light">к удалению</Badge>
    </Tooltip>
  );
}

// Бейдж зонда: OK/SYNCED зелёный, DEGRADED/LAGGING жёлтый, DOWN/DIVERGED красный,
// null (зонд молчит) — прочерк.
function ProbeBadge({ value, ok, degraded }: {
  value: string | null;
  ok: string;
  degraded: string;
}) {
  if (value === null) return <Text size="sm" c="dimmed">—</Text>;
  const color = value === ok ? 'green' : value === degraded ? 'yellow' : 'red';
  return <Badge color={color} variant="light">{value}</Badge>;
}

export function S3ClustersPage() {
  const intervalMs = usePollingIntervalMs();
  const [createOpened, setCreateOpened] = useState(false);
  const query = useQuery({
    queryKey: s3QueryKeys.clusters,
    queryFn: fetchS3Clusters,
    refetchInterval: intervalMs,
  });

  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const clusters = query.data;
  return (
    <>
      <Group justify="space-between" mb="md">
        <Title order={2}>S3-кластеры</Title>
        <Button onClick={() => setCreateOpened(true)}>Создать кластер</Button>
      </Group>
      <CreateS3ClusterModal opened={createOpened} onClose={() => setCreateOpened(false)} />
      <Card withBorder padding="md" radius="md">
        {clusters.length === 0 ? (
          <Text c="dimmed">S3-кластеров нет</Text>
        ) : (
          <Table.ScrollContainer minWidth={1000}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Кластер</Table.Th>
                  <Table.Th>Состояние</Table.Th>
                  <Table.Th>Ноды</Table.Th>
                  <Table.Th>Endpoints</Table.Th>
                  <Table.Th>Связность</Table.Th>
                  <Table.Th>Синхронность</Table.Th>
                  <Table.Th>Ротация</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {clusters.map((c) => <S3ClusterRow key={c.name} cluster={c} />)}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        )}
      </Card>
    </>
  );
}

function S3ClusterRow({ cluster }: { cluster: S3ClusterSummaryDto }) {
  const down = cluster.nodesTotal - cluster.nodesRunning;
  return (
    <Table.Tr>
      <Table.Td>
        <Anchor component={Link} to={`/s3/${cluster.name}`}>{cluster.name}</Anchor>
      </Table.Td>
      <Table.Td>
        <StateBadge state={cluster.state} />
      </Table.Td>
      <Table.Td>
        <Text c={down > 0 ? 'red' : undefined}>
          {cluster.nodesRunning}/{cluster.nodesTotal}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm" ff="monospace" lineClamp={1} maw={260}>
          {cluster.endpoints ?? '—'}
        </Text>
      </Table.Td>
      <Table.Td>
        <ProbeBadge value={cluster.connectivityState} ok="OK" degraded="DEGRADED" />
      </Table.Td>
      <Table.Td>
        <ProbeBadge value={cluster.syncState} ok="SYNCED" degraded="LAGGING" />
      </Table.Td>
      <Table.Td>
        {cluster.rotationPending ? (
          <Tooltip label="заявка ротации секретов жива: воркер переиздаёт профиль rolling-пересозданием">
            <Badge color="blue" variant="light">ротация…</Badge>
          </Tooltip>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}
