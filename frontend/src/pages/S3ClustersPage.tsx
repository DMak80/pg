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
      <Tooltip label="cluster requested, instances not up">
        <Badge color="gray" variant="light">not initialized</Badge>
      </Tooltip>
    );
  return (
    <Tooltip label="TO_REMOVE: worker tears down instances and keys">
      <Badge color="red" variant="light">to remove</Badge>
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
        <Title order={2}>S3 clusters</Title>
        <Button onClick={() => setCreateOpened(true)}>Create cluster</Button>
      </Group>
      <CreateS3ClusterModal opened={createOpened} onClose={() => setCreateOpened(false)} />
      <Card withBorder padding="md" radius="md">
        {clusters.length === 0 ? (
          <Text c="dimmed">No S3 clusters</Text>
        ) : (
          <Table.ScrollContainer minWidth={1000}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Cluster</Table.Th>
                  <Table.Th>State</Table.Th>
                  <Table.Th>Nodes</Table.Th>
                  <Table.Th>Endpoints</Table.Th>
                  <Table.Th>Connectivity</Table.Th>
                  <Table.Th>Sync</Table.Th>
                  <Table.Th>Rotation</Table.Th>
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
          <Tooltip label="secrets rotation ticket is live: worker re-issues the profile via rolling recreation">
            <Badge color="blue" variant="light">rotating…</Badge>
          </Tooltip>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}
