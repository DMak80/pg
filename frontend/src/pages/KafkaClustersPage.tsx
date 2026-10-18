// Панель Kafka: сводный список kafka-кластеров снапшота (arch/03 §7.3) + создание.
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Button, Card, Group, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link } from 'react-router';
import { useState } from 'react';
import type { KafkaClusterSummaryDto } from '../api/dto';
import { fetchKafkaClusters, kafkaQueryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { CreateKafkaClusterModal } from './kafka-cluster/CreateKafkaClusterModal';

export function KafkaClustersPage() {
  const intervalMs = usePollingIntervalMs();
  const [createOpened, setCreateOpened] = useState(false);
  const query = useQuery({
    queryKey: kafkaQueryKeys.clusters,
    queryFn: fetchKafkaClusters,
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
        <Title order={2}>Kafka clusters</Title>
        <Button onClick={() => setCreateOpened(true)}>Create cluster</Button>
      </Group>
      <CreateKafkaClusterModal opened={createOpened} onClose={() => setCreateOpened(false)} />
      <Card withBorder padding="md" radius="md">
        {clusters.length === 0 ? (
          <Text c="dimmed">No Kafka clusters found</Text>
        ) : (
          <Table.ScrollContainer minWidth={900}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Cluster</Table.Th>
                  <Table.Th>State</Table.Th>
                  <Table.Th>Brokers</Table.Th>
                  <Table.Th>Topics</Table.Th>
                  <Table.Th>Endpoints</Table.Th>
                  <Table.Th>Flags</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {clusters.map((c) => <KafkaClusterRow key={c.name} cluster={c} />)}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        )}
      </Card>
    </>
  );
}

function KafkaClusterRow({ cluster }: { cluster: KafkaClusterSummaryDto }) {
  const down = cluster.brokersTotal - cluster.brokersRunning;
  return (
    <Table.Tr>
      <Table.Td>
        <Anchor component={Link} to={`/kafka/${cluster.name}`}>{cluster.name}</Anchor>
      </Table.Td>
      <Table.Td>
        {cluster.state === 'ACTIVE' ? (
          <Badge color="green" variant="light">ACTIVE</Badge>
        ) : cluster.state === 'NOT_INITIALIZED' ? (
          <Tooltip label="cluster requested, brokers not up">
            <Badge color="gray" variant="light">not initialized</Badge>
          </Tooltip>
        ) : (
          <Tooltip label="TO_REMOVE: worker tears down containers and keys">
            <Badge color="red" variant="light">to remove</Badge>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>
        <Text c={down > 0 ? 'red' : undefined}>
          {cluster.brokersRunning}/{cluster.brokersTotal}
        </Text>
      </Table.Td>
      <Table.Td>{cluster.topicsCount}</Table.Td>
      <Table.Td>
        <Text size="sm" ff="monospace">{cluster.endpoints ?? '—'}</Text>
      </Table.Td>
      <Table.Td>
        <Group gap="xs">
          {cluster.rotationPending ? (
            <Tooltip label="app-password rotation ticket is live: executed by the worker (phases A/B/C)">
              <Badge color="blue" variant="light">rotation</Badge>
            </Tooltip>
          ) : null}
          {cluster.caRotationPending ? (
            <Tooltip label="CA/cert rotation ticket is live: worker plays the dual-trust window (P/D/R/C)">
              <Badge color="grape" variant="light">CA rotation</Badge>
            </Tooltip>
          ) : null}
        </Group>
      </Table.Td>
    </Table.Tr>
  );
}
