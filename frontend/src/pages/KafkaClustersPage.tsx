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
        <Title order={2}>Kafka-кластеры</Title>
        <Button onClick={() => setCreateOpened(true)}>Создать кластер</Button>
      </Group>
      <CreateKafkaClusterModal opened={createOpened} onClose={() => setCreateOpened(false)} />
      <Card withBorder padding="md" radius="md">
        {clusters.length === 0 ? (
          <Text c="dimmed">Kafka-кластеры не найдены</Text>
        ) : (
          <Table.ScrollContainer minWidth={900}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Кластер</Table.Th>
                  <Table.Th>Состояние</Table.Th>
                  <Table.Th>Брокеры</Table.Th>
                  <Table.Th>Топики</Table.Th>
                  <Table.Th>Endpoints</Table.Th>
                  <Table.Th>Пометки</Table.Th>
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
          <Tooltip label="кластер заявлен, брокеры не подняты">
            <Badge color="gray" variant="light">не инициализирован</Badge>
          </Tooltip>
        ) : (
          <Tooltip label="TO_REMOVE: воркер демонтирует контейнеры и ключи">
            <Badge color="red" variant="light">к удалению</Badge>
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
        {cluster.rotationPending ? (
          <Tooltip label="заявка ротации app-пароля жива: исполняет воркер (фазы A/B/C)">
            <Badge color="blue" variant="light">ротация</Badge>
          </Tooltip>
        ) : null}
      </Table.Td>
    </Table.Tr>
  );
}
