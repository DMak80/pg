// Детали kafka-кластера: шапка (state-бейджи, конфиг-мутация/ротация/удаление)
// + вкладки Брокеры/Топики/Группы (arch/03 §7.3).
import { useQuery } from '@tanstack/react-query';
import { Alert, Badge, Card, Group, SimpleGrid, Stack, Text, Title, Tooltip } from '@mantine/core';
import { useParams } from 'react-router';
import { fetchKafkaClusterDetails, kafkaQueryKeys } from '../../api/queries';
import { ErrorSection, LoadingSection } from '../../components/LoadState';
import { usePollingIntervalMs } from '../../polling/PollingContext';
import { BrokersTab } from './BrokersTab';
import { DeleteKafkaClusterButton } from './DeleteKafkaClusterButton';
import { EditClusterConfigModal } from './EditClusterConfigModal';
import { GroupsTab } from './GroupsTab';
import { RotatePasswordButton } from './RotatePasswordButton';
import { TopicsTab } from './TopicsTab';

const DAY_MS = 86_400_000;

export function KafkaClusterDetailsPage() {
  const { cluster = '' } = useParams();
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: kafkaQueryKeys.cluster(cluster),
    queryFn: () => fetchKafkaClusterDetails(cluster),
    refetchInterval: intervalMs,
  });

  if (query.data === undefined)
    return query.isError ? (
      <ErrorSection error={query.error} onRetry={() => void query.refetch()} />
    ) : (
      <LoadingSection />
    );

  const c = query.data;
  const active = c.state === 'ACTIVE';
  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Group gap="sm">
          <Title order={2}>Kafka: {c.name}</Title>
          {c.state === 'TO_REMOVE' ? (
            <Tooltip label="воркер демонтирует контейнеры/тома и ключи etcd">
              <Badge color="red" variant="light">к удалению</Badge>
            </Tooltip>
          ) : c.state === 'NOT_INITIALIZED' ? (
            <Badge color="gray" variant="light">не инициализирован</Badge>
          ) : null}
          {c.rotation !== null ? (
            <Tooltip label="заявка ротации жива: rolling-перезапуск брокеров (фазы A/B/C)">
              <Badge color="blue" variant="light">ротация app-пароля</Badge>
            </Tooltip>
          ) : null}
        </Group>
        {active ? (
          <Group gap="sm">
            <EditClusterConfigModal cluster={c} />
            <RotatePasswordButton cluster={c.name} disabled={c.rotation !== null} />
            <DeleteKafkaClusterButton cluster={c.name} />
          </Group>
        ) : null}
      </Group>

      {c.probeOk === false ? (
        <Alert color="orange" variant="light" title="Live-проба недоступна">
          {c.probeError ?? 'DescribeCluster не отвечает'} — данные etcd актуальны, live-часть скрыта.
        </Alert>
      ) : null}

      <Card withBorder padding="md" radius="md">
        <SimpleGrid cols={{ base: 2, md: 4 }}>
          <Field label="Брокеры">{String(c.brokers)}</Field>
          <Field label="RF">{String(c.replicationFactor)}</Field>
          <Field label="Min ISR">{String(c.minInSyncReplicas)}</Field>
          <Field label="Партиций по умолчанию">{String(c.defaultPartitions)}</Field>
          <Field label="Retention, дней">{String(c.defaultRetentionMs / DAY_MS)}</Field>
          <Field label="Endpoints">
            <Text size="sm" ff="monospace">{c.endpoints ?? '—'}</Text>
          </Field>
          <Field label="Топиков">{String(c.topics.length)}</Field>
          <Field label="Live-проба">
            {c.probeOk === null ? '—' : c.probeOk ? 'ok' : 'недоступна'}
          </Field>
        </SimpleGrid>
      </Card>

      <BrokersTab cluster={c.name} brokers={c.brokersList} canScale={active} />
      <TopicsTab cluster={c.name} topics={c.topics} canMutate={active} />
      <GroupsTab groups={c.groups} probeOk={c.probeOk} />
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
