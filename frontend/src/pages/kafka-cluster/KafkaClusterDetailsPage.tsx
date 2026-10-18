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
import { RebalanceButton } from './RebalanceButton';
import { RotatePasswordButton } from './RotatePasswordButton';
import { RotateAdminPasswordButton } from './RotateAdminPasswordButton';
import { RotateCaButton } from './RotateCaButton';
import { TopicsTab } from './TopicsTab';
import type { TicketOutcomeDto } from '../../api/dto';

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
            <Tooltip label="the worker tears down containers/volumes and etcd keys">
              <Badge color="red" variant="light">to remove</Badge>
            </Tooltip>
          ) : c.state === 'NOT_INITIALIZED' ? (
            <Badge color="gray" variant="light">not initialized</Badge>
          ) : null}
          {c.rotation !== null ? (
            <Tooltip label="rotation ticket is live: rolling broker restart (phases A/B/C)">
              <Badge color="blue" variant="light">app-password rotation</Badge>
            </Tooltip>
          ) : null}
          {c.caRotation !== null ? (
            <Tooltip label="CA rotation ticket is live: dual-trust window (P/D/R/C), brokers restart one by one">
              <Badge color="grape" variant="light">CA rotation</Badge>
            </Tooltip>
          ) : null}
          {c.reassignment !== null ? (
            <Tooltip label="reassignment is executed by the worker: replica moves in batches">
              <Badge color="violet" variant="light">
                {c.reassignment.mode === 'drain'
                  ? `drain ${c.reassignment.drainBroker ?? ''}`.trim()
                  : 'rebalance'}
                : {c.reassignment.partitionsRemaining}/{c.reassignment.partitionsTotal} partitions remaining
              </Badge>
            </Tooltip>
          ) : null}
          {c.regen !== null ? (
            <Tooltip label="rolling broker recreation is executed by the worker (volumes are preserved)">
              <Badge color="indigo" variant="light">
                regeneration {c.regen.brokersTotal - c.regen.brokersRemaining}/{c.regen.brokersTotal}
              </Badge>
            </Tooltip>
          ) : null}
        </Group>
        {active ? (
          <Group gap="sm">
            <EditClusterConfigModal cluster={c} />
            <RotatePasswordButton cluster={c.name} disabled={c.rotation !== null} />
            <RotateAdminPasswordButton cluster={c.name} disabled={!active} />
            <RotateCaButton cluster={c.name} disabled={!active} />
            <RebalanceButton cluster={c.name} rebalance={c.rebalance} disabled={!active} />
            <DeleteKafkaClusterButton cluster={c.name} />
          </Group>
        ) : null}
      </Group>

      {c.probeOk === false ? (
        <Alert color="orange" variant="light" title="Live probe unavailable">
          {c.probeError ?? 'DescribeCluster is not responding'} — etcd data is up to date, the live part is hidden.
        </Alert>
      ) : null}

      <Card withBorder padding="md" radius="md">
        <SimpleGrid cols={{ base: 2, md: 4 }}>
          <Field label="Brokers">{String(c.brokers)}</Field>
          <Field label="RF">{String(c.replicationFactor)}</Field>
          <Field label="Min ISR">{String(c.minInSyncReplicas)}</Field>
          <Field label="Default partitions">{String(c.defaultPartitions)}</Field>
          <Field label="Retention, days">{String(c.defaultRetentionMs / DAY_MS)}</Field>
          <Field label="Endpoints">
            <Text size="sm" ff="monospace">{c.endpoints ?? '—'}</Text>
          </Field>
          <Field label="Topics">{String(c.topics.length)}</Field>
          <Field label="Live probe">
            {c.probeOk === null ? '—' : c.probeOk ? 'ok' : 'unavailable'}
          </Field>
        </SimpleGrid>
      </Card>

      {c.regen !== null ? (
        <Text size="sm" c="indigo">
          Broker regeneration: {c.regen.brokersRemaining} of{' '}
          {c.regen.brokersTotal} remaining
          {c.regen.currentBroker !== null ? ` (current ${c.regen.currentBroker})` : ''}
        </Text>
      ) : null}

      {/* Последний исход заявки (t10): expired подсвечен warning-цветом. */}
      {c.ticketOutcome !== null ? (
        <TicketOutcomeLine outcome={c.ticketOutcome} />
      ) : null}

      <BrokersTab cluster={c.name} brokers={c.brokersList} canScale={active}
        reassignment={c.reassignment} regen={c.regen} />
      <TopicsTab
        cluster={c.name}
        topics={c.topics}
        canMutate={active}
        defaults={{ defaultPartitions: c.defaultPartitions, replicationFactor: c.replicationFactor, brokers: c.brokers }}
      />
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

// Строка «Последний исход заявки» (t10): kind → outcome (finished); expired
// подсвечен warning-цветом, причина — в тексте.
export function TicketOutcomeLine({ outcome }: { outcome: TicketOutcomeDto }) {
  const expired = outcome.outcome === 'expired';
  return (
    <Text size="sm" c={expired ? 'yellow' : undefined}>
      Last ticket outcome: {outcome.kind} → {outcome.outcome} (
      {new Date(outcome.finishedUnix * 1000).toLocaleString()})
      {outcome.reason ? ` — reason: ${outcome.reason}` : ''}
    </Text>
  );
}
