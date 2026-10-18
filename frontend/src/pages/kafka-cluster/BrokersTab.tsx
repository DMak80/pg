// Вкладка Брокеры деталей kafka-кластера (arch/03 §7.3): name/state/role/
// resources/live + «Убрать брокера» (guard-дизейблы), «Добавить брокера» и
// «Ресурсы» (мутация №15, t06) + подписи drain/регенерации.
import { useState } from 'react';
import { Badge, Button, Card, Group, Table, Text, Title, Tooltip } from '@mantine/core';
import type { KafkaBrokerDto, KafkaReassignmentDto, KafkaRegenDto } from '../../api/dto';
import { AddBrokerModal } from './AddBrokerModal';
import { EditBrokerResourcesModal } from './EditBrokerResourcesModal';
import { RemoveBrokerButton } from './RemoveBrokerButton';

export function BrokersTab({
  cluster,
  brokers,
  canScale,
  reassignment,
  regen,
}: {
  cluster: string;
  brokers: KafkaBrokerDto[];
  canScale: boolean;
  reassignment: KafkaReassignmentDto | null;
  regen: KafkaRegenDto | null;
}) {
  const [addOpened, setAddOpened] = useState(false);
  const [resourcesBroker, setResourcesBroker] = useState<KafkaBrokerDto | null>(null);
  const lastBroker = brokers.length <= 1;

  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="sm">
        <Title order={4}>Brokers</Title>
        {canScale ? <Button size="xs" onClick={() => setAddOpened(true)}>Add broker</Button> : null}
      </Group>
      <AddBrokerModal cluster={cluster} opened={addOpened} onClose={() => setAddOpened(false)} />
      {resourcesBroker !== null ? (
        <EditBrokerResourcesModal
          cluster={cluster}
          broker={resourcesBroker}
          opened
          onClose={() => setResourcesBroker(null)}
        />
      ) : null}
      {brokers.length === 0 ? (
        <Text c="dimmed">No brokers declared</Text>
      ) : (
        <Table.ScrollContainer minWidth={800}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Broker</Table.Th>
                <Table.Th>State</Table.Th>
                <Table.Th>Role</Table.Th>
                <Table.Th>CPU</Table.Th>
                <Table.Th>Memory</Table.Th>
                <Table.Th>Disk</Table.Th>
                <Table.Th>Live</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {brokers.map((b) => (
                <BrokerRow key={b.name} cluster={cluster} broker={b} canScale={canScale}
                  lastBroker={lastBroker} reassignment={reassignment} regen={regen}
                  onEditResources={setResourcesBroker} />
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}

function BrokerRow({
  cluster,
  broker,
  canScale,
  lastBroker,
  reassignment,
  regen,
  onEditResources,
}: {
  cluster: string;
  broker: KafkaBrokerDto;
  canScale: boolean;
  lastBroker: boolean;
  reassignment: KafkaReassignmentDto | null;
  regen: KafkaRegenDto | null;
  onEditResources: (broker: KafkaBrokerDto) => void;
}) {
  const isController = broker.role === 'controller';
  // Подпись drain: живой reassignment в режиме drain указывает на этот брокер.
  const draining = broker.state === 'TO_REMOVE'
    && reassignment !== null
    && reassignment.mode === 'drain'
    && reassignment.drainBroker === broker.name;
  const reason = isController
    ? 'controller node: the role is fixed at creation forever, teardown is forbidden'
    : lastBroker
      ? 'cannot remove the last broker of the cluster'
      : broker.state === 'TO_REMOVE' || broker.state === 'REMOVING'
        ? 'teardown already requested'
        : null;
  // Ресурсы не менять у демонтажа/нестабильных нод (t06, 02 §10.2-15).
  const resourcesBlocked = !canScale
    || broker.state === 'TO_REMOVE'
    || broker.state === 'REMOVING'
    || broker.state === 'NOT_INITIALIZED';
  const resourcesReason = !canScale
    ? 'cluster is not Active — mutations unavailable'
    : broker.state === 'TO_REMOVE' || broker.state === 'REMOVING'
      ? 'teardown already requested'
      : broker.state === 'NOT_INITIALIZED'
        ? 'broker is not initialized yet'
        : null;

  return (
    <Table.Tr>
      <Table.Td>
        {broker.name}
        {draining ? (
          <Text size="xs" c="violet" display="block">
            drain: {reassignment!.partitionsRemaining} partitions remaining
          </Text>
        ) : null}
        {regen !== null && regen.currentBroker === broker.name ? (
          <Text size="xs" c="indigo" display="block">
            regeneration ({regen!.brokersRemaining} remaining)
          </Text>
        ) : null}
      </Table.Td>
      <Table.Td><BrokerStateBadge state={broker.state} /></Table.Td>
      <Table.Td>
        {isController ? (
          <Tooltip label="KRaft quorum member (combined broker,controller)">
            <Badge color="grape" variant="light">controller</Badge>
          </Tooltip>
        ) : broker.role === 'broker' ? (
          <Badge variant="light">broker</Badge>
        ) : (
          <Text c="dimmed">—</Text>
        )}
      </Table.Td>
      <Table.Td>{broker.cpu !== null ? String(broker.cpu) : '—'}</Table.Td>
      <Table.Td>{broker.memGi !== null ? `${broker.memGi} GiB` : '—'}</Table.Td>
      <Table.Td>{broker.diskGi !== null ? `${broker.diskGi} GiB` : '—'}</Table.Td>
      <Table.Td>
        {broker.live === null ? (
          <Text c="dimmed">—</Text>
        ) : broker.live ? (
          <Badge color="green" variant="light" size="sm">
            live{broker.brokerId !== null ? ` · id ${broker.brokerId}` : ''}
          </Badge>
        ) : (
          <Badge color="red" variant="light" size="sm">offline</Badge>
        )}
      </Table.Td>
      <Table.Td>
        <Group gap="xs" wrap="nowrap" justify="flex-end">
          {resourcesBlocked ? (
            <Tooltip label={resourcesReason}>
              <Button size="compact-xs" variant="light" disabled>Resources</Button>
            </Tooltip>
          ) : (
            <Button size="compact-xs" variant="light" onClick={() => onEditResources(broker)}>
              Resources
            </Button>
          )}
          {!canScale ? (
            // Не-Active кластер: мутации недоступны (симметрия с pg-шапкой деталей).
            <Button size="compact-xs" variant="light" color="red" disabled>Remove</Button>
          ) : reason !== null ? (
            <Tooltip label={reason}>
              <Button size="compact-xs" variant="light" color="red" disabled>Remove</Button>
            </Tooltip>
          ) : (
            <RemoveBrokerButton cluster={cluster} broker={broker.name} />
          )}
        </Group>
      </Table.Td>
    </Table.Tr>
  );
}

function BrokerStateBadge({ state }: { state: string | null }) {
  const color = state === 'RUNNING'
    ? 'green'
    : state === 'PROVISIONING'
      ? 'blue'
      : state === 'UNREACHABLE'
        ? 'red'
        : state === 'TO_REMOVE' || state === 'REMOVING'
          ? 'orange'
          : 'gray';
  return <Badge color={color} variant="light">{state ?? '—'}</Badge>;
}
