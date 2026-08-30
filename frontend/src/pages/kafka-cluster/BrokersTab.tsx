// Вкладка Брокеры деталей kafka-кластера (arch/03 §7.3): name/state/role/
// resources/live + «Убрать брокера» (guard-дизейблы) и «Добавить брокера».
import { useState } from 'react';
import { Badge, Button, Card, Group, Table, Text, Title, Tooltip } from '@mantine/core';
import type { KafkaBrokerDto } from '../../api/dto';
import { AddBrokerModal } from './AddBrokerModal';
import { RemoveBrokerButton } from './RemoveBrokerButton';

export function BrokersTab({
  cluster,
  brokers,
  canScale,
}: {
  cluster: string;
  brokers: KafkaBrokerDto[];
  canScale: boolean;
}) {
  const [addOpened, setAddOpened] = useState(false);
  const lastBroker = brokers.length <= 1;

  return (
    <Card withBorder padding="md" radius="md">
      <Group justify="space-between" mb="sm">
        <Title order={4}>Брокеры</Title>
        {canScale ? <Button size="xs" onClick={() => setAddOpened(true)}>Добавить брокера</Button> : null}
      </Group>
      <AddBrokerModal cluster={cluster} opened={addOpened} onClose={() => setAddOpened(false)} />
      {brokers.length === 0 ? (
        <Text c="dimmed">Брокеры не заявлены</Text>
      ) : (
        <Table.ScrollContainer minWidth={800}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Брокер</Table.Th>
                <Table.Th>Состояние</Table.Th>
                <Table.Th>Роль</Table.Th>
                <Table.Th>CPU</Table.Th>
                <Table.Th>Память</Table.Th>
                <Table.Th>Диск</Table.Th>
                <Table.Th>Live</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {brokers.map((b) => (
                <BrokerRow key={b.name} cluster={cluster} broker={b} canScale={canScale}
                  lastBroker={lastBroker} />
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
}: {
  cluster: string;
  broker: KafkaBrokerDto;
  canScale: boolean;
  lastBroker: boolean;
}) {
  const isController = broker.role === 'controller';
  const reason = isController
    ? 'controller-нода: роль фиксируется при создании навсегда, демонтаж запрещён'
    : lastBroker
      ? 'нельзя снять последний брокер кластера'
      : broker.state === 'TO_REMOVE' || broker.state === 'REMOVING'
        ? 'демонтаж уже заявлен'
        : null;

  return (
    <Table.Tr>
      <Table.Td>{broker.name}</Table.Td>
      <Table.Td><BrokerStateBadge state={broker.state} /></Table.Td>
      <Table.Td>
        {isController ? (
          <Tooltip label="участник KRaft-кворума (combined broker,controller)">
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
        {!canScale ? (
          // Не-Active кластер: мутации недоступны (симметрия с pg-шапкой деталей).
          <Button size="compact-xs" variant="light" color="red" disabled>Убрать</Button>
        ) : reason !== null ? (
          <Tooltip label={reason}>
            <Button size="compact-xs" variant="light" color="red" disabled>Убрать</Button>
          </Tooltip>
        ) : (
          <RemoveBrokerButton cluster={cluster} broker={broker.name} />
        )}
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
