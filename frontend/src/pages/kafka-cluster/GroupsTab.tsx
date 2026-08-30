// Вкладка Группы деталей kafka-кластера (arch/03 §7.3, план C4): live-данные
// пробы (group/state/members/totalLag, сортировка по лагу); fallback при
// выключенной/недоступной пробе.
import { Alert, Badge, Card, Table, Text, Title } from '@mantine/core';
import type { KafkaGroupDto } from '../../api/dto';

export function GroupsTab({
  groups,
  probeOk,
}: {
  groups: KafkaGroupDto[] | null;
  probeOk: boolean | null;
}) {
  return (
    <Card withBorder padding="md" radius="md">
      <Title order={4} mb="sm">Группы</Title>
      {groups === null ? (
        <Alert color={probeOk === false ? 'orange' : 'gray'} variant="light">
          {probeOk === false
            ? 'Live-проба недоступна — группы консьюмеров не видны (etcd-данные кластера актуальны).'
            : 'Проба ещё не собрала данные о группах — подождите тика (15 c) или включите AdminPanel:Probes:Kafka.'}
        </Alert>
      ) : groups.length === 0 ? (
        <Text c="dimmed">Консьюмер-групп нет</Text>
      ) : (
        <Table.ScrollContainer minWidth={600}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Группа</Table.Th>
                <Table.Th>Состояние</Table.Th>
                <Table.Th>Участников</Table.Th>
                <Table.Th>Total lag</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {/* Группы приходят отсортированными по лагу (проба, план C3). */}
              {groups.map((g) => (
                <Table.Tr key={g.group}>
                  <Table.Td>{g.group}</Table.Td>
                  <Table.Td><GroupStateBadge state={g.state} /></Table.Td>
                  <Table.Td>{String(g.members)}</Table.Td>
                  <Table.Td>
                    {g.totalLag > 0 ? (
                      <Text fw={g.totalLag > 100_000 ? 700 : 400}>
                        {g.totalLag.toLocaleString('ru-RU')}
                      </Text>
                    ) : (
                      <Text c="dimmed">0</Text>
                    )}
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

function GroupStateBadge({ state }: { state: string | null }) {
  const color = state === 'Stable'
    ? 'green'
    : state === 'Empty'
      ? 'gray'
      : state === 'Dead'
        ? 'red'
        : state === 'PreparingRebalance' || state === 'CompletingRebalance'
          ? 'yellow'
          : 'gray';
  return <Badge color={color} variant="light">{state ?? '—'}</Badge>;
}
