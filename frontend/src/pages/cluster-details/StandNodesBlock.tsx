// Блок «Стендовая топология»: реестр /cluster/nodes/ из снапшота, скрыт при пустом (t08 spec §4.13).
import { Card, Table, Text } from '@mantine/core';
import type { StandNodeDto } from '../../api/dto';

export function StandNodesBlock({ standNodes }: { standNodes: StandNodeDto[] }) {
  if (standNodes.length === 0) return null;
  return (
    <Card withBorder padding="md" radius="md">
      <Text fw={600} mb="xs">Стендовая топология</Text>
      <Table w="50%" highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Нода</Table.Th>
            <Table.Th>Адрес</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {standNodes.map((n) => (
            <Table.Tr key={n.name}>
              <Table.Td>{n.name}</Table.Td>
              <Table.Td>{n.address ?? 'есть ключ, адрес пуст'}</Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Card>
  );
}
