// Вкладка «Heals»: журнал авто-починки, новые сверху (t08 spec §4.12).
import { Table, Text } from '@mantine/core';
import type { HealDto } from '../../api/dto';
import { formatUnix } from '../../utils/format';

export function HealsTab({ heals }: { heals: HealDto[] }) {
  if (heals.length === 0) return <Text c="dimmed">Журнал пуст</Text>;
  return (
    <Table.ScrollContainer minWidth={700}>
      <Table highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Бакет</Table.Th>
            <Table.Th>Было</Table.Th>
            <Table.Th>Стало</Table.Th>
            <Table.Th>Причина</Table.Th>
            <Table.Th>Время</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {heals.map((h) => (
            <Table.Tr key={`${h.bucket}-${h.tsUnix ?? 'null'}`}>
              <Table.Td>{h.bucket}</Table.Td>
              <Table.Td>{h.was ?? '—'}</Table.Td>
              <Table.Td>{h.now ?? '—'}</Table.Td>
              <Table.Td>{h.reason ?? '—'}</Table.Td>
              <Table.Td>{formatUnix(h.tsUnix)}</Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}
