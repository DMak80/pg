// Вкладка «Переезды»: только не-ACTIVE бакеты — фаза, штампы, lastError (t08 spec §4.11).
import { Table, Text, Tooltip } from '@mantine/core';
import type { BucketDto } from '../../api/dto';
import { BucketStateBadge } from '../../components/BucketStateBadge';
import { formatAge, formatUnix, formatUnixAge } from '../../utils/format';

// Усечение длинной ошибки: до limit символов + «…»; полный текст — в Tooltip (t08 spec §4.11).
function truncateText(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

export function MovesTab({ buckets }: { buckets: BucketDto[] }) {
  const moves = buckets.filter((b) => b.state !== 'ACTIVE');
  if (moves.length === 0) return <Text c="dimmed">Активных переездов нет</Text>;
  return (
    <Table.ScrollContainer minWidth={900}>
      <Table highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Id</Table.Th>
            <Table.Th>Состояние</Table.Th>
            <Table.Th>Маршрут</Table.Th>
            <Table.Th>Фаза</Table.Th>
            <Table.Th>Начат</Table.Th>
            <Table.Th>Обновлён</Table.Th>
            <Table.Th>Возраст</Table.Th>
            <Table.Th>Ошибка</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {moves.map((b) => (
            <Table.Tr key={b.id}>
              <Table.Td>{b.id}</Table.Td>
              <Table.Td><BucketStateBadge state={b.state} /></Table.Td>
              <Table.Td>{b.move === null ? '—' : `${b.move.owner ?? '—'} → ${b.move.target ?? '—'}`}</Table.Td>
              <Table.Td>{b.move?.phase ?? '—'}</Table.Td>
              <Table.Td>
                <Tooltip label={formatUnix(b.move?.startedUnix ?? null)}>
                  <span>{formatUnix(b.move?.startedUnix ?? null)}</span>
                </Tooltip>
              </Table.Td>
              <Table.Td>
                <Tooltip label={formatUnix(b.move?.updatedUnix ?? null)}>
                  <span>{formatUnixAge(b.move?.updatedUnix ?? null)}</span>
                </Tooltip>
              </Table.Td>
              <Table.Td>{b.ageSec === null ? '—' : formatAge(b.ageSec * 1000)}</Table.Td>
              <Table.Td>
                {b.move?.lastError == null ? '—' : (
                  <Tooltip label={b.move.lastError}>
                    <Text size="sm" c="red">{truncateText(b.move.lastError, 20)}</Text>
                  </Tooltip>
                )}
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}
