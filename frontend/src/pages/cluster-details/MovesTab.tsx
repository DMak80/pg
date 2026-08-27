// Вкладка «Переезды»: только не-ACTIVE бакеты — фаза, штампы, lastError (t08 spec §4.11)
// + очередь заявок /pgworker/moves/ (arch/02 §2.3.1): что стоит, куда, кем, возраст.
import { Badge, Group, Table, Text, Tooltip } from '@mantine/core';
import type { BucketDto, MoveTicketDto } from '../../api/dto';
import { BucketStateBadge } from '../../components/BucketStateBadge';
import { formatAge, formatUnix, formatUnixAge } from '../../utils/format';

// Усечение длинной ошибки: до limit символов + «…»; полный текст — в Tooltip (t08 spec §4.11).
function truncateText(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

export function MovesTab({ buckets, pendingMoves }: {
  buckets: BucketDto[]; pendingMoves: MoveTicketDto[];
}) {
  // Только реальные переезды: NOT_INITIALIZED — начальное состояние, не перемещение (spec t12 §3.8).
  const moves = buckets.filter((b) => b.state === 'SYNCING' || b.state === 'FROZEN' || b.state === 'ABORTING');
  return (
    <>
      {moves.length === 0 ? (
        <Text c="dimmed">Активных переездов нет</Text>
      ) : (
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
      )}
      <Group justify="space-between" mt="md">
        <Text fw={500}>Очередь заявок</Text>
      </Group>
      {pendingMoves.length === 0 ? (
        <Text c="dimmed">Очередь заявок пуста</Text>
      ) : (
        <>
          <Table.ScrollContainer minWidth={700}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Бакет</Table.Th>
                  <Table.Th>Операция</Table.Th>
                  <Table.Th>Куда</Table.Th>
                  <Table.Th>Возраст заявки</Table.Th>
                  <Table.Th>Кем</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {pendingMoves.map((t) => (
                  <Table.Tr key={t.bucket}>
                    <Table.Td>{t.bucketId === null ? t.bucket : `bucket_${t.bucketId}`}</Table.Td>
                    <Table.Td>
                      <Badge color={t.op === 'move' ? 'blue' : 'grape'} variant="light">{t.op}</Badge>
                    </Table.Td>
                    <Table.Td>{t.to ?? '—'}</Table.Td>
                    <Table.Td>{formatUnixAge(t.requestedUnix)}</Table.Td>
                    <Table.Td>{t.requestedBy ?? '—'}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
          <Text size="sm" c="dimmed">
            Переезды выполняются по одному бакету за раз — старейшая заявка берётся первой.
          </Text>
        </>
      )}
    </>
  );
}
