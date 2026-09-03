// Вкладка «Переезды»: только не-ACTIVE бакеты — фаза, штампы, lastError (t08 spec §4.11)
// + очередь заявок /pgworker/moves/ (arch/02 §2.3.1): что стоит, куда, кем, возраст.
// t07: per-row «Отменить переезд» (abort, arch/03 §3.6), «Снять заявку» в очереди
// (§9.7.5) и блок «Журнал воркера» — последний процесс /pgworker/work/<C>.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Badge, Button, Group, Modal, Stack, Table, Text, Tooltip } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { cancelMoveTicket, queryKeys } from '../../api/queries';
import type { BucketDto, ClusterWorkDto, MoveTicketDto } from '../../api/dto';
import { BucketStateBadge } from '../../components/BucketStateBadge';
import { formatAge, formatUnix, formatUnixAge } from '../../utils/format';
import { AbortMoveModal } from './AbortMoveModal';

// Усечение длинной ошибки: до limit символов + «…»; полный текст — в Tooltip (t08 spec §4.11).
function truncateText(text: string, limit: number): string {
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

export function MovesTab({ cluster, canScale, buckets, pendingMoves, work }: {
  cluster: string; canScale: boolean; buckets: BucketDto[];
  pendingMoves: MoveTicketDto[]; work?: ClusterWorkDto | null;
}) {
  const queryClient = useQueryClient();
  // Только реальные переезды: NOT_INITIALIZED — начальное состояние, не перемещение (spec t12 §3.8).
  const moves = buckets.filter((b) => b.state === 'SYNCING' || b.state === 'FROZEN' || b.state === 'ABORTING');
  const [abortId, setAbortId] = useState<number | null>(null);
  const [confirmTicket, setConfirmTicket] = useState<MoveTicketDto | null>(null);
  const abortBucket = buckets.find((b) => b.id === abortId) ?? null;

  // Снятие заявки: подтверждение «начатый доедет»; 404 «заявки нет» — тихо
  // инвалидировать (оператора опередил тик воркера, arch/03 §3.6).
  const cancel = useMutation({
    mutationFn: (bucket: string) => cancelMoveTicket(cluster, bucket),
    onSuccess: async () => {
      setConfirmTicket(null);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
    onError: async (error) => {
      if (error instanceof ApiError && error.status === 404) {
        setConfirmTicket(null);
        await queryClient.invalidateQueries({ queryKey: ['clusters'] });
        await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
        return;
      }
      // прочие (503) — текстом в подтверждении
    },
  });
  const cancelError = cancel.error instanceof ApiError && cancel.error.status !== 404
    ? cancel.error
    : null;

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
                {canScale ? <Table.Th>Действия</Table.Th> : null}
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
                  {canScale ? (
                    <Table.Td>
                      <Button color="red" variant="light" size="xs" onClick={() => setAbortId(b.id)}>
                        Отменить переезд
                      </Button>
                    </Table.Td>
                  ) : null}
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
      {abortBucket !== null ? (
        <AbortMoveModal cluster={cluster} bucket={abortBucket}
          opened={abortId !== null} onClose={() => setAbortId(null)} />
      ) : null}
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
                  {canScale ? <Table.Th>Снять заявку</Table.Th> : null}
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
                    {canScale ? (
                      <Table.Td>
                        <Button color="red" variant="light" size="xs"
                          onClick={() => setConfirmTicket(t)}>
                          Снять
                        </Button>
                      </Table.Td>
                    ) : null}
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
      <Modal opened={confirmTicket !== null} onClose={() => setConfirmTicket(null)}
        title="Снять заявку" centered>
        <Stack gap="sm">
          <Text>
            Заявка <b>{`${confirmTicket?.op ?? ''} ${confirmTicket?.bucket ?? ''}`}</b> будет
            удалена из очереди. Если переезд уже начат — он доедет до конца; остановка
            начатого переезда — только «Отменить переезд» (abort).
          </Text>
          {cancelError !== null ? (
            <Text size="sm" c="red">{cancelError.detail ?? cancelError.message}</Text>
          ) : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setConfirmTicket(null)}>Отмена</Button>
            <Button color="red" loading={cancel.isPending}
              onClick={() => confirmTicket !== null && cancel.mutate(confirmTicket.bucket)}>
              Снять заявку
            </Button>
          </Group>
        </Stack>
      </Modal>
      {work != null ? (
        <>
          <Group justify="space-between" mt="md">
            <Text fw={500}>Журнал воркера</Text>
          </Group>
          <Group gap="sm">
            <Badge color="blue" variant="light">{work.op}</Badge>
            <Text size="sm">{work.phase}</Text>
            <Text size="sm" c="dimmed">обновлён {formatUnixAge(work.updatedUnix)}</Text>
            {work.lastError !== null ? (
              <Tooltip label={work.lastError}>
                <Text size="sm" c="red">{truncateText(work.lastError, 40)}</Text>
              </Tooltip>
            ) : null}
          </Group>
          <Text size="sm" c="dimmed">
            Последний процесс воркера кластера; отвергнутые заявки — с причиной.
          </Text>
        </>
      ) : null}
    </>
  );
}
