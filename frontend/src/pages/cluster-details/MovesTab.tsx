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
        <Text c="dimmed">No active moves</Text>
      ) : (
        <Table.ScrollContainer minWidth={900}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Id</Table.Th>
                <Table.Th>State</Table.Th>
                <Table.Th>Route</Table.Th>
                <Table.Th>Phase</Table.Th>
                <Table.Th>Started</Table.Th>
                <Table.Th>Updated</Table.Th>
                <Table.Th>Age</Table.Th>
                <Table.Th>Error</Table.Th>
                {canScale ? <Table.Th>Actions</Table.Th> : null}
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
                        Abort move
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
        <Text fw={500}>Ticket queue</Text>
      </Group>
      {pendingMoves.length === 0 ? (
        <Text c="dimmed">Ticket queue is empty</Text>
      ) : (
        <>
          <Table.ScrollContainer minWidth={700}>
            <Table highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Bucket</Table.Th>
                  <Table.Th>Operation</Table.Th>
                  <Table.Th>To</Table.Th>
                  <Table.Th>Ticket age</Table.Th>
                  <Table.Th>By</Table.Th>
                  {canScale ? <Table.Th>Withdraw</Table.Th> : null}
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
                          Withdraw
                        </Button>
                      </Table.Td>
                    ) : null}
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
          <Text size="sm" c="dimmed">
            Moves run one bucket at a time — the oldest ticket is taken first.
          </Text>
        </>
      )}
      <Modal opened={confirmTicket !== null} onClose={() => setConfirmTicket(null)}
        title="Withdraw ticket" centered>
        <Stack gap="sm">
          <Text>
            Ticket <b>{`${confirmTicket?.op ?? ''} ${confirmTicket?.bucket ?? ''}`}</b> will
            be removed from the queue. If the move has already started, it will run to
            completion; a started move can only be stopped via "Abort move" (abort).
          </Text>
          {cancelError !== null ? (
            <Text size="sm" c="red">{cancelError.detail ?? cancelError.message}</Text>
          ) : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setConfirmTicket(null)}>Cancel</Button>
            <Button color="red" loading={cancel.isPending}
              onClick={() => confirmTicket !== null && cancel.mutate(confirmTicket.bucket)}>
              Withdraw ticket
            </Button>
          </Group>
        </Stack>
      </Modal>
      {work != null ? (
        <>
          <Group justify="space-between" mt="md">
            <Text fw={500}>Worker journal</Text>
          </Group>
          <Group gap="sm">
            <Badge color="blue" variant="light">{work.op}</Badge>
            <Text size="sm">{work.phase}</Text>
            <Text size="sm" c="dimmed">updated {formatUnixAge(work.updatedUnix)}</Text>
            {work.lastError !== null ? (
              <Tooltip label={work.lastError}>
                <Text size="sm" c="red">{truncateText(work.lastError, 40)}</Text>
              </Tooltip>
            ) : null}
          </Group>
          <Text size="sm" c="dimmed">
            Last process of the cluster worker; rejected tickets include a reason.
          </Text>
        </>
      ) : null}
    </>
  );
}
