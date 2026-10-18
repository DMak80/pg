// Форма «Перенести бакеты» (arch/03 §3.3): источник/приёмник/чекбоксы бакетов;
// заявки ставит POST /api/clusters/{c}/moves (02 §9.7) — переезды выполняет
// PgWorker последовательно, порядок — по возрастанию id.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Badge, Button, Checkbox, Group, Modal, ScrollArea, Select, Stack, Text } from '@mantine/core';
import { useMemo, useState } from 'react';
import { ApiError } from '../../api/client';
import { moveBuckets, queryKeys } from '../../api/queries';
import type { BucketDto, MoveTicketDto, MovesQueuedDto, ShardDto } from '../../api/dto';

interface Props {
  cluster: string;
  shards: ShardDto[];
  buckets: BucketDto[];
  pendingMoves: MoveTicketDto[];
  opened: boolean;
  onClose: () => void;
}

export function MoveBucketsModal({ cluster, shards, buckets, pendingMoves, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const [from, setFrom] = useState<string | null>(null);
  const [to, setTo] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [result, setResult] = useState<MovesQueuedDto | null>(null);

  // Кандидаты источника: все шарды (TO_REMOVE допустим — эвакуация, Д9).
  const bucketCounts = useMemo(
    () => Object.fromEntries(shards.map((s) => [s.name, buckets.filter((b) => b.owner === s.name).length])),
    [shards, buckets],
  );
  const fromData = shards.map((s) => ({
    value: s.name,
    label: `${s.name} (${bucketCounts[s.name] ?? 0} buckets)${s.state === 'TO_REMOVE' ? ' · to remove' : ''}`,
  }));
  // Приёмники: кроме источника и не TO_REMOVE (Д9).
  const toData = shards
    .filter((s) => s.name !== from && s.state !== 'TO_REMOVE')
    .map((s) => ({ value: s.name, label: s.name }));

  const sourceBuckets = useMemo(
    () => buckets.filter((b) => b.owner === from).sort((a, b) => a.id - b.id),
    [buckets, from],
  );
  // Бакеты с уже стоящей заявкой — disabled с бейджем (arch/03 §3.3):
  // op=move → «в очереди»; иные op (finalize/rollback/abort) — бейдж с самим op.
  const claimed = useMemo(() => {
    const map = new Map<number, MoveTicketDto>();
    for (const t of pendingMoves) {
      if (t.bucketId !== null) map.set(t.bucketId, t);
    }
    return map;
  }, [pendingMoves]);

  const mutation = useMutation({
    mutationFn: (body: { from: string; to: string; buckets: number[] }) => moveBuckets(cluster, body),
    onSuccess: async (data) => {
      setResult(data);
      setSelected(new Set());
      // Список и детали (очередь заявок) обновит следующий тик refresher'а.
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  function toggle(id: number) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  function submit() {
    if (from === null || to === null || selected.size === 0) return;
    mutation.mutate({ from, to, buckets: [...selected].sort((a, b) => a - b) });
  }

  // Успех: сводка результата вместо тоста (решение при планировании — РП-4).
  if (result !== null) {
    return (
      <Modal opened={opened} onClose={() => { setResult(null); onClose(); }} title="Move buckets" centered>
        <Stack gap="sm">
          <Alert color="teal" variant="light">
            Queued: {result.queued.length}
            {result.skipped.length > 0 ? ` (already queued: ${result.skipped.length})` : ''}.
            Moves will be started by PgWorker — see the "Moves" tab.
          </Alert>
          <Group justify="flex-end">
            <Button onClick={() => { setResult(null); onClose(); }}>Done</Button>
          </Group>
        </Stack>
      </Modal>
    );
  }

  // Ошибка сервера: 409 «конфликт/guard» (yellow) / 400 по полям (red) /
  // 503 «etcd недоступен» (red) — ProblemDetails (образец AddShardModal).
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title="Move buckets" centered size="lg">
      <Stack gap="sm">
        <Group grow gap="sm">
          <Select label="Source shard" data={fromData} value={from}
            onChange={(v) => { setFrom(v); setTo(null); setSelected(new Set()); }}
            nothingFoundMessage="No shards" />
          <Select label="Target shard" data={toData} value={to} onChange={setTo}
            nothingFoundMessage="Choose another source" />
        </Group>
        {from !== null ? (
          sourceBuckets.length === 0 ? (
            <Text size="sm" c="dimmed">The source has no buckets</Text>
          ) : (
            <ScrollArea.Autosize mah={260}>
              <Stack gap={4}>
                {sourceBuckets.map((b) => {
                  const ticket = claimed.get(b.id);
                  const active = b.state === 'ACTIVE';
                  return (
                    <Checkbox key={b.id}
                      label={<Group gap={6}><span>{`bucket_${b.id}`}</span>
                        {ticket !== undefined ? (
                          ticket.op === 'move' ? (
                            <Badge color="grape" variant="light">queued</Badge>
                          ) : (
                            <Badge color="orange" variant="light">{ticket.op}</Badge>
                          )
                        ) : null}
                        {!active ? <Badge color="yellow" variant="light">{b.state}</Badge> : null}
                      </Group>}
                      checked={selected.has(b.id)}
                      disabled={!active || ticket !== undefined}
                      onChange={() => toggle(b.id)} />
                  );
                })}
              </Stack>
            </ScrollArea.Autosize>
          )
        ) : null}
        {from !== null && sourceBuckets.length > 0 ? (
          <Group gap="xs">
            <Button size="xs" variant="subtle"
              onClick={() => setSelected(new Set(sourceBuckets
                .filter((b) => b.state === 'ACTIVE' && !claimed.has(b.id)).map((b) => b.id)))}>
              select all
            </Button>
            <Button size="xs" variant="subtle" onClick={() => setSelected(new Set())}>clear</Button>
          </Group>
        ) : null}
        <Text size="sm" c="dimmed">
          Moves run sequentially, one bucket at a time (handled by PgWorker);
          order is by ascending id.
        </Text>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Move rejected')
              : serverError.status === 400
                ? (serverError.detail ?? 'Check the parameters')
                : 'etcd is unavailable — retry later'}
          </Alert>
        ) : null}
        <Group justify="flex-end" mt="xs">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button loading={mutation.isPending} disabled={from === null || to === null || selected.size === 0}
            onClick={submit}>
            Queue
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
