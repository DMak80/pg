// Форма «Откатить бакет» (t07, arch/03 §3.4): направление определяет воркер
// по живой обратной подписке sub_bucket_<i>_rb; подсказка — best-effort по
// SQL-пробе (shards[].runtime.subscriptions); заявка — POST .../moves/rollback.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { ApiError } from '../../api/client';
import { queryKeys, rollbackBuckets } from '../../api/queries';
import type { ShardDto } from '../../api/dto';

interface Props {
  cluster: string;
  bucketId: number;
  shards: ShardDto[];
  opened: boolean;
  onClose: () => void;
}

export function RollbackBucketModal({ cluster, bucketId, shards, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: () => rollbackBuckets(cluster, { buckets: [bucketId] }),
    onSuccess: async () => {
      // Заявка появится в очереди вкладки «Переезды» со следующего тика.
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  // Подсказка направления: шард с живой обратной подпиской бакета (SQL-проба).
  const rbSub = `sub_bucket_${bucketId}_rb`;
  const hintShard = shards.find((s) => (s.runtime?.subscriptions ?? []).includes(rbSub));

  // Ошибка сервера: 409 guard'ы (yellow) / 400/503 (red) — ProblemDetails.
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title={`Roll back bucket_${bucketId}`} centered>
      <Stack gap="sm">
        <Text>
          Roll back <b>{`bucket_${bucketId}`}</b> to its previous shard — the direction
          is determined by the worker from the live reverse subscription <b>{rbSub}</b>.
        </Text>
        <Text size="sm" c="dimmed">
          {hintShard !== undefined
            ? `It will return to ${hintShard.name} (the live subscription is visible to the SQL probe).`
            : 'The target will be determined by the worker from the reverse subscription (the probe is off or does not see it).'}
        </Text>
        <Alert color="yellow" variant="light" title="Warning">
          Rollback is a mirrored cutover with a seconds-long write freeze. If the
          reverse subscription is absent — the worker will reject the ticket (rollback
          is only a full re-copy).
        </Alert>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Rollback rejected')
              : serverError.status === 400
                ? (serverError.detail ?? 'Check the parameters')
                : 'etcd is unavailable — retry later'}
          </Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button variant="light" loading={mutation.isPending} onClick={() => mutation.mutate()}>
            Queue
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
