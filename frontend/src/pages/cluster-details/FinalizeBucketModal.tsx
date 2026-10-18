// Форма «Финализировать бакет» (t07, arch/03 §3.5): выбор шарда ≠ владельца,
// где убрать артефакты (DROP SCHEMA СО ДАННЫМИ — необратимо); подсказки по
// живым подпискам SQL-пробы; TO_REMOVE допустим (финализация перед демонтажем).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Select, Stack } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { finalizeBucket, queryKeys } from '../../api/queries';
import type { ShardDto } from '../../api/dto';

interface Props {
  cluster: string;
  bucketId: number;
  owner: string;
  shards: ShardDto[];
  opened: boolean;
  onClose: () => void;
}

export function FinalizeBucketModal({ cluster, bucketId, owner, shards, opened, onClose }: Props) {
  const queryClient = useQueryClient();
  const [oldShard, setOldShard] = useState<string | null>(null);
  const mutation = useMutation({
    mutationFn: (shard: string) => finalizeBucket(cluster, { bucket: bucketId, oldShard: shard }),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  // Кандидаты: шарды ≠ текущего владельца; метки — живая подписка / к удалению.
  const sub = `sub_bucket_${bucketId}`;
  const subRb = `sub_bucket_${bucketId}_rb`;
  const shardData = shards
    .filter((s) => s.name !== owner)
    .map((s) => {
      const labels: string[] = [];
      if ((s.runtime?.subscriptions ?? []).some((n) => n === sub || n === subRb))
        labels.push('live subscription');
      if (s.state === 'TO_REMOVE') labels.push('to remove');
      return { value: s.name, label: labels.length > 0 ? `${s.name} (${labels.join(', ')})` : s.name };
    });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title={`Finalize bucket_${bucketId}`} centered>
      <Stack gap="sm">
        <Select
          label="Clean up artifacts on shard"
          placeholder="Select a shard"
          data={shardData}
          value={oldShard}
          onChange={setOldShard}
          nothingFoundMessage="No other shards"
        />
        <Alert color="red" variant="light" title="Irreversible">
          The selected shard will get DROP SCHEMA <b>{`bucket_${bucketId}`}</b> INCLUDING DATA
          (irreversible); subscriptions/publications/slots are dropped; the owner <b>{owner}</b> is untouched.
        </Alert>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Finalization rejected')
              : serverError.status === 400
                ? (serverError.detail ?? 'Check the parameters')
                : 'etcd is unavailable — retry later'}
          </Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button color="red" variant="light" disabled={oldShard === null}
            loading={mutation.isPending}
            onClick={() => oldShard !== null && mutation.mutate(oldShard)}>
            Clean up artifacts
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
