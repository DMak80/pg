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
        labels.push('живая подписка');
      if (s.state === 'TO_REMOVE') labels.push('к удалению');
      return { value: s.name, label: labels.length > 0 ? `${s.name} (${labels.join(', ')})` : s.name };
    });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened={opened} onClose={onClose} title={`Финализировать bucket_${bucketId}`} centered>
      <Stack gap="sm">
        <Select
          label="Убрать артефакты на шарде"
          placeholder="Выберите шард"
          data={shardData}
          value={oldShard}
          onChange={setOldShard}
          nothingFoundMessage="Нет других шардов"
        />
        <Alert color="red" variant="light" title="Необратимо">
          На выбранном шарде будет DROP SCHEMA <b>{`bucket_${bucketId}`}</b> СО ДАННЫМИ
          (необратимо); подписки/публикации/слоты срезаются; владелец <b>{owner}</b> не трогается.
        </Alert>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Финализация отклонена')
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button color="red" variant="light" disabled={oldShard === null}
            loading={mutation.isPending}
            onClick={() => oldShard !== null && mutation.mutate(oldShard)}>
            Убрать артефакты
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
