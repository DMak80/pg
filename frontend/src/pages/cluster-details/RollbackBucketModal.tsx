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
    <Modal opened={opened} onClose={onClose} title={`Откатить bucket_${bucketId}`} centered>
      <Stack gap="sm">
        <Text>
          Откатить <b>{`bucket_${bucketId}`}</b> на прежний шард — направление определяет
          воркер по живой обратной подписке <b>{rbSub}</b>.
        </Text>
        <Text size="sm" c="dimmed">
          {hintShard !== undefined
            ? `Вернётся на ${hintShard.name} (живая подписка видна SQL-пробой).`
            : 'Куда — определит воркер по обратной подписке (проба выключена или не видит её).'}
        </Text>
        <Alert color="yellow" variant="light" title="Внимание">
          Откат — зеркальный cutover с секундной заморозкой записи. Если обратной
          подписки нет — воркер отвергнет заявку (откат только полным re-copy).
        </Alert>
        {serverError !== null ? (
          <Alert color={serverError.status === 409 ? 'yellow' : 'red'} variant="light">
            {serverError.status === 409
              ? (serverError.detail ?? 'Откат отклонён')
              : serverError.status === 400
                ? (serverError.detail ?? 'Проверьте параметры')
                : 'etcd недоступен — повторите позже'}
          </Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button variant="light" loading={mutation.isPending} onClick={() => mutation.mutate()}>
            Поставить в очередь
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
