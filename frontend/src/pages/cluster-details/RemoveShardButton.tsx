// Кнопка «Убрать шард» (t06, arch/03 §3): диалог со счётчиком бакетов шарда;
// при N>0 кнопка подтверждения дизейблится («сначала перевезите бакеты»);
// серверный 409 (guard-пред-проверки Д4) показывается текстом ProblemDetails.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { queryKeys, removeShard } from '../../api/queries';

export function RemoveShardButton({ cluster, shard, bucketCount }: {
  cluster: string; shard: string; bucketCount: number;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => removeShard(cluster, shard),
    onSuccess: async () => {
      // Следующий тик refresher'а (≤3 с) подхватит маркер; бейдж перерисуется.
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: ['clusters'] });
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster(cluster) });
    },
  });

  // Серверный 409 — текст ProblemDetails (detail причины пред-проверок Д4).
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="red" variant="light" size="xs" onClick={() => setOpened(true)}>Убрать шард</Button>
      <Modal opened={opened} onClose={() => setOpened(false)} title="Убрать шард" centered>
        <Stack gap="sm">
          <Text>
            Шард <b>{shard}</b> будет помечен к удалению (<b>TO_REMOVE</b>). Демонтаж выполнит
            PgWorker — после того, как все бакеты уедут со шарда.
          </Text>
          {bucketCount > 0 ? (
            <Alert color="yellow" variant="light">
              На шарде {bucketCount} бакет(ов) — сначала явно перевезите их (UI переездов — t07)
            </Alert>
          ) : null}
          {serverError !== null ? (
            <Alert color="red" variant="light">{serverError.message}</Alert>
          ) : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button color="red" disabled={bucketCount > 0} loading={mutation.isPending}
              onClick={() => mutation.mutate()}>Убрать шард</Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
