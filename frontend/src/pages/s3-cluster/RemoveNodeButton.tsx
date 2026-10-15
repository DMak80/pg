// Кнопка «Убрать ноду» (arch/03 §12.3; 02 §12.2-4): маркер демонтажа one-way —
// DELETE ноды (202; 204 — уже в демонтаже; 409 «последняя нода» — текстом).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { removeS3Node, s3QueryKeys } from '../../api/queries';

export function RemoveNodeButton({ cluster, node }: { cluster: string; node: string }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => removeS3Node(cluster, node),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="red" variant="light" size="xs" onClick={() => setOpened(true)}>
        Убрать ноду
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Убрать ноду ${node} — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Инстанс <b>{node}</b> уйдёт в демонтаж (one-way): S3Worker снесёт
            его контейнер и volume. Копия данных останется на остальных
            инстансах при настроенных связях репликации; без связей объекты
            этого инстанса теряются.
          </Text>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button color="red" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Убрать
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
