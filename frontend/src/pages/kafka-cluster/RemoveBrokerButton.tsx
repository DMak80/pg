// Кнопка «Убрать брокера» (arch/02 §10.2-5): маркер TO_REMOVE (one-way);
// демонтаж выполняет воркер (guards: не controller/не последний/без реплик).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { removeKafkaBroker } from '../../api/queries';

export function RemoveBrokerButton({ cluster, broker }: { cluster: string; broker: string }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => removeKafkaBroker(cluster, broker),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button size="compact-xs" variant="light" color="red" onClick={() => setOpened(true)}>
        Убрать
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Убрать брокера ${broker}`} centered>
        <Stack gap="sm">
          <Text>
            Брокер <b>{broker}</b> кластера <b>{cluster}</b> получит маркер{' '}
            <b>TO_REMOVE</b> (обратного перехода нет). ВОРКЕР перепроверит guards
            авторитетно: на брокере не должно быть реплик партиций — иначе демонтаж
            подождёт reassignment (roadmap).
          </Text>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button color="red" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Убрать брокера
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
