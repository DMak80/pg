// Кнопка «Удалить кластер» kafka (arch/02 §10.2-2): config.state=TO_REMOVE;
// контейнеры/тома и весь префикс /kafka/clusters/<C>/ удалит воркер.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { deleteKafkaCluster } from '../../api/queries';

export function DeleteKafkaClusterButton({ cluster }: { cluster: string }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => deleteKafkaCluster(cluster),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="red" variant="light" onClick={() => setOpened(true)}>Удалить кластер</Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Удалить kafka-кластер ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Кластер <b>{cluster}</b> перейдёт в <b>TO_REMOVE</b> (обратного перехода нет).
            ВОРКЕР снимет контейнеры и тома брокеров, очистит весь префикс
            /kafka/clusters/{cluster}/ и координационные ключи (включая заявку ротации).
          </Text>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button color="red" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Удалить
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
