// Кнопка «Удалить кластер» valkey (arch/02 §11.2): DELETE проксируется панелью
// в API воркера (202); воркер снимет контейнер ноды и весь префикс
// /valkey/clusters/<C>/ (демонтаж X0–X3, arch/21 §5).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useNavigate } from 'react-router';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { deleteValkeyCluster, valkeyQueryKeys } from '../../api/queries';

export function DeleteValkeyClusterButton({ cluster }: { cluster: string }) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => deleteValkeyCluster(cluster),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.clusters });
      navigate('/valkey');
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="red" variant="light" onClick={() => setOpened(true)}>Удалить кластер</Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Удалить valkey-кластер ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Кластер <b>{cluster}</b> уйдёт в демонтаж (обратного перехода нет).
            ВОРКЕР снимет контейнер ноды, очистит весь префикс
            /valkey/clusters/{cluster}/ и ключи координации (включая заявку ротации).
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
