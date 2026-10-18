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
      <Button color="red" variant="light" onClick={() => setOpened(true)}>Delete cluster</Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Delete Valkey cluster ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Cluster <b>{cluster}</b> will go into teardown (no way back).
            THE WORKER will remove the node container, clean the entire
            /valkey/clusters/{cluster}/ prefix and coordination keys (including
            the rotation ticket).
          </Text>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button color="red" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Delete
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
