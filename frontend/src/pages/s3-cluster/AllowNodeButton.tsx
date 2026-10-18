// Кнопка «Снять отзыв» allow (arch/03 §12.3; 02 §12.2-8): POST allow — 202
// (204 уже снят). Доступна только у ноды в denylist.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { allowS3Node, s3QueryKeys } from '../../api/queries';

export function AllowNodeButton({ cluster, node }: { cluster: string; node: string }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => allowS3Node(cluster, node),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="green" variant="light" size="xs" onClick={() => setOpened(true)}>
        Allow
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Allow ${node} — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            The key revocation of instance <b>{node}</b> will be lifted: the name
            leaves the denylist, future joins of the instance are allowed again.
            Replication links are restored by worker convergence.
          </Text>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button color="green" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Allow
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
