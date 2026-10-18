// Кнопка «Отозвать ключ» deny (arch/03 §12.3; 02 §12.2-7): POST deny — 202
// (204 уже отозван). Confirm: связи с инстансом сносятся, будущие join запрещены.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { denyS3Node, s3QueryKeys } from '../../api/queries';

export function DenyNodeButton({ cluster, node }: { cluster: string; node: string }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => denyS3Node(cluster, node),
    onSuccess: async () => {
      setOpened(false);
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button color="orange" variant="light" size="xs" onClick={() => setOpened(true)}>
        Revoke key
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Revoke key of ${node} — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Instance <b>{node}</b> will be revoked: link settings with it are torn
            down by the worker, future joins are forbidden (denylist). The instance
            stays readable for its objects until teardown.
          </Text>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button color="orange" loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Revoke
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
