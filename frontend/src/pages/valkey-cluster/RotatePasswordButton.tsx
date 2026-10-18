// Кнопка «Сменить пароль» valkey-кластера, общий компонент для ролей app|admin
// (arch/02 §11.2, arch/20 §3): заявка /valkeyworker/rotations/<C>; воркер
// применяет окно двух паролей БЕЗ рестартов ноды. 409 «уже запрошена» —
// текстом из ProblemDetails.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateValkeyPassword, valkeyQueryKeys } from '../../api/queries';

export function RotatePasswordButton({
  cluster,
  role,
  disabled,
}: {
  cluster: string;
  role: 'app' | 'admin';
  disabled: boolean;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateValkeyPassword(cluster, role),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Ticket submitted',
        message: `The ${role}-password rotation will be performed by the worker: a two-password window without a node restart. `
          + 'After it is applied, connections with the old password are rejected — clients must re-read the credentials.',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.clusters });
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;
  const label = role === 'app' ? 'Rotate app password' : 'Rotate admin password';

  return (
    <>
      <Button
        variant="light"
        disabled={disabled}
        onClick={() => setOpened(true)}
      >
        {label}
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`${label} — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            The worker will give the node TWO passwords (old and new), then atomically
            replace <b> {role}_password</b> in etcd and remove the old credential.
            No node restart.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            After it is applied, connections with the old password are rejected until
            the credentials are re-read — the two-password window lives until the
            rotation ends.
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Rotate password
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
