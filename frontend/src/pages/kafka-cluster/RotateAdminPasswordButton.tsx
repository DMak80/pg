// Кнопка «Сменить admin-пароль» kafka-кластера (мутация №16, t03, arch/02
// §10.2-16): заявка /kafkaworker/admin_rotations/<C>; исполняет воркер фазами
// A/B/C — rolling-рестарт брокеров; приложения (роль app) не затрагиваются.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateKafkaAdminPassword } from '../../api/queries';

export function RotateAdminPasswordButton({
  cluster,
  disabled,
}: {
  cluster: string;
  disabled: boolean;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateKafkaAdminPassword(cluster),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Ticket submitted',
        message: 'The admin-password rotation will be performed by the worker (phases A/B/C, '
          + 'rolling broker restart). Applications with the app role are not affected.',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button
        variant="light"
        color="orange"
        disabled={disabled}
        onClick={() => setOpened(true)}
      >
        Rotate admin password
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Rotate admin password — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            The worker will rotate the admin credential in phases: A) rolling broker
            restart with TWO admin credentials (old and new), B) atomic replacement of
            <b> admin_password</b> in etcd, C) rolling removal of the old credential.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            Rolling broker restart — perform this in a quiet window.
            <List size="sm" mt={4}>
              <List.Item>applications with the app role run continuously throughout the window</List.Item>
              <List.Item>after phase C, admin-role connections with the old password are rejected</List.Item>
            </List>
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
