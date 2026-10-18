// Кнопка «Сменить app-пароль» kafka-кластера (arch/02 §10.2-8): заявка
// /kafkaworker/rotations/<C>; исполняет воркер фазами A/B/C — rolling-перезапуск
// брокеров без окна недоступности; предупреждение в модалке.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateKafkaPassword } from '../../api/queries';

export function RotatePasswordButton({
  cluster,
  disabled,
}: {
  cluster: string;
  disabled: boolean;
}) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateKafkaPassword(cluster),
    onSuccess: async () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Ticket submitted',
        message: 'The rotation will be performed by the worker (phases A/B/C, rolling broker restart). '
          + 'After it is applied, clients must re-read app_password from etcd.',
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
        disabled={disabled}
        onClick={() => setOpened(true)}
      >
        Rotate app password
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Rotate app password — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            The worker will rotate the password in phases: A) rolling broker restart
            with TWO credentials (old and new), B) atomic replacement of
            <b> app_password</b> in etcd, C) rolling removal of the old credential.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            Rolling broker restart — perform this in a quiet window.
            <List size="sm" mt={4}>
              <List.Item>a "broker does not accept the working credential" window is impossible by design</List.Item>
              <List.Item>after phase C, connections with the old password are rejected</List.Item>
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
