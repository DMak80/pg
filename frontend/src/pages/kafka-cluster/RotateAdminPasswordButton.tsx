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
        title: 'Заявка отправлена',
        message: 'Ротацию admin-пароля выполнит воркер (фазы A/B/C, rolling-рестарт брокеров). '
          + 'Приложения с ролью app не затрагиваются.',
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
        Сменить admin-пароль
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Сменить admin-пароль — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Воркер выполнит ротацию admin-креда фазами: A) rolling-рестарт брокеров
            с ДВУМЯ admin-кредами (старым и новым), B) атомарная замена
            <b> admin_password</b> в etcd, C) rolling-снятие старого креда.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            Rolling-рестарт брокеров — выполняйте в тихое окно.
            <List size="sm" mt={4}>
              <List.Item>приложения с ролью app работают непрерывно на всём окне</List.Item>
              <List.Item>после фазы C подключения с ролью admin по старому паролю отвергаются</List.Item>
            </List>
          </Alert>
          {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Сменить пароль
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
