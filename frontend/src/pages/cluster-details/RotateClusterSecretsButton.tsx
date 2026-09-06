// Кнопка «Сменить per-cluster секреты» в шапке деталей кластера: подтверждение →
// POST /api/clusters/{cluster}/secrets/rotate → заявка /pgworker/rotations/<C>
// (arch/02 §9.8, t02). Выполняет PgWorker (ALTER ROLE app/bucket_admin/
// bucket_mover на всех шардах + новые креды и dsn в etcd); после применения
// подключения со старыми паролями отвергаются, пока клиент не перечитает креды
// из etcd — предупреждение в модалке (spec О2) и success-нотификация после 201.
import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateClusterSecrets } from '../../api/queries';

export function RotateClusterSecretsButton({ name }: { name: string }) {
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateClusterSecrets(name),
    onSuccess: () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Заявка отправлена',
        message: 'Ротацию per-cluster секретов (app, bucket_admin, bucket_mover) выполнит '
          + 'PgWorker (фоновые тики). После применения клиенты должны перечитать '
          + 'креды и dsn из etcd.',
        autoClose: 8000,
      });
    },
  });

  // Ошибка сервера: 409 «уже запрошена» / 503 etcd / прочие ProblemDetails.
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button variant="light" onClick={() => setOpened(true)}>Сменить per-cluster секреты</Button>
      <Modal opened={opened} onClose={() => setOpened(false)} title="Сменить per-cluster секреты" centered>
        <Stack gap="sm">
          <Text>
            Кластер <b>{name}</b>: PgWorker сменит пароли ролей <b>app</b>,{' '}
            <b>bucket_admin</b> и <b>bucket_mover</b> на всех шардах, обновит ключи
            app_password/mover_password/bucket_admin_password и перезапишет dsn в etcd.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            После применения (секунды) подключения со старыми паролями начнут отвергаться,
            пока клиенты не перечитают креды (app) и dsn (bucket_admin) из etcd.
            Выполняйте в тихое окно.
            <List size="sm" mt={4}>
              <List.Item>заявка ставится в очередь и выполняется фоново (тики PgWorker)</List.Item>
              <List.Item>при недоступном шарде ротация повторяется автоматически</List.Item>
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
