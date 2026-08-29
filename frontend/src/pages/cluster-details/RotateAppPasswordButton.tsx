// Кнопка «Сменить app-пароль» в шапке деталей кластера: подтверждение →
// POST /api/clusters/{cluster}/app-password/rotate → заявка /pgworker/rotations/<C>
// (arch/02 §9.8). Выполняет PgWorker (ALTER ROLE на всех шардах + новый пароль в
// etcd); после применения подключения со старым паролем отвергаются, пока
// приложение не перечитает app_password — предупреждение в модалке (spec О2) и
// success-нотификация после 201 (spec §4.6).
import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Group, List, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateAppPassword } from '../../api/queries';

export function RotateAppPasswordButton({ name }: { name: string }) {
  const [opened, setOpened] = useState(false);
  const mutation = useMutation({
    mutationFn: () => rotateAppPassword(name),
    onSuccess: () => {
      setOpened(false);
      notifications.show({
        color: 'green',
        title: 'Заявка отправлена',
        message: 'Смену app-пароля выполнит PgWorker (фоновые тики). После применения '
          + 'приложение должно перечитать app_password из etcd.',
        autoClose: 8000,
      });
    },
  });

  // Ошибка сервера: 409 «уже запрошена» / 503 etcd / прочие ProblemDetails.
  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <>
      <Button variant="light" onClick={() => setOpened(true)}>Сменить app-пароль</Button>
      <Modal opened={opened} onClose={() => setOpened(false)} title="Сменить app-пароль" centered>
        <Stack gap="sm">
          <Text>
            Кластер <b>{name}</b>: PgWorker сменит пароль роли <b>app</b> на всех нодах и
            обновит ключ <b>app_password</b> в etcd.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            После применения (секунды) подключения со старым паролем начнут отвергаться,
            пока приложение не перечитает app_password из etcd. Выполняйте в тихое окно.
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
