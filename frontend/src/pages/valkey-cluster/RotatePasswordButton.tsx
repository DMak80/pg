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
        title: 'Заявка отправлена',
        message: `Ротацию ${role}-пароля выполнит воркер: окно двух паролей без рестарта ноды. `
          + 'После применения подключения со старым паролем отвергаются — клиенты должны перечитать креды.',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.clusters });
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;
  const label = role === 'app' ? 'Сменить app-пароль' : 'Сменить admin-пароль';

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
            Воркер поставит ноде ДВА пароля (старый и новый), затем атомарно заменит
            <b> {role}_password</b> в etcd и снимет старый кред. Рестарта ноды нет.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            После применения подключения со старым паролем отвергаются до
            перечитывания кредов — окно двух паролей живёт до конца ротации.
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
