// Кнопка «Ротация секретов» (arch/03 §12.3; 02 §12.2-9): заявка
// /s3worker/rotations/<C> — rolling-пересоздание инстансов воркером с новым
// env профиля; PKI переиспользуется (ключи переживают пересоздание).
// Успех 201 (создана) / 200 (уже стоит); 404/503 — текстом ProblemDetails.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { rotateS3Secrets, s3QueryKeys } from '../../api/queries';

export function RotateSecretsButton({ cluster, disabled }: { cluster: string; disabled: boolean }) {
  const queryClient = useQueryClient();
  const [opened, setOpened] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const mutation = useMutation({
    mutationFn: () => rotateS3Secrets(cluster),
    onSuccess: async () => {
      setOpened(false);
      setError(null);
      notifications.show({
        color: 'green',
        title: 'Заявка ротации отправлена',
        message: 'S3Worker переиздаст env-профиль rolling-пересозданием инстансов; '
          + 'бейдж ротации появится следующим тиком refresher\'а.',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.clusters });
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
    onError: (e) => setError(e instanceof ApiError ? e.message : String(e)),
  });

  return (
    <>
      <Button variant="light" disabled={disabled} onClick={() => setOpened(true)}>
        Ротация секретов
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Ротация секретов профиля — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            Воркер выполнит rolling-пересоздание инстансов кластера с новым env
            профиля <b>{cluster}</b>. На время пересоздания каждого инстанса
            хранилище деградирует по ёмкости.
          </Text>
          <Alert color="yellow" variant="light" title="Внимание">
            PKI переиспользуется — ключи нод переживают пересоздание; отмены
            заявки из панели нет (снятие — только воркером).
          </Alert>
          {error !== null ? <Alert color="red" variant="light">{error}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Отмена</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Заявить ротацию
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
