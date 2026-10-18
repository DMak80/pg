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
        title: 'Rotation ticket submitted',
        message: 'S3Worker will re-issue the env profile via rolling instance recreation; '
          + 'the rotation badge will appear on the next refresher tick.',
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
        Rotate secrets
      </Button>
      <Modal opened={opened} onClose={() => setOpened(false)}
        title={`Profile secrets rotation — ${cluster}`} centered>
        <Stack gap="sm">
          <Text>
            The worker will perform a rolling recreation of the cluster instances with
            the new env of profile <b>{cluster}</b>. During each instance recreation
            the storage degrades in capacity.
          </Text>
          <Alert color="yellow" variant="light" title="Warning">
            PKI is reused — node keys survive recreation; the ticket cannot be
            cancelled from the panel (withdrawal is only by the worker).
          </Alert>
          {error !== null ? <Alert color="red" variant="light">{error}</Alert> : null}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setOpened(false)}>Cancel</Button>
            <Button loading={mutation.isPending} onClick={() => mutation.mutate()}>
              Submit rotation
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
