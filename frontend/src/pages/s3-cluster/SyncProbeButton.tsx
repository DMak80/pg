// Кнопка «Проверить сейчас» (arch/03 §12.3; 02 §12.2-10): внеплановый зонд
// синхронности/связности — POST sync-probe (202; отказы — текстом ProblemDetails).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Button, Group, Modal, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { s3QueryKeys, s3SyncProbe } from '../../api/queries';

export function SyncProbeButton({ cluster }: { cluster: string }) {
  const queryClient = useQueryClient();
  const [error, setError] = useState<string | null>(null);
  const mutation = useMutation({
    mutationFn: () => s3SyncProbe(cluster),
    onSuccess: async () => {
      setError(null);
      notifications.show({
        color: 'green',
        title: 'Зонд заявлен',
        message: 'S3Worker прогонит связность/синхронность — checked_unix обновится следующим тиком.',
        autoClose: 6000,
      });
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
    onError: (e) => setError(e instanceof ApiError ? e.message : String(e)),
  });

  return (
    <>
      <Button variant="light" loading={mutation.isPending} onClick={() => mutation.mutate()}>
        Проверить сейчас
      </Button>
      <Modal opened={error !== null} onClose={() => setError(null)} title="Зонд не заявлен" centered>
        <Stack gap="sm">
          {/* 409 «кластер не Active» и прочие отказы — текстом ProblemDetails. */}
          <Text size="sm">{error}</Text>
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setError(null)}>Понятно</Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
