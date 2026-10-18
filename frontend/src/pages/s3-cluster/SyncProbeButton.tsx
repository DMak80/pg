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
        title: 'Probe submitted',
        message: 'S3Worker will run the connectivity/sync probe — checked_unix will update on the next tick.',
        autoClose: 6000,
      });
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
    onError: (e) => setError(e instanceof ApiError ? e.message : String(e)),
  });

  return (
    <>
      <Button variant="light" loading={mutation.isPending} onClick={() => mutation.mutate()}>
        Check now
      </Button>
      <Modal opened={error !== null} onClose={() => setError(null)} title="Probe not submitted" centered>
        <Stack gap="sm">
          {/* 409 «кластер не Active» и прочие отказы — текстом ProblemDetails. */}
          <Text size="sm">{error}</Text>
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setError(null)}>Got it</Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}
