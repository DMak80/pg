// Модал подтверждения удаления сироты (reliability t04, arch/02 §9.10):
// осознанная потеря DR-источника — кнопка активируется вводом префикса
// "<C>/<X>" (образец DeleteTopicModal).
import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text, TextInput } from '@mantine/core';
import { useState } from 'react';
import { notifications } from '@mantine/notifications';
import { ApiError } from '../../api/client';
import { deleteOrphan } from '../../api/queries';

export function DeleteOrphanModal({
  prefix,
  cluster,
  shard,
  onClose,
  onDone,
}: {
  prefix: string;
  cluster: string;
  shard: string;
  onClose: () => void;
  onDone: () => void;
}) {
  const [confirmText, setConfirmText] = useState('');

  const mutation = useMutation({
    mutationFn: () => deleteOrphan(cluster, shard, prefix),
    onSuccess: async (accepted) => {
      notifications.show({
        color: 'green',
        message: `The delete ticket for ${accepted.prefix} was accepted — the sweeper will execute it on the next pass`,
      });
      onDone();
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened onClose={onClose} title={`Delete orphan ${prefix}`} centered>
      <Stack gap="sm">
        <Text size="sm" c="red">
          The orphan data {prefix} (including a potential DR source) will be deleted
          from S3 IRREVERSIBLY: the ticket bypasses the hold and the full-backup
          auto-protection. Execution — the next pass of the worker sweeper.
        </Text>
        <TextInput
          label="Type the <C>/<X> prefix to confirm"
          placeholder={prefix}
          value={confirmText}
          onChange={(e) => setConfirmText(e.currentTarget.value)}
        />
        {serverError ? (
          <Alert color="red" variant="light">{serverError.detail ?? serverError.message}</Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button
            color="red"
            loading={mutation.isPending}
            disabled={confirmText !== prefix}
            onClick={() => mutation.mutate()}
          >
            Delete
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
