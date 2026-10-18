// Модал подтверждения удаления топика (arch/02 §10.2-10, t01): деструктивная
// lifecycle-заявка desired.delete; кнопка активируется вводом имени топика.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, Stack, Text, TextInput } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { deleteKafkaTopic } from '../../api/queries';

export function DeleteTopicModal({
  cluster,
  topic,
  onClose,
}: {
  cluster: string;
  topic: string;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [confirmName, setConfirmName] = useState('');

  const mutation = useMutation({
    mutationFn: () => deleteKafkaTopic(cluster, topic),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: ['kafka-clusters'] });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  return (
    <Modal opened onClose={onClose} title={`Delete topic — ${topic}`} centered>
      <Stack gap="sm">
        <Text size="sm" c="red">
          The topic "{topic}" and all its data will be deleted from Kafka irreversibly.
          The ticket will be executed within ~15 s — until then it can be cancelled in the tab.
        </Text>
        <TextInput
          label="Type the topic name to confirm"
          placeholder={topic}
          value={confirmName}
          onChange={(e) => setConfirmName(e.currentTarget.value)}
        />
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button
            color="red"
            loading={mutation.isPending}
            disabled={confirmName !== topic}
            onClick={() => mutation.mutate()}
          >
            Delete
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
