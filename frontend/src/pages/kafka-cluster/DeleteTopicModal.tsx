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
    <Modal opened onClose={onClose} title={`Удалить топик — ${topic}`} centered>
      <Stack gap="sm">
        <Text size="sm" c="red">
          Топик «{topic}» и все его данные будут удалены из Kafka безвозвратно.
          Заявка исполнится в течение ~15 с — до этого её можно отменить во вкладке.
        </Text>
        <TextInput
          label="Введите имя топика для подтверждения"
          placeholder={topic}
          value={confirmName}
          onChange={(e) => setConfirmName(e.currentTarget.value)}
        />
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button
            color="red"
            loading={mutation.isPending}
            disabled={confirmName !== topic}
            onClick={() => mutation.mutate()}
          >
            Удалить
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
