// Модалка «Изменить ресурсы брокера» (t06, arch/02 §10.2-15): PUT декларации
// через мутацию №15; применяется NodeRegenerator'ом воркера rolling-ит.
// Предупреждение при уменьшении CPU/памяти — риск OOM на операторе (arch/16 R7).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { kafkaQueryKeys, updateKafkaBrokerResources } from '../../api/queries';
import type { KafkaBrokerDto } from '../../api/dto';

export function EditBrokerResourcesModal({
  cluster,
  broker,
  opened,
  onClose,
}: {
  cluster: string;
  broker: KafkaBrokerDto;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [cpu, setCpu] = useState<number>(broker.cpu ?? 2);
  const [memGi, setMemGi] = useState<number>(broker.memGi ?? 2);
  const [diskGi, setDiskGi] = useState<number>(broker.diskGi ?? 20);

  const mutation = useMutation({
    mutationFn: () => updateKafkaBrokerResources(cluster, broker.name, { cpu, memGi, diskGi }),
    onSuccess: async () => {
      onClose();
      notifications.show({
        color: 'green',
        title: 'Resource declaration updated',
        message: 'The worker will apply it automatically: rolling broker recreation, '
          + 'one per tick (data is preserved).',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: kafkaQueryKeys.cluster(cluster) });
    },
  });

  const serverError = mutation.error instanceof ApiError ? mutation.error : null;

  // Кнопка дизейблена, пока значения не менялись (мутация №15 требует хотя бы одно поле).
  const unchanged = cpu === (broker.cpu ?? 2)
    && memGi === (broker.memGi ?? 2)
    && diskGi === (broker.diskGi ?? 20);
  // Предупреждение: уменьшение CPU/памяти может привести к OOM/деградации (arch/16 R7).
  const decreasing = cpu < (broker.cpu ?? 2) || memGi < (broker.memGi ?? 2);

  return (
    <Modal opened={opened} onClose={onClose}
      title={`Resources of ${broker.name} — ${cluster}`} centered>
      <Stack gap="sm">
        <Group grow>
          <NumberInput label="CPU" value={cpu} step={0.5} min={0.01} max={64}
            onChange={(v) => setCpu(Number(v ?? 0))} />
          <NumberInput label="Memory, GiB" value={memGi} min={1} max={65536}
            onChange={(v) => setMemGi(Number(v ?? 0))} />
          <NumberInput label="Disk, GiB" value={diskGi} min={1} max={65536}
            onChange={(v) => setDiskGi(Number(v ?? 0))} />
        </Group>
        {decreasing ? (
          <Alert color="yellow" variant="light">
            Decreasing CPU/memory may cause OOM or broker degradation
            (the risk is on the operator).
          </Alert>
        ) : null}
        <Text size="sm" c="dimmed">
          Applied automatically: rolling broker recreation, one per tick; data is
          preserved. Disk is an advisory field (no quotas).
        </Text>
        {serverError ? <Alert color="red" variant="light">{serverError.message}</Alert> : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Cancel</Button>
          <Button disabled={unchanged} loading={mutation.isPending}>Apply</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
