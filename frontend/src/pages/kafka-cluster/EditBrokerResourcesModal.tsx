// Модалка «Изменить ресурсы брокера» (t06, arch/02 §10.2-15) — обёртка над
// общим ResourcesForm (u02): валидация границ — validation.ts (единый модуль);
// мутация/нотификация — здесь. Предупреждение при уменьшении CPU/памяти —
// риск OOM на операторе (arch/16 R7); применяется NodeRegenerator rolling-ит.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import { kafkaQueryKeys, updateKafkaBrokerResources } from '../../api/queries';
import type { KafkaBrokerDto } from '../../api/dto';
import { ResourcesForm } from '../../components/actions/ResourcesForm';

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
  const initial = { cpu: broker.cpu ?? 2, memGi: broker.memGi ?? 2, diskGi: broker.diskGi ?? 20 };

  const mutation = useMutation({
    mutationFn: (v: { cpu?: number; memGi?: number; diskGi?: number }) =>
      updateKafkaBrokerResources(cluster, broker.name, v),
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

  // Предупреждение об уменьшении CPU/памяти (arch/16 R7) — в подписи формы.
  const applyNote = 'Применяется автоматически: rolling-пересоздание брокера, по одному за тик; '
    + 'данные сохраняются. Disk — информационное поле (без квот).'
    + (initial.cpu !== undefined && initial.memGi !== undefined
      ? ' Уменьшение CPU/памяти может привести к OOM/деградации брокера — риск на операторе.'
      : '');

  return (
    <ResourcesForm
      title={`Ресурсы ${broker.name} — ${cluster}`}
      initial={initial}
      applyNote={applyNote}
      opened={opened}
      onClose={onClose}
      onSubmit={(v) => mutation.mutateAsync(v)}
      error={mutation.error}
    />
  );
}
