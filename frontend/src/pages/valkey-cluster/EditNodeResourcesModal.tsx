// Модалка «Изменить ресурсы ноды» valkey (02 §11.2-4) — обёртка над общим
// ResourcesForm (u02): валидация — validation.ts; применяет автоконверге
// надзора (arch/21 §5 C — пересоздание контейнера, одно за тик).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import { updateValkeyNodeResources, valkeyQueryKeys } from '../../api/queries';
import type { ValkeyNodeDto } from '../../api/dto';
import { ResourcesForm } from '../../components/actions/ResourcesForm';

export function EditNodeResourcesModal({
  cluster,
  node,
  opened,
  onClose,
}: {
  cluster: string;
  node: ValkeyNodeDto;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: (v: { cpu?: number; memGi?: number; diskGi?: number }) =>
      updateValkeyNodeResources(cluster, node.name, v),
    onSuccess: async () => {
      onClose();
      notifications.show({
        color: 'green',
        title: 'Resource declaration updated',
        message: 'Applied by the supervision auto-converge: container recreation '
          + '(cache is refilled cold by the applications).',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: valkeyQueryKeys.cluster(cluster) });
    },
  });

  return (
    <ResourcesForm
      title={`Ресурсы ноды ${node.name} — ${cluster}`}
      initial={{ cpu: node.cpu ?? 1, memGi: node.memGi ?? 1, diskGi: node.diskGi ?? 10 }}
      applyNote={'Применяется пересозданием контейнера (автоконверге надзора, одно за тик) — '
        + 'кеш восполняется холодным стартом приложений; disk — информационное поле.'}
      opened={opened}
      onClose={onClose}
      onSubmit={(v) => mutation.mutateAsync(v)}
      error={mutation.error}
    />
  );
}
