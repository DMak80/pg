// Модалка «Изменить ресурсы инстанса» s3 (02 §12.2-5) — обёртка над общим
// ResourcesForm (u02): валидация — validation.ts; cpu/mem — rolling-пересоздание
// контейнера надзором (disk — декларация/наблюдение).
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import { s3QueryKeys, updateS3NodeResources } from '../../api/queries';
import type { S3NodeDto } from '../../api/dto';
import { ResourcesForm } from '../../components/actions/ResourcesForm';

export function EditNodeResourcesModal({
  cluster,
  node,
  opened,
  onClose,
}: {
  cluster: string;
  node: S3NodeDto;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: (v: { cpu?: number; memGi?: number; diskGi?: number }) =>
      updateS3NodeResources(cluster, node.name, v),
    onSuccess: async () => {
      onClose();
      notifications.show({
        color: 'green',
        title: 'Resource declaration updated',
        message: 'cpu/mem — rolling container recreation by the supervisor; '
          + 'disk is an advisory declaration (no action).',
        autoClose: 8000,
      });
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
  });

  return (
    <ResourcesForm
      title={`Ресурсы инстанса ${node.name} — ${cluster}`}
      initial={{
        cpu: node.resources?.cpu ?? undefined,
        memGi: node.resources?.memGi ?? undefined,
        diskGi: node.resources?.diskGi ?? undefined,
      }}
      applyNote={'cpu/mem — rolling-пересоздание контейнера надзором; disk — декларация '
        + '(наблюдение, действий не вызывает).'}
      opened={opened}
      onClose={onClose}
      onSubmit={(v) => mutation.mutateAsync(v)}
      error={mutation.error}
    />
  );
}
