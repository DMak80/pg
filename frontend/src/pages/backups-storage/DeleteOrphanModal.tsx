// Модал подтверждения удаления сироты (arch/02 §9.10) — обёртка над общим
// ConfirmModal (u02): подтверждение вводом префикса «<C>/<X>»; осознанная
// потеря DR-источника — заявка обходит hold и автозащиту валидного полного.
import { useMutation } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import { deleteOrphan } from '../../api/queries';
import { ConfirmModal } from '../../components/actions/ConfirmModal';

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

  return (
    <ConfirmModal
      opened
      onClose={onClose}
      title={`Удалить сироту ${prefix}`}
      text={`Данные сироты ${prefix} (включая потенциальный DR-источник) будут удалены из S3 НЕОБРАТИМО: `
        + 'заявка обходит hold и автозащиту валидного полного. Исполнение — ближайший проход sweeper воркера.'}
      expectedInput={prefix}
      confirmLabel="Удалить"
      busy={mutation.isPending}
      error={mutation.error}
      onConfirm={() => void mutation.mutate()}
    />
  );
}
