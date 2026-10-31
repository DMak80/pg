// Вкладка «Секреты» pg-кластера (u07/D4): заявка ротации per-cluster секретов
// — кнопка перенесена сюда из шапки; состав ротируемых ролей — текст (без
// значений); предупреждение о разрыве подключений; живая заявка — бейдж
// «ротация…» из data.secretsRotationPending (источник — /pgworker/rotations/<C>).
import { Badge, Group, Stack, Text } from '@mantine/core';
import { RotateClusterSecretsButton } from './RotateClusterSecretsButton';

export function SecretsTab({ cluster, state, rotationPending }: {
  cluster: string;
  state: string;
  rotationPending: boolean;
}) {
  return (
    <Stack gap="md" maw={640}>
      <Group gap="sm">
        <Text fw={500}>Ротация per-cluster секретов</Text>
        {/* Живая заявка — бейдж дублируется в шапке страницы */}
        {rotationPending ? (
          <Badge color="blue" variant="light">ротация…</Badge>
        ) : null}
      </Group>
      <Text size="sm">
        Смена трёх ролей кластера сразу:{' '}
        <Text span ff="monospace">app</Text>,{' '}
        <Text span ff="monospace">bucket_admin</Text>,{' '}
        <Text span ff="monospace">bucket_mover</Text> (+{' '}
        <Text span ff="monospace">rest_password</Text>). Значения паролей панели
        неизвестны и не показываются — PgWorker генерирует их и атомарно
        перезаписывает ключи etcd и dsn-шардов.
      </Text>
      <Text size="sm" c="yellow">
        Предупреждение: после применения подключения со старыми паролями
        отвергаются до перечитывания кредов приложением — выполняйте в тихое окно.
      </Text>
      {/* Ротация — только Active (у NOT_INITIALIZED пароль ещё не используется);
          у TO_REMOVE кнопка скрыта (обратного перехода нет, arch/02 §9.4). */}
      {state === 'ACTIVE' ? <RotateClusterSecretsButton name={cluster} /> : null}
    </Stack>
  );
}
