// Заглушка панели etcd — наполнение в t08 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function EtcdPage() {
  return (
    <Container>
      <Title order={2}>etcd</Title>
      <Text c="dimmed">Панель будет реализована в t08-frontend-clusters.</Text>
    </Container>
  );
}
