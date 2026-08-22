// Заглушка панели Кластеры — наполнение в t08 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function ClustersPage() {
  return (
    <Container>
      <Title order={2}>Кластеры</Title>
      <Text c="dimmed">Панель будет реализована в t08-frontend-clusters.</Text>
    </Container>
  );
}
