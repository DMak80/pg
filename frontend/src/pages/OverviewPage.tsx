// Заглушка панели Обзор — наполнение в t08 (spec §7.9).
import { Container, Text, Title } from '@mantine/core';

export function OverviewPage() {
  return (
    <Container>
      <Title order={2}>Обзор</Title>
      <Text c="dimmed">Дашборд будет реализован в t08-frontend-clusters.</Text>
    </Container>
  );
}
