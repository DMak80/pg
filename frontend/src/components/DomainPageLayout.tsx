// Единый шаблон доменной страницы (u01, arch/03 §3): шапка-статус (заголовок +
// state-бейджи + режимы + endpoints + заявки в полёте + действия уровня
// кластера) + вкладки сущностей + нижний блок журнала (для доменов, где
// журнал — не вкладка).
import { Card, Group, SimpleGrid, Stack, Tabs, Text, Title } from '@mantine/core';
import type { ReactNode } from 'react';

export interface DomainTab {
  value: string;
  label: string;
  content: ReactNode;
}

export function DomainPageLayout({
  title,
  statusBadges,
  modeBadges,
  endpoints,
  inFlightBadges,
  actions,
  tabs,
  hideTabs = [],
  journal,
  infoFields,
  activeTab,
  onTabChange,
}: {
  title: ReactNode;
  statusBadges?: ReactNode;
  modeBadges?: ReactNode;
  endpoints?: string | null;
  inFlightBadges?: ReactNode;
  actions?: ReactNode;
  tabs: DomainTab[];
  hideTabs?: string[];
  journal?: ReactNode;
  infoFields?: { label: string; value: ReactNode }[];
  // u04: управляемые вкладки — программный переход (бакет → «Переезды»);
  // не заданы — uncontrolled (defaultValue).
  activeTab?: string;
  onTabChange?: (value: string | null) => void;
}) {
  const visibleTabs = tabs.filter((t) => !hideTabs.includes(t.value));
  return (
    <Stack gap="md">
      <Group justify="space-between" wrap="nowrap">
        <Group gap="sm" wrap="nowrap">
          <Title order={2}>{title}</Title>
          {statusBadges}
          {modeBadges}
        </Group>
        {actions}
      </Group>
      {endpoints ? (
        <Text size="sm" ff="monospace" c="dimmed">{endpoints}</Text>
      ) : null}
      {inFlightBadges ? <Group gap="xs">{inFlightBadges}</Group> : null}
      {infoFields && infoFields.length > 0 ? (
        <Card withBorder padding="md" radius="md">
          <SimpleGrid cols={{ base: 2, md: 4 }}>
            {infoFields.map((f) => (
              <Field key={f.label} label={f.label}>{f.value}</Field>
            ))}
          </SimpleGrid>
        </Card>
      ) : null}
      <Tabs
        value={activeTab}
        defaultValue={visibleTabs[0]?.value}
        onChange={onTabChange}
        keepMounted={false}
      >
        <Tabs.List>
          {visibleTabs.map((t) => (
            <Tabs.Tab key={t.value} value={t.value}>{t.label}</Tabs.Tab>
          ))}
        </Tabs.List>
        {visibleTabs.map((t) => (
          <Tabs.Panel key={t.value} value={t.value} pt="md">
            {t.content}
          </Tabs.Panel>
        ))}
      </Tabs>
      {journal}
    </Stack>
  );
}

// Поле карточки параметров (перенос Field из доменных страниц).
export function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Stack gap={2}>
      <Text size="xs" c="dimmed" tt="uppercase">{label}</Text>
      {typeof children === 'string' ? <Text>{children}</Text> : children}
    </Stack>
  );
}
