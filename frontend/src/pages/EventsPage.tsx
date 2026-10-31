// Лента событий /events (u09): таблица событий in-memory store (cap 500,
// новые сверху) с фильтрами домена и entityKind; клик по событию — deep-link
// на сущность (та же функция маршрутизации, что у алертов — alertRoute, шаг 10).
// Лента живёт в открытой вкладке; после перезагрузки пуста.
import { Anchor, Badge, Button, Card, Group, Select, Stack, Table, Text, Title } from '@mantine/core';
import { useSyncExternalStore } from 'react';
import { Link } from 'react-router';
import { clearEvents, getEvents, subscribe } from '../events/eventsStore';
import { alertRoute } from '../utils/alertRoutes';
import type { EventDomain, EventEntityKind, EventItem } from '../events/eventsStore';

const DOMAIN_OPTIONS = [
  { value: 'all', label: 'Все' },
  { value: 'pg', label: 'pg' },
  { value: 'kafka', label: 'kafka' },
  { value: 'valkey', label: 'valkey' },
  { value: 's3', label: 's3' },
  { value: 'etcd', label: 'etcd' },
];

const ENTITY_KIND_OPTIONS: { value: string; label: string }[] = [
  { value: 'all', label: 'Все' },
  { value: 'cluster', label: 'cluster' },
  { value: 'shard', label: 'shard' },
  { value: 'node', label: 'node' },
  { value: 'broker', label: 'broker' },
  { value: 'bucket', label: 'bucket' },
  { value: 'topic', label: 'topic' },
  { value: 'ticket', label: 'ticket' },
  { value: 'heal', label: 'heal' },
  { value: 'evacuation', label: 'evacuation' },
  { value: 'rotation', label: 'rotation' },
  { value: 'instance', label: 'instance' },
];

export function EventsPage() {
  const events = useSyncExternalStore(subscribe, getEvents);

  return (
    <Stack gap="md">
      <Group justify="space-between" align="flex-end">
        <Title order={2}>События</Title>
        <Group gap="xs">
          <Select
            w={140}
            size="xs"
            label="Домен"
            data={DOMAIN_OPTIONS}
            defaultValue="all"
            id="events-domain-filter"
          />
          <Select
            w={160}
            size="xs"
            label="Тип сущности"
            data={ENTITY_KIND_OPTIONS}
            defaultValue="all"
            id="events-kind-filter"
          />
          <Button size="xs" variant="light" onClick={() => clearEvents()}>Очистить</Button>
        </Group>
      </Group>

      <Text size="sm" c="dimmed">
        Лента живёт в открытой вкладке; после перезагрузки пуста (истории нет —
        журналы воркеров перезаписываются, хранения панель не ведёт).
      </Text>

      <EventsTable events={events} />
    </Stack>
  );
}

// Таблица-лента: фильтры читаются из DOM-select'ов напрямую (лента маленькая,
// кап 500 — пере-фильтрация на каждом рендере дешёвая; controlled state не
// нужен для чтения двух select'ов).
function EventsTable({ events }: { events: EventItem[] }) {
  const domainFilter = (document.getElementById('events-domain-filter') as HTMLSelectElement | null)?.value ?? 'all';
  const kindFilter = (document.getElementById('events-kind-filter') as HTMLSelectElement | null)?.value ?? 'all';

  const filtered = events.filter((e) =>
    (domainFilter === 'all' || e.domain === (domainFilter as EventDomain | 'all'))
    && (kindFilter === 'all' || e.entityKind === (kindFilter as EventEntityKind | 'all')));

  return (
    <Card withBorder padding="md" radius="md">
      {filtered.length === 0 ? (
        <Text c="dimmed" size="sm">Событий нет (лента наполняется диффами опроса: переходы состояний, заявки, исходы).</Text>
      ) : (
        <Table.ScrollContainer minWidth={820}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Время</Table.Th>
                <Table.Th>Домен</Table.Th>
                <Table.Th>Тип сущности</Table.Th>
                <Table.Th>Сущность</Table.Th>
                <Table.Th>Событие</Table.Th>
                <Table.Th>Детали</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {filtered.map((e) => {
                // u04: единая таблица маршрутов с алертами (kind фиктивный —
                // маршрут по домену/сущности события).
                const fakeKind = e.domain === 'kafka' ? 'kafka-rotation-pending'
                  : e.domain === 'valkey' ? 'valkey-rotation-pending'
                    : e.domain === 's3' ? 's3-rotation-pending'
                      : 'cluster-incomplete';
                const route = alertRoute(fakeKind, e.entity);
                const change = e.type === 'state-change' && e.from !== undefined && e.to !== undefined
                  ? `${e.from} → ${e.to}`
                  : e.type;
                return (
                  <Table.Tr key={e.id}>
                    <Table.Td>
                      <Text size="sm" c="dimmed">
                        {new Date(e.tsUnix * 1000).toLocaleTimeString()}
                      </Text>
                    </Table.Td>
                    <Table.Td><Badge color="gray" variant="light" size="xs">{e.domain}</Badge></Table.Td>
                    <Table.Td><Text size="sm" ff="monospace">{e.entityKind}</Text></Table.Td>
                    <Table.Td>
                      {route !== null ? (
                        <Anchor component={Link} to={route} size="sm" ff="monospace">
                          {e.entity}
                        </Anchor>
                      ) : (
                        <Text size="sm" ff="monospace">{e.entity}</Text>
                      )}
                    </Table.Td>
                    <Table.Td>
                      <Group gap={6}>
                        <Badge
                          color={e.type === 'gone' ? 'orange' : e.type === 'outcome' ? 'violet' : 'gray'}
                          variant="light" size="xs"
                        >
                          {e.type}
                        </Badge>
                        {change !== e.type ? (
                          <Text size="sm" ff="monospace">{change}</Text>
                        ) : null}
                      </Group>
                    </Table.Td>
                    <Table.Td>
                      {e.detail ? <Text size="sm">{e.detail}</Text> : <Text c="dimmed">—</Text>}
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}
