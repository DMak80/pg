// Лента событий /events (u09): таблица событий in-memory store (cap 500,
// новые сверху) с фильтрами домена и entityKind (контролируемые useState —
// Mantine Select не синхронизирует DOM-value с опциями); клик по событию —
// deep-link на сущность (та же функция маршрутизации, что у алертов —
// alertRoute). Лента живёт в открытой вкладке; после перезагрузки пуста.
// Пагинация (первые PAGE строк + «Показать ещё») и memo строк — сотни событий
// не рендерят тысячи Mantine-элементов на каждый тик.
import { memo, useState } from 'react';
import { Anchor, Badge, Button, Card, Group, Select, Stack, Table, Text, Title } from '@mantine/core';
import { useSyncExternalStore } from 'react';
import { Link } from 'react-router';
import { clearEvents, getEvents, subscribe } from '../events/eventsStore';
import { getDiffErrorCount } from '../events/EventsProvider';
import { alertRoute } from '../utils/alertRoutes';
import type { EventDomain, EventEntityKind, EventItem } from '../events/eventsStore';

// Домены ленты = домены, которые диффы реально эмитят (pg/kafka/valkey/s3;
// etcd-домен диффы не эмитят — ключи etcd-контура в ленту не наблюдаются).
const DOMAIN_OPTIONS = [
  { value: 'all', label: 'Все' },
  { value: 'pg', label: 'pg' },
  { value: 'kafka', label: 'kafka' },
  { value: 'valkey', label: 'valkey' },
  { value: 's3', label: 's3' },
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
  { value: 'restore', label: 'restore' },
  { value: 'drill', label: 'drill' },
  { value: 'orphan', label: 'orphan' },
];

// Строк на страницу (пагинация вместо виртуализации: сотни строк — не тысячи,
// полный ре-рендер вьюпорта дёшев; «Показать ещё» растит окно).
const PAGE_SIZE = 50;

export function EventsPage() {
  const events = useSyncExternalStore(subscribe, getEvents);
  // Контролируемые фильтры: value из state (не из DOM) — чтение `.value`
  // input'а Mantine Select возвращало label («Все»), фильтр не работал.
  const [domainFilter, setDomainFilter] = useState('all');
  const [kindFilter, setKindFilter] = useState('all');
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);

  const filtered = events.filter((e) =>
    (domainFilter === 'all' || e.domain === (domainFilter as EventDomain | 'all'))
    && (kindFilter === 'all' || e.entityKind === (kindFilter as EventEntityKind | 'all')));

  // Окно пагинации сбрасывается при смене фильтров (список другой).
  const visible = filtered.slice(0, visibleCount);
  const hidden = filtered.length - visible.length;

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
            value={domainFilter}
            onChange={(v) => {
              setDomainFilter(v ?? 'all');
              setVisibleCount(PAGE_SIZE);
            }}
          />
          <Select
            w={160}
            size="xs"
            label="Тип сущности"
            data={ENTITY_KIND_OPTIONS}
            value={kindFilter}
            onChange={(v) => {
              setKindFilter(v ?? 'all');
              setVisibleCount(PAGE_SIZE);
            }}
          />
          <Button size="xs" variant="light" onClick={() => clearEvents()}>Очистить</Button>
        </Group>
      </Group>

      <Text size="sm" c="dimmed">
        Лента живёт в открытой вкладке; после перезагрузки пуста (истории нет —
        журналы воркеров перезаписываются, хранения панель не ведёт).
      </Text>

      {/* Отказ диффа (дрейф DTO) не глушится молча: счётчик ошибок диффа —
          признак «лента неполна» (консоль браузера — причина по queryKey). */}
      {getDiffErrorCount() > 0 ? (
        <Text size="sm" c="orange">
          Лента неполна: ошибок диффа — {getDiffErrorCount()} (детали — в консоли браузера).
        </Text>
      ) : null}

      <EventsTable events={visible} />

      {hidden > 0 ? (
        <Button
          variant="subtle"
          size="xs"
          onClick={() => setVisibleCount((c) => c + PAGE_SIZE)}
        >
          Показать ещё ({hidden} скрыто)
        </Button>
      ) : null}
    </Stack>
  );
}

// Строка ленты — memo: ре-рендер при новом событии касается только добавленных
// строк (prop по событию неизменяем — сотни строк не перерисовываются).
const EventRow = memo(function EventRow({ event: e }: { event: EventItem }) {
  // u04: единая таблица маршрутов с алертами (kind фиктивный —
  // маршрут по домену/сущности события; restore/drill/orphan —
  // свои kind'ы бэкапов → грань /backups-storage).
  const fakeKind = e.entityKind === 'restore' ? 'restore-failed'
    : e.entityKind === 'drill' ? 'backup-drill-failed'
      : e.entityKind === 'orphan' ? 'backup-orphan'
        // глобальный стоп бэкапов → /backups-storage (иначе
        // fallback дал бы /clusters/backups-disabled — 404)
        : e.entityKind === 'ticket' && e.entity === 'backups-disabled' ? 'backups-disabled'
          : e.domain === 'kafka' ? 'kafka-rotation-pending'
            : e.domain === 'valkey' ? 'valkey-rotation-pending'
              : e.domain === 's3' ? 's3-rotation-pending'
                : 'cluster-incomplete';
  const route = alertRoute(fakeKind, e.entity);
  const change = e.type === 'state-change' && e.from !== undefined && e.to !== undefined
    ? `${e.from} → ${e.to}`
    : e.type;
  return (
    <Table.Tr>
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
});

function EventsTable({ events }: { events: EventItem[] }) {
  return (
    <Card withBorder padding="md" radius="md">
      {events.length === 0 ? (
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
              {events.map((e) => <EventRow key={e.id} event={e} />)}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Card>
  );
}
