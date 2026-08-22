// Вкладка «Шарды»: dsn, реплики, master+lease, runtime-колонки проб (t08 spec §4.10).
import { Badge, Stack, Table, Text, Tooltip } from '@mantine/core';
import type { ShardDto } from '../../api/dto';
import { formatBytes } from '../../utils/format';

export function ShardsTab({ shards }: { shards: ShardDto[] }) {
  if (shards.length === 0) return <Text c="dimmed">Шарды не найдены</Text>;
  const probesOff = shards.every((s) => s.runtime === null);
  const probeErrors = shards.filter((s) => s.runtime?.error != null);
  return (
    <Stack gap="xs">
      <Table.ScrollContainer minWidth={1000}>
        <Table highlightOnHover>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Шард</Table.Th>
              <Table.Th>DSN</Table.Th>
              <Table.Th>Реплики</Table.Th>
              <Table.Th>Мастер</Table.Th>
              <Table.Th>Sync-standby</Table.Th>
              <Table.Th>Лаг слотов</Table.Th>
              <Table.Th>WAL lost</Table.Th>
              <Table.Th>Подписки</Table.Th>
              <Table.Th>Схемы</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {shards.map((s) => <ShardRow key={s.name} shard={s} />)}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {probesOff ? (
        <Text c="dimmed" size="sm">Пробы отключены — runtime-данные отсутствуют</Text>
      ) : null}
      {probeErrors.map((s) => (
        <Text key={s.name} c="red" size="sm">Ошибки проб: шард {s.name}: {s.runtime?.error}</Text>
      ))}
    </Stack>
  );
}

function ShardRow({ shard }: { shard: ShardDto }) {
  const runtime = shard.runtime;
  return (
    <Table.Tr>
      <Table.Td>{shard.name}</Table.Td>
      <Table.Td>
        <Tooltip label={shard.dsn} position="top">
          <Text ff="monospace" size="sm">{shard.hosts.join(', ')}</Text>
        </Tooltip>
      </Table.Td>
      <Table.Td>{shard.replicasDeclared ?? '—'}</Table.Td>
      <Table.Td>
        {shard.masterAddress === null ? (
          <Badge color="red" variant="light">нет мастера</Badge>
        ) : (
          <Tooltip label="master-lease жив (ключ присутствует)">
            <span>
              <Text size="sm" ff="monospace" span>{shard.masterAddress} </Text>
              <Badge color="teal" variant="light">lease</Badge>
            </span>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>{runtime === null ? '—' : runtime.standbiesSync ?? '—'}</Table.Td>
      <Table.Td>{runtime === null ? '—' : formatBytes(runtime.slotsLagMaxBytes)}</Table.Td>
      <Table.Td>
        {runtime === null || runtime.walStatusLost.length === 0 ? '—' : (
          <Tooltip multiline label={runtime.walStatusLost.join('\n')}>
            <Badge color="red" variant="light">{runtime.walStatusLost.length}</Badge>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>{runtime === null ? '—' : runtime.subscriptions.length}</Table.Td>
      <Table.Td>{runtime === null ? '—' : runtime.bucketSchemas.length}</Table.Td>
    </Table.Tr>
  );
}
