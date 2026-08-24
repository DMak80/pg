// Вкладка «Шарды»: dsn, реплики, master+lease, плановые ноды, заявка ресурсов,
// runtime-колонки проб (t08 spec §4.10; ноды/заявка — t12); кнопки add/remove
// шарда и бейдж «к удалению» — t06 (arch/03 §3).
import { Badge, Button, Group, Stack, Table, Text, Tooltip } from '@mantine/core';
import { useState } from 'react';
import type { ShardDto } from '../../api/dto';
import { formatBytes } from '../../utils/format';
import { AddShardModal } from './AddShardModal';
import { RemoveShardButton } from './RemoveShardButton';

// canScale — кластер Active (кнопки скрыты для NOT_INITIALIZED/TO_REMOVE —
// симметрия с «Удалить кластер», t06 spec §6.3); bucketCounts — счётчик
// бакетов шарда по routing для диалога удаления.
export function ShardsTab({ cluster, canScale, shards, bucketCounts }: {
  cluster: string; canScale: boolean; shards: ShardDto[]; bucketCounts: Record<string, number>;
}) {
  const [addOpened, setAddOpened] = useState(false);
  const probesOff = shards.every((s) => s.runtime === null);
  const probeErrors = shards.filter((s) => s.runtime?.error != null);
  return (
    <Stack gap="xs">
      <Group justify="space-between">
        <Text fw={500}>Шарды</Text>
        {canScale ? (
          <Group gap="xs">
            <Button size="xs" variant="light" onClick={() => setAddOpened(true)}>Добавить шард</Button>
            <AddShardModal cluster={cluster} opened={addOpened} onClose={() => setAddOpened(false)} />
          </Group>
        ) : null}
      </Group>
      {shards.length === 0 ? (
        <Text c="dimmed">Шарды не найдены</Text>
      ) : (
        <Table.ScrollContainer minWidth={1200}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Шард</Table.Th>
                <Table.Th>DSN</Table.Th>
                <Table.Th>Реплики</Table.Th>
                <Table.Th>Мастер</Table.Th>
                <Table.Th>Ноды</Table.Th>
                <Table.Th>Ресурсы на ноду</Table.Th>
                <Table.Th>Sync-standby</Table.Th>
                <Table.Th>Лаг слотов</Table.Th>
                <Table.Th>WAL lost</Table.Th>
                <Table.Th>Подписки</Table.Th>
                <Table.Th>Схемы</Table.Th>
                {canScale ? <Table.Th>Действия</Table.Th> : null}
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {shards.map((s) => (
                <ShardRow key={s.name} shard={s} cluster={cluster} canScale={canScale}
                  bucketCount={bucketCounts[s.name] ?? 0} />
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
      {probesOff ? (
        <Text c="dimmed" size="sm">Пробы отключены — runtime-данные отсутствуют</Text>
      ) : null}
      {probeErrors.map((s) => (
        <Text key={s.name} c="red" size="sm">Ошибки проб: шард {s.name}: {s.runtime?.error}</Text>
      ))}
    </Stack>
  );
}

function ShardRow({ shard, cluster, canScale, bucketCount }: {
  shard: ShardDto; cluster: string; canScale: boolean; bucketCount: number;
}) {
  const runtime = shard.runtime;
  return (
    <Table.Tr>
      <Table.Td>
        <Group gap={6}>
          <span>{shard.name}</span>
          {shard.state === 'TO_REMOVE' ? (
            <Badge color="red" variant="light">к удалению</Badge>
          ) : null}
        </Group>
      </Table.Td>
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
      <Table.Td>
        {shard.nodes.length === 0 ? '—' : (
          <Group gap={4}>
            {shard.nodes.map((n) => (
              <Tooltip key={n.name} label={n.state ?? '—'}>
                <Badge color={n.state === 'NOT_INITIALIZED' ? 'gray' : 'teal'} variant="light">
                  {n.name}
                </Badge>
              </Tooltip>
            ))}
          </Group>
        )}
      </Table.Td>
      <Table.Td>
        {shard.requests === null ? '—' : (
          <Text ff="monospace" size="sm">
            {shard.requests.cpu} CPU · {shard.requests.mem} · {shard.requests.disk}
          </Text>
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
      {canScale ? (
        <Table.Td>
          <RemoveShardButton cluster={cluster} shard={shard.name} bucketCount={bucketCount} />
        </Table.Td>
      ) : null}
    </Table.Tr>
  );
}
