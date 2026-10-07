// Грань «Надёжность» (read-only, arch/03 §3): таблица кластер×шард — RPO-блок
// + RTO-блок; ongoing — бейдж «идёт» с тикающей длительностью (пересчёт каждым
// тиком снапшота); клик по id полного — детали шарда грани бэкапов; форм ввода нет.
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router';
import { Badge, Card, Group, Stack, Table, Text, Title } from '@mantine/core';
import { reliabilityQueryKeys, fetchReliability } from '../api/queries';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatUnixAge } from '../utils/format';
import type { HaFactRtoDto, OpRtoDto, ReliabilityClusterDto, RpoDto } from '../api/dto';

export function ReliabilityPage() {
  const intervalMs = usePollingIntervalMs();
  const query = useQuery({
    queryKey: reliabilityQueryKeys.all,
    queryFn: fetchReliability,
    refetchInterval: intervalMs,
  });
  if (query.isPending) return <Text>Загрузка…</Text>;
  if (query.isError) return <Text c="red">Ошибка: {(query.error as Error).message}</Text>;
  return (
    <Stack gap="md">
      <Title order={2}>Надёжность</Title>
      {query.data.clusters.map((c) => (
        <ClusterCard key={c.cluster} cluster={c} />
      ))}
    </Stack>
  );
}

function ClusterCard({ cluster }: { cluster: ReliabilityClusterDto }) {
  return (
    <Card withBorder padding="md" radius="md" mb="md">
      <Group justify="space-between" mb="xs">
        <Text fw={600}>Кластер {cluster.cluster}</Text>
      </Group>
      <Table striped highlightOnHover>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Шард</Table.Th>
            <Table.Th>Полный (возраст)</Table.Th>
            <Table.Th>WAL лаг</Table.Th>
            <Table.Th>WAL возраст</Table.Th>
            <Table.Th>Потеряем ≈</Table.Th>
            <Table.Th>Failover</Table.Th>
            <Table.Th>Rebuild</Table.Th>
            <Table.Th>Drill</Table.Th>
            <Table.Th>Restore</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {cluster.shards.map((s) => (
            <Table.Tr key={s.shard}>
              <Table.Td>{s.shard}</Table.Td>
              <RpoCells cluster={cluster.cluster} shard={s.shard} rpo={s.rpo} />
              <FactCell fact={s.rto?.lastFailover} />
              <FactCell fact={s.rto?.lastRebuild} />
              <OpCell op={s.rto?.lastDrill} />
              <OpCell op={s.rto?.lastRestore} />
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Card>
  );
}

// Секунды → человекочитаемо («75 с», «3 мин 5 с»); null → «—».
function formatSec(sec: number | null | undefined): string {
  if (sec === null || sec === undefined) return '—';
  if (sec < 60) return `${sec} с`;
  const minutes = Math.floor(sec / 60);
  const rest = sec % 60;
  if (minutes < 60) return rest === 0 ? `${minutes} мин` : `${minutes} мин ${rest} с`;
  return `${minutes} мин`;
}

// RPO-ячейки: возраст валидного полного с индикацией по порогу + клик к деталям
// грани бэкапов, лаг/возраст WAL, сводный «потеряем ≈ N» с бейджем источника.
function RpoCells({ cluster, shard, rpo }: { cluster: string; shard: string; rpo?: RpoDto | null }) {
  if (!rpo)
    return (
      <>
        <Table.Td>—</Table.Td>
        <Table.Td>—</Table.Td>
        <Table.Td>—</Table.Td>
        <Table.Td>
          <Badge color="gray">off</Badge>
        </Table.Td>
      </>
    );
  const fullAge = (
    <>
      {rpo.fullId ? (
        <Link to={`/backups-storage/${encodeURIComponent(cluster)}/${encodeURIComponent(shard)}`}>
          {rpo.fullId}
        </Link>
      ) : (
        '—'
      )}
      {rpo.fullAgeSec !== null && rpo.fullAgeSec !== undefined && (
        <Text component="span" c={rpo.fullAgeSec > rpo.thresholdFullAgeSec ? 'red' : 'green'} ml={6} size="sm">
          {formatSec(rpo.fullAgeSec)}
        </Text>
      )}
      {(rpo.fullAgeSec === null || rpo.fullAgeSec === undefined) && (
        <Text component="span" c="dimmed" ml={6} size="sm">
          никогда
        </Text>
      )}
    </>
  );
  return (
    <>
      <Table.Td>{fullAge}</Table.Td>
      <Table.Td>{rpo.walLagSegments ?? '—'}</Table.Td>
      <Table.Td>{formatSec(rpo.walAgeSec)}</Table.Td>
      <Table.Td>
        {rpo.mode === 'off' ? (
          <Badge color="gray">off</Badge>
        ) : (
          <Group gap={6} wrap="nowrap">
            <Text size="sm">{formatSec(rpo.rpoPotentialSec)}</Text>
            <Badge color={rpo.mode === 'wal' ? 'green' : 'orange'}>{rpo.mode}</Badge>
          </Group>
        )}
      </Table.Td>
    </>
  );
}

// HA-факт: ongoing → бейдж «идёт Nс»; закрытый → длительность + cause/node + когда.
function FactCell({ fact }: { fact?: HaFactRtoDto | null }) {
  if (!fact) return <Table.Td>—</Table.Td>;
  if (fact.ongoing)
    return (
      <Table.Td>
        <Badge color="red">идёт {formatSec(fact.ongoingSec)}</Badge>
      </Table.Td>
    );
  return (
    <Table.Td>
      <Text size="sm">
        {formatSec(fact.durationSec)} <Text component="span" c="dimmed" size="xs">({fact.cause}/{fact.node})</Text>
      </Text>
      <Text c="dimmed" size="xs">{fact.resolvedUnix !== null && fact.resolvedUnix !== undefined ? `${formatUnixAge(fact.resolvedUnix)} назад` : '—'}</Text>
    </Table.Td>
  );
}

// Drill/Restore: терминальный → state + длительность (ошибка — красным);
// ongoing → бейдж «идёт Nс»; нет данных → «—».
function OpCell({ op }: { op?: OpRtoDto | null }) {
  if (!op) return <Table.Td>—</Table.Td>;
  if (op.ongoingSec !== null && op.ongoingSec !== undefined)
    return (
      <Table.Td>
        <Badge color="red">идёт {formatSec(op.ongoingSec)}</Badge>
      </Table.Td>
    );
  return (
    <Table.Td>
      <Text size="sm" c={op.error ? 'red' : undefined}>
        {op.state} {op.durationSec !== null && op.durationSec !== undefined ? `· ${formatSec(op.durationSec)}` : ''}
      </Text>
    </Table.Td>
  );
}
