// Панель «HA»: список scope'ов — лидер, члены healthy/total, макс. лаг,
// пометка unmatched (t09 spec §4.3).
import { useQuery } from '@tanstack/react-query';
import { Anchor, Badge, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { Link } from 'react-router';
import type { HaScopeSummaryDto } from '../api/dto';
import { fetchHaScopes, queryKeys } from '../api/queries';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { formatBytes } from '../utils/format';

export function HaPage() {
  const intervalMs = usePollingIntervalMs();
  const scopes = useQuery({
    queryKey: queryKeys.haScopes,
    queryFn: fetchHaScopes,
    refetchInterval: intervalMs,
  });

  if (scopes.data === undefined)
    return scopes.isError ? (
      <ErrorSection error={scopes.error} onRetry={() => void scopes.refetch()} />
    ) : (
      <LoadingSection />
    );

  return (
    <Stack gap="md">
      <Title order={2}>HA</Title>
      {scopes.data.length === 0 ? (
        <Text c="dimmed">HA-scope'ы не найдены</Text>
      ) : (
        <Table.ScrollContainer minWidth={700}>
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Scope</Table.Th>
                <Table.Th>Кластер/шард</Table.Th>
                <Table.Th>Лидер</Table.Th>
                <Table.Th>Члены</Table.Th>
                <Table.Th>Макс. лаг</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {scopes.data.map((s) => (
                <ScopeRow key={s.scope} scope={s} />
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Stack>
  );
}

// Строка скопа: «нет лидера» красным только у matched — рифма с алертом
// shard-no-leader (t06 §3.10); unmatched — чужой скоп, не алерт (arch/02 §7).
function ScopeRow({ scope }: { scope: HaScopeSummaryDto }) {
  return (
    <Table.Tr>
      <Table.Td>
        <Anchor component={Link} to={`/ha/${scope.scope}`} size="sm" ff="monospace">
          {scope.scope}
        </Anchor>
      </Table.Td>
      <Table.Td>
        {scope.matched ? (
          <Text size="sm" ff="monospace">{scope.cluster ?? '—'}/{scope.shard ?? '—'}</Text>
        ) : (
          <Tooltip label="scope не сопоставлен кластеру (arch/02 §7)">
            <span>
              <Text size="sm" c="dimmed" span>— </Text>
              <Badge color="yellow" variant="light">unmatched</Badge>
            </span>
          </Tooltip>
        )}
      </Table.Td>
      <Table.Td>
        {scope.leaderName === null ? (
          scope.matched ? (
            <Badge color="red" variant="light">нет лидера</Badge>
          ) : (
            <Text size="sm" c="dimmed">нет лидера</Text>
          )
        ) : (
          <Text ff="monospace" size="sm">{scope.leaderName}</Text>
        )}
      </Table.Td>
      <Table.Td>
        <Text
          size="sm"
          ff="monospace"
          c={scope.membersHealthy < scope.membersTotal ? 'yellow' : undefined}
        >
          {scope.membersHealthy}/{scope.membersTotal}
        </Text>
      </Table.Td>
      <Table.Td>{formatBytes(scope.lagMaxBytes)}</Table.Td>
    </Table.Tr>
  );
}
