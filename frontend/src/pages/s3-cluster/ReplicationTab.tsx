// Вкладка «Репликация» (arch/03 §12.3): просмотр — таблица пар + матрица
// смежности нода×нода (deny-ноды приглушены; анти-аффинити — совпадения
// placed_on подсвечены); редактор — LinksEditorModal (PUT links целиком).
import { Badge, Button, Group, Stack, Table, Text, Title } from '@mantine/core';
import { useState } from 'react';
import type { S3ClusterDto } from '../../api/dto';
import { LinksEditorModal } from './LinksEditorModal';

export function ReplicationTab({ cluster, canMutate }: { cluster: S3ClusterDto; canMutate: boolean }) {
  const [editorOpened, setEditorOpened] = useState(false);
  const names = cluster.nodesList.map((n) => n.name);
  const denied = new Set(cluster.denylist);
  const pairSet = new Set(cluster.links.map((l) => [l.a, l.b].sort().join('|')));

  // Анти-аффинити: у пары концов связи одинаковый непустой placed_on.
  const placedOn = new Map(cluster.nodesList.map((n) => [n.name, n.placedOn]));

  return (
    <Stack gap="md">
      <Group justify="space-between">
        <Title order={4}>Replication graph</Title>
        {canMutate ? (
          <Button size="xs" onClick={() => setEditorOpened(true)}>Edit graph</Button>
        ) : null}
      </Group>
      <LinksEditorModal
        cluster={cluster}
        opened={editorOpened}
        onClose={() => setEditorOpened(false)}
      />

      {/* Таблица пар. */}
      <Table.ScrollContainer minWidth={420}>
        <Table verticalSpacing="xs" withTableBorder>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Link</Table.Th>
              <Table.Th>Hosts</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {cluster.links.length === 0 ? (
              <Table.Tr><Table.Td colSpan={2}><Text c="dimmed">no links configured</Text></Table.Td></Table.Tr>
            ) : (
              cluster.links.map((l) => {
                const sameHost = placedOn.get(l.a) != null && placedOn.get(l.a) === placedOn.get(l.b);
                return (
                  <Table.Tr key={`${l.a}-${l.b}`}>
                    <Table.Td>
                      <Group gap="xs">
                        <Text>{l.a}</Text>
                        {denied.has(l.a) ? <Badge color="orange" variant="light">revoked</Badge> : null}
                        <Text>→</Text>
                        <Text>{l.b}</Text>
                        {denied.has(l.b) ? <Badge color="orange" variant="light">revoked</Badge> : null}
                      </Group>
                    </Table.Td>
                    <Table.Td>
                      {sameHost ? (
                        <Text size="sm" c="red">same host: {placedOn.get(l.a)}</Text>
                      ) : (
                        <Text size="sm" c="dimmed">
                          {placedOn.get(l.a) ?? '—'} / {placedOn.get(l.b) ?? '—'}
                        </Text>
                      )}
                    </Table.Td>
                  </Table.Tr>
                );
              })
            )}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>

      {/* Матрица смежности нода×нода: клетка отмечена, если пара есть в links. */}
      {names.length > 0 ? (
        <Table.ScrollContainer minWidth={200 + 90 * names.length}>
          <Table withTableBorder verticalSpacing="xs">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>node × node</Table.Th>
                {names.map((n) => <Table.Th key={n}>{n}</Table.Th>)}
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {names.map((row) => (
                <Table.Tr key={row} bg={denied.has(row) ? 'gray.0' : undefined}>
                  <Table.Td>
                    <Group gap="xs">
                      <Text c={denied.has(row) ? 'dimmed' : undefined}>{row}</Text>
                      {denied.has(row) ? <Badge color="orange" variant="light">revoked</Badge> : null}
                    </Group>
                  </Table.Td>
                  {names.map((col) => {
                    if (row === col) return <Table.Td key={col} c="dimmed">—</Table.Td>;
                    const linked = pairSet.has([row, col].sort().join('|'));
                    return (
                      <Table.Td key={col}>
                        {linked ? (
                          <Text c="green" style={{ textAlign: 'center' }}>●</Text>
                        ) : (
                          <Text c="gray.3" style={{ textAlign: 'center' }}>·</Text>
                        )}
                      </Table.Td>
                    );
                  })}
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      ) : (
        <Text c="dimmed">No nodes — the graph is empty (add nodes in the "Nodes" tab)</Text>
      )}

      <Text size="sm" c="dimmed">
        A full mesh is not required; nodes of one cluster live on different hosts/sites
        (geo separation is the operator's responsibility). A placed_on match between
        link ends is highlighted in the pair table.
      </Text>
    </Stack>
  );
}
