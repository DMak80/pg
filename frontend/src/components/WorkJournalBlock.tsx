// Блок «Что делает воркер» (u13): work-журнал домена — op/phase/instance/
// updated/last_error (arch/03 §3.2-вкладка «Журнал»). Журналы воркеров
// перезаписываются — показывается последняя фаза.
import { Table, Text } from '@mantine/core';
import type { ReactNode } from 'react';
import { formatUnix } from '../utils/format';
import type { ClusterWorkDto } from '../api/dto';

export function WorkJournalBlock({ work }: { work: ClusterWorkDto | null | undefined }) {
  if (!work)
    return <Text size="sm" c="dimmed">Журнала нет (воркер ничего не делал или домен не тикал)</Text>;
  return (
    <Table highlightOnHover>
      <Table.Tbody>
        <Row label="op"><Text ff="monospace" size="sm">{work.op}</Text></Row>
        <Row label="phase"><Text ff="monospace" size="sm">{work.phase}</Text></Row>
        <Row label="updated">{formatUnix(work.updatedUnix)}</Row>
        <Row label="last_error">
          {work.lastError
            ? <Text size="sm" c="red" ff="monospace">{work.lastError}</Text>
            : <Text size="sm" c="dimmed">—</Text>}
        </Row>
      </Table.Tbody>
    </Table>
  );
}

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Table.Tr>
      <Table.Td w={140}><Text size="xs" c="dimmed" tt="uppercase">{label}</Text></Table.Td>
      <Table.Td>{children}</Table.Td>
    </Table.Tr>
  );
}
