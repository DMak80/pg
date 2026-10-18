// Вкладка «Ноды» (arch/03 §12.3): таблица name/state/placed_on/placement/
// resources/usage (used/total + процент от декларации disk), бейдж «отозван»,
// кнопки Deny/Allow и колонка действий (ресурсы / убрать ноду).
import { Button, Badge, Group, Table, Text } from '@mantine/core';
import { useState } from 'react';
import type { S3ClusterDto, S3NodeDto } from '../../api/dto';
import { AllowNodeButton } from './AllowNodeButton';
import { DenyNodeButton } from './DenyNodeButton';
import { EditNodeResourcesModal } from './EditNodeResourcesModal';
import { RemoveNodeButton } from './RemoveNodeButton';

const GIB = 1024 * 1024 * 1024;

// Человекочитаемый селектор размещения: docker_host=h1 / swarm_node=n1 /
// swarm_labels: geo=dc1,tier=fast.
function placementText(node: S3NodeDto): string {
  if (node.placement === null) return '—';
  const parts: string[] = [];
  if (node.placement.dockerHost) parts.push(`docker_host=${node.placement.dockerHost}`);
  if (node.placement.swarmNode) parts.push(`swarm_node=${node.placement.swarmNode}`);
  if (node.placement.swarmLabels !== null && node.placement.swarmLabels !== undefined) {
    const labels = Object.entries(node.placement.swarmLabels)
      .map(([k, v]) => `${k}=${v}`)
      .join(',');
    if (labels !== '') parts.push(`swarm_labels: ${labels}`);
  }
  return parts.length === 0 ? '—' : parts.join(' · ');
}

function NodeStateBadge({ state }: { state: string | null }) {
  const color = state === 'RUNNING'
    ? 'green'
    : state === 'PROVISIONING'
      ? 'blue'
      : state === 'UNREACHABLE'
        ? 'red'
        : state === 'TO_REMOVE' || state === 'REMOVING'
          ? 'orange'
          : 'gray';
  return <Badge color={color} variant="light">{state ?? '—'}</Badge>;
}

export function NodesTab({ cluster, canMutate }: { cluster: S3ClusterDto; canMutate: boolean }) {
  const [resourcesNode, setResourcesNode] = useState<S3NodeDto | null>(null);

  return (
    <>
      {resourcesNode !== null ? (
        <EditNodeResourcesModal
          cluster={cluster.name}
          node={resourcesNode}
          opened
          onClose={() => setResourcesNode(null)}
        />
      ) : null}
      <Table.ScrollContainer minWidth={900}>
        <Table highlightOnHover verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Name</Table.Th>
              <Table.Th>State</Table.Th>
              <Table.Th>placed_on</Table.Th>
              <Table.Th>Placement</Table.Th>
              <Table.Th>Resources</Table.Th>
              <Table.Th>Usage</Table.Th>
              <Table.Th>Actions</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {cluster.nodesList.map((node) => (
              <NodeRow key={node.name} cluster={cluster} node={node} canMutate={canMutate}
                onEditResources={() => setResourcesNode(node)} />
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </>
  );
}

function NodeRow({ cluster, node, canMutate, onEditResources }: {
  cluster: S3ClusterDto;
  node: S3NodeDto;
  canMutate: boolean;
  onEditResources: () => void;
}) {
  // Занятость: used/total байтами + процент от total; при декларации diskGi —
  // доп. процент used/diskGi·GiB; пороговая подсветка > 85%.
  const usage = node.usage;
  const usagePercent = usage !== null ? Math.round((usage.usedBytes / usage.totalBytes) * 100) : null;
  const declaredPercent = usage !== null && node.resources?.diskGi != null
    ? Math.round((usage.usedBytes / (node.resources.diskGi * GIB)) * 100)
    : null;
  const hot = usagePercent !== null && usagePercent > 85;

  return (
    <Table.Tr>
      <Table.Td>
        <Group gap="xs">
          <Text>{node.name}</Text>
          {node.denied ? (
            <Badge color="orange" variant="light">revoked</Badge>
          ) : null}
        </Group>
      </Table.Td>
      <Table.Td><NodeStateBadge state={node.state} /></Table.Td>
      <Table.Td><Text size="sm" ff="monospace">{node.placedOn ?? '—'}</Text></Table.Td>
      <Table.Td><Text size="sm">{placementText(node)}</Text></Table.Td>
      <Table.Td>
        <Text size="sm">
          {node.resources !== null
            ? `${node.resources.cpu ?? '—'} cpu · ${node.resources.memGi ?? '—'} Gi · ${node.resources.diskGi ?? '—'} Gi`
            : '—'}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm" c={hot ? 'red' : undefined}>
          {usage !== null
            ? `${usagePercent}% · ${Math.round(usage.usedBytes / GIB)}/${Math.round(usage.totalBytes / GIB)} GB`
            : '—'}
          {declaredPercent !== null ? ` · of declared ${declaredPercent}%` : ''}
        </Text>
      </Table.Td>
      <Table.Td>
        <Group gap="xs">
          {canMutate && !node.denied ? <DenyNodeButton cluster={cluster.name} node={node.name} /> : null}
          {canMutate && node.denied ? <AllowNodeButton cluster={cluster.name} node={node.name} /> : null}
          {canMutate ? (
            <Button size="xs" variant="default" onClick={onEditResources}>Edit resources</Button>
          ) : null}
          {canMutate ? <RemoveNodeButton cluster={cluster.name} node={node.name} /> : null}
        </Group>
      </Table.Td>
    </Table.Tr>
  );
}
