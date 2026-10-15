// Модалка «Добавить ноду» (arch/03 §12.3; 02 §12.2-3): имя + placement +
// ресурсы; валидация-зеркало 02 §12.3; 409 «инстанс уже существует» — текстом.
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Alert,
  Button,
  Group,
  Modal,
  NumberInput,
  Radio,
  Stack,
  TextInput,
} from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../../api/client';
import { addS3Node, s3QueryKeys } from '../../api/queries';
import type { S3PlacementDto } from '../../api/dto';

const NAME_RE = /^[a-z][a-z0-9_]{0,62}$/;
const RESERVED_NAME = 'orchestrator';

type PlacementMode = 'dockerHost' | 'swarmNode';

export function AddNodeModal({
  cluster,
  opened,
  onClose,
}: {
  cluster: string;
  opened: boolean;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [name, setName] = useState('');
  const [mode, setMode] = useState<PlacementMode>('dockerHost');
  const [dockerHost, setDockerHost] = useState('');
  const [swarmNode, setSwarmNode] = useState('');
  const [cpu, setCpu] = useState('');
  const [memGi, setMemGi] = useState('');
  const [diskGi, setDiskGi] = useState('');
  const [error, setError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: () =>
      addS3Node(cluster, {
        name,
        placement: placement(),
        resources: resources(),
      }),
    onSuccess: async () => {
      onClose();
      await queryClient.invalidateQueries({ queryKey: s3QueryKeys.cluster(cluster) });
    },
    onError: (e) => setError(e instanceof ApiError ? e.message : String(e)),
  });

  function placement(): S3PlacementDto {
    return mode === 'dockerHost' ? { dockerHost } : { swarmNode };
  }

  function resources() {
    if (cpu === '' && memGi === '' && diskGi === '') return null;
    return {
      cpu: cpu === '' ? null : Number(cpu),
      memGi: memGi === '' ? null : Number(memGi),
      diskGi: diskGi === '' ? null : Number(diskGi),
    };
  }

  function submit() {
    // Валидация-зеркало 02 §12.3.
    if (!NAME_RE.test(name)) return void setError('имя: [a-z][a-z0-9_]{0,62}');
    if (name === RESERVED_NAME) return void setError('имя orchestrator зарезервировано');
    if (mode === 'dockerHost' && dockerHost.trim() === '') return void setError('укажите docker_host');
    if (mode === 'swarmNode' && swarmNode.trim() === '') return void setError('укажите swarm_node');
    if (cpu !== '' && (Number(cpu) < 0.01 || Number(cpu) > 64)) return void setError('cpu 0.01..64');
    if (memGi !== '' && (!Number.isInteger(Number(memGi)) || Number(memGi) < 1 || Number(memGi) > 65536))
      return void setError('memGi — целое 1..65536');
    if (diskGi !== '' && (!Number.isInteger(Number(diskGi)) || Number(diskGi) < 1 || Number(diskGi) > 65536))
      return void setError('diskGi — целое 1..65536');
    setError(null);
    mutation.mutate();
  }

  return (
    <Modal opened={opened} onClose={onClose} title={`Добавить ноду — ${cluster}`} centered>
      <Stack gap="sm">
        <TextInput
          label="Имя инстанса"
          placeholder="node2"
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
        />
        <Radio.Group label="Размещение (placement)" value={mode}
          onChange={(v) => setMode(v as PlacementMode)}>
          <Group mt="xs">
            <Radio value="dockerHost" label="docker_host" />
            <Radio value="swarmNode" label="swarm_node" />
          </Group>
        </Radio.Group>
        {mode === 'dockerHost' ? (
          <TextInput label="docker_host" placeholder="h2" value={dockerHost}
            onChange={(e) => setDockerHost(e.currentTarget.value)} />
        ) : (
          <TextInput label="swarm_node" placeholder="n1" value={swarmNode}
            onChange={(e) => setSwarmNode(e.currentTarget.value)} />
        )}
        <Group grow>
          <NumberInput label="CPU" placeholder="0.5" value={cpu || ''} step={0.1}
            min={0.01} max={64} onChange={(v) => setCpu(v === undefined ? '' : String(v))} />
          <NumberInput label="Память, GiB" placeholder="4" value={memGi || ''}
            min={1} onChange={(v) => setMemGi(v === undefined ? '' : String(v))} />
          <NumberInput label="Диск, GiB" placeholder="200" value={diskGi || ''}
            min={1} onChange={(v) => setDiskGi(v === undefined ? '' : String(v))} />
        </Group>
        {error !== null ? <Alert color="red" variant="light">{error}</Alert> : null}
        {mutation.error instanceof ApiError ? (
          <Alert color="red" variant="light">{mutation.error.message}</Alert>
        ) : null}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={mutation.isPending} onClick={submit}>Добавить</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
