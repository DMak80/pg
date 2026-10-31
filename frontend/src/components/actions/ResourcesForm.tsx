// Общая форма ресурсов (u02): модал cpu/memGi/diskGi с валидацией из единого
// модуля validation.ts (LIMITS) + подпись способа применения (rolling или
// пересоздание). Заменяет доменные копии EditBrokerResourcesModal/
// EditNodeResourcesModal ×2 — submit-мутацию передаёт вызывающая сторона.
import { Button, Group, Modal, NumberInput, Stack, Text } from '@mantine/core';
import { useState } from 'react';
import { validateCpu, validateDiskGi, validateMemGi } from '../../utils/validation';
import { ActionErrorAlert } from './ConfirmModal';

export interface ResourcesValue {
  cpu?: number;
  memGi?: number;
  diskGi?: number;
}

export function ResourcesForm({
  title,
  initial,
  applyNote,
  opened,
  onClose,
  onSubmit,
  error,
}: {
  title: string;
  initial: ResourcesValue;
  // Подпись способа применения (rolling/пересоздание) — arch/03 §3.x.
  applyNote: string;
  opened: boolean;
  onClose: () => void;
  onSubmit: (v: ResourcesValue) => Promise<unknown>;
  error?: unknown;
}) {
  const [cpu, setCpu] = useState<number | undefined>(initial.cpu);
  const [memGi, setMemGi] = useState<number | undefined>(initial.memGi);
  const [diskGi, setDiskGi] = useState<number | undefined>(initial.diskGi);
  const [busy, setBusy] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);

  const validationError =
    cpu !== undefined ? validateCpu(cpu)
      : memGi !== undefined ? validateMemGi(memGi)
        : diskGi !== undefined ? validateDiskGi(diskGi)
          : null;

  async function submit(): Promise<void> {
    const invalid =
      cpu !== undefined ? validateCpu(cpu)
        : memGi !== undefined ? validateMemGi(memGi)
          : diskGi !== undefined ? validateDiskGi(diskGi)
            : 'Хотя бы одно поле (cpu/mem/disk) обязательно';
    if (invalid !== null) {
      setLocalError(invalid);
      return;
    }
    setLocalError(null);
    setBusy(true);
    try {
      await onSubmit({ cpu, memGi, diskGi });
      onClose();
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal opened={opened} onClose={onClose} title={title} centered>
      <Stack gap="sm">
        <NumberInput
          label="CPU (ядра, 0.01..64)"
          value={cpu}
          step={0.1}
          onChange={(v) => setCpu(v === '' ? undefined : Number(v ?? 0))}
        />
        <NumberInput
          label="Память (GiB, 1..65536)"
          value={memGi}
          onChange={(v) => setMemGi(v === '' ? undefined : Number(v ?? 0))}
        />
        <NumberInput
          label="Диск (GiB, 1..65536)"
          value={diskGi}
          onChange={(v) => setDiskGi(v === '' ? undefined : Number(v ?? 0))}
        />
        <Text size="sm" c="dimmed">{applyNote}</Text>
        {validationError !== null ? <Text size="sm" c="red">{validationError}</Text> : null}
        {localError !== null ? <Text size="sm" c="red">{localError}</Text> : null}
        <ActionErrorAlert error={error} />
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>Отмена</Button>
          <Button loading={busy} onClick={() => void submit()}>Применить</Button>
        </Group>
      </Stack>
    </Modal>
  );
}
