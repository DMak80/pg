// Цветовая карта состояний бакета — единый источник всех панелей (t08 spec §4.17).
import { Badge, Tooltip } from '@mantine/core';
import type { BucketStateName } from '../api/dto';

// Подпись — русская; каноническое значение — в Tooltip.
const STATE_META: Record<BucketStateName, { color: string; label: string }> = {
  ACTIVE: { color: 'teal', label: 'активен' },
  SYNCING: { color: 'blue', label: 'синхронизация' },
  FROZEN: { color: 'yellow', label: 'заморожен' },
  ABORTING: { color: 'red', label: 'отменяется' },
  NOT_INITIALIZED: { color: 'gray', label: 'не инициализирован' },
};

export function BucketStateBadge({ state }: { state: BucketStateName }) {
  const meta = STATE_META[state];
  return (
    <Tooltip label={state}>
      <Badge color={meta.color} variant="light">{meta.label}</Badge>
    </Tooltip>
  );
}
