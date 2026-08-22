// Переключатель polling-интервала: 2 c / 5 c / 15 c / off (spec §3.9, §7.8).
import { SegmentedControl } from '@mantine/core';
import { usePollingInterval } from '../polling/PollingContext';
import type { PollingInterval } from '../polling/PollingContext';

export function PollingToggle() {
  const { interval, setInterval } = usePollingInterval();
  return (
    <SegmentedControl
      size="xs"
      value={interval}
      onChange={(value) => setInterval(value as PollingInterval)}
      data={[
        { value: '2', label: '2 c' },
        { value: '5', label: '5 c' },
        { value: '15', label: '15 c' },
        { value: 'off', label: 'off' },
      ]}
    />
  );
}
