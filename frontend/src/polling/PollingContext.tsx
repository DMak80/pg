// Состояние переключателя polling-интервала: Context + localStorage (spec §3.9, arch/01 §5).
import { createContext, useContext, useMemo, useState } from 'react';
import type { ReactNode } from 'react';

// Допустимые значения переключателя; 'off' — опрос выключен.
export type PollingInterval = '2' | '5' | '15' | 'off';
export const DEFAULT_POLLING: PollingInterval = '5';
export const POLLING_STORAGE_KEY = 'adminpanel.pollingInterval';

// Чтение persisted-значения с валидацией: неизвестное — default.
function readStored(): PollingInterval {
  const raw = window.localStorage.getItem(POLLING_STORAGE_KEY);
  return raw === '2' || raw === '5' || raw === '15' || raw === 'off' ? raw : DEFAULT_POLLING;
}

export interface PollingState {
  interval: PollingInterval;
  setInterval: (value: PollingInterval) => void;
}

const PollingContext = createContext<PollingState | null>(null);

// Провайдер: любое изменение пишется в localStorage (выбор переживает перезагрузку).
export function PollingProvider({ children }: { children: ReactNode }) {
  const [interval, setIntervalState] = useState<PollingInterval>(readStored);
  const value = useMemo<PollingState>(
    () => ({
      interval,
      setInterval: (next) => {
        window.localStorage.setItem(POLLING_STORAGE_KEY, next);
        setIntervalState(next);
      },
    }),
    [interval],
  );
  return <PollingContext.Provider value={value}>{children}</PollingContext.Provider>;
}

// Доступ к переключателю; использование вне провайдера — ошибка программирования.
export function usePollingInterval(): PollingState {
  const ctx = useContext(PollingContext);
  if (ctx === null) throw new Error('usePollingInterval: нет PollingProvider выше по дереву');
  return ctx;
}

// Интервал для refetchInterval TanStack Query; 'off' → false (spec §3.9).
export function usePollingIntervalMs(): number | false {
  const { interval } = usePollingInterval();
  return interval === 'off' ? false : Number(interval) * 1000;
}
