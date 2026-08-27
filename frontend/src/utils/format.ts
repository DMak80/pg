// Возраст данных в человекочитаемом виде (spec §7.10): «12 с», «3 мин 5 с», «62 мин».
export function formatAge(ms: number): string {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  if (totalSeconds < 60) return `${totalSeconds} с`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return seconds === 0 ? `${minutes} мин` : `${minutes} мин ${seconds} с`;
  return `${minutes} мин`;
}

// Размер в байтах → «823 Б», «20.0 КБ», «4.1 МБ» (t08 spec §4.16); null → «—».
export function formatBytes(bytes: number | null): string {
  if (bytes === null) return '—';
  if (bytes < 1024) return `${bytes} Б`;
  const units = ['КБ', 'МБ', 'ГБ', 'ТБ'];
  let value = bytes;
  let unitIndex = -1;
  do {
    value /= 1024;
    unitIndex += 1;
  } while (value >= 1024 && unitIndex < units.length - 1);
  return `${value.toFixed(1)} ${units[unitIndex]}`;
}

// Кэш форматтера локального времени: один экземпляр на модуль (t08 spec §4.16).
const dateTimeFormatter = new Intl.DateTimeFormat('ru-RU', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

// Unix-секунды → локальная дата-время «22.08.2026, 14:03:05» (t08 spec §4.16); null → «—».
export function formatUnix(unix: number | null): string {
  return unix === null ? '—' : dateTimeFormatter.format(new Date(unix * 1000));
}

// ISO-строка (DateTimeOffset) → локальная дата-время — для lastRefreshUtc (t08 spec §5).
export function formatIso(iso: string | null): string {
  return iso === null ? '—' : dateTimeFormatter.format(new Date(iso));
}

// Относительный возраст от Unix-штампа: «12 с», «3 мин 5 с» (t08 spec §4.16); null → «—».
export function formatUnixAge(unix: number | null): string {
  return unix === null ? '—' : formatAge(Date.now() - unix * 1000);
}

// Относительный возраст от ISO-штампа (DateTimeOffset-строка) — для probeAtUtc
// (t09 spec §4.16); null → «—».
export function formatIsoAge(iso: string | null): string {
  return iso === null ? '—' : formatAge(Date.now() - Date.parse(iso));
}
