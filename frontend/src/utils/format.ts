// Возраст данных в человекочитаемом виде (spec §7.10): «12 с», «3 мин 5 с», «62 мин».
export function formatAge(ms: number): string {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  if (totalSeconds < 60) return `${totalSeconds} с`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return seconds === 0 ? `${minutes} мин` : `${minutes} мин ${seconds} с`;
  return `${minutes} мин`;
}
