// EventsProvider (u09, механика A5): подписка на query-кэш TanStack Query —
// при успехе опроса наблюдаемых ключей снимает prev из кэша ДО применения
// (кэш уже обновлён — prev берём из event#query.state.data предыдущего
// снимка, который TanStack кладёт в dataPrevious...) — фактически дифф
// prev(текущий кэш)→next(новые данные), снимок храним сами в Map по queryKey.
// Смонтирован внутри AppLayout (после guard'а сессии) — живёт только в
// открытой вкладке; перезагрузка = пустая лента.
import { useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';
import type { Query } from '@tanstack/react-query';
import {
  diffBackupsStorage, diffClusterDetails, diffClusterExtras, diffClusters,
  diffHa, diffKafka, diffOverview, diffS3, diffValkey,
} from './eventsDiff';
import { backupsQueryKeys } from '../api/queries';

// Снимки prev по queryKey (строковый ключ) — храним сами: у SuccessAction в
// TanStack Query v5 НЕТ поля action.data.previous (уточнение re-плана).
const prevSnapshots = new Map<string, unknown>();

function keyId(key: readonly unknown[]): string {
  return JSON.stringify(key);
}

const WATCHED = [
  keyId(['clusters']),
  keyId(['ha-scopes']),
  keyId(['overview']),
  keyId(backupsQueryKeys.storage),
] as const;

// Детальные ключи: ['clusters', name] / ['kafka-clusters', name] / ... —
// обрабатываем динамически (первый сегмент в списке доменов).

const DETAIL_ROOTS = new Set(['clusters', 'kafka-clusters', 'valkey-clusters', 's3-clusters']);

export function EventsProvider({ children }: { children: React.ReactNode }) {
  const queryClient = useQueryClient();

  useEffect(() => {
    const unsubscribe = queryClient.getQueryCache().subscribe((event) => {
      if (event.type !== 'updated' || event.action.type !== 'success') return;
      const query: Query | undefined = event.query;
      if (query === undefined) return;
      const key = query.queryKey;
      const root = String(key[0]);
      const next = query.state.data;
      if (next === undefined) return;
      const prev = prevSnapshots.get(keyId(key));

      try {
        if (keyId(key) === keyId(['clusters'])) {
          diffClusters(prev as never, next as never);
        } else if (keyId(key) === keyId(['ha-scopes'])) {
          diffHa(prev as never, next as never);
        } else if (keyId(key) === keyId(['overview'])) {
          // FINDING 1: arch/03 §3 «События» перечисляет overview среди
          // наблюдаемых — диффим переключение глобального стопа бэкапов (u10).
          diffOverview(prev as never, next as never);
        } else if (keyId(key) === keyId(backupsQueryKeys.storage)) {
          // FINDING 1: restore/drill факты шардов + сироты из ответа
          // backups-storage (arch/03 §3 «События»).
          diffBackupsStorage(prev as never, next as never);
        } else if (DETAIL_ROOTS.has(root) && key.length === 2 && typeof key[1] === 'string') {
          const name = key[1];
          if (root === 'clusters') {
            diffClusterDetails(prev as never, next as never);
            diffClusterExtras(prev as never, next as never);
            void name; // имя кластера внутри DTO
          } else if (root === 'kafka-clusters') {
            diffKafka(prev as never, next as never);
          } else if (root === 'valkey-clusters') {
            diffValkey(prev as never, next as never);
          } else if (root === 's3-clusters') {
            diffS3(prev as never, next as never);
          }
        }
      } finally {
        // Снимок next для следующего тика (храним сами — A5-уточнение).
        prevSnapshots.set(keyId(key), next);
      }
    });
    return unsubscribe;
  }, [queryClient]);

  return children;
}

// WATCHED не используется напрямую (домены обрабатываются в подписке) —
// оставлен как документация наблюдаемых корней списка.
void WATCHED;
