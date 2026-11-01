// EventsProvider (u09, механика A5): подписка на query-кэш TanStack Query —
// при успехе опроса наблюдаемых ключей дифф prev(снимок прошлого тика)→next.
// Смонтирован внутри AppLayout (после guard'а сессии) — живёт только в
// открытой вкладке; перезагрузка = пустая лента.
// Отказоустойчивость: дифф обёрнут в catch — исключение одного диффа (дрейф
// DTO, ответ не по форме) НЕ вылетает в batch-нотификацию TanStack (не рвёт
// notify-цикл остальных подписчиков) и НЕ портит ленту молча: счётчик ошибок
// диффа читается страницей /events (индикация «лента неполна»).
import { useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';
import type { Query } from '@tanstack/react-query';
import {
  diffBackupsStorage, diffClusterDetails, diffClusterExtras, diffClusters,
  diffEtcdDomain, diffHa, diffKafka, diffOverview, diffS3, diffValkey,
} from './eventsDiff';

// Снимки prev по queryKey (строковый ключ) — храним сами: у SuccessAction в
// TanStack Query v5 НЕТ поля action.data.previous (уточнение re-плана).
// Чистка: ключ удалён из кэша → снимок удаляем (не удерживаем полные ответы
// всех посещённых страниц за сессию — переживали бы logout).
const prevSnapshots = new Map<string, unknown>();

// Ключ детального запроса = корень + имя (['clusters', 'demo']).
function keyId(key: readonly unknown[]): string {
  return JSON.stringify(key);
}

// Детальные корни: ['clusters', name] / ['kafka-clusters', name] / ...
const DETAIL_ROOTS = new Set(['clusters', 'kafka-clusters', 'valkey-clusters', 's3-clusters']);

// Счётчик ошибок диффа ( diagnosability ): доступен /events через
// getDiffErrorCount() — «лента неполна» при ненулевом.
let diffErrorCount = 0;

export function getDiffErrorCount(): number {
  return diffErrorCount;
}

export function EventsProvider({ children }: { children: React.ReactNode }) {
  const queryClient = useQueryClient();

  useEffect(() => {
    const cache = queryClient.getQueryCache();
    const unsubscribe = cache.subscribe((event) => {
      // Чистка снимков удалённых из кэша ключей (запрос снят с монтирования,
      // logout/clear): снимок полного ответа больше не нужен — память не
      // растёт по числу посещённых страниц за сессию (#29).
      if (event.type === 'removed') {
        prevSnapshots.delete(keyId(event.query.queryKey));
        return;
      }
      if (event.type !== 'updated' || event.action.type !== 'success') return;
      const query: Query | undefined = event.query;
      if (query === undefined) return;
      const key = query.queryKey;
      const root = String(key[0]);
      const next = query.state.data;
      if (next === undefined) return;
      const id = keyId(key);
      const prev = prevSnapshots.get(id);

      try {
        if (id === keyId(['clusters'])) {
          diffClusters(prev as never, next as never);
        } else if (id === keyId(['ha-scopes'])) {
          diffHa(prev as never, next as never);
        } else if (id === keyId(['etcd-domain'])) {
          // etcd-домен (02 §2.3.5): состав контура/DR-слепки/декларация —
          // события, пока запрос etcdDomain опрашивается (страница /etcd).
          diffEtcdDomain(prev as never, next as never);
        } else if (id === keyId(['overview'])) {
          // arch/03 §3 «События» перечисляет overview среди наблюдаемых —
          // диффим переключение глобального стопа бэкапов (u10).
          diffOverview(prev as never, next as never);
        } else if (root === 'backups-storage'
          && (key.length === 1
            || (key.length === 2 && typeof key[1] === 'string'))) {
          // restore/drill факты шардов + сироты из ответа backups-storage:
          // полный ключ ['backups-storage'] (страница грани) и cluster-scoped
          // ['backups-storage', <C>] (?cluster= — вкладка «Бэкапы (срез)»,
          // формы деталей шарда) — иначе события restore/drill/сирот при
          // открытой странице кластера пропадали бы из ленты.
          diffBackupsStorage(prev as never, next as never);
        } else if (DETAIL_ROOTS.has(root) && key.length === 2 && typeof key[1] === 'string') {
          if (root === 'clusters') {
            diffClusterDetails(prev as never, next as never);
            diffClusterExtras(prev as never, next as never);
          } else if (root === 'kafka-clusters') {
            diffKafka(prev as never, next as never);
          } else if (root === 'valkey-clusters') {
            diffValkey(prev as never, next as never);
          } else if (root === 's3-clusters') {
            diffS3(prev as never, next as never);
          }
        }
      } catch (e) {
        // Не выпускаем исключение из слушателя: batch-notify TanStack обходит
        // остальных подписчиков через forEach без per-listener try — вылет
        // здесь рвал notify-цикл на тике. Причина — в консоли браузера.
        diffErrorCount += 1;
        console.error('[events] diff failed for query key', key, e);
      } finally {
        // Снимок next для следующего тика (храним сами — A5-уточнение).
        prevSnapshots.set(id, next);
      }
    });

    return unsubscribe;
  }, [queryClient]);

  return children;
}
