# Телеметрия: тестирование

Уровень 3. Канон уровня тестирования телеметрии. Родитель —
[README.md](README.md); соседние документы — [01-framework.md](01-framework.md),
[02-dictionary.md](02-dictionary.md), [03-exposure.md](03-exposure.md),
[04-collectors.md](04-collectors.md), [05-storage.md](05-storage.md),
[07-risks.md](07-risks.md).

- **Unit**: WorkerMetrics — семантика инструментов (счёт тиков по `ok`,
  сброс серий фаз, возраст снапшота) через тестовый MeterListener/коллектор.
- **Unit**: valkey-коллектор — парсер INFO, затирание ушедших нод
  UpdateCluster, консервативный LastSuccess, пропуски не-Active/без кредов/без
  portalloc, дефолт интервала `<=0` → 30 (фейк `IValkeyConnection`).
- **Integration**: `WebApplicationFactory` — `/metrics` отвечает 200
  text-format, содержит канонические имена
  [02-dictionary.md](02-dictionary.md) (фиксирует фактические
  экспортированные имена против словаря); `/metrics` — открытая грань,
  отвечает без авторизации ([03-exposure.md](03-exposure.md)); valkey — все 15 имён
  [02-dictionary.md](02-dictionary.md) §6 при живом демо-кластере (live-WAF
  с реальным docker), живая/остановленная нода — LastSuccess стоит, тик не падает.
- **Unit (file_sd-генератор [05-storage.md](05-storage.md) §4)**: маппинг portalloc → file_sd-группы
  (лейблы cluster/shard/node, `targets = alias:8008`, деградационно
  `host:patroni` — per-node правило [02-dictionary.md](02-dictionary.md) §5, пропуск `patroni ≤ 0`,
  битых записей и чужих префиксов, пустой префикс → `[]`, детерминизм порядка),
  атомарная запись при diff, консервативная свежесть цикла, дефолт интервала
  `<=0` → 15.
- **Integration (file_sd-генератор [05-storage.md](05-storage.md) §4)**: цикл генератора с живым etcd
  (testcontainers, динамический порт) — put portalloc → таргет в файле,
  del → таргет исчез, недоступный etcd — файл и `last_success` не изменились,
  восстановление → догоняет; hosted-сервис освежает файл по интервалу.
- **docker-E2E (file_sd-генератор [05-storage.md](05-storage.md) §4)**: реальный кластер PgWorker в
  E2E-контуре единой сети окружения класса (воркер с
  `PgWorker__Docker__ScrapeNetwork` = сеть окружения; ноды attached —
  inspect подтверждает membership) + настоящий Prometheus — таргеты
  `patroni-nodes` up по `alias:8008`, в `scrapeUrl` нет
  `host.docker.internal`, канон-минимум
  нативных `patroni_*` [02-dictionary.md](02-dictionary.md) §5 в TSDB (фактический набор фиксирует тест),
  демонтаж кластера → таргеты исчезли; контур полностью зачищен teardown'ом.
- **docker-E2E (дефолт поставки)**: маркер `Scale_AddEmptyShard` на свежем
  Release — конфиг без ключа `ScrapeNetwork` (attach нет) не ломает
  провижининг/кластерные пути.
- **E2E-чек стенда**: `checks/65-metrics.sh` — профиль `metrics` поднят,
  все scrape-джобы `up` (таргеты pgworker — сетевые `pgworker:8080`/
  `pgworker-2:8080`, extra_hosts в as-prometheus отсутствуют; patroni/
  patroni-nodes — по https), дашборды
  загружены, алерт-рулы зарегистрированы
  (`/api/v1/rules`, счётчик включает группу `backups`
  [02-dictionary.md](02-dictionary.md) §7), Alertmanager жив; 00-up/90-down не оставляют осиротевшей сети
  `pgw-metrics`. Приёмка REST-TLS — docker-E2E REST-TLS
  (E2ePatroniRestTlsScenarios) + https-ассерты docker-E2E file_sd
  (таргеты `patroni-nodes` up по https-скрейпу).
