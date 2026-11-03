# ValkeyWorker — оркестратор Valkey-кластеров

Уровень 2. Описание компонента, его обязанностей и перечень функций.
Уровень 3 — документы функций (`01`–`06`), уровень 4 — процессы и
перечисления в `processes/`.

## 1. Назначение

**ValkeyWorker** — фоновый сервис (.NET 10), исполнительная сторона
декларативного контракта [valkey-clusters/README.md](../valkey-clusters/README.md):
панель AdminPanel **заявляет** кластер через **HTTP API воркера**
([05-api.md](05-api.md)) — воркер **поднимает** standalone-контейнер Valkey
(`nodes=1`), обеспечивает per-cluster ACL-креды и TLS, сводит
`maxmemory` к декларации, пишет факт (`endpoints`, `nodes/node1/state`) и
снимает `state`; перевод в `TO_REMOVE` — воркер демонтирует кластер
полностью. Домен — разделяемый кеш инстансов приложения, потеря кеша
восполнима.

## 2. Обязанности и границы

- **Единственный писатель** префиксов `/valkey/` и `/valkeyworker/`
  (исключение — `/workers/api_tls/valkeyworker`: серверный серт API пишет
  панель). Панель и сиды ходят только через API воркера.
- **Единственный**, кто создаёт/удаляет контейнеры нод
  `vwk-<C>-node<k>`; per-cluster сеть не создаётся (нет inter-node трафика),
  домен не расходует пул подсетей docker.
- **Несколько инстансов работают одновременно**: координация — lease-клэймы
  в etcd (`/valkeyworker/`), takeover ≤ TTL 15 с + тик; операции
  идемпотентны, состояние переживает смерть контроллера (etcd; кеш
  восполним).
- **Не входит**: реплики/sentinel/cluster-топологии (кеш восполним,
  шардирование не нужно); панель valkey-домена; клиентская библиотека
  Puzzle; persistence RDB/AOF — off по канону (кеш восполним); квоты томов —
  томов нет.

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Модель размещения и нода | контейнер и аргументы, ACL/TLS/maxmemory, отсутствие per-cluster сети, portalloc, лимиты, два инстанса | [01-placement.md](01-placement.md) |
| Жизненный цикл кластера | создание, демонтаж, надзор ноды | [02-cluster-lifecycle.md](02-cluster-lifecycle.md) |
| Конфигурация и ACL | converge `maxmemory`/`maxmemory_policy` и ACL-плана без рестартов | [03-configuration.md](03-configuration.md) |
| Безопасность и ротации | per-cluster CA и серты, TLS, ACL-креды, ротации паролей и CA, TLS-миграция | [04-security.md](04-security.md) |
| Внешний HTTP API | приём мутаций панели и сида, mTLS, дискавери, restart, `GET /api/config` | [05-api.md](05-api.md) |
| Эксплуатация | классификация тика, надёжность, наблюдаемость, конфигурация, риски | [06-operability.md](06-operability.md) |

Точные автоматы состояний — в процессах уровня 4, на которые ссылается
каждая функция; классификация тика и перечисления —
[06-operability.md](06-operability.md) §1.

## 4. Взаимодействие

- **AdminPanel → ValkeyWorker**: только HTTP API (`/api`, mTLS) —
  [05-api.md](05-api.md), контракт мутаций —
  [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
  (глава Valkey).
- **ValkeyWorker → etcd**: единственный источник состояния домена
  (`/valkey/`, `/valkeyworker/`) — контракт ключей
  [valkey-clusters/README.md](../valkey-clusters/README.md).
- **ValkeyWorker → docker / Valkey**: управление контейнерами и RESP-команды
  (PING, CONFIG, ACL) — только по TLS; границы оркестрации
  [25-component-communications.md](../25-component-communications.md).
- **Приложения → etcd**: клиентский дискавери (`endpoints`, `ca_pem`,
  `app_user`/`app_password`) —
  [valkey-clusters/04-client-discovery.md](../valkey-clusters/04-client-discovery.md) §1.
- **Prometheus → ValkeyWorker**: `/metrics` на порту Kestrel `:8080` —
  [18-metrics.md](../18-metrics.md).

## 5. Свойства, на которые опираются все функции

- Идемпотентность каждого шага: повторная сверка факта (контейнер есть?
  PING по TLS отвечает? конфиг == декларации? ACL-план == канону? env серта
  валиден?) — [06-operability.md](06-operability.md) §2.
- Journal-before-manipulations, фиксированный порядок записей, клэймы и
  txn-сравнения — [17-synchronization-principles.md](../17-synchronization-principles.md)
  (S5–S7), [06-operability.md](06-operability.md) §1.
- Декларативная модель: отсутствие `config.state` = Active-кластер;
  `NOT_INITIALIZED`/`TO_REMOVE` — заявки панели; состояния ноды и заявок —
  [06-operability.md](06-operability.md) §1.
- Persistence off: потеря контейнера = холодный старт кеша
  (документированное поведение), томов данных нет.
