# KafkaWorker — оркестратор Kafka-кластеров

Уровень 2. Описание компонента, его обязанностей и перечень функций.
Уровень 3 — документы функций (`01`–`08`), уровень 4 — процессы и
перечисления в `processes/`.

## 1. Назначение

**KafkaWorker** — фоновый сервис (.NET 10), который по состоянию в etcd
управляет жизненным циклом Kafka-кластеров через docker (plain / docker
swarm). Это исполнительная сторона декларативного контракта
[kafka-clusters/README.md](../kafka-clusters/README.md): панель AdminPanel
**заявляет** кластер и изменения через **HTTP API воркера**
([07-api.md](07-api.md)) — воркер **поднимает** KRaft-кластер, обеспечивает
per-cluster SASL-креды (роли admin/app и per-cluster CA), пишет факт
(`endpoints`, состояния брокеров, реестр топиков) и снимает `state`;
перевод в `TO_REMOVE` — воркер демонтирует кластер полностью.

## 2. Обязанности и границы

- **Единственный писатель** префиксов `/kafka/` и `/kafkaworker/`
  (исключение — `/workers/api_tls/kafkaworker`: серверный серт API пишет
  панель). Панель и сиды ходят только через API воркера.
- **Единственный**, кто создаёт/удаляет контейнеры брокеров, сеть
  `kfw-net-<C>` и тома; фактическое состояние docker сверяет инспекцией,
  состояние системы — в etcd.
- **Несколько инстансов работают одновременно**: координация — lease-клэймы
  в etcd (`/kafkaworker/`), смерть контролирующего инстанса не роняет
  процессы (takeover ≤ TTL 15 с + тик); операции идемпотентны, состояние
  переживает смерть контроллера (etcd + тома брокеров).
- **Не входит**: бэкапы Kafka (их нет; RPO держится RF×minISR —
  [kafka-clusters/README.md](../kafka-clusters/README.md)), метрики самих брокеров
  (JMX-exporter, лаги/USR снимает коллектор —
  [18-metrics.md](../18-metrics.md) §4), bandwidth-throttle reassignment,
  preferred leader election, клиентская библиотека дискавери —
  [roadmap/kafkaworker.md](../roadmap/kafkaworker.md). Ротация per-cluster CA и сертов брокеров — входит
  ([04-security.md](04-security.md)).

## 3. Функции

| Функция | Содержание | Документ |
|---|---|---|
| Модель размещения и узлы кластера | контейнер `apache/kafka`, KRaft-роли, listeners и сеть, env-набор, portalloc, режимы Plain/Swarm | [01-placement.md](01-placement.md) |
| Жизненный цикл кластера | создание, демонтаж, надзор нод, конвергенция конфигурации Active-кластера | [02-cluster-lifecycle.md](02-cluster-lifecycle.md) |
| Масштабирование брокеров | добавление, снятие и rolling-регенерация ресурсов | [03-broker-scaling.md](03-broker-scaling.md) |
| Безопасность и ротации | per-cluster CA и серты, SASL/ACL, секреты, ротации паролей и CA, миграция премиграционных кластеров | [04-security.md](04-security.md) |
| Топики | автосинк реестра, desired-заявки, lifecycle-создание/удаление | [05-topics.md](05-topics.md) |
| Ребалансировка партиций | drain демонтируемых брокеров и balance по заявке панели | [06-rebalancing.md](06-rebalancing.md) |
| Внешний HTTP API | приём мутаций панели и сида, mTLS, дискавери, restart, `GET /api/config` | [07-api.md](07-api.md) |
| Эксплуатация | классификация тика, надёжность, наблюдаемость, конфигурация, риски | [08-operability.md](08-operability.md) |

Точные автоматы состояний — в процессах уровня 4, на которые ссылается
каждая функция; классификация тика и перечисления —
[08-operability.md](08-operability.md) §1.

## 4. Взаимодействие

- **AdminPanel → KafkaWorker**: только HTTP API (`/api`, mTLS) —
  [07-api.md](07-api.md), контракт мутаций —
  [adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md)
  (глава Kafka).
- **KafkaWorker → etcd**: единственный источник состояния домена
  (`/kafka/`, `/kafkaworker/`) — контракт ключей
  [kafka-clusters/README.md](../kafka-clusters/README.md).
- **KafkaWorker → docker / Kafka**: управление контейнерами и Kafka-протокол
  (AdminClient, протокольный миниклиент reassignment) — только штатные
  протоколы, границы оркестрации
  [25-component-communications.md](../25-component-communications.md).
- **Приложения → etcd**: клиентский дискавери (`endpoints`, `ca_pem`,
  `app_user`/`app_password`, реестр `topics/`) —
  [kafka-clusters/05-client-discovery.md](../kafka-clusters/05-client-discovery.md).
- **Prometheus → KafkaWorker**: `/metrics` на порту Kestrel `:8080` —
  [18-metrics.md](../18-metrics.md).

## 5. Свойства, на которые опираются все функции

- Идемпотентность каждого шага: повторная сверка факта (контейнер есть?
  брокер в кластере? заявка уже применена?) — [08-operability.md](08-operability.md) §2.
- Journal-before-manipulations, фиксированный порядок записей, клэймы и
  txn-сравнения — [17-synchronization-principles.md](../17-synchronization-principles.md)
  (S5–S7), [08-operability.md](08-operability.md) §2.
- Декларативная модель: отсутствие `config.state` = Active-кластер;
  `NOT_INITIALIZED`/`TO_REMOVE` — заявки панели; состояния брокеров и
  заявок — [08-operability.md](08-operability.md) §1.
- Все Kafka-контакты воркера — `admin`-креды и `ca_pem` по CLIENT-endpoints
  из etcd (SASL_SSL), см. [04-security.md](04-security.md) §4.
