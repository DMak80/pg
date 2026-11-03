# KafkaWorker: жизненный цикл кластера

Уровень 3. Функция «жизненный цикл»: создание кластера, демонтаж, надзор нод
и конвергенция конфигурации Active-кластера. Полные автоматы состояний и
переходов — процессы [A](processes/a-provisioning.md),
[B](processes/b-deprovisioning.md), [C](processes/c-node-supervisor.md),
[E](processes/e-config-converger.md); перечисления —
[processes/enums.md](processes/enums.md).

## 1. Состояния кластера

- `config.state=NOT_INITIALIZED` — заявка панели на создание; `TO_REMOVE` —
  заявка на полный демонтаж; отсутствие `state` — Active-кластер. Семантика
  полей `config` — [kafka-clusters/02-keys.md](../kafka-clusters/02-keys.md) §1.
- Состояния брокеров и их владельцы — [processes/enums.md](processes/enums.md);
  процессы не трогают ноды `TO_REMOVE`/`REMOVING`/`PROVISIONING` чужих
  процессов.

## 2. Создание кластера (A)

От `NOT_INITIALIZED` до рабочего кластера: план размещения и ролей
(`broker1..m` — controller, `m=min(3,B)`; роль фиксируется навсегда), ensure
секретов кластера, создание томов и контейнеров, ожидание готовности,
стартовый converge конфигурации и ACL, публикация `endpoints` и снятие
`config.state`. Порты — под глобальным portalloc-клэймом; повтор процесса
идемпотентен; `TO_REMOVE`, пришедший посреди provisioning, безопасно
прекращает процесс.

Идемпотентность, точные фазы K0–K6 и условия ожиданий —
[A. ProvisioningProcess](processes/a-provisioning.md).

## 3. Демонтаж кластера (B)

От `TO_REMOVE` до чистого etcd и удалённых docker-объектов; порядок —
«сначала docker, потом etcd». Удаляются контейнеры/тома `kfw-<C>-*`,
префикс `/kafka/clusters/<C>/` и вся координация `/kafkaworker/…/<C>`
(клэймы, журнал, portalloc, заявки, прогресс, исходы) — заявки не переживают
удаление кластера.

Точные шаги X0–X3 — [B. DeprovisioningProcess](processes/b-deprovisioning.md).

## 4. Надзор нод (C)

Постоянная сверка декларации с фактом docker и живой пробой кластера:
снесённый контейнер пересоздаётся, молчащий брокер помечается и
пересоздаётся, адреса восстанавливаются из portalloc. Инварианты:
**данные неприкосновенны** (том сохраняется всегда, кроме доказанной
физической утраты), слепота воркера не считается смертью брокеров, одно
пересоздание за тик, чужие состояния не трогаются.

Правила проб, порогов и восстановления адресов —
[C. NodeSupervisor](processes/c-node-supervisor.md); backoff недоступного
кластера — [08-operability.md](08-operability.md) §3.

## 5. Конвергенция конфигурации Active-кластера (E)

Dynamic broker configs и ACL роли app сводятся к декларации без рестартов:
расхождение по управляемым полям применяется идемпотентным Set; ACL живут в
KRaft-метаданных. Фактические топики не трогаются — только desired-заявками
([05-topics.md](05-topics.md)).

Маппинг полей и ACL-план — [E. ClusterConfigConverger](processes/e-config-converger.md).
