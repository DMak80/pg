# S3Worker: жизненный цикл кластера

Уровень 3. Функция «жизненный цикл»: создание кластера, демонтаж, надзор
инстансов и bucket'ы инстансов. Полные автоматы состояний и переходов —
процессы [A](processes/a-provisioning.md), [B](processes/b-deprovisioning.md),
[C](processes/c-node-supervisor.md); перечисления —
[processes/enums.md](processes/enums.md).

## 1. Состояния кластера и инстанса

- `config.state=NOT_INITIALIZED` — заявка панели на создание; `TO_REMOVE` —
  заявка на полный демонтаж; отсутствие `state` — Active-кластер; незнакомое
  значение — толерантно Active. Семантика полей `config` —
  [s3-clusters/02-keys.md](../s3-clusters/02-keys.md).
- Состояния инстанса (`NOT_INITIALIZED` → `PROVISIONING` → `RUNNING`,
  `UNREACHABLE`, `REMOVING`, `TO_REMOVE`) и их владельцы —
  [processes/enums.md](processes/enums.md); процессы не трогают инстансы
  `TO_REMOVE`/`REMOVING`/`NOT_INITIALIZED`/`PROVISIONING` чужих процессов.

## 2. Создание кластера (A)

От `NOT_INITIALIZED` до рабочего кластера: резолв placement всех инстансов и
portalloc (под глобальным клэймом), выпуск per-cluster секретов и
репликационного PKI, создание volume и контейнеров, ожидание health-грани,
bucket'ы на каждом инстансе, доводка состава mesh по `links`, публикация
`endpoints` и снятие `config.state`. Кластер с нулём инстансов проходит
docker-пустые фазы (etcd-операции: секреты, `state` снимается; `endpoints`
не пишется). `TO_REMOVE`, пришедший посреди provisioning, безопасно
прекращает процесс.

Точные фазы P0–P5, условия ожиданий и идемпотентность —
[A. ProvisioningProcess](processes/a-provisioning.md).

## 3. Демонтаж кластера (B)

От `TO_REMOVE` до чистого etcd и снесённых docker-объектов; порядок —
«сначала docker, потом etcd». Удаляются контейнеры и volume данных
`s3w-<C>-*` (все хосты `Hosts[]`), префикс `/s3/clusters/<C>/` и вся
координация `/s3worker/…/<C>` — заявки и прогрессы не переживают удаление
кластера.

Снос кластера уничтожает данные бэкапов его инстансов — осознанное действие
оператора (панель — confirm); фиксируется в рисках
([08-operability.md](08-operability.md) §6).

Точные шаги X0–X3 — [B. DeprovisioningProcess](processes/b-deprovisioning.md).

## 4. Надзор инстансов (C)

Постоянная сверка декларации с фактом docker и health-пробой каждой ноды:

- **снесённый или остановленный контейнер** (положительное свидетельство
  инспекции, S7 [17](../17-synchronization-principles.md)) — пересоздание на
  том же хосте (`placed_on` фиксирован): volume сохраняется (данные и
  targets/cursors репликации не теряются), PKI-материал переиспользуется из
  etcd;
- **расхождение лимитов** живого контейнера cpu/mem с декларацией
  `nodes/<I>/resources` — пересоздание с лимитами декларации (docker не
  меняет лимиты живого контейнера; `disk` не сверяется);
- **молчание** health-грани дольше `NodeDeadSec` — `state=UNREACHABLE` без
  деструктива (флап ≠ смерть);
- **сбор `usage`** (гейт `HealthIntervalSec` по `updated_unix`) — занятость
  диска платформенной админ-грани → ключ `nodes/<I>/usage`: квота-наблюдение
  `resources.disk` и алерты панели;
- **переход в `RUNNING`** — по зрячей health-пробе, включая инстанс,
  оставленный чужим provisioning.

Инварианты: **одно пересоздание за тик**, слепая инспекция (docker-хост
молчит) — ошибка тика без пересозданий вслепую, отсутствие/битость
secrets-ключа — диагностический отказ с отложенным пересозданием. Данные
инстанса надзор никогда не уничтожает.

Правила проб, порогов и восстановления адресов —
[C. NodeSupervisor](processes/c-node-supervisor.md).

## 5. Bucket'ы инстансов

На каждом инстансе воркер поддерживает bucket per-install
(`S3Worker:Bucket`) и — у etcd-backup кластера — фиксированный
etcd-слепковый bucket `etcd-backups` (имя-константа, потребитель —
EtcdWorker, [etcdworker/04-snapshots.md](../etcdworker/04-snapshots.md) §1). Создание —
идемпотентное `CreateBucket` root-кредом из secrets-ключа (права admin,
[owns3/05-access-config.md](../../../OwnS3/arch/owns3/05-access-config.md) §3):
exists → пропуск; выполняется в provisioning и при добавлении инстанса
([03-node-scaling.md](03-node-scaling.md) §1).
