# ValkeyWorker: жизненный цикл кластера

Уровень 3. Функция «жизненный цикл»: создание кластера, демонтаж и надзор
ноды. Полные автоматы — процессы [A](processes/a-provisioning.md),
[B](processes/b-deprovisioning.md), [C](processes/c-node-supervisor.md);
перечисления — [processes/enums.md](processes/enums.md).

## 1. Состояния

- `config.state=NOT_INITIALIZED` — заявка панели на создание; `TO_REMOVE` —
  заявка на полный демонтаж; отсутствие `state` — Active-кластер. Семантика
  полей `config` — [valkey-clusters/02-keys.md](../valkey-clusters/02-keys.md) §1.
- Состояния ноды (`nodes/node<k>/state`) — [processes/enums.md](processes/enums.md);
  процессы не трогают ноды `TO_REMOVE`/`REMOVING`/`PROVISIONING` чужих
  процессов.

## 2. Создание кластера (A)

От `NOT_INITIALIZED` до рабочего кластера: план размещения и порт (под
глобальным portalloc-клэймом), ensure секретов (admin/app/CA), контейнер с
аргументами ACL/maxmemory, TLS-env и лимитами, ожидание готовности по PING
через TLS, публикация `endpoints` и снятие `config.state`. Пересоздание
(re-run) сверяет cmd-обёртку, валидность env-серта, порт и лимиты; при
расхождении — пересоздание со свежим env. `TO_REMOVE`, пришедший посреди
provisioning, безопасно прекращает процесс.

Точные фазы V0–V5 и условия ожиданий —
[A. ProvisioningProcess](processes/a-provisioning.md).

## 3. Демонтаж кластера (B)

От `TO_REMOVE` до чистого etcd и удалённых контейнеров; порядок — «сначала
docker, потом etcd». TLS-материал жил в env контейнеров, отдельного
docker-объекта у домена нет. Удаляются контейнеры `vwk-<C>-*`, префикс
`/valkey/clusters/<C>/` (включая `ca_next_*` staging) и координация
`/valkeyworker/…/<C>` (клэймы, журнал, portalloc, заявки ротаций, исходы) —
заявки не переживают удаление кластера.

Точные шаги X0–X3 — [B. DeprovisioningProcess](processes/b-deprovisioning.md).

## 4. Надзор ноды (C)

Сверка декларации с фактом docker и PING-проба по TLS (admin-кред). Надзор:

- пересоздаёт снесённый контейнер с аргументами из текущей декларации и
  кредов etcd (включая TLS-env и cmd-обёртку); CA в etcd отсутствует —
  пересоздание отложено до миграции [T](processes/t-tls-migrator.md);
- сверяет env живой ноды против `ca_pem` (валидность цепочки, key↔cert,
  NotAfter, SAN); невалиден — пересоздание;
- удаляет осиротевший том `vwk-<C>-tls` старой volume-модели, когда все ноды
  уже на env-модели (409 volume-in-use — ретрай следующим тиком);
- автоконвергит лимиты `resources` пересозданием при расхождении cpu/mem;
- нода молчит дольше `NodeDeadSec` → `UNREACHABLE` + пересоздание без тома
  (данных не жалко, кеш восполним).

Инварианты: **одно пересоздание за тик** по любой причине; слепая проба —
никаких действий (S7 [17](../17-synchronization-principles.md)); надзор
никогда не чистит ключи домена из-за недоступности ноды; ноды чужих
состояний не трогаются. Восстановление адресов безадресной ноды —
лестница E9 ([17](../17-synchronization-principles.md)).

Правила сверок, порогов и лестницы E9 —
[C. NodeSupervisor](processes/c-node-supervisor.md).
