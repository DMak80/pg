# Valkey-кластеры: координация воркера `/valkeyworker/`

Уровень 3. Служебный слой ValkeyWorker. Порт схемы `/kafkaworker/`
([kafka-clusters/04-coordination.md](../kafka-clusters/04-coordination.md)) один в один, свой
префикс.

## 1. Ключи

| Ключ | Тип | Назначение |
|---|---|---|
| `/valkeyworker/leader` | lease TTL 15 с | лидер singleton-задач (регулярные снапшоты контроль-плейна `/valkey/`+`/valkeyworker/`) |
| `/valkeyworker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/valkeyworker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` (сохранение накопленных треков — S6, [17-synchronization-principles.md](../17-synchronization-principles.md)). Вложенный ключ `work/<C>/rotation` — стейт доигрывания ротации: `{"phase":"e1-pending"\|"e1-added"\|"e2-committed","role","old","new","requested_by","request"?}` (`e1-pending` — транзитная: стейт пишется до E1, чтобы NEW, добавленный на ноду, всегда доигрывался тем же значением; фаза+пароли переживают тик — ключ журнала `work/<C>` надзор перезаписывает каждый тик, фаза ротации в нём не живёт; `request` — исходный payload заявки `rotations/<C>`: compare условного del в E2 — чужая/снятая панелью заявка не трогается; del ротатором после E3, чистка — демонтаж кластера) |
| `/valkeyworker/portalloc/<C>` | обычный | `{"node<k>":{"host":"h","client":17001}}` — закрепление клиентских портов (переживает пересоздание контейнера) |
| `/valkeyworker/locks/portalloc` | lease TTL 15 с | **глобальный portalloc-клэйм**: взаимоисключение секции довыделения клиентских портов «чтение занятости → выбор портов → запись `/valkeyworker/portalloc/<C>`» — пер-кластерные клэймы кросс-кластерную гонку не закрывают; txn `version==0` + put-with-lease, del + revoke lease по завершении секции; не взял → InProgress (следующий тик) |
| `/valkeyworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/valkeyworker/api/<id>` | lease TTL 15 с | **дискавери API воркера** (паттерн [kafkaworker/README.md](../kafkaworker/README.md)): `{"url","instance","since_unix","cert_thumbprint"?}` — ставит сам инстанс; ключ жив = инстанс жив и URL валиден. Читает панель (мутации valkey-домена — через API воркера) |
| `/valkeyworker/rotations/<C>` | обычный | заявка ротации креда `{"role":"app"\|"admin","requested_unix","requested_by"}` (панель через API воркера — клэйм-txn `version==0`; del воркером по завершении или возрастным таймаутом не-начатой — [valkeyworker/04-security.md](../valkeyworker/04-security.md) §4; отмены из панели нет) |
| `/valkeyworker/ca_rotations/<C>` | обычный | заявка ротации per-cluster CA/сертов `{"requested_unix","requested_by"}` (панель через API воркера — клэйм-txn `version==0`, протокол §9.8 один в один с ротациями кредов; del воркером в коммите фазы C или возрастным таймаутом не-начатой — [valkeyworker/04-security.md](../valkeyworker/04-security.md) §6) |
| `/valkeyworker/ticket_outcomes/<C>` | обычный | последний исход заявки — пишет только воркер: `{"kind":"password-app"\|"password-admin"\|"ca","outcome":"expired"\|"done","reason"?,"requested_unix","requested_by","finished_unix"}`; перезаписывается новым исходом, чистится демонтажом ([valkeyworker/processes/b-deprovisioning.md](../valkeyworker/processes/b-deprovisioning.md)); панель читает (алерт `valkey-ticket-expired`, [adminpanel/03-panels.md](../adminpanel/03-panels.md)) |

**Заявка ротации — один ключ с полем `role`** (не два, как у kafka — там
разделение app/admin-ротаций наследие JAAS-механики пересозданий; здесь
ротация без рестартов единая для обеих ролей,
[valkeyworker/04-security.md](../valkeyworker/04-security.md) §4).

## 2. Писатель и читатели

- **Пишет** весь префикс — только ValkeyWorker; исключение вне префикса —
  серверный серт API `/workers/api_tls/valkeyworker` (пишет панель,
  [valkeyworker/05-api.md](../valkeyworker/05-api.md) §3).
- **Панель читает** только `rotations/` (очередь ротаций в UI),
  `ca_rotations/` (очередь ротаций CA), `ticket_outcomes/` (исходы заявок) и
  `api/` (дискавери API; мутации панели идут через HTTP-грань воркера);
  остальные ключи не читает и не пишет.
- **Снятие заявки ротации** — только воркером: del по завершении процесса
  (`rotations` — финал ротации кредов, `ca_rotations` — коммит фазы C
  CaRotator) или возрастной таймаут не-начатой заявки
  (`RotationTicketTimeoutSec`, исход — `ticket_outcomes/<C>`); отмены из
  панели НЕТ (окно «передумать» мало — заявка исполняется тиками за секунды;
  начатая ротация доигрывается, staging не сиротеет).
- **Реализация координации** — переиспользование `Shared.Etcd`
  (`ClaimStore`/`PortAllocLock`/`WorkJournal` — `keyPrefix="/valkeyworker"`,
  префикс уже параметр конструктора; сборка подключена, новый код не
  пишется).
