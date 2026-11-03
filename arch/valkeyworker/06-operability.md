# ValkeyWorker: эксплуатация

Уровень 3. Функция «эксплуатация»: классификация тика и владение клэймом,
надёжность, наблюдаемость, конфигурация и риски. Процессы —
[A](processes/a-provisioning.md)…[K](processes/k-ca-rotator.md); перечисления —
[processes/enums.md](processes/enums.md); общие принципы —
[17-synchronization-principles.md](../17-synchronization-principles.md).

## 1. Тик и порядок веток

**Состояния ноды** (`nodes/node<k>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт контейнер) → `RUNNING` (PING-проба
отвечает); `UNREACHABLE` (молчит дольше `NodeDeadSec`); `REMOVING`
(демонтаж); `TO_REMOVE` (маркер панели, one-way). Семантика каждого
значения — [processes/enums.md](processes/enums.md).

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning
([A](processes/a-provisioning.md)); `TO_REMOVE` → Deprovisioning
([B](processes/b-deprovisioning.md)); иначе Active-ветка: **миграция TLS
([T](processes/t-tls-migrator.md)) — ПЕРВЫМ шагом** → **ротация CA
([K](processes/k-ca-rotator.md)) — ВТОРЫМ шагом, до надзора** → надзор
([C](processes/c-node-supervisor.md)) → converger
([D](processes/d-config-converger.md)) → ротация кредов
([E](processes/e-password-rotator.md)).

- Детект миграции до надзора: Active-кластер без `ca_pem`/`ca_key` ИЛИ
  контейнер без TLS-args (`--tls-port` в args не найден) → TlsMigrator;
  InProgress ⇒ остальные шаги Active-ветки в этом тике не идут (миграция
  доигрывает тиками).
- **CaRotator — окно эксклюзивно**: окно ОТКРЫТО (staging `ca_next_*` есть ИЛИ
  journal `op=rotate-ca` фаза вне `{done, waiting-*}`) ⇒ InProgress ⇒
  надзор/converger/ротация кредов в этом тике не идут; ждущие исходы K
  (`waiting-*`) ветку НЕ блокируют (ротация кредов доигрывает этим же тиком
  ниже по ветке).
- Основание эксклюзивности: env-сверка надзора валидирует серт ноды против
  одного issuer'а `ca_pem` — в окне D→R это bundle, корректная сверка
  невозможна, а пересоздание надзором в окне собрало бы OLD-серт от
  OLD-key; после коммита C нода осталась бы с недоверенным сертом до
  пересоздания следующим тиком. В окне пересоздаёт ТОЛЬКО CaRotator (фаза R
  — она же лечение: мёртвая нода пересоздаётся ротацией, преф-чека живости
  нет — nodes=1, persistence off, кеш восполним).

Все операции — только под живым клэймом `<C>`; владение держится на всём
протяжении мутации: потеря lease прерывает исполняющийся тик
(cancellation-гвард), критические шаги — после живой сверки клэйма
(S5 [17](../17-synchronization-principles.md)); journal-before-manipulations.

## 2. Клэймы, идемпотентность, takeover

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть? PING
  по TLS отвечает? конфиг == декларации? ACL-план == канону? env серта ноды
  валиден против текущего CA — в окне ротации против staging `ca_next_pem`?
  bundle уже содержит nextPem?); именование и cmd-обёртка детерминированы
  (`vwk-<C>-node<k>`, порты в portalloc; свежий серт случаен — env сверяется
  по валидности `IsValidNodeEnv`, не побайтово).
- **Takeover**: состояние в etcd (journal, states, portalloc, endpoints,
  креды); смерть инстанса гасит lease ≤ 15 с — следующий продолжает с
  journal-фазы. Двойной контроллер невозможен: операции — только под живым
  клэймом.
- **Атомарность etcd**: переходы — txn с compare (`mod_revision` config,
  `version==0` клэймы/заявки, RMW endpoints).
- **Самозачистка зависших заявок**: заявки ротаций (кредов/CA) не висят
  вечно — не-начатая заявка (нет стейта `work/<C>/rotation` для кредов, окно
  CA не открыто) старше `RotationTicketTimeoutSec` снимается воркером в
  waiting-точке (исход — `ticket_outcomes/<C>`, метрика expired); начатые
  ротации доигрываются идемпотентно по стейту/фактам (takeover/ретраи).

## 3. Надёжность

- **Транспорт проб/команд**: все RESP-соединения воркера к нодам
  (V4-проба, надзор C, converger D, ротатор E) — TLS: `SslStream` + ручная
  валидация цепочки против `ca_pem` кластера (CustomRootTrust, системные
  якоря не участвуют) + сверка SAN с advertised-хостом endpoint'а;
  plain-ветки нет (plain-порт закрыт).
- **Снапшоты** контроль-плейна `/valkey/`+`/valkeyworker/`: лидер регулярно +
  «до/после» в provisioning/deprovisioning (переиспользуемый `SnapshotJob`
  Shared.Etcd, retention).
- **Ретраи**: короткие сетевые — Polly jitter; ожидание подъёма ноды —
  транзиент-толерантный цикл с бюджетом `NodeBootSec`.
- **Отказ etcd**: контроль-плейн заморожен; живые Valkey-ноды от него не
  зависят (клиенты работают по последнему снапшоту дискавери — fail-open).
- **Watchdog зависших циклов**: внутренний компонент `LoopWatchdog`
  (`Shared.Core`, `BackgroundService`) следит за активностью (тик или
  прогресс-отметка; долгие фазы — создание контейнеров нод, PING-цикл
  ожидания готовности (бюджет `NodeBootSec`); сон snapshot-лидера длиннее
  окна проверки — пульсирующим сном) циклов (reconcile/keepalive/snapshot)
  по отметкам `HealthState`; возраст активности сверх порога
  `Loops:Watchdog:Multiplier` × `Loops:Watchdog:CheckIntervalSec` («2 по 15»
  = 30 с; порог healthz loops-alive в формуле НЕ участвует — watchdog знает
  только свои опции, `WatchdogOptions` без изменений; сон snapshot-лидера
  длиннее порога — пульсирующим сном; интервалы `ScanIntervalSec`/
  `KeepaliveSec` < порога сноса — fail-fast старта, общий
  `WatchdogConfigGuard` в Program.cs трёх воркеров) → журнал (critical) +
  метрика `worker_watchdog_restarts_total{loop}` + graceful
  `StopApplication` — контейнер поднимает docker-политика, lease гаснут
  ≤15 с, takeover вторым инстансом. Механика (инварианты живости/поллинга,
  границы) — канон [14-pgworker.md](../14-pgworker.md) §6; формулы порогов
  healthz loops-alive — общий хелпер `LoopStaleness` Shared.Core (healthz
  watchdog больше не потребляет).

## 4. Наблюдаемость

Health `/healthz` по канону честного health: последнее состояние цикла, а не
первый сбой; структура всегда. Секции: `etcd-reachable`, `docker-hosts`,
`loops-alive`, `claims`, `snapshot-freshness`, `watchdog` (armed/stale —
состояние компонента). Etcd-клиент — SocketsHttpHandler + IPv4-first-резолв
(паттерн [kafkaworker/README.md](../kafkaworker/README.md)).

**Единая правда для панели**: опрос `/healthz` живых инстансов по URL из
`/valkeyworker/api/<id>`. Prometheus-метрики — единый каркас
[18-metrics.md](../18-metrics.md) §2.2 (воркер-паттерн: циклы/клэймы/фазы/
операции/снапшоты; `ValkeyWorker` в Meter-именах); коллектор доменных метрик
INFO и дашборд — [18-metrics.md](../18-metrics.md) §4.2/§2.6; обязан ходить
по TLS-транспорту воркера ([01-placement.md](01-placement.md) §2). Diag-ключи:
`/valkeyworker/work/<C>`, `nodes/node<k>/state`.

## 5. Конфигурация (appsettings + env-оверрайды)

```
ValkeyWorker:Etcd { Endpoints[] }                  # список всех узлов HA-контура — 04 §8 (env VWK_ETCD_ENDPOINT_0..2)
ValkeyWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                      PortRange{From=17000,To=17999}, Images{Node="valkey/valkey:<пин>"},
                      ContourNetwork=null }   # сеть контура (стенд/E2E), 01 §3;
                                             # не поддерживается в Mode=Swarm
ValkeyWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000,
                     Watchdog { Enabled=true, Multiplier=2, CheckIntervalSec=15, StopDelaySec=1 } }
                     # watchdog зависших циклов: порог сноса = Multiplier ×
                     # CheckIntervalSec («2 по 15» = 30 с); порог healthz в
                     # формуле НЕ участвует; Enabled=false — компонент не
                     # регистрируется; ScanIntervalSec/KeepaliveSec < порога
                     # сноса — fail-fast старта
ValkeyWorker:Thresholds { NodeBootSec=120, NodeDeadSec=90, RotationTicketTimeoutSec=3600 }
                      # RotationTicketTimeoutSec: возраст не-начатой
                      # заявки ротации (кредов/CA), после которого воркер
                      # снимает её в waiting-точке (§2)
ValkeyWorker:Parallelism { MaxClusters=4 }
ValkeyWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
ValkeyWorker:AdvertisedClientHost=null   # правило 01 §4; стенды — host.docker.internal
ValkeyWorker:Api { AdvertiseUrl, EnableSeedEndpoint=false,
                   Tls { ... } }         # HTTPS-грань, mTLS обязателен только на /api (05 §2)
# env-секреты: VWK_API_TLS_{CERT,KEY,CLIENT_CA} (04 §3)
```

`AdvertisedClientHost=null` допустим только когда имя docker-хоста резолвимо
клиентами само по себе; для локальных стендов — `host.docker.internal`
(правило [01-placement.md](01-placement.md) §4).

## 6. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ `valkey/valkey` сторонний (CVE/брейкинг-чейнджи) | версия пин (`Images:Node`); обновление — правкой настройки |
| R2 | Порт-коллизии 17000–17999 с ручными контейнерами | portalloc проверяет фактическую занятость; коллизия → сдвиг порта |
| R3 | `maxmemory_bytes` ≥ mem-лимит контейнера → OOM-килл | валидация заявки + journal-warning конвергера; ответственность оператора (UI-предупреждение); OOM-рестарт подхватит надзор C (кеш восполним) |
| R4 | Ротация: отказ между фазами E1–E3 | оба пароля валидны (окно двух паролей); повтор тика доигрывает; пересоздание контейнера в окне безопасно (аргументы из etcd) |
| R5 | Трафик/креды в docker-сети | TLS клиентских подключений (per-cluster CA, `--tls-port`, доверие `ca_pem`); остаточный вектор — внутри закрытой сети контроль-плейна (etcd/панель/воркер) |
| R6 | Потеря кеша при пересоздании контейнера (persistence off) | документированное поведение: холодный старт, кеш восполним приложениями |
| R7 | ACL-пароли в etcd — компрометация etcd = доступ к кешам | etcd — уже хранилище per-cluster-секретов (pg/kafka); закрытая сеть установки; ротация — процесс E |
| R8 | Смена `AdvertisedClientHost` — SAN серта перестаёт покрывать хост | серт пересобирается при каждом пересоздании по правилу [01](01-placement.md) §4; до пересоздания клиенты по новому хосту получают TLS-отказ — ответственность оператора (образец kafka R8) |
| R9 | Окно миграции plain→TLS: TLS-неготовые клиенты получают отказ после пересоздания | заявлено одним релизом (панель, воркер, Puzzle-библиотеки обновляются тем же релизом); окно = одно пересоздание контейнера |
| R10 | `ca_key` в etcd — компрометация etcd = выпуск валидных сертов | зона доверия контроль-плейна (как все per-cluster-секреты, R7); ротация CA — процесс K: компрометированный OLD-ключ уничтожается перезаписью в коммите C, серт ноды перевыпускается от NEW CA, клиенты забирают NEW из `ca_pem` (окно двойного доверия — без недоступности) |
| R11 | Окно ротации CA: клиенты с закэшированным OLD-доверием получают TLS-отказ после R до перечитывания `ca_pem`; одноблочные читатели (панельные пробы, метрики-коллектор) транзиентно недоверяют NEW-серту между D и C | poll-дискавери библиотеки перечитывает снапшот (секунды); окно D→C — один тик воркера (nodes=1 — одно пересоздание); панельные пробы/метрики самокорректируются следующим poll-циклом; многосертовые читатели окна не видят ([valkey-clusters/04-client-discovery.md](../valkey-clusters/04-client-discovery.md) §1) |
