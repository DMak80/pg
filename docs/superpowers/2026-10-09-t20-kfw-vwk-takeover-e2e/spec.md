# t20-kfw-vwk-takeover-e2e — env-TLS у ValkeyWorker + host-kill E2E для KafkaWorker / ValkeyWorker

Статус: спека (Фаза 1 dev-flow). Задача ДВУХСОСТАВНАЯ (решение пользователя:
«нужный результат, а не кипа задач»):

1. **Прод-изменение**: перевод TLS-доставки сертов нод ValkeyWorker с named
   volume `vwk-<C>-tls` + helper-контейнера на **PEM в env ноды + cmd-обёртку
   старта** (модель KafkaWorker); демонтаж helper-механики из `Shared.Docker`;
   миграция живых кластеров надзором.
2. **Тестовая часть**: docker-E2E host-kill takeover для KafkaWorker /
   ValkeyWorker (исходная цель t20).

Мотивация прод-части: при docker kill держателя клэйма посреди операции с
TLS-томом helper-контейнер (`sleep 120`, bind тома) переживает kill и держит
том до 120 с; демонтаж X1 получает 409 volume-in-use и ретраит ~100 с —
финальный демонтаж Valkey не укладывается в целевой бюджет **15 с**. Бюджеты
двигать нельзя — устраняется сам класс зависимости (тома больше нет).
Мотивация тестовой части — разрыв R reliability-report («сценарии отказа
хоста/DC не отработаны»); развилка roadmap —
`arch/roadmap/reliability.md` (`t20-kfw-vwk-takeover-e2e`).

Поряд работ — **arch-first**: сначала правка канона TLS-модели в `arch/21`,
затем код (AGENTS.base §1).

## 1. Цель

**Часть A (прод)**. Нода Valkey получает TLS-материал (node.crt/node.key/
ca.pem) через env контейнера; при старте cmd-обёртка раскатывает PEM в
`/tls` (каталог в ФС контейнера, НЕ том) и exec'ает `valkey-server` с прежним
каноническим набором args. TLS-том `vwk-<C>-tls` и helper-механика
(`Put/GetVolumeArchiveAsync`, `CreateHelperAsync`) выпадают из модели и
демонтируются. Финальный демонтаж Valkey-кластера — ≤15 с (класс «409
volume-in-use» исчезает).

**Часть B (тесты)**. Доказать интеграционно (docker-E2E, контейнерные
инстансы воркеров), что при внезапной смерти ОДНОГО инстанса KafkaWorker /
ValkeyWorker посреди длительной надзорной операции второй инстанс бесшовно
забирает надзор и ДОИГРЫВАЕТ операцию до конца — без дублей, без потери
контроль-плейна, с живым датаплейном. Эталон формы — PgWorker
`E2eSecondInstanceScenarios`, с усилением: kill целился именно в ДЕРЖАТЕЛЯ
клэйма (резолв по etcd-фактам перед kill). E2E-часть валидирует и часть A
(takeover посреди демонтажа кластера — демонтаж в env-модели без
remove-tls-volume, бюджет ≤15 с).

## 2. Проверяемые инварианты t20-кейсов (общие для обоих воркеров)

- **I1 Takeover клэйма**: после смерти держателя lease-клэйм `<prefix>/claims/<C>`
  гаснет ≤15 с (TTL) и пере-захватывается выжившим; дискавери-ключи мертвеца
  (`<prefix>/api/<id>`, `<prefix>/instances/<id>`) гаснут вместе с его lease —
  после kill жив ровно один api-ключ и один instance-ключ, и это survivor;
  **погашение дискавери мертвеца — ассерт С БЮДЖЕТОМ ожидания ≤20 с, НЕ
  мгновенный** (ключи умирают с lease мертвеца ≤15 с; мгновенный `HaveCount(1)`
  сразу после kill — дефект теста: погашение ещё не случилось — подтверждено
  диагностическим прогоном 2026-10-09-182646).
- **I2 Доигрывание операции**: операция-жертва доведена выжившим до терминальной
  фазы — по etcd-фактам (config/ключи домена + work-журнал
  `<prefix>/work/<C>` с `op`/`phase=done` и `instance` = выжившего). Для
  V-кейса (жертва — демонтаж): демонтаж, начатый по заявке убитого, доведён
  ДО КОНЦА выжившим — доказательство конструктивное (kill-до-X0 +
  единственность survivor + терминальная чистота) + документальный лог
  survivor'а (§8 A5); journal-полл неприменим — survivor проходит X0→X2
  одним тиком <0,5 с, короче полла 500 мс (записи `phase=done` у демонтажа
  и так нет по канону arch/21 §5 B: journal после чистки воскресил бы
  удалённый work; координация `<C>` остаётся пустой).
- **I3 Отсутствие дублей**: контейнерная картина строго по декларации
  (Kafka: контейнеров `kfw-<C>-broker*` ровно 3; **Valkey: после takeover
  контейнеров `vwk-<C>-node1` ровно 0 — дублей/призраков нет; тома в модели
  нет, ассерт только на контейнер**).
- **I4 Целостность контроль-плейна**: portalloc жив и цел
  (`<prefix>/portalloc/<C>`), клэйм/дискавери — у survivor (I1), дублей ключей нет.
- **I5 Датаплейн жив**: Kafka — admin-клиент из etcd-дискавери (endpoints +
  креды + ca_pem) видит 3 брокеров; Valkey (жертва — демонтаж): живого
  датаплейна после финиша нет — датаплейн-проверки кейса: RESP `PING`
  admin-кредом по TLS в arrange (кластер жив до kill) + чистое исчезновение
  всех датаплейн-объектов в финале (I3).
- **I6 Финальный демонтаж руками выжившего**: Kafka — после takeover survivor
  штатно демонтирует кластер (config `state=TO_REMOVE` в стиле панели →
  DeprovisioningProcess): домен чист (ни контейнеров, ни ключей префикса,
  portalloc снят). Для V-кейса отдельного финального демонтажа НЕТ — жертва
  и есть демонтаж: домен чист средствами survivor'а, wait-clean ≤15 с, без
  remove-tls-volume (механики нет).
- **I7 Чистота хоста**: teardown фикстуры при любом исходе убирает ВСЁ своё
  (ассерт чистоты own-only по guid/тегу прогона); сети per-cluster
  `kfw-net-<C>`, созданные движком (ryuk их не подбирает), удаляются демонтажом
  и попадают в own-only чистку/ассерт.

## 3. Готовая база (факт-проверено по коду; используется как есть)

| Что | Где | Факт |
|---|---|---|
| Координация инстансов | `src/Shared.Etcd/Coordination/ClaimStore.cs` | пер-кластерные lease-клэймы `<prefix>/claims/<C>` (payload `{"instance","since_unix","phase"}`), TTL 15 с / keepalive 5 с; смерть инстанса гасит lease ≤15 с → takeover. Дискавери: `<prefix>/instances/<id>` и `<prefix>/api/<id>` (payload `{"url","instance","since_unix","cert_thumbprint"}`) — на одном lease с instance-ключом |
| Гейт мутаций | `CaRotator.RunAsync` (`src/ValkeyWorker.Provisioning/Processes/CaRotator.cs`), `ProvisioningProcess.TickAsync` (`src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs`) | мутации только при `claims.IsMine(cluster)` — заявка НЕ «прилипает» к instance-id, takeover доигрывает |
| Цикл KafkaWorker | `src/KafkaWorker.App/Loops/ReconcileLoop.cs` | тик = Range `/kafka/clusters/` → классификация → клэйм → процесс; интервалы `Loops.ScanIntervalSec`/`ErrorDelayMs` (env-оверрайды) |
| Цикл ValkeyWorker | `src/ValkeyWorker.App/Loops/ValkeyClusterProcesses.cs` (+ `ReconcileLoop.cs`) | Active-ветка = tlsMigrator → **caRotator** → supervisor → converger → rotator; интервалы `Loops.*` (env-оверрайды) |
| CA-ротация Valkey | `CaRotator.cs` | фазы P (staging put-if-absent) → D (bundle) → R (пересоздание node1; факт-детект «серт уже NEW» — сейчас `GetTlsArchiveAsync`+`IsValidTar`, при env-модели — по env контейнера, §5) → C (атомарный txn) → K4 done. Заявка `/valkeyworker/ca_rotations/<C>`; staging `ca_next_{key,pem}` живут от P до C; `WindowOpenAsync` уводит открытое окно в доигрывание БЕЗ ждущих проверок |
| Ограничение v1 | `src/ValkeyWorker.App/Api/Operations/ValkeyLimits.cs` (`MaxNodes=1`), `CreateClusterHandler.cs` | multi-node Valkey-кластер невозможен → операция-жертва Valkey — CA-ротация |
| Provisioning Kafka | `ProvisioningProcess.cs` | K1 planning (portalloc+роли под глобальным `PortAllocLock`, TTL 15 с) → K3 `EnsureNodesAsync` (контейнеры `kfw-<C>-broker<N>`, `state=PROVISIONING`) → K4 `waiting-brokers` (DescribeCluster, самая долгая фаза) → K5 converge + `endpoints` + `CommitConfigAsync` → done |
| Классификация | `KafkaClusterClassifier.cs` / `ValkeyClusterClassifier.cs` | `NOT_INITIALIZED`→Provision, `TO_REMOVE`→Deprovision, без `state`→Active |
| Сид кластера Kafka | `src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaClusterFixture.cs` → `SeedClusterAsync(cluster, brokers)` | config NOT_INITIALIZED + `brokers/brokerN/state` + `resources`; тег прогона в имени кластера |
| Эталон второго инстанса | `src/tests/PgWorker.IntegrationTests/E2e/E2eSecondInstanceScenarios.cs` | два контейнера воркера, docker kill первого посреди операции, ассерты survivor/нет дублей/api=1. Резолва держателя клэйма перед kill НЕТ — t20 добавляет его |
| Канон фазовых ожиданий | `src/tests/PgWorker.IntegrationTests/E2e/E2ePhase.cs` | `[PHASE]`/`[PHASE-TICK]` (тик 5 с, полл 500 мс), slow-phase 60 с, failed-phase сбор до возврата |
| Эталон окружения Valkey | `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eEnvironment.cs` | сеть `vwk-en-{runId}`, etcd `vwk-ee-{runId}`, воркер `vwk-ew-{runId}` (образ `valkeyworker:e2e`), TLS-пакет bind-mount `/tls`, PortRange из `FreePortWindow`, retry старта, телеметрия, MarkFailed-стоп-без-удаления, ассерт чистоты |
| Два инстанса в деплое | `deploy/docker-compose.yml` (`kafkaworker`/`kafkaworker-2`, `valkeyworker`/`valkeyworker-2`) | общий TLS-пакет и etcd, уникальный `AdvertiseUrl` каждому — модель воспроизводима в тесте |
| Формат work-журнала | `src/Shared.Etcd/Coordination/WorkJournal.cs` | `{prefix}/work/<C>` = `{"op","phase","instance","updated_unix","last_error",…}` |
| **Env контейнеров в движке** | `src/Shared.Docker/Engine/ContainerSpec.cs` (`Env`), `IDockerEngine.InspectContainerEnvAsync/InspectServiceEnvAsync` | `ContainerSpec.Env` уже поддержан (словарь), инспекция env контейнера/сервиса уже есть — движок готов к env-модели |
| **Модель Kafka (эталон env-PEM)** | `src/KafkaWorker.Core/Templates/NodeEnvBuilder.cs:91-98` | PEM передаётся брокеру env-строками (KEYSTORE_TYPE=PEM, переносы экранированы `\n` — требование Java Properties у kafka; у valkey этого ограничения нет). Volume/сертификатных helper'ов у KafkaWorker нет |
| **Helper-механика Shared.Docker** | `src/Shared.Docker/Engine/DockerEngine.cs:274,317` (`Put/GetVolumeArchiveAsync`), `:1132` (`CreateHelperAsync` — контейнер `sleep 120` с bind тома, убирает ТОЛЬКО создатель в `finally`) | при docker kill создателя helper держит том до 120 с → 409 volume-in-use у X1. Прод-потребители — ТОЛЬКО `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs` (Plain `:312,321`; Swarm-делегаты `:462,466`). PgWorker/KafkaWorker прод helper-механику НЕ используют |

Префиксы координации: KafkaWorker `/kafkaworker`, ValkeyWorker `/valkeyworker`.

## 4. Проектное решение: env-TLS у ValkeyWorker

### 4.1. Механика (канон после правки arch/21 §2)

- **Env-имена** (по канону кода: env контейнеров НОД несёт префикс домена ноды —
  у KafkaWorker брокеры получают `KAFKA_*`; `VWK_*` — env САМОГО воркера,
  ноде не подходит):

  | env | Значение |
  |---|---|
  | `VALKEY_TLS_CERT` | PEM серверного серта ноды (CN=`node<k>`, SAN advertised-хоста, подпись `ca_key`) |
  | `VALKEY_TLS_KEY` | PEM приватного ключа ноды (PKCS#8, RSA-2048) |
  | `VALKEY_TLS_CA` | PEM per-cluster CA (`ca_pem` кластера; в окне ротации фазы R — NEW из staging) |

  Значения — многострочный PEM (нормализация `\n`→переносы на границе
  сборки; в etcd канон «одной строкой с `\n`» — arch/20 §2.1 — НЕ меняется).

- **Файлы в контейнере**: `/tls/node.crt`, `/tls/node.key`, `/tls/ca.pem` —
  ТЕ ЖЕ имена и пути, что писались в volume: канонические TLS-args
  (`--tls-cert-file /tls/node.crt` и т.д.) не меняются — ассерты args
  существующих тестов меняются минимально (см. §8).
- **Cmd-обёртка старта**: `Cmd = ["sh","-c","<раскатка>; exec valkey-server
  <прежний канонический набор args>"]`, где раскатка:

  ```
  umask 077; mkdir -p /tls;
  printf %s "$VALKEY_TLS_CERT" > /tls/node.crt;
  printf %s "$VALKEY_TLS_KEY"  > /tls/node.key;
  printf %s "$VALKEY_TLS_CA"   > /tls/ca.pem;
  ```

  а `<args>` — в точности argv после `valkey-server` из `NodeArgsBuilder.Build`
  (ACL/maxmemory/persistence/TLS-хвост), shell-экранированный (пустой аргумент
  `--save ''`, пароли `[A-Za-z0-9]`). Образной entrypoint при первом
  аргументе `sh` делает `exec "$@"` как есть — обёртка срабатывает ДО
  valkey-server. `/tls` — каталог в ФС контейнера, не том, не volume-mount.
- **Uid процесса**: gosu-ветка образного entrypoint срабатывает только при
  `$1 == valkey-server`; с обёрткой `$1 == sh` → valkey-server стартует от
  root. Серты — 0600 root (umask 077). Изоляция секрета — периметром
  контейнера, как и прежде: PEM и так доступен из `docker inspect` ноды (env),
  а доступ к inspect/демону = доступ к docker-хосту. Точка верификации на фазе
  кода: сверить фактический entrypoint пина `valkey/valkey:<пин>` (образ
  официальный; ожидание — паттерн redis-entrypoint).
- **Сверка факта (идемпотентность по факту)** — env-эквивалент прежнего
  «volume жив и валиден → переиспользование»: свежий серт генерируется
  случайно (`ValkeyPki.IssueNodeCertificate`), поэтому побайтовая сверка env
  НЕВОЗМОЖНА (бесконечные пересоздания). Вместо неё:
  - `IsValidNodeEnv(env, advertisedHost, caPem, clock)` — эквивалент
    `NodeTlsProvisioner.IsValidTar` по тем же критериям (CA == ожидаемому,
    цепочка валидна, SAN покрывает advertised, NotAfter жив, key↔cert), но
    источник — env контейнера, не tar тома;
  - Cmd-обёртка ДЕТЕРМИНИРОВАНА от декларации и кредов etcd (как прежний
    args) — сверяется на точное совпадение.
  - Решение «пересоздать/пропустить» = env валиден И Cmd каноничен И порт/лимиты
    совпадают → пропуск; иначе — RemoveNode + EnsureNode со свежим env.

### 4.2. Что меняется в коде (ValkeyWorker)

| Компонент | Изменение |
|---|---|
| `IClusterDriver` / `PlainClusterDriver` / `SwarmClusterDriver` | `ValkeyNodeSpec.TlsVolume` → `Env` (`IReadOnlyDictionary<string,string>?`); `EnsureNodeAsync` передаёт `Env` в `ContainerSpec` (Plain-контейнер и Swarm-сервис — поле общее). УДАЛЯЮТСЯ `EnsureTlsVolumeAsync`/`PutTlsArchiveAsync`/`GetTlsArchiveAsync`/`RemoveTlsVolumeAsync`/`TlsVolumeName`. ДОБАВЛЯЕТСЯ `NodeEnvAsync(cluster, node)` → env фактического контейнера/сервиса (Plain — перебор хостов `InspectContainerEnvAsync`; Swarm — `InspectServiceEnvAsync`; null = объекта нет) |
| `NodeArgsBuilder` | `Build(...)` возвращает прежний список args (вкл. `valkey-server`); НОВОЕ: сборка cmd-обёртки `BuildCmd(args)` = `["sh","-c", раскатка + "; exec " + shell-escape(args)]` — единая точка канона обёртки |
| `NodeTlsProvisioner` | `EnsureNodeTlsAsync` (ensure volume + put tar) УДАЛЯЕТСЯ. Замена: `BuildNodeTlsEnv(caPem, caKeyPem, node, advertisedHost)` → словарь `VALKEY_TLS_{CERT,KEY,CA}` (свежий `IssueNodeCertificate`); `IsValidTar` → `IsValidNodeEnv` (§4.1). Вызовы «ДО EnsureNodeAsync» исчезают — env входит в spec создания |
| `ProvisioningProcess` (V3) | сверка re-run: существующий объект — env валиден против `creds.CaPem` + Cmd == канонической обёртке + порт + лимиты → пропуск; иначе/нет — Remove+Ensure с env. `EnsureNodeTlsAsync`-вызов убирается |
| `CaRotator` (R) | факт-детект «R завершён»: `driver.NodeEnvAsync` → `IsValidNodeEnv(env, advertised, nextPem)` (серт уже NEW) — вместо чтения тома; пересоздание: RemoveNode → EnsureNode с env от `ca_next_key`/`ca_next_pem` (ca.pem в env = NEW, НЕ bundle). Остальные фазы (P/D/C) не меняются |
| `NodeSupervisor` (C) | НОВОЕ: сверка env живой ноды (supervisable) — env отсутствует/невалиден против `snap.CaPem` → пересоздание с env (в рамках «одно пересоздание за тик»). RecreateAsync собирает env + cmd-обёртку вместо TLS-volume. НОВОЕ: легаси-чистка — после успешной обработки нод, если все ноды кластера на env-модели, попытка `DeleteVolumeAsync(vwk-<C>-tls)` на engine'ах (404/409 = успех/ретрай тиком) — убирает осиротевшие томы старой модели у живых кластеров |
| `TlsMigrator` (T) | детект прежний (нет `ca_pem`/`ca_key` ИЛИ args живого контейнера без `--tls-port`); T2: RemoveNode → EnsureNode с env (тома больше не касается; осиротевший легаси-том уберёт надзорная легаси-чистка) |
| `DeprovisioningProcess` (X1) | `RemoveTlsVolumeAsync`-шаг УДАЛЯЕТСЯ — тома в модели нет; X1 = только rm контейнеров `vwk-<C>-*` |

### 4.3. Демонтаж helper-механики из Shared.Docker

- Удаляются из `DockerEngine` + `IDockerEngine`: `PutVolumeArchiveAsync`,
  `GetVolumeArchiveAsync`, приватные `CreateHelperAsync`/`ExecInHelperAsync`/
  `HelperName`.
- `TarArchive` (Shared.Docker) после этого не имеет прод-потребителей —
  удаляется вместе с `src/tests/Shared.Docker.UnitTests/Engine/TarArchiveTests.cs`
  (мёртвый код не храним). Если на фазе кода вскроется иной потребитель —
  TarArchive остаётся, зафиксировать в плане.
- `EnsureVolumeAsync`/`DeleteVolumeAsync`/`VolumeExistsAsync`/`RemoveVolumeAsync`
  остаются (утилиты движка; `VolumeExistsAsync` используют E2E-окружения).
- **Гейт перед демонтажем** (повторно, на фазе кода): grep-проверка
  прод-потребителей `Put/GetVolumeArchiveAsync` вне ValkeyWorker. Найдётся
  скрытый прод-потребитель — СТОП и анализ (промпт задачи: «не демонтировать
  без анализа»). На момент спеки: потребители — только
  `ValkeyWorker.Docker/Drivers/ClusterDriver.cs`; прочие вхождения —
  тестовые фейки (`PgWorker.UnitTests` ×3, `PgWorker.IntegrationTests/Backups`
  ×3, `ValkeyWorker.UnitTests` ×2) —
  чистятся синхронно (§7.3).

### 4.4. Миграция живых кластеров (стендовые `vwk-demo` и пр.)

- Живой кластер старой модели (нода с volume-mount `vwk-<C>-tls`, без
  `VALKEY_TLS_*`): первый же тик надзора после обновления воркера видит
  env-расхождение → пересоздание ноды с env (state=PROVISIONING → RUNNING по
  PING) → легаси-чистка удаляет осиротевший том. Отдельного кода миграции
  нет — это штатная надзорная сверка §4.2. Кеш восполним (persistence off) —
  окно = одно пересоздание.
- Кластер, демонтированный ДО миграции (X1 старым кодом том не почистил бы,
  новым — тома не знает): осиротевший `vwk-<C>-tls` не убирает никто (домен
  удалён, надзора нет) — переходный случай, чистка ручная по runbook
  (§7.5). Для дев-стенда существующий `vwk-demo-tls` уйдёт легаси-чисткой
  надзора при живом демо-кластере.

### 4.5. Arch-first (правки канона — ДО кода)

- `arch/21-valkeyworker.md` §2 (модель размещения): TLS-абзац — доставка
  сертов = PEM в env ноды (`VALKEY_TLS_{CERT,KEY,CA}`) + cmd-обёртка старта
  (раскатка в `/tls`, exec valkey-server с прежними args); volume/helper
  исключаются; «объекты домена: кластер = контейнер(ы) `vwk-<C>-node<k>`»
  (без volume); uid-примечание (root, 0600, периметр контейнера).
  §5 A (V3), §5 B (X1 — без remove-tls-volume), §5 C (сверка env + легаси-
  чистка), §5 T, §5 K (R — факт-детект по env), §6 (идемпотентность — env
  вместо volume). Границы (строка про TLS-volume в «Границы») и §2-строки про
  helper-транспорт.
- `arch/20-valkey-clusters.md`: клиентская сторона (формат `ca_pem`,
  дискавери-доверие, окно ротации) НЕ меняется; проверка grep'ом на
  «volume»-упоминания доставки сертов (на момент спеки таких строк нет —
  правка не требуется; если фаза кода найдёт — править тем же arch-first
  коммитом).
- Исторические пассажи не пишем (AGENTS.md): документы описывают модель как
  «устроено», без «раньше было volume».

## 5. Границы задачи (что НЕ делаем)

- **НЕ трогаем прод**: `src/KafkaWorker.*`, `src/PgWorker.*`,
  `src/AdminPanel.*`, `src/Shared.{Core,Etcd,Tls,Metrics}` — только чтение.
  Правки `src/ValkeyWorker.*` — ТОЛЬКО TLS-механика §4 (доставка сертов,
  демонтаж helper-зависимостей); правки `src/Shared.Docker` — ТОЛЬКО демонтаж
  helper-механики §4.3 (TarArchive — по гейту §4.3). Продовые Dockerfile
  (`docker/*`) не трогаем.
- **Тесты**: правим `src/tests/ValkeyWorker.*` (адаптация + новые),
  `src/tests/PgWorker.*` (только синхронное удаление фейк-реализаций
  демонтируемых методов: `PgWorker.UnitTests/Docker/BackupJobsCleanerTests.cs`,
  `ClusterDriverTests.cs`, `PgWorker.UnitTests/Backups/BackupProcessTests.cs`;
  `PgWorker.IntegrationTests/Backups/FakeBackupEngine.cs`,
  `BackupVerifyProcessTests.cs`, `RestoreDrillProcessTests.cs`),
  `src/tests/Shared.Docker.UnitTests` (удаление TarArchiveTests). Тесты
  KafkaWorker — только добавление E2e-папки.
- **Панель**: не трогаем — проверено, панель для TLS читает только `ca_pem`
  из etcd (live-пробы); TLS-том нигде не читает (`src/AdminPanel.Etcd/
  Valkey*`, `src/AdminPanel.Core/Valkey` — только тексты алертов про
  контейнер `vwk-<C>-node1`).
- **Операции-жертвы E2E зафиксированы решением пользователя (2026-10-09)**:
  Kafka — provisioning 3-брокерного кластера; Valkey — демонтаж кластера
  (ротация отменена: в env-модели ~0,2 с — окно kill исчезло). Стабильно
  недостаточное окно kill у Valkey — СТОП и пересогласование (§15).
- **Никаких общих контуров** между сценариями E2E: каждый кейс поднимает
  СВОЙ etcd, свою сеть, свои контейнеры; имена guid-уникальны
  (docs/e2e-isolation.md).
- **Никаких хардкод-портов**: все host-порты — зонды/`assignRandomHostPort`;
  никаких литералов `:16000`/`:17000` в expects; диапазоны вне зоны dev-станда.
- **Никаких sleep-поллингов**: `Task.Delay(500)`-циклы с конечным бюджетом;
  прогресс-тик фаз — раз в 5 с; docker-CLI в тиках запрещён (только
  etcd-чтения).
- **НЕ пускаем фильтром `~Takeover`**: цепляет 7 чужих тестов (вкл. тяжёлый
  PgWorker docker-E2E) — только точные имена новых классов (§12).
- **Бюджеты демонтажа не двигаем**: целевой бюджет финального демонтажа
  Valkey — 15 с; достижение — устранением TLS-тома (не увеличением таймаутов).

## 6. Компоненты (тестовая часть)

### 6.1. Новая инфраструктура Kafka: `src/tests/KafkaWorker.IntegrationTests/E2e/`

Папка E2e в проекте отсутствует — создаётся. Ядро — `KafkaE2eEnvironment`
(порт `ValkeyE2eEnvironment`, сразу ДВА воркера):

- **Имена**: runId = полный guid; сеть `kfw-en-{runId}`, etcd `kfw-ee-{runId}`,
  воркеры `kfw-ew1-{runId}` / `kfw-ew2-{runId}` (образ `kafkaworker:e2e`).
  Кластер сценария — `tkw{ClusterTag}` (tag = `runId[..8]`); движковые объекты
  `kfw-{C}-broker<N>` и сеть `kfw-net-{C}` содержат тег → own-only чистка.
- **Порты**: etcd-порт и api-порты воркеров `ew1`/`ew2` — каждый свой
  `FreePort()`-зонд; `KafkaWorker__Docker__PortRange__{From,To}` — окно из
  `KafkaWorker.IntegrationTests.Kafka.FreePortWindow.Find()` (ширина 64, зона
  21000–31000). Api-порты тест знает по построению — основа резолва держателя
  (§7 шаг A4).
- **Сборка образа**: `docker build -f docker/KafkaWorker.Dockerfile
  -t kafkaworker:e2e .` — по прецеденту Valkey (`ValkeyE2eEnvironment.BuildImageAsync`)
  и PgWorker. Обязательны: `[PHASE]`-строки ДО/ПОСЛЕ с таймингом (тихая фаза
  запрещена), полный вывод сборки — в `{ArtifactsDir}/process-build-*.log`,
  бюджет CLI 120 с; гейт `PGW_TEST_E2E_NOBUILD=1` — пропустить сборку.
- **Контейнеры воркеров** (оба): bind-mount `/var/run/docker.sock`,
  bind-mount TLS-пакета `/tls` (per-run CA + server + client, как у Valkey),
  `--add-host host.docker.internal:host-gateway`, env:
  `KafkaWorker__Etcd__Endpoints__0=http://host.docker.internal:{etcdPort}`,
  `KafkaWorker__AdvertisedClientHost=host.docker.internal`,
  `KafkaWorker__Api__AdvertiseUrl=https://host.docker.internal:{apiN}` —
  УНИКАЛЬНЫЙ каждому (одинаковые URL гасят оба дискавери-ключа),
  `KafkaWorker__Docker__Mode=Plain` + `Hosts__0` = unix-сокет, PortRange (окно),
  ускоренные циклы `KafkaWorker__Loops__ScanIntervalSec=1`,
  `KafkaWorker__Loops__KeepaliveSec=1`,
  `KafkaWorker__Loops__ErrorDelayMs=500`,
  `KafkaWorker__Thresholds__BrokerBootSec=100` (≤100 с — канон),
  `KFW_API_TLS_{CERT,KEY,CLIENT_CA}_PATH=/tls/...`,
  `ASPNETCORE_URLS=https://+:8080`.
- **Retry старта**: 3 попытки с пересозданием контура на распознанную гонку
  сети (по образцу Valkey); быстрый фейл с диагностикой.
- **Телеметрия**: `CollectDiagnosticsAsync(mark)` — docker logs+inspect etcd,
  ew1, ew2 и всех `kfw-{tag}*` ДО любого удаления; каталог
  `/tmp/pgw-e2e-artifacts-{runId}/`; `README-cleanup.txt` с own-only командами.
- **Teardown (`DisposeAsync`)**: телеметрия → (MarkFailed: только `docker stop`
  ew1/ew2/etcd, ничего не удалять) → иначе: kill/rm ew1, ew2, etcd → own-only
  `docker rm -f` остатков `{runId}`/`{tag}` → own-only удаление сетей:
  `kfw-en-{runId}` и осиротевших `kfw-net-*` СВОЕГО тега (фильтр содержит
  runId/tag, НЕ голый `kfw-*` — на хосте живёт dev-стенд) → **ассерт
  чистоты**: ни контейнера, ни сети, содержащих runId/tag (томов сценарий
  Kafka не порождает; после демонтажа движок их не оставляет).
- **`KafkaE2ePhase`** — порт паттерна `E2ePhase` (PgWorker): `WaitAsync(fx,
  phase, condition, budget, ct, progress)` со строками строгого формата,
  `[PHASE-TICK]`-тиками каждые 5 с, поллом 500 мс, slow-phase-сбором >60 с и
  failed-phase-сбором ДО возврата; дублирование в
  `{ArtifactsDir}/phases.log` с UTC-меткой. Прогресс-делегаты — только
  дешёвые etcd-чтения.
- **Хелперы**: etcd Get/Range/Put (обёртки над `EtcdGateway`),
  `RunDockerAsync(args)` (docker-CLI с выводом в исключение),
  `ListContainerNamesAsync(prefix)`.

### 6.2. Расширение Valkey: `ValkeyE2eEnvironment.StartTwoAsync` + `ValkeyE2ePhase`

- **`StartTwoAsync(slug)`** — новый статический метод: тот же каркас, что
  `StartAsync` (TLS-пакет, сеть, etcd, retry, телеметрия), но поднимает ДВА
  контейнера `vwk-ew1-{runId}` / `vwk-ew2-{runId}` образа `valkeyworker:e2e`
  (общий TLS-пакет `/tls`, общий etcd, общий PortRange — гонки portalloc между
  инстансами закрывает клэйм кластера). У каждого свой `FreePort()`-api-порт
  и свой `ValkeyWorker__Api__AdvertiseUrl`. Возвращает окружение, знающее
  ОБА контейнера: `Api1BaseUrl`/`Api2BaseUrl`, оба имени; телеметрия и
  teardown объезжают оба; ассерт чистоты проверяет оба имени. Поведение
  `StartAsync` (одиночный воркер) структурно не меняется — правки одиночного
  каркаса ограничены адаптацией к env-модели (§7.4).
- **`ValkeyE2ePhase`** — отдельный статический класс в E2e-папке (порт
  паттерна `E2ePhase`): тот же контракт `WaitAsync` с тиками. Существующий
  `ValkeyE2eEnvironment.WaitPhaseAsync` не трогаем (легальный прецедент);
  новые кейсы используют только `ValkeyE2ePhase`.

### 6.3. Кейс-классы (единицы изоляции — класс, окружение per класс)

- `src/tests/KafkaWorker.IntegrationTests/E2e/KafkaE2eTakeoverTests.cs` —
  один Fact: `Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates`.
- `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eTakeoverTests.cs` —
  один Fact: `Kill_ClaimHolderMidDeprovision_SurvivorFinishesNoDuplicates`.

Оба: гейт `PGW_TEST_DOCKER=1` (нет — `Assert.skip`), try/catch с `MarkFailed()`
и rethrow (e2e-launch §3). Комментарии кейсов — про env-модель
(«сертификаты нод — env, TLS-тома нет»); упоминаний helper'а/vwk-tls-writer
в новых тестах НЕ должно быть.

## 7. Сценарий K — Kafka takeover посреди provisioning

Операция-жертва: provisioning 3-брокерного KRaft-кластера (сид в стиле панели —
копия `KafkaClusterFixture.SeedClusterAsync(cluster, brokers: 3)`). Мотивация:
K4 `waiting-brokers` — стабильно длинное окно (бут 3 брокеров), K3 создаёт
контейнеры при живом `state`, K5 убирает `state` только в конце. У KafkaWorker
TLS-материал брокеров передаётся env при создании контейнеров — helper'ов и
томов в его операциях нет, «хвостов», держащих объекты после kill воркера,
модель не порождает.

Шаги (бюджеты фаз — в скобках; все ожидания — `KafkaE2ePhase.WaitAsync`):

- **A1 Arrange-окружение** [build ≤120 c CLI; старт ≤100 c]:
  `KafkaE2eEnvironment.StartAsync` → образ (§6.1) → ew1+ew2 → готовность
  ОБОИХ: `Range("/kafkaworker/api/")` = 2 И `Range("/kafkaworker/instances/")`
  = 2. Прогресс-тик: `api={n}, instances={n}`.
- **A2 Arrange-кластер**: сид `tkw{tag}` (config NOT_INITIALIZED,
  brokers:3, `state`+`resources` на broker1..3) — put'ами в etcd, как панель.
- **A3 Якорь «посередине»** [≤120 c]: ВСЕ три условия одновременно:
  1) контейнер `kfw-{C}-broker1` существует (`docker ps -a --filter name=`,
     проверка в теле условия — docker-CLI в тиках запрещён, тик_progress —
     только etcd-срез: work-журнал + наличие claims/portalloc);
  2) config `/kafka/clusters/{C}/config` ещё содержит `"state"` (K5 не дошёл);
  3) клэйм `/kafkaworker/claims/{C}` существует.
  Это K3..K5 — внутри K4-окна (бут брокеров десятки секунд).
- **A4 Act — резолв держателя и kill** [одномоментно]:
  1) Ассерт: клэйм существует (перечитать);
  2) `claims/{C}` → JSON `instance`;
  3) `/kafkaworker/api/{instance}` → JSON `url` → host-порт из url;
  4) порт сопоставляется с api-портом ew1/ew2, выделенным окружением: ровно
     одно совпадение → целевой контейнер; ни одного/два — FAIL с дампом
     (claims + api-ключи + известные порты) — это баг теста или окружения,
     ослаблять ассерт нельзя;
  5) `docker kill` целевого контейнера-держателя (контейнер без рестарта —
     воскресать не должен; доносит ТОЛЬКО выживший).
- **A5 Assert-takeover** [≤360 c; прогресс-тик: config.state, work-фаза,
  число ключей api/instances]:
  - I2: config без `"state"`; `endpoints` записан; `work/{C}`:
    `op=provision`, `phase=done`, `instance` = survivor (id из оставшегося
    api-ключа); brokers state = RUNNING;
  - I3: контейнеров `kfw-{C}-broker*` (вкл. stopped) ровно 3;
  - I1: `Range("/kafkaworker/api/")` = 1, `instances/` = 1;
    `claims/{C}.instance` = survivor;
  - I4: `/kafkaworker/portalloc/{C}` существует, содержит broker1..3;
  - I5: admin-клиент из etcd-дискавери (bootstrap = `endpoints` КАК ЕСТЬ:
    SAN сертов брокеров = advertised-хост `host.docker.internal`; с хоста
    macOS он резолвится в 127.0.0.1, published-порты работают;
    `admin_user`/`admin_password`, `ca_pem`, SASL_SSL) → `GetMetadata`/
    `DescribeCluster` видит 3 брокеров.
- **A6 Финал — демонтаж выжившим** [≤60 c]: put `config.state=TO_REMOVE`
  (стиль панели) → DeprovisioningProcess выжившего: контейнеров `kfw-{C}-*`
  нет (вкл. stopped), portalloc снят, сеть `kfw-net-{C}` удалена движком.
  Прогресс-тик: число живых `kfw-{C}-*` (etcd-срез: portalloc/work).
- **A7 Teardown**: `DisposeAsync` окружения (§6.1) — ассерт чистоты I7.

## 8. Сценарий V — Valkey takeover посреди демонтажа (deprovision)

Операция-жертва: ДЕМОНТАЖ кластера (фазы X0–X3) — решение пользователя
2026-10-09: CA-ротация в env-модели завершилась ~0,2 с (диагностический
прогон `~/pgw-diag-t20/2026-10-09-182646`: якорь phase-r 15:27:10.646 →
done 15:27:10.821) — окно kill у ротации исчезло. Демонтаж состоит из
docker-чистки (rm контейнера) и etcd-чистки (домен + координация) — по коду
~1–3 с; заявка TO_REMOVE не привязана к instance-id (гейт `claims.IsMine`),
каждый шаг идемпотентен (404 = успех) — выживший доигрывает с любой фазы.

Сверенные факты X-фаз (DeprovisioningProcess — якоря и ассерты строятся
только на них):
- DELETE `/api/valkey/clusters/{C}` → 202 синхронно: RMW-txn ставит
  `config.state=TO_REMOVE` (`DeleteClusterHandler`); исполняет процесс B
  первым тиком держателя (`ScanIntervalSec=1` в тесте);
- X0: `work/{C}` = `{"op":"deprovision","phase":"started","instance":<держатель>}`
  + снапшот «до»;
- X1: rm контейнера `vwk-{C}-node1` (stop + rm -f; 404 = успех);
- X2: `del --prefix /valkey/clusters/{C}/` (уносит и config с TO_REMOVE) +
  del координации: `claims`, `work`, `work/rotation`, `portalloc`,
  `rotations`, `ca_rotations`, `ticket_outcomes`;
- X3: verify config==null + `claims.ReleaseClusterAsync` (del ключа +
  revoke lease — не ждём TTL);
- journal-записи `done` у демонтажа НЕТ (канон arch/21 §5 B: запись после
  чистки воскресила бы удалённый work; координация `<C>` остаётся пустой) —
  терминальные факты: пустой домен + пустая координация + нет контейнеров;
- судьба `claims/{C}`: после kill держателя клэйм жив на lease мертвеца
  ≤15 с (TTL) — выживший пере-захватывает его тиками (`EnsureClaimed` при
  живом чужом клэйме — ожидание), затем снимает сам (X2-del + X3-release);
  после финиша ключа нет.

Шаги:

- **A1 Arrange-окружение** [build ≤120 c CLI; старт ≤100 c]:
  `ValkeyE2eEnvironment.StartTwoAsync` → ew1+ew2 (`vwk-ew{1,2}-{runId}`) →
  готовность обоих: `Range("/valkeyworker/api/")` = 2 И
  `Range("/valkeyworker/instances/")` = 2.
- **A2 Arrange-кластер** [wait-worker ≤30 c; wait-provision ≤100 c]:
  mTLS-клиент к `Api1BaseUrl` → `POST /api/valkey/clusters` (name=`tvw{tag}`,
  nodes=1, defaults) → 201 → `nodes/node1/state=RUNNING` + контейнер
  `vwk-{C}-node1` жив (env-модель: нода поднята с `VALKEY_TLS_*`, без тома —
  доп-проверка env контейнера). Датаплейн-проверка (I5-часть): RESP `PING`
  admin-кредом по TLS (endpoints + ca_pem + admin-кред из etcd) — `PONG`.
- **A3 Act-заявка + якорь «посередине»** [DELETE сразу; якорь ≤30 c]:
  1) `DELETE /api/valkey/clusters/{C}` → 202 — mTLS-клиент ЛЮБОГО из
     ew1/ew2 (демонтаж ведёт держатель клэйма — адресат заявки не важен);
  2) якорь — ВСЕ условия одновременно (kill гарантированно внутри демонтажа;
     «мимо» — демонтаж уже завершён — исключено условием а):
     а) config `/valkey/clusters/{C}/config` существует и содержит
        `"state":"TO_REMOVE"` (живёт от DELETE-202 до X2 — почти весь
        демонтаж);
     б) клэйм `/valkeyworker/claims/{C}` существует;
     в) демонтаж в полёте: контейнер `vwk-{C}-node1` существует (docker ps —
        тело условия, docker-CLI в тиках запрещён) ИЛИ `work/{C}` существует
        с `op=deprovision` (ставится в X0, живёт до X2).
     Покрытие окна непрерывно: до X0 — config+клэйм+контейнер; X0..X1 —
     + work; X1..X2 — config+клэйм+work. Прогресс-тик: config.state +
     claims + наличие work (etcd-срез).
- **A4 Act — резолв держателя и kill**: идентично §7 A4 (префикс
  `/valkeyworker`, контейнеры `vwk-ew{1,2}-{runId}`).
- **A5 Assert-takeover** [≤60 c; прогресс-тик: наличие config/work,
  api/instances]: I2 — ПО ПОСТРОЕНИЮ + лог (journal-полл непоймаем:
  survivor проходит X0→X2 одним тиком <0,5 с — короче полла 500 мс;
  доказано прогоном e055e1e0: 120 поллов за 60 с ни разу не видели
  `work.instance=survivor`; факт из лога ew1: txn-захват клэйма → X0 put →
  X1 rm → 8×X2 del → X3, все etcd-опы 0,8–3,6 мс). Цепь доказательства:
  1) пре-факт (на момент kill, сверяется в A3/A4): демонтаж заявлен (config
     `state=TO_REMOVE`) И X0 жертвы НЕ начат — `work/{C}` отсутствует ИЛИ
     `op != deprovision` (ассерт на момент kill; хвост прошлого цикла —
     `op=provision`/`phase=done` — демонтажом не является);
  2) kill доказательно держателя клэйма (A4), контейнер без рестарта;
  3) I1: после kill единственный живой инстанс — survivor
     (`Range("/valkeyworker/api/")` = 1 И `instances/` = 1, ассерт С
     БЮДЖЕТОМ ожидания ≤20 с — дискавери мертвеца гаснет с его lease
     ≤15 с, мгновенный ассерт неверен);
  4) терминальная чистота: контейнер `vwk-{C}-node1` ровно 0 (вкл.
     stopped; I3 — helper-контейнеров и TLS-томов префикса нет, env-модель),
     префикс `/valkey/clusters/{C}/` пуст, координация пуста (`work`,
     `portalloc`, `rotations`, `ca_rotations`, `ticket_outcomes`,
     `claims`; I4 — portalloc снят), wait-clean ≤15 с от re-захвата клэйма
     (целевой бюджет демонтажа; превышение без объяснения по логам — фейл
     фазы, разбор по телеметрии) — других исполнителей, кроме survivor,
     нет;
  5) документальный факт: ПОСЛЕ терминала `docker logs` survivor'а содержит
     подстроку «deprovision {C}» — имя кластера `tvw{tag}` guid-уникально,
     жертва убита до X0: эту строку мог записать только survivor
     (docker-CLI — однократный вызов после терминала, вне тиков, не полл).
- Отдельный финальный демонтаж (бывший A6) ОТСУТСТВУЕТ: жертва и есть
  демонтаж — домен чист средствами survivor'а.
- **A7 Teardown**: `DisposeAsync` (обе воркер-ноды + etcd + сеть + ассерт
  чистоты по тегу — прецедент `ValkeyE2eEnvironment`, расширенный на ew2).

## 9. Регресс-обязательства (адаптация к env-модели)

- **Интеграции Valkey** (`src/tests/ValkeyWorker.IntegrationTests/Valkey/`) —
  все адаптируются и обязаны быть зелёными на Release:
  - `ProvisioningTests` (args-ассерт `args[0]=="valkey-server"` → обёртка
    `sh -c` с `exec valkey-server`; доп-ассерт env `VALKEY_TLS_*`);
  - `DeprovisioningTests` (сверка «TLS-volume удалён» → доп-ассерт «нет
    env/объектов, чистота домена»; шаг remove-tls-volume исчез);
  - `TlsMigrationTests` (премиграционный контейнер — старый канон args без
    TLS-хвоста, БЕЗ env; после миграции — env + обёртка);
  - `CaRotationTests` (R-факт по env; NEW `VALKEY_TLS_CA` в env ноды);
  - `SupervisionTests` (env-сверка надзора; легаси-чистка тома;
    пересоздание «env отсутствует/невалиден»);
  - `ConvergeTests`, `AclMatrixTests`, `ResourcesAutorecreateTests`,
    `RotationTests`, `RotationTimeoutTests`, `MetricsCollectorTests` —
    правки точечные (фикстура/teardown без RemoveTlsVolumeAsync);
  - `ValkeyClusterFixture` — чистка: убрать `RemoveTlsVolumeAsync`-шаг,
    `NewTlsProvisioner` → фабрика env-набора; комментарий «TLS-volume» —
    переписать под env.
- **Api-тесты**: `MetricsDomainTests` (teardown `RemoveTlsVolumeAsync` —
  убрать).
- **E2E Valkey**: 3 кейса `ValkeyE2eLifecycleTests` — адаптируются (args =
  обёртка с прежними путями `/tls/node.crt` и флагами `--tls-*` — ассерты
  `Contain("--tls-cert-file")` остаются валидны как вхождения в строку
  обёртки; кавычечные ассерты вида `"\"6379\""` — заменить на форму обёртки;
  ассерты «ни тома vwk-<C>-tls» остаются валидны — том не создаётся) и
  обязаны быть зелёными.
- **Юниты ValkeyWorker** (`src/tests/ValkeyWorker.UnitTests/`):
  - `Docker/SwarmClusterDriverTlsTests` — УДАЛЯЕТСЯ (тестирует демонтируемую
    volume-механику драйвера); НОВОЕ: тесты env-передачи в spec (Plain и
    Swarm) + `NodeEnvAsync`-роутинга — на усмотрение плана, минимум —
    покрытие `BuildCmd`/`BuildNodeTlsEnv`/`IsValidNodeEnv`;
  - `Docker/ClusterDriverLimitsTests` — фейк-движок чистится от
    демонтируемых методов;
  - `Provisioning/CaRotatorTests` (ассерт `spec.TlsVolume==TlsVolumeName` →
    env-набор от `ca_next_*`), прочие — по компиляции/поведению;
  - `Fakes/Fakes.cs` — `FakeClusterDriver`: `TlsVolumes`-хранилище →
    env-хранилище (Get/запись env, NodeEnvAsync).
- **Юниты PgWorker**: фейк-реализации `Put/GetVolumeArchiveAsync` удаляются
  из 5 файлов (§5); поведение тестов не меняется.
- `dotnet build src/PgWorker.slnx` — 0 warnings (`TreatWarningsAsErrors`).

## 10. Стенд и документация

- **Стендовые чеки** (`dev-stand/adminpanel/checks/`): `51-valkey-api.sh`
  (строки 159–170) — ассерты `docker volume inspect vwk-$TAG-tls` (ожидание
  удаления тома) заменить: том в модели не существует, ассерт чистоты —
  «контейнер/ключи удалены» (проверка отсутствия тома не нужна; комментарий
  про TLS-volume — переписать). Остальные чеки том не упоминают (`05-seed`,
  `90-down` — только контейнер `vwk-demo-node1`).
- **Runbook** (`docs/runbook.md`): §«TLS-подключения к Valkey» (стр. 298
  «volume vwk-<C>-tls» → env-модель: «сертификаты доставляются env ноды,
  перевыпуск — пересозданием контейнера») и §«Ротация CA» (стр. 310 —
  «(R) пересоздание node1 с сертом от NEW (env ноды)»). Добавить абзац:
  осиротевшие `vwk-*-tls` старой модели (демонтаж до миграции) — удаляются
  вручную `docker volume rm`; живые кластеры чистит воркер сам.
- **deploy/docker-compose.yml**: без изменений (`vw-snapshots`/`vw-api-tls` —
  per-install тома API воркера, не per-cluster TLS-том).

## 11. Сквозные правила (оба E2E-кейса)

- **Язык логов/строк** — русский; идентификаторы — английские.
- **Таймауты**: бюджеты фаз выше; `BrokerBootSec`/`NodeBootSec` ≤100 с; любое
  ожидание — конечный бюджет, провал — `CollectDiagnosticsAsync` до мутаций
  teardown'а и фейл с контекстом.
- **Параллелизм**: классы самодостаточны (свои контуры); в мерж-гейте каждый
  класс — отдельный прогон (§12); канон потолка N=5 на PgWorker-серию этими
  кейсами не расширяется (они в других проектах).
- **Зачистка сетей**: own-only по runId/тегу; страховочный гейт МЕЖДУ сериями
  (после финальной строки каждого прогона) — контроль
  `docker network ls | grep kfw-net-tkw` (остатки серии — только при нулевом
  числе живых контуров серии) и `docker network prune -f` — уровень хоста, не
  теста. У Valkey per-cluster сетей нет (nodes=1, standalone).

## 12. Фазы работ (dev-flow)

1. **Ф2 (plan)**: разбить на таски в порядке зависимостей:
   (а) arch-правка `arch/21` (+проверка `arch/20`) — отдельный коммит;
   (б) движок ValkeyWorker: `NodeArgsBuilder.BuildCmd` + `NodeTlsProvisioner`
   (BuildNodeTlsEnv/IsValidNodeEnv) + драйверы (Env в spec, `NodeEnvAsync`,
   демонтаж TLS-volume API) + процессы (V3/R/X1/надзор/T) + юниты;
   (в) демонтаж helper-механики Shared.Docker + фейки (Valkey ×3, PgWorker ×5,
   Shared.Docker.UnitTests) — ПОСЛЕ (б), одним куском с grep-гейтом §4.3;
   (г) адаптация интеграций Valkey + 3 E2E lifecycle кейсов + чеки + runbook;
   (д) E2E-инфраструктура Kafka + Kafka-кейс;
   (е) `StartTwoAsync` + `ValkeyE2ePhase` + Valkey-кейс;
   (ж) roadmap-правка + мерж-гейт (§14). Каждая таска — зелёный локальный
   прогон своей серии + зачистка после КАЖДОЙ серии.
2. **Ф3 (code)**: к прод-части (б)–(г) применяется TDD (юниты BuildCmd/
   IsValidNodeEnv/сверки — сначала тесты); к самим E2E TDD не применяется
   (они и есть проверка); после каждого кейса — контрольный прогон, разбор
   артефактов, зачистка сетей/контейнеров.
3. **Ф4 (review/merge)**: прогон §13, снятие тега t20, ревью.

## 13. Критерии приёмки (AC)

- **AC1 (прод)**: env-модель в коде и arch: нода Valkey несёт
  `VALKEY_TLS_{CERT,KEY,CA}` + cmd-обёртку; в `IClusterDriver`/`DockerEngine`
  нет TLS-volume/helper-методов (`EnsureTlsVolumeAsync`, `Put/GetTlsArchiveAsync`,
  `RemoveTlsVolumeAsync`, `Put/GetVolumeArchiveAsync`, `CreateHelperAsync`);
  grep по `src` не находит прод-потребителей демонтированного; `arch/21`
  описывает env-модель (тома/helper нет).
- **AC2 (прод)**: миграция живого кластера старой модели: нода без env →
  пересоздание надзором с env ≤2 тиков; осиротевший `vwk-<C>-tls` удалён
  легаси-чисткой (юнит/интеграция-покрытие).
- **AC3**: `KafkaE2eTakeoverTests.Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates`
  зелёный на Release с `PGW_TEST_DOCKER=1`: kill ДОКАЗАТЕЛЬНО попал в держателя
  клэйма (резолв по etcd-фактам перед kill, ассерт «клэйм существует» до kill),
  все инварианты I1–I7 выполнены.
- **AC4**: `ValkeyE2eTakeoverTests.Kill_ClaimHolderMidDeprovision_SurvivorFinishesNoDuplicates`
  зелёный на тех же условиях: та же доказательность kill, инварианты I1–I7;
  жертва — демонтаж: доигран выжившим (journal-факт instance=survivor +
  терминальная чистота домена/координации), wait-clean ≤15 с.
- **AC5**: три кейса `ValkeyE2eLifecycleTests` зелёные (адаптированные под
  env-модель); интеграции/юниты §9 зелёные; `dotnet build src/PgWorker.slnx` —
  0 warnings.
- **AC6**: полная самоочистка: после зелёного прогона обоих кейсов на хосте
  нет ни контейнеров, ни сетей их runId/тегов (ассерт чистоты зелёный);
  упавший прогон оставляет stop-без-удаления + `README-cleanup.txt`.
- **AC7**: телеметрия: `[PHASE]`/`[PHASE-TICK]`-строки (тик 5 с) в журнале и
  `phases.log`; сбор логов всех контейнеров в `/tmp/pgw-e2e-artifacts-{runId}/`;
  любая фаза >60 с объясняется отчётом по логам (или отсутствует).
- **AC8**: diff за пределами границ §5 пуст (прод KafkaWorker/PgWorker/
  AdminPanel/Shared.Core,Etcd,Tls,Metrics — нет изменений).

## 14. Мерж-гейт

Прогоны docker-серий — ОТДЕЛЬНЫМИ запусками (точные имена классов — фильтр
`~Takeover` цепляет 7 чужих тестов), каждый со своей финальной строкой и
зачисткой после:

```
# 1) юниты затронутых проектов (без docker)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.UnitTests"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~PgWorker.UnitTests"
# 2) интеграции Valkey (docker) + lifecycle E2E + новые кейсы — раздельные серии
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyWorker.IntegrationTests"
# финальная строка → зачистка серии
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~KafkaE2eTakeoverTests"
# финальная строка → зачистка серии → контроль kfw-net-tkw*
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~ValkeyE2eTakeoverTests"
# финальная строка → зачистка серии
# 3) Shared.Docker тронут → кейс-маркер PgWorker docker-E2E на свежем Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
# финальная строка → зачистка серии
```

Плюс `dotnet build src/PgWorker.slnx -c Release` без warnings. Roadmap-правка —
ОДНИМ мерж-коммитом (канон AGENTS.md):
- `arch/roadmap/reliability.md`: удалить пункт `t20-kfw-vwk-takeover-e2e`
  (вкл. «P4 (t20–t22)» → «P4 (t21–t22)»);
- `arch/roadmap/reliability-report.md`: строка t20 из «Осталось» удалена
  (вкл. шапку «(`t20`, `t21`)» → без t20), сводка R — без t20.

## 15. Риски и СТОП-условия

| # | Риск | Митигация |
|---|---|---|
| Р1 | Сборка `kafkaworker:e2e` из продового Dockerfile при холодном кэше дольше 120 с CLI-бюджета | Прецедент Valkey/PgWorker; `[PHASE]`-метки ДО/ПОСЛЕ + полный вывод в лог-файл; тёплый кэш — секунды; при устойчивом переполнении — СТОП и пересогласование (НЕ изобретать свой Dockerfile самовольно) |
| Р2 | Окно kill у Valkey: жертва-ротация ОТМЕНЕНА (решение пользователя 2026-10-09) — в env-модели ротация ~0,2 с (диагностический прогон `~/pgw-diag-t20/2026-10-09-182646`: якорь phase-r 15:27:10.646 → done 15:27:10.821), окно исчезло; жертва = демонтаж (X0–X3). Исход: окно демонтажа тоже <полла — X-фазы проходятся одним тиком <0,5 с (прогон e055e1e0: txn-захват → X0 → X1 rm → 8×X2 del → X3, etcd-опы 0,8–3,6 мс) — journal-наблюдение заменено КОНСТРУКТИВНЫМ доказательством I2 (kill-до-X0 + единственность survivor + терминальная чистота + документальный лог survivor'а; решение пользователя 2026-10-09). СТОП-блокер «окно <1 с» снят этим решением. Ассерт «посередине» на момент kill обязателен (config TO_REMOVE + клэйм + контейнер/work — по сверенным фактам §8) |
| Р3 | Держатель клэйма резолвится, но url-порт не совпадает ни с одним известным api-портом | Ассерт-fail с полным дампом (claims/api-ключи/порты) — причина в тесте/окружении; чинить тест, не ослаблять проверку |
| Р4 | Осиротевшие `kfw-net-tkw*` от убитых прогонов исчерпают подсети | own-only чистка в teardown + демонтаж движком (§7 A6) + ассерт чистоты + контроль между сериями (§11) |
| Р5 | Нагрузка docker-хоста растягивает K4/бут → бюджеты фаз | Бюджеты 120–360 с с прогресс-тиками каждые 5 с; slow-phase >60 с — автосбор логов и отчёт; перезапуск упавших — только после анализа (e2e-launch §4) |
| Р6 | Коллизия PortRange с dev-стендом/чужими сериями | Окна `FreePortWindow` (21000–31000, вне зоны стенда) + api/etcd-порты из эфемерной зоны; никаких литералов |
| Р7 | Entry point пина `valkey/valkey` ведёт себя иначе ожидания (gosu-ветка/аргументы) — обёртка не раскатывает серты, нода не стартует | Точка верификации на первом шаге фазы кода: ручной прогон обёртки на пине образа (docker run + PING по TLS) ДО вписывания в процессы; расхождение с §4.1 — СТОП и пересогласование обёртки |
| Р8 | Cmd-обёртка/shell-экранирование args (пустой `--save ''`, спецсимволы) собрана неверно → нода стартует с битыми args | Юнит-тесты `BuildCmd` (TDD): канонический набор args → строка обёртки → обратный парсинг; интеграционный ProvisioningTests ловит фактический boot |
| Р9 | Случайная генерация серта в env ломает идемпотентность (бесконечные пересоздания) | Сверка ТОЛЬКО по валидности env (IsValidNodeEnv) + детерминированная обёртка (§4.1); юнит-тест «валидный env не триггерит пересоздание» |
| Р10 | Скрытый прод-потребитель helper-механики/TarArchive (не ValkeyWorker) | grep-гейт §4.3 на фазе кода перед демонтажом; найден — СТОП, демонтаж только после анализа с пользователем |
| Р11 | Демонтаж до миграции оставляет осиротевший `vwk-<C>-tls` навсегда | Переходный случай: живые чистит надзорная легаси-чистка (AC2); прочие — runbook-инструкция ручной чистки (§10) |
| Р12 | Нужна правка в НЕ-граничных местах (панель читает том, KafkaWorker-прод, etcd-контракт) | Проверено спекой: панель — только ca_pem из etcd; если фаза кода опровергнет — СТОП и пересогласование границ |

**СТОП-условия (прекратить и доложить)**: (1) нужна правка за пределами
границ §5 (прод KafkaWorker/PgWorker/AdminPanel, контракт etcd arch/20,
поведение вне TLS-механики); (2) скрытый прод-потребитель helper-механики —
демонтаж без анализа; (3) entrypoint/обёртка на реальном образе не работает
как §4.1 (Р7); (4) окно kill у Valkey стабильно недостаточно — пересогласование
жертвы; (5) каноны E2E (изоляция/порты/телеметрия) невозможно соблюсти без
компромисса; (6) резолв держателя недетерминирован и не чинится правкой теста.

## 16. Оценка длительности кейсов (тёплый кэш образов)

- **Kafka**: build ~30–60 с + контур ~30 с + provisioning-до-якоря ~10–30 с +
  takeover-докрутка (TTL ≤15 с + K4-бут ~30–90 с) + ассерты + демонтаж
  ~10–30 с ≈ **3–5 мин**.
- **Valkey**: build ~20–40 с + контур ~30 с + provisioning ~20–60 с + якорь
  ~0,5–3 с + takeover (TTL клэйма ≤15 с + доигрывание демонтажа ~2–5 с) +
  ассерты чистоты ≈ **2–3,5 мин** (жертва-демонтаж короче ротации с
  R-пересозданием; отдельного финального демонтажа нет).
- Холодная пересборка образов (правка исходников → инвалидация sdk-слоя)
  добавляет минуты — контроль по [PHASE]-таймингу.
- Прод-часть: arch ~0,5 дня; движок+юниты ~1–1,5 дня; демонтаж+фейки ~0,5 дня;
  адаптация интеграций/E2E/чеков/runbook ~1 день; E2E-инфра и кейсы ~1 день.

## 17. Открытые вопросы (не решаются самовольно)

- **Осиротевшие легаси-томы демонтажа-до-миграции**: спека решает «живые —
  надзор, мёртвые — ручная чистка по runbook» (Р11). Если пользователь
  захочет автоматическую чистку мёртвых (например, разовый проход по
  `docker volume ls vwk-*-tls` при старте воркера) — это отдельное решение:
  остаточных кластеров в etcd нет, а воркер с docker-демоном видит тома всех
  кластеров хоста, включая чужие/живые у другого инстанса — риск сноса
  чужого. По умолчанию НЕ делаем.
- **Объём демонтажа TarArchive**: спека решает «удаляем вместе с helper-
  механикой» (мёртвый код). Если план найдёт потребителя вне ValkeyWorker —
  TarArchive остаётся (гейт §4.3/Р10), тогда правка этой спеки не нужна,
  фиксируется в плане.
