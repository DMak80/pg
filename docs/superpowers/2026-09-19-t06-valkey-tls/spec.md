# t06-valkey-tls — TLS клиентских подключений Valkey-кластеров

- **Дата**: 2026-09-19
- **Roadmap**: [`arch/roadmap/valkey.md`](../../../arch/roadmap/valkey.md), тег `t06-valkey-tls` (снимается тем же коммитом мержа в `main` — мерж-гейт, §10)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/feat-t06-valkey-tls` (ветка `feat-t06-valkey-tls`); **Puzzle** — `/Users/demakaev/ZCodeProject/Puzzle`, ветка `feat-t06-valkey-tls` (клиентская сторона — решение §1.1)
- **Тип**: новая функциональность — транспорт безопасности домена (per-cluster CA, tls-port, дискавери-ключ `ca_pem`, авто-миграция)
- **Канон**: контракт — [`arch/20-valkey-clusters.md`](../../../arch/20-valkey-clusters.md), оркестратор — [`arch/21-valkeyworker.md`](../../../arch/21-valkeyworker.md), панельная проекция — [`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md) §11. **Образец** — kafka t03: `arch/16-kafkaworker.md` §2.3 (PKI), §5 M (миграция), §2.1 (advertised/SAN); спека `docs/superpowers/2026-09-04-t03-kafka-security/spec.md`.

## 1. Цель

Перевести клиентские подключения Valkey-кластеров с открытого docker-трафика на
**TLS** (шифрование + аутентификация сервера per-cluster CA), закрыв риск R5
arch/21 (пароли ACL и данные в доверенной, но открытой сети):

1. **Per-cluster CA**: ключи `ca_pem`/`ca_key` в `/valkey/clusters/<C>/`
   (ensure воркером при provisioning, txn put-if-absent — единый механизм с
   кредами); `ca_pem` — точка дискавери внешних клиентов.
2. **Серверный серт ноды**: CN=`node<k>` + SAN advertised-хоста (правило
   arch/21 §2 / образец 16 §2.1); генерация воркером (CertificateRequest .NET,
   порт kafka `ClusterPki`), подпись `ca_key`.
3. **tls-port контейнера**: тот же клиентский порт portalloc — контейнерный
   6379 слушает TLS (`--tls-port 6379 --port 0`), plain-порт закрыт;
   `endpoints`/portalloc не меняются (адреса стабильны, меняется только
   транспорт). `--tls-auth-clients no` — принципалы из ACL (образец 16 §2.3
   «клиентская аутентификация listener'ов — none»).
4. **Доставка сертов в контейнер**: named volume `vwk-<C>-tls` — воркер кладёт
   `node.crt`/`node.key`/`ca.pem` через Docker API (PUT /archive, tar) до
   старта контейнера; mount → `/tls`; аргументы ссылаются на файлы. Volume
   переживает пересоздания контейнера, удаляется при демонтаже кластера (X1).
5. **Дискавери**: `GetClientConfig()` внешней библиотеки после t06 отдаёт
   `ssl=true` + CA; контракт кред/endpoints не меняется — добавляется только
   чтение `ca_pem` (обратная совместимость читателя arch/20 §4).
6. **Авто-миграция** существующих кластеров (поднятых без TLS): отдельный шаг
   Active-ветки по образцу kafka M (детект → ensure CA → пересоздание
   контейнера → PING по TLS → RUNNING); nodes=1, persistence off — окно
   миграции = одно пересоздание контейнера (секунды; кеш восполним).
7. **Клиентская сторона (Puzzle) — этой же задачей** (решение §1.1):
   `HA.Valkey` читает `ca_pem` и вычисляет `ssl`; `App.Valkey` строит
   TLS-соединение StackExchange.Redis с доверием CA.
8. **Панель и стенд — этой же задачей**: панель читает `ca_pem` (internal-стор,
   образец kafka) и live-пробы PING ходят по TLS (SslStream + валидация CA);
   стендовые чеки dev-stand — TLS-вызовы.

### 1.1. Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Скоуп клиентской стороны Puzzle (HA.Valkey `SslValue=false` константой, CA-поля нет) | **Всё в t06 одной задачей**: pg-worktree (воркер/панель/стенд/контракт) + Puzzle-ветка (HA.Valkey: чтение `ca_pem`, вычисление `ssl`, поле CA; App.Valkey: доверие CA в ConfigurationOptions); один spec/plan на оба репозитория |
| Существующие кластеры без TLS | **Авто-миграция** (ensure CA + пересоздание контейнера; заявленный breaking change для TLS-неготовых клиентов — прецедент kafka t03) |
| Порт TLS | **Тот же client-порт portalloc**: контейнерный 6379 → `--tls-port 6379 --port 0`; endpoints/portalloc/диапазон 17000–17999 не меняются |
| Доставка сертов в контейнер (valkey требует файлы; inline-PEM невозможен) | **Named volume `vwk-<C>-tls` + Docker tar-archive API**: воркер пишет файлы до старта контейнера, mount → `/tls` |
| Панель/стенд | **В скоупе t06**: панель — ca_pem + SslStream-пробы; чеки dev-stand — TLS (панель и воркер обязаны мигрировать одним релизом) |
| Механизм миграции в Active-ветке | **Отдельный шаг до надзора** (по образцу kafka M): собственные journal-фазы `op=migrate-tls`, видимость в `work/<C>` |

Решения по букве roadmap/образца kafka (не оспорены, фиксируются): CA
RSA-2048, 10 лет, subject уникален на генерацию (отпечаток ключа — фикс t07
kafka про бандлы openssl); серт ноды 10 лет RSA-2048, EKU ServerAuth; SAN —
только advertised-хост (DNS либо IP; inter-node-алиаса нет — standalone);
`ca_next_*` НЕ вводятся (окно двойного доверия — ротация CA по потребности,
roadmap-пункт §10.2); клиентские сертификаты к Valkey — нет (ACL-креды);
TLS-тюнинг (шифры/версии) — дефолты valkey/.NET.

> **Примечание исполнения (2026-09-20, решение пользователя).** Эндпоинты
> volume-archive API (`PUT|GET /volumes/{name}/archive`) на демоне без swarm
> отвергают локальные тома (503 «volume update only valid for cluster
> volumes», проверено на Docker Engine 29.8.0; конфиг-фиксы нет). Все решения
> этой строки сохраняются (named volume `vwk-<C>-tls`, tar, файлы до старта,
> mount → `/tls`); меняется только транспорт внутри `DockerEngine`:
> короткоживущий helper-контейнер (образ ноды) с volume в `/mnt` — запись
> exec'ом с `chmod` из заголовка tar, чтение `GET /containers/<helper>/archive`
> сквозь mount с переупаковкой tar в корень. Следствие из образа: процесс ноды
> стартует НЕ root (entrypoint gosu valkey, uid 999) — файлам ключа ставится
> `0644` (изоляция секрета — периметром контейнера: volume монтируется только
> в контейнер ноды).

## 2. Принципы

1. **arch-first**: канон arch/20, arch/21, adminpanel/02 §11 обновляется ДО
   кода (фаза 1); спека описывает отражение контракта в коде. Новый пробел при
   реализации — сначала arch/, потом код, с явной фиксацией.
2. **Образец-канон — kafka t03/t07**: PKI (ClusterPki → ValkeyPki), SAN-правило,
   ensure CA единым механизмом с кредами, миграционный детект до всех
   Active-шагов, панельный internal-стор ca_pem. Отличия фиксируются явно:
   файлы-серты вместо PEM-env (volume+tar), один порт вместо listener-карты,
   ACL вместо JAAS, nodes=1 (миграция = одно пересоздание, без «рестарт
   кластера разом»).
3. **Переиспользование `Shared.*`/kafka-паттернов**: CertificateRequest .NET
   без внешних инструментов; SslStream + X509Chain для проб; никаких новых
   внешних пакетов ни в pg, ни в Puzzle (StackExchange.Redis уже подключён).
4. **Идемпотентность каждого шага; journal-before-manipulations; операции
   только под живым клэймом `<C>`; takeover ≤ TTL + тик**. Миграция и
   provisioning перепроверяют факт (ключи CA есть? контейнер с TLS-args?
   PING по TLS отвечает?).
5. **Кеш восполним**: пересоздание контейнера в любой момент безопасно
   (аргументы/серты пересобираются из etcd-факта + CA; persistence off —
   задокументированное поведение).
6. **Обратная совместимость дискавери**: появление `ca_pem` не ломает
   существующих читателей (unknownKeys-толерантность arch/20 §5); смена
   `ssl=false→true` — заявленный breaking change одного релиза (панель,
   воркер, Puzzle-библиотеки обновляются тем же релизом — все стороны в скоупе
   t06).
7. **Тесты — по канонам проекта** (AGENTS.md): динамические порты, guid-изоляция,
   полный teardown + ассерт чистоты (вкл. volume `vwk-*`), NodeBootSec ≤ 100,
   зачистка серий, E2E-телеметрия (`docs/e2e-launch.md`), AAA-комментарии.
8. **Язык**: документация/комментарии — русские; идентификаторы — английские;
   .NET 10, Nullable, TreatWarningsAsErrors; пакеты — Directory.Packages.props.

## 3. Правки канона arch/ (вносятся в этом worktree ДО кода)

### 3.1. `arch/20-valkey-clusters.md` (контракт etcd)

- **§2 таблица ключей**: + `ca_pem` (PEM публичного CA-серта, точка дискавери;
  пишет воркер ensure'ом, панель читает для проб) + `ca_key` (PEM PKCS#8
  приватного ключа CA, подпись сертов нод; секрет воркера, панель НЕ читает).
  Сноска «отличия от kafka» обновляется: `ca_pem`/`ca_key` есть (t06);
  `ca_next_*` нет (ротация CA — roadmap).
- **§2.1 примеры**: канонические значения `ca_pem`/`ca_key` — PEM одной
  строкой с `\n` (формат kafka arch/15 §2.1).
- **§4 клиентский дискавери**: + шаг чтения `/valkey/clusters/<C>/ca_pem` →
  TLS-доверие; после t06 `GetClientConfig()` отдаёт `ssl=true` + CA (правило:
  `ssl=true ⟺ ca_pem` прочитан); неполный набор (нет `ca_pem` у
  Active-кластера) — переходное/миграционное состояние (см. §5).
- **§5 сбои**: + битый PEM в `ca_pem`/`ca_key` → parseError + warning
  `valkey-key-malformed`; Active-кластер без `ca_pem` — critical-алерт
  `valkey-security-missing` (миграция ещё не доиграна/ключ потерян).

### 3.2. `arch/21-valkeyworker.md` (оркестратор)

- **Преамбула/границы**: TLS клиентских подключений — входит (t06); из границ
  убирается, вместо неё — «ротация CA/сертов (окно двойного доверия) —
  roadmap». Коллектор метрик t05 (когда появится) обязан ходить по TLS-транспорту
  воркера (§7).
- **§2 модель размещения**: нода слушает `--tls-port 6379 --port 0`
  (тот же клиентский host-порт из portalloc; plain закрыт); аргументы:
  `--tls-cert-file /tls/node.crt --tls-key-file /tls/node.key
  --tls-ca-cert-file /tls/ca.pem --tls-auth-clients no` (+ `--tls-replication
  no` — реплик нет); серт CN=`node<k>`, SAN advertised-хоста (DNS|IP по
  правилу §2), 10 лет, RSA-2048, подпись `ca_key`; доставка — volume
  `vwk-<C>-tls` (воркер пишет файлы Docker tar-API до старта, mount → `/tls`;
  volume переживает пересоздания контейнера, удаляется в X1 демонтажа);
  перечисление объектов домена дополняется volume.
- **§3.1/§3.2**: читаемые/пишемые ключи + `ca_pem`/`ca_key` (ensure txn
  put-if-absent при provisioning V2 и миграции).
- **§4 секреты**: + группа CA (`ca_key` — подпись, `ca_pem` — дискавери;
  компрометация etcd = зона доверия контроль-плейна, образец kafka R10).
- **§5 процессы**:
  - классификация Active-ветки: **миграция TLS — первым шагом** (детект до
    надзора; по образцу 16 §5 M): Active-кластер без `ca_pem`/`ca_key` ИЛИ
    контейнер без TLS-args (`--tls-port` в args не найден) → TlsMigrator;
  - **A. Provisioning V2**: ensure кредов + CA (одна txn put-if-absent по
    отсутствующим); **V3**: генерация серта ноды (подпись CA) + запись файлов
    в volume + контейнер с TLS-args; **V4**: PING по TLS (SslStream, доверие
    `ca_pem`);
  - **новый процесс T. TlsMigrator (t06)**: `T0` claim + journal
    (op=migrate-tls) + снапшот «до»; `T1` ensure CA (+ креды re-read);
    `T2` пересоздание контейнера с каноническими TLS-args (серт в volume,
    порт/лимиты те же — portalloc не меняется); `T3` PING по TLS →
    state=RUNNING; снапшот «после»; journal done. Идемпотентность по факту
    (ключи есть? args TLS? PING TLS?); отработавший миграцию кластер
    неотличим от поднятенного канонически (повторный детект — no-op);
  - **C/D/E** работают по TLS-соединению (RESP-клиент с CA).
- **§6/§7**: пробы/команды воркера — TLS (SslStream + ручная валидация цепочки
  против `ca_pem` + SAN-хост); коллектор t05 — на том же транспорте.
- **§9 риски**: R5 переписан (TLS есть; остаточный вектор — внутри закрытой
  сети контроль-плейна); +R8-аналог (SAN vs смена `AdvertisedClientHost` —
  серт пересобирается при каждом пересоздании по тому же правилу); +R9-аналог
  (окно миграции: TLS-неготовые клиенты получают отказ после пересоздания —
  заявлено релизом t06); +R10-аналог (`ca_key` в etcd — зона доверия,
  ротация CA — roadmap).

### 3.3. `arch/adminpanel/02-etcd-contract.md` (панель, §11)

- **§11.1**: панель читает `ca_pem` в internal-стор (рядом с admin-кредами,
  наружу не отдаёт); live-пробы PING — TLS (SslStream + доверие `ca_pem`).
- Строка `admin_user`/`admin_password` таблицы дополняется `ca_pem`; ошибок
  чтения/валидации PEM — толерантность как у кредов.

### 3.4. Puzzle-канон (`docs/01.21-ha-valkey.md`, правится Puzzle-стороной)

Дискавери: + ключ `ca_pem`; `ValkeyClientConfig` получает поле CA и
вычисляемый `ssl` (`ssl=true ⟺ ca_pem`); «модуль пока без TLS»-оговорки
(01.21/01.22) актуализируются.

## 4. Структура и компоненты (отражение в коде)

### 4.1. pg — `ValkeyWorker.Core`

- **`Valkey/ValkeyPki.cs`** (новый, порт kafka `ClusterPki`): `GenerateCa`
  (RSA-2048, CN=`vwk-<C>-ca-<отпечаток>`, BasicConstraints CA, NotAfter +10
  лет) → `(caPem, caKeyPem PKCS#8)`; `IssueNodeCertificate` (CN=`node<k>`, SAN
  DNS `<AdvertisedClientHost>` либо IP по разбору, EKU ServerAuth, NotAfter
  зажат в CA) → `(certPem, keyPem)`; PEM-парсеры/TryParse (round-trip из etcd).
  PEM-формат значений etcd — одной строкой с `\n`.
- **`Valkey/ValkeyConnection.cs`** (расширение): TLS-режим — `SslStream`
  поверх TcpClient c ручной валидацией (`X509Chain` с anchor `ca_pem` +
  сверка SAN-хоста endpoint'а); `ValkeyEndpoint` получает CA (Pem) —
  plain-ветка удаляется (plain-порт закрыт; WAF-фейки — без сети). Все
  потребители (V4, надзор, converger, rotator) ходят по TLS неявно — один
  транспорт.

### 4.2. pg — `ValkeyWorker.Docker`

- **`Drivers/ClusterDriver.cs`**: `ValkeyNodeSpec` + поле TLS-volume
  (`TlsVolume`); драйверы (plain/swarm): создание named volume
  `vwk-<C>-tls`, **запись tar-архива** (`node.crt`/`node.key`/`ca.pem`,
  права 0600 для ключа) до старта контейнера, mount → `/tls`; демонтаж
  (`RemoveNodeAsync`, депровижининг X1) удаляет и volume. Инспект-сверка V3
  (args/лимиты/порт) дополняется наличием TLS-args (см. 4.4).

### 4.3. pg — `ValkeyWorker.Etcd`

- **`Parsing/ValkeySnapshotParser.cs`**: + `ca_pem`/`ca_key` (строковые поля
  снапшота; битый PEM — parseError, парсер не падает); примеры §2.1
  (канонические) — приёмочный тест.

### 4.4. pg — `ValkeyWorker.Provisioning`

- **`ClusterSecretEnsurer`**: ensure CA вместе с кредами (одна txn
  put-if-absent по отсутствующим из шести ключей); возвращает креды + CA.
- **`NodeArgsBuilder`**: + TLS-аргументы (`--tls-port 6379 --port 0
  --tls-cert-file /tls/node.crt --tls-key-file /tls/node.key
  --tls-ca-cert-file /tls/ca.pem --tls-auth-clients no`); вход дополняется
  фактом «серт в volume записан» (сертификат — файлы, не аргументы).
- **`ProvisioningProcess`**: V2 — ensure CA; V3 — генерация серта ноды (подпись
  `ca_key`) + запись в volume (переиспользуется существующий валидный;
  отсутствие/битость — перевыпуск) + контейнер; V4 — PING по TLS.
- **`TlsMigrator.cs`** (новый, arch/21 §5 T): детект (нет `ca_pem`/`ca_key` в
  etcd ИЛИ args живого контейнера без `--tls-port`) → T0–T3 (§3.2); вызывается
  из Active-ветки первым шагом (порт вставки — `ValkeyClusterProcesses`/
  классификатор, до надзора C).
- **`NodeSupervisor`**: пересоздание собирает TLS-args (NodeArgsBuilder) +
  серт (volume жив — переиспользование; пуст/бит — перевыпуск); PING — TLS.
- **`ConfigConverger`/`PasswordRotator`**: без изменений механики — соединение
  теперь TLS (транспорт 4.1).

### 4.5. pg — `ValkeyWorker.App`

- **`Loops` (классификация)**: Active-ветка = TlsMigrator → C → D → E.
- Конфигурация — без новых настроек (TLS-параметры — канонические константы;
  срок/размеры — как у kafka, настройками не вынесены).

### 4.6. pg — панель `AdminPanel.*`

- **`AdminPanel.Etcd/ValkeySnapshotRefresher` + `ValkeySecretsStore`**:
  `ValkeyClusterSecrets` + `CaPem`; чтение `ca_pem` тем же проходом (internal,
  наружу не отдаётся).
- **`AdminPanel.Probes/Valkey/ValkeyConnection`**: TLS-режим — SslStream +
  ручная валидация против `CaPem` (SAN-хост endpoint'а); plain-путь удаляется.

### 4.7. pg — стенд/доки

- **`dev-stand/adminpanel/checks/51-valkey-api.sh`** и живые проверки valkey:
  TLS-подключения (`redis-cli --tls --cacert …` / openssl-проба; CA читается
  из etcd по надобности чека — деталь плана); новый/расширенный чек
  «plain-порт закрыт» (подключение без TLS отклоняется).
- `docs/runbook.md` — заметка о TLS-транспорте домена (если упоминал plain).

### 4.8. Puzzle (ветка `feat-t06-valkey-tls`)

- **`HA.Valkey`**: парсер + `ca_pem` (поле снапшота); `ValkeyClientConfig` +
  `CaPem`; `SslValue`-константа заменяется вычислением `ssl = CaPem != null`
  (прозрачное окно миграции: снапшот без CA → ssl=false — старые контуры не
  ломаются, актуализация подтянет); `GetClientConfig()` отдаёт CA.
- **`Infrastructure.App.Valkey`**: `ValkeyConnectionParams` + `CaPem`
  (value-equality фильтра OnChange — учесть); маппинг в `ConfigurationOptions`:
  `Ssl=true` + доверие CA через certificate-validation callback
  (`X509Chain` anchor CaPem + SAN endpoint-хоста; SE.Redis не имеет прямой
  PEM-CA-опции — callback; точная подпись — план). Обе ветки провайдера
  (Aspire-ветка — без CA, ssl=false — не трогается).
- **Доки**: `docs/01.21-ha-valkey.md` (ca_pem, ssl), `docs/01.22-valkey.md`
  (TLS-маппинг); строка индекса при необходимости.

### 4.9. Тесты

- **Юниты pg** (`ValkeyWorker.UnitTests`): ValkeyPki (CA/серт, SAN DNS|IP,
  PEM round-trip, уникальный CN генераций); NodeArgsBuilder (полный
  TLS-набор); ClusterSecretEnsurer (ensure шести ключей, re-read); парсер
  (ca_pem/ca_key, битый PEM); TlsMigrator (детект по ключам/args, фазы,
  идемпотентность re-run, гонка TO_REMOVE); ValkeyConnection TLS-валидация
  (фейк-сервер SslStream с самоподписанным CA — доверие/недоверие/чужой CA);
  панельный probe-клиент TLS (юнит-уровень тем же фейком); панель — чтение
  CaPem в стор.
- **Интеграционные pg** (`ValkeyWorker.IntegrationTests`, docker): новый
  кластер поднимается TLS (plain-порт закрыт — подключение без TLS
  отклоняется; app-кред roundtrip по TLS с `ca_pem` из etcd; ACL-матрица
  прежняя); миграция plain→TLS (существующий контейнер без TLS → тик →
  TLS-args, тот же порт/endpoints, volume с сертами); пересоздание надзором
  сохраняет TLS; ротация/конфиг-конверге по TLS; демонтаж — ни контейнера, ни
  volume `vwk-*`, ни ключей.
- **E2E Release** (мерж-гейт): свежий Release, полный прогон
  ValkeyWorker-серии + маркер-кейс TLS-поднятия (имя фиксирует план; сценарий:
  API-создание → контейнер TLS → дискавери-ключи вкл. `ca_pem` → RESP-проба
  по TLS app-кредом → plain отклонён → удаление → чистота вкл. volume).
- **Puzzle**: юниты HA.Valkey (ca_pem-парсер, ssl-вычисление, GetClientConfig
  с CA); App.Valkey (маппинг CaPem → TLS-доверие, value-equality вкл. CaPem);
  интеграция — контур с TLS-valkey (fixture: CA генерируется, контейнер с
  `--tls-port`, ключи в etcd вкл. `ca_pem`) — roundtrip по TLS + окно «CA
  появился» (ssl false→true через актуализацию).

## 5. Обработка сбоев (дополнения к arch/20 §5 / arch/21 §6)

| Случай | Поведение |
|---|---|
| `ca_pem`/`ca_key` частично (один без другого) | ensure добирает отсутствующие txn NotExists (миграция/provisioning); парсер — parseError на битый PEM |
| Active без `ca_pem` (миграция не доиграна) | TlsMigrator доигрывает тиками (journal-фаза); панель — critical-алерт `valkey-security-missing` |
| Volume с битыми/чужими сертами | перевыпуск при ensure (запись поверх); PING по TLS — критерий валидности |
| Смена `AdvertisedClientHost` | SAN пересобирается при следующем пересоздании (правило то же); до пересоздания клиенты по новому хосту получают TLS-отказ — ответственность оператора (образец kafka R8) |
| Слет volume (ручное удаление) | надзор/provisioning: контейнер пересоздаётся, серты перевыпускаются (кеш восполним) |
| Отказ между T1 и T3 миграции | повтор тика доигрывает (ключи/args — факты; PING — критерий); окно plain→TLS одно пересоздание |
| Клиент без TLS после миграции | отказ на TLS-хендшейке — заявленный breaking change релиза t06 |
| t05-коллектор (будущее) | обязан использовать TLS-транспорт воркера (arch/21 §7 — правка фазы 1) |

## 6. Фазы (скелет для plan-фазы)

1. **Arch-правки** (arch-first): arch/20, arch/21, adminpanel/02 §11 — до
   любого кода; коммит в ветке.
2. **PKI-ядро**: `ValkeyPki` + юниты (SAN/PEM/round-trip).
3. **Docker-механика**: volume + tar + mount в драйвере; `NodeArgsBuilder`
   TLS-набор + юниты.
4. **Контракт**: ClusterSecretEnsurer (CA), парсер, канонические примеры.
5. **Транспорт воркера**: ValkeyConnection TLS (SslStream+валидация) — все
   процессы на TLS; юниты на фейк-SslStream-сервере.
6. **Процессы**: provisioning V2/V3/V4 TLS; TlsMigrator + классификация;
   надзор/конвергер/ротатор на TLS; юниты (детект, фазы, идемпотентность).
7. **Панель**: CaPem в стор + TLS-пробы; юниты.
8. **Puzzle** (ветка): HA.Valkey (ca_pem, ssl, CaPem) + App.Valkey
   (CA-доверие, value-equality) + доки 01.21/01.22; юниты.
9. **Интеграция/E2E pg**: сценарии §4.9; серии с зачисткой (контейнеры,
   volume, сети).
10. **Стенд**: чеки TLS (51-valkey-api + plain-закрыт); прогон на стенде.
11. **Мерж-гейт**: roadmap-правки §10 тем же коммитом мержа (в каждом репо —
    свой: pg-тег снимается в pg).

Каждая фаза — чистая сборка (TreatWarningsAsErrors) + зелёные тесты своей
области.

## 7. Ограничения и out of scope

- **Ротация CA и серверных сертов** — НЕ входит (roadmap-пункт §10.2: окно
  двойного доверия по образцу CaRotator 16 §5 K; серты долгоживущие, 10 лет).
  `ca_next_*` не вводятся.
- **Клиентские сертификаты (mTLS к Valkey)** — нет: принципалы из ACL
  (`--tls-auth-clients no`), по образцу kafka.
- **TLS inter-node** — нет (standalone nodes=1; `--tls-replication no`).
- **TLS-тюнинг** (шифры, TLS-версии, OCSP) — дефолты valkey/.NET.
- **Реплики/sentinel/cluster-топологии, persistence** — вне канона (arch/20).
- **t05-метрики** (не слита): коллектор INFO — вне t06; канон фиксирует
  обязанность TLS-транспорта (§3.2).
- **Код KafkaWorker/PgWorker не трогается** (общие файлы поставки — только при
  необходимости; ValkeyPki — своя копия, как Docker-движок t02).
- Puzzle: правки HA.Valkey/App.Valkey — только описанные (§4.8); staged-файл
  `src/global.json` — не трогать (решение t08); коммит/мерж — по явной просьбе.
- Локальные образы в registry не класть; новых внешних образов нет (образ нод
  прежний `valkey/valkey:9.1.2`).

## 8. Критерии приёмки

1. **Контракт**: новый кластер после provisioning имеет `ca_pem`/`ca_key`
   (PEM одной строкой, §2.1-примеры), `endpoints` прежнего формата; парсеры
   воркера/панели/HA.Valkey не падают на новых ключах; Active без `ca_pem` —
   алерт `valkey-security-missing`.
2. **Транспорт**: контейнер слушает только TLS на клиентском порту portalloc
   (`--tls-port 6379 --port 0`); plain-подключение отклоняется (интеграционный
   ассерт); серт валиден против `ca_pem`, SAN покрывает advertised-хост.
3. **Воркер**: V2 ensure шести секретов одной txn; V3 — volume
   `vwk-<C>-tls` с `node.crt/node.key/ca.pem` + TLS-args; V4/надзор/конвергер/
   ротатор — по TLS; пересоздание контейнера сохраняет TLS-канон.
4. **Миграция**: существующий plain-кластер после обновления воркера
   автоматически: ensure CA → пересоздание контейнера → PING TLS → RUNNING;
   endpoints/portalloc не изменились; повторный детект — no-op; фазы в
   `work/<C>` (op=migrate-tls).
5. **Панель**: live-пробы valkey по TLS (SslStream + `ca_pem` из internal-стора);
   CaPem не отдаётся в UI/API; чеки dev-stand проходят на TLS-кластере
   (включая проверку «plain закрыт»).
6. **Puzzle**: `GetClientConfig()` при наличии `ca_pem` отдаёт CA и
   `ssl=true` (без CA — `ssl=false`, обратная совместимость); App.Valkey
   подключается по TLS с доверием CA; ротация кредов hot-reload жива
   (value-equality вкл. CaPem).
7. **Тесты**: юниты/интеграция/E2E §4.9 зелёные; каноны проекта соблюдены
   (динамические порты, guid-изоляция, teardown + ассерт чистоты вкл. volume
   `vwk-*`, NodeBootSec ≤ 100, AAA-комментарии); после серий — ни остаточных
   контейнеров/томов/сетей.
8. **Мерж-гейт**: docker-E2E ValkeyWorker на свежем Release (полный прогон +
   TLS-маркер §4.9) зелёный; TreatWarningsAsErrors чисто (оба репо).
9. **Roadmap** (§10): тег `t06-valkey-tls` снят тем же коммитом мержа;
   добавлен пункт ротации CA (§10.2).

## 9. Открытые вопросы

Нет — принципиальные развилки закрыты решениями §1.1; детали имплементации
(точная подпись certificate-validation callback в SE.Redis, права файлов tar,
имена тест-кейсов, формат чеков стенда) — plan-фаза.

## 10. Roadmap-правки (мерж-гейт)

1. **Снять тег `t06-valkey-tls`**: удалить пункт из `arch/roadmap/valkey.md`
   (+ `←`-зависимости, если появились) — тем же мерж-коммитом pg.
2. **Добавить пункт ротации CA** (долг фиксируется сразу, исполняется по
   потребности): «`tNN-valkey-ca-rotation` — ротация per-cluster CA и сертов
   Valkey (окно двойного доверия, образец CaRotator arch/16 §5 K; ca_next_*
   staging, bundle ca_pem, пересоздание нод с перевыпуском)» — в
   `arch/roadmap/valkey.md` со ссылкой на канон после t06.
3. Puzzle-коммиты — ветка `feat-t06-valkey-tls`; мерж в main Puzzle — по
   отдельному явному запросу (как t08).
