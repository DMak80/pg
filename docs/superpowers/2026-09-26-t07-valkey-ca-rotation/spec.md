# t07-valkey-ca-rotation — ротация per-cluster CA и серверных сертов Valkey

- **Дата**: 2026-09-26
- **Roadmap**: [`arch/roadmap/valkey.md`](../../../arch/roadmap/valkey.md), тег `t07-valkey-ca-rotation` (снимается из roadmap тем же коммитом мержа в `main` — мерж-гейт, §11)
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/t07-valkey-ca-rotation` (ветка `t07-valkey-ca-rotation` от `main@3cad86d`); правки монорепо — только здесь. **Puzzle** — `/Users/demakaev/ZCodeProject/Puzzle` (клиентская сторона — §3.3, скоуп расширен решением пользователя)
- **Тип**: новая функциональность домена — процесс ротации PKI (окно двойного доверия) + заявочная механика (etcd + API воркера + панель) + клиентская сторона (Puzzle, многосертовое чтение `ca_pem`) + E2E-покрытие
- **Канон (обновлён этой задачей, arch-first, до кода)**: контракт — [`arch/20-valkey-clusters.md`](../../../arch/20-valkey-clusters.md) §2/§2.1/§3/§4/§5, оркестратор — [`arch/21-valkeyworker.md`](../../../arch/21-valkeyworker.md) §2/§3/§5 K/§9, панельная проекция — [`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md) §11.1/§11.2
- **Образец**: kafka t07 — `arch/16-kafkaworker.md` §5 K (CaRotator) + §2.3 (окно двойного доверия); реализация `src/KafkaWorker.Provisioning/Processes/CaRotator.cs`, `src/KafkaWorker.App/Api/Operations/RotateCaHandler.cs`, `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs` (`RotateCaCommandHandler`), `frontend/src/pages/kafka-cluster/RotateCaButton.tsx`
- **Решения пользователя (гейты уточнений, зафиксированы)**: (1) скоуп панели — полный аналог kafka: воркер + панель (endpoint + UI + чтение очереди `ca_rotations`); (2) CaRotator — **эксклюзивный второй шаг Active-ветки** (после миграции T, до надзора C; окно открыто ⇒ C/D/E тика не идут) — обоснованное отклонение от буквы kafka-образца; (3) тесты — юниты + интеграции + docker-E2E кейс ротации; (4) **гейт user-review пройден: скоуп расширен — доработка Puzzle ВКЛЮЧАЕТСЯ в эту задачу, санкция на касание `../Puzzle` выдана явно** (ранее действовал консервативный дефолт «только монорепо» — отменён решением пользователя; временный roadmap-пункт t15-valkey-client-bundle-multicert удалён этой же правкой, работа вернулась внутрь t07).

## 1. Цель

Дать оператору безопасную ротацию per-cluster CA Valkey-кластера и серверных
сертов нод **без остановки обслуживания** — окно двойного доверия по образцу
kafka CaRotator (arch/16 §5 K): клиенты ни в один момент не видят
недоверенного серта, компрометированный (или просто старый) `ca_key`
уничтожается, серт ноды перевыпускается от новой генерации CA.

Закрывает риск R10 arch/21 (`ca_key` в etcd — компрометация etcd = выпуск
валидных сертов; до t07 митигация была «ротация CA — roadmap»).

**Скоуп**: монорепо pg + репозиторий `../Puzzle` (клиентская сторона —
§3.3; санкция пользователя на гейте user-review). Риск клиентского окна
закрывается **обеими сторонами одной задачи**: правки pg и Puzzle идут
одной задачей; поставка Puzzle — отдельным мержем в своём репозитории.

Функциональные результаты:

1. **Процесс CaRotator (K)** в ValkeyWorker: исполнение заявки
   `/valkeyworker/ca_rotations/<C>` фазами P→D→R→C (staging `ca_next_*`,
   bundle `ca_pem`, пересоздание `node1` с перевыпуском серта, атомарный
   коммит). nodes=1: «rolling» = одно пересоздание, весь цикл — один тик.
2. **Заявка через API воркера**: `POST /api/valkey/clusters/{c}/ca/rotate`
   (клэйм-txn `version==0`; 409 при живой заявке; 409 при не-Active
   state — образец kafka, отличается от ротации паролей, где state-гейта
   нет: ротация не поднятого кластера бессмысленна).
3. **Панель**: мутация №6 (adminpanel/02 §11.2), чтение очереди
   `ca_rotations` в снапшот панели, кнопка в UI (порт kafka
   `RotateCaButton`).
4. **Клиентская сторона (Puzzle)**: TLS-доверие `HA.Valkey`/`App.Valkey`
   строится по **всем CERTIFICATE-блокам** `ca_pem` — bundle OLD+NEW окна
   ротации D→C (сегодня `X509Certificate2.CreateFromPem` читает только
   первый блок — риск окна; §3.3).
5. **Чистка демонтажа**: X2 сносит заявку `ca_rotations` вместе с прочей
   координацией; `del --prefix /valkey/clusters/<C>/` уже забирает
   staging `ca_next_*`.
6. **Тесты**: юниты фаз/guard'ов, интеграционный сценарий (реальный etcd +
   TLS-сервер), docker-E2E кейс полного цикла ротации в живом контуре,
   юнит bundle-чтения в Puzzle.

## 2. Принципы

1. **Arch-first**: контракт в `arch/20`/`arch/21`/`adminpanel/02` обновлён
   ДО кода (уже выполнено в worktree этим коммитом спеки); код зеркалит
   канон. Спека — развёртка канона в решения кода.
2. **Окно двойного доверия** (порядок фаз гарантирует отсутствие
   недоверенных сертов для перечитавших дискавери клиентов):
   (P) staging → (D) `ca_pem` = bundle OLD+NEW → (R) серт ноды NEW →
   (C) коммит: `ca_pem`/`ca_key` = NEW, staging и заявка удалены. Между
   R и C клиенты с bundle доверяют NEW; после C — все доверяют NEW.
3. **Идемпотентность по факту, не по треку**: каждая фаза перепроверяет
   факт в etcd/docker (staging есть? bundle содержит nextPem? серт
   volume валиден против nextPem?) — отказ/краш в любой точке доигрывается
   повтором тика; in-memory-треков нет (отличие от kafka `_rolled`:
   nodes=1 позволяет факт-детект через `GetTlsArchiveAsync`).
4. **Эксклюзивность окна (решение пользователя)**: CaRotator — второй шаг
   Active-ветки (после TlsMigrator T, до надзора C); **окно открыто**
   (staging `ca_next_*` есть ИЛИ journal op=rotate-ca фаза вне
   {done, waiting-*}) ⇒ InProgress ⇒ надзор/converger/ротация кредов в
   этом тике не идут. **Ждущие исходы (waiting-*) ветку НЕ блокируют** —
   ротация кредов E доиграет тем же тиком ниже по ветке (иначе deadlock:
   K ждёт E, а E заблокирован K). Основание эксклюзивности: у valkey серт
   в volume сверяется только при пересоздании ноды («env-сверки» надзора
   нет, как у kafka) — пересоздание надзором в окне D→R собрало бы
   OLD-серт от bundle+OLD-`ca_key`, а после коммита C нода осталась бы с
   недоверенным сертом без самокоррекции. В окне пересоздаёт ТОЛЬКО
   CaRotator (фаза R — она же лечение мёртвой ноды: преф-чека живости
   нет — nodes=1, persistence off, кеш восполним; отличие от kafka, где
   rolling мёртвого кластера ронял ISR).
5. **Journal-before-manipulations**: каждая мутирующая фаза предваряется
   записью journal (`work/<C>`, op=`rotate-ca`); хвост после коммита
   (phase=committed, заявка снята) доигрывается финалом K4 идемпотентно.
6. **Состояние — в etcd**: staging/bundle/journal переживают рестарт
   воркера и takeover клэйма (≤ TTL 15 с + тик); двойной контроллер
   невозможен (мутации — только под живым клэймом `<C>`).
7. **Парсер доверия воркера не расширяется** (`ValkeyConnection`/
   `ValkeyPki.TryParseCertificate` — первый PEM-блок, как в t06): в фазе R
   пробы идут с якорем **nextPem (NEW)** — CaRotator передаёт нужный якорь
   в `ValkeyEndpoint.CaPem`; механика клиента не трогается. Одноблочные
   читатели монорепо (панельные live-пробы, метрики-коллектор) в окне D→C
   транзиентно недоверяют NEW-серту (секунды, один poll-цикл,
   самокоррекция после C) — документированное окно, риск R11 arch/21.
   **Клиентская библиотека — исключение** (§3.3): дорабатывается до
   многосертового чтения (риск закрыт в рамках t07).
8. **Язык/качество**: комментарии и документация — русские; идентификаторы
   — английские; .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`
   (0 warnings — критерий приёмки); тесты — AAA-комментарии.

## 3. Контракт etcd (канон arch/20 §2/§3 — уже обновлён)

### 3.1. Ключи

| Ключ | Значение | Кто пишет | Жизненный цикл |
|---|---|---|---|
| `/valkey/clusters/<C>/ca_next_key` | PEM PKCS#8 НОВОЙ CA | CaRotator, фаза P: txn `[NotExists(ca_next_key), NotExists(ca_next_pem)]` + put обеих | staging: живут только в окне P→C; фаза C — del одной txn с коммитом; ensure/provisioning НЕ создаёт |
| `/valkey/clusters/<C>/ca_next_pem` | PEM-серт НОВОЙ CA | там же | там же; в UI/API не отдаётся |
| `/valkey/clusters/<C>/ca_pem` | обычно — один PEM; **в окне D→C — bundle** `OLD + "\n" + NEW` (оба однострочные, канон §2.1 arch/20) | фаза D: txn `[ValueEqual(ca_pem, OLD)] [put bundle]`; фаза C: put NEW | после C — только NEW |
| `/valkey/clusters/<C>/ca_key` | PEM PKCS#8 текущей CA | фаза C: put NEW (OLD уничтожается перезаписью — после окна никем не доверяется) | вне ротации не меняется |
| `/valkeyworker/ca_rotations/<C>` | `{"requested_unix":<u>,"requested_by":"<user>"}` (§9.8-паттерн, БЕЗ role) | панель через API воркера (клэйм-txn `version==0`); del — ТОЛЬКО воркер, атомарно коммиту фазы C | заявка: 409 при живой; отмены из панели нет |

Идемпотентность фаз: (P) повторный тик re-read'ит staging — существующая
(в т.ч. чужая, из txn-гонки) staging используется как есть (одна генерация
на жизнь ротации; проигрыш txn-гонки разрешается re-read — образец kafka
`EnsureStagingAsync`);
(D) готовность распознаётся вхождением `nextPem` в текущее значение
`ca_pem` (string.Contains, как kafka) — put пропускается; (R) готовность —
`IsValidTar(tar, advertisedHost, nextPem)` по archive из
`GetTlsArchiveAsync` (валиден ⇒ пересоздание не нужно); (C) compare
`ValueEqual(ca_next_key, stagingKey)` закрывает гонку параллельной ротации.

### 3.2. Отражение в коде (существующие компоненты НЕ меняются)

- `ValkeyPki.GenerateCa(cluster)` — генерация NEW (случайная, отпечаток
  subject — уже уникален на генерацию, фикс t07 kafka перенесён в t06);
  `NodeTlsProvisioner.EnsureNodeTlsAsync(cluster, node, host,
  advertisedHost, caPem, caKeyPem, ct)` — вызов с `(nextPem, nextKey)`:
  пишет `ca.pem`(volume)=NEW и серт, подписанный NEW (сигнатура достаточна,
  правок NodeTlsProvisioner нет); `IsValidTar` — internal, доступен из
  сборки Provisioning для R-детекта.
- `ClusterSecretEnsurer` НЕ трогается (ensure не знает про `ca_next_*` —
  staging вне его зоны).
- `ValkeySnapshotParser` НЕ расширяется на `ca_next_*` (unknownKeys-
  толерантность arch/20 §5 — как у kafka); CaRotator читает staging
  прямыми `GetAsync` с failover (образец kafka).
- `DeprovisioningProcess` (X2): в чистку координации добавить
  `ca_rotations` — del `/valkeyworker/{claims,work,portalloc,rotations,
  ca_rotations}/<C>*` (сегодня ca_rotations отсутствует в перечне).

### 3.3. Клиентская сторона (Puzzle, `../Puzzle`) — В СКОПЕ (решение user-review)

Канон arch/20 §4 (обновлён): доверие клиента строится по **всем блокам
CERTIFICATE** значения `ca_pem` — bundle OLD+NEW окна ротации D→C.
Исследование кода Puzzle (зафиксировано этой спекой):

- `PuzzleServer.Infrastructure.App.HA.Valkey` — `Parsing/
  ValkeyClusterParser.cs` читает `ca_pem` как raw-строку без PEM-разбора
  (снапшот → `ValkeyClientConfig.CaPem`) — **правка не нужна** (bundle
  проходит как есть).
- `PuzzleServer.Infrastructure.App.Valkey` — `ValkeyConnectionOptions.cs`,
  `TryBuildCertificateValidation`: `X509Certificate2.CreateFromPem(caPem)`
  читает **только первый CERTIFICATE-блок**, PFX round-trip (macOS-паттерн)
  и `ValidateAgainstCa` кладут в `CustomTrustStore` **один** серт —
  в окне D→C клиент с bundle доверяет только OLD → NEW-серт после R
  отклоняется до перечитывания `ca_pem` (риск R11). **Точка доработки.**

Доработка (границы — только многосертовое чтение `ca_pem`/доверия и его
тесты):

1. `TryBuildCertificateValidation`: вместо `CreateFromPem` —
   `X509Certificate2Collection.ImportFromPem(caPem)` (стандартный .NET
   способ прочитать ВСЕ PEM-блоки; kafka-паттерна в коде Puzzle нет —
   TLS-конфигурация kafka-клиентов в Puzzle не кодируется, доверие
   решается transports вне репо), PFX round-trip для каждого серта
   коллекции (действующий macOS-паттерн сохраняется);
   `ValidateAgainstCa` — `CustomTrustStore` получает **все** серты
   коллекции (валидация сервера проходит против любого якоря bundle:
   OLD до пересоздания, NEW после). SAN-сверка — без изменений.
2. Пустая коллекция (нет валидных блоков) — действующая семантика:
   callback не строится (`false`), `Ssl` не сбрасывается, подключение
   честно упадёт на хендшейке; catch-набор исключений расширить по факту
   `ImportFromPem` (та же группа ArgumentException/CryptographicException/
   FormatException).
3. Тесты: `PuzzleServer.UnitTests/Valkey/ValkeyConnectionOptionsTests.cs`
   (+ `TestPki.cs` — генератор тестовых CA) — кейс bundle из двух сертов
   (OLD+NEW): серверный серт, подписанный NEW, валидируется против
   bundle; серт от посторонней CA — отклоняется; один PEM — поведение
   t06 сохраняется (регресс-кейс).

Поставка: правки pg и Puzzle — **одной задачей t07**; в `../Puzzle` —
свой отдельный коммит/мерж в своём репозитории (**вне мерж-гейта этого
флоу**, прецедент t06). Риск клиентского окна закрывается только обеими
сторонами: до мержа Puzzle-стороны клиенты в окне R→C получают TLS-отказ
на reconnect до перечитывания `ca_pem` (самокоррекция poll-циклом,
секунды).

## 4. Структура и компоненты

### 4.1. ValkeyWorker.Provisioning — `Processes/CaRotator.cs` (новый)

Порт `src/KafkaWorker.Provisioning/Processes/CaRotator.cs` на механику
valkey (одна нода, факт-детект, эксклюзивность):

```csharp
public sealed class CaRotator(
    IEtcdGateway gateway,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    NodeTlsProvisioner tlsProvisioner,
    IValkeyConnection valkey,
    ValkeyProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null,
    TimeProvider? clock = null)
{
    public const string Op = "rotate-ca";
    public enum RotationOutcome { NotNeeded, Waiting, InProgress }
    public async Task<Result<RotationOutcome>> RunAsync(
        ValkeyClusterSnapshot snap, CancellationToken ct);
}
```

- `NotNeeded` — заявки и хвостов нет (пустой шаг ветки, ветка продолжается);
  `Waiting` — заявка жива, окно ещё НЕ открыто (ждущие причины; без мутаций,
  ветка продолжается — E доиграет ниже по ветке); `InProgress` — окно
  открыто/доигрывается (вентиль Active-ветки блокирует C/D/E).
- Сигнатуры существующих зависимостей: `IClusterDriver.RemoveNodeAsync/
  EnsureNodeAsync/NodeArgsAsync/GetTlsArchiveAsync`,
  `NodeTlsProvisioner.EnsureNodeTlsAsync`, `IValkeyConnection.PingAsync`,
  `ProcessCommon.{ConfigKey,PortAllocKey,ParsePortAlloc,WriteNodeStateAsync,
  ParseResources}`, `NodeArgsBuilder.Build` — используются как есть.
- Внутренние ключи-хелперы: `TicketKey` = `/valkeyworker/ca_rotations/<C>`;
  `NextKeyKey`/`NextPemKey` = `/valkey/clusters/<C>/ca_next_{key,pem}`;
  etcd-обращения — failover-цикл по `endpoints` (образец TlsMigrator/
  CaRotator kafka).
- Journal-фазы (строки): `phase-p`, `phase-d`, `phase-r`,
  `phase-r/node1`, `committed`, `done`; ждущие: `waiting-cluster`,
  `waiting-password-rotation`; аварийные: `aborted-state-changed`,
  финальные ошибки — с `last_error` (механика `FailAsync` TlsMigrator).

### 4.2. ValkeyWorker.App — API-грань и вентиль ветки

- **`Api/Operations/RotateCaHandler.cs` (новый)** — порт kafka
  `RotateCaHandler`: валидация имени (`ValkeyLimits.ClusterPattern`, иначе
  404) → чтение config (нет — 404) → **state-гейт: `config.State != null` →
  409 `ValkeyClusterNotActiveException`** (НЕ-Active: NOT_INITIALIZED/
  TO_REMOVE; новое исключение по образцу kafka `KafkaClusterNotActive-
  Exception`, в `ValkeyExceptions.cs`) → живая заявка → 409
  `ValkeyRotationAlreadyRequestedException` (переиспользуется) → клэйм-txn
  `[NotExists(key)] [put {"requested_unix","requested_by"}]` → 202-DTO
  `ValkeyCaRotatedDto(Cluster, RequestedUnix, RequestedBy)`;
  `requested_by` — заголовок `X-Requested-By`, fallback `"api"`.
- **`Api/ApiModule.cs`**: `POST /api/valkey/clusters/{cluster}/ca/rotate`
  рядом с `password/rotate` (тот же маппинг-стиль, 202-ответ).
- **`Loops/ValkeyClusterProcesses.cs`** (вентиль Active-ветки): после
  `tlsMigrator.RunAsync` (T, InProgress-гейт уже есть) — вызов
  `caRotator.RunAsync` (K): `InProgress` (окно открыто) ⇒ `return
  Result.Success()` — надзор C / converger D / ротатор E в этом тике не
  идут (симметрично миграции T); `Waiting`/`NotNeeded` ⇒ ветка
  продолжается (ждущие исходы ничего не мутировали; E доиграет заявку
  ротации кредов ниже по ветке). DI-регистрация CaRotator в `Program.cs`.

### 4.3. ValkeyWorker.Provisioning — `DeprovisioningProcess.cs`

X2: перечень del-ключей координации дополняется `ca_rotations` (см. §3.2).

### 4.4. AdminPanel — чтение очереди, команда, UI

- **`AdminPanel.Etcd/Parsing/ValkeyParser.cs` + `AdminPanel.Core/Valkey/
  ValkeySnapshot.cs`**: разбор `/valkeyworker/ca_rotations/<C>` →
  `ValkeyCaRotationTicket(Cluster, RequestedUnix, RequestedBy)` (порт
  `ValkeyRotationTicket` без role); чтение — в `ValkeySnapshotRefresher.cs`
  рядом с `rotations/`.
- **`AdminPanel.Api/Operations/Valkey/ValkeyCommands.cs`**: команда
  `RotateValkeyCaCommand(string Cluster, string RequestedBy)` +
  `RotateValkeyCaCommandHandler` (порт kafka `RotateCaCommandHandler`):
  `WorkerProxy.SendAsync` → `POST /api/valkey/clusters/{c}/ca/rotate`.
  **`ValkeyOperationsModule.cs`**: endpoint
  `POST /api/valkey/clusters/{cluster}/ca/rotate` (аудит — username сессии).
- **UI**: `frontend/src/pages/valkey-cluster/RotateCaButton.tsx` (порт
  `kafka-cluster/RotateCaButton.tsx`) — кнопка «Ротация CA» в
  `ValkeyClusterDetailsPage.tsx` рядом с «Ротация пароля»: подтверждение с
  предупреждением «нода будет пересоздана (кеш холодный старт), окно
  секунд»; отображение живой заявки из очереди `ca_rotations` (порт
  отображения `rotations`); API-слой — `frontend/src/api/{queries,dto}.ts`.

### 4.5. Puzzle — `App.Valkey/ValkeyConnectionOptions.cs` (многосертовое чтение)

Единственная точка доработки `../Puzzle`: `TryBuildCertificateValidation` —
`X509Certificate2Collection.ImportFromPem` → PFX round-trip каждого серта →
`CustomTrustStore` из всей коллекции (детали и тесты — §3.3). Прочие файлы
Puzzle не трогаются; `HA.Valkey` (парсер raw-строки) — без правок.

### 4.6. Что НЕ меняется (границы правки)

Монорепо: `ValkeyPki`, `ValkeyConnection` (парсер доверия одноблочный —
якорь для R передаётся параметром), `NodeTlsProvisioner` (сигнатура
достаточна), `NodeSupervisor`, `ConfigConverger`, `PasswordRotator`,
`TlsMigrator`, `ClusterSecretEnsurer`, `ProvisioningProcess`,
portalloc-механика, `Shared.*`, docker-драйвер, deploy/compose. Вентиль
ветки — единственная правка `ValkeyClusterProcesses` (вставка шага K);
`NodeSupervisor` и остальные процессы про окно ротации не знают
(эксклюзивность гарантируется вентилем, а не их правкой). Puzzle: всё вне
`ValkeyConnectionOptions` (+ его тесты) — не трогается.

## 5. Фазы процесса K (детальная развёртка)

`RunAsync(snap, ct)`, все шаги — под живым клэймом (`claims.IsMine`,
иначе `Failed` «мутации запрещены»).

**Диспетчеризация (порядок строгий P→D→R→C; инвариант окна: серт NEW
появляется на ноде (R) ТОЛЬКО после bundle в `ca_pem` (D))**:

**K0 — guard'ы ДО открытия окна** (после открытия не проверяются — окно
уже эксклюзивно, живая пароль-ротация, пришедшая в окно, доиграет E
следующим тиком после done):
1. Чтение заявки `ca_rotations/<C>` и journal (`journal.ReadAsync`).
2. Хвост после коммита: journal `{Op=rotate-ca, Phase=committed}` И заявки
   нет → финал K4 (идемпотентное завершение; staging/ноду не трогаем) →
   `InProgress` (один тик на финал).
3. **Окно уже открыто?** staging `ca_next_*` есть ИЛИ journal
   `{Op=rotate-ca, Phase ∉ {done, waiting-*}}` → путь доигрывания P→D→R→C
   (без K0-проверок 4–6).
4. Заявки нет и хвоста/окна нет → `NotNeeded` (no-op ветки).
5. Заявка жива, окно не открыто — ждущие причины (исход `Waiting`, без
   мутаций, journal-запись ждущей фазы):
   - кластер не поднят: `snap.Endpoints is null || AdminPassword is null
     || AppPassword is null || CaPem is null || CaKey is null` →
     `waiting-cluster` (премиграционный — миграция T, первый шаг ветки,
     доведёт);
   - живая ротация креда: заявка `rotations/<C>` жива ИЛИ стейт
     `work/<C>/rotation` с фазой `e1-pending|e1-added|e2-committed` →
     `waiting-password-rotation` (E доиграет ниже по ветке этим же тиком;
     старт окна — следующим тиком).
6. Перечитка `config` (`ProcessCommon.ConfigKey`): `state=TO_REMOVE` →
   journal `aborted-state-changed`, `Waiting` (клэйм жив; демонтаж B всё
   почистит, вкл. staging — X2 `del --prefix`; на следующем тике
   классификатор уже не Active).

**P — открытие окна, staging НОВОЙ CA**: journal `phase-p` →
`ValkeyPki.GenerateCa(cluster)` → txn `[NotExists(ca_next_key),
NotExists(ca_next_pem)] [put обе]` → re-read обоих ключей (проигрыш
txn-гонки → чужая staging валидна, используем её). Формат значений — PEM
одной строкой с `\n` (канон arch/20 §2.1). С этого момента окно открыто.

**D — bundle в точке дискавери**: если `snap.CaPem.Contains(nextPem)` —
фаза уже отработана (skip). Иначе journal `phase-d` → txn
`[ValueEqual(ca_pem, snap.CaPem)] [put ca_pem = snap.CaPem + "\n" +
nextPem]`. Срыв compare (внешняя запись `ca_pem`) → `Failed` «ретрай
тиком». После фазы клиенты, перечитавшие дискавери, доверяют обоим
поколениям.

**R — пересоздание node1 с перевыпуском (лечение ЛЮБОГО состояния ноды;
преф-чек живости отсутствует — мёртвая/снесённая нода пересоздаётся
здесь же; отличие от kafka, см. §2.4)**:
1. Факт-детект: `driver.GetTlsArchiveAsync(cluster, host, nodeImage)` →
   `NodeTlsProvisioner.IsValidTar(tar, advertisedHost, nextPem)` —
   `true` ⇒ R завершён (рестарт воркера посреди R идемпотентен, без
   in-memory трека — отличие от kafka `_rolled`).
2. Иначе: portalloc-адрес node1 (нет → `Failed` «не закреплён»);
   перечитка config: TO_REMOVE → abort (journal
   `aborted-state-changed`); `tlsProvisioner.EnsureNodeTlsAsync(cluster,
   "node1", host, advertisedHost, nextPem, nextKey, ct)` — серт от NEW
   CA, `ca.pem` volume = NEW (НЕ bundle: `--tls-auth-clients no` —
   отличие от kafka truststore, канон arch/21 §2); journal `phase-r`.
3. `driver.RemoveNodeAsync(cluster, "node1", ct)` → собрать args
   (`NodeArgsBuilder.Build` от config/кредов — как TlsMigrator.
   RecreateNodeAsync) → `driver.EnsureNodeAsync(ValkeyNodeSpec …,
   TlsVolume: PlainClusterDriver.TlsVolumeName(cluster))` — порт/лимиты
   из portalloc/декларации, адреса не меняются → state=PROVISIONING →
   journal `phase-r/node1`.
4. AwaitBoot: PING по TLS **с якорем `nextPem`** (серт уже NEW;
   OLD/bundle-якорь одноблочного парсера неверен), бюджет
   `NodeBootSec`, цикл 100 мс (порт `TlsMigrator.AwaitBootAsync`) →
   state=RUNNING.

**C — атомарный коммит**: journal `committed` → ОДНА txn
`[ValueEqual(ca_next_key, stagingKey)] [put ca_pem=nextPem;
put ca_key=stagingKey; del ca_next_pem; del ca_next_key;
del ca_rotations/<C>]`. Срыв compare (параллельная ротация) → `Failed`
«ретрай тиком». OLD-ключ уничтожен перезаписью; заявка снята атомарно
коммиту (del отсутствующего ключа в txn — no-op, ручное снятие заявки
безопасно).

**K4 — финал**: снапшот P12 «после» (делегат `snapshot`, как у
TlsMigrator) → journal `done`. Хвост (краш между C и K4) доигрывается
веткой K0.2.

Отказ etcd/docker между фазами: `Failed` c `last_error` в journal —
следующий тик доигрывает с той же фазы по факту (staging/bundle/серт —
стабильные состояния etcd/volume). Смерть инстанса: клэйм гаснет ≤ 15 с,
следующий инстанс продолжает с journal-фазы (все состояния — в etcd).

## 6. API-контракты

### 6.1. Воркер: `POST /api/valkey/clusters/{c}/ca/rotate`

- Транспорт: mTLS-грань `/api` (как все мутации valkey-домена, arch/21 §1.1).
- Запрос: пустое тело; заголовок `X-Requested-By` (панель шлёт оператора).
- Ответ 202: `{"cluster":"<C>","requestedUnix":<u>,"requestedBy":"<user>"}`.
- Отказы: 404 (имя не каноническое / кластера нет), 409 (живая заявка |
  `state≠Active` — `ValkeyClusterNotActiveException`), 503 (etcd-сбой),
  ProblemDetails по общим правилам исполнителя (adminpanel/02 §11.2).
- Идемпотентность: повтор после исполнения валиден (заявка снята → новая).

### 6.2. Панель: `POST /api/valkey/clusters/{cluster}/ca/rotate`

- Прокси на API воркера (живой `/valkeyworker/api/<id>`; все умерли — 503
  + критический алерт `worker-api-unreachable`). Коды маппятся 1:1.
- Очередь в UI: живые заявки `ca_rotations` (снапшот панели) — бейдж в
  деталях кластера (порт отображения `rotations`).

## 7. Тесты

### 7.1. Юниты — `src/tests/ValkeyWorker.UnitTests/Provisioning/CaRotatorTests.cs` (новый; порт KafkaWorker CaRotatorTests на valkey-механику)

Кейсы (AAA, фейки etcd/driver/valkey — существующие `Fakes/`):
- no-op: нет заявки и хвоста → `NotNeeded`, ноль мутаций.
- K0-ждущие (исход `Waiting`, без мутаций): нет endpoints/кредов/CA →
  `waiting-cluster`, заявка жива; живая пароль-ротация (заявка/стейт
  e1*/e2) → `waiting-password-rotation`; TO_REMOVE →
  `aborted-state-changed`.
- Диспетчеризация окна: staging есть → guard'ы K0 НЕ проверяются
  (живая пароль-заявка при открытом окне не блокирует доигрывание
  P→D→R→C); journal-хвост `committed` без заявки → финал K4 без мутаций
  staging.
- P: генерация + put-if-absent; повторный тик re-read (чужая staging).
- D: bundle = OLD+"\n"+NEW, compare по OLD; повтор (bundle содержит
  nextPem) — put пропущен; срыв compare → Failed.
- Порядок-инвариант: R не выполняется до успешного D (bundle обязан
  быть в `ca_pem` ДО появления NEW-серта на ноде).
- R: факт-детект (валидный NEW-серт в archive ⇒ без пересоздания);
  пересоздание: EnsureNodeTls с (nextPem, nextKey), RemoveNode→EnsureNode,
  state PROVISIONING→RUNNING, AwaitBoot с якорем nextPem (фейк PING
  валидирует якорь), порт/лимиты из portalloc/декларации; TO_REMOVE
  перед R → abort.
- C: одна txn с пятью операциями и compare по staging; срыв compare →
  Failed; del заявки в составе txn; ручное снятие заявки (del вне txn)
  не ломает коммит.
- K4: снапшот «после» + done.
- Вентиль: `ValkeyClusterProcesses`-тест (существующий набор App-тестов
  цикла) — при `InProgress` ротации CA надзор/converge/ротатор не
  вызываются; при `Waiting`/`NotNeeded` — вызываются (моки процессов).

### 7.2. Юниты API/панели

- `RotateCaHandler` (UnitTests/Api): 404/409(state)/409(живая)/202-DTO/
  payload заявки `{"requested_unix","requested_by"}`.
- Панельный парсер: `ca_rotations` → `ValkeyCaRotationTicket`; битый JSON —
  parseError-толерантность (порт кейсов rotations).
- Панельная команда: прокси-URL и маппинг кодов (порт kafka-кейсов).

### 7.3. Интеграция — `src/tests/ValkeyWorker.IntegrationTests/Valkey/CaRotationTests.cs` (новый; порт Kafka/CaRotationTests)

Реальный etcd (тестовый контейнер, префикс сценария, динамические порты) +
TlsTestServer + фейк docker-драйвера (существующие механики фикстур):
- полный happy-path: заявка → тик(и) → staging создан → bundle записан →
  EnsureNodeTls от NEW → коммит: `ca_pem`=NEW, `ca_key`=NEW, staging и
  заявка удалены, journal done;
- сбой между D и R (инжект отказа EnsureNode) → повторный тик доигрывает;
- сбой между R и C → повторный тик: факт-детект R (без второго
  пересоздения) → коммит;
- X2-чистка: TO_REMOVE кластера с живой заявкой → ключей `ca_rotations`/
  staging нет после демонтажа.

### 7.4. Docker-E2E — кейс в `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eLifecycleTests.cs` (новый тест-метод в существующей фикстуре `ValkeyE2eEnvironment`, решение пользователя)

`CaRotation_ClusterRotatesWithoutDowntimeOfTrust`: поднять кластер (TLS,
канон t06) → проверить PING с OLD-CA → `POST /api/valkey/clusters/{c}/
ca/rotate` через HTTP-грань воркера (полный прод-контур заявки; mTLS-клиент
фикстуры — по механике существующих API-вызовов фикстуры) → дождаться
journal done →
ассерты: `ca_pem`/`ca_key` — NEW (не bundle), staging-ключей нет, заявки
нет, контейнер пересоздан (`vwk-<C>-node1`), PING по NEW-CA отвечает,
PING по OLD-CA отказывает (OLD-ключ уничтожен); полная самоочистка
фикстуры (teardown при любом исходе, ассерт чистоты — правила
docs/e2e-isolation.md, телеметрия docs/e2e-launch.md).

### 7.5. Puzzle — юниты bundle-чтения — `../Puzzle/src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionOptionsTests.cs`

- bundle из двух сертов (OLD+NEW, "\n"-разделитель): серверный серт,
  подписанный NEW-CA, проходит валидацию против bundle (CustomTrustStore
  содержит оба якоря); серт, подписанный OLD-CA, — тоже (окно двойного
  доверия);
- серт посторонней CA — отклоняется (bundle не расширяет доверие за
  пределы перечисленных якорей);
- один PEM (вне окна) — поведение t06 сохраняется (регресс);
- SAN-сверка — без изменений (кейс от t06 зелёный);
- генерация тестовых CA — действующий `TestPki.cs`.

## 8. Ограничения

1. nodes=1 (v1 канон): «rolling» — одно пересоздание; многонодовая
   ротация — вместе с репликами/sentinel (roadmap).
2. Отмены заявки из панели нет (симметрия rotations; зависшая заявка/
   осиротевший staging — runbook: etcdctl del, следующая ротация
   переиспольз staging put-if-absent).
3. Ротация не затрагивает ACL-креды, maxmemory, endpoints, portalloc.
4. **Клиентское окно bundle — закрывается этой же задачей** (решение
   user-review): окно ротации D→C отдаёт `ca_pem` как bundle из двух
   сертов (OLD+NEW); клиентская библиотека `HA.Valkey`/`App.Valkey`
   дорабатывается в рамках t07 на чтение полного bundle (§3.3) — после
   обеих сторон задачи окно для клиентов прозрачно. До мержа
   Puzzle-стороны сохраняется остаточный риск: reconnect в окне R→C
   даёт TLS-отказ до перечитывания `ca_pem` (самокоррекция poll-циклом,
   секунды). Отдельная roadmap-задача не нужна (временный t15 удалён).
   Одноблочные читатели монорепо (панельные live-пробы,
   метрики-коллектор) транзиентно недоверяют NEW-серту в окне D→C —
   секунды, самокоррекция после C (R11 arch/21).
5. Enterprise-механики (HSM, внешние CA, автоматическое расписание ротации
   по сроку) — вне скоупа (домашняя система; заявка — вручную «по
   потребности», формулировка roadmap).
6. Обновление стендовых чеков (`dev-stand/adminpanel/checks/`) — не входит;
   при желании оператор проверяет вручную.

## 9. Диагностика и наблюдаемость

- Journal `work/<C>` (op=rotate-ca, фазы) — основной diag-ключ; логи фаз
  в стиле существующих процессов (инфо по фазам, warning ждущих).
- Метрики: без новых счётчиков — фазы видны в journal/health-канонах
  (`loops-alive`, `claims`); YAGNI.
- Runbook (`docs/runbook.md`): раздел «ротация CA valkey» — ручная
  проверка состояния окна (ключи staging/bundle), снятие зависшей
  заявки; примечание: окно D→C безопасно для клиентов после исполнения
  обеих сторон задачи t07 (pg + Puzzle-мерж многосертового чтения).

## 10. Критерии приёмки

1. Сборка Release: 0 ошибок, 0 warnings (`TreatWarningsAsErrors`).
2. Юниты: `ValkeyWorker.UnitTests` — все зелёные, вкл. новый
   `CaRotatorTests` и API/вентиль-кейсы; `AdminPanel.UnitTests` — зелёные
   (парсер/команда).
3. Интеграция: `ValkeyWorker.IntegrationTests` — зелёные, вкл. новый
   `CaRotationTests` (реальный etcd, TLS-сервер).
4. Docker-E2E на свежем Release: `ValkeyE2eLifecycleTests` — все кейсы
   (существующие 2 + новый CaRotation) зелёные; teardown-чистота
   подтверждена (контейнеры/тома/сети префикса = 0; телеметрия по
   docs/e2e-launch.md).
5. Соседние серии не регрессировали: KafkaWorker/PgWorker/AdminPanel/
   Shared юниты зелёные (затронутые сборки пересобраны).
6. Греп-гейты чистоты: `ca_rotations` в чистке X2; `RotateValkeyCa`-ручки
   панели не читают `/valkey/` напрямую (мутации — только через API
   воркера); панельный снапшот НЕ выносит `ca_next_*` в UI/API.
7. Канон синхронизирован: arch/20 §2/§2.1/§3/§4/§5, arch/21 §2/§3/§5/§9,
   adminpanel/02 §11.1/§11.2 отражают реализацию один в один (правки уже
   в worktree этим же коммитом; рассинхронов после кода — нет).
8. **Puzzle**: `../Puzzle` изменён ТОЛЬКО в границах §3.3 (многосертовое
   чтение `ca_pem` в `ValkeyConnectionOptions` + тесты); в `../Puzzle` —
   свой отдельный коммит/мерж в своём репозитории, в мерж-гейт этого
   флоу НЕ попадает; юнит-тест чтения bundle из двух сертов (OLD+NEW)
   зелёный в `../Puzzle` (§7.5).

## 11. Мерж-гейт

- Тег `t07-valkey-ca-rotation` удаляется из `arch/roadmap/valkey.md` **тем
  же коммитом** мержа в `main` (правила arch/roadmap/README.md).
- E2E на свежем Release — обязательная часть гейта (код воркера затронут,
  §7.4/§10.4).
- Puzzle-сторона: правки в `../Puzzle` поставляются отдельным мержем в
  его репозитории (вне этого гейта; прецедент t06); в журнале исполнения
  фиксируется ссылка/статус Puzzle-мержа — риск окна закрыт обеими
  сторонами.
- Ревью plan↔spec и код-ревью — по канону dev-flow.

## 12. Open questions (статус)

1. **Puzzle HA.Valkey, многосертовое чтение `ca_pem`** — ЗАКРЫТ решением
   пользователя на гейте user-review: скоуп расширен, `../Puzzle` в
   скоупе задачи, санкция на касание выдана явно (§3.3). Временный
   roadmap-пункт t15-valkey-client-bundle-multicert удалён той же правкой
   (работа внутри t07). Открытых вопросов нет.
