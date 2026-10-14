# owns3-denylist-replace — полная перезапись deny-list репликации ownS3 по сертификату вызывающего

Спецификация задачи dev-flow. Канон —
[`arch/owns3/07-replication.md`](../../../arch/owns3/07-replication.md)
(§2 состав mesh/deny-list, §6 replication-грань, §10 наблюдаемость);
политика/конфиг/админ-грань —
[`arch/owns3/05-access-config.md`](../../../arch/owns3/05-access-config.md);
конвейер подписи —
[`arch/owns3/03-protocol.md`](../../../arch/owns3/03-protocol.md).
Контракт обновлён arch-first (правки §2/§6/§10 внесены в рамках данной
spec-фазы, см. §4.7).

## 1. Цель

Deny-list репликации сегодня наполняется только точечно: отзыв —
`DELETE /admin/replication/targets/{name}?revoke=true`, снятие одного
отзыва — `DELETE /admin/replication/denylist/{name}`; обе операции —
admin-грань под SigV4 admin/root. Отлучение пира по всей mesh —
оркестратор зовёт точечный revoke-удаление на каждом сервере. Для
декларативного управления списком (единый желаемый список на всю mesh,
заливка «состояние целиком», снятие всех отзывов одним вызовом)
точечных операций недостаточно.

Задача:

1. **метод полной перезаписи deny-list** — идемпотентная замена всего
   списка одним вызовом (семантика PUT, по прецеденту
   `PUT /admin/replication/targets`);
2. **авторизация вызова — только сертификат**: метод принимает вызов
   исключительно в до-join контуре replication-грани — сертификат
   вызывающего в заголовке `X-OwnS3-Peer-Certificate` + подпись
   `OWNS3-PEER-ECC-P256-SHA256`; доверяем вызову ⟺ сертификат
   вызывающего **подписан CA trust-anchor** ∧ **имя/ключ самого
   вызывающего не в deny-list** — та же проверка, что входящий join
   применяет к пиру. SigV4-креды (включая admin/root) к методу не
   ведут.

Вызывающий — оркестратор (владелец инсталляции): не обязан числиться
соседом в targets, не требует SigV4-кредов, раскладывает единый список
по всем серверам mesh той же оркестрацией, что и join.

## 2. Принципы

1. **Arch-first**: контракт `arch/owns3/07-replication.md` меняется
   раньше кода (§4.7).
2. **До-join контур переиспользуется, не дублируется**: проверка
   сертификата вызывающего — ровно те же примитивы, что у входящего
   join (`PeerCertificate.FromJson` → `PeerCertificateAuthority.Verify`
   против trust-anchor → `DenyList.IsDenied` → `PeerRequestVerifier`);
   новых крипто-механик нет.
3. **Область действия deny-list не расширяется**: deny-list решает
   только будущие join (как сегодня); перезапись не трогает существующие
   линки, курсоры, состав targets, outbox, fetch-очереди.
4. **Existing-регресс неприкосновенен**: без identity/trust-anchor
   метод отвечает 403 (ручной режим состава — как join); всё прочее
   поведение сервера байт-в-байт как сегодня; admin-операции над
   deny-list (`GET /admin/replication/denylist`,
   `DELETE /admin/replication/denylist/{name}`, revoke при delete
   targets) остаются без изменений.
5. **Идемпотентность PUT**: повторный вызов с тем же составом —
   успешная замена (время записей обновляется на момент применения);
   пустой `entries` — снятие всех отзывов.
6. **Локальность**: deny-list по-прежнему не реплицируется;
   консистентность по mesh обеспечивает оркестратор, заливая один и
   тот же список на каждый сервер.
7. **Доверие до данных**: сертификат/подпись вызывающего проверяются
   ДО разбора тела — malformed-тело от недоверенного источника не
   рулит кодом ответа.

## 3. Решения (согласованы с пользователем)

| Развилка | Решение |
|---|---|
| Прочтение «доверяем только сертификату, подписанному CA, не входящим в deny-list» | **Авторизация метода по сертификату вызывающего**: сертификат подписан CA trust-anchor ∧ имя/ключ вызывающего не в deny-list — проверка та же, что у входящего join. Записи CA в deny-list (блокировка всех подписанных CA сертификатов) — НЕ вводится |
| Размещение метода | **`PUT /replication/denylist`** — replication-грань, до-join контур (второе исключение peer-конвейера рядом с join); admin-грань и её SigV4-конвейер не расширяются |
| Формат тела | **`{entries:[{name, publicKeyPem}]}`**; `revokedAtUnixMs` каждой записи ставит сервер (момент применения перезаписи); время в теле не передаётся |

## 4. Контракт

### 4.1. Метод `PUT /replication/denylist`

- Путь: `PUT /replication/denylist` — replication-грань, до-join
  контур: диспетчеризация в peer-ветке S3Middleware ДО резолва пира
  по составу (как join); SigV4-ветка грани этот метод не обслуживает:
  admin/root → 403 `AccessDenied` (A13-изоляция — политика грани
  строго replication), replication-ключ числящегося соседа → 400
  `InvalidArgument` (PUT не входит в пост-join диспетчеризацию) —
  оба пути — регресс-кейсы фазы 3.
- Заголовки вызова: `Authorization: OWNS3-PEER-ECC-P256-SHA256
  Credential=<имя вызывающего>, SignedHeaders=host;x-owns3-peer-date;x-owns3-peer-content-sha256,
  Signature=<base64 P1363>` + `x-owns3-peer-date` +
  `x-owns3-peer-content-sha256` (hex SHA-256 тела) +
  `X-OwnS3-Peer-Certificate: <base64 JSON сертификата>` — тот же
  формат, что у join.
- Тело: JSON camelCase `{entries:[{name, publicKeyPem}]}`.
- Успех: **202 Accepted**, без тела; denylist.json заменён целиком,
  горячая замена в памяти — следующий join видит новый список.
- Лог-событие: полная перезапись deny-list (число записей) — arch §10
  дополнен.

### 4.2. Порядок проверок и исходы

Проверки идут в порядке: конфигурация → сертификат → доверие →
подпись → тело (принцип 7).

| # | Условие | Исход |
|---|---|---|
| 1 | identity или trust-anchor сервера не заданы (ручной режим состава) | **403** с пояснением — как join |
| 2 | Заголовок `X-OwnS3-Peer-Certificate` отсутствует | **400** (JSON `error`) — как join |
| 3 | Сертификат не парсится (`PeerCertificate.FromJson`) | **403** (AccessDenied, XML) — как join |
| 4 | Сертификат не проходит `PeerCertificateAuthority.Verify` (не тот CA/подпись CA/истёк) | **403** (AccessDenied, XML) — как join |
| 5 | Вызывающий в deny-list: `DenyList.IsDenied(cert.Name, cert.PublicKeyPem)` — имя ИЛИ ключ | **403** (AccessDenied, XML); список не менялся — отозванный не переписывает список отзывов |
| 6 | Подпись запроса не проходит `PeerRequestVerifier` (ключ — публичный ключ сертификата) | **403** XML с кодом каталога подписи (`SignatureDoesNotMatch`/`RequestTimeTooSkewed`…); malformed Authorization → **400** `AuthorizationHeaderMalformed` — та же пара исходов, что у join |
| 7 | Тело: не-JSON / `entries` отсутствует | **400** (JSON `error`), список не менялся |
| 8 | Запись тела: `name`/`publicKeyPem` пусты; PEM не импортируется как публичный ключ ECDSA P-256 (`ECDsa.ImportFromPem`); дубли имён; дубли ключей | **400** с пояснением (JSON `error`), список не менялся — валидация ДО персиста, по прецеденту `ReplicationTargets.Validate` |
| 9 | Всё валидно | **202**; персист tmp+rename; каждая запись получает `revokedAtUnixMs` = момент применения |

Нюансы:

- `Credential=` в Authorization не обязан совпадать с `cert.Name` и не
  резолвится по targets (вызывающий — не сосед) — как у join.
- Пустое `entries: []` — валидное тело: снятие всех отзывов, файл
  становится `[]`, ответ 202.
- Формат ошибок — стиль join-грани: отказы доверия/подписи — XML
  (`S3ErrorXmlWriter`, AccessDenied/коды каталога); malformed
  заголовка сертификата/тела — 400 JSON `{error}`.
- Лимит тела — дефолт Kestrel; собственного лимита не вводится.

### 4.3. `DenyList.Replace`

Новый метод `DenyList.Replace(IReadOnlyList<(string Name, string PublicKeyPem)> entries)`:

- валидация записей (непустые поля, PEM ECDSA P-256, уникальность имён
  и ключей) — ДО персиста; невалидное — `ArgumentException`, снапшот
  не тронут (прецедент `ReplicationTargets.Replace`);
- под тем же `_lock`, что `Add`/`Remove`/`IsDenied`: полный обмен
  словаря, все записи с `revokedAtUnixMs = timeProvider.GetUtcNow()`;
- персист tmp+rename (существующий `Persist`), каталог — при первой
  записи;
- `Add`/`Remove`/`Snapshot`/`IsDenied` — без изменений.

### 4.4. Сервис входящей перезаписи — `DenyListReplaceService`

Новый класс `src/OwnS3.Replication/DenyListReplaceService.cs` —
входящая сторона перезаписи, тестируемая без хоста (по прецеденту
`JoinPeerService`):

- ctor: `PeerIdentity? identity, PeerTrustAnchor? trustAnchor,
  DenyList denyList, TimeProvider timeProvider,
  ILogger<DenyListReplaceService> logger`;
- `Result<DenyListReplaced> HandleIncoming(S3RequestModel model,
  string certificateBase64, ReadOnlyMemory<byte> rawBody)` — шаги
  1–9 из §4.2: конфигурация → парсинг сертификата → Verify →
  IsDenied → `PeerRequestVerifier.Verify(model, ключ сертификата)` →
  десериализация/валидация тела → `denyList.Replace`;
- ошибки — Result-конвенция: `DenyReplaceRefusedException` (отказ
  доверия/подписи; опциональный `S3ErrorCode` — для отказов подписи,
  грань отдаст `errorCode ?? AccessDenied`) и
  `DenyReplaceBadRequestException` (malformed тело — 400);
- положительный исход — запись с числом применённых записей
  (`DenyListReplaced(int Count)` — для лога/тестов);
- собственных HttpClient/транспорта нет (метод только входящий).

### 4.5. Маршрутизация и хендлер

- `S3Middleware` peer-ветка (`/replication/*`): рядом с join-под-веткой
  добавляется ветка `PUT /replication/denylist` → чтение тела в байты
  → `ReplicationHandlers.HandleDenyReplaceAsync(context, requestId,
  hostId)` → `DenyListReplaceService.HandleIncoming` → маппинг исходов
  по §4.2 (202 / 400 JSON / 403 XML / 400 `AuthorizationHeaderMalformed`
  XML). Модель — та же сборка `S3ModelFactory.Create` (верификатор
  читает только заголовки/путь).
- Хендлер живёт в `ReplicationHandlers` (replication-грань), НЕ в
  `ReplicationAdminHandlers` (admin-грань не участвует).
- DI: регистрация `DenyListReplaceService` в Program рядом с
  `JoinPeerService` (те же identity/trust-anchor/denyList).

### 4.6. Наблюдаемость

- Лог: `LogInformation` — полная перезапись deny-list, число записей;
  `LogWarning` — отказы (проверка сертификата/deny/подписи) с именем
  вызывающего.
- Новых метрик нет: перезапись — редкая админ-операция, канал
  наблюдения — журнал (как у join/отзыва).

### 4.7. Arch-first правки (внесены в рамках spec-фазы)

`arch/owns3/07-replication.md`:

- §2: новый пункт «Полная перезапись deny-list» — контракт метода
  (тело, валидация, исходы, до-join контур, локальность/оркестратор);
  пункт «Удаление и deny-list» скорректирован (ссылка на перезапись);
- §6: join переописан как «запрос грани от ещё не числящегося пира;
  до-join контур — как и `PUT /replication/denylist`»; добавлен пункт
  `PUT /replication/denylist` (аутентификация/исходы/границы
  действия); в таблицу операций добавлена строка `denylist replace`;
- §10: лог-события дополнены «полная перезапись deny-list (число
  записей)».

Код обязан зеркалить эти правки; расхождение — блокирующее замечание
ревью.

### 4.8. Кодовая раскладка

| Файл | Изменение |
|---|---|
| `src/OwnS3.Replication/DenyList.cs` | `Replace(...)` + валидация записей; существующие члены не меняются |
| `src/OwnS3.Replication/DenyListReplaceService.cs` | новый сервис (§4.4) |
| `src/OwnS3.Replication/Transport/ReplicationErrors.cs` | `DenyReplaceRefusedException`, `DenyReplaceBadRequestException` (рядом с join-исключениями) |
| `src/OwnS3.App/Pipeline/S3Middleware.cs` | ветка `PUT /replication/denylist` в peer-конвейере (до резолва пира) |
| `src/OwnS3.App/Handlers/ReplicationHandlers.cs` | `HandleDenyReplaceAsync` |
| `src/OwnS3.App/Program.cs` | DI `DenyListReplaceService` |
| `src/tests/OwnS3.UnitTests/Replication/DenyListTests.cs` | кейсы `Replace` |
| `src/tests/OwnS3.UnitTests/Replication/DenyListReplaceServiceTests.cs` | новый файл — контур доверия/тела без хоста |
| `src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs` | api-кейсы метода |
| `src/tests/OwnS3.IntegrationTests/E2e/Replication/PeerAuthScenarios.cs` | docker-E2E кейсы (§5, фаза 4) |

## 5. Фазы

### Фаза 0 — arch-first

Правки `arch/owns3/07-replication.md` §2/§6/§10 — уже внесены
(§4.7); фаза кода начинается с их сверки (буква канона).

### Фаза 1 — `DenyList.Replace` (юниты)

TDD: кейсы Replace — полный обмен списка; пустой список → файл `[]`;
время записей = момент применения (FakeTimeProvider); идемпотентность
повторным вызовом; невалидные записи (пустые поля/битый PEM/дубли имён/
дубли ключей) → `ArgumentException`, файл/снапшот не тронуты;
персист tmp+rename (файл заменён, `.tmp` не остался); сосуществование
с `Add`/`Remove`/`IsDenied` под локом.

### Фаза 2 — `DenyListReplaceService` (юниты)

По прецеденту `JoinPeerServiceTests` (реальные мини-PKI материалы
`PeerCertificateAuthority.CreateCa`/`IssueServer`, подпись
`PeerRequestSigner`, fake-модель запроса):

- нет identity/trust-anchor → отказ (403-тип);
- сертификат другого CA / истёкший / битый → отказ;
- вызывающий в deny-list по имени; по ключу → отказ;
- подпись чужим ключом / skew / malformed Authorization → отказ с
  кодом каталога;
- валидный вызов → `Replace` вызван с составом тела, исход содержит
  count; пустой `entries` → `Replace([])`;
- malformed JSON / `entries` нет / дубли / битый PEM / пустые поля →
  bad-request-тип, `Replace` НЕ вызван;
- порядок: тело не разбирается при отказе доверия (мусорное тело +
  плохой сертификат → отказ доверия, не 400).

### Фаза 3 — грань (маршрут + хендлер + api-интеграции)

Маршрутизация §4.5; api-интеграции в
`src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs`:

- 202: перезапись списка → `GET /admin/replication/denylist` показывает
  новый состав (время сервера);
- пустой `entries` → 202, список пуст;
- 403: вызывающий отозван (список не изменился); сертификат не от
  trust-anchor; сервер без trust-anchor (ручной режим);
- 400: нет заголовка сертификата; malformed тело; дубли/битый PEM;
- регресс: SigV4-путь закрыт — admin/root → 403 `AccessDenied`
  (A13), replication-ключ соседа → 400 `InvalidArgument`; без
  изменений работают
  `DELETE /admin/replication/denylist/{name}` и revoke при delete.

### Фаза 4 — docker-E2E и полные прогоны

Расширение `src/tests/OwnS3.IntegrationTests/E2e/Replication/PeerAuthScenarios.cs`
(каноны изоляции: guid во всех именах, полный teardown при любом
исходе, динамические порты, `[PHASE]`-телеметрия):

- mesh с CA: перезапись на сервере A списка, содержащего пир B →
  join B→A отказан (403); перезапись пустым списком → join успешен;
- перезапись НЕ рвёт действующую репликацию: после заливки списка без
  пира C линк с C (уже в targets) продолжает доставлять;
- полные прогоны: `dotnet build` (warnings-as-errors) + юниты +
  интеграции OwnS3; зачистка контейнеров/сетей после серии.

## 6. Ограничения и границы

- **Не вводится**: репликация deny-list между серверами; записи CA в
  deny-list (блокировка «всех подписанных данным CA» — прочтение
  отвергнуто пользователем); вторая схема аутентификации на
  admin-грани; CLI-клиент вызова (вызывающий — оркестратор:
  подписывает запрос своим инструментом; тесты используют
  `PeerRequestSigner`); пакетная частичная правка (только полная
  перезапись и существующие точечные операции).
- **Не меняется**: join-контракт; `GET /admin/replication/denylist`;
  `DELETE /admin/replication/denylist/{name}`; revoke при
  `DELETE /admin/replication/targets/{name}?revoke=true`; модель
  доставки (курсоры/outbox/LWW/reconcile); клиентская S3-грань;
  формат denylist.json на диске (запись из трёх полей, camelCase).
- Deny-list остаётся локальным файлом тома
  `.owns3.sys/replication/denylist.json`; гарантии консистентности по
  mesh — оркестратор (локальные файлы — без durability-гарантий,
  реплицированного источника истины для deny-list нет — как сегодня).
- Перезапись действует только на будущие join: уже установленные
  соседи продолжают репликацию (отлучение действующего соседа —
  существующий путь: delete targets с revoke).

## 7. Критерии приёмки

1. `PUT /replication/denylist` с валидным сертификатом вызывающего
   (подписан CA trust-anchor, не отозван) и валидным телом → **202**;
   `denylist.json` заменён целиком (tmp+rename), `GET
   /admin/replication/denylist` возвращает новый состав с серверным
   `revokedAtUnixMs`; следующий join от имени/ключа из списка → 403.
2. Отозванный вызывающий → **403**, файл и снапшот не изменились.
3. Сертификат не подписан trust-anchor / истёк / битый → **403**;
   заголовок сертификата отсутствует → **400**; malformed
   Authorization → **400 AuthorizationHeaderMalformed**; плохая
   подпись → **403 SignatureDoesNotMatch**.
4. Malformed/невалидное тело (не-JSON, нет `entries`, пустые поля,
   битый PEM, дубли имён/ключей) → **400** с пояснением, список не
   менялся; отказ доверия при мусорном теле — 403, не 400.
5. Пустой `entries: []` → **202**, все отзывы сняты, файл `[]`,
   ранее отозванный пир проходит join.
6. Сервер без trust-anchor → **403** (ручной режим); всё остальное
   поведение сервера — регрессионно неизменно (существующие тесты
   зелёные без правок ожиданий, кроме добавленных кейсов).
7. Перезапись не влияет на действующие линки/курсоры/targets (E2E:
   доставка продолжает работать после заливки списка).
8. SigV4-путь к методу закрыт: admin/root → 403 `AccessDenied`
   (A13-изоляция replication-грани); replication-ключ соседа → 400
   `InvalidArgument` (метод — только сертификатный контур).
9. Сборка без ворнингов (`TreatWarningsAsErrors`), юниты +
   интеграции + docker-E2E серии зелёные; E2E — с полным teardown и
   ассертом чистоты (guid-изоляция, динамические порты).
