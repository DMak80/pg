# t39-owns3-repl-pubkey-ca — асимметричная идентификация репликации ownS3 (публичные ключи + опциональный CA)

Спецификация задачи dev-flow. Канон механики репликации —
[`arch/owns3/07-replication.md`](../../../arch/owns3/07-replication.md)
(§2 состав mesh, §6 replication-грань, §11 раскладка/env, §12 границы);
политика/конфиг/админ-грань —
[`arch/owns3/05-access-config.md`](../../../arch/owns3/05-access-config.md);
конвейер подписи —
[`arch/owns3/03-protocol.md`](../../../arch/owns3/03-protocol.md).
Постановка — тег `t39-owns3-repl-pubkey-ca` в
[`arch/roadmap/owns3.md`](../../../arch/roadmap/owns3.md) (вносится
фазой 0). Потребитель развёртывания — оркестратор трека s3
(`t36-s3-pool-integration`): разворачивает контейнеры ownS3 уже с
ключами (env) и управляет составом пула join/удалением по API.

## 1. Цель

Сегодня сосед-источник идентифицируется на приёмнике **симметричным
секретом**: SigV4-HMAC с per-соседними peer-кредами, и пара
`accessKey/secretKey` хранится на томах **обеих** сторон (в
targets.json приёмника — secretKey соседа, глава 07 §2/§6). Следствия:

- **компрометация тома одного сервера даёт credential имперсонации
  соседа** (разделяемый секрет украден вместе с targets.json);
- **добавление/удаление серверов требует ручного обмена секретами**:
  на каждом сервере PUT targets с полным списком, включающим креды
  нового соседа — файлы/секреты готовятся вне API.

Решение — **асимметричная идентификация** (согласовано с
пользователем):

1. у каждого сервера пара ключей ECDSA P-256: приватный — только у
   него; публичный — в записи соседа у приёмника;
2. исходящие запросы replication-грани подписываются приватным ключом
   (**прикладная подпись**, не mTLS — TLS остаётся опциональной
   опцией per-install, глава 03 §6); приёмник верифицирует публичным
   ключом и резолвит `sourceName` по имени из сертификата;
3. **опциональный per-install trust-anchor** — публичный ключ CA:
   сервер, представивший сертификат, подписанный этим CA, принимается
   join-handshake'ом автоматически, обмен ключами вручную не нужен;
4. **управление составом — только API** (join / удаление / deny-list),
   развёртывание — env-поставкой: оркестратор разворачивает контейнеры
   уже с ключами, никаких правок конфигурационных файлов извне;
   сервер собственные ключи **не создаёт** — только принимает из env.

## 2. Принципы

1. **Arch-first**: канон `arch/owns3/` меняется раньше кода (фаза 0).
2. **Модель доставки не меняется**: уведомления + pull, курсоры/ACK,
   reconcile, LWW, outbox, анти-петля, метрики — канон 07 нетронут
   (правки только §2/§6/§10/§11/§12 — идентификация и состав).
3. **Клиентская S3-грань не меняется**: SigV4 21 операции, XML-ошибки,
   presigned, чанковая подпись — без изменений; заголовок
   `OWNS3-PEER-*` на клиентской грани — прежний 400 `InvalidRequest`
   (глава 03 §1, «алгоритм не поддерживается»).
4. **Existing-регресс неприкосновенен**: без identity/CA поведение
   байт-в-байт как сегодня — legacy peer-креды работают, старый
   targets.json (4 поля) читается, `PUT /admin/replication/targets`
   прежний; без соседей — как single (глава 07 §12).
5. **Состав mesh остаётся явным списком targets.json** на каждом
   сервере: CA — способ наполнения состава без ручного обмена
   ключами, а НЕ «пуск всех подписанных»: входящие запросы валидны
   только от числящихся соседей (как сегодня — «accessKey числится»,
   §6); единственное исключение — сам join-запрос (§4.4).
6. **Путь записи клиента не затрагивается** (принцип 1 §1): join/
   удаление/deny-list — админ-операции, вне пути 21 операции.
7. **Ключи и доверие не связаны с шифрованием канала**: подпись
   защищает аутентичность/целостность запроса прикладным уровнем;
   транспорт HTTP/TLS — как задан per-install (глава 03 §6).

## 3. Решения (согласованы с пользователем)

| Развилка | Решение |
|---|---|
| Крипто-уровень | **Прикладная подпись запроса**: канонический запрос (канонизация SigV4Core переиспользуется) → SHA-256 → ECDSA P-256. mTLS/X.509 не вводится; TLS остаётся опцией per-install |
| Механика подключения нового сервера | **API-handshake**: `POST /admin/replication/join {endpoint}` на инициаторе → прикладной join-запрос на replication-грань соседа с сертификатом; без discovery и пассивного доверия |
| Full-mesh | **Оркестратор join-ит пары** «новый ↔ существующий» (по одному вызову на пару): механики репликации состава не появляются, состав наблюдаем на каждом сервере |
| Удаление сервера | **Двухступенчато**: `DELETE /admin/replication/targets/{name}` — базово удаляет из targets (курсор сохраняется, как сегодня §2); `?revoke=true` — дополнительно пишет имя+публичный ключ в локальный deny-list; снятие deny — явная админ-команда |
| Заверение серверов | **Собственный мини-сертификат** `{name, publicKeyPem, issuedAtUnixMs, expiresAtUnixMs, caName, signature}` + CLI-утилита в репо (выпуск CA/серверов); X.509/openssl не вводится |
| Развёртывание | **env/конфиг каждого старта — по прецеденту TLS-материала (глава 05 §4), персиста на томе нет**: сервер ключи не создаёт, их поставляет инсталляция (CLI §4.6 → env): `OWNS3_REPLICATION_CERT(_PATH)` / `OWNS3_REPLICATION_PRIVATE_KEY(_PATH)`; trust-anchor `OWNS3_REPLICATION_TRUST_CA(_PATH)`; advertised endpoint `OWNS3_REPLICATION_ADVERTISED_ENDPOINT` |
| Сосуществование | **Записи targets двух видов**: `legacy` (accessKey/secretKey, SigV4 — нынешняя механика) и `key` (publicKey, схема OWNS3-PEER); вид записи определяет схему проверки на грани |
| Идентификация источника | `sourceName` = имя ключевой записи targets = `name` из сертификата (для legacy — прежний резолв по accessKey, §2); вся механика (дедуп `(источник, seqNo)`, fetch-очереди, курсоры, метрики) не меняется |

## 4. Контракт

### 4.1. PeerCertificate и trust-anchor (мини-PKI)

- **Сертификат пира** — JSON camelCase:

```json
{"name": "owns3-n1",
 "publicKeyPem": "-----BEGIN PUBLIC KEY-----…",
 "issuedAtUnixMs": 1760000000000,
 "expiresAtUnixMs": 1791536000000,
 "caName": "owns3-ca",
 "signature": "<base64>"}
```

  - `publicKeyPem` — SubjectPublicKeyInfo ECDSA P-256 (BCL
    `ECDsa.ExportSubjectPublicKeyInfoPem`/`ImportFromPem`);
  - подпись CA: ECDSA P-256 over SHA-256 канонической строки
    `name\npublicKeyPem\nissuedAtUnixMs\nexpiresAtUnixMs\ncaName`,
    формат подписи — **P1363 r||s fixed-length, base64**
    (`DSASignatureFormat.IeeeP1363FixedFieldConcatenation`);
  - `expiresAtUnixMs` ≤ 0 — бессрочный;
  - **Trust-anchor** — JSON `{"name": "owns3-ca", "publicKeyPem": "…"}`:
    сертификат валиден ⟺ `cert.caName == trust.name` ∧ подпись
    верифицирована ключом trust ∧ не истёк `expiresAtUnixMs`.
- **Издатель** (CA-операции — классы в `OwnS3.Protocol`, переиспользуются
  CLI и тестами): генерация пары CA, выпуск сертификата сервера.

### 4.2. Подпись запроса `OWNS3-PEER-ECC-P256-SHA256`

Исходящие запросы replication-грани для key-записей соседей (и
join-запрос) подписываются вместо SigV4:

- заголовки: `Authorization: OWNS3-PEER-ECC-P256-SHA256
  Credential=<peerName>, SignedHeaders=host;x-owns3-peer-date;
  x-owns3-peer-content-sha256, Signature=<base64 P1363>`,
  `x-owns3-peer-date` (`yyyyMMdd'T'HHmmss'Z'`, UTC),
  `x-owns3-peer-content-sha256` (hex SHA-256 тела; GET без тела —
  хэш пустой строки);
- канонический запрос — строителем `SigV4Core` (метод/URI/query/
  headers — та же канонизация, свой список signed-заголовков);
  string-to-sign: `OWNS3-PEER-ECC-P256-SHA256\n<x-owns3-peer-date>\n
  <hex-sha256(canonical request)>`; подпись — приватный ключ identity;
- **clock skew ±15 минут** — тот же допуск, что SigV4 (глава 03 §1);
- исходы на replication-грани — прежний каталог кодов главы 03 §5:
  неверная подпись → **403 `SignatureDoesNotMatch`**; skew → **403
  `RequestTimeTooSkewed`**; malformed Authorization → **400
  `AuthorizationHeaderMalformed`**; peer не числится соседом → **403
  `AccessDenied`** (зеркало нынешней семантики «валидный
  replication-ключ, не числящийся соседом», §6).

### 4.3. Записи targets и резолв приёмником

- **TargetRecord** расширяется:
  `{name, endpoint, auth: "legacy"|"key", accessKey?, secretKey?,
  publicKeyPem?}`; JSON targets.json camelCase; **старый формат
  (без `auth`) читается как `legacy`** — обратная совместимость
  дискового формата. Валидация: имя уникально; `legacy` — пара
  кредов, `key` — валидный PEM публичного ключа ECDSA P-256
  (уникальность accessKey — среди legacy-записей, прежняя семантика).
- **Резолв приёмником** (replication-грань, до диспетчеризации):
  1. `Authorization: OWNS3-PEER-*` → `peerName` из Credential →
     запись в targets с этим именем и `auth=key` → верификация подписи
     ключом записи; записи нет / не key → **403** (не числится
     соседом). Для `/replication/join` — отдельная под-ветка (§4.4).
  2. `Authorization: AWS4-HMAC-SHA256` → существующий конвейер без
     изменений (политика `replication`, резолв по accessKey).
- Статус `GET /admin/replication/status`: per-target добавляется поле
  `auth` (`legacy|key`) — наблюдаемость состава.

### 4.4. Join-протокол (двусторонний handshake)

Инициатор — сервер, которого добавляют (оркестратор зовёт join на
каждой паре «новый ↔ существующий»):

1. `POST /admin/replication/join` тело `{"endpoint":
   "http://peer:9000"}` — права admin/root (глава 05 §7), прочим —
   403. Нет identity, trust-anchor или advertised endpoint (§4.6) →
   **400** с пояснением (fail-fast: без trust-anchor инициатор не
   сможет проверить сертификат ответа соседа — §4.4 п.4). Исполнение
   синхронное (как probe, не 202).
2. Инициатор → сосед: `POST /replication/join` (replication-грань)
   с телом `{"advertisedEndpoint": "<…>"}`, заголовками подписи
   §4.2 и `X-OwnS3-Peer-Certificate: <base64 сертификата>`.
3. Сосед (до-join аутентификация — единственный запрос грани от
   ещё не числящегося пира):
   - trust-anchor не задан → **403** (ручной режим: состав — только
     PUT targets);
   - сертификат невалиден (CA/истёк/deny-list — имя **или** публичный
     ключ) → **403**;
   - имя уже числится с **тем же** публичным ключом → идемпотентное
     обновление endpoint, **200**; с **другим** ключом → **409**;
   - успех → запись `{name, endpoint: advertisedEndpoint, auth: key,
     publicKeyPem}` в targets (merge + персист, горячая замена —
     координатор перестраивает исполнителей по `TargetsChanged`,
     принципы §2) → ответ **200** `{"name": "<своё имя>",
     "advertisedEndpoint": "<свой advertised>", "certificate":
     "<base64 своего сертификата>"}`, тело подписано приватным ключом
     соседа (заголовок `X-OwnS3-Peer-Signature` — подпись §4.2 по
     SHA-256 тела).
4. Инициатор проверяет: сертификат соседа — против **своего**
   trust-anchor, подпись тела — ключом сертификата; валидно →
   добавляет запись соседа в свои targets → админ-ответ **200**
   `{"joined": true, "peer": "<имя>"}`. Невалидно/транспортная
   ошибка → **502** (join не состоялся, состав не менялся).
- Дальше — штатная механика «добавленный сосед» (§2): notifier с
  конца журнала (курсор −1) + reconcile-забор данных.
- Join не ставит событий репликации (анти-петля) и не меняет курсоры.

### 4.5. Удаление и deny-list

- `DELETE /admin/replication/targets/{name}` → **202**: удаление из
  targets (курсор сохраняется — возврат соседа продолжится с места,
  как сегодня §2); `?revoke=true` → дополнительно запись
  `{name, publicKeyPem, revokedAtUnixMs}` в
  `.owns3.sys/replication/denylist.json` (персист tmp+rename);
  `revoke=true` для legacy-записи → **400** (креды отлучаются
  удалением записи и ротацицией — нынешняя модель §2). Неизвестное
  имя → **404**.
- `GET /admin/replication/denylist` → список записей; `DELETE
  /admin/replication/denylist/{name}` → **202** — снятие отзыва
  (следующий join пройдёт). Deny-list локален (не реплицируется):
  отлучение по всей mesh — оркестратор зовёт delete на каждом
  сервере, как и join.
- `PUT /admin/replication/targets` (полная замена) остаётся
  универсальной — работает для записей обоих видов.

### 4.6. Identity, env, развёртывание

| Env | Секция appsettings | Назначение |
|---|---|---|
| `OWNS3_REPLICATION_CERT` / `OWNS3_REPLICATION_CERT_PATH` | `OwnS3:Replication:Identity:Certificate` | сертификат пира (JSON, значение или путь — по прецеденту TLS, глава 05 §4) |
| `OWNS3_REPLICATION_PRIVATE_KEY` / `OWNS3_REPLICATION_PRIVATE_KEY_PATH` | `OwnS3:Replication:Identity:PrivateKeyPem` | приватный ключ (PEM) |
| `OWNS3_REPLICATION_TRUST_CA` / `OWNS3_REPLICATION_TRUST_CA_PATH` | `OwnS3:Replication:TrustCa:Certificate` | trust-anchor `{"name","publicKeyPem"}` |
| `OWNS3_REPLICATION_ADVERTISED_ENDPOINT` | `OwnS3:Replication:AdvertisedEndpoint` | `scheme://host:port` себя для join (обязателен при использовании join) |

- **Identity — материал инсталляции, персиста на томе НЕТ**: читается
  из env/конфига при **каждом** старте (прецедент TLS-материала,
  глава 05 §4); сервер ключи **не генерирует**. Валидация при старте
  fail-fast: сертификат парсится, ключ соответствует публичному ключу
  сертификата, `expiresAt` не истёк — иначе отказ старта.
- Trust-anchor и advertised endpoint — тот же класс материала
  (env/конфиг каждого старта, политика доверия/адресации — не
  состояние узла); на томе живут только состояние и состав
  (targets.json, cursors, denylist.json — §4.5).
- Identity без соседей — репликация всё равно неактивна (состав
  пуст); каталог `replication/` не создаётся вовсе (Existing-регресс
  без исключений — персист identity отсутствует).
- **CLI-утилита** `src/OwnS3.Pki` (console, без внешних пакетов,
  крипто-классы — из `OwnS3.Protocol` §4.7): `ca create --name
  <ca> --out <dir>` → `ca.json` + `ca.private.pem`;
  `server issue --name <n> --ca <ca.json> --ca-key <ca.private.pem>
  [--expires-days N] --out <dir>` → `<n>.cert.json` +
  `<n>.private.pem` + печать готовых env-строк. Оркестратор трека
  s3 использует утилиту и разворачивает контейнеры с env-ключами.

### 4.7. Кодовая раскладка

| Артефакт | Изменение |
|---|---|
| `src/OwnS3.Protocol/PeerAuth/` (новый каталог) | `PeerCertificate` (модель/JSON/валидация), `PeerTrustAnchor`, `PeerCertificateAuthority` (генерация CA, выпуск/верификация сертификатов), `PeerRequestSigner`/`PeerRequestVerifier` (подпись §4.2 поверх канонизации `SigV4Core`, строковые контракты — без ASP.NET, по образцу `SigV4Signer`) |
| `src/OwnS3.Replication/ReplicationTargets.cs` | `TargetRecord` + `auth`/`publicKeyPem`; чтение старого формата как legacy; валидация PEM; `AddOrReplace`/`Remove` (join/delete) |
| `src/OwnS3.Replication/DenyList.cs` (новый) | `denylist.json`: персист tmp+rename, `IsDenied(name, publicKey)`, add/remove |
| `src/OwnS3.Replication/PeerIdentityLoader.cs` (новый) | загрузка identity из env/конфига (значение/путь — прецедент TLS, глава 05 §4), fail-fast валидация; экспонирование `PeerIdentity {Name, Certificate, Key}`; на томе не персистится |
| `src/OwnS3.Replication/JoinPeerService.cs` (новый) | обе стороны handshake (§4.4): исходящий join-запрос + обработка ответа; входящий join (валидация CA/deny/конфликт имени, добавление записи, подписанный ответ); исходы — **Result-конвенция монорепо** (глава 01 §4: Success — терминальный исход, Failed — технический отказ с доменной ошибкой); тестируем без хоста (фейк-транспорт, прецедент `ReplicationProber`) |
| `src/OwnS3.Replication/Transport/ReplicationHttpClient.cs` | для key-записей при наличии identity — подпись `PeerRequestSigner` вместо `SigV4Signer` (выбор по `TargetRecord.Auth`); Result-сигнатуры интерфейса (глава 07 §11) без изменений; legacy — без изменений |
| `src/OwnS3.App/Pipeline/S3Middleware.cs` | replication-ветка: дискриминация `Authorization` (`OWNS3-PEER-*` → peer-конвейер, §4.3) + маршрут `POST /replication/join`; admin-ветка: `POST /admin/replication/join`, `DELETE /admin/replication/targets/{name}`, `GET|DELETE /admin/replication/denylist[/{name}]` |
| `src/OwnS3.App/Handlers/ReplicationHandlers.cs` | `HandleJoinAsync` (входящий join → `JoinPeerService`) |
| `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs` | `HandleJoinTriggerAsync`, `HandleDeleteTargetAsync`, `HandleDenyListAsync`, `HandleDenyRemoveAsync` |
| `src/OwnS3.App/Program.cs` | env-переменные §4.6, DI (`DenyList`, `JoinPeerService`; `PeerIdentity` — результат fail-fast-блока загрузчика, без персиста) |
| `src/OwnS3.Pki` (новый проект) | CLI §4.6; включён в `src/PgWorker.slnx` |

### 4.8. Arch-first правки (фаза 0, до кода)

| Документ | Правка |
|---|---|
| `arch/roadmap/owns3.md` | Постановка задачи — тег `t39-owns3-repl-pubkey-ca` |
| `arch/owns3/07-replication.md` §2 | Записи состава двух видов (legacy/key) + формат targets.json с обратной совместимостью; trust-anchor CA; идентификация источника по имени из сертификата (дополнение к резолву по accessKey); админ-команды join/delete/denylist |
| `arch/owns3/07-replication.md` §6 | Схемы аутентификации грани: SigV4 (legacy) и `OWNS3-PEER-ECC-P256-SHA256` (key) — заголовки/канонизация/исходы; join-под-ветка с сертификатом |
| `arch/owns3/07-replication.md` §10 | Статус per-target `auth`; лог-события join/удаление/отзыв |
| `arch/owns3/07-replication.md` §11 | Раскладка: `denylist.json` (identity на томе не живёт — материал env, как TLS, глава 05 §4); env-таблица — строки §4.6 |
| `arch/owns3/07-replication.md` §12 | Граница «один trust-домен» расширяется: peer-креды **или** асимметричные ключи с опциональным CA per-install; ротация ключей/CA и CRL — вне (roadmap-паттерн) |
| `arch/owns3/05-access-config.md` §3 | Политика `replication` — уточнение: peer-креды legacy; key-соседи аутентифицируются на грани мимо access keys |
| `arch/owns3/05-access-config.md` §4 | Env-таблица — зеркала строк §4.6 |
| `arch/owns3/05-access-config.md` §7 | Админ-грань Mesh-репликации: join/DELETE targets/denylist |

## 5. Фазы

### Фаза 0 — arch-first

Правки по §4.8; канон раньше кода.

### Фаза 1 — крипто-ядро (`src/OwnS3.Protocol`)

`PeerAuth/*` по §4.1–4.2. Юнит-тесты: выпуск/верификация сертификата
(валидный, чужой CA, истёкший, tamper); тест-векторы подписи запроса
(подпись/верификация, tamper canonical request, skew, malformed
Authorization); P1363-формат.

### Фаза 2 — репликация (`src/OwnS3.Replication`)

`TargetRecord`-расширение (+обратная совместимость формата),
`DenyList`, `PeerIdentityLoader`, `JoinPeerService`, подпись в
`ReplicationHttpClient`. Юниты: валидация состава (legacy/key/мусор),
denylist-решения, identity загрузка (значение/путь/fail-fast),
join-механика на фейках
(успех, 403 без trust-anchor, 403 deny-listed, 409 конфликт имени,
идемпотентность того же ключа, 502 при невалидном ответе).

### Фаза 3 — грани и конфиг (`src/OwnS3.App`)

Маршруты/handlers/DI/env по §4.7. Интеграции
(`src/tests/OwnS3.IntegrationTests/Api/`): replication-грань с
peer-подписью (валидный key-сосед → 200; чужой ключ/имя → 403;
tamper → 403 `SignatureDoesNotMatch`; SigV4-legacy — регресс без
изменений); join (успех двусторонний, без trust-anchor → 403,
deny-list → 403, конфликт имени → 409, без identity → 400);
delete (базовый, `?revoke=true` → deny-list блокирует join, снятие
deny → join проходит; legacy+revoke → 400); статус с `auth`.

### Фаза 4 — CLI (`src/OwnS3.Pki`)

Утилита §4.6 (smoke-тест: create → issue → env → контейнер старта).

### Фаза 5 — E2E mesh (`src/tests/OwnS3.IntegrationTests/E2e/Replication/`)

`PeerAuthScenarios.cs` поверх `OwnS3MeshE2eEnvironment` (изоляция/
teardown — `docs/e2e-isolation.md`, `docs/e2e-launch.md`); ключи/CA
тест генерирует в коде (`PeerCertificateAuthority`). Сценарии:

1. mesh 3 ноды на key-записях (env identity+CA у всех): join пар
   оркестратором (тест зовёт admin join) → full mesh; PUT на ноде A
   реплицируется на B/C (доставка уведомлений+фетчей работает на
   peer-подписи).
2. удаление с `revoke=true` на всех → повторный join → 403; снятие
   deny-list → join снова проходит, данные доезжают reconcile.
3. базовое удаление (без revoke) → возврат join'ом → курсор
   продолжается (уведомления не дублируются — дедуп `(источник,
   seqNo)` по имени).
4. сосуществование: mesh из legacy-ноды и key-нод — доставка в обе
   стороны (регресс миграции).
5. trust-anchor не задан у инициатора → admin-join **400**, у соседя →
   join-грань **403** (§4.4); ручной PUT targets с publicKey-записью
   работает без CA.

### Фаза 6 — полные прогоны

Юниты → интеграции → docker-E2E на свежем Release
(`PGW_TEST_DOCKER=1`); зачистка контуров после каждой серии
(AGENTS.md); телеметрия — `docs/e2e-launch.md`.

## 6. Ограничения и границы

- **Вне задачи**: ротация приватных ключей серверов и CA, CRL/списки
  отзыва у CA, краткоживущие сертификаты с автоперевыпуском,
  обязательный TLS/mTLS для репликации, шифрование сверх
  существующей TLS-опции, интеграция утилиты в оркестратор s3
  (потребитель — его задача), enterprise-IAM.
- Отзыв только локальным deny-list (без CRL): «удалил = отлучил»
  гарантируется вызовами delete с revoke на всех серверах mesh —
  той же оркестрацией, что и join.
- Приватный ключ CA существует только у владельца инсталляции
  (вне контейнеров ownS3); ownS3 хранит trust-anchor (публичный
  ключ) и свой приватный ключ identity.
- Сервер ownS3 ключи не генерирует: выпуск — CLI у владельца
  (оркестратора), поставка — env инсталляции; замена ключа = redeploy
  контейнера с новым env.
- Пассивное доверие «любой CA-подписанный» НЕ вводится: членство в
  targets обязательно (принцип 5).
- Existing-регресс подтверждается прогонами сценариев без соседей и
  legacy-кредов unchanged.

## 7. Критерии приёмки

1. Arch-правки (§4.8) закоммичены раньше кодовых (arch-first) и
   соответствуют реализации дословно.
2. Идентификация: replication-грань принимает обе схемы — SigV4 для
   legacy-записей (регресс) и `OWNS3-PEER-ECC-P256-SHA256` для
   key-записей; резолв `sourceName` по имени записи; нечислящийся
   пир → 403; tamper → 403 `SignatureDoesNotMatch`; skew ±15 мин.
3. Join: один вызов `POST /admin/replication/join` → двустороннее
   соседство (обе стороны — через CA-верификацию сертификатов);
   идемпотентность того же ключа; 409 при чужом ключе под тем же
   именем; 403 без trust-anchor/при deny; 502 при невалидном ответе.
4. Удаление: `DELETE targets/{name}` — 202, курсор сохранён;
   `?revoke=true` — deny-list блокирует повторный join (403) до
   явного снятия; `GET/DELETE /admin/replication/denylist` работают.
5. Развёртывание: identity/CA/advertised — из env/конфига при
   каждом старте, персиста на томе нет; сервер ключи не создаёт;
   fail-fast на битый материал; CLI выпускает CA/сертификаты,
   печатает env-строки.
6. Модель доставки не затронута: E2E mesh на key-записях — объекты
   доставлены (уведомления+фетч+reconcile), дедуп по имени,
   метрики/статус работают (статус с полем `auth`).
7. Серии зелёные: юниты (`OwnS3.UnitTests` + новые PeerAuth/Join/
   DenyList/Identity) и Api-интеграции — Debug; docker-E2E серия
   E2e Replication (включая `PeerAuthScenarios`) — на свежем
   Release; между сериями — зачистка контуров.
8. Existing-регресс: прогоны AccessScenarios/RoutingScenarios/
   MetricsHealthScenarios и legacy-peer-сценарии unchanged; старый
   targets.json читается.
9. Мерж-гейт: код трогает `src/OwnS3.*` — docker-E2E на свежем
   Release обязателен (AGENTS.md).
