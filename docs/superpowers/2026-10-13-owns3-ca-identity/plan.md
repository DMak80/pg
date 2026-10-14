# t39-owns3-repl-pubkey-ca — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Асимметричная идентификация репликации ownS3: пары ключей ECDSA P-256 у серверов (env-поставка, без персиста на томе), прикладная подпись `OWNS3-PEER-ECC-P256-SHA256` исходящих запросов replication-грани для key-записей соседей, опциональный per-install trust-anchor CA с join-handshake'ом по API, двухступенчатое удаление с deny-list и CLI-утилита `OwnS3.Pki` — Existing-регресс (SigV4-legacy, поведение без identity/CA) байт-в-байт неприкосновенен.

**Architecture:** Arch-first (канон `arch/owns3/07` §2/§6/§10/§11/§12 и `05` §3/§4/§7 — до кода). Крипто-ядро в `OwnS3.Protocol/PeerAuth/` (строковые контракты без ASP.NET, по образцу `SigV4Signer`/`SigV4HeaderVerifier`; канонизация — переиспользование `CanonicalRequestBuilder`/`SigV4Core`). Состав/identity/join/deny-list в `OwnS3.Replication` (Result-конвенция главы 01 §4, тестируемость без хоста по прецеденту `ReplicationProber`). Грани/DI/env — `OwnS3.App` (дискриминация `Authorization` в replication-ветке `S3Middleware`, новые admin-маршруты). CLI — новый console-проект `src/OwnS3.Pki` поверх крипто-классов Protocol.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), BCL `System.Security.Cryptography` (ECDsa P-256, `DSASignatureFormat.IeeeP1363FixedFieldConcatenation`), xUnit v3 + FluentAssertions, Testcontainers (docker-E2E `PGW_TEST_DOCKER=1`).

**Spec:** [`docs/superpowers/2026-10-13-owns3-ca-identity/spec.md`](spec.md) — план аргументируется от spec; исполнители читают оба документа.

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-owns3-ca-identity` (ветка `feat-owns3-ca-identity`, ребейзнута на `main` 37dd19db). Все пути ниже — от корня worktree; все команды — из корня worktree.

## Global Constraints

- **Arch-first (spec §2.1, критерий 1):** Task 0 (правки `arch/` + roadmap) коммитится РАНЬШЕ любого кодового коммита; arch-текст соответствует реализации дословно.
- **Existing-регресс неприкосновенен (spec §2.4, критерии 2/8):** без identity/CA поведение байт-в-байт как сегодня: legacy peer-креды работают, старый targets.json (4 поля) читается как `legacy`, `PUT /admin/replication/targets` прежний, без соседей — как single; клиентская S3-грань на `OWNS3-PEER-*` отвечает прежним 400 `InvalidRequest` (глава 03 §1 — код не трогаем).
- **Модель доставки не меняется (spec §2.2):** уведомления+pull, курсоры/ACK, reconcile, LWW, outbox, анти-петля, метрики — канон 07 нетронут; правки только §2/§6/§10/§11/§12 (идентификация и состав). `IReplicationTransport` — Result-сигнатуры без изменений (spec §4.7).
- **Сервер собственные ключи не создаёт (spec §3/§4.6):** identity/trust/advertised — env/конфиг каждого старта, персиста на томе НЕТ (прецедент TLS, глава 05 §4); на томе из нового — только `denylist.json`. Identity без соседей — каталог `replication/` не создаётся вовсе.
- **Состав mesh остаётся явным списком targets.json (spec §2.5):** CA — способ наполнения состава (join), НЕ «пуск всех подписанных»: входящие валидны только от числящихся соседей; исключение — сам join-запрос.
- **Result-конвенция (арх 01 §4):** исходы JoinPeerService/пограничных операций — `Result`/`Result<T>` из `Shared.Core`: Success — терминальный исход, Failed — технический/доменный отказ с типизированной ошибкой. `OwnS3.Protocol` Result-сигнатур не получает (служебная ссылка на `Shared.Core` — конвенция t38): строковые контракты/`null`-маркеры, маппинг в HTTP-коды — в App.
- **Язык:** комментарии/документация — русские; идентификаторы — английские; тесты — AAA-комментарии (`// Arrange`, `// Act`, `// Assert`).
- **`TreatWarningsAsErrors=true`, `Nullable=enable`** — каждый коммит собирается чисто.
- **Вне задачи (spec §6):** ротация ключей/CA, CRL, автоперевыпуск, обязательный TLS/mTLS, интеграция CLI в оркестратор s3, enterprise-IAM.
- **Запуск команд:** все dotnet-прогоны — `DOTNET_CLI_UI_LANGUAGE=en`; docker-E2E — `PGW_TEST_DOCKER=1`, Release, E2eFixture собирает образ сам; после КАЖДОЙ тестовой серии — зачистка контуров + контрольные grep (AGENTS.md, телеметрия `docs/e2e-launch.md`); порты только динамические; `BrokerBootSec`/ожидания ≤100 с.
- **Мерж-гейт:** тег `t39-owns3-repl-pubkey-ca` удаляется из `arch/roadmap/owns3.md` мерж-коммитом задачи (канон AGENTS.md).

## Контракт кодировки (общий для всех задач)

- **Сертификат пира** — JSON camelCase `{"name","publicKeyPem","issuedAtUnixMs","expiresAtUnixMs","caName","signature"}`; `publicKeyPem` — SubjectPublicKeyInfo ECDSA P-256; подпись CA — ECDSA P-256 over SHA-256 строки `name\npublicKeyPem\nissuedAtUnixMs\nexpiresAtUnixMs\ncaName`, формат **P1363 r||s fixed-length, base64**; `expiresAtUnixMs ≤ 0` — бессрочный.
- **Trust-anchor** — JSON `{"name","publicKeyPem"}`; сертификат валиден ⟺ `cert.caName == trust.name` ∧ подпись верна ∧ не истёк.
- **Подпись запроса `OWNS3-PEER-ECC-P256-SHA256`** (spec §4.2): заголовки `x-owns3-peer-date` (`yyyyMMdd'T'HHmmss'Z'` UTC), `x-owns3-peer-content-sha256` (hex SHA-256 тела; GET без тела — хэш пустой строки); `Authorization: OWNS3-PEER-ECC-P256-SHA256 Credential=<peerName>, SignedHeaders=host;x-owns3-peer-date;x-owns3-peer-content-sha256, Signature=<base64 P1363>`; канонический запрос — `CanonicalRequestBuilder.Build` с фиксированным списком `host;x-owns3-peer-date;x-owns3-peer-content-sha256`; string-to-sign `OWNS3-PEER-ECC-P256-SHA256\n<date>\n<hex-sha256(canonical)>`; подпись — `ECDsa.SignData(utf8(stringToSign), SHA256)` P1363 base64; skew ±15 мин.
- **Сертификат «на проводе»** (заголовок `X-OwnS3-Peer-Certificate`, поле `certificate` ответа join) — base64 стандарта от UTF-8 байтов JSON сертификата.
- **Подпись тела join-ответа** (`X-OwnS3-Peer-Signature`) — `ECDsa.SignData(utf8(тело), SHA256)` P1363 base64 приватным ключом отвечающего; инициатор проверяет `VerifyData` ключом сертификата отвечающего.
- **Join требует на ОБЕИХ сторонах** identity, trust-anchor и advertised endpoint (spec §4.4 п.1: нет любого у инициатора → **400 с пояснением** — fail-fast: без trust-anchor инициатор не сможет проверить сертификат ответа соседа, §4.4 п.4); на replication-грани соседа отсутствие trust/identity → 403. Проксирование исходов: сосед ответил 403/409 — тот же код админ-ответу; не-200/битое тело/невалидная подпись ответа/транспорт → 502.

---

## Фаза 0 — arch-first (Task 0)

## Task 0: Канон `arch/owns3/` + постановка roadmap

**Files:**
- Modify: `arch/roadmap/owns3.md` (раздел «Задачи» — тег постановки)
- Modify: `arch/owns3/07-replication.md` (§2, §6, §10, §11, §12)
- Modify: `arch/owns3/05-access-config.md` (§3, §4, §7)

**Interfaces:**
- Consumes: ничего (канон до кода).
- Produces: канонические формулировки, которые Tasks 1–15 зеркалят дословно (см. тексты правок ниже).

- [ ] **Step 0.1: `arch/roadmap/owns3.md` — постановка задачи**

В раздел «Задачи» (после вводной части) добавить пункт:

```markdown
- `t39-owns3-repl-pubkey-ca` — асимметричная идентификация репликации
  ownS3: пары ключей ECDSA P-256 per-сервер (env-поставка без персиста на
  томе), прикладная подпись `OWNS3-PEER-ECC-P256-SHA256` исходящих
  replication-запросов для key-записей соседей, опциональный per-install
  trust-anchor CA + join-handshake по API (двусторонний, оркестратор
  join-ит пары), двухступенчатое удаление `?revoke=true` с локальным
  deny-list, CLI `src/OwnS3.Pki` (выпуск CA/серверов). Existing-регресс
  (legacy SigV4-креды, поведение без identity/CA) — байт-в-байт.
```

- [ ] **Step 0.2: `07-replication.md` §2 — записи двух видов, CA, join/delete/denylist**

В §2 заменить первый подпункт «**Сосед (target)** — запись `TargetRecord {name, endpoint, accessKey, secretKey}`…» на (остальной текст §2 сохраняется):

```markdown
- **Сосед (target)** — запись двух видов (поле `auth`):
  - `legacy` — `{name, endpoint, accessKey, secretKey}`: симметричная
    SigV4-идентификация (прежняя механика §6); формат без поля `auth`
    (старый targets.json) читается как `legacy` — обратная
    совместимость дискового формата;
  - `key` — `{name, endpoint, publicKeyPem}`: асимметричная
    идентификация — исходящие запросы к соседю подписываются приватным
    ключом identity (§6, схема OWNS3-PEER), входящие от него
    верифицируются `publicKeyPem` записи.
  `endpoint` — `scheme://host:port`, advertised. Валидация состава:
  имена уникальны; `legacy` — пара кредов и уникальность accessKey
  среди legacy-записей; `key` — валидный PEM публичного ключа ECDSA
  P-256.
```

И заменить существующий буллет §2 «**Идентификация соседа-источника на приёмнике — по accessKey подписи** запроса к replication-грани (§6): креды пер-соседние, имя источника (`sourceName`) — запись `TargetRecord.Name` приёмника, чей accessKey совпал с подписью. Заголовок уведомлений `originHostId`…» на:

```markdown
- **Идентификация соседа-источника на приёмнике**: `legacy` — по
  accessKey подписи запроса к replication-грани (креды пер-соседние,
  §6); `key` — по имени из сертификата/записи (§6, схема OWNS3-PEER).
  Имя источника (`sourceName`) в обоих случаях — запись
  `TargetRecord.Name` приёмника. Заголовок уведомлений `originHostId`
  (env `OWNS3_HOST_ID`, глава 05 §4) — **диагностика конфликтов**,
  НЕ ключ идентификации.
```

И добавить в конец §2 (после абзаца про оркестратор трека s3) подразделы:

```markdown
- **Identity сервера** — пара ключей ECDSA P-256 + мини-сертификат
  `{name, publicKeyPem, issuedAtUnixMs, expiresAtUnixMs, caName,
  signature}` (подпись CA — ECDSA P-256 over SHA-256 канонической
  строки `name\npublicKeyPem\nissuedAtUnixMs\nexpiresAtUnixMs\ncaName`,
  формат P1363 base64; `expiresAtUnixMs ≤ 0` — бессрочный).
  **Материал инсталляции**: env `OWNS3_REPLICATION_CERT`/`_PATH` +
  `OWNS3_REPLICATION_PRIVATE_KEY`/`_PATH` (значение/путь — прецедент
  TLS-материала, глава 05 §4), читается при каждом старте, на томе не
  персистится; сервер ключи НЕ генерирует — выпуск CLI `OwnS3.Pki` у
  владельца инсталляции (оркестратора), замена = redeploy с новым env.
  Fail-fast старта: сертификат парсится, ключ соответствует публичному
  ключу сертификата, `expiresAt` не истёк. `OWNS3_REPLICATION_ADVERTISED_
  ENDPOINT` — scheme://host:port себя для join.
- **Trust-anchor (опциональный, per-install)** — публичный ключ CA
  `{"name","publicKeyPem"}` (env `OWNS3_REPLICATION_TRUST_CA`/`_PATH`);
  сертификат валиден ⟺ `caName` совпадает ∧ подпись CA верна ∧ не истёк.
- **Join-протокол** (двусторонний handshake, оркестратор трека s3 зовёт
  `POST /admin/replication/join` телом `{"endpoint"}` на каждой паре
  «новый ↔ существующий»; права admin/root, исполнение синхронное).
  Нет identity, trust-anchor или advertised endpoint (§11) → **400 с
  пояснением** (fail-fast: без trust-anchor инициатор не сможет
  проверить сертификат ответа соседа). Инициатор шлёт
  `POST /replication/join` телом `{"advertisedEndpoint"}` на
  replication-грань соседа — подпись §6, заголовок
  `X-OwnS3-Peer-Certificate: <base64 сертификата>`.
  Сосед: trust-anchor не задан → **403** (ручной режим состава);
  сертификат невалиден (CA/истёк/deny-list — имя ИЛИ публичный ключ) →
  **403**; имя числится с тем же ключом → идемпотентное обновление
  endpoint, **200**; с другим ключом → **409**; успех → запись
  `{name, endpoint: advertisedEndpoint, auth: key, publicKeyPem}` в
  targets (merge + персист, горячая замена по `TargetsChanged`) и ответ
  **200** `{"name","advertisedEndpoint","certificate"}` с заголовком
  `X-OwnS3-Peer-Signature` (подпись тела приватным ключом соседа).
  Инициатор проверяет сертификат соседа против СВОЕГО trust-anchor и
  подпись тела — валидно → добавляет запись соседа в свои targets →
  админ-ответ 200 `{"joined":true,"peer"}`; отказ соседа 403/409
  проксируется тем же кодом; невалидный ответ/транспорт → **502**.
  Join не ставит событий репликации (анти-петля) и не меняет курсоры;
  дальше — штатная механика «добавленный сосед» (курсор −1 +
  reconcile-забор).
- **Удаление и deny-list**: `DELETE /admin/replication/targets/{name}`
  → **202** (удаление из targets, курсор сохраняется — возврат соседа
  продолжится с места); `?revoke=true` — дополнительно запись
  `{name, publicKeyPem, revokedAtUnixMs}` в
  `.owns3.sys/replication/denylist.json` (персист tmp+rename; для
  legacy-записи → **400**); неизвестное имя → **404**. `GET
  /admin/replication/denylist` — список; `DELETE /admin/replication/
  denylist/{name}` → **202** — снятие отзыва. Deny-list локален (не
  реплицируется): отлучение по всей mesh — оркестратор зовёт delete с
  revoke на каждом сервере (той же оркестрацией, что и join).
```

- [ ] **Step 0.3: `07-replication.md` §6 — вторая схема аутентификации + join-ветка**

В §6 первый абзац («Служебная грань вне 21 операции…») дополнить предложением и добавить после него блок (таблица операций §6 сохраняется):

```markdown
Грань принимает две схемы аутентификации: `AWS4-HMAC-SHA256`
(legacy-записи, конвейер без изменений) и `OWNS3-PEER-ECC-P256-SHA256`
(key-записи, §2) — выбор по префиксу заголовка `Authorization`.

- **Схема OWNS3-PEER-ECC-P256-SHA256** (key-соседи): заголовки
  `x-owns3-peer-date`, `x-owns3-peer-content-sha256` (hex SHA-256 тела);
  `Authorization: OWNS3-PEER-ECC-P256-SHA256 Credential=<peerName>,
  SignedHeaders=host;x-owns3-peer-date;x-owns3-peer-content-sha256,
  Signature=<base64 P1363>`; канонический запрос — канонизация SigV4
  (глава 03 §1, тот же строитель); string-to-sign
  `OWNS3-PEER-ECC-P256-SHA256\n<x-owns3-peer-date>\n<hex-sha256(
  canonical request)>`; подпись — ECDSA P-256 приватным ключом identity
  отправителя. Исходы (каталог главы 03 §5): неверная подпись → 403
  `SignatureDoesNotMatch`; clock skew ±15 мин → 403
  `RequestTimeTooSkewed`; malformed Authorization → 400
  `AuthorizationHeaderMalformed`; peer не числится соседом (или запись
  не key) → 403 `AccessDenied`. Резолв: `peerName` из Credential →
  запись targets с этим именем и `auth=key` → верификация ключом
  записи; `sourceName` = имя записи. Клиентская S3-грань эту схему не
  принимает (400 `InvalidRequest`, глава 03 §1).
- **`POST /replication/join`** (единственный запрос грани от ещё не
  числящегося пира): до-join аутентификация по сертификату заголовка
  `X-OwnS3-Peer-Certificate` — trust-anchor не задан → 403;
  CA/истёк/deny-list (имя или ключ) → 403; подпись запроса
  верифицируется публичным ключом сертификата; тело
  `{"advertisedEndpoint"}`; конфликты и ответ — §2 (409/идемпотентность/
  200 + `X-OwnS3-Peer-Signature`).
```

В таблицу операций §6 добавить строку:

```markdown
| join | `POST /replication/join` — JSON `{advertisedEndpoint}` + заголовок `X-OwnS3-Peer-Certificate` | До-join аутентификация по сертификату (§2/§6); ответ 200 `{name, advertisedEndpoint, certificate}` + `X-OwnS3-Peer-Signature`; 403/409/400 — по §2 |
```

- [ ] **Step 0.4: `07-replication.md` §10/§11/§12**

§10: в абзац «**Статус** — `GET /admin/replication/status`…» добавить `auth` в перечисление полей соседа (`{name, auth, online, …}` — наблюдаемость состава); в «**Лог-события**» дополнить: «join (обе стороны: имя/endpoint пира), удаление соседа, отзыв (deny-list) и снятие отзыва».

§11: в раскладку тома добавить строку `├── denylist.json ← локальный deny-list отзыва (§2)` (после `targets.json`); в env-таблицу добавить 4 строки:

```markdown
| `OWNS3_REPLICATION_CERT` / `OWNS3_REPLICATION_CERT_PATH` | сертификат пира (JSON, значение/путь — прецедент TLS, глава 05 §4); секция `OwnS3:Replication:Identity:Certificate`/`CertificatePath` | нет — peer-схема не активна |
| `OWNS3_REPLICATION_PRIVATE_KEY` / `OWNS3_REPLICATION_PRIVATE_KEY_PATH` | приватный ключ identity (PEM); секция `OwnS3:Replication:Identity:PrivateKeyPem`/`PrivateKeyPath` | нет |
| `OWNS3_REPLICATION_TRUST_CA` / `OWNS3_REPLICATION_TRUST_CA_PATH` | trust-anchor CA `{"name","publicKeyPem"}`; секция `OwnS3:Replication:TrustCa:Certificate`/`CertificatePath` | нет — join-ручной режим |
| `OWNS3_REPLICATION_ADVERTISED_ENDPOINT` | `scheme://host:port` себя для join; секция `OwnS3:Replication:AdvertisedEndpoint` | нет |
```

и абзац: «Identity/trust/advertised — материал env каждого старта (как TLS, глава 05 §4), на томе не живёт; на томе из репликационного состояния добавляется только `denylist.json`.»

§12: заменить пункт «**Один trust-домен**…» на:

```markdown
- **Один trust-домен**: peer-креды per-install (targets.json — секрет на
  локальном томе — допускается по прецеденту главы 05 §2) ИЛИ
  асимметричные ключи с опциональным CA per-install (§2: legacy/key);
  ротация ключей/CA и CRL — вне (roadmap-паттерн); enterprise-IAM/mTLS
  — вне.
```

- [ ] **Step 0.5: `05-access-config.md` §3/§4/§7**

§3: в абзац «**Политика `replication`**…» добавить: «legacy peer-креды; key-соседи аутентифицируются на replication-грани мимо access keys (схема OWNS3-PEER, глава 07 §6) — записи `auth=key` в кредах не нуждаются».

§4: в env-таблицу добавить 4 строки-зеркала из Step 0.4 (те же env/секция/назначение/дефолт) и абзац: «Identity-материал (сертификат/ключ/trust/advertised) — пара значение/путь по прецеденту TLS (п. 4); неполная пара (серт без ключа и наоборот), битый JSON/PEM, ключ не соответствует сертификату, истёкший `expiresAt` — отказ старта (fail-fast)».

§7 (Mesh-репликация): добавить три эндпоинта:

```markdown
- **`POST /admin/replication/join`** — join-handshake с соседом (тело
  `{"endpoint"}`): 200 `{"joined":true,"peer"}` / 400 (нет identity,
  trust-anchor или advertised endpoint — глава 07 §2) / 403, 409
  (прокси исхода соседа) / 502 (невалидный ответ/транспорт) — глава
  07 §2.
- **`DELETE /admin/replication/targets/{name}[?revoke=true]`** —
  удаление соседя: 202 (курсор сохраняется); `revoke=true` — запись в
  локальный deny-list (legacy-запись → 400; неизвестное имя → 404) —
  глава 07 §2.
- **`GET /admin/replication/denylist`, `DELETE /admin/replication/
  denylist/{name}`** — список отозванных / снятие отзыва (202) —
  глава 07 §2.
```

- [ ] **Step 0.6: Проверка и коммит**

Проверка: `git diff --stat` — только 3 arch-файла; вычитка на соответствие spec §4.8 (все 9 строк таблицы закрыты: roadmap, 07 §2/§6/§10/§11/§12, 05 §3/§4/§7) и самосогласованность §2 (буллет идентификации переписан под два вида записей — дублей со старой формулировкой нет).

```bash
git add arch/roadmap/owns3.md arch/owns3/07-replication.md arch/owns3/05-access-config.md
git commit -m "t39 arch: асимметричная идентификация репликации ownS3 — записи targets legacy/key, схема OWNS3-PEER-ECC-P256-SHA256, join/delete/denylist, env identity/CA, roadmap-постановка"
```

**Выход:** канон опережает код (критерий 1 spec). **Spec:** §4.8, §5 фаза 0.

---

## Фаза 1 — крипто-ядро `src/OwnS3.Protocol` (Tasks 1–3)

## Task 1: `PeerAuth/PeerCertificate` + `PeerTrustAnchor`

**Files:**
- Create: `src/OwnS3.Protocol/PeerAuth/PeerCertificate.cs`
- Create: `src/OwnS3.Protocol/PeerAuth/PeerTrustAnchor.cs`
- Test: `src/tests/OwnS3.UnitTests/PeerAuth/PeerCertificateTests.cs`

**Interfaces:**
- Produces (используют Tasks 2, 3, 6, 7, CLI, E2E):
  `PeerCertificate(string Name, string PublicKeyPem, long IssuedAtUnixMs, long ExpiresAtUnixMs, string CaName, string Signature)` c `ToJson()`/`FromJson(string)` (camelCase, бросает `JsonException`/`InvalidOperationException`), `CanonicalForm()` → `name\npublicKeyPem\nissuedAtUnixMs\nexpiresAtUnixMs\ncaName`, `IsExpired(DateTimeOffset now)`; `PeerTrustAnchor(string Name, string PublicKeyPem)` c `ToJson()`/`FromJson(string)`.

- [ ] **Step 1.1: Пишу failing-тесты** `PeerCertificateTests.cs` (файл `GlobalUsings.cs` юнитов уже даёт xUnit/FluentAssertions):

```csharp
namespace OwnS3.UnitTests.PeerAuth;

// Модель сертификата пира (глава 07 §2): JSON camelCase roundtrip,
// каноническая строка подписи, бессрочность, валидация trust-anchor.
public class PeerCertificateTests
{
    private static readonly PeerCertificate Sample = new(
        "owns3-n1", "-----BEGIN PUBLIC KEY-----\nAAEB\n-----END PUBLIC KEY-----\n",
        1760000000000, 1791536000000, "owns3-ca", "c2ln");

    [Fact]
    public void Json_Roundtrip_CamelCase() { /* Arrange — Sample; Act — ToJson → FromJson;
        Assert — эквивалентность всех полей; JSON содержит "publicKeyPem"/"issuedAtUnixMs" */ }

    [Fact]
    public void CanonicalForm_FiveLines_OrderFixed() { /* "owns3-n1\n<pem>\n1760000000000\n1791536000000\nowns3-ca" */ }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ExpiresAt_NonPositive_NeverExpires(long expires) { /* IsExpired(любое далёкое now) == false */ }

    [Fact]
    public void IsExpired_Past_Expires() { /* expires=1000, now=2026 → true */ }

    [Fact]
    public void FromJson_Malformed_Throws() { /* мусорная строка → Exception (не null) */ }

    [Fact]
    public void TrustAnchor_Json_Roundtrip() { /* {"name","publicKeyPem"} camelCase roundtrip */ }
}
```

- [ ] **Step 1.2: Прогон — падают** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~PeerCertificateTests` → FAIL (типы не существуют).

- [ ] **Step 1.3: Реализация** `PeerCertificate.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace OwnS3.Protocol.PeerAuth;

// Мини-сертификат пира ownS3 (глава 07 §2): JSON camelCase; подпись CA —
// P1363 base64 по канонической строке CanonicalForm(); expiresAtUnixMs ≤ 0 —
// бессрочный. X.509 сознательно не вводится.
public sealed record PeerCertificate(
    string Name, string PublicKeyPem, long IssuedAtUnixMs, long ExpiresAtUnixMs,
    string CaName, string Signature)
{
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public string ToJson() => JsonSerializer.Serialize(this, Json);

    // Бросает при malformed JSON / пустых обязательных полях (fail-fast загрузки).
    public static PeerCertificate FromJson(string json)
    {
        var cert = JsonSerializer.Deserialize<PeerCertificate>(json, Json)
            ?? throw new InvalidOperationException("пустой JSON сертификата пира");
        if (string.IsNullOrWhiteSpace(cert.Name) || string.IsNullOrWhiteSpace(cert.PublicKeyPem)
            || string.IsNullOrWhiteSpace(cert.CaName) || string.IsNullOrWhiteSpace(cert.Signature))
            throw new InvalidOperationException("сертификат пира: обязательные поля пусты");
        return cert;
    }

    // Каноническая строка подписи CA (глава 07 §2).
    public string CanonicalForm() =>
        $"{Name}\n{PublicKeyPem}\n{IssuedAtUnixMs}\n{ExpiresAtUnixMs}\n{CaName}";
}
```

`PeerTrustAnchor.cs` — по образцу (`Name`, `PublicKeyPem`, `ToJson`/`FromJson` c проверкой непустоты).

- [ ] **Step 1.4: Прогон — зелёные; коммит** (проверка как в 1.2 → PASS).

```bash
git add src/OwnS3.Protocol/PeerAuth/ src/tests/OwnS3.UnitTests/PeerAuth/
git commit -m "t39: модель сертификата пира и trust-anchor (Protocol/PeerAuth) — JSON camelCase, каноническая строка, бессрочность"
```

**Вход:** Task 0 слит. **Выход:** модели для CA/подписи/загрузки. **Spec:** §4.1.

## Task 2: `PeerAuth/PeerCertificateAuthority`

**Files:**
- Create: `src/OwnS3.Protocol/PeerAuth/PeerCertificateAuthority.cs`
- Test: `src/tests/OwnS3.UnitTests/PeerAuth/PeerCertificateAuthorityTests.cs`

**Interfaces:**
- Consumes: `PeerCertificate`, `PeerTrustAnchor` (Task 1).
- Produces (используют Tasks 6, 7, 12, 13, 14 — CLI и тесты генерируют материал этой же библиотекой):
  `static (string CaJson, string CaPrivatePem) CreateCa(string name)`; `static (PeerCertificate Cert, string PrivatePem) IssueServer(string name, PeerTrustAnchor ca, string caPrivatePem, DateTimeOffset at, int expiresDays = 365)`; `static bool Verify(PeerCertificate cert, PeerTrustAnchor trust, DateTimeOffset now)`; `const int DefaultExpiresDays = 365`.

- [ ] **Step 2.1: Failing-тесты** — кейсы (тела по AAA, материал генерится `CreateCa` в Arrange):

```csharp
public class PeerCertificateAuthorityTests
{
    [Fact]
    public void IssueAndVerify_ValidCertificate_True() { /* CA + серт → Verify(now) == true */ }

    [Fact]
    public void Verify_OtherCa_False() { /* серт CA-A против trust CA-B */ }

    [Fact]
    public void Verify_Expired_False() { /* expiresDays=1, now = at + 2 дня */ }

    [Fact]
    public void Verify_TamperedCertificate_False() { /* изменили Name/PublicKeyPem/IssuedAt в JSON → false */ }

    [Fact]
    public void Verify_WrongCaName_False() { /* caName серта ≠ name trust при том же ключе → false */ }

    [Fact]
    public void Signature_P1363_64Bytes() { /* base64 → 64 байта (r||s по 32) — формат фиксированной длины */ }

    [Fact]
    public void IssuedKey_Matches_CertificatePublicKey() { /* ImportFromPem(cert.PublicKeyPem) валиден, ExportSubjectPublicKeyInfoPem == PublicKeyPem */ }

    [Fact]
    public void ServerKey_SignVerify_P256() { /* приватный ключ выпуска подписывает данные, публичный ключ серта верифицирует (ECDSA P-256) */ }
}
```

- [ ] **Step 2.2: Прогон — падают** (фильтр `FullyQualifiedName~PeerCertificateAuthorityTests`).

- [ ] **Step 2.3: Реализация**:

```csharp
using System.Security.Cryptography;
using System.Text;

namespace OwnS3.Protocol.PeerAuth;

// Издатель мини-PKI ownS3 (глава 07 §2): генерация CA, выпуск и верификация
// сертификатов серверов. Используется CLI OwnS3.Pki и тестами; сервер ownS3
// ключи не генерирует. Подписи — ECDSA P-256 over SHA-256, P1363 fixed-length.
public static class PeerCertificateAuthority
{
    public const int DefaultExpiresDays = 365;

    public static (string CaJson, string CaPrivatePem) CreateCa(string name)
    {
        using var ca = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var trust = new PeerTrustAnchor(name, ca.ExportSubjectPublicKeyInfoPem());
        return (trust.ToJson(), ca.ExportPkcs8PrivateKeyPem());
    }

    public static (PeerCertificate Cert, string PrivatePem) IssueServer(string name,
        PeerTrustAnchor ca, string caPrivatePem, DateTimeOffset at, int expiresDays = DefaultExpiresDays)
    {
        using var caKey = ECDsa.Create();
        caKey.ImportFromPem(caPrivatePem);
        using var server = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var publicKeyPem = server.ExportSubjectPublicKeyInfoPem();
        var issued = at.ToUnixTimeMilliseconds();
        var expires = expiresDays <= 0 ? 0 : at.AddDays(expiresDays).ToUnixTimeMilliseconds();
        var canonical = $"{name}\n{publicKeyPem}\n{issued}\n{expires}\n{ca.Name}";
        var signature = Convert.ToBase64String(caKey.SignData(Encoding.UTF8.GetBytes(canonical),
            HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation));
        return (new PeerCertificate(name, publicKeyPem, issued, expires, ca.Name, signature),
            server.ExportPkcs8PrivateKeyPem());
    }

    // Сертификат валиден ⟺ caName совпадает ∧ подпись CA верна ∧ не истёк (глава 07 §2).
    public static bool Verify(PeerCertificate cert, PeerTrustAnchor trust, DateTimeOffset now)
    {
        if (!string.Equals(cert.CaName, trust.Name, StringComparison.Ordinal)) return false;
        if (cert.IsExpired(now)) return false;
        using var caKey = ECDsa.Create();
        caKey.ImportFromPem(trust.PublicKeyPem);
        return caKey.VerifyData(Encoding.UTF8.GetBytes(cert.CanonicalForm()),
            Convert.FromBase64String(cert.Signature), HashAlgorithmName.SHA256,
            DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
    }
}
```

`IsExpired` (Task 1): `ExpiresAtUnixMs > 0 && now.ToUnixTimeMilliseconds() >= ExpiresAtUnixMs`.

- [ ] **Step 2.4: Прогон — зелёные; коммит**

```bash
git add src/OwnS3.Protocol/PeerAuth/PeerCertificateAuthority.cs src/tests/OwnS3.UnitTests/PeerAuth/PeerCertificateAuthorityTests.cs
git commit -m "t39: PeerCertificateAuthority — выпуск/верификация сертификатов ECDSA P-256 P1363"
```

**Вход:** Task 1. **Выход:** издатель для CLI/тестов/верификации join. **Spec:** §4.1, §4.6 (CLI).

## Task 3: `PeerAuth/PeerRequestSigner` + `PeerRequestVerifier`

**Files:**
- Create: `src/OwnS3.Protocol/PeerAuth/PeerRequestSigner.cs`
- Create: `src/OwnS3.Protocol/PeerAuth/PeerRequestVerifier.cs`
- Test: `src/tests/OwnS3.UnitTests/PeerAuth/PeerRequestSignerTests.cs`

**Interfaces:**
- Consumes: `CanonicalRequestBuilder.Build`, `S3RequestModel`, `S3HeaderCollection` (существуют), `FixedTimeProvider` (юниты).
- Produces (Tasks 7, 8 — исходящая подпись; Task 9 — верификация грани):
  `static (string Authorization, string PeerDate, string ContentSha256) PeerRequestSigner.Sign(ECDsa privateKey, string peerName, string method, string path, string canonicalQuery, IReadOnlyList<(string Name, string Value)> headers, string payloadString, DateTimeOffset at)` — по образцу `SigV4Signer.Sign`;
  `PeerVerifyResult` (`Ok(string PeerName)` / `Fail(S3ErrorCode Code, string? Detail = null)`) и `PeerRequestVerifier(TimeProvider).Verify(S3RequestModel model, ECDsa publicKey)`;
  статический хелпер `static string? PeerRequestVerifier.CredentialOf(S3RequestModel model)` — имя пира из `Credential=` Authorization ИЛИ `null` при malformed (используется гранью ДО резолва записи, Task 9; Protocol — без Result-сигнатур, маппинг `null → 400 AuthorizationHeaderMalformed` делает middleware).
  Константа `PeerRequestSigner.Algorithm = "OWNS3-PEER-ECC-P256-SHA256"`.

- [ ] **Step 3.1: Failing-тесты** (roundtrip по образцу `SigV4SignerTests.OutgoingSignature_PassesOwnVerifier`):

```csharp
public class PeerRequestSignerTests
{
    // Фикстура: пара ECDsa P-256 + FixedTimeProvider(At) — как SigV4SignerFixture.

    [Fact]
    public void Sign_VerifyRoundtrip_Ok() { /* POST /replication/notifications с телом → Verify → Ok(peerName) */ }

    [Fact]
    public void Sign_GetWithQuery_Roundtrip() { /* GET /replication/listing?bucket=b&marker=m → Ok */ }

    [Fact]
    public void Verify_TamperedPayload_SignatureDoesNotMatch() { /* подписали hash-A, в model hash-B → Fail(S3ErrorCode.SignatureDoesNotMatch) */ }

    [Fact]
    public void Verify_TamperedPath_SignatureDoesNotMatch() { /* подпись пути A, проверка пути B → Fail(SignatureDoesNotMatch) */ }

    [Fact]
    public void Verify_Skew_TooSkewed() { /* подпись at=At, верификатор at=At+16мин → Fail(RequestTimeTooSkewed) */ }

    [Fact]
    public void Verify_Skew_Within15Min_Ok() { /* +14 мин → Ok */ }

    [Theory]
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Credential=x Signature=y")]            // нет SignedHeaders
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Credential=x, SignedHeaders=host, Signature=y")] // неполный список
    [InlineData("OWNS3-PEER-FOO Credential=x")]                                     // чужой алгоритм семейства
    [InlineData("мусор")]
    public void Verify_MalformedAuthorization_AuthorizationHeaderMalformed(string header) { /* 400-код */ }

    [Fact]
    public void Verify_WrongKey_SignatureDoesNotMatch() { /* подпись ключом A, проверка ключом B → 403-код */ }

    [Fact]
    public void Signature_IsP1363Base64_64Bytes() { /* Signature из Authorization → 64 байта */ }

    [Fact]
    public void Authorization_Format_FixedSignedHeaders() { /* "Credential=<name>, SignedHeaders=host;x-owns3-peer-date;x-owns3-peer-content-sha256, Signature=…" — ровно этот список */ }

    [Theory]
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Credential=peer-a, SignedHeaders=host, Signature=AA==")]  // валидная структура — имя
    [InlineData("OWNS3-PEER-ECC-P256-SHA256 Signature=AA==")]   // нет Credential — null
    [InlineData("мусор")]                                       // нет Credential — null
    public void CredentialOf_ParsesName_OrNull(string header) { /* Ok-случай → "peer-a"; прочие → null */ }
}
```

- [ ] **Step 3.2: Прогон — падают** (фильтр `FullyQualifiedName~PeerRequestSignerTests`).

- [ ] **Step 3.3: Реализация Signer** (строковый контракт, без ASP.NET — по образцу `SigV4Signer`):

```csharp
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Requests;

namespace OwnS3.Protocol.PeerAuth;

// Исходящая прикладная подпись replication-грани для key-записей (глава 07 §6):
// канонизация — CanonicalRequestBuilder (переиспользование SigV4, не дублирование),
// string-to-sign "OWNS3-PEER-ECC-P256-SHA256\n<date>\n<hex-sha256(canonical)>",
// подпись ECDSA P-256 P1363 base64.
public static class PeerRequestSigner
{
    public const string Algorithm = "OWNS3-PEER-ECC-P256-SHA256";

    // Фиксированный список подписанных заголовков схемы (глава 07 §6).
    public static readonly string[] SignedHeaderNames =
        ["host", "x-owns3-peer-date", "x-owns3-peer-content-sha256"];

    public static (string Authorization, string PeerDate, string ContentSha256) Sign(
        ECDsa privateKey, string peerName, string method, string path, string canonicalQuery,
        IReadOnlyList<(string Name, string Value)> headers, string payloadString, DateTimeOffset at)
    {
        var peerDate = at.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var host = headers.FirstOrDefault(h => string.Equals(h.Name, "host", StringComparison.OrdinalIgnoreCase))
            .Value ?? string.Empty;
        var model = new S3RequestModel
        {
            Method = method.ToUpperInvariant(), RawPath = path, RawQuery = canonicalQuery,
            Headers = S3HeaderCollection.FromPairs(headers), Host = host, OpenBody = () => Stream.Null,
        };
        var canonical = CanonicalRequestBuilder.Build(model, SignedHeaderNames, payloadString);
        var stringToSign = $"{Algorithm}\n{peerDate}\n{SigV4Core.HexSha256(Encoding.UTF8.GetBytes(canonical))}";
        var signature = Convert.ToBase64String(privateKey.SignData(Encoding.UTF8.GetBytes(stringToSign),
            HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation));
        var authorization = $"{Algorithm} Credential={peerName}, " +
            $"SignedHeaders={string.Join(\";\", SignedHeaderNames)}, Signature={signature}";
        return (authorization, peerDate, payloadString);
    }
}
```

**Verifier** — по зеркалу `SigV4HeaderVerifier`: парсинг `Authorization` (split по запятым с trim: `Credential=`, `SignedHeaders=`, `Signature=`; алгоритм ≠ `Algorithm` или пропуск поля / список ≠ `SignedHeaderNames` → `Fail(AuthorizationHeaderMalformed)`), парсинг `x-owns3-peer-date` (невалидный → `Fail(AuthorizationHeaderMalformed)`), skew ±15 мин (`Fail(RequestTimeTooSkewed)`), сборка canonical `CanonicalRequestBuilder.Build(model, SignedHeaderNames, model.Headers["x-owns3-peer-content-sha256"])`, string-to-sign, `publicKey.VerifyData(..., P1363)` → `Ok(peerName)` / `Fail(SignatureDoesNotMatch)`. Плюс `static string? CredentialOf(S3RequestModel model)` — только разбор `Credential=` (та же логика парсинга, `null` при любом malformed).

- [ ] **Step 3.4: Прогон — зелёные; полный юнит-фильтр фазы; коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug --filter "FullyQualifiedName~PeerAuth"
git add src/OwnS3.Protocol/PeerAuth/ src/tests/OwnS3.UnitTests/PeerAuth/PeerRequestSignerTests.cs
git commit -m "t39: схема подписи запроса OWNS3-PEER-ECC-P256-SHA256 — signer/verifier поверх канонизации SigV4"
```

**Вход:** Tasks 1–2. **Выход:** схема §4.2 целиком. **Spec:** §4.2, критерий 2.

---

## Фаза 2 — репликация `src/OwnS3.Replication` (Tasks 4–8)

## Task 4: `TargetRecord` двух видов + `AddOrReplace`/`Remove`

**Files:**
- Modify: `src/OwnS3.Replication/ReplicationTargets.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/ReplicationTargetsTests.cs` (расширить)

**Interfaces:**
- Produces (Tasks 5, 7, 8, 9, 10, 12):
  `TargetRecord(string Name, string Endpoint, string Auth, string? AccessKey = null, string? SecretKey = null, string? PublicKeyPem = null)` c константами `AuthLegacy = "legacy"`, `AuthKey = "key"` (`Auth` обязателен — значений по умолчанию нет; поэтому ВСЕ места конструирования (env-seed `Load`, `HandlePutTargetsAsync`, юниты) обновляются этим же таском на явную форму `(name, endpoint, TargetRecord.AuthLegacy, ak, sk)`);
  `ReplicationTargets.AddOrReplace(TargetRecord target)` (merge + персист + `TargetsChanged`), `bool Remove(string name)` (true — был; персист + `TargetsChanged`; курсоры не трогает), `bool TryGet(string name, out TargetRecord target)`.
  Граница: env-seed (`TargetOptions`) остаётся legacy-only (4 поля — глава 07 §2) и НЕ расширяется: key-записи попадают в состав только через PUT admin-грани или join (аналогично Task 14).
- Consumes: PEM-валидация — `ECDsa.ImportFromPem` (`ImportSubjectPublicKeyInfoPem`).

- [ ] **Step 4.1: Failing-тесты** — добавить в `ReplicationTargetsTests`:

```csharp
[Fact]
public void Load_OldFormatFile_ReadsAsLegacy() { /* targets.json без auth (4 поля) → Snapshot[0].Auth == "legacy", креды на месте */ }

[Fact]
public void Replace_KeyRecord_PersistsAuthAndPem() { /* Replace с auth=key + валидный PEM → файл перечитан Load'ом: поля на месте */ }

[Fact]
public void Replace_KeyRecord_InvalidPem_Throws() { /* publicKeyPem = "not-a-pem" → ArgumentException */ }

[Fact]
public void Replace_KeyRecord_MissingPublicKey_Throws() { /* key без PEM → ArgumentException */ }

[Fact]
public void Replace_LegacyRecord_MissingCreds_Throws() { /* legacy без пары кредов → ArgumentException */ }

[Fact]
public void Replace_MixedList_AccessKeyUniqueAmongLegacyOnly() { /* две key-записи с одинаковым (пустым) accessKey — ок; дубль среди legacy → throw */ }

[Fact]
public void AddOrReplace_NewTarget_PersistsAndRaisesChanged() { /* AddOrReplace → Snapshot содержит, TargetsChanged вызван, файл перечитан */ }

[Fact]
public void AddOrReplace_ExistingName_ReplacesEndpointAndKey() { /* вторая запись тем же именем — заменена (не дубль) */ }

[Fact]
public void Remove_Existing_TrueAndGone() { /* Remove("b") → true, Snapshot без b, файл без b; каталог cursors/ не тронут (курсор соседа жив — ассерт отдельным файлом) */ }

[Fact]
public void Remove_Unknown_False() { /* false, персист не менялся */ }
```

PEM для тестов — генерация `ECDsa.Create(nistP256).ExportSubjectPublicKeyInfoPem()` в Arrange.

- [ ] **Step 4.2: Прогон — падают** (новые кейсы FAIL).

- [ ] **Step 4.3: Реализация** в `ReplicationTargets.cs`:
  - `TargetRecord` — новая сигнатура (см. Interfaces); обновить все места конструирования в этом файле (`Load`: seed → `TargetRecord.AuthLegacy` c кредами).
  - Сериализация через приватный JSON-DTO (обратная совместимость):

```csharp
// Дисковый формат targets.json (глава 07 §2): camelCase; записи без auth
// (старый формат) читаются как legacy.
private sealed record TargetRecordDto(string Name, string Endpoint, string? Auth,
    string? AccessKey, string? SecretKey, string? PublicKeyPem);

private static TargetRecord FromDto(TargetRecordDto dto) =>
    new(dto.Name, dto.Endpoint, dto.Auth ?? TargetRecord.AuthLegacy,
        dto.AccessKey, dto.SecretKey, dto.PublicKeyPem);
```

  `LoadFromFile`/`Persist` — через DTO; `Validate` расширяется: `legacy` → оба креда непусты; `key` → `PublicKeyPem` непуст и `ECDsa.Create().ImportSubjectPublicKeyInfoPem(...)` валиден (try/catch → `ArgumentException` с пояснением); дедуп `accessKey` — только среди legacy-записей (ключи дедупа — `AccessKey` не-null и не-пустые); имена — как сейчас; endpoint — как сейчас.
  - `AddOrReplace`/`Remove`/`TryGet` (внутри `Replace`-механики: персист tmp+rename, атомарная замена снапшота, `TargetsChanged?.Invoke`; `Remove` возвращает false без IO, если имени нет).
  - Поправить потребители `TargetRecord(...)` по дереву (коммит обязан собираться): `ReplicationAdminHandlers.HandlePutTargetsAsync` — на явную legacy-форму (DTO c `Auth/PublicKeyPem` доводит Task 10), юниты/интеграции, где конструируется запись — grep `new TargetRecord(` / `new OwnS3.Replication.TargetRecord(`.

- [ ] **Step 4.4: Прогон юнитов репликации — зелёный; регресс всего юнит-проекта**:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug
```

Ожидание: PASS (включая прежние кейсы `ReplicationTargetsTests` — обратная совместимость).

- [ ] **Step 4.5: Коммит**

```bash
git add -A src/OwnS3.Replication/ReplicationTargets.cs src/tests/OwnS3.UnitTests/
git commit -m "t39: TargetRecord auth legacy/key — обратная совместимость формата, валидация PEM, AddOrReplace/Remove"
```

**Вход:** Task 0 (канон §2). **Выход:** состав двух видов. **Spec:** §4.3, принцип 4/5.

## Task 5: `DenyList`

**Files:**
- Create: `src/OwnS3.Replication/DenyList.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/DenyListTests.cs`

**Interfaces:**
- Produces (Tasks 7, 9, 10): `record DenyEntry(string Name, string PublicKeyPem, long RevokedAtUnixMs)`; `DenyList(string filePath, TimeProvider)` c `bool IsDenied(string name, string publicKeyPem)`, `void Add(string name, string publicKeyPem)` (идемпотентен: обновляет `RevokedAtUnixMs`; персист tmp+rename; каталог создаётся при первой записи), `bool Remove(string name)`, `IReadOnlyList<DenyEntry> Snapshot` (конструктор: файла нет → пуст, битый JSON → `InvalidOperationException` — fail-fast, файл повреждён вручную).

- [ ] **Step 5.1: Failing-тесты** (файл `DenyListTests.cs`, fixture temp-каталог по образцу `ReplicationTargetsFixture`):

```csharp
[Fact]
public void Add_Persists_ReloadKeeps() { /* Add → new DenyList(тот же путь) видит запись */ }
[Fact]
public void IsDenied_ByName_True() { /* Add("n1", keyA) → IsDenied("n1", keyB) == true — имя */}
[Fact]
public void IsDenied_ByPublicKey_True() { /* Add("n1", keyA) → IsDenied("другое", keyA) == true — ключ */}
[Fact]
public void IsDenied_Neither_False() { /* IsDenied("x", keyX) == false */}
[Fact]
public void Remove_UnknownName_False() { /* — */}
[Fact]
public void Remove_Existing_TrueNotDenied() { /* Add → Remove("n1") → IsDenied("n1", keyA) == false; персист: перечитанный тоже чист */}
[Fact]
public void Add_IdempotentSameKey_UpdatesTime() { /* повторный Add — одна запись, обновлённый revokedAt */}
[Fact]
public void Constructor_MalformedFile_Throws() { /* мусор в файле → InvalidOperationException */}
```

- [ ] **Step 5.2: Прогон — падают.** **Step 5.3: Реализация** (JSON camelCase, tmp+rename по образцу `ReplicationTargets.Persist`; `TimeProvider` — `RevokedAtUnixMs`).

- [ ] **Step 5.4: Прогон — зелёные; коммит**

```bash
git add src/OwnS3.Replication/DenyList.cs src/tests/OwnS3.UnitTests/Replication/DenyListTests.cs
git commit -m "t39: локальный deny-list репликации — персист tmp+rename, решения по имени/ключу"
```

**Вход:** Task 4. **Выход:** механизм отзыва §4.5. **Spec:** §4.5.

## Task 6: `PeerIdentityLoader` + опции

**Files:**
- Modify: `src/OwnS3.Replication/ReplicationOptions.cs`
- Create: `src/OwnS3.Replication/PeerIdentityLoader.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/PeerIdentityLoaderTests.cs`

**Interfaces:**
- Consumes: `PeerCertificate`, `PeerTrustAnchor`, `PeerCertificateAuthority` (Tasks 1–2).
- Produces (Tasks 7, 8, 9, 11, 12, 14):
  `ReplicationOptions` += `PeerIdentityOptions? Identity { get; set; }`, `PeerTrustCaOptions? TrustCa { get; set; }`, `string? AdvertisedEndpoint { get; set; }`;
  `PeerIdentityOptions { string? Certificate; string? CertificatePath; string? PrivateKeyPem; string? PrivateKeyPath; }`, `PeerTrustCaOptions { string? Certificate; string? CertificatePath; }`;
  `record PeerIdentity(string Name, PeerCertificate Certificate, ECDsa Key, string CertificateJson)`;
  `static PeerIdentity PeerIdentityLoader.Load(PeerIdentityOptions o)` (исключения — fail-fast), `static PeerTrustAnchor? PeerIdentityLoader.LoadTrustAnchor(PeerTrustCaOptions? o)` (null — не задан).

- [ ] **Step 6.1: Failing-тесты** (материал — `PeerCertificateAuthority.CreateCa`/`IssueServer` в Arrange):

```csharp
[Fact]
public void Load_ValueAndPath_BothWork() { /* Certificate=JSON-строка + PrivateKeyPem → identity; то же через файлы (_PATH) */}
[Fact]
public void Load_PartialPair_Throws() { /* только сертификат / только ключ → InvalidOperationException */}
[Fact]
public void Load_KeyMismatchesCertificate_Throws() { /* ключ другой пары → InvalidOperationException */}
[Fact]
public void Load_ExpiredCertificate_Throws() { /* expiresDays=1, at=вчера → Load сегодня бросает */}
[Fact]
public void Load_MalformedJson_Throws() { /* мусор в Certificate → исключение */}
[Fact]
public void Load_NeitherGiven_Throws() { /* пустой Identity-объект → исключение (секция задана — материала нет); секция НЕ задана (null) — обрабатывает Program: identity отсутствует, это НЕ ошибка */}
[Fact]
public void LoadTrustAnchor_NotSet_Null() { /* null / пустые поля → null */}
[Fact]
public void LoadTrustAnchor_Value_Parses() { /* JSON → PeerTrustAnchor */}
[Fact]
public void Load_NameFromCertificate() { /* identity.Name == cert.Name; CertificateJson — исходный JSON */}
```

- [ ] **Step 6.2: Прогон — падают.** **Step 6.3: Реализация**: `Load` — значение приоритетнее пути (прецедент `OwnS3Tls.Resolve`): неполная пара → `InvalidOperationException` с перечислением env-имён; `PeerCertificate.FromJson` → `IsExpired(TimeProvider.System.GetUtcNow())` → throw; `ECDsa.ImportFromPem(privatePem)`; сверка ключа: `key.ExportSubjectPublicKeyInfoPem() == cert.PublicKeyPem` (нормализация концов строк — сравнение после `Replace("\r\n", "\n")`); `LoadTrustAnchor` — null при пустых обоих полях, иначе `PeerTrustAnchor.FromJson` (значение приоритетнее пути; задано одно из пары value/path — берётся заданное).

- [ ] **Step 6.4: Прогон — зелёные; коммит**

```bash
git add src/OwnS3.Replication/PeerIdentityLoader.cs src/OwnS3.Replication/ReplicationOptions.cs src/tests/OwnS3.UnitTests/Replication/PeerIdentityLoaderTests.cs
git commit -m "t39: загрузка identity/trust-anchor из env-конфига — значение/путь, fail-fast валидация"
```

**Вход:** Tasks 1–2. **Выход:** env-поставка §4.6. **Spec:** §4.6.

## Task 7: `JoinPeerService` (обе стороны handshake)

**Files:**
- Create: `src/OwnS3.Replication/JoinPeerService.cs`
- Modify: `src/OwnS3.Replication/Transport/ReplicationErrors.cs` (новые доменные ошибки)
- Test: `src/tests/OwnS3.UnitTests/Replication/JoinPeerServiceTests.cs`

**Interfaces:**
- Consumes: `PeerIdentity`, `PeerTrustAnchor`, `PeerCertificateAuthority.Verify`, `PeerRequestSigner`/`PeerRequestVerifier`, `ReplicationTargets.AddOrReplace/TryGet`, `DenyList`, `TargetRecord` (Tasks 2–6).
- Produces (Tasks 9, 10, 11):

```csharp
// ReplicationErrors.cs — дополнить (конвенция 01 §4: кладутся в Result.Error):
public sealed class JoinBadConfigurationException(string message) : Exception(message);   // → 400
// → 403; ErrorCode — исходный S3ErrorCode каталога подписи (SignatureDoesNotMatch/
// RequestTimeTooSkewed/…), когда отказ вызван именно проверкой подписи запроса;
// грань (Task 9) маппит errorCode ?? S3ErrorCode.AccessDenied
public sealed class JoinRefusedException(string message, S3ErrorCode? errorCode = null)
    : Exception(message)
{
    public S3ErrorCode? ErrorCode { get; } = errorCode;
}
public sealed class JoinConflictException(string message) : Exception(message);            // → 409
public sealed class JoinBadGatewayException(string message) : Exception(message);          // → 502

public sealed record PeerJoined(string Name, string Endpoint, string PublicKeyPem);        // исход JoinAsync
public sealed record JoinAccept(string Name, string AdvertisedEndpoint,
    string CertificateBase64, string SignatureBase64);                                     // исход HandleIncoming
```

Ctor (8 параметров): `JoinPeerService(PeerIdentity? identity, PeerTrustAnchor? trustAnchor, ReplicationTargets targets, DenyList denyList, HttpClient http, TimeProvider timeProvider, ILogger<JoinPeerService> logger, string? advertisedEndpoint)` — `advertisedEndpoint` передаёт Program из `ReplicationOptions.AdvertisedEndpoint` (Task 11).
Методы:
- `Task<Result<PeerJoined>> JoinAsync(string endpoint, CancellationToken ct)` — исходящая сторона: identity/trust/advertised — нет любого → `Failed(JoinBadConfigurationException)` (spec §4.4 п.1: 400 с пояснением — без trust-anchor инициатор не сможет проверить сертификат ответа соседа); POST `{endpoint}/replication/join` телом `{"advertisedEndpoint":...}`, заголовки peer-подписи (`PeerRequestSigner.Sign` с payloadHash = SHA-256 тела) + `X-OwnS3-Peer-Certificate` = base64(identity.CertificateJson); ответ 200 → парс `{name, advertisedEndpoint, certificate}` + `X-OwnS3-Peer-Signature` → `PeerCertificateAuthority.Verify` против СВОЕГО trust + `VerifyData` подписи тела ключом серта соседа → валидно → `targets.AddOrReplace(new TargetRecord(name, advertNeighbor, TargetRecord.AuthKey, PublicKeyPem: cert.PublicKeyPem))` → `Success(new PeerJoined(...))`; ответ 403/409 → `Failed(JoinRefusedException/JoinConflictException)`; прочие не-200/битый JSON/невалидная подпись/транспорт (`HttpRequestException`/`TaskCanceledException` не по внешнему ct) → `Failed(JoinBadGatewayException)`; внешняя отмена ct — проброс.
- `Result<JoinAccept> HandleIncoming(S3RequestModel model, string certificateBase64, string advertisedEndpoint)` — входящая сторона: identity null (нечем подписать ответ) или trust null → `Failed(JoinRefusedException)`; base64 → UTF8 JSON → `PeerCertificate.FromJson` (битый → `Failed(JoinRefusedException)`, лог); `PeerCertificateAuthority.Verify(cert, trust, now)` false → `Failed(JoinRefusedException)`; `denyList.IsDenied(cert.Name, cert.PublicKeyPem)` → `Failed(JoinRefusedException)`; `PeerRequestVerifier(timeProvider).Verify(model, ключ серта)` → `Fail(code)` → `Failed(new JoinRefusedException(пояснение, code))` — грань отдаст `ErrorCode ?? AccessDenied`; конфликт: `targets.TryGet(cert.Name, out var existing)` ∧ `existing.Auth == AuthKey` ∧ `existing.PublicKeyPem != cert.PublicKeyPem` → `Failed(JoinConflictException)`; успех/идемпотентность → `AddOrReplace(TargetRecord(cert.Name, advertisedEndpoint, AuthKey, PublicKeyPem: cert.PublicKeyPem))` → тело ответа `{"name": identity.Name, "advertisedEndpoint": advertised, "certificate": base64(identity.CertificateJson)}` (сериализация stable, camelCase) → подпись `SignData(utf8(тело), SHA256, P1363)` → `Success(new JoinAccept(...))`. Лог-события join (обе стороны — LogInformation, канон §10).

- [ ] **Step 7.1: Failing-тесты** `JoinPeerServiceTests` — исходящая сторона на фейк-`HttpMessageHandler` (запрограммированные ответы; прецедент тестируемости «без хоста» — `ReplicationProber`), входящая — прямые вызовы:

```csharp
// Фейк: записи запросов (заголовки/тело) + запрограммированный ответ.
private sealed class FakeHandler : HttpMessageHandler { public HttpRequestMessage? Last; public HttpResponseMessage Reply; ... }

[Fact] public void JoinAsync_Success_AddsTargetAndReturnsPeer() { /* фейк отвечает валидным 200 (серт против своего CA + подпись) → targets.AddOrReplace вызван (запись auth=key, publicKey из серта), Result Success */
[Fact] public void JoinAsync_NoIdentity_400config() { /* identity null → Failed(JoinBadConfigurationException) — HTTP не зовётся */
[Fact] public void JoinAsync_NoAdvertised_400config() { /* — */}
[Fact] public void JoinAsync_NoTrust_400config() { /* trust null → Failed(JoinBadConfigurationException) — spec §4.4 п.1 */}
[Fact] public void JoinAsync_Peer403_Proxied() { /* фейк 403 → Failed(JoinRefusedException) */}
[Fact] public void JoinAsync_Peer409_Proxied() { /* фейк 409 → Failed(JoinConflictException) */}
[Fact] public void JoinAsync_InvalidSignature_502() { /* 200 с телом, подписанным ДРУГИМ ключом → Failed(JoinBadGatewayException); targets НЕ изменён */}
[Fact] public void JoinAsync_CertAgainstOtherCa_502() { /* серт соседа от чужого CA → Failed(JoinBadGatewayException) */}
[Fact] public void JoinAsync_Transport_502() { /* фейк кидает HttpRequestException → Failed(JoinBadGatewayException) */}
[Fact] public void JoinAsync_RequestCarriesCertAndSignature() { /* Last: Authorization начинается "OWNS3-PEER-ECC-P256-SHA256 Credential=<наш name>"; X-OwnS3-Peer-Certificate = base64(наш cert JSON); тело содержит advertisedEndpoint */}

[Fact] public void Incoming_ValidCert_AddsKeyRecord() { /* model, подписанная ключом серта пира; HandleIncoming → Success; targets: запись {name серта, endpoint: advert, auth=key} */}
[Fact] public void Incoming_NoTrust_403() { /* trust null → Failed(JoinRefusedException) */}
[Fact] public void Incoming_NoIdentity_403() { /* identity null → Failed(JoinRefusedException) — нечем подписать ответ */}
[Fact] public void Incoming_OtherCa_403() { /* серт от чужого CA → Failed(JoinRefusedException) */}
[Fact] public void Incoming_Expired_403() { /* истёкший серт → 403-семантика */}
[Fact] public void Incoming_DeniedByName_403() { /* denyList.Add(cert.Name, другойКлюч) → 403 */}
[Fact] public void Incoming_DeniedByKey_403() { /* denyList.Add(другоеИмя, cert.PublicKeyPem) → 403 */}
[Fact] public void Incoming_RequestSignatureMismatch_403signature() { /* подпись запроса чужим ключом → Failed(JoinRefusedException) c ErrorCode == S3ErrorCode.SignatureDoesNotMatch */}
[Fact] public void Incoming_SameNameSameKey_IdempotentEndpointUpdate() { /* существующая запись с тем же ключом → Success; endpoint обновлён; записей 1 */}
[Fact] public void Incoming_SameNameOtherKey_409() { /* Failed(JoinConflictException) */}
[Fact] public void Incoming_ResponseSigned() { /* JoinAccept.SignatureBase64 VerifyData(тело name+advertised+certificate) ключом НАШЕГО identity */}
[Fact] public void Incoming_DoesNotTouchCursors() { /* cursors-файлов нет после HandleIncoming (join курсоры не меняет — §4.4) — ассерт по каталогу */
```

- [ ] **Step 7.2: Прогон — падают.** **Step 7.3: Реализация** `JoinPeerService.cs` + расширение `ReplicationErrors.cs` по Interfaces-блоку (комментарии — русские, ссылки на §4.4/глава 07 §2).

- [ ] **Step 7.4: Прогон — зелёные; коммит**

```bash
git add src/OwnS3.Replication/JoinPeerService.cs src/OwnS3.Replication/Transport/ReplicationErrors.cs src/tests/OwnS3.UnitTests/Replication/JoinPeerServiceTests.cs
git commit -m "t39: JoinPeerService — двусторонний join-handshake на Result (фейк-транспорт), CA/deny/конфликт/идемпотентность"
```

**Вход:** Tasks 2–6. **Выход:** механика §4.4. **Spec:** §4.4, критерий 3.

## Task 8: peer-подпись в `ReplicationHttpClient`

**Files:**
- Modify: `src/OwnS3.Replication/Transport/ReplicationHttpClient.cs`
- Test: `src/tests/OwnS3.UnitTests/Replication/ReplicationHttpClientTests.cs` (новый)

**Interfaces:**
- Consumes: `PeerRequestSigner`, `PeerIdentity`, `TargetRecord.Auth` (Tasks 3, 4, 6).
- Produces: `ReplicationHttpClient(HttpClient http, TimeProvider? timeProvider = null, PeerIdentity? identity = null)` — третий параметр опционален (существующие вызовы `ReplicationTransportFactory` и юнитов не меняются); `IReplicationTransport` — без изменений.

- [ ] **Step 8.1: Failing-тесты** на фейк-`HttpMessageHandler` (ловит запрос, отвечает 200):

```csharp
[Fact] public void Ping_KeyTargetWithIdentity_SignedAsPeer() { /* target auth=key → Last.Authorization начинается OWNS3-PEER… Credential=<identity.Name>; заголовки x-owns3-peer-date/x-owns3-peer-content-sha256 присутствуют; x-amz-date НЕТ */}
[Fact] public void Ping_LegacyTarget_SignedAsSigV4() { /* прежний формат AWS4-HMAC-SHA256 — регресс */}
[Fact] public void Ping_KeyTargetWithoutIdentity_FailedPeerRejected() { /* identity null, auth=key → Result.Failed(PeerRejectedException), HTTP не зовётся (фейк-хендлер: вызовов 0) */}
[Fact] public void Notifications_KeyTargetWithoutIdentity_FailedPeerRejected() { /* identity null, auth=key, SendNotificationsAsync → Result.Failed(PeerRejectedException), HTTP не зовётся — пре-чек ДО BuildRequest (Step 8.3): throw из BuildRequest вылетел бы сквозь метод (BuildRequest вызывается до try) и уронил цикл нотификатора вместо Failed → бэкофф */}
[Fact] public void Notifications_KeyTarget_BodyHashInHeader() { /* payload → x-owns3-peer-content-sha256 == SHA-256(тела) */}
[Fact] public void KeySignature_PassesOwnVerifier() { /* Authorization+заголовки из пойманного запроса → PeerRequestVerifier.Verify → Ok — roundtrip грани */}
```

- [ ] **Step 8.2: Прогон — падают.** **Step 8.3: Реализация** — два среза:

1. **Пре-чек «key без identity» в начале КАЖДОГО метода интерфейса** (`SendNotificationsAsync`, `FetchObjectAsync`, `ListAsync`, `ListingBucketsAsync`, `PingAsync`) — прямой `Result.Failed`, БЕЗ HTTP и до `BuildRequest`:

```csharp
// Ключ-сосед без identity: исходящая peer-подпись невозможна — терминальный
// отказ транспорта (бэкофф вызывающего), НЕ исключение сквозь границу.
// Пре-чек обязателен ДО BuildRequest: в SendNotificationsAsync/FetchObjectAsync
// BuildRequest вызывается ВНЕ try — throw вылетел бы из метода и уронил цикл
// нотификатора (NotificationDispatcher.RunLoopAsync зовёт transport без catch),
// нарушение Result-контракта (глава 07 §11, конвенция 01 §4).
if (target.Auth == TargetRecord.AuthKey && identity is null)
    return Result<...>.Failed(new PeerRejectedException(
        $"нет identity для key-записи {target.Name} (env OWNS3_REPLICATION_CERT/_PRIVATE_KEY)"));
```

2. **Ветвление подписи в `BuildRequest`**:

```csharp
if (target.Auth == TargetRecord.AuthKey)
{
    // identity != null — гарантировано пре-чеками всех публичных методов;
    // throw — страховка будущих точек вызова
    // ... PeerRequestSigner.Sign(identity.Key, identity.Name, method.Method, path, query,
    //     headersPeer, payloadHashValue, at); заголовки host/x-owns3-peer-date/x-owns3-peer-content-sha256
}
else { /* существующий SigV4Signer-путь байт-в-байт */ }
```

`IReplicationTransport` не тронут; существующие catch каждого метода сохраняются.

- [ ] **Step 8.4: Прогон юнитов проекта целиком — зелёный; коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug
git add src/OwnS3.Replication/Transport/ReplicationHttpClient.cs src/tests/OwnS3.UnitTests/Replication/ReplicationHttpClientTests.cs
git commit -m "t39: ReplicationHttpClient — выбор схемы подписи по TargetRecord.Auth (peer для key-записей)"
```

**Вход:** Tasks 3, 4, 6. **Выход:** исходящая доставка на peer-подписи. **Spec:** §4.7 (транспорт), критерий 6.

---

## Фаза 3 — грани и конфиг `src/OwnS3.App` (Tasks 9–12)

## Task 9: replication-грань — peer-конвейер + входящий join

**Files:**
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (ветка `/replication/*`)
- Modify: `src/OwnS3.App/Handlers/ReplicationHandlers.cs` (`HandleJoinAsync`, `TryResolvePeer`)
- Test: покрывается интеграциями Task 12 (грань = хост-код); юнит-покрытие резолва — `src/tests/OwnS3.UnitTests/App/...` не заводим (резолв тривиален, покрыт WAF-сценариями).

**Interfaces:**
- Consumes: `PeerRequestVerifier` (+ `CredentialOf` — `string?`, Task 3), `PeerVerifyResult`, `JoinPeerService.HandleIncoming`, `TargetRecord.AuthKey` (Tasks 3, 4, 7).
- Produces (Task 12 тестирует): peer-конвейер грани и маршрут `POST /replication/join`.
- `ReplicationHandlers` ctor += `JoinPeerService joinService`; новый метод `Task HandleJoinAsync(HttpContext context, string requestId, string? errorHostId)`; резолв: `bool TryResolvePeer(string name, out TargetRecord target)` (замена/дополнение `TryResolveSource` — legacy-резолв по accessKey остаётся для SigV4-ветки).

- [ ] **Step 9.1: S3Middleware — дискриминация схемы в `/replication`-ветке.** В начале ветки (до `S3Authenticator`) — выбор конвейера по заголовку:

```csharp
if (context.Request.Path.StartsWithSegments("/replication"))
{
    operationLabel = "replication";
    var replModel = S3ModelFactory.Create(context);
    var authorizationHeader = context.Request.Headers.Authorization.ToString();
    if (authorizationHeader.StartsWith("OWNS3-PEER-", StringComparison.Ordinal))
    {
        // Peer-конвейер (глава 07 §6): join-под-ветка или key-резолв по имени
        if (HttpMethods.IsPost(method) && context.Request.Path.Value == "/replication/join")
        {
            await replicationHandlers.HandleJoinAsync(context, requestId, options.Value.HostId);
            return;
        }
        // Порядок: CredentialOf (string?, Task 3) → null → 400 AuthorizationHeaderMalformed (маппинг
        // здесь — Protocol без Result-сигнатур); имя есть → TryResolvePeer → нет записи / не key →
        // 403 AccessDenied; запись есть → ECDsa.ImportFromPem(PublicKeyPem) → PeerRequestVerifier.Verify →
        // Fail(code) → WriteErrorAsync (SignatureDoesNotMatch/RequestTimeTooSkewed/…) →
        // Ok(sourceName) → та же диспетчеризация notifications/object/listing/ping
        // (existing-блок ветки reused: вынести диспетчеризацию в локальную функцию)
        return;
    }
    // ... существующий SigV4-конвейер байт-в-байт (без изменений)
}
```

- [ ] **Step 9.2: `ReplicationHandlers.HandleJoinAsync`** — входящий join: читает `X-OwnS3-Peer-Certificate` (отсутствует → 400 `InvalidArgument` JSON-ошибкой по образцу existing-хендлеров), тело `{"advertisedEndpoint"}` (malformed → 400), `joinService.HandleIncoming(replModel, certBase64, advertised)`:
  - `Success(accept)` → 200 JSON `{"name", "advertisedEndpoint", "certificate"}` + заголовок `X-OwnS3-Peer-Signature: accept.SignatureBase64` (тело ответа — та же строка, что подписана: собрать JSON один раз, подписать её);
  - `Failed` → маппинг по типу Error: `JoinRefusedException` → 403 c кодом `ex.ErrorCode ?? S3ErrorCode.AccessDenied` (отказ проверки подписи зеркалит каталог §6: SignatureDoesNotMatch/RequestTimeTooSkewed/…; прочие причины отказа — AccessDenied), тело — JSON `{"error": пояснение}` либо XML каталога при ErrorCode; `JoinConflictException` → 409 JSON `{"error": ...}` (join-грань отвечает JSON — стиль admin/replication-JSON-контрактов).
  - `S3RequestModel` для `HandleIncoming` — `replModel` middleware.

- [ ] **Step 9.3: Сборка + регресс юнитов**:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/OwnS3.App -c Debug && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug
```

Ожидание: build чист (`TreatWarningsAsErrors`), юниты зелёные.

- [ ] **Step 9.4: Коммит**

```bash
git add src/OwnS3.App/Pipeline/S3Middleware.cs src/OwnS3.App/Handlers/ReplicationHandlers.cs
git commit -m "t39: replication-грань — peer-конвейер OWNS3-PEER + маршрут входящего join"
```

**Вход:** Tasks 3, 4, 7. **Выход:** грань принимает обе схемы (критерий 2). **Spec:** §4.3, §4.4 (п.3), §4.7 (S3Middleware/ReplicationHandlers).

## Task 10: админ-грань — join-trigger, delete, denylist, статус `auth`

**Files:**
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (ветка `/admin/replication/*` — маршрутизация)
- Modify: `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs`
- Test: интеграции Task 12.

**Interfaces:**
- Consumes: `JoinPeerService.JoinAsync`, `ReplicationTargets.Remove/TryGet`, `DenyList`, `TargetRecord.Auth` (Tasks 4, 5, 7).
- Produces (Task 12 тестирует): `HandleJoinTriggerAsync`, `HandleDeleteTargetAsync(name, revoke)`, `HandleDenyListAsync`, `HandleDenyRemoveAsync(name)`; `ReplicationAdminHandlers` ctor += `DenyList denyList`, `JoinPeerService joinService`; статус — поле `auth` per-target; `PutTargetsRequest.TargetDto` += `Auth/PublicKeyPem` (PUT targets работает для обоих видов).

- [ ] **Step 10.1: S3Middleware admin-маршруты.** Существующий `isAdminRepl` (точные совпадения) заменить на разбор сегментов пути: `/admin/replication/{join|targets|status|reconcile|sync-report|probe|denylist}` + хвостовые сегменты `targets/{name}` (DELETE) и `denylist/{name}` (DELETE); неизвестный ресурс — прежний 400 `InvalidArgument` «Unknown admin resource». Диспетчеризация:

```csharp
else if (HttpMethods.IsPost(method) && replAdminPath == "/admin/replication/join")
    await replicationAdminHandlers.HandleJoinTriggerAsync(context);
else if (HttpMethods.IsDelete(method) && replAdminSegments is ["targets", var name])
    await replicationAdminHandlers.HandleDeleteTargetAsync(context, name,
        context.Request.Query["revoke"] == "true");
else if (HttpMethods.IsGet(method) && replAdminPath == "/admin/replication/denylist")
    await replicationAdminHandlers.HandleDenyListAsync(context);
else if (HttpMethods.IsDelete(method) && replAdminSegments is ["denylist", var name])
    await replicationAdminHandlers.HandleDenyRemoveAsync(context, name);
// PUT targets / GET status / POST reconcile / GET sync-report / POST probe — прежние ветки
```

- [ ] **Step 10.2: Хендлеры** (все — тот же стиль JSON-ответов/ошибок, что существующие):
  - `HandleJoinTriggerAsync`: тело `{"endpoint"}` (malformed → 400); `joinService.JoinAsync(endpoint, context.RequestAborted)` → `Success` → 200 `{"joined": true, "peer": name}`; `Error is JoinBadConfigurationException` → 400 `{"error": msg}` (нет identity/trust-anchor/advertised — spec §4.4 п.1); `JoinRefusedException` → 403; `JoinConflictException` → 409; `JoinBadGatewayException` → 502; внешняя отмена — проброс.
  - `HandleDeleteTargetAsync(name, revoke)`: `targets.TryGet(name)` → нет → 404; `revoke && record.Auth == legacy` → 400 (`{"error": "revoke применим только к key-записям — legacy-креды отлучаются удалением записи"}`); `revoke` → `denyList.Add(name, record.PublicKeyPem!)`; `targets.Remove(name)` → 202; лог-событие удаления/отзыва.
  - `HandleDenyListAsync`: 200 `{"entries": [{name, publicKeyPem, revokedAtUnixMs}]}`.
  - `HandleDenyRemoveAsync(name)`: `denyList.Remove(name)` → 202 (false — тоже 202: снятие несуществующего = идемпотентность; зафиксировано arch «202 — снятие отзыва»).
  - `HandlePutTargetsAsync`: `TargetDto(Name, Endpoint, Auth?, AccessKey?, SecretKey?, PublicKeyPem?)` → `TargetRecord` (`auth ?? legacy`); валидация — в `ReplicationTargets.Validate` (уже Task 4).
  - `HandleStatusAsync`: в `neighbors` добавить `auth = t.Auth`.

- [ ] **Step 10.3: Сборка + коммит**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/OwnS3.App -c Debug
git add src/OwnS3.App/Pipeline/S3Middleware.cs src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs
git commit -m "t39: админ-грань — POST join, DELETE targets/{name}[?revoke], GET/DELETE denylist, статус auth"
```

**Вход:** Tasks 4, 5, 7, 9. **Выход:** управление составом по API (критерии 3–4). **Spec:** §4.4–§4.5, §4.7.

## Task 11: Program.cs — env, DI, fail-fast

**Files:**
- Modify: `src/OwnS3.App/Program.cs`
- Test: юниты Task 6 (механика fail-fast лоадера) + WAF-кейс отказастарта Task 12 (`FailFast_BrokenIdentityMaterial_RefusesStart`).

**Interfaces:**
- Consumes: `PeerIdentityLoader`, `DenyList`, `JoinPeerService`, `ReplicationHttpClient(.., identity)` (Tasks 5–8).
- Produces: DI-граф для граней; env-маппинг §4.6.

- [ ] **Step 11.1: Env-строки** — в существующий массив `OWNS3_REPLICATION_*` добавить:

```csharp
("OWNS3_REPLICATION_CERT", "OwnS3:Replication:Identity:Certificate"),
("OWNS3_REPLICATION_CERT_PATH", "OwnS3:Replication:Identity:CertificatePath"),
("OWNS3_REPLICATION_PRIVATE_KEY", "OwnS3:Replication:Identity:PrivateKeyPem"),
("OWNS3_REPLICATION_PRIVATE_KEY_PATH", "OwnS3:Replication:Identity:PrivateKeyPath"),
("OWNS3_REPLICATION_TRUST_CA", "OwnS3:Replication:TrustCa:Certificate"),
("OWNS3_REPLICATION_TRUST_CA_PATH", "OwnS3:Replication:TrustCa:CertificatePath"),
("OWNS3_REPLICATION_ADVERTISED_ENDPOINT", "OwnS3:Replication:AdvertisedEndpoint"),
```

- [ ] **Step 11.2: DI + fail-fast** (по прецеденту TLS-блока): после `builder.Build()` и до прослушивания — резолв identity/trust (try/catch → `LogCritical` + `Environment.Exit(1)`, сообщение про `OWNS3_REPLICATION_*`); регистрация (до Build, лямбдами с уже вычисленными значениями через замыкание — как `tlsCert`):

```csharp
// PeerAuth-контур (t39, глава 07 §2/§11): identity/trust — материал env каждого
// старта (персиста на томе нет); DenyList — .owns3.sys/replication/denylist.json.
builder.Services.AddSingleton(sp => new OwnS3.Replication.DenyList(
    Path.Combine(ReplicationSysDir(sp), "denylist.json"), sp.GetRequiredService<TimeProvider>()));
builder.Services.AddSingleton(sp => new OwnS3.Replication.JoinPeerService(
    peerIdentity, peerTrust, /* замыкания резолва ниже Build */
    sp.GetRequiredService<OwnS3.Replication.ReplicationTargets>(),
    sp.GetRequiredService<OwnS3.Replication.DenyList>(),
    new HttpClient(), sp.GetRequiredService<TimeProvider>(),
    sp.GetRequiredService<ILogger<OwnS3.Replication.JoinPeerService>>(),
    sp.GetRequiredService<IOptions<OwnS3.Replication.ReplicationOptions>>().Value.AdvertisedEndpoint));
```

`ReplicationTransportFactory` → `new ReplicationHttpClient(new HttpClient(), tp, peerIdentity)`; `ReplicationHandlers`/`ReplicationAdminHandlers` — новые параметры ctor. `peerIdentity`/`peerTrust` — результат fail-fast-блока загрузчика (замыкание, как `tlsCert`; spec §4.7: DI — `DenyList`, `JoinPeerService`; `PeerIdentity` — результат fail-fast-блока загрузчика, без персиста — отдельного IdentityStore-синглтона нет). Секция задана частично/битая → исключение → exit 1; не задана → null (Existing-регресс).

- [ ] **Step 11.3: Сборка App + регресс юнитов + запуск Api-интеграций (существующие сценарии — без правок ожиданий)**:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/OwnS3.App -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Debug
```

Ожидание: PASS (существующие Access/Routing/Replication-сценарии unchanged — принцип 4).

- [ ] **Step 11.4: Коммит**

```bash
git add src/OwnS3.App/Program.cs
git commit -m "t39: Program — env identity/CA/advertised, DI DenyList/JoinPeerService, fail-fast материала"
```

**Вход:** Tasks 5–10. **Выход:** развёртывание env-поставкой (критерий 5). **Spec:** §4.6–§4.7.

## Task 12: Api-интеграции `PeerAuthScenarios`

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs` (фабрики и хелперы — в том же файле, прецедент `ReplicationScenarios.cs`); `OwnS3AppFactory.cs` не трогать.

**Interfaces:**
- Consumes: всё из Tasks 1–11; `OwnS3AppFactory`/`ReplicationAppFactory` (WAF, in-memory конфиг), `FixedTimeProvider`/`HostTime`.
- Produces: контрактное покрытие граней (критерии 2–5 без docker) + WAF-кейс отказастарта на битый identity-материал (для Task 11).

- [ ] **Step 12.1: Фабрика `PeerAuthAppFactory : ReplicationAppFactory`** — статический конструктор генерит материал `PeerCertificateAuthority` (CA + свой серт «own» + серт пира «peer-x» + серт «peer-y» чужим CA для негативных кейсов); `ConfigureWebHost` добавляет in-memory: `OwnS3:Replication:Identity:Certificate/PrivateKeyPem` (свой серт), `TrustCa:Certificate`, `AdvertisedEndpoint` (legacy-записи peer-a/b/c и их креды наследуются от `ReplicationAppFactory` — регресс). **Key-запись peer-x сеется НЕ конфигом** (env-seed `TargetOptions` — legacy-only, Task 4 не расширяет; тот же приём, что Task 14): хелпер сценария `EnsureKeyTargetAsync(factory)` делает `PUT /admin/replication/targets` root-SigV4-клиентом с полным списком (legacy-наследие + key-запись peer-x с publicKey из его серта) — вызывается Arrange'ом кейсов, которым нужен peer-x в составе (Ping/Notifications-кейсы; join-кейсы — по своей логике). Тестовый peer-подписчик — статический хелпер `SignPeerRequest(cert, key, method, path, query, body)` через `PeerRequestSigner` (клиент WAF шлёт заголовки вручную, по образцу raw-SigV4 клиента `ReplicationScenarios`).

- [ ] **Step 12.2: Сценарии** (файл `PeerAuthScenarios.cs`, все AAA):

```csharp
public class PeerAuthScenarios : IClassFixture<PeerAuthAppFactory>
{
    // — replication-грань, peer-схема (после EnsureKeyTargetAsync) —
    [Fact] Ping_KeyNeighborSigned_200();                    // GET /replication/ping с подписью peer-x → 200
    [Fact] Ping_TamperedSignature_403SignatureDoesNotMatch(); // Signature подменена → 403 XML SignatureDoesNotMatch
    [Fact] Ping_OtherKeyForKnownPeer_403SignatureDoesNotMatch(); // подпись ключом peer-y при Credential=peer-x (запись найдена, ключ чужой) → 403 SignatureDoesNotMatch
    [Fact] Ping_UnknownPeerName_403();                      // Credential=ghost (записи нет) → 403 AccessDenied
    [Fact] Ping_Skew_403RequestTimeTooSkewed();             // дата HostTime+16мин → 403 RequestTimeTooSkewed
    [Fact] Ping_MalformedAuthorization_400();               // мусорный OWNS3-PEER-заголовок → 400 AuthorizationHeaderMalformed
    [Fact] Notifications_KeyNeighborSigned_Accepted();      // POST notifications peer-подписью → 200 {acceptedToSeqNo}
    [Fact] ClientEdge_PeerHeader_Still400InvalidRequest();  // PUT /b/k с Authorization OWNS3-PEER-* → 400 InvalidRequest (принцип 3)
    [Fact] SigV4_LegacyPeer_Regression200();                // ping/notifications SigV4 peer-a → 200 (как в ReplicationScenarios)

    // — join (входящий, через CreateClient напрямую POST /replication/join) —
    [Fact] JoinIncoming_ValidCert_AddsKeyTarget();          // подпись peer-x, его серт, валидный CA → 200 {name, advertisedEndpoint, certificate} + X-OwnS3-Peer-Signature; статус содержит peer-x auth=key
    [Fact] JoinIncoming_NoTrustAnchor_403();                // фабрика без TrustCa → 403
    [Fact] JoinIncoming_DeniedPeer_403();                   // Arrange: EnsureKeyTarget → DELETE admin targets/peer-x?revoke=true → 202; join снова → 403
    [Fact] JoinIncoming_ConflictingKey_409();               // Arrange: EnsureKeyTarget с ДРУГИМ publicKey под именем peer-x → join с настоящим сертом peer-x → 409
    [Fact] JoinIncoming_SameKey_IdempotentEndpointUpdate(); // повторный join тем же ключом → 200, endpoint обновлён
    [Fact] JoinIncoming_MissingCertHeader_400();

    // — join-trigger (исходящий; сосед подменяется фейк-HttpMessageHandler через ConfigureTestServices ReplaceAll<JoinPeerService>) —
    [Fact] JoinTrigger_NoIdentity_400();                    // фабрика без Identity → 400 {"error": пояснение} (spec §4.4 п.1)
    [Fact] JoinTrigger_NoAdvertised_400();
    [Fact] JoinTrigger_NoTrust_400();                       // у инициатора нет trust → 400 (spec §4.4 п.1)
    [Fact] JoinTrigger_PeerRefused403_Proxied();            // фейк-сосед 403 → админ 403
    [Fact] JoinTrigger_InvalidResponse_502();               // фейк: 200 c невалидной подписью → 502
    [Fact] JoinTrigger_Success_AddsPeerTarget();            // фейк: валидный 200 (серт против нашего CA + подпись) → 200 {joined, peer}, targets содержит соседа

    // — удаление/deny-list/статус —
    [Fact] DeleteTarget_UnknownName_404();
    [Fact] DeleteTarget_LegacyRevoke_400();                 // legacy-запись (peer-a) + revoke=true → 400
    [Fact] DeleteTarget_KeyRevoke_BlocksJoin_202();         // EnsureKeyTarget → delete peer-x?revoke=true → 202; повторный входящий join → 403
    [Fact] DenyRemove_AllowsJoinAgain();                    // DELETE /admin/replication/denylist/peer-x → 202; join → 200
    [Fact] DenyList_Get_ListsEntries();
    [Fact] Status_PerTargetAuthField();                     // neighbors[]: auth == "legacy"/"key"
    [Fact] DeleteTarget_Plain_202_CursorKept();             // delete без revoke → 202; файл cursors/<name>.json не тронут (прямой доступ к тому фабрики — TempVolumeDir)
}
```

Fail-fast стартасервера на битом материале (покрытие Task 11; фабрика `PeerAuthBrokenIdentityAppFactory` — Identity с ключом ОТ ДРУГОЙ пары):

```csharp
// По прецеденту src/tests/OwnS3.IntegrationTests/Api/FailFastScenarios.cs:
// Program fail-fast (Environment.Exit/LogCritical) всплывает из WAF при старте хоста.
public class PeerAuthFailFastScenarios
{
    [Fact]
    public void FailFast_BrokenIdentityMaterial_RefusesStart()
    {
        // Arrange — фабрика: Identity.Certificate = свой серт, PrivateKeyPem = ключ другой пары
        using var factory = new PeerAuthBrokenIdentityAppFactory();
        // Act + Assert — CreateClient() бросает (отказ старта по прецеденту FailFastScenarios);
        // ни один endpoint не отвечает
    }
}
```

Подмена исходящего транспорта: в фабрике-наследнице `ConfigureTestServices(services => { services.RemoveAll<JoinPeerService>(); services.AddSingleton(sp => new JoinPeerService(identity, trust, targets, denyList, new HttpClient(fakeHandler), timeProvider, logger, advertised)); })` — зависимости через `sp` (материалы — статика фабрики).

- [ ] **Step 12.3: Прогон (сначала новые, затем ВСЯ Api-серия — регресс)**:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter FullyQualifiedName~PeerAuth
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Debug
```

Ожидание: PASS; существующие сценарии без правок ожиданий (критерий 8).

- [ ] **Step 12.4: Коммит**

```bash
git add src/tests/OwnS3.IntegrationTests/Api/PeerAuthScenarios.cs
git commit -m "t39: Api-интеграции PeerAuth — peer-грань/join/delete/denylist/статус/fail-fast + регресс legacy"
```

**Вход:** Tasks 9–11. **Выход:** фаза 3 spec закрыта. **Spec:** §5 фаза 3, критерии 2–5, 8.

---

## Фаза 4 — CLI (Task 13)

## Task 13: `src/OwnS3.Pki` + slnx + smoke

**Files:**
- Create: `src/OwnS3.Pki/OwnS3.Pki.csproj`
- Create: `src/OwnS3.Pki/Program.cs`
- Modify: `src/PgWorker.slnx` (папка `/owns3/`)

**Interfaces:**
- Consumes: `PeerCertificateAuthority`, `PeerCertificate`, `PeerTrustAnchor` (Tasks 1–2).
- Produces: CLI `ca create --name <ca> --out <dir>` → `ca.json` + `ca.private.pem`; `server issue --name <n> --ca <ca.json> --ca-key <ca.private.pem> [--expires-days N] --out <dir>` → `<n>.cert.json` + `<n>.private.pem` + печать env-строк.

- [ ] **Step 13.1: csproj** (консоль, без внешних пакетов; `Directory.Build.props` даёт net10.0/TreatWarningsAsErrors):

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\OwnS3.Protocol\OwnS3.Protocol.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 13.2: Program.cs** — ручной разбор args (`args[0]` — команда; `--name/--out/--ca/--ca-key/--expires-days` — пары), русские сообщения об использовании, коды выхода 0/1; `ca create`: `CreateCa(name)` → файлы `ca.json`/`ca.private.pem` (создание каталога); `server issue`: чтение `ca.json` (`PeerTrustAnchor.FromJson`) + `ca.private.pem` → `IssueServer(name, ca, caKey, DateTimeOffset.UtcNow, expiresDays)` → `<n>.cert.json`/`<n>.private.pem` + печать:

```
OWNS3_REPLICATION_CERT_PATH=<out>/<n>.cert.json
OWNS3_REPLICATION_PRIVATE_KEY_PATH=<out>/<n>.private.pem
OWNS3_REPLICATION_ADVERTISED_ENDPOINT=http://<n>:9000
```

(подсказка про `OWNS3_REPLICATION_TRUST_CA_PATH=<ca.json>` — в выдаче `ca create`).

- [ ] **Step 13.3: slnx** — в `<Folder Name="/owns3/">` добавить `<Project Path="OwnS3.Pki/OwnS3.Pki.csproj" />`.

- [ ] **Step 13.4: Сборка решения** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug` → чисто.

- [ ] **Step 13.5: Smoke (bash, каталог `/tmp`)**: `ca create` → `server issue` → ассерты: файлы существуют; `ca.json` парсится `PeerTrustAnchor.FromJson`; серт валиден `PeerCertificateAuthority.Verify` против CA (мини-проверка `dotnet run` не нужна — верификация тем же кодом в E2E Task 14); **контейнер старта**: `docker run -d --rm --name t39-pki-smoke -p <свободный порт>:9000` образ `owns3:e2e` (собран E2E-сборщиком; если не собран — `docker/OwnS3.Dockerfile` по канону 05 §6) с env из CLI (CERT_PATH-версии через `-v` монтирование или значения `cat`) + root-пара → `curl /healthz` → 200 → `docker rm -f`. Зачистка: контейнер удалён (`docker ps -a --filter name=t39-pki-smoke -q` → пусто).

- [ ] **Step 13.6: Коммит**

```bash
git add src/OwnS3.Pki/ src/PgWorker.slnx
git commit -m "t39: CLI OwnS3.Pki — выпуск CA/серверных сертификатов, env-строки; строка в slnx"
```

**Вход:** Tasks 1–2. **Выход:** утилита §4.6 (критерий 5). **Spec:** §4.6, §4.7, фаза 4.

---

## Фаза 5 — docker-E2E mesh (Task 14)

## Task 14: `E2e/Replication/PeerAuthScenarios`

**Files:**
- Modify: `src/tests/OwnS3.IntegrationTests/E2e/Replication/OwnS3MeshE2eEnvironment.cs` (identity-режим: генерация материала + env; через существующий хук `configureNode` — если его гибкости не хватит, добавить параметр `Action<MeshPeerAuthMaterial>?` / перегрузку, не ломая существующие вызовы)
- Create: `src/tests/OwnS3.IntegrationTests/E2e/Replication/PeerAuthScenarios.cs`
- Test: сам файл.

**Interfaces:**
- Consumes: `OwnS3MeshE2eEnvironment.StartAsync(slug, nodes, fullMeshTargets, configureNode)` (существует: network/volumes/динамические порты/tелеметрия/teardown/чистота), `PeerCertificateAuthority` (генерация материала в коде теста — spec фаза 5), admin-грань SigV4-клиент `MeshClients`, `DockerTrait`.
- Produces: mesh-хелп `MeshPeerAuth` (CA + серты per-node + advertised) — переиспользуется всеми 5 сценариями.

- [ ] **Step 14.1: Окружение — ключи в env.** Новый статический хелпер в файле окружения (или рядом): `MeshPeerAuth.Create(nodes, runTag)` → CA + per-node `(certJson, privatePem)`; узел i получает env (через `configureNode` колбэк из сценариев, чтобы НЕ менять сигнатуру `StartAsync`):

```
OWNS3_REPLICATION_CERT=<certJson узла i>
OWNS3_REPLICATION_PRIVATE_KEY=<privatePem узла i>
OWNS3_REPLICATION_TRUST_CA=<caJson>
OWNS3_REPLICATION_ADVERTISED_ENDPOINT=http://<NodeName(i)>:9000
```

(env-значениями — без монтирования файлов). `fullMeshTargets: false` — состав строится join'ом/ручным PUT сценария. Существующие сценарии (legacy) — не затронуты (configureNode по умолчанию null).

- [ ] **Step 14.2: Сценарии** (`[DockerTrait]`-gated, изоляция/teardown/`MarkFailed`/ассерт чистоты — наследуются окружением; все AAA; ключи — только из `MeshPeerAuth`):

```csharp
public sealed class PeerAuthScenarios
{
    // 1. mesh 3 ноды на key-записях (spec фаза 5, сценарий 1)
    [Fact] KeyMesh_JoinFullMesh_ReplicatesToAll();      // env identity+CA у всех; join пар (0,1)(0,2)(1,2) через
                                                        // POST /admin/replication/join (root SigV4) → статус каждой
                                                        // ноды: 2 соседа auth=key; PUT на A → объект на B и C
                                                        // (поллинг MeshClients ≤ бюджет E2E); join-ответ идемпотентен
                                                        // при повторе той же пары (200, записей не прибавилось)
    // 2. отзыв (сценарий 2)
    [Fact] RevokeOnAll_DenyBlocksJoin_UndenyAllows();   // join пары → PUT данные → DELETE targets/{name}?revoke=true
                                                        // НА ОБОИХ → повторный join → 403; GET denylist видит записи;
                                                        // DELETE denylist/{name} на обоих → join снова 200 → reconcile
                                                        // доезжает (объекты сравнялись)
    // 3. базовое удаление (сценарий 3)
    [Fact] DeleteWithoutRevoke_JoinBack_CursorContinues(); // join → PUT k1 → DELETE targets (без revoke) → join снова
                                                        // → PUT k2 → k2 доехал (курсор продолжился); дублей применения
                                                        // нет (applied-счётчики статуса без повторного всплеска k1;
                                                        // GET объекта один и тот же контент)
    // 4. сосуществование (сценарий 4)
    [Fact] MixedMesh_LegacyPairAndKeyPair_BothDeliver(); // 3 ноды: A — legacy-узел (без identity; pair-креды пары
                                                        // A-B), B и C — key-узлы (identity+CA, join B-C
                                                        // оркестратором). Пара A↔B — legacy С ОБЕИХ сторон (паттерн
                                                        // t35 OwnS3MeshE2eEnvironment ~строки 139–141: per-pair
                                                        // креды, ОБА узла регистрируют их policy=replication env'ом
                                                        // и кладут в targets записи друг друга): у A — legacy-запись
                                                        // B, у B — legacy-запись A, одни и те же креды. У B состав
                                                        // смешанный: legacy-запись A + key-запись C (из join).
                                                        // Ноги: PUT на A → объект на B (SigV4-нога A→B); PUT на B →
                                                        // объект на A (SigV4-нога B→A) И на C (peer-нога B→C по
                                                        // key-записи из join — единственная E2E-проверка исходящей
                                                        // peer-подписи из СМЕШАННОГО списка targets: B одновременно
                                                        // шлёт legacy-ногу A и key-ногу C); PUT на C → объект на B
                                                        // (peer-нога C→B)
    // 5. без trust-anchor (сценарий 5)
    [Fact] NoTrustAnchor_AdminJoin400_EdgeJoin403_ManualKeyTargetsWork(); // 2 ноды с identity БЕЗ TRUST_CA env
                                                        // (spec §5 сц. 5: trust-anchor не задан у инициатора →
                                                        // admin-join 400, у соседа → join-грань 403, §4.4):
                                                        // admin join на A → 400; прямой POST /replication/join
                                                        // peer-подписью на грань B → 403; ручной PUT targets с
                                                        // publicKey-записями на обоих → доставка работает без CA
}
```

Тонкости MixedMesh: узел A не получает identity-env; pair-креды A-B регистрируются env `OWNS3_ACCESS_KEYS__<slot>__*` (policy=replication) на ОБОИХ узлах — слоты без пропусков (комментарий окружения); legacy-записи в targets A и B — env-seed `OWNS3_REPLICATION_TARGETS__<i>__*` (legacy-формат из 4 полей — env-seed его умеет); key-запись C у B появляется join'ом (env-seed key-записей не существует — Task 4).

- [ ] **Step 14.3: Прогон серии (Release, docker)**:

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter "FullyQualifiedName~E2e.Replication.PeerAuthScenarios"
```

Ожидание: PASS; `[PHASE]`-метки в журнале; телеметрия в `/tmp/pgw-e2e-artifacts-<guid>/`.

- [ ] **Step 14.4: Зачистка контура после серии** (правила AGENTS.md):

```bash
docker ps -a --filter name=os3m- -q | xargs -r docker rm -f
docker volume ls -q --filter name=os3m- | xargs -r docker volume rm -f
docker network ls --filter name=os3m- -q | xargs -r docker network rm
docker network ls | grep -c os3m-   # → 0
docker ps -a --filter name=os3m- -q | wc -l   # → 0
```

- [ ] **Step 14.5: Коммит**

```bash
git add src/tests/OwnS3.IntegrationTests/E2e/Replication/
git commit -m "t39: docker-E2E PeerAuthScenarios — key-mesh join/отзыв/возврат/сосуществование/без-CA"
```

**Вход:** Tasks 1–13 (полный контур). **Выход:** фаза 5 spec, критерии 3/4/6. **Spec:** §5 фаза 5, §7 п.3/4/6.

---

## Фаза 6 — полные прогоны (Task 15)

## Task 15: Полные серии + зачистка

**Files:** — (только прогоны; правки — по находкам ревью сетей/сценариев).

- [ ] **Step 15.1: Юниты (Debug, весь проект)**:

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug
```

Ожидание: PASS (все, включая новые PeerAuth/Join/DenyList/Identity и прежние 834+). Зачистка не требуется (без docker).

- [ ] **Step 15.2: Api-интеграции (Debug, весь проект)** — `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.IntegrationTests -c Debug` → PASS (264+ существующих unchanged + новые PeerAuth). Зачистка: WAF без docker-контура — проверить отсутствие мусора не требуется; `docker ps -a -q | wc -l` контроль после серии (0 при чистом хосте).

- [ ] **Step 15.3: Docker-E2E на свежем Release — полная серия E2e.Replication** (включая PeerAuthScenarios и регресс legacy-сценариев mesh):

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter "FullyQualifiedName~E2e.Replication"
```

Ожидание: PASS (51+ существующих + новые 5). По каждому падению — разбор по логам БЕЗ перезапуска (правила `docs/e2e-launch.md`).

- [ ] **Step 15.4: Зачистка после серии** (команды Step 14.4 + общие):

```bash
docker ps -a --filter name=os3 -q | xargs -r docker rm -f
docker volume ls -q --filter name=os3 | xargs -r docker volume rm -f
docker network ls --filter name=os3 -q | xargs -r docker network rm
docker network ls | grep -c os3   # → 0 (осиротевших нет — правило kfw-сетей AGENTS.md)
```

- [ ] **Step 15.5: Финальный контроль и итог** — `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` чисто; соответствие критериям приёмки spec §7 (1–9) проверено по прогонам; при необходимости точечные правки + повтор ПОРАЖЁННОЙ серии только (не всего). Итоговое резюме в журнал задачи (коммитов нет, если правок нет).

**Вход:** все задачи. **Выход:** серии зелёные, контуры зачищены (критерии 7–9). **Spec:** §5 фаза 6, §7.

---

## Self-Review плана (заполнено при написании)

- **Покрытие spec:** §4.1→Tasks 1–2; §4.2→Task 3; §4.3→Tasks 4, 9; §4.4→Tasks 7, 9, 10, 11; §4.5→Tasks 5, 10; §4.6→Tasks 6, 11, 13; §4.7→все кодовые; §4.8→Task 0; §5 фазы 0–6→Tasks 0, 1–3, 4–8, 9–12, 13, 14, 15; §7 критерии 1–9→Tasks 0, 9/12, 7/12/14, 5/10/12/14, 6/11/13, 8/14, 15, 12/15, 15.
- **Типы:** имена `PeerCertificate/PeerTrustAnchor/PeerCertificateAuthority/PeerRequestSigner/PeerRequestVerifier/PeerIdentity/PeerIdentityLoader/DenyList/JoinPeerService/JoinAccept/PeerJoined/DenyEntry/JoinBadConfiguration|Refused|Conflict|BadGatewayException` согласованы между задачами; `JoinRefusedException.ErrorCode` (S3ErrorCode?) объявлен в Task 7 и потреблён маппингом `ErrorCode ?? AccessDenied` Task 9 и юнитом `Incoming_RequestSignatureMismatch`; `TargetRecord.Auth`-константы `AuthLegacy/AuthKey` едины; `CredentialOf` — единственный вариант `static string?` (Task 3 Produces = Task 9 Uses); ctor `JoinPeerService` — один список из 8 параметров (Task 7 Interfaces = Task 11 DI = Task 12 подмена); Task 11 — без `IdentityStore` (зеркало spec §4.7).
- **Result-контракт транспорта:** отказ «key-запись без identity» — пре-чек в начале каждого метода `ReplicationHttpClient` → `Result.Failed(PeerRejectedException)` ДО `BuildRequest` (Step 8.3: BuildRequest вне try в SendNotificationsAsync/FetchObjectAsync — throw вылетел бы в цикл нотификатора); покрыто юнитами Ping/Notifications_KeyTargetWithoutIdentity.
- **Согласованность сценариев:** MixedMesh (Task 14) — пара A↔B legacy с обеих сторон (per-pair креды, паттерн t35), key/join — только B↔C; захвачены все четыре ноги (A→B, B→A SigV4; B→C, C→B peer — B→C из смешанного списка targets B); сценарий 5 — `NoTrustAnchor_AdminJoin400_EdgeJoin403_ManualKeyTargetsWork` (зеркало spec §5 сц. 5); key-записи в состав попадают исключительно PUT admin-грани/join (env-seed legacy-only) — Task 4 (граница), Task 12 (EnsureKeyTargetAsync), Task 14 (тонкости MixedMesh) согласованы.
- **Прецеденты:** строковые контракты Protocol (SigV4Signer; CredentialOf — string?, без Result — конвенция t38), тестируемость без хоста (ReplicationProber), WAF in-memory (ReplicationAppFactory), fail-fast стартасервера (FailFastScenarios), env значение/путь (OwnS3Tls), mesh-E2E изоляция (OwnS3MeshE2eEnvironment + configureNode).
