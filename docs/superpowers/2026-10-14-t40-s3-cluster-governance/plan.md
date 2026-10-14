# t40-s3-cluster-governance — план реализации

Спецификация — [spec.md](spec.md) (одобрена, решения user-review закрыты §9).
Каноны: [arch/23-s3-clusters.md](../../../arch/23-s3-clusters.md) (контракт
etcd), [arch/24-s3worker.md](../../../arch/24-s3worker.md) (процессы, API),
[arch/owns3/07-replication.md](../../../arch/owns3/07-replication.md)
(peer-PKI/join/deny-list). Arch-правки Фазы 1 уже в ветке — план строится
поверх них; отдельный arch-шаг не нужен.

Worktree: `/Users/demakaev/ZCodeProject/worktrees/feat-t40-s3-cluster-governance`
(ветка `feat-t40-s3-cluster-governance`). Все пути ниже — от корня worktree.
Arch-правки Фазы 1 уже в ветке — план строится поверх них; единственное
исключение — обязательная arch-микроправка формата `pki/ca` (шаг 1, п. 0,
arch-first внутри шага, ДО кода).

## Сводка работ

Backend-часть управления кластерами ownS3 силами S3Worker:

1. Контракт/модель: arch-микроправка формата `pki/ca` (arch/23 §2 ← §2.1,
   первым действием); парсер новых ключей (`links`, `denylist`, `pki/*`,
   `nodes/<I>/pki`), домен, ключи-хелперы.
2. PKI per-cluster в etcd: `PkiStore` (ленивый выпуск через библиотеку
   peer-PKI ownS3), `InstanceEnvComposer` (единая сборка env инстанса).
3. Клиент грани ownS3: `GET /admin/replication/status`,
   `POST /admin/replication/join`, `DELETE /admin/replication/targets/{name}`
   (SigV4 root-креды), `PUT /replication/denylist` (подпись OWNS3-PEER
   сертификатом оркестратора).
4. HTTP API: создание с `instances[]` 0..9, add-node с именем из запроса,
   гварда «не последний инстанс» (уже есть — сверка), `PUT links`,
   `POST deny`/`allow`, seed-demo под новые форматы.
5. Процессы: новый `MeshGovernanceProcess` (G); P2/P4/D/E/надзор/ротатор
   через PKI + env-композер; docker-драйвер — имена `s3w-<C>-<I>`,
   перечисление объектов по префиксу+label (без перебора `node<k>`).
6. Тесты: юниты → интеграция (API/S3) → docker-E2E (Release) по канонам
   изоляции/телеметрии; мерж-гейт — E2E-кейс-маркер PgWorker
   `Scale_AddEmptyShard` (задача трогает воркеров) + полная S3Worker E2E-серия.

## Соглашения исполнения (обязательные)

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`: каждый шаг
  завершается безварнинговой сборкой. Централизованные версии пакетов —
  `Directory.Packages.props` (новых внешних пакетов НЕ появляется).
- Комментарии/документация — по-русски; идентификаторы — английские.
- Все команды сборки/тестов исполнитель запускает в фоне
  (`run_in_background: true`), waits ≤ 30 с, control по task_id.
- Тестовые серии — строго по очереди (юниты → интеграция → E2E), после
  КАЖДОЙ серии: дождаться финальной строки, зачистка остатков
  (`docker ps -a | grep s3w-`, `docker volume ls | grep s3w-`,
  `docker network ls | grep s3w-en-`; `docker network prune -f` —
  страховочно). Не запускать следующую серию поверх незачищенной.
- Docker-порты в тестах — только динамические (фикстуры уже следуют);
  таймауты ожидания нод ≤ 100 с (`NodeBootSec`); никаких литералов портов.
- E2E: guid во всех именах, полный teardown при любом исходе, ассерт
  чистоты, телеметрия `[PHASE]` + артефакты `/tmp/pgw-e2e-artifacts-<guid>/`
  ДО удаления, `MarkFailed()` останавливает без удаления (разбор по логам,
  перезапуск упавших — только с согласия пользователя). Каноны:
  `docs/e2e-isolation.md`, `docs/e2e-launch.md`.
- Порядок шагов фиксирован; шаг N рассчитан на завершённость шага N-1.

Общие команды проверки (используются в шагах):

```
# сборка всего решения (worktree)
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
# юниты S3Worker
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --no-build --filter <фильтр>
# регрессии ownS3 (peer-PKI/грани не должны деградировать)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug --no-build
```

---

## Шаг 1. Контракт/модель: домен, парсер, ключи-хелперы

**Вход (предусловие).** Arch-канон arch/23 §2/§2.1 обновлён (Фаза 1);
существующие парсер/модель — `src/S3Worker.Core/Model/S3Domain.cs`,
`src/S3Worker.Etcd/Parsing/S3SnapshotParser.cs` — читают только
`config`/`endpoints`/`nodes/<I>/{placement,resources,state,placed_on,usage}`.

**Действие (файлы/изменения).**

0. Arch-микроправка (первое действие шага, ДО кода — arch-first): в
   `arch/23-s3-clusters.md` §2 строка таблицы ключа `pki/ca` — выровнять
   поле приватного ключа с каноническим примером §2.1: `privateKey_pem` →
   `private_key_pem` (внутренняя несогласованность §2 ↔ §2.1; spec §3.2
   следует §2.1 — правка канона, не спека). Итог: и таблица §2, и пример
   §2.1 несут `private_key_pem`.
1. `src/S3Worker.Core/Model/S3Domain.cs`:
   - новый record `S3ReplicationLink(string A, string B)` — пара графа
     репликации (arch/23 §2 `links`);
   - `S3ClusterSnapshot` расширяется полями:
     - `IReadOnlyList<S3ReplicationLink>? Links` — декларация графа (null —
       ключа нет ИЛИ битое значение; битость — в `ParseError`);
     - `IReadOnlyList<string>? DenyList` — декларация отзыва (null —
       ключа нет ИЛИ битое значение);
   - PKI-материал в снапшот НЕ входит (spec §5.1: pki — чтение отдельным
     стором при выпуске env, не в снапшот тика).
2. `src/S3Worker.Etcd/Parsing/S3SnapshotParser.cs`:
   - распознавание ключей кластера `links`, `denylist`, `pki/ca`,
     `pki/orchestrator` (сегменты 5 уровня) и листа `nodes/<I>/pki`;
   - `links` → `[{a,b},…]` в `Links` (строки; отсутствие поля/не-строка —
     битое → parseError); `denylist` → `["I",…]` в `DenyList`;
   - pki-ключи распознаются как известные (НЕ unknownKeys), значения в
     модель не разбираются (PkiStore читает их отдельным GET по месту);
   - битые JSON `links`/`denylist` → строка в `ParseError` кластера
     (`"/s3/clusters/<C>/links: битый JSON"` и т.п.), `Links`/`DenyList` =
     null; тик не падает (arch/23 §9);
   - прочие неизвестные ключи — прежний путь unknownKeys.
3. Ключи-хелперы (одна точка истины, используются API и процессами):
   - `src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs`:
     `LinksKey(cluster)`, `DenyListKey(cluster)`, `ClusterPkiCaKey(cluster)`
     → `/s3/clusters/<C>/pki/ca`, `OrchestratorPkiKey(cluster)` →
     `…/pki/orchestrator`, `NodePkiKey(cluster, node)` →
     `…/nodes/<I>/pki`;
   - `src/S3Worker.App/Api/Operations/S3Api.cs` (`S3ApiHelpers`): те же
     ключи делегируют S3ProcessCommon (или дублируют формат-константой —
     по существующему стилю файла).
4. Юнит-тесты `src/tests/S3Worker.UnitTests/Etcd/S3SnapshotParserTests.cs`
   (дополнить кейсами):
   - канонические примеры arch/23 §2.1: `links` частичного графа
     (`[{"a":"east1","b":"east2"},{"a":"east1","b":"west1"}]`), `denylist`
     `["west1"]`, `nodes/<I>/pki` (сертификат как экранированная JSON-строка
     — значение НЕ разбирается), `pki/ca`, `pki/orchestrator`,
     `config` с `nodes=0` → модель;
   - битые `links`/`denylist` → parseError, `Links`/`DenyList` null, тик
     (парсер) не падает;
   - `pki`-ключи НЕ попадают в unknownKeys; реально неизвестный ключ —
     попадает.

**Выход.** Модель и парсер понимают полный контракт arch/23 §2; ключи
централизованы; юнит-тесты парсера зелёные.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --no-build --filter FullyQualifiedName~S3SnapshotParser
```

Критерий: build 0 warnings; все тесты фильтра зелёные (включая прежние
кейсы — регрессия парсера).

**Связь со spec.** §3.1 (структура `nodes/<I>/…` сохранена), §3.3 (links),
§3.4 (denylist), §3.2 (ключи pki), §5.1–5.2, §8.1.1.

---

## Шаг 2. PKI per-cluster: PkiStore + InstanceEnvComposer

**Вход.** Шаг 1 завершён (ключи-хелперы). Библиотека peer-PKI доступна в
`src/OwnS3.Protocol/PeerAuth/` (`PeerCertificateAuthority.CreateCa/
IssueServer/Verify`, `PeerCertificate`, `PeerTrustAnchor`,
`PeerRequestSigner`); образец загрузки — `src/OwnS3.Replication/
PeerIdentityLoader.cs` (сверка ключа с сертификатом).

**Действие (файлы/изменения).**

1. `src/S3Worker.Core/S3Worker.Core.csproj`:
   `<ProjectReference Include="..\OwnS3.Protocol\OwnS3.Protocol.csproj"/>`
   (переиспользование крипты, копирование запрещено — spec §3.2).
2. Новый `src/S3Worker.Provisioning/PkiStore.cs` (конструктор:
   `IEtcdGateway gateway, string[] endpoints, TimeProvider clock`):
   - записи материала:
     - `ClusterCaMaterial(PeerTrustAnchor Ca, string CaPrivateKeyPem)` —
       хранение `pki/ca` = JSON `{"name":"<C>","publicKeyPem":…,
       "private_key_pem":…}` (формат arch/23 §2.1 — смешанный регистр полей
       канона, сериализация вручную `Utf8JsonWriter`, не атрибутами);
     - `IssuedPkiMaterial(PeerCertificate Certificate, string
       CertificateJson, string PrivateKeyPem)` — хранение `pki/orchestrator`
       и `nodes/<I>/pki` = JSON `{"certificate":"<PeerCertificate JSON как
       экранированная строка>","private_key_pem":…}`;
   - `EnsureClusterPkiAsync(cluster, ct) -> Result<(ClusterCaMaterial,
     IssuedPkiMaterial orch)>`: ленивый выпуск CA+оркестратора —
     `PeerCertificateAuthority.CreateCa(<C>)`, затем `IssueServer(
     "orchestrator", ca, caKey, at, expiresDays: 0)` (бессрочные, §3.2);
     запись txn put-if-absent ОБЕИХ ключей (compare NotExists на оба);
     проигрыш → re-read — первый записавший истина (arch/17 S5);
     частичное предсуществование после re-read — два дисъюнктных случая:
     - **ca есть / orchestrator нет** → добор: оркестратор выпускается
       СУЩЕСТВУЮЩИМ CA отдельной txn put-if-absent (идемпотентно, тот же
       паттерн) → возврат пары (ca, orch);
     - **orchestrator есть / CA нет** → `Result.Failed` с диагностикой
       «приватный ключ CA невосстановим, перевыпуск CA разорвал бы
       trust-домен (сертификат оркестратора подписан неизвестным CA;
       инстансы держат прежний OWNS3_REPLICATION_TRUST_CA)» — частный
       случай существующей ветки «несогласованная пара, не добираемая
       так — Failed» (иные битости материалов — тот же Failed-путь);
     итог: успешный возврат — всегда согласованная пара (ca,
     orchestrator), подписанная одним CA; идемпотентность — повторный
     вызов возвращает существующее;
   - `EnsureNodePkiAsync(cluster, node, ca, ct) -> Result<IssuedPkiMaterial>`:
     CA обязан существовать (передан вызывающим); `IssueServer(<I>, …,
     expiresDays: 0)`; txn put-if-absent + re-read;
   - чтение `TryReadNodePkiAsync/TryReadCaAsync` (для deny-доведения G и
     env-композера): разбор JSON, проверка целостности — приватный ключ
     соответствует публичному ключу сертификата (по образцу
     `PeerIdentityLoader`: ImportFromPem + ExportSubjectPublicKeyInfoPem,
     нормализация `\r\n`), сертификат проходит `PeerCertificateAuthority.
     Verify` против trust-anchor своего CA; битый/несогласованный материал —
     `Result.Failed` с диагностикой (без исключений наружу);
   - failover по endpoints — по образцу `S3ProcessCommon.
     GetWithFailoverAsync`/`TxnAsync` (можно вынести общий хелпер или
     дублировать локально — по стилю проекта).
3. Новый `src/S3Worker.Provisioning/InstanceEnvComposer.cs` (чистый класс
   без etcd — сборка из готового материала):
   - `Compose(OwnS3NodeSecrets secrets, IssuedPkiMaterial nodePki,
     PeerTrustAnchor ca, string advertisedEndpoint) ->
     IReadOnlyDictionary<string,string>`:
     - клиентская грань (без изменений): `OWNS3_ROOT_USER`,
       `OWNS3_ROOT_PASSWORD`, `OWNS3_ACCESS_KEYS__<i>__*` (из
       `secrets.ToContainerEnv()`), `OWNS3_DATA_DIR=/data`;
     - репликация (значения, не пути — §3.6): `OWNS3_REPLICATION_CERT` =
       `nodePki.CertificateJson`, `OWNS3_REPLICATION_PRIVATE_KEY` =
       PEM, `OWNS3_REPLICATION_TRUST_CA` = `ca.ToJson()` (JSON
       `{"name","publicKeyPem"}`), `OWNS3_REPLICATION_ADVERTISED_ENDPOINT`
       = `http://<advertised-host>:<s3-port>`;
   - состав mesh env'ом НЕ передаётся (§3.6: только join/G).
4. Юнит-тесты (новый файл
   `src/tests/S3Worker.UnitTests/Provisioning/PkiStoreTests.cs`):
   - существующий `Fakes.FakeEtcd` (уже поддерживает txn-семантику):
     повторный `EnsureClusterPkiAsync`/`EnsureNodePkiAsync` возвращает ТОТ ЖЕ
     материал (if-absent, §8.1.3);
   - частичное предсуществование — добор: в FakeEtcd предзаполнен только
     `pki/ca` → `EnsureClusterPkiAsync` добирает оркестратор
     put-if-absent СУЩЕСТВУЮЩИМ CA и возвращает согласованную пару
     (CA не перевыпускается — publicKeyPem тот же);
   - частичное предсуществование — отказ: в FakeEtcd предзаполнен только
     `pki/orchestrator` (CA нет) → `EnsureClusterPkiAsync` = Failed с
     диагностикой «ключ CA невосстановим / перевыпуск разорвал бы
     trust-домен», CA не создаётся (ключ `pki/ca` не появляется);
   - сертификат инстанса проходит `PeerCertificateAuthority.Verify` против
     trust-anchor своего CA и НЕ проходит против CA другого кластера
     (изоляция trust-доменов, §3.2);
   - сертификат ↔ ключ соответствуют; подмена ключа в etcd →
     `TryRead*` Failed;
   - `InstanceEnvComposerTests` (новый): полный набор `OWNS3_*` (клиентская
     грань + 4 репликационных переменных), advertised-endpoint из
     portalloc/AdvertisedHost (§8.1.4).

**Выход.** PKI выпускается и хранится в etcd лениво/идемпотентно; env
инстанса собирается одной точкой; изоляция кластеров доказана юнитами.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --no-build --filter "FullyQualifiedName~PkiStore|FullyQualifiedName~InstanceEnvComposer"
```

**Связь со spec.** §3.2 (PKI per-cluster), §3.6 (env), §5.1 (ProjectReference),
§5.3 (PkiStore/InstanceEnvComposer), §8.1.3–8.1.4.

---

## Шаг 3. Клиент грани ownS3: расширения OwnS3NodeClient

**Вход.** Шаг 2 завершён (материал оркестратора для подписи). Контракты
грани ownS3: `GET /admin/replication/status` → `{active, outbox{…},
neighbors:[{name, auth, online, …}]}` (ReplicationAdminHandlers.
HandleStatusAsync); `POST /admin/replication/join` тело `{"endpoint"}` →
200 `{joined,peer}` / 403 / 409 / 502; `DELETE /admin/replication/
targets/{name}` → 202/404; `PUT /replication/denylist` — до-join контур
(подпись OWNS3-PEER + сертификат в `X-OwnS3-Peer-Certificate`, owns3/07
§6); SigV4 админ-грани принимает `UNSIGNED-PAYLOAD` (PayloadHashModeClassifier).

**Действие (файлы/изменения).**

1. `src/S3Worker.Core/OwnS3/SigV4GetSigner.cs` — обобщение до подписи
   произвольного метода: `Sign(method, uriPath, hostHeader, accessKey,
   secretKey)` (canonical request строится с фактическим методом; payload
   — UNSIGNED-PAYLOAD, как эталонный клиент owns3/05 §7; GET-поведение
   сохраняется, существующие вызовы не ломаются).
2. `src/S3Worker.Core/OwnS3/OwnS3NodeClient.cs` — расширение интерфейса
   `IOwnS3NodeClient` и реализации (все таймауты 7.5 с — именованный
   HttpClient; ретраи — дело тика, spec §5.1):
   - `GetReplicationStatusAsync(host, port, creds, ct) -> Result<
     OwnS3ReplicationStatus>`: SigV4 GET `/admin/replication/status`;
     модель `OwnS3ReplicationStatus(bool Active, IReadOnlyList<string>
     NeighborNames)` (из `neighbors[].name`; детальные счётчики G не
     нужны — только состав);
   - `AdminJoinAsync(host, port, creds, endpoint, ct) -> Result<
     OwnS3JoinOutcome>`: SigV4 POST `/admin/replication/join` телом
     `{"endpoint":"<b-advertised>"}`; исходы-enum:
     `Joined(name)` (200) / `Conflict` (409 — имя числится с другим
     ключом) / `Refused` (403 — в т.ч. deny) / `Transient` (502/прочие
     HTTP/транспорт/таймаут) — G различает исходы для решения (§3.5);
   - `DeleteTargetAsync(host, port, creds, name, ct) -> Result<
     bool removed>`: SigV4 DELETE `/admin/replication/targets/{name}`;
     202 → true; 404 → false (идемпотентность); прочее — Failed;
   - `PutDenyListAsync(host, port, IReadOnlyList<(string Name, string
     PublicKeyPem)> entries, IssuedPkiMaterial orchestrator, ct) ->
     Result<bool>`: `PUT /replication/denylist` телом
     `{entries:[{name,publicKeyPem}]}`; подпись — `PeerRequestSigner.Sign`
     приватным ключом оркестратора (имя `orchestrator`, путь
     `/replication/denylist`, заголовки host/`x-owns3-peer-date`/
     `x-owns3-peer-content-sha256` = hex-sha256 тела), сертификат —
     заголовок `X-OwnS3-Peer-Certificate: base64(UTF8(JSON))` — ТОЧНО по
     образцу `JoinPeerService.JoinAsync` (src/OwnS3.Replication/
     JoinPeerService.cs строки построения запроса); 202 → true; 403 →
     false+диагностика (trust не принят — transient-обработка тиком);
     400/транспорт → Failed;
   - Result-конвенция: никаких исключений наружу (образец — существующие
     методы клиента).
3. Юнит-тесты:
   - `src/tests/S3Worker.UnitTests/OwnS3/OwnS3NodeClientTests.cs`
     (дополнить): FakeHttpHandler-исходы каждого метода (200/202/409/403/
     404/502/таймаут) — маппинг в исходы;
   - новый кейс «подпись PUT /replication/denylist проходит
     `PeerRequestVerifier` ownS3» (§8.1.6): внутренняя точка подписи
     вынесена в `internal static` (InternalsVisibleTo у Core уже есть) —
     тест собирает подписанные заголовки, строит `S3RequestModel` (по
     образцу тестов JoinPeerService в src/tests/OwnS3.UnitTests) и
     прогоняет `new PeerRequestVerifier(time).Verify(model, ключ
     оркестратора)` → Ok;_verifier_библиотека — ProjectReference уже из
     шага 2.
   - `SigV4GetSignerTests` — кейс POST/DELETE (метод в canonical request).

**Выход.** Воркер умеет читать состав targets, звать join, сносить target
и доводить deny-list; подписи взаимно совместимы с ownS3 (юнит-доказано).

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --no-build --filter "FullyQualifiedName~OwnS3NodeClient|FullyQualifiedName~SigV4"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests -c Debug --no-build
```

(вторая команда — регрессия OwnS3: peer-грани не тронуты.)

**Связь со spec.** §3.4 (deny-доведение), §3.5 (join/delete-операции G),
§3.9 (роли доступа: SigV4 root для админ-грани, OWNS3-PEER оркестратора
для до-join), §5.1, §8.1.6.

---

## Шаг 4. HTTP API: хендлеры, валидация, роутинг, seed-demo

**Вход.** Шаг 1 завершён (ключи-хелперы). Существующие: CreateClusterHandler
(генерит `node<k>`, тело `nodes`+`placements`), AddNodeHandler (генерит
`node<max+1>`), DeleteNodeHandler (гварда «последняя нода → 409» уже есть),
`S3ApiHelpers.ListNodeNamesAsync` (фильтр `StartsWith("node")`).

**Действие (файлы/изменения).**

1. `src/S3Worker.App/Api/Operations/S3Api.cs`:
   - `S3Limits`: `InstanceNamePattern()` — тот же генератор-regex, что
     `ClusterPattern()` (переиспользование); константа
     `ReservedInstanceName = "orchestrator"`;
   - DTO: `CreateS3ClusterRequest` → `{name, platform, secretProfile,
     instances:[{name, placement, resources}]?, resources?}` (новый record
     `S3InstanceDto(string? Name, S3PlacementDto? Placement,
     S3ResourcesDto? Resources)`; поля `nodes`/`placements` УДАЛЯЮТСЯ —
     миграции формата нет, spec §7); `AddNodeRequest` → `{name, placement,
     resources}`; новый `PutLinksRequest(IReadOnlyList<S3LinkDto> Links)`,
     `S3LinkDto(string? A, string? B)`;
   - `S3ApiHelpers.ListNodeNamesAsync`: убрать фильтр
     `StartsWith("node")` — все сегменты `nodes/<I>/*` дают имена
     (произвольные имена; старые `node<k>` читаются как валидные имена);
   - новые исключения по стилю файла: `S3NodeAlreadyExistsException`
     (409 коллизия имени инстанса), `S3InvalidLinksException` (400 —
     наследует S3ValidationException-паттерн с errors).
2. `src/S3Worker.App/Api/Operations/CreateClusterHandler.cs`:
   - валидатор `S3ClusterValidator`: кластер — как прежде; `instances`
     0..9; каждый инстанс: имя паттерн + НЕ `orchestrator` + уникальность
     в теле (400); placement обязателен (каждому инстансу — свой или
     инхерит общего правила валидации: поле обязательное, минимум один
     атрибут); resources — per-instance с fallback на общий (валидация
     диапазонов прежняя);
   - клэйм-txn: compare `NotExists(config)` + пакет ops: config
     (`nodes=len(instances)`, `state=NOT_INITIALIZED`) + для каждого
     инстанса `nodes/<I>/{placement,resources,state=NOT_INITIALIZED}`;
     0 инстансов — только config (P1–P4 пройдут пустыми — arch/24 §5 A);
     сбой midway (частичный PUT между txn-попытками не возникает — одна
     txn; тем не менее сохранить документированную компенсацию del
     `--prefix` на случай исключения после записи, arch/24 §1.1);
   - ответ 201: `{name, state:"NOT_INITIALIZED", nodes, secretProfile,
     instances:[имена]}`.
3. `src/S3Worker.App/Api/Operations/ClusterHandlers.cs`:
   - `AddNodeHandler`: имя из тела (валидация: паттерн, не
     `orchestrator`); кластер Active (как прежде); предел состава ≤9 по
     фактическому списку имён; клэйм-txn `NotExists(nodes/<I>/state)` +
     пакет placement/resources/state; проигрыш txn → 409 (коллизия имени);
     ответ 201 `{cluster, node: имя}`;
   - `DeleteNodeHandler`: сверка со спецификацией §3.7 — 404 нет инстанса
     (уже есть), already TO_REMOVE → 204 (есть), «последний инстанс» →
     409 `S3LastNodeException` (есть — обновить текст сообщения под
     термин «инстанс», семантика гварды сохранена, решение user-review
     §9.3); прочее без изменений;
   - новый файл `src/S3Worker.App/Api/Operations/LinksDenyHandlers.cs`:
     - `PutLinksHandler(gateway, endpoints)`: кластер существует (404) —
       единственная гварда состояния; гварды Active НЕТ (spec §3.7 /
       arch/24 §1.1 для links фиксируют только 202/400): запись `links` в
       ещё NOT_INITIALIZED кластер штатна и полезна — P4 применит граф
       при provisioning;
       валидация: имена a/b существуют в фактическом составе
       (ListNodeNamesAsync), `a≠b`, пары уникальны с точностью до
       зеркальности (`{a,b}` ≡ `{b,a}` — одна связь; зеркальный дубль =
       400 дубликата); валидатор — чистая функция (links × фактические
       имена) — юнит-тестируемая; put ключа `links` (замена целиком, канонический
       JSON `[{"a":…,"b":…},…]` в порядке запроса) → 202;
     - `DenyHandler`: 404 если инстанса нет; RMW `denylist` (read →
       append имени если нет → put; txn по mod_revision, проигрыш —
       повтор внутренним циклом/следующим запросом): уже в списке → 204
       идемпотентно; добавили → 202;
     - `AllowHandler`: 404 если инстанса нет; имени нет в списке → 204
       идемпотентно; удалили → 202.
4. `src/S3Worker.App/Api/ApiModule.cs` — роутинг:
   - `PUT /api/s3/clusters/{cluster}/links` → 202/400/404/503
     (409 не производится: гварды состояния кластера нет);
   - `POST /api/s3/clusters/{cluster}/nodes/{node}/deny` → 202/204/404/503;
   - `POST /api/s3/clusters/{cluster}/nodes/{node}/allow` → 202/204/404/503;
   - все эндпоинты инкрементируют `s3worker_api_operations_total`
     фактическим кодом (`metrics.Track("put-links"/"deny-node"/
     "allow-node", …)`) — по существующему паттерну.
5. `src/S3Worker.App/Api/Operations/SeedDemoHandler.cs`: демо-сид под новые
   форматы — кластер `demo`, instances `east1`,`east2` (placement
   docker_host=local, ресурсы прежние), после создания — put `links`
   `[{east1,east2}]` (демонстрация графа); идемпотентность прежняя.
6. `src/S3Worker.App/Program.cs`: DI новых хендлеров (PutLinksHandler,
   DenyHandler, AllowHandler) по существующему паттерну.
7. Юнит-тесты валидаторов (spec §8.1.2) —
   `src/tests/S3Worker.UnitTests/Api/S3ValidationTests.cs` (дополнить):
   - имя инстанса: нарушение паттерна `^[a-z][a-z0-9_]{0,62}$` → 400 (в
     теле создания и add-node), резерв `orchestrator` → 400, дубль имени
     внутри тела создания → 400 (коллизия с существующим в etcd — 409,
     интеграционный кейс ниже);
   - links (чистый валидатор, §8.1.2): несуществующее имя → 400;
     `a==b` → 400; дубль пары → 400; зеркальный дубль `{a,b}`+`{b,a}` → 400.
8. Интеграционные тесты API (etcd-контур, `src/tests/S3Worker.
   IntegrationTests/Api/ClusterMutationsApiTests.cs` + новый кейс-файл при
   необходимости; фикстура `S3ApiFactory`):
   - §8.2.1: создание с `instances=[]` → 201; config `nodes=0`,
     `state=NOT_INITIALIZED`; (снятие state воркером — вне API-теста,
     проверяется E2E §8.3.5);
   - §8.2.2: создание с 2 именованными инстансами → ключи
     `nodes/<I>/*` записаны; коллизия имени → 409; имя `orchestrator` →
     400; паттерн-нарушения → 400;
   - §8.2.3: `PUT links` валидный → 202 + значение ключа в etcd;
     несуществующее имя → 400; `a==b` → 400; дубль пары (в т.ч.
     зеркальный) → 400;
   - §8.2.4: deny → 202, повтор → 204, denylist в etcd содержит имя;
     несуществующий инстанс → 404; allow → 202, повтор → 204, имя убрано;
   - §8.2.5: удаление инстанса при ≥2 → 202 (TO_REMOVE); удаление
     последнего → 409;
   - существующие кейсы create/delete/rotations/resources — перевод на
     новый формат тела (старый формат удалён).

**Выход.** API воркера полностью покрывает таблицу arch/24 §1.1 (создание с
instances[], add с именем, гварда последнего, links, deny/allow);
декларации пишутся корректно; интеграционные тесты API зелёные.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --no-build --filter FullyQualifiedName~S3Validation
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests -c Debug --no-build --filter "FullyQualifiedName~ClusterMutations|FullyQualifiedName~LinksDeny"
```

(интеграционные API-тесты поднимают etcd-фикстуру `S3EtcdFixture` — после
серии зачистка контейнеров фикстуры.)

**Связь со spec.** §3.1 (имена, 0 инстансов, резерв `orchestrator`), §3.7
(таблица эндпоинтов), §3.8 (гварды), §5.5 (хендлеры/роутинг/seed), §8.2.

---

## Шаг 5. Процесс G (MeshGovernanceProcess) — логика на фейках

**Вход.** Шаги 2–3 завершены (PkiStore, клиент грани). Существующие фейки:
`src/tests/S3Worker.UnitTests/Fakes/Fakes.cs` (`FakeEtcd`, `FakeS3Driver`,
`FakeNodeClient` — расширить).

**Действие (файлы/изменения).**

1. Новый `src/S3Worker.Provisioning/Processes/MeshGovernanceProcess.cs`
   (конструктор: `IEtcdGateway, string[] endpoints, ClaimStore claims,
   WorkJournal journal, SecretProfileStore secrets, PkiStore pki,
   IOwnS3NodeClient nodeClient, S3ProvisioningOptions options,
   System.Diagnostics.Metrics.Meter meter, …`):
   - `TickAsync(S3ClusterSnapshot snap, CancellationToken ct) -> Result`
     (вызов — только под клэймом `<C>`: `S3ProcessCommon.EnsureClaimed`,
     op=`mesh-governance`, journal-before-manipulations); отдельный
     публичный вход `RunProvisioningPhaseAsync` НЕ нужен — P4 зовёт тот же
     `TickAsync` (arch/24 §5: P4 — та же механика, что G);
   - вход: `snap` (`Links`, `DenyList`, состав/state инстансов) +
     перечитка portalloc (`S3ProcessCommon.ParsePortAlloc`) + креды
     профиля (отказ профиля — диагностический skip с journal-записью, по
     образцу надзора);
   - **шаг 1 deny-доведение** (§3.4/§3.5): для КАЖДОГО RUNNING-инстанса —
     `PutDenyListAsync` полной перезаписью: entries = имена из
     `snap.DenyList` × publicKeyPem из `nodes/<I>/pki` (`PkiStore.
     TryReadNodePkiAsync`; имени нет в pki → пропуск имени + warning в
     journal, доведение остального); перед первой перезаписью —
     `EnsureClusterPkiAsync` (CA+оркестратор лениво); недоступный инстанс
     (transport/403) — transient, следующий тик; deny-перезапись — всем
     живым сразу (дешёвая, идемпотентная — не «одна за тик»);
   - **шаг 2 сверка состава**: для каждого RUNNING-инстанса X —
     `GetReplicationStatusAsync` → фактические `NeighborNames`;
     желаемый состав X:
     - **`X ∈ DenyList` → ∅** (полная изоляция, arch/24 §5 G п.3 /
       spec §3.4(б), решение user-review §9.2): все собственные
       target-записи deny-инстанса — лишние; снос — теми же
       delete-парами, одна за тик (соседи X по links в DenyList не
       числятся, поэтому без этого правила записи X не опустошаются —
       правило явно гасит формулу);
     - иначе = `{соседи по links}` минус `DenyList` минус несуществующие
       имена минус не-RUNNING (связь с PROVISIONING-соседом — после его
       RUNNING);
     расхождение → ОДНА структурная операция mesh за тик
     (первая по Ordinal-порядку):
     - недостающая пара {a,b} → `AdminJoinAsync` на a (endpoint b =
       `http://<AdvertisedHost ?? b-host>:<b-s3-port>` из portalloc — то
       же правило, что endpoints, §3.5 границы G); join двусторонний;
       исход `Conflict` (409) → `DeleteTargetAsync(конфликтное имя)` на
       конфликтующей стороне + повтор следующим тиком; `Refused`/
       `Transient` → journal waiting-join, ретрай тиком;
     - лишняя запись (фактический сосед не в желаемых: убран из links,
       сосед в DenyList, инстанс выведен; у deny-инстанса X — ЛЮБАЯ его
       собственная запись, желаемый состав пуст) →
       `DeleteTargetAsync(name)` на КАЖДОЙ стороне пары (курсор
       сохраняется — owns3/07 §2) — одна пара за тик; journal
       `cleaned-targets`;
   - **шаг 3 диагностика/метрики**: journal-фазы (`deny-pushed`,
     `waiting-join`, `cleaned-targets`, ошибки — `last_error`); метрики
     Meter `s3worker` (arch/24 §7): `s3worker_mesh_join_total{result}`,
     `s3worker_mesh_delete_total{result}`, `s3worker_mesh_deny_total{
     result}` (result: accepted/conflict/transient/cleaned — словарь
     согласовать с §7 при реализации, каркас `Shared.Metrics`);
   - битые `links`/`denylist` (parseError снапшота, Links/DenyList null
     при наличии ключа) → G пишет диагностику в journal и НЕ применяет
     битую часть (§3.3); отсутствующие ключи (links нет) — пустой граф:
     только deny-доведение + снос лишних связей (полный разрыв — валидная
     декларация);
   - 0 инстансов / нет RUNNING — no-op (journal-фаза idle).
2. `src/tests/S3Worker.UnitTests/Fakes/Fakes.cs`: расширение
   `FakeNodeClient` — записываемые вызовы status/join/delete/denylist +
   программируемые исходы (защёлки вызовов для assertions).
3. Юнит-тесты `src/tests/S3Worker.UnitTests/Provisioning/
   MeshGovernanceTests.cs` (§8.1.5, на FakeEtcd+FakeNodeClient+FakeS3Driver):
   - недостающая пара → join с корректным endpoint b (из portalloc,
     AdvertisedHost-приоритет);
   - лишняя запись → delete на ОБЕИХ сторонах пары;
   - deny-инстанс: join с ним НЕ зовётся, его записи у соседей и свои —
     сносятся (имя вне желаемого состава);
   - join → 409 → delete конфликтной стороны, join повторится следующим
     тиком (второй TickAsync);
   - не-RUNNING сосед → ожидание (нет join), после перевода в RUNNING —
     join;
   - deny-перезапись отправлена ВСЕМ RUNNING (каждому — полный список
     entries с publicKeyPem из etcd-ключей);
   - одна структурная операция за тик (два расхождения → первый тик
     закрывает одно, второй — следующее);
   - битый links (raw ключ в FakeEtcd + parseError) → применения нет,
     journal содержит диагностику.

**Выход.** Логика доводки G реализована и юнит-доказана без docker; P4
provisioning'а будет переиспользовать её напрямую.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --no-build --filter FullyQualifiedName~MeshGovernance
```

**Связь со spec.** §3.3 (применение links), §3.4 (deny-механика), §3.5
(полная механика G), §3.8 (G = P4), §5.3, §8.1.5.

---

## Шаг 6. Интеграция процессов: P2/P4, D/E, надзор/ротатор, драйвер, лупы, DI

**Вход.** Шаги 2–5 завершены. Существующие процессы собирают env вручную
(`secrets.ToContainerEnv() + OWNS3_DATA_DIR`) в 4 местах:
ProvisioningProcess.EnsureNodesAsync, AddNodeProcess, NodeSupervisor.
RecreateAsync, SecretRotator; P4 — заглушка «t35 no-op»;
ClusterDriver.ListNodeObjectsAsync перебирает `node1..node9`.

**Действие (файлы/изменения).**

1. `src/S3Worker.Provisioning/Processes/ProvisioningProcess.cs`:
   - конструктор += `PkiStore pki, InstanceEnvComposer envComposer,
     MeshGovernanceProcess mesh` (передача через Program);
   - **P2** `EnsureNodesAsync`: перед созданием контейнера — ленивый выпуск
     `EnsureClusterPkiAsync` (ca+orchestrator) + `EnsureNodePkiAsync(<I>)`
     (первого использования; идемпотентно на повторных тиках); env —
     `envComposer.Compose(secrets, nodePki, ca, advertised)` (advertised =
     `http://{AdvertisedHost ?? addr.Host}:{addr.S3Port}`); отказ
     PKI-секции — Fail фазы `pki` (journal last_error);
   - **P2.5/P3**: без изменений (пустой список нод — пустые циклы);
   - **P4**: вместо no-op — `await mesh.TickAsync(snap, ct)` (deny-доведение
     + доводка состава по links; links пуст/0 инстансов — no-op внутри G);
     отказ P4 — transient (Fail фазы `mesh`, ретрай тиком) — но не должен
     блокировать P5 бессрочно: расхождение состава — не отказ
     provisioning'а; решение: P4-ошибка НЕ фейлит RunAsync — journal
     `waiting-mesh` и P5 продолжает (mesh додоводит Active-тиками G;
     endpoints/config не зависят от состава targets). Это соответствует
     arch/24 §5 A: P4 «та же механика, что G» — G-операции ретраятся
     тиками;
   - **путь 0 инстансов**: P1 — план пуст (portalloc-транзакция не
     создаётся: `wanted` пуст → ранний выход без клэйма), P2/P3 пустые,
     P4 no-op, P5 — `BuildEndpoints` пуст → ключ `endpoints` НЕ пишется
     (пропуск put при пустом составе), commit config `nodes=0`;
   - профиль секретов: прежняя permanent-проверка P1 сохраняется.
2. `src/S3Worker.Provisioning/Processes/AddNodeProcess.cs` (D):
   - mini-A фаза 2: PKI-выпуск узла (ca уже есть у живого кластера —
     `EnsureClusterPkiAsync` идемпотентен) + env через `envComposer`;
   - включение в mesh — G по links (комментарий; вызова в D нет — Active-G
     подхватит; arch/24 §5 D);
   - финал (ConvergeTopology) без изменений (nodes актуализируется).
3. `src/S3Worker.Provisioning/Processes/RemoveNodeProcess.cs` (E):
   - чистка ключей ноды: += `S3ProcessCommon.NodePkiKey` (имя
     освобождается; повторное создание — новый выпуск, §3.2);
   - += удаление имени из `denylist` кластера (read → remove → put txn по
     mod_revision; имени нет — no-op) — иначе deny-доведение потеряет
     publicKeyPem (§3.4);
   - гварды без изменений (сохранены по user-review §9.3).
4. `src/S3Worker.Provisioning/Processes/NodeSupervisor.cs` и
   `SecretRotator.cs`: `RecreateAsync`/rolling-пересоздание — env через
   `envComposer` (PKI-материал из etcd: `PkiStore.Ensure*` идемпотентен —
   «ключи переживают пересоздание», §3.6; тот же сертификат). Отказ
   PKI-чтения — как отказ профиля: пересоздание отложено (диагностика).
5. `src/S3Worker.Docker/Drivers/ClusterDriver.cs` + `src/Shared.Docker/
   Engine/{IDockerEngine,DockerEngine}.cs`:
   - `ListNodeObjectsAsync`: перечисление по префиксу `s3w-<C>-` + label
     `s3worker` (БЕЗ перебора `node<k>`): plain — `ListContainersAsync(
     prefix)` (label-точность: расширить `DockerContainer` полем Labels и
     фильтровать `s3worker==<C>`, ЛИБО добавить label-фильтр в
     ListContainersAsync — по меньшему diffs); тома — новый
     `IDockerEngine.ListVolumesAsync(namePrefix, ct)` (docker `GET
     /volumes` c name-фильтром) + `EnsureVolumeAsync` расширяется
     опциональным label (`s3worker=<C>`) для точной атрибуции; swarm —
     `ListServicesAsync(prefix)` + тот же volume-листинг;
   - причина: имена инстансов произвольные, перебор детерминированных
     имён невозможен (arch/24 §2); префикс один — `s3w-<C>-`, точность —
     label (значение = имя кластера);
   - затронутые consumers: DeprovisioningProcess X2 (сироты) — работает
     поверх нового перечисления без правок семантики.
6. `src/S3Worker.App/Loops/S3ClusterProcesses.cs`: Active-ветка — порядок
   C → D → E → **G** (вызов `mesh.TickAsync` после removeNode; F —
   заглушка t33 остаётся); ошибки G не прерывают ветку (RunClusterOpAsync
   per-процессно — по существующей дисциплине: supervisor failed → return).
7. `src/S3Worker.App/Program.cs`: DI — `PkiStore`,
   `InstanceEnvComposer`, `MeshGovernanceProcess` (+Meter-передача),
   прокидка в ProvisioningProcess/AddNode/Supervisor/SecretRotator.
8. Юнит-тесты процессов (обновить существующие + добавить):
   - `ProvisioningProcessTests`: P2 пишет `pki/ca`, `pki/orchestrator`,
     `nodes/<I>/pki` (FakeEtcd) и передаёт контейнеру полный env
     (FakeS3Driver защёлкивает env — assert `OWNS3_REPLICATION_*`);
     0-инстансовый provisioning — P1–P4 без docker-мутаций, endpoints НЕ
     пишется, config `nodes=0` без state; P4 → вызов mesh (фейк/real на
     FakeNodeClient);
     повторный provisioning — PKI материал тот же (идемпотентность);
   - `AddNodeProcessTests`: env с репликационным материалом; имя из
     декларации (не node<k>);
   - `RemoveNodeProcessTests`: del `nodes/<I>/pki`; имя убрано из
     denylist; гварды прежние;
   - `NodeSupervisorTests`/`SecretRotatorTests`: пересоздание — env через
     композер, сертификат тот же (FakeEtcd pki предзаполнен);
   - `ClusterDriverTests` (юнит, фейк-движок): ListNodeObjects по
     label/префиксу, volume-листинг; при правке Shared.Docker — сверка с
     существующими тестами `src/tests/Shared.Docker.UnitTests`.

**Выход.** Все процессы применяют PKI/граф/deny по arch/24 §5; env
собирается единой точкой; 0-инстансовый путь работает; драйвер перечисляет
объекты по label; G встроен в Active-ветку; юниты процессов зелёные.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Debug --no-build
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Docker.UnitTests -c Debug --no-build
```

Критерий: ПОЛНЫЙ юнит-набор S3Worker зелёный (включая регрессии
классификатора/health — App-тесты не задет сборкой DI).

**Связь со spec.** §3.5 (G в ветке), §3.6 (env при создании/пересоздании),
§3.8 (A/C/D/E), §5.3–5.5, §8.1.3–8.1.5 (интеграция), §8.4 (регрессии).

---

## Шаг 7. Интеграционные docker-тесты S3 (S3ClusterFixture)

**Вход.** Шаг 6 завершён. Существующие: `src/tests/S3Worker.
IntegrationTests/S3/{ProvisioningTests,SupervisionTests,S3ClusterFixture,
S3OwnS3Image,FreePortWindow}.cs` — поднимают реальный ownS3-контейнер.

**Действие.**

1. `S3ClusterFixture`/`ProvisioningTests`/`SupervisionTests`: перевод
   заявок на формат `instances[]` с именами (замена `nodes`+`placements`);
   ассерты: PKI-ключи появляются в etcd (`pki/ca`, `pki/orchestrator`,
   `nodes/<I>/pki`), контейнер получает `OWNS3_REPLICATION_*` (docker
   inspect env), state нод RUNNING, endpoints формируются;
2. Проверка advertised-endpoint из portalloc в env
   (`OWNS3_REPLICATION_ADVERTISED_ENDPOINT`);
3. Соблюдение канонов: динамические порты (FreePortWindow), зачистка
   после серии.

**Выход.** Docker-интеграционный слой подтверждает PKI+env на реальном
образе ownS3.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests -c Debug --no-build --filter "FullyQualifiedName~S3Worker.IntegrationTests.S3"
```

(env `PGW_TEST_DOCKER=1` — по гейту фикстуры; после серии — зачистка
`s3w-*` контейнеров/томов/сетей.)

**Связь со spec.** §5.6 (тесты), §8.4 (регрессии сценариев provision/
supervise), §3.6.

---

## Шаг 8. Docker-E2E (Release): сценарии mesh-управления

**Вход.** Шаги 4–7 завершены. Фикстура `src/tests/S3Worker.IntegrationTests/
E2e/S3E2eEnvironment.cs` (guid-изоляция, свежий Release-образ
`s3worker:e2e-<runId>` publish-на-хосте, телеметрия, teardown/ассерт
чистоты) и существующие сценарии (Lifecycle/Scale/Supervision/Rotation/
Takeover/Api) — формат create обновляется на `instances[]`.

**Действие.**

1. Обновление существующих E2E-сценариев: тело create →
   `{name, platform, secretProfile, instances:[{name, placement,
   resources}], …}`; ожидания по именам инстансов (замена `node1/node2`
   литералов); прочая механика без изменений.
2. Новый `src/tests/S3Worker.IntegrationTests/E2e/S3E2eMeshTests.cs`
   (сценарии §8.3; class-изоляция = своё окружение `S3E2eEnvironment.
   StartAsync(<slug>)`; S3-клиент — AWSSDK root-креды профиля по
   host-портам из portalloc; все ожидания — поллинг ≤ 30 с шагами, бюджет
   сценария в `[PHASE]`-журнале):
   - **PKI-провижининг + репликация** (§8.3.1): create 2 инстанса
     (`east1`,`east2`) + `PUT links {east1,east2}` → обе ноды RUNNING;
     docker inspect: `OWNS3_REPLICATION_{CERT,PRIVATE_KEY,TRUST_CA,
     ADVERTISED_ENDPOINT}` заданы; status обеих нод: сосед числится
     (join прошёл); PUT объекта в east1 → объект появляется на east2
     (поллинг в бюджете);
   - **переживание переезда** (§8.3.2): `docker rm -f` контейнера east1 →
     надзор пересоздал (inspect нового контейнера: сертификат ТОТ ЖЕ —
     env `OWNS3_REPLICATION_CERT` идентичен снятому до сноса); связь
     жива; объекты не потеряны;
   - **deny/allow** (§8.3.3): `POST …/nodes/east2/deny` → у east1 target
     east2 снесен (status), denylist доведён обеим (admin GET
     denylist — SigV4 root или через эффект: join-проба невозможна);
     повторный join невозможен (G не восстанавливает связь; ручной
     admin-join → 403 — при проверке через G: связей нет в течение
     нескольких тиков); `allow` → связь восстановлена доводкой G;
   - **частичный граф** (§8.3.4): 3 инстанса, links только
     `{a,b},{a,c}` → по статусам: у b нет c и наоборот; связи a-b, a-c
     есть; это не дефект;
   - **0 инстансов** (§8.3.5): create `instances:[]` → config без state,
     `nodes=0`, ключа endpoints НЕТ; `POST …/nodes` с именем → инстанс
     поднимается; `PUT links` → join работает (объект реплицируется);
   - каждый сценарий: MarkFailed-дисциплина, teardown + ассерт чистоты
     guid-контура (контейнеры/тома/сеть/ключи etcd префикса кластера).
3. E2E-запуски по канонам: `PGW_TEST_DOCKER=1`, Release-сборка
   фикстурой (publish на хосте — секунды; `[PHASE]`-метки фаз > 60 с).

**Выход.** Сквозные сценарии mesh-управления зелёные на свежем Release;
изоляция/телеметрия по канонам.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/S3Worker.IntegrationTests -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.E2e"
```

Критерий: все E2E-сценарии (новые + обновлённые) зелёные; после прогона —
ноль остаточных `s3w-*` объектов и `s3w-en-*` сетей.

**Связь со spec.** §8.3 (все шесть пунктов), §5.6 фаза 6, каноны
AGENTS.md/docs/e2e-*.

---

## Шаг 9. Регрессии: полные серии и зачистка

**Вход.** Шаги 1–8 завершены.

**Действие.**

1. Полная серия юнитов решения: S3Worker + OwnS3 + Shared.* (затронут
   Shared.Docker);
2. Полная серия интеграционных S3Worker (Api + S3 + Etcd);
3. Полная серия E2E S3Worker (Release, свежая сборка);
4. После КАЖДОЙ серии — финальная строка прогона, зачистка остатков
   (контейнеры/тома/сети), проверка `docker network ls | grep -c
   's3w-en-\|kfw-net'` → 0 при нуле контейнеров;
5. Сверка: `dotnet build` без предупреждений (`TreatWarningsAsErrors`),
   Release-конфигурация тоже собирается.

**Выход.** Ветка функционально целостна и чиста; готовность к мерж-гейту.

**Проверка.**

```
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.UnitTests -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/OwnS3.UnitTests src/tests/Shared.Docker.UnitTests -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/S3Worker.IntegrationTests -c Release
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/S3Worker.IntegrationTests -c Release --filter "FullyQualifiedName~S3Worker.IntegrationTests.E2e"
```

**Связь со spec.** §8.4 (регрессии, сборка без предупреждений).

---

## Шаг 10. Мерж-гейт (после code-review, перед Фазой 8)

**Вход.** Execute + code-review пройдены.

**Действие.** Мерж-гейт задачи (трогает воркеров):

1. E2E-кейс-маркер PgWorker на свежем Release:

```
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```

2. Полная S3Worker E2E-серия (шаг 8) на свежем Release;
3. Полный юнит+интеграционный наборы S3Worker/OwnS3 (шаг 9);
4. Зачистка сетей/контейнеров после серий;Roadmap-чистка: тег
   `t40-s3-cluster-governance` удалить из `arch/roadmap/s3.md` тем же
   мерж-коммитом (включая `←`-зависимости других пунктов, если ссылаются).

**Выход.** Готовность к мержу в main (по явному приказу пользователя).

**Проверка.** Все серии зелёные; `docker network ls`/`docker ps -a` чисты
от остатков серий.

**Связь со spec.** §8 (критерии приёмки целиком), AGENTS.md (мерж-гейт
E2E задач воркеров; roadmap-правило).

---

## Границы (НЕ делается в этой задаче)

- UI панели — t34 (панель — читатель; управление ключами скрыто бэком).
- Зонд синхронности — t33 (G ключей зонда не трогает; sync-probe —
  заглушка).
- Клиентская интеграция пула (`endpoints`-дискавери) — t36.
- Ротация ключей репликации/CA, CRL, enterprise-IAM/mTLS — roadmap;
  отзыв — deny-list.
- Односторонние потоки репликации — не выражаются механикой ownS3
  (граф парный).
- Миграция старых форматов etcd НЕ выполняется: `nodes/node<k>/*`
  читаются как имена `node<k>` (валидные произвольные); сид-демо
  обновляется.
- PgWorker/KafkaWorker/ValkeyWorker/AdminPanel-код не трогается (кроме
  Shared.Docker-расширения листинга volume/label — минимальное, с
  регрессиями Shared.Docker.UnitTests).
