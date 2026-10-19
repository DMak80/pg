# t32-etcdworker — план реализации (Фаза 3 dev-flow)

**Цель:** выделенный воркер EtcdWorker — хозяин etcd-контура установки:
доводка состава managed-узлов (learner+promote, авто-замена), DR-слепки etcd в
S3 (bucket-константа `etcd-backups`), изъятие зоны бэкапа etcd из PgWorker,
панель, deploy/стенд.

**Архитектура:** каркас — копия паттернов S3Worker (Program/DI/циклы/mTLS-API/
healthz/watchdog); координация — Shared.Etcd (ClaimStore/PortAllocLock) на
префиксе `/etcdworker`; перенос механики t08 (`EtcdSnapshotSink` и пр.) из
PgWorker.Backups; адресация контура — только имена узлов в общей сети
`Docker:Network` (Plain — один хост, Swarm — раснос сервисов); детект
managed-членов — peer-URL-паттерн `/etcdw-node-<k>:`.

**Стек:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), центральные
версии пакетов (`Directory.Packages.props`), AWSSDK.S3 (как PgWorker.Backups),
React/Mantine (панель), docker (testcontainers в тестах).

**Spec:** `docs/superpowers/2026-10-19-t32-etcdworker/spec.md` — план аргументируется
от spec; исполнители читают spec+plan. Канон — `arch/25-etcdworker.md`.

## Глобальные ограничения

- Пути ниже — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2`.
- Комментарии/документация — по-русски; идентификаторы — на английском; тесты —
  с AAA-комментариями.
- Константа слепкового bucket: `Shared.Core` → `EtcdSnapshotBucket.Name == "etcd-backups"`
  (spec §3.1, решение 7). Нигде литералом.
- Домен ключей: `/etcd/` + `/etcdworker/` (spec §3.3); префикс координации —
  строка `"/etcdworker"`; порт API 8087/8088 (deploy); Meter `"EtcdWorker"`.
- Порты docker-контейнеров в тестах — только динамические
  (`WithPortBinding(..., assignRandomHostPort: true)` + `GetMappedPublicPort`);
  boot-бюджеты ≤ 100 с; каждый сценарий полностью чистит за собой (контейнеры,
  тома, сеть сценария, ключи своего префикса; ассерт чистоты) — каноны
  AGENTS.md и docs/e2e-isolation.md.
- Любые команды dotnet test / dotnet build — запускать в фоне
  (`run_in_background: true`), вывод целиком в лог-файл (`> файл 2>&1`),
  читать лог из файла; между сериями — зачистка контейнеров/сетей.
- Сборка решения: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c
  Release --nologo` — отдельной командой ДО тестов; тесты — `--no-build`.
- Мерж-гейт: `PGW_TEST_DOCKER=1 ... --filter FullyQualifiedName~Scale_AddEmptyShard`
  минимум; E2ePrepare ДО docker-E2E PgWorker (AGENTS.md).
- Коммиты — в feature-ветке свободно, после каждого шага (зелёная сборка).

## Review Focus (вводные, которые spec не гоняет тестами — закреплены тестами шагов)

1. Битый/чужой JSON в статус-ключе и config-ключах не роняет читателя —
   толерантные парсеры (шаги 7, 15: Parse битого → null/parseError).
2. Коллизия имён слепков в одну секунду — put поверх (новее побеждает) —
   сохраняется из t08 (шаг 8: юнит Serialize/метки).
3. `MemberRemoveAsync`/`MemberPromoteAsync` на чужом/несуществующем memberId —
   Result.Failed без исключения наружу, transient-ретраи тиком (шаг 1: юнит
   ошибки; шаг 12: гварды по member list до вызова).
4. Пустой `/s3/clusters/` и etcd-отказ в резолве цели — FAILED-статус пишется
   всегда (даже когда etcd-запись сама падает — алерт stale заметит), снятие
   слепка не роняется (шаги 8–9).
5. Watchdog не сносит лидера в длинном S3-put — пульс конечного вызова +
   пер-попыточный таймаут клиента (шаги 8–9: пульс перенесён 1:1).

---

## Фаза 1 — Shared.Etcd member-API (spec §3.2, §4.1)

### Шаг 1. MemberAdd/MemberRemove/MemberPromote в EtcdGateway

**Вход:** ветка чистая; `IEtcdGateway`/`EtcdGateway` существуют
(`src/Shared.Etcd/Client/`), `MemberListAsync` уже есть; `EtcdMember` — без
`IsLearner`.

**Действие (кодер):** расширяет интерфейс и реализацию тремя методами member-API
(тела/DTO по фактическим proto-именам etcd 3.5 grpc-gateway, как существующие
DTO файла); `EtcdMember` получает `IsLearner` (парсится из `"isLearner"` ответа
member list; отсутствие поля = false). Ошибки не-2xx — уже общий путь
`PostAsync` → `EtcdHttpException` → `Result.Failed` (не меняется). Пишет тесты
на JSON-маппинг по действующему паттерну `EtcdGatewayTests` (FakeHandler).

**Выход:** `MemberAddAsync`/`MemberRemoveAsync`/`MemberPromoteAsync` в
`IEtcdGateway`+`EtcdGateway`; `EtcdMember.IsLearner`; зелёные юниты.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/Shared.Etcd.UnitTests -c Release --no-build --filter "FullyQualifiedName~EtcdGatewayTests"` (после
сборки) — зелёные; сборка `src/Shared.Etcd` без warnings.

**Связь со spec:** §3.2 (тела/ошибки), §7.1 (юниты member-API), §8-риск 2
(механика имени — инвариант паттерна).

**Подпункты (слабой модели):**

1.1. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/Shared.Etcd/Client/EtcdModels.cs`
замени запись `EtcdMember` на:
```csharp
// Член etcd-кластера из /v3/cluster/member/list (isLeader в DTO вычисляет API t04 по EtcdStatus).
// IsLearner — из поля "isLearner" ответа (3.5); отсутствие поля = false (voting-член).
public sealed record EtcdMember(
    ulong Id,
    string? Name,
    IReadOnlyList<string> PeerUrls,
    IReadOnlyList<string> ClientUrls,
    bool IsLearner = false);
```

1.2. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/Shared.Etcd/Client/IEtcdGateway.cs`
добавь после комментария `// POST /v3/cluster/member/list.` (метод
`MemberListAsync`) три новых метода интерфейса:
```csharp
    // POST /v3/cluster/member/add — член по peer-URL (isLearner=true: безголосый
    // learner до promote); имя члена etcd выводит из hostname peer-URL.
    Task<Result<EtcdMember>> MemberAddAsync(string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct);

    // POST /v3/cluster/member/remove — вывод члена (ID — decimal-строка protojson).
    Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct);

    // POST /v3/cluster/member/promote — перевод learner в voting-член (отказ
    // not-in-sync — обычная ошибка HTTP → Result.Failed, ретраи вызывающим).
    Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct);
```

1.3. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/Shared.Etcd/Client/EtcdGateway.cs`:
а) в методе `MemberListAsync` замени маппинг на передачу learner-флага:
```csharp
        return result.Map(r => (IReadOnlyList<EtcdMember>)(r.Members ?? [])
            .Select(m => new EtcdMember(m.Id, m.Name, m.PeerUrls ?? [], m.ClientUrls ?? [], m.IsLearner))
            .ToList());
```
б) добавь три метода реализации сразу после `MemberListAsync`:
```csharp
    public async Task<Result<EtcdMember>> MemberAddAsync(
        string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
    {
        var body = new { peerURLs = peerUrls, isLearner };
        var result = await Result<MemberAddResponse>.FromAsync(
            async () => await PostAsync<MemberAddResponse>(endpoint, "/v3/cluster/member/add", body, ct));
        return result.Bind(r => (r.Members ?? []).FirstOrDefault() is { } added
            ? Result<EtcdMember>.Success(new EtcdMember(
                added.Id, added.Name, added.PeerUrls ?? [], added.ClientUrls ?? [], added.IsLearner))
            : Result<EtcdMember>.Failed(new EtcdHttpException(
                endpoint, 500, "member/add response has no added member")));
    }

    public async Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
    {
        var body = new { ID = memberId };
        return await Result.FromAsync(
            async () => await PostAsync<RemoveMemberResponse>(endpoint, "/v3/cluster/member/remove", body, ct));
    }

    public async Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
    {
        var body = new { ID = memberId };
        return await Result.FromAsync(
            async () => await PostAsync<PromoteMemberResponse>(endpoint, "/v3/cluster/member/promote", body, ct));
    }
```
в) в приватный класс `MemberDto` добавь свойство (после `ClientUrls`):
```csharp
        [JsonPropertyName("isLearner")]
        public bool IsLearner { get; set; }
```
г) после класса `MemberListResponse` добавь три DTO ответов:
```csharp
    private sealed class MemberAddResponse
    {
        [JsonPropertyName("members")]
        public List<MemberDto>? Members { get; set; }
    }

    private sealed class RemoveMemberResponse
    {
        [JsonPropertyName("header")]
        public object? Header { get; set; }
    }

    private sealed class PromoteMemberResponse
    {
        [JsonPropertyName("header")]
        public object? Header { get; set; }
    }
```

1.4. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/Shared.Etcd.UnitTests/EtcdGatewayTests.cs`
добавь в конец класса `EtcdGatewayTests` четыре теста (паттерн файла:
FakeHandler + Json + AAA-комментарии по-русски; peerURLs в
/v3/cluster/member/* — plain URL-строки, НЕ base64 — base64 только kv-операции,
как фиксирует существующий `MemberList_ParsesUrls`):
- `MemberAdd_BodyHasPeerUrlsAndIsLearner_ResponseMapped`: FakeHandler отвечает
  `{"header":{},"members":[{"ID":"123","name":"","peerURLs":["http://etcdw-node-1:2380"],"clientURLs":[],"isLearner":true}]}`
  (plain URL — как в member list). Act: `MemberAddAsync("http://etcd:2379", ["http://etcdw-node-1:2380"], true, None)`.
  Assert: запрос уходит на `/v3/cluster/member/add`; тело содержит
  `"peerURLs":["http://etcdw-node-1:2380"]` и `"isLearner":true`;
  результат успешен, `Value.Id == 123`, `Value.IsLearner` true, `Value.PeerUrls`
  содержит `"http://etcdw-node-1:2380"`.
- `MemberAdd_EmptyMembers_Failed`: ответ `{"header":{}}` → результат Failed,
  исключение наружу не летит.
- `MemberRemove_BodyHasDecimalId`: ответ `{"header":{}}`; Act
  `MemberRemoveAsync(..., 42, ...)`; Assert: тело содержит `"ID":42`, путь
  `/v3/cluster/member/remove`, успех.
- `MemberRemove_HttpError_Failed`: responder возвращает статус 400 с телом
  `{"error":"..."}` → Result.Failed (создай локальный хелпер `Http(int code, string body)`
  по образцу `Json`, если его нет).
Плюс `MemberPromote_BodyHasDecimalId` — симметрично remove (путь
`/v3/cluster/member/promote`).

### Шаг 2. Интеграционная сверка member-API на etcd 3.5

**Вход:** шаг 1 слит; тест-проекта EtcdWorker.IntegrationTests ещё нет.

**Действие (кодер):** создаёт тест-проект `src/tests/EtcdWorker.IntegrationTests`
(csproj по образцу `src/tests/S3Worker.IntegrationTests/S3Worker.IntegrationTests.csproj`:
ссылки на Shared.Etcd, xunit, FluentAssertions, Testcontainers DockerTrait-паттерн)
и фикстуру окружения `Docker/EtcdWorkerEnvironment.cs` — по образцу
`S3EtcdFixture`/`S3ClusterFixture`: testcontainers-сеть с guid-именем
(`etcdw-<guid>-net`), один etcd-контейнер `quay.io/coreos/etcd:v3.5.21`
(команда `etcd --name seed --data-dir /etcd-data --listen-client-urls
http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
--listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls
http://localhost:2380 --initial-cluster seed=http://localhost:2380
--initial-cluster-state new`, client-порт `WithPortBinding(2379, true)`),
помощники `Endpoint` (host:динамический порт), `Gateway`, `PutAsync/GetAsync`,
полный teardown (контейнер+сеть) при любом исходе + ассерт чистоты. Затем
пишет `Etcd/MemberApiBehaviorTests.cs` (сильная модель — по спецификации ниже).

**Выход:** фиксация фактического поведения etcd 3.5 по member-API; фикстура
`EtcdWorkerEnvironment` — переиспользуемая база интеграций фаз 4–5.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/EtcdWorker.IntegrationTests -c Release --no-build` — зелёные; после серии — `docker ps -a | grep etcdw-` пусто, `docker network ls | grep etcdw-` пусто.

**Связь со spec:** §8-риск 2 («сверка фактического поведения etcd 3.5»), §7.2
(самоочистка/динамические порты), база фикстур §7.2 «Состав»/«Слепки».

**Спецификация теста MemberApiBehaviorTests (пишет сильная модель):** кейсы
фиксируют фактическое поведение etcd 3.5, инвариант детекта — peer-URL-паттерн:
- `MemberAdd_Learner_AppearsUnstartedWithEmptyName`: add
  `http://etcdw-node-1:2380` isLearner=true → member list содержит члена с
  этим peerURL и `IsLearner=true`, `Name == ""` (unstarted). Если фактическое
  поведение отличается (например, name ненулевой) — зафиксировать факт в
  ассерте; инвариант (peerURL-паттерн) не зависит от name.
- `MemberRemove_Unstarted_Succeeds`: remove добавленного learner → member list
  больше его не содержит.
- `MemberPromote_UnstartedLearner_Fails`: promote до старта узла — etcd
  отвечает ошибкой → `Result.Failed` (не исключение).
- `MemberList_ManagedPatternParse`: после add с peerURL
  `http://etcdw-node-2:2380` — строка peerURL содержит `/etcdw-node-2:` (паттерн
  детекта шага 5).

Подпунктов нет (тесты и фикстура требуют знания паттернов проекта).

---

## Фаза 2 — Каркас EtcdWorker (spec §3.1/§3.4 API/§3.6, §4.2)

### Шаг 3. Сборки, Options, StartupChecks

**Вход:** шаги 1–2 слиты.

**Действие (кодер):** создаёт 5 сборок (csproj по образцам S3Worker:
App — Worker SDK + FrameworkReference; Core/Etcd/Docker/Provisioning —
библиотеки с ProjectReference-цепочкой App→{Core,Etcd,Docker,Provisioning},
Provisioning→{Core,Etcd,Docker}, Etcd→Core, Docker→Core; все → Shared.Core;
Docker→Shared.Docker; Etcd→Shared.Etcd; Core→Shared.Tls), включает в
`src/PgWorker.slnx` (папка `/etcd/` по образцу `/s3/`), создаёт тест-проект
`src/tests/EtcdWorker.UnitTests`. Пишет `EtcdWorkerOptions` (дерево §3.6 —
один в один, включая `Docker.Network` со значением по умолчанию `etcdw-net`,
`Images.Etcd` = `quay.io/coreos/etcd:v3.5.21`, `AdvertisedHost`), appsettings.json,
`EtcdStartupChecks` (чистые предикаты + Plain-гварды) и юниты чеков.

**Выход:** решение собирается; fail-fast-валидации конфигурации EtcdWorker
покрыты юнитами.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t32-build.log 2>&1` — 0 errors/warnings; юниты
`EtcdWorker.UnitTests` (фильтр `StartupChecks`) зелёные.

**Связь со spec:** §3.1 (сборки/константа Meter), §3.6 (дерево конфигурации,
fail-fast), решение 18 (Plain=1 хост + Network).

**Подпункты (слабой модели):**

3.1. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/EtcdWorker.Core.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">
    <ItemGroup>
        <ProjectReference Include="..\Shared.Core\Shared.Core.csproj"/>
        <ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <Using Include="Shared.Core"/>
    </ItemGroup>
</Project>
```

3.2. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Etcd/EtcdWorker.Etcd.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">
    <ItemGroup>
        <ProjectReference Include="..\EtcdWorker.Core\EtcdWorker.Core.csproj"/>
        <ProjectReference Include="..\Shared.Etcd\Shared.Etcd.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <Using Include="Shared.Core"/>
        <Using Include="Shared.Etcd.Coordination"/>
    </ItemGroup>
</Project>
```

3.3. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Docker/EtcdWorker.Docker.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">
    <ItemGroup>
        <ProjectReference Include="..\EtcdWorker.Core\EtcdWorker.Core.csproj"/>
        <ProjectReference Include="..\Shared.Docker\Shared.Docker.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <Using Include="Shared.Core"/>
        <Using Include="Shared.Docker"/>
    </ItemGroup>
</Project>
```

3.4. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Provisioning/EtcdWorker.Provisioning.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">
    <ItemGroup>
        <ProjectReference Include="..\EtcdWorker.Core\EtcdWorker.Core.csproj"/>
        <ProjectReference Include="..\EtcdWorker.Etcd\EtcdWorker.Etcd.csproj"/>
        <ProjectReference Include="..\EtcdWorker.Docker\EtcdWorker.Docker.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <Using Include="Shared.Core"/>
        <Using Include="Shared.Etcd.Coordination"/>
    </ItemGroup>
    <ItemGroup>
        <InternalsVisibleTo Include="EtcdWorker.UnitTests"/>
        <InternalsVisibleTo Include="EtcdWorker.IntegrationTests"/>
    </ItemGroup>
</Project>
```

3.5. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/EtcdWorker.App.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk.Worker">
    <ItemGroup>
        <InternalsVisibleTo Include="EtcdWorker.UnitTests"/>
        <InternalsVisibleTo Include="EtcdWorker.IntegrationTests"/>
    </ItemGroup>
    <ItemGroup>
        <FrameworkReference Include="Microsoft.AspNetCore.App"/>
    </ItemGroup>
    <ItemGroup>
        <ProjectReference Include="..\EtcdWorker.Core\EtcdWorker.Core.csproj"/>
        <ProjectReference Include="..\EtcdWorker.Etcd\EtcdWorker.Etcd.csproj"/>
        <ProjectReference Include="..\EtcdWorker.Docker\EtcdWorker.Docker.csproj"/>
        <ProjectReference Include="..\EtcdWorker.Provisioning\EtcdWorker.Provisioning.csproj"/>
        <ProjectReference Include="..\Shared.Metrics\Shared.Metrics.csproj"/>
        <ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>
    </ItemGroup>
    <ItemGroup>
        <Using Include="Shared.Core"/>
        <Using Include="Shared.Docker"/>
        <Using Include="Shared.Core.Writing"/>
        <Using Include="Shared.Etcd.Maintenance"/>
        <Using Include="Shared.Etcd.Coordination"/>
    </ItemGroup>
</Project>
```

3.6. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.slnx`
вставь после закрывающего `</Folder>` папки `/s3/`:
```xml
    <Folder Name="/etcdw/">
        <Project Path="EtcdWorker.Core/EtcdWorker.Core.csproj" />
        <Project Path="EtcdWorker.Etcd/EtcdWorker.Etcd.csproj" />
        <Project Path="EtcdWorker.Docker/EtcdWorker.Docker.csproj" />
        <Project Path="EtcdWorker.Provisioning/EtcdWorker.Provisioning.csproj" />
        <Project Path="EtcdWorker.App/EtcdWorker.App.csproj" />
    </Folder>
```
и в папку `/tests/` после строки S3Worker.IntegrationTests:
```xml
        <Project Path="tests/EtcdWorker.UnitTests/EtcdWorker.UnitTests.csproj" />
        <Project Path="tests/EtcdWorker.IntegrationTests/EtcdWorker.IntegrationTests.csproj" />
```

3.7. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/EtcdWorker.UnitTests.csproj`
копией `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/S3Worker.UnitTests/S3Worker.UnitTests.csproj`
с заменой всех вхождений `S3Worker.UnitTests` → `EtcdWorker.UnitTests` и
ProjectReference `..\..\src\S3Worker.*` → `..\..\src\EtcdWorker.*` (те же пять
сборок EtcdWorker, что в 3.5, плюс Shared-ссылки из образца).

3.8. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Options.cs`
с namespace `EtcdWorker.App` — полная копия структуры
`src/S3Worker.App/Options.cs` со следующими отличиями: класс
`EtcdWorkerOptions` (без `Bucket`, без `Parallelism`); секции:
```csharp
public sealed class EtcdWorkerOptions
{
    public EtcdOptions Etcd { get; set; } = new();
    public DockerOptions Docker { get; set; } = new();
    public LoopsOptions Loops { get; set; } = new();
    public ThresholdsOptions Thresholds { get; set; } = new();
    public SnapshotOptions Snapshots { get; set; } = new();
    public ApiOptions Api { get; set; } = new();
}
```
`EtcdOptions`, `ApiOptions`/`TlsOptions` (без `EnableSeedEndpoint`),
`DockerHostOptions`, `PortRangeOptions`, `LoopsOptions` (без
`SnapshotIntervalMin` — интервал слепков живёт в Snapshots), `ThresholdsOptions`
(только `NodeBootSec=100`, `NodeDeadSec=300`, `HealthIntervalSec=15`) — как в
образце. Отличия от S3Worker:
```csharp
/// <summary>docker: режим (Plain|Swarm), сеть контура, порты, образы (arch/25 §2/§8).</summary>
public sealed class DockerOptions
{
    /// <summary>Plain | Swarm (регистронезависимо).</summary>
    public string Mode { get; set; } = "Plain";

    /// <summary>Таблица хостов plain-режима: Plain — РОВНО ОДИН хост (fail-fast
    /// иначе, arch/25 §2: мультихост — Swarm).</summary>
    public DockerHostOptions[] Hosts { get; set; } = [];

    /// <summary>Endpoint manager-узла swarm.</summary>
    public string? SwarmManager { get; set; }

    /// <summary>Общая сеть контура etcd (arch/25 §2): Plain — ensure-bridge
    /// воркером; Swarm — overlay воркера; стенд/E2E — сеть контура/сценария.</summary>
    public string Network { get; set; } = "etcdw-net";

    /// <summary>Диапазон КЛИЕНТСКИХ host-портов managed-узлов (peer — имена
    /// сети, host-портов не имеет).</summary>
    public PortRangeOptions PortRange { get; set; } = new();

    /// <summary>Образ etcd-узла (внешний, зеркалирован).</summary>
    public DockerImagesOptions Images { get; set; } = new();

    public Shared.Docker.DockerTlsOptions? Tls { get; set; }

    public Shared.Docker.SshTunnelOptions? Ssh { get; set; }

    /// <summary>Advertised-хост КЛИЕНТСКОЙ грани (только client-advertise,
    /// arch/25 §2): null → имя docker-хоста размещения.</summary>
    public string? AdvertisedHost { get; set; }
}

public sealed class DockerImagesOptions
{
    public string Etcd { get; set; } = "quay.io/coreos/etcd:v3.5.21";
}

/// <summary>DR-слепки (arch/25 §4/§8): расписание, ретенция, выгрузка;
/// креды/endpoint'ы/bucket НЕ здесь — цель резолвится из /s3/clusters/.</summary>
public sealed class SnapshotOptions
{
    public string Dir { get; set; } = "/snapshots";
    public int RetentionFiles { get; set; } = 10;
    public int IntervalMin { get; set; } = 360;
    public int MinIntervalSec { get; set; } = 300;
    public int MaintenanceIntervalMin { get; set; } = 60;
    public int RetentionObjects { get; set; } = 28;
    public int RetryIntervalSec { get; set; } = 300;
    public int TimeoutSec { get; set; } = 30;

    /// <summary>CA-bundle https S3-endpoint цели слепков (опц.).</summary>
    public SnapshotS3Options S3 { get; set; } = new();
}

public sealed class SnapshotS3Options
{
    public string? CaBundlePath { get; set; }
}
```
Комментарии классов — по-русски, в стиле образца.

3.9. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/EtcdStartupChecks.cs`
(namespace `EtcdWorker.App`, `internal static class EtcdStartupChecks`) — чистые
предикаты `EtcdWorkerOptions → bool` (образец `S3StartupChecks`):
```csharp
    // etcd-endpoints: без них воркер бессмысленен.
    public static bool EtcdEndpoints(EtcdWorkerOptions o) => o.Etcd.Endpoints is { Length: > 0 };

    // AdvertiseUrl: непустой; https:// (mTLS-only) либо AllowInsecureHttp (WAF-тесты).
    public static bool AdvertiseUrl(EtcdWorkerOptions o)
        => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl)
           && (o.Api.Tls.AllowInsecureHttp
               || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal));

    // Docker-топология (arch/25 §2): Plain → РОВНО один хост (мультихост — Swarm)
    // + непустая сеть контура; Swarm → SwarmManager.
    public static bool DockerTopology(EtcdWorkerOptions o)
    {
        if (string.Equals(o.Docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase))
            return !string.IsNullOrWhiteSpace(o.Docker.SwarmManager);
        var hosts = o.Docker.Hosts;
        return hosts.Length == 1
               && !string.IsNullOrWhiteSpace(hosts[0].Name)
               && !string.IsNullOrWhiteSpace(hosts[0].Endpoint)
               && !string.IsNullOrWhiteSpace(o.Docker.Network);
    }

    // Пороги: положительные; NodeBootSec ≤ 100.
    public static bool Thresholds(EtcdWorkerOptions o)
        => o.Thresholds.NodeBootSec > 0 && o.Thresholds.NodeBootSec <= 100
           && o.Thresholds.NodeDeadSec > 0 && o.Thresholds.HealthIntervalSec > 0;

    // Диапазон клиентских портов: 0 < From < To ≤ 65535.
    public static bool PortRange(EtcdWorkerOptions o)
        => o.Docker.PortRange.From > 0
           && o.Docker.PortRange.From < o.Docker.PortRange.To
           && o.Docker.PortRange.To <= 65535;

    // Интервалы слепков (arch/25 §8): положительные; ретенция ≥ 1.
    public static bool Snapshots(EtcdWorkerOptions o)
        => o.Snapshots.IntervalMin > 0 && o.Snapshots.MinIntervalSec > 0
           && o.Snapshots.RetentionFiles >= 1 && o.Snapshots.RetentionObjects >= 1
           && o.Snapshots.RetryIntervalSec > 0 && o.Snapshots.TimeoutSec > 0
           && o.Snapshots.MaintenanceIntervalMin > 0;
```

3.10. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/appsettings.json`
копией `src/S3Worker.App/appsettings.json` с заменой корневой секции `"S3Worker"`
на `"EtcdWorker"` и деревом:
```json
{
  "EtcdWorker": {
    "Etcd": { "Endpoints": [] },
    "Docker": {
      "Mode": "Plain",
      "Hosts": [],
      "Network": "etcdw-net",
      "PortRange": { "From": 18100, "To": 18199 },
      "Images": { "Etcd": "quay.io/coreos/etcd:v3.5.21" },
      "AdvertisedHost": null
    },
    "Snapshots": {
      "Dir": "/snapshots", "RetentionFiles": 10, "IntervalMin": 360,
      "MinIntervalSec": 300, "MaintenanceIntervalMin": 60,
      "RetentionObjects": 28, "RetryIntervalSec": 300, "TimeoutSec": 30,
      "S3": { "CaBundlePath": null }
    },
    "Thresholds": { "NodeBootSec": 100, "NodeDeadSec": 300, "HealthIntervalSec": 15 },
    "Loops": { "ScanIntervalSec": 5, "KeepaliveSec": 5, "ErrorDelayMs": 2000,
               "Watchdog": { "Enabled": true, "Multiplier": 2, "CheckIntervalSec": 15 } },
    "Api": { "AdvertiseUrl": "", "Tls": {} },
    "Metrics": { "Enabled": true }
  },
  "Logging": { "LogLevel": { "Default": "Information" } }
}
```
(секцию Metrics возьми такой, какая реально нужна `AddAppMetrics` в образце —
сверись с appsettings.json S3Worker; не выдумывай полей).

3.11. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/App/StartupChecksTests.cs`
(xunit, FluentAssertions, AAA-комментарии по-русски): для каждого предиката
`EtcdStartupChecks` — кейс «валидная конфигурация → true» и негативные кейсы
(пустые endpoints; AdvertiseUrl http без AllowInsecureHttp; Plain с 0 и 2
хостами; Plain без Network; Swarm без SwarmManager; NodeBootSec=101; PortRange
From≥To; RetentionObjects=0; IntervalMin=0). Собери валидный объект
`EtcdWorkerOptions` локальным helper-методом `Valid()`.

### Шаг 4. Program-каркас: DI, mTLS, KeepaliveLoop, healthz, watchdog

**Вход:** шаг 3 слит.

**Действие (кодер):** копирует из S3Worker.App (с заменой namespace/префиксов
S3W_→ETCDW_, S3Worker→EtcdWorker, "s3worker"→"etcdworker", "/s3worker"→
"/etcdworker", Meter "EtcdWorker"): `EtcdConnectCallback.cs`, `DockerEnvBindings.cs`
(env-имена `ETCDW_DOCKER_TLS_*`/`ETCDW_DOCKER_SSH_*`), `Api/TlsEndpoints.cs`
(env `ETCDW_API_TLS_*`), `Api/WorkerApiCertReader.cs` (worker-ключ
`etcdworker`), `HealthState.cs`, `LoopsVitality.cs` → `EtcdWorkerLoopsVitality`,
`Loops/KeepaliveLoop.cs`. Пишет `Program.cs` (каркас S3Worker-Program с
отличиями: без кластерных хендлеров/процессов — их блоки появятся в фазах 4–5;
регистрирует Options+валидации `EtcdStartupChecks`, метрики, etcd-http
(таймаут 7.5 с, EtcdConnectCallback), ClaimStore/PortAllocLock с префиксом
`"/etcdworker"`, серт `/workers/api_tls/etcdworker` + mTLS :8080, KeepaliveLoop,
watchdog + fail-fast инвариант живости, healthz-checks, `MapHealthChecks`,
`MapAppMetrics`; оставляет `public partial class Program;` для WAF). Создаёт
`EtcdWorkJournal` (EtcdWorker.Provisioning): журнал фаз — ключ
`/etcdworker/work` (БЕЗ суффикса — формат spec §3.3; Shared WorkJournal
суффиксный и перегружен HA-полями — не используется), JSON
`{"op","phase","updated_unix","instance","last_error"?}` (null-поля опускаются),
failover-put, событие `PhaseWritten(string Op, string Phase)` (метрики
подпиской `OnJournalPhase("etcd", op, phase)` — кластерный параметр
WorkerMetricsInstrumentation = константа `"etcd"`, домен один).

**Выход:** `EtcdWorker.App` стартует: keepalive-контур пишет lease-ключи
`/etcdworker/instances/<id>` и `/etcdworker/api/<id>`; /healthz за mTLS;
watchdog-инвариант fail-fast.

**Проверка:** сборка зелёная; WAF-тест фазы 2 (шаг 5) поднимает Program и
видит healthz; юнитов новых нет (каркас), проверка интеграционная на шаге 5.

**Связь со spec:** §3.1 (каркас/App), §3.3 (ключи instances/api/work),
§3.4 (healthz-грань, mTLS, серт-ключ, дискавери), §3.6 (env-секреты
ETCDW_*), решение 10.

**Подпункты (слабой модели):**

4.1. Скопируй файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/EtcdConnectCallback.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/EtcdConnectCallback.cs`:
замени namespace `S3Worker.App` → `EtcdWorker.App`; больше ничего не меняй.

4.2. Скопируй `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/DockerEnvBindings.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/DockerEnvBindings.cs`:
namespace → `EtcdWorker.App`; константы префикса env `S3W_DOCKER_` →
`ETCDW_DOCKER_`; конфиг-префикс в вызовах `AddEnvOverrides` с
`"S3Worker:Docker:..."` → `"EtcdWorker:Docker:..."` (все вхождения; комментарий
шапки — по-русски, упоминание спец-ключа S3W_ETCD_BACKUP из шапки удали — это
зона S3Worker).

4.3. Скопируй `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/Api/TlsEndpoints.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Api/TlsEndpoints.cs`:
namespace `S3Worker.App.Api` → `EtcdWorker.App.Api`; env-префикс `S3W_API_TLS_`
→ `ETCDW_API_TLS_` (все вхождения, включая таблицу EnvBindings и комментарии);
конфиг-секцию `"S3Worker:Api:Tls"` → `"EtcdWorker:Api:Tls"`.

4.4. Скопируй `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/Api/WorkerApiCertReader.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Api/WorkerApiCertReader.cs`:
namespace → `EtcdWorker.App.Api`; если в файле есть литерал имени воркера
`"s3worker"` (ключ /workers/api_tls/...) — замени на `"etcdworker"`; строку
ключа `/workers/api_tls/` не меняй.

4.5. Скопируй `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/HealthState.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/HealthState.cs`
(namespace → `EtcdWorker.App`; содержимое 1:1 — метки активности циклов
healthz). Скопируй также `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/LoopsVitality.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/EtcdWorkerLoopsVitality.cs`:
namespace → `EtcdWorker.App`; имя класса `S3WorkerLoopsVitality` →
`EtcdWorkerLoopsVitality` (все вхождения).

4.6. Скопируй `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/Loops/KeepaliveLoop.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Loops/KeepaliveLoop.cs`:
namespace `S3Worker.App.Loops` → `EtcdWorker.App.Loops`; тип опций
`S3WorkerOptions` → `EtcdWorkerOptions`; строки в комментариях
`/s3worker/instances|api` → `/etcdworker/...`.

4.7. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Provisioning/EtcdWorkJournal.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning;

/// <summary>Журнал фаз состава (arch/25 §5): ключ /etcdworker/work — БЕЗ
/// суффикса (домен один); формат {"op","phase","updated_unix","instance",
/// "last_error"?} — null-поля опускаются. Failover-put по endpoints.
/// Событие PhaseWritten — подписка метрик WorkerMetricsInstrumentation
/// (кластер-параметр — константа "etcd").</summary>
public sealed class EtcdWorkJournal(IEtcdGateway gateway, string[] endpoints)
{
    public const string Key = "/etcdworker/work";

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public sealed record PhaseEntry(string Op, string Phase);

    public event Action<PhaseEntry>? PhaseWritten;

    public async Task<Result> WriteAsync(
        string op, string phase, string instance, string? lastError, CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(
            new Payload(op, phase, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), instance, lastError), Json);
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var put = await gateway.PutAsync(endpoint, Key, payload, lease: null, ct);
            if (put.IsSuccess)
            {
                try { PhaseWritten?.Invoke(new PhaseEntry(op, phase)); }
                catch { /* наблюдатель не влияет на журнал */ }
                return Result.Success();
            }
            last = put;
        }

        return Result.Failed(last.Error ?? new Shared.Etcd.Client.EtcdUnreachableException("etcd endpoints are not set"));
    }

    private sealed record Payload(
        [property: JsonPropertyName("op")] string Op,
        [property: JsonPropertyName("phase")] string Phase,
        [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("last_error")] string? LastError);
}
```

4.8. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Program.cs`
по образцу `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/Program.cs`
(прочитай его целиком) со следующей структурой (комментарии по-русски; шапка —
«Точка входа EtcdWorker (arch/25 §8): host-builder с mTLS-гранью HTTP API...»):
- `WebApplication.CreateBuilder`; `Configure<EtcdWorkerOptions>(Configuration.GetSection("EtcdWorker"))`;
  `AddSingleton(TimeProvider.System)`, `HealthState` + `ILoopProgress`.
- `AddAppMetrics("EtcdWorker", GetSection("EtcdWorker:Metrics"))` +
  `WorkerMetricsInstrumentation`-синглтон, подписка:
  `etcdJournal.PhaseWritten += e => m.OnJournalPhase("etcd", e.Op, e.Phase);`
  (journal регистрируется до подписки).
- `AddHttpClient("etcd", c => c.Timeout = TimeSpan.FromSeconds(7.5)).ConfigurePrimaryHttpMessageHandler(EtcdConnectCallback.CreateHandler)`.
- `DockerEnvBindings.ApplyTlsEnvOverrides/ApplySshEnvOverrides`.
- `AddOptions<EtcdWorkerOptions>().Validate(...)` — все шесть предикатов
  `EtcdStartupChecks` (EtcdEndpoints/AdvertiseUrl/DockerTopology/Thresholds/
  PortRange/Snapshots; сообщения с env-подсказками: `ETCDW_ETCD_ENDPOINT_*`,
  `ETCDW_API_ADVERTISE_URL`, Plain-один-хост/Network, `ETCDW_*` docker) +
  `.ValidateOnStart()`.
- `const string KeyPrefix = "/etcdworker";` + `IEtcdGateway`-синглтон,
  `EtcdWorkJournal`, `ClaimStore(KeyPrefix, endpoints, gateway, clock,
  AdvertiseUrl, thumbprint)`, `PortAllocLock(KeyPrefix, endpoints, gateway,
  clock, ClaimStore.InstanceId)` (порт-механика подключается фазой 5 — клэйм
  уже регистрируется).
- Управляемый серт: `WorkerApiCertReader.ReadAsync(etcdEndpoints, "etcdworker", CancellationToken.None)`
  (endpoints — из конфигурации, как в образце); `TlsEndpoints.ApplyEnvOverrides`;
  `TlsEndpoints.ConfigureMtls(builder, port: 8080, managedCert)`; thumbprint —
  как в образце.
- `SnapshotJob`-регистрации НЕТ (фаза 4); циклы: `AddSingleton<KeepaliveLoop>()`
  + `AddHostedService`; healthz-обвязка `ServiceProbes` + агрегатор
  `EtcdWorkerHealth` (создаётся на шаге 5-проверках сильной моделью по образцу
  `S3WorkerHealth`: checks `etcdworker`/`keepalive-loop`; расширения фаз 4–5);
  `AddHealthChecks().AddCheck...("keepalive-loop")`.
- Watchdog: `EtcdWorkerLoopsVitality` + `AddLoopWatchdog` +
  `WatchdogConfigGuard.EnsureFastLoopsBelowStaleThreshold(loopsCfg.ScanIntervalSec, loopsCfg.KeepaliveSec, watchdog)`
  (по образцу; fail-fast инвариант).
- `var app = builder.Build();` + лог-строки образца (AllowInsecureHttp,
  cert source/warning) + `MapAppMetrics()` + `MapHealthChecks("/healthz")` +
  `MapWorkerApi()` (появится шагом 5 — до него строку не пиши, добавь TODO-комментарий
  `// MapWorkerApi — шаг 5 (API)`) + `await app.RunAsync();` +
  `public partial class Program;`.
Если какой-то блок образца не перечислен (например, счётчик операций API) —
его на этом шаге НЕ переноси: фазы 4–5 добавят свои блоки.

### Шаг 5. API воркера: PUT /api/etcd/config + POST /api/restart (+ WAF)

**Вход:** шаг 4 слит; Program компилируется (MapWorkerApi — TODO).

**Действие (кодер):** пишет `Api/Operations/RestartHandler.cs` (копия
PgWorker-паттерна), `Api/Operations/PutEtcdConfigHandler.cs` и `Api/ApiModule.cs`
(MapWorkerApi: два эндпоинта + метрика операций по образцу S3ApiMetrics), а
также домен-подпункты: `EtcdContourMembers` (EtcdWorker.Core — единый
managed/external-детект по peer-URL-паттерну, используется и гвардом API, и
процессами состава) и `EtcdDomainKeys`+`EtcdConfigJson` (EtcdWorker.Etcd —
ключи и формат `/etcd/config`, spec §3.1). Логика handler'а (все решения — из
spec §3.4-API и adminpanel/02 §9.13): валидация `N` целое 0..7 (иначе 400
ProblemDetails); member list (failover) → `EtcdContourMembers.Build(...)` →
`external.Count`; гвард `external + N >= 3` иначе 409 «состав ниже
безопасного»; etcd-отказ → 503; запись RMW: get `/etcd/config` — нет →
txn `NotExists` + put `{"nodes":N,"created_unix":now}`; есть → txn
`ModRevisionEqual` + put (created_unix исходной декларации сохраняется);
проигрыш txn → 503 (ретрай клиентом). Успех → 200 `{"nodes":N}`. Пишет
WAF-фабрику `Api/EtcdWorkerApiFactory.cs` (по образцу `S3ApiFactory`:
AllowInsecureHttp + env-override etcd-endpoints на фикстуру/фейк) и
WAF-тесты `Api/` (spec-кейсы §7.2-API). Подключает `MapWorkerApi()` в Program.

**Выход:** API-грань работает: 200/400/409/503 + 202 restart; WAF-интеграции
зелёные; healthz жив; домен-детект/config-парсер — с юнитами.

**Проверка:** юниты `dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build --filter "FullyQualifiedName~EtcdContourMembers|FullyQualifiedName~EtcdConfigJson"` зелёные; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/EtcdWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~Api"` — зелёные (после сборки).

**Связь со spec:** §3.4 (API-таблица, гварды, RMW), §7.2-API, adminpanel/02 §9.13.

**Подпункты (слабой модели):**

5.1. Скопируй файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.App/Api/Operations/RestartHandler.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Api/Operations/RestartHandler.cs`:
namespace `PgWorker.App.Api.Operations` → `EtcdWorker.App.Api.Operations`;
комментарий «(spec §3.2 п.2/§4.4)» замени на «(arch/25 §1.1)»; код 1:1.

5.2. Скопируй `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.App/Api/S3ApiMetrics.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Api/EtcdApiMetrics.cs`:
namespace → `EtcdWorker.App.Api`; имя класса `S3ApiMetrics` → `EtcdApiMetrics`
(включая конструктор); метрика `s3worker_api_operations_total` →
`etcdworker_api_operations_total`.

5.3. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/EtcdContourMembers.cs`:
```csharp
using Shared.Etcd.Client;

namespace EtcdWorker.Core;

/// <summary>Managed-член контура: номер узла из peer-URL-паттерна + исходный член.</summary>
public sealed record ManagedMember(int K, EtcdMember Member);

/// <summary>Разбор member list (arch/25 §2): managed — peerURL содержит
/// "/etcdw-node-<k>:" (имя члена не дискриминант); прочие — external.</summary>
public static class EtcdContourMembers
{
    public const string NamePrefix = "etcdw-node-";

    public static (IReadOnlyList<ManagedMember> Managed, IReadOnlyList<EtcdMember> External) Build(
        IReadOnlyList<EtcdMember> members)
    {
        var managed = new List<ManagedMember>();
        var external = new List<EtcdMember>();
        foreach (var m in members)
        {
            var k = ParseK(m);
            if (k is { } number)
                managed.Add(new ManagedMember(number, m));
            else
                external.Add(m);
        }

        return (managed, external);
    }

    // Номер <k> из peer-URL ".../etcdw-node-<k>:..." (int; битовый — null → external).
    public static int? ParseK(EtcdMember member)
    {
        foreach (var url in member.PeerUrls)
        {
            var idx = url.IndexOf($"/{NamePrefix}", StringComparison.Ordinal);
            if (idx < 0)
                continue;
            var rest = url[(idx + 1 + NamePrefix.Length)..];
            var colon = rest.IndexOf(':');
            if (colon <= 0)
                continue;
            return int.TryParse(rest[..colon], out var k) && k > 0 ? k : null;
        }

        return null;
    }
}
```

5.4. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Etcd/EtcdDomainKeys.cs`
(парсеры ключей /etcd/ живут в EtcdWorker.Etcd — spec §3.1; потребители:
API-хендлер (шаг 5.5), MembershipProcess/NodeSupervisor (шаги 12–13)):
```csharp
namespace EtcdWorker.Etcd;

/// <summary>Ключи домена /etcd/ (arch/25 §5).</summary>
public static class EtcdDomainKeys
{
    public const string ConfigKey = "/etcd/config";
    public const string NodesPrefix = "/etcd/nodes/";

    public static string StateKey(int k) => $"{NodesPrefix}{k}/state";

    public static string PlacedOnKey(int k) => $"{NodesPrefix}{k}/placed_on";
}

/// <summary>JSON /etcd/config {"nodes":N,"created_unix":T}: serialize +
/// толерантный parse (битый/неполный → null-компоненты; spec §3.3).</summary>
public static class EtcdConfigJson
{
    public sealed record EtcdConfig(int Nodes, long CreatedUnix);

    public static string Serialize(int nodes, long createdUnix)
        => $"{{\"nodes\":{nodes},\"created_unix\":{createdUnix}}}";

    public static EtcdConfig? Parse(string raw)
    {
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != System.Text.Json.JsonValueKind.Object)
                return null;
            var nodes = root.TryGetProperty("nodes", out var n)
                        && n.ValueKind == System.Text.Json.JsonValueKind.Number
                        && n.TryGetInt32(out var nodesValue)
                ? nodesValue
                : null;
            var created = root.TryGetProperty("created_unix", out var c)
                          && c.ValueKind == System.Text.Json.JsonValueKind.Number
                          && c.TryGetInt64(out var createdValue)
                ? createdValue
                : null;
            return nodes is null || created is null ? null : new EtcdConfig(nodes.Value, created.Value);
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }
}
```

5.5. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Api/Operations/PutEtcdConfigHandler.cs`:
```csharp
using System.Text.Json.Serialization;
using EtcdWorker.Core;
using EtcdWorker.Etcd;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.App.Api.Operations;

// Тело PUT /api/etcd/config (arch/25 §1.1): {"nodes":N} — замещение декларации.
public sealed record PutEtcdConfigRequest([property: JsonPropertyName("nodes")] int Nodes);

// 409: итоговый состав ниже безопасного (external + N < 3).
public sealed class EtcdQuorumGuardException(int nodes, int external)
    : Exception($"unsafe membership: {external} external + {nodes} managed < 3 members (quorum guard, arch/25 §1.1)");

// 400: N вне 0..7.
public sealed class EtcdConfigValidationException(string message) : Exception(message);

/// <summary>PUT /api/etcd/config (arch/25 §1.1/§5): валидация 0..7 → гвард
/// кворума (external + N ≥ 3) → RMW /etcd/config по mod_revision (нет ключа →
/// txn version==0; проигрыш гонки → Failed-транзиент, вызывающий отдаёт 503).
/// created_unix — исходная метка появления декларации (сохраняется при замене).
/// Формат/ключи домена — EtcdDomainKeys (EtcdWorker.Etcd); managed/external —
/// EtcdContourMembers (единый peer-URL-детект, EtcdWorker.Core).</summary>
public sealed class PutEtcdConfigHandler(
    IEtcdGateway gateway,
    string[] endpoints,
    TimeProvider clock)
{
    public async Task<Result> HandleAsync(PutEtcdConfigRequest request, CancellationToken ct)
    {
        if (request.Nodes is < 0 or > 7)
            return Result.Failed(new EtcdConfigValidationException(
                $"nodes must be an integer in 0..7, got {request.Nodes}"));

        // Гвард кворума: итог external + managed ≥ 3 (external — члены без
        // managed-паттерна peer-URL; единый детект с процессами состава).
        var members = await MemberListWithFailoverAsync(ct);
        if (!members.IsSuccess)
            return Result.Failed(members.Error!);
        var (_, external) = EtcdContourMembers.Build(members.Value);
        if (external.Count + request.Nodes < 3)
            return Result.Failed(new EtcdQuorumGuardException(request.Nodes, external.Count));

        var current = await GetWithFailoverAsync(EtcdDomainKeys.ConfigKey, ct);
        if (!current.IsSuccess)
            return Result.Failed(current.Error!);

        var now = clock.GetUtcNow().ToUnixTimeSeconds();
        TxnRequest txn;
        if (current.Value is { } kv)
        {
            // created_unix исходной декларации сохраняется (битый JSON → now).
            var createdUnix = EtcdConfigJson.Parse(kv.Value)?.CreatedUnix ?? now;
            txn = TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(EtcdDomainKeys.ConfigKey, (long)kv.ModRevision)],
                [new TxnOp.Put(EtcdDomainKeys.ConfigKey, EtcdConfigJson.Serialize(request.Nodes, createdUnix), null)]);
        }
        else
        {
            txn = TxnRequest.Of(
                [TxnCompare.NotExists(EtcdDomainKeys.ConfigKey)],
                [new TxnOp.Put(EtcdDomainKeys.ConfigKey, EtcdConfigJson.Serialize(request.Nodes, now), null)]);
        }

        var written = await TxnWithFailoverAsync(txn, ct);
        if (!written.IsSuccess)
            return Result.Failed(written.Error!);
        return written.Value.Succeeded
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"{EtcdDomainKeys.ConfigKey} changed since read (mod_revision mismatch) — retried by client"));
    }

    private async Task<Result<IReadOnlyList<EtcdMember>>> MemberListWithFailoverAsync(CancellationToken ct)
    {
        Result<IReadOnlyList<EtcdMember>> last = default;
        foreach (var endpoint in endpoints)
        {
            var list = await gateway.MemberListAsync(endpoint, ct);
            if (list.IsSuccess)
                return list;
            last = list;
        }
        return Result<IReadOnlyList<EtcdMember>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var got = await gateway.GetAsync(endpoint, key, ct);
            if (got.IsSuccess)
                return got;
            last = got;
        }
        return Result<Kv?>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<TxnResult>> TxnWithFailoverAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var txn = await gateway.TxnAsync(endpoint, req, ct);
            if (txn.IsSuccess)
                return txn;
            last = txn;
        }
        return Result<TxnResult>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}
```

5.6. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.App/Api/ApiModule.cs`
(namespace `EtcdWorker.App.Api`; структура — упрощённый образец
`S3Worker.App/Api/ApiModule.cs`; комментарии по-русски):
```csharp
public static class ApiModule
{
    public static IEndpointRouteBuilder MapWorkerApi(this IEndpointRouteBuilder endpoints)
    {
        var metrics = endpoints.ServiceProvider.GetRequiredService<EtcdApiMetrics>();

        // PUT /api/etcd/config — 200 / 400 (N вне 0..7) / 409 (кворум-гвард) /
        // 503 (etcd-отказ/RMW-гонка — ретрай клиентом).
        endpoints.MapPut("/api/etcd/config", async (
            PutEtcdConfigRequest request, PutEtcdConfigHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleAsync(request, ct);
            var response = result.IsSuccess
                ? Results.Ok(new { nodes = request.Nodes })
                : result.Error switch
                {
                    EtcdConfigValidationException => Results.Problem(
                        statusCode: StatusCodes.Status400BadRequest,
                        title: "Validation failed", detail: result.Error.Message),
                    EtcdQuorumGuardException => Results.Problem(
                        statusCode: StatusCodes.Status409Conflict,
                        title: "Unsafe membership", detail: result.Error.Message),
                    _ => Results.Problem(
                        statusCode: StatusCodes.Status503ServiceUnavailable,
                        title: "Etcd unavailable", detail: result.Error!.Message),
                };
            metrics.Track("put-etcd-config", GetStatusCode(response));
            return response;
        });

        // POST /api/restart — 202 graceful self-stop (arch/14 §1.1).
        endpoints.MapPost("/api/restart", (HttpRequest http, RestartHandler handler) =>
        {
            var dto = handler.Handle(RequestedBy(http));
            return Results.Accepted();
            // Оператор панели (fallback "api" — заголовок X-Requested-By).
            static string? RequestedBy(HttpRequest r)
                => r.Headers.TryGetValue("X-Requested-By", out var v) && v.Count > 0 ? v[0] : null;
        });

        return endpoints;
    }

    private static int GetStatusCode(IResult result)
        => result switch
        {
            IStatusCodeHttpResult withCode => withCode.StatusCode ?? 500,
            _ => 500,
        };
}
```
(сверь сигнатуру `RestartHandler.Handle` из 5.1 — если она принимает
`string?` и возвращает DTO, адаптируй вызов так, чтобы код компилировался и
возвращал 202). В Program.cs замени TODO-строку на `app.MapWorkerApi();` и
зарегистрируй синглтоны `EtcdApiMetrics` (метр из DI) и `PutEtcdConfigHandler`/
`RestartHandler` (по образцу регистрации хендлеров S3Worker.Program).

5.7. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/Core/EtcdContourMembersTests.cs`
(xunit + FluentAssertions, AAA-комментарии по-русски; EtcdMember собирай
прямо в тесте): managed по peerURL `http://etcdw-node-3:2380` → K=3; peerURL
без паттерна (`http://seed:2380`) → external; паттерн с нечисловым K
(`http://etcdw-node-abc:2380`) → external (edge-детект совпадает с гвардом
API — один и тот же EtcdContourMembers); несколько peerURL — до первого
совпадения; `IsLearner` сквозь разбор. Рядом (`EtcdConfigJsonTests.cs`) —
юниты `EtcdConfigJson`: Serialize → ровно `{"nodes":2,"created_unix":123}`;
Parse валидного; битый JSON → null; без created_unix → null.

Подпунктов на WAF-фабрику/тесты нет — их пишет сильная модель по образцу
`S3Worker.IntegrationTests/Api/{S3ApiFactory,MtlsApiTests}.cs`: кейсы
`PutConfig_Valid_200` (фейк/реальный etcd — предпочтительно WAF на
EtcdWorkerEnvironment шага 2), `PutConfig_NegativeAndEight_400`,
`PutConfig_BelowQuorum_409` (external=1, N=1), `PutConfig_EtcdDown_503`,
`PutConfig_RmwRace_503` (подмена ключа между чтением и txn — через
инъекцию/двойной вызов с конкурентной записью), `Restart_202` +
`Healthz_RequireClientCert` (mTLS).

---

## Фаза 3 — S3Worker: фиксированный слепковый bucket (spec §3.4-правка, §4.3)

### Шаг 6. Константа + создание `etcd-backups` в P3/AddNode

**Вход:** ветка зелёная; `EnsureBucketsAsync` (ProvisioningProcess) и
bucket-шаг `AddNodeProcess` (строка ~141) — создают только per-install.

**Действие (кодер):** добавляет `EtcdSnapshotBucket` в Shared.Core; чистую
функцию отбора bucket'ов в S3Worker; вызывает её в P3 и AddNode; юнит +
docker-интеграционный тест (по образцу `ProvisioningTests`: seed etcd_backup
кластера → тики ProvisioningProcess до Active → на инстансе ОБА bucket'а
(AWSSDK-проверка корнем/root-кредами фикстуры); повторный прогон — no-op;
AddNode — второй инстанс получает оба).

**Выход:** etcd-backup кластер S3Worker'а несёт оба bucket'а идемпотентно.

**Проверка:** юнит `S3Worker.UnitTests` (BucketsForInstance) зелёный;
`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/S3Worker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~EtcdBackupBucket"` — зелёный; зачистка серии.

**Связь со spec:** §3.4 (контрактная правка S3Worker), §3.1 (константа),
решение 7, §7.1 (юнит отбора), §7.2 (интеграция).

**Подпункты (слабой модели):**

6.1. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/Shared.Core/EtcdSnapshotBucket.cs`:
```csharp
namespace Shared.Core;

/// <summary>Имя фиксированного etcd-слепкового bucket (arch/19 §5, arch/23 §5,
/// arch/25 §4): одинаково на всех установках; создаёт S3Worker на инстансах
/// etcd-backup кластера, выгружает в него EtcdWorker. НЕ конфигурируется.</summary>
public static class EtcdSnapshotBucket
{
    public const string Name = "etcd-backups";
}
```

6.2. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.Provisioning/Processes/S3ProcessCommon.cs`
добавь в конец класса `S3ProcessCommon` (или как новый статический метод рядом с
прочими helpers этого файла — по его стилю):
```csharp
    /// <summary>Отбор bucket'ов для инстанса (arch/23 §5): per-install — всегда;
    /// фиксированный etcd-слепковый — только у etcd-backup кластера
    /// (EtcdWorker выгружает в него DR-слепки etcd).</summary>
    public static IReadOnlyList<string> BucketsForInstance(string perInstallBucket, bool etcdBackup)
    {
        var buckets = new List<string> { perInstallBucket };
        if (etcdBackup)
            buckets.Add(Shared.Core.EtcdSnapshotBucket.Name);
        return buckets;
    }
```

6.3. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.Provisioning/Processes/ProvisioningProcess.cs`
в методе `EnsureBucketsAsync` (приватный, комментарий «P3: bucket per-install
на каждой ноде...») замени цикл foreach на:
```csharp
        // P3 (arch/23 §5): per-install bucket на каждой ноде; для etcd-backup
        // кластера — ДОПОЛНИТЕЛЬНО фиксированный etcd-слепковый bucket
        // (создаёт EtcdWorker'у цель выгрузки; идемпотентно: exists → пропуск).
        var buckets = S3ProcessCommon.BucketsForInstance(
            options.Bucket, snap.Config?.EtcdBackup ?? false);
        foreach (var node in snap.Nodes.OrderBy(n => n.Node, StringComparer.Ordinal))
        {
            var addr = addresses[node.Node];
            foreach (var bucket in buckets)
            {
                var ensured = await nodeClient.EnsureBucketAsync(
                    options.AdvertisedHost ?? addr.Host, addr.S3Port, nodeSecrets, bucket, ct);
                if (!ensured.IsSuccess)
                    return Result.Failed(ensured.Error!);
            }
        }
```

6.4. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.Provisioning/Processes/AddNodeProcess.cs`
найди блок `var bucket = await nodeClient.EnsureBucketAsync(probeHost,
address.S3Port, nodeSecrets!, options.Bucket, ct); if (!bucket.IsSuccess)
return Fail(cluster, node.Node, bucket.Error!, "ensure-bucket");` и замени на:
```csharp
        // arch/23 §5: per-install + (etcd_backup кластер) фиксированный
        // etcd-слепковый bucket — новый инстанс получает оба (идемпотентно).
        var bucketNames = S3ProcessCommon.BucketsForInstance(
            options.Bucket, snap.Config?.EtcdBackup ?? false);
        foreach (var bucketName in bucketNames)
        {
            var bucket = await nodeClient.EnsureBucketAsync(
                probeHost, address.S3Port, nodeSecrets!, bucketName, ct);
            if (!bucket.IsSuccess)
                return Fail(cluster, node.Node, bucket.Error!, "ensure-bucket");
        }
```
(если переменная `snap` в этой точке метода называется иначе — используй
фактическое имя снапшота кластера, из которого читается `Config.EtcdBackup`).

6.5. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/S3Worker.UnitTests/Provisioning/BucketsForInstanceTests.cs`
(xunit + FluentAssertions, AAA-комментарии по-русски): три факта —
`OrdinaryCluster_OnlyPerInstall` (`BucketsForInstance("pgworker-backups", false)`
→ ровно `["pgworker-backups"]`), `EtcdBackupCluster_BothBuckets`
(`(..., true)` → `["pgworker-backups", "etcd-backups"]` — вторым элементом,
имя — `Shared.Core.EtcdSnapshotBucket.Name`), `EmptyPerInstall_StillListed`
(пустая строка возвращается как есть — валидация имени вне функции).

---

## Фаза 4 — Слепки/экспорт (spec §3.4 SnapshotLoop, §4.4)

### Шаг 7. Перенос статус/meta/ретенции в EtcdWorker.Etcd

**Вход:** фаза 2 слита; исходники
`src/PgWorker.Backups/EtcdExport/{EtcdSnapshotStatus,EtcdSnapshotMeta,EtcdExportRetention}.cs`
не тронуты (демонтаж — фаза 6).

**Действие (кодер):** переносит механику 1:1 с тремя заменами: namespace
`PgWorker.Backups.EtcdExport` → `EtcdWorker.Etcd`; статус-ключ
`/pgworker/etcd-snapshots` → `/etcdworker/snapshots`; поле `enabled` удаляется
из записи/JSON-формата (`IsBehind`: `status is null ? latestLocalTakenUnix is
not null : status.State == "FAILED" || (latestLocalTakenUnix is { } taken &&
(status.LastUploadedUnix is not { } uploaded || taken > uploaded))` — выгрузка
безусловна, решение 20). `EtcdExportRetention` переносится с локальным типом
объекта: в `EtcdWorker.Core` создаётся `record EtcdS3Object(string Key, long
SizeBytes, DateTimeOffset LastModified)` (перенос `S3ObjectInfo` без
зависимости от PgWorker.Backups). Дополняет EtcdWorker.Etcd парсер листов
домена (spec §3.1 — парсеры ключей /etcd/ живут здесь): `EtcdNodesParser` —
подпункт 7.6; потребляют MembershipProcess/NodeSupervisor/ReconcileLoop
(шаги 12–13; ключи — `EtcdDomainKeys` с шага 5.4). Юниты — перенос
соответствующих тестов t08 (статус-JSON OK/FAILED/битый, IsBehind-ветки,
TakenUnixFromName, retention: guard ≥1, парность db+meta, сирота-meta, N пар)
+ юниты EtcdNodesParser.

**Выход:** `EtcdWorker.Etcd` несёт полный формат статуса/меты/ретенции и
парсеры домена /etcd/; юниты зелёные.

**Проверка:** `dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build --filter "FullyQualifiedName~EtcdSnapshot|FullyQualifiedName~EtcdNodesParser"` — зелёные.

**Связь со spec:** §3.1 (EtcdWorker.Etcd), §3.7 (перенос 1:1), решение 20,
§7.1 (статус-JSON/IsBehind/ретенция).

**Подпункты (слабой модели):**

7.1. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/EtcdS3Object.cs`:
```csharp
namespace EtcdWorker.Core;

/// <summary>Объект bucket'а слепков (list-результат ретенции): ключ + размер
/// + время последней модификации (перенос S3ObjectInfo t08 без зависимости
/// от PgWorker.Backups).</summary>
public sealed record EtcdS3Object(string Key, long SizeBytes, DateTimeOffset LastModified);
```

7.2. Скопируй файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/EtcdExport/EtcdSnapshotStatus.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Etcd/EtcdSnapshotStatus.cs`
и внеси ровно следующие правки: namespace → `EtcdWorker.Etcd`; из записи
`EtcdSnapshotStatus` удали параметр `bool Enabled` (первым) — запись:
`EtcdSnapshotStatus(string State, long? LastUploadedUnix, string? LastObject,
string? LastSha256, long? SizeBytes, int? IntervalMin, string? Error)`;
`IsBehind` замени на:
```csharp
    /// <summary>Выгрузка отстаёт: FAILED, либо ключа нет вовсе, либо новейший
    /// локальный слепок снят позже подтверждённого покрытия. Выгрузка
    /// безусловна (enabled-поля нет, arch/25 §5) — «нет ключа» = отстаёт при
    /// наличии локальных слепков.</summary>
    public static bool IsBehind(EtcdSnapshotStatus? status, long? latestLocalTakenUnix)
        => status is null
            ? latestLocalTakenUnix is not null
            : status.State == "FAILED"
              || (latestLocalTakenUnix is { } taken
                  && (status.LastUploadedUnix is not { } uploaded || taken > uploaded));
```
в `EtcdSnapshotStatusJson`: `Key = "/etcdworker/snapshots"`; из приватной записи
`Payload` удали поле `Enabled` (и из конструкторов всех вызовов); `Ok(...)`:
`Serialize(new Payload("OK", coveredTakenUnix, lastObject, lastSha256,
sizeBytes, intervalMin, null))`; `Failed(...)`:
`Serialize(new Payload("FAILED", lastOk?.LastUploadedUnix, lastOk?.LastObject,
lastOk?.LastSha256, lastOk?.SizeBytes, intervalMin, error))`; в `Parse` верни
`new EtcdSnapshotStatus(payload.State, payload.LastUploadedUnix,
payload.LastObject, payload.LastSha256, payload.SizeBytes,
payload.IntervalMin, payload.Error)`; XML-комментарий шапки замени: «Значение
статус-ключа /etcdworker/snapshots (arch/25 §5): факт последней выгрузки
слепка etcd в S3 (перенос t08; поле enabled удалено — выгрузка безусловна)».

7.3. Скопируй файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/EtcdExport/EtcdSnapshotMeta.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Etcd/EtcdSnapshotMeta.cs`
с единственной правкой namespace → `EtcdWorker.Etcd` (комментарий шапки
обнови: «(t08 → EtcdWorker, arch/19 §5)»).

7.4. Скопируй файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/EtcdExport/EtcdExportRetention.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Etcd/EtcdExportRetention.cs`
с правками: namespace → `EtcdWorker.Etcd`; тип элементов `IReadOnlyList<S3ObjectInfo>`
→ `IReadOnlyList<EtcdWorker.Core.EtcdS3Object>` (добавь using
`EtcdWorker.Core`; компаратор `x.Key == $"{Prefix}{id}.db"` остаётся).

7.5. Перенеси тесты статуса/ретенции: скопируй из
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/PgWorker.UnitTests/`
файлы с тестами `EtcdSnapshotStatus`/`EtcdExportRetention`/`TakenUnixFromName`
(найди их grep'ом `EtcdSnapshotStatusJson\|EtcdExportRetention\|TakenUnixFromName`
в `src/tests/PgWorker.UnitTests`) в
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/Etcd/`
(новые файлы, namespace `EtcdWorker.UnitTests.Etcd`, usings → `EtcdWorker.Etcd`),
правя: все конструкторы `EtcdSnapshotStatus(...)` — без первого bool-аргумента
Enabled (в тестовых данных, где `enabled:false` ожидал «не отстаёт» — кейс
удаляется, формат без enabled); ключ в ожиданиях `/pgworker/etcd-snapshots` →
`/etcdworker/snapshots`; `S3ObjectInfo` → `EtcdS3Object`. Исходные PgWorker-тесты
НЕ удаляй (фаза 6).

7.6. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Etcd/EtcdNodesParser.cs`
(парсер листов домена /etcd/ — spec §3.1: парсеры ключей /etcd/ в
EtcdWorker.Etcd; ключи — EtcdDomainKeys из шага 5.4):
```csharp
using Shared.Etcd.Client;

namespace EtcdWorker.Etcd;

/// <summary>Разбор листов /etcd/nodes/&lt;k&gt;/{state,placed_on} (arch/25 §5):
/// Kv[] префикса → узлы; толерантно (битые номера/чужие листы — игнор,
/// значения — raw-строки с обрезкой кавычек JSON-строк).</summary>
public static class EtcdNodesParser
{
    public sealed record ManagedNodeState(int K, string? State, string? PlacedOn);

    public static IReadOnlyList<ManagedNodeState> Parse(IReadOnlyList<Kv> kvs)
    {
        var nodes = new Dictionary<int, ManagedNodeState>();
        foreach (var kv in kvs)
        {
            // Схема: /etcd/nodes/<k>/<leaf> (5 сегментов).
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[1] != "etcd" || segments[2] != "nodes")
                continue;
            if (!int.TryParse(segments[3], out var k) || k <= 0)
                continue;
            var value = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim().Trim('"');
            var leaf = segments[4];
            var current = nodes.TryGetValue(k, out var existing)
                ? existing
                : new ManagedNodeState(k, null, null);
            nodes[k] = leaf switch
            {
                "state" => current with { State = value },
                "placed_on" => current with { PlacedOn = value },
                _ => current, // unknown-лист — не ошибка
            };
        }

        return nodes.Values.OrderBy(n => n.K).ToList();
    }
}
```
Добавь юнит-файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/Etcd/EtcdNodesParserTests.cs`
(xunit + FluentAssertions, AAA-комментарии по-русски; Kv собирай прямо в
тесте): state+placed_on одного узла; два узла — сортировка по K; JSON-кавычки
значения (`"RUNNING"`) снимаются; битый номер K (`/etcd/nodes/abc/state`) —
игнор; чужой ключ вне схемы — игнор; unknown-лист (`/etcd/nodes/1/usage`) —
не ошибка, узел не создаётся листом.

### Шаг 8. S3-клиент + EtcdBackupTargetResolver + SnapshotExporter

**Вход:** шаг 7 слит; константа `EtcdSnapshotBucket` существует (шаг 6).

**Действие (кодер):** переносит в EtcdWorker.Core минимальный S3-клиент
(осознанное дублирование BackupS3 — риск-таблица §8): интерфейс
`IEtcdSnapshotS3 { Task<Result> PutObjectAsync(string key, byte[] data,
string? sha256, CancellationToken ct); Task<Result<IReadOnlyList<EtcdS3Object>>>
ListPrefixAsync(string prefix, CancellationToken ct); Task<Result>
DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct); }` +
`EtcdSnapshotS3 : IEtcdSnapshotS3, IAsyncDisposable` — реализация
механикой `BackupS3` (AWSSDK, ForcePathStyle, пер-попыточный таймаут 10 с,
ChecksumSHA256 base64-конвертация hex→base64, list-v2 пагинация, batch-delete
чанками 1000; конструктор `(string endpoint, string bucket, string accessKey,
string secretKey, string? caBundlePath = null, TimeSpan? requestTimeout = null)`
+ доверие CA через `Shared.Tls.TlsMaterial.LoadPem` и локальную копию
механики `S3CaHttpClientFactory` (файл `EtcdWorker.Core/S3CaHttpClientFactory.cs`
— копия `src/PgWorker.Backups/S3CaHttpClientFactory.cs` + `BackupCaPem`-механика
чтения пути: создавай `EtcdWorker.Core/BackupCaPem.cs` копией
`src/PgWorker.Backups/BackupCaPem.cs` с namespace `EtcdWorker.Core`)).
В EtcdWorker.Provisioning: `EtcdBackupTargetResolver` (живой резолв: range
`/s3/clusters/` → config-ключи 5 сегментов c `etcd_backup:true` → единственный
(первый по Ordinal ключей; единственность гарантирует гвард S3Worker) → GET
config (state TO_REMOVE → Failed) → GET endpoints (парсинг «h:p,h:p» —
перенос логики `S3PoolModel.ParseEndpoints` в локальный приватный метод
резолвера) → GET secrets (ключ `backups` — перенос логики
`S3PoolModel.ParseSecrets` локально); успех → `EtcdBackupTarget(IReadOnlyList<string>
Endpoints /* http://host:port */, string AccessKey, string SecretKey /*,
Bucket = EtcdSnapshotBucket.Name через свойство */)`; все отказы —
`ApplicationException` c литералом-маркером `etcd-backup-missing` в сообщении)
и `SnapshotExporter` (перенос `EtcdSnapshotSink` 1:1: конструктор
`(IEtcdGateway etcd, string[] etcdEndpoints, EtcdBackupTargetResolver resolver,
Func<EtcdBackupTarget, IEtcdSnapshotS3> clientFactory, int retentionObjects,
int timeoutSec, string instanceId, int intervalMin, Action? pulse = null,
TimeSpan? pulsePeriod = null)`; отличие от оригинала: цель резолвится ВНУТРИ
`ExportAsync` (resolver.Resolve → Failed → `FailAsync(lastOk, error, ct)` со
статусом FAILED+маркер); клиент создаётся per-попытка через factory и
dispose'ится; `ReadStatusAsync`/`PutStatusAsync`/`CatchUpAsync`/`LatestLocalFile`/
`PulseAsync` — 1:1). Юниты: резолвер (отсутствие кластера/TO_REMOVE/битые
secrets/пустые endpoints → маркер; успех — креды+endpoints+bucket-константа),
сборка статуса OK/FAILED-сохранение-полей, IsBehind — уже шаг 7.

**Выход:** экспортёр полностью переносим; юниты резолвера/экспортёра зелёные.

**Проверка:** `dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build --filter "FullyQualifiedName~EtcdBackupTargetResolver|FullyQualifiedName~SnapshotExporter"` — зелёные.

**Связь со spec:** §3.4 (EtcdBackupTargetResolver, SnapshotExporter, маркер),
§3.1 (S3-клиент Core), решение 7/21, §7.1, §8-риски (дублирование клиента).

**Подпункты (слабой модели):**

8.1. Скопируй `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/S3CaHttpClientFactory.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/S3CaHttpClientFactory.cs`
(namespace `PgWorker.Backups` → `EtcdWorker.Core`; остальное 1:1). Скопируй
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/BackupCaPem.cs`
в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/BackupCaPem.cs`
(namespace → `EtcdWorker.Core`; остальное 1:1).

8.2. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/EtcdSnapshotS3.cs`:
интерфейс `IEtcdSnapshotS3` (сигнатуры из шага 8-Действие; XML-комментарии
по-русски: put с SHA256-проверкой транспорта / list-пагинация / batch-delete
идемпотентно) и класс `EtcdSnapshotS3(string endpoint, string bucket, string
accessKey, string secretKey, string? caBundlePath = null, TimeSpan?
requestTimeout = null) : IEtcdSnapshotS3, IAsyncDisposable`. Реализацию пиши
механикой методов `PutObjectAsync`/`ListPrefixCoreAsync`/`DeleteKeysAsync`
файла `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/BackupS3.cs`
(прочитай его): конструктор — повтори механику конструктора `BackupS3`
(настройка `AmazonS3Config`: `ServiceURL = endpoint`, `ForcePathStyle = true`,
`Timeout = requestTimeout ?? 10 c`, при непустом caBundlePath —
`config.HttpClientFactory = new S3CaHttpClientFactory(
Shared.Tls.TlsMaterial.LoadPem(BackupCaPem.ReadIfConfigured(caBundlePath)!))`)
напрямую из аргументов конструктора — БЕЗ `S3ClientOptions`: этот record живёт
в `PgWorker.Core` и недоступен `EtcdWorker.Core`;
put — конвертация hex-sha256 → base64 в `ChecksumSHA256`; list — list-v2
пагинация с маппингом в `EtcdS3Object`; delete — чанки по 1000; все методы —
`Result`-обёртки с сообщениями `S3 put/list/batch-delete ...` как в образце;
`DisposeAsync` — dispose клиента.

8.3. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Provisioning/EtcdBackupTargetResolver.cs`:
```csharp
using System.Text.Json;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning;

/// <summary>Цель выгрузки DR-слепков (arch/25 §4): bucket — константа
/// EtcdSnapshotBucket.Name; endpoint'ы/креды — живой резолв из /s3/clusters/
/// (единственный кластер с etcd_backup=true). Все отказы — маркер
/// etcd-backup-missing в сообщении (алерт панели, transient для цикла).</summary>
public sealed record EtcdBackupTarget(
    IReadOnlyList<string> Endpoints, string AccessKey, string SecretKey)
{
    public string Bucket => Shared.Core.EtcdSnapshotBucket.Name;
}

/// <summary>Маркер недоступности цели (литерал — панельный алерт-признак).</summary>
public static class EtcdBackupMarkers
{
    public const string Missing = "etcd-backup-missing";
}

public sealed class EtcdBackupTargetResolver(IEtcdGateway gateway, string[] endpoints)
{
    private const string ClustersPrefix = "/s3/clusters/";

    public async Task<Result<EtcdBackupTarget>> ResolveAsync(CancellationToken ct)
    {
        var range = await RangeWithFailoverAsync(ClustersPrefix, ct);
        if (!range.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(range.Error!);

        // Единственный etcd_backup=true (гвард создания у S3Worker); первый по
        // Ordinal — детерминизм.
        string? cluster = null;
        foreach (var kv in range.Value.OrderBy(k => k.Key, StringComparer.Ordinal))
        {
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[^1] != "config")
                continue;
            try
            {
                using var doc = JsonDocument.Parse(kv.Value);
                if (doc.RootElement.ValueKind == JsonValueKind.Object
                    && doc.RootElement.TryGetProperty("etcd_backup", out var flag)
                    && flag.ValueKind == JsonValueKind.True)
                {
                    cluster = segments[3];
                    break;
                }
            }
            catch (JsonException)
            {
                // битый config — пропустить
            }
        }

        if (cluster is null)
            return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                $"{EtcdBackupMarkers.Missing}: no /s3/clusters/ config with etcd_backup=true"));

        var config = await GetWithFailoverAsync($"{ClustersPrefix}{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(config.Error!);
        if (config.Value is { } cfg)
        {
            try
            {
                using var doc = JsonDocument.Parse(cfg.Value);
                if (doc.RootElement.TryGetProperty("state", out var st)
                    && st.ValueKind == JsonValueKind.String
                    && st.GetString() == "TO_REMOVE")
                    return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                        $"{EtcdBackupMarkers.Missing}: etcd-backup cluster {cluster} is in TO_REMOVE state"));
            }
            catch (JsonException)
            {
                // битый config не должен был попасть в отбор — прозрачно пропускаем
            }
        }

        var endpointsKv = await GetWithFailoverAsync($"{ClustersPrefix}{cluster}/endpoints", ct);
        if (!endpointsKv.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(endpointsKv.Error!);
        var endpointList = endpointsKv.Value is { } ep
            ? ParseEndpoints(ep.Value)
            : [];
        if (endpointList.Count == 0)
            return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                $"{EtcdBackupMarkers.Missing}: etcd-backup cluster {cluster} has no readable endpoints"));

        var secretsKv = await GetWithFailoverAsync($"{ClustersPrefix}{cluster}/secrets", ct);
        if (!secretsKv.IsSuccess)
            return Result<EtcdBackupTarget>.Failed(secretsKv.Error!);
        var secrets = secretsKv.Value is { } sec ? ParseBackupsSecrets(sec.Value) : null;
        if (secrets is null)
            return Result<EtcdBackupTarget>.Failed(new ApplicationException(
                $"{EtcdBackupMarkers.Missing}: etcd-backup cluster {cluster} has broken secrets (key backups)"));

        return Result<EtcdBackupTarget>.Success(new EtcdBackupTarget(
            endpointList.Select(e => $"http://{e}").ToList(), secrets.Value.Access, secrets.Value.Secret));
    }

    // "h1:p1,h2:p2" → ["h1:p1",...] (битые элементы пропускаются; перенос
    // механики S3PoolModel.ParseEndpoints — PgWorker.Etcd недоступен сборке).
    internal static IReadOnlyList<string> ParseEndpoints(string raw)
    {
        var result = new List<string>();
        foreach (var item in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var colon = item.LastIndexOf(':');
            if (colon <= 0 || colon == item.Length - 1)
                continue;
            if (!int.TryParse(item[(colon + 1)..], out var port) || port <= 0)
                continue;
            result.Add(item);
        }
        return result;
    }

    // secrets-JSON → пара ключа backups (перенос механики S3PoolModel.ParseSecrets).
    internal static (string Access, string Secret)? ParseBackupsSecrets(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("access_keys", out var keys)
                || keys.ValueKind != JsonValueKind.Array)
                return null;
            foreach (var item in keys.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object
                    || !item.TryGetProperty("name", out var name) || name.GetString() != "backups"
                    || !item.TryGetProperty("access_key", out var access) || access.ValueKind != JsonValueKind.String
                    || !item.TryGetProperty("secret_key", out var secret) || secret.ValueKind != JsonValueKind.String)
                    continue;
                var a = access.GetString();
                var s = secret.GetString();
                if (!string.IsNullOrEmpty(a) && !string.IsNullOrEmpty(s))
                    return (a, s);
            }
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }
        return Result<IReadOnlyList<Kv>>.Failed(
            last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
    {
        var range = await RangeWithFailoverAsync(key, ct);
        return range.Bind(kvs => Result<Kv?>.Success(kvs.Count > 0 ? kvs[0] : null));
    }
}
```

8.4. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Provisioning/SnapshotExporter.cs`
переносом файла `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/EtcdExport/EtcdSnapshotSink.cs`
(прочитай его целиком; класс `EtcdSnapshotSink : ISnapshotSink` →
`EtcdWorker.Provisioning.SnapshotExporter` БЕЗ интерфейса ISnapshotSink):
- конструктор: `(IEtcdGateway etcd, string[] etcdEndpoints, EtcdBackupTargetResolver
  resolver, Func<EtcdBackupTarget, IEtcdSnapshotS3> clientFactory, int
  retentionObjects, int timeoutSec, string instanceId, int intervalMin,
  Action? pulse = null, TimeSpan? pulsePeriod = null)` (usings: `EtcdWorker.Core`,
  `EtcdWorker.Etcd`, `Shared.Core`, `Shared.Etcd.Client`);
- `ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct)`:
  первые строки — резолв цели:
```csharp
        var target = await resolver.ResolveAsync(ct);
        if (!target.IsSuccess)
            return await FailAsync(
                (await ReadStatusAsync(ct)).IsSuccess ? (await ReadStatusAsync(ct)).Value : null,
                target.Error!, ct);
```
  (прочитай статус один раз в локальную переменную — двойной вызов в примере
  выше замени на один; Failed-резолв → статус FAILED с маркером-текстом ошибки);
  далее — оригинальный конвейер 1:1 (sha, прошлый статус, taken-метка, put
  db-пары, put .meta.json с ключом без ".db", ретенция `EtcdExportRetention.Select`
  по `EtcdS3Object`, статус OK семантики покрытия), вызовы `s3.*` — через
  созданный per-попытку клиент:
```csharp
        using var s3 = clientFactory(target.Value);
```
  (в `using var` — клиент должен реализовывать IDisposable/IAsyncDisposable —
  `EtcdSnapshotS3` уже IAsyncDisposable; убедись, что использует
  `await using var s3 = clientFactory(target.Value);` если тип —
  IAsyncDisposable);
- `CatchUpAsync`/`ReadStatusAsync`/`PutStatusAsync`/`LatestLocalFile`/
  `PulseAsync`/`FailAsync` — 1:1 (статус-ключ — из `EtcdSnapshotStatusJson.Key`
  новой сборки; `IsBehind` — новая сигнатура без Enabled);
- XML-комментарий шапки: «Перенос EtcdSnapshotSink t08 (arch/25 §4): цель —
  живой резолв etcd-backup кластера, bucket etcd-backups; статус/ретенция/
  CatchUp/пульс — механика 1:1».

8.5. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/Provisioning/EtcdBackupTargetResolverTests.cs`:
юниты резолвера на фейковом `IEtcdGateway` (создай в
`src/tests/EtcdWorker.UnitTests/Fakes/FakeEtcdGateway.cs` копией фейка из
`src/tests/S3Worker.UnitTests/Fakes/` если есть, либо простым классом,
реализующим интерфейс словарём ключ→значение и списком отказов). Кейсы
(AAA, по-русски): пустой `/s3/clusters/` → Failed с `EtcdBackupMarkers.Missing`
в сообщении; config с etcd_backup=true + TO_REMOVE → маркер; битые secrets
(нет ключа backups) → маркер; пустой endpoints-ключ → маркер; валидный контур
→ Success: Endpoints = http://хост:порт, креды из backups, `Bucket ==
EtcdSnapshotBucket.Name`; `ParseEndpoints`/`ParseBackupsSecrets` — табличные
кейсы битых входов.

### Шаг 9. SnapshotLoop + интеграции слепков

**Вход:** шаг 8 слит; `SnapshotJob` (Shared.Etcd.Maintenance) принимает sink
опционально — EtcdWorker подключает sink КАК НЕ БЫЛО: цикл сам снимает и сам
экспортирует.

**Действие (кодер):** пишет `EtcdWorker.App/Loops/SnapshotLoop.cs` (по образцу
PgWorker SnapshotLoop + расписание с детекцией изменений): лидерство
ClaimStore `/etcdworker/leader`; последовательность тика лидера: CatchUpAsync
(до снятия) → решает `SnapshotSchedule.ShouldTake(...)` → снятие
`SnapshotJob.TakeAsync` (retention local) → чтение файла
`File.ReadAllBytesAsync(path)` → `SnapshotExporter.ExportAsync(fileName, data,
revision, ct)` (revision — `StatusAsync` failover best-effort) → повторный
расчёт отставания (свежий ReadStatusAsync + LatestLocalFile) → maintenance
`MaintainAsync` после успешного снятия → сон: отставание/FAILED →
`RetryIntervalSec`; иначе `ScanIntervalSec` (короткий пульсирующий —
`PulsingDelay.SleepAsync` с чанком < CheckIntervalSec и
`health.MarkSnapshotActivity`). Чистая функция
`SnapshotSchedule.ShouldTake(long nowUnix, long? lastTakeUnix, long?
lastUploadedUnix, long currentRevision, long? lastExportedRevision, int
intervalMin, int minIntervalSec)` (EtcdWorker.Core): плановый
`now - lastTake ≥ intervalMin*60` (нет снятий — true); внеочередной
`currentRevision > lastExportedRevision && now - lastUploaded ≥ minIntervalSec`
(нет выгрузки — true). Метки revision в статике цикла (прецедент
`SnapshotJob._lastMaintenanceUtc`). Подключает в Program: `SnapshotJob`
(retentionFiles, maintenanceIntervalMin), `EtcdBackupTargetResolver`,
`SnapshotExporter` (clientFactory → `new EtcdSnapshotS3(t.Endpoints[0],
t.Bucket, ...)` — первый endpoint списка; CaBundlePath из опций), SnapshotLoop
+ health-check + `snapshot-export` в healthz (HealthState.MarkSnapshotExport /
`EtcdWorkerHealth`). Интеграции (`EtcdWorker.IntegrationTests/Snapshots/`):
расширяет `EtcdWorkerEnvironment` ownS3-нодой (по образцу
`S3ClusterFixture`/`E2eEnvironment`: контейнер owns3:dev в сети сценария,
динамический порт, skip-if-unavailable, root-креды) и хелпером засева
`/s3/clusters/eb/config|endpoints|secrets` (config
`{"platform":"owns3","nodes":1,"etcd_backup":true,"created_unix":...}`;
endpoints `host:порт`; secrets c ключом backups); кейсы (перенос
`E2eEtcdSnapshotExportScenarios` + новые): плановая пара db+meta в
`etcd-backups` + статус OK + sha256 совпадает; транзиент цели (битый endpoints)
→ FAILED+error → восстановление → CatchUp догоняет; ретенция пар
(RetentionObjects мал, напр. 2); maintenance (compact/defrag — по факту
revision/DbSize статуса); детекция изменений (IntervalMin огромный,
MinIntervalSec мал: put-ключ в etcd → revision вырос → внеочередной слепок;
тишина → один).

**Выход:** SnapshotLoop-цикл полный; интеграции слепков зелёные; healthz
`snapshot-export` живой.

**Проверка:** юниты `SnapshotSchedule` зелёные;
`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/EtcdWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~Snapshots"` — зелёные; зачистка серии (`docker ps -a | grep etcdw-` пусто).

**Связь со spec:** §3.4 SnapshotLoop (расписание/детекция/Retry-сон/
maintenance), §7.1 (детекция — юниты), §7.2-слепки, решение 4.

**Подпункты (слабой модели):**

9.1. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/SnapshotSchedule.cs`:
```csharp
namespace EtcdWorker.Core;

/// <summary>Чистое решение тика SnapshotLoop (arch/25 §4): плановый слепок
/// раз в IntervalMin; внеочередной — revision вырос относительно последней
/// выгрузки И выгрузка старше MinIntervalSec (RPO ≈ 5 мин при активности,
/// без лишних слепков в тишине).</summary>
public static class SnapshotSchedule
{
    public static bool ShouldTake(
        long nowUnix,
        long? lastTakeUnix,
        long? lastUploadedUnix,
        long currentRevision,
        long? lastExportedRevision,
        int intervalMin,
        int minIntervalSec)
    {
        // Плановый: никогда не снимали либо прошло ≥ IntervalMin.
        if (lastTakeUnix is not { } taken || nowUnix - taken >= intervalMin * 60L)
            return true;

        // Внеочередной: есть изменения и выгрузка отстала ≥ MinIntervalSec
        // (никогда не выгружали — снимаем).
        return lastUploadedUnix is not { } uploaded
            ? true
            : currentRevision > (lastExportedRevision ?? long.MinValue)
              && nowUnix - uploaded >= minIntervalSec;
    }
}
```

9.2. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/App/SnapshotScheduleTests.cs`
(xunit, FluentAssertions, AAA-комментарии по-русски): факты — `NeverTaken_Planned`,
`WithinInterval_NoChanges_False`, `WithinInterval_RevisionGrewAndUploadStale_True`,
`WithinInterval_RevisionGrewButUploadFresh_False`, `NoUploadEver_True`,
`IntervalElapsed_True` (граничные значения: ровно intervalMin*60 — true;
ровно minIntervalSec — true).

Тесты интеграций и SnapshotLoop — сильная модель (по спецификации шага).

---

## Фаза 5 — Состав (spec §3.4 Membership/Supervisor, §3.5, §4.5)

### Шаг 10. Домен Core: member-модель, placement, spec узла

**Вход:** фазы 1–4 слиты; `EtcdMember.IsLearner` доступен.

**Действие (кодер):** пишет в EtcdWorker.Core чистые функции домена
(`EtcdContourMembers` уже существует с шага 5 — единый managed/external-детект):
- `ManagedNodeStates` (константы строк состояний + `IsTerminal`);
- `MembershipDecisions.NextNumber(IReadOnlyList<int> existing)` → max+1 (пусто → 1);
- `PlacementPlanner.PickHost(IReadOnlyList<string> swarmNodes,
  IReadOnlyList<string> placedOnExisting)` → Result<string>: кандидаты без
  managed-узлов (анти-аффинити); пусто → least-loaded (min вхождений в
  placedOnExisting), tie-break — имя Ordinal; пустой список нод → Failed;
- `EtcdNodeSpecBuilder.Build(int k, string? advertisedHost, int clientHostPort)`
  → `(string[] Cmd, IReadOnlyDictionary<string,string> Env)`: etcd-флаги узла
  (arch/04 §3 + spec §3.4): `--name etcdw-node-<k> --data-dir /etcd-data
  --listen-peer-urls http://0.0.0.0:2380 --listen-client-urls http://0.0.0.0:2379
  --initial-advertise-peer-urls http://etcdw-node-<k>:2380
  --advertise-client-urls http://etcdw-node-<k>:2379,http://<advertised-host>:<client-port>
  --initial-cluster-state existing --heartbeat-interval 250
  --election-timeout 2000 --auto-compaction-retention 1
  --quota-backend-bytes 8589934592`; при `advertisedHost == null` клиентский
  URL-элемент — только `http://etcdw-node-<k>:2379`; Env — пустой (все флаги
  в Cmd). Юниты: все функции (детерминизм placement, паттерн managed,
  грани парсинга K, флаги узла 1:1).

**Выход:** домен состава — чистые функции с юнитами; spec узла зафиксирован тестами.

**Проверка:** `dotnet test src/tests/EtcdWorker.UnitTests -c Release --no-build --filter "FullyQualifiedName~PlacementPlanner|FullyQualifiedName~EtcdNodeSpecBuilder|FullyQualifiedName~MembershipDecisions"` — зелёные (EtcdContourMembers — с шага 5).

**Связь со spec:** §3.4 (флаги, паттерн, гварды), §3.5 (модель узла),
решения 8/13/14, §7.1 («формирование env/advertise узла», placement).

**Подпункты (слабой модели):**

10.1. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/ManagedNodeStates.cs`:
```csharp
namespace EtcdWorker.Core;

/// <summary>Значения /etcd/nodes/&lt;k&gt;/state (arch/25 §5) — семантика
/// kafka/valkey/s3.</summary>
public static class ManagedNodeStates
{
    public const string NotInitialized = "NOT_INITIALIZED";
    public const string Provisioning = "PROVISIONING";
    public const string Running = "RUNNING";
    public const string Unreachable = "UNREACHABLE";
    public const string Removing = "REMOVING";
}
```

10.2. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/MembershipDecisions.cs`:
```csharp
namespace EtcdWorker.Core;

/// <summary>Чистые решения доводки состава (arch/25 §3.1).</summary>
public static class MembershipDecisions
{
    /// <summary>Номер нового узла: максимальный существующий + 1 (имена не
    /// переиспользуются, сквозная нумерация); пусто → 1.</summary>
    public static int NextNumber(IReadOnlyList<int> existingNumbers)
        => existingNumbers.Count == 0 ? 1 : existingNumbers.Max() + 1;
}
```

10.3. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/PlacementPlanner.cs`:
```csharp
using Shared.Core;

namespace EtcdWorker.Core;

/// <summary>Размещение managed-узла (arch/25 §2; Swarm): анти-аффинити —
/// swarm-ноды без managed-узлов; исчерпаны → least-loaded (минимум занятых
/// слотов), tie-break — имя Ordinal (детерминизм).</summary>
public static class PlacementPlanner
{
    public static Result<string> PickHost(
        IReadOnlyList<string> swarmNodes, IReadOnlyList<string> placedOnExisting)
    {
        if (swarmNodes.Count == 0)
            return Result<string>.Failed(new ApplicationException(
                "placement: no swarm nodes available (arch/25 §2)"));

        var free = swarmNodes
            .Where(n => !placedOnExisting.Contains(n, StringComparer.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        if (free.Count > 0)
            return Result<string>.Success(free[0]);

        var leastLoaded = swarmNodes
            .GroupBy(n => placedOnExisting.Count(p => string.Equals(p, n, StringComparison.Ordinal)))
            .OrderBy(g => g.Key)
            .First()
            .OrderBy(n => n, StringComparer.Ordinal)
            .First();
        return Result<string>.Success(leastLoaded);
    }
}
```

10.4. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/EtcdNodeSpecBuilder.cs`:
```csharp
namespace EtcdWorker.Core;

/// <summary>Флаги managed-узла etcd (arch/04 §3, arch/25 §2): peer — только
/// имя узла сети; client — host-публикация; initial-cluster-state=existing
/// (член добавлен member-API ДО старта).</summary>
public static class EtcdNodeSpecBuilder
{
    public static (string[] Cmd, IReadOnlyDictionary<string, string> Env) Build(
        int k, string? advertisedHost, int clientHostPort)
    {
        var clientAdvertise = advertisedHost is { Length: > 0 } host
            ? $"http://etcdw-node-{k}:2379,http://{host}:{clientHostPort}"
            : $"http://etcdw-node-{k}:2379";
        string[] cmd =
        [
            "etcd",
            $"--name=etcdw-node-{k}",
            "--data-dir=/etcd-data",
            "--listen-peer-urls=http://0.0.0.0:2380",
            "--listen-client-urls=http://0.0.0.0:2379",
            $"--initial-advertise-peer-urls=http://etcdw-node-{k}:2380",
            $"--advertise-client-urls={clientAdvertise}",
            "--initial-cluster-state=existing",
            "--heartbeat-interval=250",
            "--election-timeout=2000",
            "--auto-compaction-retention=1",
            "--quota-backend-bytes=8589934592", // 8 GiB (arch/04 §3)
        ];
        return (cmd, new Dictionary<string, string>());
    }
}
```

10.5. Создай файлы юнитов в
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/Core/`:
`MembershipDecisionsTests.cs`
(пусто → 1; [1,2] → 3; [1,3] → 4 — сквозная нумерация), `PlacementPlannerTests.cs`
(свободные ноды — первая по Ordinal; все заняты → least-loaded; tie → Ordinal;
пустой список → Failed), `EtcdNodeSpecBuilderTests.cs` (с advertisedHost —
client-список из двух URL ровно в порядке «имя, host:порт»; без — один URL;
initial-cluster-state=existing; heartbeat/election/quota — точные значения).
Все тесты — xunit + FluentAssertions + AAA-комментарии по-русски
(EtcdContourMembersTests — уже с шага 5).

### Шаг 11. Docker-драйверы + portalloc + EtcdNodeProvisioner

**Вход:** шаг 10 слит; `Shared.Docker` (DockerEngineFactory, ContainerSpec,
PortMap, ServiceSpec) и образцы Plain/Swarm-драйверов S3Worker изучены.

**Действие (кодер):** пишет `EtcdWorker.Docker/Drivers/EtcdNodeDriver.cs`
(интерфейс `IEtcdNodeDriver` + `PlainEtcdNodeDriver`/`SwarmEtcdNodeDriver` по
образцу S3Worker-драйверов с отличиями домена):
`EnsureNetworkAsync` (Plain — bridge `Docker:Network`: create, конфликт-409 =
успех; Swarm — overlay; сеть уже есть → no-op), `GetBusyPortsAsync` (docker
публикации), `GetSwarmNodesAsync` (Swarm — ready-ноды по Hostname; Plain —
список из одного Hosts[0].Name), `EnsureNodeAsync(EtcdNodeSpec)`,
`RemoveNodeAsync(int k, bool removeVolume)`, `InspectNodeAsync(int k)` →
`(string? PlacedOn, bool Running, int? ClientHostPort)`,
`ListNodeObjectsAsync()` (контейнеры/сервисы + тома по префиксу `etcdw-node-`
с label `etcdworker`). `EtcdNodeSpec(int K, string Host, int ClientHostPort,
string Image, string[] Cmd, IReadOnlyDictionary<string,string> Env, string
Network)`. Особенности против S3Worker: контейнер без host-публикации peer
(порт публикуется ТОЛЬКО client: `new PortMap(2379, ClientHostPort)`); Swarm —
`ServiceSpec` с constraint по Hostname==Host (образец); volume
`etcdw-node-<k>-data` → `/etcd-data`. В EtcdWorker.Provisioning:
`EtcdPortAllocStore` (ключ `/etcdworker/portalloc`, JSON
`{"<k>":{"host","client"}}`; под глобальным клэймом `PortAllocLock`:
`AllocateAsync(int k, string host, CancellationToken)` — RMW txn по
mod_revision: читает ключ, выбирает наименьший свободный client-порт из
PortRange, не занятый записями ключа (занятость docker-публикаций передаёт
вызывающий: `busyPorts`), пишет запись; проигрыш txn → повтор (цикл до
успеха/бюджета 3 попытки → Failed transient); `ReleaseAsync(int k)` — RMW
удаление поля; `ReadAllAsync()`); `EtcdNodeProvisioner` (EnsureNode по spec:
EnsureNetwork → EnsureVolume → контейнер/сервис; health-поллинг
`StatusAsync(http://<host-alias>:<clientPort>)` — точнее: клиентский host-порт
двигателя; поллинг-итерации с Mark/elapsed-логом до бюджета NodeBootSec).

**Выход:** создание/снос/инспекция узлов и порт-механика готовы к процессам.

**Проверка:** юниты `EtcdPortAllocStore` (free-порт отбор, конфликт-ретраи,
release) зелёные; сборка зелёная (docker-код покрыт интеграциями шага 13).

**Связь со spec:** §3.4 (portalloc под клэймом, занятость), §3.5 (сеть,
флаги, имена), решения 17/18, §7.1.

**Подпункты (слабой модели):**

11.1. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Docker/Drivers/EtcdNodeDriver.cs`:
интерфейс и спецификации — по образцу
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/S3Worker.Docker/Drivers/ClusterDriver.cs`
(прочитай его). Общие типы движка — `Shared.Docker` (`DockerEngineFactory`,
`IDockerEngine`, `ContainerSpec`, `PortMap`, `ServiceSpec` — доступны через
Using в csproj); `HostEndpoint` определён в сборке S3Worker.Docker, на которую
EtcdWorker.Docker НЕ ссылается — заведи собственную запись ПЕРВОЙ строкой
файла (namespace `EtcdWorker.Docker.Drivers`):
```csharp
/// <summary>Хост docker-топологии: имя + endpoint Engine API
/// (tcp://host:2375 | unix:///var/run/docker.sock).</summary>
public sealed record HostEndpoint(string Name, string Endpoint);
```
и далее интерфейс:
```csharp
/// <summary>Спецификация managed-узла etcd (arch/25 §2): имя etcdw-node-&lt;k&gt;,
/// host-публикация ТОЛЬКО client-порта; peer — имена сети контура.</summary>
public sealed record EtcdNodeSpec(
    int K,
    string Host,
    int ClientHostPort,
    string Image,
    string[] Cmd,
    IReadOnlyDictionary<string, string> Env,
    string Network);

/// <summary>Факт узла: размещение/живость/фактический client host-порт.</summary>
public sealed record EtcdNodeInspection(string? PlacedOn, bool Running, int? ClientHostPort);

public interface IEtcdNodeDriver
{
    Task<Result> EnsureNetworkAsync(CancellationToken ct);
    Task<Result<IReadOnlySet<int>>> GetBusyPortsAsync(CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> GetSwarmNodesAsync(CancellationToken ct);
    Task<Result> EnsureNodeAsync(EtcdNodeSpec spec, CancellationToken ct);
    Task<Result> RemoveNodeAsync(int k, bool removeVolume, CancellationToken ct);
    Task<Result<EtcdNodeInspection?>> InspectNodeAsync(int k, CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> ListNodeObjectsAsync(CancellationToken ct);
}
```
Реализации `PlainEtcdNodeDriver(IReadOnlyList<HostEndpoint> hosts,
DockerEngineFactory factory, string network)` и
`SwarmEtcdNodeDriver(string managerEndpoint, DockerEngineFactory factory,
IReadOnlyList<HostEndpoint>? hosts, string network)` — механика 1:1 с
`PlainClusterDriver`/`SwarmClusterDriver` образца с заменами: имя
контейнера/сервиса `$"etcdw-node-{k}"`, том `$"etcdw-node-{k}-data"` →
`/etcd-data`, label-пара `("etcdworker", "member")`, порты —
`[new PortMap(2379, spec.ClientHostPort)]` (ТОЛЬКО client), restart-политика
`unless-stopped`, `Network: spec.Network` (Plain; Swarm — сеть в service spec
если образец её несёт, иначе только constraint по `Hostname == spec.Host` по
образцу), `EnsureNetworkAsync`: Plain — `engine.EnsureNetworkAsync(network)`
(если такой метод Shared.Docker отсутствует — реализуй create через движок:
конфликт уже-существует → успех; Swarm — overlay-создание аналогично; посмотри
фактические методы `IDockerEngine` в `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/Shared.Docker/`
и используй существующие), `GetSwarmNodesAsync`: Plain → `[hosts[0].Name]`;
Swarm — ready-ноды по Hostname (образец `GetHostsAsync` SwarmClusterDriver),
`ListNodeObjectsAsync` — префикс `etcdw-node-` + label-фильтр + тома
`etcdw-node-*-data` (образец).

11.2. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Provisioning/EtcdPortAllocStore.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using Shared.Etcd.Client;

namespace EtcdWorker.Provisioning;

/// <summary>Закрепление client host-портов managed-узлов (arch/25 §5): один
/// JSON-ключ /etcdworker/portalloc {"<k>":{"host","client"}}; RMW по
/// mod_revision ПОД глобальным клэймом PortAllocLock (занятость дополнительно
/// сверяется с docker-публикациями вызывающего).</summary>
public sealed class EtcdPortAllocStore(IEtcdGateway gateway, string[] endpoints)
{
    public const string Key = "/etcdworker/portalloc";

    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase, // {"host":...,"client":...}
        WriteIndented = false,
    };

    public sealed record NodePort(string Host, int Client);

    public async Task<Result<IReadOnlyDictionary<int, NodePort>>> ReadAllAsync(CancellationToken ct)
    {
        var kv = await GetAsync(ct);
        if (!kv.IsSuccess)
            return Result<IReadOnlyDictionary<int, NodePort>>.Failed(kv.Error!);
        if (kv.Value is null)
            return Result<IReadOnlyDictionary<int, NodePort>>.Success(
                new Dictionary<int, NodePort>());
        try
        {
            using var doc = JsonDocument.Parse(kv.Value.Value);
            var map = new Dictionary<int, NodePort>();
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                if (!int.TryParse(prop.Name, out var k)
                    || !prop.Value.TryGetProperty("host", out var host)
                    || host.ValueKind != JsonValueKind.String
                    || !prop.Value.TryGetProperty("client", out var client)
                    || !client.TryGetInt32(out var port))
                    continue; // битая запись — пропустить (толерантный читатель)
                map[k] = new NodePort(host.GetString()!, port);
            }
            return Result<IReadOnlyDictionary<int, NodePort>>.Success(map);
        }
        catch (JsonException e)
        {
            return Result<IReadOnlyDictionary<int, NodePort>>.Failed(
                new ApplicationException($"{Key}: broken portalloc JSON: {e.Message}", e));
        }
    }

    /// <summary>Закрепить client-порт для узла k: наименьший свободный порт
    /// диапазона [from,to], не занятый записями ключа и busyPorts. RMW-txn по
    /// mod_revision; проигрыш — повтор (до 3 попыток → transient-Failed).</summary>
    public async Task<Result<NodePort>> AllocateAsync(
        int k, string host, int from, int to, IReadOnlySet<int> busyPorts, CancellationToken ct)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var kv = await GetAsync(ct);
            if (!kv.IsSuccess)
                return Result<NodePort>.Failed(kv.Error!);
            // Разбор текущих значений (номера → {host,client}).
            var current = new Dictionary<int, NodePort>();
            var modRevision = 0L;
            if (kv.Value is { } existing)
            {
                modRevision = (long)existing.ModRevision;
                try
                {
                    using var doc = JsonDocument.Parse(existing.Value);
                    foreach (var prop in doc.RootElement.EnumerateObject())
                    {
                        if (int.TryParse(prop.Name, out var kk)
                            && prop.Value.TryGetProperty("host", out var h)
                            && h.ValueKind == JsonValueKind.String
                            && prop.Value.TryGetProperty("client", out var c)
                            && c.TryGetInt32(out var p))
                            current[kk] = new NodePort(h.GetString()!, p);
                    }
                }
                catch (JsonException e)
                {
                    return Result<NodePort>.Failed(
                        new ApplicationException($"{Key}: broken portalloc JSON: {e.Message}", e));
                }
            }

            var taken = current.Values.Select(v => v.Client).Concat(busyPorts).ToHashSet();
            var port = Enumerable.Range(from, to - from + 1).FirstOrDefault(p => !taken.Contains(p));
            if (port == 0)
                return Result<NodePort>.Failed(new ApplicationException(
                    $"{Key}: no free client port in [{from},{to}]"));

            current[k] = new NodePort(host, port);
            var payload = Serialize(current);
            var txn = await TxnAsync(
                kv.Value is null
                    ? TxnRequest.Of([TxnCompare.NotExists(Key)], [new TxnOp.Put(Key, payload, null)])
                    : TxnRequest.Of([TxnCompare.ModRevisionEqual(Key, modRevision)],
                        [new TxnOp.Put(Key, payload, null)]),
                ct);
            if (!txn.IsSuccess)
                return Result<NodePort>.Failed(txn.Error!);
            if (txn.Value.Succeeded)
                return Result<NodePort>.Success(current[k]);
            // гонка — повтор RMW
        }

        return Result<NodePort>.Failed(new ApplicationException(
            $"{Key}: allocate race lost 3 times — retried next tick"));
    }

    /// <summary>Снять закрепление узла k (RMW; отсутствующий узел — успех).</summary>
    public async Task<Result> ReleaseAsync(int k, CancellationToken ct)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var kv = await GetAsync(ct);
            if (!kv.IsSuccess)
                return Result.Failed(kv.Error!);
            if (kv.Value is null)
                return Result.Success();
            var modRevision = (long)kv.Value.ModRevision;
            Dictionary<int, NodePort> current;
            try
            {
                current = Parse(kv.Value.Value);
            }
            catch (JsonException e)
            {
                return Result.Failed(new ApplicationException($"{Key}: broken JSON: {e.Message}", e));
            }

            if (!current.Remove(k))
                return Result.Success();
            var txn = await TxnAsync(
                TxnRequest.Of([TxnCompare.ModRevisionEqual(Key, modRevision)],
                    [new TxnOp.Put(Key, Serialize(current))]),
                ct);
            if (!txn.IsSuccess)
                return Result.Failed(txn.Error!);
            if (txn.Value.Succeeded)
                return Result.Success();
        }

        return Result.Failed(new ApplicationException($"{Key}: release race lost 3 times"));
    }

    private static Dictionary<int, NodePort> Parse(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        var map = new Dictionary<int, NodePort>();
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            if (int.TryParse(prop.Name, out var k)
                && prop.Value.TryGetProperty("host", out var h)
                && h.ValueKind == JsonValueKind.String
                && prop.Value.TryGetProperty("client", out var c)
                && c.TryGetInt32(out var p))
                map[k] = new NodePort(h.GetString()!, p);
        }
        return map;
    }

    private static string Serialize(IReadOnlyDictionary<int, NodePort> map)
        => JsonSerializer.Serialize(map.ToDictionary(
            p => p.Key.ToString(System.Globalization.CultureInfo.InvariantCulture),
            p => p.Value), Json);

    private async Task<Result<Kv?>> GetAsync(CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var got = await gateway.GetAsync(endpoint, Key, ct);
            if (got.IsSuccess)
                return got;
            last = got;
        }
        return Result<Kv?>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var txn = await gateway.TxnAsync(endpoint, req, ct);
            if (txn.IsSuccess)
                return txn;
            last = txn;
        }
        return Result<TxnResult>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}
```

11.3. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/Provisioning/EtcdPortAllocStoreTests.cs`
на фейк-гейтвее (шаг 8.5): Allocate на пустом ключе → наименьший свободный;
учёт busyPorts (docker) — не выбирается занятый; повторный allocate нового k —
следующий порт; RMW-гонка (фейк подменяет значение между Get и Txn) → ретраи
до успеха; Release — поле исчезает, повтор — успех; JSON-формат записи —
ровно `{"1":{"host":"h","client":18100}}`. AAA, комментарии по-русски.

### Шаг 12. MembershipProcess: решения + процесс

**Вход:** шаг 11 слит; `MemberAdd/Remove/Promote` и `EtcdContourMembers` готовы.

**Действие (кодер):** пишет чистые решения в
`EtcdWorker.Core/MembershipDecisions` (дополняет): `PickRemoveCandidate(
IReadOnlyList<ManagedMember> managed, ulong? leaderId)` → `ManagedMember?`:
старейший по K с `Member.Id != leaderId`; все — лидеры/пусто → null;
`QuorumRemoveGuard(int totalMembers)` → `totalMembers - 1 >= 3`;
`DetectDeadReplacement(IReadOnlyList<ManagedDeadFact> facts, int nodeDeadSec,
ulong? leaderId, long nowUnix)` → `ManagedMember?` (ManagedDeadFact:
`ManagedMember Member, long? ProblemSinceUnix, bool EnvironmentMissing` —
метка первого наблюдения проблемы члена (unstarted ИЛИ health-молчание —
трек supervisor'а, шаг 13) + факт «окружение не восстановлено»; замена когда
проблема наблюдается ≥ NodeDeadSec — порог для ОБОИХ случаев — и member ≠
leader). Пишет
`EtcdWorker.Provisioning/Processes/MembershipProcess.cs`: тик (только лидер
ReconcileLoop, вход: snapshot member list + status + supervisor-факты):
кворум-гейт (status.LeaderMemberId null → transient-выход); журнал перед
манипуляциями (`EtcdWorkJournal`, фазы `add-learner`/`promoting`/`removing`/
`replacing`); чтение `/etcd/config` — `EtcdConfigJson` (шаг 5.4); недобор:
NextNumber → placement (Plain — единственный хост;
Swarm — PickHost) → portalloc AllocateAsync (busyPorts драйвера) → put
листов `/etcd/nodes/<k>/state=NOT_INITIALIZED`+`placed_on` (ключи —
`EtcdDomainKeys`) → MemberAddAsync
(isLearner=true, peerUrls=[http://etcdw-node-<k>:2380]) → state=PROVISIONING →
EnsureNode (EtcdNodeProvisioner: флаги EtcdNodeSpecBuilder, сеть контура,
client-порт) → health-поллинг до NodeBootSec (iter + Mark + elapsed-лог) →
MemberPromoteAsync-ретраи тиками (not-in-sync — transient, состояние
`promoting` живёт в /etcd/nodes/<k>/state=PROVISIONING) → state=RUNNING; провал
бюджета → REMOVING: MemberRemoveAsync → снос → чистка state/placed_on/portalloc
→ retry следующим тиком новым номером; перебор: PickRemoveCandidate +
QuorumRemoveGuard → MemberRemoveAsync → снос+volume → чистка; авто-замена:
DetectDeadReplacement (факты supervisor) → тот же путь remove → недобор.
Идемпотентность: каждый шаг перепроверяет факт (member в списке? контейнер
есть? порт закреплён?).

**Выход:** процесс доводки состава реализован с журналом и гвардами.

**Проверка:** юниты решений (PickRemoveCandidate старейший-не-лидер; гвард 3;
DetectDeadReplacement порог/лидер-гвард) зелёные.

**Связь со spec:** §3.4 MembershipProcess (недобор/перебор/замена/гварды/
журнал), решения 2/15/16, §7.1.

**Подпункты (слабой модели):**

12.1. Дополни файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/EtcdWorker.Core/MembershipDecisions.cs`
(после `NextNumber`) тремя функциями и записью:
```csharp
/// <summary>Факт наблюдения supervisor за managed-узлом: ProblemSinceUnix —
/// метка ПЕРВОГО наблюдения проблемы члена (unstarted ИЛИ health-молчание;
/// null — проблем нет); EnvironmentMissing — окружение не восстановлено
/// (контейнер отсутствует/машина недоступна/volume потерян).</summary>
public sealed record ManagedDeadFact(
    ManagedMember Member, long? ProblemSinceUnix, bool EnvironmentMissing);

/// <summary>Кандидат вывода (arch/25 §3.1): старейший по номеру НЕ лидер;
/// null — кандидата нет (пусто/все — лидер).</summary>
public static ManagedMember? PickRemoveCandidate(
    IReadOnlyList<ManagedMember> managed, ulong? leaderId)
    => managed
        .Where(m => leaderId is not { } leader || m.Member.Id != leader)
        .OrderBy(m => m.K)
        .FirstOrDefault();

/// <summary>Кворум-гвард remove: после вывода останется ≥ 3 членов.</summary>
public static bool QuorumRemoveGuard(int totalMembersAfterRemove)
    => totalMembersAfterRemove >= 3;

/// <summary>Детект авто-замены (решение 2, arch/25 §3.1): проблема члена
/// (unstarted ИЛИ health-молчание) наблюдается ≥ NodeDeadSec И окружение не
/// восстановлено; не лидер. Порог — для ОБОИХ случаев: unstarted-ветка тоже
/// гейтится меткой первого наблюдения (ProblemSinceUnix) — немедленной
/// замены нет; метку трекает supervisor (шаг 13).</summary>
public static ManagedMember? DetectDeadReplacement(
    IReadOnlyList<ManagedDeadFact> facts, int nodeDeadSec, ulong? leaderId, long nowUnix)
    => facts
        .Where(f => f.ProblemSinceUnix is { } since
                    && nowUnix - since >= nodeDeadSec
                    && f.EnvironmentMissing
                    && (leaderId is not { } leader || f.Member.Member.Id != leader))
        .OrderBy(f => f.Member.K)
        .FirstOrDefault();
```

12.2. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/EtcdWorker.UnitTests/Core/MembershipDecisionTests.cs`
(AAA, по-русски): PickRemoveCandidate — пусто → null; единственный и он
лидер → null; [k=1,k=3], лидер = k=1 → k=3; без лидера → k=1 (старейший);
QuorumRemoveGuard — 3 → true, 2 → false; DetectDeadReplacement — ProblemSinceUnix
299 с назад → null (порог не истёк, в т.ч. для unstarted-члена), 300 с назад +
EnvironmentMissing → кандидат; ProblemSinceUnix = null → null; 300 с назад, но
EnvironmentMissing=false (supervisor восстановил) → null; кандидат — лидер →
null; несколько кандидатов → минимальный K.

Процесс MembershipProcess — сильная модель (по спецификации шага; сложные
идемпотентные переходы — не механика для слабой модели).

### Шаг 13. NodeSupervisor + ReconcileLoop + интеграции состава

**Вход:** шаг 12 слит.

**Действие (кодер):** пишет
`EtcdWorker.Provisioning/Processes/NodeSupervisor.cs`: по каждому managed-узлу
(листы `/etcd/nodes/` — `EtcdNodesParser`, шаг 7.6; member list —
`EtcdContourMembers`): health-проба `StatusAsync` по
`http://<AdvertisedHost|host>:<client-port>` (portalloc-запись) раз в
HealthIntervalSec (внутритиковый гейт по времени); трек первого наблюдения
проблемы члена — unstarted-член (пустой name в member list) ИЛИ health-молчание
— словарь в памяти инстанса `Dictionary<int,long>` (ProblemSinceUnix для
`DetectDeadReplacement`, шаг 12; восстановление — сброс; деструктива нет) —
молчание > NodeDeadSec → state=UNREACHABLE + ретраи; один
repair-шаг за тик: контейнер узла отсутствует на живом окружении → пересоздание
через provisioner (volume жив; сверка имени+client-порта по InspectNodeAsync —
расхождение → remove контейнера + recreate, volume сохраняется);
orphan-cleanup: ListNodeObjectsAsync → объект с label `etcdworker`, чей номер
k не имеет члена в member list (EtcdContourMembers) → снос контейнера+volume,
журнал `orphan-cleanup`; слепая проба (docker-хост молчит) — никаких действий.
Пишет `EtcdWorker.App/Loops/ReconcileLoop.cs` (по образцу S3Worker
ReconcileLoop): лидерство `/etcdworker/leader` (клэйм ClaimStore — singleton),
тик: кворум-гейт (StatusAsync failover → LeaderMemberId) → чтение
`/etcd/config` (нет/N=0 — только наблюдение: health-пробы supervisor'а без
структурных операций) → MembershipProcess → NodeSupervisor; метрики
LoopTick/LoopDuration; watchdog-метки. Подключает в Program (драйвер по Mode,
процессы, ReconcileLoop + health-check). Интеграции состава
(`EtcdWorker.IntegrationTests/Membership/`, фикстура: сеть сценария + SEED
etcd-контейнер в ней — peer `http://seed:2380` НЕ публикуется, client
динамический; воркер-конфиг `Docker:Network` = сеть сценария, `Hosts[0]`
локальный docker): кейсы §7.2-состав: nodes=2 → 3 started-члена (learner →
promote; peer-URL всех — имена); nodes=1 → rolling-вывод (не-лидер);
авто-замена (снос контейнера+volume, NodeDeadSec 5–10 с) → новый номер;
orphan (member-API вручную remove члена существующего контейнера → снос);
нет etcd-backup кластера → статус FAILED маркер (если не покрыт шагом 9).
Самоочистка каждого кейса: свои контейнеры/тома/сеть/ключи префикса; ассерт чистоты.

**Выход:** ядро приёмки §7.2-состав продемонстрировано на docker.

**Проверка:** `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/EtcdWorker.IntegrationTests -c Release --no-build --filter "FullyQualifiedName~Membership"` — зелёные; `docker ps -a | grep etcdw-` пусто после серии; `docker network ls | grep etcdw-` пусто.

**Связь со spec:** §3.4 (NodeSupervisor/ReconcileLoop/классификация), §7.2-состав, решение 2.

Подпунктов нет (процессы/интеграции — сильная модель; вся механика ниже по
плану уже подготовлена шагами 10–12).

---

## Фаза 6 — Демонтаж PgWorker (spec §3.7, §4.6)

### Шаг 14. Изъятие зоны EtcdExport + зелёные серии

**Вход:** фазы 1–5 слиты (механика перенесена и покрыта у EtcdWorker).

**Действие (кодер):** удаляет из PgWorker: файлы
`src/PgWorker.Backups/EtcdExport/EtcdSnapshotSink.cs`,
`EtcdSnapshotStatus.cs`, `EtcdSnapshotMeta.cs`, `EtcdExportRetention.cs`,
`src/PgWorker.App/EtcdSnapshotSinkFactory.cs`; из `src/PgWorker.App/Options.cs`
— классы `SnapshotExportOptions`/`SnapshotExportS3Options` и свойство
`Export` из `SnapshotOptions` (+ поле `MaintenanceIntervalMin` из
`SnapshotOptions` — решение 19); из `src/PgWorker.App/Program.cs` — регистрацию
sink/snapshot-export-обвязки; из `src/PgWorker.App/Loops/SnapshotLoop.cs` —
CatchUp/behind-логику и `MaintainAsync`-вызов (остаётся: лидерский регулярный
локальный слепок + ретенция + пульсирующий сон); из
`src/PgWorker.App/HealthState.cs`/healthz — экспорт-поля, если были; из
`deploy/docker-compose.yml` — строки `PgWorker__Snapshots__Export__*`; из
`deploy/.env.example` — блок `PGW_SNAPSHOTS_S3_*`/`PGW_SNAPSHOTS_EXPORT_ENABLED`.
Правит тесты PgWorker: удаляет `SnapshotExportOptionsTests`,
`SnapshotSinkFactoryTests`, `SnapshotLoopExportTests` и юниты
EtcdExport-механики (перенесены шагами 7–8); `BackupsOptionsValidationTests` —
снимает Export-кейсы; интеграционные `E2eEtcdSnapshotExportScenarios.cs` —
удаляет (кейсы перенесены шагом 9; сверяет полноту переноса по списку Fact'ов).
Прогоняет серии: юниты PgWorker, интеграции PgWorker, E2E минимум
`Scale_AddEmptyShard` (после E2ePrepare) — PgWorker стартует без
`PGW_SNAPSHOTS_*`.

**Выход:** дублирующей механики в PgWorker нет; серии зелёные.

**Проверка:** `grep -rn "EtcdSnapshotSink\|SnapshotExport\|etcd-snapshots" src/PgWorker.App src/PgWorker.Backups deploy` — пусто; `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --no-build` зелёные; далее мерж-гейт-прогоны шага 18.

**Связь со spec:** §3.7 (полный список удалений), решения 6/19/24, §7.2-PgWorker.

**Подпункты (слабой модели):**

14.1. Удали файлы: `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.Backups/EtcdExport/EtcdSnapshotSink.cs`,
`.../EtcdExport/EtcdSnapshotStatus.cs`, `.../EtcdExport/EtcdSnapshotMeta.cs`,
`.../EtcdExport/EtcdExportRetention.cs`,
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.App/EtcdSnapshotSinkFactory.cs`,
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/PgWorker.UnitTests/App/SnapshotExportOptionsTests.cs`,
`.../App/SnapshotSinkFactoryTests.cs`, `.../App/SnapshotLoopExportTests.cs`,
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/PgWorker.IntegrationTests/E2e/E2eEtcdSnapshotExportScenarios.cs`.

14.2. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.App/Options.cs`:
удали классы `SnapshotExportOptions` и `SnapshotExportS3Options` целиком (с
XML-комментариями); из класса `SnapshotOptions` удали свойства `Export` и
`MaintenanceIntervalMin` (остаются `Dir`, `RetentionFiles`); XML-комментарий
класса замени на: «Локальные слепки P12 (arch/14 §8 после t32): каталог тома и
ретеншн файлов; выгрузка/обслуживание — зона EtcdWorker (arch/25 §4)».

14.3. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.App/Loops/SnapshotLoop.cs`:
удали параметр `EtcdSnapshotSink? exportSink = null` из конструктора и все
блоки `if (exportSink is not null)` (доводка до снятия, повторный расчёт
после, Retry-сон — сон всегда `SnapshotIntervalMin`), вызов
`snapshots.MaintainAsync(...)` с его лог-блоком, using
`PgWorker.Backups.EtcdExport`. Тело цикла остаётся: лидерство → TakeAsync →
метрики/healthz-метки → пульсирующий сон. XML-комментарий шапки дополни:
«(t32: выгрузка/обслуживание — зона EtcdWorker arch/25 §4)».

14.4. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/PgWorker.App/Program.cs`
найди grep'ом `EtcdSnapshotSink|SnapshotExport|EtcdSnapshotStatus` все строки
подключения sink (создание S3ClientOptions-цели, конструкция sink, передача в
SnapshotLoop) и удали их; регистрацию `SnapshotJob` приведи к вызову без
maintenance-аргумента (конструктор SnapshotJob допускает меньше аргументов —
проверь сигнатуру `src/Shared.Etcd/Maintenance/SnapshotJob.cs`; MaintenanceIntervalMin
в вызове больше не передаётся).

14.5. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/deploy/docker-compose.yml`
в блоке `x-pgworker-env` удали ТОЛЬКО Snapshots-Export-блок: четыре строки
комментария, начинающегося «# Экспорт etcd-снапшотов в S3 (t36, arch/14 §6/§8)…»,
и семь env-строк `PgWorker__Snapshots__Export__*` под ним (фактически строки
55–65; ориентируйся по содержимому `PgWorker__Snapshots__Export__` — строки
46–54, `PgWorker__Backups__{Enabled,S3__Bucket,S3__CaBundlePath,Job__Image}`,
легитимны и НЕ трогаются). В файле
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/deploy/.env.example`
удали строки с `PGW_SNAPSHOTS_S3_*`, `PGW_SNAPSHOTS_EXPORT_ENABLED` и
комментарий-абзац про них (строки ~101–107, сверься grep'ом).

Остальные правки тестов/сборки после удалений — сильная модель (список
упоминаний grep'ом `EtcdExport|EtcdSnapshotStatus|SnapshotExportOptions` по
`src/PgWorker.*` и `src/tests/PgWorker.*`; правка до зелёной сборки).

---

## Фаза 7 — Панель (spec §3.8, §4.7)

### Шаг 15. Модель, парсеры, refresher, алерты, воркер-грань

**Вход:** фазы 1–6 слиты; панель читает `/pgworker/etcd-snapshots` (устаревший
ключ — после фазы 6 ключа нет, панель молчит).

**Действие (кодер):** AdminPanel.Core: в `BackupsInfo.cs` правит
`EtcdSnapshotExportInfo` — удаляет поле `Enabled`; в `EtcdSnapshot.cs` —
добавляет `EtcdDomainInfo? EtcdDomain = null` и
`IReadOnlyList<WorkerEndpoint> EtcdWorkerEndpoints = []`; новый файл
`EtcdDomainInfo.cs` (`int? DeclaredNodes, long? CreatedUnix,
IReadOnlyList<ManagedNodeView> Nodes`; `ManagedNodeView(int K, string? State,
string? PlacedOn)`). AdminPanel.Etcd/Parsing: `EtcdSnapshotsParser` — ключ и
формат `/etcdworker/snapshots` (без enabled — битый state → parseError+null);
новый `EtcdDomainParser` (Kv[] префикса `/etcd/`: config JSON →
DeclaredNodes/CreatedUnix (битый → parseError), листы `nodes/<k>/{state,
placed_on}` → ManagedNodeView; unknown-листы — ignore). `SnapshotRefresher`:
читает `/etcd/config` + `/etcd/nodes/` (префикс), точечный
`/etcdworker/snapshots`, `/etcdworker/api/`; отказный тик — прежние значения
переживают (перенос в новый EtcdSnapshot с previous). Alerting: правила
`EtcdSnapshotExportFailedRule`/`EtcdSnapshotExportStaleRule` — источник
`snapshot.EtcdSnapshots` без проверки Enabled (молчание при отсутствии ключа);
новое `EtcdBackupClusterMissingRule` (`etcd-backup-cluster-missing`, critical:
S3Snapshot живого домена не содержит кластера с `EtcdBackup==true` и
`!ToRemove`; молчание, если S3-домен не читался). Workers:
`WorkerApiGateway.ResolveEndpoints("etcdworker")` — из EtcdSnapshotStore
(новый IEtcdDomainReader/store по образцу S3: `EtcdDomainStore` в
AdminPanel.Etcd + ModuleExtensions-регистрация); `WorkerCertService` —
etcdworker в перечень (3 switch + текст исключения); healthz-поллер —
etcdworker-ряд. Юниты панели: парсеры (толерантность/parseError), правила
(failed/stale/missing/молчание).

**Выход:** панельная модель домена etcdworker полна; юниты зелёные.

**Проверка:** `dotnet test src/tests/AdminPanel.UnitTests -c Release --no-build` — зелёные (новые+обновлённые).

**Связь со spec:** §3.8 (модель/парсеры/refresher/алерты/Воркеры), §7.1-панель.

**Подпункты (слабой модели):**

15.1. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/AdminPanel.Core/BackupsInfo.cs`
найди запись `EtcdSnapshotExportInfo` и удали первый параметр `bool Enabled`
(остальные — как есть; обнови XML-комментарий: «(t08 → t32: ключ
/etcdworker/snapshots, arch/25 §5; поле enabled удалено — выгрузка безусловна)»).

15.2. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/AdminPanel.Core/EtcdDomainInfo.cs`:
```csharp
namespace AdminPanel.Core;

/// <summary>Домен EtcdWorker (adminpanel/02 §2.3.5): декларация состава +
/// managed-узлы /etcd/nodes/*.</summary>
public sealed record EtcdDomainInfo(
    int? DeclaredNodes,
    long? CreatedUnix,
    IReadOnlyList<ManagedNodeView> Nodes);

/// <summary>Managed-узел: номер, состояние, размещение.</summary>
public sealed record ManagedNodeView(int K, string? State, string? PlacedOn);
```

15.3. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/AdminPanel.Core/EtcdSnapshot.cs`
добавь в конец списка параметров записи `EtcdSnapshot` (после `EtcdSnapshots`):
```csharp
    EtcdDomainInfo? EtcdDomain = null,                       // домен EtcdWorker /etcd/ (t32, 02 §2.3.5)
    IReadOnlyList<WorkerEndpoint> EtcdWorkerEndpoints = []); // живые ключи /etcdworker/api/ (t32, 02 §2.3.5)
```

15.4. Замени файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/AdminPanel.Etcd/Parsing/EtcdSnapshotsParser.cs`
содержимым (сохраняя namespace/стиль):
```csharp
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>Толерантный парсер ключа /etcdworker/snapshots (t32, arch/25 §5):
/// битый JSON/state → parseError-запись + null (правила молчат); отсутствующие
/// поля — null-компоненты; поле enabled из формата удалено.</summary>
public static class EtcdSnapshotsParser
{
    public static (EtcdSnapshotExportInfo? Info, IReadOnlyList<KeyParseError> Errors) Parse(Kv? kv)
    {
        if (kv is null)
            return (null, []);
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            var state = JsonValues.ReadString(root, "state");
            var uploaded = JsonValues.ReadLong(root, "last_uploaded_unix");
            var lastObject = JsonValues.ReadString(root, "last_object");
            var sha = JsonValues.ReadString(root, "last_sha256");
            var size = JsonValues.ReadLong(root, "size_bytes");
            var interval = JsonValues.ReadInt(root, "interval_min");
            var error = JsonValues.ReadString(root, "error");
            if (state is not null and not ("OK" or "FAILED"))
                return (null, [new KeyParseError(kv.Key, $"unknown export status state: {state}")]);
            return (new EtcdSnapshotExportInfo(state, uploaded, lastObject, sha, size, interval, error), []);
        }
        catch (System.Text.Json.JsonException ex)
        {
            return (null, [new KeyParseError(kv.Key, $"broken etcdworker snapshots status JSON: {ex.Message}")]);
        }
    }
}
```

15.5. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/AdminPanel.Etcd/Parsing/EtcdDomainParser.cs`
(по стилю EtcdSnapshotsParser; толерантность unknownKeys):
```csharp
using AdminPanel.Core;
using Shared.Etcd.Client;

namespace AdminPanel.Etcd.Parsing;

/// <summary>Парсер домена /etcd/ (t32, adminpanel/02 §2.3.5): config →
/// декларация; nodes/&lt;k&gt;/{state,placed_on} → managed-узлы. Битые значения —
/// parseError-записи, тик не падает; unknown-листы узлов игнорируются.</summary>
public static class EtcdDomainParser
{
    public static (EtcdDomainInfo? Domain, IReadOnlyList<KeyParseError> Errors) Parse(
        Kv? configKv, IReadOnlyList<Kv> nodeKvs)
    {
        var errors = new List<KeyParseError>();
        int? declared = null;
        long? created = null;
        if (configKv is { } cfg)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(cfg.Value);
                var root = doc.RootElement;
                if (root.ValueKind == System.Text.Json.JsonValueKind.Object)
                {
                    if (root.TryGetProperty("nodes", out var n) && n.ValueKind == System.Text.Json.JsonValueKind.Number && n.TryGetInt32(out var nodes))
                        declared = nodes;
                    if (root.TryGetProperty("created_unix", out var c) && c.ValueKind == System.Text.Json.JsonValueKind.Number && c.TryGetInt64(out var createdUnix))
                        created = createdUnix;
                }
                else
                    errors.Add(new KeyParseError(cfg.Key, "config is not a JSON object"));
            }
            catch (System.Text.Json.JsonException ex)
            {
                errors.Add(new KeyParseError(cfg.Key, $"broken /etcd/config JSON: {ex.Message}"));
            }
        }

        // nodes/<k>/{state,placed_on}: сегменты ["", "etcd", "nodes", <k>, <leaf>].
        var nodes = new Dictionary<int, ManagedNodeView>();
        foreach (var kv in nodeKvs)
        {
            var segments = kv.Key.Split('/');
            if (segments.Length != 5 || segments[1] != "etcd" || segments[2] != "nodes")
                continue;
            if (!int.TryParse(segments[3], out var k) || k <= 0)
                continue;
            var value = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim().Trim('"');
            switch (segments[4])
            {
                case "state":
                    nodes[k] = new ManagedNodeView(k, value, nodes.TryGetValue(k, out var byState) ? byState.PlacedOn : null);
                    break;
                case "placed_on":
                    nodes[k] = new ManagedNodeView(k, nodes.TryGetValue(k, out var byPlaced) ? byPlaced.State : null, value);
                    break;
                // прочие листы — unknown, не ошибка
            }
        }

        return (new EtcdDomainInfo(declared, created,
            nodes.Values.OrderBy(n => n.K).ToList()), errors);
    }
}
```

15.6. Обнови `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/AdminPanel.Core/Alerting/Rules/EtcdSnapshotExportFailedRule.cs`:
в `Evaluate` замени условие на `if (export is not { State: "FAILED" }) yield
break;` (проверка Enabled удалена); текст Hint/Remedy обнови на ключ
`/etcdworker/snapshots` и «EtcdWorker» вместо PGW-переменных (Remedy:
«docs/runbook.md §"Восстановление etcd из S3-выгрузки": check the
etcd-backup S3 cluster (/s3/clusters/, etcd_backup=true) and the status key
/etcdworker/snapshots; EtcdWorker completes the export on a RetryIntervalSec
tick»). Симметрично `EtcdSnapshotExportStaleRule` — убери проверку Enabled.

15.7. Юниты панели: обнови
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/src/tests/AdminPanel.UnitTests/EtcdSnapshotExportDtoTests.cs`
и `EtcdSnapshotExportRuleTests.cs` (конструкторы без Enabled; правило failed
срабатывает без enabled-поля; stale — по interval; оба молчат при null).
Создай `Parsing/EtcdDomainParserTests.cs` (config валидный/битый/отсутствует;
узлы state+placed_on; битовый номер K — игнор). AAA, по-русски.

Правило `EtcdBackupClusterMissingRule` и его юниты
(`Alerting/EtcdBackupClusterMissingRuleTests.cs`: нет etcd_backup → алерт;
есть → молчание; S3-домен не читался → молчание) — сильная модель (в
подпункты не выделены: конструкция зависит от того, как AlertEngine доставляет
S3-домен в правила — найди паттерн по правилам `BackupPoolMissingRule`/
`BackupS3UnreachableRule` и следуй ему; требование — critical-алерт
`etcd-backup-cluster-missing`, target `"etcd"`, условие «S3-домен прочитан И
нет живого (не TO_REMOVE) кластера с `EtcdBackup == true`», молчание при
нечитанном домене; текст — «etcd backup cluster is missing: no /s3/clusters/
config with etcd_backup=true — DR snapshots cannot be uploaded»).

Refresher/WorkerApiGateway/WorkerCertService/поллер — сильная модель (по
образцу S3-домена в ModuleExtensions/SnapshotRefresher: новый блок чтения
префиксов `/etcd/config`+`/etcd/nodes/`, `/etcdworker/snapshots`,
`/etcdworker/api/`, перенос в новый EtcdSnapshot с previous; регистрации
`EtcdDomainStore` НЕ заводится — endpoints etcdworker лежат в основном
EtcdSnapshot.EtcdWorkerEndpoints, Reader берёт из ISnapshotStore).

### Шаг 16. Панель API + фронтенд

**Вход:** шаг 15 слит; фронтенд `frontend/src/pages/EtcdPage.tsx` существует.

**Действие (кодер):** AdminPanel.Api: `Operations/Etcd/PutEtcdConfigCommand.cs`
(прокси по adminpanel/02 §9.13: `WorkerProxy.SendAsync<EtcdConfigPanelDto>(api,
"etcdworker", HttpMethod.Put, "/api/etcd/config", new { nodes }, requestedBy:
null, ct)`; коды 1:1) + маршрут в OperationsModule `PUT /api/etcd/config` (по
образцу S3-маршрутов); DTO inspection: `EtcdStatusQuery`/новый
`EtcdDomainQuery` → `/api/etcd/domain` (declared/created/nodes) + поле
etcdworker-endpoints в DTO воркеров (по образцу s3worker-рядов);
`BackupStorageQuery`-карточка etcd-снапшотов — источник EtcdSnapshots
(уже в снапшоте после шага 15 — сверяет DTO-поля, `enabled` убирает).
Фронтенд: `api/dto.ts` — `EtcdDomainDto` + расширение `EtcdStatusDto`;
`api/queries.ts` — `fetchEtcdDomain` + `usePutEtcdConfig` (мутация, двойной
клик защищён — `isPending`); `pages/EtcdPage.tsx` — блок «Состав» (заявлено N
или «только наблюдение», managed-таблица K/state-бейдж/placed_on, фактическое
число членов + пометки managed/external в MembersCard), карточка «DR-слепки»
(состояние/возраст/sha-префикс/размер/ошибка), модал «Поддерживать N узлов»
(NumberInput 0..7 + валидация + текст 409-ошибки из ProblemDetails).
Стиль — Mantine, по соседним страницам (S3ClustersPage — модалы).

**Выход:** полный UI состава + мутация через API воркера.

**Проверка:** сборка панели зелёная (docker/AdminPanel.Dockerfile собирает SPA);
ручная проверка необязательна — юниты/DTO шага 15 зелёные; фронтенд-сборка
без TS-ошибок (`cd frontend && npm run build > /tmp/t32-fe-build.log 2>&1` — в фоне).

**Связь со spec:** §3.8 (страница/форма/карточки/прокси §9.13), решение 5.

Подпункты не выделяются: правки разрозненные, требуют знания панельных
паттернов (сильная модель).

---

## Фаза 8 — deploy / dev-stand / мерж-гейт (spec §3.9, §4.8)

### Шаг 17. Dockerfile, deploy-compose, seed-рецепт, 00-up.sh

**Вход:** фазы 1–7 слиты; `docker/S3Worker.Dockerfile` — образец; порты
8087/8088 свободны (заняты 8080–8086/8089).

**Действие (кодер):** создаёт `docker/EtcdWorker.Dockerfile` (копия
S3Worker.Dockerfile: ARG ETCDW_PUBLISH_DIR, ENTRYPOINT EtcdWorker.App.dll);
`deploy/docker-compose.yml`: `x-etcdworker-env` (Endpoints 0..2 =
`ETCDW_ETCD_ENDPOINT_0..2` дефолты `http://host.docker.internal:{2379,2381,2383}`,
`EtcdWorker__Docker__Network` = `ETCDW_DOCKER_NETWORK:-etcdw-net`, docker-transport
`ETCDW_DOCKER_*`, TLS-API `ETCDW_API_TLS_*_PATH` по образцу s3w TLS-тома,
AdvertisedHost `ETCDW_ADVERTISED_HOST:-host.docker.internal`) +
`x-etcdworker-base` (build args `ETCDW_PUBLISH_DIR: artifacts/etcdworker/publish`,
image `etcdworker:dev`, том `etcdw-snapshots:/snapshots`, TLS-том) + сервисы
`etcdworker` (порт `${ETCDW_API_HOST_PORT:-8087}:8080`, AdvertiseUrl
`https://host.docker.internal:${ETCDW_API_HOST_PORT:-8087}`) и `etcdworker-2`
(`8088`); volume `etcdw-snapshots`. `deploy/.env.example`: ETCDW-блок
(закомментированные дефолты). `deploy/etcd/`: НОВЫЙ seed-рецепт (docker-compose.yml
+ etcd.env.example; старые файлы заменяются: NODE_IP/PEERS/host-network —
удаляются). `dev-stand/adminpanel/checks/00-up.sh`: publish
EtcdWorker.App → `artifacts/etcdworker/publish` ([PHASE]-строки по образцу
s3worker), env-порты `ETCDW_API_HOST_PORT{,2}` (8087/8088), compose up
дополняется `etcdworker etcdworker-2`, чек: healthz mTLS ×2 (до 60 с) + ключи
`/etcdworker/api/` ≥ 2 (по образцу s3w-блока 7c).

**Выход:** поставка и стенд поднимают etcdworker; seed-рецепт — новый.

**Проверка:** `bash dev-stand/adminpanel/checks/00-up.sh` (в фоне, лог в файл) —
чек etcdworker зелёный (healthz ×2, ключи /etcdworker/api/ ≥ 2), чек-строка
наблюдения статуса слепков выводится (значение ключа
`/etcdworker/snapshots` либо явное «no key» — критерий §7.3-стенда:
появление статуса при поднятом etcd-backup S3-кластере), существующие чеки не
сломаны; teardown стенда после прогона; `docker compose -f
deploy/etcd/docker-compose.yml config` валиден.

**Связь со spec:** §3.9 (все позиции), решения 9/10, §7.3-dev-стенд.

**Подпункты (слабой модели):**

17.1. Создай файл `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/docker/EtcdWorker.Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1
# Образ EtcdWorker (arch/25 §8): dotnet publish НА ХОСТЕ, в контейнер — ТОЛЬКО
# publish-вывод (runtime-слой; канон E2E-образов AGENTS.md — без sdk-стадии).
# CLI: dotnet publish src/EtcdWorker.App -c Release -o artifacts/etcdworker/publish
#      docker build -f docker/EtcdWorker.Dockerfile -t etcdworker:dev artifacts/etcdworker/publish
# Compose: context .., args { ETCDW_PUBLISH_DIR: artifacts/etcdworker/publish }
ARG ETCDW_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG ETCDW_PUBLISH_DIR
WORKDIR /app
COPY ${ETCDW_PUBLISH_DIR} ./
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
# /healthz за mTLS (arch/25 §1.1): клиентская пара healthcheck из TLS-пакета.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz || exit 1
ENTRYPOINT ["dotnet", "EtcdWorker.App.dll"]
```

17.2. ЗАМЕНИ ЦЕЛИКОМ файл
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/deploy/etcd/docker-compose.yml`:
```yaml
# Seed-узел etcd-контура контроль-плейна воркер-инсталляции (arch/04 §8 п.5,
# arch/25 §1): ПОДНИМАЕТСЯ ОПЕРАТОРОМ ОДИН раз для бутстрапа контура; состав
# до целевого доводит EtcdWorker (PUT /api/etcd/config nodes=N), после чего
# seed рекомендуется вывести (runbook, arch/04 §8 п.4).
#   docker network create etcdw-net            # перед первым стартом
#   sudo mkdir -p /opt/etcd-seed && cd /opt/etcd-seed
#   # скопировать docker-compose.yml + etcd.env (из etcd.env.example)
#   docker compose --env-file etcd.env up -d
# Адресация контура — ТОЛЬКО имена узлов в общей сети etcdw-net (arch/25 §2):
# peer managed-узлов http://etcdw-node-<k>:2380, client host-порты из
# PortRange EtcdWorker. Мультихост — Swarm (arch/04 §8 п.5, runbook).
# ВАЖНО: env_file передаёт переменные ТОЛЬКО внутрь контейнера; интерполяция
# ${…} в этом YAML (image/volumes/command) идёт из --env-file — запуск всегда
# `docker compose --env-file etcd.env up -d`.
services:
  etcd-seed:
    image: ${ETCD_IMAGE}
    container_name: ${NODE_NAME}
    restart: unless-stopped
    env_file: etcd.env
    networks: [etcdw-net]        # общая сеть контура (external, arch/25 §2)
    logging:
      driver: json-file
      options: { max-size: "10m", max-file: "3" }
    volumes:
      - ${DATA_DIR}:/etcd-data
    healthcheck:
      test: ["CMD", "etcdctl", "--endpoints=http://localhost:2379", "endpoint", "health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
    command:
      - /usr/local/bin/etcd
      - --name=${NODE_NAME}
      - --data-dir=/etcd-data
      - --listen-peer-urls=http://0.0.0.0:2380
      - --listen-client-urls=http://0.0.0.0:2379
      - --initial-advertise-peer-urls=http://${NODE_NAME}:2380
      - --advertise-client-urls=http://${NODE_NAME}:2379
      - --initial-cluster=${NODE_NAME}=http://${NODE_NAME}:2380
      - --initial-cluster-token=${CLUSTER_TOKEN}
      - --initial-cluster-state=new
      - --heartbeat-interval=250
      - --election-timeout=2000
      - --auto-compaction-retention=1
      - --quota-backend-bytes=8589934592   # 8 ГБ (arch/04 §3)
networks:
  etcdw-net:
    external: true
```

17.3. ЗАМЕНИ ЦЕЛИКОМ файл
`/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/deploy/etcd/etcd.env.example`:
```bash
# deploy/etcd/etcd.env.example — env SEED-узла etcd-контура (arch/04 §8 п.5,
# arch/25 §1). Один узел: состав доводит EtcdWorker; после доводки seed
# выводится (runbook). Образ — из локального registry (docs/runbook.md).
NODE_NAME=etcd-seed
ETCD_IMAGE=192.168.0.1:5000/quay.io/coreos/etcd:v3.5.21
CLUSTER_TOKEN=pgworker-controlplane-2026
DATA_DIR=/data/etcd-seed

# bootstrap-флаг: seed всегда new (единственный узел initial-cluster);
# INITIAL_CLUSTER_STATE=existing — управляемые узлы ставит EtcdWorker.
INITIAL_CLUSTER_STATE=new
```

17.4. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/deploy/docker-compose.yml`
после блока сервиса `s3worker-2` добавь (по образцу x-s3worker-env/base —
сверь стиль volume TLS-тома `pgw-api-tls` у s3worker и повтори его):
```yaml
x-etcdworker-env: &etcdworker-env
  EtcdWorker__Etcd__Endpoints__0: ${ETCDW_ETCD_ENDPOINT_0:-http://host.docker.internal:2379}
  EtcdWorker__Etcd__Endpoints__1: ${ETCDW_ETCD_ENDPOINT_1:-http://host.docker.internal:2381}
  EtcdWorker__Etcd__Endpoints__2: ${ETCDW_ETCD_ENDPOINT_2:-http://host.docker.internal:2383}
  EtcdWorker__Docker__Network: ${ETCDW_DOCKER_NETWORK:-etcdw-net}
  EtcdWorker__Docker__Mode: ${ETCDW_DOCKER_MODE:-Plain}
  EtcdWorker__Docker__Hosts__0__Name: ${ETCDW_DOCKER_HOST_NAME:-local}
  EtcdWorker__Docker__Hosts__0__Endpoint: ${ETCDW_DOCKER_HOST_ENDPOINT:-unix:///var/run/docker.sock}
  EtcdWorker__Docker__AdvertisedHost: ${ETCDW_ADVERTISED_HOST:-host.docker.internal}
  ETCDW_API_TLS_CERT_PATH: /tls/server.crt
  ETCDW_API_TLS_KEY_PATH: /tls/server.key
  ETCDW_API_TLS_CLIENT_CA_PATH: /tls/ca.pem

x-etcdworker-base: &etcdworker-base
  build:
    context: ..
    dockerfile: docker/EtcdWorker.Dockerfile
    args:
      ETCDW_PUBLISH_DIR: artifacts/etcdworker/publish
  image: etcdworker:dev
  restart: unless-stopped
  volumes:
    # docker-транспорт Plain-режима: docker.sock хоста (как x-s3worker-base;
    # без него Plain-драйвер не создаёт контейнеры managed-узлов).
    - /var/run/docker.sock:/var/run/docker.sock
    - etcdw-snapshots:/snapshots
    # mTLS HTTP API (arch/25 §1.1): общий TLS-volume pgw-api-tls (серты
    # kfw/vwk/s3w-ряда с SAN etcdworker* — добавить SAN в deploy/tls/gen.sh,
    # наполнение 00-up.sh); симметрично s3worker.
    - pgw-api-tls:/tls:ro
  extra_hosts:
    # docker-хост "local" (Docker:Hosts) + advertised-хост client-грани —
    # из контейнера резолвим в шлюз docker-хоста (как x-s3worker-base).
    - "local:host-gateway"
    - "host.docker.internal:host-gateway"
  logging:
    driver: json-file
    options: { max-size: "10m", max-file: "3" }
```
и два сервиса (в стиле s3worker/s3worker-2):
```yaml
  # EtcdWorker (arch/25): хозяин etcd-контура — состав managed-узлов + DR-слепки
  # в bucket etcd-backups; портовой ряд 8087 (после s3w 8086/8089).
  etcdworker:
    <<: *etcdworker-base
    ports:
      - "${ETCDW_API_HOST_PORT:-8087}:8080"
    environment:
      <<: *etcdworker-env
      EtcdWorker__Api__AdvertiseUrl: ${ETCDW_API_ADVERTISE_URL:-https://host.docker.internal:${ETCDW_API_HOST_PORT:-8087}}

  # Второй инстанс EtcdWorker (takeover ≤ TTL 15 с); том etcdw-snapshots — общий.
  etcdworker-2:
    <<: *etcdworker-base
    ports:
      - "${ETCDW_API_HOST_PORT2:-8088}:8080"
    environment:
      <<: *etcdworker-env
      EtcdWorker__Api__AdvertiseUrl: https://host.docker.internal:${ETCDW_API_HOST_PORT2:-8088}
```
в секцию volumes добавь `etcdw-snapshots:`. Сверься по файлу-образцу
(x-s3worker-base, deploy/docker-compose.yml:222): поля сервиса приведи к
фактическому стилю s3worker (env_file у базового якоря в образце НЕТ — не
добавляй; имена TLS-тома/extra_hosts — как в образце).

17.5. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/deploy/.env.example`
в конец S3Worker-блока добавь блок (комментарии по-русски, по стилю файла):
```bash
# EtcdWorker (arch/25): хозяин etcd-контура (состав managed-узлов + DR-слепки
# в фиксированный bucket etcd-backups — создаёт S3Worker на etcd-backup
# S3-кластере; S3-переменных у EtcdWorker нет). Порты ряда 8087/8088.
#ETCDW_ETCD_ENDPOINT_0=http://host.docker.internal:2379
#ETCDW_ETCD_ENDPOINT_1=http://host.docker.internal:2381
#ETCDW_ETCD_ENDPOINT_2=http://host.docker.internal:2383
#ETCDW_DOCKER_NETWORK=etcdw-net
#ETCDW_ADVERTISED_HOST=host.docker.internal
#ETCDW_API_HOST_PORT=8087
#ETCDW_API_HOST_PORT2=8088
```

17.6. В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t32-etcdworker-2/dev-stand/adminpanel/checks/00-up.sh`:
после PHASE-блока publish S3Worker добавь по его образцу:
```bash
# EtcdWorker (arch/25): publish НА ХОСТЕ → образ publish-COPY (канон
# «сборка и запуск двумя отдельными командами»).
echo ">>> [PHASE] publish EtcdWorker.App ($(date +%H:%M:%S))"
dotnet publish "$ROOT/src/EtcdWorker.App/EtcdWorker.App.csproj" \
  -c Release -o "$ROOT/artifacts/etcdworker/publish" --nologo \
  || { echo "❌ EtcdWorker publish failed"; exit 1; }
echo ">>> [PHASE] EtcdWorker publish ready ($(date +%H:%M:%S))"
```
рядом с экспортом `S3W_API_HOST_PORT{,2}` добавь:
```bash
# Хост-публикации API etcdworker (ряд 8087/8088; advertise следует за портом).
export ETCDW_API_HOST_PORT="${ETCDW_API_HOST_PORT:-8087}"
export ETCDW_API_HOST_PORT2="${ETCDW_API_HOST_PORT2:-8088}"
```
в синхронизацию `.env` (for k in S3W_...) добавь оба `ETCDW_API_HOST_PORT*`; в
строку `docker compose ... up -d --build --force-recreate pgworker pgworker-2
s3worker s3worker-2` допиши `etcdworker etcdworker-2`; после блока 7c (s3w)
добавь по его образцу блок 7d:
```bash
# 7d) etcdworker жив ×2 (arch/25): healthz по mTLS healthcheck-парой +
#     дискавери /etcdworker/api/ по ключам (по образцу s3worker 7c).
for i in $(seq 1 60); do "${MTLS[@]}" https://localhost:${ETCDW_API_HOST_PORT:-8087}/healthz >/dev/null 2>&1 && break; sleep 1; done
"${MTLS[@]}" https://localhost:${ETCDW_API_HOST_PORT:-8087}/healthz >/dev/null \
  || { echo "❌ etcdworker did not come alive within 60 s (docker logs deploy-etcdworker-1)"; exit 1; }
for i in $(seq 1 60); do "${MTLS[@]}" https://localhost:${ETCDW_API_HOST_PORT2:-8088}/healthz >/dev/null 2>&1 && break; sleep 1; done
"${MTLS[@]}" https://localhost:${ETCDW_API_HOST_PORT2:-8088}/healthz >/dev/null \
  || { echo "❌ etcdworker-2 did not come alive within 60 s (docker logs deploy-etcdworker-2-1)"; exit 1; }
etcdw_keys=0
for i in $(seq 1 30); do
  etcdw_keys="$(ect get /etcdworker/api/ --prefix --keys-only 2>/dev/null | grep -c . || true)"
  [ "${etcdw_keys:-0}" -ge 2 ] && break
  sleep 1
done
[ "${etcdw_keys:-0}" -ge 2 ] \
  || { echo "❌ /etcdworker/api/ contains ${etcdw_keys:-0} keys (<2; docker compose logs etcdworker etcdworker-2)"; exit 1; }
echo "  etcdworker alive ×2 (:${ETCDW_API_HOST_PORT:-8087} + :${ETCDW_API_HOST_PORT2:-8088}/healthz mTLS; /etcdworker/api/ keys: ${etcdw_keys})"
# Наблюдение статуса DR-слепков (spec §7.3-стенд): ключ /etcdworker/snapshots
# появляется при поднятых etcd-backup S3-кластере и воркере — отсутствие НЕ fail
# (стенд может жить без etcd-backup кластера); значение — в чек-вывод.
snap_status="$(ect get /etcdworker/snapshots 2>/dev/null || true)"
echo "  etcdworker snapshots status: ${snap_status:-<no key — etcd-backup S3 cluster is not up>}"
```
(строка чек-вывода чек-листа стенда, если такой есть в конце скрипта, —
дополни по стилю файла).

### Шаг 18. Финальные прогоны, seed-сценарий, roadmap-гейт

**Вход:** шаг 17 слит; все фазы в коде.

**Действие (кодер, сильная модель):** прогоняет полный мерж-гейт (порядок
серий с зачисткой между ними: `docker network prune -f` при осиротевших):
1) сборка `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
--nologo` (отдельной командой, лог в файл); 2) юниты всех серий
(Shared.Etcd, EtcdWorker, S3Worker, PgWorker, AdminPanel); 3)
`DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare
-c Release` (ДО docker-серий); 4) интеграции EtcdWorker (полный прогон) +
S3Worker (bucket-фильтр) + PgWorker-интеграции; 5) docker-E2E минимум:
`DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c
Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`; 6)
зачистка тестовых остатков: `docker ps -a | grep etcdw-` пусто, `docker
network ls | grep etcdw-` пусто, `docker ps -a | grep pgw-` пусто (стенд —
отдельный контур, его не трогаем; `docker network prune -f` при осиротевших).
Затем seed-сценарий приёмки §7.3: локальный контур по
runbook — `docker network create etcdw-net`, deploy/etcd seed-рецепт,
EtcdWorker nodes=3 (через PUT /api/etcd/config или панель), сверка 4
started-членов, вывод seed (nodes=3 → снос seed вручную после кворума managed
— шаги по docs/runbook.md), полный teardown. Roadmap-гейт (тем же коммитом
мержа): удалить тег `t32-etcdworker` из
`arch/roadmap/reliability.md` (список + `←`-ссылки других пунктов); строку
перенести в «Сделано» `arch/roadmap/reliability-report.md` (сводки R/D;
актуализация строк «etcd-обслуживание»/t08: экспортёр — EtcdWorker, ключ
`/etcdworker/snapshots`).

**Выход:** мерж-гейт зелёный; roadmap очищен.

**Проверка:** все серии зелёные (логи в файлах, финальные строки прочитаны);
осиротевших контейнеров/сетей нет; `grep -rn "t32-etcdworker" arch/roadmap/` —
только «Сделано»-строка reliability-report.md.

**Связь со spec:** §5 (мерж-гейт), §7.3 (все критерии), решение 9
(seed-сценарий), arch/roadmap/README (правило тега).

Подпунктов нет (прогоны/документы — сильная модель).

---

## Самопроверка плана (выполнена составителем)

1. **Покрытие spec:** §3.2 → шаги 1–2; §3.1/§3.6 → 3–4; §3.4-API → 5;
§3.4-S3Worker-правка → 6; §3.4-SnapshotLoop → 7–9; §3.4-Membership/Supervisor/
Reconcile + §3.5 → 10–13; §3.7 → 14; §3.8 → 15–16; §3.9 → 17; §4.8/§7.3 → 18.
Решения пользователя 1–9 распределены: 1→шаг 8; 2→12–13; 3→17 (стенд без
перестройки); 4→9; 5→16; 6→14; 7→6–8; 8→10–11; 9→17–18. §7.1-юниты: 1(шаг 1),
EtcdWorker-статус/ретенция/резолвер/placement/решения/spec-узла (7,8,10,12),
S3Worker-отбор (6), панель (15). §7.2-интеграции: S3Worker (6), слепки (9),
состав (13), API (5), PgWorker-демонтаж (14). §7.3: шаги 17–18.
2. **Развилки:** все точки выбора закрыты решениями spec, выписанными в шаги
(bucket-константа — решения 7; peer-URL/сеть/детект — 8; Plain-1-хост — 18;
RMW-гонка → 503; created_unix сохраняется; сон Retry/Scan; S3W-клиент первый
endpoint списка; журнал — собственный EtcdWorkJournal по формату §3.3).
Порог авто-замены NodeDeadSec — для ОБОИХ случаев (unstarted и health-молчание)
через метку первого наблюдения ProblemSinceUnix (решение 2, шаги 12–13).
Парсеры ключей /etcd/ — в EtcdWorker.Etcd (spec §3.1): EtcdDomainKeys/
EtcdConfigJson (шаг 5.4, потребляет API-хендлер) + EtcdNodesParser (шаг 7.6,
потребляют процессы состава). Managed/external-детект — один
EtcdContourMembers (шаг 5.3) и у гварда API, и у процессов (edge-URL
`etcdw-node-abc:` — external в обоих местах).
3. **Типы/сигнатуры** — из реального кода (IEtcdGateway/EtcdGateway — peerURLs
plain-строками, как фиксирует MemberList_ParsesUrls; ClaimStore/PortAllocLock/
SnapshotJob/EtcdSnapshotSink/BackupS3/S3PoolModel, EnsureBucketsAsync/
AddNodeProcess, S3Worker-файлы каркаса, x-s3worker-base compose-якорь,
панельные правила); новые имена зафиксированы в шагах; HostEndpoint у
EtcdWorker.Docker — собственный (S3Worker-сборка недоступна);
S3ClientOptions не используется EtcdWorker.Core (механика конструктора
BackupS3 повторена напрямую).
4. **Подпункты** самодостаточны: файл+место+полная спецификация/код; внешних
ссылок нет (кроме образцов для копирования, указанных путём).
