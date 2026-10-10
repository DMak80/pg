# t36-owns3-protocol — план реализации: каркас сервиса и протокольная обвязка ownS3

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Создать протокольный каркас ownS3 — проекты `src/OwnS3.{Protocol,Storage,App}` с полной реализацией главы `arch/owns3/03-protocol.md` (SigV4 трёх режимов, XML, формат ошибок), контрактом объектного слоя с заглушкой, хостом Kestrel с конвейером и роутингом 22 операций, юнитами на тест-векторах подписи и интеграционными тестами на `WebApplicationFactory`.

**Architecture:** Три проекта по канону главы 01: `Protocol` — чистые типы без ASP.NET/диска (подпись, XML, ошибки, имена); `Storage` — контракт 22 операций в доменных терминах + заглушка `NotWiredObjectStore` (без ссылки на Protocol); `App` — Kestrel-хост с собственным конвейером (RequestId → OPTIONS → роутер → аутентификация SigV4 → авторизация по матрице → хендлер → обработчик ошибок → метрики и структурный лог). Хендлеры t36 — полный протокольный контур с финальным шагом-заглушкой; t37/t38 подставляют реализацию `IObjectStore` без изменения конвейера.

**Tech Stack:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), CPM (`Directory.Packages.props`), `System.IO.Hashing` (CRC32/CRC32C), XmlSerializer/HMAC/SHA — BCL, `Shared.Metrics` (OTel Prometheus), xunit.v3 + FluentAssertions, `WebApplicationFactory`.

**Spec:** [`docs/superpowers/2026-10-10-t36-owns3-protocol/spec.md`](spec.md) — план аргументируется от спеки; исполнители читают оба документа.

**Канон (источник истины):** [`arch/22-owns3.md`](../../../arch/22-owns3.md) + главы [`arch/owns3/01…05`](../../../arch/owns3/01-overview.md). Референс MinIO (точечный): `/Users/demakaev/ZCodeProject/minio` (вне worktree, абсолютный путь; карта файлов — spec §1.1).

## Global Constraints

- .NET 10, `TreatWarningsAsErrors=true`, `Nullable=enable`, `ImplicitUsings=enable` (наследуются от `src/Directory.Build.props` — в csproj НЕ дублируются).
- Централизованное версионирование: НОВЫЙ пакет один — `System.IO.Hashing` в `src/Directory.Packages.props`; прочих новых пакетов нет.
- Решение: `src/PgWorker.slnx` — только добавление узлов (папка `/owns3/` + два тестовых в `/tests/`); существующие узлы не трогаются.
- Границы изменений (спека §6.9): только `arch/owns3/03-protocol.md` (задача 1), `src/OwnS3.*`, `src/tests/OwnS3.*`, `src/PgWorker.slnx`, `src/Directory.Packages.props`, `docs/superpowers/**`. PgWorker/KafkaWorker/AdminPanel/ValkeyWorker/Shared — НЕ трогать.
- Без докера, образов, dev-станда, E2E реальными клиентами, registry-загрузок, правок `deploy/**`/`dev-stand/**` (границы t36, спека §1.3). ownS3 никогда не запускается хост-процессом (arch/owns3/01 §5) — в t36 сервис существует только в тестовом хосте WAF.
- Без TLS, без virtual-host style, без `memory.md`.
- Язык: комментарии/документация — русский; идентификаторы — английский; тесты — AAA-комментарии (`// Arrange/Act/Assert`).
- Все команды сборки/тестов — из корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol` (в bash-шагах — `cd` в начало команды, т.к. cwd субагента сбрасывается).
- Каждый таск завершается коммитом в feature-ветку `feat-t36-owns3-protocol` (коммит свободен в feature-ветках — базовые правила §7).
- Порты/контейнеры: в тестах t36 их нет вообще (in-memory хост); хардкоды портов запрещены правилами проекта.
- TDD: в каждой задаче кода сначала пишется падающий тест, затем реализация (скилл superpowers:test-driven-development).

## Соответствие фазам спеки (§4)

| Фаза спеки | Задачи плана |
|---|---|
| 1. Arch-правки (десять дополнений) | Задача 1 |
| 2. Каркас решений | Задача 2 |
| 3. Protocol: заголовочная SigV4 | Задачи 3–4 |
| 4. Protocol: presigned + чанковая | Задачи 5–6 |
| 5. Protocol: XML + ошибки + имена | Задача 7 (имена — в задаче 3, канонизация там же) |
| 6. Storage | Задача 8 |
| 7. App: конвейер | Задача 9 |
| 8. App: 22 хендлера | Задача 10 |
| 9. Интеграционные тесты | Задача 11 |
| 10. Финальный прогон | Задача 12 |

## Тест-векторы SigV4 (вычислены независимо, зафиксированы литералами)

Все векторы построены на одной учётке/дате (источник входных данных — официальные
примеры AWS S3 SigV4; подписи вычислены независимой цепочкой HMAC (python), векторы
1–3 совпадают с опубликованными значениями AWS дословно; векторы 4–6 выведены из того
же seed математически и самодостаточны — канонический запрос зафиксирован в тесте):

| # | Запрос | Ожидание |
|---|---|---|
| 1 | GET `/test.txt`, `Range: bytes=0-9`, host `examplebucket.s3.amazonaws.com`, x-amz-date `20130524T000000Z` | creq-sha256 `7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95b48bbb2dc43964946972`; подпись `f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41` |
| 2 | GET `/?max-keys=2&prefix=J` | подпись `34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7` |
| 3 | Presigned GET `/test.txt`, `X-Amz-Expires=86400`, SignedHeaders=host | подпись `aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404` |
| 4 | PUT `/test%24file.text`, тело `Welcome to Amazon S3.`, `x-amz-storage-class: REDUCED_REDUNDANCY` | body-sha256 `44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072`; подпись `1ee3a9a719bf9cd67d34043a52b3d1f8b674e378dc99c0748019b43f49b5b9bb` |
| 5 | Чанковая цепочка от seed вектора 1, чанк данных `Welcome to Amazon S3.` + финальный 0-чанк | chunk-1 `c115618c80d5492b8fbd802845b0449cdbc16a5dc617f64114b42f23320f47de`; chunk-0 `b01db302cacbbd831a862a7bdc9c4fa4919d2b93bc99a8523ce727e9a14f49b0` |
| 6 | Trailer-режим вектора 5, трейлер `x-amz-checksum-crc32` | crc32-base64 `Ox7nCg==`; trailer-подпись `570042c8f9828c63e1aa1cf9c13ca35aa2efe47cec642c4cc890d46bd5916783`; sha256-base64 тела `RM591nyVng01JP+sF3Hfu6h9K2tLTpnkIDSouAP4sHI=` |

Учётка векторов: AccessKey `AKIAIOSFODNN7EXAMPLE`, SecretKey `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`, регион `us-east-1`, scope-дата `20130524`.

---

### Задача 1: Arch-правки — десять дополнений в `arch/owns3/03-protocol.md`

**Вход (предусловие):** worktree чист (кроме `docs/superpowers/**`), ветка `feat-t36-owns3-protocol`; спека одобрена; arch-first — правки канона ДО любого кода.

**Files:**
- Modify: `arch/owns3/03-protocol.md`

**Interfaces:**
- Consumes: spec §3.1 (таблица десяти пробелов с решениями и критериями).
- Produces: канон, по которому строятся задачи 3–11; каталог ошибок главы 03 §5 расширяется кодом `InvalidAccessKeyId`; §1 дополняется Trimall-семантикой (п.4), исходом для неподдерживаемого алгоритма и правилом percent-кодирования query (п.3); §2 получает presigned-skew-семантику «только будущее + строгая граница просрочки» (п.10, с уточнением условия `RequestTimeTooSkewed` в таблице §5).

**Действия (внести ровно десять правок, без перестройки остального текста):**

- [ ] **Шаг 1.1. Правка §1 (исходы проверки подписи) — пробелы 1 и 2.** В конец раздела 1 (после абзаца «Исходы проверки подписи») добавить два абзаца:

```markdown
**Несуществующий accessKey** (в `Authorization`/`X-Amz-Credential`) → **403**
`InvalidAccessKeyId` (Message «The AWS access key Id you provided does not
exist in our records.»). Проверяется до сверки подписи: подпись неизвестного
ключа не вычисляется (критерий — референс: `ErrInvalidAccessKeyID` 403).

**Отсутствие/невалидный формат `x-amz-date`** (и `Date` при подстановке) →
**400** `AuthorizationHeaderMalformed` (Message «Missing/Invalid x-amz-date
header»); отдельный код не заводится (критерий — референс:
`ErrMissingDateHeader`/`ErrMalformedDate` → 400).
```

- [ ] **Шаг 1.2. Правка §1 — пробел 8 (неподдерживаемый алгоритм).** Сразу за абзацами шага 1.1 добавить:

```markdown
**Алгоритм, отличный от `AWS4-HMAC-SHA256`** (в `Authorization`, включая
SigV2-заголовок `AWS …`) → **400** `InvalidRequest` (Message «The
authorization mechanism you have provided is not supported. Please use
AWS4-HMAC-SHA256.») — семантика «механизм не поддерживается» ≠
«малформированный заголовок», отдельный код не заводится (критерий —
референс: `ErrSignatureVersionNotSupported` → Code `InvalidRequest`, 400;
документированное поведение Amazon S3 для SigV2).
```

- [ ] **Шаг 1.3. Правка §5 (таблица маппинга) — строка `InvalidAccessKeyId` (пробел 1).** В таблицу раздела 5 (между строками `SignatureDoesNotMatch` и `AccessDenied`) добавить строку:

```markdown
| `InvalidAccessKeyId` | 403 | accessKey не существует (раздел 1) |
```

- [ ] **Шаг 1.4. Правка §1 п.4 (canonical headers) — пробел 7 (Trimall).** В пункте 4 раздела 1 (canonical headers) фразу о значениях заголовков «(значение — с усечёнными по краям пробелами)» заменить на:

```markdown
(значение — с усечёнными по краям пробелами и схлопнутыми внутренними
последовательностями пробелов в один пробел — Trimall стандарта SigV4;
референс: `signV4TrimAll` — `strings.Fields` → join одним пробелом)
```

- [ ] **Шаг 1.5. Правка §1 п.3 (canonical query) — пробел 9 (percent-кодирование пробела и `+`).** В пункт 3 раздела 1 (canonical query string), после слов «компоненты сортировки — до кодирования», добавить предложение:

```markdown
**Кодирование значений и ключей — RFC 3986: пробел → `%20`, литеральный
`+` → `%2B`**; «плюс как пробел» — семантика form-декодирования при
разборе query-параметров и в канонизации не участвует (стандарт SigV4;
референс: `getCanonicalRequest` — `Form.Encode()` даёт пробел как `+`
(form-кодирование), `ReplaceAll("+", "%20")` возвращает `%20`;
литеральный `+` на входе уже `%2B` и не затрагивается).
```

- [ ] **Шаг 1.6. Правка §2 и таблицы §5 — пробел 10 (presigned-skew: только будущее + строгая просрочка).** Две точки правки:
  (а) в разделе 2 строку «Clock skew ±15 минут применяется и к `X-Amz-Date`.» заменить на:

```markdown
**Skew для presigned — только на будущее**: `X-Amz-Date > now + 15 минут`
→ **403** `RequestTimeTooSkewed`; для прошедших дат skew-отказов НЕТ —
URL валиден всё время окна. Просрочка — строгое неравенство
`now − X-Amz-Date > X-Amz-Expires` → **403** `AccessDenied`
(непросроченный presigned принимается независимо от возраста)
(критерий — референс: Abs-skew `auth-handler.go` — только
заголовочно-подписанные типы; presigned
(`doesPresignedSignatureMatch`, `signature-v4.go`) — «дата из будущего
за skew» + просрочка `now − date > Expires`; отступление: «будущее за
skew» у референса — `AccessDenied`, ownS3 нормализует в
`RequestTimeTooSkewed` — единый код каталога «время вне допуска»,
статус 403 совпадает).
```

  (б) в таблице §5 условие строки `RequestTimeTooSkewed` заменить на: «x-amz-date вне ±15 минут (заголовочная подпись); X-Amz-Date в будущем дальше now + 15 минут (presigned)».

- [ ] **Шаг 1.7. Правка §3 (чанковая подпись) — пробелы 3 и 4.** В конец раздела 3 добавить два абзаца:

```markdown
**Невалидный синтаксис aws-chunked-фрейма** → **400** `InvalidRequest`
(Message «Malformed chunked encoding»); **лимит одного чанка — 16 МиБ**
(`maxChunkSize = 16 << 20`; превышение — тот же исход 400 `InvalidRequest`)
(критерий — референс: `errMalformedEncoding`/`errChunkTooBig` → 400).

**Чанковые режимы применяются только к PUT с телом** (`PutObject`,
`UploadPart`): значение `x-amz-content-sha256` = `STREAMING-*` на прочих
методах → **400** `InvalidRequest` (критерий — референс:
`isRequestSignStreamingV4` = значение заголовка ∧ `MethodPut`; стандарт
SigV4).
```

- [ ] **Шаг 1.8. Правка §6 (транспорт и стиль) — пробелы 5 и 6.** В конец раздела 6 добавить два абзаца:

```markdown
**OPTIONS-запрос** → пустой ответ **200** без CORS-заголовков, до
аутентификации (CORS-модели нет; критерий — референс: ранний `return` в
`errorResponseHandler`).

**Запрос, не матчатщийся ни на одну из 22 операций** (метод+path+query), и
запрос с известным путём, но неподдерживаемым методом → **400**
`InvalidArgument` (Message «Unsupported request»); отдельный код не
заводится (критерий — референс: `ErrUnknownAPIRequest` 400; простота).
Вне-наборные query-параметры-не-сабресурсы (например `x-id`) при этом
игнорируются (глава 02, раздел 1).
```

- [ ] **Шаг 1.9. Проверка.** Прочитать изменённый файл: все десять правок на месте (§1 — п. 1–2 и 7–9; §2 — п. 10; §3 — п. 3–4; §5 — строка `InvalidAccessKeyId` и уточнение условия `RequestTimeTooSkewed`; §6 — п. 5–6); прочие разделы/главы не тронуты (`git diff --stat` — один файл).

- [ ] **Шаг 1.10. Коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol && \
  git add arch/owns3/03-protocol.md && \
  git commit -m "arch(owns3): десять дополнений главы 03 — пробелы протокола t36, вкл. presigned-skew (только будущее)"
```

**Проверка задачи:** `git -C /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol diff HEAD~1 --stat` — ровно `arch/owns3/03-protocol.md`.

**Выход:** канон главы 03 закрывает все десять пробелов; код последующих задач ссылается на канон, а не на спеку.

**Связь со spec:** §3.1 (таблица десяти пробелов), принцип 1 (arch-first), критерий приёмки 1.

---

### Задача 2: Каркас решений — 5 csproj, slnx, CPM, Program-минимум

**Вход:** задача 1 закоммичена.

**Files:**
- Create: `src/OwnS3.Protocol/OwnS3.Protocol.csproj`
- Create: `src/OwnS3.Storage/OwnS3.Storage.csproj`
- Create: `src/OwnS3.App/OwnS3.App.csproj`
- Create: `src/OwnS3.App/Program.cs`
- Create: `src/OwnS3.App/OwnS3Options.cs`
- Create: `src/OwnS3.App/appsettings.json`
- Create: `src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj`
- Create: `src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj`
- Create: `src/tests/OwnS3.IntegrationTests/Api/OwnS3AppFactory.cs` (каркас фабрики, сценарии — задача 11)
- Modify: `src/Directory.Packages.props` (+`System.IO.Hashing`)
- Modify: `src/PgWorker.slnx` (папка `/owns3/` + тестовые проекты)

**Interfaces:**
- Consumes: паттерны соседей (`ValkeyWorker.App` — Worker SDK + FrameworkReference; `ValkeyWorker.UnitTests`/`KafkaWorker.IntegrationTests` — тестовые csproj; `Shared.Metrics/MetricsModuleExtensions` — `AddAppMetrics`/`MapAppMetrics`).
- Produces: `public partial class Program` (для WAF); `OwnS3Options` со схемой `OwnS3:*`; собираемое решение; тестовые проекты, готовые принимать тесты задач 3–11.

- [ ] **Шаг 2.1. `Directory.Packages.props`** — в алфавитном порядке ItemGroup добавить (актуальная стабильная версия под net10.0; проверить `dotnet package search System.IO.Hashing --take 1` и взять старшую стабильную):

```xml
<PackageVersion Include="System.IO.Hashing" Version="10.0.0" />
```

(если найденная версия отличается — вписать найденную; это единственный новый пакет).

- [ ] **Шаг 2.2. `src/OwnS3.Protocol/OwnS3.Protocol.csproj`** — чистая библиотека, единственная внешняя зависимость `System.IO.Hashing` (спека §3.2):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <InternalsVisibleTo Include="OwnS3.UnitTests"/>
    </ItemGroup>

    <ItemGroup>
        <PackageReference Include="System.IO.Hashing"/>
    </ItemGroup>

</Project>
```

- [ ] **Шаг 2.3. `src/OwnS3.Storage/OwnS3.Storage.csproj`** — без HTTP и БЕЗ ссылки на Protocol (глава 01 §3):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <InternalsVisibleTo Include="OwnS3.UnitTests"/>
    </ItemGroup>

</Project>
```

- [ ] **Шаг 2.4. `src/OwnS3.App/OwnS3.App.csproj`** — паттерн ValkeyWorker.App (Worker SDK + FrameworkReference):

```xml
<Project Sdk="Microsoft.NET.Sdk.Worker">

    <ItemGroup>
        <InternalsVisibleTo Include="OwnS3.UnitTests"/>
        <InternalsVisibleTo Include="OwnS3.IntegrationTests"/>
    </ItemGroup>

    <ItemGroup>
        <FrameworkReference Include="Microsoft.AspNetCore.App"/>
    </ItemGroup>

    <ItemGroup>
        <ProjectReference Include="..\OwnS3.Protocol\OwnS3.Protocol.csproj"/>
        <ProjectReference Include="..\OwnS3.Storage\OwnS3.Storage.csproj"/>
        <ProjectReference Include="..\Shared.Metrics\Shared.Metrics.csproj"/>
    </ItemGroup>

</Project>
```

- [ ] **Шаг 2.5. `src/OwnS3.App/OwnS3Options.cs`** — схема главы 05 §4 (секреты — только env, в appsettings их нет):

```csharp
namespace OwnS3.App;

// Конфигурация ownS3 (arch/owns3/05 §4): секция OwnS3 / env OWNS3_*.
// DataDir в t36 читается, но не используется (том — t37).
public sealed class OwnS3Options
{
    public const string SectionName = "OwnS3";

    public ServerOptions Server { get; set; } = new();
    public string DataDir { get; set; } = "/data";
    public RootOptions Root { get; set; } = new();
    public AccessKeyOptions[] AccessKeys { get; set; } = [];
    public string? HostId { get; set; }
}

public sealed class ServerOptions
{
    public int Port { get; set; } = 9000;
}

public sealed class RootOptions
{
    public string User { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}

// Статический access key (arch/owns3/05 §2): env OWNS3_ACCESS_KEYS__<i>__{ACCESSKEY,SECRETKEY,POLICY}.
public sealed class AccessKeyOptions
{
    public string AccessKey { get; set; } = string.Empty;
    public string SecretKey { get; set; } = string.Empty;
    public string Policy { get; set; } = "read-only";
}
```

- [ ] **Шаг 2.6. `src/OwnS3.App/appsettings.json`**:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information"
    }
  },
  "OwnS3": {
    "Server": {
      "Port": 9000
    },
    "DataDir": "/data",
    "Metrics": {
      "Enabled": true,
      "Path": "/metrics"
    }
  }
}
```

- [ ] **Шаг 2.7. `src/OwnS3.App/Program.cs`** — минимум фазы 2 (Kestrel + fail-fast root-пары + `/healthz` + `/metrics`; S3-конвейер подключается задачей 9):

```csharp
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OwnS3.App;
using Shared.Metrics;

// Точка входа ownS3 (arch/owns3/01 §3, arch/owns3/05): Kestrel-хост S3-грани,
// конфигурация OwnS3:* / OWNS3_*, fail-fast root-пары, /healthz и /metrics.
// Протокольный конвейер (роутинг/подпись/права) — задачи 9–10.

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<OwnS3Options>(builder.Configuration.GetSection(OwnS3Options.SectionName));
builder.Services.AddSingleton(TimeProvider.System);

// Fail-fast root-пары (arch/owns3/05 §1): user >= 3, password >= 8, оба непусты.
builder.Services.AddOptions<OwnS3Options>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.Root.User) && o.Root.User.Length >= 3,
        "OwnS3:Root:User обязателен и не короче 3 символов (env OWNS3_ROOT_USER)")
    .Validate(o => !string.IsNullOrWhiteSpace(o.Root.Password) && o.Root.Password.Length >= 8,
        "OwnS3:Root:Password обязателен и не короче 8 символов (env OWNS3_ROOT_PASSWORD)")
    .ValidateOnStart();

// Метрики (arch/18; arch/owns3/05 §5): имя Meter = ownS3 (строчными — финальные
// серии ownS3_*_total/ownS3_request_duration_seconds совпадают со словарём главы 05).
builder.Services.AddAppMetrics("ownS3", builder.Configuration.GetSection("OwnS3:Metrics"));

var app = builder.Build();

// Kestrel: any-IP, h1+h2c (arch/owns3/03 §6), лимит тела отключён — лимит 5 ГБ
// уровня хендлера (глава 02), не транспорта.
var options = app.Services.GetRequiredService<IOptions<OwnS3Options>>().Value;
app.Urls.Clear();
app.Urls.Add($"http://*:{options.Server.Port}");
((IApplicationBuilder)app).Use(async (ctx, next) =>
{
    ctx.Request.ContentLengthLimit = null; // MaxRequestBodySize = null
    await next();
});

// Вне S3-конвейера: healthz (в t36 — 200 без валидации тома; том — t37) и metrics.
app.MapGet("/healthz", () => Results.Ok());
app.MapAppMetrics();

await app.RunAsync();

/// <summary>Маркер для WebApplicationFactory интеграционных тестов.</summary>
public partial class Program;
```

Примечание: `ContentLengthLimit` через middleware применяет ограничение Kestrel корректно (`HttpContext.Request.ContentLengthLimit`); если при исполнении типовая проверка `TreatWarningsAsErrors` потребует иного способа (`builder.WebHost.ConfigureKestrel(o => o.Limits.MaxRequestBodySize = null)`), использовать вариант с `ConfigureKestrel` — семантика та же (лимит снят на уровне сервера), оба каноничны.

- [ ] **Шаг 2.8. Тестовые csproj.** `src/tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj` (по образцу `ValkeyWorker.UnitTests`):

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <Using Include="Xunit"/>
        <Using Include="FluentAssertions"/>
    </ItemGroup>

    <ItemGroup>
        <PackageReference Include="coverlet.collector">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="FluentAssertions"/>
        <PackageReference Include="Microsoft.NET.Test.Sdk"/>
        <PackageReference Include="xunit.runner.visualstudio">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="xunit.v3"/>
    </ItemGroup>

    <ItemGroup>
        <ProjectReference Include="..\..\OwnS3.Protocol\OwnS3.Protocol.csproj"/>
        <ProjectReference Include="..\..\OwnS3.Storage\OwnS3.Storage.csproj"/>
        <ProjectReference Include="..\..\OwnS3.App\OwnS3.App.csproj"/>
    </ItemGroup>

</Project>
```

`src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj` — то же плюс:

```xml
        <PackageReference Include="Microsoft.AspNetCore.Mvc.Testing"/>
```

и ссылки только на `OwnS3.App` (Protocol/Storage приходят транзитивно; лишних ProjectReference не добавлять).

- [ ] **Шаг 2.9. Каркас WAF-фабрики** `src/tests/OwnS3.IntegrationTests/Api/OwnS3AppFactory.cs` (сценарии — задача 11):

```csharp
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace OwnS3.IntegrationTests.Api;

// In-memory хост ownS3 (решение пользователя 3): без докера/портов; конфигурация
// — in-memory секция OwnS3 с root-парой и тремя статическими ключами (роли
// read-only/read-write/admin). Тестовые креды фиксированы литералами.
public sealed class OwnS3AppFactory : WebApplicationFactory<Program>
{
    public const string RootAccessKey = "testroot";
    public const string RootSecretKey = "testrootsecret";
    public const string ReaderAccessKey = "testreader";
    public const string ReaderSecretKey = "testreadersecret";
    public const string WriterAccessKey = "testwriter";
    public const string WriterSecretKey = "testwritersecret";
    public const string AdminAccessKey = "testadmin";
    public const string AdminSecretKey = "testadminsecret";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OwnS3:Root:User"] = "root",
            ["OwnS3:Root:Password"] = "rootpassword",
            ["OwnS3:HostId"] = "owns3-test",
            ["OwnS3:AccessKeys:0:AccessKey"] = ReaderAccessKey,
            ["OwnS3:AccessKeys:0:SecretKey"] = ReaderSecretKey,
            ["OwnS3:AccessKeys:0:Policy"] = "read-only",
            ["OwnS3:AccessKeys:1:AccessKey"] = WriterAccessKey,
            ["OwnS3:AccessKeys:1:SecretKey"] = WriterSecretKey,
            ["OwnS3:AccessKeys:1:Policy"] = "read-write",
            ["OwnS3:AccessKeys:2:AccessKey"] = AdminAccessKey,
            ["OwnS3:AccessKeys:2:SecretKey"] = AdminSecretKey,
            ["OwnS3:AccessKeys:2:Policy"] = "admin",
        }));
    }
}
```

- [ ] **Шаг 2.10. `src/PgWorker.slnx`** — в `<Solution>` добавить после папки `/admin/`:

```xml
    <Folder Name="/owns3/">
        <Project Path="OwnS3.Protocol/OwnS3.Protocol.csproj" />
        <Project Path="OwnS3.Storage/OwnS3.Storage.csproj" />
        <Project Path="OwnS3.App/OwnS3.App.csproj" />
    </Folder>
```

и в `<Folder Name="/tests/">` — два проекта:

```xml
        <Project Path="tests/OwnS3.UnitTests/OwnS3.UnitTests.csproj" />
        <Project Path="tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj" />
```

- [ ] **Шаг 2.11. Проверка сборки всего решения** (соседние проекты не должны сломаться добавлением узлов):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && \
  dotnet build PgWorker.slnx -c Release
```

Ожидание: `Build succeeded. 0 Error(s)` (warnings = errors — их нет).

- [ ] **Шаг 2.12. Коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol && \
  git add src/OwnS3.Protocol src/OwnS3.Storage src/OwnS3.App src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests src/PgWorker.slnx src/Directory.Packages.props && \
  git commit -m "feat(owns3): каркас решений t36 — 5 проектов, slnx, System.IO.Hashing, Program-минимум с fail-fast и healthz"
```

**Проверка задачи:** сборка решения зелёная (шаг 2.11). Ручной запуск `dotnet run` хост-процессом ЗАПРЕЩЁН каноном (arch/owns3/01 §5: ownS3 всегда в докере; в t36 сервис существует только в тестовом хосте WAF); поведение fail-fast root-пары фиксируется интеграционным сценарием `FailFastScenarios` задачи 11 (WAF с невалидной парой → старт падает с `OptionsValidationException`).

**Выход:** собираемое решение с пятью новыми проектами; Program с fail-fast, Kestrel, `/healthz`, `/metrics`; тестовые проекты готовы.

**Связь со spec:** §3.6 (инфраструктура), §3.4 (Program-минимум), §4 фаза 2, критерии приёмки 8; НЕ-цели §1.3 (без ручного хост-запуска).

---

### Задача 3: Protocol — модель запроса, канонизация, парсинг пути, имена

**Вход:** задача 2 закоммичена; `OwnS3.Protocol` собирается.

**Files:**
- Create: `src/OwnS3.Protocol/Requests/S3RequestModel.cs`
- Create: `src/OwnS3.Protocol/Requests/S3HeaderCollection.cs`
- Create: `src/OwnS3.Protocol/Requests/S3PathParser.cs`
- Create: `src/OwnS3.Protocol/Uris/UriEncoding.cs`
- Create: `src/OwnS3.Protocol/Validation/BucketNameValidator.cs`
- Create: `src/OwnS3.Protocol/Validation/ObjectKeyValidator.cs`
- Test: `src/tests/OwnS3.UnitTests/CanonicalizationTests.cs`, `src/tests/OwnS3.UnitTests/NameValidationTests.cs`, `src/tests/OwnS3.UnitTests/S3PathParserTests.cs`

**Interfaces:**
- Consumes: arch/owns3/03 §1 (canonical URI/query/headers — вкл. arch-правки 7 и 9), §6 (path-style, лимиты имён).
- Produces (используют задачи 4–6, 9–11):
  - `S3RequestModel { string Method; string RawPath; string RawQuery; S3HeaderCollection Headers; string Host; Stream Body }`
  - `S3HeaderCollection`: `string? First(string name)`, `IReadOnlyList<string> Values(string name)`, `IEnumerable<(string Name, string Value)> Raw` — регистронезависимый доступ, сырые ключи сохранены.
  - `S3PathParser.Parse(string rawPath) → S3Path(string? Bucket, string? Key)` — Key из URL-декодированных сегментов.
  - `UriEncoding.EncodePath(string rawPath)`, `UriEncoding.EncodeQuery(IEnumerable<(string, string?)> pairs)`.
  - `BucketNameValidator.IsValid(string)`, `ObjectKeyValidator.IsValid(string)`.

- [ ] **Шаг 3.1. Тесты канонизации (падающие).** `CanonicalizationTests.cs` — TDD: тесты на канонизацию пути/query/заголовков по правилам §1 (Trimall — усечение краёв + схлопывание внутренних пробелов; сортировка query до кодирования; кодирование query по arch-правке 9: пробел → `%20`, литеральный `+` → `%2B`). Ключевые кейсы:

```csharp
using OwnS3.Protocol.Requests;
using OwnS3.Protocol.Uris;

namespace OwnS3.UnitTests;

// Канонизация SigV4 (arch/owns3/03 §1, вкл. arch-правки 7 и 9): URI по образцу
// s3utils.EncodePath референса (сохранение отправленного %XX), query — сортировка
// до кодирования и RFC 3986-кодирование (пробел -> %20, '+' -> %2B).
public sealed class CanonicalizationTests
{
    [Theory]
    [InlineData("/b/k", "/b/k")]                                 // без спецсимволов — как есть
    [InlineData("/b/key with space", "/b/key%20with%20space")]   // пробел → %20
    [InlineData("/b/key+with space", "/b/key%2Bwith%20space")]   // литеральный '+' в ПУТИ → %2B (RFC 3986: '+' не unreserved; референс s3utils.EncodePath)
    [InlineData("/b/caf%C3%A9", "/b/caf%C3%A9")]                 // существующее кодирование сохраняется как отправлено
    [InlineData("/b/100%", "/b/100%25")]                         // незакодированный % кодируется
    public void EncodePath_Canonicalizes(string raw, string expected)
    {
        // Arrange / Act
        var actual = UriEncoding.EncodePath(raw);
        // Assert: сегментное кодирование RFC 3986 ('/' — разделитель, '+' — кодируется как %2B)
        actual.Should().Be(expected);
    }

    [Fact]
    public void EncodeQuery_SortsBeforeEncoding_AndEncodesSpacesAsPercent20()
    {
        // Arrange: НЕотсортированные пары со спецсимволами и '+'
        var pairs = new[] { ("prefix", "a b"), ("marker", "z"), ("max-keys", "2") };
        // Act
        var actual = UriEncoding.EncodeQuery(pairs);
        // Assert: сортировка по ключу до кодирования, пробел → %20 (не '+');
        // '+' в ЗНАЧЕНИИ query кодируется как %2B
        actual.Should().Be("marker=z&max-keys=2&prefix=a%20b");
    }

    [Fact]
    public void EncodeQuery_EncodesPlusInValueAsPercent2B()
    {
        // Arrange: значение со знаком '+'
        var pairs = new[] { ("prefix", "a+b") };
        // Act
        var actual = UriEncoding.EncodeQuery(pairs);
        // Assert: RFC 3986-кодирование значения — '+' → %2B (не остаётся литералом)
        actual.Should().Be("prefix=a%2Bb");
    }
}
```

Внимание (arch-правка 9): литеральный `+` кодируется как `%2B` и в пути, и в значении query — оба кейса выше; `%20` — только пробел. «Плюс как пробел» — семантика form-декодирования при разборе query-параметров, в канонизации не участвует.

- [ ] **Шаг 3.2. Реализация `UriEncoding.cs`** (минимальная для прохождения; `EncodePath` — по образцу `s3utils.EncodePath`: unreserved `A–Z a–z 0–9 - . _ ~` и `/` не кодируются; существующая валидная `%XX`-тройка сохраняется как есть; прочее — `%XX` верхнего регистра, включая `+` → `%2B`; `EncodeQuery` — сортировка по ключу, затем по значению (сравнение до кодирования, ordinal), RFC 3986-кодирование ключей и значений: пробел → `%20`, `+` → `%2B` (arch-правка 9), `key=value`; значение null → пустое).

- [ ] **Шаг 3.3. Тесты модели/заголовков/имён/пути (падающие).** `NameValidationTests.cs`: валидные имена (`my-bucket`, `a1`, 63 символа) и невалидные (`ab` — короче 3, `A-upper`, `-lead`, `trail-`, `64-символьный`, `under_score`, `точка.net`); ключи: пустой — невалиден, 1023 байта UTF-8 — валиден, 1024 — валиден ровно на границе, 1025 байт (многобайтовые символы, например `é`×512) — невалиден. `S3PathParserTests.cs`: `/` → (null, null); `/b` → (`b`, null); `/b/k1/k2/x` → (`b`, `k1/k2/x`); `/b/ключ%20с%20пробелом` → ключ декодирован `ключ с пробелом`; `%2F` в сегменте — декодируется в `/` внутри ключа (слэши — часть ключа, глава 02/03).

- [ ] **Шаг 3.4. Реализация** `S3HeaderCollection.cs`, `S3RequestModel.cs`, `S3PathParser.cs`, `BucketNameValidator.cs` (3–63, `[a-z0-9-]`, края — буква/цифра), `ObjectKeyValidator.cs` (непустой, `Encoding.UTF8.GetByteCount(key) <= 1024`). Модель:

```csharp
namespace OwnS3.Protocol.Requests;

// Транспортно-независимая модель запроса (spec §3.2): Protocol работает только
// с ней; App строит из HttpRequest, тесты — напрямую.
public sealed class S3RequestModel
{
    public required string Method { get; init; }          // верхний регистр
    public required string RawPath { get; init; }          // как прислал клиент, без декодирования
    public required string RawQuery { get; init; }         // без ведущего '?'
    public required S3HeaderCollection Headers { get; init; }
    public required string Host { get; init; }
    public required Func<Stream> OpenBody { get; init; }   // ленивый доступ к телу (повторно не открывается)
}
```

- [ ] **Шаг 3.5. Прогон юнитов.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && \
  dotnet test tests/OwnS3.UnitTests -c Release
```

Ожидание: PASS (0 failed).

- [ ] **Шаг 3.6. Коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol && \
  git add src/OwnS3.Protocol src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-protocol): модель запроса, канонизация URI/query (+ -> %2B, arch-правка 9), парсер пути, валидаторы имён"
```

**Проверка задачи:** юниты канонизации/имён/пути зелёные; кейсы `+`→`%2B` (путь и query-значение) зелёные.

**Выход:** базовые типы Protocol, на которых строятся верификаторы задач 4–6.

**Связь со spec:** §3.2 (модель, транспортные утилиты, canonical query по arch-правке 9), §3.5 (юниты канонизации/имён), критерий 2.

---

### Задача 4: Protocol — заголовочная SigV4 (парсер, canonical, verifier) + тест-векторы

**Вход:** задача 3 закоммичена.

**Files:**
- Create: `src/OwnS3.Protocol/Auth/PayloadHashMode.cs`
- Create: `src/OwnS3.Protocol/Auth/AuthorizationHeaderParser.cs`
- Create: `src/OwnS3.Protocol/Auth/CanonicalRequestBuilder.cs`
- Create: `src/OwnS3.Protocol/Auth/SigV4Core.cs`
- Create: `src/OwnS3.Protocol/Auth/SigV4HeaderVerifier.cs`
- Create: `src/OwnS3.Protocol/Errors/S3ProtocolException.cs` (исключение с кодом S3 — несут чанкковый ридер и сверки тела)
- Test: `src/tests/OwnS3.UnitTests/SigV4HeaderVerifierTests.cs`
- Create: `src/tests/OwnS3.UnitTests/TestVectors.cs` (литералы векторов 1–6 — общий файл юнитов)
- Create: `src/tests/OwnS3.UnitTests/TestSigV4Signer.cs` (независимый signer — источник векторов (б))

**Interfaces:**
- Consumes: `S3RequestModel`, `UriEncoding` (задача 3); arch/owns3/03 §1; arch-правки задачи 1 (`InvalidAccessKeyId` п.1, отсутствие/невалидный x-amz-date п.2, Trimall п.7, неподдерживаемый алгоритм п.8, percent-кодирование query п.9).
- Produces (задачи 5, 6, 9):
  - `enum PayloadHashMode { HexSha256, UnsignedPayload, Streaming, StreamingTrailer }` + `PayloadHashModeClassifier.Classify(string? value, string method)` → `PayloadHashModeClassification(Mode, string HexValue)` | ошибка `InvalidRequest` (вне перечня значений) / чанковый режим на не-PUT (arch-правка 4).
  - `AuthorizationHeaderParser.Parse(string header)` → `ParsedAuthorization(Credential Credential, IReadOnlyList<string> SignedHeaders, string Signature)`; ошибки структуры/scope — `S3ProtocolException(AuthorizationHeaderMalformed)`; алгоритм ≠ `AWS4-HMAC-SHA256` (вкл. SigV2-заголовок `AWS …`) — `S3ProtocolException(InvalidRequest)` с Message «The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.» (arch-правка 8).
  - `CanonicalRequestBuilder.Build(S3RequestModel, IReadOnlyList<string> signedHeaders, string payloadString)` → string.
  - `SigV4Core.StringToSign(amzDate, scope, canonical)`, `SigV4Core.SigningKey(secretKey, date, region)` → byte[], `SigV4Core.SignHex(key, stringToSign)` → string (hex); `SigV4Core.EmptySha256` = `e3b0c442...855`.
  - `SigV4HeaderVerifier(TimeProvider).Verify(S3RequestModel, Func<string, string?> secretResolver)` → `SigV4Result`:
    - `Ok(AccessKey, SeedSignature, AmzDate, Scope, PayloadHashMode Mode)` — SeedSignature = подпись запроса (для чанковой цепочки);
    - `Fail(S3ErrorCode Code, string? Detail = null)` — `SignatureDoesNotMatch` / `AuthorizationHeaderMalformed` / `RequestTimeTooSkewed` (Abs-skew ±15 мин — ТОЛЬКО заголовочный режим, arch-правка 10 разделяет семантики) / `InvalidAccessKeyId` / `InvalidRequest` (payload-значение вне перечня; неподдерживаемый алгоритм Authorization — arch-правка 8) / `AccessDenied`-семантика НЕ здесь (аноним определяет App).
  - `S3ProtocolException(S3ErrorCode Code, string Message)`.

- [ ] **Шаг 4.1. `TestVectors.cs`** — литералы всех шести векторов из таблицы выше (константы: `GetObjCreqSha`, `GetObjSignature`, `ListObjSignature`, `PresignedSignature`, `PutBodySha`, `PutSignature`, `Chunk1Signature`, `Chunk0Signature`, `Crc32B64`, `Sha256B64`, `TrailerSignature`, учётка/дата/регион/host). Плюс фиксация даты «сейчас» для skew-тестов: `FixedTime = 2013-05-24T00:05:00Z` (внутри ±15 мин от 20130524T000000Z) и `SkewedTime = 2013-05-24T00:20:00Z` (вне).

- [ ] **Шаг 4.2. `TestSigV4Signer.cs`** — независимая клиентская реализация (НЕ переиспользует Protocol: собственные канонизация/HMAC через BCL):

```csharp
using System.Security.Cryptography;
using System.Text;

namespace OwnS3.UnitTests;

// Независимый SigV4-signer тестов (spec §3.5, источник векторов (б)): собственная
// канонизация и HMAC-цепочка; код OwnS3.Protocol не переиспользует. Служит
// «вторым клиентом» против верификатора и строителем aws-chunked-тел.
public static class TestSigV4Signer
{
    public static readonly DateTimeOffset DefaultDate = new(2013, 5, 24, 0, 0, 0, TimeSpan.Zero);

    public static string HexSha256(byte[] data) => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    public static byte[] SigningKey(string secret, DateTimeOffset date, string region) { /* HMAC-цепочка AWS4<secret> → date → region → s3 → aws4_request */ return []; }

    public static string Sign(string secret, string stringToSign, DateTimeOffset date, string region) { /* hex-HMAC */ return ""; }

    // Заголовочная подпись: canonical request строится по §1 из переданных частей.
    public static string HeaderSignature(string secret, string method, string canonicalUri,
        string canonicalQuery, IEnumerable<(string Name, string Value)> headers, string payloadString,
        DateTimeOffset date, string region) { /* ... */ return ""; }

    // Чанковая цепочка: подписи чанков от seed (формулы §3).
    public static string ChunkSignature(string secret, string prevSignature, byte[] chunk,
        DateTimeOffset date, string region) { /* ... */ return ""; }
    public static string TrailerSignature(string secret, string prevSignature, string trailerString,
        DateTimeOffset date, string region) { /* ... */ return ""; }

    // Сборка aws-chunked-тела (фрейминг + трейлеры) — для AwsChunkedReaderTests и интеграционных.
    public static byte[] BuildChunkedBody(string secret, byte[] data, string seedSignature,
        DateTimeOffset date, string region, string? trailerName = null, byte[]? trailerChecksum = null) { /* ... */ return []; }
}
```

(Реализовать тела методов полностью — канонизация своя, через `StringBuilder` + `HMACSHA256`; при неверной реализации вектор 1 не сойдётся, что и является проверкой signer'а.)

- [ ] **Шаг 4.3. Падающие тесты верификатора** `SigV4HeaderVerifierTests.cs` — по покрытию §3.5: позитив (вектор 1: собрать `S3RequestModel` с заголовками `host/range/x-amz-content-sha256/x-amz-date` и `Authorization` с подписью `f0e8bd...`; resolver отдаёт секрет векторной учётки; TimeProvider = FixedTime → `Ok`); битая подпись (последний байт hex заменён) → `SignatureDoesNotMatch`; битый scope (`s3`→`sns`) → `AuthorizationHeaderMalformed`; неподдерживаемый алгоритм: `Authorization: AWS AKID:base64` (SigV2) и `AWS4-HMAC-SHA512 Credential=...` → `InvalidRequest` с Message «The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.» (arch-правка 8); skew заголовочной подписи > 15 мин (`FakeTimeProvider`/кастомный `TimeProvider` на SkewedTime) → `RequestTimeTooSkewed` (Abs-skew ±15 мин — заголовочный режим; для presigned семантика иная — arch-правка 10, задача 5); несуществующий accessKey (resolver → null) → `InvalidAccessKeyId`; отсутствие `x-amz-date` и `Date` → `AuthorizationHeaderMalformed` (arch-правка 2); **невалидная строка `x-amz-date`** (не формат `yyyyMMdd'T'HHmmss'Z'`: `2013-05-24T00:00:00Z` и `garbage`) → тот же `AuthorizationHeaderMalformed` (arch-правка 2: Missing/Invalid — оба исхода покрыты); `UNSIGNED-PAYLOAD`-позитив (через signer); hex-sha256 с телом (вектор 4: PUT, canonical URI `/test%24file.text`); payload-значение вне перечня (`x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER`) → `InvalidRequest`; канонизация: `+`→`%2B` в query-значении (arch-правка 9: пробел → `%20`, литеральный `+` → `%2B`), многозначные заголовки (join через запятую), Trimall — краевые пробелы И внутренние последовательности (`a  b \t c` → `a b c`, arch-правка 7), регистр имён (`X-Amz-Date` == `x-amz-date`), `host` отсутствует в SignedHeaders → `SignatureDoesNotMatch`. Вектор 2 (query `max-keys=2&prefix=J`) — позитив через signer и литерал.

Пример позитивного теста (AAA):

```csharp
[Fact]
public void Verify_AwsVector1_GetObject_Ok()
{
    // Arrange: официальный пример AWS GET Object (вектор 1) — подпись f0e8bd...
    var model = new S3RequestModel
    {
        Method = "GET",
        RawPath = "/test.txt",
        RawQuery = "",
        Host = "examplebucket.s3.amazonaws.com",
        Headers = S3HeaderCollection.FromPairs(
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Range", "bytes=0-9"),
            ("x-amz-content-sha256", TestVectors.EmptySha256),
            ("x-amz-date", "20130524T000000Z"),
            ("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=" + TestVectors.GetObjSignature)),
        OpenBody = () => Stream.Null,
    };
    var sut = new SigV4HeaderVerifier(new FixedTimeProvider(TestVectors.FixedTime));

    // Act
    var result = sut.Verify(model, ak => ak == "AKIAIOSFODNN7EXAMPLE" ? TestVectors.SecretKey : null);

    // Assert: подпись официального примера проходит; seed = значение подписи
    result.Should().BeOfType<SigV4Ok>()
        .Which.SeedSignature.Should().Be(TestVectors.GetObjSignature);
}
```

- [ ] **Шаг 4.4. Реализация** `PayloadHashMode.cs`, `AuthorizationHeaderParser.cs` (3 поля после алгоритма; accessKey может содержать `/` — Credential-часть сплитится справа: scope = ровно 4 последних сегмента; service обязан `s3`; терминал `aws4_request`; дата `yyyyMMdd`; префикс алгоритма сравнивается строго с `AWS4-HMAC-SHA256`, прочее — `InvalidRequest` arch-правки 8), `CanonicalRequestBuilder.cs` (Trimall значений — усечение краёв + схлопывание внутренних пробелов, arch-правка 7; query — через `UriEncoding.EncodeQuery`, arch-правка 9), `SigV4Core.cs`, `SigV4HeaderVerifier.cs` (порядок проверок: парсинг → классификация payload → resolver(accessKey) → `InvalidAccessKeyId` → дата формат (`yyyyMMdd'T'HHmmss'Z'`, иначе arch-правка 2) /skew → канонический запрос → подпись → **`CryptographicOperations.FixedTimeEqual`** байтов подписи). Сверка семантик с референсом `signature-v4-parser.go`/`signature-v4-utils.go` (`signV4TrimAll`, `extractSignedHeaders`).

- [ ] **Шаг 4.5. Прогон.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && \
  dotnet test tests/OwnS3.UnitTests -c Release
```

Ожидание: PASS; векторные тесты сходятся с первого прогона реализации (иначе — искать расхождение канонизации, НЕ править вектор).

- [ ] **Шаг 4.6. Коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol && \
  git add src/OwnS3.Protocol src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-protocol): заголовочная SigV4 — парсер/canonical (Trimall)/verifier (constant-time) + тест-векторы AWS"
```

**Проверка задачи:** векторные юниты (внешний источник (а) + независимый signer (б)) зелёные; кейсы неподдерживаемого алгоритма, Trimall и ОБА исхода x-amz-date (отсутствие + невалидный формат) покрыты.

**Выход:** `SigV4HeaderVerifier` — режим по умолчанию для `S3Authenticator` (задача 9); `TestSigV4Signer` — переиспользуется задачами 5–6, 11.

**Связь со spec:** §3.2 (SigV4 заголовочный режим, вкл. arch-правки п. 1/2/7/8/9), §3.5 («отсутствующий/невалидный x-amz-date» — оба исхода), критерии 2, 6.

---

### Задача 5: Protocol — presigned-верификатор

**Вход:** задача 4 закоммичена (`SigV4Core`, `CanonicalRequestBuilder`, `TestSigV4Signer` доступны).

**Files:**
- Create: `src/OwnS3.Protocol/Auth/S3PresignedOperations.cs`
- Create: `src/OwnS3.Protocol/Auth/PresignedRequestVerifier.cs`
- Test: `src/tests/OwnS3.UnitTests/PresignedRequestVerifierTests.cs`

**Interfaces:**
- Consumes: `SigV4Core`, `CanonicalRequestBuilder`, `S3RequestModel`; arch/owns3/03 §2 (после arch-правки 10: skew presigned — только на будущее, просрочка — строгое неравенство).
- Produces (задача 9): `S3PresignedOperations.Allowed` — `IReadOnlySet<string>` из 10 имён: `GetObject, PutObject, DeleteObject, HeadObject, CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts`; `PresignedRequestVerifier(TimeProvider).Verify(S3RequestModel, string operation, Func<string, string?> secretResolver)` → `SigV4Result` (те же Ok/Fail; `Ok.SeedSignature` = query-подпись).

- [ ] **Шаг 5.1. Падающие тесты** по покрытию §3.5 (skew-кейсы — по arch-правке 10; ЗАМЕНЯЮТ ошибочный прежний юнит вида `Verify_SkewedXAmzDate_RequestTimeTooSkewed`, проверявший Abs-skew прошедшей даты — такой исход в каноне отсутствует): валидный presigned (вектор 3: GET `/test.txt`, query-параметры `X-Amz-Algorithm/-Credential/-Date/-Expires=86400/-SignedHeaders=host/-Signature`, resolver — секрет вектора, FixedTime) → Ok; **использование через N > 15 мин внутри окна** (TimeProvider = дата подписи + 20 мин, `X-Amz-Expires=86400`) → Ok — Abs-skew для прошедших дат отсутствует (arch-правка 10); **дата в будущем дальше +15 мин** (подпись «из будущего»: TimeProvider = дата подписи − 20 мин) → 403 `RequestTimeTooSkewed` (arch-правка 10; отступление от референса — нормализация `AccessDenied` → `RequestTimeTooSkewed`); **просрочка — строгое неравенство** `now − X-Amz-Date > X-Amz-Expires` → 403 `AccessDenied`; кейс границы: `now − X-Amz-Date == X-Amz-Expires` (ровно на границе, Expires=60) → Ok; `Expires=604801` → `AuthorizationQueryParametersError`; `Expires=-1`/не число → `AuthorizationQueryParametersError`; отсутствие любого из 6 обязательных параметров (`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-SignedHeaders`, `X-Amz-Signature` — параметризованно по каждому, спека §3.2 «все параметры обязательны; отсутствие → `AuthorizationQueryParametersError`») → `AuthorizationQueryParametersError`; операция вне списка (`ListObjects`) → `AuthorizationQueryParametersError`; несуществующий accessKey → `InvalidAccessKeyId`; битая подпись → `SignatureDoesNotMatch`; canonical query = все параметры КРОМЕ `X-Amz-Signature` (позитив вектора 3 фиксирует это: подпихнуть лишний `response-content-type` в query — должен входить в подпись; signer-позитив).

- [ ] **Шаг 5.2. Реализация**: parse query-параметров (регистр значений `X-Amz-*` сохраняется); payload-строка canonical = `UNSIGNED-PAYLOAD`; порядок проверок (arch-правка 10): полнота всех 6 параметров → операция в списке → resolver → формат даты → **будущее-skew** (`X-Amz-Date > now + 15 мин` → `RequestTimeTooSkewed`; прошедшие даты skew-проверкой НЕ отвергаются) → диапазон `Expires` → **просрочка строго** (`now − X-Amz-Date > X-Amz-Expires` → `AccessDenied`) → подпись.

- [ ] **Шаг 5.3. Прогон + коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && dotnet test tests/OwnS3.UnitTests -c Release && \
cd .. && git add src/OwnS3.Protocol src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-protocol): presigned SigV4 — 10 операций, Expires<=604800, skew только на будущее (arch-правка 10), строгая просрочка"
```

**Проверка задачи:** все presigned-исходы §3.5 покрыты зелёными тестами: позитив «N > 15 мин внутри окна → Ok», негатив «будущее дальше +15 мин → 403», строгая просрочка с кейсом границы, отсутствие каждого из 6 параметров.

**Выход:** `PresignedRequestVerifier` для конвейера.

**Связь со spec:** §3.2 (presigned: все 6 параметров; skew-семантика arch-правки 10), §3.5 (обязательные кейсы presigned), критерии 2, 6.

---

### Задача 6: Protocol — чанковая потоковая подпись `AwsChunkedReader`

**Вход:** задача 4 закоммичена (`SigV4Core`, arch-правки 3/4 в каноне).

**Files:**
- Create: `src/OwnS3.Protocol/Auth/AwsChunkedReader.cs`
- Test: `src/tests/OwnS3.UnitTests/AwsChunkedReaderTests.cs`

**Interfaces:**
- Consumes: `SigV4Core`; arch/owns3/03 §3 + arch-правки; референс `cmd/streaming-signature-v4.go` (фрейминг, `maxChunkSize = 16 << 20`, формулы `getChunkSignature`/`getTrailerChunkSignature`).
- Produces (задача 9): `AwsChunkedReader : Stream` — конструктор `AwsChunkedReader(Stream inner, AwsChunkedReadingContext context)`; контекст:

```csharp
namespace OwnS3.Protocol.Auth;

// Контекст чанковой верификации (arch/owns3/03 §3): seed-подпись из
// SigV4HeaderVerifier, размер декодированного тела, список трейлеров.
public sealed record AwsChunkedReadingContext(
    string SecretKey,
    string AmzDate,          // yyyyMMdd'T'HHmmss'Z'
    string Scope,            // <date>/<region>/s3/aws4_request
    string SeedSignature,
    long DecodedContentLength,
    bool WithTrailers,
    IReadOnlyList<string> TrailerNames);   // имена из x-amz-trailer, нижний регистр
```

поведение: чтение данных чанков; ошибка — `S3ProtocolException` с каноническим кодом (`SignatureDoesNotMatch` — чанк/трейлер-подпись; `InvalidRequest` — битый фрейм/недопустимая hex-подпись фрейма/чанк > 16 МиБ/несовпадение `x-amz-decoded-content-length`; `BadDigest` — trailer-checksum). После финального 0-чанка (и трейлеров) — `EOF`.

- [ ] **Шаг 6.1. Падающие тесты** `AwsChunkedReaderTests.cs` по покрытию §3.5 (тела строит `TestSigV4Signer.BuildChunkedBody`):
  - валидный мультичанковый поток (2 чанка данных + 0-чанк; seed/дата/регион векторные) читается полностью, содержимое равно конкатенации чанков; EOF после 0-чанка;
  - вектор 5: тело из одного чанка `Welcome to Amazon S3.` с подписью `c115618c...` и 0-чанком `b01db302...` — чтение успешно (фиксирует цепочку литералами: подписи фреймов в теле — литералы вектора);
  - битая подпись чанка N=2 (порча hex в фрейме — валидные 64 hex, но неверное значение) → при чтении ДАННЫХ чанка 2 (не раньше) бросается `S3ProtocolException(SignatureDoesNotMatch)`; данные чанка 1 уже прочитаны (проверить: прочитанное до исключения == чанк 1);
  - битый фрейм → `InvalidRequest` «Malformed chunked encoding»: `;chunk-signature=` отсутствует / размер не-hex / нет `\r\n` / **подпись фрейма не-hex (например `chunk-signature=ZZ...`) или не 64 hex-символа (63/65)** — hex-валидация подписи ДО сравнения (impl-фикс код-ревью: не-hex вход в декодер/компаратор недопустим);
  - чанк > 16 МиБ (размерный hex `1000001`) → `InvalidRequest` (лимит 16 МиБ, arch-правка 3) — тест с фреймом, объявляющим размер больше лимита (данные не нужны: отказ по заголовку фрейма);
  - сумма данных ≠ `x-amz-decoded-content-length` → `InvalidRequest` при EOF;
  - трейлер-режим (вектор 6): 0-чанк + `x-amz-checksum-crc32: Ox7nCg==` + `x-amz-trailer-signature: 570042c8...` — чтение успешно; сверка crc32 пройдена;
  - **known-answer контрольных сумм** (фиксация против endian-ошибок при base64-кодировании; impl-фикс код-ревью): CRC32C(`"123456789"`) = `0xE3069283` → base64 `4waSgw==` (эталон RFC 3720); CRC32(`"123456789"`) = `0xCBF43926` → base64 `y/Q5Jg==` (эталон ISO-HDLC) — через те же функции подсчёта/кодирования, что использует ридер для trailer-checksum;
  - `BadDigest`: crc32-трейлер с другим base64 → `S3ProtocolException(BadDigest)`; sha256-трейлер с корректным `RM591nyV...` — ок; crc32c-трейлер с корректным `4waSgw==` (для тела `123456789`) — ок; sha1 (signer, BCL) — ок;
  - неподдерживаемое имя трейлера (`x-amz-checksum-crc64nvme`) → `InvalidRequest` «Unsupported trailer header»;
  - битая trailer-подпись → `SignatureDoesNotMatch`.

- [ ] **Шаг 6.2. Реализация `AwsChunkedReader.cs`**: конечный автомат чтения (заголовок фрейма `<hex>;chunk-signature=<hex64>\r\n` → данные → `\r\n`); **hex-валидация подписи фрейма** — ровно 64 символа `[0-9a-f]` (регистр по референсу — нижний hex), иначе `InvalidRequest` до какого-либо декодирования/сравнения; цепочка: string-to-sign чанка = `AWS4-HMAC-SHA256-PAYLOAD\n<AmzDate>\n<Scope>\n<пред. подпись>\n<sha256("")>\n<sha256(чанк)>`; постоянное сравнение подписей (`FixedTimeEqual`); после 0-чанка в trailer-режиме — чтение trailer-строк до `x-amz-trailer-signature:<hex>`: string-to-sign = `AWS4-HMAC-SHA256-TRAILER\n<AmzDate>\n<Scope>\n<подпись 0-чанка>\n<sha256(трейлер-строки, каждая с \n)>` (референс `getTrailerChunkSignature`; строка трейлера при отсутствии завершающего `\n` — нормализуется добавлением, как в референсе); контрольные суммы считаются по декодированным данным по ходу чтения (CRC32/CRC32C — `System.IO.Hashing` с big-endian упаковкой результата в 4 байта перед base64, SHA-1/SHA-256 — BCL), сверка при EOF; счётчик прочитанных байт против `DecodedContentLength`.

- [ ] **Шаг 6.3. Прогон + коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && dotnet test tests/OwnS3.UnitTests -c Release && \
cd .. && git add src/OwnS3.Protocol src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-protocol): AwsChunkedReader — фрейминг aws-chunked (hex-валидация подписи), цепочка, трейлеры, лимит 16 МиБ, known-answer CRC32C"
```

**Проверка задачи:** все чанковые сценарии §3.5 зелёные, включая векторные подписи цепочки/трейлера, hex-валидацию подписи фрейма и known-answer CRC32C/CRC32.

**Выход:** обёртка тела для чанковых PUT — используется аутентификатором задачи 9; сверка тела «по ходу» наблюдаема в интеграционных тестах (задача 11).

**Связь со spec:** §3.2 (чанковая подпись), §3.5, критерии 2, 6; риск-митигация «чанковая подпись — сложнейшая часть»; impl-фиксы код-ревью (г) hex-валидация и (д) known-answer.

---

### Задача 7: Protocol — XML-схемы (все 12 ответных эталонов), кодпоинты, каталог ошибок, writer ошибок

**Вход:** задачи 4–6 закоммичены.

**Files:**
- Create: `src/OwnS3.Protocol/Xml/S3XmlModels.cs` (запросные + ответные типы)
- Create: `src/OwnS3.Protocol/Xml/S3Xml.cs` (единый XML-хелпер: сериализация/десериализация XmlSerializer, UTF-8, namespace; безопасный writer числовых ссылок для недопустимых кодпоинтов)
- Create: `src/OwnS3.Protocol/Errors/S3ErrorCode.cs` (+ `S3ErrorCatalog`)
- Create: `src/OwnS3.Protocol/Errors/S3Error.cs` (+ `S3ErrorXmlWriter`)
- Test: `src/tests/OwnS3.UnitTests/S3XmlTests.cs`, `src/tests/OwnS3.UnitTests/S3ErrorTests.cs`

**Interfaces:**
- Consumes: arch/owns3/03 §4 (XML-схемы-образцы — источник эталонов; «недопустимые кодпоинты — числовыми ссылками»), §5 (таблица маппинга + `InvalidAccessKeyId` из задачи 1).
- Produces (задачи 9–11):
  - `enum S3ErrorCode` — ровно 25 значений по канонной таблице §5 (+ `InvalidAccessKeyId` из arch-правки 1): `NoSuchBucket, NoSuchKey, BucketAlreadyExists` (справочный алиас стандарта 409, ownS3 не эмитирует), `BucketAlreadyOwnedByYou, BucketNotEmpty, InvalidRange, PreconditionFailed, NotModified, EntityTooLarge, InvalidPart, InvalidPartOrder, MalformedXML, AuthorizationHeaderMalformed, AuthorizationQueryParametersError, SignatureDoesNotMatch, InvalidAccessKeyId, AccessDenied, RequestTimeTooSkewed, BadDigest, NoSuchUpload, InvalidArgument, InvalidBucketName, InvalidRequest, NotImplemented, InternalError`. СЛУЖЕБНОГО значения для заглушки Storage в enum НЕТ: `ObjectStoreUnavailableException` — исключение Storage, App ловит его по типу и отвечает `InternalError` (задача 9).
  - `S3ErrorCatalog.Get(S3ErrorCode)` → `S3ErrorInfo(string Code, int HttpStatus, string Message)` — сообщения/статусы точно по таблице §5 (вкл. `NotImplemented` Message «A header you provided implies functionality that is not implemented»; условие `RequestTimeTooSkewed` — по arch-правке 10: заголовочная ±15 мин / presigned только будущее).
  - `S3Error { S3ErrorCode Code; string? Resource; string? RequestId; string? HostId; string? MessageOverride }` + `S3ErrorXmlWriter.Write(S3Error)` → string (канонический `<Error><Code/><Message/><Resource/><RequestId/><HostId/></Error>`).
  - `S3Xml.Serialize<T>(T value)` / `S3Xml.Deserialize<T>(string xml)` — namespace `http://s3.amazonaws.com/doc/2006-03-01/`; сериализация через безопасный writer (см. шаг 7.3): спецсимволы — XML-сущности, недопустимые для XML 1.0 кодпоинты в значениях — числовыми ссылками `&#x<hex>;` (спека §3.2/канон §4); десериализация нераспарсиваемого → `S3ProtocolException(MalformedXML)`. Единое имя хелпера — `S3Xml` (используется хендлерами задачи 10 и тестами).
  - Ответные XML-типы (12, по образцам §4): `ListBucketResult` (+`ContentsEntry`, `CommonPrefixEntry`; поля v1/V2 опциональны), `ListVersionsResult` (+`VersionEntry`), `DeleteResult` (+`DeletedEntry`, `DeleteErrorEntry`), `InitiateMultipartUploadResult`, `CompleteMultipartUploadResult`, `ListPartsResult` (+`PartEntry`), `ListMultipartUploadsResult` (+`UploadEntry`), `ListAllMyBucketsResult` (+`OwnerEntry`, `BucketEntry`), `LocationConstraint` (пустой элемент), `CopyObjectResult`, `CopyPartResult`, `GetObjectAttributesOutput` (+`ObjectParts`, `AttributesPartEntry`).
  - Запросные: `DeleteRequest` (+`DeleteObjectEntry`), `CompleteMultipartUploadRequest` (+`ManifestPart`), `CreateBucketConfigurationRequest`.

- [ ] **Шаг 7.1. Падающие XML-тесты** `S3XmlTests.cs`:
  1. **Параметризованный Theory по ВСЕМ 12 ответным схемам** (спека §3.5 «сериализация всех схем главы 03 §4 — сравнение с эталонными XML-строками»): каждый кейс — заполненный тестовый экземпляр + ПОЛНЫЙ эталонный XML-литерал (строка), построенный по образцу §4 канона с фиксированными тестовыми значениями. Кейсы Theory (тип → обязательные заполняемые поля тестового экземпляра):

     | Тип | Заполняемые поля |
     |---|---|
     | `ListBucketResult` | Name, Prefix, Marker, MaxKeys=1000, IsTruncated=false, 2×Contents (Key/LastModified/ETag с кавычками/Size/StorageClass=STANDARD), 1×CommonPrefixes |
     | `ListVersionsResult` | Name, Prefix, KeyMarker="", VersionIdMarker="", MaxKeys, IsTruncated, 1×Version (Key/LastModified/ETag/Size/StorageClass) — БЕЗ VersionId/IsLatest |
     | `DeleteResult` | 1×Deleted/Key + 1×Error/Key+Code+Message |
     | `InitiateMultipartUploadResult` | Bucket, Key, UploadId |
     | `CompleteMultipartUploadResult` | Location `http://host/bucket/key`, Bucket, Key, ETag `"2-md5"` |
     | `ListPartsResult` | Bucket, Key, UploadId, PartNumberMarker=0, MaxParts=1000, IsTruncated, 1×Part (PartNumber/LastModified/ETag/Size), Initiator/Owner-заполнители, StorageClass |
     | `ListMultipartUploadsResult` | Bucket, KeyMarker="", UploadIdMarker="", MaxUploads, IsTruncated, 1×Upload (Key/UploadId/Initiated/StorageClass/Owner), 1×CommonPrefixes |
     | `ListAllMyBucketsResult` | Owner (ID/DisplayName = `owns3`), 2×Bucket (Name/CreationDate, по алфавиту) |
     | `LocationConstraint` | пустой элемент |
     | `CopyObjectResult` | LastModified, ETag |
     | `CopyPartResult` | LastModified, ETag |
     | `GetObjectAttributesOutput` | ETag, ObjectSize, ObjectParts (PartsCount/PartNumberMarker/MaxParts/IsTruncated/1×Part PartNumber+Size) |

     Эталон собирается как verbatim-строка по образцу §4 (вкл. `<?xml version="1.0" encoding="utf-8"?>`-декларацию и xmlns); дат-поля — фиксированные литералы (например `2026-01-01T00:00:00.000Z`), чтобы эталон был детерминирован. Пример полного эталона (InitiateMultipartUploadResult):

     ```csharp
     [Fact]
     public void Serialize_InitiateMultipartUpload_MatchesCanonicalSample()
     {
         // Arrange: экземпляр с фиксированными значениями + эталон по образцу arch/owns3/03 §4
         var value = new InitiateMultipartUploadResult { Bucket = "b", Key = "k", UploadId = "u" };
         const string Expected =
             """<?xml version="1.0" encoding="utf-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket><Key>k</Key><UploadId>u</UploadId></InitiateMultipartUploadResult>""";

         // Act
         var actual = S3Xml.Serialize(value);

         // Assert: побайтовое совпадение с эталоном канона (без пробелов между элементами)
         actual.Should().Be(Expected);
     }
     ```

     Остальные 11 кейсов — той же структурой (Theory с `(объект, эталон)` или отдельными [Fact] — по усмотрению исполнителя; ТРЕБОВАНИЕ: каждая из 12 схем имеет полный эталонный литерал и побайтовый ассерт).
  2. **Спецсимволы — сущности**: ключ `a<b&c` в `ListBucketResult/Contents/Key` сериализуется как `a&lt;b&amp;c` (без числовых ссылок — допустимые символы экранируются сущностями).
  3. **Недопустимые кодпоинты — числовые ссылки** (спека §3.2/канон §4; без механизма XmlWriter бросает исключение → 500 вместо канонического ответа): ключ `key\u0001x` в `Contents/Key` → в сериализованном XML `key&#x1;x`, сериализация НЕ бросает исключение; аналогичный кейс для `ListVersionsResult/Version/Key` с `\u000C` (form feed, недопустим в XML 1.0) → `&#xC;`.
  4. **Десериализация запросных XML**: валидный `Delete`-XML (3 ключа, Quiet=true) через `S3Xml.Deserialize<DeleteRequest>`; `CompleteMultipartUploadRequest` (2 части); битый XML → `S3ProtocolException(MalformedXML)`.

- [ ] **Шаг 7.2. Падающие тесты ошибок** `S3ErrorTests.cs`: весь каталог — параметризованный `Theory` (код → статус → канонический Message) для всех 25 значений enum по таблице §5 (статусы: NoSuchBucket 404, NoSuchKey 404, BucketAlreadyExists 409 — алиас, BucketAlreadyOwnedByYou 409, BucketNotEmpty 409, InvalidRange 416, PreconditionFailed 412, NotModified 304, EntityTooLarge 400, InvalidPart 400, InvalidPartOrder 400, MalformedXML 400, AuthorizationHeaderMalformed 400, AuthorizationQueryParametersError 400, SignatureDoesNotMatch 403, InvalidAccessKeyId 403, AccessDenied 403, RequestTimeTooSkewed 403, BadDigest 400, NoSuchUpload 404, InvalidArgument 400, InvalidBucketName 400, InvalidRequest 400, NotImplemented 501, InternalError 500); `S3ErrorXmlWriter` — полный XML-литерал для `NoSuchKey` из §5; `MessageOverride` подменяет канонический Message.

- [ ] **Шаг 7.3. Реализация** типов и каталога (XmlSerializer-атрибуты; пустые опциональные элементы — `XmlElement(IsNullable=true)` только где стандарт требует явный пустой элемент — `LocationConstraint`, `KeyMarker`/`VersionIdMarker` в versions-виде). **Безопасный writer** (внутри `S3Xml`): обёртка над `XmlWriter` (делегирующий подкласс), у которой `WriteString(string)` проходит по символам текста — символы допустимых диапазонов XML 1.0 (`#x9 | #xA | #xD | [#x20–#xD7FF] | [#xE000–#xFFFD] | [#x10000–#x10FFFF]`) пишутся штатно, прочие кодпоинты (control-символы и др.) пишутся числовой ссылкой `&#x<hex>;` через `WriteRaw`; `S3Xml.Serialize` создаёт XmlSerializer поверх этого writer'а. Без обёртки XmlWriter бросает `ArgumentException` на недопустимых символах — поведение разошлось бы с каноном (спека §3.2: «недопустимые кодпоинты — числовыми ссылками»; проявится в t37 на ключах с control-символами). Сущности для допустимых спецсимволов — штатная сериализация XmlSerializer.

- [ ] **Шаг 7.4. Прогон + коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && dotnet test tests/OwnS3.UnitTests -c Release && \
cd .. && git add src/OwnS3.Protocol src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-protocol): XML-схемы S3 — 12 эталонов, числовые ссылки недопустимых кодпоинтов, каталог 25 кодов ошибок"
```

**Проверка задачи:** все 12 ответных схем покрыты побайтовыми эталонами; кейс недопустимых кодпоинтов (`&#x1;`) зелёный; каталог полный — 25 кодов, служебного `ObjectStoreUnavailable`-значения нет (маппинг заглушки — catch в App, задача 9).

**Выход:** `Protocol` реализует главу 03 целиком (все компоненты §3.2 готовы, вкл. кодирование недопустимых кодпоинтов).

**Связь со spec:** §3.2 (XML: «недопустимые кодпоинты — числовыми ссылками»), §3.5 («сериализация всех схем §4 — сравнение с эталонными XML-строками, вкл. спецсимволы/кодпоинты»), критерии 2, 6.

---

### Задача 8: Storage — контракт `IObjectStore` + заглушка `NotWiredObjectStore`

**Вход:** задача 2 закоммичена (Storage-проект пуст и не зависит от Protocol); канон главы 02.

**Files:**
- Create: `src/OwnS3.Storage/ObjectStoreErrorCode.cs`, `ObjectStoreException.cs`
- Create: `src/OwnS3.Storage/Domain/*.cs` (доменные типы)
- Create: `src/OwnS3.Storage/IObjectStore.cs`
- Create: `src/OwnS3.Storage/NotWiredObjectStore.cs`
- Test: `src/tests/OwnS3.UnitTests/NotWiredObjectStoreTests.cs`

**Interfaces (ФИНАЛИЗИРОВАННЫЕ сигнатуры — решение спеки §3.3 «финализируются планом»):**

```csharp
namespace OwnS3.Storage;

// Доменные типы (глава 02; собственные имена Storage, без ссылки на Protocol).
public enum ObjectStoreErrorCode
{
    NoSuchBucket, NoSuchKey, BucketAlreadyOwnedByYou, BucketNotEmpty,
    InvalidPart, InvalidPartOrder, NoSuchUpload, InvalidRange,
    PreconditionFailed, NotModified, EntityTooLarge
}

public sealed class ObjectStoreException(ObjectStoreErrorCode code, string? message = null)
    : Exception(message ?? code.ToString()) { public ObjectStoreErrorCode Code { get; } = code; }

// Заглушка t36 (spec §3.3): сервис без хранилища.
public sealed class ObjectStoreUnavailableException : Exception
{
    public ObjectStoreUnavailableException() : base("object store is not wired (t37)") { }
}

// — записи листингов/метаданные —
public sealed record BucketEntry(string Name, DateTimeOffset CreationDate);
public sealed record ObjectUploadMetadata(string ContentType, IReadOnlyDictionary<string, string> UserMetadata);
public sealed record PutResult(string ETag);
public sealed record ObjectMetadata(string Key, string ETag, long Size, DateTimeOffset LastModified,
    string ContentType, IReadOnlyDictionary<string, string> UserMetadata);
public sealed record ObjectContent(ObjectMetadata Metadata, Stream Body);   // Body — освобождает вызывающий
public sealed record ObjectConditions(string? IfMatch, string? IfNoneMatch,
    DateTimeOffset? IfModifiedSince, DateTimeOffset? IfUnmodifiedSince);     // оценка — t37
public sealed record ByteRange(long? Start, long? End);                       // оценка — t37
public sealed record ObjectReadOptions(ObjectConditions? Conditions, ByteRange? Range);
public sealed record CopyRequest(string SourceBucket, string SourceKey, string DestBucket, string DestKey,
    bool ReplaceMetadata, ObjectUploadMetadata? NewMetadata, ObjectConditions? SourceConditions);
public sealed record PartCopyRequest(string SourceBucket, string SourceKey,
    string DestBucket, string DestKey, string UploadId, int PartNumber,
    ByteRange? SourceRange, ObjectConditions? SourceConditions);
public sealed record DeletedKeyResult(string Key, bool Deleted, string? ErrorCode, string? ErrorMessage);
public sealed record ListEntry(string Key, string ETag, long Size, DateTimeOffset LastModified);
public sealed record CommonPrefixEntry(string Prefix);
public enum ListVariant { V1, V2, Versions }
public sealed record ListQuery(string? Prefix, string? Delimiter, string? Marker, string? StartAfter,
    string? ContinuationToken, int? MaxKeys, string? EncodingType, bool FetchOwner, ListVariant Variant);
public sealed record ListPage(IReadOnlyList<ListEntry> Contents, IReadOnlyList<CommonPrefixEntry> CommonPrefixes,
    bool IsTruncated, string? NextMarker, string? NextContinuationToken, int KeyCount);
public sealed record PartEtag(int PartNumber, string ETag);
public sealed record CompleteResult(string ETag);   // составной <N>-<md5> — вычисляет t37/t38
public sealed record PartEntry(int PartNumber, string ETag, long Size, DateTimeOffset LastModified);
public sealed record PartsPage(IReadOnlyList<PartEntry> Parts, bool IsTruncated, int? NextPartNumberMarker);
public sealed record UploadEntry(string Key, string UploadId, DateTimeOffset Initiated);
public sealed record UploadsQuery(string? Prefix, string? Delimiter, string? KeyMarker,
    string? UploadIdMarker, int? MaxUploads, string? EncodingType);
public sealed record UploadsPage(IReadOnlyList<UploadEntry> Uploads, IReadOnlyList<CommonPrefixEntry> CommonPrefixes,
    bool IsTruncated, string? NextKeyMarker, string? NextUploadIdMarker);
public enum ObjectAttributeName { ETag, ObjectSize, StorageClass, ObjectParts }
public sealed record ObjectPartsAttributes(int PartsCount, int PartNumberMarker, int? NextPartNumberMarker,
    int MaxParts, bool IsTruncated, IReadOnlyList<(int PartNumber, long Size)> Parts);
public sealed record ObjectAttributes(string ETag, long ObjectSize, string StorageClass, ObjectPartsAttributes? Parts);

// Контракт 22 операций в доменных терминах главы 02 (реализация xl — t37/t38).
public interface IObjectStore
{
    // бакеты
    Task CreateBucketAsync(string bucket, CancellationToken ct);                       // исходы: BucketAlreadyOwnedByYou
    Task DeleteBucketAsync(string bucket, CancellationToken ct);                       // исходы: NoSuchBucket, BucketNotEmpty
    Task<bool> BucketExistsAsync(string bucket, CancellationToken ct);
    Task<IReadOnlyList<BucketEntry>> ListBucketsAsync(CancellationToken ct);

    // объекты
    Task<PutResult> PutObjectAsync(string bucket, string key, Stream body, long contentLength,
        ObjectUploadMetadata metadata, CancellationToken ct);
    Task<ObjectContent> GetObjectAsync(string bucket, string key, ObjectReadOptions options, CancellationToken ct);
    Task<ObjectMetadata> HeadObjectAsync(string bucket, string key, CancellationToken ct);
    Task DeleteObjectAsync(string bucket, string key, CancellationToken ct);           // идемпотентен (204-семантика)
    Task<IReadOnlyList<DeletedKeyResult>> DeleteObjectsAsync(string bucket,
        IReadOnlyList<string> keys, bool quiet, CancellationToken ct);
    Task<PutResult> CopyObjectAsync(CopyRequest request, CancellationToken ct);
    Task<ObjectAttributes> GetObjectAttributesAsync(string bucket, string key,
        IReadOnlyList<ObjectAttributeName> attributes, int? maxParts, int? partNumberMarker, CancellationToken ct);

    // листинги
    Task<ListPage> ListObjectsAsync(string bucket, ListQuery query, CancellationToken ct);

    // multipart
    Task<string> CreateMultipartUploadAsync(string bucket, string key,
        ObjectUploadMetadata metadata, CancellationToken ct);                          // → uploadId (UUID v4)
    Task<PutResult> UploadPartAsync(string bucket, string key, string uploadId, int partNumber,
        Stream body, long contentLength, CancellationToken ct);
    Task<PutResult> UploadPartCopyAsync(PartCopyRequest request, CancellationToken ct);
    Task<CompleteResult> CompleteMultipartUploadAsync(string bucket, string key, string uploadId,
        IReadOnlyList<PartEtag> parts, CancellationToken ct);
    Task AbortMultipartUploadAsync(string bucket, string key, string uploadId, CancellationToken ct);
    Task<PartsPage> ListPartsAsync(string bucket, string key, string uploadId,
        int? maxParts, int? partNumberMarker, CancellationToken ct);
    Task<UploadsPage> ListMultipartUploadsAsync(string bucket, UploadsQuery query, CancellationToken ct);
}
```

`NotWiredObjectStore : IObjectStore` — решение пользователя 1: методы с телом (`PutObjectAsync`, `UploadPartAsync`) сначала дочитывают поток до конца (drain — форсирует полную сверку подписи/хэшей конвейером), затем бросают `ObjectStoreUnavailableException`; методы без тела бросают её сразу.

- [ ] **Шаг 8.1. Падающие тесты** `NotWiredObjectStoreTests.cs`:
  - каждый из 17 методов без тела (в контракте 19 методов, из них `PutObjectAsync`/`UploadPartAsync` — с телом) → `ObjectStoreUnavailableException` (Theory по всем 17);
  - `PutObjectAsync(body)` с потоком 3 МиБ → исключение, но поток ДОЧИТАН: тест оборачивает поток счётчиком (`CountingStream`), после исключения `BytesRead == 3 МиБ` (drain-семантика — сверка тела наблюдаема);
  - `UploadPartAsync` — тот же drain;
  - drain не ломается на уже-закрытом потоке (Stream.Null).

- [ ] **Шаг 8.2. Реализация** всех файлов Storage (records/enums/exception/interface/заглушка). Комментарий-заголовок заглушки: семантика drain — вынужденная сверка тела конвейером App до отказа (spec §3.3).

- [ ] **Шаг 8.3. Прогон + коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && dotnet test tests/OwnS3.UnitTests -c Release && \
cd .. && git add src/OwnS3.Storage src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-storage): контракт IObjectStore (22 операции, доменные типы) + заглушка NotWiredObjectStore с drain"
```

**Проверка задачи:** юниты заглушки зелёные; `OwnS3.Storage` не ссылается на `OwnS3.Protocol` (проверить: в csproj Storage нет ProjectReference на Protocol).

**Выход:** граница t37 — сигнатуры контракта финализированы планом и закреплены кодом.

**Связь со spec:** §3.3 (весь раздел), решения пользователя 1–2, критерий 3.

---

### Задача 9: App — конвейер: RequestId, OPTIONS, роутер 22 операций, аутентификация, авторизация, ошибки, метрики, структурный лог

**Вход:** задачи 2–8 закоммичены (Protocol и Storage полностью готовы).

**Files:**
- Create: `src/OwnS3.App/Access/AccessPolicy.cs`, `AccessKeyRecord.cs`, `AccessKeyRegistry.cs`, `OperationAccessMatrix.cs`
- Create: `src/OwnS3.App/Routing/S3Operation.cs`, `S3Route.cs`, `S3Router.cs`
- Create: `src/OwnS3.App/Pipeline/AuthenticatedIdentity.cs`, `S3Authenticator.cs`, `S3Middleware.cs`, `RequestContext.cs`
- Create: `src/OwnS3.App/Pipeline/OwnS3Metrics.cs`
- Create: `src/OwnS3.App/Pipeline/S3ModelFactory.cs` (HttpRequest → S3RequestModel)
- Modify: `src/OwnS3.App/Program.cs` (DI + подключение конвейера)
- Test: `src/tests/OwnS3.UnitTests/S3RouterTests.cs`, `src/tests/OwnS3.UnitTests/OperationAccessMatrixTests.cs`, `src/tests/OwnS3.UnitTests/AccessKeyRegistryTests.cs`

**Interfaces:**
- Consumes: `SigV4HeaderVerifier`, `PresignedRequestVerifier`, `S3PresignedOperations`, `PayloadHashModeClassifier`, `AwsChunkedReader` (Protocol); `IObjectStore`/`ObjectStoreUnavailableException` (Storage); `AddAppMetrics`/`MapAppMetrics` (Shared.Metrics).
- Produces (задача 10–11): `S3Operation` — enum 22 значений + `None`; `S3Route { S3Operation Operation; string? Bucket; string? Key; string? RejectedSubresource }`; `S3Router.Route(method, rawPath, rawQuery, hasCopySource) → S3Route`; `AuthenticatedIdentity(string AccessKey, AccessPolicy Policy)`; `S3Middleware` — точка диспетчеризации хендлеров (задача 10 встраивает словарь операция→хендлер вместо точечного стаба); `OwnS3Metrics(Meter)` с `RequestCompleted(string operation, int code, TimeSpan duration)`; структурный лог запроса (шаг 9.7 п.8).

- [ ] **Шаг 9.1. Падающие тесты роутера** `S3RouterTests.cs` — ПОЛНАЯ таблица маршрутов (порядок специфичное→общее; дискриминатор = НАЛИЧИЕ query-ключа со значением, игнорируемым при матчинге, КРОМЕ `list-type` — дискриминатор СО ЗНАЧЕНИЕМ: матчится ровно `list-type=2` (канон гл. 02: «`list-type=2` — обязательный дискриминатор»); `?list-type` с иным или пустым значением → маршрут ListObjects v1 — поведение референса; `x-amz-copy-source` — заголовочный дискриминатор):

```csharp
using OwnS3.App.Routing;

namespace OwnS3.UnitTests;

// Таблица маршрутов 22 операций (arch/owns3/02; порядок — референс api-router.go,
// spec §3.4). Дискриминатор = наличие query-ключа (значение игнорируется),
// КРОМЕ list-type: дискриминатор со значением — матчится ровно "2"; иное
// значение/пусто → маршрут v1 (референс ведёт себя как v1). UploadPart-семейство
// = оба ключа (partNumber И uploadId); одиночные параметры — вне-наборные
// не-сабресурсы, игнорируются (глава 02 §1).
public sealed class S3RouterTests
{
    // (метод, путь, query, copy-source) → ожидаемая операция
    public static TheoryData<string, string, string, bool, S3Operation> Routes = new()
    {
        // корень
        { "GET",     "/",                 "",         false, S3Operation.ListBuckets },
        // бакет: GET-дискриминаторы от специфичного к общему
        { "GET",     "/b",                "location",  false, S3Operation.GetBucketLocation },
        { "GET",     "/b",                "uploads",   false, S3Operation.ListMultipartUploads },
        { "GET",     "/b",                "versions",  false, S3Operation.ListObjectVersions },
        { "GET",     "/b",                "list-type=2", false, S3Operation.ListObjectsV2 },
        { "GET",     "/b",                "list-type=1", false, S3Operation.ListObjects }, // иное значение → v1 (референс)
        { "GET",     "/b",                "list-type=",  false, S3Operation.ListObjects }, // пустое значение → v1
        { "GET",     "/b",                "",          false, S3Operation.ListObjects },
        { "GET",     "/b",                "prefix=x&marker=y", false, S3Operation.ListObjects }, // листинговые параметры — не дискриминаторы
        // бакет: прочие методы
        { "PUT",     "/b",                "",          false, S3Operation.CreateBucket },
        { "DELETE",  "/b",                "",          false, S3Operation.DeleteBucket },
        { "HEAD",    "/b",                "",          false, S3Operation.HeadBucket },
        { "POST",    "/b",                "delete",    false, S3Operation.DeleteObjects },
        // объект GET: attributes раньше uploadId раньше GetObject
        { "GET",     "/b/k",              "attributes", false, S3Operation.GetObjectAttributes },
        { "GET",     "/b/k",              "uploadId=x", false, S3Operation.ListParts },
        { "GET",     "/b/k",              "",          false, S3Operation.GetObject },
        { "HEAD",    "/b/k",              "",          false, S3Operation.HeadObject },
        // объект PUT: copy-семейство и part-семейство раньше PutObject
        { "PUT",     "/b/k",              "partNumber=1&uploadId=x", true,  S3Operation.UploadPartCopy },
        { "PUT",     "/b/k",              "partNumber=1&uploadId=x", false, S3Operation.UploadPart },
        { "PUT",     "/b/k",              "",          true,  S3Operation.CopyObject },
        { "PUT",     "/b/k",              "",          false, S3Operation.PutObject },
        // объект DELETE/POST
        { "DELETE",  "/b/k",              "uploadId=x", false, S3Operation.AbortMultipartUpload },
        { "DELETE",  "/b/k",              "",          false, S3Operation.DeleteObject },
        { "POST",    "/b/k",              "uploads",   false, S3Operation.CreateMultipartUpload },
        { "POST",    "/b/k",              "uploadId=x", false, S3Operation.CompleteMultipartUpload },
    };

    [Theory, MemberData(nameof(Routes))]
    public void Route_MatchesOperations(string method, string path, string query, bool copySource, S3Operation expected)
    {
        // Arrange / Act
        var route = S3Router.Route(method, path, query, copySource);
        // Assert
        route.Operation.Should().Be(expected);
    }

    // Вне-наборные сабресурсы (глава 02 §1) → операция None + имя сабресурса (501)
    public static TheoryData<string, string, string, string> RejectedSubresources = new()
    {
        { "GET",     "/b/k", "acl",       "acl" },
        { "PUT",     "/b/k", "tagging",   "tagging" },
        { "GET",     "/b",   "versioning", "versioning" },
        { "GET",     "/b",   "lifecycle", "lifecycle" },
        { "PUT",     "/b",   "policy",    "policy" },
        { "GET",     "/b/k", "retention", "retention" },
        // attributes на не-GET — конфигурация вне контракта GetObjectAttributes
        { "PUT",     "/b/k", "attributes", "attributes" },
    };

    [Theory, MemberData(nameof(RejectedSubresources))]
    public void Route_RejectsOutOfScopeSubresources(string method, string path, string query, string subresource)
    {
        // Arrange / Act
        var route = S3Router.Route(method, path, query, copySource: false);
        // Assert: сабресурс известен, но вне набора 22 операций → 501
        route.Operation.Should().Be(S3Operation.None);
        route.RejectedSubresource.Should().Be(subresource);
    }

    // Не-матч: известный путь, неподдерживаемый метод → None без сабресурса (400)
    public static TheoryData<string, string, string> UnmatchedRequests = new()
    {
        { "POST",    "/b",   "" },
        { "POST",    "/b/k", "" },
        { "PATCH",   "/b/k", "" },
        { "POST",    "/",    "" },
    };

    [Theory, MemberData(nameof(UnmatchedRequests))]
    public void Route_Unmatched_YieldsNoneWithoutSubresource(string method, string path, string query)
    {
        // Arrange / Act
        var route = S3Router.Route(method, path, query, copySource: false);
        // Assert: не-матч → 400 InvalidArgument «Unsupported request» (arch-правка 6)
        route.Operation.Should().Be(S3Operation.None);
        route.RejectedSubresource.Should().BeNull();
    }
}
```

- [ ] **Шаг 9.2. Реализация роутера.** `S3Operation` — enum: `None` + 22 значения (по одному разу, без дублей): `ListBuckets, CreateBucket, DeleteBucket, HeadBucket, GetBucketLocation, ListObjects, ListObjectsV2, ListObjectVersions, ListMultipartUploads, DeleteObjects, PutObject, CopyObject, GetObject, HeadObject, DeleteObject, GetObjectAttributes, CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts`. `S3Router`:
  1. разбор пути `S3PathParser.Parse` (сырой путь — для подписи остаётся нетронутым в модели);
  2. проверка вне-наборных сабресурсов — 24 имени из перечня спеки §3.4 (`acl, tagging, retention, legal-hold, torrent, restore, versioning, lifecycle, replication, encryption, policy, cors, website, notification, accelerate, object-lock, logging, metrics, inventory, intelligent-tiering, ownershipControls, publicAccessBlock, requestPayment, select`) плюс `attributes` на не-GET-методах (глава 02 §1: «?attributes-конфигурации вне контракта GetObjectAttributes» — расширение перечня самим планом) → `RejectedSubresource` (501);
  3. матчинг по таблице шага 9.1 (метод → path-форма → дискриминаторы по порядку); `list-type` — дискриминатор СО ЗНАЧЕНИЕМ: `ListObjectsV2` матчится только при `list-type=2`; `list-type` с иным/пустым значением маршрутизируется как v1 (референс);
  4. не-матч → `None` без сабресурса (400).
  Имя операции для метрик: `S3Operation.None` + сабресурс → метка = имя сабресурса; чистый не-матч → `unknown`.

- [ ] **Шаг 9.3. Падающие тесты доступа** `AccessKeyRegistryTests.cs` (root из опций → admin; статические ключи → роли; неизвестный ключ → null; неизвестная policy в конфиге → fail-fast при построении registry — ArgumentException) и `OperationAccessMatrixTests.cs` — параметризованная матрица 3 роли × 22 операции ровно по таблице главы 05 §3 (read-only: GetObject, HeadObject, ListObjects, ListObjectsV2, ListObjectVersions, HeadBucket, ListBuckets, GetBucketLocation, ListMultipartUploads, ListParts; read-write: те же + PutObject, DeleteObject, DeleteObjects, CopyObject, GetObjectAttributes, CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload; admin: все 22 + CreateBucket, DeleteBucket).

- [ ] **Шаг 9.4. Реализация доступа.** `AccessPolicy` enum (`ReadOnly, ReadWrite, Admin`); `AccessKeyRecord(string AccessKey, string SecretKey, AccessPolicy Policy)`; `AccessKeyRegistry(OwnS3Options)` — root-пара становится записью с ключом = `Root:User`, secret = `Root:Password`, policy = Admin; `OperationAccessMatrix.IsAllowed(AccessPolicy, S3Operation) → bool` — статическая таблица (замечание t36: read-only допускается к ListParts/ListMultipartUploads без фильтра «своих» — фильтр появляется в t38, spec §3.4 шаг 5).

- [ ] **Шаг 9.5. `S3ModelFactory`** — `HttpRequest → S3RequestModel`: сырой путь и query извлекаются из `IHttpRequestFeature.RawTarget` (`HttpContext.Features.Get<IHttpRequestFeature>()!.RawTarget`, сплит по первому `?` → путь и query) — `Request.Path`/`Request.QueryString` ASP.NET НЕсут частично декодированные значения и для подписи НЕ годятся: canonical URI обязан строиться по байтам, как прислал клиент (impl-фикс код-ревью: RawTarget — единственный источник сырой строки request-line); host; заголовки; `OpenBody = () => request.Body` (Kestrel-поток читается один раз).

- [ ] **Шаг 9.6. `S3Authenticator`** — выбор режима (арх-канон §1–2 и решения §3.4 шаг 4):
  - нет `Authorization` и нет `X-Amz-Algorithm` в query → аноним → `AccessDenied` (403);
  - `Authorization` присутствует → `SigV4HeaderVerifier`; при payload-режиме Streaming/StreamingTrailer: заголовочная подпись верифицируется с payload-строкой = значению режима (seed), тело оборачивается `AwsChunkedReader` (контекст из `x-amz-decoded-content-length`, `x-amz-trailer`);
  - иначе (presigned-параметры) → `PresignedRequestVerifier` с именем операции от роутера (skew-семантика presigned — arch-правка 10, внутри верификатора);
  - результат — `AuthenticatedIdentity` (из `AccessKeyRegistry`: найденный ключ; не найден → `InvalidAccessKeyId` через верификаторы).
  
  `OwnS3Metrics`:

```csharp
using System.Diagnostics.Metrics;

namespace OwnS3.App.Pipeline;

// Операционные метрики ownS3 (arch/18 + arch/owns3/05 §5): counter
// ownS3.requests{operation,code} и histogram ownS3.request.duration{operation};
// финальные имена экспорта — ownS3_requests_total / ownS3_request_duration_seconds.
public sealed class OwnS3Metrics(Meter meter)
{
    private readonly Counter<long> _requests = meter.CreateCounter<long>(
        "ownS3.requests", unit: "{request}",
        description: "S3-запросы по операциям и кодам ответов");
    private readonly Histogram<double> _duration = meter.CreateHistogram<double>(
        "ownS3.request.duration", unit: "s",
        description: "Длительность обработки S3-запроса по операциям");

    public void RequestCompleted(string operation, int code, TimeSpan duration)
    {
        _requests.Add(1, new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("code", code.ToString()));
        _duration.Record(duration.TotalSeconds, new KeyValuePair<string, object?>("operation", operation));
    }
}
```

- [ ] **Шаг 9.7. `S3Middleware`** — конвейер по шагам §3.4 (до хендлеров):
  0. **пропуск служебных путей**: точное совпадение `Request.Path` с `/healthz` или путём метрик (из `MetricsOptions.Path`, дефолт `/metrics`) → `await next()` и выход — запрос уходит в зарегистрированные endpoint'ы (они исполняются в конце конвейера); без этого шага S3-конвейер перехватил бы `/healthz` как GET-корень (`ListBuckets`) или бакет `healthz`;
  1. RequestId (UUID v4, заголовок `x-amz-request-id` в каждом ответе, лог-скоуп);
  2. OPTIONS → пустой 200;
  3. `S3Router` → вне-наборный сабресурс → 501-ошибка, не-матч → 400-ошибка (`InvalidArgument` «Unsupported request»);
  4. `S3Authenticator` (ошибки — канонические S3Error);
  5. `OperationAccessMatrix` (отказ → 403 `AccessDenied`);
  6. диспетчеризация хендлера — в этой задаче точечный стаб `PipelineNotReady` → 500 `InternalError` (задача 10 заменяет на реальные хендлеры);
  7. обработчик ошибок: `S3ProtocolException` → его код; `ObjectStoreException` → S3-код по каталогу; `ObjectStoreUnavailableException` → catch по типу → `S3Error(InternalError)` (служебного кода в enum `S3ErrorCode` нет); прочее → 500 `InternalError` (без внутренних деталей в Message, диагностика в лог); HEAD-ответ ошибки — без тела;
  8. метрики И структурный лог в `finally`: `OwnS3Metrics.RequestCompleted(operation, code, duration)` и запись `ILogger.LogInformation` уровня Information с шаблоном
     `"s3 request: requestId={RequestId} operation={Operation} bucket={Bucket} key={Key} method={Method} status={Status} durationMs={DurationMs}"`
     — все семь полей спеки §3.4 п.8: requestId, operation, bucket, key, метод, код ответа, длительность (bucket/key — пустые строки для бакетных/корневых операций).
  Ответы-ошибки — XML `S3ErrorXmlWriter`, `Content-Type: application/xml`.

- [ ] **Шаг 9.8. `Program.cs`** — DI: `AccessKeyRegistry`, `IObjectStore → NotWiredObjectStore`, `OwnS3Metrics` (из `Meter`-сингтона AddAppMetrics), хендлеры (задача 10); подключение: `app.UseMiddleware<S3Middleware>()`. ВАЖНО: Map-эндпоинты (`/healthz`, `/metrics`) исполняются в КОНЦЕ конвейера независимо от порядка вызовов `Use`/`Map` — «UseMiddleware после маппингов» эффекта не даёт; изоляция служебных путей обеспечивается ТОЛЬКО шагом 0 самого `S3Middleware` (точное совпадение пути → `next()`).

- [ ] **Шаг 9.9. Прогон + коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && \
  dotnet build PgWorker.slnx -c Release && dotnet test tests/OwnS3.UnitTests -c Release && \
cd .. && git add src/OwnS3.App src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-app): конвейер S3 — RequestId/OPTIONS/роутер 22 операций (list-type=2 по значению)/RawTarget-модель/SigV4/матрица/ошибки/метрики/лог"
```

**Проверка задачи:** юниты роутера (вся таблица, вкл. `list-type` со значением 2/1/пусто; 501/400-исходы раздельными Theory) и матрицы (3×22) зелёные; сборка решения зелёная.

**Выход:** работающий конвейер до хендлерного слоя; `S3Route`/`AuthenticatedIdentity` — вход хендлеров; структурный лог запроса; модель строится по `RawTarget`.

**Связь со spec:** §3.4 (конвейер, вся секция, вкл. п.8 — структурный лог; роутер — дискриминатор `list-type=2` по канону гл. 02), §3.5 (юниты матрицы), решения пользователя 1/4, критерии 4–5; plan-фикс код-ревью №6 (list-type по значению) и impl-фикс (а) (RawTarget).

---

### Задача 10: App — 22 хендлера протокольного контура

**Вход:** задача 9 закоммичена (конвейер диспетчеризирует); Protocol/Storage готовы.

**Files:**
- Create: `src/OwnS3.App/Handlers/OperationHandlerBase.cs`
- Create: `src/OwnS3.App/Handlers/BucketHandlers.cs` (5: ListBuckets, CreateBucket, DeleteBucket, HeadBucket, GetBucketLocation)
- Create: `src/OwnS3.App/Handlers/ObjectHandlers.cs` (7: PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects, CopyObject, GetObjectAttributes)
- Create: `src/OwnS3.App/Handlers/ListHandlers.cs` (3: ListObjects, ListObjectsV2, ListObjectVersions)
- Create: `src/OwnS3.App/Handlers/MultipartHandlers.cs` (7: CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads)
- Create: `src/OwnS3.App/Handlers/HashingBodyStream.cs` (обёртка тела: sha256 + MD5 по ходу чтения, пост-сверка)
- Modify: `src/OwnS3.App/Pipeline/S3Middleware.cs` (словарь операция→хендлер)
- Test: `src/tests/OwnS3.UnitTests/HandlersValidationTests.cs` (чистые валидации аргументов без хоста)

**Interfaces:**
- Consumes: `S3Route`, `AuthenticatedIdentity`, `IObjectStore`, XML-типы и хелпер `S3Xml` (задача 7), `S3Error`/каталог, `HashingBodyStream`.
- Produces (задача 11): `IOperationHandler { Task HandleAsync(S3HandlerContext context); }` — `S3HandlerContext` несёт `HttpContext`, `S3Route`, `AuthenticatedIdentity`, requestId; базовый класс даёт парсинг/валидацию/вызовы Storage. Скелет:

```csharp
namespace OwnS3.App.Handlers;

// Каркас хендлера t36 (spec §3.4.1): полный протокольный контур — парсинг →
// валидация → (оборачивание тела) → IObjectStore → сборка ответа. Финальный шаг
// в t36 — заглушка (500 InternalError); t37/t38 подставляют реализацию без
// изменения контура.
public interface IOperationHandler
{
    S3Operation Operation { get; }
    Task HandleAsync(S3HandlerContext context, CancellationToken ct);
}

public abstract class OperationHandlerBase(IObjectStore store) : IOperationHandler
{
    protected readonly IObjectStore Store = store;
    public abstract S3Operation Operation { get; }
    public abstract Task HandleAsync(S3HandlerContext context, CancellationToken ct);
}
```

- [ ] **Шаг 10.1. Валидации (падающие юниты).** `HandlersValidationTests.cs` — тестируемые чистые функции каркаса: `partNumber` (отсутствует/не число/0/10001 → `InvalidArgument`; 1 и 10000 → ок); лимит DeleteObjects (0 ключей → `MalformedXML`; 1001 → `MalformedXML`; 1000 → ок); Complete-манифест (пустой → `MalformedXML`; порядок 1,3,2 → `InvalidPartOrder`; строго возрастающий — ок); `encoding-type` (`url` ок; `xml` → `InvalidArgument`); `max-keys`/`max-parts`/`max-uploads` (дефолт 1000; > 1000 обрезается до 1000; отрицательное → `InvalidArgument`); `x-amz-metadata-directive` (СТРОГО `COPY`/`REPLACE` — точное сравнение, как в референсе; `copy` в нижнем регистре → `InvalidArgument`; `MERGE` → `InvalidArgument`); `x-amz-object-attributes` (пустой/отсутствует → `InvalidArgument`; `ETag,ObjectSize` ок; `Checksum` → вне-наборная грань — `NotImplemented` 501 по главе 02); **парсер `response-*` → канонические имена заголовков** (юнит-маппинг: `response-cache-control` → `Cache-Control`, `response-content-disposition` → `Content-Disposition`, `response-content-encoding` → `Content-Encoding`, `response-content-language` → `Content-Language`, `response-content-type` → `Content-Type`, `response-expires` → `Expires`; прочие `response-*` игнорируются — impl-фикс код-ревью: в ответе ставятся КАНОНИЧЕСКИЕ имена, не `response-*`); EntityTooLarge (Content-Length 5 ГБ + 1 → `EntityTooLarge`; ровно 5 ГБ — ок; чанковый режим — по `x-amz-decoded-content-length`).

- [ ] **Шаг 10.2. `HashingBodyStream.cs`** — обёртка `Stream`: читает сквозь, параллельно считает SHA-256 (режим hex) и MD5 (при `Content-MD5`); по завершении (EOF/Dispose) — сверка: sha256 ≠ `x-amz-content-sha256` → `S3ProtocolException(InvalidRequest)`; MD5 ≠ заголовка → `S3ProtocolException(BadDigest)`.

- [ ] **Шаг 10.3. Каркас + бакетные хендлеры.** Общий шаблон хендлера (пример полностью рабочей операции t36 — `GetBucketLocation`):

```csharp
// GetBucketLocation — единственная полностью протокольная операция t36
// (spec §3.4.1): регион — константа us-east-1, объектный слой не нужен.
public sealed class GetBucketLocationHandler(IObjectStore store) : OperationHandlerBase(store)
{
    public override S3Operation Operation => S3Operation.GetBucketLocation;

    public override async Task HandleAsync(S3HandlerContext context, CancellationToken ct)
    {
        // Validate: имя бакета (глава 03 §6) — ошибка каноническая
        BucketNameValidator.EnsureValid(context.Route.Bucket!);
        // Respond: LocationConstraint — пустой элемент (= us-east-1)
        await context.WriteXmlAsync(S3Xml.Serialize(new LocationConstraint()));
    }
}
```

Прочие бакетные (CreateBucket: парсинг опционального `CreateBucketConfiguration` (принимается и игнорируется), вызов `Store.CreateBucketAsync` — успех 200 с заголовком `Location: /{bucket}`; DeleteBucket/HeadBucket/ListBuckets — парсинг-минимум + Storage + ответ (ListBuckets — XML `ListAllMyBucketsResult` с Owner-заполнителем ownS3: ID/DisplayName фиксированные строки `owns3`)).

- [ ] **Шаг 10.4. Объектные хендлеры.** Каждый — парсинг/валидация по контракту главы 02 → тело (при наличии) через `HashingBodyStream` → Storage → ответ-заголовки. Специфика t36:
  - `PutObject`: `Content-Type` (дефолт `application/octet-stream`), `x-amz-meta-*` → `ObjectUploadMetadata.UserMetadata` (без префикса), `x-amz-storage-class` игнорируется, EntityTooLarge по Content-Length; тело → `Store.PutObjectAsync` (заглушка: drain + 500).
  - `GetObject`/`HeadObject`: парсинг `response-*` — переопределения применяются к ответу заголовками с КАНОНИЧЕСКИМИ именами (`Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Content-Type`, `Expires`; при `response-expires` — заголовок `Expires`); conditional-заголовки и `Range`/`If-Range` — парсинг формата (RFC 7231-даты невалидные игнорируются; Range-спецификация распознаётся одиночная) и передача в `ObjectReadOptions`; ETag/LastModified-заголовки — только с данными (t37).
  - `DeleteObject`: 204 в успехе (заглушка кинет 500 — это ожидаемо: финальный шаг не работает без Storage).
  - `DeleteObjects`: XML `Delete` читается из тела, ОБЁРНУТОГО в `HashingBodyStream` — при `Content-MD5` сверка MD5 при дочитывании XML → `BadDigest` (глава 02: Content-MD5 опционален, проверяется при наличии; impl-фикс код-ревью: сверка не обходится и для XML-тел) → `MalformedXML`-валидации (лимит 1000, пусто) → `Store.DeleteObjectsAsync(keys, quiet)` → `DeleteResult`-XML.
  - `CopyObject`: `x-amz-copy-source` парсинг (`/<bucket>/<key>`, URL-декодирование; невалидный → `InvalidArgument`), `x-amz-metadata-directive` (строго COPY/REPLACE), copy-условия; `Store.CopyObjectAsync`.
  - `GetObjectAttributes`: `x-amz-object-attributes` (список через запятую; `Checksum` и прочие вне набора → 501); `x-amz-max-parts`/`x-amz-part-number-marker`; conditional парсинг; `Store.GetObjectAttributesAsync`.
- [ ] **Шаг 10.5. Листинговые хендлеры.** Общие парсеры query (`prefix/delimiter/marker/start-after/continuation-token/max-keys/encoding-type/fetch-owner/key-marker`; max-keys=0 — валидный пустой листинг) → `ListQuery` с вариантом V1/V2/Versions → `Store.ListObjectsAsync`. `encoding-type ≠ url` → `InvalidArgument`.
- [ ] **Шаг 10.6. Multipart-хендлеры.** `CreateMultipartUpload` (`?uploads`); `UploadPart` (partNumber-валидация, тело как PutObject); `UploadPartCopy` (copy-source + `x-amz-copy-source-range` парсинг: `bytes=a-b`, невалидный синтаксис или a > b → `InvalidArgument` — обе причины в один код по канону; выход за размер источника оценивает Storage в t37/t38); `CompleteMultipartUpload` (XML-манифест: минимум 1 часть, порядок строго возрастает; `NoSuchUpload`-исходы — от Storage); `AbortMultipartUpload`; `ListParts` (`max-parts`, `part-number-marker`); `ListMultipartUploads` (`key-marker/upload-id-marker/max-uploads/encoding-type`).
- [ ] **Шаг 10.7. Диспетчеризация** в `S3Middleware`: словарь `S3Operation → IOperationHandler` регистрируется в DI (все 22), конвейер шага 6 берёт хендлер по `route.Operation`; отсутствующий (None уже отсечён) — 500.

- [ ] **Шаг 10.8. Прогон + коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && \
  dotnet build PgWorker.slnx -c Release && dotnet test tests/OwnS3.UnitTests -c Release && \
cd .. && git add src/OwnS3.App src/tests/OwnS3.UnitTests && \
  git commit -m "feat(owns3-app): 22 хендлера протокольного контура + GetBucketLocation полностью + HashingBodyStream (вкл. DeleteObjects MD5)"
```

**Проверка задачи:** юниты валидаций зелёные (вкл. строгое `COPY`/`REPLACE` и маппинг `response-*` → канонические имена заголовков); сборка зелёная; ручная дым-проверка хост-процессом НЕ выполняется (канон arch/owns3/01 §5) — дым остаётся интеграционным тестам задачи 11.

**Выход:** полный протокольный контур 22 операций; конвейер финален для t37/t38.

**Связь со spec:** §3.4.1 (вся секция; metadata-directive строго COPY/REPLACE; сверки тел — при наличии тела, вкл. DeleteObjects), решение пользователя 1, критерии 4–5; границы НЕ-целей (conditional/Range-оценка — t37; multipart-механика — t38) соблюдены: заголовки парсятся, не оцениваются; impl-фиксы код-ревью (б) DeleteObjects-MD5 и (в) канонические имена response-overrides.

---

### Задача 11: Интеграционные тесты — `WebApplicationFactory`, тестовый клиент, все сценарии

**Вход:** задача 10 закоммичена; полный конвейер работает.

**Files:**
- Modify: `src/tests/OwnS3.IntegrationTests/Api/OwnS3AppFactory.cs` (коллекция-фикстура + тестовый лог-провайдер)
- Create: `src/tests/OwnS3.IntegrationTests/Api/OwnS3TestClient.cs` (тестовый SigV4-клиент; использует линк `TestSigV4Signer.cs`)
- Create: `src/tests/OwnS3.IntegrationTests/Api/RoutingScenarios.cs`
- Create: `src/tests/OwnS3.IntegrationTests/Api/AuthScenarios.cs`
- Create: `src/tests/OwnS3.IntegrationTests/Api/AccessScenarios.cs`
- Create: `src/tests/OwnS3.IntegrationTests/Api/ErrorFormatScenarios.cs`
- Create: `src/tests/OwnS3.IntegrationTests/Api/BodyIntegrityScenarios.cs`
- Create: `src/tests/OwnS3.IntegrationTests/Api/MetricsHealthScenarios.cs` (метрики, healthz, структурный лог)
- Create: `src/tests/OwnS3.IntegrationTests/Api/FailFastScenarios.cs` (отказ старта на невалидной root-паре)
- Modify: `src/tests/OwnS3.IntegrationTests/OwnS3.IntegrationTests.csproj` (+ `Compile Include="..\OwnS3.UnitTests\TestSigV4Signer.cs" Link="TestSigV4Signer.cs"` и при необходимости `TestVectors.cs`)

**Interfaces:**
- Consumes: `OwnS3AppFactory` (задача 2), `TestSigV4Signer` (задача 4), Protocol-типы.
- Produces: зелёная интеграционная сюита — критерий приёмки 7 (+ фиксация fail-fast, структурного лога, RawTarget-пути, presigned-skew).

- [ ] **Шаг 11.1. `OwnS3TestClient` + фикстура** — `HttpClient` + `HttpRequestMessage` с полным контролем сырого пути/заголовков: `SignHeader(method, path, query, headers, body)` (заголовочная подпись по сырому пути), `BuildPresignedUrl(...)` (с параметром «дата подписи» — для кейсов окна/будущего), `BuildChunkedPut(...)` (aws-chunked тело, трейлеры). Фикстура-коллекция: один `OwnS3AppFactory` на все классы (паттерн `KafkaApiCollection`); фабрика без портов и контейнеров; `TimeProvider` хоста заменяется на фиксированный тестовый (WAF-овverride DI `TimeProvider` → `FixedTimeProvider` векторной даты) — deterministic skew/окно-кейсы. В фабрику добавить тестовый `ILoggerProvider` (потокобезопасный сборщик записей, `factory.LogEntries`) через `ConfigureLogging` — для проверки структурного лога (шаг 11.7).

- [ ] **Шаг 11.2. `RoutingScenarios.cs`** (AAA): каждая из 21 операций с заглушкой (все, КРОМЕ полностью протокольной `GetBucketLocation`) с валидной подписью writer-ключа → ответ 500 `InternalError` с XML `InternalError` (сигнатура «операция определена и дошла до заглушки»; параметризованный Theory по таблице маршрутов задачи 9 без строки GetBucketLocation); `list-type` — интеграционная фиксация значения-дискриминатора: `GET /{bucket}?list-type=1` с подписью по этому query → 500 `InternalError` с метрикой `operation="ListObjects"` (v1, не V2 — plan-фикс №6); **кодированный ключ — фиксация RawTarget-пути** (impl-фикс (а)): GET `/b/caf%C3%A9` и GET `/b/100%25` (ключ `100%` — литеральный процент) с заголовочной подписью, построенной по СЫРОМУ кодированному пути (signer подписывает `caf%C3%A9`/`100%25` как есть) → 500 `InternalError` (операция определена, подпись сошлась — модель получила путь из `IHttpRequestFeature.RawTarget`, а не декодированный `Request.Path`); **GetBucketLocation — отдельный сценарий**: подписанный `GET /{bucket}?location` → 200, тело — пустой `LocationConstraint` (полный успех без объектного слоя, spec §3.4.1; ожидаемая серия метрики `code="200"` — шаг 11.7); вне-наборные сабресурсы (`?acl` GET/PUT/DELETE, `?tagging`, `?versioning`, `?policy`, `?select`) → 501 `NotImplemented` ДО аутентификации (тест с битой подписью — исход тот же 501); не-матч (POST `/{bucket}`, PATCH `/{bucket}/{key}`) → 400 `InvalidArgument`; OPTIONS любой путь → 200 пустой без CORS-заголовков.

- [ ] **Шаг 11.3. `AuthScenarios.cs`**: валидная подпись → до заглушки (500); битая → 403 `SignatureDoesNotMatch`; аноним → 403 `AccessDenied`; skew заголовочной подписи (x-amz-date +20 мин) → 403 `RequestTimeTooSkewed`; несуществующий ключ → 403 `InvalidAccessKeyId`; битый scope → 400 `AuthorizationHeaderMalformed`; невалидная строка x-amz-date → 400 `AuthorizationHeaderMalformed` (arch-правка 2, оба исхода); SigV2-заголовок (`Authorization: AWS …`) и иной алгоритм → 400 `InvalidRequest` с Message «The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.» (arch-правка 8); presigned — все исходы §3.2 с skew-семантикой arch-правки 10: валидный GetObject → 500 (заглушка); **использование через N > 15 мин внутри окна `X-Amz-Expires` → Ok-до-заглушки (500)** — Abs-skew для прошедших дат отсутствует; **`X-Amz-Date` в будущем дальше +15 мин → 403 `RequestTimeTooSkewed`**; просрочка (`now − X-Amz-Date > X-Amz-Expires`) → 403 `AccessDenied`; `Expires=604801` → 400 `AuthorizationQueryParametersError`; отсутствие любого из 6 параметров → 400; операция вне 10 (ListObjects presigned) → 400.

- [ ] **Шаг 11.4. `AccessScenarios.cs`** (репрезентативный набор §3.5): reader GET `/{bucket}/{key}` → 500 (прошёл права, упал в заглушку); reader PUT → 403 `AccessDenied`; writer PUT → 500; writer CreateBucket → 403; admin CreateBucket → 500; reader ListParts/ListMultipartUploads → 500 (допуск без фильтра — t36-решение).

- [ ] **Шаг 11.5. `ErrorFormatScenarios.cs`**: XML-структура `Error` — парсинг ответа (все 5 элементов присутствуют: Code/Message/Resource/RequestId/HostId); `RequestId` — валидный GUID, равен заголовку `x-amz-request-id`; `HostId` = `owns3-test`; `Resource` = path-style путь запроса; HEAD-ошибка (`HEAD /no-bucket/k` битой подписью → 403) — тело пустое; каждый ответ (успех/ошибка) несёт `x-amz-request-id`.

- [ ] **Шаг 11.6. `BodyIntegrityScenarios.cs`**: PUT с hex-sha256 и порченным телом (подпись по другому хэшу) → 400 `InvalidRequest` (drain-сверка состоялась); PUT с корректным sha256 → 500 (заглушка после полной сверки); Content-MD5 несовпадение (PUT) → 400 `BadDigest`; **POST `/{bucket}?delete` с валидным `Delete`-XML и несовпадающим `Content-MD5` → 400 `BadDigest`** (сверка тела DeleteObjects через `HashingBodyStream` — impl-фикс (б); контрольный кейс с корректным MD5 → 500 заглушки); чанковый PUT: валидное тело (signer строит) → 500 после полной цепочки; битая подпись чанка 2 → 403 `SignatureDoesNotMatch`; битый trailer-checksum → 400 `BadDigest`; `STREAMING-*` на GET → 400 `InvalidRequest`.

- [ ] **Шаг 11.7. `MetricsHealthScenarios.cs`**: после нескольких запросов `/metrics` содержит `ownS3_requests_total{operation="GetObject",code="500"}` и (после сценария GetBucketLocation шага 11.2) `ownS3_requests_total{operation="GetBucketLocation",code="200"}`, а также `ownS3_request_duration_seconds` с лейблом operation (факт-форму лейблов OTel фиксирует тест по прогону — как `Shared.Metrics.UnitTests`; при отличии канонической формы от факта — правка той же комиссией по образцу правила arch/18 M3); счётчики растут между двумя запросами; `/healthz` → 200; `/metrics` не требует подписи (S3-конвейер пропускает — фиксация шага 0 задачи 9); **структурный лог**: после запроса GetObject в `factory.LogEntries` есть запись уровня Information, содержащая requestId (== заголовку `x-amz-request-id` ответа), operation=`GetObject`, bucket, key, метод, статус и durationMs — все семь полей спеки §3.4 п.8.

- [ ] **Шаг 11.8. `FailFastScenarios.cs`**: отдельная WAF-фабрика-оверрайд с in-memory конфигом `OwnS3:Root:User=ab` (короче 3) ИЛИ `OwnS3:Root:Password=short` (короче 8) → построение хоста (`CreateClient()`/`Factory.Server`) бросает `OptionsValidationException` — фиксация fail-fast главы 05 §1 без ручного запуска процесса; валидная конфигурация базовой фабрики стартует успешно (контрольный кейс).

- [ ] **Шаг 11.9. Прогон интеграционных (после юнитов, зачистка серий не нужна — докера нет):**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && \
  dotnet test tests/OwnS3.UnitTests -c Release && \
  dotnet test tests/OwnS3.IntegrationTests -c Release
```

Ожидание: обе сюиты зелёные; никакого docker-контейнера не создаётся (тесты in-memory).

- [ ] **Шаг 11.10. Коммит.**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol && \
  git add src/tests/OwnS3.IntegrationTests && \
  git commit -m "test(owns3): интеграционные сценарии WAF — роутинг (list-type, кодированный ключ)/подпись (presigned-skew arch-10)/права/ошибки/сверки тел (DeleteObjects MD5)/метрики+лог/healthz/fail-fast"
```

**Проверка задачи:** `dotnet test` обеих OwnS3-сюит зелёный; `docker ps` после прогона не содержит новых контейнеров.

**Выход:** критерии приёмки 6–7 закрыты тестами; fail-fast, структурный лог, RawTarget-путь и presigned-skew зафиксированы.

**Связь со spec:** §3.5 (IntegrationTests, все сценарии, вкл. «GetBucketLocation → 200» и presigned-кейсы arch-правки 10), решения пользователя 3, критерии 6–7.

---

### Задача 12: Финальный прогон и границы изменений

**Вход:** задачи 1–11 закоммичены.

**Files:** без новых файлов (кроме, при необходимости, точечных правок по итогам прогона — каждая с повторным прогоном).

- [ ] **Шаг 12.1. Полная сборка решения** (вся сюита монорепо должна собираться — соседи не тронуты, но проверяется отсутствие случайных поломок slnx/CPM):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && dotnet build PgWorker.slnx -c Release
```

Ожидание: `Build succeeded`, 0 warnings-as-errors.

- [ ] **Шаг 12.2. Все OwnS3-тесты сериями**: юниты → интеграция (порядок серий, между сериями докер не задействован — зачистка не требуется; убедиться по отсутствию обращений к docker в логах):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol/src && \
  dotnet test tests/OwnS3.UnitTests -c Release && \
  dotnet test tests/OwnS3.IntegrationTests -c Release
```

Ожидание: обе зелёные.

- [ ] **Шаг 12.3. Границы изменений** (критерий приёмки 9):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol && \
  git diff main --stat
```

Ожидание: только `arch/owns3/03-protocol.md`, `src/OwnS3.Protocol/**`, `src/OwnS3.Storage/**`, `src/OwnS3.App/**`, `src/tests/OwnS3.UnitTests/**`, `src/tests/OwnS3.IntegrationTests/**`, `src/PgWorker.slnx`, `src/Directory.Packages.props`, `docs/superpowers/**`. Всё постороннее — откатить/исправить до коммита.

- [ ] **Шаг 12.4. Стилевая самопроверка** (критерий 10): grep по новым исходникам `grep -rn "TBD\|TODO\|FIXME" src/OwnS3.* src/tests/OwnS3.*` — пусто; комментарии в новых файлах — русские; плейсхолдеров нет.

- [ ] **Шаг 12.5. Финальный коммит** (если шаги 12.1–12.4 потребовали правок; иначе — коммит не нужен):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t36-owns3-protocol && \
  git add -A && git commit -m "chore(owns3): финальный прогон t36 — границы изменений и стиль"
```

**Проверка задачи:** сборка всего решения зелёная; обе тестовые сюиты OwnS3 зелёные; diff соответствует границам.

**Выход:** ветка `feat-t36-owns3-protocol` готова к ревью dev-flow (дальше — гейт код-ревью; roadmap не правится здесь — пункт t36 снимается мерж-коммитом в `main` по правилу трека, spec §4 фаза 10).

**Связь со spec:** §4 фаза 10, критерии 8–10.

---

## Self-Review плана (выполнен после ревью Фазы 4 ×2 и код-ревью Фазы 7)

1. **Покрытие спеки:** §3.1 (десять arch-правок, вкл. №7 Trimall, №8 неподдерживаемый алгоритм, №9 percent-кодирование query, №10 presigned-skew «только будущее + строгая просрочка») → задача 1 (шаги 1.1–1.8) с отражением в задачах 3–5, 9, 11; §3.2 → задачи 3–7; §3.3 → задача 8; §3.4/§3.4.1 (вкл. п.8 — структурный лог) → задачи 9–10; §3.5 → юниты 3–8, 10 + задача 11 (вкл. обязательные presigned-кейсы §3.5); §3.6 → задача 2; фазы §4 → таблица соответствия; критерии 1–10 → задачи 1–12. Пробелов нет.
2. **Замечания ревью Фазы 4 (обе итерации) закрыты:** `+`→`%2B` (арх-правка 9); без `dotnet run`; GetBucketLocation-200 отдельным сценарием; структурный лог; все 6 presigned-параметров; без служебного enum-кода; строгое COPY/REPLACE; 17/19 методов; пропуск служебных путей п.0; единый `S3Xml`; enum без дублей + атрибуция 24+1; числовые ссылки кодпоинтов + безопасный writer; 12 ответных эталонов; оба исхода x-amz-date.
3. **Код-ревью Фазы 7 отражено в плане:** plan-finding №6 — `list-type` как дискриминатор СО ЗНАЧЕНИЕМ «2» (шаг 9.1: кейсы `list-type=1`/`list-type=` → v1; шаг 9.2 п.3; интеграционная фиксация в шаге 11.2 с метрикой `operation="ListObjects"`); impl-фиксы как требования/тесты: (а) `S3ModelFactory` — путь/query из `IHttpRequestFeature.RawTarget` (шаг 9.5) + интеграционные кейсы кодированного ключа `caf%C3%A9`/`100%25` с подписью по сырому пути (шаг 11.2); (б) DeleteObjects — тело через `HashingBodyStream`, Content-MD5 → `BadDigest` (шаг 10.4; интеграционный кейс в шаге 11.6); (в) `response-*` → канонические имена заголовков (`Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Content-Type`, `Expires`) — юнит-маппинг (шаг 10.1) и применение (шаг 10.4); (г) hex-валидация подписи фрейма (ровно 64 hex) до декодирования/сравнения → `InvalidRequest` (шаги 6.1/6.2); (д) known-answer CRC32C(`"123456789"`) = `0xE3069283` → base64 `4waSgw==` (RFC 3720) и CRC32 = `0xCBF43926` → `y/Q5Jg==` (ISO-HDLC) — юниты шага 6.1, big-endian упаковка в шаге 6.2. Замечания к коду (impl), не меняющие границ/проверок шагов, в план не переносились.
4. **Синхронизация со спекой (10 arch-правок):** задача 1 — новый шаг 1.6 с готовым текстом обеих точек правки №10 (замена формулировки §2 «Clock skew ±15 минут применяется и к X-Amz-Date» на skew-только-в-будущее + строгое `now − X-Amz-Date > X-Amz-Expires`; условие строки `RequestTimeTooSkewed` в таблице §5 — заголовочная ±15 мин / presigned только будущее); счётчики и проверки «десять» (§1 — п. 1–2 и 7–9, §2 — п. 10, §3 — п. 3–4, §5 — строка + уточнение, §6 — п. 5–6); задача 4 — skew заголовочной подписи помечен Abs-семантикой (разграничение с arch-правкой 10); задача 5 — шаг 5.1 ЗАМЕНЯЕТ ошибочный юнит Abs-skew прошедшей даты на кейсы §3.5 (позитив «N > 15 мин внутри окна → Ok», негатив «будущее дальше +15 мин → 403», строгая просрочка с кейсом границы `== Expires → Ok`), порядок проверок шага 5.2 переписан; задача 11 — presigned-кейсы шага 11.3 + фиксированный `TimeProvider` хоста в шаге 11.1.
5. **Консистентность типов:** `S3RequestModel`/`S3HeaderCollection` (задача 3) используются в 4–6, 9; `SigV4Result.Ok.SeedSignature` — вход `AwsChunkedReadingContext.SeedSignature` (4→6); `IObjectStore` (8) потребляется хендлерами (10) и маппится конвейером (9); `S3Xml.Serialize` объявлен в задаче 7 и использован в 10; имена `TestVectors`/`TestSigV4Signer` совпадают в юнитах и интеграционных (линк-включение).
6. **Плейсхолдеры:** единственные «…» остались в скелете `TestSigV4Signer` (задача 4) — каркас с точными сигнатурами и явно описанной реализацией через BCL; шаг 4.2 требует полной реализации. Прочие шаги содержат фактический контент.
7. **Делегированные плану решения зафиксированы:** полная таблица маршрутов (шаг 9.1, раздельные Theory на 501/400, list-type по значению), финальные сигнатуры `IObjectStore` (задача 8), тест-векторы (таблица в шапке; known-answer CRC32C/CRC32 вычислены независимо), расширение перечня сабресурсов `attributes`-на-не-GET (шаг 9.2, атрибуция плану).
