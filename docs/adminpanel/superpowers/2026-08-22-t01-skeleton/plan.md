# t01-skeleton — план реализации скелета решения AdminPanel

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Компилирующийся зелёный скелет решения AdminPanel (проекты по arch/01 §2, мета-файлы, переносы из Puzzle в Infrastructure, host с `/api/healthz`, тесты каркаса), готовый к задачам t02+.

**Architecture:** Минимальный инкремент от мета-файлов к хосту: `.slnx` → `Infrastructure` (копии Result/DI/CQRS/HealthChecks из `../Puzzle` с namespace `AdminPanel.*`) → пустые модули Core/Etcd/Probes → Api (модульная композиция, healthz) → тесты. Каждый шаг заканчивается зелёной проверкой и коммитом.

**Tech Stack:** .NET 10 (`net10.0`, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true`), CPM, `.slnx`, Minimal API, xunit v3 + FluentAssertions, WebApplicationFactory.

**Spec:** `docs/superpowers/2026-08-22-t01-skeleton/spec.md` (план аргументируется от спеки; исполнители читают оба документа).

## Global Constraints

- Все пути — от корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t01-skeleton`.
- `TreatWarningsAsErrors=true` — сборка обязана быть зелёной без подавления warning'ов (spec §2).
- Идентификаторы английские, комментарии в коде русские; тесты — по нотации AAA (`// Arrange` / `// Act` / `// Assert`) на русском (spec §2).
- Копирование из Puzzle (`/Users/demakaev/ZCodeProject/Puzzle/src/`) — команда `cp` + замена namespace `PuzzleServer.Infrastructure.App` → `AdminPanel.Infrastructure` (sed ниже); семантика не меняется, правки только на nullable-предупреждения (spec §6, §13).
- `Tracing.Init(...)` обязателен до первого использования Activity-хелперов (иначе NRE на null `ActivitySource`): в тестах — в `TestHost`, в хосте — в `Program.cs` (по образцу референса).
- Версии пакетов — только через CPM, `Version`-атрибутов в csproj нет (spec §5.2): `coverlet.collector 10.0.1`, `FluentAssertions 7.2.1`, `Microsoft.AspNetCore.Mvc.Testing 10.0.9`, `Microsoft.AspNetCore.OpenApi 10.0.9`, `Microsoft.Extensions.Configuration 10.0.9`, `Microsoft.Extensions.Configuration.Abstractions 10.0.9`, `Microsoft.Extensions.DependencyInjection 10.0.9`, `Microsoft.Extensions.DependencyInjection.Abstractions 10.0.9`, `Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions 10.0.9`, `Microsoft.Extensions.Options.ConfigurationExtensions 10.0.0`, `Microsoft.NET.Test.Sdk 18.6.0`, `xunit.runner.visualstudio 3.1.5`, `xunit.v3 3.2.2`. Если патч-версия 10.0.9 недоступна в nuget.org — ближайшая доступная в той же minor-линейке (spec §5.2).
- Ничего сверх spec: никакой бизнес-логики, эндпоинтов кроме `/api/healthz`, OTel/Aspire/Npgsql/Testcontainers/Dockerfile/frontend (spec §10).
- Коммит-стиль репо: conventional + русские описания (`feat(t01): …`, `docs(t01): …`).

---

### Task 1: Мета-каркас решения и фиксация спеки

**Files:**
- Create: `.gitignore` (корень)
- Create: `src/Directory.Build.props`
- Create: `src/Directory.Packages.props`
- Create: `src/NuGet.Config`
- Create: `src/.editorconfig`
- Create: `src/AdminPanel.slnx`
- Commit: `docs/superpowers/2026-08-22-t01-skeleton/` (артефакты Фаз 1 и 3)

**Interfaces:**
- Consumes: ничего (первая задача).
- Produces: дерево `src/` с мета-файлами; решение `.slnx`, к которому последующие задачи добавляют проекты.

- [ ] **Step 1: Закоммитить spec.md и plan.md (артефакты Фаз 1 и 3)**

```bash
git add docs/superpowers/2026-08-22-t01-skeleton/
git commit -m "docs(t01): спецификация и план скелета решения"
```

- [ ] **Step 2: Создать `.gitignore` (корень) — VS-набор Puzzle + специфика AdminPanel**

```bash
cp /Users/demakaev/ZCodeProject/Puzzle/.gitignore /Users/demakaev/ZCodeProject/worktrees/feat-t01-skeleton/.gitignore
```

Дописать в конец файла блок (включая `.dev-flow/` — текущая строка единственного .gitignore при копировании теряется):

```gitignore

# AdminPanel specifics
.dev-flow/
.DS_Store
node_modules/
dist/
```

- [ ] **Step 3: Создать `src/Directory.Build.props`**

```xml
<Project>
    <PropertyGroup>
        <LangVersion>latest</LangVersion>
        <TargetFramework>net10.0</TargetFramework>
        <ImplicitUsings>enable</ImplicitUsings>
        <Nullable>enable</Nullable>
        <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
        <IsPackable>false</IsPackable>
    </PropertyGroup>
</Project>
```

- [ ] **Step 4: Создать `src/Directory.Packages.props`**

```xml
<Project>
  <PropertyGroup>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <EnablePackageVersionOverride>false</EnablePackageVersionOverride>
  </PropertyGroup>
  <ItemGroup>
    <PackageVersion Include="coverlet.collector" Version="10.0.1" />
    <PackageVersion Include="FluentAssertions" Version="7.2.1" />
    <PackageVersion Include="Microsoft.AspNetCore.Mvc.Testing" Version="10.0.9" />
    <PackageVersion Include="Microsoft.AspNetCore.OpenApi" Version="10.0.9" />
    <PackageVersion Include="Microsoft.Extensions.Configuration" Version="10.0.9" />
    <PackageVersion Include="Microsoft.Extensions.Configuration.Abstractions" Version="10.0.9" />
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection" Version="10.0.9" />
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="10.0.9" />
    <PackageVersion Include="Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions" Version="10.0.9" />
    <PackageVersion Include="Microsoft.Extensions.Options.ConfigurationExtensions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="18.6.0" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="3.1.5" />
    <PackageVersion Include="xunit.v3" Version="3.2.2" />
  </ItemGroup>
</Project>
```

- [ ] **Step 5: Скопировать `src/NuGet.Config` и `src/.editorconfig` из Puzzle**

```bash
mkdir -p src
cp /Users/demakaev/ZCodeProject/Puzzle/src/NuGet.Config src/NuGet.Config
cp /Users/demakaev/ZCodeProject/Puzzle/src/.editorconfig src/.editorconfig
```

- [ ] **Step 6: Создать `src/AdminPanel.slnx` (пока только common-папка)**

```xml
<Solution>
    <Folder Name="/common/">
        <File Path="Directory.Build.props" />
        <File Path="Directory.Packages.props" />
    </Folder>
</Solution>
```

- [ ] **Step 7: Проверить restore пустого решения**

Run: `dotnet restore src/AdminPanel.slnx`
Expected: успех (0 errors); ошибки конфигурации CPM/NuGet.Config отсутствуют.

- [ ] **Step 8: Commit**

```bash
git add .gitignore src/
git commit -m "feat(t01): мета-каркас решения — slnx, Build/Packages.props, NuGet.Config, editorconfig, gitignore"
```

---

### Task 2: Проект Infrastructure с Result-монадой и UnitTests-каркас

**Files:**
- Create: `src/AdminPanel.Infrastructure/AdminPanel.Infrastructure.csproj`
- Create: `src/AdminPanel.Infrastructure/Result.cs` (копия из Puzzle)
- Modify: `src/AdminPanel.slnx` (добавить Infrastructure и tests/UnitTests)
- Create: `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`
- Test: `src/tests/AdminPanel.UnitTests/ResultTests.cs`

**Interfaces:**
- Consumes: мета-файлы Task 1.
- Produces: `AdminPanel.Infrastructure.Result`, `Result<T>`, `ResultSuccess`, `ResultError`, `ResultExtensions` (API идентичен Puzzle: `IsSuccess`, `Success()`, `Failed(Exception)`, `FromValue<T>(T?, string)`, `Bind/Map/Match/Apply` + async + `CollBind`).

- [ ] **Step 1: Создать `src/AdminPanel.Infrastructure/AdminPanel.Infrastructure.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="Microsoft.Extensions.Configuration.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks.Abstractions"/>
        <PackageReference Include="Microsoft.Extensions.Options.ConfigurationExtensions"/>
    </ItemGroup>

</Project>
```

- [ ] **Step 2: Создать тестовый проект `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="coverlet.collector">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="FluentAssertions"/>
        <PackageReference Include="Microsoft.Extensions.Configuration"/>
        <PackageReference Include="Microsoft.Extensions.DependencyInjection"/>
        <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions"/>
        <PackageReference Include="Microsoft.NET.Test.Sdk"/>
        <PackageReference Include="xunit.runner.visualstudio">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="xunit.v3"/>
    </ItemGroup>

    <ItemGroup>
        <ProjectReference Include="..\..\AdminPanel.Infrastructure\AdminPanel.Infrastructure.csproj"/>
    </ItemGroup>

</Project>
```

- [ ] **Step 3: Написать `ResultTests.cs` (красный — Result ещё не перенесён)**

```csharp
using FluentAssertions;
using AdminPanel.Infrastructure;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты монады Result, перенесённой из референса Puzzle.
public class ResultTests
{
    [Fact]
    public void Success_IsSuccessTrue_AndMatchChoosesSuccessBranch()
    {
        // Arrange
        var result = Result<string>.Success("value");

        // Act
        var matched = result.Match(v => $"ok:{v}", e => $"err:{e.Message}");

        // Assert
        result.IsSuccess.Should().BeTrue();
        matched.Should().Be("ok:value");
    }

    [Fact]
    public void Failed_IsSuccessFalse_AndMatchChoosesFailureBranch()
    {
        // Arrange
        var result = Result<string>.Failed(new InvalidOperationException("boom"));

        // Act
        var matched = result.Match(v => $"ok:{v}", e => $"err:{e.Message}");

        // Assert
        result.IsSuccess.Should().BeFalse();
        matched.Should().Be("err:boom");
    }

    [Fact]
    public void Bind_OnSuccess_ChainsNextResult()
    {
        // Arrange
        var result = Result<int>.Success(2);

        // Act
        var bound = result.Bind(v => Result<string>.Success($"v={v * 2}"));

        // Assert
        bound.IsSuccess.Should().BeTrue();
        bound.Value.Should().Be("v=4");
    }

    [Fact]
    public void Map_OnFailure_PropagatesError()
    {
        // Arrange
        var error = new InvalidOperationException("boom");
        var result = Result<int>.Failed(error);

        // Act
        var mapped = result.Map(v => v * 2);

        // Assert
        mapped.IsSuccess.Should().BeFalse();
        mapped.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void From_WithThrowingDelegate_ReturnsFailure()
    {
        // Arrange
        // Act
        var result = Result<int>.From(() => throw new ArgumentException("bad"));

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ArgumentException>();
    }

    [Fact]
    public void FromValue_WithNull_ReturnsFailure()
    {
        // Arrange
        // Act
        var result = Result.FromValue((string?)null, "no value");

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Message.Should().Be("no value");
    }

    [Fact]
    public void CollBind_StopsOnFirstFailure()
    {
        // Arrange
        var items = new[] { 1, 2, 3 };
        var visited = 0;
        var target = new InvalidOperationException("stop");

        // Act
        var result = items.CollBind<int>(i =>
        {
            visited++;
            return i == 2 ? Result.Failed(target) : Result.Success();
        });

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(target);
        visited.Should().Be(2);
    }
}
```

- [ ] **Step 4: Прогнать тест — убедиться, что красный**

Run: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`
Expected: FAIL компиляцией — `AdminPanel.Infrastructure.Result` не существует (CS0234).

- [ ] **Step 5: Скопировать Result.cs и заменить namespace**

```bash
cp /Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/Result.cs \
   src/AdminPanel.Infrastructure/Result.cs
sed -i '' 's/PuzzleServer\.Infrastructure\.App/AdminPanel.Infrastructure/g' src/AdminPanel.Infrastructure/Result.cs
```

Если `TreatWarningsAsErrors` подсветит nullable-warning'и — погасить минимальными правками (например `= null!`), не меняя семантики.

- [ ] **Step 6: Добавить проекты в `src/AdminPanel.slnx`**

Итоговое содержимое файла:

```xml
<Solution>
    <Folder Name="/common/">
        <File Path="Directory.Build.props" />
        <File Path="Directory.Packages.props" />
    </Folder>
    <Folder Name="/infrastructure/">
        <Project Path="AdminPanel.Infrastructure/AdminPanel.Infrastructure.csproj" />
    </Folder>
    <Folder Name="/tests/">
        <Project Path="tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj" />
    </Folder>
</Solution>
```

- [ ] **Step 7: Прогнать тесты — зелёные**

Run: `dotnet test src/AdminPanel.slnx`
Expected: PASS, все 7 тестов `ResultTests` зелёные, 0 warnings.

- [ ] **Step 8: Commit**

```bash
git add src/AdminPanel.slnx src/AdminPanel.Infrastructure src/tests
git commit -m "feat(t01): Infrastructure с Result-монадой (копия из Puzzle) и UnitTests-каркас"
```

---

### Task 3: Attribute-DI, [Config], UseDiBehaviours и регистрация каркаса

**Files:**
- Create: `src/AdminPanel.Infrastructure/DI/InjectAs.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/DI/ConfigAttribute.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/DI/DiTypeBehaviour.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/DI/AutoRegistrationDiTypeBehaviour.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/DI/AutoRegistrationConfigDiTypeBehaviour.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/DI/ServiceCollectionExtensions.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/DI/ServiceProviderExtensions.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/DI/UseDiBehavioursExtensions.cs` (новый)
- Create: `src/AdminPanel.Infrastructure/Traces/Tracing.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/Contexts/ServiceProviderHelper.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/ModuleExtensions.cs` (новый)
- Test: `src/tests/AdminPanel.UnitTests/AutoRegistrationTests.cs`, `src/tests/AdminPanel.UnitTests/TestHost.cs`

**Interfaces:**
- Consumes: `Result` (Task 2).
- Produces: `[InjectAsSingleton/Scoped/TransientAttribute]`, `[ConfigAttribute]`, `services.AutoRegistration(Assembly)`, `services.UseDiBehaviours(IConfiguration)` — всё в `AdminPanel.Infrastructure.DI`; `services.AddInfrastructure()` (скан сборки Infrastructure); статический `Tracing.Init(name)`/`ActivityT(...)`; `IServiceProviderHelper`.

- [ ] **Step 1: Скопировать 7 DI-файлов из Puzzle и заменить namespace**

```bash
mkdir -p src/AdminPanel.Infrastructure/DI
for f in InjectAs.cs ConfigAttribute.cs DiTypeBehaviour.cs \
         AutoRegistrationDiTypeBehaviour.cs AutoRegistrationConfigDiTypeBehaviour.cs \
         ServiceCollectionExtensions.cs ServiceProviderExtensions.cs; do
  cp "/Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/DI/$f" \
     "src/AdminPanel.Infrastructure/DI/$f"
done
sed -i '' 's/PuzzleServer\.Infrastructure\.App/AdminPanel.Infrastructure/g' src/AdminPanel.Infrastructure/DI/*.cs
```

- [ ] **Step 2: Создать `src/AdminPanel.Infrastructure/DI/UseDiBehavioursExtensions.cs`**

```csharp
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Infrastructure.DI;

// Включает DI-поведения авто-регистрации для сборок, передаваемых в AutoRegistration.
public static class UseDiBehavioursExtensions
{
    public static IServiceCollection UseDiBehaviours(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        new AutoRegistrationDiTypeBehaviour(services).UseBehaviour();
        new AutoRegistrationConfigDiTypeBehaviour(services, configuration).UseBehaviour();
        return services;
    }
}
```

- [ ] **Step 3: Скопировать Tracing и ServiceProviderHelper, создать ModuleExtensions**

```bash
mkdir -p src/AdminPanel.Infrastructure/Traces src/AdminPanel.Infrastructure/Contexts
cp /Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/Traces/Tracing.cs \
   src/AdminPanel.Infrastructure/Traces/Tracing.cs
cp /Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/Contexts/ServiceProviderHelper.cs \
   src/AdminPanel.Infrastructure/Contexts/ServiceProviderHelper.cs
sed -i '' 's/PuzzleServer\.Infrastructure\.App/AdminPanel.Infrastructure/g' \
   src/AdminPanel.Infrastructure/Traces/Tracing.cs \
   src/AdminPanel.Infrastructure/Contexts/ServiceProviderHelper.cs
```

`src/AdminPanel.Infrastructure/ModuleExtensions.cs`:

```csharp
using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Infrastructure;

// Модуль каркаса: регистрирует все типы сборки через attribute-DI.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddInfrastructure(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
```

- [ ] **Step 4: Создать `src/tests/AdminPanel.UnitTests/TestHost.cs`**

```csharp
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.UnitTests;

// Единая точка DI-регистрации тестовой сборки: скан сборок выполняется ровно один раз
// (ServiceCollectionExtensions кеширует просканированные сборки в статическом состоянии).
public static class TestHost
{
    private static readonly Lazy<ServiceCollection> Services = new(CreateCollection);

    public static IServiceProvider BuildProvider()
        => Services.Value.BuildServiceProvider();

    private static ServiceCollection CreateCollection()
    {
        // Инициализация ActivitySource до первого HandleQuery — иначе NRE в Tracing.
        Tracing.Init("AdminPanel.UnitTests");

        // Arrange-часть всех DI-тестов: in-memory конфигурация с тестовой секцией.
        var configuration = new ConfigurationBuilder()
           .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["TestConfigOptions:Value"] = "test-value",
            })
           .Build();

        var services = new ServiceCollection();
        services.UseDiBehaviours(configuration);
        services.AutoRegistration(typeof(TestHost).Assembly);
        // Скан сборки каркаса: ServiceProviderHelper, IHandler (с Task 4) и будущие сервисы.
        services.AddInfrastructure();
        return services;
    }
}
```

- [ ] **Step 5: Написать `AutoRegistrationTests.cs`**

```csharp
using AdminPanel.Infrastructure.DI;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты attribute-DI: авто-регистрация сервисов и биндинг [Config]-POCO.
public class AutoRegistrationTests
{
    [Fact]
    public void InjectAsSingleton_RegistersTypeAndInterface()
    {
        // Arrange
        var provider = TestHost.BuildProvider();

        // Act
        var bySelf = provider.GetRequiredService<SingletonService>();
        var byInterface = provider.GetRequiredService<ISingletonService>();

        // Assert
        bySelf.Should().BeSameAs(byInterface);
    }

    [Fact]
    public void Config_BindsPocoFromConfiguration()
    {
        // Arrange
        var provider = TestHost.BuildProvider();

        // Act
        var options = provider.GetRequiredService<IOptions<TestConfigOptions>>();

        // Assert
        options.Value.Value.Should().Be("test-value");
    }
}

// Тестовый сервис с singleton-регистрацией через атрибут.
[InjectAsSingleton]
public class SingletonService : ISingletonService;

public interface ISingletonService;

// Тестовый [Config]-POCO: секция "TestConfigOptions" из in-memory конфигурации TestHost.
[Config]
public class TestConfigOptions
{
    public string? Value { get; set; }
}
```

- [ ] **Step 6: Прогнать тесты — зелёные**

Run: `dotnet test src/AdminPanel.slnx`
Expected: PASS — `AutoRegistrationTests` (2 теста) + `ResultTests` (7) зелёные, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/AdminPanel.Infrastructure src/tests/AdminPanel.UnitTests
git commit -m "feat(t01): attribute-DI ([InjectAs*], [Config], AutoRegistration), UseDiBehaviours и AddInfrastructure"
```

---

### Task 4: CQRS query-only

**Files:**
- Create: `src/AdminPanel.Infrastructure/CQRS/IQuery.cs`
- Create: `src/AdminPanel.Infrastructure/CQRS/IQueryHandler.cs`
- Create: `src/AdminPanel.Infrastructure/CQRS/IHandler.cs`
- Test: `src/tests/AdminPanel.UnitTests/CQRSTests.cs`

**Interfaces:**
- Consumes: `Result` (Task 2); `[InjectAs*]`/`AutoRegistration`/`AddInfrastructure`/`Tracing`/`IServiceProviderHelper` (Task 3); `TestHost.BuildProvider()` (Task 3).
- Produces: `IQuery<T>` (`AdminPanel.Infrastructure.CQRS`), `IQueryHandler<in TQ, TR>.Handle(TQ, CancellationToken) → ValueTask<Result<TR>>`, `IHandler.HandleQuery<Q, T>(Q, CancellationToken) → ValueTask<Result<T>>` — `IHandler` регистрируется в DI через `AddInfrastructure()` (скан сборки Infrastructure).

- [ ] **Step 1: Написать `CQRSTests.cs` (красный — CQRS-типов ещё нет)**

```csharp
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты query-диспетчера: резолв IHandler из AutoRegistration и вызов хендлера из корневого провайдера.
public class CQRSTests
{
    [Fact]
    public async Task HandleQuery_FromRootProvider_ReturnsHandlerValue()
    {
        // Arrange
        var provider = TestHost.BuildProvider();
        var handler = provider.GetRequiredService<IHandler>();

        // Act
        var result = await handler.HandleQuery(new TestQuery(), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("pong");
    }
}

// Тестовый запрос.
public sealed record TestQuery : IQuery<string>;

// Тестовый хендлер: scoped — диспетчер обязан корректно его резолвить из корневого провайдера.
[InjectAsScoped]
public class TestQueryHandler : IQueryHandler<TestQuery, string>
{
    public ValueTask<Result<string>> Handle(TestQuery query, CancellationToken ct)
        => new(Result<string>.Success("pong"));
}
```

- [ ] **Step 2: Прогнать сборку — красный**

Run: `dotnet build src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj`
Expected: FAIL компиляцией — `AdminPanel.Infrastructure.CQRS` не существует.

- [ ] **Step 3: Создать `src/AdminPanel.Infrastructure/CQRS/IQuery.cs`**

```csharp
namespace AdminPanel.Infrastructure.CQRS;

// Маркерный интерфейс запроса (чтение); команды в панели не заводятся.
public interface IQuery<T>;
```

- [ ] **Step 4: Создать `src/AdminPanel.Infrastructure/CQRS/IQueryHandler.cs`**

```csharp
namespace AdminPanel.Infrastructure.CQRS;

// Хендлер запроса: чистое чтение, без транзакций и контекста БД.
public interface IQueryHandler<in TQ, TR>
    where TQ : IQuery<TR>
{
    ValueTask<Result<TR>> Handle(TQ query, CancellationToken ct);
}
```

- [ ] **Step 5: Создать `src/AdminPanel.Infrastructure/CQRS/IHandler.cs`**

```csharp
using System.Diagnostics;
using AdminPanel.Infrastructure.Contexts;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Infrastructure.CQRS;

// Диспетчер запросов: открывает scope при вызове из корневого провайдера и обрамляет выполнение Activity.
public interface IHandler
{
    ValueTask<Result<T>> HandleQuery<Q, T>(Q query, CancellationToken ct)
        where Q : IQuery<T>;
}

[InjectAsTransient]
internal class Handler(IServiceProviderHelper spHelper, IServiceProvider sp) : IHandler
{
    public async ValueTask<Result<T>> HandleQuery<Q, T>(Q query, CancellationToken ct)
        where Q : IQuery<T>
    {
        Result<T> result = null!;
        await Tracing.ActivityT(
            TypeName<Q>(),
            ActivityKind.Server,
            () => Run(async isp =>
            {
                var handler = isp.GetRequiredService<IQueryHandler<Q, T>>();
                result = await handler.Handle(query, ct);
            }));
        return result;
    }

    private async Task Run(Func<IServiceProvider, ValueTask> func)
    {
        if (spHelper.IsGlobal(sp))
        {
            using var scope = sp.CreateScope();
            await func(scope.ServiceProvider);
        }
        else
            await func(sp);
    }

    private static string TypeName<T>()
        => !typeof(T).IsGenericType
            ? typeof(T).Name
            : typeof(T).Name + string.Join(",", typeof(T).GenericTypeArguments.Select(x => x.Name));
}
```

- [ ] **Step 6: Прогнать тесты — зелёные**

Run: `dotnet test src/AdminPanel.slnx`
Expected: PASS — CQRSTests (1) + AutoRegistrationTests (2) + ResultTests (7), 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add src/AdminPanel.Infrastructure/CQRS src/tests/AdminPanel.UnitTests/CQRSTests.cs
git commit -m "feat(t01): CQRS query-only (IQuery/IQueryHandler/IHandler) с scope и Activity"
```

---

### Task 5: Health-check базис

**Files:**
- Create: `src/AdminPanel.Infrastructure/HealthChecks/IHealthCheckService.cs` (копия)
- Create: `src/AdminPanel.Infrastructure/HealthChecks/HealthCheckAbstract.cs` (копия)

**Interfaces:**
- Consumes: `Result` (Task 2), attribute-DI (Task 3).
- Produces: `IHealthCheckService` (Inited/Working/StatusError), `HealthCheckAbstract<T> : IHealthCheck` — базис для health-проверок будущих hosted-сервисов (SnapshotRefresher, t03+).

- [ ] **Step 1: Скопировать HealthChecks из Puzzle**

```bash
mkdir -p src/AdminPanel.Infrastructure/HealthChecks
cp /Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/HealthChecks/IHealthCheckService.cs \
   src/AdminPanel.Infrastructure/HealthChecks/IHealthCheckService.cs
cp /Users/demakaev/ZCodeProject/Puzzle/src/PuzzleServer.Infrastructure.App/HealthChecks/HealthCheckAbstract.cs \
   src/AdminPanel.Infrastructure/HealthChecks/HealthCheckAbstract.cs
sed -i '' 's/PuzzleServer\.Infrastructure\.App/AdminPanel.Infrastructure/g' \
   src/AdminPanel.Infrastructure/HealthChecks/*.cs
```

- [ ] **Step 2: Проверить сборку**

Run: `dotnet build src/AdminPanel.slnx`
Expected: успех, 0 errors / 0 warnings (новые тесты не добавляются — HealthCheckAbstract покрывается будущими задачами через hosted-сервисы).

- [ ] **Step 3: Commit**

```bash
git add src/AdminPanel.Infrastructure/HealthChecks
git commit -m "feat(t01): health-check базис из Puzzle (IHealthCheckService, HealthCheckAbstract)"
```

---

### Task 6: Пустые доменные проекты Core, Etcd, Probes

**Files:**
- Create: `src/AdminPanel.Core/AdminPanel.Core.csproj` + `ModuleExtensions.cs`
- Create: `src/AdminPanel.Etcd/AdminPanel.Etcd.csproj` + `ModuleExtensions.cs`
- Create: `src/AdminPanel.Probes/AdminPanel.Probes.csproj` + `ModuleExtensions.cs`
- Modify: `src/AdminPanel.slnx`

**Interfaces:**
- Consumes: `AutoRegistration` (Task 3).
- Produces: `services.AddCore()` / `AddEtcd()` / `AddProbes()` — пустые модули для задач t02+ (направления зависимостей: Core→Infrastructure, Etcd→Core, Probes→Core).

- [ ] **Step 1: Создать `src/AdminPanel.Core/AdminPanel.Core.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <ProjectReference Include="..\AdminPanel.Infrastructure\AdminPanel.Infrastructure.csproj"/>
    </ItemGroup>

</Project>
```

- [ ] **Step 2: Создать `src/AdminPanel.Core/ModuleExtensions.cs`**

```csharp
using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Core;

// Модуль домена снапшота: пока пуст, наполняется задачами t02+.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddCore(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
```

- [ ] **Step 3: Создать `src/AdminPanel.Etcd/AdminPanel.Etcd.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <ProjectReference Include="..\AdminPanel.Core\AdminPanel.Core.csproj"/>
    </ItemGroup>

</Project>
```

- [ ] **Step 4: Создать `src/AdminPanel.Etcd/ModuleExtensions.cs`**

```csharp
using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Etcd;

// Модуль etcd-клиента: пока пуст, наполняется задачами t03+.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddEtcd(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
```

- [ ] **Step 5: Создать `src/AdminPanel.Probes/AdminPanel.Probes.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <ProjectReference Include="..\AdminPanel.Core\AdminPanel.Core.csproj"/>
    </ItemGroup>

</Project>
```

- [ ] **Step 6: Создать `src/AdminPanel.Probes/ModuleExtensions.cs`**

```csharp
using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Probes;

// Модуль live-проб: пока пуст, наполняется задачами t05+.
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddProbes(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
```

- [ ] **Step 7: Добавить проекты в `src/AdminPanel.slnx`**

Вставить внутрь `<Solution>` (после `/infrastructure/`):

```xml
    <Folder Name="/core/">
        <Project Path="AdminPanel.Core/AdminPanel.Core.csproj" />
    </Folder>
    <Folder Name="/etcd/">
        <Project Path="AdminPanel.Etcd/AdminPanel.Etcd.csproj" />
    </Folder>
    <Folder Name="/probes/">
        <Project Path="AdminPanel.Probes/AdminPanel.Probes.csproj" />
    </Folder>
```

- [ ] **Step 8: Проверить сборку**

Run: `dotnet build src/AdminPanel.slnx`
Expected: успех, 0 errors / 0 warnings.

- [ ] **Step 9: Commit**

```bash
git add src/AdminPanel.slnx src/AdminPanel.Core src/AdminPanel.Etcd src/AdminPanel.Probes
git commit -m "feat(t01): пустые доменные проекты Core/Etcd/Probes с ModuleExtensions"
```

---

### Task 7: Api-хост с /api/healthz

**Files:**
- Create: `src/AdminPanel.Api/AdminPanel.Api.csproj`
- Create: `src/AdminPanel.Api/Program.cs`
- Create: `src/AdminPanel.Api/HealthzWriter.cs`
- Create: `src/AdminPanel.Api/appsettings.json`, `appsettings.Development.json`
- Create: `src/AdminPanel.Api/Properties/launchSettings.json`
- Modify: `src/AdminPanel.slnx`

**Interfaces:**
- Consumes: `UseDiBehaviours`/`AddInfrastructure` (Task 3), `AddCore/AddEtcd/AddProbes` (Task 6), `Tracing.Init` (Task 3).
- Produces: запускаемый host; `public partial class Program` для WebApplicationFactory (Task 8); `GET /api/healthz` → `200 {"status":"ok"}`.

- [ ] **Step 1: Создать `src/AdminPanel.Api/AdminPanel.Api.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">

    <ItemGroup>
        <PackageReference Include="Microsoft.AspNetCore.OpenApi"/>
    </ItemGroup>

    <ItemGroup>
        <ProjectReference Include="..\AdminPanel.Core\AdminPanel.Core.csproj"/>
        <ProjectReference Include="..\AdminPanel.Etcd\AdminPanel.Etcd.csproj"/>
        <ProjectReference Include="..\AdminPanel.Infrastructure\AdminPanel.Infrastructure.csproj"/>
        <ProjectReference Include="..\AdminPanel.Probes\AdminPanel.Probes.csproj"/>
    </ItemGroup>

</Project>
```

- [ ] **Step 2: Создать `src/AdminPanel.Api/HealthzWriter.cs`**

```csharp
using System.Text.Json;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AdminPanel.Api;

// Пишет компактный JSON-ответ контракта /api/healthz: {"status":"ok"} и производные статусы.
public static class HealthzWriter
{
    public static async Task WriteStatus(HttpContext context, HealthReport report)
    {
        var status = report.Status switch
        {
            HealthStatus.Healthy => "ok",
            HealthStatus.Degraded => "degraded",
            _ => "unhealthy",
        };
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(new { status }));
    }
}
```

- [ ] **Step 3: Создать `src/AdminPanel.Api/Program.cs`**

```csharp
using AdminPanel.Api;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using AdminPanel.Probes;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

// Точка входа панели: сборка хоста и модульная композиция сервисов.
var builder = WebApplication.CreateBuilder(args);

// Инициализация ActivitySource каркаса до первого HandleQuery (по образцу референса).
Tracing.Init(builder.Environment.ApplicationName);

builder
   .Services.UseDiBehaviours(builder.Configuration)
   .AddInfrastructure()
   .AddCore()
   .AddEtcd()
   .AddProbes()
   .AddOpenApi()
   .AddHealthChecks()
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"]);

var app = builder.Build();

// OpenAPI-схема — только в dev-окружении.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Живость самой панели; без авторизации (auth-модуль — t02).
app.MapHealthChecks(
    "/api/healthz",
    new HealthCheckOptions { ResponseWriter = HealthzWriter.WriteStatus });

app.Run();

// Экспозиция точки входа для WebApplicationFactory в интеграционных тестах.
public partial class Program;
```

- [ ] **Step 4: Создать `appsettings.json` и `appsettings.Development.json`**

`src/AdminPanel.Api/appsettings.json`:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "AllowedHosts": "*"
}
```

`src/AdminPanel.Api/appsettings.Development.json` — идентичное содержимое.

- [ ] **Step 5: Создать `src/AdminPanel.Api/Properties/launchSettings.json`**

```json
{
  "$schema": "https://json.schemastore.org/launchsettings.json",
  "profiles": {
    "http": {
      "commandName": "Project",
      "environmentVariables": {
        "ASPNETCORE_ENVIRONMENT": "Development"
      },
      "applicationUrl": "http://localhost:5000"
    }
  }
}
```

- [ ] **Step 6: Добавить Api в `src/AdminPanel.slnx`**

Вставить перед `</Solution>`:

```xml
    <Project Path="AdminPanel.Api/AdminPanel.Api.csproj" />
```

- [ ] **Step 7: Собрать**

Run: `dotnet build src/AdminPanel.slnx`
Expected: успех, 0 errors / 0 warnings.

- [ ] **Step 8: Запустить и проверить healthz**

```bash
dotnet run --project src/AdminPanel.Api --launch-profile http &
sleep 5
curl -s -w '\nHTTP %{http_code}\n' http://localhost:5000/api/healthz
pkill -f AdminPanel.Api || true
```

Expected: тело `{"status":"ok"}`, `HTTP 200`. (`pkill` гасит и дочерние dotnet-процессы; `|| true` — чтобы шаг не падал, если процесс уже завершился.)

- [ ] **Step 9: Commit**

```bash
git add src/AdminPanel.slnx src/AdminPanel.Api
git commit -m "feat(t01): Api-хост — модульная композиция и /api/healthz"
```

---

### Task 8: IntegrationTests — healthz-смоук через WebApplicationFactory

**Files:**
- Create: `src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj`
- Test: `src/tests/AdminPanel.IntegrationTests/HealthzTests.cs`
- Modify: `src/AdminPanel.slnx`

**Interfaces:**
- Consumes: `Program` (Task 7, `public partial class Program`).
- Produces: integration-проект для будущих задач (refresher против реального etcd — t03+).

- [ ] **Step 1: Создать `src/tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">

    <ItemGroup>
        <PackageReference Include="coverlet.collector">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="FluentAssertions"/>
        <PackageReference Include="Microsoft.AspNetCore.Mvc.Testing"/>
        <PackageReference Include="Microsoft.NET.Test.Sdk"/>
        <PackageReference Include="xunit.runner.visualstudio">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="xunit.v3"/>
    </ItemGroup>

    <ItemGroup>
        <ProjectReference Include="..\..\AdminPanel.Api\AdminPanel.Api.csproj"/>
    </ItemGroup>

</Project>
```

- [ ] **Step 2: Написать `HealthzTests.cs`**

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Смоук живости панели: /api/healthz без авторизации отвечает контрактом {"status":"ok"}.
public class HealthzTests
{
    [Fact]
    public async Task Healthz_ReturnsOkStatus()
    {
        // Arrange
        await using var factory = new WebApplicationFactory<Program>();
        using var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/api/healthz");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        body.GetProperty("status").GetString().Should().Be("ok");
    }
}
```

- [ ] **Step 3: Добавить проект в `src/AdminPanel.slnx`**

Вставить в папку `/tests/` (после UnitTests):

```xml
        <Project Path="tests/AdminPanel.IntegrationTests/AdminPanel.IntegrationTests.csproj" />
```

- [ ] **Step 4: Прогнать все тесты**

Run: `dotnet test src/AdminPanel.slnx`
Expected: PASS — UnitTests (10) + IntegrationTests (1) зелёные, 0 warnings, Docker не требуется.

- [ ] **Step 5: Commit**

```bash
git add src/AdminPanel.slnx src/tests/AdminPanel.IntegrationTests
git commit -m "feat(t01): IntegrationTests — healthz-смоук через WebApplicationFactory"
```

---

### Task 9: Начальный README и roadmap-деливерабл

**Files:**
- Create: `README.md` (корень)
- Modify: `arch/roadmap/infra.md` (удалить пункт `t01-skeleton`)

**Interfaces:**
- Consumes: всё построенное ранее (состав решения для README).
- Produces: roadmap без пункта `t01-skeleton` (правило мерж-гейта — тем же коммитом; зависимость `t02-auth ← t01-skeleton` в соседнем пункте остаётся — это норма).

- [ ] **Step 1: Создать `README.md`**

```markdown
# AdminPanel

Read-only панель администрирования шардированных HA-кластеров PostgreSQL
(инспектируемая система — репозиторий `../pg`): etcd, шардирование, HA, алерты.
Операций нет — панель ничего не мутирует.

Статус: скелет решения (задача `t01-skeleton`); архитектура и план — в
[`arch/`](arch/01-architecture.md), дорожная карта — в
[`arch/roadmap/`](arch/roadmap/README.md).

## Стек

.NET 10 (C# `LangVersion=latest`, `Nullable=enable`, warnings как ошибки),
ASP.NET Core Minimal API, централизованное версионирование пакетов (CPM),
решение в формате `.slnx`. Каркас (Result-монада, attribute-DI, CQRS-queries)
перенесён из референсного проекта `../Puzzle`.

## Структура

- `src/AdminPanel.Api` — host: модульная композиция, REST-эндпоинты, `/api/healthz`
- `src/AdminPanel.Core` — домен снапшота (наполняется задачами t02+)
- `src/AdminPanel.Etcd` — etcd-клиент и SnapshotRefresher (t03+)
- `src/AdminPanel.Probes` — live-пробы Patroni/SQL (t05+)
- `src/AdminPanel.Infrastructure` — каркас: Result, attribute-DI, CQRS, health-checks
- `src/tests/` — UnitTests (xunit v3 + FluentAssertions), IntegrationTests

## Сборка и запуск

    dotnet build src/AdminPanel.slnx
    dotnet test src/AdminPanel.slnx
    dotnet run --project src/AdminPanel.Api
    curl http://localhost:5000/api/healthz   # {"status":"ok"}

Тесты Docker не требуют. Фронтенд (React+Vite) и поставка в контейнере —
будущие задачи дорожной карты.
```

- [ ] **Step 2: Удалить пункт `t01-skeleton` из `arch/roadmap/infra.md`**

В файле `arch/roadmap/infra.md` удалить строки 8–18 — весь пункт:

```markdown
- `t01-skeleton` — скелет решения. `src/AdminPanel.slnx` + проекты
  `Api`, `Infrastructure`, `Core`, `Etcd`, `Probes` (пустые),
  `tests/UnitTests`, `tests/IntegrationTests`; `src/Directory.Build.props`
  (`net10.0`, `LangVersion=latest`, `Nullable=enable`,
  `TreatWarningsAsErrors=true`), `Directory.Packages.props` (CPM),
  `NuGet.Config`, `.editorconfig` — по образцу Puzzle. Скопировать в
  `Infrastructure` и адаптировать: `Result`-монада, attribute-DI
  (`[InjectAs*]`, `[Config]`, `AutoRegistration`), CQRS
  (`IQuery<T>`/`IQueryHandler`, `IHandler`; команды не заводить),
  health-check базис. `Program.cs` — модульная композиция, `GET /api/healthz`.
  Результат: `dotnet build`/`dotnet test` зелёные, пустой API отвечает.
```

Зависимость `t02-auth ← t01-skeleton` в следующем пункте НЕ трогать (правило
`arch/roadmap/README.md` — удаляется только строка-пункт; упоминание тега в
`←`-нотации соседних пунктов остаётся).

- [ ] **Step 3: Commit**

```bash
git add README.md arch/roadmap/infra.md
git commit -m "docs(t01): начальный README; мерж-гейт — пункт t01-skeleton удалён из roadmap"
```

---

### Task 10: Финальная верификация по критериям приёмки спеки

**Files:**
- Read-only проверка; правки только если верификация что-то вскрыла (тогда fix-коммит).

**Interfaces:**
- Consumes: Tasks 1–9.
- Produces: подтверждение критериев §12 спеки.

- [ ] **Step 1: Полная сборка и тесты**

Run: `dotnet build src/AdminPanel.slnx && dotnet test src/AdminPanel.slnx`
Expected: оба зелёные, 0 warnings.

- [ ] **Step 2: Живой healthz**

```bash
dotnet run --project src/AdminPanel.Api --launch-profile http &
sleep 5
curl -s -w '\nHTTP %{http_code}\n' http://localhost:5000/api/healthz
pkill -f AdminPanel.Api || true
```

Expected: `{"status":"ok"}`, `HTTP 200`.

- [ ] **Step 3: Проверки структуры и CPM**

```bash
grep TreatWarningsAsErrors src/Directory.Build.props
grep -rn 'Version=' src --include='*.csproj' | grep -v '<!--' || echo "OK: Version-атрибутов нет"
grep -n 't01-skeleton` — скелет' arch/roadmap/infra.md || echo "OK: пункт t01-skeleton удалён"
ls src src/tests
git status --short
```

Expected: `TreatWarningsAsErrors>true`; `OK: Version-атрибутов нет`; `OK: пункт t01-skeleton удалён` (анкер «`t01-skeleton` — скелет» матчит только удалённую строку-пункт; зависимость `t02-auth ← t01-skeleton` остаётся в файле и grep'ом не ловится); состав `src/` — ровно `AdminPanel.Api`, `AdminPanel.Core`, `AdminPanel.Etcd`, `AdminPanel.Probes`, `AdminPanel.Infrastructure`, мета-файлы и `tests/AdminPanel.UnitTests`, `tests/AdminPanel.IntegrationTests` — ничего сверх (spec §12.4); рабочее дерево чистое (всё закоммичено).

- [ ] **Step 4: Fix-коммит при необходимости (иначе пропустить)**

Если верификация потребовала правок:

```bash
git add -A
git commit -m "fix(t01): правки по финальной верификации скелета"
```

Если правок не потребовалось — финальным коммитом задачи остаётся коммит Task 9; состояние ветки `feat-t01-skeleton` готово к ревью и мержу (мерж — по гейту dev-flow, отдельным решением).
