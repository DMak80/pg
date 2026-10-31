using System.Reflection;
using AdminPanel.Api.Inspection;
using AdminPanel.Api.Operations;
using AdminPanel.Api.Operations.Kafka;
using AdminPanel.Api.Operations.S3;
using AdminPanel.Api.Operations.Valkey;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Shared.Core;
using Shared.Core.CQRS;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// Routing-smoke (панель): ВСЕ модули операций/инспекции строят таблицу
// маршрутов на bare-хосте (без слушателя). DI-регистрация хендлеров —
// рефлексией по сборке Api (классы *Handler + IHandler): минимальный API
// решает «сервис или тело запроса» по наличию дескриптора — незарегистрированный
// конкретный хендлер в параметре DELETE-лямбды падает ВЫВОДОМ ТЕЛА здесь,
// мгновенно (класс ошибки «Body was inferred…» — падение таблицы маршрутов,
// воркер мёртв для любого запроса). Автодискавери по namespace — без дублирующего
// списка (дрейф невозможен). AAA.
public class RoutingSmokeTests
{
    [Fact]
    public async Task MapAllModules_RouteTableBuilds()
    {
        // Arrange — bare-хост: ни сервера, ни композиции Program; дескрипторы
        // сервисов нужны только факту наличия (разрешение — по запросу).
        var builder = WebApplication.CreateBuilder();

        // DI-типы параметров лямбд модулей: минимальный API решает «сервис или
        // тело запроса» по наличию дескриптора — незарегистрированный тип
        // падает ВЫВОДОМ ТЕЛА здесь (класс ошибки DI-фикса cancel-хендлеров).
        builder.Services.AddSingleton<IHandler, SmokeHandler>(); // сервис-интерфейс лямбд
        builder.Services.AddSingleton(typeof(AdminPanel.Api.Auth.IAdminLoginService), typeof(SmokeLoginService));
        builder.Services.AddSingleton(typeof(AdminPanel.Etcd.IS3SnapshotReader), typeof(SmokeS3Reader));
        builder.Services.AddSingleton(
            typeof(Microsoft.Extensions.Options.IOptions<AdminPanel.Api.Auth.AuthOptions>),
            Microsoft.Extensions.Options.Options.Create(new AdminPanel.Api.Auth.AuthOptions()));
        // *Handler — конкретные хендлеры лямбд (автодискавери по namespace —
        // без дублирующего списка, дрейф невозможен).
        var apiAssembly = typeof(OperationsModule).Assembly;
        var handlerTypes = apiAssembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && t.Name.EndsWith("Handler", StringComparison.Ordinal)
                && (t.Namespace?.StartsWith("AdminPanel.Api", StringComparison.Ordinal) ?? false))
            .ToList();
        handlerTypes.Should().NotBeEmpty("автодискавери не должен вернуться пустым");
        foreach (var type in handlerTypes)
            builder.Services.AddSingleton(type);

        var app = builder.Build();

        // Act — маппинг всех модулей панели (мутации + инспекции);
        // исключение вывода параметров (body inference) летит прямо здесь.
        app.MapOperationsApi();
        app.MapKafkaOperationsApi();
        app.MapValkeyOperationsApi();
        app.MapS3OperationsApi();
        app.MapWorkersApi();
        app.MapInspectionApi();
        app.MapKafkaInspectionApi();
        app.MapValkeyInspectionApi();
        app.MapBackupsInspectionApi();
        app.MapS3InspectionApi();

        // Assert — конечные точки созданы; DELETE-отмены ротаций (u17) в таблице
        // DataSources — явная реализация IEndpointRouteBuilder на WebApplication.
        var routes = ((Microsoft.AspNetCore.Routing.IEndpointRouteBuilder)app).DataSources
            .SelectMany(ds => ds.Endpoints)
            .Select(e => e.DisplayName ?? string.Empty)
            .ToList();
        routes.Should().NotBeEmpty();
        routes.Should().Contain(r => r.Contains("app-password/rotate", StringComparison.Ordinal));
        routes.Should().Contain(r => r.Contains("admin-password/rotate", StringComparison.Ordinal));
        routes.Should().Contain(r => r.Contains("ca/rotate", StringComparison.Ordinal));
        routes.Should().Contain(r => r.Contains("password/rotate", StringComparison.Ordinal));
        await app.DisposeAsync();
    }

    // Заглушки DI-типов для дескрипторов: resolution не происходит (маршруты
    // только строятся) — реализации не вызываются никогда.
    private sealed class SmokeLoginService : AdminPanel.Api.Auth.IAdminLoginService
    {
        public AdminPanel.Api.Auth.LoginResult Login(
            string? username, string? password, string clientKey)
            => throw new NotSupportedException("routing-smoke не логинит");
    }

    private sealed class SmokeS3Reader : AdminPanel.Etcd.IS3SnapshotReader
    {
        public AdminPanel.Core.S3.S3Snapshot? Current => null;
    }

    private sealed class SmokeHandler : IHandler
    {
        public ValueTask<Result<T>> HandleQuery<Q, T>(Q query, CancellationToken ct)
            where Q : IQuery<T>
            => throw new NotSupportedException("routing-smoke не исполняет запросы");

        public ValueTask<Result<T>> HandleCommand<C, T>(C command, CancellationToken ct)
            where C : ICommand<T>
            => throw new NotSupportedException("routing-smoke не исполняет команды");
    }
}
