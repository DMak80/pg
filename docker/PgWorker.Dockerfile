# syntax=docker/dockerfile:1

# Образ сервиса PgWorker (spec §10; по образцу AdminPanel/Dockerfile):
# multi-stage publish → aspnet-runtime. Секреты per-install — только env (Д7).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS publish
WORKDIR /src
COPY src/ ./src/
RUN dotnet publish src/PgWorker.App/PgWorker.App.csproj -c Release -o /app --nologo

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
# curl отсутствует в базовом образе — нужен только для HEALTHCHECK (/healthz).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=publish /app ./
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/healthz || exit 1
ENTRYPOINT ["dotnet", "PgWorker.App.dll"]
