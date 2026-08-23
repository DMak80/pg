# syntax=docker/dockerfile:1

# Стадия 1 — фронт: SPA-бандл (engines node >=22.12; registry — публичный, см. frontend/.npmrc).
FROM node:22-alpine AS frontend
WORKDIR /src
COPY frontend/.npmrc frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
# vite build: outDir ../src/AdminPanel.Api/wwwroot от корня frontend → /src/AdminPanel.Api/wwwroot
RUN npm run build

# Стадия 2 — бэкенд: publish (NuGet.Config/CPM/Build.props — внутри src/, источники публичные).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS publish
WORKDIR /src
COPY src/ ./src/
RUN dotnet publish src/AdminPanel.Api/AdminPanel.Api.csproj -c Release -o /app --nologo

# Стадия 3 — runtime: один процесс, один порт, не-root, HEALTHCHECK на liveness.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
# curl отсутствует в базовом образе — нужен только для HEALTHCHECK.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=publish /app ./
COPY --from=frontend /src/AdminPanel.Api/wwwroot ./wwwroot
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER app
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/api/healthz || exit 1
ENTRYPOINT ["dotnet", "AdminPanel.Api.dll"]
