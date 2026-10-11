# syntax=docker/dockerfile:1
# Образ ownS3 (arch/owns3/01 §5, 05 §6): dotnet publish НА ХОСТЕ, в контейнер —
# ТОЛЬКО publish-вывод (runtime-слой, без sdk/исходников; канон E2E-образов).
# CLI (узкий контекст — каталог артефактов):
#   dotnet publish src/OwnS3.App/OwnS3.App.csproj -c Release -o artifacts/e2e/owns3
#   docker build -f docker/OwnS3.Dockerfile -t owns3:e2e artifacts/e2e/owns3
# Compose (контекст — корень репо, ARG указывает путь вывода):
#   build: { context: .., dockerfile: docker/OwnS3.Dockerfile,
#            args: { OWNS3_PUBLISH_DIR: artifacts/owns3/publish } }
ARG OWNS3_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG OWNS3_PUBLISH_DIR
WORKDIR /app
COPY ${OWNS3_PUBLISH_DIR} ./
# Порт S3-грани — OwnS3:Server:Port (дефолт 9000; HTTP-only, path-style).
EXPOSE 9000
ENTRYPOINT ["dotnet", "OwnS3.App.dll"]
