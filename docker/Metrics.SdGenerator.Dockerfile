
# Образ SdGenerator (t15, arch/18 §5.4): dotnet publish НА ХОСТЕ (инкрементально,
# секунды), в контейнер — ТОЛЬКО publish-вывод (runtime-слой, без sdk/исходников).
# Контекст сборки — каталог артефактов (узкий; канон AGENTS.md):
#   dotnet publish src/Metrics.SdGenerator/Metrics.SdGenerator.csproj \
#     -c Release -o artifacts/sd-generator/publish
#   docker build -f docker/Metrics.SdGenerator.Dockerfile -t sdgenerator:dev \
#     artifacts/sd-generator/publish
# Compose-вариант (BuildKit-песочница Docker Desktop не читает dockerfile вне
# контекста): context = корень репо + SD_PUBLISH_DIR с путём вывода publish.
ARG SD_PUBLISH_DIR=.
FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG SD_PUBLISH_DIR
WORKDIR /app
COPY ${SD_PUBLISH_DIR} ./
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "Metrics.SdGenerator.dll"]
