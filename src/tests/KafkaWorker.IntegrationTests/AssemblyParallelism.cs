using Xunit;

// Последовательный запуск тест-коллекций сборки: брокерные тесты
// KafkaCollection (реальные контейнеры apache/kafka) и Api-коллекция
// (десятки WAF-хостов с Testcontainers-etcd) параллельно исчерпывают
// ресурсы docker-хоста (Docker Desktop) — таймауты AdminClient
// («already exists» после повторного запроса, «only 1 broker registered»)
// на ровном месте. Ожидания тестов не меняются; xunit.runner.json не
// используется, т.к. не подхватывается VSTest-мостом dotnet test.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
