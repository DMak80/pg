using Xunit;

// Последовательный запуск тест-коллекций сборки (порт KafkaWorker:
// AssemblyParallelism.cs): интеграционные сценарии с реальными контейнерами
// и WAF-хосты с Testcontainers-etcd параллельно исчерпывают ресурсы
// docker-хоста — таймауты и коллизии портов на ровном месте. Ожидания тестов
// не меняются; xunit.runner.json не используется, т.к. не подхватывается
// VSTest-мостом dotnet test.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
