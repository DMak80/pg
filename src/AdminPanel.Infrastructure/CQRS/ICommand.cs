namespace AdminPanel.Infrastructure.CQRS;

// Маркер команды (мутация); в панели команда одна — создание кластера (arch/01 §2,
// паттерн Puzzle docs/01.03-cqrs.md без DB-слоя — spec t12 §3.4, решение §8.9).
public interface ICommand<T>;
