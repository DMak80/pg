namespace AdminPanel.Infrastructure.CQRS;

// Маркерный интерфейс запроса (чтение); команды в панели не заводятся.
public interface IQuery<T>;
