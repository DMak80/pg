# Valkey-кластеры: обработка сбоев (толерантность читателей)

Уровень 3. Поведение читателей домена на сбойных и переходных значениях.
Ключи — [02-keys.md](02-keys.md) §1; клиентская библиотека —
[04-client-discovery.md](04-client-discovery.md) §2.

| Случай | Поведение |
|---|---|
| Битый JSON в значении ключа (`config`, `resources`, заявка ротации) | ключ пропускается, в снапшот попадает parseError-запись (без исключения), warning-алерт `valkey-key-malformed` |
| Неизвестный ключ внутри `/valkey/` | лог-строка + счётчик `unknownKeys`; парсер не падает |
| Active-кластер без `endpoints` | критический алерт `valkey-endpoints-missing` (воркер ещё не дописал / потеря ключа) |
| Неполный набор кредов (`app_user` без `app_password` и наоборот) | `App = null`, `GetClientConfig() = null` — потребитель проверяет |
| `config.state` — незнакомое значение | толерантно: трактуется как Active-ветка с raw-строкой state (state-значения строкой — система развивается) |
| Пустой/пробельный `endpoints` | трактуется как отсутствующий (алерт `valkey-endpoints-missing`) |
| Битый PEM в `ca_pem`/`ca_key` | parseError-запись + warning `valkey-key-malformed`; парсер не падает, поле в снапшоте null (кластер жив) |
| `ca_pem` с несколькими CERTIFICATE-блоками (bundle окна ротации) | не ошибка: парсер читает ПЕРВЫЙ блок ([04-client-discovery.md](04-client-discovery.md) §1), доверие клиента — по всем; окно закрывается коммитом ротации |
| `ca_next_*` вне живой ротации (осиротевший staging после ручного снятия заявки) | unknownKeys-толерантность: лог + счётчик, парсер не падает; живая ротация переиспользует staging (put-if-absent), чистка — runbook |
| Active-кластер без `ca_pem` | critical-алерт `valkey-security-missing` (миграция TLS не доиграна / ключ потерян) |
