# Телеметрия: риски

Уровень 3. Риски и митигации. Родитель — [README.md](README.md); соседние
документы — [01-framework.md](01-framework.md),
[02-dictionary.md](02-dictionary.md), [03-exposure.md](03-exposure.md),
[04-collectors.md](04-collectors.md), [05-storage.md](05-storage.md),
[06-testing.md](06-testing.md).

| # | Риск | Митигация |
|---|---|---|
| M1 | Кардинальность лейблов (фазы/группы/топики) растёт | лейблы конечны ([02-dictionary.md](02-dictionary.md)), фаза сбрасывается при завершении процесса; `group`/`topic` — реальные сущности kafka-домена |
| M2 | Коллектор лагов грузит AdminClient/брокеров | один проход за тик, интервал 30 с, короткий таймаут; ошибки сбора не ретраятся поллерами циклов |
| M3 | OTel-суффиксы имён отличаются от словаря (версии экспортёра) | интеграционный тест фиксирует фактические имена против словаря [02-dictionary.md](02-dictionary.md); версия пин в Directory.Packages.props |
| M4 | Порт 3000/9090/9093 занят на хосте стенда | env-override compose ([01-framework.md](01-framework.md) §3) |
| M5 | Эмуляторы Patroni без /metrics | расширение emulator.py ([02-dictionary.md](02-dictionary.md) §5) |
