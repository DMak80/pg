# Эксплуатационные руководства (runbook)

Уровень 2. Операционные процедуры, не принадлежащие одному компоненту:
запуск и обслуживание системы, общие для всех воркеров и стенда. Процедуры
конкретного компонента живут в его каноне (ссылки ниже), чтобы факт хранился
в одном месте.

## Документы

| Документ | Процедура |
|---|---|
| [01-worker-instances.md](01-worker-instances.md) | вторые инстансы воркеров: порты, обслуживание, takeover |
| [02-registry-images.md](02-registry-images.md) | внешние docker-образы, локальный registry, сборка образа PG-ноды, каталог образов |
| [03-worker-api-cert.md](03-worker-api-cert.md) | управление серверным сертом API воркера из панели |
| [04-e2e-runs.md](04-e2e-runs.md) | запуск docker-E2E серий и подготовка окружения |

## Тематические runbook-разделы

| Процедура | Где описана |
|---|---|
| Потеря WAL-слота (`wal_status='lost'`) и авто-восстановление | [backups/09-operability.md](../backups/09-operability.md) §7 |
| PgTune-параметры PG-нод: настройка, пересчёт, исключения | [pgworker/09-pgtune.md](../pgworker/09-pgtune.md) §9 |
| Watchdog зависших циклов воркера (общий механизм) | [pgworker/08-operability.md](../pgworker/08-operability.md) §3 |
| HA-etcd контур: подъём, стендовое зеркало, чеки, потеря кворума | [etcdworker/06-operability.md](../etcdworker/06-operability.md) §7 |
| Восстановление etcd из S3-выгрузки | [etcdworker/06-operability.md](../etcdworker/06-operability.md) §8 |
| TLS-подключения к Valkey, осиротевшие TLS-тома, ротация CA | [valkeyworker/06-operability.md](../valkeyworker/06-operability.md) §7 |
| REST-TLS Patroni: диагностика отказов, бэкап пакета `deploy/tls` | [pgworker/08-operability.md](../pgworker/08-operability.md) §7, [pgworker/04-security.md](../pgworker/04-security.md) §5 |

## Границы

- `arch/` — канон (как система устроена и как должна эксплуатироваться);
  `docs/` — практики разработки, история задач и детальные правила тестовых
  прогонов ([docs/e2e-launch.md](../../docs/e2e-launch.md),
  [docs/e2e-isolation.md](../../docs/e2e-isolation.md)).
- Операторские команды с консоли — вне правила коммуникаций
  ([communications/01-rules.md](../communications/01-rules.md) §2).
