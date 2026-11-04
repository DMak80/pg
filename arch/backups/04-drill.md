# Бэкапы: дрилл восстановимости

Уровень 3. Функция «дрилл»: плановое НЕдеструктивное доказательство
восстановимости — периодический автоматический прогон DR-пути restore на
бэкапе живого шарда. Механика drill-операции на ноде —
[pgnode/06-restore-drill.md](../pgnode/06-restore-drill.md) §3; ключи —
[05-etcd-contract.md](05-etcd-contract.md); состояния —
[processes/enums.md](processes/enums.md).

## 1. Роль

Тот же кандидат (новейший COMPLETED-полный) и та же валидация (манифест
существует, WalChain от `wal_start_segment` непрерывна), что у реального
DR-restore — дрилл доказывает ровно тот путь, который сработает при аварии.
Прод-шард не трогается вовсе (scratch-каталог дрилла — НЕ PGDATA ноды); S3 —
только чтение (list + скачивание). Контроль — «старт + выход из recovery»:
postgres drill-операции стартовал на восстановленном наборе и вышел из
recovery (`pg_is_in_recovery = false`), `restored_to_lsn` фиксируется в
статус; SQL-проверок данных НЕТ (целостность файлов — verify, сверка
счётчиков с продом не имеет смысла — прод уходит вперёд точки бэкапа).

## 2. RestoreDrillProcess

Тиковая машина под клэймом `<C>` (Op `backup-drill`): супервиз активной
drill-операции PgNode первой ноды, затем отбор кандидата, валидация и запуск
(`POST /api/ops/restore` с id дрилла, latest, БЕЗ target). Бюджет
`Drill:TimeoutSec`, takeover, vanished/cancelled —
[G. RestoreDrillProcess](processes/g-drill.md).

## 3. Drill-операция PgNode

Исполняет агент PgNode ПЕРВОЙ ноды шарда — тот же единственный процесс ноды:
та же команда `POST /api/ops/restore` с полем `id`, механика restore
ЦЕЛИКОМ, но в scratch-каталог
`/home/postgres/pgdata/backup-drill/<id>/pgdata` (НЕ PGDATA ноды).
Изоляция по построению, чистка scratch при любом исходе, никаких нод/HA-scope
дрилл не создаёт. Фазы/механика — [pgnode/06-restore-drill.md](../pgnode/06-restore-drill.md) §3,
процесс — [E. DrillTask](../pgnode/processes/e-drill.md).

## 4. Отбор, валидация и стоп-семантика

Отбор — один шард за проход (MIN `finished_unix`, «никогда» первым), гварды
`interval_days`, активного дрилла/restore, `ToRemove` и отсутствия
COMPLETED. Валидация — как у restore own-source; отказ валидации → FAILED
без запуска операции. Стоп-семантика: активная restore-заявка отменяет идущий
дрилл (`yielded-to-restore`), `Backups:Enabled=false` новых запусков не даёт,
D1/D2 уносят операцию/ключ. Детали —
[G. RestoreDrillProcess](processes/g-drill.md).

