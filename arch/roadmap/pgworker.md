# Roadmap: сервис PgWorker

Отложенные задачи оркестратора PgWorker (out of scope MVP — спецификация
`docs/superpowers/2026-08-23-pgworker-backend/spec.md` §2; канон сервиса —
[../14-pgworker.md](../14-pgworker.md)).

## Задачи

- **`t02-external-secret-manager`** — интеграция с внешним secret-manager:
  публикация/чтение per-install/per-cluster секретов внешним SM поверх
  etcd-канона (per-cluster app/bucket_admin/mover + backup — arch/14 §4/§5 I,
  arch/19 §7; слито 2026-09-06, merge 5bcd467). Решение пользователя
  (2026-09-06): etcd остаётся единственным хранилищем per-cluster секретов
  до этой интеграции.
- **`t90-etcd-recipe-env-file`** — закрыть pre-existing дефект инструкции
  запуска Patroni-etcd-рецепта arch/04 (применение (а) §0): §4 «Запуск»,
  §5 «после первого старта» и §7 чек-лист инструктируют `docker compose up -d`
  (arch/04:128/159/184), шапка `arch/configs/etcd/docker-compose.yml:9` — тоже;
  рецепт использует паттерн `env_file: etcd.env` + `${…}`-интерполяцию, а
  env_file передаёт переменные только внутрь контейнера — YAML не
  интерполируется, `up` без `--env-file etcd.env` падает на пустом image.
  Тот же дефект закрыт в t09 для deploy/etcd (arch/04 §8: запуск
  `docker compose --env-file etcd.env up -d`); §1–§7 осознанно не тронуты
  (spec t09 §3.1 «правки минимальные»). Найдено код-ревью t09 (круг 2,
  неблокирующее наблюдение).

