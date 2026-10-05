# syntax=docker/dockerfile:1

# E2E-образ джобов (канон AGENTS.md «E2E-образы собираются на хосте»): тот же
# комплект, что PgWorker.Backup.Dockerfile, но контекст сборки — каталог docker/
# (узкий, без «тихой» передачи корня репозитория). Слои base/mc общие с
# dev-образом — docker-cache делает повторные сборки мгновенными.
#   docker build -f docker/PgWorker.Backup.E2E.Dockerfile -t pgworker-backup:e2e docker/
FROM postgres:18

ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

# В postgres:18 (debian trixie-slim) нет curl/wget — ставим на этапе сборки.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL -o /usr/local/bin/mc \
      "https://github.com/minio/mc/releases/download/${MC_VERSION}/mc.linux-amd64.${MC_VERSION}" \
 && chmod +x /usr/local/bin/mc \
 && mc --version

COPY backup/entrypoint.sh /usr/local/bin/pgw-backup-entrypoint
RUN chmod +x /usr/local/bin/pgw-backup-entrypoint

ENTRYPOINT ["/usr/local/bin/pgw-backup-entrypoint"]
