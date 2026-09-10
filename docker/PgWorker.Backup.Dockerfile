# Образ джоба полного бэкапа (arch/19 §2, t02): postgres-клиенты (pg_basebackup)
# + mc (MinIO/S3-клиент; версия запинена — supply-chain, arch/19 §10).
# Никаких .NET-компонентов: джоб — sh-скрипт, параметры только env.
# Сборка (контекст — корень репо): docker build -f docker/PgWorker.Backup.Dockerfile -t pgworker-backup:dev .
FROM postgres:18

ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

# В postgres:18 (debian trixie-slim) нет curl/wget — ставим на этапе сборки.
# Запиненный бинарник linux-amd64 — GitHub-ассет релиза mc (dl.min.io/archive
# не хранит истории по тегам; redirect latest указывает сюда же).
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL -o /usr/local/bin/mc \
      "https://github.com/minio/mc/releases/download/${MC_VERSION}/mc.linux-amd64.${MC_VERSION}" \
 && chmod +x /usr/local/bin/mc \
 && mc --version

COPY docker/backup/entrypoint.sh /usr/local/bin/pgw-backup-entrypoint
RUN chmod +x /usr/local/bin/pgw-backup-entrypoint

ENTRYPOINT ["/usr/local/bin/pgw-backup-entrypoint"]
