#!/bin/sh
# Д4 (spec §3.7): пустой METRICS_ALERT_WEBHOOK_URL — только UI (null-ресивер);
# непустой — generic webhook. Итоговый конфиг — в /tmp (том смонтирован ro).
# Ревью Ф7-5: спецсимволы sed в URL ('&' — совпадение, '|' — разделитель,
# '/' и '\' — паттерн/экранирование) экранируются, иначе подстановка ломает
# конфиг (или тихо искажает URL).
set -e
if [ -n "$METRICS_ALERT_WEBHOOK_URL" ]; then
  escaped=$(printf '%s' "$METRICS_ALERT_WEBHOOK_URL" | sed 's/[&|/\\]/\\&/g')
  sed "s|__WEBHOOK_URL__|$escaped|" \
    /etc/alertmanager/alertmanager.webhook.yml > /tmp/alertmanager.yml
else
  cp /etc/alertmanager/alertmanager.null.yml /tmp/alertmanager.yml
fi
exec alertmanager --config.file=/tmp/alertmanager.yml --storage.path=/alertmanager
