#!/bin/sh
# Д4 (spec §3.7): пустой METRICS_ALERT_WEBHOOK_URL — только UI (null-ресивер);
# непустой — generic webhook. Итоговый конфиг — в /tmp (том смонтирован ro).
set -e
if [ -n "$METRICS_ALERT_WEBHOOK_URL" ]; then
  sed "s|__WEBHOOK_URL__|$METRICS_ALERT_WEBHOOK_URL|" \
    /etc/alertmanager/alertmanager.webhook.yml > /tmp/alertmanager.yml
else
  cp /etc/alertmanager/alertmanager.null.yml /tmp/alertmanager.yml
fi
exec alertmanager --config.file=/tmp/alertmanager.yml --storage.path=/alertmanager
