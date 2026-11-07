#!/bin/sh
# Entrypoint узла EtcdNode (arch/etcdnode/01-agent.md §1): гвард обязательных
# env + supervisord (nodaemon, PID 1).
set -eu
: "${ETCD_ARGS:?ETCD_ARGS is required (flags of etcd, from EtcdWorker EtcdNodeSpecBuilder)}"
mkdir -p /etcd-data /snapshots
exec /usr/bin/supervisord -c /etc/supervisor/supervisord.conf
