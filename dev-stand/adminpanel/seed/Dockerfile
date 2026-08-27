# Образ сида: etcdctl 3.5.21 поверх alpine. Официальный образ etcd —
# distroless (без sh), поэтому бинарик копируем из него (arch/04 §2.2).
FROM alpine:3.20
COPY --from=quay.io/coreos/etcd:v3.5.21 /usr/local/bin/etcdctl /usr/local/bin/etcdctl
