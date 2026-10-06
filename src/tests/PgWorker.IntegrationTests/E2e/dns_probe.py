# Синтетический DNS-зонд нагрузочного генератора (t24, spec §5.1): резолв без
# пауз, агрегат каждые 5 с (storm-dns,...) — шумовой профиль E2eLoadGen.
# Встроенная E2E-телеметрия (measure-режим, подбор CollectDiagnosticsAsync)
# исключена ревизией 6 — DNS-контроль прогонов идёт по Patroni/docker-логам.
import os, socket, time

targets = [t for t in os.environ.get("DNS_PROBE_TARGETS", "").split(",") if t]
mode = os.environ.get("DNS_PROBE_MODE", "measure")
interval = float(os.environ.get("DNS_PROBE_INTERVAL", "1"))


def resolve(target):
    ts = time.time()
    try:
        socket.getaddrinfo(target, None)
        return ts, True, (time.time() - ts) * 1000.0, ""
    except Exception as e:
        return ts, False, (time.time() - ts) * 1000.0, str(e)


if mode == "storm":
    iterations = fails = 0
    total_ms = max_ms = 0.0
    window = time.time()
    while True:
        for t in targets:
            _, ok, ms, _ = resolve(t)
            iterations += 1
            total_ms += ms
            max_ms = max(max_ms, ms)
            if not ok:
                fails += 1
        if time.time() - window >= 5:
            print(f"storm-dns,{window:.0f},{iterations},{fails},"
                  f"{total_ms / max(iterations, 1):.1f},{max_ms:.1f}", flush=True)
            iterations = fails = 0
            total_ms = max_ms = 0.0
            window = time.time()
else:
    while True:
        for t in targets:
            ts, ok, ms, err = resolve(t)
            print(f"probe-dns,{ts:.3f},{t},{1 if ok else 0},{ms:.1f},{err}", flush=True)
        time.sleep(interval)
