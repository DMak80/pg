# 03. Подготовка хостов

Что должно стоять на каждой ноде **до** того, как запустим `docker compose up`.

---

## 1. Операционная система

- **Linux x86_64 или arm64**, ядро ≥ 5.x.
  Проверено на: Ubuntu 22.04/24.04, Debian 12, RHEL/Alma 9.
- Синхронизированное **время** — критично для etcd/raft. Поставь и включи `chrony` или `systemd-timesyncd`:

```bash
sudo apt update && sudo apt install -y chrony        # Debian/Ubuntu
# или
sudo dnf install -y chrony                            # RHEL-family
sudo systemctl enable --now chrony
chronyc tracking                                     # должен показывать поправку < 100ms
```

> Рассогласование часов > 500ms — etcd может отказывать в записи, Patroni — некорректно
> вычислять TTL лидера. **NTP обязателен.**

- Отключи swap под серьезными нагрузками (опционально): `sudo swapoff -a` + убери из `/etc/fstab`.

---

## 2. Docker и Compose plugin

Ставим **официальный** Docker Engine (не `docker.io` из дистриба — там старая версия):

### Ubuntu/Debian
```bash
# https://docs.docker.com/engine/install/ubuntu/
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
$(. /etc/os-release; echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### RHEL/Alma/Rocky
```bash
# https://docs.docker.com/engine/install/rhel/
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
```

### Проверка
```bash
docker --version              # Docker version 27.x+
docker compose version        # Docker Compose version v2.29+ (как plugin!)
docker run --rm hello-world   # проходит?
```

> Используем **`docker compose`** (с пробелом) — это plugin v2. `docker-compose` (через дефис)
> — старая v1, её не ставим.

---

## 3. Тюнинг ядра под PostgreSQL (обязательно)

В `/etc/sysctl.d/99-postgres.conf`:

```ini
# Память для shared buffers PostgreSQL
kernel.shmmax = 68719476736            # 64 ГБ上限 (подстрой под RAM)
kernel.shmall = 16777216

# Не «съедать» память в кэш页 до конца
vm.swappiness = 1
vm.overcommit_memory = 2
vm.dirty_ratio = 10
vm.dirty_background_ratio = 5

# Канал для etcd/PG
net.core.somaxconn = 1024
net.ipv4.tcp_max_syn_backlog = 4096
```

Применить: `sudo sysctl --system`.

### Лимиты процессов/файлов — `/etc/security/limits.d/postgres.conf`
```
postgres  soft  nofile  1048576
postgres  hard  nofile  1048576
postgres  soft  nproc   65536
postgres  hard  nproc   65536
```
Для docker-контейнеров обычно лимиты выставляются в compose (`ulimits:`), но системные
пределы тоже стоит поднять.

---

## 4. Диск под PGDATA и etcd

Подразумевается, что у каждой ноды есть отдельный блочный том под данные (см. `02-topology.md`).

```bash
# Пример: новый пустой диск /dev/sdb под PG
sudo mkfs.xfs /dev/sdb                       # или mkfs.ext4
sudo mkdir -p /data/pg
echo '/dev/sdb /data/pg xfs defaults,noatime 0 2' | sudo tee -a /etc/fstab
sudo mount /data/pg
sudo chown -R 999:999 /data/pg               # uid 999 = postgres внутри образа Spilo

# Под etcd — отдельный быстрый том /dev/sdc
sudo mkfs.ext4 /dev/sdc
sudo mkdir -p /data/etcd
echo '/dev/sdc /data/etcd ext4 defaults 0 2' | sudo tee -a /etc/fstab
sudo mount /data/etcd
```

> UID 999 — это `postgres` в Debian-based образах, которые использует Spilo. Проверь через
> `docker run --rm <image> id postgres` при первом запуске.

Проверь:
```bash
df -hT /data/pg /data/etcd
```

---

## 5. Проверка образов (предварительная pulling)

Образы лучше **скачать заранее** на каждой ноде, чтобы первый запуск был быстрым и
не зависел от реестра:

```bash
docker pull ghcr.io/zalando/spilo-16:3.3-p3
docker pull quay.io/coreos/etcd:v3.5.21
docker pull haproxy:2.8-alpine
docker images | grep -E 'spilo|etcd|haproxy'
```

> Актуальные теги проверяй:
> - Spilo: https://github.com/zalando/spilo/pkgs/container/spilo-16 (или `-15`, `-17`)
> - etcd: https://quay.io/repository/coreos/etcd?tab=tags
> - HAProxy: https://hub.docker.com/_/haproxy?tab=tags

---

## 6. Firewall (пример ufw)

Если включен ufw, открой нужные порты **только между нодами кластера**:

```bash
# На pg1 (повторить с правильными IP на pg2/pg3)
for port in 2379 2380 5432 8008; do
  sudo ufw allow from 10.0.0.12 to any port $port proto tcp   # pg2
  sudo ufw allow from 10.0.0.13 to any port $port proto tcp   # pg3
  sudo ufw allow from 10.0.0.10 to any port $port proto tcp   # pg-lb
done
sudo ufw allow 22/tcp
```

> Не открывай 5432/8008 наружу публично — только внутри сети кластера. Доступ для
> приложений — через HAProxy.

---

## 7. Чек-лист готовности одной ноды

```text
[ ] Linux x86_64/arm64, ядро >=5
[ ] chrony синхронизирует время (chronyc tracking)
[ ] docker + docker compose plugin установлены (docker compose version)
[ ] hello-world контейнер запускается
[ ] /data/pg и /data/etcd смонтированы и принадлежат правильному uid
[ ] sysctl/limits применены
[ ] образы spilo / etcd / haproxy скачаны
[ ] firewall открывает 2379/2380/5432/8008 между нодами
[ ] /etc/hosts содержит имена всех нод кластера
[ ] ping до всех нод кластера проходит
```

Готово на всех 3 нодах? → [04-deploy-etcd.md](04-deploy-etcd.md).

---

## 8. Дополнительно: утилиты для админа (опционально, но удобно)

На свою рабочую машину (откуда будешь админить):
```bash
brew install etcd           # даст etcdctl (macOS)
# или apt install etcd-client, или просто используй через docker run etcd ...
```

`patronictl` не нужно ставить отдельно — он есть **внутри образа Spilo**, мы зовём его
через docker exec (см. `scripts/patronictl.sh`).
