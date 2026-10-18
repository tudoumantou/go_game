#!/usr/bin/env bash
# 本地 Docker 部署；保留已有密钥及数据库。
set -euo pipefail
cd "$(dirname "$0")/.."
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
mkdir -p deploy/keys
for name in gateway_rsa login_rsa; do
  private="deploy/keys/$name.pem"
  public="deploy/keys/${name}_pub.pem"
  if [[ ! -f "$private" ]]; then
    if [[ -f "$public" ]]; then
      echo "缺少 $private，但公钥已存在；请恢复对应私钥。" >&2
      exit 1
    fi
    (umask 077; openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$private")
  fi
  if [[ ! -f "$public" ]]; then
    openssl pkey -in "$private" -pubout -out "$public"
  fi
done
# 通过 docker cp 更新命名卷，避免 Docker Desktop 宿主机目录共享依赖。
docker compose -f deploy/docker-compose.yaml create assets
assets_id="$(docker compose -f deploy/docker-compose.yaml ps -a -q assets)"
docker cp configs/. "$assets_id:/configs/"
docker cp -a deploy/keys/. "$assets_id:/keys/"
docker cp sql/schema/. "$assets_id:/schema/"
docker cp deploy/monitoring/. "$assets_id:/monitoring/"
docker compose -f deploy/docker-compose.yaml up -d --build etcd redis mysql kafka center cross flusher game login gateway
for ((i=0; i<60; i++)); do
  ready=true
  for port in 8081 9101 9102 9201 9301; do
    if ! curl --silent --fail --max-time 2 "http://127.0.0.1:$port/readyz" >/dev/null; then
      ready=false
      break
    fi
  done
  if "$ready"; then
    docker compose -f deploy/docker-compose.yaml ps
    echo "服务已就绪：Login http://127.0.0.1:8080，Gateway 127.0.0.1:9001"
    exit 0
  fi
  sleep 2
done
docker compose -f deploy/docker-compose.yaml logs --tail=40 login gateway game center cross
exit 1
