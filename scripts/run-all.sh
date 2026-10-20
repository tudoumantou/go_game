#!/usr/bin/env bash
# scripts/run-all.sh —— 本地按 19.5 依赖顺序起五进程（需先 make dev && db-init && db-seed）
#   etcd → 中间件 → center → game/cross → gateway → login
# 任一进程退出不拖累其他；Ctrl-C 统一收尾。
set -uo pipefail
cd "$(dirname "$0")/.."

CONFIG_ROOT="configs"
LOG_DIR="${LOG_DIR:-bin/logs}"
mkdir -p "$LOG_DIR"

# 本地 Kafka EXTERNAL listener 默认地址（docker-compose 暴露 9093）；
# 未设置时不启用 Kafka（配置里 ${KAFKA_BROKERS} 展开为空会被过滤，服务自动降级）
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9093}"

PIDS=()
cleanup() {
  echo ""
  echo "==> stopping all (reverse order)"
  for ((i=${#PIDS[@]}-1; i>=0; i--)); do
    kill "${PIDS[$i]}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "==> all stopped"
}
trap cleanup INT TERM EXIT

start() { # $1=服务名 $2=配置文件
  local name="$1" cfg="$2"
  echo "==> start $name ($cfg)"
  "./bin/$name" -config "$cfg" >"$LOG_DIR/$name.log" 2>&1 &
  PIDS+=($!)
}

# 顺序即依赖顺序（19.5 表）；网关/登录最后起
start center "$CONFIG_ROOT/center/center-dev.yaml"
start flusher "$CONFIG_ROOT/game/game-dev.yaml"
sleep 1
start map     "$CONFIG_ROOT/map/map-dev.yaml"
start game    "$CONFIG_ROOT/game/game-dev.yaml"
start cross   "$CONFIG_ROOT/cross/cross-dev.yaml"
sleep 1
start gateway "$CONFIG_ROOT/gateway/gateway-dev.yaml"
sleep 1
start login   "$CONFIG_ROOT/login/login-dev.yaml"

echo "==> all started, logs in $LOG_DIR/*.log, Ctrl-C to stop"
wait
