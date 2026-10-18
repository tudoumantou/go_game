#!/usr/bin/env bash
# scripts/smoke.sh —— E2E 冒烟（设计文档 19.5 第 6 条五步 / 23.7 每日冒烟）
#
# 五步：登录 → 握手 → 创角 → 进游戏 → 角色信息
# 依赖：五服务已启动（make dev + make db-init + make db-seed + make run-all）
#
# 用法：
#   ./scripts/smoke.sh                        # 默认本地端口
#   LOGIN=http://127.0.0.1:8080 GW=127.0.0.1:9001 ./scripts/smoke.sh
set -euo pipefail

LOGIN="${LOGIN:-http://127.0.0.1:8080}"
GW="${GW:-127.0.0.1:9001}"
DEVICES="${DEVICES:-1}"

cd "$(dirname "$0")/.."

echo "==> E2E 冒烟：login=$LOGIN gateway=$GW devices=$DEVICES"
go run ./tools/loadtest -mode smoke -login "$LOGIN" -gateway "$GW" -devices "$DEVICES"

echo "==> 冒烟通过"
