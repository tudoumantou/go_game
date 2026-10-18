#!/usr/bin/env bash
# scripts/build.sh —— 交叉编译五服务 → bin/（16.4 CI 卡点第 5 步）
# 用法: ./scripts/build.sh [GOOS] [GOARCH]   默认 linux/amd64
set -euo pipefail
cd "$(dirname "$0")/.."

GOOS="${1:-linux}"
GOARCH="${2:-amd64}"
VERSION="$(git describe --tags --always 2>/dev/null || echo dev)"
OUT="bin"
mkdir -p "$OUT"

SERVICES=(login gateway game center cross flusher)
for s in "${SERVICES[@]}"; do
  echo "==> building $s ($GOOS/$GOARCH)"
  CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" \
    go build -trimpath -ldflags "-s -w -X main.version=$VERSION" \
      -o "$OUT/$s" "./cmd/$s"
done
echo "build done: $OUT/{$(IFS=,; echo "${SERVICES[*]}")} ($GOOS/$GOARCH, version=$VERSION)"
