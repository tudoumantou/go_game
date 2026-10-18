#!/usr/bin/env bash
# scripts/lint.sh —— lint 全量（16.4 CI 卡点第 2/3 步）
#   ① buf lint（proto 规范）
#   ② golangci-lint run（Go 静态检查）
#   ③ 生成物一致性（make gen 后 git diff 必须干净）
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> [1/3] buf lint"
( cd api/proto && buf lint )

echo "==> [2/3] golangci-lint run"
golangci-lint run ./...

echo "==> [3/3] 生成物一致性（gen 后 git diff --exit-code）"
./scripts/gen.sh >/dev/null
if git rev-parse --git-dir >/dev/null 2>&1; then
  if ! git diff --exit-code -- pkg/pb pkg/gamedata pkg/model; then
    echo "FAIL: 生成物与代码不一致——请执行 ./scripts/gen.sh 并提交产物（16.4）" >&2
    exit 1
  fi
  echo "生成物一致性 OK"
else
  echo "SKIP: 非 git 仓库，跳过 diff 校验"
fi
echo "lint done"
