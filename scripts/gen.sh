#!/usr/bin/env bash
# scripts/gen.sh —— 一键生成三大代码产物（16.4 CI 卡点第 1 步）
#   pkg/pb      (buf generate)      ← api/proto
#   pkg/gamedata(gendata)           ← configs/gamedata/xml
#   pkg/model  (gendb)              ← sql/schema
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> [1/3] proto → pkg/pb"
( cd api/proto && buf generate )

echo "==> [2/3] gamedata xml → pkg/gamedata"
go run ./tools/gendata -in configs/gamedata/xml -out pkg/gamedata

echo "==> [3/3] DDL → pkg/model"
go run ./tools/gendb -in sql/schema -out pkg/model

echo "gen done. 如 CI 环境请执行 git diff --exit-code 校验生成物已随代码提交"
