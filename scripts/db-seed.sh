#!/usr/bin/env bash
# scripts/db-seed.sh —— 灌初始数据（19.5 清单第 4 步；道具/等级等安全表在 XML，不在此）
set -euo pipefail
cd "$(dirname "$0")/.."

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-game_study}"
MYSQL_DATABASE="${MYSQL_DATABASE:-game_study}"

export MYSQL_PWD="$MYSQL_PASSWORD"
for f in sql/seed/*.sql; do
  echo "  -> $f"
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" "$MYSQL_DATABASE" < "$f"
done
echo "db-seed done（幂等：重复执行以 INSERT 语义为准，冲突请先清表）"
