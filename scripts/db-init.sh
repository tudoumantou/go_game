#!/usr/bin/env bash
# scripts/db-init.sh —— DDL → MySQL 建表（19.5 首次部署清单第 4 步）
# 依赖环境变量: MYSQL_HOST/PORT/USER/PASSWORD/DATABASE（默认本地开发值）
set -euo pipefail
cd "$(dirname "$0")/.."

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-game_study}"
MYSQL_DATABASE="${MYSQL_DATABASE:-game_study}"

export MYSQL_PWD="$MYSQL_PASSWORD"
# MYSQL_DOCKER=<容器名>：无本机 mysql 客户端时走 docker exec。
if [[ -n "${MYSQL_DOCKER:-}" ]]; then
  MYSQL=(docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_DOCKER" mysql -u"$MYSQL_USER" "$MYSQL_DATABASE")
else
  MYSQL=(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u"$MYSQL_USER" "$MYSQL_DATABASE")
fi

echo "==> 建库（如不存在）"
if [[ -n "${MYSQL_DOCKER:-}" ]]; then
  docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_DOCKER" mysql -u"$MYSQL_USER" \
    -e "CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
else
  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" \
    -e "CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
fi

# 按月分表：建当前月 + 下个月的 login_log（12.3 表清单注释）
CUR_MONTH="$(date +%Y%m)"
NEXT_MONTH="$(date -d '+1 month' +%Y%m 2>/dev/null || date +%Y%m)"

echo "==> 执行 sql/schema/*.sql（account 先行，其余无外键依赖顺序无关）"
for f in sql/schema/*.sql; do
  echo "  -> $f"
  "${MYSQL[@]}" < "$f"
done

echo "==> 提示：新库完成后执行 make db-migrate ARGS=--baseline 标记迁移基线"
echo "==> login_log 按月分表: login_log_$CUR_MONTH / login_log_$NEXT_MONTH"
for m in "$CUR_MONTH" "$NEXT_MONTH"; do
  "${MYSQL[@]}" -e "CREATE TABLE IF NOT EXISTS \`login_log_$m\` LIKE \`login_log\`;"
done

echo "==> 账号中心分表: acc_summary_0..31 / user_role_0..14（LIKE 模板，与 login_log 同范式）"
for i in $(seq 0 31); do
  "${MYSQL[@]}" -e "CREATE TABLE IF NOT EXISTS \`acc_summary_$i\` LIKE \`acc_summary\`;"
done
for i in $(seq 0 14); do
  "${MYSQL[@]}" -e "CREATE TABLE IF NOT EXISTS \`user_role_$i\` LIKE \`user_role\`;"
done

echo "db-init done（$MYSQL_DATABASE @ $MYSQL_HOST:$MYSQL_PORT）"
