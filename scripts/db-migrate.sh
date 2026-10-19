#!/usr/bin/env bash
# scripts/db-migrate.sh —— 已有库增量迁移（R11.2）
#
# 与 db-init.sh 的分工：
#   db-init.sh     新库：按 sql/schema/*.sql 建全量表
#   db-migrate.sh  已有库：只执行 sql/migrations/ 下未应用的增量
#
# 用法：
#   ./scripts/db-migrate.sh              # 执行待应用迁移
#   ./scripts/db-migrate.sh --status     # 只看状态，不改库
#   ./scripts/db-migrate.sh --baseline   # 把 0001 标记为已应用（配合 db-init 使用）
#
# 失败即退出，不继续后续迁移：半套 schema 比停在旧版本更难排查。
set -euo pipefail
cd "$(dirname "$0")/.."

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-game_study}"
MYSQL_DATABASE="${MYSQL_DATABASE:-game_study}"

export MYSQL_PWD="$MYSQL_PASSWORD"
# MYSQL_DOCKER=<容器名> 时走 docker exec：CI/开发机常常没有 mysql 客户端。
if [[ -n "${MYSQL_DOCKER:-}" ]]; then
  MYSQL=(docker exec -i -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_DOCKER" mysql -u"$MYSQL_USER" "$MYSQL_DATABASE")
else
  MYSQL=(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" "$MYSQL_DATABASE")
fi

MODE="${1:-apply}"

"${MYSQL[@]}" -e "CREATE TABLE IF NOT EXISTS schema_migrations (
  version    VARCHAR(128) NOT NULL,
  applied_at BIGINT NOT NULL,
  PRIMARY KEY (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='已应用的迁移脚本';"

applied() {
  "${MYSQL[@]}" -N -B -e "SELECT 1 FROM schema_migrations WHERE version='$1' LIMIT 1" | grep -q 1
}

record() {
  "${MYSQL[@]}" -e "INSERT INTO schema_migrations (version, applied_at) VALUES ('$1', UNIX_TIMESTAMP())"
}

if [[ "$MODE" == "--baseline" ]]; then
  # 已用 db-init 建好表的库：把基线标记为已应用，避免重复建表。
  if applied "0001_baseline.sql"; then
    echo "基线已标记，无需重复"
  else
    record "0001_baseline.sql"
    echo "已标记基线：0001_baseline.sql"
  fi
  exit 0
fi

if [[ "$MODE" == "--status" ]]; then
  echo "==> 迁移状态（库：$MYSQL_DATABASE）"
  for f in sql/migrations/*.sql; do
    name="$(basename "$f")"
    if applied "$name"; then
      echo "  [x] $name"
    else
      echo "  [ ] $name"
    fi
  done
  exit 0
fi

echo "==> 执行待应用迁移"
count=0
for f in sql/migrations/*.sql; do
  name="$(basename "$f")"
  if applied "$name"; then
    continue
  fi
  echo "  -> $name"
  "${MYSQL[@]}" < "$f"
  record "$name"
  count=$((count + 1))
done

if [[ $count -eq 0 ]]; then
  echo "无待应用迁移"
else
  echo "完成：应用 $count 个迁移"
fi
