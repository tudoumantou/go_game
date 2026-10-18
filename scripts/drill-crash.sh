#!/usr/bin/env bash
# scripts/drill-crash.sh —— 故障演练：kill -9 游戏服（设计文档 23.5 第 1 行）
#
# 验证的不变量（23.5）：**掉线不丢数据、重连可恢复**
#
# 演练步骤：
#   1. 记录演练前 MySQL 中的玩家数据快照
#   2. 用 SIGKILL 强杀游戏服（不走优雅关闭，模拟进程崩溃）
#   3. 重启游戏服
#   4. 机器人重新登录，校验角色数据与演练前一致
#   5. 比对前后快照，确认没有数据丢失或重复
#
# 前置条件：
#   - 五服务已启动，游戏服 PID 可查（脚本按可执行名 bin/game 查找）
#   - 有测试账号（脚本会自己造一个）
#
# 用法：
#   ./scripts/drill-crash.sh
#   MYSQL_USER=game MYSQL_PASS=secret ./scripts/drill-crash.sh
#   MYSQL_DOCKER=deploy-mysql-1 ./scripts/drill-crash.sh    # 无本机 mysql 客户端时
set -uo pipefail

CONFIG="${CONFIG:-configs/game/game-dev.yaml}"
GAME_BIN="${GAME_BIN:-bin/game}"
LOGIN="${LOGIN:-http://127.0.0.1:8080}"
GW="${GW:-127.0.0.1:9001}"

cd "$(dirname "$0")/.."

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

# mysql_q 执行查询：优先本机客户端；没有客户端时用 MYSQL_DOCKER 指定的容器（CI/容器环境常见）
mysql_q() {
  if [ -n "${MYSQL_DOCKER:-}" ]; then
    docker exec -i "$MYSQL_DOCKER" mysql -uroot -p"${MYSQL_ROOT_PASS:-game_study}" \
      "${MYSQL_DB:-game_study}" -N -B -e "$1" 2>/dev/null
    return
  fi
  mysql -h"${MYSQL_HOST:-127.0.0.1}" -P"${MYSQL_PORT:-3306}" \
    -u"${MYSQL_USER:-game_study}" -p"${MYSQL_PASS:-game_study}" "${MYSQL_DB:-game_study}" \
    -N -B -e "$1" 2>/dev/null
}

# ---------- 0. 前置检查 ----------
log "0/6 前置检查"
if [ -z "${MYSQL_DOCKER:-}" ]; then
  command -v mysql >/dev/null || fail "需要 mysql 客户端（或设置 MYSQL_DOCKER=<容器名> 用 docker exec）"
fi
[ -x "$GAME_BIN" ] || fail "找不到游戏服可执行文件 $GAME_BIN（先 make build）"
curl -sf "${LOGIN}/healthz" >/dev/null || fail "登录服未就绪：$LOGIN"

GAME_PID=$(pgrep -f "$GAME_BIN" | head -1 || true)
[ -n "$GAME_PID" ] || fail "游戏服未运行，请先启动（make run-all）"
echo "游戏服 PID=$GAME_PID"

# ---------- 1. 造数：建一个角色 ----------
log "1/6 造测试角色"
go run ./tools/loadtest -mode smoke -login "$LOGIN" -gateway "$GW" -devices 1 || fail "冒烟失败，无法造数"

PLAYER_ID=$(mysql_q "SELECT id FROM player ORDER BY id DESC LIMIT 1")
[ -n "$PLAYER_ID" ] || fail "查不到刚创建的角色"
echo "测试角色 player_id=$PLAYER_ID"

# ---------- 2. 快照 ----------
log "2/6 记录演练前快照"
SNAP_DIR=$(mktemp -d)
trap 'rm -rf "$SNAP_DIR"' EXIT

mysql_q "SELECT id, name, level, exp, coin FROM player WHERE id=$PLAYER_ID" > "$SNAP_DIR/player.before"
mysql_q "SELECT item_id, count FROM player_item WHERE player_id=$PLAYER_ID ORDER BY item_id" > "$SNAP_DIR/item.before"
# player_attr 是单行实体（PK=player_id），不存在 attr_id/value 列 —— 原脚本这里写错了
mysql_q "SELECT career, hp, mp, atk, def, spd FROM player_attr WHERE player_id=$PLAYER_ID" > "$SNAP_DIR/attr.before"

cat "$SNAP_DIR/player.before"
[ -s "$SNAP_DIR/player.before" ] || fail "快照为空"

# ---------- 3. kill -9 ----------
log "3/6 SIGKILL 游戏服（模拟进程崩溃）"
kill -9 "$GAME_PID"
sleep 2
if pgrep -f "$GAME_BIN" >/dev/null; then
  fail "游戏服仍在运行，kill -9 未生效"
fi
echo "游戏服已被强杀"

# ---------- 4. 重启 ----------
log "4/6 重启游戏服"
nohup "$GAME_BIN" -config "$CONFIG" >> /tmp/drill-game.log 2>&1 &
NEW_PID=$!
echo "新 PID=$NEW_PID，等待就绪…"

for i in $(seq 1 30); do
  if grep -q "GameServer started" /tmp/drill-game.log 2>/dev/null; then
    echo "游戏服已就绪"
    break
  fi
  sleep 1
  [ "$i" -eq 30 ] && fail "游戏服 30s 内未就绪，见 /tmp/drill-game.log"
done

# ---------- 5. 重连校验 ----------
log "5/6 重新登录，校验数据一致"
go run ./tools/loadtest -mode smoke -login "$LOGIN" -gateway "$GW" -devices 1 || fail "重启后冒烟失败"

mysql_q "SELECT id, name, level, exp, coin FROM player WHERE id=$PLAYER_ID" > "$SNAP_DIR/player.after"
mysql_q "SELECT item_id, count FROM player_item WHERE player_id=$PLAYER_ID ORDER BY item_id" > "$SNAP_DIR/item.after"
mysql_q "SELECT career, hp, mp, atk, def, spd FROM player_attr WHERE player_id=$PLAYER_ID" > "$SNAP_DIR/attr.after"

# ---------- 6. 比对 ----------
log "6/6 比对前后快照"
RC=0
for t in player item attr; do
  if diff -u "$SNAP_DIR/$t.before" "$SNAP_DIR/$t.after" > "$SNAP_DIR/$t.diff"; then
    echo "  $t: 一致 ✓"
  else
    echo "  $t: 不一致 ✗"
    cat "$SNAP_DIR/$t.diff"
    RC=1
  fi
done

if [ "$RC" -eq 0 ]; then
  log "演练通过：kill -9 后数据零丢失，重连可恢复"
else
  fail "演练失败：强杀后数据出现差异（C 类：数据丢失）"
fi
