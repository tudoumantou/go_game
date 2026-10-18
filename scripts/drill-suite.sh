#!/usr/bin/env bash
# scripts/drill-suite.sh —— 故障演练套件（设计文档 23.5）
#
# 覆盖 23.5 清单中可自动化的五项：
#   1. MySQL 断连    → /healthz 仍 200（进程活着）、/readyz 503（摘流量）、恢复后自动回 200
#   2. Redis 断连    → 同上（Redis 是 Tier-2 热数据，断了不能登录但进程不能崩）
#   3. Kafka 停 broker → 服务存活且**仍就绪**（异步链路降级，不该影响登录）
#   4. etcd 重启     → 服务重新注册，smoke 可通
#   5. 单分片 panic  → 由 flush 单测覆盖（进程内 recover），不在本脚本重复
#
# kill -9 游戏服（23.5 第 1 行）在 scripts/drill-crash.sh，因为它要校验数据快照，流程更重。
#
# 前置：五服务已启动、docker 可用、容器名可探测。
# 用法：
#   ./scripts/drill-suite.sh
#   DRILL_WAIT=10 ./scripts/drill-suite.sh          # 每步注入后等待秒数
#   MYSQL_CONTAINER=deploy-mysql-1 ./scripts/drill-suite.sh
set -uo pipefail

cd "$(dirname "$0")/.."

LOGIN="${LOGIN:-http://127.0.0.1:8080}"
GAME_METRICS="${GAME_METRICS:-http://127.0.0.1:9102}"
GW="${GW:-127.0.0.1:9001}"
DRILL_WAIT="${DRILL_WAIT:-15}"
RECOVER_WAIT="${RECOVER_WAIT:-60}"

OUT="${OUT:-reports/drill-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"

log()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
pass() { printf '  \033[1;32m✓ %s\033[0m\n' "$*"; RESULTS+=("PASS|$CASE|$*"); }
fail() { printf '  \033[1;31m✗ %s\033[0m\n' "$*"; RESULTS+=("FAIL|$CASE|$*"); CASE_FAILED=1; }

RESULTS=()
CASE=""
CASE_FAILED=0

# find_container 按 compose 服务名找容器（兼容自定义 project 名）。
find_container() {
  docker ps --format '{{.Names}}' 2>/dev/null | grep -E "(^|[-_])$1[-_][0-9]+$" | head -1
}

MYSQL_C="${MYSQL_CONTAINER:-$(find_container mysql)}"
REDIS_C="${REDIS_CONTAINER:-$(find_container redis)}"
KAFKA_C="${KAFKA_CONTAINER:-$(find_container kafka)}"
ETCD_C="${ETCD_CONTAINER:-$(find_container etcd)}"

# 就绪探针：返回 0 表示 200。
alive() { curl -sf --max-time 3 "${GAME_METRICS}/healthz" >/dev/null 2>&1; }
ready() { curl -sf --max-time 3 "${GAME_METRICS}/readyz"  >/dev/null 2>&1; }

# wait_ready 轮询直到就绪或超时。
wait_ready() {
  local limit=$1 i
  for i in $(seq 1 "$limit"); do
    ready && return 0
    sleep 1
  done
  return 1
}

# smoke 跑五步冒烟（每次用唯一 device，避免复用角色掩盖问题）。
smoke() {
  go run ./tools/loadtest -mode smoke -login "$LOGIN" -gateway "$GW" -devices 1 \
    >"$OUT/smoke-${CASE// /_}.log" 2>&1
}

# ---------- 0. 前置 ----------
log "0/5 前置检查"
command -v docker >/dev/null || { echo "需要 docker" >&2; exit 1; }
curl -sf "${LOGIN}/healthz" >/dev/null || { echo "登录服未就绪：$LOGIN" >&2; exit 1; }
alive || { echo "游戏服 metrics 未就绪：$GAME_METRICS" >&2; exit 1; }

echo "MySQL=${MYSQL_C:-未找到}  Redis=${REDIS_C:-未找到}  Kafka=${KAFKA_C:-未找到}  etcd=${ETCD_C:-未找到}"
[ -z "$MYSQL_C" ] && echo "  提示：可用 MYSQL_CONTAINER=xxx 指定容器名"

# ---------- 1. MySQL ----------
CASE="MySQL 断连"
CASE_FAILED=0
log "1/5 $CASE"
if [ -z "$MYSQL_C" ]; then
  fail "未找到 MySQL 容器，跳过"
else
  ready || fail "注入前就未就绪，无法判断恢复情况"
  docker pause "$MYSQL_C" >/dev/null 2>&1 || fail "pause 失败"
  echo "  已注入，等待 ${DRILL_WAIT}s…"
  sleep "$DRILL_WAIT"

  alive && pass "进程存活（/healthz 200）" || fail "进程崩溃（/healthz 不通）"
  if ready; then
    fail "/readyz 仍为 200，就绪探针未反映 MySQL 故障"
  else
    pass "就绪探针摘除（/readyz 503）"
  fi

  docker unpause "$MYSQL_C" >/dev/null 2>&1 || fail "unpause 失败"
  if wait_ready "$RECOVER_WAIT"; then
    pass "恢复后就绪（${RECOVER_WAIT}s 内）"
  else
    fail "恢复后 ${RECOVER_WAIT}s 仍未就绪"
  fi
  smoke && pass "恢复后冒烟通过" || fail "恢复后冒烟失败"
fi

# ---------- 2. Redis ----------
CASE="Redis 断连"
CASE_FAILED=0
log "2/5 $CASE"
if [ -z "$REDIS_C" ]; then
  fail "未找到 Redis 容器，跳过"
else
  ready || fail "注入前就未就绪"
  docker pause "$REDIS_C" >/dev/null 2>&1 || fail "pause 失败"
  echo "  已注入，等待 ${DRILL_WAIT}s…"
  sleep "$DRILL_WAIT"

  alive && pass "进程存活（Redis 断了也不能崩）" || fail "进程崩溃"
  if ready; then
    fail "/readyz 仍为 200，就绪探针未反映 Redis 故障"
  else
    pass "就绪探针摘除（/readyz 503）"
  fi

  docker unpause "$REDIS_C" >/dev/null 2>&1 || fail "unpause 失败"
  if wait_ready "$RECOVER_WAIT"; then
    pass "恢复后就绪"
  else
    fail "恢复后 ${RECOVER_WAIT}s 仍未就绪"
  fi
  smoke && pass "恢复后冒烟通过（Actor 可从 DB 重建）" || fail "恢复后冒烟失败"
fi

# ---------- 3. Kafka ----------
CASE="Kafka 停 broker"
CASE_FAILED=0
log "3/5 $CASE"
if [ -z "$KAFKA_C" ]; then
  fail "未找到 Kafka 容器，跳过"
else
  docker stop "$KAFKA_C" >/dev/null 2>&1 || fail "stop 失败"
  echo "  已注入，等待 ${DRILL_WAIT}s…"
  sleep "$DRILL_WAIT"

  alive && pass "进程存活" || fail "进程崩溃（Kafka 不该影响主链路）"
  # Kafka 是异步链路：停掉后登录/背包等同步功能应照常，readiness 不应摘除
  if ready; then
    pass "仍就绪（异步链路降级，不影响登录）"
  else
    fail "/readyz 被摘除，Kafka 不应影响就绪判定"
  fi
  smoke && pass "Kafka 不可用时冒烟通过" || fail "Kafka 不可用时冒烟失败"

  docker start "$KAFKA_C" >/dev/null 2>&1 || fail "start 失败"
  echo "  已恢复，等待 ${DRILL_WAIT}s 让消费者重连…"
  sleep "$DRILL_WAIT"
  alive && pass "恢复后存活" || fail "恢复后进程异常"
fi

# ---------- 4. etcd ----------
CASE="etcd 重启"
CASE_FAILED=0
log "4/5 $CASE"
if [ -z "$ETCD_C" ]; then
  fail "未找到 etcd 容器，跳过"
else
  docker restart "$ETCD_C" >/dev/null 2>&1 || fail "restart 失败"
  echo "  已重启，等待 ${DRILL_WAIT}s 让各服务重新注册…"
  sleep "$DRILL_WAIT"

  alive && pass "进程存活" || fail "进程崩溃"
  # 注册恢复需要时间：轮询 readiness（它依赖 Redis/MySQL，与 etcd 无关，
  # 所以这里主要验证「etcd 抖动没有把服务带崩」+ 冒烟可达）
  if wait_ready 30; then
    pass "就绪恢复"
  else
    fail "etcd 重启后未恢复就绪"
  fi
  smoke && pass "etcd 重启后冒烟通过（服务列表已恢复）" || fail "etcd 重启后冒烟失败"
fi

# ---------- 5. 汇总 ----------
log "5/5 汇总"
{
  echo "# 故障演练结果"
  echo
  echo "时间：$(date '+%Y-%m-%d %H:%M:%S')"
  echo "注入等待：${DRILL_WAIT}s  恢复等待：${RECOVER_WAIT}s"
  echo
  echo "| 结论 | 用例 | 检查项 |"
  echo "| --- | --- | --- |"
  for r in "${RESULTS[@]}"; do
    IFS='|' read -r verdict case item <<< "$r"
    if [ "$verdict" = "PASS" ]; then icon="✅"; else icon="❌"; fi
    echo "| $icon | $case | $item |"
  done
} | tee "$OUT/REPORT.md"

FAILED=$(printf '%s\n' "${RESULTS[@]}" | grep -c '^FAIL' || true)
TOTAL=${#RESULTS[@]}
if [ "$FAILED" -eq 0 ]; then
  log "演练通过：$TOTAL 项检查全部符合预期"
  exit 0
fi
printf '\n\033[1;31m演练失败：%d/%d 项不符合预期，详见 %s\033[0m\n' "$FAILED" "$TOTAL" "$OUT"
exit 1
