#!/usr/bin/env bash
# scripts/bench.sh —— 压测并生成报告（设计文档 23.4 压测与验收指标）
#
# 依次跑三种负载，产出 markdown + JSON：
#   1. login  登录吞吐（HTTP 游客登录 + RSA 握手 + 加密登录请求）
#   2. biz    业务消息（进游戏后混合「背包/角色信息/聊天」，测 Actor 全链路）
#   3. biz-ro 只读业务（只发背包/角色信息，排除聊天写库，单独看 Actor 主链路延迟）
#   4. conn   连接保持（只建连不断开，测网关承载上限）
#
# 用法：
#   ./scripts/bench.sh                          # 用默认规模（约 2 分钟）
#   LOGIN_N=2000 BIZ_N=200 CONN_N=5000 ./scripts/bench.sh
#   CONN_N=100000 CONN_HOLD=5m ./scripts/bench.sh   # 10W 连接需先 ulimit -n 200000
#
# 前置：五服务已启动（make run-all），登录服 /healthz 可访问。
set -uo pipefail

cd "$(dirname "$0")/.."

LOGIN="${LOGIN:-http://127.0.0.1:8080}"
GW="${GW:-127.0.0.1:9001}"

LOGIN_N="${LOGIN_N:-500}"
LOGIN_C="${LOGIN_C:-50}"
BIZ_N="${BIZ_N:-50}"
BIZ_C="${BIZ_C:-50}"
BIZ_ROUNDS="${BIZ_ROUNDS:-20}"
CONN_N="${CONN_N:-1000}"
CONN_HOLD="${CONN_HOLD:-30s}"

OUT="${OUT:-reports/bench-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33mWARN: %s\033[0m\n' "$*"; }

# ---------- 0. 前置 ----------
log "0/5 前置检查"
curl -sf "${LOGIN}/healthz" >/dev/null || {
  echo "登录服未就绪：${LOGIN}" >&2
  echo "先执行：make dev && make db-init && make db-seed && make run-all" >&2
  exit 1
}
echo "登录服 OK，网关 ${GW}"

if [ "$CONN_N" -gt 5000 ]; then
  fd_limit=$(ulimit -n)
  if [ "$fd_limit" -lt $((CONN_N * 2)) ]; then
    warn "ulimit -n=${fd_limit} 低于 ${CONN_N} 连接所需，请先 ulimit -n 200000"
  fi
fi

GOFLAGS="${GOFLAGS:-}"
go build -o /tmp/loadtest ./tools/loadtest || exit 1
LT=/tmp/loadtest

# ---------- 1. 登录吞吐 ----------
log "1/5 登录吞吐（${LOGIN_N} 次，并发 ${LOGIN_C}）"
"$LT" -mode login -n "$LOGIN_N" -c "$LOGIN_C" -login "$LOGIN" -gateway "$GW" \
  -json "$OUT/login.json" 2>&1 | tee "$OUT/login.log"
LOGIN_RC=${PIPESTATUS[0]}

# ---------- 2. 业务消息 ----------
log "2/5 业务消息（${BIZ_N} 玩家 × ${BIZ_ROUNDS} 轮，混合）"
"$LT" -mode biz -n "$BIZ_N" -c "$BIZ_C" -rounds "$BIZ_ROUNDS" \
  -login "$LOGIN" -gateway "$GW" -json "$OUT/biz.json" 2>&1 | tee "$OUT/biz.log"
BIZ_RC=${PIPESTATUS[0]}

# ---------- 3. 只读业务 ----------
log "3/5 只读业务（${BIZ_N} 玩家 × ${BIZ_ROUNDS} 轮，背包/角色）"
"$LT" -mode biz -n "$BIZ_N" -c "$BIZ_C" -rounds "$BIZ_ROUNDS" -kind bag \
  -login "$LOGIN" -gateway "$GW" -json "$OUT/biz-ro.json" 2>&1 | tee "$OUT/biz-ro.log"
BIZRO_RC=${PIPESTATUS[0]}

# ---------- 4. 连接保持 ----------
log "4/5 连接保持（${CONN_N} 连接，保持 ${CONN_HOLD}）"
"$LT" -mode conn -n "$CONN_N" -hold "$CONN_HOLD" -gateway "$GW" \
  -json "$OUT/conn.json" 2>&1 | tee "$OUT/conn.log"
CONN_RC=${PIPESTATUS[0]}

# ---------- 5. 报告 ----------
log "5/5 生成报告"

# 采集指标快照（若 metrics 端口可访问，用于佐证资源占用）
for svc in login:8081 gateway:9101 game:9102; do
  name="${svc%%:*}"; port="${svc##*:}"
  curl -sf "http://127.0.0.1:${port}/metrics" -o "$OUT/metrics-${name}.txt" 2>/dev/null || true
done

python3 - "$OUT" "$LOGIN_N" "$BIZ_N" "$BIZ_ROUNDS" "$CONN_N" <<'PY'
import json, os, sys, datetime

out, login_n, biz_n, biz_rounds, conn_n = sys.argv[1:6]

def load(name):
    p = os.path.join(out, name)
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# 设计文档 23.4 的验收目标
TARGETS = {
    "登录吞吐": "≥ 2000 TPS",
    "业务消息": "P99 < 50ms",
    "连接保持": "≥ 100000 连接",
}

rows = []
for key, title in (("login", "登录吞吐"), ("biz", "业务消息（混合）"),
                   ("biz-ro", "业务消息（只读）"), ("conn", "连接保持")):
    d = load(key + ".json")
    if not d:
        rows.append(f"| {title} | - | - | - | - | - | 未执行 |")
        continue
    ok = d["errors"] == 0
    rows.append(
        f"| {title} | {d['count']} | {d['tps']:.0f}/s | "
        f"{d['p50_ms']:.2f} | {d['p95_ms']:.2f} | {d['p99_ms']:.2f} | "
        f"{'✅' if ok else '❌ ' + str(d['errors']) + ' 失败'} |"
    )

now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
report = f"""# 压测报告

生成时间：{now}
环境：单机（五服务 + MySQL/Redis/etcd/Kafka 同机）

## 负载

| 项 | 规模 |
| --- | --- |
| 登录 | {login_n} 次，并发 50 |
| 业务消息 | {biz_n} 玩家 × {biz_rounds} 轮 |
| 连接保持 | {conn_n} 连接 |

## 结果

| 场景 | 请求数 | 吞吐 | P50 (ms) | P95 (ms) | P99 (ms) | 状态 |
| --- | --- | --- | --- | --- | --- | --- |
{chr(10).join(rows)}

## 对照设计目标（23.4）

| 场景 | 目标 | 说明 |
| --- | --- | --- |
| 登录吞吐 | {TARGETS['登录吞吐']} | 单机部署，未横向扩展 |
| 业务消息 | {TARGETS['业务消息']} | 混合背包/角色/聊天 |
| 连接保持 | {TARGETS['连接保持']} | 受本机 ulimit -n 限制 |

> 本报告是**单机基线**，用于回归对比（19.5 第 7 条）。
> 每次里程碑复跑一次，指标劣化超过 20% 视为回归。
"""
path = os.path.join(out, "REPORT.md")
with open(path, "w", encoding="utf-8") as f:
    f.write(report)
print(report)
print(f"\n报告已写入 {path}")
PY

RC=0
[ "$LOGIN_RC" -ne 0 ] && RC=1
[ "$BIZ_RC" -ne 0 ] && RC=1
[ "$BIZRO_RC" -ne 0 ] && RC=1
[ "$CONN_RC" -ne 0 ] && RC=1

if [ "$RC" -eq 0 ]; then
  log "压测完成：$OUT"
else
  warn "压测有失败项，详见 $OUT/*.log"
fi
exit "$RC"
