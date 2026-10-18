# deploy/k8s —— 生产 K8s 编排（设计文档 19.6）

**落地顺序**（19.6 节末）：P7 先把网关/登录服迁上 K8s（无状态、收益快）；游戏服/中心服
等优雅关闭（第 18 章）验收通过后再迁——有状态服务上 K8s 的前提是关闭编排可靠。

## 结构

```
base/         公共清单（namespace + configmaps + secrets）
workloads/    五类服务工作负载（gateway/login = Deployment+HPA；game/center = StatefulSet；cross = Deployment）
overlays/     dev / prod kustomize overlay（副本数、镜像 tag）
middlewares/  中间件 Operator CR 说明（不手写 Deployment）
```

## 使用

```bash
# 本地渲染校验（不 apply）
kustomize build overlays/prod

# 部署（P7 阶段，且优雅关闭验收后）
kustomize build overlays/prod | kubectl apply -f -
```

## 探针说明（19.6 表）

P0 占位用 TCP 探针保证可运行；P1/P2 按探针表替换：

| 服务 | startupProbe | readinessProbe | livenessProbe |
| --- | --- | --- | --- |
| 网关/登录 | HTTP `/healthz`（metrics 端口） | HTTP `/readyz`（校验 Redis/MySQL） | HTTP `/healthz` |
| 游戏 | HTTP `/healthz`（metrics 端口） | HTTP `/readyz`（校验 Redis/MySQL） | HTTP `/healthz` |
| 中心/跨服 | HTTP `/healthz` | HTTP `/readyz` | HTTP `/healthz` |

**探针语义（P7 已全部落地）**：

- `/healthz` —— 只回答「进程活着吗」，**不查任何依赖**。中间件短时抖动不应让全部 Pod 一起重启。
- `/readyz` —— 回答「能接业务流量吗」，Redis/MySQL 探活失败返回 503，K8s 从 Endpoints 摘除该实例。
- `/metrics` —— Prometheus 抓取（见下方监控一节）。

> P0 阶段的 TCP 探针已全部替换为 HTTP 探针（TCP 探针在「进程活着但 DB 断了」时仍报就绪，会让流量打到不可用实例）。

## 监控（P7）

| 文件 | 用途 |
| --- | --- |
| `deploy/k8s/monitoring/servicemonitor.yaml` | ServiceMonitor + PrometheusRule（Prometheus Operator 场景） |
| `deploy/monitoring/prometheus.yml` | 抓取配置（docker compose 场景） |
| `deploy/monitoring/rules/gs-alerts.yml` | 告警规则（9 条） |
| `deploy/monitoring/grafana/` | 看板 `gs-overview`（5 分区 15 面板）+ 数据源自动注册 |

前提：集群装了 kube-prometheus-stack。各 workload 的 Service 已打 `metrics: enabled` 标签，ServiceMonitor 据此动态发现，扩容后自动纳入。

```bash
kubectl apply -f deploy/k8s/monitoring/
```

指标清单与告警阈值见设计文档 20.3.1。
