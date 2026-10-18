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
| 网关/登录 | HTTP `/healthz` | HTTP `/healthz` | HTTP `/healthz` |
| 游戏/跨服 | TCP gRPC 端口 | etcd 注册 + idtab ready（exec） | actor 分片心跳（exec） |
| 中心服 | TCP gRPC 端口 | etcd 注册 + 选主完成（exec） | 主循环心跳（exec） |
