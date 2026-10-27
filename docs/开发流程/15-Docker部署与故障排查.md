# 15 Docker部署与故障排查

[上一章](14-测试监控与优雅停服.md) · [课程总览](00-总览与阅读顺序.md)

## 这一章做出什么

用当前 Docker 方案构建、启动并排查五服务，不混用宿主机地址和容器地址。

## 前置条件与本章文件

Docker 可用；OpenSSL、curl、bash 可用。已有项目使用 docker-up，不要求本机 Go 构建镜像。

- [scripts/docker-up.sh](../../scripts/docker-up.sh)：生成缺失密钥、复制卷、启动与等待。
- [deploy/docker-compose.yaml](../../deploy/docker-compose.yaml)：服务、网络、卷和依赖。
- [deploy/docker/game.Dockerfile](../../deploy/docker/game.Dockerfile)：多阶段静态编译。
- [configs/game/game-docker.yaml](../../configs/game/game-docker.yaml)：容器内服务地址。
- [deploy/monitoring/prometheus.yml](../../deploy/monitoring/prometheus.yml)：指标抓取。
- [Makefile](../../Makefile)：docker-up、down、logs。

## 按顺序实现

第一步先看 Compose 定义四个中间件：etcd、Redis、MySQL、Kafka；再看五个业务服务（login/gateway/game/center/cross）与 map；Prometheus/Grafana 为可选监控。assets 是复制命名卷内容的辅助容器，不是第六个业务服务。

第二步理解构建路径：Compose 文件位于 deploy，build.context 用 .. 回到仓库根。Dockerfile 先下载依赖，仅复制 cmd/internal/pkg 代码，使用共享编译缓存，再复制静态二进制到 distroless 运行阶段。密钥没有 COPY 进镜像。

第三步区分配置。宿主机 go run 使用 dev.yaml；容器使用 docker.yaml。容器 Redis 是 redis:6379，MySQL 是 mysql:3306，etcd 是 etcd:2379；Kafka 内部 kafka:9092、宿主机 127.0.0.1:9093。

第四步运行启动脚本。它为缺失密钥生成 RSA 对，保留已有私钥；通过 docker cp 将 configs、keys、schema、monitoring 更新到命名卷，避免依赖 Docker Desktop 的宿主机目录共享。

```bash
# 在现有仓库根目录执行，会构建并启动本地服务。
make docker-up
# 查看状态和最近日志，只读。
docker compose -f deploy/docker-compose.yaml ps
make docker-logs
# 另一个终端执行业务冒烟，会创建游客与角色。
make smoke
```

第五步 MySQL 首次空数据卷才执行 schema 初始化；会创建 game 业务用户。seed_dev 是可选旧占位数据，机器人不依赖它。已有库增加 DDL 需要迁移，生成 Go model 和复制 schema 都不等于迁移执行。

第六步验证健康与业务：8081/9101/9102/9201/9301 的 readyz；Login HTTP 8080；Gateway TCP 9001。不要用浏览器期待 9001 返回网页。

第七步停止：make docker-down 保留命名卷与本地密钥。不要为了“重启一下”加 down -v，它会删除持久数据。当前应用停服可靠性边界详见 14，不把容器被停止等同于所有内存已提交。

## 逐段解释与知识点

编译阶段与运行阶段隔离，减小运行镜像；CGO_ENABLED=0 便于生成静态 Go 二进制。非 root 用户读取 0600 私钥需要正确 UID/GID，脚本按当前用户设置 Gateway 身份并用 docker cp -a 保留密钥所有者。

命名卷里放的是文件快照。改 configs 后重新 make docker-up 复制；但已经加载配置的进程是否重载，不能只看文件变了。必要时有针对性重启对应服务，再验证实际值。建议后续给配置加入内容哈希，使部署能自动判断重建或重启需求。

distroless 没有普通 shell，不应指望 docker exec game sh 来排查；使用日志、探针或独立诊断容器。宿主机的 go env 不影响镜像内编译器，但会影响本机 make smoke。

版本与密码目前是开发配置，端口只绑定 127.0.0.1。迁移到远程机器还需确定外部公布地址、访问方式、凭据和持久备份，不直接沿用 localhost 的网关列表。

## 怎么验证与定位错误

常见问题按下面定位：

| 现象 | 先看什么 | 应采取的动作 |
|---|---|---|
| 拉镜像 TLS timeout | Docker 镜像代理与网络 | 重试或使用可达镜像源，不改业务代码 |
| 容器 Created 无日志 | 是否卡在宿主机 bind mount/资源限制 | 当前方案用命名卷；读 Docker 日志确认 |
| Config read failed | 文件是否已复制卷、command 路径 | 用 docker-up 准备卷，不直接裸 up 第一次启动 |
| Gateway RSA permission denied | 私钥所有者、启动用户 | 用创建密钥的系统用户与启动脚本 |
| MySQL 表不存在 | 是否已有空/旧数据卷、初始化日志 | 对练习库执行迁移，保留已有数据 |
| etcd 有节点但不可访问 | Node.Addr 是否对其他容器可达 | 检查 game:9100 与监听/公布地址 |
| Kafka 连接后失败 | advertised listener | 宿主机用 9093，容器用 9092 |
| readyz=200 但业务超时 | Actor、Module、stream 的日志 | 按 16 章纵向追踪 |
| go compile version mismatch | GOROOT 与实际 go 工具 | 使用匹配的工具链路径 |

可选监控先准备配置卷后运行 `make monitoring`，访问 3000（开发账户 admin/admin）与 9090。Prometheus 抓取成功不等于所有业务规则通过。

## 独立练习与通过标准

重新从启动脚本读一遍，每一步写下输入、输出与是否可重复执行。通过标准：能解释镜像、容器、卷、服务名、端口映射的差别，并且不靠删除数据库来解决配置问题。

下一章：[第 16 章 全链路验收与独立开发](16-全链路验收与独立开发.md)。
