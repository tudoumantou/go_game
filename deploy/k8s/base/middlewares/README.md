# middlewares/ —— 中间件 Operator CR（19.6）

etcd/Redis/MySQL/Kafka **不手写 Deployment**，用官方 Operator 或云托管：

| 中间件 | 方案 |
| --- | --- |
| etcd | etcd Operator（`EtcdCluster` CR） |
| Redis | Redis Operator（`RedisCluster`/`RedisReplication` CR） |
| MySQL | MySQL Operator 或云 RDS（19.4 compose 的 mysql:8.0 仅本地） |
| Kafka | Strimzi（`Kafka` CR，topic `gs.login.log`/`gs.recharge.order`/`gs.mail`/`gs.player.event` 等） |

Operator CR 清单在 P7 迁移时按环境补充，自带故障自愈、备份与版本升级。
