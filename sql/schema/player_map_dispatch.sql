-- sql/schema/player_map_dispatch.sql
-- 玩家侧出征许可镜像（设计文档 14.1 最后一行）
--
-- 属于 player 的写入方（Persister），不归地图事务管。分表键与其它
-- player_* 表一致（player_id % 512）；这里不建唯一键之外的约束，
-- 避免与玩家 Actor 的写入路径争锁。
CREATE TABLE `player_map_dispatch` (
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家',
  `data_json`   JSON            NOT NULL COMMENT '许可数据',
  `updated_at`  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家侧出征许可';
