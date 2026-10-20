-- sql/schema/map_city_owner.sql
-- 玩家 → 主堡唯一映射（设计文档 14.1）
--
-- 两个唯一键缺一不可：world+player 防止一个玩家两座主堡；
-- world+city 防止两座城指向同一玩家（并发创建时总有一个事务失败）。
CREATE TABLE `map_city_owner` (
  `world_id`  BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `player_id` BIGINT          NOT NULL COMMENT '玩家编号',
  `city_id`   BIGINT UNSIGNED NOT NULL COMMENT '主堡对象编号',
  PRIMARY KEY (`world_id`, `player_id`),
  UNIQUE KEY `uk_city` (`world_id`, `city_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家主堡唯一映射';
