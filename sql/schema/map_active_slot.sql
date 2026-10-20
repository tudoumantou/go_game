-- sql/schema/map_active_slot.sql
-- 出征槽位占用（INV-05）：一个槽位同时只能有一支部队（设计文档 14.1）
--
-- 部队 CLOSED 才删除本行；SETTLING 仍然占用（货还没交接完，
-- 允许复用槽位会让玩家用"未结算部队"绕过并发上限）。
CREATE TABLE `map_active_slot` (
  `world_id`  BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `player_id` BIGINT          NOT NULL COMMENT '玩家编号',
  `slot_id`   INT             NOT NULL COMMENT '出征槽位',
  `troop_id`  BIGINT UNSIGNED NOT NULL COMMENT '占用槽位的部队',
  PRIMARY KEY (`world_id`, `player_id`, `slot_id`),
  UNIQUE KEY `uk_troop` (`world_id`, `troop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='出征槽位占用';
