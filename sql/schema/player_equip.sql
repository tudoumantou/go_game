-- 装备实例整体进玩家快照：穿戴/卸下与属性加成同一次 Redis CAS 提交（R2.2）。
-- @flat:equip
CREATE TABLE `player_equip` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `equips`    TEXT NOT NULL COMMENT '装备实例 JSON 数组：[{uid,equip_id,enhance_level,quality,equipped}]',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色装备实例';
