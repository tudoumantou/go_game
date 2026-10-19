-- 玩家伙伴：整体进快照，激活/升星/上阵与材料消耗同一次 Redis CAS 提交（R6.2/R6.3）。
-- 单行 JSON：{"stars":{"1":2},"field":[1]}
-- @flat:partner
CREATE TABLE `player_partner` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `partners`  TEXT NOT NULL COMMENT '伙伴状态 JSON：{stars:{partner_id:star}, field:[partner_id]}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色伙伴状态';
