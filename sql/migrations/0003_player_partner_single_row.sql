-- 伙伴表改为单行快照（R6）
--
-- 旧结构是多行实体表（id/player_id/partner_id/star/level/exp），
-- 新结构把伙伴状态整体放进玩家快照，落库是 player_id 单行 JSON。
--
-- 这是**破坏性变更**：不保留旧数据。开发期无存量数据，直接重建；
-- 生产环境需先导出旧表、确认无读取方后再执行（见 sql/migrations/README.md）。

DROP TABLE IF EXISTS `player_partner`;

CREATE TABLE `player_partner` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `partners`  TEXT NOT NULL COMMENT '伙伴状态 JSON：{stars:{partner_id:star}, field:[partner_id]}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色伙伴状态';
