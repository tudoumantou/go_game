-- 技能等级整体进玩家快照：学习/升级与消耗在同一次 Redis CAS 提交（R2.2）。
-- @flat:skill
CREATE TABLE `player_skill` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `skills`    TEXT NOT NULL COMMENT '技能等级 JSON：{skill_id:level}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色技能等级';
