-- 任务状态整体进玩家快照：一行装下全部任务的进度与领取事实，
-- 领取奖励与状态变更在同一次 Redis CAS 里提交（R2.2）。
-- @flat:quest
CREATE TABLE `player_quest` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `quests`    TEXT NOT NULL COMMENT '任务状态 JSON：{quest_id:{status,progress}}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色任务状态';
