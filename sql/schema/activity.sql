-- 活动进度与领取位图整体进玩家快照，领奖与发放同一次 Redis CAS 提交（R2.2）。
-- @flat:activity
CREATE TABLE `activity_record` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `records`   TEXT NOT NULL COMMENT '活动状态 JSON：{activity_id:{progress,reward_mask}}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色活动状态';
