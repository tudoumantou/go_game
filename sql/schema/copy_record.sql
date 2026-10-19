-- 副本进度整体进玩家快照：结算、首通奖励与经验同一次 Redis CAS 提交（R2.2）。
-- @flat:copy
CREATE TABLE `copy_record` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `records`   TEXT NOT NULL COMMENT '副本状态 JSON：{copy_id:{star,pass_count,first_pass}}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色副本进度';
