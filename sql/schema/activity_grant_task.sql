-- 里程碑发奖任务（R7.4）：达成时给每个贡献玩家建一条，逐条发邮件并标记完成。
-- 可恢复：status 落库，进程退出后从 pending 继续；mail_id 由 (instance, milestone, player)
-- 派生，重复执行不会重复发奖。
CREATE TABLE `activity_grant_task` (
  `id`          VARCHAR(96) NOT NULL COMMENT '{instance_id}:{milestone_id}:{player_id}',
  `instance_id` VARCHAR(64) NOT NULL,
  `milestone_id` INT NOT NULL,
  `player_id`   BIGINT UNSIGNED NOT NULL,
  `status`      TINYINT NOT NULL DEFAULT 0 COMMENT '0 待发 1 已完成',
  `updated_at`  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='里程碑发奖任务（断点续跑）';
