-- 玩家七日签到锚点；epoch_day 是北京时间自然日对应的连续日序号。
CREATE TABLE `player_signin` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `anchor_day` BIGINT NOT NULL COMMENT '角色首个签到周期起点日序号',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色签到周期锚点';

-- 奖励快照用于幂等重试，配置修改不会改变已领取事实。
CREATE TABLE `signin_claim` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `player_id` BIGINT UNSIGNED NOT NULL,
  `sign_day` BIGINT NOT NULL COMMENT '业务日序号',
  `cycle_start_day` BIGINT NOT NULL,
  `day_index` INT NOT NULL COMMENT '周期内第几天1-7',
  `rewards` TEXT NOT NULL COMMENT 'JSON实际奖励快照',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player_sign_day` (`player_id`, `sign_day`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='签到领取流水';
