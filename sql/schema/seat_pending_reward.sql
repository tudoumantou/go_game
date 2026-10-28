-- 抢位置：座位离线奖励挂起表。
--
-- 座位释放（超时/被抢夺）时玩家可能已离线，玩家 Actor 不在内存，
-- 奖励先落这张表，登录时由 seat 模块补发并删除。
-- 主键 (player_id, seq)：同一释放事件重复投递只留一行，天然幂等。
CREATE TABLE IF NOT EXISTS `seat_pending_reward` (
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `seq`        BIGINT NOT NULL COMMENT '释放事件序号（幂等键）',
  `tier`       TINYINT NOT NULL DEFAULT 0 COMMENT '档位 1=高 2=中 3=低（0 非法）',
  `minutes`    INT NOT NULL DEFAULT 0 COMMENT '占据分钟数',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`, `seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='座位离线挂起奖励';
