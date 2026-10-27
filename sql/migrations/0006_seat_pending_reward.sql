-- 0006_seat_pending_reward.sql —— 抢位置座位离线奖励表
--
-- 供已有库增量升级；新建库由 sql/schema/seat_pending_reward.sql 建出，
-- 两处 DDL 必须保持一致（make gen-check 校验模型与 schema 一致）。
CREATE TABLE IF NOT EXISTS `seat_pending_reward` (
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `seq`        BIGINT NOT NULL COMMENT '释放事件序号（幂等键）',
  `tier`       VARCHAR(16) NOT NULL DEFAULT '' COMMENT '档位 high/mid/low',
  `minutes`    INT NOT NULL DEFAULT 0 COMMENT '占据分钟数',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`, `seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='座位离线挂起奖励';
