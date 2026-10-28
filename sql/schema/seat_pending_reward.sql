-- 抢位置：座位离线/待结算奖励表（outbox）。
--
-- 座位释放（超时/被抢夺）时玩家可能已离线，玩家 Actor 不在内存；即使在线，
-- 「先落库释放座位、再发奖」之间也可能崩溃。因此结算统一走 outbox：
--   写本表（幂等）→ 座位置空并落库 → 尝试在线发奖 → 成功则删本行。
-- 任何一步崩溃，玩家下次登录由 seat 模块按 occupy_id 补发。
--
-- 主键 (player_id, occupy_id)：occupy_id 是「本次占用实例」的 ID，重启后
-- 重放同一笔结算仍然是同一个 ID，天然幂等；用结算时刻当键则做不到。
CREATE TABLE IF NOT EXISTS `seat_pending_reward` (
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `occupy_id`  BIGINT UNSIGNED NOT NULL COMMENT '占用实例ID（结算幂等键）',
  `tier`       TINYINT NOT NULL DEFAULT 0 COMMENT '档位 1=高 2=中 3=低（0 非法）',
  `minutes`    INT NOT NULL DEFAULT 0 COMMENT '占据分钟数',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`, `occupy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='座位待结算奖励（outbox）';
