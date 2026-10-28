-- 抢位置：座位状态表（单服）。
--
-- 每行一个座位（10 房 × 10 座 = 100 行）。内存仍是权威，这张表解决两件事：
--  1. 进程重启后恢复「谁坐着、从什么时候开始坐」；
--  2. 用 occupy_id 标识「本次占用实例」——结算奖励以它为幂等键，
--     重启后重放同一笔结算不会重复发奖（用结算时刻当键做不到这点，
--     因为重放会生成新的时间戳）。
CREATE TABLE IF NOT EXISTS `seat_slot` (
  `tier`        TINYINT NOT NULL COMMENT '档位 1=高 2=中 3=低',
  `room_index`  INT NOT NULL COMMENT '档位内房号（0 基）',
  `seat_index`  INT NOT NULL COMMENT '座号 0..9',
  `state`       TINYINT NOT NULL DEFAULT 0 COMMENT '0=空 1=占用',
  `occupant`    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '占用者 player_id；空为 0',
  `occupy_id`   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本次占用实例ID（结算幂等键）',
  `occupied_at` BIGINT NOT NULL DEFAULT 0 COMMENT '占用起始 unix 秒；空为 0',
  `updated_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tier`, `room_index`, `seat_index`),
  KEY `idx_occupant` (`occupant`),
  KEY `idx_state_occupied` (`state`, `occupied_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抢位置座位状态';
