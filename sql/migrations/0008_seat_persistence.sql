-- 抢位置：房间/座位状态落库，挂起表幂等键改为 occupy_id。
--
-- 背景：座位原先是纯内存设施，重启即清空。改为落库后：
--  * 重启恢复「谁坐着、坐了多久」，并立即结算重启期间已到期的座位；
--  * 结算幂等键由「结算时刻」改为「占用实例 ID」，重放不会重复发奖。

CREATE TABLE IF NOT EXISTS `seat_room` (
  `tier`       TINYINT NOT NULL COMMENT '档位 1=高 2=中 3=低',
  `room_index` INT NOT NULL COMMENT '档位内房号（0 基）',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0=开放 1=关闭',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tier`, `room_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抢位置房间状态';

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

-- 挂起表主键列 seq → occupy_id。
-- 旧值（结算时刻纳秒）继续沿用即可保证唯一性，不必清洗数据：它同样能
-- 阻止同一行被重复补发，只是语义上不再对应「占用实例」。
ALTER TABLE `seat_pending_reward`
  CHANGE COLUMN `seq` `occupy_id` BIGINT UNSIGNED NOT NULL COMMENT '占用实例ID（结算幂等键）';
