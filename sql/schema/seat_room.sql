-- 抢位置：房间状态表（单服）。
--
-- 房间是固定设施（高 1 / 中 3 / 低 6），行由进程启动时幂等补齐；
-- 这里只落「可变状态」——目前是开放/关闭，留给运营临时关房。
CREATE TABLE IF NOT EXISTS `seat_room` (
  `tier`       TINYINT NOT NULL COMMENT '档位 1=高 2=中 3=低',
  `room_index` INT NOT NULL COMMENT '档位内房号（0 基）',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0=开放 1=关闭',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`tier`, `room_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抢位置房间状态';
