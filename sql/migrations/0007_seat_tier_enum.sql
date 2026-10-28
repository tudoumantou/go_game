-- 抢位置：档位由字符串改为数字（1 高 / 2 中 / 3 低）。
--
-- 0006 建表时 tier 是 VARCHAR(16)（'high'/'mid'/'low'），现统一为数字：
-- 与协议 int32、内存 Tier 枚举一一对应，且 0 天然表示非法值。
--
-- 必须先回填再改类型：MySQL 把非数字字符串转 TINYINT 会静默变 0，
-- 直接 MODIFY 会让存量行全部丢失档位（奖励发错档）。
UPDATE `seat_pending_reward` SET `tier` = '1' WHERE `tier` = 'high';
UPDATE `seat_pending_reward` SET `tier` = '2' WHERE `tier` = 'mid';
UPDATE `seat_pending_reward` SET `tier` = '3' WHERE `tier` = 'low';

ALTER TABLE `seat_pending_reward`
  MODIFY COLUMN `tier` TINYINT NOT NULL DEFAULT 0 COMMENT '档位 1=高 2=中 3=低（0 非法）';
