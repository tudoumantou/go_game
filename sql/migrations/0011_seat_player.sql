-- 抢位置：房间引入稳定唯一 ID，新增玩家玩法数据表。
--
-- room_id = tier*100 + room_index，确定性编码而不是自增：重建库/换环境后
-- 同一间房仍是同一个 ID，玩家侧数据不会指向错房间。

ALTER TABLE `seat_room`
  ADD COLUMN `id` INT NOT NULL DEFAULT 0 COMMENT '房间唯一ID：tier*100+room_index' FIRST;

UPDATE `seat_room` SET `id` = `tier` * 100 + `room_index`;

ALTER TABLE `seat_room`
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uk_tier_index` (`tier`, `room_index`),
  ALTER COLUMN `id` DROP DEFAULT;

-- 玩家占座数据：主键 player_id 天然就是「一人一座」约束。
CREATE TABLE IF NOT EXISTS `seat_player` (
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `room_id`     INT NOT NULL COMMENT '所在房间ID（seat_room.id）',
  `seat_index`  INT NOT NULL COMMENT '座位号 0..9',
  `occupied_at` BIGINT NOT NULL COMMENT '开始占用 unix 秒',
  `updated_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`),
  KEY `idx_room_seat` (`room_id`, `seat_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抢位置玩家数据（占座索引）';

-- 从现有房间快照回填玩家数据（重启后不用等第一次变更才补上）。
INSERT IGNORE INTO `seat_player` (`player_id`, `room_id`, `seat_index`, `occupied_at`)
SELECT jt.`occupant`, r.`id`, jt.`index`, jt.`occupied_at`
FROM `seat_room` r,
     JSON_TABLE(r.`slots`, '$[*]' COLUMNS (
       `index`       INT     PATH '$.index',
       `state`       INT     PATH '$.state',
       `occupant`    BIGINT  PATH '$.occupant',
       `occupied_at` BIGINT  PATH '$.occupied_at'
     )) AS jt
WHERE jt.`state` = 1 AND jt.`occupant` > 0;
