-- 抢位置：座位并入房间表的 slots 字段，删除 seat_slot 独立表。
--
-- 理由：座位是房间的组成部分（值对象），读写粒度始终是整房——落库写整房
-- 快照、重启读全量。拆成 100 行只会让写入变 10 行拼接、并带来「部分写入」
-- 的一致性问题；且项目既有模式（player_equip.equips、player_quest.quests）
-- 也是把子结构收进一行。

-- 5.7 的 JSON 列不支持 DEFAULT，只能先加可空列 → 回填 → 再改 NOT NULL。
ALTER TABLE `seat_room`
  ADD COLUMN `slots` JSON NULL COMMENT '10 个座位状态（按 index 升序）';

UPDATE `seat_room` r
LEFT JOIN (
  SELECT `tier`, `room_index`,
         JSON_ARRAYAGG(JSON_OBJECT(
           'index',       `seat_index`,
           'state',       `state`,
           'occupant',    `occupant`,
           'occupy_id',   `occupy_id`,
           'occupied_at', `occupied_at`
         )) AS j
  FROM `seat_slot`
  GROUP BY `tier`, `room_index`
) s ON s.`tier` = r.`tier` AND s.`room_index` = r.`room_index`
SET r.`slots` = COALESCE(s.j, JSON_ARRAY());

ALTER TABLE `seat_room`
  MODIFY COLUMN `slots` JSON NOT NULL COMMENT '10 个座位状态（按 index 升序）';

DROP TABLE IF EXISTS `seat_slot`;
