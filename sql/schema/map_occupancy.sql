-- sql/schema/map_occupancy.sql
-- 静态格占位（设计文档 5.3 / 14.1）
--
-- 唯一键 (world,cell_x,cell_y,layer) 是占位的**最终约束**：内存预检只是加速，
-- 两个并发创建同时在内存里判定"空格"时，只有一个事务能在这里插成功，
-- 另一个拿 1062 后返回 PositionOccupied。
CREATE TABLE `map_occupancy` (
  `world_id`  BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `cell_x`    BIGINT          NOT NULL COMMENT '格 X',
  `cell_y`    BIGINT          NOT NULL COMMENT '格 Y',
  `layer`     INT             NOT NULL DEFAULT 0 COMMENT '占位层(第一版只有 0)',
  `object_id` BIGINT UNSIGNED NOT NULL COMMENT '占用对象',
  PRIMARY KEY (`world_id`, `cell_x`, `cell_y`, `layer`),
  KEY `idx_object` (`world_id`, `object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='静态格占位';
