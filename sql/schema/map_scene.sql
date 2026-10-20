-- sql/schema/map_scene.sql
-- 场景行：revision 是场景级乐观锁（设计文档 14.1）
--
-- 每个事务都必须带 expected_revision 并对本行 FOR UPDATE。revision 只增不减，
-- 是"这个场景的状态被改过几次"的单调计数器，也是投影缺口检测的依据。
CREATE TABLE `map_scene` (
  `world_id`       BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `scene_id`       INT             NOT NULL COMMENT '场景编号(1..cols*rows)',
  `revision`       BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已提交版本，事务校验并 +1',
  `schema_version` INT UNSIGNED    NOT NULL DEFAULT 1 COMMENT '场景快照结构版本',
  PRIMARY KEY (`world_id`, `scene_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SLG 场景版本行';
