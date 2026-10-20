-- sql/schema/map_object.sql
-- 地图对象：城池/部队/资源点统一存本表，payload 按 kind 分 schema（14.1）
--
-- 第一版用类型化 JSON 快照（city_payload/troop_payload/resource_payload），
-- 不允许 map[string]any 混写：混写会让"字段是否必填"无法在编译期校验，
-- 也会让 SQL 侧的 JSON 查询无法建立稳定索引。
-- 墓碑用 deleted=1 表示，不物理删除：历史回执与审计需要能解释"这里曾经有什么"。
CREATE TABLE `map_object` (
  `world_id`       BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `object_id`      BIGINT UNSIGNED NOT NULL COMMENT '对象编号(SQL 分配)',
  `kind`           TINYINT UNSIGNED NOT NULL COMMENT '1=CITY 2=TROOP 3=RESOURCE',
  `owner_scene`    INT             NOT NULL COMMENT '当前唯一写入场景',
  `version`        BIGINT UNSIGNED NOT NULL COMMENT '对象版本，乐观并发',
  `schema_version` INT UNSIGNED    NOT NULL COMMENT 'payload 结构版本',
  `player_id`      BIGINT          NOT NULL DEFAULT 0 COMMENT '归属玩家(资源点为 0)',
  `payload`        JSON            NOT NULL COMMENT '类型化快照',
  `deleted`        TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '墓碑标记',
  PRIMARY KEY (`world_id`, `object_id`),
  KEY `idx_scene` (`world_id`, `owner_scene`, `deleted`),
  KEY `idx_player` (`world_id`, `player_id`, `kind`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SLG 地图对象';
