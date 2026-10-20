-- sql/schema/map_collect.sql
-- 采集会话：采集进度的唯一真源（设计文档 10.1 / 14.1）
--
-- 为什么不在 troop 上记进度：资源点与部队两端各存一份计数必然漂移，
-- 结算时无法判断谁对。会话归属于资源点一侧，部队只持有已结算 Cargo。
CREATE TABLE `map_collect` (
  `world_id`     BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `collect_id`   VARCHAR(64)     NOT NULL COMMENT '会话编号',
  `resource_id`  BIGINT UNSIGNED NOT NULL COMMENT '资源点',
  `troop_id`     BIGINT UNSIGNED NOT NULL COMMENT '采集部队',
  `start_at_ms`  BIGINT          NOT NULL COMMENT '开始时刻',
  `end_at_ms`    BIGINT          NOT NULL COMMENT '计划结束时刻',
  `planned_qty`  BIGINT          NOT NULL COMMENT '计划数量',
  `rate_milli`   BIGINT          NOT NULL COMMENT '速率(千分之一物品/秒)',
  `status`       TINYINT UNSIGNED NOT NULL COMMENT '1=ACTIVE 2=SETTLED',
  `settled_qty`  BIGINT          NOT NULL DEFAULT 0 COMMENT '实际结算数量',
  `rule_version` VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '规则版本',
  PRIMARY KEY (`world_id`, `collect_id`),
  KEY `idx_active` (`world_id`, `status`, `end_at_ms`),
  KEY `idx_resource` (`world_id`, `resource_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='采集会话';
