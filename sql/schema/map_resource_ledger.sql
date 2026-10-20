-- sql/schema/map_resource_ledger.sql
-- 资源流水（设计文档 14.1 / 10.3）
--
-- 采集扣减必须有账：出现"资源总量对不上"时，只能靠流水逐条对。
-- UNIQUE(world,command_id,resource_id,kind) 保证同一命令的同一动作只记一次，
-- 重试不会写出重复账。
CREATE TABLE `map_resource_ledger` (
  `event_id`        CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '流水号',
  `world_id`        BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `resource_id`     BIGINT UNSIGNED NOT NULL COMMENT '资源点',
  `command_id`      VARCHAR(64)     NOT NULL COMMENT '命令编号',
  `kind`            TINYINT UNSIGNED NOT NULL COMMENT '1=CREATE 2=COLLECT 3=DESTROY',
  `qty`             BIGINT          NOT NULL COMMENT '数量(正数)',
  `remaining_after` BIGINT          NOT NULL COMMENT '操作后余量',
  `created_at`      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`event_id`),
  UNIQUE KEY `uk_command` (`world_id`, `command_id`, `resource_id`, `kind`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资源流水';
