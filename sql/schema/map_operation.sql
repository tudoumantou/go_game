-- sql/schema/map_operation.sql
-- 长操作进度（设计文档 7.1 / 14.1）
--
-- 与命令回执分离：回执是"首次结果"，进度是"现在到哪一步"。
-- 用进度覆盖回执会让重试拿到与首次不同的结果（典型事故：
-- 首次失败、重试时已经成功，客户端看到两个矛盾答复）。
CREATE TABLE `map_operation` (
  `operation_id`      VARCHAR(64)     NOT NULL COMMENT '操作编号',
  `world_id`          BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `player_id`         BIGINT          NOT NULL COMMENT '玩家',
  `request_id`        VARCHAR(64)     NOT NULL COMMENT '发起命令的请求键',
  `kind`              TINYINT UNSIGNED NOT NULL COMMENT '1=MARCH 2=RECALL 3=SETTLEMENT',
  `status`            TINYINT UNSIGNED NOT NULL COMMENT '0=PENDING 1=DONE 2=FAILED',
  `result_json`       JSON            NULL COMMENT '最新进度',
  `related_object_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '关联对象',
  `updated_at_ms`     BIGINT          NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`operation_id`),
  UNIQUE KEY `uk_request` (`world_id`, `player_id`, `request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='长操作进度';
