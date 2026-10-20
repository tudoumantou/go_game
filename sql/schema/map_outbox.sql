-- sql/schema/map_outbox.sql
-- 投影事件 outbox（设计文档 7.3 / 14.1）
--
-- 为什么不用内存 channel：进程崩溃会丢事件，而投影落后可以通过
-- revision 缺口重建、事件丢失不能。outbox 与业务同事务写入，
-- 由后台 worker 拉取推送，失败只重试不回滚业务。
CREATE TABLE `map_outbox` (
  `event_id`      CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '事件编号',
  `world_id`      BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `scene_id`      INT             NOT NULL COMMENT '场景',
  `revision`      BIGINT UNSIGNED NOT NULL COMMENT '对应场景版本',
  `kind`          VARCHAR(32)     NOT NULL COMMENT '事件类型',
  `payload`       JSON            NOT NULL COMMENT '事件体',
  `status`        TINYINT UNSIGNED NOT NULL COMMENT '0=PENDING 1=PUBLISHED',
  `next_retry_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '下次重试',
  PRIMARY KEY (`event_id`),
  KEY `idx_pending` (`status`, `next_retry_ms`),
  KEY `idx_scene` (`world_id`, `scene_id`, `revision`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='投影事件 outbox';
