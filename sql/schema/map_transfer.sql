-- sql/schema/map_transfer.sql
-- 跨场景所有权移交（设计文档 8 / 14.1）
--
-- 移交是两阶段：PREPARED（目标场景已备好，源场景还可回滚）→
-- COMMITTED（不可回滚）→ ACTIVATED（目标场景接管写入）。
-- 三态必须落库：进程崩溃时靠 status 判断该回滚还是该继续推进。
CREATE TABLE `map_transfer` (
  `world_id`        BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `transfer_id`     VARCHAR(64)     NOT NULL COMMENT '移交编号',
  `object_id`       BIGINT UNSIGNED NOT NULL COMMENT '被移交对象',
  `source_scene`    INT             NOT NULL COMMENT '源场景',
  `dest_scene`      INT             NOT NULL COMMENT '目标场景',
  `before_version`  BIGINT UNSIGNED NOT NULL COMMENT '移交前版本',
  `after_version`   BIGINT UNSIGNED NOT NULL COMMENT '移交后版本',
  `source_revision` BIGINT UNSIGNED NOT NULL COMMENT '源场景 revision',
  `dest_revision`   BIGINT UNSIGNED NOT NULL COMMENT '目标场景 revision',
  `status`          TINYINT UNSIGNED NOT NULL COMMENT '1=PREPARED 2=COMMITTED 3=ACTIVATED 4=RELEASED 5=ABORTED',
  `command_id`      VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '发起命令',
  PRIMARY KEY (`world_id`, `transfer_id`),
  KEY `idx_object` (`world_id`, `object_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨场景移交记录';
