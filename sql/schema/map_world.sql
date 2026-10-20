-- sql/schema/map_world.sql
-- 世界主表：租约、epoch、拓扑/配置版本（设计文档 14.1）
--
-- 一个 world 一行。epoch 是世界代次：重新加载世界时 +1，所有携带旧 epoch
-- 的提交都会被拒绝（防止"上一代遗留事务"写进新世界）。lease_until_ms 是
-- 进程级租约，过期后其他进程可以接管（M2 单实例也保留，便于灰度）。
CREATE TABLE `map_world` (
  `world_id`         BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `owner_id`         VARCHAR(128)    NOT NULL DEFAULT '' COMMENT '当前持有进程标识',
  `epoch`            BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '世界代次，恢复时 +1',
  `lease_until_ms`   BIGINT          NOT NULL DEFAULT 0 COMMENT '租约到期(UTC ms)',
  `topology_version` INT UNSIGNED    NOT NULL COMMENT '拓扑版本，变更需停世界迁移',
  `config_version`   VARCHAR(64)     NOT NULL COMMENT '规则配置版本',
  `status`           TINYINT UNSIGNED NOT NULL COMMENT '1=READY 2=RECOVERING 3=NOT_READY',
  PRIMARY KEY (`world_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SLG 世界主表';
