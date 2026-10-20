-- sql/schema/map_delivery.sql
-- 回城交接单（设计文档 9.5 / 13.2 / 14.1）
--
-- 铁律：先留单再清货。主堡事务里 Troop→SETTLING 并插入本单，Cargo 保留；
-- 玩家侧确认资产入包后才置 APPLIED 并清 Cargo。单号全局唯一，业务去重键
-- slg:delivery:{id} 让"确认报文丢失后重投"可以幂等。
CREATE TABLE `map_delivery` (
  `delivery_id`             CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '交接单号',
  `world_id`                BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `troop_id`                BIGINT UNSIGNED NOT NULL COMMENT '来源部队',
  `player_id`               BIGINT          NOT NULL COMMENT '玩家',
  `payload_hash`            BINARY(32)      NOT NULL COMMENT '货物摘要',
  `cargo`                   JSON            NOT NULL COMMENT '货物快照',
  `status`                  TINYINT UNSIGNED NOT NULL COMMENT '1=PENDING 2=APPLIED 3=QUARANTINED',
  `attempts`                INT             NOT NULL DEFAULT 0 COMMENT '投递尝试次数',
  `next_retry_ms`           BIGINT          NOT NULL DEFAULT 0 COMMENT '下次重试时刻',
  `settlement_no`           BIGINT          NOT NULL COMMENT '同部队回城序号',
  `applied_player_version`  BIGINT          NOT NULL DEFAULT 0 COMMENT '玩家侧确认版本',
  `created_at_ms`           BIGINT          NOT NULL COMMENT '创建时刻',
  PRIMARY KEY (`delivery_id`),
  UNIQUE KEY `uk_settlement` (`world_id`, `troop_id`, `settlement_no`),
  KEY `idx_retry` (`status`, `next_retry_ms`),
  KEY `idx_player` (`world_id`, `player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回城交接单';
