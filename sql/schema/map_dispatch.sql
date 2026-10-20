-- sql/schema/map_dispatch.sql
-- 出征许可（设计文档 13.1 / 14.1）
--
-- 许可由 game 服签发、map 服消费：玩家资产扣减发生在 game 侧，
-- 地图侧只认"这次出征是否被授权"。request_hash 绑定参数，
-- permit_hash 防止许可被改写后重放。
CREATE TABLE `map_dispatch` (
  `world_id`     BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `dispatch_id`  VARCHAR(64)     NOT NULL COMMENT '许可编号',
  `player_id`    BIGINT          NOT NULL COMMENT '玩家',
  `request_hash` BINARY(32)      NOT NULL COMMENT '请求摘要',
  `status`       TINYINT UNSIGNED NOT NULL COMMENT '1=ISSUED 2=CONSUMED 3=REVOKED',
  `troop_id`     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '消费后的部队',
  `permit_hash`  BINARY(32)      NOT NULL COMMENT '许可摘要',
  PRIMARY KEY (`world_id`, `dispatch_id`),
  KEY `idx_player` (`world_id`, `player_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='出征许可';
