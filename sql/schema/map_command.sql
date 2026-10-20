-- sql/schema/map_command.sql
-- 命令回执（设计文档 7.1 / 14.1）
--
-- 幂等键 (world,actor_key,request_id)；actor_key 由 player_id 归一化而来，
-- 内部命令用 "sys:<kind>"，避免与玩家 request_id 撞域。
-- payload_hash 用于识别"同键不同参"：命中不同 hash 必须返回冲突，
-- 不能按新参数重新执行，否则等于绕过幂等。
CREATE TABLE `map_command` (
  `world_id`       BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `actor_key`      VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'actor(p:<id> / sys:<kind>)',
  `request_id`     VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '请求幂等键',
  `command_id`     CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '命令编号',
  `payload_hash`   BINARY(32)  NOT NULL COMMENT '参数摘要',
  `result_code`    INT         NOT NULL COMMENT '业务码',
  `result_json`    JSON        NOT NULL COMMENT '首次结果',
  `committed_at_ms` BIGINT     NOT NULL COMMENT '提交时刻',
  PRIMARY KEY (`world_id`, `actor_key`, `request_id`),
  UNIQUE KEY `uk_command` (`command_id`),
  KEY `idx_committed` (`world_id`, `committed_at_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='命令幂等回执';
