-- sql/schema/map_battle.sql
-- 战斗记录（设计文档 11.1 / 14.1）
--
-- input_hash 保证相同输入得到相同裁决；random_seed 保证可回放；
-- applied_operation_id 保证结果只应用一次（INV-06：迟到结果不能反转终态）。
CREATE TABLE `map_battle` (
  `world_id`              BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `battle_id`             VARCHAR(64)     NOT NULL COMMENT '战斗编号',
  `target_id`             BIGINT UNSIGNED NOT NULL COMMENT '目标对象',
  `attacker_id`           BIGINT UNSIGNED NOT NULL COMMENT '攻方部队',
  `defender_id`           BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '守方对象(资源点/部队)',
  `input_hash`            BINARY(32)      NOT NULL COMMENT '输入摘要',
  `attacker_stats`        JSON            NOT NULL COMMENT '攻方属性快照',
  `defender_stats`        JSON            NOT NULL COMMENT '守方属性快照',
  `rule_version`          VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '规则版本',
  `random_seed`           BIGINT          NOT NULL DEFAULT 0 COMMENT '裁决随机种子',
  `status`                TINYINT UNSIGNED NOT NULL COMMENT '1=PENDING 2=RESOLVED 3=TIMED_OUT',
  `deadline_ms`           BIGINT          NOT NULL COMMENT '超时时刻',
  `result`                JSON            NULL COMMENT '裁决结果',
  `applied_operation_id`  VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '已应用的操作(幂等)',
  PRIMARY KEY (`world_id`, `battle_id`),
  KEY `idx_pending` (`world_id`, `status`, `deadline_ms`),
  KEY `idx_target` (`world_id`, `target_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='战斗记录';
