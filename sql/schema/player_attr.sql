-- sql/schema/player_attr.sql
-- 玩家属性表（游戏服，分表键：player_id % 512）
-- 单行实体：一个玩家一行（Actor 全字段热数据，见 12.5 idtab/e: 双轨）
CREATE TABLE `player_attr` (
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `career`     TINYINT NOT NULL DEFAULT 0 COMMENT '职业(见 career.xml)',
  `hp`         INT     NOT NULL DEFAULT 0 COMMENT '当前生命',
  `mp`         INT     NOT NULL DEFAULT 0 COMMENT '当前法力',
  `atk`        INT     NOT NULL DEFAULT 0 COMMENT '攻击',
  `def`        INT     NOT NULL DEFAULT 0 COMMENT '防御',
  `spd`        INT     NOT NULL DEFAULT 0 COMMENT '速度',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家属性表';
