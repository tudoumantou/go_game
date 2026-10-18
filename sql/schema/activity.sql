-- sql/schema/activity.sql
-- 活动记录表（游戏服，分表键：player_id % 512；Redis activity:state:{id} 管开闭状态）
CREATE TABLE `activity_record` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '记录ID',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `activity_id` INT   NOT NULL COMMENT '活动配置ID(见 activity.xml)',
  `progress`    INT   NOT NULL DEFAULT 0 COMMENT '当前进度',
  `reward_mask` BIGINT NOT NULL DEFAULT 0 COMMENT '已领奖励位图',
  `joined_at`   DATETIME NULL DEFAULT NULL COMMENT '首次参与时间',
  `created_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player_activity` (`player_id`, `activity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='活动记录表';
