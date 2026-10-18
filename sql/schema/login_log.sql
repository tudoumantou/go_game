-- sql/schema/login_log.sql
-- 登录流水表（登录服，分表键：按月 —— login_log_202601 ...，由 db-init 建当前月表）
-- Kafka gs.login.log 削峰消费落库（12.3 落库通道选择表）
CREATE TABLE `login_log` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `account_id` BIGINT UNSIGNED NOT NULL COMMENT '账号ID',
  `player_id`  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '玩家ID(未创角为0)',
  `ip`         VARCHAR(45)  NOT NULL DEFAULT '' COMMENT '客户端IP(兼容IPv6)',
  `device`     VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '设备标识',
  `result`     TINYINT      NOT NULL DEFAULT 0 COMMENT '0成功 1失败',
  `reason`     VARCHAR(128) NOT NULL DEFAULT '' COMMENT '失败原因(成功为空)',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_account_created` (`account_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录流水表(按月分表)';
