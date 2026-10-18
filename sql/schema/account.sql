-- sql/schema/account.sql
-- 账号表（登录服，分表键：无 —— 账号量级小，登录服单库单表）
-- 依据：设计文档 12.3 表清单；密码哈希算法 P2 定稿（bcrypt/argon2），P0 占位
CREATE TABLE `account` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '账号ID',
  `account`       VARCHAR(64)  NOT NULL COMMENT '账号名(全局唯一)',
  `password_hash` CHAR(64)     NOT NULL COMMENT '密码哈希(P0占位,P2换bcrypt)',
  `salt`          CHAR(32)     NOT NULL COMMENT '密码盐',
  `status`        TINYINT      NOT NULL DEFAULT 0 COMMENT '0正常 1冻结 2注销',
  `device`        VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '注册设备',
  `created_at`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `last_login_at` TIMESTAMP    NULL DEFAULT NULL COMMENT '最近登录时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账号表';
