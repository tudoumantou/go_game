-- sql/schema/mail.sql
-- 邮件表 + 附件表（游戏服，分表键：player_id % 512；Kafka gs.mail 消费落库）
CREATE TABLE `mail` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '邮件ID',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `template_id` INT     NOT NULL DEFAULT 0 COMMENT '模板ID(见 mail_template.xml)',
  `title`       VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '标题',
  `content`     VARCHAR(1024) NOT NULL DEFAULT '' COMMENT '正文',
  `channel`     TINYINT NOT NULL DEFAULT 0 COMMENT '0系统 1GM 2活动',
  `status`      TINYINT NOT NULL DEFAULT 0 COMMENT '0未读 1已读 2附件已领',
  `biz_id`      VARCHAR(64) NULL DEFAULT NULL COMMENT '业务幂等键（充值订单号等），离线发放去重',
  `expire_at`   DATETIME NULL DEFAULT NULL COMMENT '过期时间',
  `created_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player_biz` (`player_id`, `biz_id`),
  KEY `idx_player_status` (`player_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='邮件表';

CREATE TABLE `mail_attachment` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '附件ID',
  `mail_id`    BIGINT UNSIGNED NOT NULL COMMENT '所属邮件',
  `item_id`    INT NOT NULL COMMENT '道具配置ID',
  `count`      INT NOT NULL DEFAULT 1 COMMENT '数量',
  `claimed`    TINYINT NOT NULL DEFAULT 0 COMMENT '0未领 1已领',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_mail` (`mail_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='邮件附件表';
