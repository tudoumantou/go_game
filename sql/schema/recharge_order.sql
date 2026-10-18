-- sql/schema/recharge_order.sql
-- 充值订单表 + 发货记录表（登录服，分表键：player_id % 512；Tier-1 幂等设计）
CREATE TABLE `recharge_order` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `order_id`    VARCHAR(64)  NOT NULL COMMENT '业务订单号(全局唯一,幂等键)',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `product_id`  INT          NOT NULL COMMENT '商品档位(见 recharge_product.xml)',
  `amount_fen`  INT          NOT NULL COMMENT '金额(分)',
  `channel`     TINYINT      NOT NULL COMMENT '支付渠道',
  `status`      TINYINT      NOT NULL DEFAULT 0 COMMENT '0待支付 1已支付 2已发货 3已关闭',
  `paid_at`     DATETIME    NULL DEFAULT NULL,
  `delivered_at` DATETIME   NULL DEFAULT NULL,
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_id` (`order_id`),          -- 幂等：重复回调直接冲突忽略
  KEY `idx_player_status` (`player_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='充值订单表';

CREATE TABLE `recharge_record` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `order_id`   VARCHAR(64)  NOT NULL COMMENT '业务订单号(幂等键)',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `product_id` INT  NOT NULL COMMENT '商品档位',
  `amount_fen` INT  NOT NULL COMMENT '金额(分)',
  `channel`    TINYINT NOT NULL COMMENT '支付渠道',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_id` (`order_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='充值发货记录表';
