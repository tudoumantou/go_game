-- sql/schema/shop_record.sql
-- 商店购买记录表（游戏服，分表键：player_id % 512；Redis shop:refresh:{pid} 管刷新）
CREATE TABLE `shop_record` (
  `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '记录ID',
  `player_id`    BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `shop_id`      INT  NOT NULL COMMENT '商店配置ID(见 shop.xml)',
  `goods_id`     INT  NOT NULL COMMENT '商品配置ID(见 shop_goods.xml)',
  `buy_count`    INT  NOT NULL DEFAULT 0 COMMENT '本周期已购次数',
  `refreshed_at` TIMESTAMP NULL DEFAULT NULL COMMENT '上次刷新时间',
  `created_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player_goods` (`player_id`, `shop_id`, `goods_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商店购买记录表';
