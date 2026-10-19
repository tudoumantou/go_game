-- 本周期购买次数整体进玩家快照：扣费、发货与次数累加同一次 Redis CAS（R2.2）。
-- @flat:shop
CREATE TABLE `shop_record` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `bought`    TEXT NOT NULL COMMENT '购买次数 JSON：{"shop_id:goods_id":count}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色商店购买次数';
