-- 独立落库进程的版本栅栏，与快照内所有业务行在同一事务更新。
CREATE TABLE `player_writebehind` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `version` BIGINT NOT NULL DEFAULT 0,
  `applied_grants` JSON NULL COMMENT '已应用发奖业务键，与资产同事务落库',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家异步落库版本';
