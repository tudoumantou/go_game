-- 登录礼包状态：上次领取的业务日与连续天数，整体进玩家快照（R3.2）。
-- @flat:login
CREATE TABLE `player_login` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `last_day`  BIGINT NOT NULL DEFAULT 0 COMMENT '上次领取礼包的业务日序号（北京时间）',
  `streak`    INT    NOT NULL DEFAULT 0 COMMENT '连续登录天数',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色登录礼包状态';
