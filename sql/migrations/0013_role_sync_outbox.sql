-- 角色信息同步 outbox（game → login）。
--
-- 背景：角色真源在游戏服库（player 表），选角界面在登录服（user_role 索引表）。
-- 创角成功后必须让登录服知道「这个账号在这个区服多了一个角色」，否则玩家
-- 在选角界面看不到刚建的角色。
--
-- 为什么用 outbox 而不是「事务提交后直接发 Kafka」：
--   提交成功但进程立刻被 kill / Broker 抖动，消息就永久丢了——玩家有角色、
--   选角列表里没有，且没有任何路径能自愈。把「写角色」和「写待投递事件」
--   放进同一个事务，投递失败就退化成「延迟」，而不是「丢失」。
--
-- 一个角色一行（uk_player）：本表语义是「该角色的索引尚未同步完成」，
-- 投递成功后置 PUBLISHED 即闲置；将来支持改名/升级同步时，把同一行
-- 重置为 PENDING 并更新冗余字段即可，不会无限追加。
CREATE TABLE IF NOT EXISTS `role_sync_outbox` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `player_id`     BIGINT UNSIGNED NOT NULL COMMENT '角色ID（全局唯一，幂等键）',
  `account_id`    BIGINT UNSIGNED NOT NULL COMMENT '账号ID（MQ 分区键，保证同账号有序）',
  `server_id`     INT             NOT NULL DEFAULT 0 COMMENT '区服ID',
  `nickname`      VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '角色昵称（冗余展示）',
  `level`         INT             NOT NULL DEFAULT 1 COMMENT '角色等级（冗余展示）',
  `status`        TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=PENDING 1=PUBLISHED 2=DEAD',
  `retry_count`   INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '已重试次数',
  `next_retry_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '下次可投递时间（unix 毫秒）',
  `created_at`    BIGINT          NOT NULL DEFAULT 0 COMMENT '创建时间（unix 毫秒）',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player` (`player_id`),
  KEY `idx_pending` (`status`, `next_retry_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色信息同步 outbox（game→login）';
