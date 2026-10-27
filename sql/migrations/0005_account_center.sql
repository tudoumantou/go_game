-- 0005_account_center —— 账号中心三张表 + 物理分片
--
-- 逻辑模板表 serv_info / acc_summary / user_role 与 sql/schema/account_center.sql
-- 完全一致（gen-check 保证二者不漂移）；物理分片 acc_summary_0..31、
-- user_role_0..14 用 CREATE TABLE ... LIKE 模板批量建出。
--
-- 分片约定见 go_game_docs/架构设计/数据归属与分表边界.md：
--   acc_summary  按 accuid % 32 路由
--   user_role    按 account_id % 15 路由（0..14，与 acc_summary 同款 0 索引）

-- 服务器列表（全局少量配置，Redis 全量预热）
CREATE TABLE `serv_info` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '区服ID',
  `name`       VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '区服名',
  `ip`         VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '服务器IP',
  `port`       INT          NOT NULL DEFAULT 0  COMMENT '端口',
  `open_time`  DATETIME     NULL     DEFAULT NULL COMMENT '开服时间',
  `status`     TINYINT      NOT NULL DEFAULT 0  COMMENT '后台状态 0维护 1正常 2火爆 3停服',
  `recommend`  TINYINT      NOT NULL DEFAULT 0  COMMENT '推荐标记 0否 1是',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_recommend` (`recommend`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='服务器列表';

-- 账号摘要（按 accuid % 32 物理分片）
CREATE TABLE `acc_summary` (
  `accuid`        BIGINT UNSIGNED NOT NULL COMMENT '账号ID（== account.id）',
  `nickname`      VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '昵称',
  `avatar`        VARCHAR(255) NOT NULL DEFAULT '' COMMENT '头像URL',
  `level`         INT          NOT NULL DEFAULT 1  COMMENT '等级（展示用摘要）',
  `last_login_at` DATETIME     NULL     DEFAULT NULL COMMENT '最近登录时间',
  `created_at`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`accuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账号摘要（逻辑模板，分片 acc_summary_0..31）';

-- 角色索引（按 account_id % 15 物理分片）
CREATE TABLE `user_role` (
  `account_id` BIGINT UNSIGNED NOT NULL COMMENT '账号ID（分片键）',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '角色ID（全局唯一，游戏服真源）',
  `server_id`  INT            NOT NULL DEFAULT 0  COMMENT '角色所在区服ID',
  `nickname`   VARCHAR(64)    NOT NULL DEFAULT '' COMMENT '角色昵称（冗余展示，避免选角回源）',
  `level`      INT            NOT NULL DEFAULT 1  COMMENT '角色等级（冗余展示）',
  `created_at` TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`account_id`, `player_id`),
  UNIQUE KEY `uk_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色索引（逻辑模板，分片 user_role_0..14）';

-- 物理分片：acc_summary_0..31 LIKE acc_summary
CREATE TABLE IF NOT EXISTS `acc_summary_0` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_1` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_2` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_3` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_4` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_5` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_6` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_7` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_8` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_9` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_10` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_11` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_12` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_13` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_14` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_15` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_16` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_17` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_18` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_19` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_20` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_21` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_22` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_23` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_24` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_25` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_26` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_27` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_28` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_29` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_30` LIKE `acc_summary`;
CREATE TABLE IF NOT EXISTS `acc_summary_31` LIKE `acc_summary`;

-- 物理分片：user_role_0..14 LIKE user_role
CREATE TABLE IF NOT EXISTS `user_role_0` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_1` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_2` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_3` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_4` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_5` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_6` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_7` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_8` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_9` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_10` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_11` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_12` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_13` LIKE `user_role`;
CREATE TABLE IF NOT EXISTS `user_role_14` LIKE `user_role`;
