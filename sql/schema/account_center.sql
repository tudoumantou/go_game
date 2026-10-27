-- 账号中心表结构（登录服，单一事实源）。
-- 物理分片不在本目录：acc_summary_0..31、user_role_0..14 由
-- scripts/db-init.sh 用「CREATE TABLE ... LIKE 模板」批量建出，
-- 不进 gendb 解析范围——与 login_log 按月分表的既有范式一致
-- （见 db-init.sh 的 login_log_YYYYMM LIKE login_log）。
--
-- 分片约定见 go_game_docs/架构设计/数据归属与分表边界.md（冻结版）：
--   acc_summary  按 accuid % 32 路由（accuid == account.id）
--   user_role    按 account_id % 15 路由（0..14，与 acc_summary 同款 0 索引）
-- 两张逻辑模板表只作分片的列定义来源与 gendb 的模型来源；
-- 运行时读写一律落到分片物理表，逻辑表本身不承载业务数据。

-- ============================================================================
-- serv_info.sql
-- ============================================================================
-- 服务器列表（账号中心，全局少量配置，Redis 全量预热）
-- 真源 MySQL，缓存 cache-aside；低频读，可整表预热进 Redis。
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

-- ============================================================================
-- acc_summary.sql（逻辑模板，物理分片 acc_summary_0..31 LIKE 本表）
-- ============================================================================
-- 账号摘要（登录服，按 accuid % 32 物理分片）
-- 真源 MySQL，Redis cache-aside。控制单表 B+ 树规模，避免账号表无限膨胀。
-- accuid 即 account.id（账号中心账号表主键），见数据归属文档 §4。
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

-- ============================================================================
-- user_role.sql（逻辑模板，物理分片 user_role_0..14 LIKE 本表）
-- ============================================================================
-- 角色索引（登录服，按 account_id % 15 物理分片）
-- 真源在游戏服（角色真源），本表是经 MQ 同步 + 回源兜底的轻量展示副本，
-- 用于选角界面列出某账号全部角色而无需回源游戏服。
-- 分片键 account_id % 15；同一账号的全部角色落在同一分片，选角只需查一张表。
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
