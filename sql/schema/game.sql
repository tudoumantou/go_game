-- 全量表结构（单一事实源）。按表名排序，每张表前保留原文件说明与 @flat 标注。
-- 增量变更仍写 sql/migrations/00xx_*.sql；全量与迁移的一致性由 CI 重建校验。
--
-- 表目录：
--   account
--   activity_record
--   activity_grant_task
--   activity_instance
--   center_season
--   rank_history
--   feature_stats
--   chat_record
--   copy_record
--   friend
--   friend_apply
--   guild
--   guild_member
--   login_log
--   mail
--   mail_attachment
--   map_active_slot
--   map_battle
--   map_battle_queue
--   map_city_owner
--   map_collect
--   map_command
--   map_delivery
--   map_dispatch
--   map_id_allocator
--   map_object
--   map_occupancy
--   map_operation
--   map_outbox
--   map_refresh
--   map_resource_ledger
--   map_scene
--   map_transfer
--   map_world
--   player
--   player_attr
--   player_equip
--   player_item
--   player_login
--   player_map_dispatch
--   player_partner
--   player_quest
--   player_signin
--   player_skill
--   player_writebehind
--   rank_archive_task
--   recharge_order
--   recharge_record
--   shop_record
--   team
--   team_member

-- ============================================================================
-- account.sql
-- ============================================================================
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
  `last_login_at` DATETIME    NULL DEFAULT NULL COMMENT '最近登录时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_account` (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账号表';

-- ============================================================================
-- activity.sql
-- ============================================================================
-- 活动进度与领取位图整体进玩家快照，领奖与发放同一次 Redis CAS 提交（R2.2）。
-- @flat:activity
CREATE TABLE `activity_record` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `records`   TEXT NOT NULL COMMENT '活动状态 JSON：{activity_id:{progress,reward_mask}}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色活动状态';

-- ============================================================================
-- activity_grant_task.sql
-- ============================================================================
-- 里程碑发奖任务（R7.4）：达成时给每个贡献玩家建一条，逐条发邮件并标记完成。
-- 可恢复：status 落库，进程退出后从 pending 继续；mail_id 由 (instance, milestone, player)
-- 派生，重复执行不会重复发奖。
CREATE TABLE `activity_grant_task` (
  `id`          VARCHAR(96) NOT NULL COMMENT '{instance_id}:{milestone_id}:{player_id}',
  `instance_id` VARCHAR(64) NOT NULL,
  `milestone_id` INT NOT NULL,
  `player_id`   BIGINT UNSIGNED NOT NULL,
  `status`      TINYINT NOT NULL DEFAULT 0 COMMENT '0 待发 1 已完成',
  `updated_at`  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='里程碑发奖任务（断点续跑）';

-- ============================================================================
-- activity_instance.sql
-- ============================================================================
-- 活动实例（R7.1）：每个活动每个周期一行，由中心服 leader 写入并下发。
-- 周期 = 自固定基准起按 activity.xml 的持续天数切分，instance_id = {activity_id}-{period}。
-- version 取 start_at（单调递增）：游戏服按它丢弃乱序/重复的调度消息。
CREATE TABLE `activity_instance` (
  `instance_id` VARCHAR(64) NOT NULL COMMENT '{activity_id}-{period}',
  `activity_id` INT NOT NULL COMMENT '活动配置 ID',
  `period`      BIGINT NOT NULL COMMENT '周期序号',
  `start_at`    BIGINT NOT NULL COMMENT '开始（unix 秒，北京时间零点对齐）',
  `end_at`      BIGINT NOT NULL COMMENT '结束（unix 秒）',
  `version`     BIGINT NOT NULL COMMENT '版本（= start_at）',
  `updated_at`  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`instance_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='活动实例（中心服调度）';

-- ============================================================================
-- center_season.sql
-- ============================================================================
-- 中心服赛季状态（单行）：赛季名与起点。
-- 归档流程只有 leader 执行，赛季起点落库才能保证换主/重启后不重新计时。
CREATE TABLE `center_season` (
  `id`         TINYINT NOT NULL DEFAULT 1 COMMENT '固定 1（单行表）',
  `season`     VARCHAR(32) NOT NULL COMMENT '当前赛季，如 s1',
  `started_at` BIGINT NOT NULL COMMENT '赛季开始时间（unix 秒）',
  `updated_at` BIGINT NOT NULL DEFAULT 0 COMMENT '最近更新（unix 秒）',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='中心服赛季状态';

-- ============================================================================
-- center_stats.sql
-- ============================================================================
-- sql/schema/center_stats.sql
-- 中心服统计表（分表键：group_id % 10）：排行榜历史快照 + 分组功能统计
CREATE TABLE `rank_history` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `group_id`    INT   NOT NULL COMMENT '分组ID',
  `rank_type`   TINYINT NOT NULL COMMENT '排行榜类型(见 rank.xml)',
  `season`      INT   NOT NULL COMMENT '赛季',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `score`       BIGINT NOT NULL DEFAULT 0 COMMENT '分数',
  `rank_no`     INT   NOT NULL DEFAULT 0 COMMENT '名次',
  `snapshot_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '快照时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_type_season_player` (`group_id`, `rank_type`, `season`, `player_id`),
  KEY `idx_rank` (`group_id`, `rank_type`, `season`, `rank_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='排行榜历史快照表';

CREATE TABLE `feature_stats` (
  `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `group_id`     INT   NOT NULL COMMENT '分组ID',
  `feature`      VARCHAR(64) NOT NULL COMMENT '功能标识(如 online/dau/retain)',
  `window_start` TIMESTAMP NOT NULL COMMENT '统计窗口起',
  `window_end`   TIMESTAMP NOT NULL COMMENT '统计窗口止',
  `value`        BIGINT NOT NULL DEFAULT 0 COMMENT '统计值',
  `updated_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_feature_window` (`group_id`, `feature`, `window_start`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分组功能统计表';

-- ============================================================================
-- chat_record.sql
-- ============================================================================
-- sql/schema/chat_record.sql
-- 聊天记录表（游戏服，分表键：player_id % 512；双端各存一份，见 17.3）
CREATE TABLE `chat_record` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `owner_id`   BIGINT UNSIGNED NOT NULL COMMENT '归属玩家(历史查询方)',
  `peer_id`    BIGINT UNSIGNED NOT NULL COMMENT '对方玩家',
  `msg_id`     BIGINT UNSIGNED NOT NULL COMMENT '全局消息ID(幂等键)',
  `direction`  TINYINT      NOT NULL COMMENT '0 发出 1 收到',
  `channel`    TINYINT      NOT NULL DEFAULT 1 COMMENT '1 私聊',
  `content`    VARBINARY(1024) NOT NULL COMMENT '消息内容(业务序列化)',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_owner_msg` (`owner_id`, `msg_id`),   -- 幂等：双端落库不重复
  KEY `idx_owner_peer` (`owner_id`, `peer_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='聊天记录表(每服从本服玩家视角各存一份)';

-- ============================================================================
-- copy_record.sql
-- ============================================================================
-- 副本进度整体进玩家快照：结算、首通奖励与经验同一次 Redis CAS 提交（R2.2）。
-- @flat:copy
CREATE TABLE `copy_record` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `records`   TEXT NOT NULL COMMENT '副本状态 JSON：{copy_id:{star,pass_count,first_pass}}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色副本进度';

-- ============================================================================
-- friend.sql
-- ============================================================================
-- sql/schema/friend.sql
-- 好友表 + 申请表（游戏服，分表键：player_id % 512；Redis idx:friend:{pid}）
CREATE TABLE `friend` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '关系ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `friend_id`  BIGINT UNSIGNED NOT NULL COMMENT '好友玩家ID',
  `intimacy`   INT NOT NULL DEFAULT 0 COMMENT '亲密度',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pair` (`player_id`, `friend_id`),
  KEY `idx_friend` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友表';

CREATE TABLE `friend_apply` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '申请ID',
  `from_id`    BIGINT UNSIGNED NOT NULL COMMENT '申请方玩家ID',
  `to_id`      BIGINT UNSIGNED NOT NULL COMMENT '接收方玩家ID',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0待处理 1已同意 2已拒绝',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_to_status` (`to_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友申请表';

-- ============================================================================
-- guild.sql
-- ============================================================================
-- sql/schema/guild.sql
-- 公会表 + 成员表（游戏服，分表键：guild_id % 64；低频聚合数据，直连 Model CRUD）
CREATE TABLE `guild` (
  `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '公会ID',
  `name`         VARCHAR(64)  NOT NULL COMMENT '公会名(全局唯一)',
  `leader_id`    BIGINT UNSIGNED NOT NULL COMMENT '会长玩家ID',
  `level`        TINYINT NOT NULL DEFAULT 1 COMMENT '公会等级(见 guild_level.xml)',
  `exp`          INT     NOT NULL DEFAULT 0 COMMENT '公会经验',
  `notice`       VARCHAR(255) NOT NULL DEFAULT '' COMMENT '公会公告',
  `member_count` INT     NOT NULL DEFAULT 1 COMMENT '成员数(冗余)',
  `created_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公会表';

CREATE TABLE `guild_member` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '成员记录ID',
  `guild_id`    BIGINT UNSIGNED NOT NULL COMMENT '公会ID',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `title`       TINYINT NOT NULL DEFAULT 0 COMMENT '0成员 1会长 2官员',
  `contributed` INT     NOT NULL DEFAULT 0 COMMENT '累计贡献',
  `joined_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_guild_player` (`guild_id`, `player_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公会成员表';

-- ============================================================================
-- login_log.sql
-- ============================================================================
-- sql/schema/login_log.sql
-- 登录流水表（登录服，分表键：按月 —— login_log_202601 ...，由 db-init 建当前月表）
-- Kafka gs.login.log 削峰消费落库（12.3 落库通道选择表）
CREATE TABLE `login_log` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `account_id` BIGINT UNSIGNED NOT NULL COMMENT '账号ID',
  `player_id`  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '玩家ID(未创角为0)',
  `ip`         VARCHAR(45)  NOT NULL DEFAULT '' COMMENT '客户端IP(兼容IPv6)',
  `device`     VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '设备标识',
  `result`     TINYINT      NOT NULL DEFAULT 0 COMMENT '0成功 1失败',
  `reason`     VARCHAR(128) NOT NULL DEFAULT '' COMMENT '失败原因(成功为空)',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_account_created` (`account_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录流水表(按月分表)';

-- ============================================================================
-- mail.sql
-- ============================================================================
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

-- ============================================================================
-- map_active_slot.sql
-- ============================================================================
-- sql/schema/map_active_slot.sql
-- 出征槽位占用（INV-05）：一个槽位同时只能有一支部队（设计文档 14.1）
--
-- 部队 CLOSED 才删除本行；SETTLING 仍然占用（货还没交接完，
-- 允许复用槽位会让玩家用"未结算部队"绕过并发上限）。
CREATE TABLE `map_active_slot` (
  `world_id`  BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `player_id` BIGINT          NOT NULL COMMENT '玩家编号',
  `slot_id`   INT             NOT NULL COMMENT '出征槽位',
  `troop_id`  BIGINT UNSIGNED NOT NULL COMMENT '占用槽位的部队',
  PRIMARY KEY (`world_id`, `player_id`, `slot_id`),
  UNIQUE KEY `uk_troop` (`world_id`, `troop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='出征槽位占用';

-- ============================================================================
-- map_battle.sql
-- ============================================================================
-- sql/schema/map_battle.sql
-- 战斗记录（设计文档 11.1 / 14.1）
--
-- input_hash 保证相同输入得到相同裁决；random_seed 保证可回放；
-- applied_operation_id 保证结果只应用一次（INV-06：迟到结果不能反转终态）。
CREATE TABLE `map_battle` (
  `world_id`              BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `battle_id`             VARCHAR(64)     NOT NULL COMMENT '战斗编号',
  `target_id`             BIGINT UNSIGNED NOT NULL COMMENT '目标对象',
  `attacker_id`           BIGINT UNSIGNED NOT NULL COMMENT '攻方部队',
  `defender_id`           BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '守方对象(资源点/部队)',
  `input_hash`            BINARY(32)      NOT NULL COMMENT '输入摘要',
  `attacker_stats`        JSON            NOT NULL COMMENT '攻方属性快照',
  `defender_stats`        JSON            NOT NULL COMMENT '守方属性快照',
  `rule_version`          VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '规则版本',
  `random_seed`           BIGINT          NOT NULL DEFAULT 0 COMMENT '裁决随机种子',
  `status`                TINYINT UNSIGNED NOT NULL COMMENT '1=PENDING 2=RESOLVED 3=TIMED_OUT',
  `deadline_ms`           BIGINT          NOT NULL COMMENT '超时时刻',
  `result`                JSON            NULL COMMENT '裁决结果',
  `applied_operation_id`  VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '已应用的操作(幂等)',
  PRIMARY KEY (`world_id`, `battle_id`),
  KEY `idx_pending` (`world_id`, `status`, `deadline_ms`),
  KEY `idx_target` (`world_id`, `target_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='战斗记录';

-- ============================================================================
-- map_battle_queue.sql
-- ============================================================================
-- sql/schema/map_battle_queue.sql
-- 目标战斗队列（设计文档 11.1 / 14.1）
--
-- 用独立表而不是"战斗记录加标记"：队列需要 enqueue_seq 保证 FIFO，
-- 而战斗记录按 battle_id 排序没有语义。UNIQUE(world,target,troop)
-- 防止同一支部队重复入队（客户端重试会打进来）。
CREATE TABLE `map_battle_queue` (
  `world_id`    BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `target_id`   BIGINT UNSIGNED NOT NULL COMMENT '目标对象',
  `enqueue_seq` BIGINT          NOT NULL COMMENT '入队序号(单调递增)',
  `troop_id`    BIGINT UNSIGNED NOT NULL COMMENT '排队部队',
  PRIMARY KEY (`world_id`, `target_id`, `enqueue_seq`),
  UNIQUE KEY `uk_target_troop` (`world_id`, `target_id`, `troop_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='目标战斗队列';

-- ============================================================================
-- map_city_owner.sql
-- ============================================================================
-- sql/schema/map_city_owner.sql
-- 玩家 → 主堡唯一映射（设计文档 14.1）
--
-- 两个唯一键缺一不可：world+player 防止一个玩家两座主堡；
-- world+city 防止两座城指向同一玩家（并发创建时总有一个事务失败）。
CREATE TABLE `map_city_owner` (
  `world_id`  BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `player_id` BIGINT          NOT NULL COMMENT '玩家编号',
  `city_id`   BIGINT UNSIGNED NOT NULL COMMENT '主堡对象编号',
  PRIMARY KEY (`world_id`, `player_id`),
  UNIQUE KEY `uk_city` (`world_id`, `city_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家主堡唯一映射';

-- ============================================================================
-- map_collect.sql
-- ============================================================================
-- sql/schema/map_collect.sql
-- 采集会话：采集进度的唯一真源（设计文档 10.1 / 14.1）
--
-- 为什么不在 troop 上记进度：资源点与部队两端各存一份计数必然漂移，
-- 结算时无法判断谁对。会话归属于资源点一侧，部队只持有已结算 Cargo。
CREATE TABLE `map_collect` (
  `world_id`     BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `collect_id`   VARCHAR(64)     NOT NULL COMMENT '会话编号',
  `resource_id`  BIGINT UNSIGNED NOT NULL COMMENT '资源点',
  `troop_id`     BIGINT UNSIGNED NOT NULL COMMENT '采集部队',
  `start_at_ms`  BIGINT          NOT NULL COMMENT '开始时刻',
  `end_at_ms`    BIGINT          NOT NULL COMMENT '计划结束时刻',
  `planned_qty`  BIGINT          NOT NULL COMMENT '计划数量',
  `rate_milli`   BIGINT          NOT NULL COMMENT '速率(千分之一物品/秒)',
  `status`       TINYINT UNSIGNED NOT NULL COMMENT '1=ACTIVE 2=SETTLED',
  `settled_qty`  BIGINT          NOT NULL DEFAULT 0 COMMENT '实际结算数量',
  `rule_version` VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '规则版本',
  PRIMARY KEY (`world_id`, `collect_id`),
  KEY `idx_active` (`world_id`, `status`, `end_at_ms`),
  KEY `idx_resource` (`world_id`, `resource_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='采集会话';

-- ============================================================================
-- map_command.sql
-- ============================================================================
-- sql/schema/map_command.sql
-- 命令回执（设计文档 7.1 / 14.1）
--
-- 幂等键 (world,actor_key,request_id)；actor_key 由 player_id 归一化而来，
-- 内部命令用 "sys:<kind>"，避免与玩家 request_id 撞域。
-- payload_hash 用于识别"同键不同参"：命中不同 hash 必须返回冲突，
-- 不能按新参数重新执行，否则等于绕过幂等。
CREATE TABLE `map_command` (
  `world_id`       BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `actor_key`      VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'actor(p:<id> / sys:<kind>)',
  `request_id`     VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '请求幂等键',
  `command_id`     CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '命令编号',
  `payload_hash`   BINARY(32)  NOT NULL COMMENT '参数摘要',
  `result_code`    INT         NOT NULL COMMENT '业务码',
  `result_json`    JSON        NOT NULL COMMENT '首次结果',
  `committed_at_ms` BIGINT     NOT NULL COMMENT '提交时刻',
  PRIMARY KEY (`world_id`, `actor_key`, `request_id`),
  UNIQUE KEY `uk_command` (`command_id`),
  KEY `idx_committed` (`world_id`, `committed_at_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='命令幂等回执';

-- ============================================================================
-- map_delivery.sql
-- ============================================================================
-- sql/schema/map_delivery.sql
-- 回城交接单（设计文档 9.5 / 13.2 / 14.1）
--
-- 铁律：先留单再清货。主堡事务里 Troop→SETTLING 并插入本单，Cargo 保留；
-- 玩家侧确认资产入包后才置 APPLIED 并清 Cargo。单号全局唯一，业务去重键
-- slg:delivery:{id} 让"确认报文丢失后重投"可以幂等。
CREATE TABLE `map_delivery` (
  `delivery_id`             CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '交接单号',
  `world_id`                BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `troop_id`                BIGINT UNSIGNED NOT NULL COMMENT '来源部队',
  `player_id`               BIGINT          NOT NULL COMMENT '玩家',
  `payload_hash`            BINARY(32)      NOT NULL COMMENT '货物摘要',
  `cargo`                   JSON            NOT NULL COMMENT '货物快照',
  `status`                  TINYINT UNSIGNED NOT NULL COMMENT '1=PENDING 2=APPLIED 3=QUARANTINED',
  `attempts`                INT             NOT NULL DEFAULT 0 COMMENT '投递尝试次数',
  `next_retry_ms`           BIGINT          NOT NULL DEFAULT 0 COMMENT '下次重试时刻',
  `settlement_no`           BIGINT          NOT NULL COMMENT '同部队回城序号',
  `applied_player_version`  BIGINT          NOT NULL DEFAULT 0 COMMENT '玩家侧确认版本',
  `created_at_ms`           BIGINT          NOT NULL COMMENT '创建时刻',
  PRIMARY KEY (`delivery_id`),
  UNIQUE KEY `uk_settlement` (`world_id`, `troop_id`, `settlement_no`),
  KEY `idx_retry` (`status`, `next_retry_ms`),
  KEY `idx_player` (`world_id`, `player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回城交接单';

-- ============================================================================
-- map_dispatch.sql
-- ============================================================================
-- sql/schema/map_dispatch.sql
-- 出征许可（设计文档 13.1 / 14.1）
--
-- 许可由 game 服签发、map 服消费：玩家资产扣减发生在 game 侧，
-- 地图侧只认"这次出征是否被授权"。request_hash 绑定参数，
-- permit_hash 防止许可被改写后重放。
CREATE TABLE `map_dispatch` (
  `world_id`     BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `dispatch_id`  VARCHAR(64)     NOT NULL COMMENT '许可编号',
  `player_id`    BIGINT          NOT NULL COMMENT '玩家',
  `request_hash` BINARY(32)      NOT NULL COMMENT '请求摘要',
  `status`       TINYINT UNSIGNED NOT NULL COMMENT '1=ISSUED 2=CONSUMED 3=REVOKED',
  `troop_id`     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '消费后的部队',
  `permit_hash`  BINARY(32)      NOT NULL COMMENT '许可摘要',
  PRIMARY KEY (`world_id`, `dispatch_id`),
  KEY `idx_player` (`world_id`, `player_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='出征许可';

-- ============================================================================
-- map_id_allocator.sql
-- ============================================================================
-- sql/schema/map_id_allocator.sql
-- 对象 ID 分配器（设计文档 14.2 末尾）
--
-- 为什么必须走 SQL 自增：对象 ID 要跨进程、跨重启唯一。time.Now().UnixNano()
-- 在多实例下会碰撞，进程内计数器重启后重复。允许空洞：ID 分配成功但事务
-- 回滚是正常现象，"ID 存在"不等于"对象存在"。
CREATE TABLE `map_id_allocator` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '分配出去的对象 ID',
  `allocated_at` TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对象 ID 分配器';

-- ============================================================================
-- map_object.sql
-- ============================================================================
-- sql/schema/map_object.sql
-- 地图对象：城池/部队/资源点统一存本表，payload 按 kind 分 schema（14.1）
--
-- 第一版用类型化 JSON 快照（city_payload/troop_payload/resource_payload），
-- 不允许 map[string]any 混写：混写会让"字段是否必填"无法在编译期校验，
-- 也会让 SQL 侧的 JSON 查询无法建立稳定索引。
-- 墓碑用 deleted=1 表示，不物理删除：历史回执与审计需要能解释"这里曾经有什么"。
CREATE TABLE `map_object` (
  `world_id`       BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `object_id`      BIGINT UNSIGNED NOT NULL COMMENT '对象编号(SQL 分配)',
  `kind`           TINYINT UNSIGNED NOT NULL COMMENT '1=CITY 2=TROOP 3=RESOURCE',
  `owner_scene`    INT             NOT NULL COMMENT '当前唯一写入场景',
  `version`        BIGINT UNSIGNED NOT NULL COMMENT '对象版本，乐观并发',
  `schema_version` INT UNSIGNED    NOT NULL COMMENT 'payload 结构版本',
  `player_id`      BIGINT          NOT NULL DEFAULT 0 COMMENT '归属玩家(资源点为 0)',
  `payload`        JSON            NOT NULL COMMENT '类型化快照',
  `deleted`        TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '墓碑标记',
  PRIMARY KEY (`world_id`, `object_id`),
  KEY `idx_scene` (`world_id`, `owner_scene`, `deleted`),
  KEY `idx_player` (`world_id`, `player_id`, `kind`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SLG 地图对象';

-- ============================================================================
-- map_occupancy.sql
-- ============================================================================
-- sql/schema/map_occupancy.sql
-- 静态格占位（设计文档 5.3 / 14.1）
--
-- 唯一键 (world,cell_x,cell_y,layer) 是占位的**最终约束**：内存预检只是加速，
-- 两个并发创建同时在内存里判定"空格"时，只有一个事务能在这里插成功，
-- 另一个拿 1062 后返回 PositionOccupied。
CREATE TABLE `map_occupancy` (
  `world_id`  BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `cell_x`    BIGINT          NOT NULL COMMENT '格 X',
  `cell_y`    BIGINT          NOT NULL COMMENT '格 Y',
  `layer`     INT             NOT NULL DEFAULT 0 COMMENT '占位层(第一版只有 0)',
  `object_id` BIGINT UNSIGNED NOT NULL COMMENT '占用对象',
  PRIMARY KEY (`world_id`, `cell_x`, `cell_y`, `layer`),
  KEY `idx_object` (`world_id`, `object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='静态格占位';

-- ============================================================================
-- map_operation.sql
-- ============================================================================
-- sql/schema/map_operation.sql
-- 长操作进度（设计文档 7.1 / 14.1）
--
-- 与命令回执分离：回执是"首次结果"，进度是"现在到哪一步"。
-- 用进度覆盖回执会让重试拿到与首次不同的结果（典型事故：
-- 首次失败、重试时已经成功，客户端看到两个矛盾答复）。
CREATE TABLE `map_operation` (
  `operation_id`      VARCHAR(64)     NOT NULL COMMENT '操作编号',
  `world_id`          BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `player_id`         BIGINT          NOT NULL COMMENT '玩家',
  `request_id`        VARCHAR(64)     NOT NULL COMMENT '发起命令的请求键',
  `kind`              TINYINT UNSIGNED NOT NULL COMMENT '1=MARCH 2=RECALL 3=SETTLEMENT',
  `status`            TINYINT UNSIGNED NOT NULL COMMENT '0=PENDING 1=DONE 2=FAILED',
  `result_json`       JSON            NULL COMMENT '最新进度',
  `related_object_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '关联对象',
  `updated_at_ms`     BIGINT          NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`operation_id`),
  UNIQUE KEY `uk_request` (`world_id`, `player_id`, `request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='长操作进度';

-- ============================================================================
-- map_outbox.sql
-- ============================================================================
-- sql/schema/map_outbox.sql
-- 投影事件 outbox（设计文档 7.3 / 14.1）
--
-- 为什么不用内存 channel：进程崩溃会丢事件，而投影落后可以通过
-- revision 缺口重建、事件丢失不能。outbox 与业务同事务写入，
-- 由后台 worker 拉取推送，失败只重试不回滚业务。
CREATE TABLE `map_outbox` (
  `event_id`      CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '事件编号',
  `world_id`      BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `scene_id`      INT             NOT NULL COMMENT '场景',
  `revision`      BIGINT UNSIGNED NOT NULL COMMENT '对应场景版本',
  `kind`          VARCHAR(32)     NOT NULL COMMENT '事件类型',
  `payload`       JSON            NOT NULL COMMENT '事件体',
  `status`        TINYINT UNSIGNED NOT NULL COMMENT '0=PENDING 1=PUBLISHED',
  `next_retry_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '下次重试',
  PRIMARY KEY (`event_id`),
  KEY `idx_pending` (`status`, `next_retry_ms`),
  KEY `idx_scene` (`world_id`, `scene_id`, `revision`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='投影事件 outbox';

-- ============================================================================
-- map_refresh.sql
-- ============================================================================
-- sql/schema/map_refresh.sql
-- 富饶块刷新进度（设计文档 10.5 / 14.1）
--
-- (block,generation) 一行，cursor 记录已处理的 slot，finished 标记完成。
-- 重启后从 cursor 继续：已创建的资源点由稳定键 (block,gen,slot) 去重，
-- 因此重复处理不会多刷。
CREATE TABLE `map_refresh` (
  `world_id`       BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `rich_block_id`  BIGINT          NOT NULL COMMENT '富饶块',
  `generation`     BIGINT          NOT NULL COMMENT '刷新代次',
  `config_version` VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '规则版本',
  `seed`           BIGINT          NOT NULL DEFAULT 0 COMMENT '随机种子(可回放)',
  `cursor`         INT             NOT NULL DEFAULT 0 COMMENT '已处理 slot',
  `finished`       TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否完成',
  `version`        BIGINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '乐观并发',
  PRIMARY KEY (`world_id`, `rich_block_id`, `generation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='富饶块刷新进度';

-- ============================================================================
-- map_resource_ledger.sql
-- ============================================================================
-- sql/schema/map_resource_ledger.sql
-- 资源流水（设计文档 14.1 / 10.3）
--
-- 采集扣减必须有账：出现"资源总量对不上"时，只能靠流水逐条对。
-- UNIQUE(world,command_id,resource_id,kind) 保证同一命令的同一动作只记一次，
-- 重试不会写出重复账。
CREATE TABLE `map_resource_ledger` (
  `event_id`        CHAR(36)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '流水号',
  `world_id`        BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `resource_id`     BIGINT UNSIGNED NOT NULL COMMENT '资源点',
  `command_id`      VARCHAR(64)     NOT NULL COMMENT '命令编号',
  `kind`            TINYINT UNSIGNED NOT NULL COMMENT '1=CREATE 2=COLLECT 3=DESTROY',
  `qty`             BIGINT          NOT NULL COMMENT '数量(正数)',
  `remaining_after` BIGINT          NOT NULL COMMENT '操作后余量',
  `created_at`      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`event_id`),
  UNIQUE KEY `uk_command` (`world_id`, `command_id`, `resource_id`, `kind`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资源流水';

-- ============================================================================
-- map_scene.sql
-- ============================================================================
-- sql/schema/map_scene.sql
-- 场景行：revision 是场景级乐观锁（设计文档 14.1）
--
-- 每个事务都必须带 expected_revision 并对本行 FOR UPDATE。revision 只增不减，
-- 是"这个场景的状态被改过几次"的单调计数器，也是投影缺口检测的依据。
CREATE TABLE `map_scene` (
  `world_id`       BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `scene_id`       INT             NOT NULL COMMENT '场景编号(1..cols*rows)',
  `revision`       BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已提交版本，事务校验并 +1',
  `schema_version` INT UNSIGNED    NOT NULL DEFAULT 1 COMMENT '场景快照结构版本',
  PRIMARY KEY (`world_id`, `scene_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SLG 场景版本行';

-- ============================================================================
-- map_transfer.sql
-- ============================================================================
-- sql/schema/map_transfer.sql
-- 跨场景所有权移交（设计文档 8 / 14.1）
--
-- 移交是两阶段：PREPARED（目标场景已备好，源场景还可回滚）→
-- COMMITTED（不可回滚）→ ACTIVATED（目标场景接管写入）。
-- 三态必须落库：进程崩溃时靠 status 判断该回滚还是该继续推进。
CREATE TABLE `map_transfer` (
  `world_id`        BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `transfer_id`     VARCHAR(64)     NOT NULL COMMENT '移交编号',
  `object_id`       BIGINT UNSIGNED NOT NULL COMMENT '被移交对象',
  `source_scene`    INT             NOT NULL COMMENT '源场景',
  `dest_scene`      INT             NOT NULL COMMENT '目标场景',
  `before_version`  BIGINT UNSIGNED NOT NULL COMMENT '移交前版本',
  `after_version`   BIGINT UNSIGNED NOT NULL COMMENT '移交后版本',
  `source_revision` BIGINT UNSIGNED NOT NULL COMMENT '源场景 revision',
  `dest_revision`   BIGINT UNSIGNED NOT NULL COMMENT '目标场景 revision',
  `status`          TINYINT UNSIGNED NOT NULL COMMENT '1=PREPARED 2=COMMITTED 3=ACTIVATED 4=RELEASED 5=ABORTED',
  `command_id`      VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '发起命令',
  PRIMARY KEY (`world_id`, `transfer_id`),
  KEY `idx_object` (`world_id`, `object_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨场景移交记录';

-- ============================================================================
-- map_world.sql
-- ============================================================================
-- sql/schema/map_world.sql
-- 世界主表：租约、epoch、拓扑/配置版本（设计文档 14.1）
--
-- 一个 world 一行。epoch 是世界代次：重新加载世界时 +1，所有携带旧 epoch
-- 的提交都会被拒绝（防止"上一代遗留事务"写进新世界）。lease_until_ms 是
-- 进程级租约，过期后其他进程可以接管（M2 单实例也保留，便于灰度）。
CREATE TABLE `map_world` (
  `world_id`         BIGINT UNSIGNED NOT NULL COMMENT '世界编号',
  `owner_id`         VARCHAR(128)    NOT NULL DEFAULT '' COMMENT '当前持有进程标识',
  `epoch`            BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '世界代次，恢复时 +1',
  `lease_until_ms`   BIGINT          NOT NULL DEFAULT 0 COMMENT '租约到期(UTC ms)',
  `topology_version` INT UNSIGNED    NOT NULL COMMENT '拓扑版本，变更需停世界迁移',
  `config_version`   VARCHAR(64)     NOT NULL COMMENT '规则配置版本',
  `status`           TINYINT UNSIGNED NOT NULL COMMENT '1=READY 2=RECOVERING 3=NOT_READY',
  PRIMARY KEY (`world_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SLG 世界主表';

-- ============================================================================
-- player.sql
-- ============================================================================
-- sql/schema/player.sql
-- 玩家表（游戏服，分表键：id % 512，依据 12.3 表清单）
CREATE TABLE `player` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '玩家ID',
  `account_id` BIGINT UNSIGNED NOT NULL COMMENT '账号ID',
  `name`       VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '昵称',
  `level`      INT          NOT NULL DEFAULT 1  COMMENT '等级',
  `exp`        BIGINT       NOT NULL DEFAULT 0  COMMENT '经验',
  `coin`       BIGINT       NOT NULL DEFAULT 0  COMMENT '金币',
  `scene_id`   INT          NOT NULL DEFAULT 0  COMMENT '所在场景',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`),            -- 角色名全局唯一（重名校验兜底）
  KEY `idx_account_id` (`account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家表';

-- ============================================================================
-- player_attr.sql
-- ============================================================================
-- sql/schema/player_attr.sql
-- 玩家属性表（游戏服，分表键：player_id % 512）
-- 单行实体：一个玩家一行（Actor 全字段热数据，见 12.5 idtab/e: 双轨）
CREATE TABLE `player_attr` (
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `career`     TINYINT NOT NULL DEFAULT 0 COMMENT '职业(见 career.xml)',
  `hp`         INT     NOT NULL DEFAULT 0 COMMENT '当前生命',
  `mp`         INT     NOT NULL DEFAULT 0 COMMENT '当前法力',
  `atk`        INT     NOT NULL DEFAULT 0 COMMENT '攻击',
  `def`        INT     NOT NULL DEFAULT 0 COMMENT '防御',
  `spd`        INT     NOT NULL DEFAULT 0 COMMENT '速度',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家属性表';

-- ============================================================================
-- player_equip.sql
-- ============================================================================
-- 装备实例整体进玩家快照：穿戴/卸下与属性加成同一次 Redis CAS 提交（R2.2）。
-- @flat:equip
CREATE TABLE `player_equip` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `equips`    TEXT NOT NULL COMMENT '装备实例 JSON 数组：[{uid,equip_id,enhance_level,quality,equipped}]',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色装备实例';

-- ============================================================================
-- player_item.sql
-- ============================================================================
-- sql/schema/player_item.sql
-- 玩家道具表（游戏服，分表键：player_id % 512；属主索引 idx:player_item:{pid}）
CREATE TABLE `player_item` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '道具实例ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `item_id`    INT            NOT NULL COMMENT '道具配置ID(见 item.xml)',
  `count`      INT            NOT NULL DEFAULT 1 COMMENT '堆叠数量',
  `expire_at`  DATETIME      NULL DEFAULT NULL COMMENT '过期时间( null=永久 )',
  `created_at` TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  -- 堆叠语义：同一玩家同一道具只有一行。没有这个唯一键，多行 UPSERT 无法生效
  -- （ON DUPLICATE KEY 只对主键/唯一键生效，会退化成插入重复行）
  UNIQUE KEY `uk_player_item` (`player_id`, `item_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家道具表';

-- ============================================================================
-- player_login.sql
-- ============================================================================
-- 登录礼包状态：上次领取的业务日与连续天数，整体进玩家快照（R3.2）。
-- @flat:login
CREATE TABLE `player_login` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `last_day`  BIGINT NOT NULL DEFAULT 0 COMMENT '上次领取礼包的业务日序号（北京时间）',
  `streak`    INT    NOT NULL DEFAULT 0 COMMENT '连续登录天数',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色登录礼包状态';

-- ============================================================================
-- player_map_dispatch.sql
-- ============================================================================
-- sql/schema/player_map_dispatch.sql
-- 玩家侧出征许可镜像（设计文档 14.1 最后一行）
--
-- 属于 player 的写入方（Persister），不归地图事务管。分表键与其它
-- player_* 表一致（player_id % 512）；这里不建唯一键之外的约束，
-- 避免与玩家 Actor 的写入路径争锁。
CREATE TABLE `player_map_dispatch` (
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家',
  `data_json`   JSON            NOT NULL COMMENT '许可数据',
  `updated_at`  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家侧出征许可';

-- ============================================================================
-- player_partner.sql
-- ============================================================================
-- 玩家伙伴：整体进快照，激活/升星/上阵与材料消耗同一次 Redis CAS 提交（R6.2/R6.3）。
-- 单行 JSON：{"stars":{"1":2},"field":[1]}
-- @flat:partner
CREATE TABLE `player_partner` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `partners`  TEXT NOT NULL COMMENT '伙伴状态 JSON：{stars:{partner_id:star}, field:[partner_id]}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色伙伴状态';

-- ============================================================================
-- player_quest.sql
-- ============================================================================
-- 任务状态整体进玩家快照：一行装下全部任务的进度与领取事实，
-- 领取奖励与状态变更在同一次 Redis CAS 里提交（R2.2）。
-- @flat:quest
CREATE TABLE `player_quest` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `quests`    TEXT NOT NULL COMMENT '任务状态 JSON：{quest_id:{status,progress}}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色任务状态';

-- ============================================================================
-- player_signin.sql
-- ============================================================================
-- 七日签到：周一为一周起点，状态只有「已领取天数 + 已领取奖励汇总」。
--
-- 周切换的清零由周期回调模块负责，本表不含周锚点；-- @flat:signin 让 gendb
-- 生成 FlatOps，模块侧一行 RegisterFlat 完成持久化接入。
CREATE TABLE `player_signin` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `claimed_days` TEXT NOT NULL COMMENT '已领取天数 JSON 数组，1=周一…7=周日',
  `claimed_rewards` TEXT NOT NULL COMMENT '已领取奖励 JSON 对象 {item_id:count}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色七日签到';

-- ============================================================================
-- player_skill.sql
-- ============================================================================
-- 技能等级整体进玩家快照：学习/升级与消耗在同一次 Redis CAS 提交（R2.2）。
-- @flat:skill
CREATE TABLE `player_skill` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `skills`    TEXT NOT NULL COMMENT '技能等级 JSON：{skill_id:level}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色技能等级';

-- ============================================================================
-- player_writebehind.sql
-- ============================================================================
-- 独立落库进程的版本栅栏，与快照内所有业务行在同一事务更新。
CREATE TABLE `player_writebehind` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `version` BIGINT NOT NULL DEFAULT 0,
  `applied_grants` JSON NULL COMMENT '已应用发奖业务键，与资产同事务落库',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家异步落库版本';

-- ============================================================================
-- rank_archive_task.sql
-- ============================================================================
-- 赛季归档任务（R8.3）：每个 (season, rank_type) 一行，记录断点。
-- 归档写 rank_history 是按唯一键 UPSERT 的，所以「running 重跑」天然幂等；
-- 这张表只负责「哪些榜单已经确认归档完成」和排障信息。
-- 主键用 season:rank_type 单列：gendb 的 Find/Upsert 按单列主键生成。
CREATE TABLE `rank_archive_task` (
  `id`         VARCHAR(80) NOT NULL COMMENT 'season:rank_type',
  `season`     VARCHAR(32) NOT NULL COMMENT '赛季',
  `rank_type`  VARCHAR(32) NOT NULL COMMENT '榜单类型',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0 待归档 1 归档中 2 已完成',
  `archived_rows` INT NOT NULL DEFAULT 0 COMMENT '已写入行数（原名 rows，与 MySQL 8.0 保留字冲突）',
  `updated_at` BIGINT NOT NULL DEFAULT 0 COMMENT '最近更新（unix 秒）',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='赛季归档任务（断点续跑）';

-- ============================================================================
-- recharge_order.sql
-- ============================================================================
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

-- ============================================================================
-- shop_record.sql
-- ============================================================================
-- 本周期购买次数整体进玩家快照：扣费、发货与次数累加同一次 Redis CAS（R2.2）。
-- @flat:shop
CREATE TABLE `shop_record` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `day`       BIGINT NOT NULL DEFAULT 0 COMMENT '购买次数所属业务日（每日刷新时归零）',
  `bought`    TEXT NOT NULL COMMENT '购买次数 JSON：{"shop_id:goods_id":count}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色商店购买次数';

-- ============================================================================
-- team.sql
-- ============================================================================
-- sql/schema/team.sql
-- 组队表 + 成员表（游戏服，分表键：无 —— 短期数据，文档注明可只存 Redis，
-- 落库仅作崩溃恢复兜底；P0 建表备用）
CREATE TABLE `team` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '队伍ID',
  `leader_id`  BIGINT UNSIGNED NOT NULL COMMENT '队长玩家ID',
  `copy_id`    INT  NOT NULL DEFAULT 0 COMMENT '目标副本(见 copy.xml)',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0招募中 1副本中 2已解散',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='组队表';

CREATE TABLE `team_member` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '成员记录ID',
  `team_id`    BIGINT UNSIGNED NOT NULL COMMENT '队伍ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `ready`      TINYINT NOT NULL DEFAULT 0 COMMENT '0未准备 1已准备',
  `joined_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_team_player` (`team_id`, `player_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='队伍成员表';
