-- 跨服玩法：报名 / 玩家数据 / 分组。
--
-- 三张表只存「当前这一期」，主键不带活动维度：期次、阶段、开赛时间由活动
-- 管理器按周算法算出（见 docs/跨服玩法时序与积分同步设计.md §1），切期时清表，
-- 所以不需要 activity_id / period / instance_id。重启后重算即可，落库反而会
-- 多一份和算法对不上的数据。
--
-- 中心服与游戏服各三张，字段完全一致，只是前缀不同：
--   dev 两侧连同一个 game_study 库（configs/*/device.yaml），同名表会互相覆盖；
--   生产分库时只建自己那三张，前缀可去掉，DDL 不用改。Go 侧共用一套结构体
--   和 codec，只参数化表名。
--
-- 为什么分组表用 JSON 整列而不是一人一行：
--   100 人一组，组内排行读一行（约 6KB）就够，不需要 join、不需要 ORDER BY；
--   拆成一人一行反而要建 (group_id, score) 索引才能出排名。
--   代价是写放大——积分每 60s 变一次，整列重写 6KB。这靠「写入方唯一」兜住：
--   center 侧单 goroutine 串行合并后整列覆盖，游戏服侧只吃下发、改内存镜像、
--   5 分钟懒落库，两边都不存在两个人同时改同一组 JSON 的情况。

-- 报名表：一期一行，玩家点报名后写入。
--
-- 报名由中心服同步裁决（重复报名、是否在报名期都只有 center 知道全局），
-- 写成功后回写游戏服这份副本，所以没有「待上报」状态列：
--   有行 = 已报名未分组；无行 + cross_player 有行 = 已入组；都无 = 未报名。
-- 玩家报了名却没进任何组（center 漏人、人数不足）时，本行会留到切期，
-- 是排查这类客诉的唯一依据。
--
-- power 必须是独立列而不是塞进 info：分组要按它排序，塞进 JSON 就只能
-- 全表读出来内存排序。
CREATE TABLE IF NOT EXISTS `cross_enroll` (
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID（一期一行）',
  `server_id`   INT             NOT NULL DEFAULT 0 COMMENT '报名所在区服',
  `power`       BIGINT          NOT NULL DEFAULT 0 COMMENT '分组排序依据（报名时战力快照）',
  `info`        JSON            NULL                COMMENT '玩家基本信息列表 [{"k":"level","v":"120"}]',
  `enrolled_at` BIGINT          NOT NULL DEFAULT 0 COMMENT '报名时间（unix 秒）',
  PRIMARY KEY (`player_id`),
  KEY `idx_server` (`server_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨服报名（当前期）';

-- 玩家数据表：玩家维度的实时数据，进界面第一屏就读它。
--
-- 与分组表的冗余是故意的：分组表的 JSON 是「全组 100 人的快照」，按 60s
-- 下发节拍更新；本表是「本服玩家的当前值」，积分一变就改，是上报 center 的
-- 数据源。两者滞后程度不同，合并会让上报路径被下发节拍绑架。
--
-- power 在分组时写入，活动期内不再变：跨服玩法锁定报名战力，避免中途变强
-- 影响分组公平性。
CREATE TABLE IF NOT EXISTS `cross_player` (
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `group_id`   INT             NOT NULL DEFAULT 0 COMMENT '组号（0=未分组）',
  `server_id`  INT             NOT NULL DEFAULT 0 COMMENT '所属区服',
  `power`      BIGINT          NOT NULL DEFAULT 0 COMMENT '分组时战力快照',
  `score`      BIGINT          NOT NULL DEFAULT 0 COMMENT '活动内积分',
  `info`       JSON            NULL                COMMENT '玩家基本信息列表 [{"k":"level","v":"120"}]',
  `updated_at` BIGINT          NOT NULL DEFAULT 0 COMMENT '更新时间（unix 秒）',
  PRIMARY KEY (`player_id`),
  KEY `idx_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨服玩家数据（当前期）';

-- 分组表：一组一行，100 人一组，一期 1..N 组。
--
-- players：组内排行快照，界面读这一行就能出榜。
-- servers：从 players 派生的反向索引，两个用途——
--   ① center 下发时按服聚合（一组 100 人来自约 30 个服，不聚合会变成
--      1000 组 × 30 服 = 30000 条报文/分钟，把 tickLoop 打满）；
--   ② 游戏服跨服分数变更时查「这个玩家归哪个服管」，owner 服才能落账。
-- 两份必须同一次事务写入，否则会成为两份对不上的数据。
CREATE TABLE IF NOT EXISTS `cross_group` (
  `group_id` INT  NOT NULL COMMENT '组号（一期内 1..N）',
  `players`  JSON NULL     COMMENT '玩家列表 [{"player_id":1,"power":100,"score":0,"updated_at":0}]',
  `servers`  JSON NULL     COMMENT '服务器玩家列表 [{"server_id":1,"players":[1,2]}]',
  PRIMARY KEY (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨服分组（当前期，长期保留）';

-- ===== 中心服侧（字段与上面完全一致，仅前缀不同）=====

CREATE TABLE IF NOT EXISTS `center_cross_enroll` (
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID（一期一行）',
  `server_id`   INT             NOT NULL DEFAULT 0 COMMENT '报名所在区服',
  `power`       BIGINT          NOT NULL DEFAULT 0 COMMENT '分组排序依据（报名时战力快照）',
  `info`        JSON            NULL                COMMENT '玩家基本信息列表 [{"k":"level","v":"120"}]',
  `enrolled_at` BIGINT          NOT NULL DEFAULT 0 COMMENT '报名时间（unix 秒）',
  PRIMARY KEY (`player_id`),
  KEY `idx_server` (`server_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨服报名（中心服，当前期）';

CREATE TABLE IF NOT EXISTS `center_cross_player` (
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `group_id`   INT             NOT NULL DEFAULT 0 COMMENT '组号（0=未分组）',
  `server_id`  INT             NOT NULL DEFAULT 0 COMMENT '所属区服',
  `power`      BIGINT          NOT NULL DEFAULT 0 COMMENT '分组时战力快照',
  `score`      BIGINT          NOT NULL DEFAULT 0 COMMENT '活动内积分',
  `info`       JSON            NULL                COMMENT '玩家基本信息列表 [{"k":"level","v":"120"}]',
  `updated_at` BIGINT          NOT NULL DEFAULT 0 COMMENT '更新时间（unix 秒）',
  PRIMARY KEY (`player_id`),
  KEY `idx_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨服玩家数据（中心服，当前期）';

CREATE TABLE IF NOT EXISTS `center_cross_group` (
  `group_id` INT  NOT NULL COMMENT '组号（一期内 1..N）',
  `players`  JSON NULL     COMMENT '玩家列表 [{"player_id":1,"power":100,"score":0,"updated_at":0}]',
  `servers`  JSON NULL     COMMENT '服务器玩家列表 [{"server_id":1,"players":[1,2]}]',
  PRIMARY KEY (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨服分组（中心服，当前期，长期保留）';
