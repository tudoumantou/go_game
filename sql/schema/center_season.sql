-- 中心服赛季状态（单行）：赛季名与起点。
-- 归档流程只有 leader 执行，赛季起点落库才能保证换主/重启后不重新计时。
CREATE TABLE `center_season` (
  `id`         TINYINT NOT NULL DEFAULT 1 COMMENT '固定 1（单行表）',
  `season`     VARCHAR(32) NOT NULL COMMENT '当前赛季，如 s1',
  `started_at` BIGINT NOT NULL COMMENT '赛季开始时间（unix 秒）',
  `updated_at` BIGINT NOT NULL DEFAULT 0 COMMENT '最近更新（unix 秒）',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='中心服赛季状态';
