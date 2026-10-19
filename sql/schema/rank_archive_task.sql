-- 赛季归档任务（R8.3）：每个 (season, rank_type) 一行，记录断点。
-- 归档写 rank_history 是按唯一键 UPSERT 的，所以「running 重跑」天然幂等；
-- 这张表只负责「哪些榜单已经确认归档完成」和排障信息。
-- 主键用 season:rank_type 单列：gendb 的 Find/Upsert 按单列主键生成。
CREATE TABLE `rank_archive_task` (
  `id`         VARCHAR(80) NOT NULL COMMENT 'season:rank_type',
  `season`     VARCHAR(32) NOT NULL COMMENT '赛季',
  `rank_type`  VARCHAR(32) NOT NULL COMMENT '榜单类型',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0 待归档 1 归档中 2 已完成',
  `rows`       INT NOT NULL DEFAULT 0 COMMENT '已写入行数',
  `updated_at` BIGINT NOT NULL DEFAULT 0 COMMENT '最近更新（unix 秒）',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='赛季归档任务（断点续跑）';
