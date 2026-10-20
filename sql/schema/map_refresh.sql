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
