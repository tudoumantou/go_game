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
