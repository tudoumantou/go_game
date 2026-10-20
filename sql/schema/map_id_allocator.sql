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
