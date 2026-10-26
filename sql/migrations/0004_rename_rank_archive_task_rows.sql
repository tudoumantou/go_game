-- 0004: rank_archive_task.rows → archived_rows
--
-- `rows` 是 MySQL 8.0 的保留字（窗口函数 ROWS BETWEEN ...），而 gendb 生成的
-- SQL 不给列名加反引号，于是 SELECT/INSERT 全部语法错误（Error 1064）——
-- 这张表从建表起就没成功读写过，只是没有任何测试碰它。
--
-- 改成非保留字；同时 gendb 加了保留字检查，下次再想用保留字会在生成阶段报错。
ALTER TABLE `rank_archive_task`
  CHANGE COLUMN `rows` `archived_rows` INT NOT NULL DEFAULT 0 COMMENT '已写入行数（原名 rows，与 MySQL 8.0 保留字冲突）';
