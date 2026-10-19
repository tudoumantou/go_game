-- 0001_baseline —— 基线占位，不包含 SQL。
--
-- 表的完整定义在 sql/schema/*.sql，由 `make db-init` 建库时执行。
-- 已有库升级时执行 `make db-migrate -- --baseline` 把本文件标记为已应用，
-- 后续增量从 0002 开始。
SELECT 1;
