-- 好友申请唯一键（R5.2）
--
-- 防「同一对人重复申请」：一对 (from_id, to_id) 只保留一行，重复申请走 UPDATE 回到待处理，
-- 而不是不断插入新行导致同意时有多条待处理。

-- 历史数据先去重：保留每个 (from_id,to_id) 里 id 最小的一行。
DELETE a FROM friend_apply a
JOIN friend_apply b ON a.from_id = b.from_id AND a.to_id = b.to_id AND a.id > b.id;

ALTER TABLE friend_apply ADD UNIQUE KEY uk_from_to (from_id, to_id);
