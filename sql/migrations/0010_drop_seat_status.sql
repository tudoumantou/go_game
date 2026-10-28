-- 抢位置：去掉 seat_room.status。
--
-- 这一列从建表起就没被读过（运营关房功能没做），属于预留字段。
-- 预留但没有读写路径的列只会让「这个字段是干什么的」成为排查负担，
-- 真要做关房时再加也不迟。
ALTER TABLE `seat_room` DROP COLUMN `status`;
