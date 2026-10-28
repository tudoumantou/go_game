-- 抢位置：删除 seat_pending_reward。
--
-- 座位奖励的离线补发改用项目已有的离线事件队列（pend:seat:{pid}，Redis List
-- + 上限 + TTL），登录时由 drainOffline 统一消费，与离线私聊同一条通道。
-- 没必要为单一玩法再维护一张只用来「暂存待发奖励」的表。
DROP TABLE IF EXISTS `seat_pending_reward`;
