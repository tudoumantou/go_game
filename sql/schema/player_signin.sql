-- 七日签到：周一为一周起点，状态只有「已领取天数 + 已领取奖励汇总」。
--
-- 周切换的清零由周期回调模块负责，本表不含周锚点；-- @flat:signin 让 gendb
-- 生成 FlatOps，模块侧一行 RegisterFlat 完成持久化接入。
CREATE TABLE `player_signin` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `claimed_days` TEXT NOT NULL COMMENT '已领取天数 JSON 数组，1=周一…7=周日',
  `claimed_rewards` TEXT NOT NULL COMMENT '已领取奖励 JSON 对象 {item_id:count}',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色七日签到';
