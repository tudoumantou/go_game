-- 活动实例（R7.1）：每个活动每个周期一行，由中心服 leader 写入并下发。
-- 周期 = 自固定基准起按 activity.xml 的持续天数切分，instance_id = {activity_id}-{period}。
-- version 取 start_at（单调递增）：游戏服按它丢弃乱序/重复的调度消息。
CREATE TABLE `activity_instance` (
  `instance_id` VARCHAR(64) NOT NULL COMMENT '{activity_id}-{period}',
  `activity_id` INT NOT NULL COMMENT '活动配置 ID',
  `period`      BIGINT NOT NULL COMMENT '周期序号',
  `start_at`    BIGINT NOT NULL COMMENT '开始（unix 秒，北京时间零点对齐）',
  `end_at`      BIGINT NOT NULL COMMENT '结束（unix 秒）',
  `version`     BIGINT NOT NULL COMMENT '版本（= start_at）',
  `updated_at`  BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`instance_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='活动实例（中心服调度）';
