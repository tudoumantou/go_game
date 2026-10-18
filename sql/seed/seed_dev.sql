-- sql/seed/seed_dev.sql —— 开发环境最小种子数据（仅 dev/test，禁止用于生产）
-- 道具/等级/职业等配置数据走 configs/gamedata/xml（16.1），不在此处；
-- 本文件只灌账号/玩家等必须落在 MySQL 的种子（19.5 首次部署清单第 4 步）。

-- 账号：test001/test002/gm001。P0 密码哈希为占位符（格式与 P2 bcrypt 不兼容），
-- P2 登录模块实现后用 tools/passhash 重新生成。
INSERT INTO `account` (`id`, `account`, `password_hash`, `salt`, `status`, `device`) VALUES
  (1, 'test001', 'P0_PLACEHOLDER_HASH', 'P0_SALT', 0, 'dev'),
  (2, 'test002', 'P0_PLACEHOLDER_HASH', 'P0_SALT', 0, 'dev'),
  (3, 'gm001',   'P0_PLACEHOLDER_HASH', 'P0_SALT', 0, 'dev');

-- 玩家：两个测试角色 + 一个 GM 角色（P2 起 gm001 走 GM 指令通道）
INSERT INTO `player` (`id`, `account_id`, `name`, `level`, `exp`, `coin`, `scene_id`) VALUES
  (1, 1, '测试战士', 1, 0,    10000, 1001),
  (2, 2, '测试法师', 1, 0,    10000, 1001),
  (3, 3, 'GM管理员', 1, 0,    0,     0);

-- 属性：职业 1=战士 / 2=法师（见 career.xml），数值取 level.xml 1 级基线
INSERT INTO `player_attr` (`player_id`, `career`, `hp`, `mp`, `atk`, `def`, `spd`) VALUES
  (1, 1, 500,  50, 60, 30, 100),
  (2, 2, 320, 200, 80, 18, 110),
  (3, 1, 500,  50, 60, 30, 100);
