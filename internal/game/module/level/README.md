# level（占位目录）

升级逻辑不在本目录，真实实现在 `internal/game/player/actor.go`：
`State.AddExp` 连续升级 + 协议 2011 `LevelInfoNotify` 推送；配置 `level.xml`，数据落 `player.level/exp`。

不要为此目录复制业务代码；是否需要等级查询协议另行评审。
