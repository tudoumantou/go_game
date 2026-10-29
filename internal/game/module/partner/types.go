// Package partner 伙伴（R6）：激活 → 升星 → 上阵/下阵。
//
// 状态整体进玩家快照（Ext["partner"]），表是 player_id 单行：
// 激活/升星要「扣金币 + 改状态」同成同败，走快照 CAS 天然满足；
// 拆成多行实体表反而要自己维护事务。
//
// 架构职责：伙伴收集度 + 上阵编成 + 攻击加成维护。
// Ext 槽位：player_partner.Partners 单列 JSON（stars + field）。
// 事件订阅：无。
// 配置表：partner.xml（Col1=名称，Col2=品质，Col3=每星攻击加成）、
// partner_star.xml（col1=伙伴，col2=当前星级，col3=升下一星消耗）。
// 懒初始化：未激活任何伙伴时 Ext 为空，Get 返回空 Data；
// 「上阵位」上限 fieldLimit 是玩法常量而非配置。
//
// 属性加成只由**已上阵**伙伴提供，且只在「登录 / 上阵 / 下阵 / 升星」四个时点
// 精确增减一次，不在每次读列表时重算——避免反复累加（R6.3）。
//
// 写法（v3.6 迁移）：模块声明一行（Define）、路由若干行、业务函数只碰
// Ctx.Repo；协议号由消息类型反查，错误码来自统一枚举。

package partner

type Data struct {
	Stars map[int32]int32 `json:"stars"`
	Field []int32         `json:"field"`
}

// M 是伙伴模块的唯一实例。
//
// 没有 New()：日志、状态编解码、分发、生命周期都由 Define 兜底。
// 登录钩子重算加成——玩家层在 LoginAll 之前已 ClearBonus，所以是「从零叠加」。
