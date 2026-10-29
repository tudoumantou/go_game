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

import (
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
)

const (
	// stateKey 同时是 Ext 槽位键与落库归属的模块名。
	stateKey = "partner"

	// fieldLimit 上阵位数。配置里没有这一项，作为玩法常量维护。
	fieldLimit = 3
	// itemCoin 金币道具 ID（激活与升星消耗同一种货币）。
	itemCoin int32 = 1001
)

// Data 伙伴状态：partner_id → 星级；Field 是上阵顺序。
//
// 两个容器分工：Stars 是 map（稀疏、按 ID 随机访问），
// Field 是 slice（稠密、保序、最多 3 个元素）——用对容器比用对算法重要。
//
// json tag 是**落库契约**（整份序列化进 player_partner.partners 一列），
// 改 tag 等于让旧数据读不出来。

var M = module.Define[Data](stateKey, newData).WithLogin(onLogin)

func init() {
	// 列表请求无参数：语义完全由「谁在发」决定。
	M.HandleRoute(&gamev1.PartnerListReq{}, onList)
	// 其余三条都要请求体（伙伴 id / 上阵开关）。
	module.Route(M, &gamev1.PartnerActivateReq{}, onActivate)
	module.Route(M, &gamev1.PartnerStarUpReq{}, onStarUp)
	module.Route(M, &gamev1.PartnerFieldReq{}, onField)
}

// onLogin 重新叠加已上阵伙伴的加成。
//
// 状态损坏时吞掉错误继续登录：加成可以下次写操作时修复，
// 但把玩家拦在登录外的影响更大（可用性优先）。
