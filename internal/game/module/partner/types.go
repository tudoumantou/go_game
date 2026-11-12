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

import "fmt"

// Data 是伙伴模块的私有状态：星级表 + 上阵编成。
//
// 两个字段必须整体存一列（whole=partners）：Field 是上阵顺序，脱离
// Stars 无法解释（上阵但未拥有是坏状态）。拆成两列会把「先写哪个」
// 变成一致性问题，而它们本来就是一个原子的结构。
//
// @codec table=player_partner whole=partners new=newData validate=validate verify=validate
type Data struct {
	Stars map[int32]int32 `json:"stars"`
	Field []int32         `json:"field"`
}

// newData 新玩家的空状态：map 必须初始化，Field 给空切片保证 JSON 是 [] 而不是 null。
func newData() *Data { return &Data{Stars: map[int32]int32{}, Field: []int32{}} }

// validate 拦住坏快照：上阵位超限、上阵未拥有、重复上阵都会让登录给出
// 错误加成且无法自愈，宁可整批落库失败，也不把坏状态放进出战计算。
func validate(d *Data) error {
	if len(d.Field) > fieldLimit {
		return fmt.Errorf("partner field has %d entries, limit %d", len(d.Field), fieldLimit)
	}
	seen := map[int32]bool{}
	for _, id := range d.Field {
		if d.Stars[id] <= 0 {
			return fmt.Errorf("partner %d on field but not owned", id)
		}
		if seen[id] {
			return fmt.Errorf("partner %d duplicated on field", id)
		}
		seen[id] = true
	}
	return nil
}

// M 是伙伴模块的唯一实例。
//
// 没有 New()：日志、状态编解码、分发、生命周期都由 Define 兜底。
// 登录钩子重算加成——玩家层在 LoginAll 之前已 ClearBonus，所以是「从零叠加」。
