// internal/game/module/grant.go —— 发奖路径的统一入口
//
// 解决什么问题：7 个模块各自重复同一套三步——先预检容量、再逐个 AddItem、
// 最后手工 MarkDirty("bag")。三处都能出错：
//
//   - 预检漏做 → 背包满了还发，道具静默丢失；
//   - 溢出漏查 → 存量超过 int32 后在协议里变成负数（只有 signin 查了）；
//   - MarkDirty 漏写 → 改了背包却不提交（AddItem 内部其实已经标脏，
//     这行本来就是冗余的，属于「靠人记得」的历史写法）。
//
// Grant 把三步收成一次调用，并且保证「要么全发，要么一个都不改」：
// 先全量预检，通过后才逐项入包。
package module

import (
	"errors"
	"math"
)

// ErrItemLimit 单种道具的存量将超过 int32（协议 Count 的宽度）。
//
// 与 ErrBagFull 分开：满包是"清包就能领"，超限是"数量异常"，运维处理方式不同。
var ErrItemLimit = errors.New("item count exceeds int32")

// BagChange 一次发放后某个道具的变化（协议无关）。
//
// 模块按自己的响应结构转成 proto；不直接在 module 包返回 proto 消息，
// 是为了让基座不依赖 pb 包（协议变更不该牵动基座）。
type BagChange struct {
	ItemID int32
	// Count 发放后的存量（客户端直接刷新显示，不必再算一次）。
	Count int64
	// Delta 本次发放数量。
	Delta int64
}

// PrecheckGrant 只做校验、不改状态。
//
// 用于「校验 → 中间还有别的事（扣费）→ 才发货」的场景（商店购买）。
// 大多数模块直接用 Grant 即可；这里单独暴露，是为了让这类模块不必
// 自己重抄一遍容量与溢出判断。
func PrecheckGrant(state PlayerState, grants []ItemGrant) error {
	valid, err := validGrants(state, grants)
	if err != nil {
		return err
	}
	if len(valid) == 0 {
		return nil
	}
	if !state.CanHold(valid) {
		return ErrBagFull
	}
	return nil
}

// Grant 发奖：溢出预检 → 容量预检 → 入包。
//
// 返回 nil, nil 表示没有任何有效道具可发（配置为空），调用方不必当失败处理。
// 失败时保证**没有修改任何状态**——预检全过才入包。
//
// Go 知识点：errors.Is 判定这两个哨兵错误，模块据此映射成自己的业务码；
// 基座不持有业务码表，避免协议/错误码变更牵动框架层。
func Grant(state PlayerState, grants []ItemGrant) ([]BagChange, error) {
	valid, err := validGrants(state, grants)
	if err != nil {
		return nil, err
	}
	if len(valid) == 0 {
		return nil, nil
	}
	// 容量按「新增种类」算，必须在入包前判断：入包后再判就得回滚。
	if !state.CanHold(valid) {
		return nil, ErrBagFull
	}
	out := make([]BagChange, 0, len(valid))
	for _, g := range valid {
		state.AddItem(g.ItemID, g.Count) // AddItem 内部标脏，调用方无需再 MarkDirty
		out = append(out, BagChange{ItemID: g.ItemID, Count: state.GetItemCount(g.ItemID), Delta: g.Count})
	}
	return out, nil
}

// validGrants 过滤空配置并做 int32 溢出预检。
func validGrants(state PlayerState, grants []ItemGrant) ([]ItemGrant, error) {
	valid := make([]ItemGrant, 0, len(grants))
	for _, g := range grants {
		if g.ItemID <= 0 || g.Count <= 0 {
			continue // 空配置按「不发」处理，不当作错误
		}
		cur := state.GetItemCount(g.ItemID)
		// cur < 0 是脏数据（存量不该为负），同样按超限拒绝，避免负数继续扩散。
		if cur < 0 || g.Count > math.MaxInt32-cur {
			return nil, ErrItemLimit
		}
		valid = append(valid, g)
	}
	return valid, nil
}
