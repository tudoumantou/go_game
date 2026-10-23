// internal/game/module/spend.go —— 扣费路径的统一入口（与 Grant 对称）
//
// 解决什么问题：四处「先查后扣」各写一遍——
//
//	if ctx.State.GetItemCount(id) < cost { resp.Code = xxx; return ... }
//	ctx.State.RemoveItem(id, cost)         // 返回值甚至没接
//
// 两个问题：
//   - RemoveItem 的返回值被忽略。它内部还会再校验一次，若两次判断之间
//     有任何变化（未来的并发路径、或 GetItemCount 与 RemoveItem 语义不一致），
//     扣费会静默失败——游戏里表现为「升级成功但钱没扣」。
//   - 「免费」这种边界每次都要自己写 if cost > 0。
//
// Spend 把「够不够」和「扣不扣得掉」合成一个答案，失败时状态零改动。
package module

import "errors"

// ErrNotEnough 余额不足（扣费前置校验失败）。
//
// 与 Grant 的哨兵错误对称：基座只给语义，模块自己映射业务码
// （skill 的「材料不足」、shop 的「钻石不足」、partner 的「金币不足」）。
var ErrNotEnough = errors.New("item not enough")

// Spend 扣费：余额够才扣，不够返回 ErrNotEnough 且不改任何状态。
//
// count <= 0 视为免费直接成功——调用方不必再写 if cost > 0，
// 配置里「消耗填 0」这类玩法不需要额外分支。
func Spend(state PlayerState, itemID int32, count int64) error {
	if count <= 0 {
		return nil
	}
	if state.GetItemCount(itemID) < count {
		return ErrNotEnough
	}
	// RemoveItem 自带「数量不足返回 false 且不部分扣减」。
	// 上面刚查过一次，这里失败只可能是状态在两次读之间变了；
	// 按余额不足处理——宁可让玩家重试，也不能静默扣失败。
	if !state.RemoveItem(itemID, count) {
		return ErrNotEnough
	}
	return nil
}
