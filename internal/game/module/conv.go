// internal/game/module/conv.go —— 发奖结果的协议转换
//
// 为什么放在 module 包而不是各业务模块：六个模块都要把「发放结果」转成
// BagChangeNotify 的 ItemChange，字段映射完全一样（ItemUid 用 item_id 占位，
// Change 恒 1，Count 是发放后存量）。这份重复没有业务含义，属于胶水。
//
// 依赖说明：module 包因此 import 了 pb。这是有意的——Grant 本身不碰协议
// （保持可复用），但没有 pb 就写不出这层胶水，而它本该只有一份。
package module

import gamev1 "game_study/pkg/pb/game/v1"

// ToItemChanges 把发放结果转成协议变更列表（nil 输入返回 nil）。
//
// 字段约定：
//   - ItemUid 用 item_id 占位：demo 期道具无唯一实例 id，协议字段不可省；
//   - Count 是发放后的存量：客户端拿它直接刷新显示，不必自己累加；
//   - Change 恒 1：调用方若同时有扣减，自己追加 Change=2 的条目。
func ToItemChanges(changes []BagChange) []*gamev1.ItemChange {
	if len(changes) == 0 {
		return nil
	}
	out := make([]*gamev1.ItemChange, 0, len(changes))
	for _, c := range changes {
		out = append(out, &gamev1.ItemChange{
			ItemUid: int64(c.ItemID),
			ItemId:  c.ItemID,
			Count:   int32(c.Count),
			Change:  1,
		})
	}
	return out
}
