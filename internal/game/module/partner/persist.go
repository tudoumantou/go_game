// internal/game/module/partner/persist.go —— 伙伴模块的持久化适配
//
// 与 quest/activity 不同：伙伴的 Data 本身就适合整份序列化
// （Stars 是 map、Field 是有序切片），所以不需要像签到那样拆列。
package partner

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

func init() {
	writebehind.RegisterStateful(stateKey, model.PlayerPartnerFlat,
		writebehind.StateCodec[model.PlayerPartner, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// newData 新玩家的空状态：map 必须初始化，Field 给空切片保证 JSON 是 [] 而不是 null。
func newData() *Data { return &Data{Stars: map[int32]int32{}, Field: []int32{}} }

// toRow 整份业务结构存一个 JSON 列。
func toRow(playerID int64, d *Data) (*model.PlayerPartner, error) {
	raw, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("encode partner state: %w", err)
	}
	return &model.PlayerPartner{PlayerId: playerID, Partners: string(raw)}, nil
}

// fromRow 冷启动读回业务结构；读的时候顺带过一次不变量校验。
//
// 热路径（Ext → Data）由基座直接 json.Unmarshal，不再单独校验：
// 那份数据是本进程上一次提交写进去的，可信；跨进程的坏数据只可能从
// 冷启动进来，而这里正是入口。
func fromRow(row *model.PlayerPartner) (*Data, error) {
	d := newData()
	if row == nil || row.Partners == "" {
		return d, nil
	}
	if err := json.Unmarshal([]byte(row.Partners), d); err != nil {
		return nil, fmt.Errorf("decode partner state: %w", err)
	}
	if d.Stars == nil {
		d.Stars = map[int32]int32{}
	}
	if d.Field == nil {
		d.Field = []int32{}
	}
	if err := validate(d); err != nil {
		return nil, err
	}
	return d, nil
}

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
