// view.go —— 缓存实体行 → 领域行的还原。
//
// 只服务「用归属索引展开列表」这条路径：SQL 路径直接 Scan 成 MailRow，
// 不经过这里。字段名是列名、值统一是字符串，这是 cache.EntityRow 的约定。
package mail

import (
	"context"
	"database/sql"
	"sort"
	"strconv"
	"time"

	"game_study/pkg/cache"
)

// listRows 取邮件列表：归属索引展开优先，未启用/失败回落 SQL。
//
// 为什么值得走缓存：SQL 版本是「100 行 × 100 次 EXISTS 探测」；缓存版本是
// 「1 次 ZRANGEBYSCORE + 1 次 pipeline HGETALL + 1 次 IN 查附件」——邮件越多，
// EXISTS 的次数越多，而 IN 始终一次。少的那 100 次探测就是这里的收益。
//
// 回落条件刻意宽（索引为 nil、展开报错都回落）：列表是读路径，任何缓存
// 故障都不该让玩家看不到邮件——顶多多打一次库。
func (m *Module) listRows(ctx context.Context, playerID int64) ([]MailRow, error) {
	if m.index != nil {
		entityRows, removed, err := m.index.Expand(ctx, playerID)
		if err != nil {
			m.logger.Warn("mail expand failed, falling back to sql",
				"player_id", playerID, "err", err)
		} else {
			if len(removed) > 0 {
				m.logger.Info("mail index self-healed",
					"player_id", playerID, "removed", len(removed))
			}
			return m.rowsFromCache(ctx, entityRows)
		}
	}
	return m.repo.ListByPlayer(ctx, playerID, mailListLimit)
}

// rowsFromCache 把展开出的实体行组装成列表行。
//
// 与 SQL 版本的两处差异，都是有意为之：
//   - 倒序与限长在内存做（SQL 是 ORDER BY id DESC LIMIT）：索引返回升序，
//     而邮件量级是几十到几百，内存排序的代价远小于一次额外往返；
//   - 附件标记多一次 IN 查询：e:mail:{id} 只缓存 mail 表的列，
//     mail_attachment 是另一张表。不做冗余字段——那会在每次领取附件时
//     多一处必须同步的缓存写，而「不同步」正是这套设计最难查的问题。
func (m *Module) rowsFromCache(ctx context.Context, rows []cache.EntityRow) ([]MailRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]MailRow, 0, len(rows))
	for _, er := range rows {
		row, ok := mailRowFrom(er)
		if !ok {
			// 单行字段损坏不该让整页失败：跳过它，其余照常返回。
			m.logger.Warn("mail entity row malformed", "id", er.ID)
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if len(out) > mailListLimit {
		out = out[:mailListLimit]
	}
	if len(out) == 0 {
		return nil, nil
	}

	ids := make([]int64, len(out))
	for i := range out {
		ids[i] = out[i].ID
	}
	withAtt, err := m.repo.HasAttachments(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].HasAttachment = withAtt[out[i].ID]
	}
	return out, nil
}

// mailRowFrom 把缓存里的动态列还原成列表行。
//
// 字段名是列名（`id`/`title`/…），值统一是字符串——这是 EntityRow 的约定
// （与 IDTable.LoadEntity 同构）。时间列由 toString 格式化为 RFC3339
// （见 pkg/cache 的 toString），这里按 RFC3339 解析。
func mailRowFrom(er cache.EntityRow) (MailRow, bool) {
	var row MailRow
	id, err := strconv.ParseInt(er.Fields["id"], 10, 64)
	if err != nil || id <= 0 {
		return row, false
	}
	row.ID = id
	row.Title = er.Fields["title"]
	row.Channel = parseInt32(er.Fields["channel"])
	row.Status = parseInt32(er.Fields["status"])
	if t, ok := parseTimeField(er.Fields["created_at"]); ok {
		row.CreatedAt = t
	}
	if t, ok := parseTimeField(er.Fields["expire_at"]); ok {
		row.ExpireAt = sql.NullTime{Time: t, Valid: true}
	}
	return row, true
}

// parseInt32 宽松解析：坏值按 0 处理（列表展示用，不值得让整行失败）。
func parseInt32(s string) int32 {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}

// parseTimeField 解析缓存里的时间列。
//
// 两种格式都认：RFC3339（parseTime=true 时 toString 的产出）与
// MySQL 原生 "2006-01-02 15:04:05"（parseTime=false 或 []byte 直传）。
// 只认前者会让「换一次驱动配置」变成「列表时间全变 1970」。
func parseTimeField(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local); err == nil {
		return t, true
	}
	return time.Time{}, false
}
