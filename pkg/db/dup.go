// pkg/db/dup.go —— 唯一键冲突判定（MySQL Error 1062）
//
// 为什么要收敛成函数：过去各模块用 strings.Contains(err.Error(), "1062")
// 判重（team/guild/chat 各写一遍），依赖错误文本的措辞——换语言包、换驱动
// 版本、加上下文包装都会让它静默失效，而失效的表现是「幂等变成报错」或
// 「冲突被当成功」，两类都是脏数据。
//
// 这里用 errors.As 取结构化的 *mysql.MySQLError 判 Number：
// 与错误文本无关，也不会被 %w 包装链打断。
package db

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// IsDuplicate 判断 err 是否为 MySQL 唯一键冲突（Error 1062）。
//
// 调用场景：
//   - 「插入 + 冲突即幂等命中」：邮件 biz_id、聊天 msg_id、好友双向关系；
//   - 「插入 + 冲突即业务错误」：角色名重名、公会名重名、重复入队/入会。
//
// 两者都先判定 1062，再按业务语义翻译——判定与翻译分开，是因为同一类
// 冲突在不同业务里的含义不同，但「它是不是 1062」只有一个答案。
func IsDuplicate(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}
