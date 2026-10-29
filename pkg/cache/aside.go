// pkg/cache/aside.go —— cache-aside 读取的统一实现
//
// 解决的问题：cache-aside 是「先查缓存，miss 回源，再回填」这三步，
// 但真正容易出错的是其中的**四个判断**，而它们此前在三个仓储里各写
// 一遍（login/repo 的 acc_summary、serv_info、user_role）：
//
//	rdb 为 nil 时怎么办      —— 三处都写 if r.rdb != nil
//	反序列化失败怎么办        —— 三处都把坏缓存当 miss（这个决定最容易写错：
//	                            当错误返回会让一次缓存污染变成读失败）
//	回填失败怎么办            —— 三处都是 _ = ...Set(...).Err()（忽略，
//	                            缓存是加速层，回填失败不该让读失败）
//	回源结果是否值得回填      —— 三处各不相同（有的判 nil，有的不判）
//
// 前三项是**必须一致**的判断：任何一处写反，同一份数据在不同仓储里就
// 有了不同的故障语义——A 仓储缓存坏了照常读，B 仓储缓存坏了报错，而
// 排查的人看到的是「同一个 Redis，只有某个页面 500」。第四项是业务
// 策略，因此由 load 的返回值显式表态，而不是让 helper 猜。
//
// 为什么没做成「Base 的方法」：pkg/sqlstore 不该依赖 redis（它是 SQL
// 仓储基座，登录服无 Redis 的单测也要能构造它）。cache-aside 是缓存
// 策略，放 pkg/cache 才对。
package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// Aside 一次 cache-aside 读取。
//
// 流程：rdb 非空则 GET → 命中且能反序列化就返回 → 否则 load → 按
// cacheable 决定是否回填。
//
// load 的第二个返回值是「这次回源结果值不值得写进缓存」：
//   - 查到实体但实体为空（比如账号没有摘要）→ false，避免把空值缓存
//     成 null 挡住后续写入后的首次读；
//   - 查到列表且列表为空是正常状态（新账号没角色）→ true，因为空列表
//     本身就是正确答案，缓存它省掉重复回源。
//
// 这个区分不能由 helper 用 nil 判断代替：Go 里「nil 切片」和「空切片」
// 在语义上常常被当作同一件事，而在缓存决策上它们不一定该同等对待。
//
// key 必须先由调用方经 keygen 构造（见 keygen.go）：本函数不拼 key，
// 也就不会成为「第二个 key 定义点」。
func Aside[T any](
	ctx context.Context,
	rdb redis.UniversalClient,
	key string,
	ttl time.Duration,
	load func() (T, bool, error),
) (T, error) {
	if rdb != nil {
		if raw, err := rdb.Get(ctx, key).Bytes(); err == nil {
			var v T
			if json.Unmarshal(raw, &v) == nil {
				return v, nil
			}
			// 坏缓存当 miss：缓存是加速层，一次反序列化失败不该让读失败。
			// 回源会覆盖掉这份坏数据，下次读就正常了。
		}
	}
	v, cacheable, err := load()
	if err != nil {
		// 回源失败原样上抛：与「缓存里没有」是两种语义，不能混成零值。
		return v, err
	}
	if cacheable && rdb != nil {
		if raw, mErr := json.Marshal(v); mErr == nil {
			// 回填失败忽略：本次读已经成功，缓存没写上是下一个请求慢一点，
			// 不是这个请求错。
			_ = rdb.Set(ctx, key, raw, ttl).Err()
		}
	}
	return v, nil
}
