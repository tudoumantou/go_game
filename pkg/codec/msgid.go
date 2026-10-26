// pkg/codec/msgid.go —— 按消息类型反查协议号
//
// 消息号仍由 proto 的 `// @msg` 注解声明、genmsgid 生成 msgIDs 表；
// 本文件只提供「proto.Message → 号」的便捷入口，让业务注册处理器时
// 写类型而不是数字：
//
//	m.OnMsg(&gamev1.SigninInfoReq{}, &gamev1.SigninInfoResp{}, onInfo)
//
// 缺失注解时 panic 而不是返回 0：注册发生在进程启动期，宁可起不来，
// 也不要等到某个消息永远不回包才发现。
package codec

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

// ProtoID 返回消息类型对应的协议号。
//
// 用 protobuf 的全局注册表取全名（game_study.game.v1.SigninInfoReq），
// 再去掉包前缀查生成的 msgIDs；同包内类型名唯一，短名足够。
func ProtoID(m proto.Message) uint32 {
	if m == nil {
		panic("codec: ProtoID(nil)")
	}
	name := string(proto.MessageName(m))
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	id, ok := MsgID(name)
	if !ok {
		panic(fmt.Sprintf("codec: message %q 没有 // @msg 注解（跑 make gen-msgid）", name))
	}
	return id
}
