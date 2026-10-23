// internal/game/player/remote.go —— 玩家消息的跨节点编解码注册（P0）
//
// 为什么单独一个文件：远程化是「横切能力」而不是玩家业务——注册表与
// 类型标识常量聚在一处，将来新增可远程消息（SaveMsg/KickMsg…）只改
// 这里，actor.go 保持纯业务。
//
// 注册时机：包 init（进程启动期）。actor.RegisterRemote 重复注册会
// panic——漏注册的故障现象是「远程消息解码报 type not registered」，
// 在启动期/测试期暴露，而不是线上某条消息突然丢。
package player

import (
	"encoding/json"

	"game_study/pkg/actor"
)

// 可跨节点投递的消息类型标识（全局唯一，编解码注册表的键）。
//
// 命名空间前缀 game_study.player. 防与其他包的类型 URL 撞车。
const (
	remoteTypeURLPacketMsg = "game_study.player.PacketMsg"
	remoteTypeURLPushMsg   = "game_study.player.PushMsg"
)

func init() {
	actor.RegisterRemote(remoteTypeURLPacketMsg, func(b []byte) (any, error) {
		var m PacketMsg
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &m, nil
	})
	actor.RegisterRemote(remoteTypeURLPushMsg, func(b []byte) (any, error) {
		var m PushMsg
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return &m, nil
	})
}
