// tools/bench —— 压测客户端（设计文档 23.4 / 19.5 第 7 条）
// TODO(P7): 模拟加密长连接（RSA+AES 握手）→ 登录/进游戏/聊天/战斗混合消息，
// 输出连接数/QPS/P99 延迟基线；当前为占位。
package main

import (
	"flag"
	"fmt"
)

func main() {
	conns := flag.Int("conns", 100, "连接数")
	addr := flag.String("addr", "127.0.0.1:9001", "网关地址")
	flag.Parse()
	fmt.Printf("bench 占位（P7 实现）：目标 %s，连接 %d\n", *addr, *conns)
}
