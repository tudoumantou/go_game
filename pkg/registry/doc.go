// Package registry 服务注册发现。
//
// 具体实现在子包 etcd（租约 + KeepAlive + Watch）；本文件只声明包用途，
// 不提供抽象接口——使用方直接依赖 registry/etcd 的具体类型。
package registry
