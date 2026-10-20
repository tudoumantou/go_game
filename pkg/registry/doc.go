// Package registry 服务注册发现。
//
// 具体实现在子包 etcd（租约 + KeepAlive + Watch）；本文件只声明包用途，
// 不提供抽象接口——使用方直接依赖 registry/etcd 的具体类型。
//
// 架构职责：声明「本层解决进程发现问题」，把 key 格式、序列化格式、
// 租约语义全部下沉到 etcd 子包。
// 协作关系：各进程启动时用 etcd.Registry 注册自身，gRPC 客户端经
// pkg/rpc/resolver 反向读取同一套 key 完成服务发现——读写共享
// /services/{service}/{id} 前缀约定。
// 设计取舍：不在本层定义 Registry 接口。当前只有 etcd 一种实现，
// 提前抽象（如为未来接 Consul/Nacos 留接口）属于为假想需求增加间接层；
// 真出现第二实现时再抽不迟（YAGNI）。代价是使用方与 etcd 具体类型耦合，
// 换实现时需要改 import——接受，因为项目生命周期内 etcd 是既定选型。
package registry
