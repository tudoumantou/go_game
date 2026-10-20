// pkg/config/doc.go —— 包级文档（实现在 config.go）

// Package config 配置加载层（设计文档 19.1）。
// 详见 config.go 文件头注释。
//
// 架构位置：全项目最底层的「叶子包」之一——只依赖 yaml.v3 与标准库，
// 不被任何业务包反向依赖；五个进程（gateway/game/center/cross/login）
// 的 cmd 入口在启动第一步调用 Load。
// 目录铁律（3.2）：配置文件按 configs/<service>/<service>-<env>.yaml
// 组织，敏感值（MySQL DSN、Redis 密码、各类密钥）一律 ${VAR} 注入，
// 不落库、不进镜像。
package config
