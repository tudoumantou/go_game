// Package codegen 只放「生成物规约」的测试，没有生产代码。
//
// 为什么单独一个包：这条规约管的是全仓 113 个生成文件（pkg/model、pkg/gamedata、
// pkg/pb、pkg/codec、各模块 persist_gen.go），不属于任何一个被生成的包，
// 塞进哪个业务包都会让那个包的测试多一份无关职责。
package codegen
