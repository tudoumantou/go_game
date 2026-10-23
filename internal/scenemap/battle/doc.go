// Package battle 在 scenemap（ARPG 场景框架）之上实现"实时技能战斗结算"，
// 对应《战斗.md》§10 与 §13。
//
// # 架构位置
//
//	battle → mgr → scenemap / grid        （单向依赖，MGR 不认识 battle）
//
// battle 不是 Actor，也不持有 goroutine：它是**挂在 MGR 上的结算算法库**。
// 所有入口（CastCommand/Tick/Ensure）都必须在 MGR 的 goroutine 内执行，
// 直接读写 *mgr.State —— 与 entity 包同一条单写者纪律。
//
// # 为什么 MGR 不 import battle
//
// MGR 的职责是"串行 + 权威"，不是"认识伤害公式"。反过来让 MGR 依赖
// battle 会把"改一个技能系数"变成"改场景核心"。所以 battle 以两种方式
// 挂进 MGR：
//   - 命令：battle.NewCastCommand 返回 mgr.Command，玩家 Actor 通过
//     Mgr.Call 投递，在 MGR 串行队列里执行；
//   - 定时钩子：装配层把 battle.Tick 塞进 mgr.Deps.BuffTick（500ms，
//     吃倍速），由 MGR 统一驱动 buff/子弹/火墙/延时施法。
//
// # 一次出手的完整链路（§10.1）
//
//	C2S 16000
//	  → CastCommand 校验（主城/暂停/禁攻/CD/目标合法）
//	  → LAUNCH（delay=0）   释放 Buff / 被动 / 时停，**不飘字**
//	  → CAST（delay=前摇）  选目标 → 伤害公式 → 飘字广播 + 结果事件
//
// # 双回路（§10.7 不可合并）
//
//	表现回路：MGR → AOI 广播 HarmBroadcast（客户端即时飘字/特效）
//	结果回路：MGR → 目标主人的 Actor 投递 SkillResultEvent / MonDeadEvent
//	          （玩家 Actor 在自己的事务里回写 HP/Buff/死亡，MGR 不直接改）
//
// # 不变量（§13，逐条对应测试见 docs/RPG战斗系统设计.md）
//
//	1  场景权威态只在 MGR：battle 所有写入口都要求 MGR goroutine；
//	5  死亡必须走统一入口 + 幂等标记：Runtime.dead 兜重复死亡；
//	18 LAUNCH 不飘字，只有 CAST 发 HarmBroadcast；
//	9  主城不结算：Options.NoBattle 在 CastCommand 入口直接拒绝；
//	15 镜像施法白名单：Options.MirrorMaps 缺省为空 = 一律拒绝；
//	21 前摇吃攻速与距离：prefixMS 按 SpeedAtk 与格子距离缩放；
//	22 远程技能在 CAST 后再按格子距离延迟一次才命中。
package battle
