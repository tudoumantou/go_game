// internal/game/module/itemid.go —— 跨模块共享的道具 ID
//
// 为什么需要这个文件：道具 ID 是**数据事实**（配置表
// configs/gamedata/xml/item.xml 里的一行），不是某个模块的私有知识。
// 它此前在六个地方各写一遍——shop/equip/activity 叫 itemDiamond、
// partner 叫 itemCoin、skill 叫 upgradeItemID，quest 直接写裸字面量 1001。
// 五处注释互相引用（「与 shop/skill/quest 共用 1001」），形成一圈只能靠
// 人记住的契约：改 ID 时漏改一处不会有编译错误，只会表现为「强化扣错
// 东西」或「任务发不出奖励」——而这两类症状都不会指向常量定义。
//
// 更糟的是名字与事实相反：item.xml 里 1001 是「金币」，五处注释却写
// 「钻石」。名字错误的常量比裸字面量更危险——读代码的人会相信它。
//
// skill.go 里曾有一句注释为这种重复辩护：「各模块分别声明常量而不是
// 抽公共包，避免模块间耦合」。这个理由站不住：耦合的不是模块，是
// 同一个道具 ID；各写一份并不消除依赖，只是把依赖从编译期挪到了
// 运行时（靠人保证六份相等）。真正的解耦是让唯一那份定义不依赖任何模块。
package module

// ItemGold 金币的道具 ID（item.xml: id=1001, name=金币）。
//
// 唯一权威。新增货币时在这里加一行，并同步 configs/gamedata/xml/item.xml：
// 常量给服务端编译期引用，配置给客户端展示与掉率表，改 ID 必须两边一起改。
const ItemGold int32 = 1001
