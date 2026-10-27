// internal/game/service/role_service.go —— 角色创建与查询（进游戏前的会话级操作）
//
// 架构职责：登录服（login）在玩家「进游戏前」的会话级服务，与游戏服内的
// module.Module 是两层不同的东西：
//  1. 不在 Actor 内执行——由 login handler 直接调用，并发安全由 RoleStore
//     的事务 + 唯一键保证（与 module 层的「Actor 串行化免锁」不同）；
//  2. 不过 Persister/Ext——角色是数据库行本身（player / player_attr /
//     player_item 三表），没有快照概念；
//  3. 错误用哨兵 error 表达（ErrNoRole/ErrNameUsed...），由 handler 经
//     errcode 包翻译成协议错误码（对应设计文档 4.10 的 2003-2005）。
//
// 分层边界（本轮整改的重点）：本文件只做**业务规则**——名字/职业校验、
// 初始属性与初始背包的配置解读；一切 SQL 与驱动细节下沉到 RoleStore 的
// 实现（internal/game/player/role_store.go，用 gendb 生成的 CRUD）。
// 因此本文件不 import database/sql，也不持有 *db.DB。
//
// 协作关系：login/repo 负责账号与会话，本服务负责角色域；
// IDTable（cache）登记是 12.5 的可选加速——创建后同步登记，
// 避免对账窗口内把新角色判断成「不存在」。
package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"game_study/pkg/cache"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
)

// 角色服务错误（对应 4.10 错误码 2003-2005）。
//
// 哨兵 error 模式：handler 层用 errors.Is 判定后经 errcode 包翻译成
// 协议错误码；DB 真故障（驱动错误）不走哨兵，由 %w 包装后按
// 「内部错误」处理——「可预期失败」与「真故障」两条通道分离。
//
// 注意 ErrNoRole 与 ErrNameUsed 由 RoleStore 的实现翻译后返回：
// 「行不存在」「唯一键冲突」是数据库事实，翻译成领域语义是持久化层的
// 职责（与 login/repo 把 sql.ErrNoRows 翻译成 ErrNoRole 同型）。
var (
	// ErrNoRole 账号还没有角色（调用方据此走创角流程）
	ErrNoRole = errors.New("no role")
	// ErrNameUsed 角色名撞唯一键（uk_name），转「角色名已被占用」
	ErrNameUsed = errors.New("name already used")
	// ErrNameIllegal 名字不匹配 namePattern（长度/字符集）
	ErrNameIllegal = errors.New("name illegal")
	// ErrCareerBad 职业 id 不在 career.xml（防客户端传任意值）
	ErrCareerBad = errors.New("career not exist")
)

// InitItem 一行初始道具（创角草稿的组成部分）。
type InitItem struct {
	// ItemID 道具配置 id（item.xml 主键）
	ItemID int32
	// Count 数量（已按堆叠上限截断）
	Count int64
}

// RoleDraft 创角草稿：业务算好的全部落库输入。
//
// 为什么用草稿而不是让 store 自己读配置：初始属性来自 baseAttrs、初始背包
// 来自 init_bag.xml——两者都是业务规则（改配置不该改持久化层）。store 只负责
// 「把这份草稿原子地写成三张表的行」，不解释配置。
type RoleDraft struct {
	// AccountID 归属账号
	AccountID int64
	// Name 角色名（已 TrimSpace、已校验）
	Name string
	// Career 职业 id（已校验存在于 career.xml）
	Career int32
	// Hp/Mp/Atk/Def/Spd 职业初始属性（player_attr 一行）
	Hp, Mp, Atk, Def, Spd int32
	// Items 初始背包（已合并、截断、按 item_id 排序）
	Items []InitItem
}

// RoleStore 角色持久化契约（由持久化层实现）。
//
// 为什么由 service 定义接口：消费方定义接口是 Go 的惯例，接口只含本服务
// 真正用到的两个方法，实现方（player.RoleStore）可以自由选择 SQL 形态，
// 单测也只需一个几行的内存假实现，不必拉起 sqlmock。
type RoleStore interface {
	// FindByAccount 返回账号下最早创建的角色；无角色返回 ErrNoRole。
	FindByAccount(ctx context.Context, accountID int64) (*model.Player, error)
	// Create 原子写 player + player_attr + player_item，返回新角色 ID；
	// 角色名撞唯一键返回 ErrNameUsed。
	Create(ctx context.Context, d RoleDraft) (int64, error)
}

// RoleService 角色服务。
type RoleService struct {
	store  RoleStore
	logger *log.Logger
	// idTable 可选：创角后登记 ID 表（12.5）。
	// nil 表示未启用（无 cache 环境）——可选依赖用后置注入而非构造参数，
	// 避免每个调用方都被迫传 nil（与 battle.Module.SetRanker 同型）。
	idTable *cache.IDTable
}

// NewRoleService 创建服务。
func NewRoleService(store RoleStore, logger *log.Logger) *RoleService {
	return &RoleService{store: store, logger: logger}
}

// SetIDTable 注入 ID 表（12.5：创建记录后同步登记，避免对账窗口内判断为「不存在」）。
//
// 后置注入的原因：IDTable 依赖 cache 组件，login 组装层按配置决定是否
// 启用；用 setter 保持 NewRoleService 的签名稳定。
// 登记失败只 Warn（见 Create 内）：ID 表是对账加速缓存，不是权威数据源。
func (s *RoleService) SetIDTable(t *cache.IDTable) { s.idTable = t }

// FindByAccount 查账号下角色；无角色返回 ErrNoRole。
//
// 一账号一角色（P6）：store 用 ORDER BY id LIMIT 1 取最早创建的那一行，
// 脏数据多行时也不会把查询打挂（与 guild.FindByPlayer 同型）。
func (s *RoleService) FindByAccount(ctx context.Context, accountID int64) (*model.Player, error) {
	p, err := s.store.FindByAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Create 创建角色：校验名字/职业 → 事务插入 player + player_attr + 初始背包。
//
// 三层校验的顺序有讲究：先纯本地校验（正则/职业表，零 IO），
// 再查库（一账号一角色），最后才进事务——把便宜且高拦不住的检查
// 前置，DB 压力留给真正合法的请求。
func (s *RoleService) Create(ctx context.Context, accountID int64, name string, career int32) (int64, error) {
	// 先去空白再校验：客户端常把输入框的空格一起提交
	name = strings.TrimSpace(name)
	// 正则 + RuneCount 双保险（见 namePattern 注释）
	if !namePattern.MatchString(name) || utf8.RuneCountInString(name) > 16 {
		return 0, ErrNameIllegal
	}
	// 职业必须存在于 career.xml：防客户端传任意值，否则后面
	// player_attr 会插入一行来源不明的属性
	if _, ok := gamedata.GetCareerCfg(int64(career)); !ok {
		return 0, ErrCareerBad
	}

	// 一个账号只允许一个角色（P6 简化，P7 支持多角色选择）
	// 哨兵读法：err==nil = 已有角色（拒绝）；ErrNoRole = 没角色
	// （继续创角）；其余 err = DB 真故障（上抛，绝不能当成「没角色」
	// 而放行第二次插入——那会造出双角色脏数据）
	if _, err := s.FindByAccount(ctx, accountID); err == nil {
		return 0, fmt.Errorf("account %d already has a role", accountID)
	} else if !errors.Is(err, ErrNoRole) {
		return 0, err
	}

	// 初始道具与属性在事务外算好（配置是进程内快照，热更新不会在
	// 本次创角中途换表）：store 拿到的是完整草稿，事务里不再读配置。
	hp, mp, atk, def, spd := baseAttrs(career)
	draft := RoleDraft{
		AccountID: accountID,
		Name:      name,
		Career:    career,
		Hp:        hp, Mp: mp, Atk: atk, Def: def, Spd: spd,
		Items: initialItems(s.logger),
	}
	roleID, err := s.store.Create(ctx, draft)
	if err != nil {
		return 0, err
	}
	// 登记 ID 表：不阻塞创角返回，失败也只是等下次对账补上
	// 放在事务外是有意的——登记是旁路加速，失败不该回滚已创成的角色；
	// 此时请求 ctx 仍有效（Create 还未返回），可继续沿用
	if s.idTable != nil {
		if err := s.idTable.Add(ctx, "player", roleID); err != nil {
			s.logger.Warn("idtab add player failed", "player_id", roleID, "err", err)
		}
	}

	// 创角审计日志：谁（account）、发了什么职业、发了几件初始道具，
	// 客诉「没收到新手礼包」时这一行就是对账起点
	s.logger.Info("role created", "role_id", roleID, "account_id", accountID, "career", career, "init_items", len(draft.Items))
	return roleID, nil
}

// initialItems 读取 init_bag.xml，返回创角时要写入背包的道具。
//
// 容错策略：配置行非法（ID/数量 <= 0）、道具不存在时只告警跳过，不阻断创角——
// 创角失败的影响远大于少发一件新手道具；错误配置应在启动/发布期通过日志发现。
// 同一道具多行会合并，并按 item.xml 的堆叠上限截断，避免写出非法数量。
// 结果按 item_id 排序，保证写入顺序稳定（便于测试与排查）。
//
// 包一层 buildInitialItems 是为了可测性：配置行由参数注入，单测用内存
// 构造 rows 即可覆盖合并/截断/跳过边界，不依赖 XML 文件。
func initialItems(logger *log.Logger) []InitItem {
	return buildInitialItems(logger, gamedata.InitBagCfgList())
}

// buildInitialItems 与 initialItems 同逻辑，但配置行由调用方传入，便于单测覆盖
// 重复行合并、堆叠上限截断、未知道具跳过等边界。
//
// 两阶段「先合并后输出」：map[int32]int64 作累加器（key 存在性即
// 「见过该道具」），汇总完再一次性转切片——避免在行序上做增量插入。
func buildInitialItems(logger *log.Logger, rows []gamedata.InitBagCfg) []InitItem {
	merged := make(map[int32]int64)
	for _, cfg := range rows {
		itemID, count := int32(cfg.Col1), cfg.Col2
		// 非法行跳过而非报错：与函数级容错策略一致——配置错误在启动期
		// 就该从日志里发现，不该让线上玩家为策划的手误买单
		if itemID <= 0 || count <= 0 {
			logger.Warn("init bag: invalid row", "row_id", cfg.Id, "item_id", itemID, "count", count)
			continue
		}
		// 道具必须存在于 item.xml：init_bag 引用未知道具 = 配置断裂，
		// 插进行会造出玩家背包里的「幽灵道具」（能看不能数）
		item, ok := gamedata.GetItemCfg(int64(itemID))
		if !ok {
			logger.Warn("init bag: item not in item.xml", "row_id", cfg.Id, "item_id", itemID)
			continue
		}
		// 同道具多行累加合并（+= 对零值 key 天然成立——map 取值零值语义）
		merged[itemID] += count
		// 堆叠上限截断：item.Col2 是 item.xml 的堆叠上限列。
		// 截断而非报错，与跳过非法行同一容错哲学——少发好过创角失败
		if item.Col2 > 0 && merged[itemID] > item.Col2 {
			merged[itemID] = item.Col2
		}
	}
	out := make([]InitItem, 0, len(merged))
	// map range 是随机序（Go 规范不保证遍历顺序，且运行时故意随机化）——
	// 直接输出会让每次创角的 SQL 顺序不同，测试难复现、日志难比对，
	// 所以必须显式排序把随机序压成确定序
	for id, n := range merged {
		out = append(out, InitItem{ItemID: id, Count: n})
	}
	// sort.Slice 就地排序；itemID 唯一故不需要稳定排序（sort.SliceStable）
	sort.Slice(out, func(i, j int) bool { return out[i].ItemID < out[j].ItemID })
	return out
}

// baseAttrs 职业初始属性（P6 内置表；P7 改读 attr_formula.xml）。
//
// Go switch 语义：每个 case 自带隐式 break，不会 fall through 到下一个
// case（与 C 相反，不需要显式 break）。
// default 兜底是纵深防御：GetCareerCfg 已校验过职业存在，但若将来
// 配置放行了职业 3 而这里忘了加 case，默认属性能防止造出零属性角色。
func baseAttrs(career int32) (hp, mp, atk, def, spd int32) {
	switch career {
	case 1: // 战士
		return 1000, 100, 50, 30, 10
	case 2: // 法师
		return 600, 300, 80, 15, 12
	default:
		return 800, 150, 40, 20, 10
	}
}

// namePattern 角色名规则：2-16 字符，中英文/数字，禁止空白与特殊符号。
//
// \p{Han} 是 Unicode 属性类：Go 的 regexp 按 UTF-8 码点匹配，
// {2,16} 对汉字数的是「字」不是字节——2 个汉字=6 字节也合法。
// （Create 里的 RuneCountInString>16 是对此的双保险：防将来正则被
// 改成按字节计数的写法时静默失效。）
// TrimSpace 先行：「  龙傲天  」应按去空白后的内容判定。
var namePattern = regexp.MustCompile(`^[\p{Han}A-Za-z0-9]{2,16}$`)
