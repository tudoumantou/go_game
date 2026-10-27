package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	"github.com/stretchr/testify/require"
)

// fakeRoleStore 内存实现：业务逻辑照原样跑，不拉数据库。
//
// 上一版这里用 sqlmock 钉死「INSERT player → INSERT player_attr → INSERT player_item」
// 的语句序列——那是把测试绑在手写 SQL 上：SQL 一移到持久化层，测试就全部失效。
// 现在测试只关心业务契约：校验拦在哪、草稿里装了什么、错误怎么翻译。
type fakeRoleStore struct {
	// findPlayer/findErr FindByAccount 的固定返回
	findPlayer *model.Player
	findErr    error
	// created 记录最后一次 Create 收到的草稿
	created *RoleDraft
	// createID/createErr Create 的固定返回
	createID  int64
	createErr error
	// findCalls 调用次数（验证「先查后建」只查一次）
	findCalls int
}

func (f *fakeRoleStore) FindByAccount(context.Context, int64) (*model.Player, error) {
	f.findCalls++
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.findPlayer, nil
}

func (f *fakeRoleStore) Create(_ context.Context, d RoleDraft) (int64, error) {
	f.created = &d
	return f.createID, f.createErr
}

func newTestService(store RoleStore) *RoleService {
	return NewRoleService(store, log.New("role-service-test", log.WithLevel("error")))
}

// TestBuildInitialItems 覆盖初始背包配置的合并、截断与容错：
//   - 同一道具多行合并；
//   - 数量超过 item.xml 堆叠上限时截断；
//   - 未知道具、非法 ID/数量只跳过，不阻断创角。
func TestBuildInitialItems(t *testing.T) {
	logger := log.New("role-service-test", log.WithLevel("error"))
	got := buildInitialItems(logger, []gamedata.InitBagCfg{
		{Id: 1, Col1: 2001, Col2: 5},   // 新手剑上限 1 → 截断为 1
		{Id: 2, Col1: 1001, Col2: 100}, // 金币
		{Id: 3, Col1: 1001, Col2: 200}, // 与上一行合并 → 300
		{Id: 4, Col1: 9999, Col2: 1},   // item.xml 不存在 → 跳过
		{Id: 5, Col1: 0, Col2: 1},      // 非法道具 ID → 跳过
		{Id: 6, Col1: 1002, Col2: -1},  // 非法数量 → 跳过
	})
	want := []InitItem{{ItemID: 1001, Count: 300}, {ItemID: 2001, Count: 1}}
	require.EqualValues(t, len(want), len(got))
	for i := range want {
		require.EqualValues(t, want[i], got[i])
	}
}

// TestInitialItemsFromConfig 校验线上生效的 init_bag.xml：
// 至少配置一件道具，且每件都在 item.xml 中存在、数量不超过堆叠上限。
func TestInitialItemsFromConfig(t *testing.T) {
	logger := log.New("role-service-test", log.WithLevel("error"))
	items := initialItems(logger)
	require.NotEmpty(t, items, "init_bag.xml 未配置任何初始道具")
	for _, it := range items {
		cfg, ok := gamedata.GetItemCfg(int64(it.ItemID))
		require.True(t, ok)
		require.False(t, it.Count <= 0 || (cfg.Col2 > 0 && it.Count > cfg.Col2), fmt.Sprintf("初始道具 %d 数量 %d 非法（上限 %d）", it.ItemID, it.Count, cfg.Col2))
	}
}

// TestCreateValidatesBeforeStore 名字/职业的纯本地校验必须拦在持久化之前：
// 非法请求不该产生任何存储调用（包括那次「账号是否已有角色」的查询）。
func TestCreateValidatesBeforeStore(t *testing.T) {
	cases := []struct {
		name   string
		role   string
		career int32
		want   error
	}{
		{"太短", "张", 1, ErrNameIllegal},
		{"含空格", "张 三", 1, ErrNameIllegal},
		{"含符号", "张三!", 1, ErrNameIllegal},
		{"超长", "张三张三张三张三张三张三张三张三张", 1, ErrNameIllegal},
		{"职业不存在", "张三", 99, ErrCareerBad},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeRoleStore{findErr: ErrNoRole}
			_, err := newTestService(store).Create(context.Background(), 1001, tc.role, tc.career)
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, store.findCalls, "校验失败不应触达存储")
			require.Nil(t, store.created, "校验失败不应建角色")
		})
	}
}

// TestCreateRejectsSecondRole 账号已有角色时拒绝创角；真实的库故障必须上抛，
// 不能被当成「没角色」放行第二次插入（那会造出双角色脏数据）。
func TestCreateRejectsSecondRole(t *testing.T) {
	store := &fakeRoleStore{findPlayer: &model.Player{Id: 7, AccountId: 1001}}
	_, err := newTestService(store).Create(context.Background(), 1001, "张三", 1)
	require.Error(t, err)
	require.Nil(t, store.created)

	boom := errors.New("db down")
	store = &fakeRoleStore{findErr: boom}
	_, err = newTestService(store).Create(context.Background(), 1001, "张三", 1)
	require.ErrorIs(t, err, boom, "库故障必须上抛，不能当作「没角色」")
	require.Nil(t, store.created)
}

// TestCreatePassesDraft 草稿内容：职业初始属性取自 baseAttrs，
// 初始道具来自 init_bag.xml（已排序），账号与名字原样带入。
func TestCreatePassesDraft(t *testing.T) {
	store := &fakeRoleStore{findErr: ErrNoRole, createID: 10001}
	id, err := newTestService(store).Create(context.Background(), 1001, "  张三  ", 1)
	require.NoError(t, err)
	require.EqualValues(t, 10001, id)
	require.NotNil(t, store.created)

	d := store.created
	require.EqualValues(t, 1001, d.AccountID)
	require.Equal(t, "张三", d.Name, "名字应先去空白")
	require.EqualValues(t, 1, d.Career)
	// 战士基础属性
	require.EqualValues(t, 1000, d.Hp)
	require.EqualValues(t, 100, d.Mp)
	require.EqualValues(t, 50, d.Atk)
	require.EqualValues(t, 30, d.Def)
	require.EqualValues(t, 10, d.Spd)
	require.NotEmpty(t, d.Items, "初始背包不应为空（init_bag.xml 有配置）")
	for i := 1; i < len(d.Items); i++ {
		require.Less(t, d.Items[i-1].ItemID, d.Items[i].ItemID, "初始道具应按 item_id 升序")
	}
}

// TestCreateTranslatesStoreErrors 持久化层的领域错误原样透传（handler 靠
// errors.Is 翻译成协议码），不做二次包装丢失链。
func TestCreateTranslatesStoreErrors(t *testing.T) {
	store := &fakeRoleStore{findErr: ErrNoRole, createErr: ErrNameUsed}
	_, err := newTestService(store).Create(context.Background(), 1001, "张三", 1)
	require.ErrorIs(t, err, ErrNameUsed)
}
