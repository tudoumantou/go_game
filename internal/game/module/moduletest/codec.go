// internal/game/module/moduletest/codec.go —— 持久化编解码的统一检查
//
// 背景：各模块的 ToRow/FromRow 现在由 tools/gencodec 从 DDL + db tag 生成，
// 手写版本已经删掉。生成器只保证「照 tag 搬字段」，不保证两件事：
//
//  1. 往返是否丢字段（生成器把 map 序列化成 JSON 列，键名由 json tag 决定；
//     一旦谁把 tag 改成小写，旧快照就读不出来，而编译与单测都不会报）；
//  2. 空行读回后容器字段是否为 nil。`&Data{}` 的 map/slice 是 nil，
//     业务里 d.Records[id] = v 会 panic；nil 切片 Marshal 成 null，
//     与空切片的 [] 在数据库里是两种内容，对账脚本会当成两种状态。
//
// 这两件事只能靠「拿一份真实形态的样本走一遍」发现，所以在测试里做，
// 而不是在生成器里做——生成器不该知道业务语义。
package moduletest

import (
	"reflect"
	"testing"
)

// CodecChecks 对一对 ToRow/FromRow 做往返与空行检查。
//
// 泛型参数由调用方实例化（Row 是 model.Xxx，Data 是模块私有结构），
// 所以这里不需要 import 任何业务模块——否则会和模块测试构成 import cycle。
func CodecChecks[Row, Data any](
	t *testing.T,
	newData func() *Data,
	toRow func(int64, *Data) (*Row, error),
	fromRow func(*Row) (*Data, error),
	sample *Data,
) {
	t.Helper()

	row, err := toRow(7, sample)
	if err != nil {
		t.Fatalf("ToRow 失败：%v", err)
	}
	out, err := fromRow(row)
	if err != nil {
		t.Fatalf("FromRow 失败：%v", err)
	}
	if !reflect.DeepEqual(sample, out) {
		t.Fatalf("往返不相等（字段丢了或被改写）：\n  写入: %#v\n  读回: %#v", sample, out)
	}

	// 空行 = 该玩家从未写过这份状态，必须得到「可安全写入的空状态」。
	var zero Row
	empty, err := fromRow(&zero)
	if err != nil {
		t.Fatalf("FromRow(空行) 失败：%v", err)
	}
	if empty == nil {
		t.Fatal("FromRow(空行) 返回 nil")
	}
	// 生成器在给了 new= 时用 newData() 起手，这里同时守住「new 没退化」：
	// 若哪天有人把 @codec 的 new= 删了，&Data{} 会让本检查立刻红。
	if want := newData(); !reflect.DeepEqual(empty, want) {
		// 用 %#v 而不是 %+v：nil map 与空 map 的 %+v 都是 map[]，看不出差别，
		// 而这里的失败几乎总是「某个容器字段是 nil」。
		t.Fatalf("空行读回与 newData() 不一致（检查 @codec 是否漏了 new=）：\n  读回   : %#v\n  newData: %#v", empty, want)
	}
	assertNoNilContainer(t, reflect.ValueOf(empty).Elem(), reflect.TypeOf(*empty).Name())
}

// assertNoNilContainer 递归检查结构体里的 map/slice 字段不为 nil。
func assertNoNilContainer(t *testing.T, v reflect.Value, path string) {
	t.Helper()
	switch v.Kind() {
	case reflect.Map, reflect.Slice:
		if v.IsNil() {
			t.Errorf("%s 为 nil：空状态应给空容器（nil map 写入 panic，nil 切片落库成 null）", path)
		}
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			assertNoNilContainer(t, v.Elem(), path)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if !f.IsExported() {
				continue
			}
			assertNoNilContainer(t, v.Field(i), path+"."+f.Name)
		}
	}
}

// JSONColumn 锁定一列的存储格式（字节级）。
//
// 为什么需要它：ToRow/FromRow 用同一对函数往返，所以「改错 json tag」在
// 往返测试里永远能过——写进去和读出来是一致的，只是与**库里的历史数据**
// 不一致。而 json.Unmarshal 对未知字段不报错，结果是旧快照被静默读成零值：
// 连续天数归零、活动进度清零，没有任何日志。
//
// loginreward 的注释里记着这个坑曾经真实发生过（Ext 里存的是 model 结构的
// 大写键名），这条断言就是为了让下一次踩坑变成测试红。
func JSONColumn(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("存储格式变了（旧快照会读成零值且不报错）：\n  实际: %s\n  期望: %s", got, want)
	}
}
