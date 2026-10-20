// flusher 是独立的 Tier-2 玩家状态合批落库进程，不承载客户端业务。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/writebehind"

	// 落库进程不导入业务模块，靠这里注册各模块的 Persister，
	// 否则框架会拒绝写入未注册的模块状态（防静默丢数据）。
	_ "game_study/internal/game/module/register"
)

func main() {
	// flusher 不读独立配置：与游戏服共用 Redis/MySQL/Flush 参数，
	// 保证两侧对「批量大小、间隔」的认知一致（Tier-2 同库同参）
	path := flag.String("config", "configs/game/game-dev.yaml", "复用游戏服 Redis/MySQL/Flush 配置")
	flag.Parse()
	logger := log.New("flusher")
	if err := run(*path, logger); err != nil {
		logger.Error("flusher stopped", "err", err)
		os.Exit(1)
	}
}

func run(path string, logger *log.Logger) error {
	// ① 配置（与游戏服同源；环境变量注入规则见 19.1）
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	// Redis 是 Tier-2 的任务队列（写穿透的脏数据先落 Redis 再由此进程合批刷盘），
	// 地址为空说明配置错误，必须显式失败而不是连本地默认库
	if len(cfg.Redis.Addrs) == 0 {
		return fmt.Errorf("flusher requires explicit Redis addresses")
	}
	// ② MySQL：合批落库的目标库（与游戏服同库，见 FlatPersister 设计）
	database, err := db.New(db.Config{DSN: cfg.MySQL.DSN, MaxOpen: cfg.MySQL.MaxOpenConns, MaxIdle: cfg.MySQL.MaxIdleConns})
	if err != nil {
		return err
	}
	defer database.Close()
	// ③ Redis：取任务队列的连接（defer 保证停机时归还）
	rdb, err := cache.New(cache.RedisConfig{Addrs: cfg.Redis.Addrs, Password: cfg.Redis.Password, DB: cfg.Redis.DB, PoolSize: cfg.Redis.PoolSize, MinIdle: cfg.Redis.MinIdleConn})
	if err != nil {
		return err
	}
	defer rdb.Close()
	// ④ 信号上下文：SIGINT/SIGTERM 取消后 worker 停止取新任务，
	// 已取出未落库的任务保留在 Redis（重启后继续），不丢数据
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// ⑤ 刷盘间隔来自配置；解析失败直接退出而非静默用默认值（避免刷盘频率失控）
	interval, err := config.ParseDuration(cfg.Flush.Interval)
	if err != nil {
		return err
	}
	// ⑥ writebehind worker：按 partition 扫描 Redis 任务队列，合批写 MySQL；
	//    失败仅记警告并保留任务，靠下一轮重试（见 pkg/writebehind）
	worker := writebehind.NewWorker(writebehind.NewStore(rdb.GetClient()), database.DB, cfg.Flush.BatchRows)
	logger.Info("writebehind worker started", "partitions", writebehind.Partitions, "interval", interval)
	return worker.Run(ctx, interval, func(err error) { logger.Warn("flush failed; Redis task retained", "err", err) })
}
