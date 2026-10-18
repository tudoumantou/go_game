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
)

func main() {
	path := flag.String("config", "configs/game/game-dev.yaml", "复用游戏服 Redis/MySQL/Flush 配置")
	flag.Parse()
	logger := log.New("flusher")
	if err := run(*path, logger); err != nil {
		logger.Error("flusher stopped", "err", err)
		os.Exit(1)
	}
}

func run(path string, logger *log.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if len(cfg.Redis.Addrs) == 0 {
		return fmt.Errorf("flusher requires explicit Redis addresses")
	}
	database, err := db.New(db.Config{DSN: cfg.MySQL.DSN, MaxOpen: cfg.MySQL.MaxOpenConns, MaxIdle: cfg.MySQL.MaxIdleConns})
	if err != nil {
		return err
	}
	defer database.Close()
	rdb, err := cache.New(cache.RedisConfig{Addrs: cfg.Redis.Addrs, Password: cfg.Redis.Password, DB: cfg.Redis.DB, PoolSize: cfg.Redis.PoolSize, MinIdle: cfg.Redis.MinIdleConn})
	if err != nil {
		return err
	}
	defer rdb.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	interval, err := config.ParseDuration(cfg.Flush.Interval)
	if err != nil {
		return err
	}
	worker := writebehind.NewWorker(writebehind.NewStore(rdb.GetClient()), database.DB, cfg.Flush.BatchRows)
	logger.Info("writebehind worker started", "partitions", writebehind.Partitions, "interval", interval)
	return worker.Run(ctx, interval, func(err error) { logger.Warn("flush failed; Redis task retained", "err", err) })
}
