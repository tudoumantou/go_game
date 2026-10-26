# game_study 工程入口（与设计文档 19.3 对齐）
#   生成体系：make gen = gen-proto + gen-data + gen-db（CI 强制一致性卡点，见 16.4）
GO      ?= go
BIN     := bin
VERSION := $(shell git describe --tags --always 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help gen gen-proto gen-msgid gen-check gen-data gen-db gen-codec new-module keygen db-init db-migrate db-seed build test test-race test-integration lint fmt vet dev run-all \
        monitoring smoke loadtest bench drill-crash drill-suite clean

help: ## 查看全部命令
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[32m%-12s\033[0m %s\n", $$1, $$2}'

gen: gen-proto gen-msgid gen-data gen-db gen-codec ## 一键生成全部代码

gen-proto: ## proto → pkg/pb（buf，需先安装 buf）
	cd api/proto && buf generate

gen-msgid: ## proto 注解 // @msg → pkg/codec/msgids_gen.go（消息号注册表）
	$(GO) run ./tools/genmsgid

gen-check: ## 校验生成物与源码一致（生成到临时目录逐文件比对，与工作区状态无关）
	@$(GO) run ./tools/genmsgid -check
	@$(GO) run ./tools/gencodec -check
	@set -e; tmp=$$(mktemp -d); trap 'rm -rf $$tmp' EXIT; \
	 $(GO) run ./tools/gendata -in configs/gamedata/xml -out $$tmp/gamedata >/dev/null; \
	 $(GO) run ./tools/gendb -in sql/schema -out $$tmp/model >/dev/null; \
	 for f in $$tmp/gamedata/*_gen.go $$tmp/gamedata/data.go; do \
	   cmp -s "$$f" "pkg/gamedata/$$(basename $$f)" || { echo "不一致: pkg/gamedata/$$(basename $$f)（运行 make gen）"; exit 1; }; \
	 done; \
	 for f in $$tmp/model/*_gen.go; do \
	   cmp -s "$$f" "pkg/model/$$(basename $$f)" || { echo "不一致: pkg/model/$$(basename $$f)（运行 make gen）"; exit 1; }; \
	 done; \
	 echo "生成物一致"

gen-data: ## XML 游戏数据 → pkg/gamedata（全量覆盖 *_gen.go）
	$(GO) run ./tools/gendata -in configs/gamedata/xml -out pkg/gamedata

gen-db: ## DDL → pkg/model（覆盖 *_gen.go，仅新建 *_model.go）
	$(GO) run ./tools/gendb -in sql/schema -out pkg/model

gen-codec: ## DDL + 业务结构 db tag → <模块>/persist_gen.go（列↔字段双向校验）
	$(GO) run ./tools/gencodec -schema sql/schema -model pkg/model -modules internal/game/module

keygen: ## 生成网关握手 RSA 密钥对（输出 deploy/keys，私钥不入库）
	$(GO) run ./tools/keygen -out deploy/keys

db-migrate: ## 已有库增量迁移（ARGS='--status' 只看状态，ARGS='--baseline' 标记基线）
	@./scripts/db-migrate.sh $(ARGS)

db-init: ## DDL → MySQL 建表（首次部署）
	./scripts/db-init.sh

db-seed: ## sql/seed → 初始数据（道具/等级等安全表 + 测试账号）
	./scripts/db-seed.sh

new-module: ## 生成模块骨架（NAME=mailbox [MSGS=MailboxListReq:MailboxListResp,...] [TABLE=player_mailbox]）
	@test -n "$(NAME)" || (echo "用法: make new-module NAME=mailbox [MSGS=Req:Resp,...] [TABLE=player_mailbox]"; exit 1)
	@go run ./tools/newmodule -name=$(NAME) -msgs="$(MSGS)" -table="$(TABLE)"

build: ## 交叉编译六进程（五个服务 + map）→ bin/
	./scripts/build.sh

test: ## 单元测试（纯逻辑，无外部依赖；集成测试见 test-integration）
	$(GO) test ./...

test-race: ## 竞态检测（Actor / 落库 / 玩家核心包）
	$(GO) test -race ./pkg/actor ./pkg/flush ./internal/game/player

lint: ## golangci-lint + buf lint + gen 一致性检查
	./scripts/lint.sh

fmt: ## go fmt 全量
	$(GO) fmt ./...

vet: ## go vet 全量
	$(GO) vet ./...

dev: ## 仅起中间件（etcd/redis/mysql/kafka）
	docker compose -f deploy/docker-compose.yaml up -d etcd redis mysql kafka

run-all: ## 本地跑五个服务 + map（需先 make dev && make db-init && make db-seed）
	@./scripts/run-all.sh

test-integration: ## 集成测试（testcontainers 起真实中间件）
	$(GO) test -tags=integration -timeout 20m ./test/integration/

monitoring: ## 起 Prometheus + Grafana（http://localhost:3000，admin/admin）
	LOCAL_UID=$$(id -u) LOCAL_GID=$$(id -g) docker compose -f deploy/docker-compose.yaml up -d prometheus grafana
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"

smoke: ## E2E 五步冒烟（需要五服务已启动）
	@./scripts/smoke.sh

loadtest: ## 网关连接压测（默认 1000 连接；10W 需 ulimit -n 200000）
	$(GO) run ./tools/loadtest -mode conn -n $(or $(N),1000) -hold $(or $(HOLD),30s)

bench: ## 压测并生成报告（login/biz/conn，产物在 reports/）
	@./scripts/bench.sh

drill-crash: ## 故障演练：kill -9 游戏服后校验数据零丢失
	@./scripts/drill-crash.sh

drill-suite: ## 故障演练套件：MySQL/Redis/Kafka/etcd 注入与恢复
	@./scripts/drill-suite.sh

clean: ## 清理构建产物
	rm -rf $(BIN)

.PHONY: docker-up docker-down docker-logs

docker-up: ## Docker 构建并启动完整游戏后端（自动生成缺失密钥、首次建表）
	./scripts/docker-up.sh

docker-down: ## 停止 Docker 服务，保留数据库与密钥
	docker compose -f deploy/docker-compose.yaml down

docker-logs: ## 查看 Docker 服务日志
	docker compose -f deploy/docker-compose.yaml logs -f --tail=100
