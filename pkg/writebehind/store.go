package writebehind

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrMiss     = errors.New("player cache missing")
	ErrConflict = errors.New("player cache version conflict")
)

type Store struct{ rdb redis.UniversalClient }

func NewStore(rdb redis.UniversalClient) *Store { return &Store{rdb: rdb} }
func (s *Store) stateKey(id int64) string {
	return fmt.Sprintf("wb:{player-%d}:state:%d", Partition(id), id)
}
func (s *Store) dirtyKey(part int) string { return fmt.Sprintf("wb:{player-%d}:dirty", part) }

// Lua keys 使用同一 hash tag，可在 Redis Cluster 中原子执行。
var commitScript = redis.NewScript(`
local version=redis.call('HGET',KEYS[1],'version')
if not version or version~=ARGV[1] then return -1 end
local queueType=redis.call('TYPE',KEYS[2]).ok
if queueType~='none' and queueType~='zset' then return redis.error_reply('invalid dirty queue type') end
local next=redis.call('HINCRBY',KEYS[1],'version',1)
redis.call('HSET',KEYS[1],'data',ARGV[2])
redis.call('ZADD',KEYS[2],'NX',ARGV[4],ARGV[3])
return next
`)

func (s *Store) Commit(ctx context.Context, state Snapshot, expected int64) (int64, error) {
	if err := state.Validate(); err != nil {
		return 0, err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return 0, err
	}
	version, err := commitScript.Run(ctx, s.rdb, []string{s.stateKey(state.PlayerID), s.dirtyKey(Partition(state.PlayerID))}, expected, string(raw), state.PlayerID, time.Now().UnixMilli()).Int64()
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, ErrConflict
	}
	return version, nil
}

var seedScript = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==1 then return 0 end
if redis.call('ZSCORE',KEYS[2],ARGV[3]) then return redis.error_reply('missing dirty snapshot; cannot recover from stale SQL') end
redis.call('HSET',KEYS[1],'version',ARGV[1],'data',ARGV[2])
return 1
`)

func (s *Store) Seed(ctx context.Context, state Snapshot, version int64) (Record, error) {
	if err := state.Validate(); err != nil {
		return Record{}, err
	}
	if version < 0 {
		return Record{}, fmt.Errorf("invalid version")
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return Record{}, err
	}
	if _, err = seedScript.Run(ctx, s.rdb, []string{s.stateKey(state.PlayerID), s.dirtyKey(Partition(state.PlayerID))}, version, string(raw), state.PlayerID).Result(); err != nil {
		return Record{}, err
	}
	return s.Load(ctx, state.PlayerID)
}

func (s *Store) Load(ctx context.Context, id int64) (Record, error) {
	values, err := s.rdb.HGetAll(ctx, s.stateKey(id)).Result()
	if err != nil {
		return Record{}, err
	}
	if len(values) == 0 {
		return Record{}, ErrMiss
	}
	version, err := strconv.ParseInt(values["version"], 10, 64)
	if err != nil || version < 0 {
		return Record{}, fmt.Errorf("invalid cached version")
	}
	var state Snapshot
	if err := json.Unmarshal([]byte(values["data"]), &state); err != nil {
		return Record{}, fmt.Errorf("decode player cache: %w", err)
	}
	if state.PlayerID != id {
		return Record{}, fmt.Errorf("cached player mismatch")
	}
	if err := state.Validate(); err != nil {
		return Record{}, err
	}
	return Record{Version: version, State: state}, nil
}

func (s *Store) Pending(ctx context.Context, partition, limit int) ([]int64, error) {
	if partition < 0 || partition >= Partitions || limit <= 0 {
		return nil, fmt.Errorf("invalid queue parameters")
	}
	members, err := s.rdb.ZRange(ctx, s.dirtyKey(partition), 0, int64(limit)-1).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(members))
	for _, member := range members {
		id, err := strconv.ParseInt(member, 10, 64)
		if err != nil || id <= 0 || Partition(id) != partition {
			return nil, fmt.Errorf("invalid pending player %q", member)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

var ackScript = redis.NewScript(`
if redis.call('HGET',KEYS[1],'version')==ARGV[1] then return redis.call('ZREM',KEYS[2],ARGV[2]) end
return 0
`)

func (s *Store) Ack(ctx context.Context, id, version int64) error {
	return ackScript.Run(ctx, s.rdb, []string{s.stateKey(id), s.dirtyKey(Partition(id))}, version, id).Err()
}
