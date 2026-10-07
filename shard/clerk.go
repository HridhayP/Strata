package shard

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/HridhayP/strata/kv"
	"github.com/HridhayP/strata/proto/ctrlpb"
	"github.com/HridhayP/strata/proto/kvpb"
)

// Clerk is a client of the sharded store. It caches the latest config,
// routes each key to the group owning its shard, and refreshes the config
// when a group says it no longer owns the shard. One operation at a time.
type Clerk struct {
	ctrl   *CtrlClerk
	call   kv.Caller
	id     uint64
	seq    uint64
	cfg    *ctrlpb.Config
	leader map[int64]int
	// AttemptTimeout bounds each RPC attempt.
	AttemptTimeout time.Duration
}

// NewClerk returns a sharded-store client.
func NewClerk(ctrl *CtrlClerk, call kv.Caller) *Clerk {
	return &Clerk{ctrl: ctrl, call: call, id: rand.Uint64(), leader: map[int64]int{}, AttemptTimeout: 500 * time.Millisecond}
}

// Get returns the value of key and whether it exists.
func (ck *Clerk) Get(ctx context.Context, key string) (string, bool, error) {
	resp, err := ck.do(ctx, &kvpb.Request{Op: kvpb.Op_GET, Key: key})
	if err != nil {
		return "", false, err
	}
	return string(resp.Value), resp.Err == kvpb.Err_OK, nil
}

// Put sets key to value.
func (ck *Clerk) Put(ctx context.Context, key, value string) error {
	ck.seq++
	_, err := ck.do(ctx, &kvpb.Request{Op: kvpb.Op_PUT, Key: key, Value: []byte(value), ClientId: ck.id, Seq: ck.seq})
	return err
}

// Append appends value to key.
func (ck *Clerk) Append(ctx context.Context, key, value string) error {
	ck.seq++
	_, err := ck.do(ctx, &kvpb.Request{Op: kvpb.Op_APPEND, Key: key, Value: []byte(value), ClientId: ck.id, Seq: ck.seq})
	return err
}

func (ck *Clerk) refresh(ctx context.Context) error {
	cfg, err := ck.ctrl.Query(ctx, -1)
	if err != nil {
		return err
	}
	ck.cfg = cfg
	return nil
}

func (ck *Clerk) do(ctx context.Context, req *kvpb.Request) (*kvpb.Response, error) {
	shard := kv.KeyShard(req.Key)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if ck.cfg == nil {
			if err := ck.refresh(ctx); err != nil {
				return nil, err
			}
		}
		gid := ck.cfg.Shards[shard]
		servers := ck.cfg.Groups[gid].GetIds()
		req.Group = kv.GroupName(gid)
		for i := 0; i < len(servers); i++ {
			pos := (ck.leader[gid] + i) % len(servers)
			actx, cancel := context.WithTimeout(ctx, ck.AttemptTimeout)
			resp, err := ck.call.Do(actx, int(servers[pos]), req)
			cancel()
			if err != nil {
				continue
			}
			if resp.Err == kvpb.Err_OK || resp.Err == kvpb.Err_NO_KEY {
				ck.leader[gid] = pos
				return resp, nil
			}
			if resp.Err == kvpb.Err_WRONG_GROUP {
				break
			}
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if err := ck.refresh(ctx); err != nil {
			return nil, err
		}
	}
}
