package kv

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/HridhayP/strata/proto/kvpb"
)

// Caller delivers a request to one server. Implemented over gRPC and over
// the simulated network.
type Caller interface {
	Do(ctx context.Context, server int, req *kvpb.Request) (*kvpb.Response, error)
}

// Clerk is a client of one replica group. A Clerk issues one operation at a
// time; use one Clerk per concurrent client.
type Clerk struct {
	servers []int
	group   string
	call    Caller
	id      uint64
	seq     uint64
	leader  int // index into servers
	// AttemptTimeout bounds each RPC attempt before trying another server.
	AttemptTimeout time.Duration
}

// NewClerk returns a client for the group served by servers.
func NewClerk(servers []int, group string, call Caller) *Clerk {
	return &Clerk{
		servers:        servers,
		group:          group,
		call:           call,
		id:             rand.Uint64(),
		AttemptTimeout: 500 * time.Millisecond,
	}
}

// ID returns the client's unique identifier.
func (c *Clerk) ID() uint64 { return c.id }

// Get returns the value of key and whether it exists.
func (c *Clerk) Get(ctx context.Context, key string) (string, bool, error) {
	resp, err := c.do(ctx, &kvpb.Request{Op: kvpb.Op_GET, Key: key})
	if err != nil {
		return "", false, err
	}
	return string(resp.Value), resp.Err == kvpb.Err_OK, nil
}

// Put sets key to value.
func (c *Clerk) Put(ctx context.Context, key, value string) error {
	c.seq++
	_, err := c.do(ctx, &kvpb.Request{Op: kvpb.Op_PUT, Key: key, Value: []byte(value), ClientId: c.id, Seq: c.seq})
	return err
}

// Append appends value to key's current value.
func (c *Clerk) Append(ctx context.Context, key, value string) error {
	c.seq++
	_, err := c.do(ctx, &kvpb.Request{Op: kvpb.Op_APPEND, Key: key, Value: []byte(value), ClientId: c.id, Seq: c.seq})
	return err
}

// do retries the request until a leader answers or ctx expires. Retrying a
// write is safe because (client ID, seq) deduplicates it.
func (c *Clerk) do(ctx context.Context, req *kvpb.Request) (*kvpb.Response, error) {
	req.Group = c.group
	tried := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		resp, err := c.call.Do(actx, c.servers[c.leader], req)
		cancel()
		if err == nil && (resp.Err == kvpb.Err_OK || resp.Err == kvpb.Err_NO_KEY) {
			return resp, nil
		}
		// Follow the leader hint when it names a known server; otherwise
		// rotate through the group.
		next := (c.leader + 1) % len(c.servers)
		if err == nil && resp.LeaderHint >= 0 {
			for i, s := range c.servers {
				if s == int(resp.LeaderHint) && i != c.leader {
					next = i
				}
			}
		}
		c.leader = next
		tried++
		if tried%len(c.servers) == 0 {
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
}
