package harness

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// Op kinds recorded in histories.
const (
	OpGet uint8 = iota
	OpPut
	OpAppend
)

// KVInput is the input of one recorded operation.
type KVInput struct {
	Op    uint8
	Key   string
	Value string
}

// KVOutput is the observed result. A missing key reads as "".
type KVOutput struct {
	Value string
}

// KVModel is the sequential specification of a single-key register with
// Put and Append, partitioned by key (keys are independent, so a history is
// linearizable iff each per-key sub-history is).
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var order []string
		for _, op := range history {
			k := op.Input.(KVInput).Key
			if _, ok := byKey[k]; !ok {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(KVInput)
		switch in.Op {
		case OpGet:
			return output.(KVOutput).Value == st, st
		case OpPut:
			return true, in.Value
		default:
			return true, st + in.Value
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(KVInput)
		switch in.Op {
		case OpGet:
			return fmt.Sprintf("get(%q) -> %q", in.Key, output.(KVOutput).Value)
		case OpPut:
			return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
		default:
			return fmt.Sprintf("append(%q, %q)", in.Key, in.Value)
		}
	},
}
