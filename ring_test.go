/*

  Copyright 2012 Dmitry Kolesnikov, All Rights Reserved

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

*/

package ring

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net"
	"testing"

	"github.com/fogfish/it"
)

func TestRing(t *testing.T) {
	nodes := []string{
		"113.181.90.103",
		"102.190.90.78",
		"140.93.207.103",
		"92.106.122.149",
		"18.54.73.101",
	}

	q := uint64(8)
	for _, m := range []uint64{8, 16, 32, 64} {
		t.Run(fmt.Sprintf("m.%d", m), func(t *testing.T) {
			r := New(withM(m), WithQ(q))
			for _, node := range nodes {
				r.Join(node)
			}
			shards := r.Shards()
			arc := ((1<<m)-1)/q + 1

			address := func(i int) (uint64, uint64, uint64) {
				hi := arc*uint64(i+1) - 1
				lo := hi - arc + 1
				ra := lo + uint64(rand.Intn(int(arc)))
				return lo, ra, hi
			}

			t.Run("ShardAddress", func(t *testing.T) {
				for i, shard := range shards {
					_, _, hi := address(i)
					it.Ok(t).If(shard.Hash()).Equal(hi)
				}
			})

			t.Run("ShardLookupByLowestAddress", func(t *testing.T) {
				for i, shard := range shards {
					lo, _, _ := address(i)
					node := r.Lookup(lo)
					it.Ok(t).
						If(node.Hash()).Equal(shard.Hash()).
						If(node.Node()).Equal(shard.Node())
				}
			})

			t.Run("ShardLookupByHighestAddress", func(t *testing.T) {
				for i, shard := range shards {
					_, _, hi := address(i)
					node := r.Lookup(hi)
					it.Ok(t).
						If(node.Hash()).Equal(shard.Hash()).
						If(node.Node()).Equal(shard.Node())
				}
			})

			t.Run("ShardLookupByAddress", func(t *testing.T) {
				for i, shard := range shards {
					_, ra, _ := address(i)
					node := r.Lookup(ra)
					it.Ok(t).
						If(node.Hash()).Equal(shard.Hash()).
						If(node.Node()).Equal(shard.Node())
				}
			})

			t.Run("ShardLookupOverflow", func(t *testing.T) {
				for i := range shards {
					_, _, hi := address(i)
					node := r.Lookup(hi + 1)
					edon := shards[uint64(i+1)%q]
					it.Ok(t).
						If(node.Hash()).Equal(edon.Hash()).
						If(node.Node()).Equal(edon.Node())
				}
			})

			t.Run("After", func(t *testing.T) {
				for i, shard := range shards {
					seq := r.After(3, shard.Hash())
					a := shards[uint64(i+0)%q]
					b := shards[uint64(i+1)%q]
					c := shards[uint64(i+2)%q]

					it.Ok(t).
						If(seq[0].Hash()).Equal(a.Hash()).
						If(seq[1].Hash()).Equal(b.Hash()).
						If(seq[2].Hash()).Equal(c.Hash())
				}
			})

			t.Run("Before", func(t *testing.T) {
				for i, shard := range shards {
					seq := r.Before(3, shard.Hash())
					a := shards[uint64(i-0)%q]
					b := shards[uint64(i-1)%q]
					c := shards[uint64(i-2)%q]

					it.Ok(t).
						If(seq[0].Hash()).Equal(a.Hash()).
						If(seq[1].Hash()).Equal(b.Hash()).
						If(seq[2].Hash()).Equal(c.Hash())
				}
			})
		})
	}
}

func TestAllocation(t *testing.T) {
	for x := 1; x <= 10; x++ {
		n := 1 << x
		t.Run(fmt.Sprintf("n.%d", n), func(t *testing.T) {
			d := 1.0 / float64(n)
			q := 4096.0
			// Note: testing a perfect allocation model
			r := New(M64_Q4096_T256, WithT(4096))
			for _, ip := range randKeys(n) {
				r.Join(ip)
			}

			for _, nodes := range r.Nodes() {
				p := float64(len(nodes)) / q
				it.Ok(t).IfTrue(math.Abs(p-d) < 0.01)
			}
		})
	}
}

func TestJoin(t *testing.T) {
	r := New(M64_Q4096_T256)
	seq := randKeys(256)
	for _, ip := range seq {
		r.Join(ip)
	}
	shards := r.Shards()

	for i := 0; i < 10; i++ {
		rand.Shuffle(len(seq), func(i, j int) { seq[i], seq[j] = seq[j], seq[i] })
		r := New(M64_Q4096_T256)
		for _, ip := range seq {
			r.Join(ip)
		}

		for id, shard := range r.Shards() {
			it.Ok(t).
				If(shard.Hash()).Equal(shards[id].Hash()).
				If(shard.Rank()).Equal(shards[id].Rank()).
				If(shard.Node()).Equal(shards[id].Node())
		}
	}
}

func randKey() string {
	buf := make([]byte, 4)
	ip := rand.Uint32()
	binary.LittleEndian.PutUint32(buf, ip)
	return net.IP(buf).String()
}

func randKeys(n int) []string {
	seq := make([]string, n)
	for i := 0; i < n; i++ {
		seq[i] = randKey()
	}
	return seq
}

//
// Benchmark
//

func BenchmarkJoin(b *testing.B) {
	r := New(M64_Q4096_T256)

	for n := 0; n < b.N; n++ {
		node := randKey()
		r.Join(node)
		delete(r.nodes, node)
	}
}

func BenchmarkWhere(b *testing.B) {
	r := New(M64_Q4096_T256)
	for i := 0; i < 100; i++ {
		r.Join(randKey())
	}

	for n := 0; n < b.N; n++ {
		r.LookupKey(randKey())
	}
}

func BenchmarkSuccessors(b *testing.B) {
	r := New(M64_Q4096_T256)
	for i := 0; i < 100; i++ {
		r.Join(randKey())
	}

	for n := 0; n < b.N; n++ {
		r.SuccessorOf(10, randKey())
	}
}
