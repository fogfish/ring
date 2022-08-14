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
	"fmt"
	"hash"
	"strings"
)

/*

Ring is consistent hashing data type.
*/
type Ring struct {
	// configuration
	m      uint64           // hash space 2^m - 1
	q      uint64           // number of shards on the ring
	t      uint64           // number of tokens to be claimed by node
	hasher func() hash.Hash // hashing algorithms

	// internal state
	arc    uint64
	hashes Hashes
	nodes  map[string]bool
}

// New creates instances of the ring
func New(opts ...Option) *Ring {
	ring := &Ring{}

	M64_Q8_T8(ring)
	for _, opt := range opts {
		opt(ring)
	}

	//
	ring.arc = ring.segment()

	//
	ring.empty()

	return ring
}

func (ring *Ring) empty() {
	ring.nodes = map[string]bool{}
	ring.hashes = make(Hashes, ring.q)

	for i, addr := range ring.addresses() {
		ring.hashes[i] = Hash{hash: addr, rank: -1}
	}
}

//------------------------------------------------------------------------------
//
// Ring algebra
//
//------------------------------------------------------------------------------

// calculate highest address of the ring
func (ring *Ring) highest() uint64 {
	return uint64(1<<ring.m - 1)
}

// calculate cardinality of rings shard
func (ring *Ring) segment() uint64 {
	return ring.highest()/ring.q + 1
}

// calculate entire address space for the ring
func (ring *Ring) addresses() []uint64 {
	seq := make([]uint64, ring.q)
	for i := uint64(0); i < ring.q; i++ {
		seq[i] = ring.addressShard(i + 1)
	}
	return seq
}

// calculate address on the ring for shard
func (ring *Ring) addressShard(shard uint64) uint64 {
	return (shard * ring.arc) - 1
}

// calculate address on the ring for hash
func (ring *Ring) addressHash(hash []byte) (int, uint64) {
	addr := uint64(hash[0])
	for i := uint64(1); i < uint64(ring.m/8); i++ {
		addr = addr | uint64(hash[i])<<(8*i)
	}

	shard := (addr / ring.arc) % ring.q
	return int(shard), addr
}

// calculate address on the ring for key
// it returns shard id and address of the key
func (ring *Ring) address(key string) (int, uint64) {
	return ring.addressHash(ring.hash(key, nil))
}

// hash the key value
func (ring *Ring) hash(key string, hash []byte) []byte {
	// TODO: hashing w/o memory allocation
	h := ring.hasher()
	h.Write([]byte(key))
	if hash != nil {
		h.Write(hash)
	}

	return h.Sum(nil)
}

//------------------------------------------------------------------------------
//
// Ring interface
//
//------------------------------------------------------------------------------

/*

Join node to the ring. Node claims Q/N shards from the ring.
*/
func (ring *Ring) Join(node string) *Ring {
	if _, exists := ring.nodes[node]; exists {
		ring.nodes[node] = true
		return ring
	}

	var hash []byte

	for rank := 0; rank < int(ring.t); rank++ {
		hash = ring.hash(node, hash)
		shard, addr := ring.addressHash(hash)

		main := ring.hashes[shard]

		switch {
		// shard is not allocated to any one
		case main.addr == 0:
			ring.hashes.update(shard, addr, rank, node)

		// this is a master shard of key (key is shard owner), claim it unconditionally
		case main.rank != 0 && rank == 0:
			ring.hashes.update(shard, addr, rank, node)

		// Key collides with allocated shard, bigger address wins
		case main.rank == rank && main.addr < addr:
			ring.hashes.update(shard, addr, rank, node)

		// Key collides with allocated shard, smaller hash wins
		case main.rank > rank:
			ring.hashes.update(shard, addr, rank, node)
		}
	}

	ring.repair()
	ring.nodes[node] = true

	return ring
}

// repair unallocated shards
func (ring *Ring) repair() {
	if ring.hashes[0].rank == -1 {
		for i := ring.q - 1; i > 0; i-- {
			if ring.hashes[i].rank != -1 {
				ring.hashes.updateNode(0, ring.hashes[i])
				break
			}
		}
	}

	for i := 1; i < int(ring.q); i++ {
		if ring.hashes[i].rank == -1 {
			ring.hashes.updateNode(i, ring.hashes[i-1])
		}
	}
}

/*

Leave node from the ring
*/
func (ring *Ring) Leave(node string) *Ring {
	if _, exists := ring.nodes[node]; !exists {
		return ring
	}

	nodes := ring.nodes
	delete(nodes, node)

	ring.empty()

	for node := range nodes {
		ring.Join(node)
	}

	return ring
}

/*

Handoff node's responsibility.
*/
func (ring *Ring) Handoff(node string) *Ring {
	ring.nodes[node] = false
	return ring
}

/*

SuccessorOf return N distinct nodes to route key.
The list of nodes is split to primary and handoff replicas.

For each node it returns the address of shard hit by the key,
the node identity, the rank of node identity and its address on the ring.
*/
func (ring *Ring) SuccessorOf(n uint64, key string) (Primary, Handoff) {
	shard, _ := ring.address(key)
	coord := ring.hashes[shard]

	last, head := ring.distinctNodes(n, shard)

	primary := ring.primaryNodes(n, coord, head)
	if len(primary) == int(n) {
		return Primary(primary), nil
	}

	hn := int(n) - len(primary)
	handoff := make(Hashes, 0, n)
	for i := 1; i < int(ring.q); i++ {
		hash := ring.hashes[(last+i)%int(ring.q)]

		if ring.nodes[hash.node] && !handoff.contains(hash.node) && !primary.contains(hash.node) {
			handoff = append(handoff, Hash{
				hash: coord.hash,
				addr: hash.addr,
				rank: hash.rank,
				node: hash.node,
			})
		}

		if len(handoff) == hn {
			break
		}
	}

	return Primary(primary), Handoff(handoff)
}

// returns N distinct nodes and position on the ring
func (ring *Ring) distinctNodes(n uint64, fromShard int) (int, Hashes) {
	last := 0
	head := make(Hashes, 0, n)
	for i := 0; i < int(ring.q); i++ {
		last = (fromShard + i) % int(ring.q)
		hash := ring.hashes[last]

		if !head.contains(hash.node) {
			head = append(head, hash)
		}

		if len(head) == int(n) {
			break
		}
	}

	return last, head
}

// build list of primary nodes from list of candidates
func (ring *Ring) primaryNodes(n uint64, coord Hash, hashes Hashes) Hashes {
	primary := make(Hashes, 0, n)
	for _, hash := range hashes {
		if ring.nodes[hash.node] {
			primary = append(primary, Hash{
				hash: coord.hash,
				addr: hash.addr,
				rank: hash.rank,
				node: hash.node,
			})
		}
	}

	return primary
}

/*

Address calculates address of key on the ring
*/
func (ring *Ring) Address(key string) uint64 {
	_, addr := ring.addressHash(ring.hash(key, nil))
	return addr
}

/*

Lookup the address position on the ring
*/
func (ring *Ring) Lookup(addr uint64) Node {
	shard := (addr / ring.arc) % ring.q
	hash := ring.hashes[shard]
	return hash
}

/*

lookup the key position on the ring
*/
func (ring *Ring) LookupKey(key string) Node {
	shard, _ := ring.address(key)
	hash := ring.hashes[shard]
	return hash
}

/*

Before returns list of N predecessors shards for the address.
*/
func (ring *Ring) Before(n uint64, addr uint64) []Node {
	shard := (addr / ring.arc) % ring.q

	return ring.predecessor(min(n, ring.q), int(shard))
}

/*

BeforeKey returns list of N predecessors shards for the key.
*/
func (ring *Ring) BeforeKey(n uint64, key string) []Node {
	shard, _ := ring.address(key)

	return ring.predecessor(min(n, ring.q), shard)
}

func (ring *Ring) predecessor(n uint64, shard int) []Node {
	q := int(ring.q)
	seq := make([]Node, 0, n)

	for i := 0; i < int(ring.q); i++ {
		seq = append(seq, ring.hashes[(q+shard-i)%q])
		if len(seq) == int(n) {
			break
		}
	}

	return seq
}

/*

After returns list of N successors shards for the address.
*/
func (ring *Ring) After(n uint64, addr uint64) []Node {
	shard := (addr / ring.arc) % ring.q

	return ring.successor(min(n, ring.q), int(shard))
}

/*

AfterKey returns list of N successors shards for the key.
*/
func (ring *Ring) AfterKey(n uint64, key string) []Node {
	shard, _ := ring.address(key)

	return ring.successor(min(n, ring.q), shard)
}

func (ring *Ring) successor(n uint64, shard int) []Node {
	seq := make([]Node, 0, n)

	for i := 0; i < int(ring.q); i++ {
		seq = append(seq, ring.hashes[(shard+i)%int(ring.q)])
		if len(seq) == int(n) {
			break
		}
	}

	return seq
}

/*

Size of ring, number of members joined the ring
*/
func (ring *Ring) Size() int {
	return len(ring.nodes)
}

/*

Has return true if key exists in the ring
*/
func (ring *Ring) Has(node string) bool {
	_, exists := ring.nodes[node]
	return exists
}

/*

Members return list of nodes registered at ring
*/
func (ring *Ring) Members() []string {
	nodes := make([]string, 0, len(ring.nodes))
	for node := range ring.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

/*

Nodes return list of nodes and its shards
*/
func (ring *Ring) Nodes() map[string][]Node {
	nodes := map[string][]Node{}
	for node := range ring.nodes {
		nodes[node] = []Node{}
	}

	for _, hash := range ring.hashes {
		nodes[hash.node] = append(nodes[hash.node], hash)
	}

	return nodes
}

/*

Shards returns ring topology and its allocation
*/
func (ring *Ring) Shards() []Node {
	hashes := make([]Node, len(ring.hashes))

	for i, hash := range ring.hashes {
		hashes[i] = hash
	}

	return hashes
}

/*

Debug represents ring to string snapshot
*/
func (ring *Ring) Debug() string {
	buf := strings.Builder{}
	buf.WriteString(fmt.Sprintf("ring: m=%d, q=%d, t=%d\n", ring.m, ring.q, ring.t))
	buf.WriteString(fmt.Sprintf("|     [0, %16x]\n", ring.highest()))
	buf.WriteString("|     [ ")
	for node := range ring.nodes {
		buf.WriteString(node)
		buf.WriteString(" ")
	}
	buf.WriteString("]\n| \n")

	for i := uint64(0); i < uint64(ring.q); i++ {
		hash := ring.hashes[i]

		buf.WriteString(fmt.Sprintf("| %5d", i))
		buf.WriteString(fmt.Sprintf(": %x", hash.hash))
		buf.WriteString(fmt.Sprintf(" ⇒ %5d %x", hash.rank, hash.addr))
		buf.WriteString(fmt.Sprintf(" [%s]", hash.node))
		buf.WriteString("\n")
	}
	return buf.String()
}

//
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}
