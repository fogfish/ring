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
)

// Node representation
type Node interface {
	Hash() uint64
	Rank() int
	Node() string
}

// Hash value on the ring
type Hash struct {
	hash uint64 // consistent hash
	addr uint64 // address
	rank int    // rand of the address
	node string // identifier
}

func (hash Hash) Hash() uint64 { return hash.hash }
func (hash Hash) Rank() int    { return hash.rank }
func (hash Hash) Node() string { return hash.node }
func (hash Hash) String() string {
	return fmt.Sprintf("{%x | %d - %s}",
		hash.hash, hash.rank, hash.node)
}

// List of Hashes
type Hashes []Hash

func (hashes Hashes) update(shard int, addr uint64, rank int, node string) {
	h := hashes[shard]
	h.addr = addr
	h.rank = rank
	h.node = node
	hashes[shard] = h
}

func (hashes Hashes) updateNode(shard int, hash Hash) {
	h := hashes[shard]
	h.node = hash.node
	hashes[shard] = h
}

func (hashes Hashes) contains(node string) bool {
	for _, x := range hashes {
		if x.node == node {
			return true
		}
	}

	return false
}

// List of primary hashes/nodes
type Primary Hashes

// List of handoff hashes/nodes
type Handoff Hashes
