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
	"crypto/sha1"
	"hash"
)

// Option for the ring structure
type Option func(ring *Ring)

// WithM8 configures the ring param m=8, so that ring space is 2^m - 1
func WithM8() Option {
	return func(ring *Ring) { ring.m = 8 }
}

// WithM16 configures the ring param m=16, so that ring space is 2^m - 1
func WithM16() Option {
	return func(ring *Ring) { ring.m = 16 }
}

// WithM32 configures the ring param m=32, so that ring space is 2^m - 1
func WithM32() Option {
	return func(ring *Ring) { ring.m = 32 }
}

// WithM64 configures the ring param m=64, so that ring space is 2^m - 1
func WithM64() Option {
	return func(ring *Ring) { ring.m = 64 }
}

// withM configures the ring param m, number of shards on the ring
func withM(m uint64) Option {
	return func(ring *Ring) { ring.m = m }
}

// WithQ configures the ring param q, number of shards on the ring
func WithQ(q uint64) Option {
	return func(ring *Ring) { ring.q = q }
}

// WithT configures the ring param t, number of tokens to be claimed by the node
func WithT(n uint64) Option {
	return func(ring *Ring) { ring.t = n }
}

// WithHash configures hashing algorithm for the ring
func WithHash(f func() hash.Hash) Option {
	return func(ring *Ring) { ring.hasher = f }
}

// WithRing clones ring configuration into the new instance
func WithRing(r *Ring) Option {
	return func(ring *Ring) {
		ring.m = r.m
		ring.q = r.q
		ring.t = r.t
		ring.hasher = r.hasher
	}
}

// Options turns a list of Option instances into an Option.
func Options(opts ...Option) Option {
	return func(ring *Ring) {
		for _, opt := range opts {
			opt(ring)
		}
	}
}

var (
	M64_Q8_T8 = Options(
		WithM64(),
		WithQ(8),
		WithT(8),
		WithHash(sha1.New),
	)

	M64_Q4096_T256 = Options(
		WithM64(),
		WithQ(4096),
		WithT(256),
		WithHash(sha1.New),
	)
)
