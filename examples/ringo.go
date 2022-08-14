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

package main

import (
	"fmt"

	"github.com/fogfish/ring"
)

func main() {
	/*
		create new ring instance with m=64, Q=8, T=8
	*/
	ringo := ring.New(ring.WithM64(), ring.WithQ(8), ring.WithT(8))

	/*
		when all nodes join the topology is following

		ring: m=64, q=8, t=8
		|     [0, ffffffffffffffff]
		|     [ 18.54.73.101 113.181.90.103 102.190.90.78 140.93.207.103 92.106.122.149 ]
		|
		|     0: 1fffffffffffffff ⇒     1  ab26472ec2ed62a [18.54.73.101]
		|     1: 3fffffffffffffff ⇒     0 228ad527296bd2d5 [113.181.90.103]
		|     2: 5fffffffffffffff ⇒     2 5949b7cc2ac07642 [140.93.207.103]
		|     3: 7fffffffffffffff ⇒     3 6c13f457b56728ec [18.54.73.101]
		|     4: 9fffffffffffffff ⇒     0 931fb3cd1fc272eb [18.54.73.101]
		|     5: bfffffffffffffff ⇒     0 a22176d726c38cb5 [102.190.90.78]
		|     6: dfffffffffffffff ⇒     1 d613972f28795b25 [140.93.207.103]
		|     7: ffffffffffffffff ⇒     0 f27d0004a29a8dff [140.93.207.103]
	*/
	ringo.Join("113.181.90.103")
	ringo.Join("102.190.90.78")
	ringo.Join("140.93.207.103")
	ringo.Join("92.106.122.149")
	ringo.Join("18.54.73.101")
	fmt.Printf("==> ring topology:\n%s\n", ringo.Debug())

	/*
		Lookup successor nodes for the key.
		It returns list of primary & handoff nodes

		Primary:
		1. {ffffffffffffffff | 0 - 140.93.207.103}
		2. {ffffffffffffffff | 1 - 18.54.73.101}
		3. {ffffffffffffffff | 0 - 113.181.90.103}

		Handoff:
		- empty
	*/
	primary, handoff := ringo.SuccessorOf(3, "One ring to rule them all")
	fmt.Printf("==> primary nodes: %v\n", primary)
	fmt.Printf("==> handoff nodes: %v\n", handoff)

	/*
		Handoff node and its shards to other
	*/
	ringo.Handoff("18.54.73.101")
	fmt.Println("==> handoff 18.54.73.101")

	/*
		Lookup successor nodes for the key, after handoff

		Primary:
		1. {ffffffffffffffff | 0 - 140.93.207.103}
		2. {ffffffffffffffff | 0 - 113.181.90.103}

		Handoff:
		1. {ffffffffffffffff | 0 - 102.190.90.78}
	*/
	primary, handoff = ringo.SuccessorOf(3, "One ring to rule them all")
	fmt.Printf("==> primary nodes: %v\n", primary)
	fmt.Printf("==> handoff nodes: %v\n", handoff)

	/*
		Permanently leaves the topology

		ring: m=64, q=8, t=8
		|     [0, ffffffffffffffff]
		|     [ 113.181.90.103 102.190.90.78 140.93.207.103 92.106.122.149 ]
		|
		|     0: 1fffffffffffffff ⇒     3 cc2612f4915eef7 [92.106.122.149]
		|     1: 3fffffffffffffff ⇒     0 228ad527296bd2d5 [113.181.90.103]
		|     2: 5fffffffffffffff ⇒     2 5949b7cc2ac07642 [140.93.207.103]
		|     3: 7fffffffffffffff ⇒    -1 0 [140.93.207.103]
		|     4: 9fffffffffffffff ⇒     0 83f2c8982d81d29f [92.106.122.149]
		|     5: bfffffffffffffff ⇒     0 a22176d726c38cb5 [102.190.90.78]
		|     6: dfffffffffffffff ⇒     1 d613972f28795b25 [140.93.207.103]
		|     7: ffffffffffffffff ⇒     0 f27d0004a29a8dff [140.93.207.103]
	*/
	ringo.Leave("18.54.73.101")
	fmt.Printf("==> ring topology after 18.54.73.101 left:\n%s\n", ringo.Debug())
}
