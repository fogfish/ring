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

package analysis_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/fogfish/ring"
	"github.com/montanaflynn/stats"
)

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

// 98% fill is achieved with Nodes x Tokens = 4xQ
func TestFactorClaimed(t *testing.T) {
	rand.Seed(time.Now().Unix())
	nn := []int{32, 64, 128, 256, 512, 1024}
	qq := []uint64{ /*1024, 2048,*/ 4096}
	tt := []uint64{32, 64, 128, 256, 512, 1024, 2048, 4096}
	ex := 6
	out := strings.Builder{}
	out.WriteString("n")
	for _, t := range tt {
		out.WriteString(fmt.Sprintf(",%d", t))
	}

	for _, q := range qq {
		for _, n := range nn {
			out.WriteString(fmt.Sprintf("\n%d", n))
			for _, t := range tt {
				data := make([]float64, ex)
				for i := 0; i < 6; i++ {
					data[i] = estimateFactorClaimed(n, q, t)
				}
				e, _ := stats.Mean(data)
				// s, _ := stats.StandardDeviation(data)
				out.WriteString(fmt.Sprintf(",%.2f", e))
			}
		}
	}
	ioutil.WriteFile("claimed.csv", []byte(out.String()), 0777)
}

func estimateFactorClaimed(n int, q, t uint64) float64 {
	r := ring.New(
		ring.M64_Q4096_T256,
		ring.WithQ(q),
		ring.WithT(t),
	)
	for _, node := range randKeys(n) {
		r.Join(node)
	}

	claimed := 0
	for _, shard := range r.Shards() {
		if shard.Rank() != -1 {
			claimed++
		}
	}

	return 100.0 * float64(claimed) / float64(q)
}

// 16+ nodes is the best handover effect
func TestFactorHandover(t *testing.T) {
	rand.Seed(time.Now().Unix())
	q := uint64(4096)
	n := 256
	x := uint64(64)

	out := strings.Builder{}
	out.WriteString("n,q,f\n")

	r := ring.New(
		ring.WithM64(),
		ring.WithQ(q),
		ring.WithT(x),
	)

	r.Join(randKey())
	shards := r.Shards()

	for i := 1; i <= n; i++ {
		r.Join(randKey())

		c := uint64(0)
		for i, shard := range r.Shards() {
			if shards[i].Node() != shard.Node() {
				c++
			}
		}
		shards = r.Shards()
		f := float64(c) / float64(q) * 100

		out.WriteString(fmt.Sprintf("%d,%d,%.2f\n", i, c, f))
	}
	ioutil.WriteFile("handover.csv", []byte(out.String()), 0777)
}

func TestFactorLoadBalancing(t *testing.T) {
	rand.Seed(time.Now().Unix())
	s := 1024000
	n := 16
	x := 4

	r := ring.New(
		ring.WithM64(),
		ring.WithQ(4096),
		ring.WithT(64),
	)

	for _, node := range randKeys(n) {
		r.Join(node)
	}

	data := make([]map[string]float64, x)
	for i := 0; i < x; i++ {
		data[i] = map[string]float64{}
	}

	for i := 0; i < s; i++ {
		for k, v := range r.AfterKey(uint64(x), randKey()) {
			data[k][v.Node()]++
		}
	}

	for r, d := range data {
		seq := []float64{}
		for _, v := range d {
			seq = append(seq, v/float64(s)*100)
		}

		ex, _ := stats.Mean(seq)
		sd, _ := stats.StandardDeviation(seq)
		p2, _ := stats.Percentile(seq, 25.0)
		p9, _ := stats.Percentile(seq, 99.0)

		fmt.Printf("r=%d | %.2f %.2f %.2f %.2f\n", r, p2, ex, sd, p9)
	}
}

func TestFactorLoadBalancingReplica(t *testing.T) {
	rand.Seed(time.Now().Unix())
	s := 1024000
	n := 16
	x := 4

	r := ring.New(
		ring.WithM64(),
		ring.WithQ(4096),
		ring.WithT(64),
	)

	for _, node := range randKeys(n) {
		r.Join(node)
	}

	data := make([]map[string]float64, x)
	for i := 0; i < x; i++ {
		data[i] = map[string]float64{}
	}

	for i := 0; i < s; i++ {
		primary, _ := r.SuccessorOf(uint64(x), randKey())
		for k, v := range primary {
			data[k][v.Node()]++
		}
	}

	for r, d := range data {
		seq := []float64{}
		for _, v := range d {
			seq = append(seq, v/float64(s)*100)
		}

		ex, _ := stats.Mean(seq)
		sd, _ := stats.StandardDeviation(seq)
		p2, _ := stats.Percentile(seq, 25.0)
		p9, _ := stats.Percentile(seq, 99.0)

		fmt.Printf("r=%d | %.2f %.2f %.2f %.2f\n", r, p2, ex, sd, p9)
	}
}
