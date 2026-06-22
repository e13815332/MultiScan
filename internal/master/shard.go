package master

import (
	"math"
	"net"
	"sort"
)

type cidrHost struct {
	CIDR  string
	Hosts int64
}

// DistributeCIDRs distributes a list of CIDRs across shardCount shards,
// using greedy bin-packing (largest networks first, assign to smallest shard).
// Returns one CIDR list per shard.
func DistributeCIDRs(cidrs []string, shardCount int) [][]string {
	if shardCount <= 1 {
		return [][]string{cidrs}
	}

	// Parse CIDRs and calculate host counts
	entries := make([]cidrHost, 0, len(cidrs))
	for _, c := range cidrs {
		_, network, err := net.ParseCIDR(c)
		if err != nil {
			continue
		}
		ones, bits := network.Mask.Size()
		hostBits := bits - ones
		if hostBits < 0 {
			continue
		}
		// Total usable IPs in this CIDR
		hosts := int64(1) << uint(hostBits)
		if hostBits >= 32 {
			hosts = math.MaxInt64 // /0 = everything
		}
		entries = append(entries, cidrHost{CIDR: c, Hosts: hosts})
	}

	if len(entries) == 0 {
		// Fallback: distribute raw CIDRs round-robin
		shards := make([][]string, shardCount)
		for i, c := range cidrs {
			shards[i%shardCount] = append(shards[i%shardCount], c)
		}
		return shards
	}

	// Sort by host count descending (largest networks first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Hosts > entries[j].Hosts
	})

	// Greedy bin-packing: each CIDR goes to shard with smallest current total
	shards := make([][]string, shardCount)
	totals := make([]int64, shardCount)

	for _, e := range entries {
		// Find shard with smallest total
		minIdx := 0
		for i := 1; i < shardCount; i++ {
			if totals[i] < totals[minIdx] {
				minIdx = i
			}
		}
		shards[minIdx] = append(shards[minIdx], e.CIDR)
		totals[minIdx] += e.Hosts
	}

	return shards
}
