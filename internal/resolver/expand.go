package resolver

import (
	"math/rand"
	"net"
)

// ExpandCIDR generates random IP samples from a list of CIDR prefixes.
// maxIPs caps the total number of IPs generated.
func ExpandCIDR(prefixes []string, maxIPs int) []string {
	if len(prefixes) == 0 {
		return nil
	}

	// Calculate how many IPs to take from each CIDR
	target := maxIPs / len(prefixes)
	if target < 1 {
		target = 1
	}

	ips := make(map[string]bool)
	for _, cidr := range prefixes {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}

		ones, bits := network.Mask.Size()
		// Skip /32 (single IP) or huge ranges (>/16 — too many)
		hostBits := bits - ones
		if hostBits > 16 {
			hostBits = 16 // cap at /16 for safety
		}
		totalHosts := 1 << uint(hostBits)
		if totalHosts < 2 {
			continue // /32
		}

		// Sample randomly
		for i := 0; i < target && len(ips) < maxIPs; i++ {
			ip := randomIPInNetwork(network, totalHosts)
			if ip != nil {
				ips[ip.String()] = true
			}
		}
	}

	result := make([]string, 0, len(ips))
	for ip := range ips {
		result = append(result, ip)
	}
	return result
}

// randomIPInNetwork generates a random IP within a given network.
func randomIPInNetwork(network *net.IPNet, totalHosts int) net.IP {
	ip := make(net.IP, len(network.IP))
	copy(ip, network.IP)

	// Random host offset (skip network and broadcast addresses if small range)
	offset := rand.Intn(totalHosts - 1) + 1

	// Add offset to IP
	for j := len(ip) - 1; j >= 0 && offset > 0; j-- {
		sum := int(ip[j]) + offset
		ip[j] = byte(sum & 0xFF)
		offset = sum >> 8
	}

	// Make sure it's still in the network
	if !network.Contains(ip) {
		return nil
	}
	return ip
}
