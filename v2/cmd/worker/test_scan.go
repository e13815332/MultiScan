//go:build ignore

package main

import (
	"fmt"
	"github.com/e13815332/multiscan/internal/scanner"
)

func main() {
	cfg := scanner.DefaultConfig()
	transport := scanner.SharedHTTPTransport()

	ips := []string{"142.250.80.46", "1.1.1.1", "8.8.8.8"}
	for _, ip := range ips {
		r := scanner.CheckProxyHTTP(transport, ip, 443, cfg)
		fmt.Printf("%s:443 -> IsCF=%v Delay=%dms\n", ip, r.IsCF, r.Delay)
	}

	// Also test an actual CF result from the scan
	fmt.Println("---")
	cfIPs := []string{"103.117.100.192", "103.117.100.27"}
	for _, ip := range cfIPs {
		r := scanner.CheckProxyHTTP(transport, ip, 443, cfg)
		fmt.Printf("%s:443 -> IsCF=%v Delay=%dms\n", ip, r.IsCF, r.Delay)
	}
}
