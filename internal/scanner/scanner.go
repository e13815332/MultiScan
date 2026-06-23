// Package scanner provides Cloudflare proxy detection logic via TLS certificate check.
package scanner

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"
)

// ScanResult holds the result of scanning a single IP:port.
type ScanResult struct {
	IP    string `json:"ip"`
	Port  int    `json:"port"`
	IsCF  bool   `json:"is_cf"`
	Delay int    `json:"delay_ms"` // TLS handshake time in ms
}

// ScannerConfig controls scanning behaviour.
type ScannerConfig struct {
	Concurrency int
	ConnectTO   time.Duration
	TargetPort  int
	SNI         string
}

// DefaultConfig returns sensible defaults for CF proxy scanning.
func DefaultConfig() ScannerConfig {
	return ScannerConfig{
		Concurrency: 100,
		ConnectTO:   1500 * time.Millisecond,
		TargetPort:  443,
		SNI:         "cloudflare.com",
	}
}

// CheckProxy tests a single IP to see if it's a Cloudflare proxy.
// Uses TLS handshake + certificate CN/SAN check — no HTTP layer.
func CheckProxy(ip string, port int, cfg ScannerConfig) ScanResult {
	start := time.Now()
	target := net.JoinHostPort(ip, fmt.Sprintf("%d", port))

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: cfg.ConnectTO}, "tcp", target, &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         cfg.SNI,
	})
	if err != nil {
		return ScanResult{IP: ip, Port: port, IsCF: false, Delay: int(time.Since(start).Milliseconds())}
	}
	defer conn.Close()

	delay := int(time.Since(start).Milliseconds())
	certs := conn.ConnectionState().PeerCertificates
	for _, cert := range certs {
		if strings.Contains(cert.Subject.CommonName, "cloudflare.com") {
			return ScanResult{IP: ip, Port: port, IsCF: true, Delay: delay}
		}
		for _, name := range cert.DNSNames {
			if strings.Contains(name, "cloudflare.com") {
				return ScanResult{IP: ip, Port: port, IsCF: true, Delay: delay}
			}
		}
	}
	return ScanResult{IP: ip, Port: port, IsCF: false, Delay: delay}
}
