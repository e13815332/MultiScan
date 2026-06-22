// Package scanner provides Cloudflare proxy detection logic,
// adapted from the cf-scanner project (github.com/e13815332/ASNIPtest).
package scanner

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Default transport used for scanning. Reused across requests for efficiency.
var defaultTransport = &http.Transport{
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "cloudflare.com",
	},
	DialContext: (&net.Dialer{
		Timeout:   1500 * time.Millisecond,
		KeepAlive: -1,
	}).DialContext,
	MaxIdleConns:        0,
	MaxIdleConnsPerHost: 0,
	IdleConnTimeout:     1 * time.Second,
	DisableKeepAlives:   true,
}

// ScanResult holds the result of scanning a single IP:port.
type ScanResult struct {
	IP     string `json:"ip"`
	Port   int    `json:"port"`
	IsCF   bool   `json:"is_cf"`
	Status string `json:"status"`   // HTTP status code or error
	CFRay  string `json:"cf_ray"`   // CF-RAY header if present
	Delay  int    `json:"delay_ms"` // response time in ms
}

// ScannerConfig controls scanning behaviour.
type ScannerConfig struct {
	Concurrency   int
	ConnectTO     time.Duration
	TotalTO       time.Duration
	TargetPort    int
	SNI           string
	Host          string
}

// DefaultConfig returns sensible defaults for CF proxy scanning.
func DefaultConfig() ScannerConfig {
	return ScannerConfig{
		Concurrency: 100,
		ConnectTO:   1500 * time.Millisecond,
		TotalTO:     2 * time.Second,
		TargetPort:  443,
		SNI:         "cloudflare.com",
		Host:        "www.cloudflare.com",
	}
}

// CheckProxy tests a single IP to see if it's a Cloudflare proxy.
func CheckProxy(ip string, port int, cfg ScannerConfig) ScanResult {
	start := time.Now()
	target := net.JoinHostPort(ip, fmt.Sprintf("%d", port))

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         cfg.SNI,
		},
		DialContext: (&net.Dialer{
			Timeout:   cfg.ConnectTO,
			KeepAlive: -1,
		}).DialContext,
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: 0,
		IdleConnTimeout:     1 * time.Second,
		DisableKeepAlives:   true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.TotalTO,
	}

	req, _ := http.NewRequest("GET", "https://"+target+"/", nil)
	req.Host = cfg.Host
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Close = true

	resp, err := client.Do(req)
	if err != nil {
		return ScanResult{IP: ip, Port: port, IsCF: false, Status: "error", Delay: int(time.Since(start).Milliseconds())}
	}
	defer resp.Body.Close()

	delay := int(time.Since(start).Milliseconds())
	serverHeader := resp.Header.Get("Server")
	cfRay := resp.Header.Get("CF-RAY")

	isCF := serverHeader == "cloudflare" || cfRay != ""
	status := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if isCF {
		if serverHeader == "cloudflare" {
			status += " server=cloudflare"
		}
		if cfRay != "" {
			status += " cf-ray=" + cfRay[:min(len(cfRay), 30)]
		}
	}

	return ScanResult{
		IP:     ip,
		Port:   port,
		IsCF:   isCF,
		Status: status,
		CFRay:  cfRay,
		Delay:  delay,
	}
}
