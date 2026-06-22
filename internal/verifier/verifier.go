// Package verifier calls the 090227 API to validate CF proxy IPs.
package verifier

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	defaultAPI = "https://api.090227.xyz/check"
	timeout    = 10 * time.Second
)

// CheckResult is the API response from 090227.xyz/check.
type CheckResult struct {
	Success bool   `json:"success"`
	IP      string `json:"ip"`
	Colo    string `json:"colo"`
	Region  string `json:"region"`
	ASN     string `json:"asn"`
	Country string `json:"country"`
}

// Verify checks a single IP:port via the 090227 API.
// Returns nil if the IP is not a valid CF proxy.
func Verify(ip string, port int) (*CheckResult, error) {
	apiURL := fmt.Sprintf("%s?proxyip=%s:%d", defaultAPI, ip, port)

	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", "https://090227.xyz")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data struct {
		Success bool   `json:"success"`
		IP      string `json:"ip"`
		Colo    string `json:"colo"`
		Region  string `json:"region"`
		ASN     string `json:"asn"`
		Country string `json:"country"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	if !data.Success {
		return nil, nil
	}

	return &CheckResult{
		Success: true,
		IP:      data.IP,
		Colo:    data.Colo,
		Region:  data.Region,
		ASN:     data.ASN,
		Country: data.Country,
	}, nil
}

// BatchResult holds verification results for one IP:port.
type BatchResult struct {
	IP      string `json:"ip"`
	Port    int    `json:"port"`
	Valid   bool   `json:"valid"`
	Colo    string `json:"colo,omitempty"`
	Region  string `json:"region,omitempty"`
	ASN     string `json:"asn,omitempty"`
	Country string `json:"country,omitempty"`
}

// VerifyBatch verifies multiple IP:port pairs concurrently.
func VerifyBatch(targets []string, concurrency int) []BatchResult {
	if concurrency < 1 {
		concurrency = 16
	}

	type job struct {
		target string
		ip     string
		port   int
	}
	type jobResult struct {
		job    job
		result *CheckResult
	}

	jobs := make(chan job, len(targets))
	results := make(chan jobResult, len(targets))

	// Parse targets
	go func() {
		for _, t := range targets {
			ip, port, err := parseTarget(t)
			if err != nil {
				continue
			}
			jobs <- job{t, ip, port}
		}
		close(jobs)
	}()

	// Workers
	for i := 0; i < concurrency; i++ {
		go func() {
			for j := range jobs {
				r, _ := Verify(j.ip, j.port)
				results <- jobResult{j, r}
			}
		}()
	}

	// Collect
	batch := make([]BatchResult, 0, len(targets))
	done := make(chan struct{})
	go func() {
		for range targets {
			r := <-results
			if r.result != nil {
				batch = append(batch, BatchResult{
					IP:      r.job.ip,
					Port:    r.job.port,
					Valid:   true,
					Colo:    r.result.Colo,
					Region:  r.result.Region,
					ASN:     r.result.ASN,
					Country: r.result.Country,
				})
			}
		}
		close(done)
	}()
	<-done

	return batch
}

func parseTarget(s string) (string, int, error) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			ip := s[:i]
			var port int
			_, err := fmt.Sscanf(s[i+1:], "%d", &port)
			if err != nil {
				return "", 0, fmt.Errorf("invalid target: %s", s)
			}
			return ip, port, nil
		}
	}
	return "", 0, fmt.Errorf("invalid target (no port): %s", s)
}
