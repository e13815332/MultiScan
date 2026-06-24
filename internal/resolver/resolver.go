// Package resolver resolves AS numbers to CIDR prefixes via the RIPEStat API.
package resolver

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// RIPEStat response for announced-prefixes.
type ripeResponse struct {
	Data struct {
		Prefixes []struct {
			Prefix string `json:"prefix"`
			Timelines []struct {
				Starttime string `json:"starttime"`
				Endtime   string `json:"endtime"`
			} `json:"timelines"`
		} `json:"prefixes"`
	} `json:"data"`
}

var httpClient = &http.Client{Timeout: 15 * time.Second}

// ResolveASN returns all announced IPv4 prefixes for an ASN.
// asn can be "AS13335" or "13335".
func ResolveASN(asn string) ([]string, error) {
	// Normalize: strip "AS" prefix if present
	if len(asn) > 2 && (asn[:2] == "AS" || asn[:2] == "as") {
		asn = asn[2:]
	}

	url := fmt.Sprintf("https://stat.ripe.net/data/announced-prefixes/data.json?resource=AS%s", asn)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("ripe API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ripe API returned status %d", resp.StatusCode)
	}

	var data ripeResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("ripe API decode failed: %w", err)
	}

	var prefixes []string
	for _, p := range data.Data.Prefixes {
		// Only include IPv4 (contains ".")
		if containsDot(p.Prefix) {
			prefixes = append(prefixes, p.Prefix)
		}
	}

	if len(prefixes) == 0 {
		return nil, fmt.Errorf("no IPv4 prefixes found for AS%s", asn)
	}

	return prefixes, nil
}

// ResolveMultipleASNs resolves multiple ASNs concurrently. Failed ASNs are skipped.
func ResolveMultipleASNs(asns []string) (map[string][]string, error) {
	type result struct {
		asn      string
		prefixes []string
		err      error
	}

	ch := make(chan result, len(asns))
	for _, asn := range asns {
		go func(a string) {
			p, err := ResolveASN(a)
			ch <- result{a, p, err}
		}(asn)
	}

	results := make(map[string][]string)
	var failed []string
	for range asns {
		r := <-ch
		if r.err != nil {
			log.Printf("[resolver] Skipping AS%s (%v)", r.asn, r.err)
			failed = append(failed, r.asn)
			continue
		}
		results[r.asn] = r.prefixes
	}

	if len(results) == 0 && len(failed) > 0 {
		return nil, fmt.Errorf("all %d ASNs failed resolution", len(asns))
	}

	return results, nil
}

func containsDot(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return true
		}
	}
	return false
}
