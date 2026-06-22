// Package masscan wraps the masscan CLI tool for port scanning.
package masscan

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// MasscanResult represents a single discovered host:port.
type MasscanResult struct {
	IP   string
	Port int
}

// Config for masscan execution.
type Config struct {
	Rate     int      // packets per second
	Ports    []int    // target ports
	CIDRs    []string // CIDR ranges to scan
	IPs      []string // individual IPs to scan (alternative to CIDRs)
	Timeout  int      // max seconds to run
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Rate:    15000,
		Ports:   []int{443, 8443},
		Timeout: 30,
	}
}

// MasscanError indicates masscan was not found or failed.
type MasscanError struct {
	Err      error
	NotFound bool // true if masscan binary is missing
}

func (e *MasscanError) Error() string {
	return fmt.Sprintf("masscan: %v", e.Err)
}

// Run executes masscan and returns discovered hosts.
// Returns MasscanError with NotFound=true if masscan is not installed.
func Run(cfg Config) ([]MasscanResult, error) {
	// Check if masscan exists
	_, err := exec.LookPath("masscan")
	if err != nil {
		return nil, &MasscanError{Err: fmt.Errorf("masscan not found in PATH"), NotFound: true}
	}

	// Create temp directory for input/output
	tmpDir, err := os.MkdirTemp("", "multiscan-masscan-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write IPs/CIDRs to input file
	inputFile := filepath.Join(tmpDir, "ips.txt")
	if len(cfg.IPs) > 0 {
		if err := writeLines(inputFile, cfg.IPs); err != nil {
			return nil, fmt.Errorf("write input: %w", err)
		}
	} else if len(cfg.CIDRs) > 0 {
		if err := writeLines(inputFile, cfg.CIDRs); err != nil {
			return nil, fmt.Errorf("write input: %w", err)
		}
	} else {
		return nil, fmt.Errorf("no IPs or CIDRs provided")
	}

	// Build port string
	var portStrs []string
	for _, p := range cfg.Ports {
		portStrs = append(portStrs, fmt.Sprintf("%d", p))
	}
	portArg := strings.Join(portStrs, ",")

	outputFile := filepath.Join(tmpDir, "output.txt")

	// Build command
	args := []string{
		"-iL", inputFile,
		"-p", portArg,
		"--rate", fmt.Sprintf("%d", cfg.Rate),
		"-oL", outputFile,
	}

	cmd := exec.Command("masscan", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// masscan returns non-zero for rate limits and warnings, but may still have results
		// Only fail if output file doesn't exist
		if _, statErr := os.Stat(outputFile); statErr != nil {
			return nil, fmt.Errorf("masscan failed: %v\nOutput: %s", err, string(output))
		}
	}

	// Parse output
	return parseMasscanOutput(outputFile)
}

// parseMasscanOutput parses the masscan -oL output format.
// Format: open tcp PORT IP TIMESTAMP
func parseMasscanOutput(path string) ([]MasscanResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open output: %w", err)
	}
	defer f.Close()

	var results []MasscanResult
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}

		// Format: open tcp PORT IP TIMESTAMP
		if fields[0] != "open" || fields[1] != "tcp" {
			continue
		}

		port, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		ip := fields[3]

		results = append(results, MasscanResult{IP: ip, Port: port})
	}

	return results, scanner.Err()
}

func writeLines(path string, lines []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, line := range lines {
		if _, err := fmt.Fprintln(f, line); err != nil {
			return err
		}
	}
	return nil
}
