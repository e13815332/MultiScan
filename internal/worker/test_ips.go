package worker

// getDefaultTestIPs returns a small set of known CF proxy IPs for testing.
// These are verified proxy IPs from previous scans.
func getDefaultTestIPs() []string {
	return []string{
		"188.95.70.211",
		"45.80.188.138",
		"92.118.190.93",
		"109.94.170.104",
		"109.94.170.148",
		"104.16.0.1",
		"104.16.1.1",
		"104.16.2.1",
		"104.16.3.1",
		"104.16.4.1",
	}
}
