package sandbox

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// downloaderReport builds a downloader self-test report: every check of
// [DownloaderChecks] blocked, the allowed connect open, then the overrides
// (name, outcome pairs).
func downloaderReport(abi int, overrides ...string) []byte {
	m := map[string]string{}
	for _, c := range DownloaderChecks {
		m[c] = OutcomeBlocked
	}
	m[CheckTCPConnectAllowed] = OutcomeOpen
	for i := 0; i+1 < len(overrides); i += 2 {
		if overrides[i+1] == "" {
			delete(m, overrides[i])
			continue
		}
		m[overrides[i]] = overrides[i+1]
	}
	b, _ := json.Marshal(selfTestReport{ControlOK: true, ABI: abi, Checks: m})
	return b
}

func TestProbeDownloaderLevels(t *testing.T) {
	cases := map[string]struct {
		out   []byte
		level api.SandboxLevel
		err   bool
	}{
		"full":                  {downloaderReport(6), api.SandboxFull, false},
		"full at abi 4":         {downloaderReport(4), api.SandboxFull, false},
		"mptcp unsupported":     {downloaderReport(6, CheckMPTCPConnect, OutcomeUnsupported), api.SandboxFull, false},
		"abi 3 is never full":   {downloaderReport(3), api.SandboxFSOnly, false},
		"other port open":       {downloaderReport(3, CheckTCPConnect, OutcomeOpen), api.SandboxFSOnly, false},
		"allowed port blocked":  {downloaderReport(6, CheckTCPConnectAllowed, OutcomeBlocked), api.SandboxFSOnly, false},
		"allowed port error":    {downloaderReport(6, CheckTCPConnectAllowed, "error: connection refused"), api.SandboxFSOnly, false},
		"udp open":              {downloaderReport(6, CheckUDPSend, OutcomeOpen), api.SandboxFSOnly, false},
		"listen open":           {downloaderReport(6, CheckTCPListen, OutcomeOpen), api.SandboxFSOnly, false},
		"missing listen check":  {downloaderReport(6, CheckTCPListen, ""), "", true},
		"unix open":             {downloaderReport(6, CheckUnixConnect, OutcomeOpen), api.SandboxFSOnly, false},
		"fs open":               {downloaderReport(6, CheckFSRead, OutcomeOpen), api.SandboxNone, false},
		"missing allowed check": {downloaderReport(6, CheckTCPConnectAllowed, ""), "", true},
		"missing tcp check":     {downloaderReport(6, CheckTCPConnect, ""), "", true},
		"no control":            {[]byte(`{"checks":{}}`), "", true},
		"not json":              {[]byte(`oops`), "", true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			res, err := interpretDownloader(c.out)
			if c.err {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.level, res.Level)
		})
	}
}

func TestDownloaderResultLists(t *testing.T) {
	res, err := interpretDownloader(downloaderReport(6, CheckUDPSend, OutcomeOpen))
	require.NoError(t, err)
	assert.Contains(t, res.Open(), CheckUDPSend)
	assert.NotContains(t, res.Open(), CheckTCPConnectAllowed, "an open allowed connect is not an escape")
	assert.NotContains(t, res.Blocked(), CheckTCPConnectAllowed)

	res, err = interpretDownloader(downloaderReport(6, CheckTCPConnectAllowed, OutcomeBlocked))
	require.NoError(t, err)
	assert.Contains(t, res.Open(), CheckTCPConnectAllowed, "a blocked allowed connect is a failure to report")
}
