package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deweysasser/whisper-network/internal/api"
)

// TestValidateFactsNamesFirstBadField: with several bad fields, the error
// names the first in field order, every time.
func TestValidateFactsNamesFirstBadField(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", 201)
	for range 20 {
		f := api.WorkerFacts{Hostname: "h", OS: long, Version: long, YtDlpVersion: long}
		err := validateFacts(&f)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "os must be")
	}
}
