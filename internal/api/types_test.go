package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailureStageValid(t *testing.T) {
	assert.True(t, StageRateLimited.Valid())
	assert.True(t, StagePreWork.Valid())
	assert.False(t, StageLoop.Valid())
	assert.False(t, FailureStage("bogus").Valid())
}

func TestFetchAdminWireKeys(t *testing.T) {
	b, err := json.Marshal(FetchPolicy{EgressKey: "e", Rules: []string{"1/3m"}})
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	for _, k := range []string{"key", "site", "egress_key", "rules", "concurrency", "backoff_initial", "backoff_cap", "updated_by", "updated_at"} {
		assert.Contains(t, m, k)
	}
	b, _ = json.Marshal(FetchPolicy{})
	assert.NotContains(t, string(b), "egress_key")

	b, _ = json.Marshal(FetchCooldown{})
	m = nil
	require.NoError(t, json.Unmarshal(b, &m))
	for _, k := range []string{"egress_key", "site", "until", "level", "manual", "set_by", "reason"} {
		assert.Contains(t, m, k)
	}
	b, _ = json.Marshal(SetCooldownRequest{})
	m = nil
	require.NoError(t, json.Unmarshal(b, &m))
	for _, k := range []string{"egress_key", "site", "duration", "reason"} {
		assert.Contains(t, m, k)
	}
}
