package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFailureStageValid(t *testing.T) {
	assert.True(t, StageRateLimited.Valid())
	assert.True(t, StagePreWork.Valid())
	assert.False(t, StageLoop.Valid())
	assert.False(t, FailureStage("bogus").Valid())
}
