package sandbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectPortsRequiresDenyNet(t *testing.T) {
	_, err := ParseArgs([]string{"--connect-port=8080", "--", "x"})
	assert.ErrorContains(t, err, "deny-net")

	inv, err := ParseArgs([]string{"--deny-net", "--connect-port=8080", "--", "x"})
	require.NoError(t, err)
	assert.Equal(t, []uint16{8080}, inv.Spec.ConnectPorts)

	assert.Error(t, Spec{ConnectPorts: []uint16{80}}.Validate())
	assert.NoError(t, Spec{DenyNet: true, ConnectPorts: []uint16{80}}.Validate())
	assert.NoError(t, Spec{}.Validate())
}

func TestConnectPortRange(t *testing.T) {
	for _, v := range []string{"0", "65536", "-1", "http"} {
		t.Run(v, func(t *testing.T) {
			_, err := ParseArgs([]string{"--deny-net", "--connect-port=" + v, "--", "x"})
			assert.Error(t, err)
		})
	}
	assert.Error(t, Spec{DenyNet: true, ConnectPorts: []uint16{0}}.Validate(), "port 0 is not a port")
}

// ConnectPorts is part of the network confinement: it does not make a spec
// confining on its own (Validate rejects it without DenyNet anyway).
func TestConnectPortsKeepsConfines(t *testing.T) {
	assert.True(t, Spec{DenyNet: true, ConnectPorts: []uint16{80}}.Confines())
}
