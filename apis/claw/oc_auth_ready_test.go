package claw

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsChannelAuthenticated(t *testing.T) {
	restore := backupOpenClawRegistry()
	t.Cleanup(restore)

	require.False(t, IsChannelAuthenticated(1))

	client := &OcClient{}
	require.NoError(t, ocRegistry.Register(client, 1, 100))
	require.True(t, IsChannelAuthenticated(1))
	require.False(t, IsChannelAuthenticated(2))
}

func TestIsChannelAuthenticatedIgnoresNotYetAuthedClient(t *testing.T) {
	restore := backupOpenClawRegistry()
	t.Cleanup(restore)

	// Register forces IsAuthed=true, so the not-yet-authed state is set up by
	// manipulating the registry directly, as the auth handshake would leave it.
	ocRegistry.mu.Lock()
	if ocRegistry.byUser[1] == nil {
		ocRegistry.byUser[1] = make(map[uint]*OcClient)
	}
	ocRegistry.byUser[1][101] = &OcClient{UserID: 1, InstanceID: 101, IsAuthed: false}
	ocRegistry.mu.Unlock()

	require.False(t, IsChannelAuthenticated(1))
}

// backupOpenClawRegistry swaps the package registry for a fresh one and
// returns a cleanup that restores the original contents.
func backupOpenClawRegistry() func() {
	ocRegistry.mu.Lock()
	defer ocRegistry.mu.Unlock()
	clients, byUser := ocRegistry.clients, ocRegistry.byUser
	ocRegistry.clients = make(map[uint]*OcClient)
	ocRegistry.byUser = make(map[int]map[uint]*OcClient)
	return func() {
		ocRegistry.mu.Lock()
		defer ocRegistry.mu.Unlock()
		ocRegistry.clients, ocRegistry.byUser = clients, byUser
	}
}
