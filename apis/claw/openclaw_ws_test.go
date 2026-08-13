package claw

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/goccy/go-json"
	fiberws "github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	. "treehole_next/models"
	"treehole_next/openclaw"
)

func TestOpenClawEventDispatchAndUnknownEventError(t *testing.T) {
	db := newClawWebSocketTestDB(t)
	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	require.NoError(t, db.Create(&OpenClawInstance{
		UserID: 1,
		State:  string(openclaw.StateReady),
	}).Error)

	status := readOpenClawEvent(t, Client{UserID: 1, IsAuthed: true}, OpenClawEvent{
		Type:      OpenClawInstanceStatusEvent,
		RequestID: "status-1",
	})
	require.Equal(t, OpenClawInstanceStatusEvent, status["type"])
	require.Equal(t, "status-1", status["request_id"])
	statusPayload, ok := status["payload"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(1), statusPayload["instance_id"])
	require.Equal(t, string(openclaw.StateReady), statusPayload["state"])

	unknown := readOpenClawEvent(t, Client{UserID: 1, IsAuthed: true}, OpenClawEvent{
		Type:      "openclaw.unknown",
		RequestID: "unknown-1",
	})
	require.Equal(t, OpenClawEventError, unknown["type"])
	require.Equal(t, "unknown-1", unknown["request_id"])
	require.Equal(t, ErrCodeUnknownType, unknown["error_code"])
}

func TestResolveOpenClawChannelRejectsCrossInstanceSession(t *testing.T) {
	db := newClawWebSocketTestDB(t)
	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	require.NoError(t, db.Create(&ClawSession{
		UserID:        1,
		InstanceID:    10,
		UserSessionID: 1,
		OC_SessionID:  "oc-session-10",
	}).Error)

	_, err := resolveOpenClawChannel(1, 11, openClawChatSendPayload{
		ChannelID: 1,
	})
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)

	_, err = resolveOpenClawChannel(1, 11, openClawChatSendPayload{
		SessionID: "oc-session-10",
	})
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestOpenClawStatusIsIsolatedByAuthenticatedUser(t *testing.T) {
	db := newClawWebSocketTestDB(t)
	previousDB := DB
	DB = db
	t.Cleanup(func() { DB = previousDB })

	require.NoError(t, db.Create(&OpenClawInstance{
		UserID: 1, State: string(openclaw.StateReady),
	}).Error)
	require.NoError(t, db.Create(&OpenClawInstance{
		UserID: 2, State: string(openclaw.StateStopped),
	}).Error)

	userOne := openClawStatusForUser(1)
	userTwo := openClawStatusForUser(2)
	missingUser := openClawStatusForUser(3)

	require.Equal(t, string(openclaw.StateReady), userOne.State)
	require.Equal(t, uint(1), userOne.InstanceID)
	require.Equal(t, string(openclaw.StateStopped), userTwo.State)
	require.Equal(t, uint(2), userTwo.InstanceID)
	require.Equal(t, string(openclaw.StateNotStarted), missingUser.State)
	require.Zero(t, missingUser.InstanceID)
}

func newClawWebSocketTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&OpenClawInstance{},
		&ClawSession{},
		&ClawMessage{},
	))
	return db
}

func readOpenClawEvent(t *testing.T, client Client, event OpenClawEvent) map[string]any {
	t.Helper()
	app := fiber.New()
	app.Get("/", fiberws.New(func(conn *fiberws.Conn) {
		handleOpenClawEvent(conn, &client, mustJSON(event))
	}))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serverDone := make(chan error, 1)
	go func() { serverDone <- app.Listener(listener) }()
	t.Cleanup(func() {
		require.NoError(t, app.Shutdown())
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("test websocket server did not stop")
		}
	})

	url := "ws://" + listener.Addr().String() + "/"
	var conn *websocket.Conn
	for attempt := 0; attempt < 20; attempt++ {
		conn, _, err = websocket.DefaultDialer.Dial(url, http.Header{})
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NoError(t, err)
	defer conn.Close()

	var response map[string]any
	require.NoError(t, conn.ReadJSON(&response))
	return response
}

func mustJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
