package claw

import (
	"errors"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/contrib/websocket"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"

	. "treehole_next/models"
)

// OcClient is one authenticated OpenClaw connection.
type OcClient struct {
	Conn       *websocket.Conn
	UserID     int
	InstanceID uint
	IsAuthed   bool
	mu         sync.Mutex
	LastPong   int64
}

type OpenClawConnectionRegistry struct {
	mu      sync.RWMutex
	clients map[uint]*OcClient
	byUser  map[int]map[uint]*OcClient
}

func NewOpenClawConnectionRegistry() *OpenClawConnectionRegistry {
	return &OpenClawConnectionRegistry{
		clients: make(map[uint]*OcClient),
		byUser:  make(map[int]map[uint]*OcClient),
	}
}

func (r *OpenClawConnectionRegistry) Register(client *OcClient, userID int, instanceID uint) error {
	if r == nil || client == nil || userID <= 0 || instanceID == 0 {
		return errors.New("invalid OpenClaw connection registration")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if previous := r.clients[instanceID]; previous != nil && previous != client {
		_ = previous.Conn.Close()
	}
	client.UserID = userID
	client.InstanceID = instanceID
	client.IsAuthed = true
	r.clients[instanceID] = client
	if r.byUser[userID] == nil {
		r.byUser[userID] = make(map[uint]*OcClient)
	}
	r.byUser[userID][instanceID] = client
	return nil
}

func (r *OpenClawConnectionRegistry) Remove(client *OcClient) {
	if r == nil || client == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current := r.clients[client.InstanceID]; current != client {
		return
	}
	delete(r.clients, client.InstanceID)
	if instances := r.byUser[client.UserID]; instances != nil {
		delete(instances, client.InstanceID)
		if len(instances) == 0 {
			delete(r.byUser, client.UserID)
		}
	}
}

func (r *OpenClawConnectionRegistry) Get(userID int, instanceID uint) (*OcClient, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	client := r.clients[instanceID]
	if client == nil || client.UserID != userID || !client.IsAuthed {
		return nil, false
	}
	return client, true
}

var ocRegistry = NewOpenClawConnectionRegistry()

// IsChannelAuthenticated reports whether the user has any authenticated
// per-instance OpenClaw connection in the registry.
func IsChannelAuthenticated(userID int) bool {
	ocRegistry.mu.RLock()
	defer ocRegistry.mu.RUnlock()
	for _, client := range ocRegistry.byUser[userID] {
		if client.IsAuthed {
			return true
		}
	}
	return false
}

// HandleOpenClawWebSocket handles one per-instance OpenClaw connection.
func HandleOpenClawWebSocket(c *websocket.Conn) {
	client := &OcClient{
		Conn:     c,
		IsAuthed: false,
		LastPong: time.Now().UnixMilli(),
	}

	// 定期向 OpenClaw 网关连接发送心跳（每 30 秒）
	pingDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ping := PingMessage{
					Type:      MessageTypePing,
					Timestamp: time.Now().UnixMilli(),
					Version:   "1.0",
				}
				client.mu.Lock()
				err := c.WriteJSON(ping)
				client.mu.Unlock()
				if err != nil {
					log.Err(err).Msg("[Claw-OC] send ping failed; closing oc connection")
					// 发送失败，主动关闭连接以触发上层清理
					_ = c.Close()
					return
				}
			case <-pingDone:
				return
			}
		}
	}()

	defer func() {
		ocRegistry.Remove(client)
		// 停止 ping 协程并关闭连接
		close(pingDone)
		c.Close()
	}()

	var rawMsg json.RawMessage
	for {
		err := c.ReadJSON(&rawMsg)
		if err != nil {
			log.Err(err).Msg("[Claw-OC] Read error")
			break
		}

		var base BaseMessage
		if err := json.Unmarshal(rawMsg, &base); err != nil {
			sendOcError(c, ErrCodeUnknownType, "消息格式错误", "", "")
			continue
		}
		log.Info().Msgf("[Claw-OC] recv type=%s raw=%s", base.Type, string(rawMsg))

		switch base.Type {
		case MessageTypeAuth:
			handleOcAuth(c, client, rawMsg)
		case MessageTypeMessage:
			if !client.IsAuthed {
				sendOcError(c, ErrCodeNotAuthed, "请先完成认证", "", "")
				continue
			}
			handleOcMessage(c, client, rawMsg)
		case MessageTypePing:
			var ping PingMessage
			if err := json.Unmarshal(rawMsg, &ping); err == nil {
				pong := PongMessage{
					Type:      MessageTypePong,
					Timestamp: time.Now().UnixMilli(),
					Version:   "1.0",
				}
				client.mu.Lock()
				_ = c.WriteJSON(pong)
				client.mu.Unlock()
			}
		case MessageTypePong:
			var pong PongMessage
			if err := json.Unmarshal(rawMsg, &pong); err == nil {
				client.mu.Lock()
				client.LastPong = time.Now().UnixMilli()
				client.mu.Unlock()
				log.Info().Msgf("[Claw-OC] recv pong timestamp=%d", pong.Timestamp)
			}
		default:
			sendOcError(c, ErrCodeUnknownType, "未知的消息类型", "", "")
		}
	}
}

func handleOcAuth(c *websocket.Conn, client *OcClient, raw json.RawMessage) {
	var authMsg AuthMessage
	if err := json.Unmarshal(raw, &authMsg); err != nil {
		sendOcError(c, ErrCodeAuthFailed, "认证消息格式错误", "", "")
		return
	}

	if authMsg.Token == "" {
		sendOcError(c, ErrCodeAuthFailed, "token不能为空", "", "")
		return
	}

	userID, err := validateOpenClawToken(authMsg.Token)
	if err != nil {
		sendOcError(c, ErrCodeAuthFailed, "token 验证失败，请重新登录", "", "")
		return
	}
	user := &User{BanDivision: make(map[int]*time.Time), ID: userID}

	if err := user.LoadUserByID(user.ID); err != nil {
		log.Err(err).Msg("[Claw-OC] load user failed")
		sendOcError(c, ErrCodeAuthFailed, "认证失败，请稍后重试", "", "")
		return
	}

	instance, err := ResolveUserInstance(user.ID)
	if err != nil {
		sendOcError(c, ErrCodeAuthFailed, "未找到用户 OpenClaw 实例", "", "")
		return
	}
	if instance.ProviderInstanceID == "" {
		sendOcError(c, ErrCodeAuthFailed, "OpenClaw 实例尚未完成创建", "", "")
		return
	}
	if err := ocRegistry.Register(client, user.ID, instance.ID); err != nil {
		sendOcError(c, ErrCodeAuthFailed, "OpenClaw 实例连接注册失败", "", "")
		return
	}

	resp := AuthSuccessMessage{
		Type:      MessageTypeAuthSuccess,
		Timestamp: time.Now().UnixMilli(),
		Version:   "1.0",
	}
	client.mu.Lock()
	_ = c.WriteJSON(resp)
	client.mu.Unlock()
	log.Info().Msgf("[Claw-OC] auth_success userID=%d", user.ID)
}

func handleOcMessage(c *websocket.Conn, client *OcClient, raw json.RawMessage) {
	var msg ClawMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		sendOcError(c, ErrCodeUnknownType, "消息格式错误", "", "")
		return
	}

	if !client.IsAuthed || client.UserID <= 0 || client.InstanceID == 0 {
		sendOcError(c, ErrCodeNotAuthed, "请先完成认证", "", "")
		return
	}
	if msg.TaskID == "" {
		sendOcError(c, ErrCodeUnknownType, "task_id 不能为空", "", msg.SessionID)
		return
	}

	correlation, err := ResolveTaskCorrelation(DB, client.UserID, client.InstanceID, msg.TaskID, msg.SessionID)
	if err != nil {
		sendOcError(c, ErrCodeUnknownType, "消息关联不存在", msg.TaskID, msg.SessionID)
		return
	}

	var duplicate ClawMessage
	if err := DB.Where(map[string]any{
		"user_id": client.UserID, "instance_id": client.InstanceID,
		"task_id": msg.TaskID, "from": "openclaw",
	}).First(&duplicate).Error; err == nil {
		// Replies are idempotent by trusted instance/task correlation. A
		// reconnect or provider retry must not duplicate persisted replies.
		return
	} else if err != gorm.ErrRecordNotFound {
		sendOcError(c, ErrCodeInternal, "查询消息失败", msg.TaskID, msg.SessionID)
		return
	}

	// 持久化消息到数据库
	msg.From = "openclaw"
	msg.ChannelID = correlation.ChannelID
	msg.InstanceID = client.InstanceID
	msg.Timestamp = time.Now().UnixMilli()
	msg.Version = "1.0"
	if err := CreateMessage(DB, &msg); err != nil {
		log.Err(err).Msg("[Claw-OC] create message failed")
		sendOcError(c, ErrCodeInternal, "保存消息失败", msg.TaskID, msg.SessionID)
		return
	}

	// 转发到对应用户的前端客户端（如果已连接），发送给前端时去掉 session_id 字段
	if correlation.UserID == client.UserID && correlation.InstanceID == client.InstanceID {
		wsMgr := GetManager()
		clients := wsMgr.GetClientsByUserID(correlation.UserID)
		if len(clients) == 0 {
			log.Info().Msgf("[Claw-OC] frontend client for user %d not connected; message saved only", correlation.UserID)
		} else {
			for _, clientWS := range clients {
				forward := msg
				forward.SessionID = ""
				clientWS.mu.Lock()
				err := clientWS.Conn.WriteJSON(forward)
				clientWS.mu.Unlock()
				if err != nil {
					log.Err(err).Msgf("[Claw-OC] forward to frontend user %d failed", correlation.UserID)
				}
			}
		}
	}
}

func sendOcError(c *websocket.Conn, code string, errMsg string, taskID string, sessionID string) {
	// 包含 task_id 和可选的 session_id，方便 OpenClaw 定位
	payload := map[string]interface{}{
		"type":          MessageTypeError,
		"code":          code,
		"error_message": errMsg,
		"timestamp":     time.Now().UnixMilli(),
	}
	if taskID != "" {
		payload["task_id"] = taskID
	}
	if sessionID != "" {
		payload["session_id"] = sessionID
	}
	_ = c.WriteJSON(payload)
}
