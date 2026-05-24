package claw

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/opentreehole/go-common"

	. "treehole_next/models"
	"treehole_next/config"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

// clawtest
//
// @Summary Temporary Test for openclaw
// @Tags claw
// @Accept application/json
// @Produce application/json
// @Router /claw/test [post]
// @Param json body OpenClawTest true "json"
// @Failure 400 {object} MessageModel
func clawtest(c *fiber.Ctx) error {
	// get user
	user, err := GetCurrLoginUser(c)
	if err != nil {
		return err
	}

	// permission check
	if !user.IsAdmin {
		return common.Forbidden()
	}

	// Ten domain-spanning questions, pick one randomly
	greetings := []string{
		"1+1 等于几？",
		"地球为什么会有昼夜？",
		"为什么树叶会变黄？",
		"水的沸点是多少摄氏度？",
		"光速大约是多少米每秒？",
		"天空为什么是蓝色的？",
		"计算机是如何运行程序的？",
		"中国的首都是哪座城市？",
		"互联网是如何把信息传到全世界的？",
		"为什么人会做梦？",
	}

	rand.Seed(time.Now().UnixNano())
	content := greetings[rand.Intn(len(greetings))]

	// Build payload similar to handleMessage forwarding
	payload := map[string]interface{}{
		"type":       MessageTypeMessage,
		"from":       "server",
		"content":    content,
		"task_id":    fmt.Sprintf("test-%d", time.Now().UnixMilli()),
		"session_id": "1", // use session '1' as requested
		"timestamp":  time.Now().UnixMilli(),
		"media":      map[string]interface{}{},
		"version":    "1.0",
	}

	// Forward to OpenClaw gateway if connected and authed
	ocClientMu.Lock()
	target := ocClient
	ocClientMu.Unlock()

	if target != nil && target.IsAuthed {
		target.mu.Lock()
		err := target.Conn.WriteJSON(payload)
		target.mu.Unlock()
		if err != nil {
			log.Err(err).Msg("[Claw] send test to OpenClaw failed")
			return c.JSON(fiber.Map{"success": false, "error": "send_failed"})
		}
		return c.JSON(fiber.Map{"success": true, "sent": true, "content": content})
	}

	log.Warn().Msg("[Claw] no OpenClaw client connected; not forwarded")
	return c.JSON(fiber.Map{"success": false, "error": "no_openclaw"})
}

// ListChannels
//
// @Summary List Users' all channels
// @Tags Claw
// @Produce application/json
// @Router /claw/channels [get]
// @Success 200 {array} ClawSession
func ListChannels(c *fiber.Ctx) error {
	// get user
	user, err := GetCurrLoginUser(c)
	if err != nil {
		return err
	}

	sessions, err := GetSessionsByUserID(DB, user.ID)
	if err != nil {
		log.Err(err).Msg("[Claw] get sessions failed")
		return common.BadRequest("获取对话列表失败")
	}
	for _, session := range sessions {
		(*session).OC_SessionID = ""
		(*session).ID = 0
		(*session).UserID = 0
		(*session).Conversation = ""
	}
	return c.JSON(sessions)
}

// ListMessages
//
// @Summary List Users' all messages in a specific channel
// @Tags Claw
// @Produce application/json
// @Router /claw/messages [get]
// @Param object query ListClawMessageModel false "query"
// @Success 200 {array} ClawMessage
func ListMessages(c *fiber.Ctx) error {
	// get user
	user, err := GetCurrLoginUser(c)
	if err != nil {
		return err
	}
	var query ListClawMessageModel
	err = common.ValidateQuery(c, &query)
	if err != nil {
		return err
	}

	// 校验频道归属
	_, err = GetSessionByUserAndSessionID(DB, user.ID, query.ChannelID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return common.NotFound("会话不存在")
		}
		log.Err(err).Msg("[Claw] get session failed")
		return common.BadRequest("获取会话信息失败")
	}

	// 获取消息列表
	if query.Size == 0 {
		query.Size = 30
	}
	messages, err := GetMessagesByChannelID(DB, query.ChannelID, query.Size, query.Offset, query.Sort)
	if err != nil {
		log.Err(err).Msg("[Claw] get messages failed")
		return common.BadRequest("获取消息列表失败")
	}

	//洗掉ID数据
	for _, message := range messages {
		(*message).ID = 0
		// 不向前端暴露后端/网关会话 ID
		(*message).SessionID = ""
	}

	return c.JSON(messages)
}

// HandleWebSocket WebSocket连接主处理函数
func HandleWebSocket(c *websocket.Conn) {
	mgr := GetManager()

	// 初始化客户端，此时未认证
	client := &Client{
		Conn:     c,
		IsAuthed: false,
	}
	mgr.AddClient(c, client)

	// 确保退出时清理
	defer func() {
		mgr.RemoveClient(c)
		c.Close()
	}()

	// 消息读取循环
	var rawMsg json.RawMessage
	for {
		err := c.ReadJSON(&rawMsg)
		if err != nil {
			// 读取错误通常是连接断开
			log.Err(err).Msg("[Claw] WebSocket read error")
			break
		}

		// 先解析消息类型
		var base BaseMessage
		if err := json.Unmarshal(rawMsg, &base); err != nil {
			sendError(c, ErrCodeUnknownType, "消息格式错误", "", 0)
			continue
		}
		log.Info().Msgf("[Claw] WS recv type=%s raw=%s", base.Type, string(rawMsg))

		// 根据类型路由到不同处理函数
		switch base.Type {
		case MessageTypeAuth:
			handleAuth(c, client, rawMsg)
		case MessageTypeMessage:
			if !client.IsAuthed {
				sendError(c, ErrCodeNotAuthed, "请先完成认证", "", 0)
				continue
			}
			handleMessage(c, client, rawMsg)
		case MessageTypePong:
			// 收到 pong，更新客户端最后活跃时间
			client.mu.Lock()
			client.LastPong = time.Now().UnixMilli()
			client.mu.Unlock()
			log.Info().Msgf("[Claw] recv pong from conn user=%d", client.UserID)
		default:
			sendError(c, ErrCodeUnknownType, "未知的消息类型", "", 0)
		}
	}
}

// handleAuth 处理认证请求
func handleAuth(c *websocket.Conn, client *Client, rawMsg json.RawMessage) {
	var authMsg AuthMessage
	if err := json.Unmarshal(rawMsg, &authMsg); err != nil {
		sendError(c, ErrCodeAuthFailed, "认证消息格式错误", "", 0)
		return
	}
	log.Info().Msgf("[Claw] auth received token_len=%d", len(authMsg.Token))

	if authMsg.Token == "" {
		sendError(c, ErrCodeAuthFailed, "token不能为空", "", 0)
		return
	}

	// 请求 auth 服务验证 token
	userID, err := validateUserToken(authMsg.Token)
	if err != nil {
		log.Err(err).Msg("[Claw] validate token failed")
		sendError(c, ErrCodeAuthFailed, "token 验证失败，请重新登录", "", 0)
		return
	}

	user := &User{BanDivision: make(map[int]*time.Time)}
	user.ID = userID

	// 从数据库加载用户完整信息
	if err := user.LoadUserByID(user.ID); err != nil {
		log.Err(err).Msg("[Claw] load user failed")
		sendError(c, ErrCodeAuthFailed, "认证失败，请稍后重试", "", 0)
		return
	}

	count, err := GetSessionCountByUserID(DB, user.ID)
	if err != nil {
		log.Err(err).Msg("[Claw] count sessions failed")
		sendError(c, ErrCodeAuthFailed, "认证失败，请稍后重试", "", 0)
		return
	}

	client.IsAuthed = true
	client.UserID = user.ID
	client.ChannelCount = int(count)

	// 注册到 Manager 的用户索引，便于后续按 userID 查找并转发
	mgr := GetManager()
	mgr.RegisterUser(c, user.ID)

	resp := AuthSuccessMessage{
		Type:         MessageTypeAuthSuccess,
		Timestamp:    time.Now().UnixMilli(),
		ChannelCount: int(count),
		Version:      "1.0",
	}

	if err := c.WriteJSON(resp); err != nil {
		log.Err(err).Msgf("[Claw] Write auth_success error: %v", err)
		return
	}
	log.Info().Msgf("[Claw] auth_success sent userID=%d channelCount=%d", user.ID, count)
}

// handleMessage 处理业务消息
func handleMessage(c *websocket.Conn, client *Client, rawMsg json.RawMessage) {
	var msg ClawMessage
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		sendError(c, ErrCodeUnknownType, "消息格式错误", "", 0)
		return
	}
	log.Info().Msgf("[Claw] message received messageID=%s channelID=%d content_len=%d", msg.MessageID, msg.ChannelID, len(msg.Content))

	// 校验必填字段
	if msg.Content == "" {
		sendError(c, ErrCodeEmptyContent, "消息内容不能为空", msg.MessageID, msg.ChannelID)
		return
	}

	if msg.MessageID == "" {
		sendError(c, ErrCodeUnknownType, "消息ID不能为空", "", msg.ChannelID)
		return
	}

	var channelID int
	if msg.ChannelID == 0 {
		// 创建新会话
		ocSessionID := fmt.Sprintf("oc-%d-%d", client.UserID, time.Now().UnixMilli())
		session, err := CreateSession(DB, client.UserID, "新会话", ocSessionID)
		if err != nil {
			log.Err(err).Msg("[Claw] create session failed")
			sendError(c, ErrCodeInternal, "创建会话失败", msg.MessageID, 0)
			return
		}
		channelID = session.UserSessionID
	} else {
		// 检查会话是否存在
		_, err := GetSessionByUserAndSessionID(DB, client.UserID, msg.ChannelID)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				sendError(c, ErrCodeUnknownType, "会话不存在", msg.MessageID, msg.ChannelID)
			} else {
				log.Err(err).Msg("[Claw] get session failed")
				sendError(c, ErrCodeInternal, "查询会话失败", msg.MessageID, msg.ChannelID)
			}
			return
		}
		channelID = msg.ChannelID
	}

	// 获取会话以得到 OC 的 session id
	session, err := GetSessionByUserAndSessionID(DB, client.UserID, channelID)
	if err != nil {
		log.Err(err).Msg("[Claw] get session after ensure failed")
		sendError(c, ErrCodeInternal, "获取会话失败", msg.MessageID, channelID)
		return
	}

	// 标注并保存用户消息到数据库（包含 task_id 与 session_id）
	msg.From = "user"
	msg.ChannelID = channelID
	msg.Timestamp = time.Now().UnixMilli()
	msg.Version = "1.0"
	// 生成 task_id，格式包含 userID 与 channelID
	msg.TaskID = fmt.Sprintf("task_%d_%d_%d", client.UserID, channelID, time.Now().UnixMilli())
	log.Info().Msgf("[Claw] generated task_id=%s user=%d channel=%d", msg.TaskID, client.UserID, channelID)
	// session_id 应为后端/网关的 session id
	msg.SessionID = session.OC_SessionID

	if err := CreateMessage(DB, &msg); err != nil {
		log.Err(err).Msg("[Claw] create message failed")
		sendError(c, ErrCodeInternal, "保存消息失败", msg.MessageID, channelID)
		return
	}

	// 如果消息以 '#' 开头（忽略前导空白），使用本地 mock 处理（不发给 OpenClaw）
	trimmed := strings.TrimSpace(msg.Content)
	if strings.HasPrefix(trimmed, "#") {
		replyMsg := ClawMessage{
			Type:      MessageTypeMessage,
			From:      "mock_openclaw",
			Content:   "hello world",
			MessageID: fmt.Sprintf("reply-%d", time.Now().UnixMilli()),
			ChannelID: channelID,
			Timestamp: time.Now().UnixMilli(),
			Version:   "1.0",
			TaskID:    msg.TaskID,
			SessionID: "",
		}
		if err := CreateMessage(DB, &replyMsg); err != nil {
			log.Err(err).Msg("[Claw] create reply message failed")
			sendError(c, ErrCodeInternal, "保存回复失败", replyMsg.MessageID, channelID)
			return
		}
		// 发送回复给前端
		if err := c.WriteJSON(replyMsg); err != nil {
			log.Err(err).Msgf("[Claw] Write reply message error: %v", err)
			sendError(c, ErrCodeInternal, "发送回复失败", replyMsg.MessageID, channelID)
		}
		return
	}

	// 否则将消息发往 OpenClaw 网关（如果已连接且认证成功）
	ocClientMu.Lock()
	target := ocClient
	ocClientMu.Unlock()

	if target != nil && target.IsAuthed {
		if msg.TaskID == "" {
			// 防御性补充：确保发送给 OpenClaw 时包含 task_id
			msg.TaskID = fmt.Sprintf("task_%d_%d_%d", client.UserID, channelID, time.Now().UnixMilli())
			log.Warn().Msgf("[Claw] task_id was empty before send; regenerated=%s", msg.TaskID)
		}
		payload := map[string]interface{}{
			"type":       MessageTypeMessage,
			"from":       "server",
			"content":    msg.Content,
			"task_id":    msg.TaskID,
			"session_id": msg.SessionID,
			"timestamp":  msg.Timestamp,
			"media":      map[string]interface{}{},
			"version":    "1.0",
		}
		log.Info().Msgf("[Claw] forwarding to OpenClaw task_id=%s session_id=%s content_len=%d", msg.TaskID, msg.SessionID, len(msg.Content))
		target.mu.Lock()
		err := target.Conn.WriteJSON(payload)
		target.mu.Unlock()
		if err != nil {
			log.Err(err).Msg("[Claw] send to OpenClaw failed")
			// 不将错误返回前端，只记录日志；消息已落库
		}
	} else {
		log.Warn().Msg("[Claw] no OpenClaw client connected; message saved but not forwarded")
	}
}

// sendError 发送错误消息给客户端
func sendError(c *websocket.Conn, code string, errMsg string, messageID string, channelID int) {
	resp := ErrorMessage{
		Type:      MessageTypeError,
		Code:      code,
		ErrorMsg:  errMsg,
		MessageID: messageID,
		ChannelID: channelID,
		Timestamp: time.Now().UnixMilli(),
	}

	if err := c.WriteJSON(resp); err != nil {
		log.Err(err).Msgf("[Claw] Write error message failed: %v", err)
	}
}

func validateUserToken(token string) (int, error) {
	url := config.Config.AuthUrl + "/validate/user"
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("validate token failed: status %d", resp.StatusCode)
	}

	var result struct {
		UserID int `json:"user_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	return result.UserID, nil
}

